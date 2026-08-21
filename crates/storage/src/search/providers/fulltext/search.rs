// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use crate::index::fulltext::query_eval::matches_query;
use crate::index::fulltext::query_parser::ParsedQuery;
use crate::index::fulltext::scoring::score_document_from_tokens_with_stats;
use crate::index::fulltext::scoring::FullTextScoreMode;
use crate::index::fulltext::text_index::{
    FullTextIndex, FullTextScoringStats, GlobalFullTextStats,
};
use crate::index::fulltext::tokenizer::tokenizer_from_config;
use crate::index::fulltext::tokenizer::Token;
use crate::index::hnsw::ScoredPoint;
use crate::index::PredicateTree;
use crate::metrics::storage_metrics;
use crate::search::artifact::ArtifactLocation;
use crate::search::capability::SearchIndexKind;
use crate::search::cursor::{
    CandidateBatch, OpenedSearchCursor, SearchBatchState, SearchCursor, SearchReadSnapshot,
    VisibleSegment,
};
use crate::search::request::analyze_fulltext_query_stats;
use crate::search::row_fetch::snapshot_epoch;
use crate::search::segment_dispatch::{dispatch_segments, SegmentDispatchResult};
use crate::search::sidecar::{
    SidecarArtifactStore, SidecarReaderCache, SidecarReaderRequest, SIDECAR_PACKAGE_CODEC,
};
use crate::search::tail::exact_merge::{ensure_tail_exact_merge_budget, TailExactMergeQueryShape};
use crate::search::tail_merge::{resolve_logical_rows, visible_row_ids};
use crate::search::telemetry::{
    GenerationTelemetryEvent, NoopSearchTelemetryCollector, QueryTelemetryEvent,
    SearchTelemetryCollector,
};
use crate::search::topk_merge::{ranked_rows_to_batch, RankedRow, TopKCollector};
use crate::tablet::TabletRef;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;

use crate::search::budget::{ResourceBudget, SearchBatchConfig};
use crate::search::cursor::PhysicalRowRef;

pub(crate) struct FullTextTopKProvider {
    tablet: TabletRef,
    column_id: usize,
    query: ParsedQuery,
    k: usize,
    config: String,
    predicate: Option<PredicateTree>,
    global_stats: Option<GlobalFullTextStats>,
    score_mode: FullTextScoreMode,
    telemetry: Arc<dyn SearchTelemetryCollector>,
}

impl FullTextTopKProvider {
    pub(crate) fn new(
        tablet: TabletRef,
        column_id: usize,
        query: &ParsedQuery,
        k: usize,
        config: &str,
        predicate: Option<PredicateTree>,
        global_stats: Option<GlobalFullTextStats>,
        score_mode: FullTextScoreMode,
    ) -> Self {
        Self {
            tablet,
            column_id,
            query: query.clone(),
            k,
            config: config.to_string(),
            predicate,
            global_stats,
            score_mode,
            telemetry: Arc::new(NoopSearchTelemetryCollector),
        }
    }

    pub(crate) fn open(self, snapshot: SearchReadSnapshot) -> Result<OpenedSearchCursor> {
        if self.k == 0 {
            return Ok(OpenedSearchCursor {
                snapshot,
                cursor: Box::new(ExhaustedFullTextCursor),
            });
        }

        let query_terms = analyze_fulltext_query_stats(&self.query).effective_query_terms();
        ensure_tail_exact_merge_budget(
            &snapshot,
            SearchIndexKind::FullText,
            self.column_id as u32,
            TailExactMergeQueryShape::FullText {
                query_terms,
                top_k: self.k,
            },
        )?;

        Ok(OpenedSearchCursor {
            snapshot: snapshot.clone(),
            cursor: Box::new(FullTextTopKCursor {
                sidecar_cache: Arc::new(SidecarReaderCache::new(SidecarArtifactStore::new(
                    self.tablet.data_dir().clone(),
                ))),
                snapshot,
                tablet: self.tablet,
                storage_col_id: self.column_id as u32,
                query: self.query,
                k: self.k,
                config: self.config,
                predicate: self.predicate,
                global_stats: self.global_stats,
                score_mode: self.score_mode,
                telemetry: self.telemetry,
                state: FullTextTopKState::Pending,
            }),
        })
    }
}

pub(crate) struct FullTextFilterProvider {
    tablet: TabletRef,
    column_id: usize,
    query: ParsedQuery,
    config: String,
    predicate: Option<PredicateTree>,
    telemetry: Arc<dyn SearchTelemetryCollector>,
}

impl FullTextFilterProvider {
    pub(crate) fn new(
        tablet: TabletRef,
        column_id: usize,
        query: &ParsedQuery,
        config: &str,
        predicate: Option<PredicateTree>,
    ) -> Self {
        Self {
            tablet,
            column_id,
            query: query.clone(),
            config: config.to_string(),
            predicate,
            telemetry: Arc::new(NoopSearchTelemetryCollector),
        }
    }

    pub(crate) fn open(self, snapshot: SearchReadSnapshot) -> Result<OpenedSearchCursor> {
        let query_terms = analyze_fulltext_query_stats(&self.query).effective_query_terms();
        ensure_tail_exact_merge_budget(
            &snapshot,
            SearchIndexKind::FullText,
            self.column_id as u32,
            TailExactMergeQueryShape::FullText {
                query_terms,
                top_k: 0,
            },
        )?;

        Ok(OpenedSearchCursor {
            snapshot: snapshot.clone(),
            cursor: Box::new(FullTextFilterCursor {
                sidecar_cache: Arc::new(SidecarReaderCache::new(SidecarArtifactStore::new(
                    self.tablet.data_dir().clone(),
                ))),
                snapshot,
                tablet: self.tablet,
                storage_col_id: self.column_id as u32,
                query: self.query,
                config: self.config,
                predicate: self.predicate,
                telemetry: self.telemetry,
                next_segment: 0,
                pending_rows: VecDeque::new(),
                rows_emitted: 0,
                candidates_produced: 0,
                started_at: None,
                finished: false,
            }),
        })
    }
}

#[derive(Debug)]
struct ExhaustedFullTextCursor;

impl SearchCursor for ExhaustedFullTextCursor {
    fn next_batch(
        &mut self,
        _batch: &SearchBatchConfig,
        _budget: &mut ResourceBudget,
    ) -> Result<SearchBatchState> {
        Ok(SearchBatchState::Exhausted)
    }
}

#[derive(Debug)]
enum FullTextTopKState {
    Pending,
    Ready { rows: Vec<RankedRow>, offset: usize },
    Exhausted,
}

struct FullTextTopKCursor {
    sidecar_cache: Arc<SidecarReaderCache>,
    snapshot: SearchReadSnapshot,
    tablet: TabletRef,
    storage_col_id: u32,
    query: ParsedQuery,
    k: usize,
    config: String,
    predicate: Option<PredicateTree>,
    global_stats: Option<GlobalFullTextStats>,
    score_mode: FullTextScoreMode,
    telemetry: Arc<dyn SearchTelemetryCollector>,
    state: FullTextTopKState,
}

impl std::fmt::Debug for FullTextTopKCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FullTextTopKCursor")
            .field("storage_col_id", &self.storage_col_id)
            .field("k", &self.k)
            .field("state", &self.state)
            .finish()
    }
}

#[derive(Debug, Default)]
struct FullTextQueryScoringTerms {
    terms: BTreeSet<String>,
    prefixes: BTreeSet<String>,
}

impl FullTextQueryScoringTerms {
    fn from_query(query: &ParsedQuery) -> Self {
        let mut terms = Self::default();
        terms.collect(query);
        terms
    }

    fn collect(&mut self, query: &ParsedQuery) {
        match query {
            ParsedQuery::Term(term) => {
                self.terms.insert(term.clone());
            }
            ParsedQuery::Prefix(prefix) => {
                self.prefixes.insert(prefix.clone());
            }
            ParsedQuery::Phrase(terms) => {
                self.terms.extend(terms.iter().cloned());
            }
            ParsedQuery::FollowedBy(items, _)
            | ParsedQuery::And(items)
            | ParsedQuery::Or(items) => {
                for item in items {
                    self.collect(item);
                }
            }
            ParsedQuery::Not(_) => {}
        }
    }

    fn matches_term(&self, term: &str) -> bool {
        self.terms.contains(term) || self.prefixes.iter().any(|prefix| term.starts_with(prefix))
    }
}

#[derive(Debug)]
struct FullTextScoringStatsBuilder {
    global: GlobalFullTextStats,
    include_index_totals: bool,
    term_doc_freqs: BTreeMap<String, u32>,
    degraded_reasons: BTreeSet<&'static str>,
}

impl FullTextScoringStatsBuilder {
    fn new(base_global_stats: Option<GlobalFullTextStats>) -> Self {
        let mut degraded_reasons = BTreeSet::new();
        if base_global_stats.is_none() {
            degraded_reasons.insert("missing_generation_stats");
        }
        Self {
            global: base_global_stats.unwrap_or_else(|| GlobalFullTextStats::from_totals(0, 0)),
            include_index_totals: base_global_stats.is_none(),
            term_doc_freqs: BTreeMap::new(),
            degraded_reasons,
        }
    }

    fn add_index(&mut self, index: &FullTextIndex, query_terms: &FullTextQueryScoringTerms) {
        let inverted = index.inverted_index();
        if self.include_index_totals {
            self.global = self
                .global
                .with_added_totals(inverted.total_docs(), inverted.total_terms());
        }

        let mut local_doc_freqs = BTreeMap::<String, u32>::new();
        for term in &query_terms.terms {
            if let Some(list) = inverted.get_posting_list(term) {
                local_doc_freqs.insert(term.clone(), list.len() as u32);
            }
        }
        for prefix in &query_terms.prefixes {
            for (term, list) in inverted.postings().range(prefix.clone()..) {
                if !term.starts_with(prefix) {
                    break;
                }
                local_doc_freqs.insert(term.clone(), list.len() as u32);
            }
        }
        for (term, doc_freq) in local_doc_freqs {
            self.add_doc_freq(term, doc_freq);
        }
    }

    fn add_tail_document(&mut self, tokens: &[Token], query_terms: &FullTextQueryScoringTerms) {
        self.global = self
            .global
            .with_added_totals(1, u64::try_from(tokens.len()).unwrap_or(u64::MAX));
        let mut seen = BTreeSet::<&str>::new();
        for token in tokens {
            if query_terms.matches_term(&token.term) {
                seen.insert(token.term.as_str());
            }
        }
        for term in seen {
            self.add_doc_freq(term.to_string(), 1);
        }
    }

    fn add_doc_freq(&mut self, term: String, doc_freq: u32) {
        let entry = self.term_doc_freqs.entry(term).or_default();
        *entry = entry.saturating_add(doc_freq);
    }

    fn finish(self) -> FullTextScoringSnapshot {
        FullTextScoringSnapshot {
            stats: FullTextScoringStats::with_term_doc_freqs(self.global, self.term_doc_freqs),
            degraded_reasons: self.degraded_reasons.into_iter().collect(),
        }
    }
}

#[derive(Debug)]
struct FullTextScoringSnapshot {
    stats: FullTextScoringStats,
    degraded_reasons: Vec<&'static str>,
}

fn open_sidecar_fulltext_index(
    snapshot: &SearchReadSnapshot,
    cache: &SidecarReaderCache,
    visible_segment: &VisibleSegment,
    column_id: u32,
) -> Result<Option<FullTextIndex>> {
    let Some(artifact) =
        snapshot.artifact_for_segment(SearchIndexKind::FullText, column_id, visible_segment)
    else {
        return Ok(None);
    };
    if !matches!(
        artifact.location,
        ArtifactLocation::SidecarArtifactFile { .. }
    ) {
        return Ok(None);
    }

    let cached = cache.open(SidecarReaderRequest {
        location: &artifact.location,
        artifact_format_version: artifact.artifact_format_version,
        provider: SearchIndexKind::FullText,
        codec: SIDECAR_PACKAGE_CODEC,
    })?;
    FullTextIndex::deserialize(cached.bytes()).map(Some)
}

fn fulltext_ranked_rows_from_points(
    snapshot: &SearchReadSnapshot,
    visible_segment: &VisibleSegment,
    points: Vec<ScoredPoint>,
) -> Vec<RankedRow> {
    if snapshot.has_overlay_delete_vectors() {
        points
            .into_iter()
            .filter_map(|point| {
                let row = PhysicalRowRef::new(
                    visible_segment.rowset_id,
                    visible_segment.segment_id,
                    crate::rowset::SegmentRowId::from_raw(point.idx as u32),
                );
                (!snapshot.is_overlay_deleted(row)).then(|| RankedRow::new(row, point.score))
            })
            .collect()
    } else {
        points
            .into_iter()
            .map(|point| {
                RankedRow::new(
                    PhysicalRowRef::new(
                        visible_segment.rowset_id,
                        visible_segment.segment_id,
                        crate::rowset::SegmentRowId::from_raw(point.idx as u32),
                    ),
                    point.score,
                )
            })
            .collect()
    }
}

fn fulltext_rows_from_bitmap(
    snapshot: &SearchReadSnapshot,
    visible_segment: &VisibleSegment,
    bitmap: roaring::RoaringBitmap,
) -> Vec<PhysicalRowRef> {
    let mut rows = Vec::with_capacity(bitmap.len() as usize);
    if snapshot.has_overlay_delete_vectors() {
        for row_id in bitmap.iter() {
            let row = PhysicalRowRef::new(
                visible_segment.rowset_id,
                visible_segment.segment_id,
                crate::rowset::SegmentRowId::from_raw(row_id),
            );
            if !snapshot.is_overlay_deleted(row) {
                rows.push(row);
            }
        }
    } else {
        for row_id in bitmap.iter() {
            rows.push(PhysicalRowRef::new(
                visible_segment.rowset_id,
                visible_segment.segment_id,
                crate::rowset::SegmentRowId::from_raw(row_id),
            ));
        }
    }
    rows
}

impl FullTextTopKCursor {
    fn build_ranked_rows(&self, budget: &ResourceBudget) -> Result<Vec<RankedRow>> {
        let started_at = Instant::now();
        let scoring_snapshot = self.build_scoring_stats()?;
        for reason in &scoring_snapshot.degraded_reasons {
            storage_metrics()
                .record_search_fulltext_degraded_score(self.snapshot.table.table_id, reason);
        }
        self.telemetry.record_generation(GenerationTelemetryEvent {
            kind: SearchIndexKind::FullText,
            definition_id: self.snapshot.generation.definition_id,
            generation_id: self.snapshot.generation.generation_id,
            build_epoch: self.snapshot.generation.build_epoch,
            coverage: self.snapshot.generation.coverage.clone(),
            artifact_count: self.snapshot.generation.artifacts.artifacts.len(),
        });
        let per_segment = dispatch_segments(
            SearchIndexKind::FullText,
            self.snapshot.table_lease.visible_segments(),
            budget.parallelism_slots.max(1),
            self.telemetry.as_ref(),
            |segment| {
                let (rows, degraded) = self.search_segment(segment, &scoring_snapshot.stats)?;
                Ok(SegmentDispatchResult {
                    candidates_produced: rows.len(),
                    degraded,
                    output: (rows, degraded),
                })
            },
        )?;

        let mut collector = TopKCollector::new(self.k);
        let mut candidates_produced = 0usize;
        let mut degraded_segments = 0usize;
        for (rows, degraded) in per_segment {
            candidates_produced += rows.len();
            degraded_segments += usize::from(degraded);
            collector.extend(rows);
        }
        let peak_heap_items = collector.len();
        let ranked_rows = collector.into_sorted_rows();
        self.telemetry.record_query(QueryTelemetryEvent {
            kind: SearchIndexKind::FullText,
            segments_searched: self.snapshot.table_lease.visible_segment_count(),
            candidates_produced,
            rows_returned: ranked_rows.len(),
            peak_heap_items,
            degraded_segments,
            degraded_score_reasons: scoring_snapshot
                .degraded_reasons
                .iter()
                .map(|reason| (*reason).to_string())
                .collect(),
            elapsed: started_at.elapsed(),
        });
        Ok(ranked_rows)
    }

    fn build_scoring_stats(&self) -> Result<FullTextScoringSnapshot> {
        let query_terms = FullTextQueryScoringTerms::from_query(&self.query);
        let mut builder = FullTextScoringStatsBuilder::new(self.global_stats);
        let (_kind, tokenizer) = tokenizer_from_config(&self.config)?;
        for visible_segment in self.snapshot.table_lease.visible_segments() {
            if let Some(index) = visible_segment.segment.fulltext_index(self.storage_col_id) {
                builder.add_index(index.as_ref(), &query_terms);
                continue;
            }
            if let Some(index) = open_sidecar_fulltext_index(
                &self.snapshot,
                self.sidecar_cache.as_ref(),
                visible_segment,
                self.storage_col_id,
            )? {
                builder.add_index(&index, &query_terms);
                continue;
            }

            let row_ids = visible_row_ids(&self.snapshot, visible_segment, None)?;
            if row_ids.is_empty() {
                continue;
            }
            let resolved = resolve_logical_rows(
                &self.tablet,
                &self.snapshot,
                visible_segment,
                &row_ids,
                self.storage_col_id,
            )?;
            let column = resolved.column(0).ok_or_else(|| {
                paro_error::internal("resolved fulltext tail stats chunk missing column")
            })?;
            for offset in 0..row_ids.len() {
                let Value::Varchar(text) = column.get_value(offset) else {
                    continue;
                };
                let tokens = tokenizer.tokenize_to_vec(&text);
                builder.add_tail_document(&tokens, &query_terms);
            }
        }
        Ok(builder.finish())
    }

    fn search_segment(
        &self,
        visible_segment: &VisibleSegment,
        scoring_stats: &FullTextScoringStats,
    ) -> Result<(Vec<RankedRow>, bool)> {
        let snapshot_version = snapshot_epoch(self.snapshot.table.visible_version);
        if visible_segment
            .segment
            .fulltext_index(self.storage_col_id)
            .is_some()
        {
            return visible_segment
                .segment
                .fulltext_search_with_epoch(
                    self.storage_col_id,
                    &self.query,
                    self.k,
                    snapshot_version,
                    self.predicate.as_ref(),
                    Some(scoring_stats),
                    self.score_mode,
                )
                .map(|rows| {
                    let ranked_rows =
                        fulltext_ranked_rows_from_points(&self.snapshot, visible_segment, rows);
                    (ranked_rows, false)
                });
        }
        if let Some(index) = open_sidecar_fulltext_index(
            &self.snapshot,
            self.sidecar_cache.as_ref(),
            visible_segment,
            self.storage_col_id,
        )? {
            let filter_bitmap = visible_segment
                .segment
                .build_filter_bitmap_with_epoch(snapshot_version, self.predicate.as_ref())?;
            if filter_bitmap
                .as_ref()
                .is_some_and(|bitmap| bitmap.is_empty())
            {
                return Ok((Vec::new(), false));
            }
            let rows = index.search(
                &self.query,
                self.k,
                filter_bitmap.as_ref(),
                Some(scoring_stats),
                self.score_mode,
            );
            return Ok((
                fulltext_ranked_rows_from_points(&self.snapshot, visible_segment, rows),
                false,
            ));
        }

        let row_ids = visible_row_ids(&self.snapshot, visible_segment, self.predicate.as_ref())?;
        if row_ids.is_empty() {
            return Ok((Vec::new(), true));
        }
        let resolved = resolve_logical_rows(
            &self.tablet,
            &self.snapshot,
            visible_segment,
            &row_ids,
            self.storage_col_id,
        )?;
        let column = resolved
            .column(0)
            .ok_or_else(|| paro_error::internal("resolved fulltext tail chunk missing column"))?;
        let (_kind, tokenizer) = tokenizer_from_config(&self.config)?;
        let mut collector = TopKCollector::new(self.k);
        let bm25 = scoring_stats.bm25();
        for (offset, row_id) in row_ids.iter().copied().enumerate() {
            let Value::Varchar(text) = column.get_value(offset) else {
                continue;
            };
            let tokens = tokenizer.tokenize_to_vec(&text);
            if !matches_query(&tokens, &self.query) {
                continue;
            }
            collector.push(RankedRow::new(
                PhysicalRowRef::new(
                    visible_segment.rowset_id,
                    visible_segment.segment_id,
                    crate::rowset::SegmentRowId::from_raw(row_id),
                ),
                score_document_from_tokens_with_stats(
                    self.score_mode,
                    &tokens,
                    &self.query,
                    &bm25,
                    scoring_stats,
                ),
            ));
        }
        Ok((collector.into_sorted_rows(), true))
    }
}

impl SearchCursor for FullTextTopKCursor {
    fn next_batch(
        &mut self,
        batch: &SearchBatchConfig,
        budget: &mut ResourceBudget,
    ) -> Result<SearchBatchState> {
        if matches!(self.state, FullTextTopKState::Pending) {
            let ranked_rows = self.build_ranked_rows(budget)?;
            self.state = if ranked_rows.is_empty() {
                FullTextTopKState::Exhausted
            } else {
                FullTextTopKState::Ready {
                    rows: ranked_rows,
                    offset: 0,
                }
            };
        }

        match &mut self.state {
            FullTextTopKState::Pending => Err(paro_error::internal(
                "fulltext top-k cursor did not transition out of pending state",
            )),
            FullTextTopKState::Exhausted => Ok(SearchBatchState::Exhausted),
            FullTextTopKState::Ready { rows, offset } => {
                let row_limit = batch.row_limit.max(1);
                let end = (*offset + row_limit).min(rows.len());
                let candidate_batch = ranked_rows_to_batch(rows[*offset..end].to_vec());
                *offset = end;
                if *offset >= rows.len() {
                    self.state = FullTextTopKState::Exhausted;
                }
                Ok(SearchBatchState::Ready(candidate_batch))
            }
        }
    }
}

struct FullTextFilterCursor {
    sidecar_cache: Arc<SidecarReaderCache>,
    snapshot: SearchReadSnapshot,
    tablet: TabletRef,
    storage_col_id: u32,
    query: ParsedQuery,
    config: String,
    predicate: Option<PredicateTree>,
    telemetry: Arc<dyn SearchTelemetryCollector>,
    next_segment: usize,
    pending_rows: VecDeque<PhysicalRowRef>,
    rows_emitted: usize,
    candidates_produced: usize,
    started_at: Option<Instant>,
    finished: bool,
}

impl std::fmt::Debug for FullTextFilterCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FullTextFilterCursor")
            .field("storage_col_id", &self.storage_col_id)
            .field("next_segment", &self.next_segment)
            .field("pending_rows", &self.pending_rows.len())
            .field("rows_emitted", &self.rows_emitted)
            .field("finished", &self.finished)
            .finish()
    }
}

impl FullTextFilterCursor {
    fn dispatch_next_segment_window(&mut self, budget: &ResourceBudget) -> Result<()> {
        let segments = self.snapshot.table_lease.visible_segments();
        if self.next_segment >= segments.len() {
            return Ok(());
        }

        let window_width = budget.parallelism_slots.max(1);
        let window_end = (self.next_segment + window_width).min(segments.len());
        let window = &segments[self.next_segment..window_end];
        self.next_segment = window_end;

        let matches = dispatch_segments(
            SearchIndexKind::FullText,
            window,
            window_width,
            self.telemetry.as_ref(),
            |segment| {
                let (rows, degraded) = self.search_segment_rows(segment)?;
                Ok(SegmentDispatchResult {
                    candidates_produced: rows.len(),
                    degraded,
                    output: rows,
                })
            },
        )?;

        for rows in matches {
            self.candidates_produced += rows.len();
            self.pending_rows.extend(rows);
        }
        Ok(())
    }

    fn search_segment_rows(&self, segment: &VisibleSegment) -> Result<(Vec<PhysicalRowRef>, bool)> {
        let snapshot_version = snapshot_epoch(self.snapshot.table.visible_version);
        if segment
            .segment
            .fulltext_index(self.storage_col_id)
            .is_some()
        {
            let bitmap = segment.segment.fulltext_filter_with_epoch(
                self.storage_col_id,
                &self.query,
                snapshot_version,
                self.predicate.as_ref(),
            )?;
            return Ok((
                fulltext_rows_from_bitmap(&self.snapshot, segment, bitmap),
                false,
            ));
        }
        if let Some(index) = open_sidecar_fulltext_index(
            &self.snapshot,
            self.sidecar_cache.as_ref(),
            segment,
            self.storage_col_id,
        )? {
            let filter_bitmap = segment
                .segment
                .build_filter_bitmap_with_epoch(snapshot_version, self.predicate.as_ref())?;
            if filter_bitmap
                .as_ref()
                .is_some_and(|bitmap| bitmap.is_empty())
            {
                return Ok((Vec::new(), false));
            }
            let bitmap = index.filter(&self.query, filter_bitmap.as_ref());
            return Ok((
                fulltext_rows_from_bitmap(&self.snapshot, segment, bitmap),
                false,
            ));
        }

        let row_ids = visible_row_ids(&self.snapshot, segment, self.predicate.as_ref())?;
        if row_ids.is_empty() {
            return Ok((Vec::new(), true));
        }
        let resolved = resolve_logical_rows(
            &self.tablet,
            &self.snapshot,
            segment,
            &row_ids,
            self.storage_col_id,
        )?;
        let column = resolved.column(0).ok_or_else(|| {
            paro_error::internal("resolved fulltext filter tail chunk missing column")
        })?;
        let (_kind, tokenizer) = tokenizer_from_config(&self.config)?;
        let mut rows = Vec::new();
        for (offset, row_id) in row_ids.iter().copied().enumerate() {
            let Value::Varchar(text) = column.get_value(offset) else {
                continue;
            };
            let tokens = tokenizer.tokenize_to_vec(&text);
            if matches_query(&tokens, &self.query) {
                rows.push(PhysicalRowRef::new(
                    segment.rowset_id,
                    segment.segment_id,
                    crate::rowset::SegmentRowId::from_raw(row_id),
                ));
            }
        }
        Ok((rows, true))
    }

    fn finish_if_needed(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        let elapsed = self
            .started_at
            .map(|started_at| started_at.elapsed())
            .unwrap_or_default();
        self.telemetry.record_query(QueryTelemetryEvent {
            kind: SearchIndexKind::FullText,
            segments_searched: self.snapshot.table_lease.visible_segment_count(),
            candidates_produced: self.candidates_produced,
            rows_returned: self.rows_emitted,
            peak_heap_items: 0,
            degraded_segments: 0,
            degraded_score_reasons: Vec::new(),
            elapsed,
        });
    }
}

impl SearchCursor for FullTextFilterCursor {
    fn next_batch(
        &mut self,
        batch: &SearchBatchConfig,
        budget: &mut ResourceBudget,
    ) -> Result<SearchBatchState> {
        if self.started_at.is_none() {
            self.started_at = Some(Instant::now());
            self.telemetry.record_generation(GenerationTelemetryEvent {
                kind: SearchIndexKind::FullText,
                definition_id: self.snapshot.generation.definition_id,
                generation_id: self.snapshot.generation.generation_id,
                build_epoch: self.snapshot.generation.build_epoch,
                coverage: self.snapshot.generation.coverage.clone(),
                artifact_count: self.snapshot.generation.artifacts.artifacts.len(),
            });
        }

        let row_limit = batch.row_limit.max(1);
        let mut rows = Vec::with_capacity(row_limit);

        loop {
            while rows.len() < row_limit {
                let Some(row) = self.pending_rows.pop_front() else {
                    break;
                };
                rows.push(row);
            }

            if rows.len() == row_limit {
                self.rows_emitted += rows.len();
                return Ok(SearchBatchState::Ready(CandidateBatch {
                    rows,
                    scores: Vec::new(),
                }));
            }

            if self.next_segment >= self.snapshot.table_lease.visible_segment_count() {
                if rows.is_empty() {
                    self.finish_if_needed();
                    return Ok(SearchBatchState::Exhausted);
                }
                self.rows_emitted += rows.len();
                if self.pending_rows.is_empty() {
                    self.finish_if_needed();
                }
                return Ok(SearchBatchState::Ready(CandidateBatch {
                    rows,
                    scores: Vec::new(),
                }));
            }

            self.dispatch_next_segment_window(budget)?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::fulltext::text_index::FullTextIndexConfig;
    use crate::index::fulltext::tokenizer::TokenizerKind;
    use crate::search::capability::{ArtifactSegmentRef, SearchArtifactRef};
    use crate::search::cursor::{
        GenerationArtifactSet, GenerationReadLease, GenerationReadSnapshot, TableReadLease,
    };
    use crate::search::stats::{GenerationMaintenanceState, GenerationStats, SearchArtifactStats};
    use crate::search::CoverageState;
    use crate::table::table_factory::TableFactory;
    use crate::test_utils::{test_chunk_from_vectors, test_string_vector};
    use paro_common::types::LogicalType;

    #[test]
    #[serial_test::serial]
    fn fulltext_topk_cursor_reads_sidecar_artifact_through_reader_cache() {
        let _metrics_guard = crate::metrics::storage_metrics_test_guard();
        storage_metrics().reset_for_tests();
        let table = TableFactory::default()
            .create_table(&[LogicalType::Varchar])
            .expect("create table");
        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "alpha beta",
                "gamma",
            ])]))
            .expect("append rows");

        let (table_snapshot, table_lease) =
            TableReadLease::open(&table.tablet(), table.tablet_id(), table.max_version())
                .expect("open table lease");
        let visible_segment = table_lease
            .visible_segments()
            .first()
            .expect("visible segment");
        assert!(visible_segment.segment.fulltext_index(0).is_none());

        let mut index = FullTextIndex::new_with_tokenizer_kind(
            TokenizerKind::Default,
            FullTextIndexConfig::default(),
        );
        index.add_document(0, "alpha beta").unwrap();
        index.add_document(1, "gamma").unwrap();
        let query = index.parse_query("alpha").unwrap();
        let bytes = index.serialize().unwrap();

        let store = SidecarArtifactStore::new(table.tablet().data_dir().clone());
        let file_id = SidecarArtifactStore::default_shard_file_id(7, 1);
        let mut writer = store.create_package_writer(file_id).unwrap();
        let location = writer.append_artifact(&bytes).unwrap();
        writer.finalize().unwrap();

        let artifact = SearchArtifactRef {
            definition_id: 7,
            generation_id: 1,
            segment: ArtifactSegmentRef {
                rowset_id: visible_segment.rowset_id,
                segment_id: visible_segment.segment_id,
            },
            column_id: 0,
            kind: SearchIndexKind::FullText,
            provider_variant: 1,
            artifact_format_version: 1,
            location,
            stats: SearchArtifactStats {
                row_count: 2,
                bytes_on_disk: bytes.len() as u64,
                provider_stats: None,
            },
            checksum: seahash::hash(&bytes),
        };
        let generation = GenerationReadSnapshot {
            definition_id: 7,
            generation_id: 1,
            build_epoch: 1,
            build_snapshot_version: table.max_version(),
            indexed_through_ts: table.max_version() as u64,
            coverage: CoverageState::Complete,
            generation_stats: GenerationStats::default(),
            maintenance_state: GenerationMaintenanceState::default(),
            artifacts: Arc::new(GenerationArtifactSet {
                artifacts: vec![artifact],
            }),
        };
        let generation_lease = GenerationReadLease::from_snapshot(&generation);
        let snapshot = SearchReadSnapshot::new(
            table_snapshot,
            SearchIndexKind::FullText,
            generation,
            table_lease,
            generation_lease,
        );

        let opened = FullTextTopKProvider::new(
            table.tablet(),
            0,
            &query,
            10,
            "simple",
            None,
            Some(GlobalFullTextStats::from_totals(2, 3)),
            FullTextScoreMode::Bm25,
        )
        .open(snapshot)
        .unwrap();
        let mut cursor = opened.cursor;
        let mut budget = ResourceBudget::default();
        let batch = match cursor
            .next_batch(&SearchBatchConfig::default(), &mut budget)
            .unwrap()
        {
            SearchBatchState::Ready(batch) => batch,
            SearchBatchState::Exhausted => panic!("expected sidecar result"),
        };

        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.rows[0].row_offset, 0);
        let metrics = storage_metrics().snapshot();
        let sidecar = metrics
            .search_sidecar_reader_by_key
            .iter()
            .find(|series| series.key.provider == SearchIndexKind::FullText)
            .expect("fulltext sidecar reader metrics");
        assert_eq!(sidecar.counters.open_count_total, 1);
        assert_eq!(sidecar.counters.cache_misses_total, 1);
        assert!(sidecar.counters.format_dispatch_total >= 1);
    }
}
