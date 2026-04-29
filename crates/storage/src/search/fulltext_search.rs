// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use crate::index::fulltext::query_eval::matches_query;
use crate::index::fulltext::query_parser::ParsedQuery;
use crate::index::fulltext::scoring::score_document_from_tokens;
use crate::index::fulltext::scoring::FullTextScoreMode;
use crate::index::fulltext::text_index::GlobalFullTextStats;
use crate::index::fulltext::tokenizer::tokenizer_from_config;
use crate::index::PredicateTree;
use crate::search::capability::SearchIndexKind;
use crate::search::cursor::{
    CandidateBatch, OpenedSearchCursor, SearchBatchState, SearchCursor, SearchReadSnapshot,
    VisibleSegment,
};
use crate::search::delta_merge::{ensure_search_delta_merge_budget, DeltaMergeQueryShape};
use crate::search::request::analyze_fulltext_query_stats;
use crate::search::row_fetch::snapshot_epoch;
use crate::search::segment_dispatch::{dispatch_segments, SegmentDispatchResult};
use crate::search::tail_merge::{resolve_logical_rows, visible_row_ids};
use crate::search::telemetry::{
    GenerationTelemetryEvent, NoopSearchTelemetryCollector, QueryTelemetryEvent,
    SearchTelemetryCollector,
};
use crate::search::topk_merge::{ranked_rows_to_batch, RankedRow, TopKCollector};
use crate::tablet::TabletRef;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;

use super::budget::{ResourceBudget, SearchBatchConfig};
use super::cursor::PhysicalRowRef;

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
        ensure_search_delta_merge_budget(
            &snapshot,
            SearchIndexKind::FullText,
            self.column_id as u32,
            DeltaMergeQueryShape::FullText {
                query_terms,
                top_k: self.k,
            },
        )?;

        Ok(OpenedSearchCursor {
            snapshot: snapshot.clone(),
            cursor: Box::new(FullTextTopKCursor {
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
        ensure_search_delta_merge_budget(
            &snapshot,
            SearchIndexKind::FullText,
            self.column_id as u32,
            DeltaMergeQueryShape::FullText {
                query_terms,
                top_k: 0,
            },
        )?;

        Ok(OpenedSearchCursor {
            snapshot: snapshot.clone(),
            cursor: Box::new(FullTextFilterCursor {
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

impl FullTextTopKCursor {
    fn build_ranked_rows(&self, budget: &ResourceBudget) -> Result<Vec<RankedRow>> {
        let started_at = Instant::now();
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
                let (rows, degraded) = self.search_segment(segment)?;
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
            elapsed: started_at.elapsed(),
        });
        Ok(ranked_rows)
    }

    fn search_segment(&self, visible_segment: &VisibleSegment) -> Result<(Vec<RankedRow>, bool)> {
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
                    self.global_stats.as_ref(),
                    self.score_mode,
                )
                .map(|rows| {
                    let ranked_rows = if self.snapshot.has_overlay_delete_vectors() {
                        rows.into_iter()
                            .filter_map(|point| {
                                let row = PhysicalRowRef::new(
                                    visible_segment.rowset_id,
                                    visible_segment.segment_id,
                                    point.idx as u32,
                                );
                                (!self.snapshot.is_overlay_deleted(row))
                                    .then(|| RankedRow::new(row, point.score))
                            })
                            .collect()
                    } else {
                        rows.into_iter()
                            .map(|point| {
                                RankedRow::new(
                                    PhysicalRowRef::new(
                                        visible_segment.rowset_id,
                                        visible_segment.segment_id,
                                        point.idx as u32,
                                    ),
                                    point.score,
                                )
                            })
                            .collect()
                    };
                    (ranked_rows, false)
                });
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
                    row_id,
                ),
                score_document_from_tokens(self.score_mode, &tokens, &self.query),
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
            let mut rows = Vec::with_capacity(bitmap.len() as usize);
            if self.snapshot.has_overlay_delete_vectors() {
                for row_id in bitmap.iter() {
                    let row = PhysicalRowRef::new(segment.rowset_id, segment.segment_id, row_id);
                    if !self.snapshot.is_overlay_deleted(row) {
                        rows.push(row);
                    }
                }
            } else {
                for row_id in bitmap.iter() {
                    rows.push(PhysicalRowRef::new(
                        segment.rowset_id,
                        segment.segment_id,
                        row_id,
                    ));
                }
            }
            return Ok((rows, false));
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
                    row_id,
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
