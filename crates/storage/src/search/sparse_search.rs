// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Instant;

use crate::index::PredicateTree;
use crate::rowset::SparseVector;
use crate::search::capability::SearchIndexKind;
use crate::search::cursor::{
    OpenedSearchCursor, SearchBatchState, SearchCursor, SearchReadSnapshot, VisibleSegment,
};
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

pub(crate) struct SparseSearchProvider {
    tablet: TabletRef,
    column_id: usize,
    query: SparseVector,
    k: usize,
    predicate: Option<PredicateTree>,
    telemetry: Arc<dyn SearchTelemetryCollector>,
}

impl SparseSearchProvider {
    pub(crate) fn new(
        tablet: TabletRef,
        column_id: usize,
        query: &SparseVector,
        k: usize,
        predicate: Option<PredicateTree>,
    ) -> Self {
        Self {
            tablet,
            column_id,
            query: query.clone(),
            k,
            predicate,
            telemetry: Arc::new(NoopSearchTelemetryCollector),
        }
    }

    pub(crate) fn open(self, snapshot: SearchReadSnapshot) -> OpenedSearchCursor {
        if self.k == 0 {
            return OpenedSearchCursor {
                snapshot,
                cursor: Box::new(ExhaustedSparseCursor),
            };
        }

        OpenedSearchCursor {
            snapshot: snapshot.clone(),
            cursor: Box::new(SparseSearchCursor {
                snapshot,
                tablet: self.tablet,
                storage_col_id: self.column_id as u32,
                query: self.query,
                k: self.k,
                predicate: self.predicate,
                telemetry: self.telemetry,
                state: SparseCursorState::Pending,
            }),
        }
    }
}

#[derive(Debug)]
struct ExhaustedSparseCursor;

impl SearchCursor for ExhaustedSparseCursor {
    fn next_batch(
        &mut self,
        _batch: &SearchBatchConfig,
        _budget: &mut ResourceBudget,
    ) -> Result<SearchBatchState> {
        Ok(SearchBatchState::Exhausted)
    }
}

#[derive(Debug)]
enum SparseCursorState {
    Pending,
    Ready { rows: Vec<RankedRow>, offset: usize },
    Exhausted,
}

struct SparseSearchCursor {
    snapshot: SearchReadSnapshot,
    tablet: TabletRef,
    storage_col_id: u32,
    query: SparseVector,
    k: usize,
    predicate: Option<PredicateTree>,
    telemetry: Arc<dyn SearchTelemetryCollector>,
    state: SparseCursorState,
}

impl std::fmt::Debug for SparseSearchCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SparseSearchCursor")
            .field("storage_col_id", &self.storage_col_id)
            .field("k", &self.k)
            .field("state", &self.state)
            .finish()
    }
}

impl SparseSearchCursor {
    fn build_ranked_rows(&self, budget: &ResourceBudget) -> Result<Vec<RankedRow>> {
        let started_at = Instant::now();
        self.telemetry.record_generation(GenerationTelemetryEvent {
            kind: SearchIndexKind::Sparse,
            definition_id: self.snapshot.generation.definition_id,
            generation_id: self.snapshot.generation.generation_id,
            build_epoch: self.snapshot.generation.build_epoch,
            coverage: self.snapshot.generation.coverage.clone(),
            artifact_count: self.snapshot.generation.artifacts.artifacts.len(),
        });
        let per_segment = dispatch_segments(
            SearchIndexKind::Sparse,
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
            kind: SearchIndexKind::Sparse,
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
            .sparse_index(self.storage_col_id)
            .is_some()
        {
            return visible_segment
                .segment
                .sparse_vector_search_with_epoch(
                    self.storage_col_id,
                    &self.query,
                    self.k,
                    snapshot_version,
                    self.predicate.as_ref(),
                )
                .map(|rows| {
                    (
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
                            .collect(),
                        false,
                    )
                });
        }

        let row_ids = visible_row_ids(
            visible_segment,
            self.snapshot.table.visible_version,
            self.predicate.as_ref(),
        )?;
        if row_ids.is_empty() {
            return Ok((Vec::new(), true));
        }
        let resolved = resolve_logical_rows(
            &self.tablet,
            self.snapshot.table.visible_version,
            visible_segment,
            &row_ids,
            self.storage_col_id,
        )?;
        let column = resolved
            .column(0)
            .ok_or_else(|| paro_error::internal("resolved sparse tail chunk missing column"))?;
        let mut collector = TopKCollector::new(self.k);
        for (offset, row_id) in row_ids.iter().copied().enumerate() {
            let Value::Varchar(text) = column.get_value(offset) else {
                continue;
            };
            let vector = SparseVector::parse(&text)?;
            let Some(score) = self.query.dot(&vector) else {
                continue;
            };
            collector.push(RankedRow::new(
                PhysicalRowRef::new(
                    visible_segment.rowset_id,
                    visible_segment.segment_id,
                    row_id,
                ),
                score,
            ));
        }
        Ok((collector.into_sorted_rows(), true))
    }
}

impl SearchCursor for SparseSearchCursor {
    fn next_batch(
        &mut self,
        batch: &SearchBatchConfig,
        budget: &mut ResourceBudget,
    ) -> Result<SearchBatchState> {
        if matches!(self.state, SparseCursorState::Pending) {
            let ranked_rows = self.build_ranked_rows(budget)?;
            self.state = if ranked_rows.is_empty() {
                SparseCursorState::Exhausted
            } else {
                SparseCursorState::Ready {
                    rows: ranked_rows,
                    offset: 0,
                }
            };
        }

        match &mut self.state {
            SparseCursorState::Pending => Err(paro_error::internal(
                "sparse cursor did not transition out of pending state",
            )),
            SparseCursorState::Exhausted => Ok(SearchBatchState::Exhausted),
            SparseCursorState::Ready { rows, offset } => {
                let row_limit = batch.row_limit.max(1);
                let end = (*offset + row_limit).min(rows.len());
                let candidate_batch = ranked_rows_to_batch(rows[*offset..end].to_vec());
                *offset = end;
                if *offset >= rows.len() {
                    self.state = SparseCursorState::Exhausted;
                }
                Ok(SearchBatchState::Ready(candidate_batch))
            }
        }
    }
}
