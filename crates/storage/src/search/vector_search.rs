// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Instant;

use crate::index::hnsw::types::SearchParams;
use crate::index::hnsw::{DistanceMetric, PreparedQuery};
use crate::index::PredicateTree;
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
use crate::tablet::{ColumnId, TabletRef};
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;

use super::budget::{ResourceBudget, SearchBatchConfig};
use super::cursor::PhysicalRowRef;

pub(crate) struct VectorSearchProvider {
    tablet: TabletRef,
    column_types: Vec<LogicalType>,
    column_id: usize,
    query: Vec<f32>,
    k: usize,
    params: SearchParams,
    predicate: Option<PredicateTree>,
    telemetry: Arc<dyn SearchTelemetryCollector>,
}

impl VectorSearchProvider {
    pub(crate) fn new(
        tablet: TabletRef,
        column_types: &[LogicalType],
        column_id: usize,
        query: &[f32],
        k: usize,
        params: SearchParams,
        predicate: Option<PredicateTree>,
    ) -> Self {
        Self {
            tablet,
            column_types: column_types.to_vec(),
            column_id,
            query: query.to_vec(),
            k,
            params,
            predicate,
            telemetry: Arc::new(NoopSearchTelemetryCollector),
        }
    }

    pub(crate) fn open(self, snapshot: SearchReadSnapshot) -> Result<OpenedSearchCursor> {
        if self.k == 0 {
            return Ok(OpenedSearchCursor {
                snapshot,
                cursor: Box::new(ExhaustedVectorCursor),
            });
        }

        let vector_dim = resolve_vector_dim(&self.column_types, self.column_id)?;
        validate_query_dim(&self.query, vector_dim)?;
        let distance = resolve_distance_metric(&self.tablet, self.column_id);
        let prepared_query = distance.prepare(&self.query);
        let cursor = VectorSearchCursor {
            snapshot: snapshot.clone(),
            tablet: self.tablet,
            query: self.query,
            prepared_query,
            k: self.k,
            storage_col_id: self.column_id as u32,
            vector_dim,
            distance,
            params: self.params,
            predicate: self.predicate,
            telemetry: self.telemetry,
            state: VectorCursorState::Pending,
        };
        Ok(OpenedSearchCursor {
            snapshot,
            cursor: Box::new(cursor),
        })
    }
}

#[derive(Debug)]
struct ExhaustedVectorCursor;

impl SearchCursor for ExhaustedVectorCursor {
    fn next_batch(
        &mut self,
        _batch: &SearchBatchConfig,
        _budget: &mut ResourceBudget,
    ) -> Result<SearchBatchState> {
        Ok(SearchBatchState::Exhausted)
    }
}

#[derive(Debug)]
enum VectorCursorState {
    Pending,
    Ready { rows: Vec<RankedRow>, offset: usize },
    Exhausted,
}

struct VectorSearchCursor {
    snapshot: SearchReadSnapshot,
    tablet: TabletRef,
    query: Vec<f32>,
    prepared_query: PreparedQuery,
    k: usize,
    storage_col_id: u32,
    vector_dim: usize,
    distance: DistanceMetric,
    params: SearchParams,
    predicate: Option<PredicateTree>,
    telemetry: Arc<dyn SearchTelemetryCollector>,
    state: VectorCursorState,
}

impl std::fmt::Debug for VectorSearchCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorSearchCursor")
            .field("storage_col_id", &self.storage_col_id)
            .field("vector_dim", &self.vector_dim)
            .field("k", &self.k)
            .field("state", &self.state)
            .finish()
    }
}

impl VectorSearchCursor {
    fn build_ranked_rows(&self, budget: &ResourceBudget) -> Result<Vec<RankedRow>> {
        let started_at = Instant::now();
        self.telemetry.record_generation(GenerationTelemetryEvent {
            kind: SearchIndexKind::Hnsw,
            definition_id: self.snapshot.generation.definition_id,
            generation_id: self.snapshot.generation.generation_id,
            build_epoch: self.snapshot.generation.build_epoch,
            coverage: self.snapshot.generation.coverage.clone(),
            artifact_count: self.snapshot.generation.artifacts.artifacts.len(),
        });
        let per_segment = dispatch_segments(
            SearchIndexKind::Hnsw,
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
            kind: SearchIndexKind::Hnsw,
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
            .hnsw_index(self.storage_col_id)
            .is_some()
        {
            return visible_segment
                .segment
                .vector_search_with_epoch(
                    self.storage_col_id,
                    &self.query,
                    self.k,
                    &self.params,
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
            .ok_or_else(|| paro_error::internal("resolved vector tail chunk missing column"))?;
        let mut collector = TopKCollector::new(self.k);
        let mut decoded = vec![0.0_f32; self.vector_dim];
        for (offset, row_id) in row_ids.iter().copied().enumerate() {
            let value = column.get_value(offset);
            let Value::Array(values, _, _) = value else {
                continue;
            };
            if values.len() != self.vector_dim {
                continue;
            }
            for (idx, value) in values.into_iter().enumerate() {
                decoded[idx] = match value {
                    Value::Float(v) => v,
                    Value::Double(v) => v as f32,
                    Value::TinyInt(v) => v as f32,
                    Value::SmallInt(v) => v as f32,
                    Value::Integer(v) => v as f32,
                    Value::BigInt(v) => v as f32,
                    Value::UTinyInt(v) => v as f32,
                    Value::USmallInt(v) => v as f32,
                    Value::UInteger(v) => v as f32,
                    Value::UBigInt(v) => v as f32,
                    _ => continue,
                };
            }
            collector.push(RankedRow::new(
                PhysicalRowRef::new(
                    visible_segment.rowset_id,
                    visible_segment.segment_id,
                    row_id,
                ),
                self.distance
                    .similarity(self.prepared_query.as_slice(), &decoded),
            ));
        }

        Ok((collector.into_sorted_rows(), true))
    }
}

impl SearchCursor for VectorSearchCursor {
    fn next_batch(
        &mut self,
        batch: &SearchBatchConfig,
        budget: &mut ResourceBudget,
    ) -> Result<SearchBatchState> {
        if matches!(self.state, VectorCursorState::Pending) {
            let ranked_rows = self.build_ranked_rows(budget)?;
            self.state = if ranked_rows.is_empty() {
                VectorCursorState::Exhausted
            } else {
                VectorCursorState::Ready {
                    rows: ranked_rows,
                    offset: 0,
                }
            };
        }

        match &mut self.state {
            VectorCursorState::Pending => Err(paro_error::internal(
                "vector cursor did not transition out of pending state",
            )),
            VectorCursorState::Exhausted => Ok(SearchBatchState::Exhausted),
            VectorCursorState::Ready { rows, offset } => {
                let row_limit = batch.row_limit.max(1);
                let end = (*offset + row_limit).min(rows.len());
                let candidate_batch = ranked_rows_to_batch(rows[*offset..end].to_vec());
                *offset = end;
                if *offset >= rows.len() {
                    self.state = VectorCursorState::Exhausted;
                }
                Ok(SearchBatchState::Ready(candidate_batch))
            }
        }
    }
}

fn resolve_vector_dim(column_types: &[LogicalType], column_id: usize) -> Result<usize> {
    match column_types.get(column_id) {
        Some(LogicalType::Array(inner, dim)) if matches!(**inner, LogicalType::Float) => Ok(*dim),
        other => Err(paro_error::invalid_input(format!(
            "Column {} is not a dense vector column: {:?}",
            column_id, other
        ))),
    }
}

fn validate_query_dim(query: &[f32], vector_dim: usize) -> Result<()> {
    if query.len() != vector_dim {
        return Err(paro_error::invalid_input(format!(
            "Query vector dimension mismatch: expected {}, got {}",
            vector_dim,
            query.len()
        )));
    }
    Ok(())
}

fn resolve_distance_metric(tablet: &TabletRef, column_id: usize) -> DistanceMetric {
    tablet
        .schema()
        .and_then(|schema| {
            schema
                .column_by_id(column_id as ColumnId)
                .map(|column| DistanceMetric::from_u8(column.hnsw_distance))
        })
        .unwrap_or(DistanceMetric::Euclidean)
}
