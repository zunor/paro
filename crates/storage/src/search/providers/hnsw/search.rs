// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Instant;

use crate::index::hnsw::types::SearchParams;
use crate::index::hnsw::{
    hnsw_artifact_compatibility, DistanceMetric, HnswArtifactCompatibility, HnswBuildContract,
    HnswFilterKind, HnswIndex, HnswSearchFilter, HnswSearchPolicy, HnswSearchStrategy,
    PreparedQuery,
};
use crate::index::ExactRowSet;
use crate::index::MmapVectorStorage;
use crate::index::PredicateTree;
use crate::rowset::encoding::PLAIN_PAGE_HEADER_SIZE;
use crate::search::artifact::ArtifactLocation;
use crate::search::capability::SearchIndexKind;
use crate::search::cursor::{
    OpenedSearchCursor, SearchBatchState, SearchCursor, SearchReadSnapshot, VisibleSegment,
};
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
use paro_common::types::LogicalType;

use crate::search::budget::{ResourceBudget, SearchBatchConfig};
use crate::search::cursor::PhysicalRowRef;

pub(crate) struct VectorSearchProvider {
    tablet: TabletRef,
    column_types: Vec<LogicalType>,
    column_id: usize,
    query: Vec<f32>,
    distance: DistanceMetric,
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
        distance: DistanceMetric,
        k: usize,
        params: SearchParams,
        predicate: Option<PredicateTree>,
    ) -> Self {
        Self {
            tablet,
            column_types: column_types.to_vec(),
            column_id,
            query: query.to_vec(),
            distance,
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
        ensure_tail_exact_merge_budget(
            &snapshot,
            SearchIndexKind::Hnsw,
            self.column_id as u32,
            TailExactMergeQueryShape::Hnsw {
                dimension: vector_dim,
                ef: self.params.ef,
                top_k: self.k,
            },
        )?;
        let provider_config = snapshot
            .generation
            .hnsw_provider_config
            .clone()
            .ok_or_else(|| {
                paro_error::data_corrupted(
                    "HNSW generation is missing its validated provider contract",
                )
            })?;
        if provider_config.distance != self.distance {
            return Err(paro_error::invalid_input(format!(
                "HNSW distance mismatch: query uses {}, index uses {}",
                self.distance.durable_name(),
                provider_config.distance.durable_name()
            )));
        }
        let distance = self.distance;
        let prepared_query = distance.prepare(&self.query);
        let expected_build_contract = provider_config.build_contract();
        let search_policy = provider_config.search_policy();
        let cursor = VectorSearchCursor {
            sidecar_cache: Arc::new(SidecarReaderCache::new(SidecarArtifactStore::new(
                self.tablet.data_dir().clone(),
            ))),
            snapshot: snapshot.clone(),
            tablet: self.tablet,
            query: self.query,
            prepared_query,
            k: self.k,
            storage_col_id: self.column_id as u32,
            vector_dim,
            distance,
            expected_build_contract,
            search_policy,
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
    sidecar_cache: Arc<SidecarReaderCache>,
    snapshot: SearchReadSnapshot,
    tablet: TabletRef,
    query: Vec<f32>,
    prepared_query: PreparedQuery,
    k: usize,
    storage_col_id: u32,
    vector_dim: usize,
    distance: DistanceMetric,
    expected_build_contract: HnswBuildContract,
    search_policy: HnswSearchPolicy,
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
        let predicate = self.predicate.as_ref();
        let snapshot_version = snapshot_epoch(self.snapshot.table.visible_version);
        let total_rows = self
            .snapshot
            .table_lease
            .visible_segments()
            .iter()
            .fold(0u64, |rows, segment| {
                rows.saturating_add(segment.segment.num_rows())
            });
        let unfiltered_strategy = HnswSearchStrategy::choose(
            HnswFilterKind::None,
            total_rows,
            total_rows,
            self.search_policy,
        );
        let per_segment = dispatch_segments(
            SearchIndexKind::Hnsw,
            self.snapshot.table_lease.visible_segments(),
            budget.parallelism_slots.max(1),
            self.telemetry.as_ref(),
            |_, segment| {
                let filter_row_set = segment
                    .segment
                    .build_hnsw_filter_with_epoch(snapshot_version, predicate)?;
                if predicate.is_some() && filter_row_set.is_none() {
                    return Err(paro_error::internal(
                        "filtered vector search did not prepare an exact segment row set",
                    ));
                }
                let _reservation = filter_row_set
                    .as_ref()
                    .map(|row_set| budget.try_reserve_memory(row_set.query_retained_bytes()))
                    .transpose()?;
                let filter_row_set = filter_row_set.as_deref();
                let filter = match (predicate.is_some(), filter_row_set) {
                    (_, None) => HnswSearchFilter::None,
                    (true, Some(row_set)) => HnswSearchFilter::Predicate(row_set),
                    (false, Some(row_set)) => HnswSearchFilter::Visibility(row_set),
                };
                let search_strategy = match filter.kind() {
                    HnswFilterKind::None => unfiltered_strategy,
                    kind => HnswSearchStrategy::choose(
                        kind,
                        filter
                            .row_set()
                            .map_or(segment.segment.num_rows(), ExactRowSet::len),
                        segment.segment.num_rows(),
                        self.search_policy,
                    ),
                };
                let (rows, degraded) =
                    self.search_segment(segment, search_strategy, filter, budget.work.as_ref())?;
                let degraded_reason = degraded
                    .then(|| segment.segment.hnsw_rebuild_reason(self.storage_col_id))
                    .flatten();
                Ok(SegmentDispatchResult {
                    candidates_produced: rows.len(),
                    degraded,
                    output: (rows, degraded, degraded_reason),
                })
            },
        )?;

        let mut collector = TopKCollector::new(self.k);
        let mut candidates_produced = 0usize;
        let mut degraded_segments = 0usize;
        let mut degraded_score_reasons = Vec::new();
        for (rows, degraded, degraded_reason) in per_segment {
            candidates_produced += rows.len();
            degraded_segments += usize::from(degraded);
            if let Some(reason) = degraded_reason {
                if !degraded_score_reasons.contains(&reason) {
                    degraded_score_reasons.push(reason);
                }
            }
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
            degraded_score_reasons,
            elapsed: started_at.elapsed(),
        });
        Ok(ranked_rows)
    }

    fn search_segment(
        &self,
        visible_segment: &VisibleSegment,
        search_strategy: HnswSearchStrategy,
        filter: HnswSearchFilter<'_>,
        work: &crate::search::SearchWorkBudget,
    ) -> Result<(Vec<RankedRow>, bool)> {
        if let Some(index) = visible_segment
            .segment
            .open_hnsw_index(self.storage_col_id)?
        {
            validate_hnsw_index_contract(index.as_ref(), &self.expected_build_contract)?;
            return visible_segment
                .segment
                .vector_search_with_filter_strategy(
                    self.storage_col_id,
                    &self.query,
                    self.k,
                    &self.params,
                    filter,
                    &self.search_policy,
                    search_strategy,
                    work,
                )
                .map(|result| {
                    let rows = result.points;
                    let ranked_rows = if self.snapshot.has_overlay_delete_vectors() {
                        rows.into_iter()
                            .filter_map(|point| {
                                let row = PhysicalRowRef::new(
                                    visible_segment.rowset_id,
                                    visible_segment.segment_id,
                                    crate::rowset::SegmentRowId::from_raw(point.idx as u32),
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
                                        crate::rowset::SegmentRowId::from_raw(point.idx as u32),
                                    ),
                                    point.score,
                                )
                            })
                            .collect()
                    };
                    (ranked_rows, false)
                });
        }

        if let Some(index) = open_sidecar_hnsw_index(
            &self.snapshot,
            self.sidecar_cache.as_ref(),
            visible_segment,
            self.storage_col_id,
            self.vector_dim,
        )? {
            validate_hnsw_index_contract(&index, &self.expected_build_contract)?;
            if filter.row_set().is_some_and(ExactRowSet::is_empty) {
                return Ok((Vec::new(), false));
            }
            return index
                .search_one_with_policy_strategy(
                    &self.query,
                    self.k,
                    &self.params,
                    filter,
                    &self.search_policy,
                    search_strategy,
                    work,
                )
                .map(|result| {
                    (
                        hnsw_ranked_rows_from_points(
                            &self.snapshot,
                            visible_segment,
                            result.points,
                        ),
                        false,
                    )
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
                    crate::rowset::SegmentRowId::from_raw(row_id),
                ),
                self.distance
                    .similarity_unindexed(self.prepared_query.as_slice(), &decoded),
            ));
        }

        Ok((collector.into_sorted_rows(), true))
    }
}

fn validate_hnsw_index_contract(index: &HnswIndex, expected: &HnswBuildContract) -> Result<()> {
    if index.build_contract != *expected {
        return Err(paro_error::data_corrupted(format!(
            "HNSW artifact build contract mismatch: artifact={:?}, definition={:?}",
            index.build_contract, expected
        )));
    }
    Ok(())
}

fn open_sidecar_hnsw_index(
    snapshot: &SearchReadSnapshot,
    cache: &SidecarReaderCache,
    visible_segment: &VisibleSegment,
    column_id: u32,
    vector_dim: usize,
) -> Result<Option<HnswIndex>> {
    let Some(artifact) =
        snapshot.artifact_for_segment(SearchIndexKind::Hnsw, column_id, visible_segment)
    else {
        return Ok(None);
    };
    if !matches!(
        artifact.location,
        ArtifactLocation::SidecarArtifactFile { .. }
    ) {
        return Ok(None);
    }

    let column_meta = visible_segment
        .segment
        .get_column_meta(column_id)
        .ok_or_else(|| {
            paro_error::column_not_found(format!(
                "column {} not found in segment {}",
                column_id, visible_segment.segment_id
            ))
        })?;
    let vector_storage = Arc::new(MmapVectorStorage::open_range(
        visible_segment.segment.file_path(),
        column_meta.data_page_pointer.offset + PLAIN_PAGE_HEADER_SIZE as u64,
        column_meta.num_rows * vector_dim as u64 * std::mem::size_of::<f32>() as u64,
        vector_dim,
    )?);
    let cached = cache.open(SidecarReaderRequest {
        location: &artifact.location,
        artifact_format_version: artifact.artifact_format_version,
        provider: SearchIndexKind::Hnsw,
        codec: SIDECAR_PACKAGE_CODEC,
    })?;
    if !matches!(
        hnsw_artifact_compatibility(cached.bytes())?,
        HnswArtifactCompatibility::Current
    ) {
        return Ok(None);
    }
    let (mmap, offset, len) = cached.mmap_range();
    HnswIndex::deserialize_mmap_range(mmap, offset, len, vector_storage).map(Some)
}

fn hnsw_ranked_rows_from_points(
    snapshot: &SearchReadSnapshot,
    visible_segment: &VisibleSegment,
    points: Vec<crate::index::hnsw::ScoredPoint>,
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
                let mut candidate_batch = ranked_rows_to_batch(rows[*offset..end].to_vec());
                for score in &mut candidate_batch.scores {
                    *score = self.distance.postprocess(*score);
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unfiltered_plain_scan_threshold_is_query_wide() {
        assert_eq!(
            HnswSearchStrategy::choose(
                HnswFilterKind::None,
                10_000,
                10_000,
                HnswSearchPolicy {
                    plain_scan_threshold: 10_000,
                    ..HnswSearchPolicy::default()
                },
            ),
            HnswSearchStrategy::ExactScan
        );
        assert_eq!(
            HnswSearchStrategy::choose(
                HnswFilterKind::None,
                10_001,
                10_001,
                HnswSearchPolicy {
                    plain_scan_threshold: 10_000,
                    ..HnswSearchPolicy::default()
                },
            ),
            HnswSearchStrategy::UnfilteredGraph
        );
    }

    #[test]
    fn filtered_plain_scan_threshold_is_query_wide() {
        let policy = HnswSearchPolicy {
            filtered_plain_scan_threshold: 20_000,
            ..HnswSearchPolicy::default()
        };
        assert_eq!(
            HnswSearchStrategy::choose(HnswFilterKind::Predicate, 20_000, 1_000_000, policy,),
            HnswSearchStrategy::ExactScan
        );
        assert_eq!(
            HnswSearchStrategy::choose(HnswFilterKind::Predicate, 20_001, 1_000_000, policy,),
            HnswSearchStrategy::AdaptiveFilteredGraph
        );
        assert_eq!(
            HnswSearchStrategy::choose(HnswFilterKind::Predicate, 500_000, 1_000_000, policy,),
            HnswSearchStrategy::AdaptiveFilteredGraph
        );
    }
}
