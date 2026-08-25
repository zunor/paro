// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

/// Minimum exact vector payload needed to amortize a cross-thread segment
/// phase. This is expressed as physical bytes rather than rows so the same
/// scheduling rule scales with vector dimension. Graph traversal remains
/// parallel because its random navigation latency is not proportional to the
/// number of predicate matches.
const MIN_PARALLEL_EXACT_VECTOR_BYTES: u64 = 2 * 1024 * 1024;

use crate::index::hnsw::types::SearchParams;
use crate::index::hnsw::{
    hnsw_artifact_compatibility, DistanceMetric, HnswArtifactCompatibility, HnswBuildContract,
    HnswFilterKind, HnswIndex, HnswQueryWideStrategy, HnswSearchFilter, HnswSearchPolicy,
    HnswSearchStrategy, HnswSegmentSearchInput, PreparedQuery,
};
use crate::index::PredicateTree;
use crate::index::{DenseRowSet, ExactRowSet, PartitionExactRowSet};
use crate::search::artifact::ArtifactLocation;
use crate::search::capability::{ArtifactSegmentRef, SearchArtifactRef, SearchIndexKind};
use crate::search::cursor::{
    OpenedSearchCursor, SearchBatchState, SearchCursor, SearchReadSnapshot, VisibleSegment,
};
use crate::search::row_fetch::snapshot_epoch;
use crate::search::segment_dispatch::{
    dispatch_segments, map_search_tasks, map_segments, SegmentDispatchResult,
};
use crate::search::sidecar::{
    DecodedSidecarReaderRequest, SearchReaderRuntime, SidecarIntegrityPolicy, SidecarReaderRequest,
    SIDECAR_PACKAGE_CODEC,
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

use crate::search::budget::{ResourceBudget, SearchBatchConfig, SearchMemoryReservation};
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

struct PreparedSegmentFilter {
    row_set: Option<Arc<dyn ExactRowSet>>,
    _reservation: Option<SearchMemoryReservation>,
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
        let reader_runtime = Arc::clone(&snapshot.reader_runtime);
        let cursor = VectorSearchCursor {
            reader_runtime,
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
    reader_runtime: Arc<SearchReaderRuntime>,
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
        let predicate_columns = predicate
            .map(crate::index::collect_predicate_columns)
            .unwrap_or_default();
        let snapshot_version = snapshot_epoch(self.snapshot.table.visible_version);
        let visible_segments = self.snapshot.table_lease.visible_segments();
        let total_rows = visible_segments.iter().fold(0u64, |rows, segment| {
            rows.saturating_add(segment.segment.num_rows())
        });
        let parallelism_slots = budget.parallelism_slots.max(1);
        let effective_ef = self.search_policy.effective_ef(self.k, self.params.ef);
        let level0_degree = self.expected_build_contract.m0 as usize;

        // Exact segment row sets are prepared once and retained at the query
        // boundary. Both singleton and generation-owned partition artifacts
        // borrow the same proof objects, so changing the physical partition
        // envelope cannot change predicate semantics.
        let prepared_filters = map_segments(
            visible_segments,
            parallelism_slots,
            |(_, segment)| -> Result<PreparedSegmentFilter> {
                let row_set = segment
                    .segment
                    .build_hnsw_filter_with_epoch(snapshot_version, predicate)?;
                if predicate.is_some() && row_set.is_none() {
                    return Err(paro_error::internal(
                        "filtered vector search did not prepare an exact segment row set",
                    ));
                }
                let reservation = row_set
                    .as_ref()
                    .map(|row_set| budget.try_reserve_memory(row_set.query_retained_bytes()))
                    .transpose()?;
                Ok(PreparedSegmentFilter {
                    row_set,
                    _reservation: reservation,
                })
            },
        )?;
        let predicate_matching_rows = predicate.map(|_| {
            prepared_filters.iter().fold(0u64, |rows, filter| {
                rows.saturating_add(filter.row_set.as_ref().map_or(0, |row_set| row_set.len()))
            })
        });
        let query_wide_strategy = match predicate_matching_rows {
            Some(matching_rows) => HnswQueryWideStrategy::choose(
                HnswFilterKind::Predicate,
                matching_rows,
                total_rows,
                self.search_policy,
            ),
            None => HnswQueryWideStrategy::choose(
                HnswFilterKind::None,
                total_rows,
                total_rows,
                self.search_policy,
            ),
        };
        let search_parallelism_slots = exact_search_parallelism_slots(
            parallelism_slots,
            query_wide_strategy,
            predicate_matching_rows.unwrap_or(total_rows),
            self.vector_dim,
        );

        let visible_by_ref = visible_segments
            .iter()
            .enumerate()
            .map(|(index, segment)| {
                (
                    ArtifactSegmentRef {
                        rowset_id: segment.rowset_id,
                        segment_id: segment.segment_id,
                    },
                    index,
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut partition_artifacts = Vec::<(&SearchArtifactRef, Arc<HnswIndex>)>::new();
        for artifact in &self.snapshot.generation.artifacts.artifacts {
            if artifact.kind != SearchIndexKind::Hnsw
                || artifact.column_id != self.storage_col_id
                || artifact.coverage.segments().len() <= 1
                || !matches!(
                    artifact.location,
                    ArtifactLocation::SidecarArtifactFile { .. }
                )
                || !artifact
                    .coverage
                    .segments()
                    .iter()
                    .all(|span| visible_by_ref.contains_key(&span.segment))
            {
                continue;
            }
            let Some(index) = open_sidecar_hnsw_artifact(
                self.reader_runtime.as_ref(),
                artifact,
                self.vector_dim,
            )?
            else {
                // An unsupported generation artifact is recoverable. Keep its
                // base segments on the exact tail path rather than making the
                // table unreadable or silently omitting their rows.
                continue;
            };
            validate_hnsw_index_contract(index.as_ref(), &self.expected_build_contract)?;
            let indexed_rows = u64::try_from(index.vector_storage.num_vectors())
                .map_err(|_| paro_error::out_of_range("HNSW vector count exceeds u64"))?;
            if indexed_rows != artifact.coverage.row_count() {
                return Err(paro_error::data_corrupted(format!(
                    "HNSW partition coverage has {} rows but artifact contains {indexed_rows} vectors",
                    artifact.coverage.row_count()
                )));
            }
            partition_artifacts.push((artifact, index));
        }
        let partition_segments = partition_artifacts
            .iter()
            .flat_map(|(artifact, _)| artifact.coverage.segments().iter().map(|span| span.segment))
            .collect::<BTreeSet<_>>();

        let per_segment = dispatch_segments(
            SearchIndexKind::Hnsw,
            visible_segments,
            search_parallelism_slots,
            self.telemetry.as_ref(),
            |index, segment| {
                if partition_segments.contains(&ArtifactSegmentRef {
                    rowset_id: segment.rowset_id,
                    segment_id: segment.segment_id,
                }) {
                    return Ok(SegmentDispatchResult {
                        candidates_produced: 0,
                        degraded: false,
                        output: (Vec::new(), false, None),
                    });
                }
                let row_set = prepared_filters[index].row_set.as_deref();
                let filter = match (predicate.is_some(), row_set) {
                    (true, Some(row_set)) => {
                        HnswSearchFilter::predicate(row_set, &predicate_columns)
                    }
                    (false, Some(row_set)) => HnswSearchFilter::Visibility(row_set),
                    (false, None) => HnswSearchFilter::None,
                    (true, None) => {
                        return Err(paro_error::internal(
                            "predicate filter disappeared after preparation",
                        ))
                    }
                };
                self.dispatch_segment_search(
                    segment,
                    query_wide_strategy,
                    filter,
                    effective_ef,
                    level0_degree,
                    budget,
                )
            },
        )?;

        let per_partition = map_search_tasks(
            &partition_artifacts,
            search_parallelism_slots,
            |_, (artifact, index)| {
                self.search_partition_artifact(
                    artifact,
                    index.as_ref(),
                    &visible_by_ref,
                    &prepared_filters,
                    predicate.is_some(),
                    &predicate_columns,
                    query_wide_strategy,
                    effective_ef,
                    level0_degree,
                    budget,
                )
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
        for rows in per_partition {
            candidates_produced += rows.len();
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

    fn dispatch_segment_search(
        &self,
        segment: &VisibleSegment,
        query_wide_strategy: HnswQueryWideStrategy,
        filter: HnswSearchFilter<'_>,
        effective_ef: usize,
        level0_degree: usize,
        budget: &ResourceBudget,
    ) -> Result<SegmentDispatchResult<(Vec<RankedRow>, bool, Option<String>)>> {
        let matching_rows = filter
            .row_set()
            .map_or(segment.segment.num_rows(), ExactRowSet::len);
        let search_strategy = query_wide_strategy.for_segment(HnswSegmentSearchInput {
            filter_kind: filter.kind(),
            matching_rows,
            total_rows: segment.segment.num_rows(),
            effective_ef,
            level0_degree,
            vector_dimension: self.vector_dim,
            parallelism: budget.parallelism_slots,
        });
        let (rows, degraded) = self.search_segment(segment, search_strategy, filter, budget)?;
        let degraded_reason = degraded
            .then(|| segment.segment.hnsw_rebuild_reason(self.storage_col_id))
            .flatten();
        Ok(SegmentDispatchResult {
            candidates_produced: rows.len(),
            degraded,
            output: (rows, degraded, degraded_reason),
        })
    }

    fn search_partition_artifact(
        &self,
        artifact: &SearchArtifactRef,
        index: &HnswIndex,
        visible_by_ref: &BTreeMap<ArtifactSegmentRef, usize>,
        prepared_filters: &[PreparedSegmentFilter],
        has_predicate: bool,
        predicate_columns: &[u32],
        query_wide_strategy: HnswQueryWideStrategy,
        effective_ef: usize,
        level0_degree: usize,
        budget: &ResourceBudget,
    ) -> Result<Vec<RankedRow>> {
        let visible_segments = self.snapshot.table_lease.visible_segments();
        let has_visibility_filter = artifact.coverage.segments().iter().any(|span| {
            visible_by_ref
                .get(&span.segment)
                .and_then(|index| prepared_filters.get(*index))
                .is_some_and(|filter| filter.row_set.is_some())
        });
        let filter_kind = if has_predicate {
            HnswFilterKind::Predicate
        } else if has_visibility_filter {
            HnswFilterKind::Visibility
        } else {
            HnswFilterKind::None
        };

        let partition_row_set = if filter_kind == HnswFilterKind::None {
            None
        } else {
            let mut parts = Vec::<(std::ops::Range<u32>, Arc<dyn ExactRowSet>)>::with_capacity(
                artifact.coverage.segments().len(),
            );
            let mut point_base = 0u32;
            for span in artifact.coverage.segments() {
                let visible_index = *visible_by_ref.get(&span.segment).ok_or_else(|| {
                    paro_error::data_corrupted(format!(
                        "HNSW partition references invisible segment {}/{}",
                        span.segment.rowset_id, span.segment.segment_id
                    ))
                })?;
                let visible_segment = visible_segments.get(visible_index).ok_or_else(|| {
                    paro_error::internal("visible HNSW segment index is out of bounds")
                })?;
                if visible_segment.segment.num_rows() != span.row_count {
                    return Err(paro_error::data_corrupted(format!(
                        "HNSW partition segment {}/{} coverage has {} rows, visible segment has {}",
                        span.segment.rowset_id,
                        span.segment.segment_id,
                        span.row_count,
                        visible_segment.segment.num_rows()
                    )));
                }
                let span_rows = u32::try_from(span.row_count).map_err(|_| {
                    paro_error::out_of_range("HNSW partition span exceeds u32 row-id domain")
                })?;
                let point_end = point_base.checked_add(span_rows).ok_or_else(|| {
                    paro_error::out_of_range("HNSW partition point-id range overflow")
                })?;
                let row_set = match &prepared_filters[visible_index].row_set {
                    Some(row_set) => Arc::clone(row_set),
                    None if has_predicate => {
                        return Err(paro_error::internal(
                            "predicate filter disappeared while composing HNSW partition",
                        ))
                    }
                    None => Arc::new(DenseRowSet::new(span_rows)),
                };
                parts.push((point_base..point_end, row_set));
                point_base = point_end;
            }
            let row_set: Arc<dyn ExactRowSet> = Arc::new(PartitionExactRowSet::try_new(parts)?);
            Some(row_set)
        };
        let _partition_reservation = partition_row_set
            .as_ref()
            .map(|row_set| budget.try_reserve_memory(row_set.query_retained_bytes()))
            .transpose()?;
        let filter = match (filter_kind, partition_row_set.as_deref()) {
            (HnswFilterKind::None, None) => HnswSearchFilter::None,
            (HnswFilterKind::Visibility, Some(row_set)) => HnswSearchFilter::Visibility(row_set),
            (HnswFilterKind::Predicate, Some(row_set)) => {
                HnswSearchFilter::predicate(row_set, predicate_columns)
            }
            _ => {
                return Err(paro_error::internal(
                    "HNSW partition filter kind and exact row set disagree",
                ))
            }
        };
        if filter.row_set().is_some_and(ExactRowSet::is_empty) {
            return Ok(Vec::new());
        }
        let matching_rows = filter
            .row_set()
            .map_or(artifact.coverage.row_count(), ExactRowSet::len);
        let strategy = query_wide_strategy.for_segment(HnswSegmentSearchInput {
            filter_kind,
            matching_rows,
            total_rows: artifact.coverage.row_count(),
            effective_ef,
            level0_degree,
            vector_dimension: self.vector_dim,
            parallelism: budget.parallelism_slots,
        });
        let points = index
            .search_one_with_policy_strategy(
                &self.query,
                self.k,
                &self.params,
                filter,
                &self.search_policy,
                strategy,
                budget,
            )?
            .points;
        points
            .into_iter()
            .filter_map(|point| {
                let Some(point_ref) = artifact.coverage.resolve_point(point.idx) else {
                    return Some(Err(paro_error::data_corrupted(format!(
                        "HNSW point {} exceeds partition coverage",
                        point.idx
                    ))));
                };
                let Some(&visible_index) = visible_by_ref.get(&point_ref.segment) else {
                    return Some(Err(paro_error::data_corrupted(
                        "HNSW result references an invisible segment",
                    )));
                };
                let Some(visible_segment) = visible_segments.get(visible_index) else {
                    return Some(Err(paro_error::internal(
                        "visible HNSW result segment index is out of bounds",
                    )));
                };
                if u64::from(point_ref.row_offset) >= visible_segment.segment.num_rows() {
                    return Some(Err(paro_error::data_corrupted(format!(
                        "HNSW result row {} exceeds segment {}/{} cardinality {}",
                        point_ref.row_offset,
                        point_ref.segment.rowset_id,
                        point_ref.segment.segment_id,
                        visible_segment.segment.num_rows()
                    ))));
                }
                let row = PhysicalRowRef::new(
                    point_ref.segment.rowset_id,
                    point_ref.segment.segment_id,
                    crate::rowset::SegmentRowId::from_raw(point_ref.row_offset),
                );
                (!self.snapshot.is_overlay_deleted(row))
                    .then(|| Ok(RankedRow::new(row, point.score)))
            })
            .collect()
    }

    fn search_segment(
        &self,
        visible_segment: &VisibleSegment,
        search_strategy: HnswSearchStrategy,
        filter: HnswSearchFilter<'_>,
        budget: &ResourceBudget,
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
                    budget,
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
            self.reader_runtime.as_ref(),
            visible_segment,
            self.storage_col_id,
            self.vector_dim,
        )? {
            validate_hnsw_index_contract(index.as_ref(), &self.expected_build_contract)?;
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
                    budget,
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

fn exact_search_parallelism_slots(
    granted_slots: usize,
    strategy: HnswQueryWideStrategy,
    matching_rows: u64,
    vector_dim: usize,
) -> usize {
    let granted_slots = granted_slots.max(1);
    if granted_slots == 1 || strategy != HnswQueryWideStrategy::ExactScan {
        return granted_slots;
    }
    let vector_bytes = matching_rows
        .saturating_mul(vector_dim as u64)
        .saturating_mul(std::mem::size_of::<f32>() as u64);
    if vector_bytes < MIN_PARALLEL_EXACT_VECTOR_BYTES {
        1
    } else {
        granted_slots
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
    runtime: &SearchReaderRuntime,
    visible_segment: &VisibleSegment,
    column_id: u32,
    vector_dim: usize,
) -> Result<Option<Arc<HnswIndex>>> {
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

    open_sidecar_hnsw_artifact(runtime, artifact, vector_dim)
}

fn open_sidecar_hnsw_artifact(
    runtime: &SearchReaderRuntime,
    artifact: &SearchArtifactRef,
    vector_dim: usize,
) -> Result<Option<Arc<HnswIndex>>> {
    let request = DecodedSidecarReaderRequest {
        sidecar: SidecarReaderRequest {
            location: &artifact.location,
            artifact_format_version: artifact.artifact_format_version,
            provider: SearchIndexKind::Hnsw,
            codec: SIDECAR_PACKAGE_CODEC,
            integrity: SidecarIntegrityPolicy::SelfValidatingArtifact,
        },
    };
    runtime.get_or_try_open_decoded(request, |cached| {
        if !matches!(
            hnsw_artifact_compatibility(cached.bytes())?,
            HnswArtifactCompatibility::Current
        ) {
            return Ok(None);
        }
        let (mmap, offset, len) = cached.mmap_range();
        let index = HnswIndex::deserialize_mmap_range(mmap, offset, len)?;
        if index.vector_storage.vector_dim() != vector_dim {
            return Err(paro_error::data_corrupted(format!(
                "HNSW artifact dimension mismatch: expected {vector_dim}, got {}",
                index.vector_storage.vector_dim()
            )));
        }
        Ok(Some(index))
    })
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
    fn exact_segment_dispatch_requires_enough_vector_work() {
        let rows_below_two_mib_at_32d =
            MIN_PARALLEL_EXACT_VECTOR_BYTES / (32 * std::mem::size_of::<f32>() as u64) - 1;
        assert_eq!(
            exact_search_parallelism_slots(
                8,
                HnswQueryWideStrategy::ExactScan,
                rows_below_two_mib_at_32d,
                32,
            ),
            1
        );
        assert_eq!(
            exact_search_parallelism_slots(
                8,
                HnswQueryWideStrategy::ExactScan,
                rows_below_two_mib_at_32d + 1,
                32,
            ),
            8
        );
        assert_eq!(
            exact_search_parallelism_slots(8, HnswQueryWideStrategy::SegmentAdaptive, 1, 32,),
            8
        );
    }

    #[test]
    fn unfiltered_plain_scan_threshold_is_query_wide() {
        assert_eq!(
            HnswQueryWideStrategy::choose(
                HnswFilterKind::None,
                10_000,
                10_000,
                HnswSearchPolicy {
                    plain_scan_threshold: 10_000,
                    ..HnswSearchPolicy::default()
                },
            ),
            HnswQueryWideStrategy::ExactScan
        );
        assert_eq!(
            HnswQueryWideStrategy::choose(
                HnswFilterKind::None,
                10_001,
                10_001,
                HnswSearchPolicy {
                    plain_scan_threshold: 10_000,
                    ..HnswSearchPolicy::default()
                },
            ),
            HnswQueryWideStrategy::SegmentAdaptive
        );
    }

    #[test]
    fn filtered_plain_scan_threshold_is_query_wide() {
        let policy = HnswSearchPolicy {
            filtered_plain_scan_threshold: 20_000,
            ..HnswSearchPolicy::default()
        };
        assert_eq!(
            HnswQueryWideStrategy::choose(HnswFilterKind::Predicate, 20_000, 10_000_000, policy,),
            HnswQueryWideStrategy::ExactScan
        );
        assert_eq!(
            HnswQueryWideStrategy::choose(HnswFilterKind::Predicate, 20_001, 10_000_000, policy,),
            HnswQueryWideStrategy::SegmentAdaptive
        );
        assert_eq!(
            HnswQueryWideStrategy::choose(HnswFilterKind::Predicate, 500_000, 10_000_000, policy,),
            HnswQueryWideStrategy::SegmentAdaptive
        );
    }
}
