// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use crate::index::hnsw::hnsw_builder::{hnsw_active_foreground_queries, HnswForegroundQueryGuard};
use crate::index::hnsw::types::SearchParams;
use crate::index::hnsw::{
    hnsw_artifact_compatibility, hnsw_artifact_uses_external_vectors, DistanceMetric,
    HnswArtifactCompatibility, HnswBuildContract, HnswExternalVectorBinding,
    HnswExternalVectorSource, HnswExternalVectorSpan, HnswFilterKind, HnswIndex, HnswSearchFilter,
    HnswSearchPolicy, HnswSearchStrategy, HnswSegmentSearchInput, PartitionedVectorStorage,
    PreparedQuery, VectorStorage,
};

/// Minimum exact vector payload needed to amortize a cross-thread segment
/// phase. This is expressed as physical bytes rather than rows so the same
/// scheduling rule scales with vector dimension. Graph traversal remains
/// parallel because its random navigation latency is not proportional to the
/// number of predicate matches.
const MIN_PARALLEL_EXACT_VECTOR_BYTES: u64 = 2 * 1024 * 1024;
/// Fair share of the process search executor for a mixed graph+exact-tail
/// query.
///
/// A static narrow grant protects throughput but strands workers for a lone
/// query; a static wide grant serializes concurrent readers. The provider
/// guard is entered before predicate preparation, so by dispatch time it is a
/// process-wide census of runnable HNSW queries. Divide the fixed executor by
/// that demand and let the admission layer enforce the physical bound.
fn mixed_tail_query_lane_limit(process_width: usize, active_queries: usize) -> usize {
    process_width
        .max(1)
        .checked_div(active_queries.max(1))
        .unwrap_or(1)
        .max(1)
}
use crate::index::PredicateTree;
use crate::index::{DenseRowSet, ExactRowSet, PartitionExactRowSet};
use crate::rowset::RowsetSharedPtr;
use crate::search::artifact::ArtifactLocation;
use crate::search::capability::{ArtifactSegmentRef, SearchArtifactRef, SearchIndexKind};
use crate::search::cursor::{
    OpenedSearchCursor, SearchBatchState, SearchCursor, SearchReadSnapshot, VisibleSegment,
};
use crate::search::row_fetch::snapshot_epoch;
use crate::search::segment_dispatch::{
    acquire_search_dispatch_lanes, dispatch_segment_indices, map_search_tasks, map_segments,
    SegmentDispatchResult,
};
use crate::search::sidecar::{
    DecodedSidecarReaderRequest, SearchReaderRuntime, SidecarIntegrityPolicy, SidecarReaderRequest,
    SIDECAR_PACKAGE_CODEC,
};
use crate::search::tail::exact_merge::{ensure_tail_exact_merge_budget, TailExactMergeQueryShape};
use crate::search::tail_merge::{
    read_segment_column_batch_direct, resolve_logical_rows_with_allocator, visible_row_ids,
};
use crate::search::telemetry::{
    GenerationTelemetryEvent, NoopSearchTelemetryCollector, QueryTelemetryEvent,
    SearchTelemetryCollector,
};
use crate::search::topk_merge::{ranked_rows_to_batch, RankedRow, TopKCollector};
use crate::tablet::TabletRef;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::ArrayVector;

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
        // Reserve foreground HNSW capacity for the complete physical query,
        // including predicate preparation, exact-tail scans, sidecar opens,
        // graph traversal, and the final merge. Guards inside an individual
        // artifact search cannot protect the exact tail that precedes it and
        // leave gaps between generation partitions where maintenance can
        // regain the full build pool.
        let _foreground_query = HnswForegroundQueryGuard::enter();
        let _definition_query = self
            .snapshot
            .generation
            .hnsw_query_activity
            .as_ref()
            .map(|activity| activity.enter());
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
        let widths =
            self.search_policy
                .effective_widths(self.k, self.params.ef, self.params.rerank_window);
        let effective_ef = widths.ef;

        // Exact segment row sets are prepared once and retained at the query
        // boundary. Both singleton and generation-owned partition artifacts
        // borrow the same proof objects, so changing the physical partition
        // envelope cannot change predicate semantics.
        let requires_segment_filters = predicate.is_some()
            || visible_segments
                .iter()
                .any(|segment| segment.has_persistent_deletes);
        let prepared_filters = if requires_segment_filters {
            map_segments(
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
            )?
        } else {
            (0..visible_segments.len())
                .map(|_| PreparedSegmentFilter {
                    row_set: None,
                    _reservation: None,
                })
                .collect()
        };
        let predicate_matching_rows = predicate.map(|_| {
            prepared_filters.iter().fold(0u64, |rows, filter| {
                rows.saturating_add(filter.row_set.as_ref().map_or(0, |row_set| row_set.len()))
            })
        });
        let search_parallelism_slots = exact_search_parallelism_slots(
            parallelism_slots,
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
            .collect::<HashMap<_, _>>();
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
                HnswIntegrityActivation::QueryUse,
                || {
                    build_external_vector_binding(
                        artifact,
                        self.storage_col_id,
                        self.vector_dim,
                        |segment_ref| {
                            visible_by_ref
                                .get(segment_ref)
                                .and_then(|index| visible_segments.get(*index))
                        },
                    )
                    .map(Some)
                },
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
        let mut covered_segments = vec![false; visible_segments.len()];
        for (artifact, _) in &partition_artifacts {
            for span in artifact.coverage.segments() {
                let visible_index = visible_by_ref.get(&span.segment).ok_or_else(|| {
                    paro_error::data_corrupted(
                        "HNSW artifact coverage references a missing segment",
                    )
                })?;
                covered_segments[*visible_index] = true;
            }
        }
        let tail_segment_indices = covered_segments
            .iter()
            .enumerate()
            .filter_map(|(index, covered)| (!covered).then_some(index))
            .collect::<Vec<_>>();
        let requested_search_lanes = if tail_segment_indices.is_empty() {
            search_parallelism_slots
        } else {
            search_parallelism_slots.min(mixed_tail_query_lane_limit(
                parallelism_slots,
                hnsw_active_foreground_queries(),
            ))
        };
        let search_lease = acquire_search_dispatch_lanes(
            requested_search_lanes,
            tail_segment_indices
                .len()
                .saturating_add(partition_artifacts.len()),
        )?;
        let admitted_search_lanes = search_lease.lanes();

        let per_segment = dispatch_segment_indices(
            SearchIndexKind::Hnsw,
            visible_segments,
            &tail_segment_indices,
            admitted_search_lanes,
            self.telemetry.as_ref(),
            |index, segment| {
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
                    filter,
                    effective_ef,
                    widths.rerank_window,
                    budget,
                )
            },
        )?;

        let per_partition = map_search_tasks(
            &partition_artifacts,
            admitted_search_lanes,
            |_, (artifact, index)| {
                self.search_partition_artifact(
                    artifact,
                    index.as_ref(),
                    &visible_by_ref,
                    &prepared_filters,
                    predicate.is_some(),
                    &predicate_columns,
                    effective_ef,
                    widths.rerank_window,
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

    fn score_exact_tail_chunk(
        &self,
        resolved: &paro_common::chunk::Chunk,
        physical_rows: &[PhysicalRowRef],
        budget: &ResourceBudget,
    ) -> Result<Vec<RankedRow>> {
        if resolved.size() != physical_rows.len() {
            return Err(paro_error::internal(
                "resolved exact tail rows and physical identities disagree",
            ));
        }
        budget.work.check_and_consume(physical_rows.len())?;
        let column = resolved
            .column(0)
            .ok_or_else(|| paro_error::internal("resolved vector tail chunk missing column"))?;
        let array_view = column.try_to_array_view(physical_rows.len())?;
        if array_view.array_size() != self.vector_dim
            || !matches!(
                column.logical_type(),
                LogicalType::Array(child, size)
                    if child.as_ref() == &LogicalType::Float && *size == self.vector_dim
            )
        {
            return Err(paro_error::data_corrupted(
                "resolved HNSW tail column does not match its VECTOR dimension",
            ));
        }
        let child = ArrayVector::get_entry(column);
        let child_values = child.as_slice::<f32>();
        let child_all_valid = array_view.child().validity().all_valid();
        let parent_all_valid = array_view.parent().validity().all_valid();
        let contiguous_start = (!physical_rows.is_empty() && parent_all_valid && child_all_valid)
            .then(|| array_view.physical_child_index(0, 0));
        let is_contiguous = contiguous_start.is_some_and(|start| {
            physical_rows.iter().enumerate().all(|(row, _)| {
                array_view.physical_child_index(row, 0)
                    == start.saturating_add(row.saturating_mul(self.vector_dim))
            })
        });

        let mut collector = TopKCollector::new(self.k);
        if let Some(start) = contiguous_start.filter(|_| is_contiguous) {
            let value_count = physical_rows.len().saturating_mul(self.vector_dim);
            let values = child_values
                .get(start..start + value_count)
                .ok_or_else(|| {
                    paro_error::data_corrupted("resolved HNSW tail child range is out of bounds")
                })?;
            let mut scores = vec![0.0; physical_rows.len()];
            self.distance.similarity_unindexed_batch_contiguous(
                self.prepared_query.as_slice(),
                values,
                self.vector_dim,
                &mut scores,
            );
            for (&row, score) in physical_rows.iter().zip(scores) {
                collector.push(RankedRow::new(row, score));
            }
            return Ok(collector.into_sorted_rows());
        }

        for (offset, &row) in physical_rows.iter().enumerate() {
            if !array_view.is_valid(offset) {
                continue;
            }
            let start = array_view.physical_child_index(offset, 0);
            let last = array_view.physical_child_index(offset, self.vector_dim - 1);
            if last != start.saturating_add(self.vector_dim - 1)
                || (!child_all_valid
                    && (0..self.vector_dim).any(|index| !array_view.child_is_valid(offset, index)))
            {
                continue;
            }
            let values = child_values
                .get(start..start + self.vector_dim)
                .ok_or_else(|| {
                    paro_error::data_corrupted("resolved HNSW tail child range is out of bounds")
                })?;
            collector.push(RankedRow::new(
                row,
                self.distance
                    .similarity_unindexed(self.prepared_query.as_slice(), values),
            ));
        }
        Ok(collector.into_sorted_rows())
    }

    fn score_exact_tail_storage_batch(
        &self,
        batch: &crate::rowset::column::ColumnBatch,
        visible_segment: &VisibleSegment,
        row_ids: &[u32],
        budget: &ResourceBudget,
    ) -> Result<Vec<RankedRow>> {
        budget.work.check_and_consume(row_ids.len())?;
        let logical_type = visible_segment
            .segment
            .schema()
            .column_by_id(self.storage_col_id)
            .map(|column| &column.logical_type)
            .ok_or_else(|| {
                paro_error::column_not_found(format!(
                    "column {} not found in exact tail segment schema",
                    self.storage_col_id
                ))
            })?;
        let vectors = crate::codec::vector_decoder::FloatArrayBatchView::try_new(
            logical_type,
            batch,
            row_ids.len(),
        )?;
        if vectors.rows() != row_ids.len() || vectors.dimension() != self.vector_dim {
            return Err(paro_error::data_corrupted(
                "stored HNSW tail batch does not match its vector dimension",
            ));
        }

        let mut collector = TopKCollector::new(self.k);
        if vectors.all_valid() {
            let mut scores = vec![0.0; row_ids.len()];
            self.distance.similarity_unindexed_batch_contiguous(
                self.prepared_query.as_slice(),
                vectors.values(),
                self.vector_dim,
                &mut scores,
            );
            for (&row_id, score) in row_ids.iter().zip(scores) {
                collector.push(RankedRow::new(
                    PhysicalRowRef::new(
                        visible_segment.rowset_id,
                        visible_segment.segment_id,
                        crate::rowset::SegmentRowId::from_raw(row_id),
                    ),
                    score,
                ));
            }
            return Ok(collector.into_sorted_rows());
        }

        for (offset, &row_id) in row_ids.iter().enumerate() {
            if !vectors.is_valid(offset) {
                continue;
            }
            let start = offset.saturating_mul(self.vector_dim);
            let values = vectors
                .values()
                .get(start..start + self.vector_dim)
                .ok_or_else(|| {
                    paro_error::data_corrupted("stored HNSW tail vector is out of bounds")
                })?;
            collector.push(RankedRow::new(
                PhysicalRowRef::new(
                    visible_segment.rowset_id,
                    visible_segment.segment_id,
                    crate::rowset::SegmentRowId::from_raw(row_id),
                ),
                self.distance
                    .similarity_unindexed(self.prepared_query.as_slice(), values),
            ));
        }
        Ok(collector.into_sorted_rows())
    }

    /// Score immutable plain vector pages directly from their mmap views.
    ///
    /// This is both the dense exact-tail path and the sparse gather path.  A
    /// plain uncompressed vector page can approach the u32 page-size ceiling;
    /// routing either shape through `ColumnIterator` would admit the whole
    /// page into `PageCache` before selecting rows.  The segment instead owns
    /// one checksum-validated mapping and queries retain only bounded score
    /// scratch.
    fn score_exact_tail_plain_vectors(
        &self,
        visible_segment: &VisibleSegment,
        row_ids: &[u32],
        budget: &ResourceBudget,
    ) -> Result<Option<Vec<RankedRow>>> {
        let Some(storage) = visible_segment
            .segment
            .open_plain_vector_storage(self.storage_col_id, self.vector_dim)?
        else {
            return Ok(None);
        };
        let segment_rows = usize::try_from(visible_segment.segment.num_rows())
            .map_err(|_| paro_error::out_of_range("exact tail segment exceeds usize"))?;
        if storage.num_vectors() != segment_rows || storage.vector_dim() != self.vector_dim {
            return Err(paro_error::data_corrupted(format!(
                "plain vector storage shape {}x{} does not match exact tail {}x{}",
                storage.num_vectors(),
                storage.vector_dim(),
                segment_rows,
                self.vector_dim
            )));
        }

        let batch_rows = paro_common::vector::VECTOR_SIZE;
        let _score_reservation = budget.try_reserve_memory(
            batch_rows.saturating_mul(std::mem::size_of::<crate::index::hnsw::ScoreType>()),
        )?;
        let mut collector = TopKCollector::new(self.k);
        let is_dense = row_ids.len() == segment_rows
            && row_ids
                .iter()
                .enumerate()
                .all(|(ordinal, row_id)| *row_id as usize == ordinal);
        if is_dense {
            let mut point_base = 0usize;
            let mut scores = Vec::with_capacity(batch_rows);
            storage.try_for_each_contiguous_chunk(&mut |vectors| {
                if vectors.len() % self.vector_dim != 0 {
                    return Err(paro_error::data_corrupted(
                        "plain exact-tail vector chunk is not row aligned",
                    ));
                }
                for vector_batch in vectors.chunks(batch_rows.saturating_mul(self.vector_dim)) {
                    let rows = vector_batch.len() / self.vector_dim;
                    budget.work.check_and_consume(rows)?;
                    scores.resize(rows, 0.0);
                    self.distance.similarity_unindexed_batch_contiguous(
                        self.prepared_query.as_slice(),
                        vector_batch,
                        self.vector_dim,
                        &mut scores,
                    );
                    for (offset, &score) in scores.iter().enumerate() {
                        let row_id = point_base.checked_add(offset).ok_or_else(|| {
                            paro_error::data_corrupted("exact tail point id overflow")
                        })?;
                        collector.push(RankedRow::new(
                            PhysicalRowRef::new(
                                visible_segment.rowset_id,
                                visible_segment.segment_id,
                                crate::rowset::SegmentRowId::from_raw(row_id as u32),
                            ),
                            score,
                        ));
                    }
                    point_base = point_base.checked_add(rows).ok_or_else(|| {
                        paro_error::data_corrupted("exact tail point count overflow")
                    })?;
                }
                Ok(())
            })?;
            if point_base != segment_rows {
                return Err(paro_error::data_corrupted(format!(
                    "plain exact-tail scan returned {point_base} rows for {segment_rows} vectors"
                )));
            }
        } else {
            for row_batch in row_ids.chunks(batch_rows) {
                budget.work.check_and_consume(row_batch.len())?;
                for &row_id in row_batch {
                    if row_id as usize >= segment_rows {
                        return Err(paro_error::data_corrupted(format!(
                            "exact tail row {row_id} exceeds segment cardinality {segment_rows}"
                        )));
                    }
                    let vector = storage.get_vector(row_id);
                    let score = self
                        .distance
                        .similarity_unindexed(self.prepared_query.as_slice(), vector);
                    collector.push(RankedRow::new(
                        PhysicalRowRef::new(
                            visible_segment.rowset_id,
                            visible_segment.segment_id,
                            crate::rowset::SegmentRowId::from_raw(row_id),
                        ),
                        score,
                    ));
                }
            }
        }
        Ok(Some(collector.into_sorted_rows()))
    }

    fn dispatch_segment_search(
        &self,
        segment: &VisibleSegment,
        filter: HnswSearchFilter<'_>,
        effective_ef: usize,
        rerank_window: usize,
        budget: &ResourceBudget,
    ) -> Result<SegmentDispatchResult<(Vec<RankedRow>, bool, Option<String>)>> {
        let matching_rows = filter
            .row_set()
            .map_or(segment.segment.num_rows(), ExactRowSet::len);
        let inline_index = segment
            .segment
            .open_hnsw_index(self.storage_col_id)?
            .filter(|index| !index.integrity_failed());
        if let Some(index) = inline_index.as_ref() {
            bind_hnsw_search_workspace(
                index,
                self.reader_runtime.as_ref(),
                HnswIntegrityActivation::QueryUse,
            )?;
        }
        let exact_scan_workload = inline_index.as_ref().map_or_else(
            || filter.exact_scan_workload(segment.segment.num_rows(), |_| false),
            |index| index.exact_scan_workload(filter),
        );
        let search_strategy = HnswSearchStrategy::choose(HnswSegmentSearchInput {
            objective: self.params.objective,
            filter_kind: filter.kind(),
            matching_rows,
            total_rows: segment.segment.num_rows(),
            top_k: self.k,
            effective_ef,
            rerank_window,
            vector_dimension: u32::try_from(self.vector_dim)
                .map_err(|_| paro_error::out_of_range("HNSW vector dimension exceeds u32"))?,
            vector_encoding: inline_index.as_ref().map_or(
                crate::index::hnsw::HnswBuildVectorEncoding::ExactF32,
                |index| index.build_contract.vector_encoding,
            ),
            exact_scan_workload,
            cost_profile: self.search_policy.distance_cost,
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
        visible_by_ref: &HashMap<ArtifactSegmentRef, usize>,
        prepared_filters: &[PreparedSegmentFilter],
        has_predicate: bool,
        predicate_columns: &[u32],
        effective_ef: usize,
        rerank_window: usize,
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
            let row_set: Arc<dyn ExactRowSet> =
                Arc::new(PartitionExactRowSet::try_new_with_directory(
                    parts,
                    artifact.coverage.partition_directory(),
                )?);
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
        let strategy = HnswSearchStrategy::choose(HnswSegmentSearchInput {
            objective: self.params.objective,
            filter_kind,
            matching_rows,
            total_rows: artifact.coverage.row_count(),
            top_k: self.k,
            effective_ef,
            rerank_window,
            vector_dimension: u32::try_from(self.vector_dim)
                .map_err(|_| paro_error::out_of_range("HNSW vector dimension exceeds u32"))?,
            vector_encoding: index.build_contract.vector_encoding,
            exact_scan_workload: index.exact_scan_workload(filter),
            cost_profile: self.search_policy.distance_cost,
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
            .filter(|index| !index.integrity_failed())
        {
            bind_hnsw_search_workspace(
                &index,
                self.reader_runtime.as_ref(),
                HnswIntegrityActivation::QueryUse,
            )?;
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
        if let Some(rows) =
            self.score_exact_tail_plain_vectors(visible_segment, &row_ids, budget)?
        {
            return Ok((rows, true));
        }
        let retained_per_row = self
            .vector_dim
            .saturating_mul(std::mem::size_of::<f32>())
            .saturating_add(32);
        let retained_bytes = row_ids
            .len()
            .checked_mul(retained_per_row)
            .ok_or_else(|| paro_error::out_of_range("exact tail working set overflow"))?;
        let _tail_reservation = budget.try_reserve_memory(retained_bytes)?;
        if let Some(batch) =
            read_segment_column_batch_direct(visible_segment, &row_ids, self.storage_col_id)?
        {
            return Ok((
                self.score_exact_tail_storage_batch(&batch, visible_segment, &row_ids, budget)?,
                true,
            ));
        }
        let resolved = resolve_logical_rows_with_allocator(
            &self.tablet,
            &self.snapshot,
            visible_segment,
            &row_ids,
            self.storage_col_id,
            Arc::clone(&budget.materialization_allocator),
        )?;
        let physical_rows = row_ids
            .iter()
            .copied()
            .map(|row_id| {
                PhysicalRowRef::new(
                    visible_segment.rowset_id,
                    visible_segment.segment_id,
                    crate::rowset::SegmentRowId::from_raw(row_id),
                )
            })
            .collect::<Vec<_>>();
        Ok((
            self.score_exact_tail_chunk(&resolved, &physical_rows, budget)?,
            true,
        ))
    }
}

fn exact_search_parallelism_slots(
    granted_slots: usize,
    matching_rows: u64,
    vector_dim: usize,
) -> usize {
    let granted_slots = granted_slots.max(1);
    if granted_slots == 1 {
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

    open_sidecar_hnsw_artifact(
        runtime,
        artifact,
        vector_dim,
        HnswIntegrityActivation::QueryUse,
        || {
            build_external_vector_binding(artifact, column_id, vector_dim, |segment_ref| {
                (segment_ref.rowset_id == visible_segment.rowset_id
                    && segment_ref.segment_id == visible_segment.segment_id)
                    .then_some(visible_segment)
            })
            .map(Some)
        },
    )
}

fn open_sidecar_hnsw_artifact<F>(
    runtime: &SearchReaderRuntime,
    artifact: &SearchArtifactRef,
    vector_dim: usize,
    integrity_activation: HnswIntegrityActivation,
    external_binding: F,
) -> Result<Option<Arc<HnswIndex>>>
where
    F: FnOnce() -> Result<Option<HnswExternalVectorBinding>>,
{
    let request = DecodedSidecarReaderRequest {
        sidecar: SidecarReaderRequest {
            location: &artifact.location,
            artifact_format_version: artifact.artifact_format_version,
            provider: SearchIndexKind::Hnsw,
            codec: SIDECAR_PACKAGE_CODEC,
            integrity: SidecarIntegrityPolicy::SelfValidatingArtifact,
        },
    };
    let index = runtime.get_or_try_open_decoded(request, |cached| {
        if !matches!(
            hnsw_artifact_compatibility(cached.bytes())?,
            HnswArtifactCompatibility::Current
        ) {
            return Ok(None);
        }
        let (mmap, offset, len) = cached.mmap_range();
        let index = if hnsw_artifact_uses_external_vectors(cached.bytes())? {
            HnswIndex::deserialize_mmap_range_with_external_vectors(
                mmap,
                offset,
                len,
                external_binding()?.ok_or_else(|| {
                    paro_error::artifact_not_ready(
                        "external HNSW sidecar has no immutable base-page binding",
                    )
                })?,
            )?
        } else {
            HnswIndex::deserialize_mmap_range(mmap, offset, len)?
        };
        if index.vector_storage.vector_dim() != vector_dim {
            return Err(paro_error::data_corrupted(format!(
                "HNSW artifact dimension mismatch: expected {vector_dim}, got {}",
                index.vector_storage.vector_dim()
            )));
        }
        Ok(Some(index))
    })?;
    if let Some(index) = index.as_ref() {
        if index.integrity_failed() {
            // Integrity failure quarantines only this rebuildable secondary
            // artifact. The caller retains the covered base segments and can
            // execute an exact fallback instead of making the table unreadable
            // or emitting the same data-corruption error on every query.
            return Ok(None);
        }
        bind_hnsw_search_workspace(index, runtime, integrity_activation)?;
    }
    Ok(index)
}

fn build_external_vector_binding<'a, F>(
    artifact: &SearchArtifactRef,
    column_id: u32,
    vector_dim: usize,
    mut resolve_visible: F,
) -> Result<HnswExternalVectorBinding>
where
    F: FnMut(&ArtifactSegmentRef) -> Option<&'a VisibleSegment>,
{
    build_external_vector_binding_from_storage(artifact, column_id, vector_dim, |segment_ref| {
        let visible = resolve_visible(segment_ref).ok_or_else(|| {
            paro_error::data_corrupted(format!(
                "external HNSW source references invisible segment {}/{}",
                segment_ref.rowset_id, segment_ref.segment_id
            ))
        })?;
        Ok(Some((visible.segment.clone(), visible.segment.num_rows())))
    })
}

fn build_external_vector_binding_from_storage<F>(
    artifact: &SearchArtifactRef,
    column_id: u32,
    vector_dim: usize,
    mut resolve_segment: F,
) -> Result<HnswExternalVectorBinding>
where
    F: FnMut(&ArtifactSegmentRef) -> Result<Option<(crate::rowset::SegmentSharedPtr, u64)>>,
{
    if artifact.column_id != column_id {
        return Err(paro_error::data_corrupted(format!(
            "external HNSW artifact column {} does not match query column {column_id}",
            artifact.column_id
        )));
    }
    let mut source_spans = Vec::with_capacity(artifact.coverage.segments().len());
    let mut vector_parts = Vec::<Arc<dyn VectorStorage>>::new();
    for span in artifact.coverage.segments() {
        let (segment, segment_rows) = resolve_segment(&span.segment)?.ok_or_else(|| {
            paro_error::data_corrupted(format!(
                "external HNSW source references invisible segment {}/{}",
                span.segment.rowset_id, span.segment.segment_id
            ))
        })?;
        if segment_rows != span.row_count {
            return Err(paro_error::data_corrupted(format!(
                "external HNSW source segment {}/{} has {} rows, expected {}",
                span.segment.rowset_id, span.segment.segment_id, segment_rows, span.row_count
            )));
        }
        let column = segment.get_column_meta(column_id).ok_or_else(|| {
            paro_error::column_not_found(format!(
                "external HNSW source segment {} has no column {column_id}",
                span.segment.segment_id
            ))
        })?;
        if column.num_rows != span.row_count {
            return Err(paro_error::data_corrupted(format!(
                "external HNSW source column {column_id} has {} rows, expected {}",
                column.num_rows, span.row_count
            )));
        }
        let storage = segment
            .open_plain_vector_storage(column_id, vector_dim)?
            .ok_or_else(|| {
                paro_error::data_corrupted(format!(
                    "external HNSW source segment {}/{} is not a plain vector column",
                    span.segment.rowset_id, span.segment.segment_id
                ))
            })?;
        vector_parts.push(storage);
        source_spans.push(HnswExternalVectorSpan {
            rowset_id: span.segment.rowset_id,
            segment_id: span.segment.segment_id,
            row_count: span.row_count,
        });
    }
    let source = HnswExternalVectorSource::try_new(column_id, source_spans)?;
    let storage: Arc<dyn VectorStorage> =
        Arc::new(PartitionedVectorStorage::try_new(vector_parts, vector_dim)?);
    Ok(HnswExternalVectorBinding { source, storage })
}

/// Materialize every typed HNSW reader before a recovered or newly published
/// generation becomes query-visible.
///
/// The manifest is immutable and sidecar artifacts are self-validating, so
/// reader construction is generation activation work rather than query work.
/// Keeping it on this boundary also reuses the segment-owned vector readers
/// already populated by catch-up construction.
pub(crate) fn prewarm_hnsw_generation_readers(
    runtime: &SearchReaderRuntime,
    artifacts: &[SearchArtifactRef],
    visible_rowsets: &[RowsetSharedPtr],
    column_id: u32,
    vector_dim: usize,
    expected_contract: &HnswBuildContract,
) -> Result<usize> {
    let rowsets = visible_rowsets
        .iter()
        .map(|rowset| (rowset.rowset_id(), rowset))
        .collect::<BTreeMap<_, _>>();
    let mut warmed = 0usize;
    for artifact in artifacts.iter().filter(|artifact| {
        artifact.kind == SearchIndexKind::Hnsw
            && artifact.column_id == column_id
            && matches!(
                artifact.location,
                ArtifactLocation::SidecarArtifactFile { .. }
            )
    }) {
        let index = open_sidecar_hnsw_artifact(
            runtime,
            artifact,
            vector_dim,
            HnswIntegrityActivation::OnFirstQuery,
            || {
                build_external_vector_binding_from_storage(
                    artifact,
                    column_id,
                    vector_dim,
                    |segment_ref| {
                        let Some(rowset) = rowsets.get(&segment_ref.rowset_id) else {
                            return Ok(None);
                        };
                        rowset.load()?;
                        Ok(rowset
                            .segments()
                            .iter()
                            .find(|segment| segment.segment_id() == segment_ref.segment_id)
                            .cloned()
                            .map(|segment| {
                                let rows = segment.num_rows();
                                (segment, rows)
                            }))
                    },
                )
                .map(Some)
            },
        )?;
        let Some(index) = index else {
            return Err(paro_error::artifact_not_ready(format!(
                "HNSW artifact for generation {} is not queryable during reader activation",
                artifact.generation_id
            )));
        };
        validate_hnsw_index_contract(index.as_ref(), expected_contract)?;
        warmed = warmed.saturating_add(1);
    }
    Ok(warmed)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HnswIntegrityActivation {
    /// Generation activation constructs and binds immutable readers before
    /// they become visible, but must not initiate a full-payload scan. The
    /// first real query activates optional sweeping while holding the
    /// foreground reservation, so low-priority I/O cannot race service
    /// readiness or make an unused generation resident.
    OnFirstQuery,
    QueryUse,
}

fn bind_hnsw_search_workspace(
    index: &Arc<HnswIndex>,
    runtime: &SearchReaderRuntime,
    integrity_activation: HnswIntegrityActivation,
) -> Result<()> {
    if let Some(buffer_pool) = runtime.buffer_pool() {
        index.bind_search_buffer_pool(buffer_pool)?;
    }
    if integrity_activation == HnswIntegrityActivation::QueryUse {
        runtime.schedule_hnsw_integrity_verification(index);
    }
    Ok(())
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
            exact_search_parallelism_slots(8, rows_below_two_mib_at_32d, 32,),
            1
        );
        assert_eq!(
            exact_search_parallelism_slots(8, rows_below_two_mib_at_32d + 1, 32,),
            8
        );
    }

    #[test]
    fn mixed_exact_tail_divides_the_process_width_by_runnable_queries() {
        assert_eq!(mixed_tail_query_lane_limit(1, 1), 1);
        assert_eq!(mixed_tail_query_lane_limit(10, 1), 10);
        assert_eq!(mixed_tail_query_lane_limit(10, 2), 5);
        assert_eq!(mixed_tail_query_lane_limit(10, 8), 1);
        assert_eq!(mixed_tail_query_lane_limit(16, 5), 3);
    }
}
