// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Provider sidecar artifact builders.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Instant;

use bytes::Bytes;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

use super::capability::{
    ArtifactSegmentRef, ArtifactSegmentSpan, SearchArtifactRef, SearchIndexKind,
    SearchPartitionCoverage,
};
use super::inline_sink::{
    BuildBudget, CostEstimate, FlushSearchMode, HnswInlineThreshold, InlineArtifactBlob,
    InlineArtifactBuilder, MaintenanceBenefit, MaintenanceCost, SegmentChunkInput, SegmentFlushCtx,
    SidecarArtifactBuildResult, SidecarArtifactBuilder, SidecarBuildInput,
};
use super::providers::fulltext::inline::FullTextInlineArtifactBuilder;
use super::providers::sparse::inline::SparseInlineArtifactBuilder;
use super::sidecar::SidecarArtifactStore;
use super::stats::{
    HnswProviderStats, MaintenancePriority, SearchArtifactStats, SearchProviderStats,
};
use super::tail::{TailMutationKind, TailPendingEntry};
use crate::index::bitmap::BitmapIndexWriter;
use crate::index::hnsw::{
    open_plain_vector_column_pages, HnswBuildExecutionPolicy, HnswBuildStopCheck, HnswBuilder,
    HnswExternalVectorSource, HnswExternalVectorSpan, HnswFilterBlock, HnswFilterBlocks,
    HnswFilterColumnBlocks, HnswFilterTopologyContract, HnswMaintenanceBuildPriority,
    PartitionedVectorStorage, VectorStorage,
};
use crate::metrics::{storage_metrics, SearchSidecarBuildMetricKey};
use crate::rowset::column::ColumnBatch;
use crate::rowset::encoding::BinaryPlainPageDecoder;
use crate::rowset::{ColumnData, RowsetSharedPtr, SegmentIterator};
use crate::statistics::HnswIndexStatistics;

const SIDECAR_BUILD_BATCH_ROWS: usize = 8192;

#[derive(Debug, Clone)]
pub(crate) struct ProviderSidecarArtifactBuilder {
    store: SidecarArtifactStore,
    hnsw_execution_policy: HnswBuildExecutionPolicy,
}

impl ProviderSidecarArtifactBuilder {
    pub(crate) fn new(store: SidecarArtifactStore) -> Self {
        Self {
            store,
            hnsw_execution_policy: HnswBuildExecutionPolicy::Foreground,
        }
    }

    pub(crate) fn for_maintenance(
        store: SidecarArtifactStore,
        priority: MaintenancePriority,
    ) -> Self {
        let priority = match priority {
            MaintenancePriority::Idle | MaintenancePriority::Opportunistic => {
                HnswMaintenanceBuildPriority::Opportunistic
            }
            MaintenancePriority::Elevated => HnswMaintenanceBuildPriority::Elevated,
            MaintenancePriority::Critical => HnswMaintenanceBuildPriority::Critical,
        };
        Self {
            store,
            hnsw_execution_policy: HnswBuildExecutionPolicy::Maintenance(priority),
        }
    }
}

impl SidecarArtifactBuilder for ProviderSidecarArtifactBuilder {
    fn estimate_cost(&self, input: &SidecarBuildInput) -> Result<CostEstimate> {
        let rows = input
            .tail_window
            .iter()
            .filter(|entry| !matches!(entry.mutation, TailMutationKind::Delete))
            .map(|entry| entry.row_count)
            .sum::<u64>()
            .max(1);
        let cpu_per_row = match input.definition.kind {
            SearchIndexKind::FullText => 8_000,
            SearchIndexKind::Sparse => 5_000,
            SearchIndexKind::Hnsw => 80_000,
        };
        let (io_write_bytes, memory_peak_bytes) = match input.definition.kind {
            SearchIndexKind::Hnsw => {
                let provider = input.definition.hnsw_provider_config()?;
                let contract = provider.build_contract();
                // Generation sidecars bind exact vectors to the immutable
                // base pages named by their coverage contract. Inline
                // envelopes remain self-contained.
                let vector_bytes = 0u64;
                let metric_bytes =
                    if provider.distance == crate::index::hnsw::DistanceMetric::Cosine {
                        rows.saturating_mul(std::mem::size_of::<f32>() as u64)
                    } else {
                        0
                    };
                let base_graph_bytes =
                    HnswInlineThreshold::estimate_graph_memory_bytes(rows, provider.m);
                let filter_graph_bytes = HnswInlineThreshold::estimate_filter_graph_memory_bytes(
                    rows,
                    provider.filter_columns.len(),
                    provider.filter_block_rows,
                    provider.filter_m,
                );
                let covering_scan_bytes = HnswInlineThreshold::estimate_filter_scan_layout_bytes(
                    rows,
                    provider.dimension,
                    provider.filter_columns.len(),
                );
                let routing_bytes =
                    HnswInlineThreshold::estimate_routing_artifact_bytes(rows, &contract);
                let protected_bytes = vector_bytes
                    .saturating_add(metric_bytes)
                    .saturating_add(routing_bytes)
                    .saturating_add(base_graph_bytes)
                    .saturating_add(filter_graph_bytes)
                    .saturating_add(covering_scan_bytes)
                    .saturating_add(1024 * 1024);
                // Hierarchical integrity stores one checksum per 4 KiB of
                // protected payload plus a compact checksum-page directory.
                let integrity_checksums = protected_bytes
                    .div_ceil(4 * 1024)
                    .saturating_mul(std::mem::size_of::<u32>() as u64);
                let integrity_directory = integrity_checksums
                    .div_ceil(4 * 1024)
                    .saturating_mul(std::mem::size_of::<u32>() as u64);
                let serialized_bytes = protected_bytes
                    .saturating_add(integrity_checksums)
                    .saturating_add(integrity_directory)
                    .saturating_add(64);
                let build_peak = HnswInlineThreshold::estimate_contract_build_peak_memory_bytes(
                    rows,
                    provider.dimension,
                    &contract,
                    crate::index::hnsw::hnsw_build_thread_count(),
                )
                // The streaming integrity writer retains the checksum vector
                // and encoded hierarchy concurrently, but never the artifact.
                .saturating_add(integrity_checksums.saturating_mul(2))
                .saturating_add(integrity_directory);
                (serialized_bytes, build_peak)
            }
            SearchIndexKind::FullText | SearchIndexKind::Sparse => {
                (rows.saturating_mul(64), rows.saturating_mul(128))
            }
        };
        Ok(CostEstimate {
            cost: MaintenanceCost {
                cpu_ns: rows.saturating_mul(cpu_per_row),
                io_read_bytes: input.tail_window.iter().map(|entry| entry.byte_count).sum(),
                io_write_bytes,
                memory_peak_bytes,
                publish_bytes: io_write_bytes,
            },
            benefit: MaintenanceBenefit {
                expected_open_cost_saved_us: rows,
                expected_tail_rows_drained: rows,
                expected_artifact_bytes_reclaimed: 0,
            },
        })
    }

    fn build(
        &self,
        input: SidecarBuildInput,
        budget: &BuildBudget,
    ) -> Result<SidecarArtifactBuildResult> {
        let started_at = Instant::now();
        let metric_key = SearchSidecarBuildMetricKey {
            definition_id: input.definition.definition_id,
            provider: input.definition.kind,
        };
        let metric_rows = sidecar_input_rows(&input);
        let metric_read_bytes = sidecar_input_read_bytes(&input);
        let rowsets = input
            .rowset_refs
            .iter()
            .map(|rowset| (rowset.rowset_id(), rowset.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut writer = self
            .store
            .create_next_package_writer(input.definition.definition_id, input.generation_id)?;
        let mut artifact_refs = Vec::new();
        if input.definition.kind == SearchIndexKind::Hnsw {
            let provider = input.definition.hnsw_provider_config()?;
            let groups = hnsw_partition_build_groups(
                &rowsets,
                &input.tail_window,
                provider.generation_layout.target_graph_rows,
            )?;
            for requested_segments in groups {
                if let Some(partition) = build_hnsw_partition_sidecar_artifact(
                    &input.definition,
                    &rowsets,
                    &requested_segments,
                    budget,
                    input.stop_check.as_ref(),
                    writer.workspace_dir(),
                    self.hnsw_execution_policy,
                )? {
                    let location = writer.append_streamed_artifact(|file, offset| {
                        let source =
                            external_vector_source(partition.column_id, &partition.coverage)?;
                        partition
                            .index
                            .serialize_into_seekable_external_vectors(file, offset, &source)
                            .map(|_| ())
                    })?;
                    artifact_refs.push(sidecar_ref_from_hnsw_partition(
                        &input.definition,
                        input.generation_id,
                        partition.coverage,
                        partition.column_id,
                        partition.provider_stats,
                        location,
                    )?);
                }
            }
        } else {
            let mut built_segments = BTreeSet::new();
            for entry in &input.tail_window {
                if let Some(stop_check) = input.stop_check.as_ref() {
                    stop_check.check()?;
                }
                if matches!(entry.mutation, TailMutationKind::Delete) {
                    continue;
                }
                check_deadline(budget.deadline)?;
                let Some(rowset) = rowsets.get(&entry.rowset_id) else {
                    continue;
                };
                rowset.load()?;
                for segment_id in &entry.segment_ids {
                    if !built_segments.insert((entry.rowset_id, *segment_id)) {
                        continue;
                    }
                    let result = build_segment_sidecar_artifact(
                        &input.definition,
                        input.generation_id,
                        rowset,
                        *segment_id,
                    )?;
                    for blob in result.blobs {
                        let location = writer.append_artifact(&blob.bytes)?;
                        artifact_refs.push(sidecar_ref_from_blob(
                            &input.definition,
                            entry.rowset_id,
                            *segment_id,
                            blob,
                            location,
                        )?);
                    }
                }
            }
        }

        if artifact_refs.is_empty() {
            writer.abort();
            return Ok(SidecarArtifactBuildResult {
                artifact_refs,
                stats_delta: None,
                bytes_written: 0,
            });
        }

        let bytes_written = writer.bytes_written();
        writer.finalize()?;
        let artifact_bytes = artifact_refs
            .iter()
            .map(|artifact| artifact.stats.bytes_on_disk)
            .sum();
        storage_metrics().record_search_sidecar_build(
            metric_key,
            metric_rows,
            metric_read_bytes,
            bytes_written,
            artifact_bytes,
            elapsed_micros_since(started_at),
        );
        Ok(SidecarArtifactBuildResult {
            artifact_refs,
            stats_delta: None,
            bytes_written,
        })
    }
}

fn external_vector_source(
    column_id: u32,
    coverage: &SearchPartitionCoverage,
) -> Result<HnswExternalVectorSource> {
    HnswExternalVectorSource::try_new(
        column_id,
        coverage
            .segments()
            .iter()
            .map(|span| HnswExternalVectorSpan {
                rowset_id: span.segment.rowset_id,
                segment_id: span.segment.segment_id,
                row_count: span.row_count,
            })
            .collect(),
    )
}

fn sidecar_input_rows(input: &SidecarBuildInput) -> u64 {
    input
        .tail_window
        .iter()
        .filter(|entry| !matches!(entry.mutation, TailMutationKind::Delete))
        .map(|entry| entry.row_count)
        .sum()
}

fn sidecar_input_read_bytes(input: &SidecarBuildInput) -> u64 {
    input
        .tail_window
        .iter()
        .filter(|entry| !matches!(entry.mutation, TailMutationKind::Delete))
        .map(|entry| entry.byte_count)
        .sum()
}

fn elapsed_micros_since(started_at: Instant) -> u64 {
    let micros = started_at.elapsed().as_micros();
    micros.min(u128::from(u64::MAX)) as u64
}

fn check_deadline(deadline: Option<Instant>) -> Result<()> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(paro_error::invalid_input(
            "search sidecar build deadline expired",
        ));
    }
    Ok(())
}

struct HnswPartitionArtifact {
    coverage: SearchPartitionCoverage,
    column_id: u32,
    index: crate::index::hnsw::HnswIndex,
    provider_stats: HnswProviderStats,
}

/// Deterministically group physical segments into generation-owned graph
/// shards. Segment identity order, not rowset publication timing or worker
/// scheduling, defines the layout. A segment larger than the target remains a
/// valid singleton because segment rows cannot be split without introducing a
/// second durable point-to-row mapping contract.
fn hnsw_partition_build_groups(
    rowsets: &BTreeMap<u64, RowsetSharedPtr>,
    tail_window: &[TailPendingEntry],
    target_graph_rows: u64,
) -> Result<Vec<Vec<ArtifactSegmentRef>>> {
    if target_graph_rows == 0 {
        return Err(paro_error::internal(
            "validated HNSW generation layout has a zero graph target",
        ));
    }
    let requested_segments = tail_window
        .iter()
        .filter(|entry| entry.mutation != TailMutationKind::Delete)
        .flat_map(|entry| {
            entry
                .segment_ids
                .iter()
                .map(|segment_id| ArtifactSegmentRef {
                    rowset_id: entry.rowset_id,
                    segment_id: *segment_id,
                })
        })
        .collect::<BTreeSet<_>>();
    let mut groups = Vec::new();
    let mut current = Vec::new();
    let mut current_rows = 0u64;
    for segment_ref in requested_segments {
        let Some(rowset) = rowsets.get(&segment_ref.rowset_id) else {
            // A durable tail manifest may still name a rowset that table
            // compaction replaced before this catch-up snapshot was captured.
            // `rowsets` is the stable physical input owned by this build, so
            // stale manifest identities are outside the requested generation
            // shard rather than evidence that its input is corrupt. Publish
            // rebasing later proves that every live input selected here is
            // represented exactly once.
            continue;
        };
        rowset.load()?;
        let row_count = rowset
            .segments()
            .iter()
            .find(|segment| segment.segment_id() == segment_ref.segment_id)
            .map(|segment| segment.num_rows() as u64)
            .ok_or_else(|| {
                paro_error::data_corrupted(format!(
                    "HNSW shard layout is missing segment {}/{}",
                    segment_ref.rowset_id, segment_ref.segment_id
                ))
            })?;
        if row_count == 0 {
            continue;
        }
        if !current.is_empty() && current_rows.saturating_add(row_count) > target_graph_rows {
            groups.push(std::mem::take(&mut current));
            current_rows = 0;
        }
        current.push(segment_ref);
        current_rows = current_rows
            .checked_add(row_count)
            .ok_or_else(|| paro_error::out_of_range("HNSW generation shard row count overflow"))?;
    }
    if !current.is_empty() {
        groups.push(current);
    }
    Ok(groups)
}

fn build_hnsw_partition_sidecar_artifact(
    definition: &super::capability::SearchIndexDefinition,
    rowsets: &BTreeMap<u64, RowsetSharedPtr>,
    requested_segments: &[ArtifactSegmentRef],
    budget: &BuildBudget,
    stop_check: Option<&super::inline_sink::SearchBuildStopCheck>,
    workspace_dir: &Path,
    execution_policy: HnswBuildExecutionPolicy,
) -> Result<Option<HnswPartitionArtifact>> {
    let column_id = definition
        .column_ids
        .first()
        .copied()
        .ok_or_else(|| paro_error::invalid_input("HNSW sidecar definition has no column"))?;
    let provider = definition.hnsw_provider_config()?;
    let dimension = provider.dimension as usize;
    let mut vector_partitions = Vec::<std::sync::Arc<dyn VectorStorage>>::new();
    let mut point_count = 0u32;
    let mut coverage = Vec::new();
    let mut partition_segments = Vec::new();
    for &segment_ref in requested_segments {
        if let Some(stop_check) = stop_check {
            stop_check.check()?;
        }
        check_deadline(budget.deadline)?;
        let Some(rowset) = rowsets.get(&segment_ref.rowset_id) else {
            continue;
        };
        rowset.load()?;
        let segment = rowset
            .segments()
            .iter()
            .find(|segment| segment.segment_id() == segment_ref.segment_id)
            .cloned()
            .ok_or_else(|| {
                paro_error::data_corrupted(format!(
                    "HNSW partition build missing segment {}/{}",
                    segment_ref.rowset_id, segment_ref.segment_id
                ))
            })?;
        let column_meta = segment.get_column_meta(column_id).ok_or_else(|| {
            paro_error::column_not_found(format!(
                "column {column_id} not found in segment {}",
                segment_ref.segment_id
            ))
        })?;
        if column_meta.num_rows == 0 {
            continue;
        }
        let schema_column = segment.schema().column_by_id(column_id).ok_or_else(|| {
            paro_error::column_not_found(format!("column {column_id} not found in segment schema"))
        })?;
        let segment_dimension = hnsw_vector_dimension(&schema_column.logical_type, column_id)?;
        if segment_dimension != dimension {
            return Err(paro_error::data_corrupted(format!(
                "HNSW partition dimension mismatch: definition={dimension}, segment={segment_dimension}"
            )));
        }
        let point_base = point_count;
        let segment_vectors =
            open_plain_vector_column_pages(segment.file_path(), column_meta, dimension)?;
        let total_points = u64::from(point_base)
            .checked_add(column_meta.num_rows)
            .ok_or_else(|| paro_error::out_of_range("HNSW partition point count overflow"))?;
        if total_points > u64::from(u32::MAX) {
            return Err(paro_error::configuration_limit_exceeded(
                "HNSW partition exceeds the u32 point-id domain",
            ));
        }
        point_count = u32::try_from(total_points).map_err(|_| {
            paro_error::configuration_limit_exceeded(
                "HNSW partition exceeds the u32 point-id domain",
            )
        })?;
        vector_partitions.extend(segment_vectors);
        coverage.push(ArtifactSegmentSpan {
            segment: segment_ref,
            row_count: column_meta.num_rows,
        });
        partition_segments.push(segment);
    }

    if coverage.is_empty() {
        return Ok(None);
    }
    let coverage = SearchPartitionCoverage::try_new(coverage)?;
    let vector_storage = std::sync::Arc::new(PartitionedVectorStorage::try_new(
        vector_partitions,
        dimension,
    )?);
    // Scalar dictionary ordinals are segment-local identities. Build one
    // partition-owned dictionary over the canonical concatenated row domain;
    // merely offsetting point ids would make ordinal 0 from every segment
    // alias and invalidate predicate covering scans.
    let filter_blocks = build_hnsw_filter_blocks_for_segments(
        partition_segments.iter().map(AsRef::as_ref),
        &provider.build_contract().filter_topology,
    )?;
    let mut builder = HnswBuilder::new()
        .with_workspace_dir(workspace_dir)
        .with_execution_policy(execution_policy);
    if let Some(stop_check) = stop_check.cloned() {
        builder =
            builder.with_stop_check(HnswBuildStopCheck::new(move || stop_check.should_stop()));
    }
    let index = builder.build_with_filter_blocks(
        vector_storage,
        provider.build_contract(),
        filter_blocks,
    )?;
    let provider_stats = HnswProviderStats::from(&HnswIndexStatistics::collect(&index)?);

    Ok(Some(HnswPartitionArtifact {
        coverage,
        column_id,
        index,
        provider_stats,
    }))
}

fn build_segment_sidecar_artifact(
    definition: &super::capability::SearchIndexDefinition,
    generation_id: u64,
    rowset: &RowsetSharedPtr,
    segment_id: u32,
) -> Result<super::inline_sink::InlineArtifactBuildResult> {
    let column_id = definition
        .column_ids
        .first()
        .copied()
        .ok_or_else(|| paro_error::invalid_input("search sidecar definition has no column"))?;
    let segment = rowset
        .segments()
        .iter()
        .find(|segment| segment.segment_id() == segment_id)
        .cloned()
        .ok_or_else(|| {
            paro_error::internal(format!(
                "search sidecar build missing segment {} in rowset {}",
                segment_id,
                rowset.rowset_id()
            ))
        })?;
    let ctx = SegmentFlushCtx {
        rowset_id: rowset.rowset_id(),
        segment_id,
        definition,
        generation_id,
        flush_mode: FlushSearchMode::TailOnly,
        admission: None,
        staging_dir: Path::new(""),
        column_schema: segment.schema().columns(),
    };
    let builder: Box<dyn InlineArtifactBuilder> = match definition.kind {
        SearchIndexKind::FullText => Box::<FullTextInlineArtifactBuilder>::default(),
        SearchIndexKind::Sparse => Box::<SparseInlineArtifactBuilder>::default(),
        SearchIndexKind::Hnsw => {
            return Err(paro_error::not_supported(
                "HNSW sidecar build must use the HNSW sidecar artifact builder",
            ));
        }
    };
    let mut sink = builder.open_sink(&ctx)?;
    let mut iter = SegmentIterator::new_with_delete_vector(&segment, vec![column_id], None)?;
    loop {
        let (rowids, columns) = iter.next_batch(SIDECAR_BUILD_BATCH_ROWS)?;
        if rowids.is_empty() {
            break;
        }
        let rows = rowids.len();
        let base_row_id = contiguous_base_row_id(&rowids)?;
        let column_batch = columns
            .into_iter()
            .find_map(|(candidate, batch)| (candidate == column_id).then_some(batch))
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "search sidecar build missing column {} batch",
                    column_id
                ))
            })?;
        let column_data = column_batch_to_column_data(column_batch, rows)?;
        sink.append_chunk(SegmentChunkInput {
            base_row_id,
            columns: &[column_data],
            column_ids: Some(&[column_id]),
        })?;
    }
    sink.finish()
}

#[cfg(test)]
fn build_hnsw_filter_blocks(
    segment: &crate::rowset::segment::Segment,
    topology: &HnswFilterTopologyContract,
) -> Result<HnswFilterBlocks> {
    build_hnsw_filter_blocks_for_segments(std::iter::once(segment), topology)
}

fn build_hnsw_filter_blocks_for_segments<'a>(
    segments: impl IntoIterator<Item = &'a crate::rowset::segment::Segment>,
    topology: &HnswFilterTopologyContract,
) -> Result<HnswFilterBlocks> {
    if !topology.is_enabled() {
        return Ok(HnswFilterBlocks::default());
    }

    let segments = segments.into_iter().collect::<Vec<_>>();
    let first_segment = segments.first().copied().ok_or_else(|| {
        paro_error::invalid_input("HNSW filter topology requires at least one segment")
    })?;

    let mut collectors = topology
        .columns()
        .iter()
        .map(|&column_id| {
            let logical_type = first_segment
                .schema()
                .column_by_id(column_id)
                .ok_or_else(|| {
                    paro_error::column_not_found(format!(
                        "HNSW filter column {column_id} not found in segment schema"
                    ))
                })?
                .logical_type
                .clone();
            if !crate::index::supports_ordered_bytes(&logical_type) {
                return Err(paro_error::not_supported(format!(
                    "HNSW filter column {column_id} has unsupported type {logical_type:?}"
                )));
            }
            Ok((column_id, logical_type, BitmapIndexWriter::new()))
        })
        .collect::<Result<Vec<_>>>()?;
    let column_ids = collectors
        .iter()
        .map(|(column_id, _, _)| *column_id)
        .collect::<Vec<_>>();
    for segment in segments {
        for (column_id, logical_type, _) in &collectors {
            let segment_type = &segment
                .schema()
                .column_by_id(*column_id)
                .ok_or_else(|| {
                    paro_error::column_not_found(format!(
                        "HNSW filter column {column_id} not found in segment schema"
                    ))
                })?
                .logical_type;
            if segment_type != logical_type {
                return Err(paro_error::data_corrupted(format!(
                    "HNSW partition filter column {column_id} type changed between segments"
                )));
            }
        }
        let mut iter = SegmentIterator::new_with_delete_vector(segment, column_ids.clone(), None)?;
        let mut next_row_id = 0u32;
        loop {
            let (row_ids, mut batches) = iter.next_batch(SIDECAR_BUILD_BATCH_ROWS)?;
            if row_ids.is_empty() {
                break;
            }
            let base_row_id = contiguous_base_row_id(&row_ids)?;
            if base_row_id != next_row_id {
                return Err(paro_error::data_corrupted(format!(
                    "HNSW filter-block scan is not a dense segment domain: expected row {next_row_id}, got {base_row_id}"
                )));
            }
            next_row_id = next_row_id
                .checked_add(row_ids.len() as u32)
                .ok_or_else(|| paro_error::out_of_range("HNSW filter-block row id overflow"))?;
            for (column_id, logical_type, collector) in &mut collectors {
                let position = batches
                    .iter()
                    .position(|(candidate, _)| candidate == column_id)
                    .ok_or_else(|| {
                        paro_error::internal(format!(
                            "HNSW filter-block scan omitted column {column_id}"
                        ))
                    })?;
                let (_, batch) = batches.swap_remove(position);
                append_hnsw_filter_batch(collector, batch, logical_type, row_ids.len())?;
            }
        }
        if u64::from(next_row_id) != segment.num_rows() {
            return Err(paro_error::data_corrupted(format!(
                "HNSW filter-block scan covered {next_row_id} rows, segment has {}",
                segment.num_rows()
            )));
        }
    }

    collectors
        .into_iter()
        .map(|(column_id, logical_type, collector)| {
            Ok(HnswFilterColumnBlocks {
                column_id,
                blocks: collector
                    .ordered_hnsw_filter_blocks(&logical_type, topology.target_block_rows as usize)?
                    .into_iter()
                    .map(|block| HnswFilterBlock {
                        dictionary_ordinals: block.dictionary_ordinals,
                        dictionary_values: block.dictionary_values,
                        ordinal_row_counts: block.ordinal_row_counts,
                        ordinal_fingerprints: block.ordinal_fingerprints,
                        point_ids: block.row_ids,
                    })
                    .collect(),
            })
        })
        .collect::<Result<Vec<_>>>()
        .map(|columns| HnswFilterBlocks { columns })
}

fn append_hnsw_filter_batch(
    collector: &mut BitmapIndexWriter,
    batch: ColumnBatch,
    logical_type: &LogicalType,
    rows: usize,
) -> Result<()> {
    let nulls = batch.nulls.as_deref();
    if let Some(dictionary) = batch.storage_dictionary {
        let mut decoder = BinaryPlainPageDecoder::new(dictionary.dictionary);
        decoder.init()?;
        for row in 0..rows {
            if row_is_null(nulls, row) {
                collector.add_nulls(1);
                continue;
            }
            let offset = row
                .checked_mul(std::mem::size_of::<u32>())
                .ok_or_else(|| paro_error::data_corrupted("dictionary code offset overflow"))?;
            let end = offset + std::mem::size_of::<u32>();
            let raw = dictionary.codes.get(offset..end).ok_or_else(|| {
                paro_error::data_corrupted("dictionary code batch shorter than row count")
            })?;
            let code = u32::from_le_bytes(raw.try_into().expect("u32 code"));
            let value = decoder.string_at(code).ok_or_else(|| {
                paro_error::data_corrupted(format!("dictionary code {code} out of range"))
            })?;
            collector.add_value(value.as_ref());
        }
        return Ok(());
    }

    if matches!(
        logical_type,
        LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::Blob
    ) {
        let mut input = batch.data.as_ref();
        for row in 0..rows {
            if input.len() < std::mem::size_of::<u32>() {
                return Err(paro_error::data_corrupted(
                    "variable-length filter batch is missing a value length",
                ));
            }
            let len = u32::from_le_bytes(input[..4].try_into().expect("u32 length")) as usize;
            input = &input[4..];
            let value = input.get(..len).ok_or_else(|| {
                paro_error::data_corrupted("variable-length filter value is truncated")
            })?;
            input = &input[len..];
            if row_is_null(nulls, row) {
                collector.add_nulls(1);
            } else {
                collector.add_value(value);
            }
        }
        if !input.is_empty() {
            return Err(paro_error::data_corrupted(
                "variable-length filter batch has trailing bytes",
            ));
        }
        return Ok(());
    }

    let width = crate::codec::physical_layout::fixed_row_width(logical_type)?;
    let expected = rows
        .checked_mul(width)
        .ok_or_else(|| paro_error::out_of_range("fixed filter batch byte length overflow"))?;
    if batch.data.len() != expected {
        return Err(paro_error::data_corrupted(format!(
            "fixed filter batch has {} bytes, expected {expected}",
            batch.data.len()
        )));
    }
    for row in 0..rows {
        if row_is_null(nulls, row) {
            collector.add_nulls(1);
        } else {
            collector.add_value(&batch.data[row * width..(row + 1) * width]);
        }
    }
    Ok(())
}

fn hnsw_vector_dimension(logical_type: &LogicalType, column_id: u32) -> Result<usize> {
    match logical_type {
        LogicalType::Array(inner, dim) if matches!(**inner, LogicalType::Float) => Ok(*dim),
        other => Err(paro_error::not_supported(format!(
            "HNSW sidecar build requires Array(Float, N), got {:?} for column {}",
            other, column_id
        ))),
    }
}

fn contiguous_base_row_id(rowids: &[crate::rowset::SegmentRowId]) -> Result<u32> {
    let Some(first) = rowids.first().copied() else {
        return Ok(0);
    };
    for (offset, rowid) in rowids.iter().copied().enumerate() {
        let expected = first
            .get()
            .checked_add(offset as u32)
            .ok_or_else(|| paro_error::out_of_range("search sidecar rowid overflow"))?;
        if rowid.get() != expected {
            return Err(paro_error::invalid_input(
                "search sidecar builder requires contiguous segment batches",
            ));
        }
    }
    Ok(first.get())
}

fn column_batch_to_column_data(batch: ColumnBatch, rows: usize) -> Result<ColumnData> {
    let rows_u32 = u32::try_from(rows)
        .map_err(|_| paro_error::out_of_range("search sidecar batch row count exceeds u32"))?;
    let data = if let Some(dictionary) = batch.storage_dictionary {
        expand_storage_dictionary_batch(
            dictionary.dictionary,
            dictionary.codes,
            batch.nulls.as_deref(),
            rows,
        )?
    } else {
        batch.data.to_vec()
    };
    let null_flags = pack_null_flags(batch.nulls.as_deref(), rows);
    if null_flags.iter().any(|byte| *byte != 0) {
        Ok(ColumnData::with_nulls(data, null_flags, rows_u32))
    } else {
        Ok(ColumnData::new(data, rows_u32))
    }
}

fn expand_storage_dictionary_batch(
    dictionary: Bytes,
    codes: Bytes,
    nulls: Option<&[u8]>,
    rows: usize,
) -> Result<Vec<u8>> {
    let mut decoder = BinaryPlainPageDecoder::new(dictionary);
    decoder.init()?;
    let mut out = Vec::new();
    for row in 0..rows {
        if row_is_null(nulls, row) {
            out.extend_from_slice(&0u32.to_le_bytes());
            continue;
        }
        let offset = row
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or_else(|| paro_error::data_corrupted("dictionary code offset overflow"))?;
        let end = offset
            .checked_add(std::mem::size_of::<u32>())
            .ok_or_else(|| paro_error::data_corrupted("dictionary code end overflow"))?;
        if end > codes.len() {
            return Err(paro_error::data_corrupted(
                "dictionary code batch shorter than row count",
            ));
        }
        let code = u32::from_le_bytes(codes[offset..end].try_into().expect("u32 code"));
        let value = decoder.string_at(code).ok_or_else(|| {
            paro_error::data_corrupted(format!("dictionary code {} out of range", code))
        })?;
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(value.as_ref());
    }
    Ok(out)
}

fn pack_null_flags(nulls: Option<&[u8]>, rows: usize) -> Vec<u8> {
    let mut flags = vec![0u8; rows.div_ceil(8)];
    for row in 0..rows {
        if row_is_null(nulls, row) {
            flags[row / 8] |= 1 << (row % 8);
        }
    }
    flags
}

fn row_is_null(nulls: Option<&[u8]>, row: usize) -> bool {
    nulls.and_then(|nulls| nulls.get(row)).copied().unwrap_or(0) != 0
}

fn sidecar_ref_from_blob(
    definition: &super::capability::SearchIndexDefinition,
    rowset_id: u64,
    segment_id: u32,
    blob: InlineArtifactBlob,
    location: super::artifact::ArtifactLocation,
) -> Result<SearchArtifactRef> {
    let coverage = SearchPartitionCoverage::singleton(
        ArtifactSegmentRef {
            rowset_id,
            segment_id,
        },
        blob.stats.row_count,
    )?;
    sidecar_ref_from_partition_blob(definition, coverage, blob, location)
}

fn sidecar_ref_from_hnsw_partition(
    definition: &super::capability::SearchIndexDefinition,
    generation_id: u64,
    coverage: SearchPartitionCoverage,
    column_id: u32,
    provider_stats: HnswProviderStats,
    location: super::artifact::ArtifactLocation,
) -> Result<SearchArtifactRef> {
    let (bytes_on_disk, checksum) = match &location {
        super::artifact::ArtifactLocation::SidecarArtifactFile { len, checksum, .. } => {
            (*len, *checksum)
        }
        super::artifact::ArtifactLocation::Inline { .. } => {
            return Err(paro_error::internal(
                "streamed HNSW partition was not written to a sidecar file",
            ))
        }
    };
    let artifact = SearchArtifactRef {
        definition_id: definition.definition_id,
        generation_id,
        coverage: coverage.clone(),
        column_id,
        kind: SearchIndexKind::Hnsw,
        provider_variant: definition.config_fingerprint as u32,
        artifact_format_version: crate::index::hnsw::HNSW_ARTIFACT_FORMAT_VERSION,
        location,
        stats: SearchArtifactStats {
            row_count: coverage.row_count(),
            bytes_on_disk,
            provider_stats: Some(SearchProviderStats::Hnsw(provider_stats)),
        },
        checksum,
    };
    artifact.validate()?;
    Ok(artifact)
}

fn sidecar_ref_from_partition_blob(
    definition: &super::capability::SearchIndexDefinition,
    coverage: SearchPartitionCoverage,
    blob: InlineArtifactBlob,
    location: super::artifact::ArtifactLocation,
) -> Result<SearchArtifactRef> {
    let bytes_on_disk = blob.bytes.len() as u64;
    let provider_stats = blob.stats.provider_stats.clone();
    let artifact = SearchArtifactRef {
        definition_id: blob.definition_id,
        generation_id: blob.generation_id,
        coverage,
        column_id: blob.column_id,
        kind: blob.kind,
        provider_variant: definition.config_fingerprint as u32,
        artifact_format_version: match blob.kind {
            SearchIndexKind::Hnsw => crate::index::hnsw::HNSW_ARTIFACT_FORMAT_VERSION,
            SearchIndexKind::Sparse | SearchIndexKind::FullText => 1,
        },
        location,
        stats: SearchArtifactStats {
            row_count: blob.stats.row_count,
            bytes_on_disk,
            provider_stats,
        },
        checksum: blob.checksum,
    };
    artifact.validate()?;
    Ok(artifact)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::hnsw::{
        DistanceMetric, HnswSearchFilter, HnswSearchPolicy, HnswSearchStrategy, SearchParams,
    };
    use crate::rowset::page::CompressionType;
    use crate::rowset::segment::{SegmentWriter, SegmentWriterOptions};
    use crate::rowset::{RowsetWriter, RowsetWriterContext};
    use crate::search::capability::{SearchFreshnessPolicy, SearchIndexDefinition};
    use crate::search::tail::{TailEntryId, TailRowImageRef};
    use crate::search::{HnswInlineConfig, HnswProviderConfig, HNSW_PROVIDER_CONFIG_VERSION};
    use crate::tablet::tablet_schema::{KeysType, TabletColumn, TabletSchema};
    use crate::tablet::Version;
    use roaring::RoaringBitmap;
    use std::io::Cursor;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn sidecar_filter_block_scan_covers_the_complete_scalar_column() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("sidecar-filter-blocks.segment");
        let schema = Arc::new(
            TabletSchema::new(
                9,
                vec![
                    TabletColumn::new(0, "bucket", LogicalType::Integer),
                    TabletColumn::new(
                        1,
                        "embedding",
                        LogicalType::Array(Box::new(LogicalType::Float), 2),
                    ),
                ],
                KeysType::DuplicateKeys,
            )
            .unwrap(),
        );
        let options = SegmentWriterOptions::new(0)
            .with_short_key_index(false)
            .with_compression(CompressionType::None);
        let mut writer = SegmentWriter::create(schema, &file_path, options).unwrap();
        let rows = 12_u32;
        let buckets = (0..rows)
            .flat_map(|row| ((row % 3) as i32).to_le_bytes())
            .collect::<Vec<_>>();
        let vectors = (0..rows)
            .flat_map(|row| [row as f32, row.wrapping_mul(3) as f32])
            .flat_map(f32::to_le_bytes)
            .collect::<Vec<_>>();
        writer
            .append_chunk(&[
                ColumnData::new(buckets, rows),
                ColumnData::new(vectors, rows),
            ])
            .unwrap();
        let segment = writer.finalize().unwrap();
        let topology = HnswFilterTopologyContract::from_columns(&[0], 4, 2).unwrap();

        let blocks = build_hnsw_filter_blocks(&segment, &topology).unwrap();
        assert_eq!(blocks.columns.len(), 1);
        assert_eq!(blocks.columns[0].column_id, 0);
        let mut points = blocks.columns[0]
            .blocks
            .iter()
            .flat_map(|block| block.point_ids.iter().copied())
            .collect::<Vec<_>>();
        points.sort_unstable();
        assert_eq!(points, (0..rows).collect::<Vec<_>>());
    }

    #[test]
    fn hnsw_sidecar_uses_stable_generation_shards_over_multi_segment_partitions() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_42");
        let schema = Arc::new(
            TabletSchema::new(
                9,
                vec![
                    TabletColumn::new(0, "bucket", LogicalType::Integer),
                    TabletColumn::new(
                        1,
                        "embedding",
                        LogicalType::Array(Box::new(LogicalType::Float), 2),
                    ),
                ],
                KeysType::DuplicateKeys,
            )
            .unwrap(),
        );
        let context =
            RowsetWriterContext::new(Arc::clone(&schema), 9, Version::singleton(0), &rowset_path)
                .with_rowset_id(42)
                .with_max_rows_per_segment(4)
                .with_short_key_index(false)
                .with_compression(CompressionType::None)
                .with_build_hnsw_indexes(false);
        let mut writer = RowsetWriter::create(context).unwrap();
        for start in [0_u32, 4] {
            let rows = 4_u32;
            let buckets = (start..start + rows)
                .flat_map(|row| ((row % 2) as i32).to_le_bytes())
                .collect::<Vec<_>>();
            let vectors = (start..start + rows)
                .flat_map(|row| [row as f32, row.saturating_mul(row) as f32])
                .flat_map(f32::to_le_bytes)
                .collect::<Vec<_>>();
            writer
                .add_chunk(&[
                    ColumnData::new(buckets, rows),
                    ColumnData::new(vectors, rows),
                ])
                .unwrap();
        }
        let rowset = Arc::new(writer.build().unwrap());
        assert_eq!(rowset.segments().len(), 2);

        let provider = HnswProviderConfig {
            version: HNSW_PROVIDER_CONFIG_VERSION,
            dimension: 2,
            distance: DistanceMetric::Euclidean,
            build_vector_encoding: crate::index::hnsw::HnswBuildVectorEncoding::symmetric_i16(2)
                .unwrap(),
            m: 4,
            ef_construct: 8,
            ef_search: 16,
            rerank_policy: crate::index::hnsw::HnswRerankPolicy::Ef,
            distance_cost: crate::index::hnsw::HnswDistanceCostProfile::default(),
            generation_layout: crate::search::HnswGenerationLayout {
                target_graph_rows: 4,
            },
            maintenance: crate::search::HnswMaintenancePolicy::default(),
            build_seed: 17,
            proposal_wave_max_size: 4,
            warmup_point_count: 4,
            filter_columns: vec![0],
            filter_block_rows: 4,
            filter_m: 4,
            inline_threshold: HnswInlineConfig {
                enabled: false,
                max_vector_count: 0,
                max_graph_memory_bytes: 0,
                max_dimension: 0,
            },
        }
        .validated()
        .unwrap();
        let provider_config = provider.to_value().unwrap();
        let definition = SearchIndexDefinition {
            definition_id: 77,
            table_id: 9,
            name: "multi_segment_hnsw".to_string(),
            kind: SearchIndexKind::Hnsw,
            column_ids: vec![1],
            expression: None,
            provider_config: provider_config.clone(),
            freshness_policy: SearchFreshnessPolicy::Opportunistic,
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::Hnsw,
                &[1],
                None,
                &provider_config,
            ),
        };
        let rowsets = BTreeMap::from([(42, Arc::clone(&rowset))]);
        let tail = [TailPendingEntry {
            entry_id: TailEntryId::UNASSIGNED,
            rowset_id: 42,
            segment_ids: vec![0, 1],
            mutation: TailMutationKind::Append,
            row_count: 8,
            byte_count: 0,
            row_image_ref: Some(TailRowImageRef::WholeRowset),
        }];
        let groups = hnsw_partition_build_groups(&rowsets, &tail, 4).unwrap();
        assert_eq!(
            groups,
            vec![
                vec![ArtifactSegmentRef {
                    rowset_id: 42,
                    segment_id: 0,
                }],
                vec![ArtifactSegmentRef {
                    rowset_id: 42,
                    segment_id: 1,
                }],
            ]
        );
        let mut tail_with_replaced_rowset = tail.to_vec();
        tail_with_replaced_rowset.push(TailPendingEntry {
            entry_id: TailEntryId::UNASSIGNED,
            rowset_id: 99,
            segment_ids: vec![0],
            mutation: TailMutationKind::Append,
            row_count: 4,
            byte_count: 0,
            row_image_ref: Some(TailRowImageRef::WholeRowset),
        });
        assert_eq!(
            hnsw_partition_build_groups(&rowsets, &tail_with_replaced_rowset, 4).unwrap(),
            groups,
            "a manifest tail replaced before the build snapshot must not poison live shard input"
        );
        let partition = build_hnsw_partition_sidecar_artifact(
            &definition,
            &rowsets,
            &[
                ArtifactSegmentRef {
                    rowset_id: 42,
                    segment_id: 0,
                },
                ArtifactSegmentRef {
                    rowset_id: 42,
                    segment_id: 1,
                },
            ],
            &BuildBudget {
                cost_envelope: MaintenanceCost::default(),
                deadline: None,
                grant_id: None,
            },
            None,
            temp_dir.path(),
            HnswBuildExecutionPolicy::Foreground,
        )
        .unwrap()
        .expect("partition artifact");

        assert_eq!(partition.coverage.segments().len(), 2);
        assert_eq!(partition.coverage.row_count(), 8);
        assert_eq!(
            partition.coverage.point_range(ArtifactSegmentRef {
                rowset_id: 42,
                segment_id: 0,
            }),
            Some(0..4)
        );
        assert_eq!(
            partition.coverage.point_range(ArtifactSegmentRef {
                rowset_id: 42,
                segment_id: 1,
            }),
            Some(4..8)
        );

        let mut encoded = Cursor::new(Vec::new());
        partition
            .index
            .serialize_into_seekable(&mut encoded, 0)
            .unwrap();
        let index = crate::index::hnsw::HnswIndex::deserialize(encoded.get_ref()).unwrap();
        assert_eq!(index.vector_storage.num_vectors(), 8);
        assert_eq!(index.vector_storage.vector_dim(), 2);
        let admitted = RoaringBitmap::from_iter([1_u32, 5]);
        let result = index
            .search_one_with_policy_strategy(
                &[5.0, 25.0],
                2,
                &SearchParams {
                    ef: Some(16),
                    rerank_window: None,
                    objective: crate::index::hnsw::HnswSearchObjective::CostOptimized,
                    random_entry_point: Some(false),
                },
                HnswSearchFilter::predicate(&admitted, &[0]),
                &HnswSearchPolicy {
                    ef_search: 16,
                    ..HnswSearchPolicy::default()
                },
                HnswSearchStrategy::AdaptiveFilteredGraph,
                &crate::search::ResourceBudget::default(),
            )
            .unwrap();
        let mut point_ids = result
            .points
            .into_iter()
            .map(|point| point.idx)
            .collect::<Vec<_>>();
        point_ids.sort_unstable();
        assert_eq!(point_ids, vec![1, 5]);

        let builder = ProviderSidecarArtifactBuilder::new(SidecarArtifactStore::new(
            temp_dir.path().join("stable-shards"),
        ));
        let input = SidecarBuildInput {
            definition,
            generation_id: 1,
            tail_window: tail.to_vec(),
            rowset_refs: vec![rowset],
            snapshot_version: 0,
            stop_check: None,
        };
        let estimate = builder.estimate_cost(&input).unwrap();
        let built = builder
            .build(
                input,
                &BuildBudget {
                    cost_envelope: estimate.cost,
                    deadline: None,
                    grant_id: None,
                },
            )
            .unwrap();
        assert_eq!(built.artifact_refs.len(), 2);
        assert!(built
            .artifact_refs
            .iter()
            .all(|artifact| artifact.stats.row_count == 4));
    }
}
