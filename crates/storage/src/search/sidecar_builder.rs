// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Provider sidecar artifact builders.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Instant;

use bytes::Bytes;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

use super::capability::{ArtifactSegmentRef, SearchArtifactRef, SearchIndexKind};
use super::inline_sink::{
    BuildBudget, CostEstimate, FlushSearchMode, InlineArtifactBlob, InlineArtifactBuilder,
    MaintenanceBenefit, MaintenanceCost, SegmentChunkInput, SegmentFlushCtx,
    SidecarArtifactBuildResult, SidecarArtifactBuilder, SidecarBuildInput,
};
use super::providers::fulltext::inline::FullTextInlineArtifactBuilder;
use super::providers::sparse::inline::SparseInlineArtifactBuilder;
use super::sidecar::SidecarArtifactStore;
use super::stats::{HnswProviderStats, SearchArtifactStats, SearchProviderStats};
use super::tail::TailMutationKind;
use crate::index::hnsw::{HnswBuildExecutionPolicy, HnswBuilder};
use crate::index::MmapVectorStorage;
use crate::metrics::{storage_metrics, SearchSidecarBuildMetricKey};
use crate::rowset::column::ColumnBatch;
use crate::rowset::encoding::BinaryPlainPageDecoder;
use crate::rowset::encoding::PLAIN_PAGE_HEADER_SIZE;
use crate::rowset::{ColumnData, RowsetSharedPtr, SegmentIterator};
use crate::statistics::HnswIndexStatistics;

const SIDECAR_BUILD_BATCH_ROWS: usize = 8192;

#[derive(Debug, Clone)]
pub(crate) struct ProviderSidecarArtifactBuilder {
    store: SidecarArtifactStore,
    hnsw_build_execution: HnswBuildExecutionPolicy,
}

impl ProviderSidecarArtifactBuilder {
    pub(crate) fn new(
        store: SidecarArtifactStore,
        hnsw_build_execution: HnswBuildExecutionPolicy,
    ) -> Self {
        Self {
            store,
            hnsw_build_execution,
        }
    }
}

impl SidecarArtifactBuilder for ProviderSidecarArtifactBuilder {
    fn estimate_cost(&self, input: &SidecarBuildInput) -> CostEstimate {
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
        CostEstimate {
            cost: MaintenanceCost {
                cpu_ns: rows.saturating_mul(cpu_per_row),
                io_read_bytes: input.tail_window.iter().map(|entry| entry.byte_count).sum(),
                io_write_bytes: rows.saturating_mul(64),
                memory_peak_bytes: rows.saturating_mul(128),
                publish_bytes: rows.saturating_mul(64),
            },
            benefit: MaintenanceBenefit {
                expected_open_cost_saved_us: rows,
                expected_tail_rows_drained: rows,
                expected_artifact_bytes_reclaimed: 0,
            },
        }
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
        let mut built_segments = BTreeSet::new();

        for entry in &input.tail_window {
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
                    self.hnsw_build_execution,
                )?;
                for blob in result.blobs {
                    let location = writer.append_artifact(&blob.bytes)?;
                    artifact_refs.push(sidecar_ref_from_blob(
                        &input.definition,
                        entry.rowset_id,
                        *segment_id,
                        blob,
                        location,
                    ));
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

fn build_segment_sidecar_artifact(
    definition: &super::capability::SearchIndexDefinition,
    generation_id: u64,
    rowset: &RowsetSharedPtr,
    segment_id: u32,
    hnsw_build_execution: HnswBuildExecutionPolicy,
) -> Result<super::inline_sink::InlineArtifactBuildResult> {
    if matches!(definition.kind, SearchIndexKind::Hnsw) {
        return build_hnsw_segment_sidecar_artifact(
            definition,
            generation_id,
            rowset,
            segment_id,
            hnsw_build_execution,
        );
    }

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

fn build_hnsw_segment_sidecar_artifact(
    definition: &super::capability::SearchIndexDefinition,
    generation_id: u64,
    rowset: &RowsetSharedPtr,
    segment_id: u32,
    execution: HnswBuildExecutionPolicy,
) -> Result<super::inline_sink::InlineArtifactBuildResult> {
    let column_id = definition
        .column_ids
        .first()
        .copied()
        .ok_or_else(|| paro_error::invalid_input("HNSW sidecar definition has no column"))?;
    let segment = rowset
        .segments()
        .iter()
        .find(|segment| segment.segment_id() == segment_id)
        .cloned()
        .ok_or_else(|| {
            paro_error::internal(format!(
                "HNSW sidecar build missing segment {} in rowset {}",
                segment_id,
                rowset.rowset_id()
            ))
        })?;
    let column_meta = segment.get_column_meta(column_id).ok_or_else(|| {
        paro_error::column_not_found(format!(
            "column {} not found in segment {}",
            column_id, segment_id
        ))
    })?;
    if column_meta.num_rows == 0 {
        return Ok(super::inline_sink::InlineArtifactBuildResult {
            blobs: Vec::new(),
            stats_delta: None,
        });
    }

    let schema_column = segment.schema().column_by_id(column_id).ok_or_else(|| {
        paro_error::column_not_found(format!("column {} not found in segment schema", column_id))
    })?;
    let dim = hnsw_vector_dimension(&schema_column.logical_type, column_id)?;
    let vector_storage = std::sync::Arc::new(MmapVectorStorage::open_range(
        segment.file_path(),
        column_meta.data_page_pointer.offset + PLAIN_PAGE_HEADER_SIZE as u64,
        column_meta.num_rows * dim as u64 * std::mem::size_of::<f32>() as u64,
        dim,
    )?);
    let provider = definition.hnsw_provider_config()?;
    if provider.dimension as usize != dim {
        return Err(paro_error::invalid_input(format!(
            "HNSW sidecar dimension mismatch: definition={}, segment={dim}",
            provider.dimension
        )));
    }
    let index = HnswBuilder::new()
        .with_execution_policy(execution)
        .build(vector_storage, provider.build_contract())?;
    let bytes = index.serialize()?;
    let checksum = seahash::hash(&bytes);
    let provider_stats = HnswProviderStats::from(&HnswIndexStatistics::collect(&index));
    let bytes_on_disk = bytes.len() as u64;

    Ok(super::inline_sink::InlineArtifactBuildResult {
        blobs: vec![InlineArtifactBlob {
            definition_id: definition.definition_id,
            generation_id,
            column_id,
            kind: SearchIndexKind::Hnsw,
            bytes,
            stats: SearchArtifactStats {
                row_count: column_meta.num_rows,
                bytes_on_disk,
                provider_stats: Some(SearchProviderStats::Hnsw(provider_stats)),
            },
            checksum,
        }],
        stats_delta: None,
    })
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
) -> SearchArtifactRef {
    let bytes_on_disk = blob.bytes.len() as u64;
    let provider_stats = blob.stats.provider_stats.clone();
    SearchArtifactRef {
        definition_id: blob.definition_id,
        generation_id: blob.generation_id,
        segment: ArtifactSegmentRef {
            rowset_id,
            segment_id,
        },
        column_id: blob.column_id,
        kind: blob.kind,
        provider_variant: definition.config_fingerprint as u32,
        artifact_format_version: 1,
        location,
        stats: SearchArtifactStats {
            row_count: blob.stats.row_count,
            bytes_on_disk,
            provider_stats,
        },
        checksum: blob.checksum,
    }
}
