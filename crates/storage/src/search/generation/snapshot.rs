// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Generation snapshot collection from visible rowsets.

use std::collections::BTreeSet;

use paro_common::error::{self as paro_error, Result};

use crate::rowset::{load_base_rowids, RowsetSharedPtr};
use crate::tablet::ColumnId;

use super::stats::{empty_generation_stats_for_definition, merge_provider_stats_into_generation};
use super::view::{coverage_for_definition, execution_modes_for_definition};
use crate::search::artifact::{ArtifactLocation, SegmentPagePointer};
use crate::search::capability::{
    ArtifactSegmentRef, CoverageState, SearchArtifactRef, SearchIndexDefinition, SearchIndexKind,
    SearchPartitionCoverage,
};
use crate::search::lifecycle::publisher::inline_artifact_checksum;
use crate::search::stats::{
    ExecutionModes, GenerationStats, SearchArtifactStats, SearchProviderStats,
};
use crate::search::tail::{
    TailEntryId, TailMutationKind, TailPendingEntry, TailPendingSet, TailRowImageRef,
};

#[derive(Debug, Clone)]
pub(crate) struct VisibleSearchSnapshot {
    pub(crate) visible_version: i64,
    pub(crate) artifacts: Vec<SearchArtifactRef>,
    pub(crate) tail_pending: TailPendingSet,
    pub(crate) coverage: CoverageState,
    pub(crate) generation_stats: GenerationStats,
    pub(crate) execution_modes: ExecutionModes,
    pub(crate) tombstone_rows: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct RowsetSearchSnapshot {
    pub(crate) generation_stats: GenerationStats,
    pub(crate) artifacts: Vec<SearchArtifactRef>,
    pub(crate) tail_entries: TailPendingSet,
}

pub(crate) fn collect_visible_snapshot(
    definition: &SearchIndexDefinition,
    visible_version: i64,
    visible_rowsets: &[RowsetSharedPtr],
) -> Result<VisibleSearchSnapshot> {
    let mut generation_stats = empty_generation_stats_for_definition(definition)?;
    let mut artifacts = Vec::new();
    let mut tail_entries = Vec::new();

    for rowset in visible_rowsets {
        rowset.load()?;
        let rowset_snapshot = collect_rowset_snapshot(definition, rowset, visible_version)?;
        generation_stats.merge_assign(&rowset_snapshot.generation_stats);
        artifacts.extend(rowset_snapshot.artifacts);
        tail_entries.extend(rowset_snapshot.tail_entries.entries);
    }

    let tail_pending = TailPendingSet {
        entries: tail_entries,
    };
    let coverage = coverage_for_definition(definition, &tail_pending);
    let execution_modes = execution_modes_for_definition(definition, &coverage);
    let tombstone_rows = tail_pending.delete_rows();
    if visible_rowsets.is_empty() {
        return Ok(VisibleSearchSnapshot {
            visible_version,
            artifacts: Vec::new(),
            tail_pending,
            coverage,
            generation_stats,
            execution_modes,
            tombstone_rows: 0,
        });
    }

    Ok(VisibleSearchSnapshot {
        visible_version,
        tail_pending,
        coverage,
        tombstone_rows,
        generation_stats,
        execution_modes,
        artifacts,
    })
}

pub(crate) fn collect_rowset_snapshot(
    definition: &SearchIndexDefinition,
    rowset: &RowsetSharedPtr,
    visible_version: i64,
) -> Result<RowsetSearchSnapshot> {
    let mut artifacts = Vec::new();
    let mut generation_stats = empty_generation_stats_for_definition(definition)?;
    let mut delete_entries = Vec::new();
    let mut missing_segments = Vec::new();

    for segment in rowset.segments() {
        let deleted_rows = segment
            .load_delete_vector_with_epoch(visible_version as u64)?
            .map(|delete_vector| delete_vector.bitmap().len() as u64)
            .unwrap_or_default();
        let segment_rows = u64::try_from(segment.num_rows()).unwrap_or_default();
        let live_rows = segment_rows.saturating_sub(deleted_rows);
        if deleted_rows > 0 {
            delete_entries.push(TailPendingEntry {
                entry_id: TailEntryId::UNASSIGNED,
                rowset_id: rowset.rowset_id(),
                segment_ids: vec![segment.segment_id()],
                mutation: TailMutationKind::Delete,
                row_count: deleted_rows,
                byte_count: segment.file_size(),
                row_image_ref: None,
            });
        }

        let mut segment_complete = true;
        let mut segment_artifacts = Vec::new();
        for column_id in &definition.column_ids {
            let artifact = segment_artifact(definition, rowset, segment.segment_id(), *column_id)?;
            let Some(artifact) = artifact else {
                segment_complete = false;
                break;
            };
            segment_artifacts.push(artifact);
        }
        if !segment_complete {
            missing_segments.push(segment.segment_id());
            continue;
        }
        generation_stats.indexed_rows = generation_stats.indexed_rows.saturating_add(live_rows);
        generation_stats.artifact_count = generation_stats
            .artifact_count
            .saturating_add(segment_artifacts.len());
        merge_provider_stats_into_generation(
            &mut generation_stats,
            segment_artifacts
                .iter()
                .filter_map(|artifact| artifact.stats.provider_stats.as_ref().cloned()),
        );
        artifacts.extend(segment_artifacts);
    }

    let mut tail_entries = delete_entries;
    if !missing_segments.is_empty() {
        tail_entries.push(rowset_tail_entry(
            rowset,
            &missing_segments,
            visible_version,
        )?);
    }

    Ok(RowsetSearchSnapshot {
        generation_stats,
        artifacts,
        tail_entries: TailPendingSet {
            entries: tail_entries,
        },
    })
}

fn segment_artifact(
    definition: &SearchIndexDefinition,
    rowset: &RowsetSharedPtr,
    segment_id: u32,
    column_id: ColumnId,
) -> Result<Option<SearchArtifactRef>> {
    let rowset_id = rowset.rowset_id();
    let segment = rowset
        .segments()
        .iter()
        .find(|segment| segment.segment_id() == segment_id)
        .cloned()
        .ok_or_else(|| {
            paro_error::internal(format!(
                "segment {} missing from rowset {}",
                segment_id, rowset_id
            ))
        })?;

    let pointer = match definition.kind {
        SearchIndexKind::Hnsw => {
            let expected = definition.hnsw_provider_config()?.build_contract();
            if !segment.hnsw_artifact_matches_contract(column_id, &expected)? {
                return Ok(None);
            }
            segment
                .get_column_meta(column_id)
                .and_then(|meta| meta.hnsw_index_pointer)
        }
        SearchIndexKind::Sparse => {
            if segment.sparse_index(column_id).is_none() {
                return Ok(None);
            }
            segment
                .get_column_meta(column_id)
                .and_then(|meta| meta.sparse_index_pointer)
        }
        SearchIndexKind::FullText => {
            let Some(index) = segment.fulltext_index(column_id) else {
                return Ok(None);
            };
            let expected_config = definition.fulltext_provider_config()?.config;
            let actual = index.tokenizer().kind().config_name();
            if !actual.eq_ignore_ascii_case(&expected_config) {
                return Ok(None);
            }
            segment
                .get_column_meta(column_id)
                .and_then(|meta| meta.fulltext_index_pointer)
        }
    };
    let Some(pointer) = pointer.filter(|pointer| pointer.is_valid()) else {
        return Ok(None);
    };

    let (bytes_on_disk, provider_stats) =
        search_artifact_metadata(definition.kind, &segment, column_id);
    let checksum = inline_artifact_checksum(
        definition.definition_id,
        definition.config_fingerprint,
        rowset_id,
        segment_id,
        column_id,
        definition.kind,
        pointer.offset,
        pointer.size,
    );

    let artifact = SearchArtifactRef {
        definition_id: definition.definition_id,
        generation_id: 0,
        coverage: SearchPartitionCoverage::singleton(
            ArtifactSegmentRef {
                rowset_id,
                segment_id,
            },
            segment.num_rows(),
        )?,
        column_id,
        kind: definition.kind,
        provider_variant: definition.config_fingerprint as u32,
        artifact_format_version: 1,
        location: ArtifactLocation::Inline {
            page: SegmentPagePointer {
                rowset_id,
                segment_id,
                column_id,
                page_offset: pointer.offset,
                page_len: pointer.size as u64,
                checksum,
            },
        },
        stats: SearchArtifactStats {
            row_count: segment.num_rows(),
            bytes_on_disk,
            provider_stats,
        },
        checksum,
    };
    artifact.validate()?;
    Ok(Some(artifact))
}

fn rowset_tail_entry(
    rowset: &RowsetSharedPtr,
    missing_segments: &[u32],
    visible_version: i64,
) -> Result<TailPendingEntry> {
    let mut touched_columns = BTreeSet::new();
    let mut base_rowids_segments = Vec::new();
    let mut row_count = 0u64;
    let mut byte_count = 0u64;

    for segment in rowset.segments() {
        if !missing_segments.contains(&segment.segment_id()) {
            continue;
        }
        let deleted_rows = segment
            .load_delete_vector_with_epoch(visible_version as u64)?
            .map(|delete_vector| delete_vector.bitmap().len() as u64)
            .unwrap_or_default();
        let segment_rows = u64::try_from(segment.num_rows()).unwrap_or_default();
        row_count = row_count.saturating_add(segment_rows.saturating_sub(deleted_rows));
        byte_count = byte_count.saturating_add(segment.file_size());
        for meta in segment.column_metas() {
            touched_columns.insert(meta.column_id);
        }
        if load_base_rowids(rowset.rowset_path(), segment.segment_id())?.is_some() {
            base_rowids_segments.push(segment.segment_id());
        }
    }

    let row_image_ref = if base_rowids_segments.is_empty() {
        Some(TailRowImageRef::WholeRowset)
    } else {
        Some(TailRowImageRef::PartialRowset {
            touched_columns: touched_columns.into_iter().collect(),
            base_rowids_segments: base_rowids_segments.clone(),
        })
    };

    Ok(TailPendingEntry {
        entry_id: TailEntryId::UNASSIGNED,
        rowset_id: rowset.rowset_id(),
        segment_ids: missing_segments.to_vec(),
        mutation: if base_rowids_segments.is_empty() {
            TailMutationKind::Append
        } else {
            TailMutationKind::Replace
        },
        row_count,
        byte_count,
        row_image_ref,
    })
}

fn search_artifact_metadata(
    kind: SearchIndexKind,
    segment: &crate::rowset::SegmentSharedPtr,
    column_id: ColumnId,
) -> (u64, Option<SearchProviderStats>) {
    let bytes_on_disk = segment
        .get_column_meta(column_id)
        .and_then(|meta| match kind {
            SearchIndexKind::Hnsw => meta.hnsw_index_pointer.map(|ptr| ptr.size as u64),
            SearchIndexKind::Sparse => meta.sparse_index_pointer.map(|ptr| ptr.size as u64),
            SearchIndexKind::FullText => meta.fulltext_index_pointer.map(|ptr| ptr.size as u64),
        })
        .unwrap_or(0);
    let provider_stats = match kind {
        SearchIndexKind::FullText => segment
            .fulltext_index_statistics(column_id)
            .map(|stats| SearchProviderStats::FullText((&stats).into())),
        SearchIndexKind::Sparse => segment
            .sparse_index_statistics(column_id)
            .map(|stats| SearchProviderStats::Sparse(stats.into())),
        SearchIndexKind::Hnsw => segment
            .hnsw_index_statistics(column_id)
            .map(|stats| SearchProviderStats::Hnsw((&stats).into())),
    };
    (bytes_on_disk, provider_stats)
}
