// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::codec::chunk_encoder::encode_chunk;
use crate::compaction::execution::workspace::{
    CompactionBuildOutput, CompactionWorkspace, StagedArtifact,
};
use crate::compaction::plan::types::CompactionPlan;
use crate::compaction::publish::record::{
    PkIndexUpsertCandidate, PkPublishDelta, SegmentDeleteDelta,
};
use crate::primary_key::{DeleteVector, PrimaryKeySerializer};
use crate::rowid_resolver;
use crate::rowset::{Rowset, RowsetSharedPtr, RowsetWriterBuilder, SegmentIterator};
use crate::search::SearchInlineBuilderSet;
use crate::tablet::{tablet_schema::KeysType, ColumnId, PhysicalRowRef, Tablet};
use paro_common::allocator::Allocator;
use paro_common::error::{self as paro_error, Result};
use std::collections::HashMap;
use std::sync::Arc;

pub struct PrimaryKeyMerger;

const COMPACTION_BATCH_SIZE: usize = 4096;

impl PrimaryKeyMerger {
    pub fn build(
        tablet: &Tablet,
        plan: Arc<CompactionPlan>,
        workspace: CompactionWorkspace,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Option<CompactionBuildOutput>> {
        Self::build_with_search_inline_builders(
            tablet,
            plan,
            workspace,
            allocator,
            SearchInlineBuilderSet::default(),
        )
    }

    pub fn build_with_search_inline_builders(
        tablet: &Tablet,
        plan: Arc<CompactionPlan>,
        workspace: CompactionWorkspace,
        allocator: Arc<dyn Allocator>,
        search_inline_builders: SearchInlineBuilderSet,
    ) -> Result<Option<CompactionBuildOutput>> {
        if plan.input_rowsets.is_empty() {
            return Ok(None);
        }

        let schema = tablet
            .schema()
            .ok_or_else(|| paro_error::internal("Tablet schema not available for compaction"))?;
        if schema.keys_type() != KeysType::PrimaryKeys {
            return Err(paro_error::invalid_input(
                "PrimaryKeyMerger only supports PRIMARY_KEYS tablets",
            ));
        }

        let mut writer = RowsetWriterBuilder::new(
            schema.clone(),
            tablet.tablet_id(),
            plan.output_version,
            &workspace.rowset_dir,
        )
        .rowset_id(plan.output_rowset_id)
        .build_hnsw_indexes(false)
        .search_inline_builders(search_inline_builders)
        .build()?;

        let serializer = PrimaryKeySerializer::from_schema_ref(&schema)?;
        let projection: Vec<ColumnId> = schema.columns().iter().map(|c| c.id).collect();
        let output_types = schema.logical_types();
        let visible_rowsets =
            tablet.capture_consistent_rowsets(plan.read_snapshot.visible_version)?;
        let rowset_lookup: HashMap<u64, RowsetSharedPtr> = visible_rowsets
            .into_iter()
            .map(|rowset| (rowset.rowset_id(), rowset))
            .collect();

        let mut keys: Vec<Vec<u8>> = Vec::new();
        let mut source_locations: Vec<PhysicalRowRef> = Vec::new();

        for input in &plan.input_rowsets {
            if workspace.is_cancelled() {
                return Err(paro_error::query_canceled());
            }

            let rowset = &input.rowset;
            rowset.load()?;
            let segments = rowset.segments();
            for segment in segments {
                let segment_projection: Vec<ColumnId> = segment
                    .column_metas()
                    .iter()
                    .map(|meta| meta.column_id)
                    .collect();
                let delete_vector = DeleteVector::load_from_dir_at_version(
                    rowset.rowset_path(),
                    segment.segment_id(),
                    plan.read_snapshot.visible_version,
                )?;
                let mut iter = SegmentIterator::new_with_delete_vector(
                    &segment,
                    segment_projection,
                    delete_vector,
                )?;
                while iter.has_next() {
                    if workspace.is_cancelled() {
                        return Err(paro_error::query_canceled());
                    }

                    let (rowids, _batch) = iter.next_batch(COMPACTION_BATCH_SIZE)?;
                    let rows_read = rowids.len();
                    if rows_read == 0 {
                        continue;
                    }

                    let raw_rowids = rowids
                        .iter()
                        .copied()
                        .map(|row_offset| {
                            tablet
                                .encode_row_location(PhysicalRowRef::new(
                                    rowset.rowset_id(),
                                    segment.segment_id(),
                                    row_offset.get(),
                                ))
                                .map(|rowid| rowid.to_raw())
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let chunk = rowid_resolver::read_chunk_by_rowids_recursive(
                        tablet,
                        &projection,
                        &output_types,
                        &raw_rowids,
                        allocator.clone(),
                        0,
                        &|rowset_id| {
                            rowset_lookup.get(&rowset_id).cloned().ok_or_else(|| {
                                paro_error::internal(format!(
                                    "rowset {} not found while resolving compaction partial rows",
                                    rowset_id
                                ))
                            })
                        },
                    )?;
                    if chunk.size() == 0 {
                        continue;
                    }

                    let logical_types = schema.logical_types();
                    let columns = encode_chunk(&logical_types, &chunk)?;
                    if columns.is_empty() {
                        continue;
                    }

                    let encoded_keys = serializer.encode_chunk(&chunk)?;
                    writer.add_chunk(&columns)?;
                    for (key, &row_id) in encoded_keys
                        .into_iter()
                        .zip(rowids.iter())
                        .take(chunk.size())
                    {
                        keys.push(key);
                        source_locations.push(PhysicalRowRef::new(
                            rowset.rowset_id(),
                            segment.segment_id(),
                            row_id.get(),
                        ));
                    }
                }
            }
        }

        let rowset = writer.build_shared()?;
        rowset.mark_compaction_output(
            plan.input_rowsets
                .iter()
                .map(|input| input.rowset.rowset_id())
                .collect(),
        );

        let row_locations = row_locations_for_rowset(&rowset)?;
        if row_locations.len() != keys.len() {
            return Err(paro_error::internal(format!(
                "Compaction row count mismatch: keys={} rows={}",
                keys.len(),
                row_locations.len()
            )));
        }

        let mut latest: HashMap<Vec<u8>, (PhysicalRowRef, PhysicalRowRef)> = HashMap::new();
        let mut delete_vectors: HashMap<u32, DeleteVector> = HashMap::new();
        for ((key, loc), src_loc) in keys
            .into_iter()
            .zip(row_locations.into_iter())
            .zip(source_locations.into_iter())
        {
            if let Some((prev_out, _prev_src)) = latest.insert(key, (loc, src_loc)) {
                let entry = delete_vectors.entry(prev_out.segment_id).or_default();
                entry.mark_deleted(prev_out.row_offset);
            }
        }

        let pk_delta = PkPublishDelta {
            snapshot_version: plan.read_snapshot.visible_version,
            max_input_version: plan
                .input_rowsets
                .iter()
                .map(|input| input.rowset.end_version())
                .max()
                .unwrap_or(-1),
            upsert_candidates: latest
                .iter()
                .map(
                    |(key, (output_location, source_location))| PkIndexUpsertCandidate {
                        key: key.clone(),
                        output_location: *output_location,
                        source_location: *source_location,
                    },
                )
                .collect(),
            internal_delete_vectors: delete_vectors
                .into_iter()
                .map(|(segment_id, delete_vector)| SegmentDeleteDelta {
                    segment_id,
                    delete_vector,
                })
                .collect(),
        };

        Ok(Some(CompactionBuildOutput::PrimaryKey {
            artifact: StagedArtifact::from_rowset(plan, workspace, rowset)?,
            pk_delta,
        }))
    }
}

fn row_locations_for_rowset(rowset: &Rowset) -> Result<Vec<PhysicalRowRef>> {
    let mut out = Vec::with_capacity(rowset.num_rows() as usize);
    for seg in rowset.segments() {
        let num_rows = seg.num_rows() as u32;
        for row_id in 0..num_rows {
            out.push(PhysicalRowRef::new(
                rowset.rowset_id(),
                seg.segment_id(),
                row_id,
            ));
        }
    }
    Ok(out)
}
