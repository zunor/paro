// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::tablet_reader::TabletReader;
use super::tablet_runtime::TabletRef;
use super::tablet_schema::TabletSchemaRef;
use crate::rowid_resolver;
use crate::rowset::{RowsetSharedPtr, SegmentRowId};
use crate::tablet::ColumnId;
use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, ValidatedVectorSelection, Vector};
use std::collections::HashSet;
use std::sync::Arc;

/// Purpose-built sparse reader for stable tablet RowIDs.
///
/// Unlike [`TabletReader`], this reader does not build scan cursors, schema
/// adapters, predicate state, or output projection maps. Its caller supplies
/// the exact pinned rowset snapshot that produced the RowIDs, so construction
/// is cheap and every materialized vector uses the caller's query allocator.
pub struct TabletRowIdReader {
    tablet: TabletRef,
    schema: TabletSchemaRef,
    rowsets: Vec<RowsetSharedPtr>,
    allocator: Arc<dyn Allocator>,
}

impl std::fmt::Debug for TabletRowIdReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TabletRowIdReader")
            .field("tablet_id", &self.tablet.tablet_id())
            .field("rowsets", &self.rowsets.len())
            .field("allocator", &self.allocator.name())
            .finish()
    }
}

impl TabletRowIdReader {
    pub fn new(
        tablet: TabletRef,
        rowsets: Vec<RowsetSharedPtr>,
        column_ids: &[ColumnId],
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        let schema = tablet
            .schema()
            .ok_or_else(|| paro_error::internal("Tablet schema not available"))?;
        let rowsets = pin_required_rowset_closure(&tablet, rowsets, column_ids)?;
        Ok(Self {
            tablet,
            schema,
            rowsets,
            allocator,
        })
    }

    pub fn get_by_rowids(&self, rowids: &[u64], column_ids: &[ColumnId]) -> Result<Chunk> {
        get_by_rowids(
            &self.tablet,
            &self.schema,
            &self.rowsets,
            self.allocator.clone(),
            rowids,
            column_ids,
            0,
            false,
        )
    }
}

impl TabletReader {
    /// Bulk read by tablet RowIDs. Handles cross-segment routing.
    pub fn get_by_rowids(&self, rowids: &[u64], column_ids: &[ColumnId]) -> Result<Chunk> {
        self.get_by_rowids_internal(rowids, column_ids, 0)
    }

    pub(super) fn get_by_rowids_internal(
        &self,
        rowids: &[u64],
        column_ids: &[ColumnId],
        depth: usize,
    ) -> Result<Chunk> {
        if !self.is_prepared {
            return Err(paro_error::internal("TabletReader not prepared"));
        }

        get_by_rowids(
            &self.tablet,
            &self.schema,
            &self.rowsets,
            self.allocator.clone(),
            rowids,
            column_ids,
            depth,
            true,
        )
    }
}

fn get_by_rowids(
    tablet: &TabletRef,
    schema: &TabletSchemaRef,
    rowsets: &[RowsetSharedPtr],
    allocator: Arc<dyn Allocator>,
    rowids: &[u64],
    column_ids: &[ColumnId],
    depth: usize,
    allow_retained_fallback: bool,
) -> Result<Chunk> {
    let column_types = column_ids
        .iter()
        .map(|&column_id| {
            schema
                .column_by_id(column_id)
                .map(|column| column.logical_type.clone())
                .ok_or_else(|| {
                    paro_error::invalid_input(format!("Column ID {column_id} not found in schema"))
                })
        })
        .collect::<Result<Vec<LogicalType>>>()?;

    if let Some(chunk) = try_get_single_segment_by_rowids(
        tablet,
        rowsets,
        allocator.clone(),
        rowids,
        column_ids,
        &column_types,
        allow_retained_fallback,
    )? {
        return Ok(chunk);
    }

    rowid_resolver::read_chunk_by_rowids_recursive(
        tablet,
        column_ids,
        &column_types,
        rowids,
        allocator,
        depth,
        &|rowset_id| resolve_rowset(tablet, rowsets, rowset_id, allow_retained_fallback),
    )
}

/// Pin only ancestors needed to materialize the reader's fixed column set.
///
/// Full rowsets may retain deep historical lineage for GC bookkeeping, but a
/// sparse fetch whose columns are present must not keep that lineage alive or
/// fail because an irrelevant ancestor was already reclaimed. Partial rowsets
/// are detected before execution and their required ancestry is pinned while
/// the query snapshot is still valid; lookup never consults the mutable
/// retained registry afterward.
fn pin_required_rowset_closure(
    tablet: &TabletRef,
    mut rowsets: Vec<RowsetSharedPtr>,
    column_ids: &[ColumnId],
) -> Result<Vec<RowsetSharedPtr>> {
    let mut pinned = rowsets
        .iter()
        .map(|rowset| rowset.rowset_id())
        .collect::<HashSet<_>>();
    let mut cursor = 0;
    while cursor < rowsets.len() {
        let needs_base = rowset_needs_base_columns(&rowsets[cursor], column_ids)?;
        let source_ids = rowsets[cursor].rowset_meta().source_rowset_ids().to_vec();
        cursor += 1;
        if !needs_base {
            continue;
        }
        for source_id in source_ids {
            if !pinned.insert(source_id) {
                continue;
            }
            let source = tablet
                .find_retained_rowset_by_id(source_id)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                    "declared base rowset {source_id} is unavailable while pinning rowid snapshot"
                ))
                })?;
            rowsets.push(source);
        }
    }
    Ok(rowsets)
}

fn rowset_needs_base_columns(rowset: &RowsetSharedPtr, column_ids: &[ColumnId]) -> Result<bool> {
    if column_ids.is_empty() || rowset.rowset_meta().source_rowset_ids().is_empty() {
        return Ok(false);
    }
    rowset.load()?;
    Ok(rowset.segments().iter().any(|segment| {
        column_ids
            .iter()
            .any(|column_id| segment.get_column_meta(*column_id).is_none())
    }))
}

fn resolve_rowset(
    tablet: &TabletRef,
    rowsets: &[RowsetSharedPtr],
    rowset_id: u64,
    allow_retained_fallback: bool,
) -> Result<RowsetSharedPtr> {
    rowsets
        .iter()
        .find(|rowset| rowset.rowset_id() == rowset_id)
        .cloned()
        .or_else(|| {
            allow_retained_fallback
                .then(|| tablet.find_retained_rowset_by_id(rowset_id))
                .flatten()
        })
        .ok_or_else(|| {
            paro_error::internal(format!(
                "Rowset {rowset_id} not found while resolving row ids"
            ))
        })
}

/// Fast path for the common TopN/late-fetch shape: every result row lives in
/// one segment and every requested column is physically present there. Decode
/// each column once in physical order, then restore the caller's order with a
/// dictionary selection instead of deep-copying every value.
fn try_get_single_segment_by_rowids(
    tablet: &TabletRef,
    rowsets: &[RowsetSharedPtr],
    allocator: Arc<dyn Allocator>,
    rowids: &[u64],
    column_ids: &[ColumnId],
    column_types: &[LogicalType],
    allow_retained_fallback: bool,
) -> Result<Option<Chunk>> {
    if rowids.is_empty() || column_ids.is_empty() {
        return Ok(None);
    }

    let mut locations = Vec::with_capacity(rowids.len());
    for (original_index, &rowid) in rowids.iter().enumerate() {
        let location = tablet.decode_row_id(crate::primary_key::RowID::from_raw(rowid))?;
        locations.push((location, original_index));
    }
    let segment_key = locations[0].0.segment_key();
    if locations
        .iter()
        .any(|(location, _)| location.segment_key() != segment_key)
    {
        return Ok(None);
    }
    locations.sort_unstable_by_key(|(location, _)| location.row_offset);

    let rowset = resolve_rowset(tablet, rowsets, segment_key.0, allow_retained_fallback)?;
    rowset.load()?;
    let segment = rowset.get_segment(segment_key.1).ok_or_else(|| {
        paro_error::internal(format!(
            "segment {} not found in rowset {} while resolving row ids",
            segment_key.1, segment_key.0
        ))
    })?;
    if column_ids
        .iter()
        .any(|column_id| segment.get_column_meta(*column_id).is_none())
    {
        return Ok(None);
    }

    // Decode each physical row only once. TopN results commonly contain the
    // same dimension row more than once (for example one supplier winning
    // several parts); the dictionary selection below restores duplicates and
    // caller order without re-reading or re-decoding them per payload column.
    let mut row_offsets = Vec::with_capacity(locations.len());
    let mut sorted_to_unique = Vec::with_capacity(locations.len());
    for (location, _) in &locations {
        let unique_index = if row_offsets.last().copied() == Some(location.row_offset) {
            row_offsets.len() - 1
        } else {
            row_offsets.push(location.row_offset);
            row_offsets.len() - 1
        };
        sorted_to_unique.push(unique_index);
    }
    let batches = segment.read_by_rowids(column_ids, SegmentRowId::as_raw_slice(&row_offsets))?;
    let mut sorted_to_original = vec![0u32; locations.len()];
    for (sorted_index, (_, original_index)) in locations.iter().enumerate() {
        sorted_to_original[*original_index] = u32::try_from(sorted_to_unique[sorted_index])
            .map_err(|_| {
                paro_error::out_of_range("row-fetch selection exceeds u32 ordinal domain")
            })?;
    }
    let selection = SelectionVector::try_from_owned_indices(sorted_to_original, allocator.clone())?;
    let selection =
        ValidatedVectorSelection::try_new(selection, row_offsets.len()).map_err(|error| {
            paro_error::data_corrupted(format!("invalid row-fetch permutation: {error}"))
        })?;

    let mut output = Vec::with_capacity(column_ids.len());
    for ((_, logical_type), (_, batch)) in
        column_ids.iter().zip(column_types).zip(batches.into_iter())
    {
        let decoded = crate::codec::vector_decoder::decode_sparse_column_batch(
            logical_type,
            &batch,
            row_offsets.len(),
            allocator.clone(),
        )?;
        output.push(Arc::new(Vector::try_dictionary_from_validated(
            Arc::new(decoded),
            selection.clone(),
        )?));
    }
    Ok(Some(Chunk::try_from_arc_vectors_with_cardinality(
        output,
        rowids.len(),
        allocator,
    )?))
}
