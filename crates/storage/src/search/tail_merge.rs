// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use crate::index::PredicateTree;
use crate::rowset::SegmentRowId;
use crate::search::cursor::{
    PhysicalRowRef as SearchPhysicalRowRef, SearchReadSnapshot, VisibleSegment,
};
use crate::tablet::tablet_reader::{TabletReader, TabletReaderParams};
use crate::tablet::{ColumnId, PhysicalRowRef, TabletRef};
use paro_common::allocator::{default_allocator, Allocator};
use paro_common::chunk::Chunk;
use paro_common::error::Result;

pub(crate) fn visible_row_ids(
    snapshot: &SearchReadSnapshot,
    segment: &VisibleSegment,
    predicate: Option<&PredicateTree>,
) -> Result<Vec<u32>> {
    let filter = segment
        .segment
        .build_filter_bitmap_with_epoch(snapshot.table.visible_version as u64, predicate)?;
    if let Some(bitmap) = filter {
        if !snapshot.has_overlay_delete_vectors() {
            return Ok(bitmap.iter().map(|row_id| row_id as u32).collect());
        }
        return Ok(bitmap
            .iter()
            .map(|row_id| row_id as u32)
            .filter(|row_id| !is_overlay_deleted(snapshot, segment, *row_id))
            .collect());
    }
    if !snapshot.has_overlay_delete_vectors() {
        return Ok((0..segment.segment.num_rows() as u32).collect());
    }
    Ok((0..segment.segment.num_rows() as u32)
        .filter(|row_id| !is_overlay_deleted(snapshot, segment, *row_id))
        .collect())
}

pub(crate) fn resolve_logical_rows(
    tablet: &TabletRef,
    snapshot: &SearchReadSnapshot,
    segment: &VisibleSegment,
    row_ids: &[u32],
    column_id: ColumnId,
) -> Result<Chunk> {
    resolve_logical_rows_with_allocator(
        tablet,
        snapshot,
        segment,
        row_ids,
        column_id,
        Arc::new(default_allocator()),
    )
}

pub(crate) fn resolve_logical_rows_with_allocator(
    tablet: &TabletRef,
    snapshot: &SearchReadSnapshot,
    segment: &VisibleSegment,
    row_ids: &[u32],
    column_id: ColumnId,
    allocator: Arc<dyn Allocator>,
) -> Result<Chunk> {
    let mut reader = TabletReader::new_with_allocator(
        tablet.clone(),
        TabletReaderParams::with_version(snapshot.table.visible_version),
        allocator,
    )?;
    reader.prepare_with_pinned_rowsets(vec![segment.rowset.clone()])?;
    let logical_row_ids = row_ids
        .iter()
        .copied()
        .map(|row_id| {
            tablet
                .encode_row_location(PhysicalRowRef::new(
                    segment.rowset_id,
                    segment.segment_id,
                    SegmentRowId::from_raw(row_id),
                ))
                .map(|row_id| row_id.to_raw())
        })
        .collect::<Result<Vec<_>>>()?;
    reader.get_by_rowids(&logical_row_ids, &[column_id])
}

/// Decode one immutable segment-local column without constructing the tablet
/// row-id resolver.
///
/// This path is valid only when the selected physical segment owns the stored
/// column and row ids are already in strict segment order. Search prepares
/// those ids from an exact MVCC/predicate proof. Partial-update segments and
/// arbitrary row order return `None` and retain the logical resolver path.
pub(crate) fn resolve_segment_rows_direct(
    segment: &VisibleSegment,
    row_ids: &[u32],
    column_id: ColumnId,
    allocator: Arc<dyn Allocator>,
) -> Result<Option<Chunk>> {
    if row_ids.is_empty()
        || row_ids.windows(2).any(|rows| rows[0] >= rows[1])
        || segment.segment.get_column_meta(column_id).is_none()
    {
        return Ok(None);
    }
    let logical_type = segment
        .segment
        .schema()
        .column_by_id(column_id)
        .map(|column| &column.logical_type)
        .ok_or_else(|| {
            paro_common::error::column_not_found(format!(
                "column {column_id} not found in exact tail segment schema"
            ))
        })?;
    let mut batches = segment.segment.read_by_rowids(&[column_id], row_ids)?;
    if batches.len() != 1 || batches[0].0 != column_id {
        return Err(paro_common::error::data_corrupted(
            "exact tail segment read returned an unexpected column set",
        ));
    }
    let (_, batch) = batches.pop().expect("one exact tail column batch");
    let decoded = crate::codec::vector_decoder::decode_sparse_column_batch(
        logical_type,
        &batch,
        row_ids.len(),
        allocator.clone(),
    )?;
    Ok(Some(Chunk::try_from_arc_vectors_with_cardinality(
        vec![Arc::new(decoded)],
        row_ids.len(),
        allocator,
    )?))
}

#[inline]
fn is_overlay_deleted(
    snapshot: &SearchReadSnapshot,
    segment: &VisibleSegment,
    row_id: u32,
) -> bool {
    snapshot.is_overlay_deleted(SearchPhysicalRowRef::new(
        segment.rowset_id,
        segment.segment_id,
        crate::rowset::SegmentRowId::from_raw(row_id),
    ))
}
