// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::index::PredicateTree;
use crate::rowset::SegmentRowId;
use crate::search::cursor::{
    PhysicalRowRef as SearchPhysicalRowRef, SearchReadSnapshot, VisibleSegment,
};
use crate::tablet::tablet_reader::{TabletReader, TabletReaderParams};
use crate::tablet::{ColumnId, PhysicalRowRef, TabletRef};
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
    let mut reader = TabletReader::new(
        tablet.clone(),
        TabletReaderParams::with_version(snapshot.table.visible_version),
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

#[inline]
fn is_overlay_deleted(
    snapshot: &SearchReadSnapshot,
    segment: &VisibleSegment,
    row_id: u32,
) -> bool {
    snapshot.is_overlay_deleted(SearchPhysicalRowRef::new(
        segment.rowset_id,
        segment.segment_id,
        row_id,
    ))
}
