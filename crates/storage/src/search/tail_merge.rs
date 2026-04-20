// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::index::PredicateTree;
use crate::search::cursor::VisibleSegment;
use crate::tablet::tablet_reader::{TabletReader, TabletReaderParams};
use crate::tablet::{ColumnId, PhysicalRowRef, TabletRef};
use paro_common::chunk::Chunk;
use paro_common::error::Result;

pub(crate) fn visible_row_ids(
    segment: &VisibleSegment,
    snapshot_version: i64,
    predicate: Option<&PredicateTree>,
) -> Result<Vec<u32>> {
    let filter = segment
        .segment
        .build_filter_bitmap_with_epoch(snapshot_version as u64, predicate)?;
    if let Some(bitmap) = filter {
        return Ok(bitmap.iter().map(|row_id| row_id as u32).collect());
    }
    Ok((0..segment.segment.num_rows() as u32).collect())
}

pub(crate) fn resolve_logical_rows(
    tablet: &TabletRef,
    snapshot_version: i64,
    segment: &VisibleSegment,
    row_ids: &[u32],
    column_id: ColumnId,
) -> Result<Chunk> {
    let mut reader = TabletReader::new(
        tablet.clone(),
        TabletReaderParams::with_version(snapshot_version),
    )?;
    reader.prepare()?;
    let logical_row_ids = row_ids
        .iter()
        .copied()
        .map(|row_id| {
            tablet
                .encode_row_location(PhysicalRowRef::new(
                    segment.rowset_id,
                    segment.segment_id,
                    row_id,
                ))
                .map(|row_id| row_id.to_raw())
        })
        .collect::<Result<Vec<_>>>()?;
    reader.get_by_rowids(&logical_row_ids, &[column_id])
}
