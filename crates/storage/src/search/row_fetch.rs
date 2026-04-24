// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Late materialization helpers for fetching projected search rows.

use std::collections::HashMap;

use crate::codec::{cell_decoder::decode_cell_into_vector, physical_layout};
use crate::rowset::RowsetId;
use crate::search::{CandidateBatch, PhysicalRowRef, SearchReadSnapshot};
use crate::tablet::TabletRef;
use paro_common::allocator::{default_allocator, Allocator};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ProjectedCell {
    pub(crate) bytes: Vec<u8>,
    pub(crate) is_null: bool,
}

impl ProjectedCell {
    pub(crate) fn null() -> Self {
        Self {
            bytes: Vec::new(),
            is_null: true,
        }
    }
}

#[inline]
pub(crate) fn snapshot_epoch(version: i64) -> u64 {
    if version < 0 {
        0
    } else {
        version as u64
    }
}

fn row_cache_key(row: PhysicalRowRef) -> (RowsetId, u32, u64) {
    (row.rowset_id, row.segment_id, row.row_id as u64)
}

pub(crate) fn materialize_column(
    tablet: &TabletRef,
    logical_type: &LogicalType,
    column_idx: usize,
    result_col_idx: usize,
    rows: &[PhysicalRowRef],
    data_cache: &HashMap<(RowsetId, u32, u64), Vec<ProjectedCell>>,
    allocator: Arc<dyn Allocator>,
) -> Result<Vector> {
    let row_count = rows.len();
    let mut vector = Vector::try_new(logical_type.clone(), row_count, allocator)?;
    let is_nullable = tablet
        .schema()
        .and_then(|schema| schema.column(column_idx).map(|col| col.is_nullable))
        .unwrap_or(true);

    for (row_idx, row) in rows.iter().enumerate() {
        let row_data = data_cache
            .get(&row_cache_key(*row))
            .ok_or_else(|| paro_error::internal("Missing projected row data"))?;
        let cell = row_data.get(result_col_idx).ok_or_else(|| {
            paro_error::internal("Projected column index out of bounds in row cache")
        })?;

        if cell.is_null {
            if is_nullable {
                vector.set_null(row_idx, true);
                continue;
            }
            return Err(paro_error::data_corrupted(format!(
                "projection decode failed: non-nullable column {} is NULL",
                column_idx
            )));
        }

        let bytes = cell.bytes.as_slice();
        if bytes.is_empty() {
            if is_nullable {
                vector.set_null(row_idx, true);
                continue;
            }
            return Err(paro_error::data_corrupted(format!(
                "projection decode failed: non-nullable column {} has empty payload",
                column_idx
            )));
        }

        decode_cell_into_vector(logical_type, bytes, &mut vector, row_idx)?;
    }

    vector.set_count(row_count);
    Ok(vector)
}

pub(crate) fn fetch_projected_columns(
    snapshot: &SearchReadSnapshot,
    rows: &[PhysicalRowRef],
    projected_columns: &[usize],
    column_types: &[LogicalType],
) -> Result<HashMap<(RowsetId, u32, u64), Vec<ProjectedCell>>> {
    let mut fetch_map = HashMap::new();
    for row in rows {
        let segment = snapshot.table_lease.resolve_segment(*row)?;
        let entry = fetch_map
            .entry(row.segment_key())
            .or_insert_with(|| (segment, Vec::new()));
        entry.1.push(row.row_id as u64);
    }

    let mut data_cache: HashMap<(RowsetId, u32, u64), Vec<ProjectedCell>> = HashMap::new();

    for ((rowset_id, segment_id), (segment, row_ids)) in fetch_map {
        for (result_col_idx, &projected_column_id) in projected_columns.iter().enumerate() {
            let mut iter = segment.new_column_iterator(projected_column_id as u32)?;
            let logical_type = column_types
                .get(projected_column_id)
                .ok_or_else(|| paro_error::internal("Invalid projected column index"))?;
            let fixed_row_width = physical_layout::fixed_row_width(logical_type).ok();

            if let Some(type_size) = fixed_row_width {
                let batch = iter.read_by_rowids(&row_ids)?;
                let bytes = batch.data;
                let nulls = batch.nulls.as_deref();
                for (row_offset, row_id) in row_ids.iter().enumerate() {
                    let start = row_offset * type_size;
                    let end = start + type_size;
                    let value = bytes.slice(start..end).to_vec();
                    let is_null = nulls
                        .and_then(|bitmap| bitmap.get(row_offset))
                        .copied()
                        .unwrap_or(0)
                        != 0;
                    data_cache
                        .entry((rowset_id, segment_id, *row_id))
                        .or_insert_with(|| vec![ProjectedCell::null(); projected_columns.len()])
                        [result_col_idx] = ProjectedCell {
                        bytes: value,
                        is_null,
                    };
                }
            } else {
                // Variable-length columns still fall back to per-row seeks today.
                // Sort rowids first so the iterator mostly moves forward within a segment.
                let mut sorted_row_ids = row_ids.clone();
                sorted_row_ids.sort_unstable();
                for row_id in sorted_row_ids {
                    iter.seek_to_ordinal(row_id)?;
                    let (_, batch) = iter.next_batch(1)?;
                    let cell = batch.varlen_row(0)?;
                    let is_null = cell.is_none();
                    let bytes = if let Some(cell) = cell {
                        let mut encoded = Vec::with_capacity(4 + cell.len());
                        encoded.extend_from_slice(&(cell.len() as u32).to_le_bytes());
                        encoded.extend_from_slice(cell.as_ref());
                        encoded
                    } else {
                        Vec::new()
                    };
                    data_cache
                        .entry((rowset_id, segment_id, row_id))
                        .or_insert_with(|| vec![ProjectedCell::null(); projected_columns.len()])
                        [result_col_idx] = ProjectedCell { bytes, is_null };
                }
            }
        }
    }

    Ok(data_cache)
}

pub(crate) fn materialize_candidate_batch(
    tablet: &TabletRef,
    column_types: &[LogicalType],
    snapshot: &SearchReadSnapshot,
    batch: CandidateBatch,
    projected_columns: &[usize],
    emit_score: bool,
) -> Result<Chunk> {
    let row_count = batch.rows.len();
    let allocator: Arc<dyn Allocator> = Arc::new(default_allocator());
    let mut output_vectors = Vec::with_capacity(projected_columns.len() + usize::from(emit_score));
    let data_cache =
        fetch_projected_columns(snapshot, &batch.rows, projected_columns, column_types)?;

    for (result_col_idx, &column_idx) in projected_columns.iter().enumerate() {
        let logical_type = column_types
            .get(column_idx)
            .ok_or_else(|| paro_error::internal("Invalid column index"))?;
        output_vectors.push(materialize_column(
            tablet,
            logical_type,
            column_idx,
            result_col_idx,
            &batch.rows,
            &data_cache,
            allocator.clone(),
        )?);
    }

    if emit_score {
        if !batch.scores.is_empty() && batch.scores.len() != row_count {
            return Err(paro_error::internal(
                "Score vector length mismatch during search materialization",
            ));
        }
        let scores = if batch.scores.is_empty() {
            vec![0.0_f32; row_count]
        } else {
            batch.scores
        };
        output_vectors.push(Vector::try_from_f32(&scores, allocator.clone())?);
    }

    Ok(Chunk::from_vectors(output_vectors, allocator))
}
