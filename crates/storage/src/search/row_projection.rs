// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use crate::codec::{cell_decoder::decode_cell_into_vector, physical_layout};
use crate::rowset::SegmentSharedPtr;
use crate::tablet::TabletRef;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

#[derive(Clone)]
pub(crate) struct ScoredRowRef {
    pub(crate) score: f32,
    pub(crate) segment: SegmentSharedPtr,
    pub(crate) row_id: u32,
}

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

impl ScoredRowRef {
    #[inline]
    pub(crate) fn segment_key(&self) -> usize {
        Arc::as_ptr(&self.segment) as usize
    }
}

impl PartialEq for ScoredRowRef {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for ScoredRowRef {}

impl PartialOrd for ScoredRowRef {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredRowRef {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.segment_key().cmp(&other.segment_key()))
            .then_with(|| self.row_id.cmp(&other.row_id))
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

pub(crate) fn materialize_column(
    tablet: &TabletRef,
    logical_type: &LogicalType,
    column_idx: usize,
    result_col_idx: usize,
    final_order: &[ScoredRowRef],
    data_cache: &HashMap<(usize, u64), Vec<ProjectedCell>>,
) -> Result<Vector> {
    let row_count = final_order.len();
    let mut vector = Vector::with_capacity(logical_type.clone(), row_count);
    let is_nullable = tablet
        .schema()
        .and_then(|schema| schema.column(column_idx).map(|col| col.is_nullable))
        .unwrap_or(true);

    for (row_idx, point) in final_order.iter().enumerate() {
        let row_data = data_cache
            .get(&(point.segment_key(), point.row_id as u64))
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
    final_order: &[ScoredRowRef],
    projected_columns: &[usize],
    column_types: &[LogicalType],
) -> Result<HashMap<(usize, u64), Vec<ProjectedCell>>> {
    let mut fetch_map: HashMap<usize, (SegmentSharedPtr, Vec<u64>)> = HashMap::new();
    for point in final_order {
        let key = point.segment_key();
        let entry = fetch_map
            .entry(key)
            .or_insert_with(|| (point.segment.clone(), Vec::new()));
        entry.1.push(point.row_id as u64);
    }

    let mut data_cache: HashMap<(usize, u64), Vec<ProjectedCell>> = HashMap::new();

    for (segment_key, (segment, row_ids)) in fetch_map {
        for (col_res_idx, &col_proj_id) in projected_columns.iter().enumerate() {
            let col_id = col_proj_id as u32;
            let mut iter = segment.new_column_iterator(col_id)?;
            let logical_type = column_types
                .get(col_proj_id)
                .ok_or_else(|| paro_error::internal("Invalid projected column index"))?;
            let fixed_row_width = physical_layout::fixed_row_width(logical_type).ok();

            if let Some(type_size) = fixed_row_width {
                let batch = iter.read_by_rowids(&row_ids)?;
                let bytes = batch.data;
                let nulls = batch.nulls.as_deref();
                for (i, row_id) in row_ids.iter().enumerate() {
                    let start = i * type_size;
                    let end = start + type_size;
                    let val = bytes.slice(start..end).to_vec();
                    let is_null = nulls.and_then(|b| b.get(i)).copied().unwrap_or(0) != 0;
                    data_cache
                        .entry((segment_key, *row_id))
                        .or_insert_with(|| vec![ProjectedCell::null(); projected_columns.len()])
                        [col_res_idx] = ProjectedCell {
                        bytes: val,
                        is_null,
                    };
                }
            } else {
                for row_id in &row_ids {
                    iter.seek_to_ordinal(*row_id)?;
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
                        .entry((segment_key, *row_id))
                        .or_insert_with(|| vec![ProjectedCell::null(); projected_columns.len()])
                        [col_res_idx] = ProjectedCell { bytes, is_null };
                }
            }
        }
    }

    Ok(data_cache)
}

pub(crate) fn materialize_results(
    tablet: &TabletRef,
    column_types: &[LogicalType],
    final_order: Vec<ScoredRowRef>,
    projected_columns: &[usize],
    emit_score: bool,
) -> Result<Vec<Chunk>> {
    if final_order.is_empty() {
        return Ok(Vec::new());
    }

    let data_cache = fetch_projected_columns(&final_order, projected_columns, column_types)?;
    let mut output_vectors = Vec::with_capacity(projected_columns.len() + usize::from(emit_score));

    for (result_col_idx, &column_idx) in projected_columns.iter().enumerate() {
        let logical_type = column_types
            .get(column_idx)
            .ok_or_else(|| paro_error::internal("Invalid column index"))?;
        output_vectors.push(materialize_column(
            tablet,
            logical_type,
            column_idx,
            result_col_idx,
            &final_order,
            &data_cache,
        )?);
    }

    if emit_score {
        let scores: Vec<f32> = final_order.iter().map(|point| point.score).collect();
        output_vectors.push(Vector::from_f32(&scores));
    }

    Ok(vec![Chunk::from_vectors(output_vectors)])
}
