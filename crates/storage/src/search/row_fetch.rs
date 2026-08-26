// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Late materialization helpers for fetching projected search rows.

use std::collections::HashMap;
use std::time::Instant;

use crate::codec::{cell_decoder::decode_cell_into_vector, physical_layout, vector_decoder};
use crate::metrics::{storage_metrics, SearchRowFetchMetricKey};
use crate::rowset::column::ColumnBatch;
use crate::rowset::encoding::BinaryPlainPageDecoder;
use crate::rowset::RowsetId;
use crate::search::{CandidateBatch, PhysicalRowRef, SearchReadSnapshot};
use crate::tablet::TabletRef;
use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, codes, Result};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowFetchMode {
    Materialize {
        row_limit: usize,
        byte_limit: usize,
    },
    #[allow(dead_code)]
    Streaming {
        batch_rows: usize,
        batch_bytes: usize,
    },
}

impl RowFetchMode {
    #[allow(dead_code)]
    const DEFAULT_STREAMING_ROWS: usize = 1024;
    #[allow(dead_code)]
    const DEFAULT_STREAMING_BYTES: usize = 4 * 1024 * 1024;
    const DEFAULT_MATERIALIZE_BYTES: usize = 16 * 1024 * 1024;

    pub(crate) fn materialize(row_limit: usize) -> Self {
        Self::Materialize {
            row_limit,
            byte_limit: Self::DEFAULT_MATERIALIZE_BYTES,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn streaming() -> Self {
        Self::Streaming {
            batch_rows: Self::DEFAULT_STREAMING_ROWS,
            batch_bytes: Self::DEFAULT_STREAMING_BYTES,
        }
    }

    fn limits(self) -> RowFetchLimits {
        match self {
            Self::Materialize {
                row_limit,
                byte_limit,
            } => RowFetchLimits {
                row_limit,
                byte_limit,
            },
            Self::Streaming {
                batch_rows,
                batch_bytes,
            } => RowFetchLimits {
                row_limit: batch_rows,
                byte_limit: batch_bytes,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RowFetchLimits {
    row_limit: usize,
    byte_limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct RowFetchStats {
    pub(crate) rows: usize,
    pub(crate) projected_columns: usize,
    pub(crate) segment_groups: usize,
    pub(crate) column_batches: usize,
    pub(crate) fixed_width_column_batches: usize,
    pub(crate) varlen_column_batches: usize,
    pub(crate) projected_bytes: usize,
    pub(crate) column_read_by_rowids_page_run_seeks: usize,
    pub(crate) elapsed_micros: u64,
}

impl RowFetchStats {
    fn finish(mut self, projected_bytes: usize, started_at: Instant) -> Self {
        self.projected_bytes = projected_bytes;
        self.elapsed_micros = elapsed_micros_since(started_at);
        self
    }
}

fn record_row_fetch_stats(snapshot: &SearchReadSnapshot, stats: RowFetchStats) {
    let metric_key = SearchRowFetchMetricKey {
        table_id: snapshot.table.table_id,
        provider: snapshot.provider_kind,
    };
    storage_metrics().record_search_row_fetch(
        metric_key,
        stats.rows,
        stats.projected_columns,
        stats.segment_groups,
        stats.column_batches,
        stats.fixed_width_column_batches,
        stats.varlen_column_batches,
        stats.projected_bytes,
        stats.column_read_by_rowids_page_run_seeks,
        stats.elapsed_micros,
    );
}

pub(crate) struct ProjectedBatch {
    pub(crate) data_cache: HashMap<(RowsetId, u32, u64), Vec<ProjectedCell>>,
    pub(crate) stats: RowFetchStats,
}

#[allow(dead_code)]
pub(crate) struct RowFetchStreamBatch<'a> {
    pub(crate) rows: &'a [PhysicalRowRef],
    pub(crate) projected: ProjectedBatch,
}

pub(crate) struct SearchRowFetcher<'a> {
    snapshot: &'a SearchReadSnapshot,
    column_types: &'a [LogicalType],
}

impl<'a> SearchRowFetcher<'a> {
    pub(crate) fn new(snapshot: &'a SearchReadSnapshot, column_types: &'a [LogicalType]) -> Self {
        Self {
            snapshot,
            column_types,
        }
    }

    pub(crate) fn fetch_batch(
        &self,
        rows: &[PhysicalRowRef],
        projected_columns: &[usize],
        mode: RowFetchMode,
    ) -> Result<ProjectedBatch> {
        fetch_projected_batch(
            self.snapshot,
            rows,
            projected_columns,
            self.column_types,
            mode,
        )
    }

    #[allow(dead_code)]
    pub(crate) fn stream<'rows, 'projection>(
        &'a self,
        rows: &'rows [PhysicalRowRef],
        projected_columns: &'projection [usize],
        mode: RowFetchMode,
    ) -> Result<SearchRowFetchStream<'a, 'rows, 'projection>> {
        let RowFetchMode::Streaming {
            batch_rows,
            batch_bytes,
        } = mode
        else {
            return Err(paro_error::invalid_input(
                "search row fetch stream requires RowFetchMode::Streaming",
            ));
        };
        if batch_rows == 0 {
            return Err(paro_error::invalid_input(
                "search row fetch streaming batch_rows must be greater than zero",
            ));
        }
        if batch_bytes == 0 {
            return Err(paro_error::invalid_input(
                "search row fetch streaming batch_bytes must be greater than zero",
            ));
        }
        Ok(SearchRowFetchStream {
            fetcher: self,
            rows,
            projected_columns,
            next_row_offset: 0,
            batch_rows,
            batch_bytes,
        })
    }
}

#[allow(dead_code)]
pub(crate) struct SearchRowFetchStream<'fetcher, 'rows, 'projection> {
    fetcher: &'fetcher SearchRowFetcher<'fetcher>,
    rows: &'rows [PhysicalRowRef],
    projected_columns: &'projection [usize],
    next_row_offset: usize,
    batch_rows: usize,
    batch_bytes: usize,
}

impl<'fetcher, 'rows, 'projection> SearchRowFetchStream<'fetcher, 'rows, 'projection> {
    #[allow(dead_code)]
    pub(crate) fn next_batch(&mut self) -> Result<Option<RowFetchStreamBatch<'rows>>> {
        if self.next_row_offset >= self.rows.len() {
            return Ok(None);
        }

        let remaining = self.rows.len() - self.next_row_offset;
        let mut candidate_rows = self.batch_rows.min(remaining).max(1);
        loop {
            let start = self.next_row_offset;
            let end = start + candidate_rows;
            let rows = &self.rows[start..end];
            match self.fetcher.fetch_batch(
                rows,
                self.projected_columns,
                RowFetchMode::Streaming {
                    batch_rows: candidate_rows,
                    batch_bytes: self.batch_bytes,
                },
            ) {
                Ok(projected) => {
                    self.next_row_offset = end;
                    return Ok(Some(RowFetchStreamBatch { rows, projected }));
                }
                Err(err)
                    if candidate_rows > 1
                        && err.is(codes::resource::CONFIGURATION_LIMIT_EXCEEDED) =>
                {
                    candidate_rows = (candidate_rows / 2).max(1);
                }
                Err(err) => return Err(err),
            }
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
    (row.rowset_id, row.segment_id, row.row_offset.get() as u64)
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

fn fetch_projected_batch(
    snapshot: &SearchReadSnapshot,
    rows: &[PhysicalRowRef],
    projected_columns: &[usize],
    column_types: &[LogicalType],
    mode: RowFetchMode,
) -> Result<ProjectedBatch> {
    let started_at = Instant::now();
    let limits = mode.limits();
    validate_row_fetch_limits(rows.len(), limits)?;
    let mut stats = RowFetchStats {
        rows: rows.len(),
        projected_columns: projected_columns.len(),
        ..Default::default()
    };

    let mut fetch_map = HashMap::new();
    for row in rows {
        let segment = snapshot.table_lease.resolve_segment(*row)?;
        let entry = fetch_map
            .entry(row.segment_key())
            .or_insert_with(|| (segment, Vec::new()));
        entry.1.push(row.row_offset.get() as u64);
    }
    stats.segment_groups = fetch_map.len();

    let mut data_cache: HashMap<(RowsetId, u32, u64), Vec<ProjectedCell>> = HashMap::new();
    let mut projected_bytes = 0usize;

    for ((rowset_id, segment_id), (segment, row_ids)) in fetch_map {
        for (result_col_idx, &projected_column_id) in projected_columns.iter().enumerate() {
            let mut iter = segment.new_column_iterator(projected_column_id as u32)?;
            let logical_type = column_types
                .get(projected_column_id)
                .ok_or_else(|| paro_error::internal("Invalid projected column index"))?;
            let fixed_row_width = physical_layout::fixed_row_width(logical_type).ok();
            stats.column_batches += 1;

            if let Some(type_size) = fixed_row_width {
                stats.fixed_width_column_batches += 1;
                let batch = iter.read_by_rowids(&row_ids)?;
                stats.column_read_by_rowids_page_run_seeks += batch.page_run_seeks;
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
                    projected_bytes =
                        add_projected_bytes(projected_bytes, type_size, limits.byte_limit)?;
                }
            } else {
                let batch = iter.read_by_rowids(&row_ids)?;
                stats.column_read_by_rowids_page_run_seeks += batch.page_run_seeks;
                projected_bytes = project_varlen_batch(
                    &mut data_cache,
                    rowset_id,
                    segment_id,
                    &row_ids,
                    result_col_idx,
                    projected_columns.len(),
                    &batch,
                    projected_bytes,
                    limits.byte_limit,
                )?;
                stats.varlen_column_batches += 1;
            }
        }
    }

    let stats = stats.finish(projected_bytes, started_at);
    record_row_fetch_stats(snapshot, stats);

    Ok(ProjectedBatch { data_cache, stats })
}

fn project_varlen_batch(
    data_cache: &mut HashMap<(RowsetId, u32, u64), Vec<ProjectedCell>>,
    rowset_id: RowsetId,
    segment_id: u32,
    row_ids: &[u64],
    result_col_idx: usize,
    projected_column_count: usize,
    batch: &ColumnBatch,
    mut projected_bytes: usize,
    byte_limit: usize,
) -> Result<usize> {
    if let Some(storage_dictionary) = &batch.storage_dictionary {
        let mut decoder = BinaryPlainPageDecoder::new(storage_dictionary.dictionary.clone());
        decoder.init()?;
        for (row_offset, row_id) in row_ids.iter().enumerate() {
            let is_null = is_null_at(batch.nulls.as_deref(), row_offset);
            let bytes = if is_null {
                Vec::new()
            } else {
                let code_offset = row_offset
                    .checked_mul(std::mem::size_of::<u32>())
                    .ok_or_else(|| {
                        paro_error::data_corrupted("storage dictionary row offset overflow")
                    })?;
                let code_end = code_offset
                    .checked_add(std::mem::size_of::<u32>())
                    .ok_or_else(|| {
                        paro_error::data_corrupted("storage dictionary code offset overflow")
                    })?;
                if code_end > storage_dictionary.codes.len() {
                    return Err(paro_error::out_of_range(format!(
                        "storage dictionary row {} out of range",
                        row_offset
                    )));
                }
                let code = u32::from_le_bytes(
                    storage_dictionary.codes[code_offset..code_end]
                        .try_into()
                        .expect("u32 code slice"),
                );
                let cell = decoder.string_at(code).ok_or_else(|| {
                    paro_error::data_corrupted(format!(
                        "storage dictionary code {} out of range",
                        code
                    ))
                })?;
                encode_varlen_cell(cell.as_ref())
            };
            set_projected_cell(
                data_cache,
                rowset_id,
                segment_id,
                *row_id,
                result_col_idx,
                projected_column_count,
                ProjectedCell { bytes, is_null },
            );
            projected_bytes = add_projected_bytes(
                projected_bytes,
                data_cache[&(rowset_id, segment_id, *row_id)][result_col_idx]
                    .bytes
                    .len(),
                byte_limit,
            )?;
        }
        return Ok(projected_bytes);
    }

    let mut offset = 0usize;
    for (row_offset, row_id) in row_ids.iter().enumerate() {
        let len_end = offset
            .checked_add(std::mem::size_of::<u32>())
            .ok_or_else(|| paro_error::data_corrupted("varlen row length offset overflow"))?;
        if len_end > batch.data.len() {
            return Err(paro_error::data_corrupted(
                "varlen projected batch length prefix truncated",
            ));
        }
        let len = u32::from_le_bytes(
            batch.data[offset..len_end]
                .try_into()
                .expect("u32 length prefix"),
        ) as usize;
        let value_end = len_end
            .checked_add(len)
            .ok_or_else(|| paro_error::data_corrupted("varlen projected batch value overflow"))?;
        if value_end > batch.data.len() {
            return Err(paro_error::data_corrupted(
                "varlen projected batch row extends past payload",
            ));
        }

        let is_null = is_null_at(batch.nulls.as_deref(), row_offset);
        let bytes = if is_null {
            Vec::new()
        } else {
            batch.data.slice(offset..value_end).to_vec()
        };
        set_projected_cell(
            data_cache,
            rowset_id,
            segment_id,
            *row_id,
            result_col_idx,
            projected_column_count,
            ProjectedCell { bytes, is_null },
        );
        projected_bytes = add_projected_bytes(
            projected_bytes,
            data_cache[&(rowset_id, segment_id, *row_id)][result_col_idx]
                .bytes
                .len(),
            byte_limit,
        )?;
        offset = value_end;
    }
    if offset != batch.data.len() {
        return Err(paro_error::data_corrupted(
            "varlen projected batch has trailing bytes",
        ));
    }
    Ok(projected_bytes)
}

fn set_projected_cell(
    data_cache: &mut HashMap<(RowsetId, u32, u64), Vec<ProjectedCell>>,
    rowset_id: RowsetId,
    segment_id: u32,
    row_id: u64,
    result_col_idx: usize,
    projected_column_count: usize,
    cell: ProjectedCell,
) {
    data_cache
        .entry((rowset_id, segment_id, row_id))
        .or_insert_with(|| vec![ProjectedCell::null(); projected_column_count])[result_col_idx] =
        cell;
}

fn is_null_at(nulls: Option<&[u8]>, row_offset: usize) -> bool {
    nulls
        .and_then(|nulls| nulls.get(row_offset))
        .copied()
        .unwrap_or(0)
        != 0
}

fn encode_varlen_cell(cell: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(4 + cell.len());
    encoded.extend_from_slice(&(cell.len() as u32).to_le_bytes());
    encoded.extend_from_slice(cell);
    encoded
}

fn validate_row_fetch_limits(row_count: usize, limits: RowFetchLimits) -> Result<()> {
    if row_count > limits.row_limit {
        return Err(paro_error::configuration_limit_exceeded(format!(
            "search row fetch row limit exceeded: {} > {}",
            row_count, limits.row_limit
        )));
    }
    Ok(())
}

fn add_projected_bytes(current: usize, added: usize, byte_limit: usize) -> Result<usize> {
    let next = current
        .checked_add(added)
        .ok_or_else(|| paro_error::out_of_memory("search row fetch projected bytes overflow"))?;
    if next > byte_limit {
        return Err(paro_error::configuration_limit_exceeded(format!(
            "search row fetch byte limit exceeded: {} > {}",
            next, byte_limit
        )));
    }
    Ok(next)
}

fn account_column_batch_bytes(
    logical_type: &LogicalType,
    batch: &ColumnBatch,
    rows: usize,
    mut projected_bytes: usize,
    byte_limit: usize,
) -> Result<usize> {
    if let Ok(type_size) = physical_layout::fixed_row_width(logical_type) {
        let added = type_size.checked_mul(rows).ok_or_else(|| {
            paro_error::out_of_memory("search row fetch projected bytes overflow")
        })?;
        return add_projected_bytes(projected_bytes, added, byte_limit);
    }

    if let Some(storage_dictionary) = &batch.storage_dictionary {
        let expected_code_bytes = rows
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or_else(|| paro_error::data_corrupted("storage dictionary code count overflow"))?;
        if storage_dictionary.codes.len() != expected_code_bytes {
            return Err(paro_error::data_corrupted(
                "storage dictionary code count does not match row count",
            ));
        }
        let mut decoder = BinaryPlainPageDecoder::new(storage_dictionary.dictionary.clone());
        decoder.init()?;
        for row in 0..rows {
            if is_null_at(batch.nulls.as_deref(), row) {
                continue;
            }
            let code_offset = row * std::mem::size_of::<u32>();
            let code = u32::from_le_bytes(
                storage_dictionary.codes[code_offset..code_offset + std::mem::size_of::<u32>()]
                    .try_into()
                    .expect("validated u32 code slice"),
            );
            let cell = decoder.string_at(code).ok_or_else(|| {
                paro_error::data_corrupted(format!("storage dictionary code {} out of range", code))
            })?;
            projected_bytes = add_projected_bytes(
                projected_bytes,
                std::mem::size_of::<u32>() + cell.len(),
                byte_limit,
            )?;
        }
        return Ok(projected_bytes);
    }

    let mut offset = 0usize;
    for row in 0..rows {
        let len_end = offset
            .checked_add(std::mem::size_of::<u32>())
            .ok_or_else(|| paro_error::data_corrupted("varlen row length offset overflow"))?;
        if len_end > batch.data.len() {
            return Err(paro_error::data_corrupted(
                "varlen projected batch length prefix truncated",
            ));
        }
        let len = u32::from_le_bytes(
            batch.data[offset..len_end]
                .try_into()
                .expect("u32 length prefix"),
        ) as usize;
        let value_end = len_end
            .checked_add(len)
            .ok_or_else(|| paro_error::data_corrupted("varlen projected batch value overflow"))?;
        if value_end > batch.data.len() {
            return Err(paro_error::data_corrupted(
                "varlen projected batch row extends past payload",
            ));
        }
        if !is_null_at(batch.nulls.as_deref(), row) {
            projected_bytes = add_projected_bytes(projected_bytes, value_end - offset, byte_limit)?;
        }
        offset = value_end;
    }
    if offset != batch.data.len() {
        return Err(paro_error::data_corrupted(
            "varlen projected batch has trailing bytes",
        ));
    }
    Ok(projected_bytes)
}

fn elapsed_micros_since(started_at: Instant) -> u64 {
    let micros = started_at.elapsed().as_micros();
    micros.min(u128::from(u64::MAX)) as u64
}

/// Materialize the common Top-K shape without first expanding each cell into
/// a row-keyed hash map. This is a row-fetch strategy, not a second execution
/// path: it shares the same limits, metrics, output allocator, and pipeline
/// lifecycle as the general multi-segment materializer.
fn try_materialize_single_segment_columns(
    column_types: &[LogicalType],
    snapshot: &SearchReadSnapshot,
    rows: &[PhysicalRowRef],
    projected_columns: &[usize],
    mode: RowFetchMode,
    allocator: Arc<dyn Allocator>,
) -> Result<Option<(Vec<Vector>, RowFetchStats)>> {
    let Some(first_row) = rows.first().copied() else {
        return Ok(None);
    };
    if rows
        .iter()
        .any(|row| row.segment_key() != first_row.segment_key())
    {
        return Ok(None);
    }

    let started_at = Instant::now();
    let limits = mode.limits();
    validate_row_fetch_limits(rows.len(), limits)?;
    let column_ids = projected_columns
        .iter()
        .map(|&column_idx| {
            column_types
                .get(column_idx)
                .ok_or_else(|| paro_error::internal("Invalid projected column index"))?;
            u32::try_from(column_idx)
                .map_err(|_| paro_error::out_of_range("projected column index exceeds u32"))
        })
        .collect::<Result<Vec<_>>>()?;
    let row_offsets = rows
        .iter()
        .map(|row| row.row_offset.get())
        .collect::<Vec<_>>();
    let segment = snapshot.table_lease.resolve_segment(first_row)?;
    let encoded_columns = segment.read_by_rowids(&column_ids, &row_offsets)?;
    if encoded_columns.len() != projected_columns.len() {
        return Err(paro_error::internal(
            "single-segment row fetch returned an unexpected column count",
        ));
    }

    let mut stats = RowFetchStats {
        rows: rows.len(),
        projected_columns: projected_columns.len(),
        segment_groups: 1,
        ..Default::default()
    };
    let mut projected_bytes = 0usize;
    let mut vectors = Vec::with_capacity(projected_columns.len());
    for ((&column_idx, &expected_column_id), (actual_column_id, batch)) in projected_columns
        .iter()
        .zip(&column_ids)
        .zip(encoded_columns)
    {
        if actual_column_id != expected_column_id {
            return Err(paro_error::internal(
                "single-segment row fetch returned columns out of order",
            ));
        }
        let logical_type = &column_types[column_idx];
        projected_bytes = account_column_batch_bytes(
            logical_type,
            &batch,
            rows.len(),
            projected_bytes,
            limits.byte_limit,
        )?;
        stats.column_batches += 1;
        stats.column_read_by_rowids_page_run_seeks += batch.page_run_seeks;
        if physical_layout::fixed_row_width(logical_type).is_ok() {
            stats.fixed_width_column_batches += 1;
        } else {
            stats.varlen_column_batches += 1;
        }
        vectors.push(vector_decoder::decode_column_batch(
            logical_type,
            &batch,
            rows.len(),
            allocator.clone(),
            None,
        )?);
    }

    let stats = stats.finish(projected_bytes, started_at);
    record_row_fetch_stats(snapshot, stats);
    Ok(Some((vectors, stats)))
}

pub(crate) fn materialize_candidate_batch(
    tablet: &TabletRef,
    column_types: &[LogicalType],
    snapshot: &SearchReadSnapshot,
    batch: CandidateBatch,
    projected_columns: &[usize],
    emit_score: bool,
    allocator: Arc<dyn Allocator>,
) -> Result<Chunk> {
    let row_count = batch.rows.len();
    let mode = RowFetchMode::materialize(row_count);
    let mut output_vectors = if let Some((vectors, stats)) = try_materialize_single_segment_columns(
        column_types,
        snapshot,
        &batch.rows,
        projected_columns,
        mode,
        allocator.clone(),
    )? {
        debug_assert!(stats.projected_bytes <= RowFetchMode::DEFAULT_MATERIALIZE_BYTES);
        vectors
    } else {
        let projected_batch = SearchRowFetcher::new(snapshot, column_types).fetch_batch(
            &batch.rows,
            projected_columns,
            mode,
        )?;
        let ProjectedBatch { data_cache, stats } = projected_batch;
        debug_assert!(stats.projected_bytes <= RowFetchMode::DEFAULT_MATERIALIZE_BYTES);
        let mut vectors = Vec::with_capacity(projected_columns.len() + usize::from(emit_score));
        for (result_col_idx, &column_idx) in projected_columns.iter().enumerate() {
            let logical_type = column_types
                .get(column_idx)
                .ok_or_else(|| paro_error::internal("Invalid column index"))?;
            vectors.push(materialize_column(
                tablet,
                logical_type,
                column_idx,
                result_col_idx,
                &batch.rows,
                &data_cache,
                allocator.clone(),
            )?);
        }
        vectors
    };

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::hnsw::{DistanceMetric, SearchParams};
    use crate::rowset::encoding::BinaryPlainPageBuilder;
    use crate::search::{
        HnswInlineConfig, HnswProviderConfig, ResourceBudget, SearchBatchConfig, SearchBatchState,
        SearchFreshnessPolicy, SearchIndexDefinition, SearchIndexKind,
        HNSW_PROVIDER_CONFIG_VERSION,
    };
    use crate::table::table_factory::TableFactory;
    use crate::test_utils::{
        test_chunk_from_vectors, test_embedding_vector, test_i64_vector, test_string_vector,
    };
    use bytes::Bytes;
    use paro_common::allocator::default_allocator;

    fn register_explicit_hnsw(
        table: &crate::table::table_handle::TableHandle,
        column_id: u32,
        dimension: u32,
    ) {
        let config = HnswProviderConfig {
            version: HNSW_PROVIDER_CONFIG_VERSION,
            dimension,
            distance: DistanceMetric::Euclidean,
            build_vector_encoding: crate::index::hnsw::HnswBuildVectorEncoding::SymmetricI16,
            build_routing_dimensions: dimension
                .min(crate::index::hnsw::DEFAULT_HNSW_BUILD_ROUTING_DIMENSIONS),
            m: 8,
            ef_construct: 64,
            ef_search: 64,
            distance_cost: crate::index::hnsw::HnswDistanceCostProfile::default(),
            build_seed: crate::index::hnsw::DEFAULT_HNSW_BUILD_SEED,
            proposal_wave_size: crate::search::DEFAULT_HNSW_PROPOSAL_WAVE_SIZE,
            warmup_point_count: crate::search::DEFAULT_HNSW_WARMUP_POINT_COUNT,
            filter_columns: Vec::new(),
            filter_block_rows: crate::search::DEFAULT_HNSW_FILTER_BLOCK_ROWS,
            filter_m: crate::search::DEFAULT_HNSW_FILTER_M,
            inline_threshold: HnswInlineConfig {
                enabled: true,
                max_vector_count: 4_096,
                max_graph_memory_bytes: 64 * 1024 * 1024,
                max_dimension: dimension,
            },
        }
        .validated()
        .expect("valid HNSW fixture");
        let provider_config = config.to_value().expect("encode HNSW fixture");
        let definition = SearchIndexDefinition {
            definition_id: 1,
            table_id: table.tablet_id(),
            name: "row_fetch_hnsw".to_string(),
            kind: SearchIndexKind::Hnsw,
            column_ids: vec![column_id],
            expression: None,
            freshness_policy: SearchFreshnessPolicy::Required,
            config_fingerprint: SearchIndexDefinition::try_compute_config_fingerprint(
                SearchIndexKind::Hnsw,
                &[column_id],
                None,
                &provider_config,
            )
            .expect("fingerprint HNSW fixture"),
            provider_config,
        };
        table
            .register_search_definition(definition)
            .expect("register explicit HNSW fixture");
    }

    #[test]
    fn project_varlen_batch_decodes_length_prefixed_rows_once() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&encode_varlen_cell(b"alpha"));
        payload.extend_from_slice(&encode_varlen_cell(b"beta"));
        payload.extend_from_slice(&encode_varlen_cell(b"ignored-null-payload"));
        let batch = ColumnBatch::new(Bytes::from(payload), Some(Bytes::from(vec![0, 0, 1])));
        let mut data_cache = HashMap::new();

        let projected_bytes =
            project_varlen_batch(&mut data_cache, 7, 3, &[11, 12, 13], 0, 1, &batch, 0, 1024)
                .expect("project varlen batch");

        assert_eq!(
            data_cache[&(7, 3, 11)][0].bytes,
            encode_varlen_cell(b"alpha")
        );
        assert_eq!(
            data_cache[&(7, 3, 12)][0].bytes,
            encode_varlen_cell(b"beta")
        );
        assert!(data_cache[&(7, 3, 13)][0].is_null);
        assert!(data_cache[&(7, 3, 13)][0].bytes.is_empty());
        assert_eq!(
            projected_bytes,
            encode_varlen_cell(b"alpha").len() + encode_varlen_cell(b"beta").len()
        );
    }

    #[test]
    fn row_fetch_limits_reject_oversized_batches() {
        validate_row_fetch_limits(
            3,
            RowFetchLimits {
                row_limit: 2,
                byte_limit: 1024,
            },
        )
        .expect_err("row limit should reject");

        add_projected_bytes(8, 4, 10).expect_err("byte limit should reject");
    }

    #[test]
    fn row_fetch_mode_defaults_match_design_contract() {
        assert_eq!(
            RowFetchMode::materialize(99).limits(),
            RowFetchLimits {
                row_limit: 99,
                byte_limit: 16 * 1024 * 1024,
            }
        );
        assert_eq!(
            RowFetchMode::streaming().limits(),
            RowFetchLimits {
                row_limit: 1024,
                byte_limit: 4 * 1024 * 1024,
            }
        );
    }

    #[test]
    fn project_varlen_batch_enforces_byte_limit() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&encode_varlen_cell(b"alpha"));
        let batch = ColumnBatch::new(Bytes::from(payload), None);
        let mut data_cache = HashMap::new();

        project_varlen_batch(&mut data_cache, 7, 3, &[11], 0, 1, &batch, 0, 4)
            .expect_err("varlen byte limit should reject");
    }

    #[test]
    fn project_varlen_batch_decodes_storage_dictionary_rows() {
        let batch = storage_dictionary_batch(
            &["apple", "banana", "cherry"],
            &[2, 0, 1, 2],
            Some(vec![0, 0, 1, 0]),
        );
        let mut data_cache = HashMap::new();

        let projected_bytes = project_varlen_batch(
            &mut data_cache,
            7,
            3,
            &[11, 12, 13, 14],
            0,
            1,
            &batch,
            0,
            1024,
        )
        .expect("project dictionary varlen batch");

        assert_eq!(
            data_cache[&(7, 3, 11)][0].bytes,
            encode_varlen_cell(b"cherry")
        );
        assert_eq!(
            data_cache[&(7, 3, 12)][0].bytes,
            encode_varlen_cell(b"apple")
        );
        assert!(data_cache[&(7, 3, 13)][0].is_null);
        assert!(data_cache[&(7, 3, 13)][0].bytes.is_empty());
        assert_eq!(
            data_cache[&(7, 3, 14)][0].bytes,
            encode_varlen_cell(b"cherry")
        );
        assert_eq!(
            projected_bytes,
            encode_varlen_cell(b"cherry").len()
                + encode_varlen_cell(b"apple").len()
                + encode_varlen_cell(b"cherry").len()
        );
    }

    #[test]
    fn search_row_fetch_stream_emits_resume_batches() {
        let table = TableFactory::default()
            .create_table(&[
                LogicalType::Array(Box::new(LogicalType::Float), 2),
                LogicalType::Varchar,
            ])
            .expect("create table");
        register_explicit_hnsw(&table, 0, 2);
        table
            .append(&test_chunk_from_vectors(vec![
                test_embedding_vector(
                    &[vec![10.0_f32, 0.0], vec![9.0_f32, 0.0], vec![8.0_f32, 0.0]],
                    2,
                ),
                test_string_vector(&["alpha", "beta", "gamma"]),
            ]))
            .expect("append");

        let opened = table
            .open_vector_search_cursor(
                0,
                &[10.0, 0.0],
                DistanceMetric::Euclidean,
                3,
                SearchParams {
                    ef: Some(64),
                    ..Default::default()
                },
                None,
                table.max_version(),
                &crate::search::SearchReadOptions::ungoverned(),
            )
            .expect("open vector cursor");
        let mut cursor = opened.cursor;
        let snapshot = opened.snapshot;
        let mut budget = ResourceBudget::standalone(64 * 1024 * 1024, 1024, 1);
        let candidates = loop {
            match cursor
                .next_batch(
                    &SearchBatchConfig {
                        row_limit: 3,
                        preferred_bytes: 1 << 20,
                    },
                    &mut budget,
                )
                .expect("next search batch")
            {
                SearchBatchState::Ready(batch) if batch.is_empty() => continue,
                SearchBatchState::Ready(batch) => break batch,
                SearchBatchState::Exhausted => panic!("expected search candidates"),
            }
        };
        assert_eq!(candidates.rows.len(), 3);

        let fetcher = SearchRowFetcher::new(&snapshot, table.types());
        let mut stream = fetcher
            .stream(
                &candidates.rows,
                &[1],
                RowFetchMode::Streaming {
                    batch_rows: 2,
                    batch_bytes: 1024,
                },
            )
            .expect("open row fetch stream");
        let first = stream
            .next_batch()
            .expect("fetch first stream batch")
            .expect("first stream batch");
        assert_eq!(first.rows.len(), 2);
        assert_eq!(first.projected.stats.rows, 2);
        assert_eq!(first.projected.stats.projected_columns, 1);
        assert!(first.projected.stats.projected_bytes > 0);
        assert_eq!(
            first.projected.stats.column_batches,
            first.projected.stats.varlen_column_batches
        );
        for row in first.rows {
            assert!(!first.projected.data_cache[&row_cache_key(*row)][0].is_null);
        }

        let second = stream
            .next_batch()
            .expect("fetch second stream batch")
            .expect("second stream batch");
        assert_eq!(second.rows.len(), 1);
        assert_eq!(second.projected.stats.rows, 1);
        assert_eq!(second.projected.stats.projected_columns, 1);
        assert!(second.projected.stats.projected_bytes > 0);
        assert!(stream
            .next_batch()
            .expect("fetch exhausted stream batch")
            .is_none());
    }

    #[test]
    fn search_row_fetch_preserves_unsorted_duplicates_and_projection_order() {
        let table = TableFactory::default()
            .create_table(&[
                LogicalType::Array(Box::new(LogicalType::Float), 2),
                LogicalType::Varchar,
                LogicalType::BigInt,
            ])
            .expect("create table");
        register_explicit_hnsw(&table, 0, 2);
        table
            .append(&test_chunk_from_vectors(vec![
                test_embedding_vector(
                    &[vec![10.0_f32, 0.0], vec![9.0_f32, 0.0], vec![8.0_f32, 0.0]],
                    2,
                ),
                test_string_vector(&["alpha", "beta", "gamma"]),
                test_i64_vector(&[10, 20, 30]),
            ]))
            .expect("append");

        let opened = table
            .open_vector_search_cursor(
                0,
                &[10.0, 0.0],
                DistanceMetric::Euclidean,
                3,
                SearchParams {
                    ef: Some(64),
                    ..Default::default()
                },
                None,
                table.max_version(),
                &crate::search::SearchReadOptions::ungoverned(),
            )
            .expect("open vector cursor");
        let snapshot = opened.snapshot;
        let segment = snapshot
            .table_lease
            .visible_segments()
            .first()
            .expect("visible segment");
        let rows = [
            PhysicalRowRef::new(
                segment.rowset_id,
                segment.segment_id,
                crate::rowset::SegmentRowId::from_raw(2),
            ),
            PhysicalRowRef::new(
                segment.rowset_id,
                segment.segment_id,
                crate::rowset::SegmentRowId::from_raw(0),
            ),
            PhysicalRowRef::new(
                segment.rowset_id,
                segment.segment_id,
                crate::rowset::SegmentRowId::from_raw(2),
            ),
            PhysicalRowRef::new(
                segment.rowset_id,
                segment.segment_id,
                crate::rowset::SegmentRowId::from_raw(1),
            ),
        ];

        let projected = SearchRowFetcher::new(&snapshot, table.types())
            .fetch_batch(&rows, &[2, 1], RowFetchMode::materialize(rows.len()))
            .expect("fetch projected rows");

        assert_eq!(projected.stats.rows, 4);
        assert_eq!(projected.stats.projected_columns, 2);
        assert_eq!(projected.stats.segment_groups, 1);
        assert_eq!(projected.stats.column_batches, 2);
        assert_eq!(projected.stats.fixed_width_column_batches, 1);
        assert_eq!(projected.stats.varlen_column_batches, 1);
        assert_eq!(
            projected.stats.projected_bytes,
            4 * std::mem::size_of::<i64>()
                + encode_varlen_cell(b"gamma").len()
                + encode_varlen_cell(b"alpha").len()
                + encode_varlen_cell(b"gamma").len()
                + encode_varlen_cell(b"beta").len()
        );

        let allocator: Arc<dyn Allocator> = Arc::new(default_allocator());
        let (direct_vectors, direct_stats) = try_materialize_single_segment_columns(
            table.types(),
            &snapshot,
            &rows,
            &[2, 1],
            RowFetchMode::materialize(rows.len()),
            allocator.clone(),
        )
        .expect("materialize single-segment columns")
        .expect("single-segment strategy should apply");
        assert_eq!(direct_stats.rows, 4);
        assert_eq!(direct_stats.segment_groups, 1);
        assert_eq!(direct_stats.column_batches, 2);
        assert_eq!(direct_stats.fixed_width_column_batches, 1);
        assert_eq!(direct_stats.varlen_column_batches, 1);
        assert_eq!(
            direct_stats.projected_bytes,
            projected.stats.projected_bytes
        );
        assert_eq!(direct_vectors[0].get_i64(0), Some(30));
        assert_eq!(direct_vectors[0].get_i64(1), Some(10));
        assert_eq!(direct_vectors[0].get_i64(2), Some(30));
        assert_eq!(direct_vectors[0].get_i64(3), Some(20));
        assert_eq!(direct_vectors[1].get_string(0), Some("gamma"));
        assert_eq!(direct_vectors[1].get_string(1), Some("alpha"));
        assert_eq!(direct_vectors[1].get_string(2), Some("gamma"));
        assert_eq!(direct_vectors[1].get_string(3), Some("beta"));

        let numbers = materialize_column(
            &table.tablet(),
            &LogicalType::BigInt,
            2,
            0,
            &rows,
            &projected.data_cache,
            allocator.clone(),
        )
        .expect("materialize numeric projection");
        let labels = materialize_column(
            &table.tablet(),
            &LogicalType::Varchar,
            1,
            1,
            &rows,
            &projected.data_cache,
            allocator,
        )
        .expect("materialize string projection");

        assert_eq!(numbers.get_i64(0), Some(30));
        assert_eq!(numbers.get_i64(1), Some(10));
        assert_eq!(numbers.get_i64(2), Some(30));
        assert_eq!(numbers.get_i64(3), Some(20));
        assert_eq!(labels.get_string(0), Some("gamma"));
        assert_eq!(labels.get_string(1), Some("alpha"));
        assert_eq!(labels.get_string(2), Some("gamma"));
        assert_eq!(labels.get_string(3), Some("beta"));
    }

    fn storage_dictionary_batch(
        values: &[&str],
        codes: &[u32],
        nulls: Option<Vec<u8>>,
    ) -> ColumnBatch {
        let mut dictionary_payload = Vec::new();
        for value in values {
            dictionary_payload.extend_from_slice(&encode_varlen_cell(value.as_bytes()));
        }
        let mut dictionary_builder = BinaryPlainPageBuilder::new(1024);
        assert_eq!(
            dictionary_builder.add_length_prefixed(&dictionary_payload),
            values.len() as u32
        );
        let dictionary = dictionary_builder
            .finish()
            .expect("finish storage dictionary page");
        let code_payload: Vec<u8> = codes.iter().flat_map(|code| code.to_le_bytes()).collect();
        ColumnBatch::with_storage_dictionary(
            dictionary,
            Bytes::from(code_payload),
            nulls.map(Bytes::from),
        )
    }
}
