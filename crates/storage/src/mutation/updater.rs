// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::mutation::{deleter, upsert, writer};
use crate::primary_key::RowID;
use crate::table::table_handle::TableHandle;
use crate::tablet::{KeysType, TabletReaderParams};
use crate::transaction::txn::Transaction;
use paro_common::allocator::default_allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;

pub(crate) fn collect_rows_by_row_ids(table: &TableHandle, row_ids: &[u64]) -> Result<Chunk> {
    let allocator = Arc::new(default_allocator());
    if row_ids.is_empty() {
        return Chunk::try_initialize(table.types(), 0, allocator);
    }

    let mut row_positions = HashMap::with_capacity(row_ids.len());
    for (idx, row_id) in row_ids.iter().copied().enumerate() {
        if row_positions.insert(row_id, idx).is_some() {
            return Err(paro_error::invalid_input(format!(
                "duplicate row_id {} in UPDATE input",
                row_id
            )));
        }
    }

    let mut rows = Chunk::try_initialize(table.types(), row_ids.len(), allocator)?;
    rows.try_set_cardinality(row_ids.len())?;

    let mut found = vec![false; row_ids.len()];
    let mut found_count = 0usize;

    let mut reader = table.create_reader(
        TabletReaderParams::with_version(table.max_version()).with_emit_row_id(true),
    )?;
    reader.prepare()?;

    while let Some(chunk) = reader.get_next_chunk()? {
        if chunk.column_count() < table.types().len() + 1 {
            return Err(paro_error::internal(format!(
                "tablet reader chunk has {} columns, expected at least {}",
                chunk.column_count(),
                table.types().len() + 1
            )));
        }

        let row_id_col = chunk
            .column(chunk.column_count() - 1)
            .ok_or_else(|| paro_error::internal("missing row_id column in UPDATE scan"))?;

        for source_row_idx in 0..chunk.size() {
            let raw_row_id = match row_id_col.get_value(source_row_idx) {
                Value::BigInt(v) if v >= 0 => v as u64,
                Value::UBigInt(v) => v,
                Value::Integer(v) if v >= 0 => v as u64,
                Value::BigInt(v) => {
                    return Err(paro_error::internal(format!(
                        "negative row_id {} in UPDATE scan",
                        v
                    )));
                }
                Value::Integer(v) => {
                    return Err(paro_error::internal(format!(
                        "negative row_id {} in UPDATE scan",
                        v
                    )));
                }
                value => {
                    return Err(paro_error::internal(format!(
                        "invalid row_id value {:?} in UPDATE scan",
                        value
                    )));
                }
            };

            let Some(&target_row_idx) = row_positions.get(&raw_row_id) else {
                continue;
            };
            if found[target_row_idx] {
                continue;
            }

            for col_idx in 0..table.types().len() {
                let source_col = chunk.column(col_idx).ok_or_else(|| {
                    paro_error::internal(format!("missing source column {}", col_idx))
                })?;
                let target_col = rows.column_mut(col_idx).ok_or_else(|| {
                    paro_error::internal(format!("missing target column {}", col_idx))
                })?;
                target_col.try_copy_at(target_row_idx, source_col, source_row_idx)?;
            }
            found[target_row_idx] = true;
            found_count += 1;
        }

        if found_count == row_ids.len() {
            return Ok(rows);
        }
    }

    if let Some(missing_idx) = found.iter().position(|is_found| !*is_found) {
        let raw = row_ids[missing_idx];
        let row_id = RowID::from_raw(raw);
        let detail = match table.tablet().decode_row_id(row_id) {
            Ok(location) => format!(
                "rowset={}, segment={}, row={}",
                location.rowset_id, location.segment_id, location.row_offset
            ),
            Err(_) => format!("raw={}", raw),
        };
        return Err(paro_error::invalid_input(format!(
            "UPDATE target row not found for row_id {} ({})",
            raw, detail
        )));
    }

    Ok(rows)
}

pub(crate) fn build_updated_rows_chunk(
    table: &TableHandle,
    base_rows: &Chunk,
    column_ids: &[usize],
    values: &[Vec<Value>],
) -> Result<Chunk> {
    if column_ids.len() != values.len() {
        return Err(paro_error::invalid_input(format!(
            "UPDATE column/value count mismatch: {} columns vs {} value vectors",
            column_ids.len(),
            values.len()
        )));
    }

    let mut seen_columns = HashSet::with_capacity(column_ids.len());
    let mut updated_rows = base_rows.clone();
    let row_count = updated_rows.size();

    for (idx, &column_id) in column_ids.iter().enumerate() {
        if column_id >= table.types().len() {
            return Err(paro_error::invalid_input(format!(
                "UPDATE target column {} out of range (columns={})",
                column_id,
                table.types().len()
            )));
        }
        if !seen_columns.insert(column_id) {
            return Err(paro_error::invalid_input(format!(
                "UPDATE target column {} specified multiple times",
                column_id
            )));
        }

        let column_values = values.get(idx).ok_or_else(|| {
            paro_error::invalid_input(format!("missing UPDATE values for column {}", column_id))
        })?;
        if column_values.len() != row_count {
            return Err(paro_error::invalid_input(format!(
                "UPDATE values for column {} have {} rows, expected {}",
                column_id,
                column_values.len(),
                row_count
            )));
        }

        let target_col = updated_rows.column_mut(column_id).ok_or_else(|| {
            paro_error::internal(format!("missing column {} in updated chunk", column_id))
        })?;
        for (row_idx, value) in column_values.iter().enumerate() {
            target_col.set_value(row_idx, value);
        }
    }

    Ok(updated_rows)
}

pub(crate) fn build_partial_updated_rows_chunk(
    table: &TableHandle,
    base_rows: &Chunk,
    column_ids: &[usize],
    values: &[Vec<Value>],
) -> Result<Chunk> {
    let row_count = base_rows.size();
    let mut partial_rows =
        Chunk::try_initialize(table.types(), row_count, base_rows.allocator().clone())?;
    partial_rows.try_set_cardinality(row_count)?;

    let schema = table
        .tablet()
        .schema()
        .ok_or_else(|| paro_error::internal("tablet schema missing"))?;
    for key_idx in 0..schema.num_key_columns() {
        let src = base_rows.column(key_idx).ok_or_else(|| {
            paro_error::internal(format!("missing key column {} in base rows", key_idx))
        })?;
        let dst = partial_rows.column_mut(key_idx).ok_or_else(|| {
            paro_error::internal(format!("missing key column {} in partial rows", key_idx))
        })?;
        for row_idx in 0..row_count {
            dst.try_copy_at(row_idx, src, row_idx)?;
        }
    }

    for (idx, &column_id) in column_ids.iter().enumerate() {
        let column_values = values.get(idx).ok_or_else(|| {
            paro_error::invalid_input(format!(
                "missing partial UPDATE values for column {}",
                column_id
            ))
        })?;
        let target_col = partial_rows.column_mut(column_id).ok_or_else(|| {
            paro_error::internal(format!("missing column {} in partial chunk", column_id))
        })?;
        for (row_idx, value) in column_values.iter().enumerate() {
            target_col.set_value(row_idx, value);
        }
    }

    Ok(partial_rows)
}

pub(crate) fn update(
    table: &TableHandle,
    row_ids: &[u64],
    column_ids: &[usize],
    values: &[Vec<Value>],
    txn: Option<Arc<Transaction>>,
) -> Result<usize> {
    if row_ids.is_empty() {
        return Ok(0);
    }

    let old_rows = collect_rows_by_row_ids(table, row_ids)?;
    let updated_rows = build_updated_rows_chunk(table, &old_rows, column_ids, values)?;

    let tablet = table.tablet();
    let schema = tablet
        .schema()
        .ok_or_else(|| paro_error::internal("tablet schema missing"))?;

    if schema.keys_type() == KeysType::PrimaryKeys {
        let num_key_columns = schema.num_key_columns();
        if num_key_columns == 0 {
            return Err(paro_error::internal(
                "PRIMARY_KEYS tablet must have at least one key column",
            ));
        }

        let old_key_chunk = upsert::build_primary_key_chunk(table, &old_rows)?;
        let removed = deleter::delete_by_primary_keys(table, &old_key_chunk, txn.clone())?;
        if removed != row_ids.len() {
            return Err(paro_error::internal(format!(
                "UPDATE removed {} rows but expected {}",
                removed,
                row_ids.len()
            )));
        }

        let touches_key = column_ids
            .iter()
            .any(|&column_id| column_id < num_key_columns);
        if touches_key {
            writer::append_with_transaction(table, &updated_rows, txn)?;
        } else {
            let partial_rows =
                build_partial_updated_rows_chunk(table, &old_rows, column_ids, values)?;
            let mut partial_columns: Vec<usize> = (0..num_key_columns).collect();
            partial_columns.extend(column_ids.iter().copied());
            partial_columns.sort_unstable();
            partial_columns.dedup();
            writer::append_partial_with_transaction(
                table,
                &partial_rows,
                partial_columns,
                row_ids,
                txn,
            )?;
        }
        return Ok(removed);
    }

    let removed = deleter::delete(table, row_ids, txn.clone())?;
    if removed != row_ids.len() {
        return Err(paro_error::internal(format!(
            "UPDATE removed {} rows but expected {}",
            removed,
            row_ids.len()
        )));
    }
    writer::append_with_transaction(table, &updated_rows, txn)?;
    Ok(removed)
}
