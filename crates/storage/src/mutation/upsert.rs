use std::collections::HashMap;
use std::sync::Arc;

use crate::mutation::{deleter, updater, writer};
use crate::primary_key::PrimaryKeySerializer;
use crate::table::table_handle::{InsertOnConflictAction, TableHandle};
use crate::tablet::KeysType;
use crate::transaction::txn::Transaction;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;

pub(crate) fn insert_on_conflict(
    table: &TableHandle,
    chunk: &Chunk,
    action: &InsertOnConflictAction,
    txn: Option<Arc<Transaction>>,
) -> Result<usize> {
    if chunk.size() == 0 {
        return Ok(0);
    }
    let tablet = table.tablet();
    let schema = tablet
        .schema()
        .ok_or_else(|| paro_error::internal("tablet schema missing"))?;
    if schema.keys_type() != KeysType::PrimaryKeys {
        return Err(paro_error::not_implemented(
            "ON CONFLICT is only supported for PRIMARY_KEYS tables",
        ));
    }

    let deduped = dedup_primary_key_chunk_last_write(table, chunk)?;
    let serializer = PrimaryKeySerializer::from_schema_ref(&schema)?;
    let encoded_keys = serializer.encode_chunk(&deduped)?;
    let mut insert_rows = Vec::new();
    let mut conflict_row_ids = Vec::new();
    let mut conflict_input_rows = Vec::new();
    let existing = tablet.lookup_primary_keys(&encoded_keys)?;

    for (row_idx, row_id) in existing.into_iter().enumerate() {
        if let Some(row_id) = row_id {
            conflict_row_ids.push(row_id.to_raw());
            conflict_input_rows.push(row_idx as u32);
        } else {
            insert_rows.push(row_idx as u32);
        }
    }

    let insert_chunk = if insert_rows.is_empty() {
        None
    } else {
        Some(materialize_rows_as_flat_chunk(&deduped, &insert_rows)?)
    };

    let mut affected = insert_rows.len();
    match action {
        InsertOnConflictAction::DoNothing => {
            if let Some(insert_chunk) = insert_chunk {
                writer::append_with_transaction(table, &insert_chunk, txn)?;
            }
        }
        InsertOnConflictAction::DoUpdate {
            target_columns,
            source_columns,
        } => {
            let conflict_chunk = if conflict_row_ids.is_empty() {
                None
            } else {
                Some(materialize_rows_as_flat_chunk(
                    &deduped,
                    &conflict_input_rows,
                )?)
            };

            if let Some(txn) = txn {
                let mut pending_chunks = Vec::new();
                if let Some(insert_chunk) = insert_chunk {
                    pending_chunks.push(insert_chunk);
                }

                if let Some(conflict_chunk) = conflict_chunk {
                    let values = extract_source_values(
                        &conflict_chunk,
                        source_columns,
                        "ON CONFLICT DO UPDATE",
                    )?;
                    let old_rows = updater::collect_rows_by_row_ids(table, &conflict_row_ids)?;
                    let updated_rows = updater::build_updated_rows_chunk(
                        table,
                        &old_rows,
                        target_columns,
                        &values,
                    )?;
                    let old_key_chunk = build_primary_key_chunk(table, &old_rows)?;
                    let removed =
                        deleter::delete_by_primary_keys(table, &old_key_chunk, Some(txn.clone()))?;
                    if removed != conflict_row_ids.len() {
                        return Err(paro_error::internal(format!(
                            "ON CONFLICT DO UPDATE removed {} rows but expected {}",
                            removed,
                            conflict_row_ids.len()
                        )));
                    }
                    pending_chunks.push(updated_rows);
                    affected += conflict_row_ids.len();
                }

                if !pending_chunks.is_empty() {
                    let combined = concatenate_flat_chunks(&pending_chunks)?;
                    writer::append_with_transaction(table, &combined, Some(txn))?;
                }
            } else {
                if let Some(insert_chunk) = insert_chunk {
                    writer::append_with_transaction(table, &insert_chunk, None)?;
                }

                if let Some(conflict_chunk) = conflict_chunk {
                    let values = extract_source_values(
                        &conflict_chunk,
                        source_columns,
                        "ON CONFLICT DO UPDATE",
                    )?;
                    affected +=
                        updater::update(table, &conflict_row_ids, target_columns, &values, None)?;
                }
            }
        }
    }

    Ok(affected)
}

pub(crate) fn dedup_primary_key_chunk_last_write(
    table: &TableHandle,
    chunk: &Chunk,
) -> Result<Chunk> {
    let schema = table
        .tablet()
        .schema()
        .ok_or_else(|| paro_error::internal("tablet schema missing"))?;
    let serializer = PrimaryKeySerializer::from_schema_ref(&schema)?;
    let encoded_keys = serializer.encode_chunk(chunk)?;
    let mut last_row_by_key = HashMap::new();
    for (row_idx, key) in encoded_keys.into_iter().enumerate() {
        last_row_by_key.insert(key, row_idx as u32);
    }
    if last_row_by_key.len() == chunk.size() {
        return Ok(chunk.clone());
    }
    let mut keep_rows: Vec<u32> = last_row_by_key.into_values().collect();
    keep_rows.sort_unstable();
    materialize_rows_as_flat_chunk(chunk, &keep_rows)
}

pub(crate) fn build_primary_key_chunk(table: &TableHandle, rows: &Chunk) -> Result<Chunk> {
    let schema = table
        .tablet()
        .schema()
        .ok_or_else(|| paro_error::internal("tablet schema missing"))?;
    let mut key_vectors = Vec::with_capacity(schema.num_key_columns());
    for idx in 0..schema.num_key_columns() {
        let column = rows.column(idx).ok_or_else(|| {
            paro_error::internal(format!("missing key column {} in rows chunk", idx))
        })?;
        key_vectors.push(column.clone());
    }
    Ok(Chunk::from_arc_vectors(key_vectors))
}

pub(crate) fn materialize_rows_as_flat_chunk(chunk: &Chunk, row_indices: &[u32]) -> Result<Chunk> {
    let mut materialized = Chunk::initialize_with_allocator(
        &chunk.types(),
        row_indices.len(),
        chunk.allocator().clone(),
    );
    materialized.set_cardinality(row_indices.len());

    for col_idx in 0..chunk.column_count() {
        let src = chunk.column(col_idx).ok_or_else(|| {
            paro_error::internal("missing source column during chunk row materialization")
        })?;
        let dst = materialized.column_mut(col_idx).ok_or_else(|| {
            paro_error::internal("missing destination column during chunk row materialization")
        })?;
        for (new_row_idx, source_row_idx) in row_indices.iter().enumerate() {
            dst.copy_at(new_row_idx, src, *source_row_idx as usize);
        }
    }

    Ok(materialized)
}

pub(crate) fn concatenate_flat_chunks(chunks: &[Chunk]) -> Result<Chunk> {
    let first = chunks
        .first()
        .ok_or_else(|| paro_error::internal("cannot concatenate empty chunk list"))?;
    let total_rows: usize = chunks.iter().map(Chunk::size).sum();
    let mut combined =
        Chunk::initialize_with_allocator(&first.types(), total_rows, first.allocator().clone());
    combined.set_cardinality(total_rows);

    let mut dst_row = 0;
    for chunk in chunks {
        for col_idx in 0..chunk.column_count() {
            let src = chunk.column(col_idx).ok_or_else(|| {
                paro_error::internal("missing source column during chunk concatenation")
            })?;
            let dst = combined.column_mut(col_idx).ok_or_else(|| {
                paro_error::internal("missing destination column during chunk concatenation")
            })?;
            for row_idx in 0..chunk.size() {
                dst.copy_at(dst_row + row_idx, src, row_idx);
            }
        }
        dst_row += chunk.size();
    }

    Ok(combined)
}

fn extract_source_values(
    chunk: &Chunk,
    source_columns: &[usize],
    context: &str,
) -> Result<Vec<Vec<Value>>> {
    let mut values = Vec::with_capacity(source_columns.len());
    for &source_column in source_columns {
        let vector = chunk.column(source_column).ok_or_else(|| {
            paro_error::internal(format!(
                "missing source column {} for {}",
                source_column, context
            ))
        })?;
        let mut column_values = Vec::with_capacity(chunk.size());
        for row_idx in 0..chunk.size() {
            column_values.push(vector.get_value(row_idx));
        }
        values.push(column_values);
    }
    Ok(values)
}
