// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::codec::chunk_encoder;
use crate::primary_key::RowID;
use crate::table::index_runtime::IndexRuntime;
use crate::table::table_handle::TableHandle;
use crate::tablet::KeysType;
use crate::transaction::txn::Transaction;
use crate::write::DeltaWriter;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

pub(crate) fn append(table: &TableHandle, chunk: &Chunk) -> Result<()> {
    append_with_transaction(table, chunk, None)
}

pub(crate) fn append_with_transaction(
    table: &TableHandle,
    chunk: &Chunk,
    txn: Option<Arc<Transaction>>,
) -> Result<()> {
    if chunk.size() == 0 {
        return Ok(());
    }

    let tablet = table.tablet();
    let art_columns = table.declared_art_columns();
    let fulltext_columns = table.declared_fulltext_columns_with_config();

    if let Some(txn) = txn {
        if !art_columns.is_empty() {
            txn.register_pending_art_columns(tablet.tablet_id(), art_columns)?;
        }
        if !fulltext_columns.is_empty() {
            txn.register_pending_fulltext_columns(tablet.tablet_id(), fulltext_columns)?;
        }
        txn.append_to_tablet(tablet, chunk)?;
        return Ok(());
    }

    let mut writer =
        DeltaWriter::open_with_allocator(tablet.clone(), now_micros(), chunk.allocator().clone())?;
    if tablet
        .schema()
        .map(|schema| schema.keys_type() == KeysType::PrimaryKeys)
        .unwrap_or(false)
    {
        writer.write_chunk(chunk)?;
    } else {
        let cols = chunk_encoder::encode_chunk(table.types(), chunk)?;
        writer.write(&cols)?;
    }
    let rowset = writer.commit()?;
    if !fulltext_columns.is_empty() {
        IndexRuntime::build_runtime_fulltext_indexes_for_rowset(&rowset, &fulltext_columns)?;
    }
    if !art_columns.is_empty() {
        if let Err(err) = IndexRuntime::build_runtime_art_indexes_for_rowset(&rowset, &art_columns)
        {
            tracing::warn!(
                error = %err,
                "ART index backfill failed for committed rowset; queries will fallback to scan"
            );
        }
    }
    Ok(())
}

pub(crate) fn append_partial_with_transaction(
    table: &TableHandle,
    chunk: &Chunk,
    partial_column_indices: Vec<usize>,
    base_row_ids: &[u64],
    txn: Option<Arc<Transaction>>,
) -> Result<()> {
    if chunk.size() == 0 {
        return Ok(());
    }
    if chunk.size() != base_row_ids.len() {
        return Err(paro_error::invalid_input(format!(
            "partial append chunk/base_row_ids length mismatch: {} vs {}",
            chunk.size(),
            base_row_ids.len()
        )));
    }

    let tablet = table.tablet();
    let mut writer = DeltaWriter::open_partial_with_allocator(
        tablet,
        now_micros(),
        partial_column_indices,
        chunk.allocator().clone(),
    )?;
    let base_row_ids: Vec<RowID> = base_row_ids.iter().copied().map(RowID::from_raw).collect();
    writer.write_partial_chunk(chunk, &base_row_ids)?;

    if let Some(txn) = txn {
        writer.commit_in_transaction(txn)?;
        return Ok(());
    }

    let _rowset = writer.commit()?;
    Ok(())
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}
