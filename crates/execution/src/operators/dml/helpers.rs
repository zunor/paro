// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;
use std::sync::Arc;

use paro_catalog::entry::{ConstraintType, TableCatalogEntry};
use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::ddl::{DdlObjectKey, DdlObjectKind};
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_context::dml_table_lock_requests;
use paro_function::scalar::FunctionExecContext;
use paro_storage::table::table_handle::TableHandle;
use paro_transaction::TableId;

use crate::physical::specs::InsertSpec;
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::state::{
    CopyToSinkGlobal, CopyToSinkLocal, DmlSinkGlobal, InsertSinkLocal, SinkGlobal, SinkLocal,
};

pub(crate) const COPY_BUFFER_SIZE_SETTING: &str = "copy_buffer_size";
pub(crate) const COPY_FLUSH_THREADS_SETTING: &str = "copy_flush_threads";
pub(crate) const DEFAULT_COPY_BUFFER_SIZE: usize = 8192;
pub(crate) const DEFAULT_COPY_FLUSH_THREADS: usize = 1;

pub(crate) fn bind_dml_write(
    ctx: &PipelineInitContext,
    table: &Arc<TableCatalogEntry>,
    record_table_read: bool,
) -> Result<()> {
    let target = DdlObjectKey::new(
        table.base.base.catalog.clone(),
        Some(table.base.schema_name.clone()),
        table.base.base.name.clone(),
        DdlObjectKind::Table,
    );
    ctx.query.session.bind_write_database(&target.database)?;
    if let Some(admission) = ctx.query.session.txn_admission() {
        let write_class = ctx
            .query
            .session
            .write_guard()
            .map(|guard| guard.class())
            .unwrap_or_default();
        admission.admit_table_dml(
            write_class,
            &target,
            table.base.base.timestamp() == ctx.query.session.transaction_id(),
        )?;
    }
    if let Some(txn) = ctx.query.session.active_transaction() {
        txn.acquire_lock_requests(dml_table_lock_requests(txn.lock_namespace(), &target))?;
        txn.record_dml_table(table.base.base.object_id.raw())?;
    }
    if let Some(write_guard) = ctx.query.session.write_guard() {
        write_guard.begin_dml_write()?;
    }
    if record_table_read {
        if let Some(storage) = table.get_storage() {
            ctx.query
                .transaction
                .read_tracker()
                .record_table_read(TableId::new(storage.table_id()));
        }
    }
    Ok(())
}

pub(crate) fn active_transaction(
    ctx: &OperatorCallContext,
    op: &'static str,
) -> Result<Arc<paro_storage::transaction::txn::Transaction>> {
    ctx.query.session.active_transaction().ok_or_else(|| {
        paro_error::internal(format!(
            "{op} reached storage without an active transaction; frontend DML must enter the commit runtime path"
        ))
    })
}

pub(crate) fn storage_table(table: &Arc<TableCatalogEntry>) -> Result<Arc<TableHandle>> {
    table.get_storage().cloned().ok_or_else(|| {
        paro_error::internal(format!("table {} has no storage", table.base.base.name))
    })
}

fn setting_to_usize(value: &Value, setting: &str) -> Result<usize> {
    let raw = match value {
        Value::Varchar(value) => value
            .trim()
            .trim_matches('\'')
            .trim_matches('"')
            .to_string(),
        _ => value.to_string(),
    };
    let parsed: i64 = raw.parse().map_err(|_| {
        paro_error::invalid_input(format!(
            "Invalid value for {}: '{}'. Expected a positive integer.",
            setting, raw
        ))
    })?;
    if parsed < 1 {
        return Err(paro_error::invalid_input(format!(
            "Invalid value for {}: '{}'. Must be >= 1.",
            setting, raw
        )));
    }
    Ok(parsed as usize)
}

fn resolve_copy_setting(
    ctx: &OperatorCallContext,
    key: &str,
    default_value: usize,
) -> Result<usize> {
    let Some(value) = ctx.query.session.get_setting(key) else {
        return Ok(default_value);
    };
    setting_to_usize(value, key)
}

pub(crate) fn initialize_insert_buffering(
    ctx: &OperatorCallContext,
    spec: &InsertSpec,
    local: &mut InsertSinkLocal,
) -> Result<()> {
    if local.initialized {
        return Ok(());
    }
    local.initialized = true;
    local.copy_buffering_enabled = spec.copy_from_read_csv;
    if !local.copy_buffering_enabled {
        return Ok(());
    }
    local.copy_buffer_size =
        resolve_copy_setting(ctx, COPY_BUFFER_SIZE_SETTING, DEFAULT_COPY_BUFFER_SIZE)?;
    local.copy_flush_threads =
        resolve_copy_setting(ctx, COPY_FLUSH_THREADS_SETTING, DEFAULT_COPY_FLUSH_THREADS)?;
    local.buffered_chunks.reserve(local.copy_flush_threads);
    Ok(())
}

pub(crate) fn flush_insert_buffered_chunks(
    ctx: &OperatorCallContext,
    storage: &Arc<TableHandle>,
    txn: Arc<paro_storage::transaction::txn::Transaction>,
    local: &mut InsertSinkLocal,
) -> Result<usize> {
    if local.buffered_rows == 0 {
        return Ok(0);
    }
    let mut flushed_rows = 0;
    for chunk in local.buffered_chunks.drain(..) {
        storage.append_with_transaction(&ctx.query.transaction, &chunk, txn.clone())?;
        flushed_rows += chunk.size();
    }
    local.buffered_rows = 0;
    Ok(flushed_rows)
}

pub(crate) fn collect_updated_column_values(
    chunk: &Chunk,
    columns: &[usize],
) -> Result<Vec<Vec<Value>>> {
    let mut column_values = Vec::with_capacity(columns.len());
    for &table_col_idx in columns {
        let col = chunk.column(table_col_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "updated table column {} not found in input chunk",
                table_col_idx
            ))
        })?;
        let mut values = Vec::with_capacity(chunk.size());
        for row_idx in 0..chunk.size() {
            values.push(col.get_value(row_idx));
        }
        column_values.push(values);
    }
    Ok(column_values)
}

pub(crate) fn collect_row_ids(
    chunk: &Chunk,
    row_id_index: usize,
    skip_nulls: bool,
) -> Result<Vec<u64>> {
    let row_id_col = chunk
        .column(row_id_index)
        .ok_or_else(|| paro_error::internal("row_id column not found"))?;
    let mut row_ids = Vec::with_capacity(chunk.size());
    match row_id_col.logical_type() {
        LogicalType::UBigInt => {
            for row_idx in 0..chunk.size() {
                match row_id_col.get_u64(row_idx) {
                    Some(value) => row_ids.push(value),
                    None if skip_nulls => {}
                    None => {
                        return Err(paro_error::internal(
                            "row_id column contains NULL in DML input",
                        ));
                    }
                }
            }
        }
        LogicalType::BigInt => {
            for row_idx in 0..chunk.size() {
                match row_id_col.get_i64(row_idx) {
                    Some(value) if value >= 0 => row_ids.push(value as u64),
                    None if skip_nulls => {}
                    None => {
                        return Err(paro_error::internal(
                            "row_id column contains NULL in DML input",
                        ));
                    }
                    Some(value) => {
                        return Err(paro_error::internal(format!(
                            "invalid row_id value: {value}"
                        )));
                    }
                }
            }
        }
        LogicalType::UInteger => {
            for row_idx in 0..chunk.size() {
                match row_id_col.get_u32(row_idx) {
                    Some(value) => row_ids.push(value as u64),
                    None if skip_nulls => {}
                    None => {
                        return Err(paro_error::internal(
                            "row_id column contains NULL in DML input",
                        ));
                    }
                }
            }
        }
        _ => {
            for row_idx in 0..chunk.size() {
                match row_id_col.get_value(row_idx) {
                    Value::Integer(value) if value >= 0 => row_ids.push(value as u64),
                    Value::Null(_) if skip_nulls => {}
                    Value::Null(_) => {
                        return Err(paro_error::internal(
                            "row_id column contains NULL in DML input",
                        ));
                    }
                    value => {
                        return Err(paro_error::internal(format!(
                            "invalid row_id value: {:?}",
                            value
                        )));
                    }
                }
            }
        }
    }
    Ok(row_ids)
}

pub(crate) fn primary_key_columns(table: &TableCatalogEntry) -> Option<Vec<usize>> {
    table
        .constraints()
        .iter()
        .find(|constraint| constraint.constraint_type == ConstraintType::PrimaryKey)
        .map(|constraint| constraint.columns.clone())
        .filter(|columns| !columns.is_empty())
}

pub(crate) fn dml_result_chunk(ctx: &OperatorFinishContext, count: u64) -> Result<Chunk> {
    let mut chunk = Chunk::try_initialize(
        &[LogicalType::BigInt],
        1,
        ctx.query.allocator(MemoryTag::Allocator),
    )?;
    let col = chunk
        .column_mut(0)
        .ok_or_else(|| paro_error::internal("DML completion column missing"))?;
    col.set_value(0, &Value::BigInt(count as i64));
    chunk.try_set_cardinality(1)?;
    Ok(chunk)
}

pub(crate) fn build_per_thread_output_path(file_path: &str, file_id: usize) -> String {
    let path = Path::new(file_path);
    let parent = path.parent();
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("copy");
    let ext = path.extension().and_then(|ext| ext.to_str());
    let filename = match ext {
        Some(ext) if !ext.is_empty() => format!("{stem}_{file_id}.{ext}"),
        _ => format!("{stem}_{file_id}"),
    };
    match parent {
        Some(parent) if !parent.as_os_str().is_empty() => {
            parent.join(filename).to_string_lossy().to_string()
        }
        _ => filename,
    }
}

#[inline(always)]
pub(crate) fn dml_global(global: &SinkGlobal) -> Result<&DmlSinkGlobal> {
    match global {
        SinkGlobal::Dml(state) => Ok(state.as_ref()),
        _ => Err(paro_error::internal("DML sink global state mismatch")),
    }
}

#[inline(always)]
pub(crate) fn insert_local(local: &mut SinkLocal) -> Result<&mut InsertSinkLocal> {
    match local {
        SinkLocal::Insert(state) => Ok(state),
        _ => Err(paro_error::internal("insert sink local state mismatch")),
    }
}

#[inline(always)]
pub(crate) fn copy_to_global(global: &SinkGlobal) -> Result<&CopyToSinkGlobal> {
    match global {
        SinkGlobal::CopyToFile(state) => Ok(state.as_ref()),
        _ => Err(paro_error::internal("COPY TO sink global state mismatch")),
    }
}

#[inline(always)]
pub(crate) fn copy_to_local(local: &mut SinkLocal) -> Result<&mut CopyToSinkLocal> {
    match local {
        SinkLocal::CopyToFile(state) => Ok(state),
        _ => Err(paro_error::internal("COPY TO sink local state mismatch")),
    }
}
