// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical Insert Operator
//!
//!
//! ## Dependencies Check
//! - Allocator: ✅ Uses ExecutionContext allocator
//! - LocalStorage: ❌ Deprecated (write path now goes to TableHandle)
//!
//! ## Design Notes
//! PhysicalInsert is a Sink + Source operator:
//! - Sink phase: Receives chunks and writes data to a table
//! - Source phase: Returns the count of inserted rows
//!
//! Data is written directly to TableHandle (Tablet/Rowset pipeline).

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::execution_context::ExecutionContext;
use crate::operator::state::{
    GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState, OperatorSinkCombineInput,
    OperatorSinkInput, OperatorSourceInput,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::{SinkCombineResultType, SinkResultType, SourceResultType};
use paro_catalog::entry::TableCatalogEntry;
use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::operator::{InsertOnConflict, InsertOnConflictAction};
use paro_storage::table::table_handle::InsertOnConflictAction as StorageInsertOnConflictAction;

const COPY_BUFFER_SIZE_SETTING: &str = "copy_buffer_size";
const COPY_FLUSH_THREADS_SETTING: &str = "copy_flush_threads";
const DEFAULT_COPY_BUFFER_SIZE: usize = 8192;
const DEFAULT_COPY_FLUSH_THREADS: usize = 1;

#[derive(Debug, Default)]
struct InsertSharedState {
    inserted_count: AtomicUsize,
}

#[derive(Debug)]
pub struct PhysicalInsert {
    /// Target table
    pub table: Arc<TableCatalogEntry>,
    /// Column mapping (input column index -> table column index)
    pub column_index_map: Vec<usize>,
    /// Expected types for the input columns
    pub expected_types: Vec<LogicalType>,
    /// Optional ON CONFLICT behavior.
    on_conflict: Option<InsertOnConflict>,
    /// Return types (always BIGINT for row count)
    pub return_types: Vec<LogicalType>,
    /// Child operator (source of data)
    pub child: Arc<dyn PhysicalOperator>,
    /// True when this insert consumes COPY FROM read_csv output.
    copy_from_read_csv: bool,
    shared_state: Arc<InsertSharedState>,
}

impl PhysicalInsert {
    pub fn new(
        table: Arc<TableCatalogEntry>,
        column_index_map: Vec<usize>,
        expected_types: Vec<LogicalType>,
        on_conflict: Option<InsertOnConflict>,
        child: Arc<dyn PhysicalOperator>,
        copy_from_read_csv: bool,
    ) -> Self {
        Self {
            table,
            column_index_map,
            expected_types,
            on_conflict,
            return_types: vec![LogicalType::BigInt],
            child,
            copy_from_read_csv,
            shared_state: Arc::new(InsertSharedState::default()),
        }
    }

    pub fn inserted_count(&self) -> usize {
        self.shared_state.inserted_count.load(Ordering::SeqCst)
    }

    fn setting_to_usize(value: &Value, setting: &str) -> Result<usize> {
        let raw = match value {
            Value::Varchar(v) => v.trim().trim_matches('\'').trim_matches('"').to_string(),
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

    fn resolve_copy_setting(ctx: &ExecutionContext, key: &str, default: usize) -> Result<usize> {
        let Some(value) = ctx.session.get_setting(key) else {
            return Ok(default);
        };
        Self::setting_to_usize(value, key)
    }

    fn initialize_copy_buffering(
        &self,
        ctx: &ExecutionContext,
        lstate: &mut InsertLocalSinkState,
    ) -> Result<()> {
        if lstate.initialized {
            return Ok(());
        }

        lstate.initialized = true;
        lstate.copy_buffering_enabled = self.copy_from_read_csv;
        if !lstate.copy_buffering_enabled {
            return Ok(());
        }

        lstate.copy_buffer_size =
            Self::resolve_copy_setting(ctx, COPY_BUFFER_SIZE_SETTING, DEFAULT_COPY_BUFFER_SIZE)?;
        lstate.copy_flush_threads = Self::resolve_copy_setting(
            ctx,
            COPY_FLUSH_THREADS_SETTING,
            DEFAULT_COPY_FLUSH_THREADS,
        )?;
        lstate.buffered_chunks.reserve(lstate.copy_flush_threads);
        Ok(())
    }

    fn flush_buffered_chunks(
        &self,
        ctx: &ExecutionContext,
        storage: &Arc<paro_storage::table::table_handle::TableHandle>,
        txn: Option<std::sync::Arc<paro_storage::transaction::txn::Transaction>>,
        lstate: &mut InsertLocalSinkState,
    ) -> Result<usize> {
        if lstate.buffered_rows == 0 {
            return Ok(0);
        }

        let table_types = storage.types();
        let mut merged = Chunk::try_initialize(
            table_types,
            lstate.buffered_rows,
            ctx.allocator(MemoryTag::MemTable),
        )?;
        merged.try_set_cardinality(lstate.buffered_rows)?;

        let mut dst_row = 0;
        for chunk in lstate.buffered_chunks.drain(..) {
            let rows = chunk.size();
            for col_idx in 0..chunk.column_count() {
                let src_col = chunk
                    .column(col_idx)
                    .ok_or_else(|| paro_error::internal("source column missing".to_string()))?;
                let dst_col = merged.column_mut(col_idx).ok_or_else(|| {
                    paro_error::internal("destination column missing".to_string())
                })?;

                dst_col.try_copy_range(dst_row, src_col, 0, rows)?;
            }
            dst_row += rows;
        }

        storage.append_with_transaction(&merged, txn)?;
        lstate.buffered_rows = 0;
        Ok(dst_row)
    }
}

#[derive(Debug)]
struct InsertGlobalSinkState {
    shared_state: Arc<InsertSharedState>,
    append_lock: Mutex<()>,
}

impl GlobalSinkState for InsertGlobalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
struct InsertLocalSinkState {
    initialized: bool,
    copy_buffering_enabled: bool,
    copy_buffer_size: usize,
    copy_flush_threads: usize,
    buffered_chunks: Vec<Chunk>,
    buffered_rows: usize,
}

impl Default for InsertLocalSinkState {
    fn default() -> Self {
        Self {
            initialized: false,
            copy_buffering_enabled: false,
            copy_buffer_size: DEFAULT_COPY_BUFFER_SIZE,
            copy_flush_threads: DEFAULT_COPY_FLUSH_THREADS,
            buffered_chunks: Vec::new(),
            buffered_rows: 0,
        }
    }
}

impl LocalSinkState for InsertLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Global source state for insert operation.
#[derive(Debug)]
struct InsertGlobalSourceState {
    /// Whether we've already returned the result.
    returned: Mutex<bool>,
    /// Reference to shared state to get inserted count.
    shared_state: Arc<InsertSharedState>,
}

impl GlobalSourceState for InsertGlobalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Local source state for insert operation.
#[derive(Debug, Default)]
struct InsertLocalSourceState;

impl LocalSourceState for InsertLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl PhysicalOperator for PhysicalInsert {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::Insert
    }

    fn types(&self) -> &[LogicalType] {
        &self.return_types
    }

    fn explain_params(&self) -> Vec<String> {
        let mut params = vec![format!("Table: {}", self.table.name())];
        if self.copy_from_read_csv {
            params.push("Mode: COPY FROM buffered insert".to_string());
            params.push("Parallel: enabled".to_string());
        }
        if !self.column_index_map.is_empty() {
            params.push(format!(
                "Column Mapping: {}",
                self.column_index_map
                    .iter()
                    .enumerate()
                    .map(|(input_idx, table_idx)| {
                        let target = self
                            .table
                            .columns
                            .get(*table_idx)
                            .map(|column| column.name.as_str())
                            .unwrap_or("?");
                        format!("input#{input_idx}->{target}")
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if let Some(on_conflict) = &self.on_conflict {
            let action = match &on_conflict.action {
                InsertOnConflictAction::DoNothing => "DO NOTHING",
                InsertOnConflictAction::DoUpdate { .. } => "DO UPDATE",
            };
            params.push(format!("OnConflict: {}", action));
        }
        params
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn is_source(&self) -> bool {
        true
    }

    fn is_sink(&self) -> bool {
        true
    }

    /// COPY FROM read_csv can use parallel sink with guarded append.
    ///
    /// For general INSERT we still run sequentially.
    fn parallel_sink(&self) -> bool {
        self.copy_from_read_csv
    }

    fn children_count(&self) -> usize {
        1
    }

    fn child(&self, index: usize) -> Option<&dyn PhysicalOperator> {
        if index == 0 {
            Some(self.child.as_ref())
        } else {
            None
        }
    }

    fn child_arc(&self, index: usize) -> Option<Arc<dyn PhysicalOperator>> {
        if index == 0 {
            Some(self.child.clone())
        } else {
            None
        }
    }

    // ========== Sink Interface ==========

    fn get_global_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        let target = paro_common::ddl::DdlObjectKey::new(
            self.table.base.base.catalog.clone(),
            Some(self.table.base.schema_name.clone()),
            self.table.base.base.name.clone(),
            paro_common::ddl::DdlObjectKind::Table,
        );
        if let Some(admission) = ctx.session.txn_admission() {
            let write_class = ctx
                .session
                .write_guard()
                .map(|guard| guard.class())
                .unwrap_or_default();
            admission.admit_table_dml(
                write_class,
                &target,
                self.table.base.base.timestamp() == ctx.transaction_id(),
            )?;
        }
        if let Some(write_guard) = ctx.session.write_guard() {
            write_guard.begin_dml_write()?;
        }
        if let Some(txn) = ctx.active_transaction() {
            txn.record_dml_table(self.table.base.base.object_id.raw())?;
        }
        Ok(Box::new(InsertGlobalSinkState {
            shared_state: self.shared_state.clone(),
            append_lock: Mutex::new(()),
        }))
    }

    fn get_local_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(InsertLocalSinkState::default()))
    }

    fn sink(
        &self,
        ctx: &ExecutionContext,
        chunk: &Chunk,
        input: &mut OperatorSinkInput,
    ) -> Result<SinkResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<InsertGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid global sink state".to_string()))?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<InsertLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid local sink state".to_string()))?;

        // 1. Resolve storage
        let storage = self.table.get_storage().ok_or_else(|| {
            paro_error::internal(format!(
                "Table {} has no storage",
                self.table.base.base.name
            ))
        })?;

        self.initialize_copy_buffering(ctx, lstate)?;

        // 2. Prepare chunk for append
        let table_types = storage.types();
        let allocator = ctx.allocator(MemoryTag::Allocator);
        let mut append_chunk = Chunk::try_initialize(table_types, chunk.size(), allocator.clone())?;

        // 3. Map input columns to table columns
        for (input_idx, table_idx) in self.column_index_map.iter().enumerate() {
            append_chunk.data[*table_idx] = chunk.data[input_idx].clone();
        }

        append_chunk.set_cardinality(chunk.size());

        // Materialize into MemTable allocator so write-buffer memory is tracked
        // under the dedicated MemTable memory tag.
        let append_chunk = append_chunk.try_deep_copy(ctx.allocator(MemoryTag::MemTable))?;

        let txn = ctx.active_transaction();
        let mut affected_rows = chunk.size();
        if let Some(on_conflict) = &self.on_conflict {
            if lstate.copy_buffering_enabled {
                return Err(paro_error::not_implemented(
                    "COPY FROM does not support ON CONFLICT yet",
                ));
            }

            let storage_action = match &on_conflict.action {
                InsertOnConflictAction::DoNothing => StorageInsertOnConflictAction::DoNothing,
                InsertOnConflictAction::DoUpdate {
                    target_columns,
                    source_columns,
                } => StorageInsertOnConflictAction::DoUpdate {
                    target_columns: target_columns.clone(),
                    source_columns: source_columns.clone(),
                },
            };

            let _append_guard = gstate
                .append_lock
                .lock()
                .map_err(|e| paro_error::internal(e.to_string()))?;
            let affected = storage.insert_on_conflict(&append_chunk, &storage_action, txn)?;
            affected_rows = affected;
            gstate
                .shared_state
                .inserted_count
                .fetch_add(affected, Ordering::SeqCst);
        } else if lstate.copy_buffering_enabled {
            lstate.buffered_rows += append_chunk.size();
            lstate.buffered_chunks.push(append_chunk);
            if lstate.buffered_rows >= lstate.copy_buffer_size {
                let _append_guard = gstate
                    .append_lock
                    .lock()
                    .map_err(|e| paro_error::internal(e.to_string()))?;
                let flushed = self.flush_buffered_chunks(ctx, storage, txn, lstate)?;
                if flushed > 0 {
                    gstate
                        .shared_state
                        .inserted_count
                        .fetch_add(flushed, Ordering::SeqCst);
                }
            }
        } else {
            // 4. Append to storage (Tablet/Rowset pipeline)
            let _append_guard = gstate
                .append_lock
                .lock()
                .map_err(|e| paro_error::internal(e.to_string()))?;
            storage.append_with_transaction(&append_chunk, txn)?;

            // 5. Update global state
            gstate
                .shared_state
                .inserted_count
                .fetch_add(chunk.size(), Ordering::SeqCst);
        }

        if let Some(txn) = ctx.active_transaction() {
            txn.record_graph_insert(self.table.base.base.object_id.raw(), affected_rows);
        }

        Ok(SinkResultType::NeedMoreInput)
    }

    fn combine(
        &self,
        ctx: &ExecutionContext,
        input: &mut OperatorSinkCombineInput,
    ) -> Result<SinkCombineResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<InsertGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid global sink state".to_string()))?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<InsertLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid local sink state".to_string()))?;

        if !lstate.copy_buffering_enabled || lstate.buffered_rows == 0 {
            return Ok(SinkCombineResultType::Finished);
        }

        let storage = self.table.get_storage().ok_or_else(|| {
            paro_error::internal(format!(
                "Table {} has no storage",
                self.table.base.base.name
            ))
        })?;

        let _append_guard = gstate
            .append_lock
            .lock()
            .map_err(|e| paro_error::internal(e.to_string()))?;
        let flushed = self.flush_buffered_chunks(ctx, storage, ctx.active_transaction(), lstate)?;
        if flushed > 0 {
            gstate
                .shared_state
                .inserted_count
                .fetch_add(flushed, Ordering::SeqCst);
        }
        Ok(SinkCombineResultType::Finished)
    }

    // ========== Source Interface ==========

    fn get_global_source_state(
        &self,
        _ctx: &ExecutionContext,
        _sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        Ok(Box::new(InsertGlobalSourceState {
            returned: Mutex::new(false),
            shared_state: self.shared_state.clone(),
        }))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(InsertLocalSourceState))
    }

    fn get_data(
        &self,
        _ctx: &ExecutionContext,
        chunk: &mut Chunk,
        input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<InsertGlobalSourceState>()
            .ok_or_else(|| paro_error::internal("Invalid global source state".to_string()))?;

        // Check if we've already returned the result
        let mut returned = gstate
            .returned
            .lock()
            .map_err(|e| paro_error::internal(e.to_string()))?;

        if *returned {
            return Ok(SourceResultType::Finished);
        }

        *returned = true;

        // Get the inserted count from shared state
        let inserted_count = gstate.shared_state.inserted_count.load(Ordering::SeqCst);

        // Return the inserted count
        let col = chunk
            .column_mut(0)
            .ok_or_else(|| paro_error::internal("Output column not found".to_string()))?;

        col.set_value(0, &Value::BigInt(inserted_count as i64));
        chunk.set_cardinality(1);

        Ok(SourceResultType::Finished)
    }
}
