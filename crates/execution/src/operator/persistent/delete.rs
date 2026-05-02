// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical Delete Operator
//!
//!
//! ## Dependencies Check
//! - Allocator: ✅ Uses ExecutionContext allocator
//! - Storage: ✅ Uses TableHandle::delete
//! - LocalStorage: ❌ Deprecated (delete path now goes to TableHandle)
//!
//! ## Known Limitations
//! - No RETURNING clause support
//! - No bound constraints checking
//! - No index update support
//! - Simple row_id based deletion
//!
//! ## Design Notes
//! PhysicalDelete is a Sink + Source operator:
//! - Sink phase: Receives chunks containing row_ids to delete
//! - Source phase: Returns the count of deleted rows
//!
//! The child operator (typically a scan + filter) produces rows with a row_id column.
//! The delete operator uses the row_id to identify and delete rows from storage.
//!
//! For PRIMARY_KEYS tables, deletes are issued by primary key.

use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use paro_catalog::entry::{ConstraintType, TableCatalogEntry};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_context::dml_table_lock_requests;
use paro_transaction::TableId;

use crate::execution_context::ExecutionContext;
use crate::operator::state::{
    GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState, OperatorSinkCombineInput,
    OperatorSinkInput, OperatorSourceInput,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::{SinkCombineResultType, SinkResultType, SourceResultType};

fn primary_key_columns(table: &TableCatalogEntry) -> Option<Vec<usize>> {
    table
        .constraints
        .iter()
        .find(|c| c.constraint_type == ConstraintType::PrimaryKey)
        .map(|c| c.columns.clone())
        .filter(|cols| !cols.is_empty())
}

/// Physical Delete operator.
///
/// Returns the count of deleted rows.
#[derive(Debug)]
pub struct PhysicalDelete {
    /// Target table to delete from.
    pub table: Arc<TableCatalogEntry>,
    /// Index of the row_id column in the input chunk.
    pub row_id_index: usize,
    /// Whether to return the deleted rows (RETURNING clause).
    /// Currently not supported, always false.
    pub return_chunk: bool,
    /// Whether this is DELETE without WHERE (full-table fast path).
    pub is_full_table_delete: bool,
    /// Return types (always BIGINT for row count).
    pub return_types: Vec<LogicalType>,
    /// Child operator (source of row_ids).
    pub child: Arc<dyn PhysicalOperator>,
    /// Stored sink state for sink+source handoff.
    sink_state: Mutex<Option<Arc<dyn GlobalSinkState>>>,
}

impl PhysicalDelete {
    /// Create a new PhysicalDelete operator.
    ///
    /// # Arguments
    /// * `table` - The table to delete from
    /// * `row_id_index` - Index of the row_id column in the input chunk
    /// * `child` - Child operator that produces rows with row_ids
    pub fn new(
        table: Arc<TableCatalogEntry>,
        row_id_index: usize,
        child: Arc<dyn PhysicalOperator>,
        is_full_table_delete: bool,
    ) -> Self {
        Self {
            table,
            row_id_index,
            return_chunk: false,
            is_full_table_delete,
            return_types: vec![LogicalType::BigInt],
            child,
            sink_state: Mutex::new(None),
        }
    }
}

// ========== States ==========

/// Global sink state for delete operation.
#[derive(Debug)]
struct DeleteGlobalSinkState {
    /// Total count of deleted rows.
    deleted_count: AtomicUsize,
    /// Guard to ensure full-table delete fast path is executed once.
    full_table_delete_executed: AtomicBool,
}

impl Default for DeleteGlobalSinkState {
    fn default() -> Self {
        Self {
            deleted_count: AtomicUsize::new(0),
            full_table_delete_executed: AtomicBool::new(false),
        }
    }
}

impl GlobalSinkState for DeleteGlobalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::scan::dummy_scan::PhysicalDummyScan;
    use crate::thread_context::ThreadContext;
    use paro_catalog::entry::{ColumnDefinition, TableCatalogEntry};

    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_scheduler::task::InterruptState;
    use paro_storage::table::table_factory::TableFactory;
    use paro_storage::transaction::txn::Transaction;
    use std::sync::Arc;

    fn create_storage(types: &[LogicalType]) -> paro_storage::table::table_handle::TableHandle {
        TableFactory::default().create_table(types).unwrap()
    }

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn build_delete_operator() -> PhysicalDelete {
        let storage = Arc::new(create_storage(&[LogicalType::Integer]));
        let table = Arc::new(TableCatalogEntry::new(
            "paro".to_string(),
            "public".to_string(),
            "t".to_string(),
            vec![ColumnDefinition::new(
                "id".to_string(),
                LogicalType::Integer,
            )],
            storage,
            0,
        ));
        let child = Arc::new(PhysicalDummyScan::new()) as Arc<dyn PhysicalOperator>;
        PhysicalDelete::new(table, 0, child, false)
    }

    #[test]
    fn delete_source_returns_deleted_count_from_sink_state() {
        let op = build_delete_operator();
        let session: Arc<StatementContext> = test_session();
        let thread = ThreadContext::single_threaded();
        let ctx = ExecutionContext::new(session, &thread, None);

        let sink_state = DeleteGlobalSinkState::default();
        sink_state.deleted_count.store(7, Ordering::SeqCst);

        let gsource = op
            .get_global_source_state(&ctx, Some(&sink_state))
            .expect("source state should be created");
        let mut lsource = op
            .get_local_source_state(&ctx, gsource.as_ref())
            .expect("local source state should be created");

        let interrupt = InterruptState::new();
        let mut input = OperatorSourceInput::new(gsource.as_ref(), lsource.as_mut(), &interrupt);
        let mut chunk =
            paro_common::test_utils::test_chunk_with_capacity(&[LogicalType::BigInt], 1);

        let result = op
            .get_data(&ctx, &mut chunk, &mut input)
            .expect("get_data should succeed");
        assert_eq!(result, SourceResultType::Finished);
        assert_eq!(chunk.column(0).unwrap().get_value(0), Value::BigInt(7));
        assert_eq!(chunk.size(), 1);

        let second = op
            .get_data(&ctx, &mut chunk, &mut input)
            .expect("second get_data should succeed");
        assert_eq!(second, SourceResultType::Finished);
    }

    #[test]
    fn delete_source_defaults_to_zero_without_sink_state() {
        let op = build_delete_operator();
        let session: Arc<StatementContext> = test_session();
        let thread = ThreadContext::single_threaded();
        let ctx = ExecutionContext::new(session, &thread, None);

        let gsource = op
            .get_global_source_state(&ctx, None)
            .expect("source state should be created");
        let mut lsource = op
            .get_local_source_state(&ctx, gsource.as_ref())
            .expect("local source state should be created");

        let interrupt = InterruptState::new();
        let mut input = OperatorSourceInput::new(gsource.as_ref(), lsource.as_mut(), &interrupt);
        let mut chunk =
            paro_common::test_utils::test_chunk_with_capacity(&[LogicalType::BigInt], 1);

        let result = op
            .get_data(&ctx, &mut chunk, &mut input)
            .expect("get_data should succeed");
        assert_eq!(result, SourceResultType::Finished);
        assert_eq!(chunk.column(0).unwrap().get_value(0), Value::BigInt(0));
    }

    #[test]
    fn delete_sink_full_table_fast_path_stages_all_rows_in_transaction() {
        let storage = Arc::new(create_storage(&[LogicalType::Integer]));
        let input_chunk = Chunk::from_vectors(
            vec![paro_common::test_utils::test_i32_vector_with_allocator(
                &[1, 2, 3],
                paro_common::test_utils::test_allocator(),
            )],
            paro_common::test_utils::test_allocator(),
        );
        storage.append(&input_chunk).expect("append should succeed");

        let table = Arc::new(TableCatalogEntry::new(
            "paro".to_string(),
            "public".to_string(),
            "t".to_string(),
            vec![ColumnDefinition::new(
                "id".to_string(),
                LogicalType::Integer,
            )],
            storage.clone(),
            0,
        ));
        let child = Arc::new(PhysicalDummyScan::new()) as Arc<dyn PhysicalOperator>;
        let op = PhysicalDelete::new(table, 0, child, true);

        let txn = Arc::new(Transaction::new(70_001, 0));
        let mut session_ctx = (*test_session()).clone();
        session_ctx.txn.active = Some(txn.clone());
        let session: Arc<StatementContext> = Arc::new(session_ctx);
        let thread = ThreadContext::single_threaded();
        let ctx = ExecutionContext::new(session, &thread, None);

        let gsink = op
            .get_global_sink_state(&ctx)
            .expect("global sink state should be created");
        let mut lsink = op
            .get_local_sink_state(&ctx)
            .expect("local sink state should be created");
        let interrupt = InterruptState::new();
        let mut sink_input = OperatorSinkInput::new(gsink.as_ref(), lsink.as_mut(), &interrupt);

        let sink_probe = Chunk::from_vectors(
            vec![paro_common::test_utils::test_i32_vector_with_allocator(
                &[0],
                paro_common::test_utils::test_allocator(),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let sink_result = op
            .sink(&ctx, &sink_probe, &mut sink_input)
            .expect("sink should succeed");
        assert_eq!(sink_result, SinkResultType::Finished);

        let gsource = op
            .get_global_source_state(&ctx, Some(gsink.as_ref()))
            .expect("source state should be created");
        let mut lsource = op
            .get_local_source_state(&ctx, gsource.as_ref())
            .expect("local source state should be created");
        let mut source_input =
            OperatorSourceInput::new(gsource.as_ref(), lsource.as_mut(), &interrupt);
        let mut out = paro_common::test_utils::test_chunk_with_capacity(&[LogicalType::BigInt], 1);
        let source_result = op
            .get_data(&ctx, &mut out, &mut source_input)
            .expect("get_data should succeed");
        assert_eq!(source_result, SourceResultType::Finished);
        assert_eq!(out.column(0).unwrap().get_value(0), Value::BigInt(3));
        assert!(txn.has_pending_storage_work());

        let remaining_rows: usize = storage
            .scan_chunks()
            .expect("scan should succeed")
            .iter()
            .map(|chunk| chunk.size())
            .sum();
        assert_eq!(remaining_rows, 3);
    }
}

/// Local sink state for delete operation.
#[derive(Debug, Default)]
struct DeleteLocalSinkState;

impl LocalSinkState for DeleteLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Global source state for delete operation.
#[derive(Debug)]
struct DeleteGlobalSourceState {
    /// Whether we've already returned the result.
    returned: Mutex<bool>,
    /// Total count of deleted rows captured from sink state.
    deleted_count: usize,
}

impl DeleteGlobalSourceState {
    fn new(deleted_count: usize) -> Self {
        Self {
            returned: Mutex::new(false),
            deleted_count,
        }
    }
}

impl GlobalSourceState for DeleteGlobalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Local source state for delete operation.
#[derive(Debug, Default)]
struct DeleteLocalSourceState;

impl LocalSourceState for DeleteLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// ========== PhysicalOperator Implementation ==========

impl PhysicalOperator for PhysicalDelete {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::Delete
    }

    fn types(&self) -> &[LogicalType] {
        &self.return_types
    }

    fn explain_params(&self) -> Vec<String> {
        vec![format!("Table: {}", self.table.name())]
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

    fn is_source(&self) -> bool {
        true
    }

    fn is_sink(&self) -> bool {
        true
    }

    fn set_sink_state(&self, state: Arc<dyn GlobalSinkState>) {
        match self.sink_state.lock() {
            Ok(mut guard) => {
                *guard = Some(state);
            }
            Err(poisoned) => {
                let mut guard = poisoned.into_inner();
                *guard = Some(state);
            }
        }
    }

    fn sink_state(&self) -> Option<Arc<dyn GlobalSinkState>> {
        match self.sink_state.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    fn parallel_sink(&self) -> bool {
        true
    }

    // ========== Sink Interface ==========

    fn get_global_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        let target = paro_common::ddl::DdlObjectKey::new(
            self.table.base.base.catalog.clone(),
            Some(self.table.base.schema_name.clone()),
            self.table.base.base.name.clone(),
            paro_common::ddl::DdlObjectKind::Table,
        );
        ctx.session.bind_write_database(&target.database)?;
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
        if let Some(txn) = ctx.active_transaction() {
            txn.acquire_lock_requests(dml_table_lock_requests(txn.lock_namespace(), &target))?;
        }
        if let Some(write_guard) = ctx.session.write_guard() {
            write_guard.begin_dml_write()?;
        }
        if let Some(txn) = ctx.active_transaction() {
            txn.record_dml_table(self.table.base.base.object_id.raw())?;
        }
        if let Some(storage) = self.table.get_storage() {
            ctx.transaction_view()
                .read_tracker()
                .record_table_read(TableId::new(storage.table_id()));
        }
        Ok(Box::new(DeleteGlobalSinkState::default()))
    }

    fn get_local_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(DeleteLocalSinkState))
    }

    fn sink(
        &self,
        _ctx: &ExecutionContext,
        chunk: &Chunk,
        input: &mut OperatorSinkInput,
    ) -> Result<SinkResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<DeleteGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid global sink state".to_string()))?;

        // Get storage
        let storage = self.table.get_storage().ok_or_else(|| {
            paro_error::internal(format!(
                "Table {} has no storage",
                self.table.base.base.name
            ))
        })?;

        let txn = _ctx.active_transaction().ok_or_else(|| {
            paro_error::internal(
                "DELETE reached storage without an active transaction; frontend DML must enter the commit runtime path",
            )
        })?;
        if self.is_full_table_delete {
            if gstate
                .full_table_delete_executed
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                _ctx.transaction_view()
                    .read_tracker()
                    .record_predicate(TableId::new(storage.table_id()), 0);
                let deleted = storage.delete_all(_ctx.transaction_view(), txn.clone())?;
                gstate.deleted_count.fetch_add(deleted, Ordering::SeqCst);
            }
            return Ok(SinkResultType::Finished);
        }

        let mut total_deleted = 0;
        if let Some(pk_cols) = primary_key_columns(&self.table) {
            let mut key_vectors = Vec::with_capacity(pk_cols.len());
            for idx in pk_cols {
                let col = chunk.column(idx).ok_or_else(|| {
                    paro_error::internal(format!("primary key column {} not found", idx))
                })?;
                key_vectors.push(col.clone());
            }
            let key_chunk = Chunk::from_arc_vectors(key_vectors, chunk.allocator().clone());
            total_deleted +=
                storage.delete_by_primary_keys(_ctx.transaction_view(), &key_chunk, txn.clone())?;
        } else {
            // Get row_id column
            let row_id_col = chunk
                .column(self.row_id_index)
                .ok_or_else(|| paro_error::internal("row_id column not found".to_string()))?;

            // Collect row_ids to delete
            let mut row_ids = Vec::with_capacity(chunk.size());
            for i in 0..chunk.size() {
                let value = row_id_col.get_value(i);
                match value {
                    Value::BigInt(row_id) => row_ids.push(row_id as u64),
                    Value::UBigInt(row_id) => row_ids.push(row_id),
                    Value::Integer(row_id) => row_ids.push(row_id as u64),
                    Value::Null(_) => {
                        // Skip null row_ids
                        continue;
                    }
                    _ => {
                        return Err(paro_error::internal(format!(
                            "Invalid row_id type: {:?}",
                            value
                        )));
                    }
                }
            }

            let deleted = storage.delete(_ctx.transaction_view(), &row_ids, txn.clone())?;
            total_deleted += deleted;
        }

        // Update deleted count
        gstate
            .deleted_count
            .fetch_add(total_deleted, Ordering::SeqCst);

        if total_deleted > 0 {
            txn.record_graph_delete(self.table.base.base.object_id.raw(), total_deleted);
        }

        Ok(SinkResultType::NeedMoreInput)
    }

    fn combine(
        &self,
        _ctx: &ExecutionContext,
        _input: &mut OperatorSinkCombineInput,
    ) -> Result<SinkCombineResultType> {
        // Nothing to combine - we use atomic counter
        Ok(SinkCombineResultType::Finished)
    }

    // ========== Source Interface ==========

    fn get_global_source_state(
        &self,
        _ctx: &ExecutionContext,
        sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        let internal_sink_state = self.sink_state();
        let deleted_count = sink_state
            .and_then(|state| state.as_any().downcast_ref::<DeleteGlobalSinkState>())
            .map(|state| state.deleted_count.load(Ordering::SeqCst))
            .or_else(|| {
                internal_sink_state
                    .as_ref()
                    .and_then(|state| state.as_any().downcast_ref::<DeleteGlobalSinkState>())
                    .map(|state| state.deleted_count.load(Ordering::SeqCst))
            })
            .unwrap_or(0);
        Ok(Box::new(DeleteGlobalSourceState::new(deleted_count)))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(DeleteLocalSourceState))
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
            .downcast_ref::<DeleteGlobalSourceState>()
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

        // Return the deleted count
        let col = chunk
            .column_mut(0)
            .ok_or_else(|| paro_error::internal("Output column not found".to_string()))?;

        let internal_sink_state = self.sink_state();
        let deleted_count = internal_sink_state
            .as_ref()
            .and_then(|state| state.as_any().downcast_ref::<DeleteGlobalSinkState>())
            .map(|state| state.deleted_count.load(Ordering::SeqCst))
            .unwrap_or(gstate.deleted_count);

        col.set_value(0, &Value::BigInt(deleted_count as i64));
        chunk.set_cardinality(1);

        Ok(SourceResultType::Finished)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
