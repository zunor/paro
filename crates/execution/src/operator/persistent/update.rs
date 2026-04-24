// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical Update Operator
//!
//!
//! ## Dependencies Check
//! - Allocator: ✅ Uses ExecutionContext allocator
//! - Storage: ✅ Uses TableHandle::update
//! - LocalStorage: ❌ Deprecated (update path now goes to TableHandle)
//!
//! ## Known Limitations
//! - No RETURNING clause support
//! - No bound constraints checking
//! - No index update support (update_is_del_and_insert)
//! - Simple row_id based update
//!
//! ## Design Notes
//! PhysicalUpdate is a Sink + Source operator:
//! - Sink phase: Receives chunks containing full updated rows and row_ids
//! - Source phase: Returns the count of updated rows
//!
//! The child operator produces rows with:
//! - Columns for the full updated row (`[col_0, col_1, ..., col_N]`)
//! - A row_id column (last column) identifying which rows to update
//!
//! For PRIMARY_KEYS tables, updates are applied as delete+insert when possible.

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use paro_catalog::entry::TableCatalogEntry;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;

use crate::execution_context::ExecutionContext;
use crate::operator::state::{
    GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState, OperatorSinkCombineInput,
    OperatorSinkInput, OperatorSourceInput,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::{SinkCombineResultType, SinkResultType, SourceResultType};

fn collect_updated_column_values(chunk: &Chunk, columns: &[usize]) -> Result<Vec<Vec<Value>>> {
    let mut column_values = Vec::with_capacity(columns.len());
    for &table_col_idx in columns {
        let col = chunk.column(table_col_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "updated table column {} not found in input chunk",
                table_col_idx
            ))
        })?;
        let mut values = Vec::with_capacity(chunk.size());
        for i in 0..chunk.size() {
            values.push(col.get_value(i));
        }
        column_values.push(values);
    }
    Ok(column_values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::scan::dummy_scan::PhysicalDummyScan;
    use crate::thread_context::ThreadContext;
    use paro_catalog::entry::{ColumnDefinition, TableCatalogEntry};
    use paro_common::chunk::Chunk;
    use paro_common::runtime_value::Value;

    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_scheduler::task::InterruptState;
    use paro_storage::table::table_factory::TableFactory;
    use paro_storage::tablet::{tablet_reader::TabletReaderParams, KeysType};
    use std::sync::Arc;

    fn create_storage(types: &[LogicalType]) -> paro_storage::table::table_handle::TableHandle {
        TableFactory::default().create_table(types).unwrap()
    }

    fn create_storage_with_keys(
        types: &[LogicalType],
        keys_type: KeysType,
    ) -> paro_storage::table::table_handle::TableHandle {
        TableFactory::default()
            .create_table_with_keys(types, keys_type)
            .unwrap()
    }

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn build_update_operator() -> PhysicalUpdate {
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
        PhysicalUpdate::new(table, vec![0], 0, child)
    }

    #[test]
    fn collect_updated_column_values_uses_table_column_indexes() {
        let chunk = Chunk::from_vectors(
            vec![
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[10, 20],
                    paro_common::test_utils::test_allocator(),
                ),
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[30, 40],
                    paro_common::test_utils::test_allocator(),
                ),
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[50, 60],
                    paro_common::test_utils::test_allocator(),
                ),
                paro_common::test_utils::test_i64_vector_with_allocator(
                    &[100, 200],
                    paro_common::test_utils::test_allocator(),
                ),
            ],
            paro_common::test_utils::test_allocator(),
        );

        let values = collect_updated_column_values(&chunk, &[2, 0])
            .expect("collecting updated column values should succeed");
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], vec![Value::Integer(50), Value::Integer(60)]);
        assert_eq!(values[1], vec![Value::Integer(10), Value::Integer(20)]);
    }

    #[test]
    fn update_source_returns_updated_count_from_sink_state() {
        let op = build_update_operator();
        let session: Arc<StatementContext> = test_session();
        let thread = ThreadContext::single_threaded();
        let ctx = ExecutionContext::new(session, &thread, None);

        let sink_state = UpdateGlobalSinkState::default();
        sink_state.updated_count.store(11, Ordering::SeqCst);

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
        assert_eq!(chunk.column(0).unwrap().get_value(0), Value::BigInt(11));
        assert_eq!(chunk.size(), 1);

        let second = op
            .get_data(&ctx, &mut chunk, &mut input)
            .expect("second get_data should succeed");
        assert_eq!(second, SourceResultType::Finished);
    }

    #[test]
    fn update_source_defaults_to_zero_without_sink_state() {
        let op = build_update_operator();
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
    fn update_sink_supports_primary_key_tables_via_delete_insert() {
        let storage = Arc::new(create_storage_with_keys(
            &[LogicalType::Integer, LogicalType::Integer],
            KeysType::PrimaryKeys,
        ));
        storage
            .append(&Chunk::from_vectors(
                vec![
                    paro_common::test_utils::test_i32_vector_with_allocator(
                        &[1, 2],
                        paro_common::test_utils::test_allocator(),
                    ),
                    paro_common::test_utils::test_i32_vector_with_allocator(
                        &[10, 20],
                        paro_common::test_utils::test_allocator(),
                    ),
                ],
                paro_common::test_utils::test_allocator(),
            ))
            .expect("append rows");
        let table = Arc::new(TableCatalogEntry::new(
            "paro".to_string(),
            "public".to_string(),
            "t".to_string(),
            vec![
                ColumnDefinition::new("id".to_string(), LogicalType::Integer),
                ColumnDefinition::new("score".to_string(), LogicalType::Integer),
            ],
            storage.clone(),
            0,
        ));
        let child = Arc::new(PhysicalDummyScan::new()) as Arc<dyn PhysicalOperator>;
        let op = PhysicalUpdate::new(table, vec![0, 1], 2, child);

        let session: Arc<StatementContext> = test_session();
        let thread = ThreadContext::single_threaded();
        let ctx = ExecutionContext::new(session, &thread, None);
        let mut gstate = op
            .get_global_sink_state(&ctx)
            .expect("global sink state should be created");
        let mut lstate = op
            .get_local_sink_state(&ctx)
            .expect("local sink state should be created");
        let interrupt = InterruptState::new();
        let mut input = OperatorSinkInput::new(gstate.as_mut(), lstate.as_mut(), &interrupt);

        let mut reader = storage
            .create_reader(
                TabletReaderParams::with_version(storage.max_version()).with_emit_row_id(true),
            )
            .expect("create reader");
        reader.prepare().expect("prepare reader");

        let mut target_row_id = None;
        while let Some(chunk) = reader.get_next_chunk().expect("read chunk") {
            let ids = chunk.column(0).expect("id column");
            let row_ids = chunk.column(2).expect("row id column");
            for idx in 0..chunk.size() {
                if ids.get_i32(idx) == Some(1) {
                    target_row_id = Some(row_ids.get_i64(idx).expect("row id") as u64);
                }
            }
        }
        let target_row_id = target_row_id.expect("row id for primary key update");

        let chunk = Chunk::from_vectors(
            vec![
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[3],
                    paro_common::test_utils::test_allocator(),
                ),
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[15],
                    paro_common::test_utils::test_allocator(),
                ),
                paro_common::test_utils::test_i64_vector_with_allocator(
                    &[target_row_id as i64],
                    paro_common::test_utils::test_allocator(),
                ),
            ],
            paro_common::test_utils::test_allocator(),
        );
        let result = op
            .sink(&ctx, &chunk, &mut input)
            .expect("PRIMARY_KEYS UPDATE should succeed");
        assert_eq!(result, SinkResultType::NeedMoreInput);

        let mut rows = Vec::new();
        for chunk in storage.scan_chunks().expect("scan rows") {
            let id_col = chunk.column(0).expect("id column");
            let score_col = chunk.column(1).expect("score column");
            for idx in 0..chunk.size() {
                rows.push((
                    id_col.get_i32(idx).expect("id"),
                    score_col.get_i32(idx).expect("score"),
                ));
            }
        }
        rows.sort_unstable_by_key(|(id, _)| *id);
        assert_eq!(rows, vec![(2, 20), (3, 15)]);
    }
}

/// Physical Update operator.
///
/// Returns the count of updated rows.
#[derive(Debug)]
pub struct PhysicalUpdate {
    /// Target table to update.
    pub table: Arc<TableCatalogEntry>,
    /// The indices of the columns being updated (in the table).
    pub columns: Vec<usize>,
    /// Index of the row_id column in the input chunk (typically the last column).
    pub row_id_index: usize,
    /// Whether to return the updated rows (RETURNING clause).
    /// Currently not supported, always false.
    pub return_chunk: bool,
    /// Return types (always BIGINT for row count).
    pub return_types: Vec<LogicalType>,
    /// Child operator (source of row_ids and new values).
    pub child: Arc<dyn PhysicalOperator>,
    /// Stored sink state for sink+source handoff.
    sink_state: Mutex<Option<Arc<dyn GlobalSinkState>>>,
}

impl PhysicalUpdate {
    /// Create a new PhysicalUpdate operator.
    ///
    /// # Arguments
    /// * `table` - The table to update
    /// * `columns` - The indices of columns being updated (in the table)
    /// * `row_id_index` - Index of the row_id column in the input chunk
    /// * `child` - Child operator that produces rows with new values and row_ids
    pub fn new(
        table: Arc<TableCatalogEntry>,
        columns: Vec<usize>,
        row_id_index: usize,
        child: Arc<dyn PhysicalOperator>,
    ) -> Self {
        Self {
            table,
            columns,
            row_id_index,
            return_chunk: false,
            return_types: vec![LogicalType::BigInt],
            child,
            sink_state: Mutex::new(None),
        }
    }
}

// ========== States ==========

/// Global sink state for update operation.
#[derive(Debug)]
struct UpdateGlobalSinkState {
    /// Total count of updated rows.
    updated_count: AtomicUsize,
}

impl Default for UpdateGlobalSinkState {
    fn default() -> Self {
        Self {
            updated_count: AtomicUsize::new(0),
        }
    }
}

impl GlobalSinkState for UpdateGlobalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Local sink state for update operation.
#[derive(Debug, Default)]
struct UpdateLocalSinkState;

impl LocalSinkState for UpdateLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Global source state for update operation.
#[derive(Debug)]
struct UpdateGlobalSourceState {
    /// Total count of updated rows collected in sink phase.
    updated_count: usize,
    /// Whether we've already returned the result.
    returned: Mutex<bool>,
}

impl UpdateGlobalSourceState {
    fn new(updated_count: usize) -> Self {
        Self {
            updated_count,
            returned: Mutex::new(false),
        }
    }
}

impl GlobalSourceState for UpdateGlobalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Local source state for update operation.
#[derive(Debug, Default)]
struct UpdateLocalSourceState;

impl LocalSourceState for UpdateLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// ========== PhysicalOperator Implementation ==========

impl PhysicalOperator for PhysicalUpdate {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::Update
    }

    fn types(&self) -> &[LogicalType] {
        &self.return_types
    }

    fn explain_params(&self) -> Vec<String> {
        let mut params = vec![format!("Table: {}", self.table.name())];
        if !self.columns.is_empty() {
            params.push(format!(
                "Set: {}",
                self.columns
                    .iter()
                    .map(|&idx| {
                        self.table
                            .columns
                            .get(idx)
                            .map(|col| col.name.clone())
                            .unwrap_or_else(|| format!("col#{idx}"))
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        params
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
        Ok(Box::new(UpdateGlobalSinkState::default()))
    }

    fn get_local_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(UpdateLocalSinkState))
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
            .downcast_ref::<UpdateGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid global sink state".to_string()))?;

        // Get storage
        let storage = self.table.get_storage().ok_or_else(|| {
            paro_error::internal(format!(
                "Table {} has no storage",
                self.table.base.base.name
            ))
        })?;
        // Collect new values from table column positions in the full-row input chunk.
        let column_values = collect_updated_column_values(chunk, &self.columns)?;

        // Get row_id column (last column in the input)
        let row_id_col = chunk
            .column(self.row_id_index)
            .ok_or_else(|| paro_error::internal("row_id column not found".to_string()))?;

        // Collect row_ids (1:1 with input rows).
        let mut row_ids = Vec::with_capacity(chunk.size());
        for i in 0..chunk.size() {
            let value = row_id_col.get_value(i);
            let row_id = match value {
                Value::BigInt(v) if v >= 0 => v as u64,
                Value::UBigInt(v) => v,
                Value::Integer(v) if v >= 0 => v as u64,
                Value::BigInt(v) => {
                    return Err(paro_error::internal(format!(
                        "Invalid negative row_id value: {}",
                        v
                    )));
                }
                Value::Integer(v) => {
                    return Err(paro_error::internal(format!(
                        "Invalid negative row_id value: {}",
                        v
                    )));
                }
                Value::Null(_) => {
                    return Err(paro_error::internal(
                        "row_id column contains NULL in UPDATE input".to_string(),
                    ));
                }
                _ => {
                    return Err(paro_error::internal(format!(
                        "Invalid row_id type: {:?}",
                        value
                    )));
                }
            };
            row_ids.push(row_id);
        }

        // Update directly in table storage (storage layer performs delete+insert).
        let total_updated = storage.update(
            &row_ids,
            &self.columns,
            &column_values,
            _ctx.active_transaction(),
        )?;

        // Update count
        gstate
            .updated_count
            .fetch_add(total_updated, Ordering::SeqCst);

        if total_updated > 0 {
            if let Some(txn) = _ctx.active_transaction() {
                let updated_columns: Vec<u32> = self
                    .columns
                    .iter()
                    .filter_map(|&idx| u32::try_from(idx).ok())
                    .collect();
                txn.record_graph_update(
                    self.table.base.base.object_id.raw(),
                    total_updated,
                    &updated_columns,
                );
            }
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
        let updated_count = sink_state
            .and_then(|state| state.as_any().downcast_ref::<UpdateGlobalSinkState>())
            .map(|state| state.updated_count.load(Ordering::SeqCst))
            .or_else(|| {
                internal_sink_state
                    .as_ref()
                    .and_then(|state| state.as_any().downcast_ref::<UpdateGlobalSinkState>())
                    .map(|state| state.updated_count.load(Ordering::SeqCst))
            })
            .unwrap_or(0);
        Ok(Box::new(UpdateGlobalSourceState::new(updated_count)))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(UpdateLocalSourceState))
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
            .downcast_ref::<UpdateGlobalSourceState>()
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

        // Return the updated count
        let col = chunk
            .column_mut(0)
            .ok_or_else(|| paro_error::internal("Output column not found".to_string()))?;

        let internal_sink_state = self.sink_state();
        let updated_count = internal_sink_state
            .as_ref()
            .and_then(|state| state.as_any().downcast_ref::<UpdateGlobalSinkState>())
            .map(|state| state.updated_count.load(Ordering::SeqCst))
            .unwrap_or(gstate.updated_count);

        col.set_value(0, &Value::BigInt(updated_count as i64));
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
