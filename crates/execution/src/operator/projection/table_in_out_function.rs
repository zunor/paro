// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical Table-In-Out Function Operator
//!
//!
//! ## Dependencies Check
//! - TableFunction: ✅ `paro_function::table`
//! - Chunk: ✅ `paro_common::chunk`
//!
//! - TableInOutFunction is an Operator (not Source) that processes input data
//! - It receives input Chunks and produces output Chunks
//! - Used for functions like `unnest` when applied to table columns
//!   - `GlobalTableFunctionState`: Shared across threads
//!   - `LocalTableFunctionState`: Thread-local state
//!
//! ## Execution Flow
//! 1. `GetOperatorState()` - Initialize local state
//! 2. `GetGlobalOperatorState()` - Initialize global state
//! 3. `Execute()` - Process input chunks via `in_out_function`
//! 4. `FinalExecute()` - Flush remaining output via `in_out_function_final`

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::table::{
    GlobalTableFunctionState, LocalTableFunctionState, OperatorFinalizeResultType,
    OperatorResultType as TableInOutResultType, TableFunction, TableFunctionBindData,
    TableFunctionInitInput, TableFunctionInput,
};

use crate::execution_context::ExecutionContext;
use crate::operator::state::{GlobalOperatorState, OperatorState};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::{
    OperatorFinalizeResultType as ExecFinalizeResultType, OperatorResultType,
};

/// Bind data wrapper for table-in-out function execution.
pub struct TableInOutBindDataWrapper {
    /// The table function definition.
    pub function: Arc<TableFunction>,
    /// Bind data returned from the bind phase.
    pub bind_data: Option<Box<dyn TableFunctionBindData>>,
    /// Input values (constants passed to the function).
    pub input_values: Vec<Value>,
    /// Column IDs to scan (for projection pushdown).
    pub column_ids: Vec<usize>,
    /// Output column types.
    pub output_types: Vec<LogicalType>,
    /// Output column names.
    pub output_names: Vec<String>,
    /// Input table types.
    pub input_table_types: Vec<LogicalType>,
    /// Input table column names.
    pub input_table_names: Vec<String>,
    /// Projected input columns (columns from input to pass through).
    pub projected_input: Vec<usize>,
}

impl fmt::Debug for TableInOutBindDataWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableInOutBindDataWrapper")
            .field("function", &self.function.name)
            .field("has_bind_data", &self.bind_data.is_some())
            .field("input_values", &self.input_values)
            .field("column_ids", &self.column_ids)
            .field("output_types", &self.output_types)
            .field("output_names", &self.output_names)
            .field("input_table_types", &self.input_table_types)
            .field("input_table_names", &self.input_table_names)
            .field("projected_input", &self.projected_input)
            .finish()
    }
}

impl TableInOutBindDataWrapper {
    /// Create new bind data wrapper.
    pub fn new(
        function: Arc<TableFunction>,
        bind_data: Option<Box<dyn TableFunctionBindData>>,
        input_values: Vec<Value>,
        column_ids: Vec<usize>,
        output_types: Vec<LogicalType>,
        output_names: Vec<String>,
        input_table_types: Vec<LogicalType>,
        input_table_names: Vec<String>,
    ) -> Self {
        Self {
            function,
            bind_data,
            input_values,
            column_ids,
            output_types,
            output_names,
            input_table_types,
            input_table_names,
            projected_input: Vec::new(),
        }
    }

    /// Set projected input columns.
    pub fn with_projected_input(mut self, projected_input: Vec<usize>) -> Self {
        self.projected_input = projected_input;
        self
    }

    /// Get estimated cardinality from bind data.
    pub fn estimated_cardinality(&self) -> Option<usize> {
        self.bind_data.as_ref().and_then(|bd| bd.cardinality())
    }
}

/// Global operator state for table-in-out function.
pub struct TableInOutGlobalState {
    /// Global state from the table function (if any).
    global_state: Option<Box<dyn GlobalTableFunctionState>>,
}

impl fmt::Debug for TableInOutGlobalState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableInOutGlobalState")
            .field("has_global_state", &self.global_state.is_some())
            .finish()
    }
}

impl GlobalOperatorState for TableInOutGlobalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Local operator state for table-in-out function.
pub struct TableInOutLocalState {
    /// Local state from the table function (if any).
    local_state: Option<Box<dyn LocalTableFunctionState>>,
    /// Current row index in the input chunk (for row-by-row processing).
    row_index: usize,
    /// Whether we're processing a new row.
    new_row: bool,
}

impl fmt::Debug for TableInOutLocalState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableInOutLocalState")
            .field("has_local_state", &self.local_state.is_some())
            .field("row_index", &self.row_index)
            .field("new_row", &self.new_row)
            .finish()
    }
}

impl OperatorState for TableInOutLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl TableInOutLocalState {
    /// Get the local state mutable reference.
    pub fn local_state_mut(&mut self) -> Option<&mut dyn LocalTableFunctionState> {
        match &mut self.local_state {
            Some(s) => Some(s.as_mut()),
            None => None,
        }
    }
}

/// Physical table-in-out function operator.
///
/// Executes table functions that process input data row-by-row.
/// Unlike regular table functions (which are Sources), table-in-out
/// functions are Operators that transform input data.
///
/// # Example
/// ```sql
/// SELECT * FROM t, unnest(t.array_column);
/// ```
pub struct TableInOutFunction {
    /// Output types of this operator.
    output_types: Vec<LogicalType>,
    /// Bind data containing the function and its state.
    bind_data: Arc<TableInOutBindDataWrapper>,
    /// Estimated cardinality.
    estimated_cardinality: usize,
    /// Child operator.
    child: Arc<dyn PhysicalOperator>,
}

impl TableInOutFunction {
    /// Create a new table-in-out function operator.
    pub fn new(bind_data: TableInOutBindDataWrapper, child: Arc<dyn PhysicalOperator>) -> Self {
        let output_types = bind_data.output_types.clone();
        let estimated_cardinality = bind_data.estimated_cardinality().unwrap_or(1000);

        Self {
            output_types,
            bind_data: Arc::new(bind_data),
            estimated_cardinality,
            child,
        }
    }

    /// Get the bind data.
    pub fn bind_data(&self) -> &TableInOutBindDataWrapper {
        &self.bind_data
    }

    /// Get the function name.
    pub fn function_name(&self) -> &str {
        &self.bind_data.function.name
    }
}

impl fmt::Debug for TableInOutFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableInOutFunction")
            .field("function", &self.bind_data.function.name)
            .field("output_types", &self.output_types)
            .field("estimated_cardinality", &self.estimated_cardinality)
            .finish()
    }
}

impl PhysicalOperator for TableInOutFunction {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::InOutFunction
    }

    fn types(&self) -> &[LogicalType] {
        &self.output_types
    }

    fn estimated_cardinality(&self) -> usize {
        self.estimated_cardinality
    }

    fn is_source(&self) -> bool {
        false
    }

    fn is_sink(&self) -> bool {
        false
    }

    fn parallel_operator(&self) -> bool {
        true
    }

    fn requires_final_execute(&self) -> bool {
        self.bind_data.function.in_out_function_final.is_some()
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

    fn get_global_operator_state(&self) -> Result<Box<dyn GlobalOperatorState>> {
        let bind_data = &self.bind_data;

        // Initialize global state if the function provides init_global
        let global_state = if let Some(init_global) = bind_data.function.init_global {
            let input = TableFunctionInitInput::new(
                bind_data.bind_data.as_ref().map(|b| b.as_ref()),
                &bind_data.column_ids,
            );
            init_global(&input)?
        } else {
            None
        };

        Ok(Box::new(TableInOutGlobalState { global_state }))
    }

    fn get_operator_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn OperatorState>> {
        Ok(Box::new(TableInOutLocalState {
            local_state: None,
            row_index: 0,
            new_row: true,
        }))
    }

    fn execute(
        &self,
        _ctx: &ExecutionContext,
        input: &Chunk,
        chunk: &mut Chunk,
        gstate: &dyn GlobalOperatorState,
        state: &mut dyn OperatorState,
        _memory: crate::memory_runtime::OperatorMemoryScope<'_>,
    ) -> Result<OperatorResultType> {
        let gstate = gstate
            .as_any()
            .downcast_ref::<TableInOutGlobalState>()
            .ok_or_else(|| paro_error::internal("Invalid global state type".to_string()))?;

        let lstate = state
            .as_any_mut()
            .downcast_mut::<TableInOutLocalState>()
            .ok_or_else(|| paro_error::internal("Invalid local state type".to_string()))?;

        // Initialize local state on first call if needed
        if lstate.local_state.is_none() {
            if let Some(init_local) = self.bind_data.function.init_local {
                let init_input = TableFunctionInitInput::new(
                    self.bind_data.bind_data.as_ref().map(|b| b.as_ref()),
                    &self.bind_data.column_ids,
                );
                lstate.local_state = init_local(
                    &init_input,
                    gstate.global_state.as_ref().map(|s| s.as_ref()),
                )?;
            }
        }

        // Get the in_out_function
        let in_out_fn = self.bind_data.function.in_out_function.ok_or_else(|| {
            paro_error::internal("Table function has no in_out_function".to_string())
        })?;

        // Build the function input
        let mut func_input = TableFunctionInput {
            bind_data: self.bind_data.bind_data.as_ref().map(|b| b.as_ref()),
            local_state: lstate.local_state_mut(),
            global_state: gstate.global_state.as_ref().map(|s| s.as_ref()),
        };

        // Check if we need row-by-row processing
        if self.bind_data.projected_input.is_empty() {
            // Straightforward case - no need to project input
            let result = in_out_fn(&mut func_input, input, chunk)?;
            return match result {
                TableInOutResultType::NeedMoreInput => Ok(OperatorResultType::NeedMoreInput),
                TableInOutResultType::HaveMoreOutput => Ok(OperatorResultType::HaveMoreOutput),
                TableInOutResultType::Blocked => Ok(OperatorResultType::Blocked),
                TableInOutResultType::Finished => Ok(OperatorResultType::Finished),
            };
        }

        // Row-by-row processing with projected input
        // For now, just process the whole chunk at once
        let result = in_out_fn(&mut func_input, input, chunk)?;
        match result {
            TableInOutResultType::NeedMoreInput => Ok(OperatorResultType::NeedMoreInput),
            TableInOutResultType::HaveMoreOutput => Ok(OperatorResultType::HaveMoreOutput),
            TableInOutResultType::Blocked => Ok(OperatorResultType::Blocked),
            TableInOutResultType::Finished => Ok(OperatorResultType::Finished),
        }
    }

    fn final_execute(
        &self,
        _ctx: &ExecutionContext,
        chunk: &mut Chunk,
        gstate: &dyn GlobalOperatorState,
        state: &mut dyn OperatorState,
        _memory: crate::memory_runtime::OperatorMemoryScope<'_>,
    ) -> Result<ExecFinalizeResultType> {
        // Check if we have a final function
        let final_fn = match self.bind_data.function.in_out_function_final {
            Some(f) => f,
            None => return Ok(ExecFinalizeResultType::Finished),
        };

        let gstate = gstate
            .as_any()
            .downcast_ref::<TableInOutGlobalState>()
            .ok_or_else(|| paro_error::internal("Invalid global state type".to_string()))?;

        let lstate = state
            .as_any_mut()
            .downcast_mut::<TableInOutLocalState>()
            .ok_or_else(|| paro_error::internal("Invalid local state type".to_string()))?;

        // Build the function input
        let mut func_input = TableFunctionInput {
            bind_data: self.bind_data.bind_data.as_ref().map(|b| b.as_ref()),
            local_state: lstate.local_state_mut(),
            global_state: gstate.global_state.as_ref().map(|s| s.as_ref()),
        };

        let result = final_fn(&mut func_input, chunk)?;

        match result {
            OperatorFinalizeResultType::HaveMoreOutput => {
                Ok(ExecFinalizeResultType::HaveMoreOutput)
            }
            OperatorFinalizeResultType::Blocked => Ok(ExecFinalizeResultType::Blocked),
            OperatorFinalizeResultType::Finished => Ok(ExecFinalizeResultType::Finished),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
