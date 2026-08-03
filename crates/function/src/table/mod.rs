// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Table Function Module
//!
//!
//!
//! ## Dependencies Check
//! - Chunk: ✅ `paro_common::chunk`
//! - LogicalType: ✅ `paro_common::types`
//!
//! ## Overview
//! Table functions are functions that return a table (set of rows).
//! Examples: `generate_series`, `range`, `unnest`, `read_csv`, etc.
//!
//! ## Key Components
//! - `TableFunction`: The function definition
//! - `TableFunctionSet`: Set of overloaded table functions
//! - `GlobalTableFunctionState`: Global state shared across threads
//! - `LocalTableFunctionState`: Thread-local state
//! - `TableFunctionBindData`: Data returned from bind phase
//!
//! - `GlobalTableFunctionState`: Initialized once per query via `init_global`
//!   - Provides `max_threads()` for parallelism control
//!   - Provides `get_progress()` for progress reporting
//! - `LocalTableFunctionState`: Initialized once per thread via `init_local`
//!   - Thread-local caching and state
//!
//! ## Built-in Table Functions
//!

pub mod range;
pub mod read_csv;
pub mod read_ndjson;
pub mod repeat;
pub mod system;
pub mod unnest;

use std::any::Any;
use std::fmt;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_storage::buffer::BufferManager;

// Re-export FunctionData from scalar module
pub use crate::scalar::FunctionData;

// ============================================================================
// Table Function State Traits
// ============================================================================

/// Global state for table function execution.
///
/// This state is shared across all threads working on the table function.
/// It is initialized once per query execution via `init_global`.
///
/// ## Thread Safety
/// Implementations must be `Send + Sync` as the state is shared across threads.
/// Internal synchronization (e.g., `Mutex`, `AtomicXxx`) should be used for
/// mutable state that needs to be accessed concurrently.
pub trait GlobalTableFunctionState: Send + Sync {
    /// Returns the maximum number of threads that can work on this function.
    ///
    /// Return `MAX_THREADS` to indicate no limit (use all available threads).
    /// Return `1` for single-threaded execution (default).
    ///
    /// This is called by the executor to determine parallelism.
    fn max_threads(&self) -> usize {
        1
    }

    /// Returns the scan progress as a percentage (0.0 to 100.0).
    ///
    /// Return a negative value to indicate progress is not available.
    /// This is used for progress bar display.
    fn get_progress(&self) -> f64 {
        -1.0 // Not available by default
    }

    /// Downcast to concrete type.
    fn as_any(&self) -> &dyn Any;

    /// Downcast to mutable concrete type.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Local state for table function execution.
///
/// This state is thread-local and is initialized once per thread via `init_local`.
/// Each worker thread gets its own `LocalTableFunctionState` instance.
///
/// ## Use Cases
/// - Thread-local buffers to avoid allocation per call
/// - Thread-local position tracking for parallel scans
/// - Thread-local caching
pub trait LocalTableFunctionState: Send + Sync {
    /// Downcast to concrete type.
    fn as_any(&self) -> &dyn Any;

    /// Downcast to mutable concrete type.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Maximum threads constant (use when there's no limit).
pub const MAX_THREADS: usize = 999999999;

/// Query-scoped services available while a table function initializes.
///
/// Runtime-owned resources must enter table functions through this context. Keeping the
/// interface query-scoped prevents process globals from mixing resources across independent
/// database instances and avoids extending resource lifetimes beyond the query that uses them.
pub trait TableFunctionRuntimeContext: Send + Sync {
    /// Return the buffer manager owned by the current database instance, when available.
    fn buffer_manager(&self) -> Option<&dyn BufferManager> {
        None
    }
}

#[cfg(test)]
struct EmptyTableFunctionRuntimeContext;

#[cfg(test)]
impl TableFunctionRuntimeContext for EmptyTableFunctionRuntimeContext {}

#[cfg(test)]
static EMPTY_TABLE_FUNCTION_RUNTIME_CONTEXT: EmptyTableFunctionRuntimeContext =
    EmptyTableFunctionRuntimeContext;

// ============================================================================
// Table Function Bind Data
// ============================================================================

/// Data returned from the bind phase of a table function.
///
/// This contains information determined at bind time, such as:
/// - Return column types and names
/// - Estimated cardinality
/// - Any function-specific bind data
pub trait TableFunctionBindData: Send + Sync {
    /// Clone the bind data.
    fn clone_box(&self) -> Box<dyn TableFunctionBindData>;

    /// Downcast to concrete type.
    fn as_any(&self) -> &dyn Any;

    /// Get estimated cardinality (number of rows).
    /// Returns None if unknown.
    fn cardinality(&self) -> Option<usize> {
        None
    }
}

// ============================================================================
// Table Function Input/Output Types
// ============================================================================

/// Input for the bind phase.
pub struct TableFunctionBindInput<'a> {
    /// Input values (constants passed to the function).
    pub inputs: &'a [Value],
    /// Named parameters.
    pub named_parameters: &'a std::collections::HashMap<String, Value>,
    /// Input table types (for table-in-out functions).
    /// Empty for standard table functions.
    pub input_table_types: &'a [LogicalType],
    /// Input table column names (for table-in-out functions).
    /// Empty for standard table functions.
    pub input_table_names: &'a [String],
}

impl<'a> TableFunctionBindInput<'a> {
    /// Create a new bind input for standard table functions.
    pub fn new(
        inputs: &'a [Value],
        named_parameters: &'a std::collections::HashMap<String, Value>,
    ) -> Self {
        Self {
            inputs,
            named_parameters,
            input_table_types: &[],
            input_table_names: &[],
        }
    }

    /// Create a new bind input for table-in-out functions.
    pub fn with_input_table(
        inputs: &'a [Value],
        named_parameters: &'a std::collections::HashMap<String, Value>,
        input_table_types: &'a [LogicalType],
        input_table_names: &'a [String],
    ) -> Self {
        Self {
            inputs,
            named_parameters,
            input_table_types,
            input_table_names,
        }
    }
}

/// Input for the init_global phase.
///
/// Contains all information needed to initialize global state.
pub struct TableFunctionInitInput<'a> {
    /// Query-scoped runtime services supplied by the executor.
    pub runtime: &'a dyn TableFunctionRuntimeContext,
    /// Bind data from the bind phase.
    pub bind_data: Option<&'a dyn TableFunctionBindData>,
    /// Column IDs to scan (for projection pushdown).
    /// Empty means scan all columns.
    pub column_ids: &'a [usize],
    /// Maximum number of threads hint from the executor.
    /// The function can use this to pre-allocate resources.
    pub max_threads_hint: usize,
}

impl<'a> TableFunctionInitInput<'a> {
    /// Create a new init input.
    pub fn new(
        runtime: &'a dyn TableFunctionRuntimeContext,
        bind_data: Option<&'a dyn TableFunctionBindData>,
        column_ids: &'a [usize],
    ) -> Self {
        Self {
            runtime,
            bind_data,
            column_ids,
            max_threads_hint: 1,
        }
    }

    /// Create a context-free input for table-function unit tests.
    #[cfg(test)]
    pub fn new_for_test(
        bind_data: Option<&'a dyn TableFunctionBindData>,
        column_ids: &'a [usize],
    ) -> Self {
        Self::new(&EMPTY_TABLE_FUNCTION_RUNTIME_CONTEXT, bind_data, column_ids)
    }

    /// Resolve the instance-owned buffer manager required by memory system functions.
    pub fn buffer_manager(&self) -> Result<&dyn BufferManager> {
        self.runtime.buffer_manager().ok_or_else(|| {
            paro_error::internal(
                "table function requires an instance-scoped buffer manager".to_string(),
            )
        })
    }

    /// Create init input with max threads hint.
    pub fn with_max_threads(mut self, max_threads: usize) -> Self {
        self.max_threads_hint = max_threads;
        self
    }
}

/// Input for the main function execution.
pub struct TableFunctionInput<'a> {
    /// Bind data from the bind phase.
    pub bind_data: Option<&'a dyn TableFunctionBindData>,
    /// Local state for this thread.
    pub local_state: Option<&'a mut dyn LocalTableFunctionState>,
    /// Global state shared across threads.
    pub global_state: Option<&'a dyn GlobalTableFunctionState>,
}

/// Result type for table function execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFunctionResult {
    /// More output is available, call function again.
    HaveMoreOutput,
    /// No more output, function is done.
    Finished,
}

/// Result type for table-in-out function execution.
///
/// Table-in-out functions process input chunks and produce output chunks.
/// This enum indicates the state after processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorResultType {
    /// Need more input data to continue processing.
    NeedMoreInput,
    /// Have more output to produce from current input.
    HaveMoreOutput,
    /// Processing is blocked (e.g., waiting for I/O).
    Blocked,
    /// Processing is finished.
    Finished,
}

/// Result type for table-in-out function finalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorFinalizeResultType {
    /// Have more output to produce.
    HaveMoreOutput,
    /// Finalization is currently blocked.
    Blocked,
    /// Finalization is finished.
    Finished,
}

// ============================================================================
// Table Function Callbacks
// ============================================================================

/// Bind function type.
///
/// Called during query planning to:
/// 1. Validate inputs
/// 2. Determine return types and column names
/// 3. Create bind data
///
/// # Arguments
/// * `input` - Bind input containing function arguments
/// * `return_types` - Output: column types to return
/// * `names` - Output: column names to return
///
/// # Returns
/// Bind data or error
pub type TableFunctionBindFn = fn(
    input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>>;

/// Global init function type.
///
/// Called once per query to initialize global state.
pub type TableFunctionInitGlobalFn =
    fn(input: &TableFunctionInitInput) -> Result<Option<Box<dyn GlobalTableFunctionState>>>;

/// Local init function type.
///
/// Called once per thread to initialize local state.
pub type TableFunctionInitLocalFn = fn(
    input: &TableFunctionInitInput,
    global_state: Option<&dyn GlobalTableFunctionState>,
) -> Result<Option<Box<dyn LocalTableFunctionState>>>;

/// Main table function type.
///
/// Called repeatedly to produce output chunks.
///
/// # Arguments
/// * `input` - Function input with bind data and states
/// * `output` - Output chunk to fill
///
/// # Returns
/// Result indicating if more output is available
pub type TableFunctionFn =
    fn(input: &mut TableFunctionInput, output: &mut Chunk) -> Result<TableFunctionResult>;

/// Cardinality estimation function type.
///
/// Returns estimated number of rows.
pub type TableFunctionCardinalityFn =
    fn(bind_data: Option<&dyn TableFunctionBindData>) -> Option<usize>;

/// Progress reporting function type.
///
/// Returns scan progress as a percentage (0.0 to 100.0).
/// Return a negative value if progress is not available.
///
/// # Arguments
/// * `bind_data` - Bind data from the bind phase
/// * `global_state` - Global state (may contain progress tracking)
pub type TableFunctionProgressFn = fn(
    bind_data: Option<&dyn TableFunctionBindData>,
    global_state: Option<&dyn GlobalTableFunctionState>,
) -> f64;

/// Table-in-out function type.
///
/// Called repeatedly to process input chunks and produce output chunks.
/// This is used for functions that transform input data row-by-row.
///
/// # Arguments
/// * `input` - Function input with bind data and states
/// * `input_chunk` - Input data chunk to process
/// * `output` - Output chunk to fill
///
/// # Returns
/// Result indicating the processing state
pub type TableInOutFunctionFn = fn(
    input: &mut TableFunctionInput,
    input_chunk: &Chunk,
    output: &mut Chunk,
) -> Result<OperatorResultType>;

/// Table-in-out function finalization type.
///
/// Called after all input has been processed to produce any remaining output.
///
/// # Arguments
/// * `input` - Function input with bind data and states
/// * `output` - Output chunk to fill
///
/// # Returns
/// Result indicating if more output is available
pub type TableInOutFunctionFinalFn =
    fn(input: &mut TableFunctionInput, output: &mut Chunk) -> Result<OperatorFinalizeResultType>;

// ============================================================================
// TableFunction
// ============================================================================

/// Definition of a table function.
///
/// Table functions return a table (set of rows) and can be used in FROM clauses.
///
/// # Example
/// ```sql
/// SELECT * FROM generate_series(1, 10);
/// SELECT * FROM range(0, 100, 5);
/// ```
///
/// ## State Management
/// Table functions use a two-level state model:
/// - `GlobalTableFunctionState`: Shared across all threads, initialized via `init_global`
/// - `LocalTableFunctionState`: Thread-local, initialized via `init_local`
///
/// ## Execution Flow
/// 1. `bind()` - Called during planning to determine return types
/// 2. `init_global()` - Called once to create global state
/// 3. `init_local()` - Called per thread to create local state
/// 4. `function()` - Called repeatedly to produce output chunks
/// 5. (Optional) `table_scan_progress()` - Called to report progress
///
/// Table-in-out functions process input data and produce output:
/// - Use `in_out_function` instead of `function`
/// - Receive input Chunk and produce output Chunk
/// - Optional `in_out_function_final` for finalization
#[derive(Clone)]
pub struct TableFunction {
    /// Function name.
    pub name: String,

    /// Argument types.
    pub arguments: Vec<LogicalType>,

    /// Bind function (required).
    pub bind: Option<TableFunctionBindFn>,

    /// Global init function (optional).
    pub init_global: Option<TableFunctionInitGlobalFn>,

    /// Local init function (optional).
    pub init_local: Option<TableFunctionInitLocalFn>,

    /// Main function (required for standard table functions).
    pub function: Option<TableFunctionFn>,

    /// Table-in-out function (for functions that process input data).
    /// Mutually exclusive with `function`.
    pub in_out_function: Option<TableInOutFunctionFn>,

    /// Table-in-out finalization function (optional).
    /// Called after all input has been processed.
    pub in_out_function_final: Option<TableInOutFunctionFinalFn>,

    /// Cardinality estimation function (optional).
    pub cardinality: Option<TableFunctionCardinalityFn>,

    /// Progress reporting function (optional).
    /// Returns scan progress as a percentage (0.0 to 100.0).
    pub table_scan_progress: Option<TableFunctionProgressFn>,

    /// Whether the function supports projection pushdown.
    pub projection_pushdown: bool,

    /// Whether the function supports filter pushdown.
    pub filter_pushdown: bool,

    /// Type for variable arguments (None = no varargs).
    pub varargs: Option<LogicalType>,

    /// Named parameters accepted by this function.
    pub named_parameters: Vec<(String, LogicalType)>,
}

impl fmt::Debug for TableFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableFunction")
            .field("name", &self.name)
            .field("arguments", &self.arguments)
            .field("has_bind", &self.bind.is_some())
            .field("has_init_global", &self.init_global.is_some())
            .field("has_init_local", &self.init_local.is_some())
            .field("has_function", &self.function.is_some())
            .field("has_in_out_function", &self.in_out_function.is_some())
            .field(
                "has_in_out_function_final",
                &self.in_out_function_final.is_some(),
            )
            .field("has_progress", &self.table_scan_progress.is_some())
            .field("projection_pushdown", &self.projection_pushdown)
            .field("filter_pushdown", &self.filter_pushdown)
            .field("varargs", &self.varargs)
            .finish()
    }
}

impl TableFunction {
    /// Create a new table function.
    pub fn new(name: impl Into<String>, arguments: Vec<LogicalType>) -> Self {
        Self {
            name: name.into(),
            arguments,
            bind: None,
            init_global: None,
            init_local: None,
            function: None,
            in_out_function: None,
            in_out_function_final: None,
            cardinality: None,
            table_scan_progress: None,
            projection_pushdown: false,
            filter_pushdown: false,
            varargs: None,
            named_parameters: Vec::new(),
        }
    }

    /// Set the bind function.
    pub fn with_bind(mut self, bind: TableFunctionBindFn) -> Self {
        self.bind = Some(bind);
        self
    }

    /// Set the global init function.
    pub fn with_init_global(mut self, init_global: TableFunctionInitGlobalFn) -> Self {
        self.init_global = Some(init_global);
        self
    }

    /// Set the local init function.
    pub fn with_init_local(mut self, init_local: TableFunctionInitLocalFn) -> Self {
        self.init_local = Some(init_local);
        self
    }

    /// Set the main function.
    pub fn with_function(mut self, function: TableFunctionFn) -> Self {
        self.function = Some(function);
        self
    }

    /// Set the cardinality function.
    pub fn with_cardinality(mut self, cardinality: TableFunctionCardinalityFn) -> Self {
        self.cardinality = Some(cardinality);
        self
    }

    /// Set the progress reporting function.
    pub fn with_progress(mut self, progress: TableFunctionProgressFn) -> Self {
        self.table_scan_progress = Some(progress);
        self
    }

    /// Enable projection pushdown.
    pub fn with_projection_pushdown(mut self) -> Self {
        self.projection_pushdown = true;
        self
    }

    /// Enable filter pushdown.
    pub fn with_filter_pushdown(mut self) -> Self {
        self.filter_pushdown = true;
        self
    }

    /// Set varargs type.
    pub fn with_varargs(mut self, varargs_type: LogicalType) -> Self {
        self.varargs = Some(varargs_type);
        self
    }

    /// Add a named parameter.
    pub fn with_named_parameter(
        mut self,
        name: impl Into<String>,
        param_type: LogicalType,
    ) -> Self {
        self.named_parameters.push((name.into(), param_type));
        self
    }

    /// Set the table-in-out function.
    pub fn with_in_out_function(mut self, in_out_function: TableInOutFunctionFn) -> Self {
        self.in_out_function = Some(in_out_function);
        self
    }

    /// Set the table-in-out finalization function.
    pub fn with_in_out_function_final(
        mut self,
        in_out_function_final: TableInOutFunctionFinalFn,
    ) -> Self {
        self.in_out_function_final = Some(in_out_function_final);
        self
    }

    /// Check if this function accepts variable arguments.
    pub fn has_varargs(&self) -> bool {
        self.varargs.is_some()
    }

    /// Check if this is a table-in-out function.
    pub fn is_in_out_function(&self) -> bool {
        self.in_out_function.is_some()
    }
}

// ============================================================================
// TableFunctionSet
// ============================================================================

/// A set of table functions with the same name but different signatures.
#[derive(Clone, Debug)]
pub struct TableFunctionSet {
    /// Function name.
    pub name: String,
    /// Function overloads.
    pub functions: Vec<TableFunction>,
}

impl TableFunctionSet {
    /// Create a new table function set.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            functions: Vec::new(),
        }
    }

    /// Add a function to the set.
    pub fn add_function(&mut self, function: TableFunction) {
        self.functions.push(function);
    }

    /// Find the best matching function for the given arguments.
    ///
    /// # Algorithm
    /// 1. Find all candidate functions with matching argument count (or varargs)
    /// 2. For each candidate, calculate total cast cost
    /// 3. Select the candidate with lowest total cost
    pub fn bind(&self, arguments: &[LogicalType]) -> Result<&TableFunction> {
        self.bind_with_types(arguments).map(|(f, _)| f)
    }

    /// Find the best matching function for the given arguments and return its parameter types.
    pub fn bind_with_types(
        &self,
        arguments: &[LogicalType],
    ) -> Result<(&TableFunction, Vec<LogicalType>)> {
        use paro_common::cast_rules::CastRules;

        let mut best_match: Option<(&TableFunction, i64, Vec<LogicalType>)> = None;

        for func in &self.functions {
            let fixed_count = func.arguments.len();
            let arg_count = arguments.len();

            // Check argument count compatibility
            if func.has_varargs() {
                if arg_count < fixed_count {
                    continue;
                }
            } else if arg_count != fixed_count {
                continue;
            }

            // Calculate total cast cost
            let mut total_cost: i64 = 0;
            let mut valid = true;
            let mut target_types = Vec::with_capacity(arg_count);

            // Process fixed arguments
            for (arg_type, param_type) in arguments.iter().take(fixed_count).zip(&func.arguments) {
                let cost = CastRules::implicit_cast_cost(arg_type, param_type);
                if cost < 0 {
                    valid = false;
                    break;
                }
                total_cost += cost;
                target_types.push(param_type.clone());
            }

            if !valid {
                continue;
            }

            // Process varargs
            if let Some(ref varargs_type) = func.varargs {
                for arg_type in arguments.iter().skip(fixed_count) {
                    // Special case: Unknown varargs type accepts any argument type
                    if *varargs_type == LogicalType::Unknown {
                        target_types.push(arg_type.clone());
                        continue;
                    }
                    let cost = CastRules::implicit_cast_cost(arg_type, varargs_type);
                    if cost < 0 {
                        valid = false;
                        break;
                    }
                    total_cost += cost;
                    target_types.push(varargs_type.clone());
                }
            }

            if !valid {
                continue;
            }

            // Update best match
            match &best_match {
                None => {
                    best_match = Some((func, total_cost, target_types));
                }
                Some((_, best_cost, _)) if total_cost < *best_cost => {
                    best_match = Some((func, total_cost, target_types));
                }
                _ => {}
            }
        }

        match best_match {
            Some((func, _, target_types)) => Ok((func, target_types)),
            None => Err(paro_common::error::catalog(format!(
                "No matching table function found for {} with arguments {:?}",
                self.name, arguments
            ))),
        }
    }
}

// ============================================================================
// Helper implementations
// ============================================================================

/// Empty global state (for functions that don't need global state).
pub struct EmptyGlobalState;

impl GlobalTableFunctionState for EmptyGlobalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Empty local state (for functions that don't need local state).
pub struct EmptyLocalState;

impl LocalTableFunctionState for EmptyLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Empty bind data (for functions that don't need bind data).
#[derive(Clone)]
pub struct EmptyBindData;

impl TableFunctionBindData for EmptyBindData {
    fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_function_builder() {
        let func = TableFunction::new("test_func", vec![LogicalType::BigInt])
            .with_projection_pushdown()
            .with_filter_pushdown();

        assert_eq!(func.name, "test_func");
        assert_eq!(func.arguments.len(), 1);
        assert!(func.projection_pushdown);
        assert!(func.filter_pushdown);
    }

    #[test]
    fn test_table_function_varargs() {
        let func = TableFunction::new("varargs_func", vec![LogicalType::BigInt])
            .with_varargs(LogicalType::BigInt);

        assert!(func.has_varargs());
        assert_eq!(func.varargs, Some(LogicalType::BigInt));
    }

    #[test]
    fn test_table_function_set_bind_exact() {
        let mut set = TableFunctionSet::new("range");

        // range(end)
        set.add_function(TableFunction::new("range", vec![LogicalType::BigInt]));

        // range(start, end)
        set.add_function(TableFunction::new(
            "range",
            vec![LogicalType::BigInt, LogicalType::BigInt],
        ));

        // range(start, end, step)
        set.add_function(TableFunction::new(
            "range",
            vec![
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
            ],
        ));

        // Test binding with 1 argument
        let func = set.bind(&[LogicalType::BigInt]).unwrap();
        assert_eq!(func.arguments.len(), 1);

        // Test binding with 2 arguments
        let func = set
            .bind(&[LogicalType::BigInt, LogicalType::BigInt])
            .unwrap();
        assert_eq!(func.arguments.len(), 2);

        // Test binding with 3 arguments
        let func = set
            .bind(&[
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
            ])
            .unwrap();
        assert_eq!(func.arguments.len(), 3);
    }

    #[test]
    fn test_table_function_set_bind_implicit_cast() {
        let mut set = TableFunctionSet::new("range");

        // range(BIGINT)
        set.add_function(TableFunction::new("range", vec![LogicalType::BigInt]));

        // Bind with INTEGER - should cast to BIGINT
        let func = set.bind(&[LogicalType::Integer]).unwrap();
        assert_eq!(func.arguments[0], LogicalType::BigInt);
    }

    #[test]
    fn test_table_function_set_bind_no_match() {
        let mut set = TableFunctionSet::new("range");

        // range(BIGINT)
        set.add_function(TableFunction::new("range", vec![LogicalType::BigInt]));

        // Bind with VARCHAR - should fail
        let result = set.bind(&[LogicalType::Varchar]);
        assert!(result.is_err());

        // Bind with wrong argument count - should fail
        let result = set.bind(&[LogicalType::BigInt, LogicalType::BigInt]);
        assert!(result.is_err());
    }

    #[test]
    fn test_table_function_result() {
        assert_eq!(
            TableFunctionResult::HaveMoreOutput,
            TableFunctionResult::HaveMoreOutput
        );
        assert_ne!(
            TableFunctionResult::HaveMoreOutput,
            TableFunctionResult::Finished
        );
    }

    #[test]
    fn test_empty_states() {
        let global = EmptyGlobalState;
        assert_eq!(global.max_threads(), 1);

        let local = EmptyLocalState;
        let _ = local.as_any();

        let bind = EmptyBindData;
        assert!(bind.cardinality().is_none());
    }

    #[test]
    fn test_table_function_named_parameters() {
        let func = TableFunction::new("read_csv", vec![LogicalType::Varchar])
            .with_named_parameter("header", LogicalType::Boolean)
            .with_named_parameter("delimiter", LogicalType::Varchar);

        assert_eq!(func.named_parameters.len(), 2);
        assert_eq!(func.named_parameters[0].0, "header");
        assert_eq!(func.named_parameters[1].0, "delimiter");
    }

    #[test]
    fn test_table_function_in_out_builder() {
        // Create a simple in-out function
        fn dummy_in_out(
            _input: &mut TableFunctionInput,
            _input_chunk: &Chunk,
            _output: &mut Chunk,
        ) -> Result<OperatorResultType> {
            Ok(OperatorResultType::Finished)
        }

        fn dummy_in_out_final(
            _input: &mut TableFunctionInput,
            _output: &mut Chunk,
        ) -> Result<OperatorFinalizeResultType> {
            Ok(OperatorFinalizeResultType::Finished)
        }

        let func = TableFunction::new("test_in_out", vec![LogicalType::BigInt])
            .with_in_out_function(dummy_in_out)
            .with_in_out_function_final(dummy_in_out_final);

        assert!(func.is_in_out_function());
        assert!(func.in_out_function.is_some());
        assert!(func.in_out_function_final.is_some());
        assert!(func.function.is_none()); // Standard function should be None
    }

    #[test]
    fn test_table_function_is_in_out_function() {
        // Standard function
        let standard_func = TableFunction::new("standard", vec![LogicalType::BigInt]);
        assert!(!standard_func.is_in_out_function());

        // In-out function
        fn dummy_in_out(
            _input: &mut TableFunctionInput,
            _input_chunk: &Chunk,
            _output: &mut Chunk,
        ) -> Result<OperatorResultType> {
            Ok(OperatorResultType::NeedMoreInput)
        }

        let in_out_func = TableFunction::new("in_out", vec![LogicalType::BigInt])
            .with_in_out_function(dummy_in_out);
        assert!(in_out_func.is_in_out_function());
    }

    #[test]
    fn test_operator_result_type() {
        assert_eq!(
            OperatorResultType::NeedMoreInput,
            OperatorResultType::NeedMoreInput
        );
        assert_ne!(
            OperatorResultType::NeedMoreInput,
            OperatorResultType::HaveMoreOutput
        );
        assert_ne!(
            OperatorResultType::HaveMoreOutput,
            OperatorResultType::Finished
        );
        assert_ne!(OperatorResultType::Blocked, OperatorResultType::Finished);
    }

    #[test]
    fn test_operator_finalize_result_type() {
        assert_eq!(
            OperatorFinalizeResultType::HaveMoreOutput,
            OperatorFinalizeResultType::HaveMoreOutput
        );
        assert_ne!(
            OperatorFinalizeResultType::HaveMoreOutput,
            OperatorFinalizeResultType::Blocked
        );
        assert_ne!(
            OperatorFinalizeResultType::Blocked,
            OperatorFinalizeResultType::Finished
        );
    }

    #[test]
    fn test_table_function_bind_input_with_input_table() {
        use std::collections::HashMap;

        let inputs = vec![Value::BigInt(42)];
        let named_params = HashMap::new();
        let input_table_types = vec![LogicalType::Integer, LogicalType::Varchar];
        let input_table_names = vec!["col1".to_string(), "col2".to_string()];

        let bind_input = TableFunctionBindInput {
            inputs: &inputs,
            named_parameters: &named_params,
            input_table_types: &input_table_types,
            input_table_names: &input_table_names,
        };

        assert_eq!(bind_input.inputs.len(), 1);
        assert_eq!(bind_input.input_table_types.len(), 2);
        assert_eq!(bind_input.input_table_names.len(), 2);
        assert_eq!(bind_input.input_table_types[0], LogicalType::Integer);
        assert_eq!(bind_input.input_table_names[0], "col1");
    }
}
