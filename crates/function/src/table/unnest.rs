//! Unnest Table Function
//!
//!
//!
//! ## Dependencies Check
//! - TableFunction: ✅ `crate::table`
//! - Chunk: ✅ `paro_common::chunk`
//! - Vector: ✅ `paro_common::vector`
//! - Value::List: ✅ `paro_common::runtime_value::Value`
//!
//! ## Overview
//! Implements the `unnest` table function that expands arrays/lists into rows.
//!
//! ## Supported Variants
//! - `unnest(list)` - Expand a single list into rows
//!
//! ## Known Limitations
//! - Does not support multi-column unnest (e.g., `unnest([1,2], ['a','b'])`)
//! - NULL elements in the list are preserved as NULL rows
//!
//! ## Example
//! ```sql
//! SELECT * FROM unnest([1, 2, 3]);
//! -- Returns:
//! -- unnest
//! -- ------
//! -- 1
//! -- 2
//! -- 3
//! ```

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;

use super::{
    GlobalTableFunctionState, LocalTableFunctionState, TableFunction, TableFunctionBindData,
    TableFunctionBindInput, TableFunctionInitInput, TableFunctionInput, TableFunctionResult,
    TableFunctionSet,
};

// ============================================================================
// Bind Data
// ============================================================================

/// Bind data for unnest function.
#[derive(Clone, Debug)]
pub struct UnnestBindData {
    /// The list values to unnest.
    pub values: Vec<Value>,
    /// Element type of the list.
    pub element_type: LogicalType,
    /// Total number of elements.
    pub cardinality: usize,
}

impl UnnestBindData {
    /// Create new bind data from a list value.
    pub fn new(list_value: &Value) -> Result<Self> {
        match list_value {
            Value::List(values, elem_type) => Ok(Self {
                values: values.clone(),
                element_type: elem_type.clone(),
                cardinality: values.len(),
            }),
            Value::Null(LogicalType::List(elem_type)) => Ok(Self {
                values: Vec::new(),
                element_type: elem_type.as_ref().clone(),
                cardinality: 0,
            }),
            _ => Err(paro_error::type_mismatch(format!(
                "unnest requires a list argument, got {}",
                list_value.logical_type()
            ))),
        }
    }
}

impl TableFunctionBindData for UnnestBindData {
    fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn cardinality(&self) -> Option<usize> {
        Some(self.cardinality)
    }
}

// ============================================================================
// Global State
// ============================================================================

/// Global state for unnest execution.
pub struct UnnestGlobalState {
    /// Current index in the list (atomic for thread safety).
    current_idx: AtomicUsize,
    /// Total number of elements.
    total_count: usize,
}

impl UnnestGlobalState {
    /// Create new global state.
    pub fn new(total_count: usize) -> Self {
        Self {
            current_idx: AtomicUsize::new(0),
            total_count,
        }
    }

    /// Get the next batch of indices to process.
    pub fn get_next_batch(&self, batch_size: usize) -> Option<(usize, usize)> {
        loop {
            let current = self.current_idx.load(Ordering::Relaxed);
            if current >= self.total_count {
                return None;
            }

            let remaining = self.total_count - current;
            let count = remaining.min(batch_size);
            let new_idx = current + count;

            if self
                .current_idx
                .compare_exchange(current, new_idx, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return Some((current, count));
            }
        }
    }
}

impl GlobalTableFunctionState for UnnestGlobalState {
    fn max_threads(&self) -> usize {
        // Single-threaded for simplicity
        1
    }

    fn get_progress(&self) -> f64 {
        if self.total_count == 0 {
            return 100.0;
        }
        let current = self.current_idx.load(Ordering::Relaxed);
        (current as f64 / self.total_count as f64) * 100.0
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// ============================================================================
// Local State
// ============================================================================

/// Local state for unnest execution.
pub struct UnnestLocalState {
    /// Whether this local state has finished.
    finished: bool,
}

impl UnnestLocalState {
    /// Create new local state.
    pub fn new() -> Self {
        Self { finished: false }
    }
}

impl Default for UnnestLocalState {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalTableFunctionState for UnnestLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// ============================================================================
// Bind Function
// ============================================================================

/// Bind function for unnest.
fn unnest_bind(
    input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    if input.inputs.is_empty() {
        return Err(paro_error::syntax("unnest requires at least one argument"));
    }

    let list_value = &input.inputs[0];

    // Handle NULL input
    if let Value::Null(t) = list_value {
        let elem_type = match t {
            LogicalType::List(elem) => elem.as_ref().clone(),
            _ => LogicalType::Unknown,
        };
        return_types.push(elem_type.clone());
        names.push("unnest".to_string());
        return Ok(Some(Box::new(UnnestBindData {
            values: Vec::new(),
            element_type: elem_type,
            cardinality: 0,
        })));
    }

    let bind_data = UnnestBindData::new(list_value)?;
    return_types.push(bind_data.element_type.clone());
    names.push("unnest".to_string());

    Ok(Some(Box::new(bind_data)))
}

// ============================================================================
// Init Functions
// ============================================================================

/// Initialize global state.
fn unnest_init_global(
    input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    let bind_data = input
        .bind_data
        .and_then(|bd| bd.as_any().downcast_ref::<UnnestBindData>());

    let total_count = bind_data.map(|bd| bd.cardinality).unwrap_or(0);
    Ok(Some(Box::new(UnnestGlobalState::new(total_count))))
}

/// Initialize local state.
fn unnest_init_local(
    _input: &TableFunctionInitInput,
    _global_state: Option<&dyn GlobalTableFunctionState>,
) -> Result<Option<Box<dyn LocalTableFunctionState>>> {
    Ok(Some(Box::new(UnnestLocalState::new())))
}

// ============================================================================
// Main Function
// ============================================================================

/// Main execution function for unnest.
fn unnest_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let bind_data = input
        .bind_data
        .and_then(|bd| bd.as_any().downcast_ref::<UnnestBindData>());

    let gstate = input
        .global_state
        .and_then(|gs| gs.as_any().downcast_ref::<UnnestGlobalState>());

    let lstate = input
        .local_state
        .as_mut()
        .and_then(|ls| ls.as_any_mut().downcast_mut::<UnnestLocalState>());

    let bind_data = match bind_data {
        Some(bd) => bd,
        None => {
            output.set_cardinality(0);
            return Ok(TableFunctionResult::Finished);
        }
    };

    let gstate = match gstate {
        Some(gs) => gs,
        None => {
            output.set_cardinality(0);
            return Ok(TableFunctionResult::Finished);
        }
    };

    let lstate = match lstate {
        Some(ls) => ls,
        None => {
            output.set_cardinality(0);
            return Ok(TableFunctionResult::Finished);
        }
    };

    // Check if already finished
    if lstate.finished {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    }

    // Get next batch
    let batch = gstate.get_next_batch(VECTOR_SIZE);

    match batch {
        Some((start_idx, count)) => {
            // Fill the output vector with values from the list
            if let Some(col) = output.column_mut(0) {
                for i in 0..count {
                    let value = &bind_data.values[start_idx + i];
                    col.set_value(i, value);
                }
            }
            output.set_cardinality(count);

            // Check if there's more data
            let current = gstate.current_idx.load(Ordering::Relaxed);
            if current >= gstate.total_count {
                lstate.finished = true;
            }

            Ok(TableFunctionResult::HaveMoreOutput)
        }
        None => {
            lstate.finished = true;
            output.set_cardinality(0);
            Ok(TableFunctionResult::Finished)
        }
    }
}

// ============================================================================
// Cardinality Function
// ============================================================================

/// Cardinality estimation function.
fn unnest_cardinality(bind_data: Option<&dyn TableFunctionBindData>) -> Option<usize> {
    bind_data
        .and_then(|bd| bd.as_any().downcast_ref::<UnnestBindData>())
        .map(|bd| bd.cardinality)
}

// ============================================================================
// Progress Function
// ============================================================================

/// Progress reporting function.
fn unnest_progress(
    _bind_data: Option<&dyn TableFunctionBindData>,
    global_state: Option<&dyn GlobalTableFunctionState>,
) -> f64 {
    global_state.map(|gs| gs.get_progress()).unwrap_or(-1.0)
}

// ============================================================================
// TableFunctionSet Creation
// ============================================================================

/// Create the unnest table function set.
///
/// Includes:
/// - `unnest(list)` - Expand a list into rows
pub fn create_unnest_function_set() -> TableFunctionSet {
    let mut set = TableFunctionSet::new("unnest");

    // unnest(LIST<ANY>)
    // We use varargs with ANY type to accept any list type
    let func = TableFunction::new("unnest", vec![])
        .with_varargs(LogicalType::Unknown) // Accept any type
        .with_bind(unnest_bind)
        .with_init_global(unnest_init_global)
        .with_init_local(unnest_init_local)
        .with_function(unnest_function)
        .with_cardinality(unnest_cardinality)
        .with_progress(unnest_progress);
    set.add_function(func);

    set
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_bind_input(values: Vec<Value>) -> (Vec<Value>, HashMap<String, Value>) {
        (values, HashMap::new())
    }

    // ========== Bind Data Tests ==========

    #[test]
    fn test_unnest_bind_data_from_list() {
        let list = Value::List(
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
            LogicalType::Integer,
        );
        let bd = UnnestBindData::new(&list).unwrap();

        assert_eq!(bd.cardinality, 3);
        assert_eq!(bd.element_type, LogicalType::Integer);
        assert_eq!(bd.values.len(), 3);
    }

    #[test]
    fn test_unnest_bind_data_empty_list() {
        let list = Value::List(vec![], LogicalType::Integer);
        let bd = UnnestBindData::new(&list).unwrap();

        assert_eq!(bd.cardinality, 0);
        assert_eq!(bd.element_type, LogicalType::Integer);
    }

    #[test]
    fn test_unnest_bind_data_null_list() {
        let null_list = Value::Null(LogicalType::List(Box::new(LogicalType::Varchar)));
        let bd = UnnestBindData::new(&null_list).unwrap();

        assert_eq!(bd.cardinality, 0);
        assert_eq!(bd.element_type, LogicalType::Varchar);
    }

    #[test]
    fn test_unnest_bind_data_non_list_error() {
        let non_list = Value::Integer(42);
        let result = UnnestBindData::new(&non_list);

        assert!(result.is_err());
    }

    // ========== Bind Function Tests ==========

    #[test]
    fn test_unnest_bind_integer_list() {
        let list = Value::List(
            vec![Value::Integer(1), Value::Integer(2)],
            LogicalType::Integer,
        );
        let (values, named) = create_bind_input(vec![list]);
        let input = TableFunctionBindInput::new(&values, &named);

        let mut types = Vec::new();
        let mut names = Vec::new();

        let result = unnest_bind(&input, &mut types, &mut names).unwrap();

        assert_eq!(types.len(), 1);
        assert_eq!(types[0], LogicalType::Integer);
        assert_eq!(names[0], "unnest");

        let bd = result.unwrap();
        let bd = bd.as_any().downcast_ref::<UnnestBindData>().unwrap();
        assert_eq!(bd.cardinality, 2);
    }

    #[test]
    fn test_unnest_bind_varchar_list() {
        let list = Value::List(
            vec![
                Value::Varchar("a".to_string()),
                Value::Varchar("b".to_string()),
            ],
            LogicalType::Varchar,
        );
        let (values, named) = create_bind_input(vec![list]);
        let input = TableFunctionBindInput::new(&values, &named);

        let mut types = Vec::new();
        let mut names = Vec::new();

        let result = unnest_bind(&input, &mut types, &mut names).unwrap();

        assert_eq!(types[0], LogicalType::Varchar);

        let bd = result.unwrap();
        let bd = bd.as_any().downcast_ref::<UnnestBindData>().unwrap();
        assert_eq!(bd.cardinality, 2);
    }

    #[test]
    fn test_unnest_bind_no_args_error() {
        let (values, named) = create_bind_input(vec![]);
        let input = TableFunctionBindInput::new(&values, &named);

        let mut types = Vec::new();
        let mut names = Vec::new();

        let result = unnest_bind(&input, &mut types, &mut names);
        assert!(result.is_err());
    }

    // ========== Global State Tests ==========

    #[test]
    fn test_global_state_get_next_batch() {
        let gstate = UnnestGlobalState::new(100);

        let batch1 = gstate.get_next_batch(30);
        assert_eq!(batch1, Some((0, 30)));

        let batch2 = gstate.get_next_batch(30);
        assert_eq!(batch2, Some((30, 30)));

        let batch3 = gstate.get_next_batch(30);
        assert_eq!(batch3, Some((60, 30)));

        let batch4 = gstate.get_next_batch(30);
        assert_eq!(batch4, Some((90, 10)));

        let batch5 = gstate.get_next_batch(30);
        assert_eq!(batch5, None);
    }

    #[test]
    fn test_global_state_progress() {
        let gstate = UnnestGlobalState::new(100);

        assert!((gstate.get_progress() - 0.0).abs() < 0.001);

        gstate.get_next_batch(50);
        assert!((gstate.get_progress() - 50.0).abs() < 0.001);

        gstate.get_next_batch(50);
        assert!((gstate.get_progress() - 100.0).abs() < 0.001);
    }

    #[test]
    fn test_global_state_empty() {
        let gstate = UnnestGlobalState::new(0);
        assert!((gstate.get_progress() - 100.0).abs() < 0.001);
        assert_eq!(gstate.get_next_batch(10), None);
    }

    // ========== Function Set Tests ==========

    #[test]
    fn test_create_unnest_function_set() {
        let set = create_unnest_function_set();

        assert_eq!(set.name, "unnest");
        assert_eq!(set.functions.len(), 1);

        // The function should have varargs
        assert!(set.functions[0].has_varargs());
    }

    // ========== Execution Tests ==========

    #[test]
    fn test_unnest_function_execution() {
        let list = Value::List(
            vec![Value::BigInt(1), Value::BigInt(2), Value::BigInt(3)],
            LogicalType::BigInt,
        );
        let bind_data = UnnestBindData::new(&list).unwrap();
        let gstate = UnnestGlobalState::new(bind_data.cardinality);
        let mut lstate = UnnestLocalState::new();

        let mut chunk = Chunk::initialize(&[LogicalType::BigInt], VECTOR_SIZE);

        let mut input = TableFunctionInput {
            bind_data: Some(&bind_data as &dyn TableFunctionBindData),
            local_state: Some(&mut lstate as &mut dyn LocalTableFunctionState),
            global_state: Some(&gstate as &dyn GlobalTableFunctionState),
        };

        let _result = unnest_function(&mut input, &mut chunk).unwrap();

        assert_eq!(chunk.size(), 3);

        // Verify values
        let col = chunk.column(0).unwrap();
        assert_eq!(col.get_i64(0).unwrap(), 1);
        assert_eq!(col.get_i64(1).unwrap(), 2);
        assert_eq!(col.get_i64(2).unwrap(), 3);
    }

    #[test]
    fn test_unnest_empty_list() {
        let list = Value::List(vec![], LogicalType::Integer);
        let bind_data = UnnestBindData::new(&list).unwrap();
        let gstate = UnnestGlobalState::new(bind_data.cardinality);
        let mut lstate = UnnestLocalState::new();

        let mut chunk = Chunk::initialize(&[LogicalType::Integer], VECTOR_SIZE);

        let mut input = TableFunctionInput {
            bind_data: Some(&bind_data as &dyn TableFunctionBindData),
            local_state: Some(&mut lstate as &mut dyn LocalTableFunctionState),
            global_state: Some(&gstate as &dyn GlobalTableFunctionState),
        };

        let result = unnest_function(&mut input, &mut chunk).unwrap();

        assert_eq!(result, TableFunctionResult::Finished);
        assert_eq!(chunk.size(), 0);
    }

    #[test]
    fn test_unnest_large_list_batching() {
        // Create a list with 5000 elements
        let values: Vec<Value> = (0..5000).map(|i| Value::BigInt(i)).collect();
        let list = Value::List(values, LogicalType::BigInt);
        let bind_data = UnnestBindData::new(&list).unwrap();
        let gstate = UnnestGlobalState::new(bind_data.cardinality);
        let mut lstate = UnnestLocalState::new();

        let mut total_rows = 0;
        let mut batch_count = 0;

        loop {
            let mut chunk = Chunk::initialize(&[LogicalType::BigInt], VECTOR_SIZE);

            let mut input = TableFunctionInput {
                bind_data: Some(&bind_data as &dyn TableFunctionBindData),
                local_state: Some(&mut lstate as &mut dyn LocalTableFunctionState),
                global_state: Some(&gstate as &dyn GlobalTableFunctionState),
            };

            let result = unnest_function(&mut input, &mut chunk).unwrap();
            total_rows += chunk.size();
            batch_count += 1;

            if result == TableFunctionResult::Finished && chunk.size() == 0 {
                break;
            }
        }

        assert_eq!(total_rows, 5000);
        assert!(batch_count > 1);
    }

    #[test]
    fn test_unnest_with_nulls() {
        let list = Value::List(
            vec![
                Value::BigInt(1),
                Value::Null(LogicalType::BigInt),
                Value::BigInt(3),
            ],
            LogicalType::BigInt,
        );
        let bind_data = UnnestBindData::new(&list).unwrap();
        let gstate = UnnestGlobalState::new(bind_data.cardinality);
        let mut lstate = UnnestLocalState::new();

        let mut chunk = Chunk::initialize(&[LogicalType::BigInt], VECTOR_SIZE);

        let mut input = TableFunctionInput {
            bind_data: Some(&bind_data as &dyn TableFunctionBindData),
            local_state: Some(&mut lstate as &mut dyn LocalTableFunctionState),
            global_state: Some(&gstate as &dyn GlobalTableFunctionState),
        };

        let _result = unnest_function(&mut input, &mut chunk).unwrap();

        assert_eq!(chunk.size(), 3);

        // First and third values should be valid
        let col = chunk.column(0).unwrap();
        assert_eq!(col.get_i64(0).unwrap(), 1);
        // Second value should be NULL (validity mask should be false)
        assert!(!col.validity().is_valid(1));
        assert_eq!(col.get_i64(2).unwrap(), 3);
    }
}
