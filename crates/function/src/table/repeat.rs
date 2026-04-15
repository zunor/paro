//! Repeat and Repeat Row Table Functions
//!
//!
//!
//! ## Dependencies Check
//! - TableFunction: ✅ `crate::table`
//! - Chunk: ✅ `paro_common::chunk`
//! - Vector: ✅ `paro_common::vector`
//!
//! ## Overview
//! Implements the `repeat` and `repeat_row` table functions:
//! - `repeat(value, count)` - Repeat a single value count times
//! - `repeat_row(value1, value2,..., num_rows => count)` - Repeat a row count times
//!
//! ## Example
//! ```sql
//! SELECT * FROM repeat(42, 5);
//! -- Returns 5 rows of 42
//!
//! SELECT * FROM repeat_row(1, 'hello', num_rows => 3);
//! -- Returns 3 rows of (1, 'hello')
//! ```

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;

use super::{
    GlobalTableFunctionState, LocalTableFunctionState, TableFunction, TableFunctionBindData,
    TableFunctionBindInput, TableFunctionInitInput, TableFunctionInput, TableFunctionResult,
    TableFunctionSet,
};

// ============================================================================
// Repeat Bind Data
// ============================================================================

/// Bind data for repeat function.
#[derive(Clone, Debug)]
pub struct RepeatBindData {
    /// The value to repeat.
    pub value: Value,
    /// Number of times to repeat.
    pub target_count: usize,
}

impl RepeatBindData {
    /// Create new bind data.
    pub fn new(value: Value, target_count: usize) -> Self {
        Self {
            value,
            target_count,
        }
    }
}

impl TableFunctionBindData for RepeatBindData {
    fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn cardinality(&self) -> Option<usize> {
        Some(self.target_count)
    }
}

// ============================================================================
// Repeat Row Bind Data
// ============================================================================

/// Bind data for repeat_row function.
#[derive(Clone, Debug)]
pub struct RepeatRowBindData {
    /// The values to repeat (one per column).
    pub values: Vec<Value>,
    /// Number of times to repeat.
    pub target_count: usize,
}

impl RepeatRowBindData {
    /// Create new bind data.
    pub fn new(values: Vec<Value>, target_count: usize) -> Self {
        Self {
            values,
            target_count,
        }
    }
}

impl TableFunctionBindData for RepeatRowBindData {
    fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn cardinality(&self) -> Option<usize> {
        Some(self.target_count)
    }
}

// ============================================================================
// Global State
// ============================================================================

/// Global state for repeat/repeat_row execution.
pub struct RepeatGlobalState {
    /// Current count of rows produced (atomic for thread safety).
    current_count: AtomicUsize,
    /// Target count of rows to produce.
    target_count: usize,
}

impl RepeatGlobalState {
    /// Create new global state.
    pub fn new(target_count: usize) -> Self {
        Self {
            current_count: AtomicUsize::new(0),
            target_count,
        }
    }

    /// Get the next batch size to produce.
    /// Returns the number of rows to produce, or 0 if done.
    pub fn get_next_batch(&self, max_batch_size: usize) -> usize {
        loop {
            let current = self.current_count.load(Ordering::Relaxed);
            if current >= self.target_count {
                return 0;
            }

            let remaining = self.target_count - current;
            let batch_size = remaining.min(max_batch_size);
            let new_count = current + batch_size;

            if self
                .current_count
                .compare_exchange(current, new_count, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return batch_size;
            }
            // CAS failed, retry
        }
    }
}

impl GlobalTableFunctionState for RepeatGlobalState {
    fn max_threads(&self) -> usize {
        // Single-threaded for simplicity
        1
    }

    fn get_progress(&self) -> f64 {
        if self.target_count == 0 {
            return 100.0;
        }
        let current = self.current_count.load(Ordering::Relaxed);
        (current as f64 / self.target_count as f64) * 100.0
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

/// Local state for repeat/repeat_row execution.
pub struct RepeatLocalState {
    /// Whether this local state has finished.
    finished: bool,
}

impl RepeatLocalState {
    /// Create new local state.
    pub fn new() -> Self {
        Self { finished: false }
    }
}

impl Default for RepeatLocalState {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalTableFunctionState for RepeatLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// ============================================================================
// Bind Functions
// ============================================================================

/// Bind function for repeat.
fn repeat_bind(
    input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    if input.inputs.len() != 2 {
        return Err(paro_common::error::syntax(
            "repeat requires exactly 2 arguments: repeat(value, count)".to_string(),
        ));
    }

    let value = &input.inputs[0];
    let count_value = &input.inputs[1];

    // Check for NULL count
    if matches!(count_value, Value::Null(_)) {
        return Err(paro_common::error::syntax(
            "repeat second parameter cannot be NULL".to_string(),
        ));
    }

    // Extract count
    let count = value_to_i64(count_value).ok_or_else(|| {
        paro_common::error::syntax("repeat second parameter must be an integer".to_string())
    })?;

    if count < 0 {
        return Err(paro_common::error::syntax(
            "repeat second parameter cannot be less than 0".to_string(),
        ));
    }

    // Return type is the type of the first argument
    return_types.push(value.logical_type());
    names.push(value.to_string());

    Ok(Some(Box::new(RepeatBindData::new(
        value.clone(),
        count as usize,
    ))))
}

/// Bind function for repeat_row.
fn repeat_row_bind(
    input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    // Check for num_rows named parameter
    let num_rows = input.named_parameters.get("num_rows").ok_or_else(|| {
        paro_common::error::syntax("repeat_row requires num_rows to be specified".to_string())
    })?;

    // Check for NULL num_rows
    if matches!(num_rows, Value::Null(_)) {
        return Err(paro_common::error::syntax(
            "repeat_row num_rows cannot be NULL".to_string(),
        ));
    }

    // Extract count
    let count = value_to_i64(num_rows).ok_or_else(|| {
        paro_common::error::syntax("repeat_row num_rows must be an integer".to_string())
    })?;

    if count < 0 {
        return Err(paro_common::error::syntax(
            "repeat_row num_rows cannot be less than 0".to_string(),
        ));
    }

    // Check for at least one column
    if input.inputs.is_empty() {
        return Err(paro_common::error::syntax(
            "repeat_row requires at least one column to be specified".to_string(),
        ));
    }

    // Return types are the types of all input arguments
    for (idx, value) in input.inputs.iter().enumerate() {
        return_types.push(value.logical_type());
        names.push(format!("column{}", idx));
    }

    Ok(Some(Box::new(RepeatRowBindData::new(
        input.inputs.to_vec(),
        count as usize,
    ))))
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Extract i64 from Value.
fn value_to_i64(v: &Value) -> Option<i64> {
    match v {
        Value::BigInt(i) => Some(*i),
        Value::Integer(i) => Some(*i as i64),
        Value::SmallInt(i) => Some(*i as i64),
        Value::TinyInt(i) => Some(*i as i64),
        Value::UBigInt(i) => Some(*i as i64),
        Value::UInteger(i) => Some(*i as i64),
        Value::USmallInt(i) => Some(*i as i64),
        Value::UTinyInt(i) => Some(*i as i64),
        _ => None,
    }
}

// ============================================================================
// Init Functions
// ============================================================================

/// Initialize global state for repeat.
fn repeat_init_global(
    input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    let target_count = input
        .bind_data
        .and_then(|bd| bd.as_any().downcast_ref::<RepeatBindData>())
        .map(|bd| bd.target_count)
        .unwrap_or(0);

    Ok(Some(Box::new(RepeatGlobalState::new(target_count))))
}

/// Initialize global state for repeat_row.
fn repeat_row_init_global(
    input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    let target_count = input
        .bind_data
        .and_then(|bd| bd.as_any().downcast_ref::<RepeatRowBindData>())
        .map(|bd| bd.target_count)
        .unwrap_or(0);

    Ok(Some(Box::new(RepeatGlobalState::new(target_count))))
}

/// Initialize local state.
fn repeat_init_local(
    _input: &TableFunctionInitInput,
    _global_state: Option<&dyn GlobalTableFunctionState>,
) -> Result<Option<Box<dyn LocalTableFunctionState>>> {
    Ok(Some(Box::new(RepeatLocalState::new())))
}

// ============================================================================
// Main Functions
// ============================================================================

/// Main execution function for repeat.
fn repeat_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let bind_data = input
        .bind_data
        .and_then(|bd| bd.as_any().downcast_ref::<RepeatBindData>());

    let gstate = input
        .global_state
        .and_then(|gs| gs.as_any().downcast_ref::<RepeatGlobalState>());

    let lstate = input
        .local_state
        .as_mut()
        .and_then(|ls| ls.as_any_mut().downcast_mut::<RepeatLocalState>());

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

    // Get next batch size
    let batch_size = gstate.get_next_batch(VECTOR_SIZE);

    if batch_size == 0 {
        lstate.finished = true;
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    }

    // Fill output with the repeated value using constant vector
    if let Some(col) = output.column_mut(0) {
        col.reference_value(&bind_data.value);
    }
    output.set_cardinality(batch_size);

    Ok(TableFunctionResult::HaveMoreOutput)
}

/// Main execution function for repeat_row.
fn repeat_row_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let bind_data = input
        .bind_data
        .and_then(|bd| bd.as_any().downcast_ref::<RepeatRowBindData>());

    let gstate = input
        .global_state
        .and_then(|gs| gs.as_any().downcast_ref::<RepeatGlobalState>());

    let lstate = input
        .local_state
        .as_mut()
        .and_then(|ls| ls.as_any_mut().downcast_mut::<RepeatLocalState>());

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

    // Get next batch size
    let batch_size = gstate.get_next_batch(VECTOR_SIZE);

    if batch_size == 0 {
        lstate.finished = true;
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    }

    // Fill each column with its repeated value using constant vector
    for (idx, value) in bind_data.values.iter().enumerate() {
        if let Some(col) = output.column_mut(idx) {
            col.reference_value(value);
        }
    }
    output.set_cardinality(batch_size);

    Ok(TableFunctionResult::HaveMoreOutput)
}

// ============================================================================
// Cardinality Functions
// ============================================================================

/// Cardinality estimation for repeat.
fn repeat_cardinality(bind_data: Option<&dyn TableFunctionBindData>) -> Option<usize> {
    bind_data
        .and_then(|bd| bd.as_any().downcast_ref::<RepeatBindData>())
        .map(|bd| bd.target_count)
}

/// Cardinality estimation for repeat_row.
fn repeat_row_cardinality(bind_data: Option<&dyn TableFunctionBindData>) -> Option<usize> {
    bind_data
        .and_then(|bd| bd.as_any().downcast_ref::<RepeatRowBindData>())
        .map(|bd| bd.target_count)
}

// ============================================================================
// Progress Functions
// ============================================================================

/// Progress reporting function.
fn repeat_progress(
    _bind_data: Option<&dyn TableFunctionBindData>,
    global_state: Option<&dyn GlobalTableFunctionState>,
) -> f64 {
    global_state.map(|gs| gs.get_progress()).unwrap_or(-1.0)
}

// ============================================================================
// TableFunctionSet Creation
// ============================================================================

/// Create the repeat table function set.
///
/// Includes:
/// - `repeat(value, count)` - Repeat a value count times
pub fn create_repeat_function_set() -> TableFunctionSet {
    let mut set = TableFunctionSet::new("repeat");

    // repeat(ANY, BIGINT)
    let func = TableFunction::new("repeat", vec![])
        .with_varargs(LogicalType::Unknown) // Accept any type for first arg
        .with_bind(repeat_bind)
        .with_init_global(repeat_init_global)
        .with_init_local(repeat_init_local)
        .with_function(repeat_function)
        .with_cardinality(repeat_cardinality)
        .with_progress(repeat_progress);
    set.add_function(func);

    set
}

/// Create the repeat_row table function set.
///
/// Includes:
/// - `repeat_row(value1, value2,..., num_rows => count)` - Repeat a row count times
pub fn create_repeat_row_function_set() -> TableFunctionSet {
    let mut set = TableFunctionSet::new("repeat_row");

    // repeat_row(..., num_rows => BIGINT)
    let func = TableFunction::new("repeat_row", vec![])
        .with_varargs(LogicalType::Unknown) // Accept any types
        .with_named_parameter("num_rows", LogicalType::BigInt)
        .with_bind(repeat_row_bind)
        .with_init_global(repeat_row_init_global)
        .with_init_local(repeat_init_local)
        .with_function(repeat_row_function)
        .with_cardinality(repeat_row_cardinality)
        .with_progress(repeat_progress);
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

    fn create_bind_input_with_named(
        values: Vec<Value>,
        named: HashMap<String, Value>,
    ) -> (Vec<Value>, HashMap<String, Value>) {
        (values, named)
    }

    // ========== RepeatBindData Tests ==========

    #[test]
    fn test_repeat_bind_data() {
        let bd = RepeatBindData::new(Value::Integer(42), 10);
        assert_eq!(bd.target_count, 10);
        assert_eq!(bd.cardinality(), Some(10));
    }

    // ========== RepeatRowBindData Tests ==========

    #[test]
    fn test_repeat_row_bind_data() {
        let values = vec![Value::Integer(1), Value::Varchar("hello".to_string())];
        let bd = RepeatRowBindData::new(values.clone(), 5);
        assert_eq!(bd.target_count, 5);
        assert_eq!(bd.values.len(), 2);
        assert_eq!(bd.cardinality(), Some(5));
    }

    // ========== Global State Tests ==========

    #[test]
    fn test_global_state_get_next_batch() {
        let gstate = RepeatGlobalState::new(100);

        let batch1 = gstate.get_next_batch(30);
        assert_eq!(batch1, 30);

        let batch2 = gstate.get_next_batch(30);
        assert_eq!(batch2, 30);

        let batch3 = gstate.get_next_batch(30);
        assert_eq!(batch3, 30);

        let batch4 = gstate.get_next_batch(30);
        assert_eq!(batch4, 10);

        let batch5 = gstate.get_next_batch(30);
        assert_eq!(batch5, 0);
    }

    #[test]
    fn test_global_state_progress() {
        let gstate = RepeatGlobalState::new(100);

        assert!((gstate.get_progress() - 0.0).abs() < 0.001);

        gstate.get_next_batch(50);
        assert!((gstate.get_progress() - 50.0).abs() < 0.001);

        gstate.get_next_batch(50);
        assert!((gstate.get_progress() - 100.0).abs() < 0.001);
    }

    #[test]
    fn test_global_state_empty() {
        let gstate = RepeatGlobalState::new(0);
        assert!((gstate.get_progress() - 100.0).abs() < 0.001);
        assert_eq!(gstate.get_next_batch(10), 0);
    }

    // ========== Bind Function Tests ==========

    #[test]
    fn test_repeat_bind_success() {
        let (values, named) = create_bind_input(vec![Value::Integer(42), Value::BigInt(5)]);
        let input = TableFunctionBindInput::new(&values, &named);

        let mut types = Vec::new();
        let mut names = Vec::new();

        let result = repeat_bind(&input, &mut types, &mut names).unwrap();

        assert_eq!(types.len(), 1);
        assert_eq!(types[0], LogicalType::Integer);
        assert_eq!(names[0], "42");

        let bd = result.unwrap();
        let bd = bd.as_any().downcast_ref::<RepeatBindData>().unwrap();
        assert_eq!(bd.target_count, 5);
    }

    #[test]
    fn test_repeat_bind_null_count_error() {
        let (values, named) =
            create_bind_input(vec![Value::Integer(42), Value::Null(LogicalType::BigInt)]);
        let input = TableFunctionBindInput::new(&values, &named);

        let mut types = Vec::new();
        let mut names = Vec::new();

        let result = repeat_bind(&input, &mut types, &mut names);
        assert!(result.is_err());
    }

    #[test]
    fn test_repeat_bind_negative_count_error() {
        let (values, named) = create_bind_input(vec![Value::Integer(42), Value::BigInt(-5)]);
        let input = TableFunctionBindInput::new(&values, &named);

        let mut types = Vec::new();
        let mut names = Vec::new();

        let result = repeat_bind(&input, &mut types, &mut names);
        assert!(result.is_err());
    }

    #[test]
    fn test_repeat_bind_wrong_arg_count_error() {
        let (values, named) = create_bind_input(vec![Value::Integer(42)]);
        let input = TableFunctionBindInput::new(&values, &named);

        let mut types = Vec::new();
        let mut names = Vec::new();

        let result = repeat_bind(&input, &mut types, &mut names);
        assert!(result.is_err());
    }

    #[test]
    fn test_repeat_row_bind_success() {
        let mut named = HashMap::new();
        named.insert("num_rows".to_string(), Value::BigInt(3));
        let (values, named) = create_bind_input_with_named(
            vec![Value::Integer(1), Value::Varchar("hello".to_string())],
            named,
        );
        let input = TableFunctionBindInput::new(&values, &named);

        let mut types = Vec::new();
        let mut names = Vec::new();

        let result = repeat_row_bind(&input, &mut types, &mut names).unwrap();

        assert_eq!(types.len(), 2);
        assert_eq!(types[0], LogicalType::Integer);
        assert_eq!(types[1], LogicalType::Varchar);
        assert_eq!(names[0], "column0");
        assert_eq!(names[1], "column1");

        let bd = result.unwrap();
        let bd = bd.as_any().downcast_ref::<RepeatRowBindData>().unwrap();
        assert_eq!(bd.target_count, 3);
        assert_eq!(bd.values.len(), 2);
    }

    #[test]
    fn test_repeat_row_bind_missing_num_rows_error() {
        let (values, named) = create_bind_input(vec![Value::Integer(1)]);
        let input = TableFunctionBindInput::new(&values, &named);

        let mut types = Vec::new();
        let mut names = Vec::new();

        let result = repeat_row_bind(&input, &mut types, &mut names);
        assert!(result.is_err());
    }

    #[test]
    fn test_repeat_row_bind_no_columns_error() {
        let mut named = HashMap::new();
        named.insert("num_rows".to_string(), Value::BigInt(3));
        let (values, named) = create_bind_input_with_named(vec![], named);
        let input = TableFunctionBindInput::new(&values, &named);

        let mut types = Vec::new();
        let mut names = Vec::new();

        let result = repeat_row_bind(&input, &mut types, &mut names);
        assert!(result.is_err());
    }

    // ========== Function Set Tests ==========

    #[test]
    fn test_create_repeat_function_set() {
        let set = create_repeat_function_set();

        assert_eq!(set.name, "repeat");
        assert_eq!(set.functions.len(), 1);
        assert!(set.functions[0].has_varargs());
    }

    #[test]
    fn test_create_repeat_row_function_set() {
        let set = create_repeat_row_function_set();

        assert_eq!(set.name, "repeat_row");
        assert_eq!(set.functions.len(), 1);
        assert!(set.functions[0].has_varargs());
        assert_eq!(set.functions[0].named_parameters.len(), 1);
        assert_eq!(set.functions[0].named_parameters[0].0, "num_rows");
    }

    // ========== Execution Tests ==========

    #[test]
    fn test_repeat_function_execution() {
        let bind_data = RepeatBindData::new(Value::BigInt(42), 5);
        let gstate = RepeatGlobalState::new(bind_data.target_count);
        let mut lstate = RepeatLocalState::new();

        let mut chunk = Chunk::initialize(&[LogicalType::BigInt], VECTOR_SIZE);

        let mut input = TableFunctionInput {
            bind_data: Some(&bind_data as &dyn TableFunctionBindData),
            local_state: Some(&mut lstate as &mut dyn LocalTableFunctionState),
            global_state: Some(&gstate as &dyn GlobalTableFunctionState),
        };

        let result = repeat_function(&mut input, &mut chunk).unwrap();

        assert_eq!(result, TableFunctionResult::HaveMoreOutput);
        assert_eq!(chunk.size(), 5);

        // Verify all values are 42
        let col = chunk.column(0).unwrap();
        for i in 0..5 {
            assert_eq!(col.get_i64(i).unwrap(), 42);
        }
    }

    #[test]
    fn test_repeat_function_zero_count() {
        let bind_data = RepeatBindData::new(Value::BigInt(42), 0);
        let gstate = RepeatGlobalState::new(bind_data.target_count);
        let mut lstate = RepeatLocalState::new();

        let mut chunk = Chunk::initialize(&[LogicalType::BigInt], VECTOR_SIZE);

        let mut input = TableFunctionInput {
            bind_data: Some(&bind_data as &dyn TableFunctionBindData),
            local_state: Some(&mut lstate as &mut dyn LocalTableFunctionState),
            global_state: Some(&gstate as &dyn GlobalTableFunctionState),
        };

        let result = repeat_function(&mut input, &mut chunk).unwrap();

        assert_eq!(result, TableFunctionResult::Finished);
        assert_eq!(chunk.size(), 0);
    }

    #[test]
    fn test_repeat_function_large_count_batching() {
        let bind_data = RepeatBindData::new(Value::BigInt(42), 5000);
        let gstate = RepeatGlobalState::new(bind_data.target_count);
        let mut lstate = RepeatLocalState::new();

        let mut total_rows = 0;
        let mut batch_count = 0;

        loop {
            let mut chunk = Chunk::initialize(&[LogicalType::BigInt], VECTOR_SIZE);

            let mut input = TableFunctionInput {
                bind_data: Some(&bind_data as &dyn TableFunctionBindData),
                local_state: Some(&mut lstate as &mut dyn LocalTableFunctionState),
                global_state: Some(&gstate as &dyn GlobalTableFunctionState),
            };

            let result = repeat_function(&mut input, &mut chunk).unwrap();
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
    fn test_repeat_row_function_execution() {
        let values = vec![Value::BigInt(1), Value::Varchar("hello".to_string())];
        let bind_data = RepeatRowBindData::new(values, 3);
        let gstate = RepeatGlobalState::new(bind_data.target_count);
        let mut lstate = RepeatLocalState::new();

        let mut chunk =
            Chunk::initialize(&[LogicalType::BigInt, LogicalType::Varchar], VECTOR_SIZE);

        let mut input = TableFunctionInput {
            bind_data: Some(&bind_data as &dyn TableFunctionBindData),
            local_state: Some(&mut lstate as &mut dyn LocalTableFunctionState),
            global_state: Some(&gstate as &dyn GlobalTableFunctionState),
        };

        let result = repeat_row_function(&mut input, &mut chunk).unwrap();

        assert_eq!(result, TableFunctionResult::HaveMoreOutput);
        assert_eq!(chunk.size(), 3);

        // Verify values
        let col0 = chunk.column(0).unwrap();
        let col1 = chunk.column(1).unwrap();
        for i in 0..3 {
            assert_eq!(col0.get_i64(i).unwrap(), 1);
            assert_eq!(col1.get_string(i).unwrap(), "hello");
        }
    }

    #[test]
    fn test_repeat_with_null_value() {
        let bind_data = RepeatBindData::new(Value::Null(LogicalType::Integer), 3);
        let gstate = RepeatGlobalState::new(bind_data.target_count);
        let mut lstate = RepeatLocalState::new();

        let mut chunk = Chunk::initialize(&[LogicalType::Integer], VECTOR_SIZE);

        let mut input = TableFunctionInput {
            bind_data: Some(&bind_data as &dyn TableFunctionBindData),
            local_state: Some(&mut lstate as &mut dyn LocalTableFunctionState),
            global_state: Some(&gstate as &dyn GlobalTableFunctionState),
        };

        let result = repeat_function(&mut input, &mut chunk).unwrap();

        assert_eq!(result, TableFunctionResult::HaveMoreOutput);
        assert_eq!(chunk.size(), 3);

        // All values should be NULL (use is_null which handles constant vectors correctly)
        let col = chunk.column(0).unwrap();
        for i in 0..3 {
            assert!(col.is_null(i));
        }
    }
}
