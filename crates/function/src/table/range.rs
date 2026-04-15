// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Range and Generate Series Table Functions
//!
//!
//!
//! ## Dependencies Check
//! - TableFunction: ✅ `crate::table`
//! - Chunk: ✅ `paro_common::chunk`
//! - Vector: ✅ `paro_common::vector`
//!
//! ## Overview
//! Implements the `range` and `generate_series` table functions:
//! - `range(end)` - Generate [0, end)
//! - `range(start, end)` - Generate [start, end)
//! - `range(start, end, step)` - Generate [start, end) with step
//! - `generate_series(start, end)` - Generate [start, end] (inclusive)
//! - `generate_series(start, end, step)` - Generate [start, end] with step (inclusive)
//!
//! ## Key Differences
//! - `range`: Exclusive upper bound (like Python's range)
//! - `generate_series`: Inclusive upper bound (like PostgreSQL)
//!
//! ## Optimization
//! Uses Sequence Vector for efficient memory usage when possible.

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{Vector, VECTOR_SIZE};

use super::{
    GlobalTableFunctionState, LocalTableFunctionState, TableFunction, TableFunctionBindData,
    TableFunctionBindInput, TableFunctionInitInput, TableFunctionInput, TableFunctionResult,
    TableFunctionSet,
};

// ============================================================================
// Bind Data
// ============================================================================

/// Bind data for range/generate_series functions.
#[derive(Clone, Debug)]
pub struct RangeBindData {
    /// Start value (inclusive).
    pub start: i64,
    /// End value (exclusive for range, inclusive for generate_series).
    pub end: i64,
    /// Step increment.
    pub step: i64,
    /// Whether this is generate_series (inclusive end) or range (exclusive end).
    pub generate_series: bool,
    /// Estimated cardinality.
    pub cardinality: usize,
}

impl RangeBindData {
    /// Create new bind data for range function.
    pub fn new_range(start: i64, end: i64, step: i64) -> Self {
        let cardinality = Self::compute_cardinality(start, end, step, false);
        Self {
            start,
            end,
            step,
            generate_series: false,
            cardinality,
        }
    }

    /// Create new bind data for generate_series function.
    pub fn new_generate_series(start: i64, end: i64, step: i64) -> Self {
        let cardinality = Self::compute_cardinality(start, end, step, true);
        Self {
            start,
            end,
            step,
            generate_series: true,
            cardinality,
        }
    }

    /// Compute the cardinality (number of rows) for the range.
    fn compute_cardinality(start: i64, end: i64, step: i64, generate_series: bool) -> usize {
        if step == 0 {
            return 0;
        }

        // Adjust end for generate_series (inclusive)
        let adjusted_end = if generate_series {
            if step > 0 {
                end.saturating_add(1)
            } else {
                end.saturating_sub(1)
            }
        } else {
            end
        };

        // Check for empty range
        if step > 0 && start >= adjusted_end {
            return 0;
        }
        if step < 0 && start <= adjusted_end {
            return 0;
        }

        // Compute cardinality using i128 to avoid overflow
        let diff = (adjusted_end as i128) - (start as i128);
        let step_128 = step as i128;
        let count = diff / step_128;

        // Handle remainder
        let count = if diff % step_128 != 0 {
            count + 1
        } else {
            count
        };

        count.max(0) as usize
    }
}

impl TableFunctionBindData for RangeBindData {
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

/// Global state for range/generate_series execution.
///
/// Tracks the current position in the sequence for thread-safe iteration.
pub struct RangeGlobalState {
    /// Current index in the sequence (atomic for thread safety).
    current_idx: AtomicUsize,
    /// Total number of elements to generate.
    total_count: usize,
    /// Start value.
    start: i64,
    /// Step increment.
    step: i64,
}

impl RangeGlobalState {
    /// Create new global state.
    pub fn new(start: i64, step: i64, total_count: usize) -> Self {
        Self {
            current_idx: AtomicUsize::new(0),
            total_count,
            start,
            step,
        }
    }

    /// Get the next batch of indices to process.
    /// Returns (start_idx, count) or None if exhausted.
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
            // CAS failed, retry
        }
    }
}

impl GlobalTableFunctionState for RangeGlobalState {
    fn max_threads(&self) -> usize {
        // Allow parallel execution for large ranges
        if self.total_count > VECTOR_SIZE * 4 {
            4
        } else {
            1
        }
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

/// Local state for range/generate_series execution.
///
/// Each thread has its own local state for tracking position.
pub struct RangeLocalState {
    /// Whether this local state has finished.
    finished: bool,
}

impl RangeLocalState {
    /// Create new local state.
    pub fn new() -> Self {
        Self { finished: false }
    }
}

impl Default for RangeLocalState {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalTableFunctionState for RangeLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
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

/// Parse range parameters from input values.
fn parse_range_params(inputs: &[Value]) -> (i64, i64, i64) {
    match inputs.len() {
        1 => {
            // range(end) - start=0, step=1
            let end = inputs.first().and_then(value_to_i64).unwrap_or(0);
            (0, end, 1)
        }
        2 => {
            // range(start, end) - step=1
            let start = inputs.first().and_then(value_to_i64).unwrap_or(0);
            let end = inputs.get(1).and_then(value_to_i64).unwrap_or(0);
            (start, end, 1)
        }
        3 => {
            // range(start, end, step)
            let start = inputs.first().and_then(value_to_i64).unwrap_or(0);
            let end = inputs.get(1).and_then(value_to_i64).unwrap_or(0);
            let step = inputs.get(2).and_then(value_to_i64).unwrap_or(1);
            (start, end, step)
        }
        _ => (0, 0, 1),
    }
}

// ============================================================================
// Bind Functions
// ============================================================================

/// Bind function for range.
fn range_bind(
    input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    // Check for NULL inputs
    for v in input.inputs {
        if matches!(v, Value::Null(_)) {
            return_types.push(LogicalType::BigInt);
            names.push("range".to_string());
            return Ok(Some(Box::new(RangeBindData::new_range(0, 0, 1))));
        }
    }

    let (start, end, step) = parse_range_params(input.inputs);

    if step == 0 {
        return Err(paro_common::error::syntax("step cannot be 0".to_string()));
    }

    return_types.push(LogicalType::BigInt);
    names.push("range".to_string());

    Ok(Some(Box::new(RangeBindData::new_range(start, end, step))))
}

/// Bind function for generate_series.
fn generate_series_bind(
    input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    // Check for NULL inputs
    for v in input.inputs {
        if matches!(v, Value::Null(_)) {
            return_types.push(LogicalType::BigInt);
            names.push("generate_series".to_string());
            return Ok(Some(Box::new(RangeBindData::new_generate_series(0, 0, 1))));
        }
    }

    let (start, end, step) = parse_range_params(input.inputs);

    if step == 0 {
        return Err(paro_common::error::syntax("step cannot be 0".to_string()));
    }

    return_types.push(LogicalType::BigInt);
    names.push("generate_series".to_string());

    Ok(Some(Box::new(RangeBindData::new_generate_series(
        start, end, step,
    ))))
}

// ============================================================================
// Init Functions
// ============================================================================

/// Initialize global state.
fn range_init_global(
    input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    let bind_data = input
        .bind_data
        .and_then(|bd| bd.as_any().downcast_ref::<RangeBindData>());

    if let Some(bd) = bind_data {
        Ok(Some(Box::new(RangeGlobalState::new(
            bd.start,
            bd.step,
            bd.cardinality,
        ))))
    } else {
        Ok(Some(Box::new(RangeGlobalState::new(0, 1, 0))))
    }
}

/// Initialize local state.
fn range_init_local(
    _input: &TableFunctionInitInput,
    _global_state: Option<&dyn GlobalTableFunctionState>,
) -> Result<Option<Box<dyn LocalTableFunctionState>>> {
    Ok(Some(Box::new(RangeLocalState::new())))
}

// ============================================================================
// Main Function
// ============================================================================

/// Main execution function for range/generate_series.
fn range_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let gstate = input
        .global_state
        .and_then(|gs| gs.as_any().downcast_ref::<RangeGlobalState>());

    let lstate = input
        .local_state
        .as_mut()
        .and_then(|ls| ls.as_any_mut().downcast_mut::<RangeLocalState>());

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
            // Calculate the starting value for this batch
            // value = start + step * idx
            let start_value = gstate
                .start
                .saturating_add(gstate.step.saturating_mul(start_idx as i64));

            // Use Sequence Vector for efficient representation
            let vec = Vector::sequence(start_value, gstate.step, count);

            // Replace the output column
            if let Some(col) = output.column_mut(0) {
                *col = vec;
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
fn range_cardinality(bind_data: Option<&dyn TableFunctionBindData>) -> Option<usize> {
    bind_data
        .and_then(|bd| bd.as_any().downcast_ref::<RangeBindData>())
        .map(|bd| bd.cardinality)
}

// ============================================================================
// Progress Function
// ============================================================================

/// Progress reporting function.
fn range_progress(
    _bind_data: Option<&dyn TableFunctionBindData>,
    global_state: Option<&dyn GlobalTableFunctionState>,
) -> f64 {
    global_state.map(|gs| gs.get_progress()).unwrap_or(-1.0)
}

// ============================================================================
// TableFunctionSet Creation
// ============================================================================

/// Create the range table function set.
///
/// Includes overloads:
/// - `range(end)` - Generate [0, end)
/// - `range(start, end)` - Generate [start, end)
/// - `range(start, end, step)` - Generate [start, end) with step
pub fn create_range_function_set() -> TableFunctionSet {
    let mut set = TableFunctionSet::new("range");

    // range(end)
    let func1 = TableFunction::new("range", vec![LogicalType::BigInt])
        .with_bind(range_bind)
        .with_init_global(range_init_global)
        .with_init_local(range_init_local)
        .with_function(range_function)
        .with_cardinality(range_cardinality)
        .with_progress(range_progress);
    set.add_function(func1);

    // range(start, end)
    let func2 = TableFunction::new("range", vec![LogicalType::BigInt, LogicalType::BigInt])
        .with_bind(range_bind)
        .with_init_global(range_init_global)
        .with_init_local(range_init_local)
        .with_function(range_function)
        .with_cardinality(range_cardinality)
        .with_progress(range_progress);
    set.add_function(func2);

    // range(start, end, step)
    let func3 = TableFunction::new(
        "range",
        vec![
            LogicalType::BigInt,
            LogicalType::BigInt,
            LogicalType::BigInt,
        ],
    )
    .with_bind(range_bind)
    .with_init_global(range_init_global)
    .with_init_local(range_init_local)
    .with_function(range_function)
    .with_cardinality(range_cardinality)
    .with_progress(range_progress);
    set.add_function(func3);

    set
}

/// Create the generate_series table function set.
///
/// Includes overloads:
/// - `generate_series(start, end)` - Generate [start, end] (inclusive)
/// - `generate_series(start, end, step)` - Generate [start, end] with step (inclusive)
pub fn create_generate_series_function_set() -> TableFunctionSet {
    let mut set = TableFunctionSet::new("generate_series");

    // generate_series(start, end)
    let func1 = TableFunction::new(
        "generate_series",
        vec![LogicalType::BigInt, LogicalType::BigInt],
    )
    .with_bind(generate_series_bind)
    .with_init_global(range_init_global)
    .with_init_local(range_init_local)
    .with_function(range_function)
    .with_cardinality(range_cardinality)
    .with_progress(range_progress);
    set.add_function(func1);

    // generate_series(start, end, step)
    let func2 = TableFunction::new(
        "generate_series",
        vec![
            LogicalType::BigInt,
            LogicalType::BigInt,
            LogicalType::BigInt,
        ],
    )
    .with_bind(generate_series_bind)
    .with_init_global(range_init_global)
    .with_init_local(range_init_local)
    .with_function(range_function)
    .with_cardinality(range_cardinality)
    .with_progress(range_progress);
    set.add_function(func2);

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

    // ========== RangeBindData Tests ==========

    #[test]
    fn test_range_bind_data_cardinality() {
        // range(10) -> [0, 10) = 10 elements
        let bd = RangeBindData::new_range(0, 10, 1);
        assert_eq!(bd.cardinality, 10);

        // range(5, 10) -> [5, 10) = 5 elements
        let bd = RangeBindData::new_range(5, 10, 1);
        assert_eq!(bd.cardinality, 5);

        // range(0, 10, 2) -> [0, 2, 4, 6, 8] = 5 elements
        let bd = RangeBindData::new_range(0, 10, 2);
        assert_eq!(bd.cardinality, 5);

        // range(0, 10, 3) -> [0, 3, 6, 9] = 4 elements
        let bd = RangeBindData::new_range(0, 10, 3);
        assert_eq!(bd.cardinality, 4);
    }

    #[test]
    fn test_generate_series_bind_data_cardinality() {
        // generate_series(1, 5) -> [1, 2, 3, 4, 5] = 5 elements
        let bd = RangeBindData::new_generate_series(1, 5, 1);
        assert_eq!(bd.cardinality, 5);

        // generate_series(1, 5, 2) -> [1, 3, 5] = 3 elements
        let bd = RangeBindData::new_generate_series(1, 5, 2);
        assert_eq!(bd.cardinality, 3);
    }

    #[test]
    fn test_range_bind_data_negative_step() {
        // range(10, 0, -1) -> [10, 9, 8, 7, 6, 5, 4, 3, 2, 1] = 10 elements
        let bd = RangeBindData::new_range(10, 0, -1);
        assert_eq!(bd.cardinality, 10);

        // range(10, 0, -2) -> [10, 8, 6, 4, 2] = 5 elements
        let bd = RangeBindData::new_range(10, 0, -2);
        assert_eq!(bd.cardinality, 5);
    }

    #[test]
    fn test_generate_series_negative_step() {
        // generate_series(5, 1, -1) -> [5, 4, 3, 2, 1] = 5 elements
        let bd = RangeBindData::new_generate_series(5, 1, -1);
        assert_eq!(bd.cardinality, 5);
    }

    #[test]
    fn test_range_bind_data_empty() {
        // range(10, 5, 1) -> empty (start > end with positive step)
        let bd = RangeBindData::new_range(10, 5, 1);
        assert_eq!(bd.cardinality, 0);

        // range(5, 10, -1) -> empty (start < end with negative step)
        let bd = RangeBindData::new_range(5, 10, -1);
        assert_eq!(bd.cardinality, 0);
    }

    // ========== Bind Function Tests ==========

    #[test]
    fn test_range_bind_single_arg() {
        let (values, named) = create_bind_input(vec![Value::BigInt(10)]);
        let input = TableFunctionBindInput::new(&values, &named);

        let mut types = Vec::new();
        let mut names = Vec::new();

        let result = range_bind(&input, &mut types, &mut names).unwrap();

        assert_eq!(types.len(), 1);
        assert_eq!(types[0], LogicalType::BigInt);
        assert_eq!(names[0], "range");

        let bd = result.unwrap();
        let bd = bd.as_any().downcast_ref::<RangeBindData>().unwrap();
        assert_eq!(bd.start, 0);
        assert_eq!(bd.end, 10);
        assert_eq!(bd.step, 1);
        assert!(!bd.generate_series);
    }

    #[test]
    fn test_range_bind_two_args() {
        let (values, named) = create_bind_input(vec![Value::BigInt(5), Value::BigInt(15)]);
        let input = TableFunctionBindInput::new(&values, &named);

        let mut types = Vec::new();
        let mut names = Vec::new();

        let result = range_bind(&input, &mut types, &mut names).unwrap();

        let bd = result.unwrap();
        let bd = bd.as_any().downcast_ref::<RangeBindData>().unwrap();
        assert_eq!(bd.start, 5);
        assert_eq!(bd.end, 15);
        assert_eq!(bd.step, 1);
    }

    #[test]
    fn test_range_bind_three_args() {
        let (values, named) = create_bind_input(vec![
            Value::BigInt(0),
            Value::BigInt(100),
            Value::BigInt(10),
        ]);
        let input = TableFunctionBindInput::new(&values, &named);

        let mut types = Vec::new();
        let mut names = Vec::new();

        let result = range_bind(&input, &mut types, &mut names).unwrap();

        let bd = result.unwrap();
        let bd = bd.as_any().downcast_ref::<RangeBindData>().unwrap();
        assert_eq!(bd.start, 0);
        assert_eq!(bd.end, 100);
        assert_eq!(bd.step, 10);
        assert_eq!(bd.cardinality, 10);
    }

    #[test]
    fn test_range_bind_zero_step_error() {
        let (values, named) =
            create_bind_input(vec![Value::BigInt(0), Value::BigInt(10), Value::BigInt(0)]);
        let input = TableFunctionBindInput::new(&values, &named);

        let mut types = Vec::new();
        let mut names = Vec::new();

        let result = range_bind(&input, &mut types, &mut names);
        assert!(result.is_err());
    }

    #[test]
    fn test_generate_series_bind() {
        let (values, named) = create_bind_input(vec![Value::BigInt(1), Value::BigInt(5)]);
        let input = TableFunctionBindInput::new(&values, &named);

        let mut types = Vec::new();
        let mut names = Vec::new();

        let result = generate_series_bind(&input, &mut types, &mut names).unwrap();

        assert_eq!(names[0], "generate_series");

        let bd = result.unwrap();
        let bd = bd.as_any().downcast_ref::<RangeBindData>().unwrap();
        assert_eq!(bd.start, 1);
        assert_eq!(bd.end, 5);
        assert!(bd.generate_series);
        assert_eq!(bd.cardinality, 5); // [1, 2, 3, 4, 5]
    }

    // ========== Global State Tests ==========

    #[test]
    fn test_global_state_get_next_batch() {
        let gstate = RangeGlobalState::new(0, 1, 100);

        // First batch
        let batch1 = gstate.get_next_batch(30);
        assert_eq!(batch1, Some((0, 30)));

        // Second batch
        let batch2 = gstate.get_next_batch(30);
        assert_eq!(batch2, Some((30, 30)));

        // Third batch
        let batch3 = gstate.get_next_batch(30);
        assert_eq!(batch3, Some((60, 30)));

        // Fourth batch (remaining 10)
        let batch4 = gstate.get_next_batch(30);
        assert_eq!(batch4, Some((90, 10)));

        // Exhausted
        let batch5 = gstate.get_next_batch(30);
        assert_eq!(batch5, None);
    }

    #[test]
    fn test_global_state_progress() {
        let gstate = RangeGlobalState::new(0, 1, 100);

        assert!((gstate.get_progress() - 0.0).abs() < 0.001);

        gstate.get_next_batch(50);
        assert!((gstate.get_progress() - 50.0).abs() < 0.001);

        gstate.get_next_batch(50);
        assert!((gstate.get_progress() - 100.0).abs() < 0.001);
    }

    #[test]
    fn test_global_state_max_threads() {
        // Small range - single thread
        let gstate = RangeGlobalState::new(0, 1, 100);
        assert_eq!(gstate.max_threads(), 1);

        // Large range - multiple threads
        let gstate = RangeGlobalState::new(0, 1, VECTOR_SIZE * 10);
        assert_eq!(gstate.max_threads(), 4);
    }

    // ========== Function Set Tests ==========

    #[test]
    fn test_create_range_function_set() {
        let set = create_range_function_set();

        assert_eq!(set.name, "range");
        assert_eq!(set.functions.len(), 3);

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
    fn test_create_generate_series_function_set() {
        let set = create_generate_series_function_set();

        assert_eq!(set.name, "generate_series");
        assert_eq!(set.functions.len(), 2);

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

    // ========== Execution Tests ==========

    #[test]
    fn test_range_function_execution() {
        // Setup: range(1, 6) -> [1, 2, 3, 4, 5]
        let bind_data = RangeBindData::new_range(1, 6, 1);
        let gstate = RangeGlobalState::new(bind_data.start, bind_data.step, bind_data.cardinality);
        let mut lstate = RangeLocalState::new();

        let mut chunk = Chunk::initialize(&[LogicalType::BigInt], VECTOR_SIZE);

        let mut input = TableFunctionInput {
            bind_data: Some(&bind_data as &dyn TableFunctionBindData),
            local_state: Some(&mut lstate as &mut dyn LocalTableFunctionState),
            global_state: Some(&gstate as &dyn GlobalTableFunctionState),
        };

        let _result = range_function(&mut input, &mut chunk).unwrap();

        assert_eq!(chunk.size(), 5);

        // Verify values using sequence vector
        let col = chunk.column(0).unwrap();
        assert_eq!(col.vector_type(), paro_common::vector::VectorType::Sequence);

        // Check values
        for i in 0..5 {
            let val = col.get_i64(i).unwrap();
            assert_eq!(val, (i + 1) as i64);
        }
    }

    #[test]
    fn test_generate_series_function_execution() {
        // Setup: generate_series(1, 5) -> [1, 2, 3, 4, 5]
        let bind_data = RangeBindData::new_generate_series(1, 5, 1);
        let gstate = RangeGlobalState::new(bind_data.start, bind_data.step, bind_data.cardinality);
        let mut lstate = RangeLocalState::new();

        let mut chunk = Chunk::initialize(&[LogicalType::BigInt], VECTOR_SIZE);

        let mut input = TableFunctionInput {
            bind_data: Some(&bind_data as &dyn TableFunctionBindData),
            local_state: Some(&mut lstate as &mut dyn LocalTableFunctionState),
            global_state: Some(&gstate as &dyn GlobalTableFunctionState),
        };

        let _result = range_function(&mut input, &mut chunk).unwrap();

        assert_eq!(chunk.size(), 5);

        // Verify values
        let col = chunk.column(0).unwrap();
        for i in 0..5 {
            let val = col.get_i64(i).unwrap();
            assert_eq!(val, (i + 1) as i64);
        }
    }

    #[test]
    fn test_range_with_step() {
        // range(0, 10, 2) -> [0, 2, 4, 6, 8]
        let bind_data = RangeBindData::new_range(0, 10, 2);
        let gstate = RangeGlobalState::new(bind_data.start, bind_data.step, bind_data.cardinality);
        let mut lstate = RangeLocalState::new();

        let mut chunk = Chunk::initialize(&[LogicalType::BigInt], VECTOR_SIZE);

        let mut input = TableFunctionInput {
            bind_data: Some(&bind_data as &dyn TableFunctionBindData),
            local_state: Some(&mut lstate as &mut dyn LocalTableFunctionState),
            global_state: Some(&gstate as &dyn GlobalTableFunctionState),
        };

        let _result = range_function(&mut input, &mut chunk).unwrap();

        assert_eq!(chunk.size(), 5);

        let col = chunk.column(0).unwrap();
        let expected = [0, 2, 4, 6, 8];
        for (i, &exp) in expected.iter().enumerate() {
            let val = col.get_i64(i).unwrap();
            assert_eq!(val, exp);
        }
    }

    #[test]
    fn test_range_negative_step() {
        // range(10, 0, -2) -> [10, 8, 6, 4, 2]
        let bind_data = RangeBindData::new_range(10, 0, -2);
        let gstate = RangeGlobalState::new(bind_data.start, bind_data.step, bind_data.cardinality);
        let mut lstate = RangeLocalState::new();

        let mut chunk = Chunk::initialize(&[LogicalType::BigInt], VECTOR_SIZE);

        let mut input = TableFunctionInput {
            bind_data: Some(&bind_data as &dyn TableFunctionBindData),
            local_state: Some(&mut lstate as &mut dyn LocalTableFunctionState),
            global_state: Some(&gstate as &dyn GlobalTableFunctionState),
        };

        let _result = range_function(&mut input, &mut chunk).unwrap();

        assert_eq!(chunk.size(), 5);

        let col = chunk.column(0).unwrap();
        let expected = [10, 8, 6, 4, 2];
        for (i, &exp) in expected.iter().enumerate() {
            let val = col.get_i64(i).unwrap();
            assert_eq!(val, exp);
        }
    }

    #[test]
    fn test_range_empty_result() {
        // range(10, 5, 1) -> empty
        let bind_data = RangeBindData::new_range(10, 5, 1);
        let gstate = RangeGlobalState::new(bind_data.start, bind_data.step, bind_data.cardinality);
        let mut lstate = RangeLocalState::new();

        let mut chunk = Chunk::initialize(&[LogicalType::BigInt], VECTOR_SIZE);

        let mut input = TableFunctionInput {
            bind_data: Some(&bind_data as &dyn TableFunctionBindData),
            local_state: Some(&mut lstate as &mut dyn LocalTableFunctionState),
            global_state: Some(&gstate as &dyn GlobalTableFunctionState),
        };

        let result = range_function(&mut input, &mut chunk).unwrap();

        assert_eq!(result, TableFunctionResult::Finished);
        assert_eq!(chunk.size(), 0);
    }

    #[test]
    fn test_range_large_result_batching() {
        // range(0, 5000) -> should be batched
        let bind_data = RangeBindData::new_range(0, 5000, 1);
        let gstate = RangeGlobalState::new(bind_data.start, bind_data.step, bind_data.cardinality);
        let mut lstate = RangeLocalState::new();

        let mut total_rows = 0;
        let mut batch_count = 0;

        loop {
            let mut chunk = Chunk::initialize(&[LogicalType::BigInt], VECTOR_SIZE);

            let mut input = TableFunctionInput {
                bind_data: Some(&bind_data as &dyn TableFunctionBindData),
                local_state: Some(&mut lstate as &mut dyn LocalTableFunctionState),
                global_state: Some(&gstate as &dyn GlobalTableFunctionState),
            };

            let result = range_function(&mut input, &mut chunk).unwrap();
            total_rows += chunk.size();
            batch_count += 1;

            if result == TableFunctionResult::Finished && chunk.size() == 0 {
                break;
            }
        }

        assert_eq!(total_rows, 5000);
        assert!(batch_count > 1); // Should have multiple batches
    }
}
