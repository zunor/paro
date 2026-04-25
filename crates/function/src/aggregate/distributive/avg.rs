// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Average Aggregate Function
//!
//!
//!
//! ## Implementation Notes
//! AVG is implemented as SUM/COUNT combination.
//! - Integer types: accumulate sum as i128, count as u64, finalize to f64
//! - Float types: accumulate sum as f64, count as u64, finalize to f64

use crate::aggregate::{AggregateFunction, AggregateFunctionSet, AggregateInputData};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

/// State for AVG aggregation on integer types.
/// Uses f64 accumulation to keep state alignment at 8 bytes.
#[repr(C)]
struct AvgStateInteger {
    sum: f64,
    count: u64,
}

/// State for AVG aggregation on float types.
#[repr(C)]
struct AvgStateFloat {
    sum: f64,
    count: u64,
}

// ============================================================================
// Integer AVG (i32 -> f64)
// ============================================================================

mod avg_i32 {
    use super::*;

    type State = AvgStateInteger;

    pub unsafe fn initialize(state: *mut u8) {
        let state = state as *mut State;
        (*state).sum = 0.0;
        (*state).count = 0;
    }

    pub unsafe fn update(
        inputs: &[&Vector],
        _input_data: &AggregateInputData,
        states: &Vector,
        count: usize,
    ) {
        let input = inputs[0];
        let state_ptrs = states.flat_data::<*mut u8>();

        for i in 0..count {
            if !input.is_null(i) {
                let state_ptr = *state_ptrs.add(i);
                let state = state_ptr as *mut State;

                let val: i32 = input.get_flat(i);
                (*state).sum += val as f64;
                (*state).count += 1;
            }
        }
    }

    pub unsafe fn simple_update(
        inputs: &[&Vector],
        _input_data: &AggregateInputData,
        state: *mut u8,
        count: usize,
    ) {
        let input = inputs[0];
        let state = state as *mut State;

        for i in 0..count {
            if !input.is_null(i) {
                let val: i32 = input.get_flat(i);
                (*state).sum += val as f64;
                (*state).count += 1;
            }
        }
    }

    pub unsafe fn combine(
        source: &Vector,
        target: &Vector,
        _input_data: &AggregateInputData,
        count: usize,
    ) {
        let source_ptrs = source.flat_data::<*mut u8>();
        let target_ptrs = target.flat_data::<*mut u8>();

        for i in 0..count {
            let source_ptr = *source_ptrs.add(i);
            let target_ptr = *target_ptrs.add(i);

            let source_state = &*(source_ptr as *const State);
            let target_state = &mut *(target_ptr as *mut State);

            target_state.sum += source_state.sum;
            target_state.count += source_state.count;
        }
    }

    pub unsafe fn finalize(
        states: &Vector,
        _input_data: &AggregateInputData,
        result: &mut Vector,
        count: usize,
    ) {
        let state_ptrs = states.flat_data::<*mut u8>();
        let result_data = result.flat_data_mut::<f64>();

        for i in 0..count {
            let state_ptr = *state_ptrs.add(i);
            let state = &*(state_ptr as *const State);

            if state.count == 0 {
                result.set_null(i, true);
            } else {
                result.set_null(i, false);
                *result_data.add(i) = state.sum / (state.count as f64);
            }
        }
    }
}

// ============================================================================
// BigInt AVG (i64 -> f64)
// ============================================================================

mod avg_i64 {
    use super::*;

    type State = AvgStateInteger;

    pub unsafe fn initialize(state: *mut u8) {
        let state = state as *mut State;
        (*state).sum = 0.0;
        (*state).count = 0;
    }

    pub unsafe fn update(
        inputs: &[&Vector],
        _input_data: &AggregateInputData,
        states: &Vector,
        count: usize,
    ) {
        let input = inputs[0];
        let state_ptrs = states.flat_data::<*mut u8>();

        for i in 0..count {
            if !input.is_null(i) {
                let state_ptr = *state_ptrs.add(i);
                let state = state_ptr as *mut State;

                let val: i64 = input.get_flat(i);
                (*state).sum += val as f64;
                (*state).count += 1;
            }
        }
    }

    pub unsafe fn simple_update(
        inputs: &[&Vector],
        _input_data: &AggregateInputData,
        state: *mut u8,
        count: usize,
    ) {
        let input = inputs[0];
        let state = state as *mut State;

        for i in 0..count {
            if !input.is_null(i) {
                let val: i64 = input.get_flat(i);
                (*state).sum += val as f64;
                (*state).count += 1;
            }
        }
    }

    pub unsafe fn combine(
        source: &Vector,
        target: &Vector,
        _input_data: &AggregateInputData,
        count: usize,
    ) {
        let source_ptrs = source.flat_data::<*mut u8>();
        let target_ptrs = target.flat_data::<*mut u8>();

        for i in 0..count {
            let source_ptr = *source_ptrs.add(i);
            let target_ptr = *target_ptrs.add(i);

            let source_state = &*(source_ptr as *const State);
            let target_state = &mut *(target_ptr as *mut State);

            target_state.sum += source_state.sum;
            target_state.count += source_state.count;
        }
    }

    pub unsafe fn finalize(
        states: &Vector,
        _input_data: &AggregateInputData,
        result: &mut Vector,
        count: usize,
    ) {
        let state_ptrs = states.flat_data::<*mut u8>();
        let result_data = result.flat_data_mut::<f64>();

        for i in 0..count {
            let state_ptr = *state_ptrs.add(i);
            let state = &*(state_ptr as *const State);

            if state.count == 0 {
                result.set_null(i, true);
            } else {
                result.set_null(i, false);
                *result_data.add(i) = state.sum / (state.count as f64);
            }
        }
    }
}

// ============================================================================
// Double AVG (f64 -> f64)
// ============================================================================

mod avg_f64 {
    use super::*;

    type State = AvgStateFloat;

    pub unsafe fn initialize(state: *mut u8) {
        let state = state as *mut State;
        (*state).sum = 0.0;
        (*state).count = 0;
    }

    pub unsafe fn update(
        inputs: &[&Vector],
        _input_data: &AggregateInputData,
        states: &Vector,
        count: usize,
    ) {
        let input = inputs[0];
        let state_ptrs = states.flat_data::<*mut u8>();

        for i in 0..count {
            if !input.is_null(i) {
                let state_ptr = *state_ptrs.add(i);
                let state = state_ptr as *mut State;

                let val: f64 = input.get_flat(i);
                (*state).sum += val;
                (*state).count += 1;
            }
        }
    }

    pub unsafe fn simple_update(
        inputs: &[&Vector],
        _input_data: &AggregateInputData,
        state: *mut u8,
        count: usize,
    ) {
        let input = inputs[0];
        let state = state as *mut State;

        for i in 0..count {
            if !input.is_null(i) {
                let val: f64 = input.get_flat(i);
                (*state).sum += val;
                (*state).count += 1;
            }
        }
    }

    pub unsafe fn combine(
        source: &Vector,
        target: &Vector,
        _input_data: &AggregateInputData,
        count: usize,
    ) {
        let source_ptrs = source.flat_data::<*mut u8>();
        let target_ptrs = target.flat_data::<*mut u8>();

        for i in 0..count {
            let source_ptr = *source_ptrs.add(i);
            let target_ptr = *target_ptrs.add(i);

            let source_state = &*(source_ptr as *const State);
            let target_state = &mut *(target_ptr as *mut State);

            target_state.sum += source_state.sum;
            target_state.count += source_state.count;
        }
    }

    pub unsafe fn finalize(
        states: &Vector,
        _input_data: &AggregateInputData,
        result: &mut Vector,
        count: usize,
    ) {
        let state_ptrs = states.flat_data::<*mut u8>();
        let result_data = result.flat_data_mut::<f64>();

        for i in 0..count {
            let state_ptr = *state_ptrs.add(i);
            let state = &*(state_ptr as *const State);

            if state.count == 0 {
                result.set_null(i, true);
            } else {
                result.set_null(i, false);
                *result_data.add(i) = state.sum / (state.count as f64);
            }
        }
    }
}

/// Get the AVG aggregate function set.
pub fn get_avg_function() -> AggregateFunctionSet {
    let mut set = AggregateFunctionSet::new("avg".to_string());

    // Integer -> Double
    set.add_function(AggregateFunction::new(
        "avg".to_string(),
        vec![LogicalType::Integer],
        LogicalType::Double,
        std::mem::size_of::<AvgStateInteger>(),
        avg_i32::initialize,
        avg_i32::update,
        avg_i32::combine,
        avg_i32::finalize,
        Some(avg_i32::simple_update),
        None,
    ));

    // BigInt -> Double
    set.add_function(AggregateFunction::new(
        "avg".to_string(),
        vec![LogicalType::BigInt],
        LogicalType::Double,
        std::mem::size_of::<AvgStateInteger>(),
        avg_i64::initialize,
        avg_i64::update,
        avg_i64::combine,
        avg_i64::finalize,
        Some(avg_i64::simple_update),
        None,
    ));

    // Double -> Double
    set.add_function(AggregateFunction::new(
        "avg".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        std::mem::size_of::<AvgStateFloat>(),
        avg_f64::initialize,
        avg_f64::update,
        avg_f64::combine,
        avg_f64::finalize,
        Some(avg_f64::simple_update),
        None,
    ));

    set
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::allocator::{default_allocator, ArenaAllocator};
    use std::sync::Arc;

    fn test_arena() -> ArenaAllocator {
        ArenaAllocator::new(Arc::new(default_allocator()))
    }

    fn preserve_input_data<'a>(
        func: &'a AggregateFunction,
        arena: &'a mut ArenaAllocator,
    ) -> AggregateInputData<'a> {
        AggregateInputData::new(
            func.bind_data.as_deref(),
            arena,
            crate::aggregate::AggregateCombineType::PreserveInput,
        )
    }

    fn destructive_input_data<'a>(
        func: &'a AggregateFunction,
        arena: &'a mut ArenaAllocator,
    ) -> AggregateInputData<'a> {
        AggregateInputData::new(
            func.bind_data.as_deref(),
            arena,
            crate::aggregate::AggregateCombineType::AllowDestructive,
        )
    }

    #[test]
    fn test_avg_integer() {
        let func_set = get_avg_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            let input = paro_common::test_utils::test_i32_vector_with_allocator(
                &[2, 4, 6, 8],
                paro_common::test_utils::test_allocator(),
            );

            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input], &input_data, state_ptr, 4);
                }
            }

            let mut result = paro_common::test_utils::test_vector(LogicalType::Double);
            result.set_count(1);

            let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
            states.set_count(1);
            let states_ptr = states.flat_data_mut::<*mut u8>();
            *states_ptr = state_ptr;

            {
                let input_data = preserve_input_data(&func, &mut arena);
                (func.finalize)(&states, &input_data, &mut result, 1);
            }

            assert!(!result.is_null(0));
            let avg: f64 = result.get_flat(0);
            assert!((avg - 5.0).abs() < 1e-10);
        }
    }

    #[test]
    fn test_avg_double() {
        let func_set = get_avg_function();
        let (func, _) = func_set.bind(&[LogicalType::Double]).unwrap();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            let input = paro_common::test_utils::test_f64_vector_with_allocator(
                &[1.5, 2.5, 3.5],
                paro_common::test_utils::test_allocator(),
            );

            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input], &input_data, state_ptr, 3);
                }
            }

            let mut result = paro_common::test_utils::test_vector(LogicalType::Double);
            result.set_count(1);

            let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
            states.set_count(1);
            let states_ptr = states.flat_data_mut::<*mut u8>();
            *states_ptr = state_ptr;

            {
                let input_data = preserve_input_data(&func, &mut arena);
                (func.finalize)(&states, &input_data, &mut result, 1);
            }

            assert!(!result.is_null(0));
            let avg: f64 = result.get_flat(0);
            assert!((avg - 2.5).abs() < 1e-10);
        }
    }

    #[test]
    fn test_avg_with_nulls() {
        let func_set = get_avg_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            let mut input = paro_common::test_utils::test_i32_vector_with_allocator(
                &[10, 0, 20],
                paro_common::test_utils::test_allocator(),
            );
            input.set_null(1, true); // [10, NULL, 20]

            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input], &input_data, state_ptr, 3);
                }
            }

            let mut result = paro_common::test_utils::test_vector(LogicalType::Double);
            result.set_count(1);

            let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
            states.set_count(1);
            let states_ptr = states.flat_data_mut::<*mut u8>();
            *states_ptr = state_ptr;

            {
                let input_data = preserve_input_data(&func, &mut arena);
                (func.finalize)(&states, &input_data, &mut result, 1);
            }

            assert!(!result.is_null(0));
            let avg: f64 = result.get_flat(0);
            assert!((avg - 15.0).abs() < 1e-10); // (10 + 20) / 2 = 15
        }
    }

    #[test]
    fn test_avg_empty() {
        let func_set = get_avg_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            // No updates - empty aggregation

            let mut result = paro_common::test_utils::test_vector(LogicalType::Double);
            result.set_count(1);

            let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
            states.set_count(1);
            let states_ptr = states.flat_data_mut::<*mut u8>();
            *states_ptr = state_ptr;

            {
                let input_data = preserve_input_data(&func, &mut arena);
                (func.finalize)(&states, &input_data, &mut result, 1);
            }

            assert!(result.is_null(0)); // AVG of empty set is NULL
        }
    }

    #[test]
    fn test_avg_combine() {
        let func_set = get_avg_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let mut arena = test_arena();

        let mut state1_buf = vec![0u8; func.state_size];
        let mut state2_buf = vec![0u8; func.state_size];
        let state1_ptr = state1_buf.as_mut_ptr();
        let state2_ptr = state2_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state1_ptr);
            (func.initialize)(state2_ptr);

            // State 1: [1, 2, 3] -> sum=6, count=3
            let input1 = paro_common::test_utils::test_i32_vector_with_allocator(
                &[1, 2, 3],
                paro_common::test_utils::test_allocator(),
            );
            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input1], &input_data, state1_ptr, 3);
                }
            }

            // State 2: [4, 5] -> sum=9, count=2
            let input2 = paro_common::test_utils::test_i32_vector_with_allocator(
                &[4, 5],
                paro_common::test_utils::test_allocator(),
            );
            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input2], &input_data, state2_ptr, 2);
                }
            }

            // Combine state1 into state2
            let mut source_states = paro_common::test_utils::test_vector(LogicalType::BigInt);
            source_states.set_count(1);
            let source_ptr = source_states.flat_data_mut::<*mut u8>();
            *source_ptr = state1_ptr;

            let mut target_states = paro_common::test_utils::test_vector(LogicalType::BigInt);
            target_states.set_count(1);
            let target_ptr = target_states.flat_data_mut::<*mut u8>();
            *target_ptr = state2_ptr;

            {
                let input_data = destructive_input_data(&func, &mut arena);
                (func.combine)(&source_states, &target_states, &input_data, 1);
            }

            // Finalize combined state
            let mut result = paro_common::test_utils::test_vector(LogicalType::Double);
            result.set_count(1);

            {
                let input_data = preserve_input_data(&func, &mut arena);
                (func.finalize)(&target_states, &input_data, &mut result, 1);
            }

            assert!(!result.is_null(0));
            let avg: f64 = result.get_flat(0);
            // (1+2+3+4+5) / 5 = 15 / 5 = 3.0
            assert!((avg - 3.0).abs() < 1e-10);
        }
    }
}
