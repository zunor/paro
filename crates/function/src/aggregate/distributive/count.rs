// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Count Aggregate Function
//!
//!

use crate::aggregate::{AggregateFunction, AggregateFunctionSet, AggregateInputData};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

struct CountFunction;

impl CountFunction {
    unsafe fn initialize(state: *mut u8) {
        *(state as *mut i64) = 0;
    }

    unsafe fn update_star(
        _inputs: &[&Vector],
        _input_data: &AggregateInputData,
        states: &Vector,
        count: usize,
    ) {
        let state_ptrs = states.flat_data::<*mut u8>();
        for i in 0..count {
            let state_ptr = *state_ptrs.add(i);
            *(state_ptr as *mut i64) += 1;
        }
    }

    unsafe fn simple_update_star(
        _inputs: &[&Vector],
        _input_data: &AggregateInputData,
        state: *mut u8,
        count: usize,
    ) {
        *(state as *mut i64) += count as i64;
    }

    unsafe fn update(
        inputs: &[&Vector],
        _input_data: &AggregateInputData,
        states: &Vector,
        count: usize,
    ) {
        let input = inputs[0];
        let state_ptrs = states.flat_data::<*mut u8>();

        // This is a simplified loop. In production we would use validity masks and vector types.
        for i in 0..count {
            if !input.is_null(i) {
                let state_ptr = *state_ptrs.add(i);
                *(state_ptr as *mut i64) += 1;
            }
        }
    }

    unsafe fn simple_update(
        inputs: &[&Vector],
        _input_data: &AggregateInputData,
        state: *mut u8,
        count: usize,
    ) {
        let input = inputs[0];
        let state_val = state as *mut i64;

        // Optimization: if no nulls, add count
        if input.validity().all_valid() {
            *state_val += count as i64;
            return;
        }

        // Slow path
        for i in 0..count {
            if !input.is_null(i) {
                *state_val += 1;
            }
        }
    }

    unsafe fn combine(
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

            let source_val = *(source_ptr as *const i64);
            *(target_ptr as *mut i64) += source_val;
        }
    }

    unsafe fn finalize(
        states: &Vector,
        _input_data: &AggregateInputData,
        result: &mut Vector,
        count: usize,
    ) {
        let state_ptrs = states.flat_data::<*mut u8>();
        let result_data = result.flat_data_mut::<i64>();

        for i in 0..count {
            let state_ptr = *state_ptrs.add(i);
            *result_data.add(i) = *(state_ptr as *const i64);
        }
    }
}

pub fn get_count_star_function() -> AggregateFunction {
    AggregateFunction::new(
        "count_star".to_string(),
        vec![],
        LogicalType::BigInt,
        std::mem::size_of::<i64>(),
        CountFunction::initialize,
        CountFunction::update_star,
        CountFunction::combine,
        CountFunction::finalize,
        Some(CountFunction::simple_update_star),
        None,
    )
}

pub fn get_count_function() -> AggregateFunctionSet {
    let mut set = AggregateFunctionSet::new("count".to_string());

    // Count accepts any type, returns BigInt.
    // For now we register for common types.
    // In full implementation we should have a wildcard or generic bind.
    let types = vec![
        LogicalType::Integer,
        LogicalType::BigInt,
        LogicalType::Double,
        LogicalType::Varchar,
    ];

    for t in types {
        set.add_function(AggregateFunction::new(
            "count".to_string(),
            vec![t],
            LogicalType::BigInt,
            std::mem::size_of::<i64>(),
            CountFunction::initialize,
            CountFunction::update,
            CountFunction::combine,
            CountFunction::finalize,
            Some(CountFunction::simple_update),
            None,
        ));
    }

    set
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::allocator::{default_allocator, ArenaAllocator};
    use paro_common::types::LogicalType;
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

    #[test]
    fn test_count_star() {
        let func = get_count_star_function();
        let mut arena = test_arena();

        // Initialize state
        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            // Update with count = 10
            // For simple_update logic:
            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[], &input_data, state_ptr, 10);
                }
            }

            // Finalize
            let mut result = paro_common::test_utils::test_vector(LogicalType::BigInt);
            result.set_count(1);

            // Construct states vector containing the pointer
            // Note: We cast pointer to i64 to store in flat vector.
            // In finalize, it reads *mut u8, so we need to ensure the data in vector is treated as pointers.
            // Our implementation uses `flat_data::<*mut u8>()`.
            // So the vector buffer should contain the pointer value (8 bytes on 64station).
            let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt); // Type doesn't really matter for data access
            states.set_count(1);
            let states_ptr = states.flat_data_mut::<*mut u8>();
            *states_ptr = state_ptr;

            {
                let input_data = preserve_input_data(&func, &mut arena);
                (func.finalize)(&states, &input_data, &mut result, 1);
            }

            assert_eq!(result.get_flat::<i64>(0), 10);
        }
    }

    #[test]
    fn test_count_col() {
        let func_set = get_count_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            // Input: [1, null, 2]
            let mut input = paro_common::test_utils::test_i32_vector_with_allocator(
                &[1, 0, 2],
                paro_common::test_utils::test_allocator(),
            );
            input.set_null(1, true); // Set 2nd element to null

            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input], &input_data, state_ptr, 3);
                }
            }

            let mut result = paro_common::test_utils::test_vector(LogicalType::BigInt);
            result.set_count(1);

            let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
            states.set_count(1);
            let states_ptr = states.flat_data_mut::<*mut u8>();
            *states_ptr = state_ptr;

            {
                let input_data = preserve_input_data(&func, &mut arena);
                (func.finalize)(&states, &input_data, &mut result, 1);
            }

            assert_eq!(result.get_flat::<i64>(0), 2);
        }
    }
}
