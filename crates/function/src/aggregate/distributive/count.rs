// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Count Aggregate Function
//!
//!

use crate::aggregate::{
    AggregateDirectUpdate, AggregateEmptyInput, AggregateFunction, AggregateFunctionSet,
    AggregateInputData, AggregateSingletonMerge, AggregateStateInput,
};
use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

struct CountFunction;

fn count_non_null_input(_source: &AggregateFunction) -> Option<AggregateFunction> {
    Some(get_count_star_function())
}

impl CountFunction {
    unsafe fn initialize(state: *mut u8) {
        *(state as *mut i64) = 0;
    }

    unsafe fn update_star(
        _inputs: &[&Vector],
        _input_data: &AggregateInputData,
        states: &AggregateStateInput,
        count: usize,
    ) {
        for i in 0..count {
            let state_ptr = states.state_ptr(i);
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
        states: &AggregateStateInput,
        count: usize,
    ) {
        let input = inputs[0];
        // This is a simplified loop. In production we would use validity masks and vector types.
        for i in 0..count {
            if !input.is_null(i) {
                let state_ptr = states.state_ptr(i);
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

    unsafe fn update_distinct_runs(
        inputs: &[&Vector],
        _input_data: &AggregateInputData,
        states: &AggregateStateInput,
        run_starts: &[u32],
        count: usize,
    ) {
        let input = inputs[0];
        let all_valid = input.validity().all_valid();
        for (run_idx, &start) in run_starts.iter().enumerate() {
            let start = start as usize;
            let end = run_starts
                .get(run_idx + 1)
                .map_or(count, |next| *next as usize);
            let increment = if all_valid {
                end - start
            } else {
                (start..end).filter(|&row| !input.is_null(row)).count()
            };
            let state = states.state_ptr(run_idx) as *mut i64;
            *state += increment as i64;
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
    ) -> Result<()> {
        let state_ptrs = states.flat_data::<*mut u8>();
        let result_data = result.flat_data_mut::<i64>();

        for i in 0..count {
            let state_ptr = *state_ptrs.add(i);
            *result_data.add(i) = *(state_ptr as *const i64);
        }
        Ok(())
    }
}

struct CountPartialMergeFunction;

impl CountPartialMergeFunction {
    unsafe fn update(
        inputs: &[&Vector],
        _input_data: &AggregateInputData,
        states: &AggregateStateInput,
        count: usize,
    ) {
        let input = inputs[0];
        for row_idx in 0..count {
            if !input.is_null(row_idx) {
                let state = states.state_ptr(row_idx) as *mut i64;
                *state += input.get_fixed::<i64>(row_idx);
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
        let state = state as *mut i64;
        for row_idx in 0..count {
            if !input.is_null(row_idx) {
                *state += input.get_fixed::<i64>(row_idx);
            }
        }
    }
}

fn count_partial_merge(_source: &AggregateFunction) -> Option<AggregateFunction> {
    let function = AggregateFunction::new(
        "count_partial_merge".to_string(),
        vec![LogicalType::BigInt],
        LogicalType::BigInt,
        std::mem::size_of::<i64>(),
        CountFunction::initialize,
        CountPartialMergeFunction::update,
        CountFunction::combine,
        CountFunction::finalize,
        Some(CountPartialMergeFunction::simple_update),
        None,
    )
    .with_empty_input(AggregateEmptyInput::NonNull)
    .with_partial_merge(count_partial_merge)
    .with_singleton_merge(AggregateSingletonMerge::InputOr(Value::BigInt(0)));
    // SAFETY: partial COUNT state is one inline i64 with no external ownership.
    Some(unsafe { function.with_trivially_copyable_state() })
}

pub fn get_count_star_function() -> AggregateFunction {
    let function = AggregateFunction::new(
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
    .with_empty_input(AggregateEmptyInput::NonNull);
    // SAFETY: COUNT state is one inline i64 with no external ownership.
    unsafe { function.with_trivially_copyable_state() }
        .with_partial_merge(count_partial_merge)
        .with_direct_update(AggregateDirectUpdate::CountStar)
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
        let function = AggregateFunction::new(
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
        )
        .with_empty_input(AggregateEmptyInput::NonNull)
        .with_non_null_input(count_non_null_input);
        // SAFETY: COUNT state is one inline i64 with no external ownership.
        let function = unsafe { function.with_trivially_copyable_state() }
            .with_partial_merge(count_partial_merge);
        set.add_function(function.with_distinct_run_update(CountFunction::update_distinct_runs));
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
                (func.finalize)(&states, &input_data, &mut result, 1).unwrap();
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
                (func.finalize)(&states, &input_data, &mut result, 1).unwrap();
            }

            assert_eq!(result.get_flat::<i64>(0), 2);
        }
    }

    #[test]
    fn finalized_count_partials_merge_without_widening_and_ignore_nulls() {
        let (count, _) = get_count_function().bind(&[LogicalType::BigInt]).unwrap();
        let merge = count.partial_merge_function().unwrap();
        assert_eq!(merge.return_type, LogicalType::BigInt);
        let mut arena = test_arena();
        let mut state = vec![0u8; merge.state_size];
        let state_ptr = state.as_mut_ptr();
        let mut input = paro_common::test_utils::test_i64_vector(&[2, 0, 5]);
        input.set_null(1, true);

        unsafe {
            (merge.initialize)(state_ptr);
            let input_data = preserve_input_data(&merge, &mut arena);
            merge.simple_update.unwrap()(&[&input], &input_data, state_ptr, 3);
            assert_eq!(*(state_ptr as *const i64), 7);
        }
    }
}
