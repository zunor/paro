// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Sum Aggregate Function
//!
//!

use crate::aggregate::{AggregateFunction, AggregateFunctionSet, AggregateInputData};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

#[repr(C)]
struct SumState<T> {
    value: T,
    is_null: bool,
}

// Function generator macro
macro_rules! define_sum_impl {
    ($mod_name:ident, $input_type:ty, $state_type:ty) => {
        mod $mod_name {
            use super::*;

            type State = SumState<$state_type>;

            pub unsafe fn initialize(state: *mut u8) {
                let state = state as *mut State;
                (*state).value = <$state_type>::default();
                (*state).is_null = true;
            }

            pub unsafe fn update(
                inputs: &[&Vector],
                _input_data: &AggregateInputData,
                states: &Vector,
                count: usize,
            ) {
                let input = inputs[0];
                let state_ptrs = states.flat_data::<*mut u8>();

                // Optimized path for Flat vectors
                /*
                // In a real implementation we would check vector type and use specific path
                 */

                for i in 0..count {
                    if !input.is_null(i) {
                        let state_ptr = *state_ptrs.add(i);
                        let state = state_ptr as *mut State;

                        let val: $input_type = input.get_flat(i);
                        (*state).value += val as $state_type;
                        (*state).is_null = false;
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
                        let val: $input_type = input.get_flat(i);
                        (*state).value += val as $state_type;
                        (*state).is_null = false;
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

                    if !source_state.is_null {
                        target_state.value += source_state.value;
                        target_state.is_null = false;
                    }
                }
            }

            pub unsafe fn finalize(
                states: &Vector,
                _input_data: &AggregateInputData,
                result: &mut Vector,
                count: usize,
            ) {
                let state_ptrs = states.flat_data::<*mut u8>();
                let result_data = result.flat_data_mut::<$state_type>();

                for i in 0..count {
                    let state_ptr = *state_ptrs.add(i);
                    let state = &*(state_ptr as *const State);

                    if state.is_null {
                        result.set_null(i, true);
                    } else {
                        result.set_null(i, false);
                        *result_data.add(i) = state.value;
                    }
                }
            }
        }
    };
}

// Define implementations for types
define_sum_impl!(sum_i32, i32, i64); // Integer -> BigInt
define_sum_impl!(sum_i64, i64, i128); // BigInt -> HugeInt
define_sum_impl!(sum_f64, f64, f64); // Double -> Double

pub fn get_sum_function() -> AggregateFunctionSet {
    let mut set = AggregateFunctionSet::new("sum".to_string());

    // Integer -> BigInt
    set.add_function(AggregateFunction::new(
        "sum".to_string(),
        vec![LogicalType::Integer],
        LogicalType::BigInt,
        std::mem::size_of::<SumState<i64>>(),
        sum_i32::initialize,
        sum_i32::update,
        sum_i32::combine,
        sum_i32::finalize,
        Some(sum_i32::simple_update),
        None,
    ));

    // BigInt -> HugeInt
    set.add_function(AggregateFunction::new(
        "sum".to_string(),
        vec![LogicalType::BigInt],
        LogicalType::HugeInt,
        std::mem::size_of::<SumState<i128>>(),
        sum_i64::initialize,
        sum_i64::update,
        sum_i64::combine,
        sum_i64::finalize,
        Some(sum_i64::simple_update),
        None,
    ));

    // Double -> Double
    set.add_function(AggregateFunction::new(
        "sum".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        std::mem::size_of::<SumState<f64>>(),
        sum_f64::initialize,
        sum_f64::update,
        sum_f64::combine,
        sum_f64::finalize,
        Some(sum_f64::simple_update),
        None,
    ));

    set
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::allocator::{default_allocator, ArenaAllocator};
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
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
    fn test_sum_integer() {
        let func_set = get_sum_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            let input = Vector::from_i32(&[1, 2, 3, 4]);

            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input], &input_data, state_ptr, 4);
                }
            }

            let mut result = Vector::new(LogicalType::BigInt);
            result.set_count(1);

            let mut states = Vector::new(LogicalType::BigInt);
            states.set_count(1);
            let states_ptr = states.flat_data_mut::<*mut u8>();
            *states_ptr = state_ptr;

            {
                let input_data = preserve_input_data(&func, &mut arena);
                (func.finalize)(&states, &input_data, &mut result, 1);
            }

            assert_eq!(result.get_flat::<i64>(0), 10);
            assert!(!result.is_null(0));
        }
    }

    #[test]
    fn test_sum_null() {
        let func_set = get_sum_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            let mut input = Vector::from_i32(&[1, 0]);
            input.set_null(1, true); // [1, NULL]

            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input], &input_data, state_ptr, 2);
                }
            }

            let mut result = Vector::new(LogicalType::BigInt);
            result.set_count(1);

            let mut states = Vector::new(LogicalType::BigInt);
            states.set_count(1);
            let states_ptr = states.flat_data_mut::<*mut u8>();
            *states_ptr = state_ptr;

            {
                let input_data = preserve_input_data(&func, &mut arena);
                (func.finalize)(&states, &input_data, &mut result, 1);
            }

            assert_eq!(result.get_flat::<i64>(0), 1);
            assert!(!result.is_null(0));
        }
    }
}
