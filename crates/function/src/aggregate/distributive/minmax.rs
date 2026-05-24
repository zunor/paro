// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Min/Max Aggregate Functions
//!
//!

use crate::aggregate::{AggregateFunction, AggregateFunctionSet, AggregateInputData};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

#[repr(C)]
struct MinMaxState<T> {
    value: T,
    is_null: bool,
}

macro_rules! define_minmax_impl {
    ($mod_name:ident, $type:ty, $cmp_fn:ident) => {
        mod $mod_name {
            use super::$cmp_fn as compare_op;
            use super::*;

            type State = MinMaxState<$type>;

            pub unsafe fn initialize(state: *mut u8) {
                let state = state as *mut State;
                (*state).value = <$type>::default();
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

                for i in 0..count {
                    if !input.is_null(i) {
                        let state_ptr = *state_ptrs.add(i);
                        let state = state_ptr as *mut State;

                        let val: $type = input.get_fixed(i);
                        if (*state).is_null || compare_op(val, (*state).value) {
                            (*state).value = val;
                            (*state).is_null = false;
                        }
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
                        let val: $type = input.get_fixed(i);
                        if (*state).is_null || compare_op(val, (*state).value) {
                            (*state).value = val;
                            (*state).is_null = false;
                        }
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
                        if target_state.is_null
                            || compare_op(source_state.value, target_state.value)
                        {
                            target_state.value = source_state.value;
                            target_state.is_null = false;
                        }
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
                let result_data = result.flat_data_mut::<$type>();

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

// Comparison functions
fn cmp_min<T: PartialOrd>(a: T, b: T) -> bool {
    a < b
}
fn cmp_max<T: PartialOrd>(a: T, b: T) -> bool {
    a > b
}

// Define distinct modules for each type and op
define_minmax_impl!(min_i32, i32, cmp_min);
define_minmax_impl!(max_i32, i32, cmp_max);

define_minmax_impl!(min_i64, i64, cmp_min);
define_minmax_impl!(max_i64, i64, cmp_max);

define_minmax_impl!(min_f64, f64, cmp_min);
define_minmax_impl!(max_f64, f64, cmp_max);

pub fn get_min_function() -> AggregateFunctionSet {
    let mut set = AggregateFunctionSet::new("min".to_string());

    // Integer
    set.add_function(AggregateFunction::new(
        "min".to_string(),
        vec![LogicalType::Integer],
        LogicalType::Integer,
        std::mem::size_of::<MinMaxState<i32>>(),
        min_i32::initialize,
        min_i32::update,
        min_i32::combine,
        min_i32::finalize,
        Some(min_i32::simple_update),
        None,
    ));

    // BigInt
    set.add_function(AggregateFunction::new(
        "min".to_string(),
        vec![LogicalType::BigInt],
        LogicalType::BigInt,
        std::mem::size_of::<MinMaxState<i64>>(),
        min_i64::initialize,
        min_i64::update,
        min_i64::combine,
        min_i64::finalize,
        Some(min_i64::simple_update),
        None,
    ));

    // Double
    set.add_function(AggregateFunction::new(
        "min".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        std::mem::size_of::<MinMaxState<f64>>(),
        min_f64::initialize,
        min_f64::update,
        min_f64::combine,
        min_f64::finalize,
        Some(min_f64::simple_update),
        None,
    ));

    set
}

pub fn get_max_function() -> AggregateFunctionSet {
    let mut set = AggregateFunctionSet::new("max".to_string());

    // Integer
    set.add_function(AggregateFunction::new(
        "max".to_string(),
        vec![LogicalType::Integer],
        LogicalType::Integer,
        std::mem::size_of::<MinMaxState<i32>>(),
        max_i32::initialize,
        max_i32::update,
        max_i32::combine,
        max_i32::finalize,
        Some(max_i32::simple_update),
        None,
    ));

    // BigInt
    set.add_function(AggregateFunction::new(
        "max".to_string(),
        vec![LogicalType::BigInt],
        LogicalType::BigInt,
        std::mem::size_of::<MinMaxState<i64>>(),
        max_i64::initialize,
        max_i64::update,
        max_i64::combine,
        max_i64::finalize,
        Some(max_i64::simple_update),
        None,
    ));

    // Double
    set.add_function(AggregateFunction::new(
        "max".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        std::mem::size_of::<MinMaxState<f64>>(),
        max_f64::initialize,
        max_f64::update,
        max_f64::combine,
        max_f64::finalize,
        Some(max_f64::simple_update),
        None,
    ));

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
    fn test_min_integer() {
        let func_set = get_min_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let mut arena = test_arena();

        // Initialize state
        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            let input = paro_common::test_utils::test_i32_vector_with_allocator(
                &[10, 5, 20],
                paro_common::test_utils::test_allocator(),
            );

            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input], &input_data, state_ptr, 3);
                }
            }

            let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);
            result.set_count(1);

            let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
            states.set_count(1);
            let states_ptr = states.flat_data_mut::<*mut u8>();
            *states_ptr = state_ptr;

            {
                let input_data = preserve_input_data(&func, &mut arena);
                (func.finalize)(&states, &input_data, &mut result, 1);
            }

            assert_eq!(result.get_flat::<i32>(0), 5);
        }
    }

    #[test]
    fn test_max_double() {
        let func_set = get_max_function();
        let (func, _) = func_set.bind(&[LogicalType::Double]).unwrap();
        let mut arena = test_arena();

        // Initialize state
        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            let input = paro_common::test_utils::test_f64_vector_with_allocator(
                &[1.5, 3.5, 2.5],
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

            assert_eq!(result.get_flat::<f64>(0), 3.5);
        }
    }
}
