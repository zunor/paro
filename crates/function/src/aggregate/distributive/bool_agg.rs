// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Boolean Aggregate Functions (bool_and, bool_or)
//!
//!
//!
//! ## Implementation Notes
//! - `bool_and(x)`: Returns TRUE if all values are TRUE, FALSE if any is FALSE, NULL if empty
//! - `bool_or(x)`: Returns TRUE if any value is TRUE, FALSE if all are FALSE, NULL if empty

use crate::aggregate::{AggregateFunction, AggregateInputData, AggregateStateInput};
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

/// State for boolean aggregation.
#[repr(C)]
struct BoolState {
    value: bool,
    is_empty: bool,
}

// ============================================================================
// BOOL_AND
// ============================================================================

mod bool_and_impl {
    use super::*;

    type State = BoolState;

    pub unsafe fn initialize(state: *mut u8) {
        let state = state as *mut State;
        (*state).value = true; // Identity for AND
        (*state).is_empty = true;
    }

    pub unsafe fn update(
        inputs: &[&Vector],
        _input_data: &AggregateInputData,
        states: &AggregateStateInput,
        count: usize,
    ) {
        let input = inputs[0];
        for i in 0..count {
            if !input.is_null(i) {
                let state_ptr = states.state_ptr(i);
                let state = state_ptr as *mut State;

                let val: bool = input.get_fixed(i);
                (*state).value = (*state).value && val;
                (*state).is_empty = false;
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
                let val: bool = input.get_fixed(i);
                (*state).value = (*state).value && val;
                (*state).is_empty = false;
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

            if !source_state.is_empty {
                target_state.value = target_state.value && source_state.value;
                target_state.is_empty = target_state.is_empty && source_state.is_empty;
            }
        }
    }

    pub unsafe fn finalize(
        states: &Vector,
        _input_data: &AggregateInputData,
        result: &mut Vector,
        count: usize,
    ) -> Result<()> {
        let state_ptrs = states.flat_data::<*mut u8>();
        let result_data = result.flat_data_mut::<bool>();

        for i in 0..count {
            let state_ptr = *state_ptrs.add(i);
            let state = &*(state_ptr as *const State);

            if state.is_empty {
                result.set_null(i, true);
            } else {
                result.set_null(i, false);
                *result_data.add(i) = state.value;
            }
        }
        Ok(())
    }
}

// ============================================================================
// BOOL_OR
// ============================================================================

mod bool_or_impl {
    use super::*;

    type State = BoolState;

    pub unsafe fn initialize(state: *mut u8) {
        let state = state as *mut State;
        (*state).value = false; // Identity for OR
        (*state).is_empty = true;
    }

    pub unsafe fn update(
        inputs: &[&Vector],
        _input_data: &AggregateInputData,
        states: &AggregateStateInput,
        count: usize,
    ) {
        let input = inputs[0];
        for i in 0..count {
            if !input.is_null(i) {
                let state_ptr = states.state_ptr(i);
                let state = state_ptr as *mut State;

                let val: bool = input.get_fixed(i);
                (*state).value = (*state).value || val;
                (*state).is_empty = false;
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
                let val: bool = input.get_fixed(i);
                (*state).value = (*state).value || val;
                (*state).is_empty = false;
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

            if !source_state.is_empty {
                target_state.value = target_state.value || source_state.value;
                target_state.is_empty = target_state.is_empty && source_state.is_empty;
            }
        }
    }

    pub unsafe fn finalize(
        states: &Vector,
        _input_data: &AggregateInputData,
        result: &mut Vector,
        count: usize,
    ) -> Result<()> {
        let state_ptrs = states.flat_data::<*mut u8>();
        let result_data = result.flat_data_mut::<bool>();

        for i in 0..count {
            let state_ptr = *state_ptrs.add(i);
            let state = &*(state_ptr as *const State);

            if state.is_empty {
                result.set_null(i, true);
            } else {
                result.set_null(i, false);
                *result_data.add(i) = state.value;
            }
        }
        Ok(())
    }
}

/// Get the BOOL_AND aggregate function.
pub fn get_bool_and_function() -> AggregateFunction {
    AggregateFunction::new(
        "bool_and".to_string(),
        vec![LogicalType::Boolean],
        LogicalType::Boolean,
        std::mem::size_of::<BoolState>(),
        bool_and_impl::initialize,
        bool_and_impl::update,
        bool_and_impl::combine,
        bool_and_impl::finalize,
        Some(bool_and_impl::simple_update),
        None,
    )
}

/// Get the BOOL_OR aggregate function.
pub fn get_bool_or_function() -> AggregateFunction {
    AggregateFunction::new(
        "bool_or".to_string(),
        vec![LogicalType::Boolean],
        LogicalType::Boolean,
        std::mem::size_of::<BoolState>(),
        bool_or_impl::initialize,
        bool_or_impl::update,
        bool_or_impl::combine,
        bool_or_impl::finalize,
        Some(bool_or_impl::simple_update),
        None,
    )
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

    #[test]
    fn test_bool_and_all_true() {
        let func = get_bool_and_function();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            let input = paro_common::test_utils::test_bool_vector_with_allocator(
                &[true, true, true],
                paro_common::test_utils::test_allocator(),
            );

            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input], &input_data, state_ptr, 3);
                }
            }

            let mut result = paro_common::test_utils::test_vector(LogicalType::Boolean);
            result.set_count(1);

            let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
            states.set_count(1);
            let states_ptr = states.flat_data_mut::<*mut u8>();
            *states_ptr = state_ptr;

            {
                let input_data = preserve_input_data(&func, &mut arena);
                (func.finalize)(&states, &input_data, &mut result, 1).unwrap();
            }

            assert!(!result.is_null(0));
            assert!(result.get_flat::<bool>(0));
        }
    }

    #[test]
    fn test_bool_and_one_false() {
        let func = get_bool_and_function();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            let input = paro_common::test_utils::test_bool_vector_with_allocator(
                &[true, false, true],
                paro_common::test_utils::test_allocator(),
            );

            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input], &input_data, state_ptr, 3);
                }
            }

            let mut result = paro_common::test_utils::test_vector(LogicalType::Boolean);
            result.set_count(1);

            let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
            states.set_count(1);
            let states_ptr = states.flat_data_mut::<*mut u8>();
            *states_ptr = state_ptr;

            {
                let input_data = preserve_input_data(&func, &mut arena);
                (func.finalize)(&states, &input_data, &mut result, 1).unwrap();
            }

            assert!(!result.is_null(0));
            assert!(!result.get_flat::<bool>(0));
        }
    }

    #[test]
    fn test_bool_and_empty() {
        let func = get_bool_and_function();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            // No updates

            let mut result = paro_common::test_utils::test_vector(LogicalType::Boolean);
            result.set_count(1);

            let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
            states.set_count(1);
            let states_ptr = states.flat_data_mut::<*mut u8>();
            *states_ptr = state_ptr;

            {
                let input_data = preserve_input_data(&func, &mut arena);
                (func.finalize)(&states, &input_data, &mut result, 1).unwrap();
            }

            assert!(result.is_null(0));
        }
    }

    #[test]
    fn test_bool_or_all_false() {
        let func = get_bool_or_function();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            let input = paro_common::test_utils::test_bool_vector_with_allocator(
                &[false, false, false],
                paro_common::test_utils::test_allocator(),
            );

            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input], &input_data, state_ptr, 3);
                }
            }

            let mut result = paro_common::test_utils::test_vector(LogicalType::Boolean);
            result.set_count(1);

            let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
            states.set_count(1);
            let states_ptr = states.flat_data_mut::<*mut u8>();
            *states_ptr = state_ptr;

            {
                let input_data = preserve_input_data(&func, &mut arena);
                (func.finalize)(&states, &input_data, &mut result, 1).unwrap();
            }

            assert!(!result.is_null(0));
            assert!(!result.get_flat::<bool>(0));
        }
    }

    #[test]
    fn test_bool_or_one_true() {
        let func = get_bool_or_function();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            let input = paro_common::test_utils::test_bool_vector_with_allocator(
                &[false, true, false],
                paro_common::test_utils::test_allocator(),
            );

            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input], &input_data, state_ptr, 3);
                }
            }

            let mut result = paro_common::test_utils::test_vector(LogicalType::Boolean);
            result.set_count(1);

            let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
            states.set_count(1);
            let states_ptr = states.flat_data_mut::<*mut u8>();
            *states_ptr = state_ptr;

            {
                let input_data = preserve_input_data(&func, &mut arena);
                (func.finalize)(&states, &input_data, &mut result, 1).unwrap();
            }

            assert!(!result.is_null(0));
            assert!(result.get_flat::<bool>(0));
        }
    }

    #[test]
    fn test_bool_or_empty() {
        let func = get_bool_or_function();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            // No updates

            let mut result = paro_common::test_utils::test_vector(LogicalType::Boolean);
            result.set_count(1);

            let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
            states.set_count(1);
            let states_ptr = states.flat_data_mut::<*mut u8>();
            *states_ptr = state_ptr;

            {
                let input_data = preserve_input_data(&func, &mut arena);
                (func.finalize)(&states, &input_data, &mut result, 1).unwrap();
            }

            assert!(result.is_null(0));
        }
    }

    #[test]
    fn test_bool_and_with_nulls() {
        let func = get_bool_and_function();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            let mut input = paro_common::test_utils::test_bool_vector_with_allocator(
                &[true, false, true],
                paro_common::test_utils::test_allocator(),
            );
            input.set_null(1, true); // [true, NULL, true]

            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input], &input_data, state_ptr, 3);
                }
            }

            let mut result = paro_common::test_utils::test_vector(LogicalType::Boolean);
            result.set_count(1);

            let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
            states.set_count(1);
            let states_ptr = states.flat_data_mut::<*mut u8>();
            *states_ptr = state_ptr;

            {
                let input_data = preserve_input_data(&func, &mut arena);
                (func.finalize)(&states, &input_data, &mut result, 1).unwrap();
            }

            assert!(!result.is_null(0));
            // true AND true = true (NULL is ignored)
            assert!(result.get_flat::<bool>(0));
        }
    }
}
