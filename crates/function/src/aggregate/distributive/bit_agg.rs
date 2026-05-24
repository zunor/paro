// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bitwise Aggregate Functions (bit_and, bit_or, bit_xor)
//!
//!
//!
//! ## Implementation Notes
//! - `bit_and(x)`: Bitwise AND of all values
//! - `bit_or(x)`: Bitwise OR of all values
//! - `bit_xor(x)`: Bitwise XOR of all values
//! - Returns NULL if no non-NULL values

use crate::aggregate::{AggregateFunction, AggregateFunctionSet, AggregateInputData};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

/// State for bitwise aggregation.
#[repr(C)]
struct BitState<T: Copy + Default> {
    value: T,
    is_set: bool,
}

// ============================================================================
// Macro for generating bitwise aggregate implementations
// ============================================================================

macro_rules! define_bit_and_impl {
    ($mod_name:ident, $type:ty) => {
        mod $mod_name {
            use super::*;

            type State = BitState<$type>;

            pub unsafe fn initialize(state: *mut u8) {
                let state = state as *mut State;
                (*state).value = <$type>::default();
                (*state).is_set = false;
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
                        if !(*state).is_set {
                            (*state).value = val;
                            (*state).is_set = true;
                        } else {
                            (*state).value &= val;
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
                        if !(*state).is_set {
                            (*state).value = val;
                            (*state).is_set = true;
                        } else {
                            (*state).value &= val;
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

                    if source_state.is_set {
                        if !target_state.is_set {
                            target_state.value = source_state.value;
                            target_state.is_set = true;
                        } else {
                            target_state.value &= source_state.value;
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

                    if !state.is_set {
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

macro_rules! define_bit_or_impl {
    ($mod_name:ident, $type:ty) => {
        mod $mod_name {
            use super::*;

            type State = BitState<$type>;

            pub unsafe fn initialize(state: *mut u8) {
                let state = state as *mut State;
                (*state).value = <$type>::default();
                (*state).is_set = false;
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
                        if !(*state).is_set {
                            (*state).value = val;
                            (*state).is_set = true;
                        } else {
                            (*state).value |= val;
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
                        if !(*state).is_set {
                            (*state).value = val;
                            (*state).is_set = true;
                        } else {
                            (*state).value |= val;
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

                    if source_state.is_set {
                        if !target_state.is_set {
                            target_state.value = source_state.value;
                            target_state.is_set = true;
                        } else {
                            target_state.value |= source_state.value;
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

                    if !state.is_set {
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

macro_rules! define_bit_xor_impl {
    ($mod_name:ident, $type:ty) => {
        mod $mod_name {
            use super::*;

            type State = BitState<$type>;

            pub unsafe fn initialize(state: *mut u8) {
                let state = state as *mut State;
                (*state).value = <$type>::default();
                (*state).is_set = false;
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
                        if !(*state).is_set {
                            (*state).value = val;
                            (*state).is_set = true;
                        } else {
                            (*state).value ^= val;
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
                        if !(*state).is_set {
                            (*state).value = val;
                            (*state).is_set = true;
                        } else {
                            (*state).value ^= val;
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

                    if source_state.is_set {
                        if !target_state.is_set {
                            target_state.value = source_state.value;
                            target_state.is_set = true;
                        } else {
                            target_state.value ^= source_state.value;
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

                    if !state.is_set {
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

// Define implementations for integer types
define_bit_and_impl!(bit_and_i32, i32);
define_bit_and_impl!(bit_and_i64, i64);

define_bit_or_impl!(bit_or_i32, i32);
define_bit_or_impl!(bit_or_i64, i64);

define_bit_xor_impl!(bit_xor_i32, i32);
define_bit_xor_impl!(bit_xor_i64, i64);

/// Get the BIT_AND aggregate function set.
pub fn get_bit_and_function() -> AggregateFunctionSet {
    let mut set = AggregateFunctionSet::new("bit_and".to_string());

    // Integer
    set.add_function(AggregateFunction::new(
        "bit_and".to_string(),
        vec![LogicalType::Integer],
        LogicalType::Integer,
        std::mem::size_of::<BitState<i32>>(),
        bit_and_i32::initialize,
        bit_and_i32::update,
        bit_and_i32::combine,
        bit_and_i32::finalize,
        Some(bit_and_i32::simple_update),
        None,
    ));

    // BigInt
    set.add_function(AggregateFunction::new(
        "bit_and".to_string(),
        vec![LogicalType::BigInt],
        LogicalType::BigInt,
        std::mem::size_of::<BitState<i64>>(),
        bit_and_i64::initialize,
        bit_and_i64::update,
        bit_and_i64::combine,
        bit_and_i64::finalize,
        Some(bit_and_i64::simple_update),
        None,
    ));

    set
}

/// Get the BIT_OR aggregate function set.
pub fn get_bit_or_function() -> AggregateFunctionSet {
    let mut set = AggregateFunctionSet::new("bit_or".to_string());

    // Integer
    set.add_function(AggregateFunction::new(
        "bit_or".to_string(),
        vec![LogicalType::Integer],
        LogicalType::Integer,
        std::mem::size_of::<BitState<i32>>(),
        bit_or_i32::initialize,
        bit_or_i32::update,
        bit_or_i32::combine,
        bit_or_i32::finalize,
        Some(bit_or_i32::simple_update),
        None,
    ));

    // BigInt
    set.add_function(AggregateFunction::new(
        "bit_or".to_string(),
        vec![LogicalType::BigInt],
        LogicalType::BigInt,
        std::mem::size_of::<BitState<i64>>(),
        bit_or_i64::initialize,
        bit_or_i64::update,
        bit_or_i64::combine,
        bit_or_i64::finalize,
        Some(bit_or_i64::simple_update),
        None,
    ));

    set
}

/// Get the BIT_XOR aggregate function set.
pub fn get_bit_xor_function() -> AggregateFunctionSet {
    let mut set = AggregateFunctionSet::new("bit_xor".to_string());

    // Integer
    set.add_function(AggregateFunction::new(
        "bit_xor".to_string(),
        vec![LogicalType::Integer],
        LogicalType::Integer,
        std::mem::size_of::<BitState<i32>>(),
        bit_xor_i32::initialize,
        bit_xor_i32::update,
        bit_xor_i32::combine,
        bit_xor_i32::finalize,
        Some(bit_xor_i32::simple_update),
        None,
    ));

    // BigInt
    set.add_function(AggregateFunction::new(
        "bit_xor".to_string(),
        vec![LogicalType::BigInt],
        LogicalType::BigInt,
        std::mem::size_of::<BitState<i64>>(),
        bit_xor_i64::initialize,
        bit_xor_i64::update,
        bit_xor_i64::combine,
        bit_xor_i64::finalize,
        Some(bit_xor_i64::simple_update),
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
    fn test_bit_and() {
        let func_set = get_bit_and_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            // 0b1111 & 0b1010 & 0b1100 = 0b1000 = 8
            let input = paro_common::test_utils::test_i32_vector_with_allocator(
                &[0b1111, 0b1010, 0b1100],
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

            assert!(!result.is_null(0));
            assert_eq!(result.get_flat::<i32>(0), 0b1000);
        }
    }

    #[test]
    fn test_bit_or() {
        let func_set = get_bit_or_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            // 0b0001 | 0b0010 | 0b0100 = 0b0111 = 7
            let input = paro_common::test_utils::test_i32_vector_with_allocator(
                &[0b0001, 0b0010, 0b0100],
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

            assert!(!result.is_null(0));
            assert_eq!(result.get_flat::<i32>(0), 0b0111);
        }
    }

    #[test]
    fn test_bit_xor() {
        let func_set = get_bit_xor_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            // 0b1010 ^ 0b1100 ^ 0b0011 = 0b0101 = 5
            let input = paro_common::test_utils::test_i32_vector_with_allocator(
                &[0b1010, 0b1100, 0b0011],
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

            assert!(!result.is_null(0));
            assert_eq!(result.get_flat::<i32>(0), 0b0101);
        }
    }

    #[test]
    fn test_bit_and_empty() {
        let func_set = get_bit_and_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            // No updates

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

            assert!(result.is_null(0));
        }
    }

    #[test]
    fn test_bit_xor_same_values() {
        let func_set = get_bit_xor_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            // XOR of same value twice = 0
            let input = paro_common::test_utils::test_i32_vector_with_allocator(
                &[42, 42],
                paro_common::test_utils::test_allocator(),
            );

            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input], &input_data, state_ptr, 2);
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

            assert!(!result.is_null(0));
            assert_eq!(result.get_flat::<i32>(0), 0);
        }
    }

    #[test]
    fn test_bit_and_with_nulls() {
        let func_set = get_bit_and_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            let mut input = paro_common::test_utils::test_i32_vector_with_allocator(
                &[0b1111, 0, 0b1010],
                paro_common::test_utils::test_allocator(),
            );
            input.set_null(1, true); // [0b1111, NULL, 0b1010]

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

            assert!(!result.is_null(0));
            // 0b1111 & 0b1010 = 0b1010 (NULL is ignored)
            assert_eq!(result.get_flat::<i32>(0), 0b1010);
        }
    }

    #[test]
    fn test_bit_or_bigint() {
        let func_set = get_bit_or_function();
        let (func, _) = func_set.bind(&[LogicalType::BigInt]).unwrap();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            let input = paro_common::test_utils::test_i64_vector_with_allocator(
                &[1i64 << 32, 1i64 << 16, 1],
                paro_common::test_utils::test_allocator(),
            );

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

            assert!(!result.is_null(0));
            let expected = (1i64 << 32) | (1i64 << 16) | 1;
            assert_eq!(result.get_flat::<i64>(0), expected);
        }
    }

    #[test]
    fn test_bit_and_combine() {
        let func_set = get_bit_and_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let mut arena = test_arena();

        let mut state1_buf = vec![0u8; func.state_size];
        let mut state2_buf = vec![0u8; func.state_size];
        let state1_ptr = state1_buf.as_mut_ptr();
        let state2_ptr = state2_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state1_ptr);
            (func.initialize)(state2_ptr);

            // State 1: 0b1111 & 0b1110 = 0b1110
            let input1 = paro_common::test_utils::test_i32_vector_with_allocator(
                &[0b1111, 0b1110],
                paro_common::test_utils::test_allocator(),
            );
            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input1], &input_data, state1_ptr, 2);
                }
            }

            // State 2: 0b1100 & 0b1010 = 0b1000
            let input2 = paro_common::test_utils::test_i32_vector_with_allocator(
                &[0b1100, 0b1010],
                paro_common::test_utils::test_allocator(),
            );
            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input2], &input_data, state2_ptr, 2);
                }
            }

            // Combine: 0b1110 & 0b1000 = 0b1000
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

            let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);
            result.set_count(1);

            {
                let input_data = preserve_input_data(&func, &mut arena);
                (func.finalize)(&target_states, &input_data, &mut result, 1);
            }

            assert!(!result.is_null(0));
            assert_eq!(result.get_flat::<i32>(0), 0b1000);
        }
    }
}
