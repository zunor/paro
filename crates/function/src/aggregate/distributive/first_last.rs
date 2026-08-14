// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! First/Last/Any Value Aggregate Functions
//!
//!
//!
//! ## Implementation Notes
//! - `first(x)`: Returns the first non-NULL value
//! - `last(x)`: Returns the last non-NULL value
//! - `any_value(x)` / `arbitrary(x)`: Returns any value (implementation uses first)

use crate::aggregate::{
    AggregateCombineType, AggregateEmptyInput, AggregateFunction, AggregateFunctionSet,
    AggregateInputData, AggregateStateInput,
};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use std::ptr;

/// State for first/last aggregation on fixed-size types.
#[repr(C)]
struct FirstState<T: Copy + Default> {
    value: T,
    is_set: bool,
    is_null: bool,
}

impl<T: Copy + Default> Default for FirstState<T> {
    fn default() -> Self {
        Self {
            value: T::default(),
            is_set: false,
            is_null: false,
        }
    }
}

// ============================================================================
// Macro for generating first/last implementations
// ============================================================================

macro_rules! define_first_impl {
    ($mod_name:ident, $type:ty) => {
        mod $mod_name {
            use super::*;

            type State = FirstState<$type>;

            pub unsafe fn initialize(state: *mut u8) {
                let state = state as *mut State;
                (*state).value = <$type>::default();
                (*state).is_set = false;
                (*state).is_null = false;
            }

            pub unsafe fn update(
                inputs: &[&Vector],
                _input_data: &AggregateInputData,
                states: &AggregateStateInput,
                count: usize,
            ) {
                let input = inputs[0];
                for i in 0..count {
                    let state_ptr = states.state_ptr(i);
                    let state = state_ptr as *mut State;

                    // First: only set if not already set
                    if !(*state).is_set {
                        if input.is_null(i) {
                            // Skip NULLs for first (SKIP_NULLS = true)
                        } else {
                            (*state).is_set = true;
                            (*state).is_null = false;
                            (*state).value = input.get_fixed(i);
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
                    if !(*state).is_set {
                        if input.is_null(i) {
                            // Skip NULLs
                        } else {
                            (*state).is_set = true;
                            (*state).is_null = false;
                            (*state).value = input.get_fixed(i);
                            return; // Early exit for first
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

                    // First: keep target if already set
                    if !target_state.is_set && source_state.is_set {
                        target_state.is_set = true;
                        target_state.is_null = source_state.is_null;
                        target_state.value = source_state.value;
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
                let result_data = result.flat_data_mut::<$type>();

                for i in 0..count {
                    let state_ptr = *state_ptrs.add(i);
                    let state = &*(state_ptr as *const State);

                    if !state.is_set || state.is_null {
                        result.set_null(i, true);
                    } else {
                        result.set_null(i, false);
                        *result_data.add(i) = state.value;
                    }
                }
                Ok(())
            }
        }
    };
}

macro_rules! define_last_impl {
    ($mod_name:ident, $type:ty) => {
        mod $mod_name {
            use super::*;

            type State = FirstState<$type>;

            pub unsafe fn initialize(state: *mut u8) {
                let state = state as *mut State;
                (*state).value = <$type>::default();
                (*state).is_set = false;
                (*state).is_null = false;
            }

            pub unsafe fn update(
                inputs: &[&Vector],
                _input_data: &AggregateInputData,
                states: &AggregateStateInput,
                count: usize,
            ) {
                let input = inputs[0];
                for i in 0..count {
                    let state_ptr = states.state_ptr(i);
                    let state = state_ptr as *mut State;

                    // Last: always update (overwrite previous)
                    if !input.is_null(i) {
                        (*state).is_set = true;
                        (*state).is_null = false;
                        (*state).value = input.get_fixed(i);
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
                        (*state).is_set = true;
                        (*state).is_null = false;
                        (*state).value = input.get_fixed(i);
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

                    // Last: always take source if set
                    if source_state.is_set {
                        target_state.is_set = true;
                        target_state.is_null = source_state.is_null;
                        target_state.value = source_state.value;
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
                let result_data = result.flat_data_mut::<$type>();

                for i in 0..count {
                    let state_ptr = *state_ptrs.add(i);
                    let state = &*(state_ptr as *const State);

                    if !state.is_set || state.is_null {
                        result.set_null(i, true);
                    } else {
                        result.set_null(i, false);
                        *result_data.add(i) = state.value;
                    }
                }
                Ok(())
            }
        }
    };
}

// Define implementations for common types
define_first_impl!(first_i32, i32);
define_first_impl!(first_i64, i64);
define_first_impl!(first_f64, f64);

define_last_impl!(last_i32, i32);
define_last_impl!(last_i64, i64);
define_last_impl!(last_f64, f64);

#[derive(Default)]
struct FirstStringState {
    value: String,
    is_set: bool,
}

mod first_string {
    use super::*;

    type State = FirstStringState;

    pub unsafe fn initialize(state: *mut u8) {
        ptr::write(state.cast::<State>(), State::default());
    }

    pub unsafe fn update(
        inputs: &[&Vector],
        _input_data: &AggregateInputData,
        states: &AggregateStateInput,
        count: usize,
    ) {
        let input = inputs[0];
        for row in 0..count {
            let state = &mut *states.state_ptr(row).cast::<State>();
            if !state.is_set {
                if let Some(value) = input.get_string(row) {
                    state.value.clear();
                    state.value.push_str(value);
                    state.is_set = true;
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
        let state = &mut *state.cast::<State>();
        if state.is_set {
            return;
        }
        let input = inputs[0];
        for row in 0..count {
            if let Some(value) = input.get_string(row) {
                state.value.clear();
                state.value.push_str(value);
                state.is_set = true;
                return;
            }
        }
    }

    pub unsafe fn combine(
        source: &Vector,
        target: &Vector,
        input_data: &AggregateInputData,
        count: usize,
    ) {
        let source_ptrs = source.flat_data::<*mut u8>();
        let target_ptrs = target.flat_data::<*mut u8>();
        for row in 0..count {
            let source = &mut *(*source_ptrs.add(row)).cast::<State>();
            let target = &mut *(*target_ptrs.add(row)).cast::<State>();
            if target.is_set || !source.is_set {
                continue;
            }
            if input_data.combine_type == AggregateCombineType::AllowDestructive {
                std::mem::swap(target, source);
            } else {
                target.value.clone_from(&source.value);
                target.is_set = true;
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
        for row in 0..count {
            let state = &*(*state_ptrs.add(row)).cast::<State>();
            if state.is_set {
                result.set_null(row, false);
                result.set_string(row, &state.value);
            } else {
                result.set_null(row, true);
            }
        }
        Ok(())
    }

    pub unsafe fn destructor(states: &Vector, _input_data: &AggregateInputData, count: usize) {
        let state_ptrs = states.flat_data::<*mut u8>();
        for row in 0..count {
            ptr::drop_in_place((*state_ptrs.add(row)).cast::<State>());
        }
    }

    pub unsafe fn serialize(
        state: *const u8,
        _input_data: &AggregateInputData,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        let state = &*state.cast::<State>();
        output.push(u8::from(state.is_set));
        let length = u64::try_from(state.value.len())
            .map_err(|_| paro_error::internal("FIRST string state exceeds u64"))?;
        output.extend_from_slice(&length.to_le_bytes());
        output.extend_from_slice(state.value.as_bytes());
        Ok(())
    }

    pub unsafe fn deserialize(
        input: &[u8],
        _input_data: &AggregateInputData,
        state: *mut u8,
    ) -> Result<()> {
        let (&marker, payload) = input
            .split_first()
            .ok_or_else(|| paro_error::internal("Truncated FIRST string state"))?;
        if payload.len() < 8 {
            return Err(paro_error::internal("Truncated FIRST string state length"));
        }
        let length = usize::try_from(u64::from_le_bytes(
            payload[..8].try_into().expect("eight length bytes"),
        ))
        .map_err(|_| paro_error::internal("FIRST string state length exceeds usize"))?;
        let bytes = payload
            .get(8..)
            .filter(|bytes| bytes.len() == length)
            .ok_or_else(|| paro_error::internal("Invalid FIRST string state length"))?;
        let value = String::from_utf8(bytes.to_vec()).map_err(|error| {
            paro_error::internal(format!("Invalid FIRST string UTF-8: {error}"))
        })?;
        let is_set = match marker {
            0 => false,
            1 => true,
            value => {
                return Err(paro_error::internal(format!(
                    "Invalid FIRST string state marker: {value}"
                )));
            }
        };
        if !is_set && !value.is_empty() {
            return Err(paro_error::internal(
                "Unset FIRST string state contains a value",
            ));
        }
        ptr::write(state.cast::<State>(), State { value, is_set });
        Ok(())
    }
}

/// Get the FIRST aggregate function set.
pub fn get_first_function() -> AggregateFunctionSet {
    let mut set = AggregateFunctionSet::new("first".to_string());
    set.set_dynamic_bind(super::decimal::bind_first);

    // Integer
    set.add_function(AggregateFunction::new(
        "first".to_string(),
        vec![LogicalType::Integer],
        LogicalType::Integer,
        std::mem::size_of::<FirstState<i32>>(),
        first_i32::initialize,
        first_i32::update,
        first_i32::combine,
        first_i32::finalize,
        Some(first_i32::simple_update),
        None,
    ));

    // BigInt
    set.add_function(AggregateFunction::new(
        "first".to_string(),
        vec![LogicalType::BigInt],
        LogicalType::BigInt,
        std::mem::size_of::<FirstState<i64>>(),
        first_i64::initialize,
        first_i64::update,
        first_i64::combine,
        first_i64::finalize,
        Some(first_i64::simple_update),
        None,
    ));

    // Double
    set.add_function(AggregateFunction::new(
        "first".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        std::mem::size_of::<FirstState<f64>>(),
        first_f64::initialize,
        first_f64::update,
        first_f64::combine,
        first_f64::finalize,
        Some(first_f64::simple_update),
        None,
    ));

    set.add_function(
        AggregateFunction::new(
            "first".to_string(),
            vec![LogicalType::Varchar],
            LogicalType::Varchar,
            std::mem::size_of::<FirstStringState>(),
            first_string::initialize,
            first_string::update,
            first_string::combine,
            first_string::finalize,
            Some(first_string::simple_update),
            Some(first_string::destructor),
        )
        .with_state_serialization(first_string::serialize, first_string::deserialize),
    );

    set.with_empty_input(AggregateEmptyInput::Null)
}

/// Get the LAST aggregate function set.
pub fn get_last_function() -> AggregateFunctionSet {
    let mut set = AggregateFunctionSet::new("last".to_string());
    set.set_dynamic_bind(super::decimal::bind_last);

    // Integer
    set.add_function(AggregateFunction::new(
        "last".to_string(),
        vec![LogicalType::Integer],
        LogicalType::Integer,
        std::mem::size_of::<FirstState<i32>>(),
        last_i32::initialize,
        last_i32::update,
        last_i32::combine,
        last_i32::finalize,
        Some(last_i32::simple_update),
        None,
    ));

    // BigInt
    set.add_function(AggregateFunction::new(
        "last".to_string(),
        vec![LogicalType::BigInt],
        LogicalType::BigInt,
        std::mem::size_of::<FirstState<i64>>(),
        last_i64::initialize,
        last_i64::update,
        last_i64::combine,
        last_i64::finalize,
        Some(last_i64::simple_update),
        None,
    ));

    // Double
    set.add_function(AggregateFunction::new(
        "last".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        std::mem::size_of::<FirstState<f64>>(),
        last_f64::initialize,
        last_f64::update,
        last_f64::combine,
        last_f64::finalize,
        Some(last_f64::simple_update),
        None,
    ));

    set.with_empty_input(AggregateEmptyInput::Null)
}

fn alias_function_set(mut set: AggregateFunctionSet, alias_name: &str) -> AggregateFunctionSet {
    set.name = alias_name.to_string();
    for function in &mut set.functions {
        function.name = alias_name.to_string();
    }
    set
}

/// Get the FIRST_VALUE aggregate function set.
/// This is currently an alias of FIRST.
pub fn get_first_value_function() -> AggregateFunctionSet {
    alias_function_set(get_first_function(), "first_value")
}

/// Get the LAST_VALUE aggregate function set.
/// This is currently an alias of LAST.
pub fn get_last_value_function() -> AggregateFunctionSet {
    alias_function_set(get_last_function(), "last_value")
}

/// Get the ANY_VALUE aggregate function set.
/// This is an alias for FIRST - returns any arbitrary value from the group.
pub fn get_any_value_function() -> AggregateFunctionSet {
    let mut set = AggregateFunctionSet::new("any_value".to_string());

    // Integer
    set.add_function(AggregateFunction::new(
        "any_value".to_string(),
        vec![LogicalType::Integer],
        LogicalType::Integer,
        std::mem::size_of::<FirstState<i32>>(),
        first_i32::initialize,
        first_i32::update,
        first_i32::combine,
        first_i32::finalize,
        Some(first_i32::simple_update),
        None,
    ));

    // BigInt
    set.add_function(AggregateFunction::new(
        "any_value".to_string(),
        vec![LogicalType::BigInt],
        LogicalType::BigInt,
        std::mem::size_of::<FirstState<i64>>(),
        first_i64::initialize,
        first_i64::update,
        first_i64::combine,
        first_i64::finalize,
        Some(first_i64::simple_update),
        None,
    ));

    // Double
    set.add_function(AggregateFunction::new(
        "any_value".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        std::mem::size_of::<FirstState<f64>>(),
        first_f64::initialize,
        first_f64::update,
        first_f64::combine,
        first_f64::finalize,
        Some(first_f64::simple_update),
        None,
    ));

    set.with_empty_input(AggregateEmptyInput::Null)
}

/// Get the ARBITRARY aggregate function set.
/// This is an alias for ANY_VALUE/FIRST.
pub fn get_arbitrary_function() -> AggregateFunctionSet {
    let mut set = AggregateFunctionSet::new("arbitrary".to_string());

    // Integer
    set.add_function(AggregateFunction::new(
        "arbitrary".to_string(),
        vec![LogicalType::Integer],
        LogicalType::Integer,
        std::mem::size_of::<FirstState<i32>>(),
        first_i32::initialize,
        first_i32::update,
        first_i32::combine,
        first_i32::finalize,
        Some(first_i32::simple_update),
        None,
    ));

    // BigInt
    set.add_function(AggregateFunction::new(
        "arbitrary".to_string(),
        vec![LogicalType::BigInt],
        LogicalType::BigInt,
        std::mem::size_of::<FirstState<i64>>(),
        first_i64::initialize,
        first_i64::update,
        first_i64::combine,
        first_i64::finalize,
        Some(first_i64::simple_update),
        None,
    ));

    // Double
    set.add_function(AggregateFunction::new(
        "arbitrary".to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        std::mem::size_of::<FirstState<f64>>(),
        first_f64::initialize,
        first_f64::update,
        first_f64::combine,
        first_f64::finalize,
        Some(first_f64::simple_update),
        None,
    ));

    set.with_empty_input(AggregateEmptyInput::Null)
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
    fn test_first_value_last_value_aliases() {
        let first_value_set = get_first_value_function();
        let last_value_set = get_last_value_function();
        let (first_value_func, _) = first_value_set.bind(&[LogicalType::Integer]).unwrap();
        let (last_value_func, _) = last_value_set.bind(&[LogicalType::Integer]).unwrap();
        assert_eq!(first_value_func.name, "first_value");
        assert_eq!(last_value_func.name, "last_value");
    }

    #[test]
    fn test_first_integer() {
        let func_set = get_first_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            let input = paro_common::test_utils::test_i32_vector_with_allocator(
                &[10, 20, 30],
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
                (func.finalize)(&states, &input_data, &mut result, 1).unwrap();
            }

            assert!(!result.is_null(0));
            assert_eq!(result.get_flat::<i32>(0), 10);
        }
    }

    #[test]
    fn first_varchar_roundtrips_owned_state() {
        let (func, targets) = get_first_function().bind(&[LogicalType::Varchar]).unwrap();
        assert_eq!(targets, [LogicalType::Varchar]);
        let mut arena = test_arena();
        let state_words = func.state_size.div_ceil(std::mem::size_of::<u64>());
        let mut source = vec![0u64; state_words];
        let mut restored = vec![0u64; state_words];

        unsafe {
            (func.initialize)(source.as_mut_ptr().cast());
            let mut input = paro_common::test_utils::test_vector(LogicalType::Varchar);
            input.set_count(3);
            input.set_null(0, true);
            input.set_string(1, "first");
            input.set_string(2, "second");
            let input_data = preserve_input_data(&func, &mut arena);
            func.simple_update.unwrap()(&[&input], &input_data, source.as_mut_ptr().cast(), 3);

            let mut serialized = Vec::new();
            func.state_serialize.unwrap()(source.as_ptr().cast(), &input_data, &mut serialized)
                .unwrap();
            func.state_deserialize.unwrap()(&serialized, &input_data, restored.as_mut_ptr().cast())
                .unwrap();

            let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
            states.set_count(2);
            let pointers = states.flat_data_mut::<*mut u8>();
            *pointers = source.as_mut_ptr().cast();
            *pointers.add(1) = restored.as_mut_ptr().cast();
            let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);
            result.set_count(2);
            (func.finalize)(&states, &input_data, &mut result, 2).unwrap();
            assert_eq!(result.get_string(0), Some("first"));
            assert_eq!(result.get_string(1), Some("first"));
            func.destructor.unwrap()(&states, &input_data, 2);
        }
    }

    #[test]
    fn test_first_reads_dictionary_input() {
        let func_set = get_first_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            let base = paro_common::test_utils::test_i32_vector_with_allocator(
                &[10, 20, 30],
                paro_common::test_utils::test_allocator(),
            );
            let selection = paro_common::vector::SelectionVector::try_from_indices(
                vec![2, 1],
                paro_common::test_utils::test_allocator(),
            )
            .unwrap();
            let input = Vector::try_dictionary(Arc::new(base), selection).unwrap();

            if let Some(simple_update) = func.simple_update {
                let input_data = preserve_input_data(&func, &mut arena);
                simple_update(&[&input], &input_data, state_ptr, 2);
            }

            let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);
            result.set_count(1);

            let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
            states.set_count(1);
            let states_ptr = states.flat_data_mut::<*mut u8>();
            *states_ptr = state_ptr;

            let input_data = preserve_input_data(&func, &mut arena);
            (func.finalize)(&states, &input_data, &mut result, 1).unwrap();

            assert!(!result.is_null(0));
            assert_eq!(result.get_flat::<i32>(0), 30);
        }
    }

    #[test]
    fn test_first_skips_leading_nulls() {
        let func_set = get_first_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            let mut input = paro_common::test_utils::test_i32_vector_with_allocator(
                &[0, 0, 30, 40],
                paro_common::test_utils::test_allocator(),
            );
            input.set_null(0, true);
            input.set_null(1, true); // [NULL, NULL, 30, 40]

            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input], &input_data, state_ptr, 4);
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
                (func.finalize)(&states, &input_data, &mut result, 1).unwrap();
            }

            assert!(!result.is_null(0));
            assert_eq!(result.get_flat::<i32>(0), 30);
        }
    }

    #[test]
    fn test_last_integer() {
        let func_set = get_last_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            let input = paro_common::test_utils::test_i32_vector_with_allocator(
                &[10, 20, 30],
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
                (func.finalize)(&states, &input_data, &mut result, 1).unwrap();
            }

            assert!(!result.is_null(0));
            assert_eq!(result.get_flat::<i32>(0), 30);
        }
    }

    #[test]
    fn test_last_skips_trailing_nulls() {
        let func_set = get_last_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let mut arena = test_arena();

        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state_ptr);

            let mut input = paro_common::test_utils::test_i32_vector_with_allocator(
                &[10, 20, 0, 0],
                paro_common::test_utils::test_allocator(),
            );
            input.set_null(2, true);
            input.set_null(3, true); // [10, 20, NULL, NULL]

            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input], &input_data, state_ptr, 4);
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
                (func.finalize)(&states, &input_data, &mut result, 1).unwrap();
            }

            assert!(!result.is_null(0));
            assert_eq!(result.get_flat::<i32>(0), 20);
        }
    }

    #[test]
    fn test_first_empty() {
        let func_set = get_first_function();
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
                (func.finalize)(&states, &input_data, &mut result, 1).unwrap();
            }

            assert!(result.is_null(0));
        }
    }

    #[test]
    fn test_any_value() {
        let func_set = get_any_value_function();
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
                (func.finalize)(&states, &input_data, &mut result, 1).unwrap();
            }

            assert!(!result.is_null(0));
            // any_value returns first non-null value
            let val: f64 = result.get_flat(0);
            assert!((val - 1.5).abs() < 1e-10);
        }
    }

    #[test]
    fn test_first_combine() {
        let func_set = get_first_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let mut arena = test_arena();

        let mut state1_buf = vec![0u8; func.state_size];
        let mut state2_buf = vec![0u8; func.state_size];
        let state1_ptr = state1_buf.as_mut_ptr();
        let state2_ptr = state2_buf.as_mut_ptr();

        unsafe {
            (func.initialize)(state1_ptr);
            (func.initialize)(state2_ptr);

            // State 1: first = 100
            let input1 = paro_common::test_utils::test_i32_vector_with_allocator(
                &[100, 200],
                paro_common::test_utils::test_allocator(),
            );
            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input1], &input_data, state1_ptr, 2);
                }
            }

            // State 2: first = 300
            let input2 = paro_common::test_utils::test_i32_vector_with_allocator(
                &[300, 400],
                paro_common::test_utils::test_allocator(),
            );
            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input2], &input_data, state2_ptr, 2);
                }
            }

            // Combine: target (state2) keeps its value since it's already set
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
                (func.finalize)(&target_states, &input_data, &mut result, 1).unwrap();
            }

            assert!(!result.is_null(0));
            // First keeps target's value (300) since it was already set
            assert_eq!(result.get_flat::<i32>(0), 300);
        }
    }
}
