// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! STRING_AGG aggregate function.
//!
//!
//!
//! ## Implementation Notes
//! - `string_agg(expr)` uses `','` as default separator.
//! - `string_agg(expr, sep)` uses row separator `sep` (NULL sep treated as empty separator).
//! - NULL `expr` rows are ignored.

use crate::aggregate::{
    AggregateCombineType, AggregateEmptyInput, AggregateFunction, AggregateFunctionSet,
    AggregateInputData, AggregateStateInput,
};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use std::ptr;

#[repr(C)]
#[derive(Debug, Default)]
struct StringAggState {
    result: String,
    is_set: bool,
    combine_separator: Option<String>,
}

#[inline]
fn append_value(state: &mut StringAggState, value: &str, separator: &str) {
    if state.is_set {
        state.result.push_str(separator);
    }
    state.result.push_str(value);
    state.is_set = true;
}

#[inline]
fn combine_states(source: &StringAggState, target: &mut StringAggState) {
    if !source.is_set {
        return;
    }
    if !target.is_set {
        target.result.clear();
        target.result.push_str(&source.result);
        target.is_set = true;
        target.combine_separator = source.combine_separator.clone();
        return;
    }

    let separator = target
        .combine_separator
        .as_deref()
        .or(source.combine_separator.as_deref())
        .unwrap_or("");
    target.result.push_str(separator);
    target.result.push_str(&source.result);

    if target.combine_separator.is_none() {
        target.combine_separator = source.combine_separator.clone();
    }
}

fn write_u64(output: &mut Vec<u8>, value: usize) -> Result<()> {
    let value = u64::try_from(value)
        .map_err(|_| paro_error::internal("string_agg state field exceeds u64"))?;
    output.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn read_u8(input: &[u8], offset: &mut usize) -> Result<u8> {
    let value = *input
        .get(*offset)
        .ok_or_else(|| paro_error::internal("Truncated string_agg state"))?;
    *offset += 1;
    Ok(value)
}

fn read_u64(input: &[u8], offset: &mut usize) -> Result<usize> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| paro_error::internal("string_agg state offset overflow"))?;
    let bytes = input
        .get(*offset..end)
        .ok_or_else(|| paro_error::internal("Truncated string_agg state length"))?;
    *offset = end;
    usize::try_from(u64::from_le_bytes(bytes.try_into().expect("u64 bytes")))
        .map_err(|_| paro_error::internal("string_agg state length exceeds usize"))
}

fn read_string(input: &[u8], offset: &mut usize) -> Result<String> {
    let len = read_u64(input, offset)?;
    let end = offset
        .checked_add(len)
        .ok_or_else(|| paro_error::internal("string_agg state string offset overflow"))?;
    let bytes = input
        .get(*offset..end)
        .ok_or_else(|| paro_error::internal("Truncated string_agg state string"))?;
    *offset = end;
    String::from_utf8(bytes.to_vec())
        .map_err(|err| paro_error::internal(format!("Invalid string_agg state UTF-8: {err}")))
}

unsafe fn serialize_state(
    state: *const u8,
    _input_data: &AggregateInputData,
    output: &mut Vec<u8>,
) -> Result<()> {
    let state = &*(state as *const StringAggState);
    output.push(u8::from(state.is_set));
    write_u64(output, state.result.len())?;
    output.extend_from_slice(state.result.as_bytes());
    match &state.combine_separator {
        Some(separator) => {
            output.push(1);
            write_u64(output, separator.len())?;
            output.extend_from_slice(separator.as_bytes());
        }
        None => output.push(0),
    }
    Ok(())
}

unsafe fn deserialize_state(
    input: &[u8],
    _input_data: &AggregateInputData,
    state: *mut u8,
) -> Result<()> {
    let mut offset = 0;
    let is_set = match read_u8(input, &mut offset)? {
        0 => false,
        1 => true,
        value => {
            return Err(paro_error::internal(format!(
                "Invalid string_agg is_set marker: {value}"
            )));
        }
    };
    let result = read_string(input, &mut offset)?;
    let combine_separator = match read_u8(input, &mut offset)? {
        0 => None,
        1 => Some(read_string(input, &mut offset)?),
        value => {
            return Err(paro_error::internal(format!(
                "Invalid string_agg separator marker: {value}"
            )));
        }
    };
    if offset != input.len() {
        return Err(paro_error::internal("Trailing bytes in string_agg state"));
    }
    ptr::write(
        state as *mut StringAggState,
        StringAggState {
            result,
            is_set,
            combine_separator,
        },
    );
    Ok(())
}

mod string_agg_one_arg {
    use super::*;

    type State = StringAggState;

    pub unsafe fn initialize(state: *mut u8) {
        debug_assert_eq!(
            (state as usize) % std::mem::align_of::<State>(),
            0,
            "string_agg state pointer is not properly aligned"
        );
        ptr::write(state as *mut State, State::default());
    }

    pub unsafe fn update(
        inputs: &[&Vector],
        _input_data: &AggregateInputData,
        states: &AggregateStateInput,
        count: usize,
    ) {
        let input = inputs[0];
        debug_assert_eq!(
            input.logical_type(),
            &LogicalType::Varchar,
            "string_agg(expr) expects VARCHAR input"
        );
        let input = input
            .try_to_utf8_view(count)
            .expect("string_agg(expr) expects textual input");
        for i in 0..count {
            if !input.is_valid(i) {
                continue;
            }
            let value = input.str(i);
            let state_ptr = states.state_ptr(i);
            let state = &mut *(state_ptr as *mut State);
            append_value(state, value, ",");
            if state.combine_separator.is_none() {
                state.combine_separator = Some(",".to_string());
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
        debug_assert_eq!(
            input.logical_type(),
            &LogicalType::Varchar,
            "string_agg(expr) expects VARCHAR input"
        );
        let input = input
            .try_to_utf8_view(count)
            .expect("string_agg(expr) expects textual input");
        let state = &mut *(state as *mut State);

        for i in 0..count {
            if !input.is_valid(i) {
                continue;
            }
            let value = input.str(i);
            append_value(state, value, ",");
            if state.combine_separator.is_none() {
                state.combine_separator = Some(",".to_string());
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
        let allow_destructive = input_data.combine_type == AggregateCombineType::AllowDestructive;
        for i in 0..count {
            let source_state_ptr = (*source_ptrs.add(i)) as *mut State;
            let target_state_ptr = (*target_ptrs.add(i)) as *mut State;

            if allow_destructive {
                let source_state = &mut *source_state_ptr;
                let target_state = &mut *target_state_ptr;
                if !source_state.is_set {
                    continue;
                }
                if !target_state.is_set {
                    std::mem::swap(target_state, source_state);
                } else {
                    combine_states(source_state, target_state);
                    source_state.result.clear();
                    source_state.is_set = false;
                    source_state.combine_separator = None;
                }
            } else {
                let source_state = &*source_state_ptr;
                let target_state = &mut *target_state_ptr;
                combine_states(source_state, target_state);
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
        for i in 0..count {
            let state = &*((*state_ptrs.add(i)) as *const State);
            if !state.is_set {
                result.set_null(i, true);
            } else {
                result.set_null(i, false);
                result.set_string(i, &state.result);
            }
        }
        Ok(())
    }

    pub unsafe fn destructor(states: &Vector, _input_data: &AggregateInputData, count: usize) {
        let state_ptrs = states.flat_data::<*mut u8>();
        for i in 0..count {
            let state = (*state_ptrs.add(i)) as *mut State;
            ptr::drop_in_place(state);
        }
    }
}

mod string_agg_two_args {
    use super::*;

    type State = StringAggState;

    pub unsafe fn initialize(state: *mut u8) {
        debug_assert_eq!(
            (state as usize) % std::mem::align_of::<State>(),
            0,
            "string_agg state pointer is not properly aligned"
        );
        ptr::write(state as *mut State, State::default());
    }

    pub unsafe fn update(
        inputs: &[&Vector],
        _input_data: &AggregateInputData,
        states: &AggregateStateInput,
        count: usize,
    ) {
        let value_input = inputs[0];
        let sep_input = inputs[1];
        debug_assert_eq!(
            value_input.logical_type(),
            &LogicalType::Varchar,
            "string_agg(expr, sep) expects VARCHAR expr"
        );
        debug_assert_eq!(
            sep_input.logical_type(),
            &LogicalType::Varchar,
            "string_agg(expr, sep) expects VARCHAR separator"
        );
        let value_input = value_input
            .try_to_utf8_view(count)
            .expect("string_agg(expr, sep) expects textual expr");
        let sep_input = sep_input
            .try_to_utf8_view(count)
            .expect("string_agg(expr, sep) expects textual separator");
        for i in 0..count {
            if !value_input.is_valid(i) {
                continue;
            }
            let value = value_input.str(i);
            let separator_is_null = !sep_input.is_valid(i);
            let separator = if separator_is_null {
                ""
            } else {
                sep_input.str(i)
            };

            let state_ptr = states.state_ptr(i);
            let state = &mut *(state_ptr as *mut State);
            append_value(state, value, separator);
            if state.combine_separator.is_none() && !separator_is_null {
                state.combine_separator = Some(separator.to_string());
            }
        }
    }

    pub unsafe fn simple_update(
        inputs: &[&Vector],
        _input_data: &AggregateInputData,
        state: *mut u8,
        count: usize,
    ) {
        let value_input = inputs[0];
        let sep_input = inputs[1];
        debug_assert_eq!(
            value_input.logical_type(),
            &LogicalType::Varchar,
            "string_agg(expr, sep) expects VARCHAR expr"
        );
        debug_assert_eq!(
            sep_input.logical_type(),
            &LogicalType::Varchar,
            "string_agg(expr, sep) expects VARCHAR separator"
        );
        let value_input = value_input
            .try_to_utf8_view(count)
            .expect("string_agg(expr, sep) expects textual expr");
        let sep_input = sep_input
            .try_to_utf8_view(count)
            .expect("string_agg(expr, sep) expects textual separator");
        let state = &mut *(state as *mut State);

        for i in 0..count {
            if !value_input.is_valid(i) {
                continue;
            }
            let value = value_input.str(i);
            let separator_is_null = !sep_input.is_valid(i);
            let separator = if separator_is_null {
                ""
            } else {
                sep_input.str(i)
            };

            append_value(state, value, separator);
            if state.combine_separator.is_none() && !separator_is_null {
                state.combine_separator = Some(separator.to_string());
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
        let allow_destructive = input_data.combine_type == AggregateCombineType::AllowDestructive;
        for i in 0..count {
            let source_state_ptr = (*source_ptrs.add(i)) as *mut State;
            let target_state_ptr = (*target_ptrs.add(i)) as *mut State;
            if allow_destructive {
                let source_state = &mut *source_state_ptr;
                let target_state = &mut *target_state_ptr;
                if !source_state.is_set {
                    continue;
                }
                if !target_state.is_set {
                    std::mem::swap(target_state, source_state);
                } else {
                    combine_states(source_state, target_state);
                    source_state.result.clear();
                    source_state.is_set = false;
                    source_state.combine_separator = None;
                }
            } else {
                let source_state = &*source_state_ptr;
                let target_state = &mut *target_state_ptr;
                combine_states(source_state, target_state);
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
        for i in 0..count {
            let state = &*((*state_ptrs.add(i)) as *const State);
            if !state.is_set {
                result.set_null(i, true);
            } else {
                result.set_null(i, false);
                result.set_string(i, &state.result);
            }
        }
        Ok(())
    }

    pub unsafe fn destructor(states: &Vector, _input_data: &AggregateInputData, count: usize) {
        let state_ptrs = states.flat_data::<*mut u8>();
        for i in 0..count {
            let state = (*state_ptrs.add(i)) as *mut State;
            ptr::drop_in_place(state);
        }
    }
}

pub fn get_string_agg_function() -> AggregateFunctionSet {
    let mut set = AggregateFunctionSet::new("string_agg".to_string());
    let state_size = std::mem::size_of::<StringAggState>();

    set.add_function(
        AggregateFunction::new(
            "string_agg".to_string(),
            vec![LogicalType::Varchar],
            LogicalType::Varchar,
            state_size,
            string_agg_one_arg::initialize,
            string_agg_one_arg::update,
            string_agg_one_arg::combine,
            string_agg_one_arg::finalize,
            Some(string_agg_one_arg::simple_update),
            Some(string_agg_one_arg::destructor),
        )
        .with_state_serialization(serialize_state, deserialize_state),
    );
    set.add_function(
        AggregateFunction::new(
            "string_agg".to_string(),
            vec![LogicalType::Varchar, LogicalType::Varchar],
            LogicalType::Varchar,
            state_size,
            string_agg_two_args::initialize,
            string_agg_two_args::update,
            string_agg_two_args::combine,
            string_agg_two_args::finalize,
            Some(string_agg_two_args::simple_update),
            Some(string_agg_two_args::destructor),
        )
        .with_state_serialization(serialize_state, deserialize_state),
    );

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

    unsafe fn finalize_single(
        func: &AggregateFunction,
        inputs: &[&Vector],
        row_count: usize,
    ) -> Vector {
        let mut arena = test_arena();
        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();
        (func.initialize)(state_ptr);

        let simple_update = func
            .simple_update
            .expect("string_agg aggregate should provide simple_update");
        {
            let input_data = preserve_input_data(func, &mut arena);
            simple_update(inputs, &input_data, state_ptr, row_count);
        }

        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);
        result.set_count(1);
        let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
        states.set_count(1);
        *states.flat_data_mut::<*mut u8>() = state_ptr;

        {
            let input_data = preserve_input_data(func, &mut arena);
            (func.finalize)(&states, &input_data, &mut result, 1).unwrap();
        }
        {
            let input_data = preserve_input_data(func, &mut arena);
            if let Some(destructor) = func.destructor {
                destructor(&states, &input_data, 1);
            }
        }

        result
    }

    #[test]
    fn string_agg_default_separator() {
        let set = get_string_agg_function();
        let (func, _) = set.bind(&[LogicalType::Varchar]).unwrap();
        let input = paro_common::test_utils::test_string_vector_with_allocator(
            &["a", "b", "c"],
            paro_common::test_utils::test_allocator(),
        );
        let result = unsafe { finalize_single(&func, &[&input], 3) };
        assert_eq!(result.get_string(0), Some("a,b,c"));
    }

    #[test]
    fn string_agg_custom_separator() {
        let set = get_string_agg_function();
        let (func, _) = set
            .bind(&[LogicalType::Varchar, LogicalType::Varchar])
            .unwrap();
        let values = paro_common::test_utils::test_string_vector_with_allocator(
            &["a", "b", "c"],
            paro_common::test_utils::test_allocator(),
        );
        let separators = paro_common::test_utils::test_string_vector_with_allocator(
            &["|", "|", "|"],
            paro_common::test_utils::test_allocator(),
        );
        let result = unsafe { finalize_single(&func, &[&values, &separators], 3) };
        assert_eq!(result.get_string(0), Some("a|b|c"));
    }

    #[test]
    fn string_agg_skips_null_values() {
        let set = get_string_agg_function();
        let (func, _) = set.bind(&[LogicalType::Varchar]).unwrap();
        let mut input = paro_common::test_utils::test_string_vector_with_allocator(
            &["a", "", "c"],
            paro_common::test_utils::test_allocator(),
        );
        input.set_null(1, true);
        let result = unsafe { finalize_single(&func, &[&input], 3) };
        assert_eq!(result.get_string(0), Some("a,c"));
    }
}
