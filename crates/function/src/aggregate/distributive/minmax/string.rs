// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::aggregate::{
    AggregateCombineType, AggregateFunction, AggregateInputData, AggregateStateInput,
};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use std::ptr;

#[derive(Default)]
struct StringExtremaState {
    value: String,
    is_set: bool,
}

#[inline]
fn retain_extreme(
    state: &mut StringExtremaState,
    candidate: &str,
    compare: fn(&str, &str) -> bool,
) {
    if !state.is_set || compare(candidate, &state.value) {
        state.value.clear();
        state.value.push_str(candidate);
        state.is_set = true;
    }
}

unsafe fn initialize(state: *mut u8) {
    ptr::write(
        state.cast::<StringExtremaState>(),
        StringExtremaState::default(),
    );
}

unsafe fn finalize(
    states: &Vector,
    _input_data: &AggregateInputData,
    result: &mut Vector,
    count: usize,
) -> Result<()> {
    let state_ptrs = states.flat_data::<*mut u8>();
    for row in 0..count {
        let state = &*(*state_ptrs.add(row)).cast::<StringExtremaState>();
        if state.is_set {
            result.try_set_string(row, &state.value)?;
        } else {
            result.set_null(row, true);
        }
    }
    Ok(())
}

unsafe fn destroy(states: &Vector, _input_data: &AggregateInputData, count: usize) {
    let state_ptrs = states.flat_data::<*mut u8>();
    for row in 0..count {
        ptr::drop_in_place((*state_ptrs.add(row)).cast::<StringExtremaState>());
    }
}

unsafe fn serialize(
    state: *const u8,
    _input_data: &AggregateInputData,
    output: &mut Vec<u8>,
) -> Result<()> {
    let state = &*state.cast::<StringExtremaState>();
    output.push(u8::from(state.is_set));
    let length = u64::try_from(state.value.len())
        .map_err(|_| paro_error::internal("string extrema state exceeds u64"))?;
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(state.value.as_bytes());
    Ok(())
}

unsafe fn deserialize(
    input: &[u8],
    _input_data: &AggregateInputData,
    state: *mut u8,
) -> Result<()> {
    let (&marker, payload) = input
        .split_first()
        .ok_or_else(|| paro_error::internal("truncated string extrema state"))?;
    let length_bytes = payload
        .get(..8)
        .ok_or_else(|| paro_error::internal("truncated string extrema state length"))?;
    let length = usize::try_from(u64::from_le_bytes(
        length_bytes.try_into().expect("eight length bytes"),
    ))
    .map_err(|_| paro_error::internal("string extrema state length exceeds usize"))?;
    let bytes = payload
        .get(8..)
        .filter(|bytes| bytes.len() == length)
        .ok_or_else(|| paro_error::internal("invalid string extrema state length"))?;
    let value = String::from_utf8(bytes.to_vec()).map_err(|error| {
        paro_error::internal(format!("invalid string extrema state UTF-8: {error}"))
    })?;
    let is_set = match marker {
        0 => false,
        1 => true,
        value => {
            return Err(paro_error::internal(format!(
                "invalid string extrema state marker: {value}"
            )));
        }
    };
    if !is_set && !value.is_empty() {
        return Err(paro_error::internal(
            "unset string extrema state contains a value",
        ));
    }
    ptr::write(
        state.cast::<StringExtremaState>(),
        StringExtremaState { value, is_set },
    );
    Ok(())
}

macro_rules! define_extrema {
    ($module:ident, $operator:tt, $display_name:literal) => {
        mod $module {
            use super::*;

            fn compare(candidate: &str, current: &str) -> bool {
                candidate $operator current
            }

            pub unsafe fn update(
                inputs: &[&Vector],
                _input_data: &AggregateInputData,
                states: &AggregateStateInput,
                count: usize,
            ) {
                let input = inputs[0]
                    .try_to_utf8_view(count)
                    .expect(concat!($display_name, "(VARCHAR) expects textual input"));
                for row in 0..count {
                    if input.is_valid(row) {
                        retain_extreme(
                            &mut *states.state_ptr(row).cast::<StringExtremaState>(),
                            input.str(row),
                            compare,
                        );
                    }
                }
            }

            pub unsafe fn simple_update(
                inputs: &[&Vector],
                _input_data: &AggregateInputData,
                state: *mut u8,
                count: usize,
            ) {
                let input = inputs[0]
                    .try_to_utf8_view(count)
                    .expect(concat!($display_name, "(VARCHAR) expects textual input"));
                let state = &mut *state.cast::<StringExtremaState>();
                for row in 0..count {
                    if input.is_valid(row) {
                        retain_extreme(state, input.str(row), compare);
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
                    let source = &mut *(*source_ptrs.add(row)).cast::<StringExtremaState>();
                    let target = &mut *(*target_ptrs.add(row)).cast::<StringExtremaState>();
                    if !source.is_set || (target.is_set && !compare(&source.value, &target.value)) {
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
        }
    };
}

define_extrema!(minimum, <, "min");
define_extrema!(maximum, >, "max");

fn function(
    name: &str,
    update: crate::aggregate::AggregateUpdateFn,
    combine: crate::aggregate::AggregateCombineFn,
    simple_update: crate::aggregate::AggregateSimpleUpdateFn,
) -> AggregateFunction {
    AggregateFunction::new(
        name.to_string(),
        vec![LogicalType::Varchar],
        LogicalType::Varchar,
        std::mem::size_of::<StringExtremaState>(),
        initialize,
        update,
        combine,
        finalize,
        Some(simple_update),
        Some(destroy),
    )
    .with_state_serialization(serialize, deserialize)
}

pub(super) fn min_varchar_function() -> AggregateFunction {
    function(
        "min",
        minimum::update,
        minimum::combine,
        minimum::simple_update,
    )
}

pub(super) fn max_varchar_function() -> AggregateFunction {
    function(
        "max",
        maximum::update,
        maximum::combine,
        maximum::simple_update,
    )
}
