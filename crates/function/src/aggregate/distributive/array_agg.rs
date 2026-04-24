// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! ARRAY_AGG aggregate function.
//!
//!
//!
//! ## Implementation Notes
//! - Preserves input order seen by the aggregate operator.
//! - Includes NULL values.
//! - Returns NULL for empty input.

use crate::aggregate::{AggregateFunction, AggregateFunctionSet, AggregateInputData};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use std::ptr;
use std::sync::Arc;

#[repr(C)]
#[derive(Debug, Default)]
struct ArrayAggState {
    values: Vec<Value>,
}

#[inline]
fn write_list_entry(vector: &mut Vector, row_idx: usize, offset: usize, length: usize) {
    if offset > u32::MAX as usize || length > u32::MAX as usize {
        panic!("array_agg list entry exceeds u32 range");
    }
    let entry_base = unsafe { vector.flat_data_mut::<u8>() };
    let entry_ptr = unsafe { entry_base.add(row_idx * 8) as *mut u32 };
    unsafe {
        std::ptr::write_unaligned(entry_ptr, offset as u32);
        std::ptr::write_unaligned(entry_ptr.add(1), length as u32);
    }
}

fn ensure_list_child_capacity(result: &mut Vector, child_type: &LogicalType, needed: usize) {
    let Some(existing_child) = result.child() else {
        panic!("array_agg result vector missing list child");
    };
    if needed <= existing_child.capacity() {
        return;
    }

    let old_child = Arc::clone(existing_child);
    let old_len = old_child.len();
    let old_capacity = old_child.capacity();
    let allocator = old_child.allocator().clone();
    let new_capacity = needed.max(old_capacity.saturating_mul(2)).max(1);

    let mut new_child = Vector::try_new(child_type.clone(), new_capacity, allocator)
        .expect("array_agg child vector allocation failed");
    new_child.set_count(old_len);
    for i in 0..old_len {
        new_child.copy_at(i, &old_child, i);
    }
    result.set_child(Arc::new(new_child));
}

unsafe fn initialize(state: *mut u8) {
    ptr::write(state as *mut ArrayAggState, ArrayAggState::default());
}

unsafe fn update(
    inputs: &[&Vector],
    _input_data: &AggregateInputData,
    states: &Vector,
    count: usize,
) {
    let input = inputs[0];
    let state_ptrs = states.flat_data::<*mut u8>();
    for i in 0..count {
        let state_ptr = *state_ptrs.add(i);
        let state = &mut *(state_ptr as *mut ArrayAggState);
        state.values.push(input.get_value(i));
    }
}

unsafe fn simple_update(
    inputs: &[&Vector],
    _input_data: &AggregateInputData,
    state: *mut u8,
    count: usize,
) {
    let input = inputs[0];
    let state = &mut *(state as *mut ArrayAggState);
    for i in 0..count {
        state.values.push(input.get_value(i));
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
        let source_state = &*((*source_ptrs.add(i)) as *const ArrayAggState);
        let target_state = &mut *((*target_ptrs.add(i)) as *mut ArrayAggState);
        target_state
            .values
            .extend(source_state.values.iter().cloned());
    }
}

unsafe fn finalize(
    states: &Vector,
    _input_data: &AggregateInputData,
    result: &mut Vector,
    count: usize,
) {
    let child_type = match result.logical_type() {
        LogicalType::List(child) => child.as_ref().clone(),
        ty => panic!("array_agg result type must be LIST, got {ty:?}"),
    };
    let state_ptrs = states.flat_data::<*mut u8>();

    for i in 0..count {
        let state = &*((*state_ptrs.add(i)) as *const ArrayAggState);
        if state.values.is_empty() {
            result.set_null(i, true);
            continue;
        }

        let Some(child) = result.child() else {
            panic!("array_agg result vector missing list child");
        };
        let dest_offset = child.len();
        let needed = dest_offset + state.values.len();
        ensure_list_child_capacity(result, &child_type, needed);

        {
            let child_arc = result
                .child_mut()
                .expect("array_agg result vector missing mutable child");
            let child = Arc::make_mut(child_arc);
            child.validity_mut().resize(needed);
            for (value_idx, value) in state.values.iter().enumerate() {
                child.set_value(dest_offset + value_idx, value);
            }
            child.set_count(needed);
        }

        write_list_entry(result, i, dest_offset, state.values.len());
        result.set_null(i, false);
    }
}

unsafe fn destructor(states: &Vector, _input_data: &AggregateInputData, count: usize) {
    let state_ptrs = states.flat_data::<*mut u8>();
    for i in 0..count {
        let state = (*state_ptrs.add(i)) as *mut ArrayAggState;
        ptr::drop_in_place(state);
    }
}

fn add_array_agg_overload(set: &mut AggregateFunctionSet, input_type: LogicalType) {
    let return_type = LogicalType::List(Box::new(input_type.clone()));
    set.add_function(AggregateFunction::new(
        "array_agg".to_string(),
        vec![input_type],
        return_type,
        std::mem::size_of::<ArrayAggState>(),
        initialize,
        update,
        combine,
        finalize,
        Some(simple_update),
        Some(destructor),
    ));
}

pub fn get_array_agg_function() -> AggregateFunctionSet {
    let mut set = AggregateFunctionSet::new("array_agg".to_string());
    add_array_agg_overload(&mut set, LogicalType::Integer);
    add_array_agg_overload(&mut set, LogicalType::BigInt);
    add_array_agg_overload(&mut set, LogicalType::Double);
    add_array_agg_overload(&mut set, LogicalType::Boolean);
    add_array_agg_overload(&mut set, LogicalType::Varchar);
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

    unsafe fn finalize_single(
        func: &AggregateFunction,
        input: &Vector,
        row_count: usize,
    ) -> Vector {
        let mut arena = test_arena();
        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();
        (func.initialize)(state_ptr);

        let simple_update = func
            .simple_update
            .expect("array_agg aggregate should provide simple_update");
        {
            let input_data = preserve_input_data(func, &mut arena);
            simple_update(&[input], &input_data, state_ptr, row_count);
        }

        let mut result = paro_common::test_utils::test_vector(func.return_type.clone());
        result.set_count(1);
        let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
        states.set_count(1);
        *states.flat_data_mut::<*mut u8>() = state_ptr;

        {
            let input_data = preserve_input_data(func, &mut arena);
            (func.finalize)(&states, &input_data, &mut result, 1);
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
    fn array_agg_integers() {
        let set = get_array_agg_function();
        let (func, _) = set.bind(&[LogicalType::Integer]).unwrap();
        let input = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2, 3],
            paro_common::test_utils::test_allocator(),
        );
        let result = unsafe { finalize_single(&func, &input, 3) };
        match result.get_value(0) {
            Value::List(values, _) => {
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], Value::Integer(1));
                assert_eq!(values[1], Value::Integer(2));
                assert_eq!(values[2], Value::Integer(3));
            }
            other => panic!("expected list result, got {other:?}"),
        }
    }

    #[test]
    fn array_agg_keeps_nulls() {
        let set = get_array_agg_function();
        let (func, _) = set.bind(&[LogicalType::Integer]).unwrap();
        let mut input = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 0, 3],
            paro_common::test_utils::test_allocator(),
        );
        input.set_null(1, true);
        let result = unsafe { finalize_single(&func, &input, 3) };
        match result.get_value(0) {
            Value::List(values, _) => {
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], Value::Integer(1));
                assert!(matches!(values[1], Value::Null(_)));
                assert_eq!(values[2], Value::Integer(3));
            }
            other => panic!("expected list result, got {other:?}"),
        }
    }
}
