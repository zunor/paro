// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Variance and Standard Deviation Aggregate Functions.
//!
//!
//!
//! ## Implementation Notes
//! - Uses Welford's online algorithm for stable variance accumulation.
//! - `variance` is an alias of `var_samp`.
//! - `stddev` is an alias of `stddev_samp`.

use crate::aggregate::{
    AggregateFinalizeFn, AggregateFunction, AggregateFunctionSet, AggregateInputData,
};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
struct VarianceState {
    count: u64,
    mean: f64,
    m2: f64,
}

#[inline]
fn update_variance_state(state: &mut VarianceState, value: f64) {
    state.count += 1;
    let delta = value - state.mean;
    state.mean += delta / state.count as f64;
    let delta2 = value - state.mean;
    state.m2 += delta * delta2;
}

#[inline]
fn combine_variance_states(source: &VarianceState, target: &mut VarianceState) {
    if source.count == 0 {
        return;
    }
    if target.count == 0 {
        *target = *source;
        return;
    }

    let total_count = target.count + source.count;
    let delta = source.mean - target.mean;
    target.mean += delta * (source.count as f64 / total_count as f64);
    target.m2 += source.m2
        + delta * delta * (target.count as f64 * source.count as f64 / total_count as f64);
    target.count = total_count;
}

macro_rules! define_variance_input_impl {
    ($mod_name:ident, $input_type:ty) => {
        mod $mod_name {
            use super::*;

            type State = VarianceState;

            pub unsafe fn initialize(state: *mut u8) {
                let state = state as *mut State;
                (*state).count = 0;
                (*state).mean = 0.0;
                (*state).m2 = 0.0;
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
                    if input.is_null(i) {
                        continue;
                    }
                    let state_ptr = *state_ptrs.add(i);
                    let state = &mut *(state_ptr as *mut State);
                    let value: $input_type = input.get_fixed(i);
                    update_variance_state(state, value as f64);
                }
            }

            pub unsafe fn simple_update(
                inputs: &[&Vector],
                _input_data: &AggregateInputData,
                state: *mut u8,
                count: usize,
            ) {
                let input = inputs[0];
                let state = &mut *(state as *mut State);

                for i in 0..count {
                    if input.is_null(i) {
                        continue;
                    }
                    let value: $input_type = input.get_fixed(i);
                    update_variance_state(state, value as f64);
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
                    let source_state = &*((*source_ptrs.add(i)) as *const State);
                    let target_state = &mut *((*target_ptrs.add(i)) as *mut State);
                    combine_variance_states(source_state, target_state);
                }
            }
        }
    };
}

define_variance_input_impl!(variance_i32, i32);
define_variance_input_impl!(variance_i64, i64);
define_variance_input_impl!(variance_f64, f64);

#[derive(Debug, Clone, Copy)]
enum FinalizeKind {
    VarPop,
    VarSamp,
    StddevPop,
    StddevSamp,
}

#[inline]
fn variance_value(state: &VarianceState, kind: FinalizeKind) -> Option<f64> {
    match kind {
        FinalizeKind::VarPop => {
            if state.count == 0 {
                None
            } else {
                Some(state.m2 / state.count as f64)
            }
        }
        FinalizeKind::VarSamp => {
            if state.count < 2 {
                None
            } else {
                Some(state.m2 / (state.count as f64 - 1.0))
            }
        }
        FinalizeKind::StddevPop => {
            if state.count == 0 {
                None
            } else {
                Some((state.m2 / state.count as f64).sqrt())
            }
        }
        FinalizeKind::StddevSamp => {
            if state.count < 2 {
                None
            } else {
                Some((state.m2 / (state.count as f64 - 1.0)).sqrt())
            }
        }
    }
}

unsafe fn finalize_impl(states: &Vector, result: &mut Vector, count: usize, kind: FinalizeKind) {
    let state_ptrs = states.flat_data::<*mut u8>();
    let result_data = result.flat_data_mut::<f64>();

    for i in 0..count {
        let state = &*((*state_ptrs.add(i)) as *const VarianceState);
        if let Some(value) = variance_value(state, kind) {
            result.set_null(i, false);
            *result_data.add(i) = value;
        } else {
            result.set_null(i, true);
        }
    }
}

unsafe fn finalize_var_pop(
    states: &Vector,
    _input_data: &AggregateInputData,
    result: &mut Vector,
    count: usize,
) {
    finalize_impl(states, result, count, FinalizeKind::VarPop);
}

unsafe fn finalize_var_samp(
    states: &Vector,
    _input_data: &AggregateInputData,
    result: &mut Vector,
    count: usize,
) {
    finalize_impl(states, result, count, FinalizeKind::VarSamp);
}

unsafe fn finalize_stddev_pop(
    states: &Vector,
    _input_data: &AggregateInputData,
    result: &mut Vector,
    count: usize,
) {
    finalize_impl(states, result, count, FinalizeKind::StddevPop);
}

unsafe fn finalize_stddev_samp(
    states: &Vector,
    _input_data: &AggregateInputData,
    result: &mut Vector,
    count: usize,
) {
    finalize_impl(states, result, count, FinalizeKind::StddevSamp);
}

fn build_variance_set(name: &str, finalize: AggregateFinalizeFn) -> AggregateFunctionSet {
    let mut set = AggregateFunctionSet::new(name.to_string());
    let state_size = std::mem::size_of::<VarianceState>();

    set.add_function(AggregateFunction::new(
        name.to_string(),
        vec![LogicalType::Integer],
        LogicalType::Double,
        state_size,
        variance_i32::initialize,
        variance_i32::update,
        variance_i32::combine,
        finalize,
        Some(variance_i32::simple_update),
        None,
    ));
    set.add_function(AggregateFunction::new(
        name.to_string(),
        vec![LogicalType::BigInt],
        LogicalType::Double,
        state_size,
        variance_i64::initialize,
        variance_i64::update,
        variance_i64::combine,
        finalize,
        Some(variance_i64::simple_update),
        None,
    ));
    set.add_function(AggregateFunction::new(
        name.to_string(),
        vec![LogicalType::Double],
        LogicalType::Double,
        state_size,
        variance_f64::initialize,
        variance_f64::update,
        variance_f64::combine,
        finalize,
        Some(variance_f64::simple_update),
        None,
    ));

    set
}

fn alias_set(mut set: AggregateFunctionSet, alias_name: &str) -> AggregateFunctionSet {
    set.name = alias_name.to_string();
    for function in &mut set.functions {
        function.name = alias_name.to_string();
    }
    set
}

pub fn get_var_pop_function() -> AggregateFunctionSet {
    build_variance_set("var_pop", finalize_var_pop)
}

pub fn get_var_samp_function() -> AggregateFunctionSet {
    build_variance_set("var_samp", finalize_var_samp)
}

pub fn get_variance_function() -> AggregateFunctionSet {
    alias_set(get_var_samp_function(), "variance")
}

pub fn get_stddev_pop_function() -> AggregateFunctionSet {
    build_variance_set("stddev_pop", finalize_stddev_pop)
}

pub fn get_stddev_samp_function() -> AggregateFunctionSet {
    build_variance_set("stddev_samp", finalize_stddev_samp)
}

pub fn get_stddev_function() -> AggregateFunctionSet {
    alias_set(get_stddev_samp_function(), "stddev")
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

    unsafe fn run_simple_update(func: &AggregateFunction, input: &Vector) -> Vector {
        let mut arena = test_arena();
        let mut state_buf = vec![0u8; func.state_size];
        let state_ptr = state_buf.as_mut_ptr();

        (func.initialize)(state_ptr);
        let simple_update = func
            .simple_update
            .expect("variance aggregate should provide simple_update");
        {
            let input_data = preserve_input_data(func, &mut arena);
            simple_update(&[input], &input_data, state_ptr, input.len());
        }

        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);
        result.set_count(1);

        let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
        states.set_count(1);
        *states.flat_data_mut::<*mut u8>() = state_ptr;

        {
            let input_data = preserve_input_data(func, &mut arena);
            (func.finalize)(&states, &input_data, &mut result, 1);
        }

        result
    }

    #[test]
    fn var_pop_basic() {
        let func_set = get_var_pop_function();
        let (func, _) = func_set.bind(&[LogicalType::Integer]).unwrap();
        let input = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2, 3],
            paro_common::test_utils::test_allocator(),
        );

        let result = unsafe { run_simple_update(&func, &input) };
        assert!(!result.is_null(0));
        let value = result.get_f64(0).unwrap();
        assert!((value - (2.0 / 3.0)).abs() < 1e-10);
    }

    #[test]
    fn var_samp_alias_variance() {
        let var_set = get_var_samp_function();
        let variance_set = get_variance_function();
        let (var_func, _) = var_set.bind(&[LogicalType::Integer]).unwrap();
        let (variance_func, _) = variance_set.bind(&[LogicalType::Integer]).unwrap();
        let input = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2, 3],
            paro_common::test_utils::test_allocator(),
        );

        let var_result = unsafe { run_simple_update(&var_func, &input) };
        let variance_result = unsafe { run_simple_update(&variance_func, &input) };
        let var_value = var_result.get_f64(0).unwrap();
        let variance_value = variance_result.get_f64(0).unwrap();
        assert!((var_value - 1.0).abs() < 1e-10);
        assert!((var_value - variance_value).abs() < 1e-10);
    }

    #[test]
    fn stddev_samp_single_row_is_null() {
        let stddev_set = get_stddev_samp_function();
        let (func, _) = stddev_set.bind(&[LogicalType::Integer]).unwrap();
        let input = paro_common::test_utils::test_i32_vector_with_allocator(
            &[42],
            paro_common::test_utils::test_allocator(),
        );
        let result = unsafe { run_simple_update(&func, &input) };
        assert!(result.is_null(0));
    }
}
