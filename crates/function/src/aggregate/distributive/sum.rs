// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Sum Aggregate Function
//!
//!

use crate::aggregate::{
    AggregateAlgebra, AggregateFunction, AggregateFunctionSet, AggregateInputData,
    AggregateStateInput,
};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

#[repr(C)]
struct SumState<T> {
    value: T,
    is_null: bool,
}

trait SumAccumulator<Input, Output: Copy>: Copy + Default {
    fn add_input(&mut self, input: Input);
    fn add_state(&mut self, source: Self);
    fn output(self) -> Result<Output>;
}

impl SumAccumulator<f64, f64> for f64 {
    #[inline]
    fn add_input(&mut self, input: f64) {
        *self += input;
    }

    #[inline]
    fn add_state(&mut self, source: Self) {
        *self += source;
    }

    #[inline]
    fn output(self) -> Result<f64> {
        Ok(self)
    }
}

trait IntegerSumOutput: Copy {
    const NAME: &'static str;

    fn try_from_i128(value: i128) -> Option<Self>;
}

impl IntegerSumOutput for i64 {
    const NAME: &'static str = "BIGINT";

    fn try_from_i128(value: i128) -> Option<Self> {
        i64::try_from(value).ok()
    }
}

impl IntegerSumOutput for i128 {
    const NAME: &'static str = "HUGEINT";

    fn try_from_i128(value: i128) -> Option<Self> {
        Some(value)
    }
}

/// Exact signed i128 accumulator using the aggregate engine's 8-byte state
/// alignment. `overflowed` is out-of-band so every i128 value remains valid.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct CheckedI128 {
    words: [u64; 2],
    overflowed: bool,
}

impl CheckedI128 {
    #[inline]
    fn value(self) -> i128 {
        (((self.words[1] as u128) << 64) | self.words[0] as u128) as i128
    }

    #[inline]
    fn set(&mut self, value: i128) {
        let value = value as u128;
        self.words = [value as u64, (value >> 64) as u64];
    }

    #[inline]
    fn add(&mut self, value: i128) {
        if self.overflowed {
            return;
        }
        match self.value().checked_add(value) {
            Some(value) => self.set(value),
            None => self.overflowed = true,
        }
    }
}

macro_rules! i128_sum_accumulator {
    ($input:ty, $output:ty) => {
        impl SumAccumulator<$input, $output> for CheckedI128 {
            #[inline]
            fn add_input(&mut self, input: $input) {
                self.add(input as i128);
            }

            #[inline]
            fn add_state(&mut self, source: Self) {
                if source.overflowed {
                    self.overflowed = true;
                } else {
                    self.add(source.value());
                }
            }

            #[inline]
            fn output(self) -> Result<$output> {
                if self.overflowed {
                    return Err(paro_error::out_of_range("Integer SUM aggregate overflow"));
                }
                <$output as IntegerSumOutput>::try_from_i128(self.value()).ok_or_else(|| {
                    paro_error::out_of_range(format!(
                        "Integer SUM result exceeds {}",
                        <$output as IntegerSumOutput>::NAME
                    ))
                })
            }
        }
    };
}

i128_sum_accumulator!(i32, i64);
i128_sum_accumulator!(i64, i64);
i128_sum_accumulator!(i64, i128);
i128_sum_accumulator!(i128, i128);

// Function generator macro
macro_rules! define_sum_impl {
    ($mod_name:ident, $input_type:ty, $accumulator_type:ty, $output_type:ty) => {
        mod $mod_name {
            use super::*;

            type State = SumState<$accumulator_type>;

            pub unsafe fn initialize(state: *mut u8) {
                let state = state as *mut State;
                (*state).value = <$accumulator_type>::default();
                (*state).is_null = true;
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

                        let value: $input_type = input.get_fixed(i);
                        <$accumulator_type as SumAccumulator<$input_type, $output_type>>::add_input(
                            &mut (*state).value,
                            value,
                        );
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
                        let value: $input_type = input.get_fixed(i);
                        <$accumulator_type as SumAccumulator<$input_type, $output_type>>::add_input(
                            &mut (*state).value,
                            value,
                        );
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
                        <$accumulator_type as SumAccumulator<$input_type, $output_type>>::add_state(
                            &mut target_state.value,
                            source_state.value,
                        );
                        target_state.is_null = false;
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
                let result_data = result.flat_data_mut::<$output_type>();

                for i in 0..count {
                    let state_ptr = *state_ptrs.add(i);
                    let state = &*(state_ptr as *const State);

                    if state.is_null {
                        result.set_null(i, true);
                    } else {
                        result.set_null(i, false);
                        *result_data.add(i) = <$accumulator_type as SumAccumulator<
                            $input_type,
                            $output_type,
                        >>::output(state.value)?;
                    }
                }
                Ok(())
            }

            pub fn function(
                name: &str,
                argument: LogicalType,
                return_type: LogicalType,
            ) -> AggregateFunction {
                let supports_input_rollup = super::exact_sum_signature(&argument, &return_type);
                let function = AggregateFunction::new(
                    name.to_string(),
                    vec![argument],
                    return_type,
                    std::mem::size_of::<State>(),
                    initialize,
                    update,
                    combine,
                    finalize,
                    Some(simple_update),
                    None,
                )
                .with_algebra(AggregateAlgebra::Sum)
                .with_partial_merge(super::sum_partial_merge);
                let function = if supports_input_rollup {
                    function.with_input_rollup(super::exact_sum_input_rollup)
                } else {
                    function
                };
                // SAFETY: primitive SUM state is an inline value and null
                // marker with no external ownership or destructor.
                unsafe { function.with_trivially_copyable_state() }
            }
        }
    };
}

// Define implementations for types
define_sum_impl!(sum_i32, i32, CheckedI128, i64); // Integer -> BigInt
define_sum_impl!(sum_i64, i64, CheckedI128, i128); // BigInt -> HugeInt
define_sum_impl!(sum_f64, f64, f64, f64); // Double -> Double
define_sum_impl!(merge_i64, i64, CheckedI128, i64); // BigInt -> BigInt
define_sum_impl!(merge_i128, i128, CheckedI128, i128); // HugeInt -> HugeInt

fn exact_sum_signature(argument: &LogicalType, return_type: &LogicalType) -> bool {
    matches!(
        (argument, return_type),
        (LogicalType::Integer, LogicalType::BigInt)
            | (
                LogicalType::BigInt,
                LogicalType::HugeInt | LogicalType::BigInt
            )
    )
}

/// Rebind the canonical exact SUM over the original input domain. Returning a
/// fresh descriptor instead of cloning `source` lets physical verification
/// detect a descriptor whose executable hooks or bind data were altered after
/// binding while retaining the capability pointer.
fn exact_sum_input_rollup(source: &AggregateFunction) -> Option<AggregateFunction> {
    match (source.arguments.as_slice(), &source.return_type) {
        ([LogicalType::Integer], LogicalType::BigInt) => Some(sum_i32::function(
            "sum_input_rollup",
            LogicalType::Integer,
            LogicalType::BigInt,
        )),
        ([LogicalType::BigInt], LogicalType::HugeInt) => Some(sum_i64::function(
            "sum_input_rollup",
            LogicalType::BigInt,
            LogicalType::HugeInt,
        )),
        ([LogicalType::BigInt], LogicalType::BigInt) => Some(merge_i64::function(
            "sum_input_rollup",
            LogicalType::BigInt,
            LogicalType::BigInt,
        )),
        _ => None,
    }
}

/// Build the closed aggregate used to merge finalized integral and floating
/// SUM partials. SQL SUM cannot provide this operation directly for integral
/// results because it widens BIGINT to HUGEINT.
fn sum_partial_merge(source: &AggregateFunction) -> Option<AggregateFunction> {
    match &source.return_type {
        LogicalType::BigInt => Some(merge_i64::function(
            "sum_partial_merge",
            LogicalType::BigInt,
            LogicalType::BigInt,
        )),
        LogicalType::HugeInt => Some(merge_i128::function(
            "sum_partial_merge",
            LogicalType::HugeInt,
            LogicalType::HugeInt,
        )),
        LogicalType::Double => Some(sum_f64::function(
            "sum_partial_merge",
            LogicalType::Double,
            LogicalType::Double,
        )),
        _ => None,
    }
}

pub fn get_sum_function() -> AggregateFunctionSet {
    let mut set = AggregateFunctionSet::new("sum".to_string());
    set.set_dynamic_bind(super::decimal::bind_sum);

    // Integer -> BigInt
    set.add_function(sum_i32::function(
        "sum",
        LogicalType::Integer,
        LogicalType::BigInt,
    ));

    // BigInt -> HugeInt
    set.add_function(sum_i64::function(
        "sum",
        LogicalType::BigInt,
        LogicalType::HugeInt,
    ));

    // Double -> Double
    set.add_function(sum_f64::function(
        "sum",
        LogicalType::Double,
        LogicalType::Double,
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

    fn execute_single(function: &AggregateFunction, input: &Vector) -> Result<Vector> {
        let mut storage = vec![0_u64; function.state_size.div_ceil(std::mem::size_of::<u64>())];
        let state_ptr = storage.as_mut_ptr().cast::<u8>();
        let mut arena = test_arena();
        unsafe {
            (function.initialize)(state_ptr);
            let input_data = preserve_input_data(function, &mut arena);
            function.simple_update.unwrap()(&[input], &input_data, state_ptr, input.len());
        }

        let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
        states.set_count(1);
        unsafe { *states.flat_data_mut::<*mut u8>() = state_ptr };
        let mut result = paro_common::test_utils::test_vector(function.return_type.clone());
        result.set_count(1);
        unsafe {
            let input_data = preserve_input_data(function, &mut arena);
            (function.finalize)(&states, &input_data, &mut result, 1)?;
        }
        Ok(result)
    }

    #[test]
    fn primitive_sum_states_respect_aggregate_alignment() {
        assert_eq!(std::mem::align_of::<SumState<CheckedI128>>(), 8);
        assert_eq!(std::mem::align_of::<SumState<f64>>(), 8);
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

            let input = paro_common::test_utils::test_i32_vector_with_allocator(
                &[1, 2, 3, 4],
                paro_common::test_utils::test_allocator(),
            );

            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input], &input_data, state_ptr, 4);
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

            let mut input = paro_common::test_utils::test_i32_vector_with_allocator(
                &[1, 0],
                paro_common::test_utils::test_allocator(),
            );
            input.set_null(1, true); // [1, NULL]

            if let Some(simple_update) = func.simple_update {
                {
                    let input_data = preserve_input_data(&func, &mut arena);
                    simple_update(&[&input], &input_data, state_ptr, 2);
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

            assert_eq!(result.get_flat::<i64>(0), 1);
            assert!(!result.is_null(0));
        }
    }

    #[test]
    fn finalized_sum_partials_merge_without_widening() {
        let cases = [
            (LogicalType::Integer, LogicalType::BigInt),
            (LogicalType::BigInt, LogicalType::HugeInt),
            (LogicalType::Double, LogicalType::Double),
        ];
        for (input_type, return_type) in cases {
            let (sum, _) = get_sum_function().bind(&[input_type]).unwrap();
            let merge = sum.partial_merge_function().unwrap();
            assert_eq!(merge.arguments, vec![return_type.clone()]);
            assert_eq!(merge.return_type, return_type);

            let next_merge = merge.partial_merge_function().unwrap();
            assert_eq!(next_merge.arguments, merge.arguments);
            assert_eq!(next_merge.return_type, merge.return_type);
        }

        let (integer_sum, _) = get_sum_function().bind(&[LogicalType::Integer]).unwrap();
        let integer_merge = integer_sum.partial_merge_function().unwrap();
        let mut integer_partials = paro_common::test_utils::test_i64_vector(&[2, 0, 5]);
        integer_partials.set_null(1, true);
        let integer_result = execute_single(&integer_merge, &integer_partials).unwrap();
        assert_eq!(integer_result.get_i64(0), Some(7));

        let (bigint_sum, _) = get_sum_function().bind(&[LogicalType::BigInt]).unwrap();
        let bigint_merge = bigint_sum.partial_merge_function().unwrap();
        let mut bigint_partials = paro_common::test_utils::test_vector(LogicalType::HugeInt);
        bigint_partials.set_count(3);
        bigint_partials.set_i128(0, i128::from(i64::MAX) + 1);
        bigint_partials.set_i128(1, -7);
        bigint_partials.set_i128(2, 11);
        let bigint_result = execute_single(&bigint_merge, &bigint_partials).unwrap();
        assert_eq!(bigint_result.get_i128(0), Some(i128::from(i64::MAX) + 5));

        let (double_sum, _) = get_sum_function().bind(&[LogicalType::Double]).unwrap();
        let double_merge = double_sum.partial_merge_function().unwrap();
        let double_partials = paro_common::test_utils::test_f64_vector(&[1.25, 2.75]);
        let double_result = execute_single(&double_merge, &double_partials).unwrap();
        assert_eq!(double_result.get_f64(0), Some(4.0));
    }

    #[test]
    fn exact_sum_exposes_canonical_input_rollup_but_double_does_not() {
        let (integer_sum, _) = get_sum_function().bind(&[LogicalType::Integer]).unwrap();
        let integer_rollup = integer_sum.input_rollup_function().unwrap();
        assert!(integer_sum.execution_semantics_equal(&integer_rollup));
        let integer_reducer = integer_sum.partial_merge_function().unwrap();
        let integer_reducer_rollup = integer_reducer.input_rollup_function().unwrap();
        assert!(integer_reducer.execution_semantics_equal(&integer_reducer_rollup));

        let (bigint_sum, _) = get_sum_function().bind(&[LogicalType::BigInt]).unwrap();
        let bigint_rollup = bigint_sum.input_rollup_function().unwrap();
        assert!(bigint_sum.execution_semantics_equal(&bigint_rollup));
        assert!(bigint_sum
            .partial_merge_function()
            .unwrap()
            .input_rollup_function()
            .is_none());

        let (double_sum, _) = get_sum_function().bind(&[LogicalType::Double]).unwrap();
        assert!(double_sum.input_rollup_function().is_none());
        assert!(double_sum
            .partial_merge_function()
            .unwrap()
            .input_rollup_function()
            .is_none());
    }

    #[test]
    fn aggregate_execution_identity_covers_hooks_and_capabilities_not_aliases() {
        let (sum, _) = get_sum_function().bind(&[LogicalType::Integer]).unwrap();
        let mut alias = sum.clone();
        alias.name = "total".to_string();
        assert!(sum.execution_semantics_equal(&alias));

        let mut different_update = sum.clone();
        different_update.update = sum_f64::update;
        assert!(!sum.execution_semantics_equal(&different_update));

        let mut without_rollup = sum.clone();
        without_rollup.input_rollup = None;
        assert!(!sum.execution_semantics_equal(&without_rollup));

        let reducer = sum.partial_merge_function().unwrap();
        assert!(!sum.execution_semantics_equal(&reducer));
    }

    #[test]
    fn finalized_sum_partials_preserve_empty_input_null() {
        let (sum, _) = get_sum_function().bind(&[LogicalType::Integer]).unwrap();
        let merge = sum.partial_merge_function().unwrap();
        let mut partials = paro_common::test_utils::test_i64_vector(&[0]);
        partials.set_null(0, true);

        let result = execute_single(&merge, &partials).unwrap();
        assert!(result.is_null(0));
    }

    #[test]
    fn integer_sum_reports_bigint_result_overflow() {
        let (sum, _) = get_sum_function().bind(&[LogicalType::Integer]).unwrap();
        let mut accumulator = CheckedI128::default();
        accumulator.set(i128::from(i64::MAX) + 1);
        let state = SumState {
            value: accumulator,
            is_null: false,
        };
        let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
        states.set_count(1);
        unsafe {
            *states.flat_data_mut::<*mut u8>() = std::ptr::from_ref(&state).cast_mut().cast();
        }
        let mut result = paro_common::test_utils::test_vector(LogicalType::BigInt);
        result.set_count(1);
        let mut arena = test_arena();
        let input_data = preserve_input_data(&sum, &mut arena);

        let error = unsafe { (sum.finalize)(&states, &input_data, &mut result, 1) }.unwrap_err();
        assert!(error
            .to_string()
            .contains("Integer SUM result exceeds BIGINT"));
    }

    #[test]
    fn finalized_bigint_partials_do_not_wrap_before_finalization() {
        let (sum, _) = get_sum_function().bind(&[LogicalType::Integer]).unwrap();
        let merge = sum.partial_merge_function().unwrap();
        for values in [[i64::MAX, 1], [i64::MIN, -1]] {
            let partials = paro_common::test_utils::test_i64_vector(&values);
            let error = execute_single(&merge, &partials).unwrap_err();
            assert!(error
                .to_string()
                .contains("Integer SUM result exceeds BIGINT"));
        }

        for boundary in [i64::MIN, i64::MAX] {
            let partials = paro_common::test_utils::test_i64_vector(&[boundary]);
            let result = execute_single(&merge, &partials).unwrap();
            assert_eq!(result.get_i64(0), Some(boundary));
        }
    }

    #[test]
    fn hugeint_sum_reports_accumulator_overflow() {
        let (sum, _) = get_sum_function().bind(&[LogicalType::BigInt]).unwrap();
        let merge = sum.partial_merge_function().unwrap();
        let mut partials = paro_common::test_utils::test_vector(LogicalType::HugeInt);
        partials.set_count(2);
        partials.set_i128(0, i128::MAX);
        partials.set_i128(1, 1);

        let error = execute_single(&merge, &partials).unwrap_err();
        assert!(error.to_string().contains("Integer SUM aggregate overflow"));

        partials.set_i128(0, i128::MIN);
        partials.set_i128(1, -1);
        let error = execute_single(&merge, &partials).unwrap_err();
        assert!(error.to_string().contains("Integer SUM aggregate overflow"));
    }

    #[test]
    fn combining_overflowed_integer_sum_state_stays_overflowed() {
        let (sum, _) = get_sum_function().bind(&[LogicalType::BigInt]).unwrap();
        let mut source = SumState {
            value: CheckedI128::default(),
            is_null: false,
        };
        source.value.add(i128::MAX);
        source.value.add(1);
        let mut target = SumState {
            value: CheckedI128::default(),
            is_null: true,
        };
        let mut source_states = paro_common::test_utils::test_vector(LogicalType::BigInt);
        let mut target_states = paro_common::test_utils::test_vector(LogicalType::BigInt);
        source_states.set_count(1);
        target_states.set_count(1);
        unsafe {
            *source_states.flat_data_mut::<*mut u8>() = std::ptr::from_mut(&mut source).cast();
            *target_states.flat_data_mut::<*mut u8>() = std::ptr::from_mut(&mut target).cast();
        }
        let mut arena = test_arena();
        let input_data = preserve_input_data(&sum, &mut arena);
        unsafe { (sum.combine)(&source_states, &target_states, &input_data, 1) };

        assert!(target.value.overflowed);
        assert!(!target.is_null);
    }
}
