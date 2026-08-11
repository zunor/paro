// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use paro_common::allocator::{default_allocator, ArenaAllocator, MemoryTag};
use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
use std::sync::Arc;

fn initialized_narrow_state() -> DecimalNarrowState {
    DecimalNarrowState {
        value_words: DecimalNarrowState::UNSET,
    }
}

fn initialized_sum_state() -> DecimalSumState {
    DecimalSumState {
        value_words: DecimalSumState::UNSET,
    }
}

fn initialized_average_state() -> DecimalAverageState {
    DecimalAverageState {
        value_words: [0; 4],
        count: 0,
        is_set: false,
        overflowed: false,
        wide: false,
    }
}

#[test]
fn narrow_decimal_state_keeps_lifecycle_in_band() {
    assert_eq!(std::mem::size_of::<DecimalNarrowState>(), 16);

    let mut state = initialized_narrow_state();
    assert!(!state.is_set());
    state.add_i64(7);
    assert!(state.is_set());
    assert!(!state.overflowed());
    assert_eq!(state.value(), 7);
    state.mark_overflowed();
    assert!(state.overflowed());
}

#[test]
fn decimal_accumulators_promote_from_i64_without_losing_exactness() {
    let mut sum = initialized_narrow_state();
    sum.add_i64(i64::MAX);
    assert!(sum.value_is_i64());
    sum.add_i64(1);
    assert!(!sum.value_is_i64());
    assert_eq!(sum.value(), i128::from(i64::MAX) + 1);

    let mut average = initialized_average_state();
    assert!(average.add_i64(i64::MAX));
    assert!(average.value_is_i64());
    assert!(average.add_i64(1));
    assert!(!average.value_is_i64());
    assert_eq!(average.value(), i256::from(i128::from(i64::MAX) + 1));
}

#[test]
fn direct_decimal_program_fuses_shared_group_and_input_updates() {
    let input_type = LogicalType::Decimal {
        precision: 15,
        scale: 2,
    };
    let (sum, _) = bind_sum(std::slice::from_ref(&input_type)).unwrap();
    let (average, _) = bind_avg(std::slice::from_ref(&input_type)).unwrap();
    let sum_offset = 0;
    let average_offset = std::mem::size_of::<DecimalNarrowState>();
    let count_offset = average_offset + std::mem::size_of::<DecimalAverageState>();
    let mut program = crate::aggregate::DirectGroupedAggregateProgram::new(3);
    assert!(program.try_add(0, sum.direct_update, sum_offset, Some(0), true));
    assert!(program.try_add(1, average.direct_update, average_offset, Some(0), true,));
    assert!(program.try_add(
        2,
        Some(AggregateDirectUpdate::CountStar),
        count_offset,
        None,
        true,
    ));
    assert!(program.has_updates());

    let state_bytes = count_offset + std::mem::size_of::<i64>();
    let mut storage = vec![0_u64; state_bytes.div_ceil(std::mem::size_of::<u64>())];
    let base = storage.as_mut_ptr().cast::<u8>();
    unsafe {
        initialize_narrow(base.add(sum_offset));
        initialize_average(base.add(average_offset));
        *base.add(count_offset).cast::<i64>() = 0;
    }
    let mut addresses = paro_common::test_utils::test_vector(LogicalType::BigInt);
    addresses.set_count(2);
    unsafe {
        let values = addresses.flat_data_mut::<*mut u8>();
        *values = base;
        *values.add(1) = base;
    }
    let mut values = paro_common::test_utils::test_vector(input_type);
    values.set_count(2);
    values.set_i64(0, 100);
    values.set_i64(1, 200);
    let payload = paro_common::test_utils::test_chunk_from_vectors(vec![values]);
    assert!(unsafe { program.execute(&payload, &addresses, 2) }.unwrap());
    let sum = unsafe { &*base.add(sum_offset).cast::<DecimalNarrowState>() };
    let average = unsafe { &*base.add(average_offset).cast::<DecimalAverageState>() };
    assert_eq!(sum.value(), 300);
    assert_eq!(average.value(), i256::from(300));
    assert_eq!(average.count, 2);
    assert_eq!(unsafe { *base.add(count_offset).cast::<i64>() }, 2);
}

#[test]
fn direct_decimal_program_uses_the_bound_i128_physical_width() {
    let input_type = LogicalType::Decimal {
        precision: 31,
        scale: 4,
    };
    let (sum, _) = bind_sum(std::slice::from_ref(&input_type)).unwrap();
    let (average, _) = bind_avg(std::slice::from_ref(&input_type)).unwrap();
    assert_eq!(
        sum.direct_update,
        Some(AggregateDirectUpdate::Decimal(
            DecimalDirectUpdate::WideSumI128
        ))
    );
    assert_eq!(
        average.direct_update,
        Some(AggregateDirectUpdate::Decimal(
            DecimalDirectUpdate::AverageI128
        ))
    );

    let sum_offset = 0;
    let average_offset = std::mem::size_of::<DecimalSumState>();
    let mut program = crate::aggregate::DirectGroupedAggregateProgram::new(2);
    assert!(program.try_add(0, sum.direct_update, sum_offset, Some(0), true));
    assert!(program.try_add(1, average.direct_update, average_offset, Some(0), true,));

    let state_bytes = average_offset + std::mem::size_of::<DecimalAverageState>();
    let mut storage = vec![0_u64; state_bytes.div_ceil(std::mem::size_of::<u64>())];
    let base = storage.as_mut_ptr().cast::<u8>();
    unsafe {
        initialize_sum(base.add(sum_offset));
        initialize_average(base.add(average_offset));
    }
    let mut addresses = paro_common::test_utils::test_vector(LogicalType::BigInt);
    addresses.set_count(2);
    unsafe {
        let values = addresses.flat_data_mut::<*mut u8>();
        *values = base;
        *values.add(1) = base;
    }
    let mut values = paro_common::test_utils::test_vector(input_type);
    values.set_count(2);
    values.set_i128(0, i128::from(i64::MAX) + 7);
    values.set_i128(1, i128::from(i64::MAX) + 11);
    let payload = paro_common::test_utils::test_chunk_from_vectors(vec![values]);
    assert!(unsafe { program.execute(&payload, &addresses, 2) }.unwrap());

    let expected = i128::from(i64::MAX) * 2 + 18;
    let sum = unsafe { &*base.add(sum_offset).cast::<DecimalSumState>() };
    let average = unsafe { &*base.add(average_offset).cast::<DecimalAverageState>() };
    assert_eq!(sum.try_i128(), Some(expected));
    assert_eq!(average.value(), i256::from(expected));
    assert_eq!(average.count, 2);
}

#[test]
fn reduced_direct_decimal_program_aggregates_i128_values_by_slot() {
    let input_type = LogicalType::Decimal {
        precision: 31,
        scale: 4,
    };
    let (sum, _) = bind_sum(std::slice::from_ref(&input_type)).unwrap();
    let (average, _) = bind_avg(std::slice::from_ref(&input_type)).unwrap();
    let sum_offset = 0;
    let average_offset = std::mem::size_of::<DecimalSumState>();
    let count_offset = average_offset + std::mem::size_of::<DecimalAverageState>();
    let state_stride = count_offset + std::mem::size_of::<i64>();
    let mut program = crate::aggregate::DirectGroupedAggregateProgram::new(3);
    assert!(program.try_add(0, sum.direct_update, sum_offset, Some(0), true));
    assert!(program.try_add(1, average.direct_update, average_offset, Some(0), true,));
    assert!(program.try_add(
        2,
        Some(AggregateDirectUpdate::CountStar),
        count_offset,
        None,
        true,
    ));

    let mut storage = vec![0_u64; (state_stride * 2).div_ceil(std::mem::size_of::<u64>())];
    let state_base = storage.as_mut_ptr().cast::<u8>();
    for slot in 0..2 {
        let base = unsafe { state_base.add(slot * state_stride) };
        unsafe {
            initialize_sum(base.add(sum_offset));
            initialize_average(base.add(average_offset));
            *base.add(count_offset).cast::<i64>() = 0;
        }
    }

    let values_raw = [
        i128::from(i64::MAX) + 7,
        i128::from(i64::MAX) + 11,
        i128::from(i64::MAX) + 13,
    ];
    let mut values = paro_common::test_utils::test_vector(input_type);
    values.set_count(values_raw.len());
    for (row, value) in values_raw.into_iter().enumerate() {
        values.set_i128(row, value);
    }
    let payload = paro_common::test_utils::test_chunk_from_vectors(vec![values]);
    let prepared = program.prepare_input(&payload).expect("prepared input");
    let slots_raw = [0_usize, 1, 0];
    let slots =
        crate::aggregate::ValidatedDirectGroupSlots::try_new(&slots_raw, slots_raw.len(), 2)
            .unwrap();
    let memory =
        MemoryAccountingContext::detached(MemoryTag::HashTable, MemoryAccountingClass::Revocable);
    let mut scratch = program.try_create_scratch(2, &memory).unwrap().unwrap();
    assert!(unsafe {
        program.execute_reduced_slots_prepared(
            &prepared,
            &slots,
            &mut scratch,
            state_base,
            state_stride,
        )
    }
    .unwrap());

    for (slot, expected_rows) in [(0, [0_usize, 2].as_slice()), (1, [1].as_slice())] {
        let base = unsafe { state_base.add(slot * state_stride) };
        let expected: i128 = expected_rows.iter().map(|row| values_raw[*row]).sum();
        let sum = unsafe { &*base.add(sum_offset).cast::<DecimalSumState>() };
        let average = unsafe { &*base.add(average_offset).cast::<DecimalAverageState>() };
        assert_eq!(sum.try_i128(), Some(expected));
        assert_eq!(average.value(), i256::from(expected));
        assert_eq!(average.count, expected_rows.len() as u64);
        assert_eq!(
            unsafe { *base.add(count_offset).cast::<i64>() },
            expected_rows.len() as i64
        );
    }
}

#[test]
fn reduced_direct_decimal_program_promotes_batch_totals_beyond_i128() {
    let input_type = LogicalType::Decimal {
        precision: 38,
        scale: 0,
    };
    let (sum, _) = bind_sum(std::slice::from_ref(&input_type)).unwrap();
    let (average, _) = bind_avg(std::slice::from_ref(&input_type)).unwrap();
    let sum_offset = 0;
    let average_offset = std::mem::size_of::<DecimalSumState>();
    let state_stride = average_offset + std::mem::size_of::<DecimalAverageState>();
    let mut program = crate::aggregate::DirectGroupedAggregateProgram::new(2);
    assert!(program.try_add(0, sum.direct_update, sum_offset, Some(0), true));
    assert!(program.try_add(1, average.direct_update, average_offset, Some(0), true,));

    let mut storage = vec![0_u64; state_stride.div_ceil(std::mem::size_of::<u64>())];
    let state_base = storage.as_mut_ptr().cast::<u8>();
    unsafe {
        initialize_sum(state_base.add(sum_offset));
        initialize_average(state_base.add(average_offset));
    }
    let maximum = 10_i128.pow(38) - 1;
    let values_raw = [maximum, maximum, -maximum];
    let mut values = paro_common::test_utils::test_vector(input_type);
    values.set_count(values_raw.len());
    for (row, value) in values_raw.into_iter().enumerate() {
        values.set_i128(row, value);
    }
    let payload = paro_common::test_utils::test_chunk_from_vectors(vec![values]);
    let prepared = program.prepare_input(&payload).expect("prepared input");
    let slots_raw = [0_usize; 3];
    let slots =
        crate::aggregate::ValidatedDirectGroupSlots::try_new(&slots_raw, slots_raw.len(), 1)
            .unwrap();
    let memory =
        MemoryAccountingContext::detached(MemoryTag::HashTable, MemoryAccountingClass::Revocable);
    let mut scratch = program.try_create_scratch(1, &memory).unwrap().unwrap();
    assert!(unsafe {
        program.execute_reduced_slots_prepared(
            &prepared,
            &slots,
            &mut scratch,
            state_base,
            state_stride,
        )
    }
    .unwrap());

    let sum = unsafe { &*state_base.add(sum_offset).cast::<DecimalSumState>() };
    let average = unsafe { &*state_base.add(average_offset).cast::<DecimalAverageState>() };
    assert_eq!(sum.try_i128(), Some(maximum));
    assert_eq!(average.value(), i256::from(maximum));
    assert_eq!(average.count, 3);
}

unsafe fn finalize_single<T>(state: &mut T, data: &DecimalAggregateBindData) -> Result<Vector> {
    let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
    states.set_count(1);
    *states.flat_data_mut::<*mut u8>() = state as *mut T as *mut u8;
    let mut result = paro_common::test_utils::test_vector(LogicalType::Decimal {
        precision: data.output_precision,
        scale: data.output_scale,
    });
    result.set_count(1);
    let mut arena = ArenaAllocator::new(Arc::new(default_allocator()));
    let input_data = AggregateInputData::new(
        Some(data),
        &mut arena,
        crate::aggregate::AggregateCombineType::PreserveInput,
    );
    finalize(&states, &input_data, &mut result, 1)?;
    Ok(result)
}

#[test]
fn decimal_state_obeys_the_eight_byte_aggregate_alignment_contract() {
    assert_eq!(std::mem::align_of::<DecimalNarrowState>(), 8);
    assert_eq!(std::mem::align_of::<DecimalSumState>(), 8);
    assert_eq!(std::mem::align_of::<DecimalAverageState>(), 8);
    assert!(std::mem::size_of::<DecimalNarrowState>() < std::mem::size_of::<DecimalSumState>());
    assert!(std::mem::size_of::<DecimalNarrowState>() < std::mem::size_of::<DecimalAverageState>());
    let state_words = std::mem::size_of::<DecimalAverageState>().div_ceil(8);
    let mut storage = vec![0_u64; state_words + 1];
    let base = storage.as_mut_ptr() as *mut u8;
    let offset = if (base as usize).is_multiple_of(16) {
        8
    } else {
        0
    };
    let state_ptr = unsafe { base.add(offset) };
    assert_eq!((state_ptr as usize) % 8, 0);
    assert_ne!((state_ptr as usize) % 16, 0);

    unsafe { initialize_average(state_ptr) };
    let state = unsafe { &mut *(state_ptr as *mut DecimalAverageState) };
    let expected = i256::from(-123_456_789_012_345_678_901_234_567_890_i128);
    state.set_value(expected);
    assert_eq!(state.value(), expected);
}

#[test]
fn decimal_aggregate_binding_preserves_exact_result_shapes() {
    let input = LogicalType::Decimal {
        precision: 15,
        scale: 2,
    };
    let (sum, targets) = bind_sum(std::slice::from_ref(&input)).unwrap();
    assert_eq!(targets, vec![input.clone()]);
    assert_eq!(sum.state_size, std::mem::size_of::<DecimalNarrowState>());
    assert_eq!(
        sum.return_type,
        LogicalType::Decimal {
            precision: 38,
            scale: 2
        }
    );

    let (avg, _) = bind_avg(&[input]).unwrap();
    assert_eq!(
        avg.return_type,
        LogicalType::Decimal {
            precision: 38,
            scale: 6
        }
    );

    let (wide_sum, _) = bind_sum(&[LogicalType::Decimal {
        precision: 38,
        scale: 0,
    }])
    .unwrap();
    assert_eq!(wide_sum.state_size, std::mem::size_of::<DecimalSumState>());

    let (wide_avg, _) = bind_avg(&[LogicalType::Decimal {
        precision: 38,
        scale: 0,
    }])
    .unwrap();
    assert_eq!(
        wide_avg.return_type,
        LogicalType::Decimal {
            precision: 38,
            scale: 0
        }
    );
}

#[test]
fn decimal_sum_set_prefers_dynamic_binding_over_double_coercion() {
    let input = LogicalType::Decimal {
        precision: 15,
        scale: 2,
    };
    let (sum, targets) = crate::aggregate::distributive::sum::get_sum_function()
        .bind(std::slice::from_ref(&input))
        .unwrap();

    assert_eq!(targets, vec![input]);
    assert_eq!(
        sum.return_type,
        LogicalType::Decimal {
            precision: 38,
            scale: 2
        }
    );
}

#[test]
fn decimal_sum_reports_declared_precision_overflow() {
    let data = DecimalAggregateBindData {
        op: DecimalAggregateOp::Sum,
        input_scale: 0,
        output_precision: 38,
        output_scale: 0,
        output_limit: 10_i128.pow(38),
        wide_sum: true,
    };
    let mut state = initialized_sum_state();
    state.set_i128(10_i128.pow(38));

    let error = unsafe { finalize_single(&mut state, &data) }.unwrap_err();
    assert!(error
        .to_string()
        .contains("Decimal SUM result exceeds precision 38"));
}

#[test]
fn decimal_sum_is_exact_across_i128_intermediate_overflow() {
    let max_decimal = 10_i128.pow(38) - 1;
    let mut state = initialized_sum_state();
    state.add_i128(max_decimal);
    state.add_i128(max_decimal);
    assert!(state.try_i128().is_none());
    state.add_i128(-max_decimal);
    assert_eq!(state.try_i128(), Some(max_decimal));
    assert!(!state.overflowed());
}

#[test]
fn decimal_sum_state_filter_preserves_null_and_precision_semantics() {
    let data = DecimalAggregateBindData {
        op: DecimalAggregateOp::Sum,
        input_scale: 2,
        output_precision: 38,
        output_scale: 2,
        output_limit: 10_i128.pow(38),
        wide_sum: false,
    };
    let mut states = [
        initialized_narrow_state(),
        initialized_narrow_state(),
        initialized_narrow_state(),
        initialized_narrow_state(),
    ];
    for (state, value) in states.iter_mut().zip([29_900, 30_000, 30_100]) {
        state.set_value(value);
    }

    let mut addresses = paro_common::test_utils::test_vector(LogicalType::BigInt);
    addresses.set_count(states.len());
    for (row, state) in states.iter_mut().enumerate() {
        unsafe {
            *addresses.flat_data_mut::<*mut u8>().add(row) = state as *mut _ as *mut u8;
        }
    }
    let state_input = AggregateStateInput::try_new(&addresses, 0, None, states.len()).unwrap();
    let mut arena = ArenaAllocator::new(Arc::new(default_allocator()));
    let input_data = AggregateInputData::new(
        Some(&data),
        &mut arena,
        crate::aggregate::AggregateCombineType::PreserveInput,
    );
    let mut selection = paro_common::vector::SelectionVector::try_with_capacity(
        states.len(),
        paro_common::test_utils::test_allocator(),
    )
    .unwrap();
    let selected = unsafe {
        filter_narrow_sum_state(
            &state_input,
            &input_data,
            AggregateComparison::GreaterThan,
            &paro_common::runtime_value::Value::Decimal(30_000, 38, 2),
            &mut selection,
            states.len(),
        )
    }
    .unwrap();
    assert_eq!(selected, 1);
    assert_eq!(selection.as_slice(), &[2]);

    let error = unsafe {
        filter_narrow_sum_state(
            &state_input,
            &input_data,
            AggregateComparison::GreaterThan,
            &paro_common::runtime_value::Value::Decimal(300_000, 38, 3),
            &mut selection,
            states.len(),
        )
    }
    .unwrap_err();
    assert!(error.to_string().contains("constant type mismatch"));

    states[0].set_value(10_i128.pow(38));
    let error = unsafe {
        filter_narrow_sum_state(
            &state_input,
            &input_data,
            AggregateComparison::GreaterThan,
            &paro_common::runtime_value::Value::Decimal(30_000, 38, 2),
            &mut selection,
            states.len(),
        )
    }
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("Decimal SUM result exceeds precision 38"));
}

#[test]
fn decimal_avg_uses_wide_accumulator_before_division() {
    let data = DecimalAggregateBindData {
        op: DecimalAggregateOp::Avg,
        input_scale: 0,
        output_precision: 38,
        output_scale: 0,
        output_limit: 10_i128.pow(38),
        wide_sum: false,
    };
    let input = 99_999_999_999_999_999_999_999_999_999_999_999_999_i128;
    let mut state = initialized_average_state();
    update_average_state(&mut state, input);
    assert!(!state.wide);
    assert_eq!(state.narrow_value(), input);
    update_average_state(&mut state, input);
    assert!(state.wide);
    assert!(state.value() > i256::from(i128::MAX));

    let result = unsafe { finalize_single(&mut state, &data) }.unwrap();
    assert_eq!(unsafe { result.get_fixed::<i128>(0) }, input);
}
