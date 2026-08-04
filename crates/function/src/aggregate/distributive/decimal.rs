// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;

use ethnum::i256;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::aggregate::{AggregateFunction, AggregateInputData, FunctionData};
use crate::decimal::{read_decimal, rescale, round_divide, to_i128, write_decimal};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecimalAggregateOp {
    Sum,
    Min,
    Max,
    Avg,
    First,
    Last,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecimalAggregateBindData {
    op: DecimalAggregateOp,
    input_scale: u8,
    output_precision: u8,
    output_scale: u8,
}

impl FunctionData for DecimalAggregateBindData {
    fn clone_box(&self) -> Box<dyn FunctionData> {
        Box::new(self.clone())
    }

    fn equals(&self, other: &dyn FunctionData) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[repr(C)]
struct DecimalAggregateState {
    // Aggregate state buffers guarantee 8-byte alignment. Store the i256
    // accumulator as four words instead of imposing i256's 16-byte alignment
    // on the aggregate state ABI.
    value_words: [u64; 4],
    count: u64,
    is_set: bool,
    overflowed: bool,
}

impl DecimalAggregateState {
    fn value(&self) -> i256 {
        let low = ((self.value_words[1] as u128) << 64) | self.value_words[0] as u128;
        let high = ((self.value_words[3] as u128) << 64) | self.value_words[2] as u128;
        i256::from_words(high as i128, low as i128)
    }

    fn set_value(&mut self, value: i256) {
        let (high, low) = value.into_words();
        let low = low as u128;
        let high = high as u128;
        self.value_words = [
            low as u64,
            (low >> 64) as u64,
            high as u64,
            (high >> 64) as u64,
        ];
    }
}

pub(crate) fn bind_sum(arguments: &[LogicalType]) -> Result<(AggregateFunction, Vec<LogicalType>)> {
    bind(arguments, DecimalAggregateOp::Sum, "sum")
}

pub(crate) fn bind_min(arguments: &[LogicalType]) -> Result<(AggregateFunction, Vec<LogicalType>)> {
    bind(arguments, DecimalAggregateOp::Min, "min")
}

pub(crate) fn bind_max(arguments: &[LogicalType]) -> Result<(AggregateFunction, Vec<LogicalType>)> {
    bind(arguments, DecimalAggregateOp::Max, "max")
}

pub(crate) fn bind_avg(arguments: &[LogicalType]) -> Result<(AggregateFunction, Vec<LogicalType>)> {
    bind(arguments, DecimalAggregateOp::Avg, "avg")
}

pub(crate) fn bind_first(
    arguments: &[LogicalType],
) -> Result<(AggregateFunction, Vec<LogicalType>)> {
    bind(arguments, DecimalAggregateOp::First, "first")
}

pub(crate) fn bind_last(
    arguments: &[LogicalType],
) -> Result<(AggregateFunction, Vec<LogicalType>)> {
    bind(arguments, DecimalAggregateOp::Last, "last")
}

fn bind(
    arguments: &[LogicalType],
    op: DecimalAggregateOp,
    name: &str,
) -> Result<(AggregateFunction, Vec<LogicalType>)> {
    let [LogicalType::Decimal { precision, scale }] = arguments else {
        return Err(paro_error::function_not_found(format!(
            "{name} with arguments {arguments:?}"
        )));
    };
    let return_type = match op {
        DecimalAggregateOp::Sum => LogicalType::Decimal {
            precision: 38,
            scale: *scale,
        },
        DecimalAggregateOp::Avg => {
            let integral_digits = precision - scale;
            let available_fractional_digits = 38 - integral_digits;
            LogicalType::Decimal {
                precision: 38,
                scale: (*scale).max(6).min(available_fractional_digits),
            }
        }
        DecimalAggregateOp::Min
        | DecimalAggregateOp::Max
        | DecimalAggregateOp::First
        | DecimalAggregateOp::Last => arguments[0].clone(),
    };
    let LogicalType::Decimal {
        precision: output_precision,
        scale: output_scale,
    } = return_type
    else {
        unreachable!()
    };
    let function = AggregateFunction::new(
        name.to_string(),
        arguments.to_vec(),
        LogicalType::Decimal {
            precision: output_precision,
            scale: output_scale,
        },
        std::mem::size_of::<DecimalAggregateState>(),
        initialize,
        update,
        combine,
        finalize,
        Some(simple_update),
        None,
    )
    .with_bind_data(DecimalAggregateBindData {
        op,
        input_scale: *scale,
        output_precision,
        output_scale,
    });
    Ok((function, arguments.to_vec()))
}

unsafe fn initialize(state: *mut u8) {
    let state = &mut *(state as *mut DecimalAggregateState);
    state.set_value(i256::ZERO);
    state.count = 0;
    state.is_set = false;
    state.overflowed = false;
}

unsafe fn update(
    inputs: &[&Vector],
    input_data: &AggregateInputData,
    states: &Vector,
    count: usize,
) {
    let state_ptrs = states.flat_data::<*mut u8>();
    for row in 0..count {
        if inputs[0].is_null(row) {
            continue;
        }
        let state = &mut *(*state_ptrs.add(row) as *mut DecimalAggregateState);
        update_state(state, read_decimal(inputs[0], row).0, bind_data(input_data));
    }
}

unsafe fn simple_update(
    inputs: &[&Vector],
    input_data: &AggregateInputData,
    state: *mut u8,
    count: usize,
) {
    let state = &mut *(state as *mut DecimalAggregateState);
    let data = bind_data(input_data);
    for row in 0..count {
        if inputs[0].is_null(row) {
            continue;
        }
        update_state(state, read_decimal(inputs[0], row).0, data);
        if data.op == DecimalAggregateOp::First && state.is_set {
            return;
        }
    }
}

fn update_state(state: &mut DecimalAggregateState, value: i128, data: &DecimalAggregateBindData) {
    let value = i256::from(value);
    match data.op {
        DecimalAggregateOp::Sum | DecimalAggregateOp::Avg => {
            if let Some(value) = state.value().checked_add(value) {
                state.set_value(value);
            } else {
                state.overflowed = true;
            }
            if let Some(count) = state.count.checked_add(1) {
                state.count = count;
            } else {
                state.overflowed = true;
            }
            state.is_set = true;
        }
        DecimalAggregateOp::Min => {
            if !state.is_set || value < state.value() {
                state.set_value(value);
                state.is_set = true;
            }
        }
        DecimalAggregateOp::Max => {
            if !state.is_set || value > state.value() {
                state.set_value(value);
                state.is_set = true;
            }
        }
        DecimalAggregateOp::First => {
            if !state.is_set {
                state.set_value(value);
                state.is_set = true;
            }
        }
        DecimalAggregateOp::Last => {
            state.set_value(value);
            state.is_set = true;
        }
    }
}

unsafe fn combine(source: &Vector, target: &Vector, input_data: &AggregateInputData, count: usize) {
    let source_ptrs = source.flat_data::<*mut u8>();
    let target_ptrs = target.flat_data::<*mut u8>();
    let data = bind_data(input_data);
    for row in 0..count {
        let source = &*(*source_ptrs.add(row) as *const DecimalAggregateState);
        let target = &mut *(*target_ptrs.add(row) as *mut DecimalAggregateState);
        if source.overflowed {
            target.overflowed = true;
        }
        if !source.is_set {
            continue;
        }
        match data.op {
            DecimalAggregateOp::Sum | DecimalAggregateOp::Avg => {
                if let Some(value) = target.value().checked_add(source.value()) {
                    target.set_value(value);
                } else {
                    target.overflowed = true;
                }
                if let Some(count) = target.count.checked_add(source.count) {
                    target.count = count;
                } else {
                    target.overflowed = true;
                }
                target.is_set = true;
            }
            DecimalAggregateOp::Min => {
                if !target.is_set || source.value() < target.value() {
                    target.set_value(source.value());
                    target.is_set = true;
                }
            }
            DecimalAggregateOp::Max => {
                if !target.is_set || source.value() > target.value() {
                    target.set_value(source.value());
                    target.is_set = true;
                }
            }
            DecimalAggregateOp::First => {
                if !target.is_set {
                    target.set_value(source.value());
                    target.is_set = true;
                }
            }
            DecimalAggregateOp::Last => {
                target.set_value(source.value());
                target.is_set = true;
            }
        }
    }
}

unsafe fn finalize(
    states: &Vector,
    input_data: &AggregateInputData,
    result: &mut Vector,
    count: usize,
) -> Result<()> {
    let state_ptrs = states.flat_data::<*mut u8>();
    let data = bind_data(input_data);
    for row in 0..count {
        let state = &*(*state_ptrs.add(row) as *const DecimalAggregateState);
        if !state.is_set || (data.op == DecimalAggregateOp::Avg && state.count == 0) {
            result.set_null(row, true);
            continue;
        }
        if state.overflowed {
            return Err(paro_error::out_of_range(format!(
                "Decimal {} aggregate overflow",
                data.op.name()
            )));
        }
        let value = if data.op == DecimalAggregateOp::Avg {
            let scaled = rescale(state.value(), data.input_scale, data.output_scale)?;
            round_divide(scaled, i256::from(state.count))?
        } else {
            rescale(state.value(), data.input_scale, data.output_scale)?
        };
        let value = to_i128(value, data.output_precision).map_err(|_| {
            paro_error::out_of_range(format!(
                "Decimal {} result exceeds precision {}",
                data.op.name(),
                data.output_precision
            ))
        })?;
        write_decimal(result, row, value)?;
    }
    Ok(())
}

fn bind_data<'a>(input_data: &'a AggregateInputData<'_>) -> &'a DecimalAggregateBindData {
    input_data
        .bind_data
        .and_then(|data| data.as_any().downcast_ref::<DecimalAggregateBindData>())
        .expect("decimal aggregate bind data")
}

impl DecimalAggregateOp {
    fn name(self) -> &'static str {
        match self {
            Self::Sum => "SUM",
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::Avg => "AVG",
            Self::First => "FIRST",
            Self::Last => "LAST",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::allocator::{default_allocator, ArenaAllocator};
    use std::sync::Arc;

    fn initialized_state() -> DecimalAggregateState {
        DecimalAggregateState {
            value_words: [0; 4],
            count: 0,
            is_set: false,
            overflowed: false,
        }
    }

    unsafe fn finalize_single(
        state: &mut DecimalAggregateState,
        data: &DecimalAggregateBindData,
    ) -> Result<Vector> {
        let mut states = paro_common::test_utils::test_vector(LogicalType::BigInt);
        states.set_count(1);
        *states.flat_data_mut::<*mut u8>() = state as *mut DecimalAggregateState as *mut u8;
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
        assert_eq!(std::mem::align_of::<DecimalAggregateState>(), 8);
        let state_words = std::mem::size_of::<DecimalAggregateState>().div_ceil(8);
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

        unsafe { initialize(state_ptr) };
        let state = unsafe { &mut *(state_ptr as *mut DecimalAggregateState) };
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
        };
        let mut state = initialized_state();
        state.set_value(crate::decimal::pow10(38).unwrap());
        state.count = 1;
        state.is_set = true;

        let error = unsafe { finalize_single(&mut state, &data) }.unwrap_err();
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
        };
        let input = 99_999_999_999_999_999_999_999_999_999_999_999_999_i128;
        let mut state = initialized_state();
        update_state(&mut state, input, &data);
        update_state(&mut state, input, &data);
        assert!(state.value() > i256::from(i128::MAX));

        let result = unsafe { finalize_single(&mut state, &data) }.unwrap();
        assert_eq!(unsafe { result.get_fixed::<i128>(0) }, input);
    }
}
