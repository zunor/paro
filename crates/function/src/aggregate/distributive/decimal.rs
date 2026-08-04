// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::aggregate::{AggregateFunction, AggregateInputData, FunctionData};

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
    input_precision: u8,
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
    // Aggregate state buffers guarantee 8-byte alignment. Store the i128 as
    // two machine-independent words instead of imposing i128's 16-byte
    // alignment on the aggregate state ABI.
    value_low: u64,
    value_high: i64,
    count: u64,
    is_set: bool,
    overflowed: bool,
}

impl DecimalAggregateState {
    fn value(&self) -> i128 {
        ((self.value_high as i128) << 64) | self.value_low as i128
    }

    fn set_value(&mut self, value: i128) {
        self.value_low = value as u64;
        self.value_high = (value >> 64) as i64;
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
        DecimalAggregateOp::Avg => LogicalType::Decimal {
            precision: 38,
            scale: (*scale).max(6),
        },
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
        input_precision: *precision,
        input_scale: *scale,
        output_precision,
        output_scale,
    });
    Ok((function, arguments.to_vec()))
}

unsafe fn initialize(state: *mut u8) {
    let state = &mut *(state as *mut DecimalAggregateState);
    state.set_value(0);
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
        update_state(state, decimal_at(inputs[0], row), bind_data(input_data));
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
        update_state(state, decimal_at(inputs[0], row), data);
        if data.op == DecimalAggregateOp::First && state.is_set {
            return;
        }
    }
}

fn update_state(state: &mut DecimalAggregateState, value: i128, data: &DecimalAggregateBindData) {
    match data.op {
        DecimalAggregateOp::Sum | DecimalAggregateOp::Avg => {
            if let Some(value) = state.value().checked_add(value) {
                state.set_value(value);
            } else {
                state.overflowed = true;
            }
            state.count = state.count.saturating_add(1);
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
                target.count = target.count.saturating_add(source.count);
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
) {
    let state_ptrs = states.flat_data::<*mut u8>();
    let data = bind_data(input_data);
    for row in 0..count {
        let state = &*(*state_ptrs.add(row) as *const DecimalAggregateState);
        if !state.is_set
            || state.overflowed
            || (data.op == DecimalAggregateOp::Avg && state.count == 0)
        {
            result.set_null(row, true);
            continue;
        }
        let value = if data.op == DecimalAggregateOp::Avg {
            let scaled = rescale(state.value(), data.input_scale, data.output_scale);
            round_divide(scaled, state.count as i128)
        } else {
            rescale(state.value(), data.input_scale, data.output_scale)
        };
        result.set_value(
            row,
            &Value::Decimal(value, data.output_precision, data.output_scale),
        );
    }
}

fn bind_data<'a>(input_data: &'a AggregateInputData<'_>) -> &'a DecimalAggregateBindData {
    input_data
        .bind_data
        .and_then(|data| data.as_any().downcast_ref::<DecimalAggregateBindData>())
        .expect("decimal aggregate bind data")
}

unsafe fn decimal_at(input: &Vector, row: usize) -> i128 {
    let LogicalType::Decimal { precision, .. } = input.logical_type() else {
        unreachable!("decimal aggregate input type")
    };
    if *precision <= 18 {
        input.get_fixed::<i64>(row) as i128
    } else {
        input.get_fixed::<i128>(row)
    }
}

fn rescale(value: i128, from_scale: u8, to_scale: u8) -> i128 {
    if to_scale >= from_scale {
        value.saturating_mul(pow10(to_scale - from_scale))
    } else {
        round_divide(value, pow10(from_scale - to_scale))
    }
}

fn pow10(scale: u8) -> i128 {
    (0..scale).fold(1_i128, |value, _| value.saturating_mul(10))
}

fn round_divide(value: i128, divisor: i128) -> i128 {
    let quotient = value / divisor;
    let remainder = value % divisor;
    let threshold = divisor.abs() / 2 + divisor.abs() % 2;
    if remainder == 0 || remainder.abs() < threshold {
        quotient
    } else if (value < 0) == (divisor < 0) {
        quotient + 1
    } else {
        quotient - 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_state_obeys_the_eight_byte_aggregate_alignment_contract() {
        assert_eq!(std::mem::align_of::<DecimalAggregateState>(), 8);
        let state_words = std::mem::size_of::<DecimalAggregateState>().div_ceil(8);
        let mut storage = vec![0_u64; state_words + 1];
        let base = storage.as_mut_ptr() as *mut u8;
        let offset = if (base as usize) % 16 == 0 { 8 } else { 0 };
        let state_ptr = unsafe { base.add(offset) };
        assert_eq!((state_ptr as usize) % 8, 0);
        assert_ne!((state_ptr as usize) % 16, 0);

        unsafe { initialize(state_ptr) };
        let state = unsafe { &mut *(state_ptr as *mut DecimalAggregateState) };
        let expected = -123_456_789_012_345_678_901_234_567_890_i128;
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
    }
}
