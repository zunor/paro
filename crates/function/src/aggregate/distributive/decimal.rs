// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;

use ethnum::i256;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector, VectorType};

use crate::aggregate::{
    AggregateAlgebra, AggregateComparison, AggregateDirectUpdate, AggregateFunction,
    AggregateInputData, AggregateStateInput, DecimalDirectUpdate, DirectAggregateStateCursor,
    FunctionData, PreparedDirectAggregateStatePredicate,
};
use crate::decimal::{
    pow10_i128, read_decimal, rescale, rescale_checked, round_divide, to_i128, write_decimal,
};
use crate::scalar::function_data_fingerprint;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum DecimalAggregateOp {
    Sum,
    Min,
    Max,
    Avg,
    First,
    Last,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DecimalAggregateBindData {
    op: DecimalAggregateOp,
    input_scale: u8,
    output_precision: u8,
    output_scale: u8,
    output_limit: i128,
    wide_sum: bool,
}

impl FunctionData for DecimalAggregateBindData {
    fn clone_box(&self) -> Box<dyn FunctionData> {
        Box::new(self.clone())
    }

    fn equals(&self, other: &dyn FunctionData) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }

    fn fingerprint(&self) -> u64 {
        function_data_fingerprint(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Largest DECIMAL input precision whose exact SUM over every addressable row
/// fits in the narrow i128 state. This threshold and the sentinel proof below
/// are one admission contract; changing either must preserve both assertions.
const NARROW_SUM_MAX_INPUT_PRECISION: u8 = 18;
const DECIMAL_18_MAX: i128 = 999_999_999_999_999_999;
const NARROW_SUM_MAX_ABS: i128 = DECIMAL_18_MAX * usize::MAX as i128;
const _: () = assert!(NARROW_SUM_MAX_ABS < i128::MAX);
const _: () = assert!(-NARROW_SUM_MAX_ABS > i128::MIN + 1);

#[repr(C)]
pub(in crate::aggregate) struct DecimalNarrowState {
    // Aggregate state buffers guarantee 8-byte alignment. Store i128 as words
    // instead of imposing its 16-byte alignment on the aggregate state ABI.
    value_words: [u64; 2],
}

impl DecimalNarrowState {
    // DECIMAL values are strictly bounded by 10^38, while these sentinels sit
    // at the bottom of the i128 domain. A narrow SUM is admitted only when the
    // maximum input magnitude times usize::MAX fits in i128, so neither a
    // valid input nor a mathematically reachable partial sum can alias them.
    // Encoding lifecycle in-band keeps the exact accumulator at 16 bytes.
    const UNSET: [u64; 2] = [0, 1_u64 << 63];
    const OVERFLOWED: [u64; 2] = [1, 1_u64 << 63];

    pub(in crate::aggregate) fn value(&self) -> i128 {
        (((self.value_words[1] as u128) << 64) | self.value_words[0] as u128) as i128
    }

    pub(in crate::aggregate) fn set_value(&mut self, value: i128) {
        let value = value as u128;
        self.value_words = [value as u64, (value >> 64) as u64];
        debug_assert_ne!(self.value_words, Self::UNSET);
        debug_assert_ne!(self.value_words, Self::OVERFLOWED);
    }

    pub(in crate::aggregate) fn reset(&mut self) {
        self.value_words = Self::UNSET;
    }

    fn set_i64(&mut self, value: i64) {
        self.value_words = [value as u64, if value < 0 { u64::MAX } else { 0 }];
        debug_assert_ne!(self.value_words, Self::UNSET);
        debug_assert_ne!(self.value_words, Self::OVERFLOWED);
    }

    pub(in crate::aggregate) fn is_set(&self) -> bool {
        self.value_words != Self::UNSET
    }

    pub(in crate::aggregate) fn overflowed(&self) -> bool {
        self.value_words == Self::OVERFLOWED
    }

    pub(in crate::aggregate) fn mark_overflowed(&mut self) {
        self.value_words = Self::OVERFLOWED;
    }

    #[inline]
    fn value_is_i64(&self) -> bool {
        self.value_words[1]
            == if (self.value_words[0] as i64) < 0 {
                u64::MAX
            } else {
                0
            }
    }

    pub(in crate::aggregate) fn add(&mut self, value: i128) {
        if self.overflowed() {
            return;
        }
        if !self.is_set() {
            self.set_value(value);
            return;
        }
        if let Ok(value) = i64::try_from(value) {
            self.add_i64(value);
            return;
        }
        match self.value().checked_add(value) {
            Some(value) => self.set_value(value),
            None => self.mark_overflowed(),
        }
    }

    pub(in crate::aggregate) fn add_i64(&mut self, value: i64) {
        if self.overflowed() {
            return;
        }
        if !self.is_set() {
            self.set_i64(value);
            return;
        }
        if self.value_is_i64() {
            if let Some(sum) = (self.value_words[0] as i64).checked_add(value) {
                self.set_i64(sum);
                return;
            }
        }
        match self.value().checked_add(i128::from(value)) {
            Some(sum) => self.set_value(sum),
            None => self.mark_overflowed(),
        }
    }

    pub(in crate::aggregate) fn add_direct_i128(&mut self, value: i128) {
        self.add(value);
    }
}

/// Exact fixed-width accumulator for DECIMAL SUM.
///
/// A valid DECIMAL(38) value has magnitude below 10^38, and one aggregate
/// cannot consume more than `usize::MAX` rows. Their product is strictly below
/// 2^191 on 64-bit platforms, so a signed 192-bit accumulator covers every
/// mathematically reachable SUM without making the state depend on input order.
/// Two otherwise unreachable values at the bottom of the signed domain encode
/// the empty and overflow states.
#[repr(C)]
pub(in crate::aggregate) struct DecimalSumState {
    value_words: [u64; 3],
}

impl DecimalSumState {
    const UNSET: [u64; 3] = [0, 0, 1_u64 << 63];
    const OVERFLOWED: [u64; 3] = [1, 0, 1_u64 << 63];

    fn is_set(&self) -> bool {
        self.value_words != Self::UNSET
    }

    fn overflowed(&self) -> bool {
        self.value_words == Self::OVERFLOWED
    }

    fn set_i128(&mut self, value: i128) {
        self.value_words = source_words(value);
    }

    fn add_i128(&mut self, value: i128) {
        // The common path stays in i128 and is as cheap as the narrow
        // accumulator. Only a genuine intermediate overflow enters the
        // three-word arithmetic below.
        if let Some(current) = self.try_i128() {
            if let Some(sum) = current.checked_add(value) {
                self.set_i128(sum);
                return;
            }
        }
        if !self.is_set() {
            self.set_i128(value);
            return;
        }
        if self.overflowed() {
            return;
        }
        self.add_words(source_words(value));
    }

    pub(in crate::aggregate) fn add_direct_i128(&mut self, value: i128) {
        self.add_i128(value);
    }

    pub(in crate::aggregate) fn add_direct_i256(&mut self, value: i256) {
        let (high, low) = value.into_words();
        let high = high as u128;
        let sign_extension = if (high as u64) >> 63 == 0 {
            0
        } else {
            u64::MAX
        };
        if (high >> 64) as u64 != sign_extension {
            self.value_words = Self::OVERFLOWED;
            return;
        }
        let low = low as u128;
        let words = [low as u64, (low >> 64) as u64, high as u64];
        if !self.is_set() {
            self.value_words = words;
        } else if !self.overflowed() {
            self.add_words(words);
        }
    }

    pub(in crate::aggregate) fn add_state(&mut self, source: &Self) {
        if self.overflowed() || source.overflowed() {
            self.value_words = Self::OVERFLOWED;
        } else if !source.is_set() {
            // Nothing to merge.
        } else if !self.is_set() {
            self.value_words = source.value_words;
        } else if let (Some(target), Some(source)) = (self.try_i128(), source.try_i128()) {
            if let Some(sum) = target.checked_add(source) {
                self.set_i128(sum);
            } else {
                self.add_words(source_words(source));
            }
        } else {
            self.add_words(source.value_words);
        }
    }

    fn try_i128(&self) -> Option<i128> {
        let value = ((self.value_words[1] as u128) << 64) | self.value_words[0] as u128;
        let expected_extension = if value >> 127 == 0 { 0 } else { u64::MAX };
        (self.value_words[2] == expected_extension).then_some(value as i128)
    }

    fn add_words(&mut self, rhs: [u64; 3]) {
        let lhs_negative = self.value_words[2] >> 63 != 0;
        let rhs_negative = rhs[2] >> 63 != 0;
        let mut result = [0_u64; 3];
        let mut carry = false;
        for word in 0..3 {
            let (partial, value_carry) = self.value_words[word].overflowing_add(rhs[word]);
            let (sum, carry_carry) = partial.overflowing_add(u64::from(carry));
            result[word] = sum;
            carry = value_carry || carry_carry;
        }
        let result_negative = result[2] >> 63 != 0;
        if lhs_negative == rhs_negative && result_negative != lhs_negative {
            self.value_words = Self::OVERFLOWED;
        } else {
            self.value_words = result;
        }
    }
}

#[inline]
fn source_words(value: i128) -> [u64; 3] {
    let value = value as u128;
    [
        value as u64,
        (value >> 64) as u64,
        if value >> 127 == 0 { 0 } else { u64::MAX },
    ]
}

#[repr(C)]
pub(in crate::aggregate) struct DecimalAverageState {
    // AVG can require a wider intermediate even when its final decimal fits.
    // Keep that exceptional cost out of SUM/MIN/MAX/FIRST/LAST state rows.
    value_words: [u64; 4],
    count: u64,
    is_set: bool,
    overflowed: bool,
    wide: bool,
}

impl DecimalAverageState {
    fn narrow_value(&self) -> i128 {
        let value = ((self.value_words[1] as u128) << 64) | self.value_words[0] as u128;
        value as i128
    }

    fn value(&self) -> i256 {
        let low = ((self.value_words[1] as u128) << 64) | self.value_words[0] as u128;
        if !self.wide {
            return i256::from(low as i128);
        }
        let high = ((self.value_words[3] as u128) << 64) | self.value_words[2] as u128;
        i256::from_words(high as i128, low as i128)
    }

    fn set_narrow_value(&mut self, value: i128) {
        let value = value as u128;
        self.value_words[0] = value as u64;
        self.value_words[1] = (value >> 64) as u64;
        self.wide = false;
    }

    fn set_i64_value(&mut self, value: i64) {
        self.value_words[0] = value as u64;
        self.value_words[1] = if value < 0 { u64::MAX } else { 0 };
        self.wide = false;
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
        self.wide = value < i256::from(i128::MIN) || value > i256::from(i128::MAX);
    }

    #[inline]
    fn value_is_i64(&self) -> bool {
        !self.wide
            && self.value_words[1]
                == if (self.value_words[0] as i64) < 0 {
                    u64::MAX
                } else {
                    0
                }
    }

    fn add_i64(&mut self, value: i64) -> bool {
        if self.value_is_i64() {
            if let Some(sum) = (self.value_words[0] as i64).checked_add(value) {
                self.set_i64_value(sum);
                return true;
            }
        }
        self.add_i128(i128::from(value))
    }

    pub(in crate::aggregate) fn update_direct_i64(&mut self, value: i64) {
        if !self.add_i64(value) {
            self.overflowed = true;
        }
        if let Some(count) = self.count.checked_add(1) {
            self.count = count;
        } else {
            self.overflowed = true;
        }
        self.is_set = true;
    }

    pub(in crate::aggregate) fn update_direct_i64_sum(&mut self, value: i64, count: u64) {
        if !self.add_i64(value) {
            self.overflowed = true;
        }
        if let Some(total) = self.count.checked_add(count) {
            self.count = total;
        } else {
            self.overflowed = true;
        }
        self.is_set = true;
    }

    pub(in crate::aggregate) fn update_direct_i128(&mut self, value: i128, count: u64) {
        if !self.add_i128(value) {
            self.overflowed = true;
        }
        if let Some(total) = self.count.checked_add(count) {
            self.count = total;
        } else {
            self.overflowed = true;
        }
        self.is_set = true;
    }

    pub(in crate::aggregate) fn update_direct_i256(&mut self, value: i256, count: u64) {
        let Some(total) = self.value().checked_add(value) else {
            self.overflowed = true;
            return;
        };
        self.set_value(total);
        if let Some(total) = self.count.checked_add(count) {
            self.count = total;
        } else {
            self.overflowed = true;
        }
        self.is_set = true;
    }

    fn add_i128(&mut self, value: i128) -> bool {
        if !self.wide {
            if let Some(value) = self.narrow_value().checked_add(value) {
                self.set_narrow_value(value);
                return true;
            }
        }
        let Some(value) = self.value().checked_add(i256::from(value)) else {
            return false;
        };
        self.set_value(value);
        true
    }

    fn add_state(&mut self, source: &Self) -> bool {
        if self.value_is_i64() && source.value_is_i64() {
            return self.add_i64(source.value_words[0] as i64);
        }
        if !self.wide && !source.wide {
            if let Some(value) = self.narrow_value().checked_add(source.narrow_value()) {
                self.set_narrow_value(value);
                return true;
            }
        }
        let Some(value) = self.value().checked_add(source.value()) else {
            return false;
        };
        self.set_value(value);
        true
    }
}

pub(crate) fn bind_sum(arguments: &[LogicalType]) -> Result<(AggregateFunction, Vec<LogicalType>)> {
    bind(arguments, DecimalAggregateOp::Sum, "sum")
}

pub(in crate::aggregate) fn prepare_direct_state_predicate(
    function: &AggregateFunction,
    comparison: AggregateComparison,
    constant: &paro_common::runtime_value::Value,
) -> Result<Option<PreparedDirectAggregateStatePredicate>> {
    if function.direct_update
        != Some(AggregateDirectUpdate::Decimal(
            DecimalDirectUpdate::NarrowSumI64,
        ))
    {
        return Ok(None);
    }
    let data = function
        .bind_data
        .as_deref()
        .and_then(|data| data.as_any().downcast_ref::<DecimalAggregateBindData>())
        .ok_or_else(|| paro_error::internal("decimal SUM lost its bind data"))?;
    validate_narrow_sum_bind_data(data)?;
    let constant = sum_filter_constant(data, constant)?;
    Ok(Some(
        PreparedDirectAggregateStatePredicate::decimal_narrow_sum(
            comparison,
            constant,
            data.output_limit,
            data.output_precision,
        ),
    ))
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
    let wide_sum = op == DecimalAggregateOp::Sum && decimal_sum_requires_wide_state(*precision)?;
    let function = AggregateFunction::new(
        name.to_string(),
        arguments.to_vec(),
        LogicalType::Decimal {
            precision: output_precision,
            scale: output_scale,
        },
        match op {
            DecimalAggregateOp::Sum if wide_sum => std::mem::size_of::<DecimalSumState>(),
            DecimalAggregateOp::Sum => std::mem::size_of::<DecimalNarrowState>(),
            DecimalAggregateOp::Avg => std::mem::size_of::<DecimalAverageState>(),
            _ => std::mem::size_of::<DecimalNarrowState>(),
        },
        match op {
            DecimalAggregateOp::Sum if wide_sum => initialize_sum,
            DecimalAggregateOp::Sum => initialize_narrow,
            DecimalAggregateOp::Avg => initialize_average,
            _ => initialize_narrow,
        },
        update,
        combine,
        finalize,
        Some(simple_update),
        None,
    );
    // SAFETY: every decimal aggregate state is an inline integer/word struct;
    // none owns external storage or installs a destructor.
    let function = unsafe { function.with_trivially_copyable_state() };
    let mut function = function.with_bind_data(DecimalAggregateBindData {
        op,
        input_scale: *scale,
        output_precision,
        output_scale,
        output_limit: pow10_i128(output_precision).ok_or_else(|| {
            paro_error::out_of_range(format!(
                "Decimal aggregate precision {output_precision} exceeds i128"
            ))
        })?,
        wide_sum,
    });
    function = match (op, *precision <= 18, wide_sum) {
        (DecimalAggregateOp::Sum, true, false) => function.with_direct_update(
            AggregateDirectUpdate::Decimal(DecimalDirectUpdate::NarrowSumI64),
        ),
        (DecimalAggregateOp::Sum, false, true) => function.with_direct_update(
            AggregateDirectUpdate::Decimal(DecimalDirectUpdate::WideSumI128),
        ),
        (DecimalAggregateOp::Avg, true, _) => function.with_direct_update(
            AggregateDirectUpdate::Decimal(DecimalDirectUpdate::AverageI64),
        ),
        (DecimalAggregateOp::Avg, false, _) => function.with_direct_update(
            AggregateDirectUpdate::Decimal(DecimalDirectUpdate::AverageI128),
        ),
        _ => function,
    };
    if op == DecimalAggregateOp::Sum {
        function = function
            .with_algebra(AggregateAlgebra::Sum)
            .with_state_filter(if wide_sum {
                filter_wide_sum_state
            } else {
                filter_narrow_sum_state
            });
    }
    Ok((function, arguments.to_vec()))
}

unsafe fn initialize_narrow(state: *mut u8) {
    let state = &mut *(state as *mut DecimalNarrowState);
    state.reset();
}

unsafe fn initialize_sum(state: *mut u8) {
    let state = &mut *(state as *mut DecimalSumState);
    state.value_words = DecimalSumState::UNSET;
}

unsafe fn initialize_average(state: *mut u8) {
    let state = &mut *(state as *mut DecimalAverageState);
    state.value_words = [0; 4];
    state.count = 0;
    state.is_set = false;
    state.overflowed = false;
    state.wide = false;
}

unsafe fn update(
    inputs: &[&Vector],
    input_data: &AggregateInputData,
    states: &AggregateStateInput,
    count: usize,
) {
    let data = bind_data(input_data);
    if let (Some(input), Some(states)) = (
        DirectDecimalInput::try_new(inputs[0]),
        states.direct_cursor(),
    ) {
        match input {
            DirectDecimalInput::I64(values) => {
                update_direct(DirectI64Input(values), data, states, count)
            }
            DirectDecimalInput::I128(values) => {
                update_direct(DirectI128Input(values), data, states, count)
            }
        }
        return;
    }
    if data.op == DecimalAggregateOp::Sum {
        if data.wide_sum {
            for row in 0..count {
                if !inputs[0].is_null(row) {
                    let state = &mut *(states.state_ptr(row) as *mut DecimalSumState);
                    state.add_i128(read_decimal(inputs[0], row).0);
                }
            }
        } else {
            for row in 0..count {
                if !inputs[0].is_null(row) {
                    let state = &mut *(states.state_ptr(row) as *mut DecimalNarrowState);
                    state.add(read_decimal(inputs[0], row).0);
                }
            }
        }
        return;
    }
    if data.op == DecimalAggregateOp::Avg {
        for row in 0..count {
            if inputs[0].is_null(row) {
                continue;
            }
            let state = &mut *(states.state_ptr(row) as *mut DecimalAverageState);
            update_average_state(state, read_decimal(inputs[0], row).0);
        }
        return;
    }
    for row in 0..count {
        if inputs[0].is_null(row) {
            continue;
        }
        let state = &mut *(states.state_ptr(row) as *mut DecimalNarrowState);
        update_narrow_state(state, read_decimal(inputs[0], row).0, data.op);
    }
}

unsafe fn simple_update(
    inputs: &[&Vector],
    input_data: &AggregateInputData,
    state: *mut u8,
    count: usize,
) {
    let data = bind_data(input_data);
    if let Some(input) = DirectDecimalInput::try_new(inputs[0]) {
        match input {
            DirectDecimalInput::I64(values) => {
                simple_update_direct(DirectI64Input(values), data, state, count)
            }
            DirectDecimalInput::I128(values) => {
                simple_update_direct(DirectI128Input(values), data, state, count)
            }
        }
        return;
    }
    if data.op == DecimalAggregateOp::Sum {
        if data.wide_sum {
            let state = &mut *(state as *mut DecimalSumState);
            for row in 0..count {
                if !inputs[0].is_null(row) {
                    state.add_i128(read_decimal(inputs[0], row).0);
                }
            }
        } else {
            let state = &mut *(state as *mut DecimalNarrowState);
            for row in 0..count {
                if !inputs[0].is_null(row) {
                    state.add(read_decimal(inputs[0], row).0);
                }
            }
        }
        return;
    }
    if data.op == DecimalAggregateOp::Avg {
        let state = &mut *(state as *mut DecimalAverageState);
        for row in 0..count {
            if !inputs[0].is_null(row) {
                update_average_state(state, read_decimal(inputs[0], row).0);
            }
        }
        return;
    }
    let state = &mut *(state as *mut DecimalNarrowState);
    for row in 0..count {
        if inputs[0].is_null(row) {
            continue;
        }
        update_narrow_state(state, read_decimal(inputs[0], row).0, data.op);
        if data.op == DecimalAggregateOp::First && state.is_set() {
            return;
        }
    }
}

#[derive(Clone, Copy)]
enum DirectDecimalInput {
    I64(*const i64),
    I128(*const i128),
}

impl DirectDecimalInput {
    fn try_new(vector: &Vector) -> Option<Self> {
        if vector.vector_type() != VectorType::Flat || !vector.validity().all_valid() {
            return None;
        }
        match vector.logical_type() {
            LogicalType::Decimal { precision, .. } if *precision <= 18 => {
                Some(Self::I64(unsafe { vector.flat_data::<i64>() }))
            }
            LogicalType::Decimal { .. } => Some(Self::I128(unsafe { vector.flat_data::<i128>() })),
            _ => None,
        }
    }
}

trait DirectDecimalAggregateInput: Copy {
    /// # Safety
    ///
    /// `row` must be within the source vector's logical cardinality.
    unsafe fn value(self, row: usize) -> i128;

    /// Add one input to a DECIMAL state using the narrowest exact physical
    /// operation available for this input representation.
    unsafe fn add_sum(self, state: &mut DecimalNarrowState, row: usize) {
        state.add(unsafe { self.value(row) });
    }

    unsafe fn add_average(self, state: &mut DecimalAverageState, row: usize) -> bool {
        state.add_i128(unsafe { self.value(row) })
    }
}

#[derive(Clone, Copy)]
struct DirectI64Input(*const i64);

impl DirectDecimalAggregateInput for DirectI64Input {
    #[inline(always)]
    unsafe fn value(self, row: usize) -> i128 {
        unsafe { *self.0.add(row) as i128 }
    }

    #[inline(always)]
    unsafe fn add_sum(self, state: &mut DecimalNarrowState, row: usize) {
        state.add_i64(unsafe { *self.0.add(row) });
    }

    #[inline(always)]
    unsafe fn add_average(self, state: &mut DecimalAverageState, row: usize) -> bool {
        state.add_i64(unsafe { *self.0.add(row) })
    }
}

#[derive(Clone, Copy)]
struct DirectI128Input(*const i128);

impl DirectDecimalAggregateInput for DirectI128Input {
    #[inline(always)]
    unsafe fn value(self, row: usize) -> i128 {
        unsafe { *self.0.add(row) }
    }
}

unsafe fn update_direct<I: DirectDecimalAggregateInput>(
    input: I,
    data: &DecimalAggregateBindData,
    states: DirectAggregateStateCursor,
    count: usize,
) {
    match data.op {
        DecimalAggregateOp::Sum if data.wide_sum => {
            for row in 0..count {
                let state = unsafe { &mut *(states.state_ptr(row) as *mut DecimalSumState) };
                state.add_i128(unsafe { input.value(row) });
            }
        }
        DecimalAggregateOp::Sum => {
            for row in 0..count {
                let state = unsafe { &mut *(states.state_ptr(row) as *mut DecimalNarrowState) };
                unsafe { input.add_sum(state, row) };
            }
        }
        DecimalAggregateOp::Avg => {
            for row in 0..count {
                let state = unsafe { &mut *(states.state_ptr(row) as *mut DecimalAverageState) };
                update_average_state_direct(state, input, row);
            }
        }
        op => {
            for row in 0..count {
                let state = unsafe { &mut *(states.state_ptr(row) as *mut DecimalNarrowState) };
                update_narrow_state(state, unsafe { input.value(row) }, op);
            }
        }
    }
}

unsafe fn simple_update_direct<I: DirectDecimalAggregateInput>(
    input: I,
    data: &DecimalAggregateBindData,
    state: *mut u8,
    count: usize,
) {
    match data.op {
        DecimalAggregateOp::Sum if data.wide_sum => {
            let state = unsafe { &mut *(state as *mut DecimalSumState) };
            for row in 0..count {
                state.add_i128(unsafe { input.value(row) });
            }
        }
        DecimalAggregateOp::Sum => {
            let state = unsafe { &mut *(state as *mut DecimalNarrowState) };
            for row in 0..count {
                unsafe { input.add_sum(state, row) };
            }
        }
        DecimalAggregateOp::Avg => {
            let state = unsafe { &mut *(state as *mut DecimalAverageState) };
            for row in 0..count {
                update_average_state_direct(state, input, row);
            }
        }
        op => {
            let state = unsafe { &mut *(state as *mut DecimalNarrowState) };
            for row in 0..count {
                update_narrow_state(state, unsafe { input.value(row) }, op);
                if op == DecimalAggregateOp::First && state.is_set() {
                    return;
                }
            }
        }
    }
}

fn update_narrow_state(state: &mut DecimalNarrowState, value: i128, op: DecimalAggregateOp) {
    match op {
        DecimalAggregateOp::Min => {
            if !state.is_set() || value < state.value() {
                state.set_value(value);
            }
        }
        DecimalAggregateOp::Max => {
            if !state.is_set() || value > state.value() {
                state.set_value(value);
            }
        }
        DecimalAggregateOp::First => {
            if !state.is_set() {
                state.set_value(value);
            }
        }
        DecimalAggregateOp::Last => {
            state.set_value(value);
        }
        DecimalAggregateOp::Sum | DecimalAggregateOp::Avg => {
            unreachable!("SUM and AVG use dedicated states")
        }
    }
}

fn update_average_state(state: &mut DecimalAverageState, value: i128) {
    if !state.add_i128(value) {
        state.overflowed = true;
    }
    if let Some(count) = state.count.checked_add(1) {
        state.count = count;
    } else {
        state.overflowed = true;
    }
    state.is_set = true;
}

#[inline(always)]
fn update_average_state_direct<I: DirectDecimalAggregateInput>(
    state: &mut DecimalAverageState,
    input: I,
    row: usize,
) {
    if !unsafe { input.add_average(state, row) } {
        state.overflowed = true;
    }
    if let Some(count) = state.count.checked_add(1) {
        state.count = count;
    } else {
        state.overflowed = true;
    }
    state.is_set = true;
}

unsafe fn combine(source: &Vector, target: &Vector, input_data: &AggregateInputData, count: usize) {
    let source_ptrs = source.flat_data::<*mut u8>();
    let target_ptrs = target.flat_data::<*mut u8>();
    let data = bind_data(input_data);
    if data.op == DecimalAggregateOp::Sum {
        if data.wide_sum {
            for row in 0..count {
                let source = &*(*source_ptrs.add(row) as *const DecimalSumState);
                let target = &mut *(*target_ptrs.add(row) as *mut DecimalSumState);
                target.add_state(source);
            }
        } else {
            for row in 0..count {
                let source = &*(*source_ptrs.add(row) as *const DecimalNarrowState);
                let target = &mut *(*target_ptrs.add(row) as *mut DecimalNarrowState);
                if source.overflowed() {
                    target.mark_overflowed();
                } else if source.is_set() {
                    target.add(source.value());
                }
            }
        }
        return;
    }
    if data.op == DecimalAggregateOp::Avg {
        for row in 0..count {
            let source = &*(*source_ptrs.add(row) as *const DecimalAverageState);
            let target = &mut *(*target_ptrs.add(row) as *mut DecimalAverageState);
            combine_average_state(source, target);
        }
        return;
    }
    for row in 0..count {
        let source = &*(*source_ptrs.add(row) as *const DecimalNarrowState);
        let target = &mut *(*target_ptrs.add(row) as *mut DecimalNarrowState);
        if !source.is_set() {
            continue;
        }
        match data.op {
            DecimalAggregateOp::Min => {
                if !target.is_set() || source.value() < target.value() {
                    target.set_value(source.value());
                }
            }
            DecimalAggregateOp::Max => {
                if !target.is_set() || source.value() > target.value() {
                    target.set_value(source.value());
                }
            }
            DecimalAggregateOp::First => {
                if !target.is_set() {
                    target.set_value(source.value());
                }
            }
            DecimalAggregateOp::Last => {
                target.set_value(source.value());
            }
            DecimalAggregateOp::Sum | DecimalAggregateOp::Avg => {
                unreachable!("SUM and AVG handled above")
            }
        }
    }
}

fn combine_average_state(source: &DecimalAverageState, target: &mut DecimalAverageState) {
    if source.overflowed {
        target.overflowed = true;
    }
    if !source.is_set {
        return;
    }
    if !target.add_state(source) {
        target.overflowed = true;
    }
    if let Some(count) = target.count.checked_add(source.count) {
        target.count = count;
    } else {
        target.overflowed = true;
    }
    target.is_set = true;
}

unsafe fn finalize(
    states: &Vector,
    input_data: &AggregateInputData,
    result: &mut Vector,
    count: usize,
) -> Result<()> {
    let state_ptrs = states.flat_data::<*mut u8>();
    let data = bind_data(input_data);
    if data.op == DecimalAggregateOp::Sum && data.wide_sum {
        return finalize_sum(state_ptrs, data, result, count);
    }
    if data.op == DecimalAggregateOp::Avg {
        return finalize_average(state_ptrs, data, result, count);
    }
    for row in 0..count {
        let state = &*(*state_ptrs.add(row) as *const DecimalNarrowState);
        if !state.is_set() {
            result.set_null(row, true);
            continue;
        }
        if state.overflowed() {
            return Err(paro_error::out_of_range(format!(
                "Decimal {} aggregate overflow",
                data.op.name()
            )));
        }
        let value = rescale_checked(state.value(), data.input_scale, data.output_scale)
            .ok_or_else(|| paro_error::out_of_range("Decimal scale overflow"))?;
        check_output_precision(value, data)?;
        write_decimal(result, row, value)?;
    }
    Ok(())
}

unsafe fn finalize_sum(
    state_ptrs: *const *mut u8,
    data: &DecimalAggregateBindData,
    result: &mut Vector,
    count: usize,
) -> Result<()> {
    for row in 0..count {
        let state = &*(*state_ptrs.add(row) as *const DecimalSumState);
        if !state.is_set() {
            result.set_null(row, true);
            continue;
        }
        let value = sum_output_value(state, data)?;
        write_decimal(result, row, value)?;
    }
    Ok(())
}

unsafe fn finalize_average(
    state_ptrs: *const *mut u8,
    data: &DecimalAggregateBindData,
    result: &mut Vector,
    count: usize,
) -> Result<()> {
    for row in 0..count {
        let state = &*(*state_ptrs.add(row) as *const DecimalAverageState);
        if !state.is_set || state.count == 0 {
            result.set_null(row, true);
            continue;
        }
        if state.overflowed {
            return Err(paro_error::out_of_range("Decimal AVG aggregate overflow"));
        }
        let scaled = rescale(state.value(), data.input_scale, data.output_scale)?;
        let value = to_i128(
            round_divide(scaled, i256::from(state.count))?,
            data.output_precision,
        )
        .map_err(|_| {
            paro_error::out_of_range(format!(
                "Decimal AVG result exceeds precision {}",
                data.output_precision
            ))
        })?;
        write_decimal(result, row, value)?;
    }
    Ok(())
}

unsafe fn filter_narrow_sum_state(
    states: &AggregateStateInput,
    input_data: &AggregateInputData,
    comparison: AggregateComparison,
    constant: &paro_common::runtime_value::Value,
    selection: &mut paro_common::vector::SelectionVector,
    count: usize,
) -> Result<usize> {
    let data = bind_data(input_data);
    validate_narrow_sum_bind_data(data)?;
    // A narrow SUM is admitted only after proving that every mathematically
    // reachable sum fits both i128 and DECIMAL(38). SUM also preserves the
    // input scale, so the bound constant and state can be compared directly.
    // Keep the wide path below for aggregates that require rescaling and an
    // output-precision check per group.
    let constant = sum_filter_constant(data, constant)?;
    validate_state_filter_selection(selection, count)?;

    macro_rules! filter_rows {
        ($state_ptr:expr, $matches:expr) => {{
            selection.set_len(count);
            let mut selected = 0usize;
            for row in 0..count {
                let state = unsafe { &*($state_ptr(row) as *const DecimalNarrowState) };
                if !state.is_set() {
                    continue;
                }
                if state.overflowed() {
                    return Err(paro_error::out_of_range("Decimal SUM aggregate overflow"));
                }
                let value = state.value();
                // The admission proof makes this branch false for all valid
                // executions. Retain the check so corrupted or manually
                // constructed states fail closed instead of crossing the
                // aggregate ABI with an out-of-domain result.
                check_output_precision(value, data)?;
                if $matches(value, constant) {
                    selection.try_set(selected, row)?;
                    selected += 1;
                }
            }
            selection.set_len(selected);
            Ok(selected)
        }};
    }

    macro_rules! dispatch_comparison {
        ($state_ptr:expr) => {{
            match comparison {
                AggregateComparison::Equal => filter_rows!($state_ptr, |a, b| a == b),
                AggregateComparison::NotEqual => filter_rows!($state_ptr, |a, b| a != b),
                AggregateComparison::LessThan => filter_rows!($state_ptr, |a, b| a < b),
                AggregateComparison::GreaterThan => filter_rows!($state_ptr, |a, b| a > b),
                AggregateComparison::LessThanOrEqual => filter_rows!($state_ptr, |a, b| a <= b),
                AggregateComparison::GreaterThanOrEqual => {
                    filter_rows!($state_ptr, |a, b| a >= b)
                }
            }
        }};
    }

    if let Some(cursor) = states.direct_cursor() {
        dispatch_comparison!(|row| cursor.state_ptr(row))
    } else {
        dispatch_comparison!(|row| states.state_ptr(row))
    }
}

unsafe fn filter_wide_sum_state(
    states: &AggregateStateInput,
    input_data: &AggregateInputData,
    comparison: AggregateComparison,
    constant: &paro_common::runtime_value::Value,
    selection: &mut paro_common::vector::SelectionVector,
    count: usize,
) -> Result<usize> {
    let data = bind_data(input_data);
    debug_assert_eq!((data.op, data.wide_sum), (DecimalAggregateOp::Sum, true));
    filter_sum_values(data, comparison, constant, selection, count, |row| {
        let state = &*(states.state_ptr(row) as *const DecimalSumState);
        if !state.is_set() {
            return Ok(None);
        }
        sum_output_value(state, data).map(Some)
    })
}

fn filter_sum_values(
    data: &DecimalAggregateBindData,
    comparison: AggregateComparison,
    constant: &paro_common::runtime_value::Value,
    selection: &mut paro_common::vector::SelectionVector,
    count: usize,
    mut value_at: impl FnMut(usize) -> Result<Option<i128>>,
) -> Result<usize> {
    let constant = sum_filter_constant(data, constant)?;
    validate_state_filter_selection(selection, count)?;
    selection.set_len(count);
    let mut selected = 0usize;
    for row in 0..count {
        let Some(value) = value_at(row)? else {
            continue;
        };
        let matches = match comparison {
            AggregateComparison::Equal => value == constant,
            AggregateComparison::NotEqual => value != constant,
            AggregateComparison::LessThan => value < constant,
            AggregateComparison::GreaterThan => value > constant,
            AggregateComparison::LessThanOrEqual => value <= constant,
            AggregateComparison::GreaterThanOrEqual => value >= constant,
        };
        if matches {
            selection.try_set(selected, row)?;
            selected += 1;
        }
    }
    selection.set_len(selected);
    Ok(selected)
}

fn sum_filter_constant(
    data: &DecimalAggregateBindData,
    constant: &paro_common::runtime_value::Value,
) -> Result<i128> {
    let paro_common::runtime_value::Value::Decimal(constant, constant_precision, constant_scale) =
        constant
    else {
        return Err(paro_error::internal(format!(
            "decimal aggregate state filter requires DECIMAL constant, got {constant:?}"
        )));
    };
    if (*constant_precision, *constant_scale) != (data.output_precision, data.output_scale) {
        return Err(paro_error::internal(format!(
            "decimal aggregate state-filter constant type mismatch: expected=DECIMAL({}, {}) actual=DECIMAL({constant_precision}, {constant_scale})",
            data.output_precision, data.output_scale
        )));
    }
    Ok(*constant)
}

fn validate_state_filter_selection(selection: &SelectionVector, count: usize) -> Result<()> {
    if selection.capacity() < count {
        return Err(paro_error::internal(format!(
            "aggregate state-filter selection too small: capacity={}, count={count}",
            selection.capacity()
        )));
    }
    Ok(())
}

#[inline]
fn sum_output_value(state: &DecimalSumState, data: &DecimalAggregateBindData) -> Result<i128> {
    if state.overflowed() {
        return Err(paro_error::out_of_range("Decimal SUM aggregate overflow"));
    }
    let value = state.try_i128().ok_or_else(|| {
        paro_error::out_of_range(format!(
            "Decimal SUM result exceeds precision {}",
            data.output_precision
        ))
    })?;
    let value = rescale_checked(value, data.input_scale, data.output_scale)
        .ok_or_else(|| paro_error::out_of_range("Decimal scale overflow"))?;
    check_output_precision(value, data)?;
    Ok(value)
}

fn decimal_sum_requires_wide_state(input_precision: u8) -> Result<bool> {
    pow10_i128(input_precision).ok_or_else(|| {
        paro_error::out_of_range(format!(
            "Decimal SUM input precision {input_precision} exceeds i128"
        ))
    })?;
    Ok(input_precision > NARROW_SUM_MAX_INPUT_PRECISION)
}

fn validate_narrow_sum_bind_data(data: &DecimalAggregateBindData) -> Result<()> {
    if (data.op, data.wide_sum) != (DecimalAggregateOp::Sum, false) {
        return Err(paro_error::internal(
            "narrow decimal SUM state filter was bound to an incompatible aggregate",
        ));
    }
    if data.input_scale != data.output_scale {
        return Err(paro_error::internal(format!(
            "narrow decimal SUM requires an unchanged scale: input={}, output={}",
            data.input_scale, data.output_scale
        )));
    }
    Ok(())
}

fn bind_data<'a>(input_data: &'a AggregateInputData<'_>) -> &'a DecimalAggregateBindData {
    input_data
        .bind_data
        .and_then(|data| data.as_any().downcast_ref::<DecimalAggregateBindData>())
        .expect("decimal aggregate bind data")
}

#[inline]
fn check_output_precision(value: i128, data: &DecimalAggregateBindData) -> Result<()> {
    if value.unsigned_abs() >= data.output_limit as u128 {
        return Err(paro_error::out_of_range(format!(
            "Decimal {} result exceeds precision {}",
            data.op.name(),
            data.output_precision
        )));
    }
    Ok(())
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
#[path = "decimal_tests.rs"]
mod tests;
