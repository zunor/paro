// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;

use ethnum::i256;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{Vector, VectorType};

use crate::aggregate::{
    AggregateComparison, AggregateDirectUpdate, AggregateFunction, AggregateInputData,
    AggregateStateInput, DirectAggregateStateCursor, FunctionData,
};
use crate::decimal::{
    pow10_i128, read_decimal, rescale, rescale_checked, round_divide, to_i128, write_decimal,
};

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

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[repr(C)]
pub(in crate::aggregate) struct DecimalNarrowState {
    // Aggregate state buffers guarantee 8-byte alignment. Store i128 as words
    // instead of imposing its 16-byte alignment on the aggregate state ABI.
    value_words: [u64; 2],
    is_set: bool,
    overflowed: bool,
    i64_mode: bool,
}

impl DecimalNarrowState {
    fn value(&self) -> i128 {
        (((self.value_words[1] as u128) << 64) | self.value_words[0] as u128) as i128
    }

    fn set_value(&mut self, value: i128) {
        let value = value as u128;
        self.value_words = [value as u64, (value >> 64) as u64];
        self.i64_mode = i64::try_from(value as i128).is_ok();
    }

    fn set_i64(&mut self, value: i64) {
        self.value_words = [value as u64, if value < 0 { u64::MAX } else { 0 }];
        self.i64_mode = true;
    }

    fn add(&mut self, value: i128) {
        if let Ok(value) = i64::try_from(value) {
            self.add_i64(value);
            return;
        }
        match self.value().checked_add(value) {
            Some(value) => self.set_value(value),
            None => self.overflowed = true,
        }
        self.is_set = true;
    }

    pub(in crate::aggregate) fn add_i64(&mut self, value: i64) {
        if !self.is_set {
            self.set_i64(value);
            self.is_set = true;
            return;
        }
        if self.i64_mode {
            if let Some(sum) = (self.value_words[0] as i64).checked_add(value) {
                self.set_i64(sum);
                return;
            }
        }
        match self.value().checked_add(i128::from(value)) {
            Some(sum) => self.set_value(sum),
            None => self.overflowed = true,
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
struct DecimalSumState {
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

    fn add_state(&mut self, source: &Self) {
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
    i64_mode: bool,
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
        self.i64_mode = i64::try_from(value as i128).is_ok();
    }

    fn set_i64_value(&mut self, value: i64) {
        self.value_words[0] = value as u64;
        self.value_words[1] = if value < 0 { u64::MAX } else { 0 };
        self.wide = false;
        self.i64_mode = true;
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
        self.i64_mode = !self.wide && i64::try_from(low as i128).is_ok();
    }

    fn add_i64(&mut self, value: i64) -> bool {
        if !self.wide && self.i64_mode {
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
        if !self.wide && !source.wide && self.i64_mode && source.i64_mode {
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
    let mut function = AggregateFunction::new(
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
    )
    .with_bind_data(DecimalAggregateBindData {
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
    function = match op {
        DecimalAggregateOp::Sum if !wide_sum => {
            function.with_direct_update(AggregateDirectUpdate::DecimalSumI64)
        }
        DecimalAggregateOp::Avg => {
            function.with_direct_update(AggregateDirectUpdate::DecimalAverageI64)
        }
        _ => function,
    };
    if op == DecimalAggregateOp::Sum {
        function = function.with_state_filter(if wide_sum {
            filter_wide_sum_state
        } else {
            filter_narrow_sum_state
        });
    }
    Ok((function, arguments.to_vec()))
}

unsafe fn initialize_narrow(state: *mut u8) {
    let state = &mut *(state as *mut DecimalNarrowState);
    state.value_words = [0; 2];
    state.is_set = false;
    state.overflowed = false;
    state.i64_mode = true;
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
    state.i64_mode = true;
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
        if data.op == DecimalAggregateOp::First && state.is_set {
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
                if op == DecimalAggregateOp::First && state.is_set {
                    return;
                }
            }
        }
    }
}

fn update_narrow_state(state: &mut DecimalNarrowState, value: i128, op: DecimalAggregateOp) {
    match op {
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
                if source.overflowed {
                    target.overflowed = true;
                }
                if source.is_set {
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
        if !source.is_set {
            continue;
        }
        match data.op {
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
        if !state.is_set {
            result.set_null(row, true);
            continue;
        }
        if state.overflowed {
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
    debug_assert_eq!((data.op, data.wide_sum), (DecimalAggregateOp::Sum, false));
    filter_sum_values(data, comparison, constant, selection, count, |row| {
        let state = &*(states.state_ptr(row) as *const DecimalNarrowState);
        if !state.is_set {
            return Ok(None);
        }
        if state.overflowed {
            return Err(paro_error::out_of_range("Decimal SUM aggregate overflow"));
        }
        let value = rescale_checked(state.value(), data.input_scale, data.output_scale)
            .ok_or_else(|| paro_error::out_of_range("Decimal scale overflow"))?;
        check_output_precision(value, data)?;
        Ok(Some(value))
    })
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
    let constant = *constant;
    if selection.capacity() < count {
        return Err(paro_error::internal(format!(
            "aggregate state-filter selection too small: capacity={}, count={count}",
            selection.capacity()
        )));
    }
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
    let maximum = pow10_i128(input_precision)
        .and_then(|limit| limit.checked_sub(1))
        .ok_or_else(|| {
            paro_error::out_of_range(format!(
                "Decimal SUM input precision {input_precision} exceeds i128"
            ))
        })?;
    Ok(maximum.checked_mul(usize::MAX as i128).is_none())
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
mod tests {
    use super::*;
    use paro_common::allocator::{default_allocator, ArenaAllocator};
    use std::sync::Arc;

    fn initialized_narrow_state() -> DecimalNarrowState {
        DecimalNarrowState {
            value_words: [0; 2],
            is_set: false,
            overflowed: false,
            i64_mode: true,
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
            i64_mode: true,
        }
    }

    #[test]
    fn decimal_accumulators_promote_from_i64_without_losing_exactness() {
        let mut sum = initialized_narrow_state();
        sum.add_i64(i64::MAX);
        assert!(sum.i64_mode);
        sum.add_i64(1);
        assert!(!sum.i64_mode);
        assert_eq!(sum.value(), i128::from(i64::MAX) + 1);

        let mut average = initialized_average_state();
        assert!(average.add_i64(i64::MAX));
        assert!(average.i64_mode);
        assert!(average.add_i64(1));
        assert!(!average.i64_mode);
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
        assert!(program.try_add(0, sum.direct_update, sum_offset, Some(0)));
        assert!(program.try_add(1, average.direct_update, average_offset, Some(0),));
        assert!(program.try_add(
            2,
            Some(AggregateDirectUpdate::CountStar),
            count_offset,
            None,
        ));
        assert!(program.is_worthwhile());

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
        assert_eq!(
            std::mem::size_of::<DecimalSumState>(),
            std::mem::size_of::<DecimalNarrowState>()
        );
        assert!(
            std::mem::size_of::<DecimalNarrowState>() < std::mem::size_of::<DecimalAverageState>()
        );
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
            state.is_set = true;
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
}
