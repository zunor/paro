// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Direct vector kernels for all-valid DECIMAL arithmetic.

use super::*;

mod factor_chain;
#[cfg(test)]
mod factor_chain_tests;

pub use factor_chain::{is_decimal_factor_fusion, try_execute_decimal_factor_chain};

trait DirectDecimalReader: Copy {
    /// # Safety
    ///
    /// `row` must be within the input vector's logical cardinality.
    unsafe fn read(self, row: usize) -> i128;
}

#[derive(Clone, Copy)]
struct DirectI64Reader(*const i64);

impl DirectDecimalReader for DirectI64Reader {
    #[inline(always)]
    unsafe fn read(self, row: usize) -> i128 {
        unsafe { *self.0.add(row) as i128 }
    }
}

#[derive(Clone, Copy)]
struct DirectSelectedI64Reader<'a>(&'a DecodedVectorRef<'a>);

impl DirectDecimalReader for DirectSelectedI64Reader<'_> {
    #[inline(always)]
    unsafe fn read(self, row: usize) -> i128 {
        unsafe { self.0.get_value::<i64>(row) as i128 }
    }
}

#[derive(Clone, Copy)]
struct DirectSelectedI128Reader<'a>(&'a DecodedVectorRef<'a>);

impl DirectDecimalReader for DirectSelectedI128Reader<'_> {
    #[inline(always)]
    unsafe fn read(self, row: usize) -> i128 {
        unsafe { self.0.get_value::<i128>(row) }
    }
}

#[derive(Clone, Copy)]
struct DirectI128Reader(*const i128);

impl DirectDecimalReader for DirectI128Reader {
    #[inline(always)]
    unsafe fn read(self, row: usize) -> i128 {
        unsafe { *self.0.add(row) }
    }
}

#[derive(Clone, Copy)]
struct DirectConstantReader(i128);

impl DirectDecimalReader for DirectConstantReader {
    #[inline(always)]
    unsafe fn read(self, _row: usize) -> i128 {
        self.0
    }
}

trait DirectI64DecimalReader: Copy {
    /// # Safety
    ///
    /// `row` must be within the input vector's logical cardinality.
    unsafe fn read_i64(self, row: usize) -> i64;
}

trait DirectFusionDecimalReader: DirectDecimalReader {
    /// Return the low i64 word and whether narrowing changed the exact value.
    ///
    /// # Safety
    ///
    /// `row` must be within the prepared input vector.
    unsafe fn read_narrow(self, row: usize) -> (i64, bool);
}

impl DirectFusionDecimalReader for DirectI64Reader {
    #[inline(always)]
    unsafe fn read_narrow(self, row: usize) -> (i64, bool) {
        (unsafe { self.read_i64(row) }, false)
    }
}

impl DirectFusionDecimalReader for DirectMaterializedI64Reader {
    #[inline(always)]
    unsafe fn read_narrow(self, row: usize) -> (i64, bool) {
        (unsafe { self.read_i64(row) }, false)
    }
}

impl DirectFusionDecimalReader for DirectSelectedI64Reader<'_> {
    #[inline(always)]
    unsafe fn read_narrow(self, row: usize) -> (i64, bool) {
        (unsafe { self.read_i64(row) }, false)
    }
}

impl DirectFusionDecimalReader for DirectI128Reader {
    #[inline(always)]
    unsafe fn read_narrow(self, row: usize) -> (i64, bool) {
        let value = unsafe { self.read(row) };
        let narrow = value as i64;
        (narrow, i128::from(narrow) != value)
    }
}

impl DirectFusionDecimalReader for DirectSelectedI128Reader<'_> {
    #[inline(always)]
    unsafe fn read_narrow(self, row: usize) -> (i64, bool) {
        let value = unsafe { self.read(row) };
        let narrow = value as i64;
        (narrow, i128::from(narrow) != value)
    }
}

impl DirectI64DecimalReader for DirectI64Reader {
    #[inline(always)]
    unsafe fn read_i64(self, row: usize) -> i64 {
        unsafe { *self.0.add(row) }
    }
}

impl DirectI64DecimalReader for DirectSelectedI64Reader<'_> {
    #[inline(always)]
    unsafe fn read_i64(self, row: usize) -> i64 {
        unsafe { self.0.get_value::<i64>(row) }
    }
}

#[derive(Clone, Copy)]
struct DirectMaterializedI64Reader {
    data: *const i64,
    selection: *const u32,
}

impl DirectMaterializedI64Reader {
    fn try_new(decoded: &DecodedVectorRef<'_>) -> Option<Self> {
        Some(Self {
            data: decoded.get_data::<i64>(),
            selection: decoded.sel().materialized_indices()?.as_ptr(),
        })
    }
}

impl DirectI64DecimalReader for DirectMaterializedI64Reader {
    #[inline(always)]
    unsafe fn read_i64(self, row: usize) -> i64 {
        let physical = unsafe { *self.selection.add(row) as usize };
        unsafe { *self.data.add(physical) }
    }
}

impl DirectDecimalReader for DirectMaterializedI64Reader {
    #[inline(always)]
    unsafe fn read(self, row: usize) -> i128 {
        i128::from(unsafe { self.read_i64(row) })
    }
}

#[derive(Clone, Copy)]
struct DirectI64ConstantReader(i64);

impl DirectI64DecimalReader for DirectI64ConstantReader {
    #[inline(always)]
    unsafe fn read_i64(self, _row: usize) -> i64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum I64ScaleTransform {
    Identity,
    Multiply(i64),
    Divide(i64),
}

impl I64ScaleTransform {
    fn try_from_native(transform: DecimalScaleTransform) -> Option<Self> {
        match transform {
            DecimalScaleTransform::Identity => Some(Self::Identity),
            DecimalScaleTransform::Multiply(factor) => {
                i64::try_from(factor).ok().map(Self::Multiply)
            }
            DecimalScaleTransform::Divide(divisor) => i64::try_from(divisor).ok().map(Self::Divide),
            DecimalScaleTransform::Overflow => None,
        }
    }

    #[inline(always)]
    fn apply(self, value: i64) -> Option<i64> {
        match self {
            Self::Identity => Some(value),
            Self::Multiply(factor) => value.checked_mul(factor),
            Self::Divide(divisor) => round_divide_i64(value, divisor),
        }
    }
}

#[inline]
fn round_divide_i64(value: i64, divisor: i64) -> Option<i64> {
    let quotient = value.checked_div(divisor)?;
    let remainder = value.checked_rem(divisor)?;
    if remainder == 0 {
        return Some(quotient);
    }
    let divisor_abs = divisor.checked_abs()?;
    let remainder_abs = remainder.checked_abs()?;
    let threshold = divisor_abs / 2 + divisor_abs % 2;
    if remainder_abs < threshold {
        return Some(quotient);
    }
    quotient.checked_add(if (value < 0) == (divisor < 0) { 1 } else { -1 })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum I64NativeDecimalOperand {
    Input(I64ScaleTransform),
    Constant(i64),
}

impl I64NativeDecimalOperand {
    fn try_from_native(operand: NativeDecimalOperand) -> Option<Self> {
        match operand {
            NativeDecimalOperand::Input(transform) => {
                Some(Self::Input(I64ScaleTransform::try_from_native(transform)?))
            }
            NativeDecimalOperand::Constant(value) => {
                Some(Self::Constant(i64::try_from(value?).ok()?))
            }
        }
    }

    #[inline(always)]
    fn apply(self, value: i64) -> Option<i64> {
        match self {
            Self::Input(transform) => transform.apply(value),
            Self::Constant(value) => Some(value),
        }
    }

    fn bind_constant(self, value: i64) -> Option<Self> {
        self.apply(value).map(Self::Constant)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum I64NativeDecimalPlan {
    Add {
        left: I64NativeDecimalOperand,
        right: I64NativeDecimalOperand,
    },
    Sub {
        left: I64NativeDecimalOperand,
        right: I64NativeDecimalOperand,
    },
    Mul(I64ScaleTransform),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SimpleI64Operand {
    Input,
    Constant(i64),
}

impl SimpleI64Operand {
    fn try_from_native(operand: I64NativeDecimalOperand) -> Option<Self> {
        match operand {
            I64NativeDecimalOperand::Input(I64ScaleTransform::Identity) => Some(Self::Input),
            I64NativeDecimalOperand::Constant(value) => Some(Self::Constant(value)),
            I64NativeDecimalOperand::Input(
                I64ScaleTransform::Multiply(_) | I64ScaleTransform::Divide(_),
            ) => None,
        }
    }

    #[inline(always)]
    fn value(self, input: i64) -> i64 {
        match self {
            Self::Input => input,
            Self::Constant(value) => value,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum PreparedFactorInner {
    Add {
        left: SimpleI64Operand,
        right: SimpleI64Operand,
    },
    Sub {
        left: SimpleI64Operand,
        right: SimpleI64Operand,
    },
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PreparedCommonFactor {
    inner: PreparedFactorInner,
    outer: I64ScaleTransform,
    inner_limit: Option<u64>,
    outer_limit: Option<u64>,
}

impl PreparedCommonFactor {
    pub(super) fn try_new(plan: DecimalFactorFusionBindData) -> Option<Self> {
        let inner = match plan.inner.narrow.0 {
            I64NativeDecimalPlan::Add { left, right } => PreparedFactorInner::Add {
                left: SimpleI64Operand::try_from_native(left)?,
                right: SimpleI64Operand::try_from_native(right)?,
            },
            I64NativeDecimalPlan::Sub { left, right } => PreparedFactorInner::Sub {
                left: SimpleI64Operand::try_from_native(left)?,
                right: SimpleI64Operand::try_from_native(right)?,
            },
            I64NativeDecimalPlan::Mul(_) => return None,
        };
        let I64NativeDecimalPlan::Mul(outer) = plan.outer.narrow.0 else {
            return None;
        };
        Some(Self {
            inner,
            outer,
            inner_limit: decimal_i64_limit(plan.inner.precision),
            outer_limit: decimal_i64_limit(plan.outer.precision),
        })
    }

    #[inline(always)]
    pub(super) fn evaluate(self, outer: i64, inner: i64) -> (i64, bool) {
        let (inner, inner_overflowed) = match self.inner {
            PreparedFactorInner::Add { left, right } => {
                left.value(inner).overflowing_add(right.value(inner))
            }
            PreparedFactorInner::Sub { left, right } => {
                left.value(inner).overflowing_sub(right.value(inner))
            }
        };
        let (multiplied, multiply_overflowed) = outer.overflowing_mul(inner);
        let (outer, transform_overflowed) = match self.outer {
            I64ScaleTransform::Identity => (multiplied, false),
            I64ScaleTransform::Multiply(factor) => multiplied.overflowing_mul(factor),
            I64ScaleTransform::Divide(divisor) if divisor > 0 => {
                (round_divide_positive_i64(multiplied, divisor), false)
            }
            I64ScaleTransform::Divide(_) => (0, true),
        };
        let invalid = inner_overflowed
            | multiply_overflowed
            | transform_overflowed
            | self
                .inner_limit
                .is_some_and(|limit| inner.unsigned_abs() >= limit)
            | self
                .outer_limit
                .is_some_and(|limit| outer.unsigned_abs() >= limit);
        (outer, invalid)
    }
}

/// Round an i64 quotient away from zero at a half boundary.
///
/// `divisor` is a positive decimal scale factor. Therefore division cannot hit
/// `i64::MIN / -1`; when rounding is needed the quotient magnitude is strictly
/// smaller than the dividend, so adding one unit cannot overflow either.
#[inline(always)]
fn round_divide_positive_i64(value: i64, divisor: i64) -> i64 {
    debug_assert!(divisor > 0);
    let quotient = value / divisor;
    let remainder = value % divisor;
    let threshold = (divisor as u64) / 2 + (divisor as u64) % 2;
    if remainder.unsigned_abs() < threshold {
        quotient
    } else if value < 0 {
        quotient - 1
    } else {
        quotient + 1
    }
}

fn decimal_i64_limit(precision: u8) -> Option<u64> {
    (precision <= 18).then(|| 10_u64.pow(u32::from(precision)))
}

impl I64NativeDecimalPlan {
    fn try_from_native(plan: NativeDecimalPlan) -> Option<Self> {
        match plan {
            NativeDecimalPlan::Add { left, right } => Some(Self::Add {
                left: I64NativeDecimalOperand::try_from_native(left)?,
                right: I64NativeDecimalOperand::try_from_native(right)?,
            }),
            NativeDecimalPlan::Sub { left, right } => Some(Self::Sub {
                left: I64NativeDecimalOperand::try_from_native(left)?,
                right: I64NativeDecimalOperand::try_from_native(right)?,
            }),
            NativeDecimalPlan::Mul(transform) => {
                Some(Self::Mul(I64ScaleTransform::try_from_native(transform)?))
            }
            NativeDecimalPlan::Div { .. } | NativeDecimalPlan::Mod { .. } => None,
        }
    }

    #[inline(always)]
    fn evaluate(self, left_value: i64, right_value: i64) -> (i64, bool) {
        let value = match self {
            Self::Add { left, right } => left
                .apply(left_value)
                .zip(right.apply(right_value))
                .and_then(|(left, right)| left.checked_add(right)),
            Self::Sub { left, right } => left
                .apply(left_value)
                .zip(right.apply(right_value))
                .and_then(|(left, right)| left.checked_sub(right)),
            Self::Mul(transform) => left_value
                .checked_mul(right_value)
                .and_then(|value| transform.apply(value)),
        };
        value.map_or((0, true), |value| (value, false))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct I64FusionPlan(I64NativeDecimalPlan);

impl I64FusionPlan {
    pub(super) fn try_from_native(plan: NativeDecimalPlan) -> Option<Self> {
        I64NativeDecimalPlan::try_from_native(plan).map(Self)
    }

    #[inline(always)]
    pub(super) fn evaluate(self, left: i64, right: i64) -> (i64, bool) {
        self.0.evaluate(left, right)
    }

    pub(super) fn bind_constant(self, side: DecimalOperandSide, value: i64) -> Option<Self> {
        let plan = match self.0 {
            I64NativeDecimalPlan::Add { left, right } => I64NativeDecimalPlan::Add {
                left: if side == DecimalOperandSide::Left {
                    left.bind_constant(value)?
                } else {
                    left
                },
                right: if side == DecimalOperandSide::Right {
                    right.bind_constant(value)?
                } else {
                    right
                },
            },
            I64NativeDecimalPlan::Sub { left, right } => I64NativeDecimalPlan::Sub {
                left: if side == DecimalOperandSide::Left {
                    left.bind_constant(value)?
                } else {
                    left
                },
                right: if side == DecimalOperandSide::Right {
                    right.bind_constant(value)?
                } else {
                    right
                },
            },
            I64NativeDecimalPlan::Mul(transform) => I64NativeDecimalPlan::Mul(transform),
        };
        Some(Self(plan))
    }
}

trait DirectDecimalWriter: Copy {
    /// # Safety
    ///
    /// `row` must be within the result allocation and `value` must satisfy the
    /// result DECIMAL's declared physical width.
    unsafe fn write(self, row: usize, value: i128);
}

#[derive(Clone, Copy)]
struct DirectI64Writer(*mut i64);

impl DirectDecimalWriter for DirectI64Writer {
    #[inline(always)]
    unsafe fn write(self, row: usize, value: i128) {
        unsafe { *self.0.add(row) = value as i64 };
    }
}

#[derive(Clone, Copy)]
struct DirectI128Writer(*mut i128);

impl DirectDecimalWriter for DirectI128Writer {
    #[inline(always)]
    unsafe fn write(self, row: usize, value: i128) {
        unsafe { *self.0.add(row) = value };
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn execute_direct_decimal_rows(
    left: &DecimalInputView<'_>,
    right: &DecimalInputView<'_>,
    output: DecimalOutput,
    precision: u8,
    precision_guard: I128DecimalPrecision,
    result_scale: u8,
    op: DecimalArithmeticOp,
    native_plan: NativeDecimalPlan,
    count: usize,
) -> Result<bool> {
    if matches!(op, DecimalArithmeticOp::Div | DecimalArithmeticOp::Mod) {
        return Ok(false);
    }
    if let DecimalOutput::I128(output) = output {
        if try_execute_speculative_narrow_i128_rows(left, right, output, native_plan, count) {
            return Ok(true);
        }
    }
    if let DecimalOutput::I64(output) = output {
        if try_execute_direct_i64_rows(
            left,
            right,
            output,
            precision,
            result_scale,
            op,
            native_plan,
            count,
        )? {
            return Ok(true);
        }
    }
    macro_rules! execute_pair {
        ($left:expr, $right:expr) => {{
            match output {
                DecimalOutput::I64(output) => execute_direct_decimal_pair(
                    $left,
                    $right,
                    DirectI64Writer(output),
                    left.scale,
                    right.scale,
                    result_scale,
                    precision,
                    precision_guard,
                    op,
                    native_plan,
                    count,
                )?,
                DecimalOutput::I128(output) => execute_direct_decimal_pair(
                    $left,
                    $right,
                    DirectI128Writer(output),
                    left.scale,
                    right.scale,
                    result_scale,
                    precision,
                    precision_guard,
                    op,
                    native_plan,
                    count,
                )?,
            }
            return Ok(true);
        }};
    }

    match (&left.access, &right.access) {
        (DecimalInputAccess::DirectI64(left), DecimalInputAccess::DirectI64(right)) => {
            execute_pair!(DirectI64Reader(*left), DirectI64Reader(*right));
        }
        (DecimalInputAccess::DirectI64(left), DecimalInputAccess::DirectI128(right)) => {
            execute_pair!(DirectI64Reader(*left), DirectI128Reader(*right));
        }
        (DecimalInputAccess::DirectI128(left), DecimalInputAccess::DirectI64(right)) => {
            execute_pair!(DirectI128Reader(*left), DirectI64Reader(*right));
        }
        (DecimalInputAccess::DirectI128(left), DecimalInputAccess::DirectI128(right)) => {
            execute_pair!(DirectI128Reader(*left), DirectI128Reader(*right));
        }
        (DecimalInputAccess::SelectedI64(left), DecimalInputAccess::DirectI64(right)) => {
            if let Some(left) = DirectMaterializedI64Reader::try_new(left) {
                execute_pair!(left, DirectI64Reader(*right));
            }
            execute_pair!(DirectSelectedI64Reader(left), DirectI64Reader(*right));
        }
        (DecimalInputAccess::DirectI64(left), DecimalInputAccess::SelectedI64(right)) => {
            if let Some(right) = DirectMaterializedI64Reader::try_new(right) {
                execute_pair!(DirectI64Reader(*left), right);
            }
            execute_pair!(DirectI64Reader(*left), DirectSelectedI64Reader(right));
        }
        (DecimalInputAccess::SelectedI64(left), DecimalInputAccess::DirectI128(right)) => {
            execute_pair!(DirectSelectedI64Reader(left), DirectI128Reader(*right));
        }
        (DecimalInputAccess::DirectI128(left), DecimalInputAccess::SelectedI64(right)) => {
            execute_pair!(DirectI128Reader(*left), DirectSelectedI64Reader(right));
        }
        (DecimalInputAccess::SelectedI64(left), DecimalInputAccess::SelectedI64(right)) => {
            execute_pair!(
                DirectSelectedI64Reader(left),
                DirectSelectedI64Reader(right)
            );
        }
        (DecimalInputAccess::SelectedI128(left), DecimalInputAccess::DirectI64(right)) => {
            execute_pair!(DirectSelectedI128Reader(left), DirectI64Reader(*right));
        }
        (DecimalInputAccess::DirectI64(left), DecimalInputAccess::SelectedI128(right)) => {
            execute_pair!(DirectI64Reader(*left), DirectSelectedI128Reader(right));
        }
        (DecimalInputAccess::SelectedI128(left), DecimalInputAccess::DirectI128(right)) => {
            execute_pair!(DirectSelectedI128Reader(left), DirectI128Reader(*right));
        }
        (DecimalInputAccess::DirectI128(left), DecimalInputAccess::SelectedI128(right)) => {
            execute_pair!(DirectI128Reader(*left), DirectSelectedI128Reader(right));
        }
        (DecimalInputAccess::SelectedI128(left), DecimalInputAccess::SelectedI64(right)) => {
            execute_pair!(
                DirectSelectedI128Reader(left),
                DirectSelectedI64Reader(right)
            );
        }
        (DecimalInputAccess::SelectedI64(left), DecimalInputAccess::SelectedI128(right)) => {
            execute_pair!(
                DirectSelectedI64Reader(left),
                DirectSelectedI128Reader(right)
            );
        }
        (DecimalInputAccess::SelectedI128(left), DecimalInputAccess::SelectedI128(right)) => {
            execute_pair!(
                DirectSelectedI128Reader(left),
                DirectSelectedI128Reader(right)
            );
        }
        (DecimalInputAccess::Constant(left), DecimalInputAccess::DirectI64(right)) => {
            execute_pair!(DirectConstantReader(*left), DirectI64Reader(*right));
        }
        (DecimalInputAccess::Constant(left), DecimalInputAccess::DirectI128(right)) => {
            execute_pair!(DirectConstantReader(*left), DirectI128Reader(*right));
        }
        (DecimalInputAccess::DirectI64(left), DecimalInputAccess::Constant(right)) => {
            execute_pair!(DirectI64Reader(*left), DirectConstantReader(*right));
        }
        (DecimalInputAccess::DirectI128(left), DecimalInputAccess::Constant(right)) => {
            execute_pair!(DirectI128Reader(*left), DirectConstantReader(*right));
        }
        (DecimalInputAccess::SelectedI64(left), DecimalInputAccess::Constant(right)) => {
            execute_pair!(DirectSelectedI64Reader(left), DirectConstantReader(*right));
        }
        (DecimalInputAccess::Constant(left), DecimalInputAccess::SelectedI64(right)) => {
            execute_pair!(DirectConstantReader(*left), DirectSelectedI64Reader(right));
        }
        (DecimalInputAccess::SelectedI128(left), DecimalInputAccess::Constant(right)) => {
            execute_pair!(DirectSelectedI128Reader(left), DirectConstantReader(*right));
        }
        (DecimalInputAccess::Constant(left), DecimalInputAccess::SelectedI128(right)) => {
            execute_pair!(DirectConstantReader(*left), DirectSelectedI128Reader(right));
        }
        (DecimalInputAccess::Constant(left), DecimalInputAccess::Constant(right)) => {
            execute_pair!(DirectConstantReader(*left), DirectConstantReader(*right));
        }
        _ => Ok(false),
    }
}

pub(super) fn execute_direct_decimal_factor_rows(
    outer: &DecimalInputView<'_>,
    inner: &DecimalInputView<'_>,
    output: DecimalOutput,
    plan: DecimalFactorFusionBindData,
    count: usize,
) -> Result<bool> {
    macro_rules! execute_pair {
        ($outer:expr, $inner:expr) => {{
            match output {
                DecimalOutput::I64(output) => execute_decimal_factor_pair(
                    $outer,
                    $inner,
                    DirectI64Writer(output),
                    plan,
                    count,
                )?,
                DecimalOutput::I128(output) => execute_decimal_factor_pair(
                    $outer,
                    $inner,
                    DirectI128Writer(output),
                    plan,
                    count,
                )?,
            }
            return Ok(true);
        }};
    }

    match (&outer.access, &inner.access) {
        (DecimalInputAccess::DirectI64(outer), DecimalInputAccess::DirectI64(inner)) => {
            execute_pair!(DirectI64Reader(*outer), DirectI64Reader(*inner));
        }
        (DecimalInputAccess::DirectI64(outer), DecimalInputAccess::SelectedI64(inner)) => {
            if let Some(inner) = DirectMaterializedI64Reader::try_new(inner) {
                execute_pair!(DirectI64Reader(*outer), inner);
            }
            execute_pair!(DirectI64Reader(*outer), DirectSelectedI64Reader(inner));
        }
        (DecimalInputAccess::SelectedI64(outer), DecimalInputAccess::DirectI64(inner)) => {
            if let Some(outer) = DirectMaterializedI64Reader::try_new(outer) {
                execute_pair!(outer, DirectI64Reader(*inner));
            }
            execute_pair!(DirectSelectedI64Reader(outer), DirectI64Reader(*inner));
        }
        (DecimalInputAccess::SelectedI64(outer), DecimalInputAccess::SelectedI64(inner)) => {
            if let (Some(outer), Some(inner)) = (
                DirectMaterializedI64Reader::try_new(outer),
                DirectMaterializedI64Reader::try_new(inner),
            ) {
                execute_pair!(outer, inner);
            }
            execute_pair!(
                DirectSelectedI64Reader(outer),
                DirectSelectedI64Reader(inner)
            );
        }
        (DecimalInputAccess::DirectI128(outer), DecimalInputAccess::DirectI64(inner)) => {
            execute_pair!(DirectI128Reader(*outer), DirectI64Reader(*inner));
        }
        (DecimalInputAccess::DirectI128(outer), DecimalInputAccess::SelectedI64(inner)) => {
            if let Some(inner) = DirectMaterializedI64Reader::try_new(inner) {
                execute_pair!(DirectI128Reader(*outer), inner);
            }
            execute_pair!(DirectI128Reader(*outer), DirectSelectedI64Reader(inner));
        }
        (DecimalInputAccess::SelectedI128(outer), DecimalInputAccess::DirectI64(inner)) => {
            execute_pair!(DirectSelectedI128Reader(outer), DirectI64Reader(*inner));
        }
        (DecimalInputAccess::SelectedI128(outer), DecimalInputAccess::SelectedI64(inner)) => {
            execute_pair!(
                DirectSelectedI128Reader(outer),
                DirectSelectedI64Reader(inner)
            );
        }
        (DecimalInputAccess::DirectI64(outer), DecimalInputAccess::DirectI128(inner)) => {
            execute_pair!(DirectI64Reader(*outer), DirectI128Reader(*inner));
        }
        (DecimalInputAccess::SelectedI64(outer), DecimalInputAccess::DirectI128(inner)) => {
            execute_pair!(DirectSelectedI64Reader(outer), DirectI128Reader(*inner));
        }
        (DecimalInputAccess::DirectI128(outer), DecimalInputAccess::DirectI128(inner)) => {
            execute_pair!(DirectI128Reader(*outer), DirectI128Reader(*inner));
        }
        (DecimalInputAccess::SelectedI128(outer), DecimalInputAccess::DirectI128(inner)) => {
            execute_pair!(DirectSelectedI128Reader(outer), DirectI128Reader(*inner));
        }
        (DecimalInputAccess::DirectI64(outer), DecimalInputAccess::SelectedI128(inner)) => {
            execute_pair!(DirectI64Reader(*outer), DirectSelectedI128Reader(inner));
        }
        (DecimalInputAccess::SelectedI64(outer), DecimalInputAccess::SelectedI128(inner)) => {
            execute_pair!(
                DirectSelectedI64Reader(outer),
                DirectSelectedI128Reader(inner)
            );
        }
        (DecimalInputAccess::DirectI128(outer), DecimalInputAccess::SelectedI128(inner)) => {
            execute_pair!(DirectI128Reader(*outer), DirectSelectedI128Reader(inner));
        }
        (DecimalInputAccess::SelectedI128(outer), DecimalInputAccess::SelectedI128(inner)) => {
            execute_pair!(
                DirectSelectedI128Reader(outer),
                DirectSelectedI128Reader(inner)
            );
        }
        _ => Ok(false),
    }
}

fn execute_decimal_factor_pair<O, I, W>(
    outer: O,
    inner: I,
    output: W,
    plan: DecimalFactorFusionBindData,
    count: usize,
) -> Result<()>
where
    O: DirectFusionDecimalReader,
    I: DirectFusionDecimalReader,
    W: DirectDecimalWriter,
{
    let specialized = try_execute_common_factor_pair(outer, inner, output, plan, count);
    if specialized == Some(true) {
        return Ok(());
    }
    if specialized.is_none() {
        let mut invalid = false;
        for row in 0..count {
            let (outer_value, outer_wide) = unsafe { outer.read_narrow(row) };
            let (inner_value, inner_wide) = unsafe { inner.read_narrow(row) };
            let (value, arithmetic_failed) = plan.evaluate_i64(outer_value, inner_value);
            invalid |= outer_wide | inner_wide | arithmetic_failed;
            unsafe { output.write(row, i128::from(value)) };
        }
        if !invalid {
            return Ok(());
        }
    }

    for row in 0..count {
        let value = plan.evaluate_exact(unsafe { outer.read(row) }, unsafe { inner.read(row) })?;
        unsafe { output.write(row, value) };
    }
    Ok(())
}

fn try_execute_common_factor_pair<O, I, W>(
    outer: O,
    inner: I,
    output: W,
    plan: DecimalFactorFusionBindData,
    count: usize,
) -> Option<bool>
where
    O: DirectFusionDecimalReader,
    I: DirectFusionDecimalReader,
    W: DirectDecimalWriter,
{
    let prepared = PreparedCommonFactor::try_new(plan)?;
    let inner_limit = prepared.inner_limit;
    let outer_limit = prepared.outer_limit;

    macro_rules! execute_inner {
        ($left:expr, $right:expr, $combine:expr) => {{
            let (Some(left), Some(right)) = (
                SimpleI64Operand::try_from_native($left),
                SimpleI64Operand::try_from_native($right),
            ) else {
                return None;
            };
            macro_rules! execute {
                ($finish_outer:expr) => {
                    execute_common_factor_loop(
                        outer,
                        inner,
                        output,
                        count,
                        inner_limit,
                        outer_limit,
                        |value| $combine(left.value(value), right.value(value)),
                        $finish_outer,
                    )
                };
            }
            return Some(match prepared.outer {
                I64ScaleTransform::Identity => {
                    execute!(|outer: i64, inner: i64| outer.checked_mul(inner))
                }
                I64ScaleTransform::Multiply(factor) => execute!(|outer: i64, inner: i64| {
                    outer
                        .checked_mul(inner)
                        .and_then(|value| value.checked_mul(factor))
                }),
                I64ScaleTransform::Divide(divisor) => execute!(|outer: i64, inner: i64| {
                    outer
                        .checked_mul(inner)
                        .and_then(|value| round_divide_i64(value, divisor))
                }),
            });
        }};
    }

    match plan.inner.narrow.0 {
        I64NativeDecimalPlan::Add { left, right } => {
            execute_inner!(left, right, i64::checked_add)
        }
        I64NativeDecimalPlan::Sub { left, right } => {
            execute_inner!(left, right, i64::checked_sub)
        }
        I64NativeDecimalPlan::Mul(_) => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_common_factor_loop<O, I, W, Inner, Outer>(
    outer: O,
    inner: I,
    output: W,
    count: usize,
    inner_limit: Option<u64>,
    outer_limit: Option<u64>,
    evaluate_inner: Inner,
    evaluate_outer: Outer,
) -> bool
where
    O: DirectFusionDecimalReader,
    I: DirectFusionDecimalReader,
    W: DirectDecimalWriter,
    Inner: Fn(i64) -> Option<i64>,
    Outer: Fn(i64, i64) -> Option<i64>,
{
    let mut invalid = false;
    for row in 0..count {
        let (outer_value, outer_wide) = unsafe { outer.read_narrow(row) };
        let (inner_value, inner_wide) = unsafe { inner.read_narrow(row) };
        let Some(inner_value) = evaluate_inner(inner_value) else {
            invalid = true;
            continue;
        };
        let Some(value) = evaluate_outer(outer_value, inner_value) else {
            invalid = true;
            continue;
        };
        invalid |= outer_wide
            | inner_wide
            | inner_limit.is_some_and(|limit| inner_value.unsigned_abs() >= limit)
            | outer_limit.is_some_and(|limit| value.unsigned_abs() >= limit);
        unsafe { output.write(row, i128::from(value)) };
    }
    !invalid
}

#[derive(Clone, Copy)]
enum SpeculativeNarrowReader<'a> {
    I64(*const i64),
    I128(*const i128),
    SelectedI64(&'a DecodedVectorRef<'a>),
    SelectedI128(&'a DecodedVectorRef<'a>),
    Constant(i128),
}

impl<'a> SpeculativeNarrowReader<'a> {
    fn try_new(access: &'a DecimalInputAccess<'a>) -> Option<Self> {
        match access {
            DecimalInputAccess::DirectI64(values) => Some(Self::I64(*values)),
            DecimalInputAccess::DirectI128(values) => Some(Self::I128(*values)),
            DecimalInputAccess::SelectedI64(values) => Some(Self::SelectedI64(values)),
            DecimalInputAccess::SelectedI128(values) => Some(Self::SelectedI128(values)),
            DecimalInputAccess::Constant(value) => Some(Self::Constant(*value)),
            DecimalInputAccess::Decoded(_) => None,
        }
    }

    /// Return the low word and whether narrowing changed the exact value.
    ///
    /// # Safety
    ///
    /// `row` must identify a logical row in the prepared input batch.
    #[inline(always)]
    unsafe fn read_i64(self, row: usize) -> (i64, bool) {
        let value = match self {
            Self::I64(values) => return (unsafe { *values.add(row) }, false),
            Self::I128(values) => unsafe { *values.add(row) },
            Self::SelectedI64(values) => {
                return (unsafe { values.get_value::<i64>(row) }, false);
            }
            Self::SelectedI128(values) => unsafe { values.get_value::<i128>(row) },
            Self::Constant(value) => value,
        };
        let narrowed = value as i64;
        (narrowed, i128::from(narrowed) != value)
    }
}

fn try_execute_speculative_narrow_i128_rows(
    left: &DecimalInputView<'_>,
    right: &DecimalInputView<'_>,
    output: *mut i128,
    native_plan: NativeDecimalPlan,
    count: usize,
) -> bool {
    let (Some(left), Some(right), Some(plan)) = (
        SpeculativeNarrowReader::try_new(&left.access),
        SpeculativeNarrowReader::try_new(&right.access),
        I64NativeDecimalPlan::try_from_native(native_plan),
    ) else {
        return false;
    };
    match plan {
        I64NativeDecimalPlan::Add { left: l, right: r } => {
            let (Some(l), Some(r)) = (
                SimpleI64Operand::try_from_native(l),
                SimpleI64Operand::try_from_native(r),
            ) else {
                return false;
            };
            execute_speculative_narrow_i128_loop(left, right, output, count, |left, right| {
                l.value(left).overflowing_add(r.value(right))
            })
        }
        I64NativeDecimalPlan::Sub { left: l, right: r } => {
            let (Some(l), Some(r)) = (
                SimpleI64Operand::try_from_native(l),
                SimpleI64Operand::try_from_native(r),
            ) else {
                return false;
            };
            execute_speculative_narrow_i128_loop(left, right, output, count, |left, right| {
                l.value(left).overflowing_sub(r.value(right))
            })
        }
        I64NativeDecimalPlan::Mul(I64ScaleTransform::Identity) => {
            execute_speculative_narrow_i128_loop(left, right, output, count, i64::overflowing_mul)
        }
        I64NativeDecimalPlan::Mul(
            I64ScaleTransform::Multiply(_) | I64ScaleTransform::Divide(_),
        ) => false,
    }
}

fn execute_speculative_narrow_i128_loop<F>(
    left: SpeculativeNarrowReader<'_>,
    right: SpeculativeNarrowReader<'_>,
    output: *mut i128,
    count: usize,
    evaluate: F,
) -> bool
where
    F: Fn(i64, i64) -> (i64, bool),
{
    let mut invalid = 0u64;
    for row in 0..count {
        let (left, left_wide) = unsafe { left.read_i64(row) };
        let (right, right_wide) = unsafe { right.read_i64(row) };
        let (value, overflowed) = evaluate(left, right);
        invalid |= u64::from(left_wide | right_wide | overflowed);
        unsafe { *output.add(row) = i128::from(value) };
    }
    invalid == 0
}

#[allow(clippy::too_many_arguments)]
fn try_execute_direct_i64_rows(
    left: &DecimalInputView<'_>,
    right: &DecimalInputView<'_>,
    output: *mut i64,
    precision: u8,
    result_scale: u8,
    op: DecimalArithmeticOp,
    native_plan: NativeDecimalPlan,
    count: usize,
) -> Result<bool> {
    let Some(plan) = I64NativeDecimalPlan::try_from_native(native_plan) else {
        return Ok(false);
    };
    macro_rules! execute_pair {
        ($left:expr, $right:expr) => {{
            execute_direct_i64_pair(
                $left,
                $right,
                output,
                left.scale,
                right.scale,
                result_scale,
                precision,
                op,
                plan,
                count,
            )?;
            return Ok(true);
        }};
    }

    match (&left.access, &right.access) {
        (DecimalInputAccess::DirectI64(left), DecimalInputAccess::DirectI64(right)) => {
            execute_pair!(DirectI64Reader(*left), DirectI64Reader(*right));
        }
        (DecimalInputAccess::SelectedI64(left), DecimalInputAccess::DirectI64(right)) => {
            if let Some(left) = DirectMaterializedI64Reader::try_new(left) {
                execute_pair!(left, DirectI64Reader(*right));
            }
            execute_pair!(DirectSelectedI64Reader(left), DirectI64Reader(*right));
        }
        (DecimalInputAccess::DirectI64(left), DecimalInputAccess::SelectedI64(right)) => {
            if let Some(right) = DirectMaterializedI64Reader::try_new(right) {
                execute_pair!(DirectI64Reader(*left), right);
            }
            execute_pair!(DirectI64Reader(*left), DirectSelectedI64Reader(right));
        }
        (DecimalInputAccess::SelectedI64(left), DecimalInputAccess::SelectedI64(right)) => {
            execute_pair!(
                DirectSelectedI64Reader(left),
                DirectSelectedI64Reader(right)
            );
        }
        (DecimalInputAccess::Constant(left), DecimalInputAccess::DirectI64(right)) => {
            let Ok(left) = i64::try_from(*left) else {
                return Ok(false);
            };
            execute_pair!(DirectI64ConstantReader(left), DirectI64Reader(*right));
        }
        (DecimalInputAccess::DirectI64(left), DecimalInputAccess::Constant(right)) => {
            let Ok(right) = i64::try_from(*right) else {
                return Ok(false);
            };
            execute_pair!(DirectI64Reader(*left), DirectI64ConstantReader(right));
        }
        (DecimalInputAccess::SelectedI64(left), DecimalInputAccess::Constant(right)) => {
            let Ok(right) = i64::try_from(*right) else {
                return Ok(false);
            };
            if let Some(left) = DirectMaterializedI64Reader::try_new(left) {
                execute_pair!(left, DirectI64ConstantReader(right));
            }
            execute_pair!(
                DirectSelectedI64Reader(left),
                DirectI64ConstantReader(right)
            );
        }
        (DecimalInputAccess::Constant(left), DecimalInputAccess::SelectedI64(right)) => {
            let Ok(left) = i64::try_from(*left) else {
                return Ok(false);
            };
            if let Some(right) = DirectMaterializedI64Reader::try_new(right) {
                execute_pair!(DirectI64ConstantReader(left), right);
            }
            execute_pair!(
                DirectI64ConstantReader(left),
                DirectSelectedI64Reader(right)
            );
        }
        (DecimalInputAccess::Constant(left), DecimalInputAccess::Constant(right)) => {
            let (Ok(left), Ok(right)) = (i64::try_from(*left), i64::try_from(*right)) else {
                return Ok(false);
            };
            execute_pair!(
                DirectI64ConstantReader(left),
                DirectI64ConstantReader(right)
            );
        }
        _ => Ok(false),
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_direct_i64_pair<L, R>(
    left: L,
    right: R,
    output: *mut i64,
    left_scale: u8,
    right_scale: u8,
    result_scale: u8,
    precision: u8,
    op: DecimalArithmeticOp,
    plan: I64NativeDecimalPlan,
    count: usize,
) -> Result<()>
where
    L: DirectI64DecimalReader,
    R: DirectI64DecimalReader,
{
    if try_execute_speculative_i64_pair(left, right, output, precision, plan, count)? {
        return Ok(());
    }
    match plan {
        I64NativeDecimalPlan::Add { left: l, right: r } => execute_direct_i64_loop(
            left,
            right,
            output,
            left_scale,
            right_scale,
            result_scale,
            precision,
            op,
            count,
            |left, right| l.apply(left)?.checked_add(r.apply(right)?),
        ),
        I64NativeDecimalPlan::Sub { left: l, right: r } => execute_direct_i64_loop(
            left,
            right,
            output,
            left_scale,
            right_scale,
            result_scale,
            precision,
            op,
            count,
            |left, right| l.apply(left)?.checked_sub(r.apply(right)?),
        ),
        I64NativeDecimalPlan::Mul(transform) => execute_direct_i64_loop(
            left,
            right,
            output,
            left_scale,
            right_scale,
            result_scale,
            precision,
            op,
            count,
            |left, right| {
                left.checked_mul(right)
                    .and_then(|value| transform.apply(value))
            },
        ),
    }
}

/// Run the common non-rescaling arithmetic shapes as an optimistic batch.
///
/// The loop records overflow and precision violations without branching out of
/// the batch. A clean batch is complete; an exceptional batch is recomputed by
/// the exact checked/wide path in `execute_direct_i64_pair`.
fn try_execute_speculative_i64_pair<L, R>(
    left: L,
    right: R,
    output: *mut i64,
    precision: u8,
    plan: I64NativeDecimalPlan,
    count: usize,
) -> Result<bool>
where
    L: DirectI64DecimalReader,
    R: DirectI64DecimalReader,
{
    let limit = i64_decimal_limit(precision)?;
    match plan {
        I64NativeDecimalPlan::Add { left: l, right: r } => {
            let (Some(l), Some(r)) = (
                SimpleI64Operand::try_from_native(l),
                SimpleI64Operand::try_from_native(r),
            ) else {
                return Ok(false);
            };
            Ok(execute_speculative_i64_loop(
                left,
                right,
                output,
                limit,
                count,
                |left, right| l.value(left).overflowing_add(r.value(right)),
            ))
        }
        I64NativeDecimalPlan::Sub { left: l, right: r } => {
            let (Some(l), Some(r)) = (
                SimpleI64Operand::try_from_native(l),
                SimpleI64Operand::try_from_native(r),
            ) else {
                return Ok(false);
            };
            Ok(execute_speculative_i64_loop(
                left,
                right,
                output,
                limit,
                count,
                |left, right| l.value(left).overflowing_sub(r.value(right)),
            ))
        }
        I64NativeDecimalPlan::Mul(I64ScaleTransform::Identity) => Ok(execute_speculative_i64_loop(
            left,
            right,
            output,
            limit,
            count,
            i64::overflowing_mul,
        )),
        I64NativeDecimalPlan::Mul(
            I64ScaleTransform::Multiply(_) | I64ScaleTransform::Divide(_),
        ) => Ok(false),
    }
}

fn execute_speculative_i64_loop<L, R, F>(
    left: L,
    right: R,
    output: *mut i64,
    limit: i64,
    count: usize,
    evaluate: F,
) -> bool
where
    L: DirectI64DecimalReader,
    R: DirectI64DecimalReader,
    F: Fn(i64, i64) -> (i64, bool),
{
    let mut invalid = 0u64;
    for row in 0..count {
        let left = unsafe { left.read_i64(row) };
        let right = unsafe { right.read_i64(row) };
        let (value, overflowed) = evaluate(left, right);
        invalid |= u64::from(overflowed) | u64::from(value.unsigned_abs() >= limit as u64);
        unsafe { *output.add(row) = value };
    }
    invalid == 0
}

fn i64_decimal_limit(precision: u8) -> Result<i64> {
    i64::try_from(pow10_checked::<i128>(precision).ok_or_else(|| {
        paro_error::out_of_range(format!("Decimal precision {precision} exceeds i128"))
    })?)
    .map_err(|_| paro_error::internal("i64 decimal precision exceeds its physical domain"))
}

#[allow(clippy::too_many_arguments)]
fn execute_direct_i64_loop<L, R, F>(
    left: L,
    right: R,
    output: *mut i64,
    left_scale: u8,
    right_scale: u8,
    result_scale: u8,
    precision: u8,
    op: DecimalArithmeticOp,
    count: usize,
    evaluate: F,
) -> Result<()>
where
    L: DirectI64DecimalReader,
    R: DirectI64DecimalReader,
    F: Fn(i64, i64) -> Option<i64>,
{
    let limit = i64_decimal_limit(precision)?;
    for row in 0..count {
        let left_value = unsafe { left.read_i64(row) };
        let right_value = unsafe { right.read_i64(row) };
        if let Some(value) = evaluate(left_value, right_value) {
            if value.unsigned_abs() >= limit as u64 {
                return Err(decimal_overflow(op));
            }
            unsafe { *output.add(row) = value };
            continue;
        }

        match evaluate_decimal_operation(
            op,
            i256::from(left_value),
            left_scale,
            i256::from(right_value),
            right_scale,
            result_scale,
        ) {
            DecimalEvaluation::Value(value) => {
                let value = to_i128(value, precision)?;
                let value = i64::try_from(value).map_err(|_| decimal_overflow(op))?;
                unsafe { *output.add(row) = value };
            }
            DecimalEvaluation::Null => {
                return Err(paro_error::internal(
                    "non-null decimal arithmetic produced NULL",
                ));
            }
            DecimalEvaluation::Overflow => return Err(decimal_overflow(op)),
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn execute_direct_decimal_pair<L, R, W>(
    left: L,
    right: R,
    output: W,
    left_scale: u8,
    right_scale: u8,
    result_scale: u8,
    precision: u8,
    precision_guard: I128DecimalPrecision,
    op: DecimalArithmeticOp,
    native_plan: NativeDecimalPlan,
    count: usize,
) -> Result<()>
where
    L: DirectDecimalReader,
    R: DirectDecimalReader,
    W: DirectDecimalWriter,
{
    match native_plan {
        NativeDecimalPlan::Add { left: l, right: r } => execute_direct_decimal_loop(
            left,
            right,
            output,
            left_scale,
            right_scale,
            result_scale,
            precision,
            precision_guard,
            op,
            count,
            |left, right| {
                l.apply(left)
                    .zip(r.apply(right))
                    .and_then(|(left, right)| left.checked_add(right))
                    .map(DecimalEvaluation::Value)
                    .unwrap_or(DecimalEvaluation::Overflow)
            },
        ),
        NativeDecimalPlan::Sub { left: l, right: r } => execute_direct_decimal_loop(
            left,
            right,
            output,
            left_scale,
            right_scale,
            result_scale,
            precision,
            precision_guard,
            op,
            count,
            |left, right| {
                l.apply(left)
                    .zip(r.apply(right))
                    .and_then(|(left, right)| left.checked_sub(right))
                    .map(DecimalEvaluation::Value)
                    .unwrap_or(DecimalEvaluation::Overflow)
            },
        ),
        NativeDecimalPlan::Mul(transform) => execute_direct_decimal_loop(
            left,
            right,
            output,
            left_scale,
            right_scale,
            result_scale,
            precision,
            precision_guard,
            op,
            count,
            |left, right| {
                left.checked_mul(right)
                    .and_then(|value| transform.apply(value))
                    .map(DecimalEvaluation::Value)
                    .unwrap_or(DecimalEvaluation::Overflow)
            },
        ),
        NativeDecimalPlan::Div { .. } | NativeDecimalPlan::Mod { .. } => {
            unreachable!("nullable decimal operations use the general validity-aware loop")
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_direct_decimal_loop<L, R, W, F>(
    left: L,
    right: R,
    output: W,
    left_scale: u8,
    right_scale: u8,
    result_scale: u8,
    precision: u8,
    precision_guard: I128DecimalPrecision,
    op: DecimalArithmeticOp,
    count: usize,
    evaluate: F,
) -> Result<()>
where
    L: DirectDecimalReader,
    R: DirectDecimalReader,
    W: DirectDecimalWriter,
    F: Fn(i128, i128) -> DecimalEvaluation<i128>,
{
    for row in 0..count {
        // SAFETY: the readers and writer were built from flat/all-valid vector
        // views whose logical cardinality is `count`.
        let left_value = unsafe { left.read(row) };
        let right_value = unsafe { right.read(row) };
        match evaluate(left_value, right_value) {
            DecimalEvaluation::Value(value) => {
                precision_guard.check(value)?;
                unsafe { output.write(row, value) };
            }
            DecimalEvaluation::Null => {
                unreachable!("add/subtract/multiply cannot produce NULL from valid inputs")
            }
            DecimalEvaluation::Overflow => match evaluate_decimal_operation(
                op,
                i256::from(left_value),
                left_scale,
                i256::from(right_value),
                right_scale,
                result_scale,
            ) {
                DecimalEvaluation::Value(value) => {
                    let value = to_i128(value, precision)?;
                    unsafe { output.write(row, value) };
                }
                DecimalEvaluation::Null => {
                    unreachable!("add/subtract/multiply cannot produce NULL from valid inputs")
                }
                DecimalEvaluation::Overflow => return Err(decimal_overflow(op)),
            },
        }
    }
    Ok(())
}
