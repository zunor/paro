// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Built-in arithmetic functions.
//!
//!
//!
//! ## Dependencies Check
//! - Vector: ✅
//! - Chunk: ✅

mod direct_decimal;

use crate::decimal::{
    pow10_checked, rescale_checked, round_divide_checked, to_i128, DecimalInteger,
    I128DecimalPrecision,
};
use crate::scalar::executor::binary::BinaryExecutor;
use crate::scalar::executor::{BinaryOperator, NullableBinaryOperator};
use crate::scalar::{
    BoundScalarFunction, ExpressionState, FunctionData, ScalarBindInput, ScalarFunction,
    ScalarFunctionSet,
};
use direct_decimal::execute_direct_decimal_rows;
use ethnum::i256;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{DataRef, DecodedVectorRef, SelectionRef, Vector};
use std::any::Any;
use std::ops::{Add, Mul, Sub};

// --- Operators ---

pub struct AddOperator;
impl<T> BinaryOperator<T, T, T> for AddOperator
where
    T: Add<Output = T> + Copy,
{
    #[inline]
    fn operation(left: T, right: T) -> T {
        left + right
    }
}

pub struct SubOperator;
impl<T> BinaryOperator<T, T, T> for SubOperator
where
    T: Sub<Output = T> + Copy,
{
    #[inline]
    fn operation(left: T, right: T) -> T {
        left - right
    }
}

pub struct MulOperator;
impl<T> BinaryOperator<T, T, T> for MulOperator
where
    T: Mul<Output = T> + Copy,
{
    #[inline]
    fn operation(left: T, right: T) -> T {
        left * right
    }
}

pub struct DivOperator;
impl NullableBinaryOperator<i32, i32, i32> for DivOperator {
    #[inline]
    fn operation(left: i32, right: i32) -> Option<i32> {
        left.checked_div(right)
    }
}
impl NullableBinaryOperator<i64, i64, i64> for DivOperator {
    #[inline]
    fn operation(left: i64, right: i64) -> Option<i64> {
        left.checked_div(right)
    }
}
impl NullableBinaryOperator<f64, f64, f64> for DivOperator {
    #[inline]
    fn operation(left: f64, right: f64) -> Option<f64> {
        (right != 0.0).then(|| left / right)
    }
}

pub struct ModOperator;
impl NullableBinaryOperator<i32, i32, i32> for ModOperator {
    #[inline]
    fn operation(left: i32, right: i32) -> Option<i32> {
        left.checked_rem(right)
    }
}
impl NullableBinaryOperator<i64, i64, i64> for ModOperator {
    #[inline]
    fn operation(left: i64, right: i64) -> Option<i64> {
        left.checked_rem(right)
    }
}
impl NullableBinaryOperator<f64, f64, f64> for ModOperator {
    #[inline]
    fn operation(left: f64, right: f64) -> Option<f64> {
        (right != 0.0).then(|| left % right)
    }
}

// --- Function Registration ---

pub fn register_arithmetic_functions(set: &mut ScalarFunctionSet) {
    let name = set.name.clone();
    crate::scalar::date::register_temporal_arithmetic_functions(set);
    match name.as_str() {
        "+" => {
            add_numeric_signatures::<AddOperator>(set, &name);
            set.set_dynamic_bind(bind_decimal_add);
        }
        "-" => {
            add_numeric_signatures::<SubOperator>(set, &name);
            set.set_dynamic_bind(bind_decimal_sub);
        }
        "*" => {
            add_numeric_signatures::<MulOperator>(set, &name);
            set.set_dynamic_bind(bind_decimal_mul);
        }
        "/" => {
            add_nullable_numeric_signatures::<DivOperator>(set, &name);
            set.set_dynamic_bind(bind_decimal_div);
        }
        "%" => {
            add_nullable_numeric_signatures::<ModOperator>(set, &name);
            set.set_dynamic_bind(bind_decimal_mod);
        }
        _ => {}
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecimalArithmeticOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

enum DecimalEvaluation<T> {
    Value(T),
    Null,
    Overflow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecimalArithmeticBindData {
    op: DecimalArithmeticOp,
}

impl FunctionData for DecimalArithmeticBindData {
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

fn bind_decimal_add(arguments: &[LogicalType]) -> Result<(ScalarFunction, Vec<LogicalType>)> {
    bind_decimal_arithmetic(arguments, DecimalArithmeticOp::Add)
}

fn bind_decimal_sub(arguments: &[LogicalType]) -> Result<(ScalarFunction, Vec<LogicalType>)> {
    bind_decimal_arithmetic(arguments, DecimalArithmeticOp::Sub)
}

fn bind_decimal_mul(arguments: &[LogicalType]) -> Result<(ScalarFunction, Vec<LogicalType>)> {
    bind_decimal_arithmetic(arguments, DecimalArithmeticOp::Mul)
}

fn bind_decimal_div(arguments: &[LogicalType]) -> Result<(ScalarFunction, Vec<LogicalType>)> {
    bind_decimal_arithmetic(arguments, DecimalArithmeticOp::Div)
}

fn bind_decimal_mod(arguments: &[LogicalType]) -> Result<(ScalarFunction, Vec<LogicalType>)> {
    bind_decimal_arithmetic(arguments, DecimalArithmeticOp::Mod)
}

fn bind_decimal_arithmetic(
    arguments: &[LogicalType],
    op: DecimalArithmeticOp,
) -> Result<(ScalarFunction, Vec<LogicalType>)> {
    let [left, right] = arguments else {
        return Err(paro_error::function_not_found(format!(
            "decimal arithmetic with arguments {arguments:?}"
        )));
    };
    let left = left.normalize_type();
    let right = right.normalize_type();
    if !matches!(&left, LogicalType::Decimal { .. })
        && !matches!(&right, LogicalType::Decimal { .. })
    {
        return Err(paro_error::function_not_found(format!(
            "decimal arithmetic with arguments {arguments:?}"
        )));
    }

    if matches!(&left, LogicalType::Float | LogicalType::Double)
        || matches!(&right, LogicalType::Float | LogicalType::Double)
    {
        let name = decimal_op_name(op).to_string();
        let function = match op {
            DecimalArithmeticOp::Add => ScalarFunction::new(
                name,
                vec![LogicalType::Double, LogicalType::Double],
                LogicalType::Double,
                |chunk, _state, result| execute_binary_numeric::<f64, AddOperator>(chunk, result),
            ),
            DecimalArithmeticOp::Sub => ScalarFunction::new(
                name,
                vec![LogicalType::Double, LogicalType::Double],
                LogicalType::Double,
                |chunk, _state, result| execute_binary_numeric::<f64, SubOperator>(chunk, result),
            ),
            DecimalArithmeticOp::Mul => ScalarFunction::new(
                name,
                vec![LogicalType::Double, LogicalType::Double],
                LogicalType::Double,
                |chunk, _state, result| execute_binary_numeric::<f64, MulOperator>(chunk, result),
            ),
            DecimalArithmeticOp::Div => ScalarFunction::new(
                name,
                vec![LogicalType::Double, LogicalType::Double],
                LogicalType::Double,
                |chunk, _state, result| {
                    execute_nullable_binary_numeric::<f64, DivOperator>(chunk, result)
                },
            ),
            DecimalArithmeticOp::Mod => ScalarFunction::new(
                name,
                vec![LogicalType::Double, LogicalType::Double],
                LogicalType::Double,
                |chunk, _state, result| {
                    execute_nullable_binary_numeric::<f64, ModOperator>(chunk, result)
                },
            ),
        };
        return Ok((function, vec![LogicalType::Double, LogicalType::Double]));
    }

    let (left_precision, left_scale) = decimal_shape(&left)?;
    let (right_precision, right_scale) = decimal_shape(&right)?;
    let return_type =
        decimal_result_type(op, left_precision, left_scale, right_precision, right_scale);
    let name = decimal_op_name(op).to_string();
    let target_types = vec![left, right];
    let function = ScalarFunction::new(
        name,
        target_types.clone(),
        return_type,
        execute_decimal_arithmetic,
    )
    .with_bind(bind_decimal_arithmetic_function);
    Ok((function, target_types))
}

fn decimal_op_name(op: DecimalArithmeticOp) -> &'static str {
    match op {
        DecimalArithmeticOp::Add => "+",
        DecimalArithmeticOp::Sub => "-",
        DecimalArithmeticOp::Mul => "*",
        DecimalArithmeticOp::Div => "/",
        DecimalArithmeticOp::Mod => "%",
    }
}

fn decimal_shape(ty: &LogicalType) -> Result<(u8, u8)> {
    match ty {
        LogicalType::Decimal { precision, scale } => Ok((*precision, *scale)),
        LogicalType::TinyInt | LogicalType::UTinyInt => Ok((3, 0)),
        LogicalType::SmallInt | LogicalType::USmallInt => Ok((5, 0)),
        LogicalType::Integer | LogicalType::UInteger => Ok((10, 0)),
        LogicalType::BigInt => Ok((19, 0)),
        LogicalType::UBigInt => Ok((20, 0)),
        LogicalType::HugeInt | LogicalType::UHugeInt => Ok((38, 0)),
        _ => Err(paro_error::function_not_found(format!(
            "decimal arithmetic operand {ty}"
        ))),
    }
}

fn decimal_result_type(
    op: DecimalArithmeticOp,
    left_precision: u8,
    left_scale: u8,
    right_precision: u8,
    right_scale: u8,
) -> LogicalType {
    let (precision, scale) = match op {
        DecimalArithmeticOp::Add | DecimalArithmeticOp::Sub | DecimalArithmeticOp::Mod => {
            let scale = left_scale.max(right_scale);
            let integral = (left_precision - left_scale).max(right_precision - right_scale);
            (
                (integral.saturating_add(scale).saturating_add(1)).min(38),
                scale,
            )
        }
        DecimalArithmeticOp::Mul => {
            let scale = left_scale.saturating_add(right_scale).min(38);
            let exact_precision = left_precision.saturating_add(right_precision);
            // Keep multiplication inside the 64-bit DECIMAL domain when both
            // operands already fit there. The exact product is computed in a
            // wider intermediate and checked against DECIMAL(18, scale), so a
            // value outside the declared result domain fails explicitly
            // instead of forcing every row and downstream operator onto the
            // substantially more expensive i128 representation. This also
            // makes physical width stable under expression composition.
            let precision = if exact_precision > 18
                && left_precision <= 18
                && right_precision <= 18
                && scale < 18
            {
                18
            } else {
                exact_precision.min(38)
            };
            (precision, scale)
        }
        DecimalArithmeticOp::Div => {
            let scale = left_scale.saturating_add(right_scale).max(6).min(18);
            (38, scale)
        }
    };
    LogicalType::Decimal {
        precision: precision.max(1),
        scale: scale.min(precision),
    }
}

fn bind_decimal_arithmetic_function(
    function: &ScalarFunction,
    _input: &ScalarBindInput,
) -> Result<BoundScalarFunction> {
    let op = match function.name.as_str() {
        "+" => DecimalArithmeticOp::Add,
        "-" => DecimalArithmeticOp::Sub,
        "*" => DecimalArithmeticOp::Mul,
        "/" => DecimalArithmeticOp::Div,
        "%" => DecimalArithmeticOp::Mod,
        _ => return Err(paro_error::internal("unknown decimal arithmetic operator")),
    };
    Ok(
        BoundScalarFunction::from(function.clone())
            .with_bind_data(DecimalArithmeticBindData { op }),
    )
}

fn execute_decimal_arithmetic(
    chunk: &Chunk,
    state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let bind_data = state
        .bind_data()
        .and_then(|data| data.as_any().downcast_ref::<DecimalArithmeticBindData>())
        .ok_or_else(|| paro_error::internal("decimal arithmetic bind data is missing"))?;
    let LogicalType::Decimal { precision, scale } = result.logical_type().clone() else {
        return Err(paro_error::internal(
            "decimal arithmetic result is not DECIMAL",
        ));
    };
    result.set_count(chunk.size());
    execute_decimal_rows(chunk, result, precision, scale, bind_data.op)
}

fn execute_decimal_rows(
    chunk: &Chunk,
    result: &mut Vector,
    precision: u8,
    result_scale: u8,
    op: DecimalArithmeticOp,
) -> Result<()> {
    let left = DecimalInputView::try_new(&chunk.data[0], chunk.size())?;
    let right = DecimalInputView::try_new(&chunk.data[1], chunk.size())?;
    let output = DecimalOutput::try_new(result)?;
    let precision_guard = I128DecimalPrecision::new(precision)?;
    let native_plan = NativeDecimalPlan::new(op, &left, &right, result_scale);
    if execute_direct_decimal_rows(
        &left,
        &right,
        output,
        precision,
        precision_guard,
        result_scale,
        op,
        native_plan,
        chunk.size(),
    )? {
        return Ok(());
    }
    for row in 0..chunk.size() {
        if !left.is_valid(row) || !right.is_valid(row) {
            result.try_set_null(row, true)?;
            continue;
        }
        let left_value = left.value_at(row)?;
        let right_value = right.value_at(row)?;
        match native_plan.evaluate(left_value, right_value) {
            DecimalEvaluation::Value(value) => {
                precision_guard.check(value)?;
                // SAFETY: `output` points at the flat result allocation and
                // `row` is bounded by the chunk/result count.
                unsafe { output.write(row, value) };
            }
            DecimalEvaluation::Null => result.try_set_null(row, true)?,
            DecimalEvaluation::Overflow => {
                match evaluate_decimal_operation(
                    op,
                    i256::from(left_value),
                    left.scale,
                    i256::from(right_value),
                    right.scale,
                    result_scale,
                ) {
                    DecimalEvaluation::Value(value) => {
                        let value = to_i128(value, precision)?;
                        // SAFETY: same bounds and allocation contract as the
                        // native path above.
                        unsafe { output.write(row, value) };
                    }
                    DecimalEvaluation::Null => result.try_set_null(row, true)?,
                    DecimalEvaluation::Overflow => return Err(decimal_overflow(op)),
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum DecimalInputKind {
    I8,
    I16,
    I32,
    I64,
    I128,
    U8,
    U16,
    U32,
    U64,
    U128,
}

struct DecimalInputView<'a> {
    access: DecimalInputAccess<'a>,
    kind: DecimalInputKind,
    scale: u8,
}

enum DecimalInputAccess<'a> {
    DirectI64(*const i64),
    DirectI128(*const i128),
    SelectedI64(DecodedVectorRef<'a>),
    SelectedI128(DecodedVectorRef<'a>),
    Constant(i128),
    Decoded(DecodedVectorRef<'a>),
}

impl<'a> DecimalInputView<'a> {
    fn try_new(vector: &'a Vector, count: usize) -> Result<Self> {
        let (kind, scale) = match vector.logical_type() {
            LogicalType::Decimal { precision, scale } if *precision <= 18 => {
                (DecimalInputKind::I64, *scale)
            }
            LogicalType::Decimal { scale, .. } => (DecimalInputKind::I128, *scale),
            LogicalType::TinyInt => (DecimalInputKind::I8, 0),
            LogicalType::SmallInt => (DecimalInputKind::I16, 0),
            LogicalType::Integer => (DecimalInputKind::I32, 0),
            LogicalType::BigInt => (DecimalInputKind::I64, 0),
            LogicalType::HugeInt => (DecimalInputKind::I128, 0),
            LogicalType::UTinyInt => (DecimalInputKind::U8, 0),
            LogicalType::USmallInt => (DecimalInputKind::U16, 0),
            LogicalType::UInteger => (DecimalInputKind::U32, 0),
            LogicalType::UBigInt => (DecimalInputKind::U64, 0),
            LogicalType::UHugeInt => (DecimalInputKind::U128, 0),
            ty => {
                return Err(paro_error::internal(format!(
                    "unsupported decimal arithmetic type {ty}"
                )))
            }
        };
        let decoded = vector.try_decode_ref(count)?;
        let access = if decoded.validity().all_valid() {
            match (kind, decoded.data(), decoded.sel()) {
                (DecimalInputKind::I64, DataRef::Ptr(data), SelectionRef::Incremental { .. }) => {
                    DecimalInputAccess::DirectI64(data.cast::<i64>())
                }
                (DecimalInputKind::I64, DataRef::Ptr(data), SelectionRef::Range { offset, .. }) => {
                    DecimalInputAccess::DirectI64(unsafe { data.cast::<i64>().add(*offset) })
                }
                (DecimalInputKind::I128, DataRef::Ptr(data), SelectionRef::Incremental { .. }) => {
                    DecimalInputAccess::DirectI128(data.cast::<i128>())
                }
                (
                    DecimalInputKind::I128,
                    DataRef::Ptr(data),
                    SelectionRef::Range { offset, .. },
                ) => DecimalInputAccess::DirectI128(unsafe { data.cast::<i128>().add(*offset) }),
                (_, _, SelectionRef::Constant { .. }) => {
                    DecimalInputAccess::Constant(read_decimal_input(&decoded, kind, 0)?)
                }
                (DecimalInputKind::I64, DataRef::Ptr(_), _) => {
                    DecimalInputAccess::SelectedI64(decoded)
                }
                (DecimalInputKind::I128, DataRef::Ptr(_), _) => {
                    DecimalInputAccess::SelectedI128(decoded)
                }
                _ => DecimalInputAccess::Decoded(decoded),
            }
        } else {
            DecimalInputAccess::Decoded(decoded)
        };
        Ok(Self {
            access,
            kind,
            scale,
        })
    }

    #[inline]
    fn is_valid(&self, row: usize) -> bool {
        match &self.access {
            DecimalInputAccess::DirectI64(_)
            | DecimalInputAccess::DirectI128(_)
            | DecimalInputAccess::SelectedI64(_)
            | DecimalInputAccess::SelectedI128(_)
            | DecimalInputAccess::Constant(_) => true,
            DecimalInputAccess::Decoded(decoded) => decoded.is_valid(row),
        }
    }

    #[inline]
    fn value_at(&self, row: usize) -> Result<i128> {
        match &self.access {
            DecimalInputAccess::DirectI64(values) => Ok(unsafe { *values.add(row) } as i128),
            DecimalInputAccess::DirectI128(values) => Ok(unsafe { *values.add(row) }),
            DecimalInputAccess::SelectedI64(decoded) => {
                Ok(unsafe { decoded.get_value::<i64>(row) } as i128)
            }
            DecimalInputAccess::SelectedI128(decoded) => {
                Ok(unsafe { decoded.get_value::<i128>(row) })
            }
            DecimalInputAccess::Constant(value) => Ok(*value),
            DecimalInputAccess::Decoded(decoded) => read_decimal_input(decoded, self.kind, row),
        }
    }

    fn constant_value(&self) -> Option<i128> {
        match self.access {
            DecimalInputAccess::Constant(value) => Some(value),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum DecimalScaleTransform {
    Identity,
    Multiply(i128),
    Divide(i128),
    Overflow,
}

impl DecimalScaleTransform {
    fn new(source_scale: u8, target_scale: u8) -> Self {
        use std::cmp::Ordering;
        match target_scale.cmp(&source_scale) {
            Ordering::Equal => Self::Identity,
            Ordering::Greater => pow10_checked::<i128>(target_scale - source_scale)
                .map_or(Self::Overflow, Self::Multiply),
            Ordering::Less => pow10_checked::<i128>(source_scale - target_scale)
                .map_or(Self::Overflow, Self::Divide),
        }
    }

    #[inline]
    fn apply(self, value: i128) -> Option<i128> {
        match self {
            Self::Identity => Some(value),
            Self::Multiply(factor) => value.checked_mul(factor),
            Self::Divide(divisor) => round_divide_checked(value, divisor),
            Self::Overflow => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum NativeDecimalOperand {
    Input(DecimalScaleTransform),
    Constant(Option<i128>),
}

impl NativeDecimalOperand {
    fn new(input: &DecimalInputView<'_>, result_scale: u8) -> Self {
        let transform = DecimalScaleTransform::new(input.scale, result_scale);
        input
            .constant_value()
            .map_or(Self::Input(transform), |value| {
                Self::Constant(transform.apply(value))
            })
    }

    #[inline]
    fn apply(self, value: i128) -> Option<i128> {
        match self {
            Self::Input(transform) => transform.apply(value),
            Self::Constant(value) => value,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum NativeDecimalPlan {
    Add {
        left: NativeDecimalOperand,
        right: NativeDecimalOperand,
    },
    Sub {
        left: NativeDecimalOperand,
        right: NativeDecimalOperand,
    },
    Mul(DecimalScaleTransform),
    Div {
        left_scale: u8,
        right_scale: u8,
        result_scale: u8,
    },
    Mod {
        left: NativeDecimalOperand,
        right: NativeDecimalOperand,
    },
}

impl NativeDecimalPlan {
    fn new(
        op: DecimalArithmeticOp,
        left: &DecimalInputView<'_>,
        right: &DecimalInputView<'_>,
        result_scale: u8,
    ) -> Self {
        match op {
            DecimalArithmeticOp::Add => Self::Add {
                left: NativeDecimalOperand::new(left, result_scale),
                right: NativeDecimalOperand::new(right, result_scale),
            },
            DecimalArithmeticOp::Sub => Self::Sub {
                left: NativeDecimalOperand::new(left, result_scale),
                right: NativeDecimalOperand::new(right, result_scale),
            },
            DecimalArithmeticOp::Mul => Self::Mul(DecimalScaleTransform::new(
                left.scale.saturating_add(right.scale),
                result_scale,
            )),
            DecimalArithmeticOp::Div => Self::Div {
                left_scale: left.scale,
                right_scale: right.scale,
                result_scale,
            },
            DecimalArithmeticOp::Mod => Self::Mod {
                left: NativeDecimalOperand::new(left, result_scale),
                right: NativeDecimalOperand::new(right, result_scale),
            },
        }
    }

    #[inline]
    fn evaluate(self, left_value: i128, right_value: i128) -> DecimalEvaluation<i128> {
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
            Self::Div { .. } if right_value == 0 => return DecimalEvaluation::Null,
            Self::Div {
                left_scale,
                right_scale,
                result_scale,
            } => divide_decimal_checked(
                left_value,
                left_scale,
                right_value,
                right_scale,
                result_scale,
            ),
            Self::Mod { .. } if right_value == 0 => return DecimalEvaluation::Null,
            Self::Mod { left, right } => left
                .apply(left_value)
                .zip(right.apply(right_value))
                .and_then(|(left, right)| left.checked_rem(right)),
        };
        value
            .map(DecimalEvaluation::Value)
            .unwrap_or(DecimalEvaluation::Overflow)
    }
}

fn read_decimal_input(
    decoded: &DecodedVectorRef<'_>,
    kind: DecimalInputKind,
    row: usize,
) -> Result<i128> {
    if matches!(decoded.data(), DataRef::SequenceI64 { .. }) {
        // Sequence vectors have one canonical i64 physical representation;
        // their logical type was validated when the vector was built.
        return Ok(unsafe { decoded.get_value::<i64>(row) as i128 });
    }

    // SAFETY: `kind` comes from the logical type that defines the decoded
    // vector's fixed-width physical representation, and `row` is in bounds.
    unsafe {
        Ok(match kind {
            DecimalInputKind::I8 => decoded.get_value::<i8>(row) as i128,
            DecimalInputKind::I16 => decoded.get_value::<i16>(row) as i128,
            DecimalInputKind::I32 => decoded.get_value::<i32>(row) as i128,
            DecimalInputKind::I64 => decoded.get_value::<i64>(row) as i128,
            DecimalInputKind::I128 => decoded.get_value::<i128>(row),
            DecimalInputKind::U8 => decoded.get_value::<u8>(row) as i128,
            DecimalInputKind::U16 => decoded.get_value::<u16>(row) as i128,
            DecimalInputKind::U32 => decoded.get_value::<u32>(row) as i128,
            DecimalInputKind::U64 => decoded.get_value::<u64>(row) as i128,
            DecimalInputKind::U128 => {
                i128::try_from(decoded.get_value::<u128>(row)).map_err(|_| {
                    paro_error::out_of_range("UHUGEINT cannot be represented as DECIMAL")
                })?
            }
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum DecimalOutput {
    I64(*mut i64),
    I128(*mut i128),
}

impl DecimalOutput {
    fn try_new(result: &mut Vector) -> Result<Self> {
        match result.logical_type() {
            LogicalType::Decimal { precision, .. } if *precision <= 18 => {
                Ok(Self::I64(result.as_mut_slice::<i64>().as_mut_ptr()))
            }
            LogicalType::Decimal { .. } => {
                Ok(Self::I128(result.as_mut_slice::<i128>().as_mut_ptr()))
            }
            _ => Err(paro_error::internal(
                "DECIMAL writer received a non-DECIMAL result vector",
            )),
        }
    }

    /// # Safety
    ///
    /// `row` must be within the output vector allocation. `value` must already
    /// satisfy the declared DECIMAL precision.
    #[inline]
    unsafe fn write(self, row: usize, value: i128) {
        match self {
            Self::I64(output) => unsafe { *output.add(row) = value as i64 },
            Self::I128(output) => unsafe { *output.add(row) = value },
        }
    }
}

fn evaluate_decimal_operation<T: DecimalInteger>(
    op: DecimalArithmeticOp,
    left: T,
    left_scale: u8,
    right: T,
    right_scale: u8,
    result_scale: u8,
) -> DecimalEvaluation<T> {
    let value = match op {
        DecimalArithmeticOp::Add | DecimalArithmeticOp::Sub => {
            let Some(left) = rescale_checked(left, left_scale, result_scale) else {
                return DecimalEvaluation::Overflow;
            };
            let Some(right) = rescale_checked(right, right_scale, result_scale) else {
                return DecimalEvaluation::Overflow;
            };
            if op == DecimalArithmeticOp::Add {
                left.checked_add(right)
            } else {
                left.checked_sub(right)
            }
        }
        DecimalArithmeticOp::Mul => left.checked_mul(right).and_then(|value| {
            rescale_checked(value, left_scale.saturating_add(right_scale), result_scale)
        }),
        DecimalArithmeticOp::Div => {
            if right == T::ZERO {
                return DecimalEvaluation::Null;
            }
            divide_decimal_checked(left, left_scale, right, right_scale, result_scale)
        }
        DecimalArithmeticOp::Mod => {
            if right == T::ZERO {
                return DecimalEvaluation::Null;
            }
            let Some(left) = rescale_checked(left, left_scale, result_scale) else {
                return DecimalEvaluation::Overflow;
            };
            let Some(right) = rescale_checked(right, right_scale, result_scale) else {
                return DecimalEvaluation::Overflow;
            };
            left.checked_rem(right)
        }
    };
    value
        .map(DecimalEvaluation::Value)
        .unwrap_or(DecimalEvaluation::Overflow)
}

fn decimal_overflow(op: DecimalArithmeticOp) -> paro_common::error::ParoError {
    paro_error::out_of_range(format!("Decimal {} overflow", decimal_op_name(op)))
}

fn divide_decimal_checked<T: DecimalInteger>(
    left: T,
    left_scale: u8,
    right: T,
    right_scale: u8,
    result_scale: u8,
) -> Option<T> {
    let exponent = result_scale as i16 + right_scale as i16 - left_scale as i16;
    let (numerator, denominator) = if exponent >= 0 {
        (left.checked_mul(pow10_checked(exponent as u8)?)?, right)
    } else {
        (left, right.checked_mul(pow10_checked((-exponent) as u8)?)?)
    };
    round_divide_checked(numerator, denominator)
}

fn execute_binary_numeric<T, OP>(chunk: &Chunk, result: &mut Vector) -> Result<()>
where
    T: Copy + 'static,
    OP: BinaryOperator<T, T, T>,
{
    BinaryExecutor::execute::<T, T, T, OP>(&chunk.data[0], &chunk.data[1], result, chunk.size())
}

fn execute_nullable_binary_numeric<T, OP>(chunk: &Chunk, result: &mut Vector) -> Result<()>
where
    T: Copy + 'static,
    OP: NullableBinaryOperator<T, T, T>,
{
    BinaryExecutor::execute_nullable::<T, T, T, OP>(
        &chunk.data[0],
        &chunk.data[1],
        result,
        chunk.size(),
    )
}

fn add_numeric_signatures<OP: 'static>(set: &mut ScalarFunctionSet, name: &str)
where
    OP: BinaryOperator<i32, i32, i32>
        + BinaryOperator<i64, i64, i64>
        + BinaryOperator<f64, f64, f64>,
{
    // INTEGER
    set.add_function(ScalarFunction::new(
        name.to_string(),
        vec![LogicalType::Integer, LogicalType::Integer],
        LogicalType::Integer,
        |chunk, _state, result| execute_binary_numeric::<i32, OP>(chunk, result),
    ));

    // BIGINT
    set.add_function(ScalarFunction::new(
        name.to_string(),
        vec![LogicalType::BigInt, LogicalType::BigInt],
        LogicalType::BigInt,
        |chunk, _state, result| execute_binary_numeric::<i64, OP>(chunk, result),
    ));

    // DOUBLE
    set.add_function(ScalarFunction::new(
        name.to_string(),
        vec![LogicalType::Double, LogicalType::Double],
        LogicalType::Double,
        |chunk, _state, result| execute_binary_numeric::<f64, OP>(chunk, result),
    ));
}

fn add_nullable_numeric_signatures<OP: 'static>(set: &mut ScalarFunctionSet, name: &str)
where
    OP: NullableBinaryOperator<i32, i32, i32>
        + NullableBinaryOperator<i64, i64, i64>
        + NullableBinaryOperator<f64, f64, f64>,
{
    set.add_function(ScalarFunction::new(
        name.to_string(),
        vec![LogicalType::Integer, LogicalType::Integer],
        LogicalType::Integer,
        |chunk, _state, result| execute_nullable_binary_numeric::<i32, OP>(chunk, result),
    ));

    set.add_function(ScalarFunction::new(
        name.to_string(),
        vec![LogicalType::BigInt, LogicalType::BigInt],
        LogicalType::BigInt,
        |chunk, _state, result| execute_nullable_binary_numeric::<i64, OP>(chunk, result),
    ));

    set.add_function(ScalarFunction::new(
        name.to_string(),
        vec![LogicalType::Double, LogicalType::Double],
        LogicalType::Double,
        |chunk, _state, result| execute_nullable_binary_numeric::<f64, OP>(chunk, result),
    ));
}

pub fn get_add_function() -> ScalarFunction {
    ScalarFunction::new(
        "+".to_string(),
        vec![LogicalType::Integer, LogicalType::Integer],
        LogicalType::Integer,
        |chunk, _state, result| execute_binary_numeric::<i32, AddOperator>(chunk, result),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct BindState {
        bind_data: Arc<dyn FunctionData>,
    }

    impl ExpressionState for BindState {
        fn current_database(&self) -> Option<&str> {
            None
        }

        fn current_schema(&self) -> Option<&str> {
            None
        }

        fn current_user(&self) -> Option<&str> {
            None
        }

        fn bind_data(&self) -> Option<&dyn FunctionData> {
            Some(self.bind_data.as_ref())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[test]
    fn decimal_arithmetic_binds_dynamic_result_shape() {
        let mut set = ScalarFunctionSet::new("-".to_string());
        register_arithmetic_functions(&mut set);

        let (function, target_types) = set
            .bind(&[
                LogicalType::IntegerLiteral(1),
                LogicalType::Decimal {
                    precision: 15,
                    scale: 2,
                },
            ])
            .unwrap();

        assert_eq!(target_types[0], LogicalType::Integer);
        assert_eq!(
            target_types[1],
            LogicalType::Decimal {
                precision: 15,
                scale: 2
            }
        );
        assert_eq!(
            function.return_type,
            LogicalType::Decimal {
                precision: 16,
                scale: 2
            }
        );
    }

    #[test]
    fn mixed_decimal_double_arithmetic_uses_double() {
        let mut set = ScalarFunctionSet::new("+".to_string());
        register_arithmetic_functions(&mut set);

        let (function, target_types) = set
            .bind(&[
                LogicalType::Decimal {
                    precision: 8,
                    scale: 2,
                },
                LogicalType::Double,
            ])
            .unwrap();

        assert_eq!(target_types, vec![LogicalType::Double, LogicalType::Double]);
        assert_eq!(function.return_type, LogicalType::Double);
    }

    #[test]
    fn decimal_multiplication_uses_wide_intermediate() {
        let operand = 20_000_000_000_000_000_000_i128;
        assert!(operand.checked_mul(operand).is_none());

        let input_type = LogicalType::Decimal {
            precision: 38,
            scale: 20,
        };
        let mut set = ScalarFunctionSet::new("*".to_string());
        register_arithmetic_functions(&mut set);
        let (function, target_types) = set.bind(&[input_type.clone(), input_type.clone()]).unwrap();
        let bound = function
            .bind(&ScalarBindInput::new(target_types, vec![None, None]))
            .unwrap();
        let state = BindState {
            bind_data: bound.bind_data.as_ref().unwrap().clone(),
        };

        let mut left = paro_common::test_utils::test_vector(input_type.clone());
        left.set_count(1);
        left.set_i128(0, operand);
        let mut right = paro_common::test_utils::test_vector(input_type);
        right.set_count(1);
        right.set_i128(0, operand);
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![left, right]);
        let mut result = paro_common::test_utils::test_vector(bound.return_type.clone());
        bound.execute(&chunk, &state, &mut result).unwrap();

        assert_eq!(
            unsafe { result.get_fixed::<i128>(0) },
            4_000_000_000_000_000_000_000_000_000_000_000_000_i128
        );
    }

    #[test]
    fn decimal_multiplication_preserves_i64_physical_domain() {
        assert_eq!(
            decimal_result_type(DecimalArithmeticOp::Mul, 15, 2, 16, 2),
            LogicalType::Decimal {
                precision: 18,
                scale: 4,
            }
        );
        assert_eq!(
            decimal_result_type(DecimalArithmeticOp::Mul, 18, 4, 16, 2),
            LogicalType::Decimal {
                precision: 18,
                scale: 6,
            }
        );
    }

    #[test]
    fn decimal_i64_multiplication_is_exact_and_checks_declared_precision() {
        let left_type = LogicalType::Decimal {
            precision: 15,
            scale: 2,
        };
        let right_type = LogicalType::Decimal {
            precision: 16,
            scale: 2,
        };
        let mut set = ScalarFunctionSet::new("*".to_string());
        register_arithmetic_functions(&mut set);
        let (function, target_types) = set.bind(&[left_type.clone(), right_type.clone()]).unwrap();
        let bound = function
            .bind(&ScalarBindInput::new(target_types, vec![None, None]))
            .unwrap();
        let state = BindState {
            bind_data: bound.bind_data.as_ref().unwrap().clone(),
        };

        let mut left = paro_common::test_utils::test_vector(left_type.clone());
        left.set_count(2);
        left.set_i64(0, 12_345);
        left.set_i64(1, 99_999);
        let mut right = paro_common::test_utils::test_vector(right_type.clone());
        right.set_count(2);
        right.set_i64(0, 9_500);
        right.set_i64(1, 8_000);
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![left, right]);
        let mut result = paro_common::test_utils::test_vector(bound.return_type.clone());
        bound.execute(&chunk, &state, &mut result).unwrap();
        assert_eq!(unsafe { result.get_fixed::<i64>(0) }, 117_277_500);
        assert_eq!(unsafe { result.get_fixed::<i64>(1) }, 799_992_000);

        let mut left = paro_common::test_utils::test_vector(left_type);
        left.set_count(1);
        left.set_i64(0, 999_999_999_999_999);
        let mut right = paro_common::test_utils::test_vector(right_type);
        right.set_count(1);
        right.set_i64(0, 9_999_999_999_999_999);
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![left, right]);
        let mut result = paro_common::test_utils::test_vector(bound.return_type.clone());
        assert!(bound.execute(&chunk, &state, &mut result).is_err());
    }

    #[test]
    fn division_invalid_integer_domain_is_null() {
        assert_eq!(
            <DivOperator as NullableBinaryOperator<i32, i32, i32>>::operation(10, 0),
            None
        );
        assert_eq!(
            <DivOperator as NullableBinaryOperator<i32, i32, i32>>::operation(i32::MIN, -1),
            None
        );
    }

    #[test]
    fn division_and_remainder_by_float_zero_are_null() {
        assert_eq!(
            <DivOperator as NullableBinaryOperator<f64, f64, f64>>::operation(1.0, 0.0),
            None
        );
        assert_eq!(
            <ModOperator as NullableBinaryOperator<f64, f64, f64>>::operation(1.0, -0.0),
            None
        );
    }
}
