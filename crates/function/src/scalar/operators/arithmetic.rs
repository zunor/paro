// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Built-in arithmetic functions.
//!
//!
//!
//! ## Dependencies Check
//! - Vector: ✅
//! - Chunk: ✅

use crate::scalar::executor::binary::BinaryExecutor;
use crate::scalar::executor::{BinaryOperator, NullableBinaryOperator};
use crate::scalar::{
    BoundScalarFunction, ExpressionState, FunctionData, ScalarBindInput, ScalarFunction,
    ScalarFunctionSet,
};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
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
            (
                left_precision.saturating_add(right_precision).min(38),
                scale,
            )
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
    for row in 0..chunk.size() {
        if chunk.data[0].is_null(row) || chunk.data[1].is_null(row) {
            result.set_null(row, true);
            continue;
        }
        let (left, left_scale) = decimal_value_parts(&chunk.data[0].get_value(row))?;
        let (right, right_scale) = decimal_value_parts(&chunk.data[1].get_value(row))?;
        let value = match bind_data.op {
            DecimalArithmeticOp::Add => rescale_decimal(left, left_scale, scale)?
                .checked_add(rescale_decimal(right, right_scale, scale)?)
                .ok_or_else(|| paro_error::out_of_range("Decimal addition overflow"))?,
            DecimalArithmeticOp::Sub => rescale_decimal(left, left_scale, scale)?
                .checked_sub(rescale_decimal(right, right_scale, scale)?)
                .ok_or_else(|| paro_error::out_of_range("Decimal subtraction overflow"))?,
            DecimalArithmeticOp::Mul => {
                let raw = left
                    .checked_mul(right)
                    .ok_or_else(|| paro_error::out_of_range("Decimal multiplication overflow"))?;
                rescale_decimal(raw, left_scale.saturating_add(right_scale), scale)?
            }
            DecimalArithmeticOp::Div => {
                if right == 0 {
                    result.set_null(row, true);
                    continue;
                }
                divide_decimal(left, left_scale, right, right_scale, scale)?
            }
            DecimalArithmeticOp::Mod => {
                if right == 0 {
                    result.set_null(row, true);
                    continue;
                }
                let left = rescale_decimal(left, left_scale, scale)?;
                let right = rescale_decimal(right, right_scale, scale)?;
                left.checked_rem(right)
                    .ok_or_else(|| paro_error::out_of_range("Decimal remainder overflow"))?
            }
        };
        check_decimal_precision(value, precision)?;
        result.set_value(row, &Value::Decimal(value, precision, scale));
    }
    Ok(())
}

fn decimal_value_parts(value: &Value) -> Result<(i128, u8)> {
    match value {
        Value::Decimal(value, _, scale) => Ok((*value, *scale)),
        Value::TinyInt(value) => Ok((*value as i128, 0)),
        Value::SmallInt(value) => Ok((*value as i128, 0)),
        Value::Integer(value) => Ok((*value as i128, 0)),
        Value::BigInt(value) => Ok((*value as i128, 0)),
        Value::HugeInt(value) => Ok((*value, 0)),
        Value::UTinyInt(value) => Ok((*value as i128, 0)),
        Value::USmallInt(value) => Ok((*value as i128, 0)),
        Value::UInteger(value) => Ok((*value as i128, 0)),
        Value::UBigInt(value) => Ok((*value as i128, 0)),
        Value::UHugeInt(value) => i128::try_from(*value)
            .map(|value| (value, 0))
            .map_err(|_| paro_error::out_of_range("UHUGEINT cannot be represented as DECIMAL")),
        _ => Err(paro_error::internal(format!(
            "unsupported decimal arithmetic value {value:?}"
        ))),
    }
}

fn pow10_i128(scale: u8) -> Result<i128> {
    (0..scale).try_fold(1_i128, |value, _| {
        value
            .checked_mul(10)
            .ok_or_else(|| paro_error::out_of_range("Decimal scale overflow"))
    })
}

fn rescale_decimal(value: i128, from_scale: u8, to_scale: u8) -> Result<i128> {
    if from_scale == to_scale {
        return Ok(value);
    }
    if from_scale < to_scale {
        return value
            .checked_mul(pow10_i128(to_scale - from_scale)?)
            .ok_or_else(|| paro_error::out_of_range("Decimal scale overflow"));
    }
    Ok(round_divide(value, pow10_i128(from_scale - to_scale)?))
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

fn divide_decimal(
    left: i128,
    left_scale: u8,
    right: i128,
    right_scale: u8,
    result_scale: u8,
) -> Result<i128> {
    let exponent = result_scale as i16 + right_scale as i16 - left_scale as i16;
    let (numerator, denominator) = if exponent >= 0 {
        (
            left.checked_mul(pow10_i128(exponent as u8)?)
                .ok_or_else(|| paro_error::out_of_range("Decimal division overflow"))?,
            right,
        )
    } else {
        (
            left,
            right
                .checked_mul(pow10_i128((-exponent) as u8)?)
                .ok_or_else(|| paro_error::out_of_range("Decimal division overflow"))?,
        )
    };
    Ok(round_divide(numerator, denominator))
}

fn check_decimal_precision(value: i128, precision: u8) -> Result<()> {
    let limit = pow10_i128(precision)?;
    if value.checked_abs().is_none_or(|absolute| absolute >= limit) {
        return Err(paro_error::out_of_range(format!(
            "Decimal value exceeds precision {precision}"
        )));
    }
    Ok(())
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

// Note: The above ScalarFunction implementation is a bit simplified because it
// currently only registers one signature (Integer, Integer) in the function object itself,
// even though the executor handles multiple types.
// In a full implementation, we would register multiple ScalarFunctions in the ScalarFunctionSet,
// one for each combination of types.

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
