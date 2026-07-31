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
use crate::scalar::{ScalarFunction, ScalarFunctionSet};
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
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
        }
        "-" => {
            add_numeric_signatures::<SubOperator>(set, &name);
        }
        "*" => {
            add_numeric_signatures::<MulOperator>(set, &name);
        }
        "/" => {
            add_nullable_numeric_signatures::<DivOperator>(set, &name);
        }
        "%" => {
            add_nullable_numeric_signatures::<ModOperator>(set, &name);
        }
        _ => {}
    }
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
