// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Comparison operators implementation.
//!
//!

use crate::scalar::executor::binary::BinaryExecutor;
use crate::scalar::executor::BinaryOperator;
use crate::scalar::{ScalarFunction, ScalarFunctionSet};
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::{InlineString, LogicalType};
use paro_common::vector::Vector;

// --- Operators ---

pub struct EqualsOperator;
impl<T> BinaryOperator<T, T, bool> for EqualsOperator
where
    T: PartialEq + Copy,
{
    #[inline]
    fn operation(left: T, right: T) -> bool {
        left == right
    }
}

pub struct NotEqualsOperator;
impl<T> BinaryOperator<T, T, bool> for NotEqualsOperator
where
    T: PartialEq + Copy,
{
    #[inline]
    fn operation(left: T, right: T) -> bool {
        left != right
    }
}

pub struct LessThanOperator;
impl<T> BinaryOperator<T, T, bool> for LessThanOperator
where
    T: PartialOrd + Copy,
{
    #[inline]
    fn operation(left: T, right: T) -> bool {
        left < right
    }
}

pub struct LessThanEqualsOperator;
impl<T> BinaryOperator<T, T, bool> for LessThanEqualsOperator
where
    T: PartialOrd + Copy,
{
    #[inline]
    fn operation(left: T, right: T) -> bool {
        left <= right
    }
}

pub struct GreaterThanOperator;
impl<T> BinaryOperator<T, T, bool> for GreaterThanOperator
where
    T: PartialOrd + Copy,
{
    #[inline]
    fn operation(left: T, right: T) -> bool {
        left > right
    }
}

pub struct GreaterThanEqualsOperator;
impl<T> BinaryOperator<T, T, bool> for GreaterThanEqualsOperator
where
    T: PartialOrd + Copy,
{
    #[inline]
    fn operation(left: T, right: T) -> bool {
        left >= right
    }
}

// --- Function Registration ---

pub fn register_comparison_functions(set: &mut ScalarFunctionSet) {
    let name = set.name.clone();
    match name.as_str() {
        "=" | "==" => {
            add_comparison_signatures::<EqualsOperator>(set, &name);
        }
        "!=" | "<>" => {
            add_comparison_signatures::<NotEqualsOperator>(set, &name);
        }
        "<" => {
            add_comparison_signatures::<LessThanOperator>(set, &name);
        }
        "<=" => {
            add_comparison_signatures::<LessThanEqualsOperator>(set, &name);
        }
        ">" => {
            add_comparison_signatures::<GreaterThanOperator>(set, &name);
        }
        ">=" => {
            add_comparison_signatures::<GreaterThanEqualsOperator>(set, &name);
        }
        _ => {}
    }
}

fn execute_binary_comparison<T, OP>(chunk: &Chunk, result: &mut Vector) -> Result<()>
where
    T: Copy + 'static,
    OP: BinaryOperator<T, T, bool>,
{
    BinaryExecutor::execute::<T, T, bool, OP>(&chunk.data[0], &chunk.data[1], result, chunk.size());
    Ok(())
}

fn add_comparison_signatures<OP: 'static>(set: &mut ScalarFunctionSet, name: &str)
where
    OP: BinaryOperator<i32, i32, bool>
        + BinaryOperator<i64, i64, bool>
        + BinaryOperator<u64, u64, bool>
        + BinaryOperator<f64, f64, bool>
        + BinaryOperator<bool, bool, bool>
        + BinaryOperator<InlineString, InlineString, bool>,
{
    // INTEGER
    set.add_function(ScalarFunction::new(
        name.to_string(),
        vec![LogicalType::Integer, LogicalType::Integer],
        LogicalType::Boolean,
        |chunk, _state, result| execute_binary_comparison::<i32, OP>(chunk, result),
    ));

    // BIGINT
    set.add_function(ScalarFunction::new(
        name.to_string(),
        vec![LogicalType::BigInt, LogicalType::BigInt],
        LogicalType::Boolean,
        |chunk, _state, result| execute_binary_comparison::<i64, OP>(chunk, result),
    ));

    // UBIGINT
    set.add_function(ScalarFunction::new(
        name.to_string(),
        vec![LogicalType::UBigInt, LogicalType::UBigInt],
        LogicalType::Boolean,
        |chunk, _state, result| execute_binary_comparison::<u64, OP>(chunk, result),
    ));

    // DOUBLE
    set.add_function(ScalarFunction::new(
        name.to_string(),
        vec![LogicalType::Double, LogicalType::Double],
        LogicalType::Boolean,
        |chunk, _state, result| execute_binary_comparison::<f64, OP>(chunk, result),
    ));

    // BOOLEAN
    set.add_function(ScalarFunction::new(
        name.to_string(),
        vec![LogicalType::Boolean, LogicalType::Boolean],
        LogicalType::Boolean,
        |chunk, _state, result| execute_binary_comparison::<bool, OP>(chunk, result),
    ));

    // VARCHAR - uses string_t for comparison
    set.add_function(ScalarFunction::new(
        name.to_string(),
        vec![LogicalType::Varchar, LogicalType::Varchar],
        LogicalType::Boolean,
        |chunk, _state, result| execute_binary_comparison::<InlineString, OP>(chunk, result),
    ));
}
