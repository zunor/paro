// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! NULLIF function implementation.
//!
//!
//!
//! ## Function
//! `nullif(a, b)` - Returns NULL if `a = b`, otherwise returns `a`.
//!
//! This is equivalent to `CASE WHEN a = b THEN NULL ELSE a END`.

use crate::{ExpressionState, FunctionNullHandling, ScalarFunction, ScalarFunctionSet};
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use super::nullif_rows;

fn nullif_eq(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    nullif_rows(input, result, |left, right| left == right)
}

fn nullif_f64(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    nullif_rows(input, result, |left, right| match (left, right) {
        (Value::Double(a), Value::Double(b)) => (a - b).abs() < f64::EPSILON,
        _ => false,
    })
}

/// Get the `nullif` function set.
pub fn get_nullif_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("nullif".to_string());

    // nullif(INTEGER, INTEGER) -> INTEGER
    set.add_function(
        ScalarFunction::new(
            "nullif".to_string(),
            vec![LogicalType::Integer, LogicalType::Integer],
            LogicalType::Integer,
            nullif_eq,
        )
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    // nullif(BIGINT, BIGINT) -> BIGINT
    set.add_function(
        ScalarFunction::new(
            "nullif".to_string(),
            vec![LogicalType::BigInt, LogicalType::BigInt],
            LogicalType::BigInt,
            nullif_eq,
        )
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    // nullif(DOUBLE, DOUBLE) -> DOUBLE
    set.add_function(
        ScalarFunction::new(
            "nullif".to_string(),
            vec![LogicalType::Double, LogicalType::Double],
            LogicalType::Double,
            nullif_f64,
        )
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    // nullif(VARCHAR, VARCHAR) -> VARCHAR
    set.add_function(
        ScalarFunction::new(
            "nullif".to_string(),
            vec![LogicalType::Varchar, LogicalType::Varchar],
            LogicalType::Varchar,
            nullif_eq,
        )
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    // nullif(BOOLEAN, BOOLEAN) -> BOOLEAN
    set.add_function(
        ScalarFunction::new(
            "nullif".to_string(),
            vec![LogicalType::Boolean, LogicalType::Boolean],
            LogicalType::Boolean,
            nullif_eq,
        )
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    set
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::chunk::Chunk;

    struct MockState;
    impl ExpressionState for MockState {
        fn current_database(&self) -> Option<&str> {
            None
        }
        fn current_schema(&self) -> Option<&str> {
            None
        }
        fn current_user(&self) -> Option<&str> {
            None
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    #[test]
    fn test_nullif_i32_not_equal() {
        let a = Vector::from_i32(&[1, 2, 3]);
        let b = Vector::from_i32(&[10, 20, 30]);
        let chunk = Chunk::from_vectors(vec![a, b]);
        let mut result = Vector::new(LogicalType::Integer);

        nullif_eq(&chunk, &MockState, &mut result).unwrap();

        // All different, return a
        assert_eq!(result.get_i32(0), Some(1));
        assert_eq!(result.get_i32(1), Some(2));
        assert_eq!(result.get_i32(2), Some(3));
    }

    #[test]
    fn test_nullif_i32_equal() {
        let a = Vector::from_i32(&[1, 2, 3]);
        let b = Vector::from_i32(&[1, 20, 3]);
        let chunk = Chunk::from_vectors(vec![a, b]);
        let mut result = Vector::new(LogicalType::Integer);

        nullif_eq(&chunk, &MockState, &mut result).unwrap();

        assert!(result.is_null(0)); // 1 == 1 -> NULL
        assert_eq!(result.get_i32(1), Some(2)); // 2 != 20 -> 2
        assert!(result.is_null(2)); // 3 == 3 -> NULL
    }

    #[test]
    fn test_nullif_i32_a_null() {
        let mut a = Vector::from_i32(&[1, 2, 3]);
        a.validity_mut().set_null(1);
        let b = Vector::from_i32(&[10, 20, 30]);
        let chunk = Chunk::from_vectors(vec![a, b]);
        let mut result = Vector::new(LogicalType::Integer);

        nullif_eq(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i32(0), Some(1));
        assert!(result.is_null(1)); // a is NULL -> NULL
        assert_eq!(result.get_i32(2), Some(3));
    }

    #[test]
    fn test_nullif_i32_b_null() {
        let a = Vector::from_i32(&[1, 2, 3]);
        let mut b = Vector::from_i32(&[10, 20, 30]);
        b.validity_mut().set_null(1);
        let chunk = Chunk::from_vectors(vec![a, b]);
        let mut result = Vector::new(LogicalType::Integer);

        nullif_eq(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i32(0), Some(1));
        assert_eq!(result.get_i32(1), Some(2)); // b is NULL, a != NULL -> a
        assert_eq!(result.get_i32(2), Some(3));
    }

    #[test]
    fn test_nullif_varchar() {
        let a = Vector::from_strings(&["hello", "world", "test"]);
        let b = Vector::from_strings(&["hello", "foo", "test"]);
        let chunk = Chunk::from_vectors(vec![a, b]);
        let mut result = Vector::new(LogicalType::Varchar);

        nullif_eq(&chunk, &MockState, &mut result).unwrap();

        assert!(result.is_null(0)); // "hello" == "hello" -> NULL
        assert_eq!(result.get_string(1), Some("world")); // "world" != "foo"
        assert!(result.is_null(2)); // "test" == "test" -> NULL
    }

    #[test]
    fn test_nullif_f64() {
        let a = Vector::from_f64(&[1.0, 2.5, 3.0]);
        let b = Vector::from_f64(&[1.0, 2.0, 3.0]);
        let chunk = Chunk::from_vectors(vec![a, b]);
        let mut result = Vector::new(LogicalType::Double);

        nullif_f64(&chunk, &MockState, &mut result).unwrap();

        assert!(result.is_null(0)); // 1.0 == 1.0 -> NULL
        assert!((result.get_f64(1).unwrap() - 2.5).abs() < 1e-10); // 2.5 != 2.0 -> 2.5
        assert!(result.is_null(2)); // 3.0 == 3.0 -> NULL
    }

    #[test]
    fn test_nullif_bool() {
        let a = Vector::from_bool(&[true, false, true]);
        let b = Vector::from_bool(&[true, true, false]);
        let chunk = Chunk::from_vectors(vec![a, b]);
        let mut result = Vector::new(LogicalType::Boolean);

        nullif_eq(&chunk, &MockState, &mut result).unwrap();

        assert!(result.is_null(0)); // true == true -> NULL
        assert_eq!(result.get_bool(1), Some(false)); // false != true -> false
        assert_eq!(result.get_bool(2), Some(true)); // true != false -> true
    }
}
