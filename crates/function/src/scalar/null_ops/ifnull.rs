// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! IFNULL function implementation.
//!
//!
//!
//! ## Function
//! `ifnull(a, b)` - Returns `b` if `a` is NULL, otherwise returns `a`.
//!
//! This is equivalent to `COALESCE(a, b)` with exactly two arguments.

use crate::{ExpressionState, FunctionNullHandling, ScalarFunction, ScalarFunctionSet};
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use super::ifnull_rows;

fn ifnull(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    ifnull_rows(input, result)
}

/// Get the `ifnull` function set.
pub fn get_ifnull_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("ifnull".to_string());

    // ifnull(INTEGER, INTEGER) -> INTEGER
    set.add_function(
        ScalarFunction::new(
            "ifnull".to_string(),
            vec![LogicalType::Integer, LogicalType::Integer],
            LogicalType::Integer,
            ifnull,
        )
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    // ifnull(BIGINT, BIGINT) -> BIGINT
    set.add_function(
        ScalarFunction::new(
            "ifnull".to_string(),
            vec![LogicalType::BigInt, LogicalType::BigInt],
            LogicalType::BigInt,
            ifnull,
        )
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    // ifnull(DOUBLE, DOUBLE) -> DOUBLE
    set.add_function(
        ScalarFunction::new(
            "ifnull".to_string(),
            vec![LogicalType::Double, LogicalType::Double],
            LogicalType::Double,
            ifnull,
        )
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    // ifnull(VARCHAR, VARCHAR) -> VARCHAR
    set.add_function(
        ScalarFunction::new(
            "ifnull".to_string(),
            vec![LogicalType::Varchar, LogicalType::Varchar],
            LogicalType::Varchar,
            ifnull,
        )
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    // ifnull(BOOLEAN, BOOLEAN) -> BOOLEAN
    set.add_function(
        ScalarFunction::new(
            "ifnull".to_string(),
            vec![LogicalType::Boolean, LogicalType::Boolean],
            LogicalType::Boolean,
            ifnull,
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
    fn test_ifnull_i32_first_not_null() {
        let a = Vector::from_i32(&[1, 2, 3]);
        let b = Vector::from_i32(&[10, 20, 30]);
        let chunk = Chunk::from_vectors(vec![a, b]);
        let mut result = Vector::new(LogicalType::Integer);

        ifnull(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i32(0), Some(1));
        assert_eq!(result.get_i32(1), Some(2));
        assert_eq!(result.get_i32(2), Some(3));
    }

    #[test]
    fn test_ifnull_i32_first_null() {
        let mut a = Vector::from_i32(&[1, 2, 3]);
        a.validity_mut().set_null(1);
        let b = Vector::from_i32(&[10, 20, 30]);
        let chunk = Chunk::from_vectors(vec![a, b]);
        let mut result = Vector::new(LogicalType::Integer);

        ifnull(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i32(0), Some(1));
        assert_eq!(result.get_i32(1), Some(20)); // NULL -> use b
        assert_eq!(result.get_i32(2), Some(3));
    }

    #[test]
    fn test_ifnull_i32_both_null() {
        let mut a = Vector::from_i32(&[1, 2, 3]);
        let mut b = Vector::from_i32(&[10, 20, 30]);
        a.validity_mut().set_null(1);
        b.validity_mut().set_null(1);
        let chunk = Chunk::from_vectors(vec![a, b]);
        let mut result = Vector::new(LogicalType::Integer);

        ifnull(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i32(0), Some(1));
        assert!(result.is_null(1)); // Both NULL -> NULL
        assert_eq!(result.get_i32(2), Some(3));
    }

    #[test]
    fn test_ifnull_varchar() {
        let mut a = Vector::from_strings(&["hello", "world", "test"]);
        a.validity_mut().set_null(1);
        let b = Vector::from_strings(&["a", "b", "c"]);
        let chunk = Chunk::from_vectors(vec![a, b]);
        let mut result = Vector::new(LogicalType::Varchar);

        ifnull(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hello"));
        assert_eq!(result.get_string(1), Some("b")); // NULL -> use b
        assert_eq!(result.get_string(2), Some("test"));
    }

    #[test]
    fn test_ifnull_f64() {
        let mut a = Vector::from_f64(&[1.5, 2.5, 3.5]);
        a.validity_mut().set_null(0);
        let b = Vector::from_f64(&[10.5, 20.5, 30.5]);
        let chunk = Chunk::from_vectors(vec![a, b]);
        let mut result = Vector::new(LogicalType::Double);

        ifnull(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 10.5).abs() < 1e-10); // NULL -> use b
        assert!((result.get_f64(1).unwrap() - 2.5).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - 3.5).abs() < 1e-10);
    }

    #[test]
    fn test_ifnull_bool() {
        let mut a = Vector::from_bool(&[true, false, true]);
        a.validity_mut().set_null(1);
        let b = Vector::from_bool(&[false, true, false]);
        let chunk = Chunk::from_vectors(vec![a, b]);
        let mut result = Vector::new(LogicalType::Boolean);

        ifnull(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_bool(0), Some(true));
        assert_eq!(result.get_bool(1), Some(true)); // NULL -> use b
        assert_eq!(result.get_bool(2), Some(true));
    }
}
