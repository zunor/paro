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
        let a = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2, 3],
            paro_common::test_utils::test_allocator(),
        );
        let b = paro_common::test_utils::test_i32_vector_with_allocator(
            &[10, 20, 30],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![a, b]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        nullif_eq(&chunk, &MockState, &mut result).unwrap();

        // All different, return a
        assert_eq!(result.get_i32(0), Some(1));
        assert_eq!(result.get_i32(1), Some(2));
        assert_eq!(result.get_i32(2), Some(3));
    }

    #[test]
    fn test_nullif_i32_equal() {
        let a = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2, 3],
            paro_common::test_utils::test_allocator(),
        );
        let b = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 20, 3],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![a, b]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        nullif_eq(&chunk, &MockState, &mut result).unwrap();

        assert!(result.is_null(0)); // 1 == 1 -> NULL
        assert_eq!(result.get_i32(1), Some(2)); // 2 != 20 -> 2
        assert!(result.is_null(2)); // 3 == 3 -> NULL
    }

    #[test]
    fn test_nullif_i32_a_null() {
        let mut a = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2, 3],
            paro_common::test_utils::test_allocator(),
        );
        a.validity_mut().set_null(1);
        let b = paro_common::test_utils::test_i32_vector_with_allocator(
            &[10, 20, 30],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![a, b]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        nullif_eq(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i32(0), Some(1));
        assert!(result.is_null(1)); // a is NULL -> NULL
        assert_eq!(result.get_i32(2), Some(3));
    }

    #[test]
    fn test_nullif_i32_b_null() {
        let a = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2, 3],
            paro_common::test_utils::test_allocator(),
        );
        let mut b = paro_common::test_utils::test_i32_vector_with_allocator(
            &[10, 20, 30],
            paro_common::test_utils::test_allocator(),
        );
        b.validity_mut().set_null(1);
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![a, b]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        nullif_eq(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i32(0), Some(1));
        assert_eq!(result.get_i32(1), Some(2)); // b is NULL, a != NULL -> a
        assert_eq!(result.get_i32(2), Some(3));
    }

    #[test]
    fn test_nullif_varchar() {
        let a = paro_common::test_utils::test_string_vector_with_allocator(
            &["hello", "world", "test"],
            paro_common::test_utils::test_allocator(),
        );
        let b = paro_common::test_utils::test_string_vector_with_allocator(
            &["hello", "foo", "test"],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![a, b]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        nullif_eq(&chunk, &MockState, &mut result).unwrap();

        assert!(result.is_null(0)); // "hello" == "hello" -> NULL
        assert_eq!(result.get_string(1), Some("world")); // "world" != "foo"
        assert!(result.is_null(2)); // "test" == "test" -> NULL
    }

    #[test]
    fn test_nullif_f64() {
        let a = paro_common::test_utils::test_f64_vector_with_allocator(
            &[1.0, 2.5, 3.0],
            paro_common::test_utils::test_allocator(),
        );
        let b = paro_common::test_utils::test_f64_vector_with_allocator(
            &[1.0, 2.0, 3.0],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![a, b]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);

        nullif_f64(&chunk, &MockState, &mut result).unwrap();

        assert!(result.is_null(0)); // 1.0 == 1.0 -> NULL
        assert!((result.get_f64(1).unwrap() - 2.5).abs() < 1e-10); // 2.5 != 2.0 -> 2.5
        assert!(result.is_null(2)); // 3.0 == 3.0 -> NULL
    }

    #[test]
    fn test_nullif_bool() {
        let a = paro_common::test_utils::test_bool_vector_with_allocator(
            &[true, false, true],
            paro_common::test_utils::test_allocator(),
        );
        let b = paro_common::test_utils::test_bool_vector_with_allocator(
            &[true, true, false],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![a, b]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Boolean);

        nullif_eq(&chunk, &MockState, &mut result).unwrap();

        assert!(result.is_null(0)); // true == true -> NULL
        assert_eq!(result.get_bool(1), Some(false)); // false != true -> false
        assert_eq!(result.get_bool(2), Some(true)); // true != false -> true
    }
}
