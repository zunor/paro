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

        ifnull(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i32(0), Some(1));
        assert_eq!(result.get_i32(1), Some(2));
        assert_eq!(result.get_i32(2), Some(3));
    }

    #[test]
    fn test_ifnull_i32_first_null() {
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

        ifnull(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i32(0), Some(1));
        assert_eq!(result.get_i32(1), Some(20)); // NULL -> use b
        assert_eq!(result.get_i32(2), Some(3));
    }

    #[test]
    fn test_ifnull_i32_both_null() {
        let mut a = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2, 3],
            paro_common::test_utils::test_allocator(),
        );
        let mut b = paro_common::test_utils::test_i32_vector_with_allocator(
            &[10, 20, 30],
            paro_common::test_utils::test_allocator(),
        );
        a.validity_mut().set_null(1);
        b.validity_mut().set_null(1);
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![a, b]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        ifnull(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i32(0), Some(1));
        assert!(result.is_null(1)); // Both NULL -> NULL
        assert_eq!(result.get_i32(2), Some(3));
    }

    #[test]
    fn test_ifnull_varchar() {
        let mut a = paro_common::test_utils::test_string_vector_with_allocator(
            &["hello", "world", "test"],
            paro_common::test_utils::test_allocator(),
        );
        a.validity_mut().set_null(1);
        let b = paro_common::test_utils::test_string_vector_with_allocator(
            &["a", "b", "c"],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![a, b]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        ifnull(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hello"));
        assert_eq!(result.get_string(1), Some("b")); // NULL -> use b
        assert_eq!(result.get_string(2), Some("test"));
    }

    #[test]
    fn test_ifnull_f64() {
        let mut a = paro_common::test_utils::test_f64_vector_with_allocator(
            &[1.5, 2.5, 3.5],
            paro_common::test_utils::test_allocator(),
        );
        a.validity_mut().set_null(0);
        let b = paro_common::test_utils::test_f64_vector_with_allocator(
            &[10.5, 20.5, 30.5],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![a, b]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Double);

        ifnull(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 10.5).abs() < 1e-10); // NULL -> use b
        assert!((result.get_f64(1).unwrap() - 2.5).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - 3.5).abs() < 1e-10);
    }

    #[test]
    fn test_ifnull_bool() {
        let mut a = paro_common::test_utils::test_bool_vector_with_allocator(
            &[true, false, true],
            paro_common::test_utils::test_allocator(),
        );
        a.validity_mut().set_null(1);
        let b = paro_common::test_utils::test_bool_vector_with_allocator(
            &[false, true, false],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![a, b]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Boolean);

        ifnull(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_bool(0), Some(true));
        assert_eq!(result.get_bool(1), Some(true)); // NULL -> use b
        assert_eq!(result.get_bool(2), Some(true));
    }
}
