// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! COALESCE function implementation.
//!
//!
//!
//! ## Function
//! `coalesce(a, b,...)` - Returns the first non-NULL argument.
//!
//! If all arguments are NULL, returns NULL.

use crate::{ExpressionState, FunctionNullHandling, ScalarFunction, ScalarFunctionSet};
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use super::coalesce_rows;

fn coalesce(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    coalesce_rows(input, result)
}

/// Get the `coalesce` function set.
pub fn get_coalesce_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("coalesce".to_string());

    // coalesce(INTEGER...) -> INTEGER
    set.add_function(
        ScalarFunction::new(
            "coalesce".to_string(),
            vec![],
            LogicalType::Integer,
            coalesce,
        )
        .with_varargs(LogicalType::Integer)
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    // coalesce(BIGINT...) -> BIGINT
    set.add_function(
        ScalarFunction::new(
            "coalesce".to_string(),
            vec![],
            LogicalType::BigInt,
            coalesce,
        )
        .with_varargs(LogicalType::BigInt)
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    // coalesce(DOUBLE...) -> DOUBLE
    set.add_function(
        ScalarFunction::new(
            "coalesce".to_string(),
            vec![],
            LogicalType::Double,
            coalesce,
        )
        .with_varargs(LogicalType::Double)
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    // coalesce(VARCHAR...) -> VARCHAR
    set.add_function(
        ScalarFunction::new(
            "coalesce".to_string(),
            vec![],
            LogicalType::Varchar,
            coalesce,
        )
        .with_varargs(LogicalType::Varchar)
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    // coalesce(BOOLEAN...) -> BOOLEAN
    set.add_function(
        ScalarFunction::new(
            "coalesce".to_string(),
            vec![],
            LogicalType::Boolean,
            coalesce,
        )
        .with_varargs(LogicalType::Boolean)
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
    fn test_coalesce_i32_first_not_null() {
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

        coalesce(&chunk, &MockState, &mut result).unwrap();

        // First column is not null, use it
        assert_eq!(result.get_i32(0), Some(1));
        assert_eq!(result.get_i32(1), Some(2));
        assert_eq!(result.get_i32(2), Some(3));
    }

    #[test]
    fn test_coalesce_i32_first_null() {
        let mut a = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2, 3],
            paro_common::test_utils::test_allocator(),
        );
        a.validity_mut().set_null(0);
        a.validity_mut().set_null(2);
        let b = paro_common::test_utils::test_i32_vector_with_allocator(
            &[10, 20, 30],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![a, b]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        coalesce(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i32(0), Some(10)); // NULL -> use b
        assert_eq!(result.get_i32(1), Some(2)); // not NULL -> use a
        assert_eq!(result.get_i32(2), Some(30)); // NULL -> use b
    }

    #[test]
    fn test_coalesce_i32_all_null() {
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

        coalesce(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i32(0), Some(1));
        assert!(result.is_null(1)); // Both NULL -> NULL
        assert_eq!(result.get_i32(2), Some(3));
    }

    #[test]
    fn test_coalesce_i32_three_args() {
        let mut a = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2, 3],
            paro_common::test_utils::test_allocator(),
        );
        let mut b = paro_common::test_utils::test_i32_vector_with_allocator(
            &[10, 20, 30],
            paro_common::test_utils::test_allocator(),
        );
        let c = paro_common::test_utils::test_i32_vector_with_allocator(
            &[100, 200, 300],
            paro_common::test_utils::test_allocator(),
        );
        a.validity_mut().set_null(0);
        a.validity_mut().set_null(1);
        b.validity_mut().set_null(1);
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![a, b, c]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        coalesce(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i32(0), Some(10)); // NULL, 10, 100 -> 10
        assert_eq!(result.get_i32(1), Some(200)); // NULL, NULL, 200 -> 200
        assert_eq!(result.get_i32(2), Some(3)); // 3, 30, 300 -> 3
    }

    #[test]
    fn test_coalesce_varchar() {
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

        coalesce(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hello"));
        assert_eq!(result.get_string(1), Some("b")); // NULL -> use b
        assert_eq!(result.get_string(2), Some("test"));
    }

    #[test]
    fn test_coalesce_f64() {
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

        coalesce(&chunk, &MockState, &mut result).unwrap();

        assert!((result.get_f64(0).unwrap() - 10.5).abs() < 1e-10); // NULL -> use b
        assert!((result.get_f64(1).unwrap() - 2.5).abs() < 1e-10);
        assert!((result.get_f64(2).unwrap() - 3.5).abs() < 1e-10);
    }

    #[test]
    fn test_coalesce_bool() {
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

        coalesce(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_bool(0), Some(true));
        assert_eq!(result.get_bool(1), Some(true)); // NULL -> use b
        assert_eq!(result.get_bool(2), Some(true));
    }

    #[test]
    fn test_coalesce_single_arg() {
        let a = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2, 3],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![a]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        coalesce(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i32(0), Some(1));
        assert_eq!(result.get_i32(1), Some(2));
        assert_eq!(result.get_i32(2), Some(3));
    }

    #[test]
    fn test_coalesce_empty() {
        let chunk = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        coalesce(&chunk, &MockState, &mut result).unwrap();

        // Empty chunk, no rows to process - size should be 0
    }

    #[test]
    fn test_coalesce_i64() {
        let mut a = paro_common::test_utils::test_i64_vector_with_allocator(
            &[1_000_000_000_000i64, 2, 3],
            paro_common::test_utils::test_allocator(),
        );
        a.validity_mut().set_null(0);
        let b = paro_common::test_utils::test_i64_vector_with_allocator(
            &[10, 20, 30],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![a, b]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::BigInt);

        coalesce(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i64(0), Some(10)); // NULL -> use b
        assert_eq!(result.get_i64(1), Some(2));
        assert_eq!(result.get_i64(2), Some(3));
    }
}
