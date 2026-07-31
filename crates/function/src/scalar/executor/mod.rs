// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Scalar Function Executors
//!
//! This module provides generic vectorized executors for scalar functions.
//! handle different vector types (Flat, Constant) and physical types (i32, f64, etc.).

pub mod binary;
pub mod ternary;
pub(crate) mod typed_loops;
pub mod unary;
pub mod variadic;
pub mod varlen;

/// Trait for a unary operator (e.g., ABS, NOT).
pub trait UnaryOperator<INPUT, RESULT> {
    /// The scalar operation logic.
    fn operation(input: INPUT) -> RESULT;
}

/// Trait for a binary operator (e.g., ADD, SUB, COMPARISON).
pub trait BinaryOperator<LEFT, RIGHT, RESULT> {
    /// The scalar operation logic.
    fn operation(left: LEFT, right: RIGHT) -> RESULT;
}

/// A binary operator whose valid inputs can still produce SQL `NULL`.
///
/// This is distinct from [`BinaryOperator`]: input validity alone does not determine result
/// validity for operations such as division and remainder, where a zero divisor yields `NULL`.
pub trait NullableBinaryOperator<LEFT, RIGHT, RESULT> {
    /// Return `None` when this input pair produces SQL `NULL`.
    fn operation(left: LEFT, right: RIGHT) -> Option<RESULT>;
}

/// Trait for a ternary operator (e.g., BETWEEN, SUBSTR, CASE).
pub trait TernaryOperator<A, B, C, RESULT> {
    /// The scalar operation logic.
    fn operation(a: A, b: B, c: C) -> RESULT;
}

#[cfg(test)]
mod tests {
    use super::binary::BinaryExecutor;
    use super::unary::UnaryExecutor;
    use super::*;
    use paro_common::types::LogicalType;

    struct AddOp;
    impl<T> BinaryOperator<T, T, T> for AddOp
    where
        T: std::ops::Add<Output = T> + Copy,
    {
        fn operation(left: T, right: T) -> T {
            left + right
        }
    }

    struct NegOp;
    impl<T> UnaryOperator<T, T> for NegOp
    where
        T: std::ops::Neg<Output = T> + Copy,
    {
        fn operation(input: T) -> T {
            -input
        }
    }

    #[test]
    fn test_binary_executor_flat_flat() {
        let left = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2, 3],
            paro_common::test_utils::test_allocator(),
        );
        let right = paro_common::test_utils::test_i32_vector_with_allocator(
            &[10, 20, 30],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        BinaryExecutor::execute::<i32, i32, i32, AddOp>(&left, &right, &mut result, 3)
            .expect("binary executor should succeed");

        assert_eq!(result.get_i32(0), Some(11));
        assert_eq!(result.get_i32(1), Some(22));
        assert_eq!(result.get_i32(2), Some(33));
    }

    #[test]
    fn test_binary_executor_flat_constant() {
        let left = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2, 3],
            paro_common::test_utils::test_allocator(),
        );
        let right = paro_common::test_utils::test_constant_with_allocator::<i32>(
            LogicalType::Integer,
            10,
            3,
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        BinaryExecutor::execute::<i32, i32, i32, AddOp>(&left, &right, &mut result, 3)
            .expect("binary executor should succeed");

        assert_eq!(result.get_i32(0), Some(11));
        assert_eq!(result.get_i32(1), Some(12));
        assert_eq!(result.get_i32(2), Some(13));
    }

    #[test]
    fn test_binary_executor_null_handling() {
        let mut left = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2, 3],
            paro_common::test_utils::test_allocator(),
        );
        left.validity_mut().set_null(1); // [1, NULL, 3]
        let right = paro_common::test_utils::test_i32_vector_with_allocator(
            &[10, 20, 30],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        BinaryExecutor::execute::<i32, i32, i32, AddOp>(&left, &right, &mut result, 3)
            .expect("binary executor should succeed");

        assert_eq!(result.get_i32(0), Some(11));
        assert!(result.is_null(1));
        assert_eq!(result.get_i32(2), Some(33));
    }

    #[test]
    fn test_unary_executor_flat() {
        let input = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, -2, 3],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        UnaryExecutor::execute::<i32, i32, NegOp>(&input, &mut result, 3)
            .expect("unary executor should succeed");

        assert_eq!(result.get_i32(0), Some(-1));
        assert_eq!(result.get_i32(1), Some(2));
        assert_eq!(result.get_i32(2), Some(-3));
    }

    // =========================================================================
    // =========================================================================

    use paro_common::types::InlineString;

    /// String equality operator for testing
    struct StringEqualsOp;
    impl BinaryOperator<InlineString, InlineString, bool> for StringEqualsOp {
        fn operation(left: InlineString, right: InlineString) -> bool {
            left == right
        }
    }

    /// String less-than operator for testing
    struct StringLessThanOp;
    impl BinaryOperator<InlineString, InlineString, bool> for StringLessThanOp {
        fn operation(left: InlineString, right: InlineString) -> bool {
            left < right
        }
    }

    /// String greater-than operator for testing
    struct StringGreaterThanOp;
    impl BinaryOperator<InlineString, InlineString, bool> for StringGreaterThanOp {
        fn operation(left: InlineString, right: InlineString) -> bool {
            left > right
        }
    }

    #[test]
    fn test_binary_executor_string_equals() {
        let left = paro_common::test_utils::test_string_vector_with_allocator(
            &["apple", "banana", "cherry"],
            paro_common::test_utils::test_allocator(),
        );
        let right = paro_common::test_utils::test_string_vector_with_allocator(
            &["apple", "orange", "cherry"],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::Boolean);

        BinaryExecutor::execute::<InlineString, InlineString, bool, StringEqualsOp>(
            &left,
            &right,
            &mut result,
            3,
        )
        .expect("string binary executor should succeed");

        assert_eq!(result.get_bool(0), Some(true)); // apple == apple
        assert_eq!(result.get_bool(1), Some(false)); // banana != orange
        assert_eq!(result.get_bool(2), Some(true)); // cherry == cherry
    }

    #[test]
    fn test_binary_executor_string_less_than() {
        let left = paro_common::test_utils::test_string_vector_with_allocator(
            &["apple", "banana", "cherry"],
            paro_common::test_utils::test_allocator(),
        );
        let right = paro_common::test_utils::test_string_vector_with_allocator(
            &["banana", "apple", "cherry"],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::Boolean);

        BinaryExecutor::execute::<InlineString, InlineString, bool, StringLessThanOp>(
            &left,
            &right,
            &mut result,
            3,
        )
        .expect("string binary executor should succeed");

        assert_eq!(result.get_bool(0), Some(true)); // apple < banana
        assert_eq!(result.get_bool(1), Some(false)); // banana > apple
        assert_eq!(result.get_bool(2), Some(false)); // cherry == cherry
    }

    #[test]
    fn test_binary_executor_string_greater_than() {
        let left = paro_common::test_utils::test_string_vector_with_allocator(
            &["banana", "apple", "delta"],
            paro_common::test_utils::test_allocator(),
        );
        let right = paro_common::test_utils::test_string_vector_with_allocator(
            &["apple", "banana", "alpha"],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::Boolean);

        BinaryExecutor::execute::<InlineString, InlineString, bool, StringGreaterThanOp>(
            &left,
            &right,
            &mut result,
            3,
        )
        .expect("string binary executor should succeed");

        assert_eq!(result.get_bool(0), Some(true)); // banana > apple
        assert_eq!(result.get_bool(1), Some(false)); // apple < banana
        assert_eq!(result.get_bool(2), Some(true)); // delta > alpha
    }

    #[test]
    fn test_binary_executor_string_with_long_strings() {
        // Test with strings >12 bytes (use heap storage)
        let left = paro_common::test_utils::test_string_vector_with_allocator(
            &[
                "this is a long string 1",
                "another very long string here",
                "same_prefix_different_end_a",
            ],
            paro_common::test_utils::test_allocator(),
        );
        let right = paro_common::test_utils::test_string_vector_with_allocator(
            &[
                "this is a long string 1",
                "another very long string here",
                "same_prefix_different_end_b",
            ],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::Boolean);

        BinaryExecutor::execute::<InlineString, InlineString, bool, StringEqualsOp>(
            &left,
            &right,
            &mut result,
            3,
        )
        .expect("string binary executor should succeed");

        assert_eq!(result.get_bool(0), Some(true)); // same
        assert_eq!(result.get_bool(1), Some(true)); // same
        assert_eq!(result.get_bool(2), Some(false)); // different suffix
    }

    #[test]
    fn test_binary_executor_string_with_constant() {
        let left = paro_common::test_utils::test_string_vector_with_allocator(
            &["apple", "banana", "cherry"],
            paro_common::test_utils::test_allocator(),
        );
        // Create a constant string vector
        let mut right = paro_common::test_utils::test_string_vector_with_allocator(
            &["banana"],
            paro_common::test_utils::test_allocator(),
        );
        right.set_vector_type(paro_common::vector::VectorType::Constant);
        right.set_count(3);

        let mut result = paro_common::test_utils::test_vector(LogicalType::Boolean);

        BinaryExecutor::execute::<InlineString, InlineString, bool, StringEqualsOp>(
            &left,
            &right,
            &mut result,
            3,
        )
        .expect("string binary executor should succeed");

        assert_eq!(result.get_bool(0), Some(false)); // apple != banana
        assert_eq!(result.get_bool(1), Some(true)); // banana == banana
        assert_eq!(result.get_bool(2), Some(false)); // cherry != banana
    }

    #[test]
    fn test_binary_executor_string_select() {
        let left = paro_common::test_utils::test_string_vector_with_allocator(
            &["a", "b", "c", "d", "e"],
            paro_common::test_utils::test_allocator(),
        );
        let right = paro_common::test_utils::test_string_vector_with_allocator(
            &["a", "x", "c", "y", "e"],
            paro_common::test_utils::test_allocator(),
        );
        let mut selection = paro_common::test_utils::test_selection_with_capacity(5);

        BinaryExecutor::select_into::<InlineString, InlineString, StringEqualsOp>(
            &left,
            &right,
            None,
            5,
            &mut selection,
        )
        .expect("select");

        // Only indices where left == right should be selected
        assert_eq!(selection.as_slice(), &[0, 2, 4]);
    }

    #[test]
    fn test_binary_executor_string_select_less_than() {
        let left = paro_common::test_utils::test_string_vector_with_allocator(
            &["apple", "banana", "cherry", "date"],
            paro_common::test_utils::test_allocator(),
        );
        let right = paro_common::test_utils::test_string_vector_with_allocator(
            &["banana", "apple", "delta", "cherry"],
            paro_common::test_utils::test_allocator(),
        );
        let mut selection = paro_common::test_utils::test_selection_with_capacity(4);

        BinaryExecutor::select_into::<InlineString, InlineString, StringLessThanOp>(
            &left,
            &right,
            None,
            4,
            &mut selection,
        )
        .expect("select");

        // apple < banana (0), cherry < delta (2)
        assert_eq!(selection.as_slice(), &[0, 2]);
    }

    #[test]
    fn test_varchar_comparison_via_executor() {
        let left = paro_common::test_utils::test_string_vector_with_allocator(
            &["aaa", "bbb", "ccc"],
            paro_common::test_utils::test_allocator(),
        );
        let right = paro_common::test_utils::test_string_vector_with_allocator(
            &["aaa", "xxx", "ccc"],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::Boolean);

        BinaryExecutor::execute::<InlineString, InlineString, bool, StringEqualsOp>(
            &left,
            &right,
            &mut result,
            3,
        )
        .expect("string binary executor should succeed");

        assert_eq!(result.get_bool(0), Some(true)); // aaa == aaa
        assert_eq!(result.get_bool(1), Some(false)); // bbb != xxx
        assert_eq!(result.get_bool(2), Some(true)); // ccc == ccc
    }

    #[test]
    fn test_varchar_select_via_executor() {
        // Test using BinaryExecutor::select_into directly for VARCHAR comparison
        let left = paro_common::test_utils::test_string_vector_with_allocator(
            &["a", "b", "c"],
            paro_common::test_utils::test_allocator(),
        );
        let right = paro_common::test_utils::test_string_vector_with_allocator(
            &["a", "b", "c"],
            paro_common::test_utils::test_allocator(),
        );
        let mut selection = paro_common::test_utils::test_selection_with_capacity(3);

        BinaryExecutor::select_into::<InlineString, InlineString, StringEqualsOp>(
            &left,
            &right,
            None,
            3,
            &mut selection,
        )
        .expect("select");

        // All strings are equal
        assert_eq!(selection.as_slice(), &[0, 1, 2]);
    }
}
