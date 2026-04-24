// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Binary Executor
//!
//!
//!
//! ## Dependencies Check
//! - Vector: ✅
//! - DecodedVectorOwned: ✅
//!
//! ## Description
//! Optimized vectorized execution for binary operators.
//! Supports all vector types: Flat, Constant, Dictionary, Sequence.

use crate::scalar::executor::typed_loops::{
    execute_binary_view, prepare_result, select_binary_view_into,
};
use crate::scalar::executor::BinaryOperator;
use paro_common::error::Result;
use paro_common::vector::{SelectionVector, Vector, VectorType};

pub struct BinaryExecutor;

impl BinaryExecutor {
    /// Execute a binary operator on two vectors.
    ///
    /// Dispatches to optimized paths for common vector type combinations,
    /// falling back to generic execution for Dictionary/Sequence vectors.
    pub fn execute<LEFT, RIGHT, RESULT, OP>(
        left: &Vector,
        right: &Vector,
        result: &mut Vector,
        count: usize,
    ) -> Result<()>
    where
        LEFT: Copy + 'static,
        RIGHT: Copy + 'static,
        RESULT: Copy,
        OP: BinaryOperator<LEFT, RIGHT, RESULT>,
    {
        if count == 0 {
            prepare_result(result, VectorType::Flat, 0)?;
            return Ok(());
        }

        if left.vector_type() == VectorType::Constant && right.vector_type() == VectorType::Constant
        {
            Self::execute_constant_constant::<LEFT, RIGHT, RESULT, OP>(left, right, result, count)
        } else {
            Self::execute_view::<LEFT, RIGHT, RESULT, OP>(left, right, result, count)
        }
    }

    fn execute_constant_constant<LEFT, RIGHT, RESULT, OP>(
        left: &Vector,
        right: &Vector,
        result: &mut Vector,
        count: usize,
    ) -> Result<()>
    where
        LEFT: Copy + 'static,
        RIGHT: Copy + 'static,
        RESULT: Copy,
        OP: BinaryOperator<LEFT, RIGHT, RESULT>,
    {
        prepare_result(result, VectorType::Constant, count)?;
        if left.validity().is_valid(0) && right.validity().is_valid(0) {
            let l_val = unsafe { *left.flat_data::<LEFT>() };
            let r_val = unsafe { *right.flat_data::<RIGHT>() };
            unsafe {
                *result.flat_data_mut::<RESULT>() = OP::operation(l_val, r_val);
            }
        } else {
            result.validity_mut().set_null(0);
        }
        Ok(())
    }

    fn execute_view<LEFT, RIGHT, RESULT, OP>(
        left: &Vector,
        right: &Vector,
        result: &mut Vector,
        count: usize,
    ) -> Result<()>
    where
        LEFT: Copy + 'static,
        RIGHT: Copy + 'static,
        RESULT: Copy,
        OP: BinaryOperator<LEFT, RIGHT, RESULT>,
    {
        execute_binary_view::<LEFT, RIGHT, RESULT, OP>(left, right, result, count)
    }

    /// Select rows that satisfy the binary operator.
    ///
    /// Returns indices of rows where `left <op> right` evaluates to true.
    pub fn select_into<LEFT, RIGHT, OP>(
        left: &Vector,
        right: &Vector,
        input_sel: Option<&SelectionVector>,
        count: usize,
        selection: &mut SelectionVector,
    ) -> Result<usize>
    where
        LEFT: Copy + 'static,
        RIGHT: Copy + 'static,
        OP: BinaryOperator<LEFT, RIGHT, bool>,
    {
        select_binary_view_into::<LEFT, RIGHT, OP>(left, right, input_sel, count, selection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scalar::executor::BinaryOperator;
    use paro_common::types::LogicalType;
    use std::sync::Arc;

    // Test operators
    struct AddOp;
    impl BinaryOperator<i32, i32, i32> for AddOp {
        fn operation(left: i32, right: i32) -> i32 {
            left + right
        }
    }
    struct GreaterThanOp;
    impl BinaryOperator<i32, i32, bool> for GreaterThanOp {
        fn operation(left: i32, right: i32) -> bool {
            left > right
        }
    }
    struct AddOpI64;
    impl BinaryOperator<i64, i64, i64> for AddOpI64 {
        fn operation(left: i64, right: i64) -> i64 {
            left + right
        }
    }

    #[test]
    fn test_execute_flat_flat() {
        let left = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2, 3, 4],
            paro_common::test_utils::test_allocator(),
        );
        let right = paro_common::test_utils::test_i32_vector_with_allocator(
            &[10, 20, 30, 40],
            paro_common::test_utils::test_allocator(),
        );
        let mut result =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::Integer, 4);

        BinaryExecutor::execute::<i32, i32, i32, AddOp>(&left, &right, &mut result, 4)
            .expect("binary executor should succeed");

        assert_eq!(result.get_i32(0), Some(11));
        assert_eq!(result.get_i32(1), Some(22));
        assert_eq!(result.get_i32(2), Some(33));
        assert_eq!(result.get_i32(3), Some(44));
    }

    #[test]
    fn test_execute_flat_constant() {
        let left = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2, 3, 4],
            paro_common::test_utils::test_allocator(),
        );
        let right = paro_common::test_utils::test_constant_with_allocator(
            LogicalType::Integer,
            10i32,
            4,
            paro_common::test_utils::test_allocator(),
        );
        let mut result =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::Integer, 4);

        BinaryExecutor::execute::<i32, i32, i32, AddOp>(&left, &right, &mut result, 4)
            .expect("binary executor should succeed");

        assert_eq!(result.get_i32(0), Some(11));
        assert_eq!(result.get_i32(1), Some(12));
        assert_eq!(result.get_i32(2), Some(13));
        assert_eq!(result.get_i32(3), Some(14));
    }

    #[test]
    fn test_execute_dictionary_flat() {
        // Dictionary: indices [2, 0, 1, 2] into child [100, 200, 300]
        // Result should be child[2]+10, child[0]+10, child[1]+10, child[2]+10
        let child = paro_common::test_utils::test_i32_vector_with_allocator(
            &[100, 200, 300],
            paro_common::test_utils::test_allocator(),
        );
        let dict = paro_common::test_utils::test_dictionary(Arc::new(child), vec![2, 0, 1, 2]);
        let right = paro_common::test_utils::test_i32_vector_with_allocator(
            &[10, 20, 30, 40],
            paro_common::test_utils::test_allocator(),
        );
        let mut result =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::Integer, 4);

        BinaryExecutor::execute::<i32, i32, i32, AddOp>(&dict, &right, &mut result, 4)
            .expect("binary executor should succeed");

        assert_eq!(result.get_i32(0), Some(310)); // 300 + 10
        assert_eq!(result.get_i32(1), Some(120)); // 100 + 20
        assert_eq!(result.get_i32(2), Some(230)); // 200 + 30
        assert_eq!(result.get_i32(3), Some(340)); // 300 + 40
    }

    #[test]
    fn test_execute_flat_dictionary() {
        let left = paro_common::test_utils::test_i32_vector_with_allocator(
            &[10, 20, 30, 40],
            paro_common::test_utils::test_allocator(),
        );
        // Dictionary: indices [1, 0, 2, 1] into child [100, 200, 300]
        let child = paro_common::test_utils::test_i32_vector_with_allocator(
            &[100, 200, 300],
            paro_common::test_utils::test_allocator(),
        );
        let dict = paro_common::test_utils::test_dictionary(Arc::new(child), vec![1, 0, 2, 1]);
        let mut result =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::Integer, 4);

        BinaryExecutor::execute::<i32, i32, i32, AddOp>(&left, &dict, &mut result, 4)
            .expect("binary executor should succeed");

        assert_eq!(result.get_i32(0), Some(210)); // 10 + 200
        assert_eq!(result.get_i32(1), Some(120)); // 20 + 100
        assert_eq!(result.get_i32(2), Some(330)); // 30 + 300
        assert_eq!(result.get_i32(3), Some(240)); // 40 + 200
    }

    #[test]
    fn test_execute_dictionary_dictionary() {
        // Left dict: indices [0, 1] into [10, 20]
        let left_child = paro_common::test_utils::test_i32_vector_with_allocator(
            &[10, 20],
            paro_common::test_utils::test_allocator(),
        );
        let left = paro_common::test_utils::test_dictionary(Arc::new(left_child), vec![0, 1]);

        // Right dict: indices [1, 0] into [100, 200]
        let right_child = paro_common::test_utils::test_i32_vector_with_allocator(
            &[100, 200],
            paro_common::test_utils::test_allocator(),
        );
        let right = paro_common::test_utils::test_dictionary(Arc::new(right_child), vec![1, 0]);

        let mut result =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::Integer, 2);

        BinaryExecutor::execute::<i32, i32, i32, AddOp>(&left, &right, &mut result, 2)
            .expect("binary executor should succeed");

        assert_eq!(result.get_i32(0), Some(210)); // 10 + 200
        assert_eq!(result.get_i32(1), Some(120)); // 20 + 100
    }

    #[test]
    fn test_execute_dictionary_constant() {
        // Dictionary: indices [2, 0, 1] into child [100, 200, 300]
        let child = paro_common::test_utils::test_i32_vector_with_allocator(
            &[100, 200, 300],
            paro_common::test_utils::test_allocator(),
        );
        let dict = paro_common::test_utils::test_dictionary(Arc::new(child), vec![2, 0, 1]);
        let constant = paro_common::test_utils::test_constant_with_allocator(
            LogicalType::Integer,
            5i32,
            3,
            paro_common::test_utils::test_allocator(),
        );
        let mut result =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::Integer, 3);

        BinaryExecutor::execute::<i32, i32, i32, AddOp>(&dict, &constant, &mut result, 3)
            .expect("binary executor should succeed");

        assert_eq!(result.get_i32(0), Some(305)); // 300 + 5
        assert_eq!(result.get_i32(1), Some(105)); // 100 + 5
        assert_eq!(result.get_i32(2), Some(205)); // 200 + 5
    }

    #[test]
    fn test_execute_sequence_flat_fast_path() {
        let left = paro_common::test_utils::test_sequence_with_allocator(
            10,
            5,
            4,
            paro_common::test_utils::test_allocator(),
        );
        let right = paro_common::test_utils::test_i64_vector_with_allocator(
            &[1, 2, 3, 4],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, 4);

        BinaryExecutor::execute::<i64, i64, i64, AddOpI64>(&left, &right, &mut result, 4)
            .expect("binary executor should succeed");

        assert_eq!(result.get_i64(0), Some(11));
        assert_eq!(result.get_i64(1), Some(17));
        assert_eq!(result.get_i64(2), Some(23));
        assert_eq!(result.get_i64(3), Some(29));
    }

    #[test]
    fn test_execute_flat_sequence_fast_path() {
        let left = paro_common::test_utils::test_i64_vector_with_allocator(
            &[1, 2, 3, 4],
            paro_common::test_utils::test_allocator(),
        );
        let right = paro_common::test_utils::test_sequence_with_allocator(
            10,
            5,
            4,
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, 4);

        BinaryExecutor::execute::<i64, i64, i64, AddOpI64>(&left, &right, &mut result, 4)
            .expect("binary executor should succeed");

        assert_eq!(result.get_i64(0), Some(11));
        assert_eq!(result.get_i64(1), Some(17));
        assert_eq!(result.get_i64(2), Some(23));
        assert_eq!(result.get_i64(3), Some(29));
    }

    #[test]
    fn test_execute_sequence_sequence_fallback() {
        let left = paro_common::test_utils::test_sequence_with_allocator(
            1,
            2,
            4,
            paro_common::test_utils::test_allocator(),
        );
        let right = paro_common::test_utils::test_sequence_with_allocator(
            10,
            -1,
            4,
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, 4);

        BinaryExecutor::execute::<i64, i64, i64, AddOpI64>(&left, &right, &mut result, 4)
            .expect("binary executor should succeed");

        assert_eq!(result.get_i64(0), Some(11));
        assert_eq!(result.get_i64(1), Some(12));
        assert_eq!(result.get_i64(2), Some(13));
        assert_eq!(result.get_i64(3), Some(14));
    }

    #[test]
    fn test_execute_with_nulls() {
        let mut left = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2, 3],
            paro_common::test_utils::test_allocator(),
        );
        left.validity_mut().set_null(1);
        let right = paro_common::test_utils::test_i32_vector_with_allocator(
            &[10, 20, 30],
            paro_common::test_utils::test_allocator(),
        );
        let mut result =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::Integer, 3);

        BinaryExecutor::execute::<i32, i32, i32, AddOp>(&left, &right, &mut result, 3)
            .expect("binary executor should succeed");

        assert_eq!(result.get_i32(0), Some(11));
        assert!(result.is_null(1)); // null + 20 = null
        assert_eq!(result.get_i32(2), Some(33));
    }

    #[test]
    fn test_execute_reuse_clears_stale_nulls() {
        let mut left = paro_common::test_utils::test_i32_vector_with_allocator(
            &[1, 2, 3],
            paro_common::test_utils::test_allocator(),
        );
        left.validity_mut().set_null(1);
        let right = paro_common::test_utils::test_i32_vector_with_allocator(
            &[10, 20, 30],
            paro_common::test_utils::test_allocator(),
        );
        let mut result =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::Integer, 3);

        BinaryExecutor::execute::<i32, i32, i32, AddOp>(&left, &right, &mut result, 3)
            .expect("binary executor should succeed");
        assert!(result.is_null(1));

        let left = paro_common::test_utils::test_i32_vector_with_allocator(
            &[4, 5, 6],
            paro_common::test_utils::test_allocator(),
        );
        BinaryExecutor::execute::<i32, i32, i32, AddOp>(&left, &right, &mut result, 3)
            .expect("binary executor should succeed");

        assert_eq!(result.get_i32(0), Some(14));
        assert_eq!(result.get_i32(1), Some(25));
        assert_eq!(result.get_i32(2), Some(36));
        assert!(!result.is_null(1));
    }

    #[test]
    fn test_select_flat_flat() {
        let left = paro_common::test_utils::test_i32_vector_with_allocator(
            &[10, 20, 30, 40],
            paro_common::test_utils::test_allocator(),
        );
        let right = paro_common::test_utils::test_i32_vector_with_allocator(
            &[15, 15, 15, 15],
            paro_common::test_utils::test_allocator(),
        );
        let mut selection = paro_common::test_utils::test_selection_with_capacity(4);

        BinaryExecutor::select_into::<i32, i32, GreaterThanOp>(
            &left,
            &right,
            None,
            4,
            &mut selection,
        )
        .expect("select");

        assert_eq!(selection.as_slice(), &[1, 2, 3]); // 20, 30, 40 > 15
    }

    #[test]
    fn test_select_dictionary() {
        // Dictionary: indices [2, 0, 1, 2] into child [5, 15, 25]
        // Values: 25, 5, 15, 25
        let child = paro_common::test_utils::test_i32_vector_with_allocator(
            &[5, 15, 25],
            paro_common::test_utils::test_allocator(),
        );
        let dict = paro_common::test_utils::test_dictionary(Arc::new(child), vec![2, 0, 1, 2]);
        let right = paro_common::test_utils::test_i32_vector_with_allocator(
            &[10, 10, 10, 10],
            paro_common::test_utils::test_allocator(),
        );
        let mut selection = paro_common::test_utils::test_selection_with_capacity(4);

        BinaryExecutor::select_into::<i32, i32, GreaterThanOp>(
            &dict,
            &right,
            None,
            4,
            &mut selection,
        )
        .expect("select");

        assert_eq!(selection.as_slice(), &[0, 2, 3]); // 25 > 10, 15 > 10, 25 > 10
    }

    #[test]
    fn test_select_with_nulls() {
        let mut left = paro_common::test_utils::test_i32_vector_with_allocator(
            &[10, 20, 30],
            paro_common::test_utils::test_allocator(),
        );
        left.validity_mut().set_null(1);
        let right = paro_common::test_utils::test_i32_vector_with_allocator(
            &[5, 5, 5],
            paro_common::test_utils::test_allocator(),
        );
        let mut selection = paro_common::test_utils::test_selection_with_capacity(3);

        BinaryExecutor::select_into::<i32, i32, GreaterThanOp>(
            &left,
            &right,
            None,
            3,
            &mut selection,
        )
        .expect("select");

        assert_eq!(selection.as_slice(), &[0, 2]); // 10 > 5, null skipped, 30 > 5
    }
}
