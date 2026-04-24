// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Unary Executor
//!
//! Optimized vectorized execution for unary operators.

use crate::scalar::executor::typed_loops::{
    execute_unary_flat, execute_unary_view, prepare_result,
};
use crate::scalar::executor::UnaryOperator;
use paro_common::error::Result;
use paro_common::vector::{Vector, VectorType};

pub struct UnaryExecutor;

impl UnaryExecutor {
    /// Execute a unary operator on a vector.
    pub fn execute<INPUT, RESULT, OP>(
        input: &Vector,
        result: &mut Vector,
        count: usize,
    ) -> Result<()>
    where
        INPUT: Copy + 'static,
        RESULT: Copy,
        OP: UnaryOperator<INPUT, RESULT>,
    {
        if count == 0 {
            prepare_result(result, VectorType::Flat, 0)?;
            return Ok(());
        }

        match input.vector_type() {
            VectorType::Constant => {
                Self::execute_constant::<INPUT, RESULT, OP>(input, result, count)
            }
            VectorType::Flat => Self::execute_flat::<INPUT, RESULT, OP>(input, result, count),
            VectorType::Dictionary | VectorType::Sequence => {
                Self::execute_view::<INPUT, RESULT, OP>(input, result, count)
            }
        }
    }

    fn execute_flat<INPUT, RESULT, OP>(
        input: &Vector,
        result: &mut Vector,
        count: usize,
    ) -> Result<()>
    where
        INPUT: Copy + 'static,
        RESULT: Copy,
        OP: UnaryOperator<INPUT, RESULT>,
    {
        execute_unary_flat::<INPUT, RESULT, OP>(input, result, count)
    }

    fn execute_constant<INPUT, RESULT, OP>(
        input: &Vector,
        result: &mut Vector,
        _count: usize,
    ) -> Result<()>
    where
        INPUT: Copy + 'static,
        RESULT: Copy,
        OP: UnaryOperator<INPUT, RESULT>,
    {
        prepare_result(result, VectorType::Constant, _count)?;
        if input.validity().is_valid(0) {
            let val = unsafe { *input.flat_data::<INPUT>() };
            unsafe {
                *result.flat_data_mut::<RESULT>() = OP::operation(val);
            }
        } else {
            result.validity_mut().set_null(0);
        }
        Ok(())
    }

    fn execute_view<INPUT, RESULT, OP>(
        input: &Vector,
        result: &mut Vector,
        count: usize,
    ) -> Result<()>
    where
        INPUT: Copy + 'static,
        RESULT: Copy,
        OP: UnaryOperator<INPUT, RESULT>,
    {
        execute_unary_view::<INPUT, RESULT, OP>(input, result, count)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use paro_common::types::LogicalType;

    struct AbsOp;
    impl UnaryOperator<i64, i64> for AbsOp {
        fn operation(input: i64) -> i64 {
            input.abs()
        }
    }

    #[test]
    fn test_execute_dictionary_input() {
        let child = paro_common::test_utils::test_i64_vector_with_allocator(
            &[-5, 0, 7],
            paro_common::test_utils::test_allocator(),
        );
        let dict = paro_common::test_utils::test_dictionary(Arc::new(child), vec![2, 0, 1]);
        let mut result = paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, 3);

        UnaryExecutor::execute::<i64, i64, AbsOp>(&dict, &mut result, 3)
            .expect("unary executor should succeed");

        assert_eq!(result.vector_type(), VectorType::Flat);
        assert_eq!(result.get_i64(0), Some(7));
        assert_eq!(result.get_i64(1), Some(5));
        assert_eq!(result.get_i64(2), Some(0));
    }

    #[test]
    fn test_execute_sequence_input() {
        let input = paro_common::test_utils::test_sequence_with_allocator(
            -3,
            2,
            4,
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, 4);

        UnaryExecutor::execute::<i64, i64, AbsOp>(&input, &mut result, 4)
            .expect("unary executor should succeed");

        assert_eq!(result.get_i64(0), Some(3));
        assert_eq!(result.get_i64(1), Some(1));
        assert_eq!(result.get_i64(2), Some(1));
        assert_eq!(result.get_i64(3), Some(3));
    }

    #[test]
    fn test_execute_dictionary_over_sequence_respects_nulls() {
        let mut sequence = paro_common::test_utils::test_sequence_with_allocator(
            -4,
            3,
            4,
            paro_common::test_utils::test_allocator(),
        );
        sequence.validity_mut().set_null(1);
        let dict = paro_common::test_utils::test_dictionary(Arc::new(sequence), vec![3_u32, 0, 1]);
        let mut result = paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, 3);

        UnaryExecutor::execute::<i64, i64, AbsOp>(&dict, &mut result, 3)
            .expect("unary executor should succeed");

        assert_eq!(result.get_i64(0), Some(5));
        assert_eq!(result.get_i64(1), Some(4));
        assert!(result.is_null(2));
    }
}
