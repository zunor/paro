// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Ternary Executor
//!
//! Optimized vectorized execution for ternary operators (e.g., BETWEEN).

use crate::scalar::executor::typed_loops::{execute_ternary_view, prepare_result};
use crate::scalar::executor::TernaryOperator;
use paro_common::error::Result;
use paro_common::vector::{Vector, VectorType};

pub struct TernaryExecutor;

impl TernaryExecutor {
    /// Execute a ternary operator on three vectors.
    /// Simplified for MVP: focus on Flat vectors.
    pub fn execute<A, B, C, RESULT, OP>(
        a_vec: &Vector,
        b_vec: &Vector,
        c_vec: &Vector,
        result: &mut Vector,
        count: usize,
    ) -> Result<()>
    where
        A: Copy + 'static,
        B: Copy + 'static,
        C: Copy + 'static,
        RESULT: Copy,
        OP: TernaryOperator<A, B, C, RESULT>,
    {
        if count == 0 {
            prepare_result(result, VectorType::Flat, 0)?;
            return Ok(());
        }

        if a_vec.vector_type() == VectorType::Constant
            && b_vec.vector_type() == VectorType::Constant
            && c_vec.vector_type() == VectorType::Constant
        {
            Self::execute_constant::<A, B, C, RESULT, OP>(a_vec, b_vec, c_vec, result, count)
        } else {
            Self::execute_view::<A, B, C, RESULT, OP>(a_vec, b_vec, c_vec, result, count)
        }
    }

    fn execute_constant<A, B, C, RESULT, OP>(
        a_vec: &Vector,
        b_vec: &Vector,
        c_vec: &Vector,
        result: &mut Vector,
        count: usize,
    ) -> Result<()>
    where
        A: Copy + 'static,
        B: Copy + 'static,
        C: Copy + 'static,
        RESULT: Copy,
        OP: TernaryOperator<A, B, C, RESULT>,
    {
        prepare_result(result, VectorType::Constant, count)?;
        if a_vec.validity().is_valid(0)
            && b_vec.validity().is_valid(0)
            && c_vec.validity().is_valid(0)
        {
            let a_val = unsafe { *a_vec.flat_data::<A>() };
            let b_val = unsafe { *b_vec.flat_data::<B>() };
            let c_val = unsafe { *c_vec.flat_data::<C>() };
            unsafe {
                *result.flat_data_mut::<RESULT>() = OP::operation(a_val, b_val, c_val);
            }
        } else {
            result.validity_mut().set_null(0);
        }
        Ok(())
    }

    fn execute_view<A, B, C, RESULT, OP>(
        a: &Vector,
        b: &Vector,
        c: &Vector,
        result: &mut Vector,
        count: usize,
    ) -> Result<()>
    where
        A: Copy + 'static,
        B: Copy + 'static,
        C: Copy + 'static,
        RESULT: Copy,
        OP: TernaryOperator<A, B, C, RESULT>,
    {
        execute_ternary_view::<A, B, C, RESULT, OP>(a, b, c, result, count)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use paro_common::types::LogicalType;

    struct BetweenOp;
    impl TernaryOperator<i64, i64, i64, bool> for BetweenOp {
        fn operation(value: i64, low: i64, high: i64) -> bool {
            value >= low && value <= high
        }
    }

    #[test]
    fn test_execute_mixed_constant_dictionary() {
        let values = paro_common::test_utils::test_dictionary(
            Arc::new(paro_common::test_utils::test_i64_vector_with_allocator(
                &[5, 15, 25],
                paro_common::test_utils::test_allocator(),
            )),
            vec![0, 2, 1],
        );
        let low = paro_common::test_utils::test_constant_with_allocator(
            LogicalType::BigInt,
            10_i64,
            3,
            paro_common::test_utils::test_allocator(),
        );
        let high = paro_common::test_utils::test_constant_with_allocator(
            LogicalType::BigInt,
            20_i64,
            3,
            paro_common::test_utils::test_allocator(),
        );
        let mut result =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::Boolean, 3);

        TernaryExecutor::execute::<i64, i64, i64, bool, BetweenOp>(
            &values,
            &low,
            &high,
            &mut result,
            3,
        )
        .expect("ternary executor should succeed");

        assert_eq!(result.vector_type(), VectorType::Flat);
        assert_eq!(result.get_bool(0), Some(false));
        assert_eq!(result.get_bool(1), Some(false));
        assert_eq!(result.get_bool(2), Some(true));
    }

    #[test]
    fn test_execute_sequence_fallback() {
        let values = paro_common::test_utils::test_sequence_with_allocator(
            5,
            5,
            4,
            paro_common::test_utils::test_allocator(),
        );
        let low = paro_common::test_utils::test_i64_vector_with_allocator(
            &[0, 8, 10, 18],
            paro_common::test_utils::test_allocator(),
        );
        let high = paro_common::test_utils::test_constant_with_allocator(
            LogicalType::BigInt,
            15_i64,
            4,
            paro_common::test_utils::test_allocator(),
        );
        let mut result =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::Boolean, 4);

        TernaryExecutor::execute::<i64, i64, i64, bool, BetweenOp>(
            &values,
            &low,
            &high,
            &mut result,
            4,
        )
        .expect("ternary executor should succeed");

        assert_eq!(result.get_bool(0), Some(true));
        assert_eq!(result.get_bool(1), Some(true));
        assert_eq!(result.get_bool(2), Some(true));
        assert_eq!(result.get_bool(3), Some(false));
    }
}
