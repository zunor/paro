// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Case expression implementation.
//!
//!

use crate::scalar::executor::TernaryOperator;
use crate::scalar::{ScalarFunction, ScalarFunctionSet};
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

pub struct CaseOperator;
impl<T: Copy> TernaryOperator<bool, T, T, T> for CaseOperator {
    #[inline]
    fn operation(check: bool, result_if_true: T, result_if_false: T) -> T {
        if check {
            result_if_true
        } else {
            result_if_false
        }
    }
}

pub struct CaseExecutor;

impl CaseExecutor {
    /// Execute a CASE expression with short-circuiting logic.
    pub fn execute(
        check: &Vector,
        result_if_true: &Vector,
        result_if_false: &Vector,
        count: usize,
    ) -> Result<Vector> {
        let return_type = result_if_true.logical_type().clone();

        let mut mask = Vec::with_capacity(count);
        for i in 0..count {
            let is_true = match check.get_bool(i) {
                Some(true) => true,
                _ => false,
            };
            mask.push(is_true);
        }

        Vector::merge_full(return_type, count, &mask, result_if_true, result_if_false)
    }
}

pub fn register_case_functions(set: &mut ScalarFunctionSet) {
    if set.name == "if" {
        // INTEGER
        set.add_function(ScalarFunction::new(
            "if".to_string(),
            vec![
                LogicalType::Boolean,
                LogicalType::Integer,
                LogicalType::Integer,
            ],
            LogicalType::Integer,
            |chunk, _state, result| {
                let res = CaseExecutor::execute(
                    &chunk.data[0],
                    &chunk.data[1],
                    &chunk.data[2],
                    chunk.size(),
                )?;
                *result = res;
                Ok(())
            },
        ));
        // Add more types here as needed (BigInt, Double, etc.)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ExpressionState;
    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;

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
    fn test_if_function() {
        let mut set = ScalarFunctionSet::new("if".to_string());
        register_case_functions(&mut set);

        let chunk = Chunk::from_vectors(
            vec![
                paro_common::test_utils::test_nullable_bool_vector(&[
                    Some(true),
                    Some(false),
                    None,
                ]),
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[1, 2, 3],
                    paro_common::test_utils::test_allocator(),
                ),
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[10, 20, 30],
                    paro_common::test_utils::test_allocator(),
                ),
            ],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::Integer);

        let func = set.functions[0].clone();
        func.dispatch
            .execute(&chunk, &MockState, &mut result)
            .unwrap();

        assert_eq!(result.get_i32(0), Some(1)); // T -> 1
        assert_eq!(result.get_i32(1), Some(20)); // F -> 20
        assert_eq!(result.get_i32(2), Some(30)); // N -> 30
    }
}
