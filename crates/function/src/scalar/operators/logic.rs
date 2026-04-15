// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical operators implementation.
//!
//!

use crate::scalar::executor::{unary::UnaryExecutor, UnaryOperator};
use crate::scalar::{ScalarFunction, ScalarFunctionSet};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

// --- Operators ---

pub struct NotOperator;
impl UnaryOperator<bool, bool> for NotOperator {
    #[inline]
    fn operation(input: bool) -> bool {
        !input
    }
}

// --- Logic Executor ---
// Specialized for 3-valued logic (3VL)

pub struct LogicExecutor;

impl LogicExecutor {
    pub fn execute_and(left: &Vector, right: &Vector, result: &mut Vector, count: usize) {
        result.set_count(count);

        for i in 0..count {
            let l_null = left.is_null(i);
            let r_null = right.is_null(i);

            let l_val = if !l_null {
                left.get_bool(i).unwrap_or(false)
            } else {
                false
            };
            let r_val = if !r_null {
                right.get_bool(i).unwrap_or(false)
            } else {
                false
            };

            if !l_null && !r_null {
                // T AND T = T, T AND F = F, F AND T = F, F AND F = F
                result.set_bool(i, l_val && r_val);
            } else if (!l_null && !l_val) || (!r_null && !r_val) {
                // F AND NULL = F, NULL AND F = F
                result.set_bool(i, false);
            } else {
                // T AND NULL = NULL, NULL AND T = NULL, NULL AND NULL = NULL
                result.set_null(i, true);
            }
        }
    }

    pub fn execute_or(left: &Vector, right: &Vector, result: &mut Vector, count: usize) {
        result.set_count(count);

        for i in 0..count {
            let l_null = left.is_null(i);
            let r_null = right.is_null(i);

            let l_val = if !l_null {
                left.get_bool(i).unwrap_or(false)
            } else {
                false
            };
            let r_val = if !r_null {
                right.get_bool(i).unwrap_or(false)
            } else {
                false
            };

            if !l_null && !r_null {
                // T OR T = T, T OR F = T, F OR T = T, F OR F = F
                result.set_bool(i, l_val || r_val);
            } else if (!l_null && l_val) || (!r_null && r_val) {
                // T OR NULL = T, NULL OR T = T
                result.set_bool(i, true);
            } else {
                // F OR NULL = NULL, NULL OR F = NULL, NULL OR NULL = NULL
                result.set_null(i, true);
            }
        }
    }
}

// --- Function Registration ---

pub fn register_logic_functions(set: &mut ScalarFunctionSet) {
    match set.name.as_str() {
        "not" | "!" => {
            set.add_function(ScalarFunction::new(
                set.name.clone(),
                vec![LogicalType::Boolean],
                LogicalType::Boolean,
                |chunk, _state, result| {
                    UnaryExecutor::execute::<bool, bool, NotOperator>(
                        &chunk.data[0],
                        result,
                        chunk.size(),
                    );
                    Ok(())
                },
            ));
        }
        "and" | "&" => {
            set.add_function(ScalarFunction::new(
                set.name.clone(),
                vec![LogicalType::Boolean, LogicalType::Boolean],
                LogicalType::Boolean,
                |chunk, _state, result| {
                    LogicExecutor::execute_and(
                        &chunk.data[0],
                        &chunk.data[1],
                        result,
                        chunk.size(),
                    );
                    Ok(())
                },
            ));
        }
        "or" | "|" => {
            set.add_function(ScalarFunction::new(
                set.name.clone(),
                vec![LogicalType::Boolean, LogicalType::Boolean],
                LogicalType::Boolean,
                |chunk, _state, result| {
                    LogicExecutor::execute_or(&chunk.data[0], &chunk.data[1], result, chunk.size());
                    Ok(())
                },
            ));
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;

    #[test]
    fn test_logic_and_3vl() {
        let left = Vector::from_nullable_bools(&[
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            None,
            None,
            None,
        ]);
        let right = Vector::from_nullable_bools(&[
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ]);
        let mut result = Vector::new(LogicalType::Boolean);

        LogicExecutor::execute_and(&left, &right, &mut result, 9);

        // T AND T = T, T AND F = F, T AND N = N
        assert_eq!(result.get_bool(0), Some(true));
        assert_eq!(result.get_bool(1), Some(false));
        assert_eq!(result.get_bool(2), None);

        // F AND T = F, F AND F = F, F AND N = F
        assert_eq!(result.get_bool(3), Some(false));
        assert_eq!(result.get_bool(4), Some(false));
        assert_eq!(result.get_bool(5), Some(false));

        // N AND T = N, N AND F = F, N AND N = N
        assert_eq!(result.get_bool(6), None);
        assert_eq!(result.get_bool(7), Some(false));
        assert_eq!(result.get_bool(8), None);
    }

    #[test]
    fn test_logic_or_3vl() {
        let left = Vector::from_nullable_bools(&[
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            None,
            None,
            None,
        ]);
        let right = Vector::from_nullable_bools(&[
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ]);
        let mut result = Vector::new(LogicalType::Boolean);

        LogicExecutor::execute_or(&left, &right, &mut result, 9);

        // T OR T = T, T OR F = T, T OR N = T
        assert_eq!(result.get_bool(0), Some(true));
        assert_eq!(result.get_bool(1), Some(true));
        assert_eq!(result.get_bool(2), Some(true));

        // F OR T = T, F OR F = F, F OR N = N
        assert_eq!(result.get_bool(3), Some(true));
        assert_eq!(result.get_bool(4), Some(false));
        assert_eq!(result.get_bool(5), None);

        // N OR T = T, N OR F = N, N OR N = N
        assert_eq!(result.get_bool(6), Some(true));
        assert_eq!(result.get_bool(7), None);
        assert_eq!(result.get_bool(8), None);
    }
}
