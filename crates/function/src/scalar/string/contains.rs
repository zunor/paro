// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # String Search Functions
//!
//! String search functions: `contains`, `position`, `instr`.
//!
//!

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::scalar::executor::varlen::{
    execute_varchar_binary_to_bool, execute_varchar_binary_to_i64,
};
use crate::{ExpressionState, ScalarFunction, ScalarFunctionSet};

/// Implementation of `contains(VARCHAR, VARCHAR) -> BOOLEAN`.
/// Returns true if haystack contains needle.
fn contains_varchar(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let haystack_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing haystack column".to_string()))?;
    let needle_vec = input
        .column(1)
        .ok_or_else(|| paro_common::error::internal("Missing needle column".to_string()))?;
    execute_varchar_binary_to_bool(
        haystack_vec,
        needle_vec,
        result,
        input.size(),
        |haystack, needle| haystack.contains(needle),
    )
}

/// Implementation of `position(VARCHAR IN VARCHAR) -> BIGINT`.
/// Returns 1-indexed position of needle in haystack, or 0 if not found.
/// Note: SQL syntax is `position(needle IN haystack)`, so args are (needle, haystack).
fn position_varchar(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let needle_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing needle column".to_string()))?;
    let haystack_vec = input
        .column(1)
        .ok_or_else(|| paro_common::error::internal("Missing haystack column".to_string()))?;
    execute_varchar_binary_to_i64(
        needle_vec,
        haystack_vec,
        result,
        input.size(),
        |needle, haystack| {
            if needle.is_empty() {
                1
            } else {
                find_position_unicode(haystack, needle)
            }
        },
    )
}

/// Implementation of `instr(VARCHAR, VARCHAR) -> BIGINT`.
/// Same as position but with (haystack, needle) argument order.
fn instr_varchar(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    let haystack_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing haystack column".to_string()))?;
    let needle_vec = input
        .column(1)
        .ok_or_else(|| paro_common::error::internal("Missing needle column".to_string()))?;
    execute_varchar_binary_to_i64(
        haystack_vec,
        needle_vec,
        result,
        input.size(),
        |haystack, needle| {
            if needle.is_empty() {
                1
            } else {
                find_position_unicode(haystack, needle)
            }
        },
    )
}

/// Find position of needle in haystack using Unicode codepoints.
/// Returns 1-indexed position, or 0 if not found.
fn find_position_unicode(haystack: &str, needle: &str) -> i64 {
    // First try byte-level search for efficiency
    if let Some(byte_pos) = haystack.find(needle) {
        // Convert byte position to character position
        let char_pos = haystack[..byte_pos].chars().count();
        return (char_pos + 1) as i64; // 1-indexed
    }
    0 // Not found
}

/// Get `contains` function set.
pub fn get_contains_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("contains".to_string());
    set.add_function(ScalarFunction::new(
        "contains".to_string(),
        vec![LogicalType::Varchar, LogicalType::Varchar],
        LogicalType::Boolean,
        contains_varchar,
    ));
    set
}

/// Get `position` function set.
pub fn get_position_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("position".to_string());
    // position(needle IN haystack) -> (needle, haystack)
    set.add_function(ScalarFunction::new(
        "position".to_string(),
        vec![LogicalType::Varchar, LogicalType::Varchar],
        LogicalType::BigInt,
        position_varchar,
    ));
    set
}

/// Get `instr` function set.
pub fn get_instr_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("instr".to_string());
    // instr(haystack, needle)
    set.add_function(ScalarFunction::new(
        "instr".to_string(),
        vec![LogicalType::Varchar, LogicalType::Varchar],
        LogicalType::BigInt,
        instr_varchar,
    ));
    set
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;

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
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[test]
    fn test_contains_basic() {
        let haystack = Vector::from_strings(&["hello world", "foo bar", "test"]);
        let needle = Vector::from_strings(&["world", "baz", ""]);
        let chunk = Chunk::from_vectors(vec![haystack, needle]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Boolean);

        contains_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_bool(0), Some(true));
        assert_eq!(result.get_bool(1), Some(false));
        assert_eq!(result.get_bool(2), Some(true));
    }

    #[test]
    fn test_contains_unicode() {
        let haystack = Vector::from_strings(&["你好世界", "hello"]);
        let needle = Vector::from_strings(&["世界", "世界"]);
        let chunk = Chunk::from_vectors(vec![haystack, needle]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Boolean);

        contains_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_bool(0), Some(true));
        assert_eq!(result.get_bool(1), Some(false));
    }

    #[test]
    fn test_contains_with_null() {
        let mut haystack = Vector::from_strings(&["hello", "world"]);
        haystack.validity_mut().set_null(1);
        let needle = Vector::from_strings(&["ell", "orl"]);
        let chunk = Chunk::from_vectors(vec![haystack, needle]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Boolean);

        contains_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_bool(0), Some(true));
        assert!(result.is_null(1));
    }

    #[test]
    fn test_position_basic() {
        let needle = Vector::from_strings(&["world", "baz", ""]);
        let haystack = Vector::from_strings(&["hello world", "foo bar", "test"]);
        let chunk = Chunk::from_vectors(vec![needle, haystack]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::BigInt);

        position_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_i64(0), Some(7));
        assert_eq!(result.get_i64(1), Some(0));
        assert_eq!(result.get_i64(2), Some(1));
    }

    #[test]
    fn test_position_unicode() {
        let needle = Vector::from_strings(&["世界"]);
        let haystack = Vector::from_strings(&["你好世界"]);
        let chunk = Chunk::from_vectors(vec![needle, haystack]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::BigInt);

        position_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_i64(0), Some(3));
    }

    #[test]
    fn test_instr_basic() {
        let haystack = Vector::from_strings(&["hello world", "foo bar"]);
        let needle = Vector::from_strings(&["world", "baz"]);
        let chunk = Chunk::from_vectors(vec![haystack, needle]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::BigInt);

        instr_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_i64(0), Some(7));
        assert_eq!(result.get_i64(1), Some(0));
    }

    #[test]
    fn test_find_position_unicode() {
        assert_eq!(find_position_unicode("hello", "ell"), 2);
        assert_eq!(find_position_unicode("hello", "xyz"), 0);
        assert_eq!(find_position_unicode("你好世界", "世界"), 3);
        assert_eq!(find_position_unicode("hello世界", "世界"), 6);
    }
}
