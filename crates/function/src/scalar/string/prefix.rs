// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Prefix/Suffix Functions
//!
//! Prefix and suffix matching functions: `prefix`, `suffix`, `starts_with`, `ends_with`.
//!
//!

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::scalar::executor::varlen::execute_varchar_binary_to_bool;
use crate::{ExpressionState, ScalarFunction, ScalarFunctionSet};

/// Implementation of `prefix(VARCHAR, VARCHAR) -> BOOLEAN`.
/// Returns true if string starts with the given prefix.
fn prefix_varchar(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    let str_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing string column".to_string()))?;
    let prefix_vec = input
        .column(1)
        .ok_or_else(|| paro_common::error::internal("Missing prefix column".to_string()))?;
    execute_varchar_binary_to_bool(
        str_vec,
        prefix_vec,
        result,
        input.size(),
        |value, prefix| value.starts_with(prefix),
    )
}

/// Implementation of `suffix(VARCHAR, VARCHAR) -> BOOLEAN`.
/// Returns true if string ends with the given suffix.
fn suffix_varchar(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    let str_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing string column".to_string()))?;
    let suffix_vec = input
        .column(1)
        .ok_or_else(|| paro_common::error::internal("Missing suffix column".to_string()))?;
    execute_varchar_binary_to_bool(
        str_vec,
        suffix_vec,
        result,
        input.size(),
        |value, suffix| value.ends_with(suffix),
    )
}

/// Get `prefix` function set (alias: `starts_with`).
pub fn get_prefix_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("prefix".to_string());
    set.add_function(ScalarFunction::new(
        "prefix".to_string(),
        vec![LogicalType::Varchar, LogicalType::Varchar],
        LogicalType::Boolean,
        prefix_varchar,
    ));
    set
}

/// Get `suffix` function set (alias: `ends_with`).
pub fn get_suffix_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("suffix".to_string());
    set.add_function(ScalarFunction::new(
        "suffix".to_string(),
        vec![LogicalType::Varchar, LogicalType::Varchar],
        LogicalType::Boolean,
        suffix_varchar,
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
    fn test_prefix_basic() {
        let str_vec = Vector::from_strings(&["hello world", "foo bar", "test"]);
        let prefix_vec = Vector::from_strings(&["hello", "bar", ""]);
        let chunk = Chunk::from_vectors(vec![str_vec, prefix_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Boolean);

        prefix_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_bool(0), Some(true));
        assert_eq!(result.get_bool(1), Some(false));
        assert_eq!(result.get_bool(2), Some(true));
    }

    #[test]
    fn test_prefix_unicode() {
        let str_vec = Vector::from_strings(&["你好世界", "hello"]);
        let prefix_vec = Vector::from_strings(&["你好", "你好"]);
        let chunk = Chunk::from_vectors(vec![str_vec, prefix_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Boolean);

        prefix_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_bool(0), Some(true));
        assert_eq!(result.get_bool(1), Some(false));
    }

    #[test]
    fn test_prefix_with_null() {
        let mut str_vec = Vector::from_strings(&["hello", "world"]);
        str_vec.validity_mut().set_null(1);
        let prefix_vec = Vector::from_strings(&["hel", "wor"]);
        let chunk = Chunk::from_vectors(vec![str_vec, prefix_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Boolean);

        prefix_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_bool(0), Some(true));
        assert!(result.is_null(1));
    }

    #[test]
    fn test_suffix_basic() {
        let str_vec = Vector::from_strings(&["hello world", "foo bar", "test"]);
        let suffix_vec = Vector::from_strings(&["world", "foo", ""]);
        let chunk = Chunk::from_vectors(vec![str_vec, suffix_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Boolean);

        suffix_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_bool(0), Some(true));
        assert_eq!(result.get_bool(1), Some(false));
        assert_eq!(result.get_bool(2), Some(true));
    }

    #[test]
    fn test_suffix_unicode() {
        let str_vec = Vector::from_strings(&["你好世界", "hello"]);
        let suffix_vec = Vector::from_strings(&["世界", "世界"]);
        let chunk = Chunk::from_vectors(vec![str_vec, suffix_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Boolean);

        suffix_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_bool(0), Some(true));
        assert_eq!(result.get_bool(1), Some(false));
    }

    #[test]
    fn test_suffix_with_null() {
        let mut str_vec = Vector::from_strings(&["hello", "world"]);
        str_vec.validity_mut().set_null(1);
        let suffix_vec = Vector::from_strings(&["llo", "rld"]);
        let chunk = Chunk::from_vectors(vec![str_vec, suffix_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Boolean);

        suffix_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_bool(0), Some(true));
        assert!(result.is_null(1));
    }
}
