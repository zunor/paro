// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Trim Functions
//!
//! Whitespace trimming functions: `trim`, `ltrim`, `rtrim`.
//!
//!

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::scalar::executor::varlen::{
    execute_varchar_binary_to_varchar, execute_varchar_unary_to_varchar,
};
use crate::{ExpressionState, ScalarFunction, ScalarFunctionSet};

/// Implementation of `trim(VARCHAR) -> VARCHAR`.
/// Removes leading and trailing whitespace.
fn trim_varchar(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    let input_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing input column".to_string()))?;
    execute_varchar_unary_to_varchar(input_vec, result, input.size(), |value, row, writer| {
        writer.write_str(row, value.trim());
        Ok(())
    })
}

/// Implementation of `trim(VARCHAR, VARCHAR) -> VARCHAR`.
/// Removes specified characters from both ends.
fn trim_chars_varchar(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let str_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing string column".to_string()))?;
    let chars_vec = input
        .column(1)
        .ok_or_else(|| paro_common::error::internal("Missing chars column".to_string()))?;
    execute_varchar_binary_to_varchar(
        str_vec,
        chars_vec,
        result,
        input.size(),
        |value, chars, row, writer| {
            let char_set: Vec<char> = chars.chars().collect();
            writer.write_str(row, value.trim_matches(|c| char_set.contains(&c)));
            Ok(())
        },
    )
}

/// Implementation of `ltrim(VARCHAR) -> VARCHAR`.
/// Removes leading whitespace.
fn ltrim_varchar(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    let input_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing input column".to_string()))?;
    execute_varchar_unary_to_varchar(input_vec, result, input.size(), |value, row, writer| {
        writer.write_str(row, value.trim_start());
        Ok(())
    })
}

/// Implementation of `ltrim(VARCHAR, VARCHAR) -> VARCHAR`.
/// Removes specified characters from the start.
fn ltrim_chars_varchar(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let str_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing string column".to_string()))?;
    let chars_vec = input
        .column(1)
        .ok_or_else(|| paro_common::error::internal("Missing chars column".to_string()))?;
    execute_varchar_binary_to_varchar(
        str_vec,
        chars_vec,
        result,
        input.size(),
        |value, chars, row, writer| {
            let char_set: Vec<char> = chars.chars().collect();
            writer.write_str(row, value.trim_start_matches(|c| char_set.contains(&c)));
            Ok(())
        },
    )
}

/// Implementation of `rtrim(VARCHAR) -> VARCHAR`.
/// Removes trailing whitespace.
fn rtrim_varchar(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    let input_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing input column".to_string()))?;
    execute_varchar_unary_to_varchar(input_vec, result, input.size(), |value, row, writer| {
        writer.write_str(row, value.trim_end());
        Ok(())
    })
}

/// Implementation of `rtrim(VARCHAR, VARCHAR) -> VARCHAR`.
/// Removes specified characters from the end.
fn rtrim_chars_varchar(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let str_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing string column".to_string()))?;
    let chars_vec = input
        .column(1)
        .ok_or_else(|| paro_common::error::internal("Missing chars column".to_string()))?;
    execute_varchar_binary_to_varchar(
        str_vec,
        chars_vec,
        result,
        input.size(),
        |value, chars, row, writer| {
            let char_set: Vec<char> = chars.chars().collect();
            writer.write_str(row, value.trim_end_matches(|c| char_set.contains(&c)));
            Ok(())
        },
    )
}

/// Get `trim` function set.
pub fn get_trim_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("trim".to_string());

    // trim(VARCHAR) -> VARCHAR
    set.add_function(ScalarFunction::new(
        "trim".to_string(),
        vec![LogicalType::Varchar],
        LogicalType::Varchar,
        trim_varchar,
    ));

    // trim(VARCHAR, VARCHAR) -> VARCHAR
    set.add_function(ScalarFunction::new(
        "trim".to_string(),
        vec![LogicalType::Varchar, LogicalType::Varchar],
        LogicalType::Varchar,
        trim_chars_varchar,
    ));

    set
}

/// Get `ltrim` function set.
pub fn get_ltrim_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("ltrim".to_string());

    // ltrim(VARCHAR) -> VARCHAR
    set.add_function(ScalarFunction::new(
        "ltrim".to_string(),
        vec![LogicalType::Varchar],
        LogicalType::Varchar,
        ltrim_varchar,
    ));

    // ltrim(VARCHAR, VARCHAR) -> VARCHAR
    set.add_function(ScalarFunction::new(
        "ltrim".to_string(),
        vec![LogicalType::Varchar, LogicalType::Varchar],
        LogicalType::Varchar,
        ltrim_chars_varchar,
    ));

    set
}

/// Get `rtrim` function set.
pub fn get_rtrim_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("rtrim".to_string());

    // rtrim(VARCHAR) -> VARCHAR
    set.add_function(ScalarFunction::new(
        "rtrim".to_string(),
        vec![LogicalType::Varchar],
        LogicalType::Varchar,
        rtrim_varchar,
    ));

    // rtrim(VARCHAR, VARCHAR) -> VARCHAR
    set.add_function(ScalarFunction::new(
        "rtrim".to_string(),
        vec![LogicalType::Varchar, LogicalType::Varchar],
        LogicalType::Varchar,
        rtrim_chars_varchar,
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
    fn test_trim_whitespace() {
        let input_vec = Vector::from_strings(&["  hello  ", "\tworld\n", "  "]);
        let chunk = Chunk::from_vectors(vec![input_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        trim_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hello"));
        assert_eq!(result.get_string(1), Some("world"));
        assert_eq!(result.get_string(2), Some(""));
    }

    #[test]
    fn test_trim_chars() {
        let str_vec = Vector::from_strings(&["xxhelloxx", "abcHIabc"]);
        let chars_vec = Vector::from_strings(&["x", "abc"]);
        let chunk = Chunk::from_vectors(vec![str_vec, chars_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        trim_chars_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hello"));
        assert_eq!(result.get_string(1), Some("HI"));
    }

    #[test]
    fn test_ltrim_whitespace() {
        let input_vec = Vector::from_strings(&["  hello  ", "\tworld"]);
        let chunk = Chunk::from_vectors(vec![input_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        ltrim_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hello  "));
        assert_eq!(result.get_string(1), Some("world"));
    }

    #[test]
    fn test_ltrim_chars() {
        let str_vec = Vector::from_strings(&["xxhelloxx"]);
        let chars_vec = Vector::from_strings(&["x"]);
        let chunk = Chunk::from_vectors(vec![str_vec, chars_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        ltrim_chars_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("helloxx"));
    }

    #[test]
    fn test_rtrim_whitespace() {
        let input_vec = Vector::from_strings(&["  hello  ", "world\t"]);
        let chunk = Chunk::from_vectors(vec![input_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        rtrim_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("  hello"));
        assert_eq!(result.get_string(1), Some("world"));
    }

    #[test]
    fn test_rtrim_chars() {
        let str_vec = Vector::from_strings(&["xxhelloxx"]);
        let chars_vec = Vector::from_strings(&["x"]);
        let chunk = Chunk::from_vectors(vec![str_vec, chars_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        rtrim_chars_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("xxhello"));
    }

    #[test]
    fn test_trim_with_null() {
        let mut input_vec = Vector::from_strings(&["  hello  ", "world"]);
        input_vec.validity_mut().set_null(1);
        let chunk = Chunk::from_vectors(vec![input_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        trim_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hello"));
        assert!(result.is_null(1));
    }

    #[test]
    fn test_trim_unicode() {
        let input_vec = Vector::from_strings(&["  你好  ", "　世界　"]);
        let chunk = Chunk::from_vectors(vec![input_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        trim_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("你好"));
        assert_eq!(result.get_string(1), Some("世界"));
    }
}
