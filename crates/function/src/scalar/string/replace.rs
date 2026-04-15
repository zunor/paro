// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Replace Function
//!
//! String replacement function: `replace`.
//!
//!

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::scalar::executor::varlen::execute_varchar_ternary_to_varchar;
use crate::{ExpressionState, ScalarFunction, ScalarFunctionSet};

/// Implementation of `replace(VARCHAR, VARCHAR, VARCHAR) -> VARCHAR`.
/// Replaces all occurrences of `from` with `to` in the input string.
fn replace_varchar(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    let str_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing string column".to_string()))?;
    let from_vec = input
        .column(1)
        .ok_or_else(|| paro_common::error::internal("Missing from column".to_string()))?;
    let to_vec = input
        .column(2)
        .ok_or_else(|| paro_common::error::internal("Missing to column".to_string()))?;
    execute_varchar_ternary_to_varchar(
        str_vec,
        from_vec,
        to_vec,
        result,
        input.size(),
        |value, from, to, row, writer| {
            if from.is_empty() {
                writer.write_str(row, value);
            } else {
                writer.write_str(row, &value.replace(from, to));
            }
            Ok(())
        },
    )
}

/// Get `replace` function set.
pub fn get_replace_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("replace".to_string());
    set.add_function(ScalarFunction::new(
        "replace".to_string(),
        vec![
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
        ],
        LogicalType::Varchar,
        replace_varchar,
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
    fn test_replace_basic() {
        let str_vec = Vector::from_strings(&["hello world", "foo bar foo"]);
        let from_vec = Vector::from_strings(&["world", "foo"]);
        let to_vec = Vector::from_strings(&["rust", "baz"]);
        let chunk = Chunk::from_vectors(vec![str_vec, from_vec, to_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        replace_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hello rust"));
        assert_eq!(result.get_string(1), Some("baz bar baz"));
    }

    #[test]
    fn test_replace_not_found() {
        let str_vec = Vector::from_strings(&["hello"]);
        let from_vec = Vector::from_strings(&["xyz"]);
        let to_vec = Vector::from_strings(&["abc"]);
        let chunk = Chunk::from_vectors(vec![str_vec, from_vec, to_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        replace_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hello"));
    }

    #[test]
    fn test_replace_empty_from() {
        let str_vec = Vector::from_strings(&["hello"]);
        let from_vec = Vector::from_strings(&[""]);
        let to_vec = Vector::from_strings(&["x"]);
        let chunk = Chunk::from_vectors(vec![str_vec, from_vec, to_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        replace_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hello"));
    }

    #[test]
    fn test_replace_empty_to() {
        let str_vec = Vector::from_strings(&["hello world"]);
        let from_vec = Vector::from_strings(&[" world"]);
        let to_vec = Vector::from_strings(&[""]);
        let chunk = Chunk::from_vectors(vec![str_vec, from_vec, to_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        replace_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hello"));
    }

    #[test]
    fn test_replace_unicode() {
        let str_vec = Vector::from_strings(&["你好世界"]);
        let from_vec = Vector::from_strings(&["世界"]);
        let to_vec = Vector::from_strings(&["Rust"]);
        let chunk = Chunk::from_vectors(vec![str_vec, from_vec, to_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        replace_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("你好Rust"));
    }

    #[test]
    fn test_replace_with_null() {
        let mut str_vec = Vector::from_strings(&["hello", "world"]);
        str_vec.validity_mut().set_null(1);
        let from_vec = Vector::from_strings(&["ell", "orl"]);
        let to_vec = Vector::from_strings(&["ELL", "ORL"]);
        let chunk = Chunk::from_vectors(vec![str_vec, from_vec, to_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        replace_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hELLo"));
        assert!(result.is_null(1));
    }
}
