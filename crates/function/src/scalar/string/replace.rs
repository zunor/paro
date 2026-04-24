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
        let str_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["hello world", "foo bar foo"],
            paro_common::test_utils::test_allocator(),
        );
        let from_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["world", "foo"],
            paro_common::test_utils::test_allocator(),
        );
        let to_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["rust", "baz"],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = Chunk::from_vectors(
            vec![str_vec, from_vec, to_vec],
            paro_common::test_utils::test_allocator(),
        );
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        replace_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hello rust"));
        assert_eq!(result.get_string(1), Some("baz bar baz"));
    }

    #[test]
    fn test_replace_not_found() {
        let str_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["hello"],
            paro_common::test_utils::test_allocator(),
        );
        let from_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["xyz"],
            paro_common::test_utils::test_allocator(),
        );
        let to_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["abc"],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = Chunk::from_vectors(
            vec![str_vec, from_vec, to_vec],
            paro_common::test_utils::test_allocator(),
        );
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        replace_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hello"));
    }

    #[test]
    fn test_replace_empty_from() {
        let str_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["hello"],
            paro_common::test_utils::test_allocator(),
        );
        let from_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &[""],
            paro_common::test_utils::test_allocator(),
        );
        let to_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["x"],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = Chunk::from_vectors(
            vec![str_vec, from_vec, to_vec],
            paro_common::test_utils::test_allocator(),
        );
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        replace_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hello"));
    }

    #[test]
    fn test_replace_empty_to() {
        let str_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["hello world"],
            paro_common::test_utils::test_allocator(),
        );
        let from_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &[" world"],
            paro_common::test_utils::test_allocator(),
        );
        let to_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &[""],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = Chunk::from_vectors(
            vec![str_vec, from_vec, to_vec],
            paro_common::test_utils::test_allocator(),
        );
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        replace_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hello"));
    }

    #[test]
    fn test_replace_unicode() {
        let str_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["你好世界"],
            paro_common::test_utils::test_allocator(),
        );
        let from_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["世界"],
            paro_common::test_utils::test_allocator(),
        );
        let to_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["Rust"],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = Chunk::from_vectors(
            vec![str_vec, from_vec, to_vec],
            paro_common::test_utils::test_allocator(),
        );
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        replace_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("你好Rust"));
    }

    #[test]
    fn test_replace_with_null() {
        let mut str_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["hello", "world"],
            paro_common::test_utils::test_allocator(),
        );
        str_vec.validity_mut().set_null(1);
        let from_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["ell", "orl"],
            paro_common::test_utils::test_allocator(),
        );
        let to_vec = paro_common::test_utils::test_string_vector_with_allocator(
            &["ELL", "ORL"],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = Chunk::from_vectors(
            vec![str_vec, from_vec, to_vec],
            paro_common::test_utils::test_allocator(),
        );
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        replace_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hELLo"));
        assert!(result.is_null(1));
    }
}
