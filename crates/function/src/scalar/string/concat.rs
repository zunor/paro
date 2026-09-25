// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Concatenation Functions
//!
//! String concatenation functions: `concat`, `concat_ws`.
//!
//!
//!
//! ## Behavior
//! - `concat(...)`: Concatenates all arguments, treating NULL as empty string
//! - `concat_ws(sep,...)`: Concatenates with separator, skipping NULL values

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::scalar::executor::variadic::{execute_concat, execute_concat_ws};
use crate::scalar::executor::varlen::VarcharResultWriter;
use crate::{ExpressionState, FunctionNullHandling, ScalarFunction, ScalarFunctionSet};

/// Implementation of `concat(VARCHAR...) -> VARCHAR`.
/// NULL values are treated as empty strings.
fn concat_varchar(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    execute_concat(input, result)
}

/// Binary SQL `||` implementation. Its default null handling deliberately
/// differs from `concat(...)`: the operator is strict, while the variadic
/// function treats NULL as an empty string.
fn string_concat_varchar(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let count = input.size();
    let left = input
        .column(0)
        .expect("bound string_concat has a left operand")
        .try_to_utf8_view(count)?;
    let right = input
        .column(1)
        .expect("bound string_concat has a right operand")
        .try_to_utf8_view(count)?;
    let mut writer = VarcharResultWriter::try_new(result, count)?;

    for row in 0..count {
        if !left.is_valid(row) || !right.is_valid(row) {
            writer.set_null(row);
            continue;
        }
        let left = left.str(row);
        let right = right.str(row);
        let mut value = String::with_capacity(left.len() + right.len());
        value.push_str(left);
        value.push_str(right);
        writer.write_str(row, &value)?;
    }
    Ok(())
}

pub fn get_string_concat_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("string_concat".to_string());
    set.add_function(ScalarFunction::new(
        "string_concat".to_string(),
        vec![LogicalType::Varchar, LogicalType::Varchar],
        LogicalType::Varchar,
        string_concat_varchar,
    ));
    set
}

/// Implementation of `concat_ws(VARCHAR, VARCHAR...) -> VARCHAR`.
/// First argument is separator, NULL values are skipped.
fn concat_ws_varchar(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_concat_ws(input, result)
}

/// Get `concat` function set.
pub fn get_concat_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("concat".to_string());

    // concat(VARCHAR...) - varargs version
    set.add_function(
        ScalarFunction::new(
            "concat".to_string(),
            vec![], // No fixed arguments
            LogicalType::Varchar,
            concat_varchar,
        )
        .with_varargs(LogicalType::Varchar)
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    set
}

/// Get `concat_ws` function set.
pub fn get_concat_ws_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("concat_ws".to_string());

    // concat_ws(VARCHAR separator, VARCHAR...) - separator + varargs
    set.add_function(
        ScalarFunction::new(
            "concat_ws".to_string(),
            vec![LogicalType::Varchar], // Separator is fixed
            LogicalType::Varchar,
            concat_ws_varchar,
        )
        .with_varargs(LogicalType::Varchar)
        .with_null_handling(FunctionNullHandling::SpecialHandling),
    );

    set
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_concat_basic() {
        let v1 = paro_common::test_utils::test_string_vector_with_allocator(
            &["hello", "foo"],
            paro_common::test_utils::test_allocator(),
        );
        let v2 = paro_common::test_utils::test_string_vector_with_allocator(
            &[" ", "-"],
            paro_common::test_utils::test_allocator(),
        );
        let v3 = paro_common::test_utils::test_string_vector_with_allocator(
            &["world", "bar"],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![v1, v2, v3]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        concat_varchar(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hello world"));
        assert_eq!(result.get_string(1), Some("foo-bar"));
    }

    #[test]
    fn test_concat_with_null() {
        let v1 = paro_common::test_utils::test_string_vector_with_allocator(
            &["hello", "foo"],
            paro_common::test_utils::test_allocator(),
        );
        let mut v2 = paro_common::test_utils::test_string_vector_with_allocator(
            &[" ", "-"],
            paro_common::test_utils::test_allocator(),
        );
        v2.validity_mut().set_null(0); // NULL in middle
        let v3 = paro_common::test_utils::test_string_vector_with_allocator(
            &["world", "bar"],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![v1, v2, v3]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        concat_varchar(&chunk, &MockState, &mut result).unwrap();

        // NULL is treated as empty string
        assert_eq!(result.get_string(0), Some("helloworld"));
        assert_eq!(result.get_string(1), Some("foo-bar"));
    }

    #[test]
    fn string_concat_operator_propagates_null() {
        let mut left = paro_common::test_utils::test_string_vector_with_allocator(
            &["hello", "ignored"],
            paro_common::test_utils::test_allocator(),
        );
        left.validity_mut().set_null(1);
        let right = paro_common::test_utils::test_string_vector_with_allocator(
            &[" world", "suffix"],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![left, right]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        string_concat_varchar(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hello world"));
        assert!(result.is_null(1));
    }

    #[test]
    fn test_concat_ws_basic() {
        let sep = paro_common::test_utils::test_string_vector_with_allocator(
            &[", ", "-"],
            paro_common::test_utils::test_allocator(),
        );
        let v1 = paro_common::test_utils::test_string_vector_with_allocator(
            &["a", "x"],
            paro_common::test_utils::test_allocator(),
        );
        let v2 = paro_common::test_utils::test_string_vector_with_allocator(
            &["b", "y"],
            paro_common::test_utils::test_allocator(),
        );
        let v3 = paro_common::test_utils::test_string_vector_with_allocator(
            &["c", "z"],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = Chunk::from_vectors(
            vec![sep, v1, v2, v3],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        concat_ws_varchar(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("a, b, c"));
        assert_eq!(result.get_string(1), Some("x-y-z"));
    }

    #[test]
    fn test_concat_ws_with_null_values() {
        let sep = paro_common::test_utils::test_string_vector_with_allocator(
            &[", "],
            paro_common::test_utils::test_allocator(),
        );
        let v1 = paro_common::test_utils::test_string_vector_with_allocator(
            &["a"],
            paro_common::test_utils::test_allocator(),
        );
        let mut v2 = paro_common::test_utils::test_string_vector_with_allocator(
            &["b"],
            paro_common::test_utils::test_allocator(),
        );
        v2.validity_mut().set_null(0); // NULL value
        let v3 = paro_common::test_utils::test_string_vector_with_allocator(
            &["c"],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = Chunk::from_vectors(
            vec![sep, v1, v2, v3],
            paro_common::test_utils::test_allocator(),
        );
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        concat_ws_varchar(&chunk, &MockState, &mut result).unwrap();

        // NULL values are skipped
        assert_eq!(result.get_string(0), Some("a, c"));
    }

    #[test]
    fn test_concat_ws_null_separator() {
        let mut sep = paro_common::test_utils::test_string_vector_with_allocator(
            &[", "],
            paro_common::test_utils::test_allocator(),
        );
        sep.validity_mut().set_null(0);
        let v1 = paro_common::test_utils::test_string_vector_with_allocator(
            &["a"],
            paro_common::test_utils::test_allocator(),
        );
        let v2 = paro_common::test_utils::test_string_vector_with_allocator(
            &["b"],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![sep, v1, v2]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        concat_ws_varchar(&chunk, &MockState, &mut result).unwrap();

        // NULL separator results in NULL
        assert!(result.is_null(0));
    }
}
