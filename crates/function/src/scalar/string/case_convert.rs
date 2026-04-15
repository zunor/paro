// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Case Conversion Functions
//!
//! Case conversion functions: `lower`, `upper`.
//!
//!

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::scalar::executor::varlen::execute_varchar_unary_to_varchar;
use crate::scalar::string::bind_storage_dictionary_unary_infallible;
use crate::{ExpressionState, ScalarFunction, ScalarFunctionSet};

/// Implementation of `lower(VARCHAR) -> VARCHAR`.
fn lower_varchar(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    let input_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing input column".to_string()))?;
    execute_varchar_unary_to_varchar(input_vec, result, input.size(), |value, row, writer| {
        writer.write_str(row, &value.to_lowercase());
        Ok(())
    })
}

/// Implementation of `upper(VARCHAR) -> VARCHAR`.
fn upper_varchar(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    let input_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing input column".to_string()))?;
    execute_varchar_unary_to_varchar(input_vec, result, input.size(), |value, row, writer| {
        writer.write_str(row, &value.to_uppercase());
        Ok(())
    })
}

/// Get `lower` function set.
pub fn get_lower_function() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("lower".to_string());
    set.add_function(
        ScalarFunction::new(
            "lower".to_string(),
            vec![LogicalType::Varchar],
            LogicalType::Varchar,
            lower_varchar,
        )
        .with_bind(bind_storage_dictionary_unary_infallible),
    );
    set
}

/// Get `upper` function set.
pub fn get_upper_function() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("upper".to_string());
    set.add_function(
        ScalarFunction::new(
            "upper".to_string(),
            vec![LogicalType::Varchar],
            LogicalType::Varchar,
            upper_varchar,
        )
        .with_bind(bind_storage_dictionary_unary_infallible),
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
    fn test_lower() {
        let input_vec = Vector::from_strings(&["HELLO", "World", "123ABC"]);
        let chunk = Chunk::from_vectors(vec![input_vec]);
        let mut result = Vector::new(LogicalType::Varchar);

        lower_varchar(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hello"));
        assert_eq!(result.get_string(1), Some("world"));
        assert_eq!(result.get_string(2), Some("123abc"));
    }

    #[test]
    fn test_upper() {
        let input_vec = Vector::from_strings(&["hello", "World", "123abc"]);
        let chunk = Chunk::from_vectors(vec![input_vec]);
        let mut result = Vector::new(LogicalType::Varchar);

        upper_varchar(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("HELLO"));
        assert_eq!(result.get_string(1), Some("WORLD"));
        assert_eq!(result.get_string(2), Some("123ABC"));
    }

    #[test]
    fn test_lower_unicode() {
        let input_vec = Vector::from_strings(&["MÜNCHEN", "HELLO"]);
        let chunk = Chunk::from_vectors(vec![input_vec]);
        let mut result = Vector::new(LogicalType::Varchar);

        lower_varchar(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("münchen"));
        assert_eq!(result.get_string(1), Some("hello"));
    }

    #[test]
    fn test_upper_unicode() {
        let input_vec = Vector::from_strings(&["münchen", "hello"]);
        let chunk = Chunk::from_vectors(vec![input_vec]);
        let mut result = Vector::new(LogicalType::Varchar);

        upper_varchar(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("MÜNCHEN"));
        assert_eq!(result.get_string(1), Some("HELLO"));
    }

    #[test]
    fn test_case_with_null() {
        let mut input_vec = Vector::from_strings(&["hello", "world"]);
        input_vec.validity_mut().set_null(1);
        let chunk = Chunk::from_vectors(vec![input_vec]);
        let mut result = Vector::new(LogicalType::Varchar);

        lower_varchar(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hello"));
        assert!(result.is_null(1));
    }
}
