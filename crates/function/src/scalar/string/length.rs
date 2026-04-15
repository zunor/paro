//! # Length Functions
//!
//! String length functions: `length`, `char_length`, `octet_length`.
//!
//!

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::scalar::executor::varlen::execute_varchar_unary_to_i64;
use crate::scalar::string::bind_storage_dictionary_unary_infallible;
use crate::{ExpressionState, ScalarFunction, ScalarFunctionSet};

/// Count UTF-8 codepoints in a string.
/// This is the standard SQL `length` / `char_length` behavior.
fn utf8_length(s: &str) -> i64 {
    s.chars().count() as i64
}

/// Count bytes in a string.
/// This is `octet_length` behavior.
fn byte_length(s: &str) -> i64 {
    s.len() as i64
}

/// Implementation of `length(VARCHAR) -> BIGINT`.
/// Returns the number of Unicode codepoints.
fn length_varchar(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    let input_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing input column".to_string()))?;
    execute_varchar_unary_to_i64(input_vec, result, input.size(), utf8_length)
}

/// Implementation of `octet_length(VARCHAR) -> BIGINT`.
/// Returns the number of bytes.
fn octet_length_varchar(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let input_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing input column".to_string()))?;
    execute_varchar_unary_to_i64(input_vec, result, input.size(), byte_length)
}

/// Get `length` function set.
pub fn get_length_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("length".to_string());
    set.add_function(
        ScalarFunction::new(
            "length".to_string(),
            vec![LogicalType::Varchar],
            LogicalType::BigInt,
            length_varchar,
        )
        .with_bind(bind_storage_dictionary_unary_infallible),
    );
    set
}

/// Get `char_length` function set (alias for length).
pub fn get_char_length_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("char_length".to_string());
    set.add_function(
        ScalarFunction::new(
            "char_length".to_string(),
            vec![LogicalType::Varchar],
            LogicalType::BigInt,
            length_varchar,
        )
        .with_bind(bind_storage_dictionary_unary_infallible),
    );
    set
}

/// Get `octet_length` function set.
pub fn get_octet_length_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("octet_length".to_string());
    set.add_function(
        ScalarFunction::new(
            "octet_length".to_string(),
            vec![LogicalType::Varchar],
            LogicalType::BigInt,
            octet_length_varchar,
        )
        .with_bind(bind_storage_dictionary_unary_infallible),
    );
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
    fn test_utf8_length() {
        assert_eq!(utf8_length("hello"), 5);
        assert_eq!(utf8_length(""), 0);
        assert_eq!(utf8_length("你好"), 2);
        assert_eq!(utf8_length("🎉"), 1);
        assert_eq!(utf8_length("hello世界"), 7);
    }

    #[test]
    fn test_byte_length() {
        assert_eq!(byte_length("hello"), 5);
        assert_eq!(byte_length(""), 0);
        assert_eq!(byte_length("你好"), 6);
        assert_eq!(byte_length("🎉"), 4);
    }

    #[test]
    fn test_length_function() {
        let input_vec = Vector::from_strings(&["hello", "世界", ""]);
        let chunk = Chunk::from_vectors(vec![input_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::BigInt);

        length_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_i64(0), Some(5));
        assert_eq!(result.get_i64(1), Some(2));
        assert_eq!(result.get_i64(2), Some(0));
    }

    #[test]
    fn test_length_with_null() {
        let mut input_vec = Vector::from_strings(&["hello", "world"]);
        input_vec.validity_mut().set_null(1);
        let chunk = Chunk::from_vectors(vec![input_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::BigInt);

        length_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_i64(0), Some(5));
        assert!(result.is_null(1));
    }

    #[test]
    fn test_octet_length_function() {
        let input_vec = Vector::from_strings(&["hello", "世界", "🎉"]);
        let chunk = Chunk::from_vectors(vec![input_vec]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::BigInt);

        octet_length_varchar(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_i64(0), Some(5));
        assert_eq!(result.get_i64(1), Some(6));
        assert_eq!(result.get_i64(2), Some(4));
    }
}
