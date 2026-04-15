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
use crate::{ExpressionState, FunctionNullHandling, ScalarFunction, ScalarFunctionSet};

/// Implementation of `concat(VARCHAR...) -> VARCHAR`.
/// NULL values are treated as empty strings.
fn concat_varchar(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    execute_concat(input, result)
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
        let v1 = Vector::from_strings(&["hello", "foo"]);
        let v2 = Vector::from_strings(&[" ", "-"]);
        let v3 = Vector::from_strings(&["world", "bar"]);
        let chunk = Chunk::from_vectors(vec![v1, v2, v3]);
        let mut result = Vector::new(LogicalType::Varchar);

        concat_varchar(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("hello world"));
        assert_eq!(result.get_string(1), Some("foo-bar"));
    }

    #[test]
    fn test_concat_with_null() {
        let v1 = Vector::from_strings(&["hello", "foo"]);
        let mut v2 = Vector::from_strings(&[" ", "-"]);
        v2.validity_mut().set_null(0); // NULL in middle
        let v3 = Vector::from_strings(&["world", "bar"]);
        let chunk = Chunk::from_vectors(vec![v1, v2, v3]);
        let mut result = Vector::new(LogicalType::Varchar);

        concat_varchar(&chunk, &MockState, &mut result).unwrap();

        // NULL is treated as empty string
        assert_eq!(result.get_string(0), Some("helloworld"));
        assert_eq!(result.get_string(1), Some("foo-bar"));
    }

    #[test]
    fn test_concat_ws_basic() {
        let sep = Vector::from_strings(&[", ", "-"]);
        let v1 = Vector::from_strings(&["a", "x"]);
        let v2 = Vector::from_strings(&["b", "y"]);
        let v3 = Vector::from_strings(&["c", "z"]);
        let chunk = Chunk::from_vectors(vec![sep, v1, v2, v3]);
        let mut result = Vector::new(LogicalType::Varchar);

        concat_ws_varchar(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("a, b, c"));
        assert_eq!(result.get_string(1), Some("x-y-z"));
    }

    #[test]
    fn test_concat_ws_with_null_values() {
        let sep = Vector::from_strings(&[", "]);
        let v1 = Vector::from_strings(&["a"]);
        let mut v2 = Vector::from_strings(&["b"]);
        v2.validity_mut().set_null(0); // NULL value
        let v3 = Vector::from_strings(&["c"]);
        let chunk = Chunk::from_vectors(vec![sep, v1, v2, v3]);
        let mut result = Vector::new(LogicalType::Varchar);

        concat_ws_varchar(&chunk, &MockState, &mut result).unwrap();

        // NULL values are skipped
        assert_eq!(result.get_string(0), Some("a, c"));
    }

    #[test]
    fn test_concat_ws_null_separator() {
        let mut sep = Vector::from_strings(&[", "]);
        sep.validity_mut().set_null(0);
        let v1 = Vector::from_strings(&["a"]);
        let v2 = Vector::from_strings(&["b"]);
        let chunk = Chunk::from_vectors(vec![sep, v1, v2]);
        let mut result = Vector::new(LogicalType::Varchar);

        concat_ws_varchar(&chunk, &MockState, &mut result).unwrap();

        // NULL separator results in NULL
        assert!(result.is_null(0));
    }
}
