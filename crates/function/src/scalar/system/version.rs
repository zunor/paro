//! version() Function
//!
//! PostgreSQL-compatible function that returns the database version string.
//!
//!
//!
//! ## PostgreSQL Reference
//! `version() -> text`
//! Returns a string describing the PostgreSQL server version.
//!
//! ## Paro Implementation
//! Returns a Paro-specific version string that mimics PostgreSQL format
//! for compatibility with tools like psql.

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::{ExpressionState, FunctionStability, ScalarFunction, ScalarFunctionSet};

/// Paro version string in PostgreSQL-compatible format.
const PARO_VERSION: &str = "Paro 0.1.0 on x86_64-apple-darwin, compiled by rustc";

/// Implementation of `version() -> VARCHAR`.
///
/// Returns the database version string.
fn version_impl(_input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    // version() is a no-argument function, but we still need to handle
    // the case where it's called in a context with multiple rows
    // For now, we assume single-row result for zero-arg functions
    result.set_count(1);
    result.set_string(0, PARO_VERSION);
    Ok(())
}

/// Get `version` function set.
pub fn get_version_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("version".to_string());
    set.add_function(
        ScalarFunction::new(
            "version".to_string(),
            vec![], // No arguments
            LogicalType::Varchar,
            version_impl,
        )
        .with_stability(FunctionStability::Consistent),
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
    fn test_version_basic() {
        let chunk = Chunk::new();
        let state = MockState;
        let mut result = Vector::new(LogicalType::Varchar);

        version_impl(&chunk, &state, &mut result).unwrap();

        let version_str = result.get_string(0).unwrap();
        assert!(version_str.starts_with("Paro"));
    }

    #[test]
    fn test_version_function_set() {
        let set = get_version_functions();
        assert_eq!(set.name, "version");
        assert_eq!(set.functions.len(), 1);

        let func = &set.functions[0];
        assert!(func.arguments.is_empty());
        assert_eq!(func.return_type, LogicalType::Varchar);
    }
}
