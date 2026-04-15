//! current_schema() Function
//!
//! PostgreSQL-compatible function that returns the current schema name.
//!
//!
//!
//! ## PostgreSQL Reference
//! `current_schema() -> name`
//! Returns the name of the schema that is first in the search path.
//!
//! ## Paro Implementation
//! Queries the session context via `ExpressionState::current_schema()`.
//! Falls back to 'public' if session context is not available.

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::{ExpressionState, FunctionStability, ScalarFunction, ScalarFunctionSet};

/// Default schema name when session context is not available.
const DEFAULT_SCHEMA: &str = "public";

/// Implementation of `current_schema() -> VARCHAR`.
///
/// Returns the current schema name from session context.
/// Falls back to 'public' if session context is not available.
fn current_schema_impl(
    input: &Chunk,
    state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let count = input.size();
    result.set_count(count);
    let schema = state.current_schema().unwrap_or(DEFAULT_SCHEMA);

    // Set the same schema name for all rows (constant function)
    for i in 0..count {
        result.set_string(i, schema);
    }
    Ok(())
}

/// Get `current_schema` function set.
pub fn get_current_schema_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("current_schema".to_string());
    set.add_function(
        ScalarFunction::new(
            "current_schema".to_string(),
            vec![], // No arguments
            LogicalType::Varchar,
            current_schema_impl,
        )
        .with_stability(FunctionStability::ConsistentWithinQuery),
    );
    set
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;

    /// Mock state without session context (returns None for all).
    struct MockStateNoContext;
    impl ExpressionState for MockStateNoContext {
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

    /// Mock state with session context.
    struct MockStateWithContext {
        schema: String,
    }
    impl MockStateWithContext {
        fn new(schema: &str) -> Self {
            Self {
                schema: schema.to_string(),
            }
        }
    }
    impl ExpressionState for MockStateWithContext {
        fn current_database(&self) -> Option<&str> {
            Some("testdb")
        }
        fn current_schema(&self) -> Option<&str> {
            Some(&self.schema)
        }
        fn current_user(&self) -> Option<&str> {
            Some("testuser")
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[test]
    fn test_current_schema_without_context() {
        let mut chunk = Chunk::new();
        chunk.set_cardinality(1);
        let state = MockStateNoContext;
        let mut result = Vector::new(LogicalType::Varchar);

        current_schema_impl(&chunk, &state, &mut result).unwrap();

        // Falls back to default 'public'
        assert_eq!(result.get_string(0), Some("public"));
    }

    #[test]
    fn test_current_schema_with_context() {
        let mut chunk = Chunk::new();
        chunk.set_cardinality(1);
        let state = MockStateWithContext::new("myschema");
        let mut result = Vector::new(LogicalType::Varchar);

        current_schema_impl(&chunk, &state, &mut result).unwrap();

        // Returns actual schema from context
        assert_eq!(result.get_string(0), Some("myschema"));
    }

    #[test]
    fn test_current_schema_function_set() {
        let set = get_current_schema_functions();
        assert_eq!(set.name, "current_schema");
        assert_eq!(set.functions.len(), 1);

        let func = &set.functions[0];
        assert!(func.arguments.is_empty());
        assert_eq!(func.return_type, LogicalType::Varchar);
        assert_eq!(func.stability, FunctionStability::ConsistentWithinQuery);
    }
}
