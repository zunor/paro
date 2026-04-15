//! current_user Function
//!
//! PostgreSQL-compatible function that returns the current user name.
//!
//!
//!
//! ## PostgreSQL Reference
//! `current_user -> name`
//! Returns the user name of the current execution context.
//! Note: In PostgreSQL, this is a special keyword, not a function call.
//!
//! ## Paro Implementation
//! Implemented as a function `current_user()` for simplicity.
//! Queries the session context via `ExpressionState::current_user()`.
//! Falls back to 'paro' if session context is not available.

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::{ExpressionState, FunctionStability, ScalarFunction, ScalarFunctionSet};

/// Default user name when session context is not available.
const DEFAULT_USER: &str = "paro";

/// Implementation of `current_user() -> VARCHAR`.
///
/// Returns the current user name from session context.
/// Falls back to 'paro' if session context is not available.
fn current_user_impl(
    input: &Chunk,
    state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let count = input.size();
    result.set_count(count);
    let user = state.current_user().unwrap_or(DEFAULT_USER);

    // Set the same user name for all rows (constant function)
    for i in 0..count {
        result.set_string(i, user);
    }
    Ok(())
}

/// Get `current_user` function set.
pub fn get_current_user_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("current_user".to_string());
    set.add_function(
        ScalarFunction::new(
            "current_user".to_string(),
            vec![], // No arguments
            LogicalType::Varchar,
            current_user_impl,
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
        user: String,
    }
    impl MockStateWithContext {
        fn new(user: &str) -> Self {
            Self {
                user: user.to_string(),
            }
        }
    }
    impl ExpressionState for MockStateWithContext {
        fn current_database(&self) -> Option<&str> {
            Some("testdb")
        }
        fn current_schema(&self) -> Option<&str> {
            Some("public")
        }
        fn current_user(&self) -> Option<&str> {
            Some(&self.user)
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[test]
    fn test_current_user_without_context() {
        let mut chunk = Chunk::new();
        chunk.set_cardinality(1);
        let state = MockStateNoContext;
        let mut result = Vector::new(LogicalType::Varchar);

        current_user_impl(&chunk, &state, &mut result).unwrap();

        // Falls back to default 'paro'
        assert_eq!(result.get_string(0), Some("paro"));
    }

    #[test]
    fn test_current_user_with_context() {
        let mut chunk = Chunk::new();
        chunk.set_cardinality(1);
        let state = MockStateWithContext::new("alice");
        let mut result = Vector::new(LogicalType::Varchar);

        current_user_impl(&chunk, &state, &mut result).unwrap();

        // Returns actual user from context
        assert_eq!(result.get_string(0), Some("alice"));
    }

    #[test]
    fn test_current_user_function_set() {
        let set = get_current_user_functions();
        assert_eq!(set.name, "current_user");
        assert_eq!(set.functions.len(), 1);

        let func = &set.functions[0];
        assert!(func.arguments.is_empty());
        assert_eq!(func.return_type, LogicalType::Varchar);
        assert_eq!(func.stability, FunctionStability::ConsistentWithinQuery);
    }
}
