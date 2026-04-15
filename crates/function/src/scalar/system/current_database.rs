// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! current_database() Function
//!
//! PostgreSQL-compatible function that returns the current database name.
//!
//!
//!
//! ## PostgreSQL Reference
//! `current_database() -> name`
//! Returns the name of the current database.
//!
//! ## Paro Implementation
//! Queries the session context via `ExpressionState::current_database()`.
//! Falls back to 'paro' if session context is not available.

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::{ExpressionState, FunctionStability, ScalarFunction, ScalarFunctionSet};

/// Default database name when session context is not available.
const DEFAULT_DATABASE: &str = "paro";

/// Implementation of `current_database() -> VARCHAR`.
///
/// Returns the current database name from session context.
/// Falls back to 'paro' if session context is not available.
fn current_database_impl(
    input: &Chunk,
    state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let count = input.size();
    result.set_count(count);
    let database = state.current_database().unwrap_or(DEFAULT_DATABASE);

    // Set the same database name for all rows (constant function)
    for i in 0..count {
        result.set_string(i, database);
    }
    Ok(())
}

/// Get `current_database` function set.
pub fn get_current_database_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("current_database".to_string());
    set.add_function(
        ScalarFunction::new(
            "current_database".to_string(),
            vec![], // No arguments
            LogicalType::Varchar,
            current_database_impl,
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
        database: String,
    }
    impl MockStateWithContext {
        fn new(database: &str) -> Self {
            Self {
                database: database.to_string(),
            }
        }
    }
    impl ExpressionState for MockStateWithContext {
        fn current_database(&self) -> Option<&str> {
            Some(&self.database)
        }
        fn current_schema(&self) -> Option<&str> {
            Some("public")
        }
        fn current_user(&self) -> Option<&str> {
            Some("testuser")
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[test]
    fn test_current_database_without_context() {
        let mut chunk = Chunk::new();
        chunk.set_cardinality(1);
        let state = MockStateNoContext;
        let mut result = Vector::new(LogicalType::Varchar);

        current_database_impl(&chunk, &state, &mut result).unwrap();

        // Falls back to default 'paro'
        assert_eq!(result.get_string(0), Some("paro"));
    }

    #[test]
    fn test_current_database_with_context() {
        let mut chunk = Chunk::new();
        chunk.set_cardinality(1);
        let state = MockStateWithContext::new("mydb");
        let mut result = Vector::new(LogicalType::Varchar);

        current_database_impl(&chunk, &state, &mut result).unwrap();

        // Returns actual database from context
        assert_eq!(result.get_string(0), Some("mydb"));
    }

    #[test]
    fn test_current_database_function_set() {
        let set = get_current_database_functions();
        assert_eq!(set.name, "current_database");
        assert_eq!(set.functions.len(), 1);

        let func = &set.functions[0];
        assert!(func.arguments.is_empty());
        assert_eq!(func.return_type, LogicalType::Varchar);
        assert_eq!(func.stability, FunctionStability::ConsistentWithinQuery);
    }
}
