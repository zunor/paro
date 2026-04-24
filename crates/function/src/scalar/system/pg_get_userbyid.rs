// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! pg_get_userbyid Function
//!
//! PostgreSQL-compatible function that returns the username for a given OID.
//!
//!
//!
//! ## PostgreSQL Reference
//! `pg_get_userbyid(oid) -> name`
//! Returns the role name for a given role OID.
//!
//! ## Paro Implementation
//! Currently returns a stub value ('paro') since Paro does not yet have
//! a full user management system. This is sufficient for psql compatibility.

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::{ExpressionState, ScalarFunction, ScalarFunctionSet};

/// Implementation of `pg_get_userbyid(BIGINT) -> VARCHAR`.
///
/// Returns the username for a given OID.
/// Currently returns 'paro' as a stub since we don't have user management.
fn pg_get_userbyid_impl(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let count = input.size();
    let input_vec = input
        .column(0)
        .ok_or_else(|| paro_error::internal("Missing input column"))?;

    result.set_count(count);

    for i in 0..count {
        if input_vec.is_null(i) {
            result.validity_mut().set_null(i);
        } else {
            // Stub implementation: always return 'paro' regardless of OID
            // In a full implementation, this would look up the user in pg_authid
            result.set_string(i, "paro");
        }
    }

    Ok(())
}

/// Get `pg_get_userbyid` function set.
pub fn get_pg_get_userbyid_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("pg_get_userbyid".to_string());
    set.add_function(ScalarFunction::new(
        "pg_get_userbyid".to_string(),
        vec![LogicalType::BigInt],
        LogicalType::Varchar,
        pg_get_userbyid_impl,
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
    fn test_pg_get_userbyid_basic() {
        let input_vec = paro_common::test_utils::test_i64_vector_with_allocator(
            &[0, 1, 10, 100],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![input_vec]);
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        pg_get_userbyid_impl(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("paro"));
        assert_eq!(result.get_string(1), Some("paro"));
        assert_eq!(result.get_string(2), Some("paro"));
        assert_eq!(result.get_string(3), Some("paro"));
    }

    #[test]
    fn test_pg_get_userbyid_with_null() {
        let mut input_vec = paro_common::test_utils::test_i64_vector_with_allocator(
            &[0, 1],
            paro_common::test_utils::test_allocator(),
        );
        input_vec.validity_mut().set_null(1);
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![input_vec]);
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Varchar);

        pg_get_userbyid_impl(&chunk, &state, &mut result).unwrap();

        assert_eq!(result.get_string(0), Some("paro"));
        assert!(result.is_null(1));
    }

    #[test]
    fn test_pg_get_userbyid_function_set() {
        let set = get_pg_get_userbyid_functions();
        assert_eq!(set.name, "pg_get_userbyid");
        assert_eq!(set.functions.len(), 1);

        let func = &set.functions[0];
        assert_eq!(func.arguments, vec![LogicalType::BigInt]);
        assert_eq!(func.return_type, LogicalType::Varchar);
    }
}
