// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Current date/time functions.
//!
//!
//!
//! ## Functions
//! - `now()` - Current timestamp (transaction start time)
//! - `current_date` - Current date
//! - `current_time` - Current time of day
//! - `current_timestamp` - Current timestamp

use crate::{ExpressionState, FunctionStability, ScalarFunction, ScalarFunctionSet};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

/// Microseconds per second
const MICROS_PER_SECOND: i64 = 1_000_000;
/// Microseconds per day
const MICROS_PER_DAY: i64 = 24 * 60 * 60 * MICROS_PER_SECOND;

fn transaction_timestamp_micros(state: &dyn ExpressionState) -> Result<i64> {
    state.transaction_timestamp_micros().ok_or_else(|| {
        paro_error::internal("transaction timestamp is missing from function execution context")
    })
}

fn transaction_date_days(state: &dyn ExpressionState) -> Result<i64> {
    Ok(transaction_timestamp_micros(state)?.div_euclid(MICROS_PER_DAY))
}

fn transaction_time_micros(state: &dyn ExpressionState) -> Result<i64> {
    Ok(transaction_timestamp_micros(state)?.rem_euclid(MICROS_PER_DAY))
}

// ============================================================================
// NOW() - Current timestamp
// ============================================================================

fn now_impl(_input: &Chunk, state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    let ts = transaction_timestamp_micros(state)?;
    result.set_count(1);
    result.set_i64(0, ts);
    Ok(())
}

/// Get the `now()` function set.
pub fn get_now_function() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("now".to_string());

    // now() -> TIMESTAMP
    set.add_function(
        ScalarFunction::new("now".to_string(), vec![], LogicalType::Timestamp, now_impl)
            .with_stability(FunctionStability::ConsistentWithinQuery),
    );

    set
}

// ============================================================================
// CURRENT_DATE - Current date
// ============================================================================

fn current_date_impl(
    _input: &Chunk,
    state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let days = transaction_date_days(state)?;
    result.set_count(1);
    result.set_i64(0, days);
    Ok(())
}

/// Get the `current_date` function set.
pub fn get_current_date_function() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("current_date".to_string());

    // current_date -> DATE
    set.add_function(
        ScalarFunction::new(
            "current_date".to_string(),
            vec![],
            LogicalType::Date,
            current_date_impl,
        )
        .with_stability(FunctionStability::ConsistentWithinQuery),
    );

    set
}

// ============================================================================
// CURRENT_TIME - Current time of day
// ============================================================================

fn current_time_impl(
    _input: &Chunk,
    state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let time_micros = transaction_time_micros(state)?;
    result.set_count(1);
    result.set_i64(0, time_micros);
    Ok(())
}

/// Get the `current_time` function set.
pub fn get_current_time_function() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("current_time".to_string());

    // current_time -> TIME
    set.add_function(
        ScalarFunction::new(
            "current_time".to_string(),
            vec![],
            LogicalType::Time,
            current_time_impl,
        )
        .with_stability(FunctionStability::ConsistentWithinQuery),
    );

    set
}

// ============================================================================
// CURRENT_TIMESTAMP - Current timestamp (alias for now())
// ============================================================================

/// Get the `current_timestamp` function set.
pub fn get_current_timestamp_function() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("current_timestamp".to_string());

    // current_timestamp -> TIMESTAMP
    set.add_function(
        ScalarFunction::new(
            "current_timestamp".to_string(),
            vec![],
            LogicalType::Timestamp,
            now_impl,
        )
        .with_stability(FunctionStability::ConsistentWithinQuery),
    );

    set
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;

    fn create_test_chunk() -> Chunk {
        Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed")
    }

    // Mock ExpressionState for testing
    const TEST_TRANSACTION_TIMESTAMP: i64 = 1_700_000_000_123_456;

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
        fn transaction_timestamp_micros(&self) -> Option<i64> {
            Some(TEST_TRANSACTION_TIMESTAMP)
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[test]
    fn test_now() {
        let chunk = create_test_chunk();
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Timestamp);

        now_impl(&chunk, &state, &mut result).unwrap();

        let ts = result.get_i64(0).unwrap();
        assert_eq!(ts, TEST_TRANSACTION_TIMESTAMP);
    }

    #[test]
    fn test_current_date() {
        let chunk = create_test_chunk();
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Date);

        current_date_impl(&chunk, &state, &mut result).unwrap();

        assert_eq!(
            result.get_i64(0),
            Some(TEST_TRANSACTION_TIMESTAMP.div_euclid(MICROS_PER_DAY))
        );
    }

    #[test]
    fn test_current_time() {
        let chunk = create_test_chunk();
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Time);

        current_time_impl(&chunk, &state, &mut result).unwrap();

        assert_eq!(
            result.get_i64(0),
            Some(TEST_TRANSACTION_TIMESTAMP.rem_euclid(MICROS_PER_DAY))
        );
    }

    #[test]
    fn current_functions_require_a_transaction_time_anchor() {
        struct MissingTimeState;
        impl ExpressionState for MissingTimeState {
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

        let mut result = paro_common::test_utils::test_vector(LogicalType::Timestamp);
        let error = now_impl(&create_test_chunk(), &MissingTimeState, &mut result)
            .expect_err("time functions must not read the wall clock independently");

        assert!(error
            .to_string()
            .contains("transaction timestamp is missing"));
    }

    #[test]
    fn test_function_stability() {
        let now_set = get_now_function();
        assert_eq!(
            now_set.functions[0].stability,
            FunctionStability::ConsistentWithinQuery
        );

        let current_date_set = get_current_date_function();
        assert_eq!(
            current_date_set.functions[0].stability,
            FunctionStability::ConsistentWithinQuery
        );
    }
}
