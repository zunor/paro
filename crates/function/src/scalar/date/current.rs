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
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use std::time::{SystemTime, UNIX_EPOCH};

/// Microseconds per second
const MICROS_PER_SECOND: i64 = 1_000_000;
/// Microseconds per day
const MICROS_PER_DAY: i64 = 24 * 60 * 60 * MICROS_PER_SECOND;

/// Get current timestamp in microseconds since Unix epoch.
fn get_current_timestamp_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// Get current date as days since Unix epoch.
fn get_current_date_days() -> i64 {
    get_current_timestamp_micros() / MICROS_PER_DAY
}

/// Get current time as microseconds since midnight.
fn get_current_time_micros() -> i64 {
    get_current_timestamp_micros() % MICROS_PER_DAY
}

// ============================================================================
// NOW() - Current timestamp
// ============================================================================

fn now_impl(_input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    let ts = get_current_timestamp_micros();
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
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let days = get_current_date_days();
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
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let time_micros = get_current_time_micros();
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
        Chunk::new()
    }

    // Mock ExpressionState for testing
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
    fn test_now() {
        let chunk = create_test_chunk();
        let state = MockState;
        let mut result = Vector::new(LogicalType::Timestamp);

        now_impl(&chunk, &state, &mut result).unwrap();

        // Result should have current timestamp
        let ts = result.get_i64(0).unwrap();
        assert!(ts > 0); // Should be positive (after Unix epoch)

        // Should be within reasonable range (after 2020)
        let year_2020_micros = 1577836800_i64 * MICROS_PER_SECOND;
        assert!(ts > year_2020_micros);
    }

    #[test]
    fn test_current_date() {
        let chunk = create_test_chunk();
        let state = MockState;
        let mut result = Vector::new(LogicalType::Date);

        current_date_impl(&chunk, &state, &mut result).unwrap();

        let days = result.get_i64(0).unwrap();
        assert!(days > 0); // Should be positive (after Unix epoch)

        // Should be within reasonable range (after 2020)
        let year_2020_days = 18262_i64; // Days from 1970-01-01 to 2020-01-01
        assert!(days > year_2020_days);
    }

    #[test]
    fn test_current_time() {
        let chunk = create_test_chunk();
        let state = MockState;
        let mut result = Vector::new(LogicalType::Time);

        current_time_impl(&chunk, &state, &mut result).unwrap();

        let time_micros = result.get_i64(0).unwrap();
        // Time should be between 0 and 24 hours in microseconds
        assert!(time_micros >= 0);
        assert!(time_micros < MICROS_PER_DAY);
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
