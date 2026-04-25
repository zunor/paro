// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Epoch conversion functions.
//!
//!
//!
//! ## Functions
//! - `epoch(timestamp)` - Convert timestamp to Unix epoch seconds
//! - `epoch_ms(timestamp)` - Convert timestamp to Unix epoch milliseconds
//! - `to_timestamp(epoch)` - Convert Unix epoch seconds to timestamp

use crate::{ExpressionState, ScalarFunction, ScalarFunctionSet};
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

/// Microseconds per second
const MICROS_PER_SECOND: i64 = 1_000_000;
/// Microseconds per millisecond
const MICROS_PER_MS: i64 = 1_000;
/// Seconds per day
const SECONDS_PER_DAY: i64 = 86400;

// ============================================================================
// EPOCH - Convert to Unix epoch seconds
// ============================================================================

fn epoch_from_timestamp(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let count = input.size();
    let src = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing input column".to_string()))?;

    result.set_count(count);

    for i in 0..count {
        if src.is_null(i) {
            result.validity_mut().set_null(i);
        } else {
            let micros = src.get_i64(i).unwrap_or(0);
            result.set_i64(i, micros / MICROS_PER_SECOND);
        }
    }

    Ok(())
}

fn epoch_from_date(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    let count = input.size();
    let src = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing input column".to_string()))?;

    result.set_count(count);

    for i in 0..count {
        if src.is_null(i) {
            result.validity_mut().set_null(i);
        } else {
            let days = src.get_i64(i).unwrap_or(0);
            result.set_i64(i, days * SECONDS_PER_DAY);
        }
    }

    Ok(())
}

pub fn get_epoch_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("epoch".to_string());

    // epoch(timestamp) -> BIGINT (seconds)
    set.add_function(ScalarFunction::new(
        "epoch".to_string(),
        vec![LogicalType::Timestamp],
        LogicalType::BigInt,
        epoch_from_timestamp,
    ));

    // epoch(date) -> BIGINT (seconds)
    set.add_function(ScalarFunction::new(
        "epoch".to_string(),
        vec![LogicalType::Date],
        LogicalType::BigInt,
        epoch_from_date,
    ));

    set
}

// ============================================================================
// EPOCH_MS - Convert to Unix epoch milliseconds
// ============================================================================

fn epoch_ms_from_timestamp(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let count = input.size();
    let src = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing input column".to_string()))?;

    result.set_count(count);

    for i in 0..count {
        if src.is_null(i) {
            result.validity_mut().set_null(i);
        } else {
            let micros = src.get_i64(i).unwrap_or(0);
            result.set_i64(i, micros / MICROS_PER_MS);
        }
    }

    Ok(())
}

fn epoch_ms_from_date(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let count = input.size();
    let src = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing input column".to_string()))?;

    result.set_count(count);

    for i in 0..count {
        if src.is_null(i) {
            result.validity_mut().set_null(i);
        } else {
            let days = src.get_i64(i).unwrap_or(0);
            result.set_i64(i, days * SECONDS_PER_DAY * 1000);
        }
    }

    Ok(())
}

pub fn get_epoch_ms_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("epoch_ms".to_string());

    // epoch_ms(timestamp) -> BIGINT (milliseconds)
    set.add_function(ScalarFunction::new(
        "epoch_ms".to_string(),
        vec![LogicalType::Timestamp],
        LogicalType::BigInt,
        epoch_ms_from_timestamp,
    ));

    // epoch_ms(date) -> BIGINT (milliseconds)
    set.add_function(ScalarFunction::new(
        "epoch_ms".to_string(),
        vec![LogicalType::Date],
        LogicalType::BigInt,
        epoch_ms_from_date,
    ));

    set
}

// ============================================================================
// TO_TIMESTAMP - Convert Unix epoch to timestamp
// ============================================================================

fn to_timestamp_from_seconds(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let count = input.size();
    let src = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing input column".to_string()))?;

    result.set_count(count);

    for i in 0..count {
        if src.is_null(i) {
            result.validity_mut().set_null(i);
        } else {
            let seconds = src.get_i64(i).unwrap_or(0);
            result.set_i64(i, seconds * MICROS_PER_SECOND);
        }
    }

    Ok(())
}

fn to_timestamp_from_double(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let count = input.size();
    let src = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing input column".to_string()))?;

    result.set_count(count);

    for i in 0..count {
        if src.is_null(i) {
            result.validity_mut().set_null(i);
        } else {
            let seconds = src.get_f64(i).unwrap_or(0.0);
            result.set_i64(i, (seconds * MICROS_PER_SECOND as f64) as i64);
        }
    }

    Ok(())
}

pub fn get_to_timestamp_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("to_timestamp".to_string());

    // to_timestamp(bigint) -> TIMESTAMP (from seconds)
    set.add_function(ScalarFunction::new(
        "to_timestamp".to_string(),
        vec![LogicalType::BigInt],
        LogicalType::Timestamp,
        to_timestamp_from_seconds,
    ));

    // to_timestamp(double) -> TIMESTAMP (from fractional seconds)
    set.add_function(ScalarFunction::new(
        "to_timestamp".to_string(),
        vec![LogicalType::Double],
        LogicalType::Timestamp,
        to_timestamp_from_double,
    ));

    set
}

// ============================================================================
// Tests
// ============================================================================

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
    fn test_epoch_from_timestamp() {
        // 1 second = 1_000_000 microseconds
        let input = paro_common::test_utils::test_i64_vector_with_allocator(
            &[1_000_000, 2_000_000, 0],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![input]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::BigInt);

        epoch_from_timestamp(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i64(0), Some(1));
        assert_eq!(result.get_i64(1), Some(2));
        assert_eq!(result.get_i64(2), Some(0));
    }

    #[test]
    fn test_epoch_ms_from_timestamp() {
        // 1 millisecond = 1_000 microseconds
        let input = paro_common::test_utils::test_i64_vector_with_allocator(
            &[1_000, 2_000, 0],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![input]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::BigInt);

        epoch_ms_from_timestamp(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i64(0), Some(1));
        assert_eq!(result.get_i64(1), Some(2));
        assert_eq!(result.get_i64(2), Some(0));
    }

    #[test]
    fn test_to_timestamp_from_seconds() {
        let input = paro_common::test_utils::test_i64_vector_with_allocator(
            &[1, 2, 0],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![input]);
        let mut result = paro_common::test_utils::test_vector(LogicalType::Timestamp);

        to_timestamp_from_seconds(&chunk, &MockState, &mut result).unwrap();

        assert_eq!(result.get_i64(0), Some(1_000_000));
        assert_eq!(result.get_i64(1), Some(2_000_000));
        assert_eq!(result.get_i64(2), Some(0));
    }

    #[test]
    fn test_roundtrip() {
        // epoch(to_timestamp(x)) == x
        let original_seconds = 1704067200_i64; // 2024-01-01 00:00:00 UTC

        let input = paro_common::test_utils::test_i64_vector_with_allocator(
            &[original_seconds],
            paro_common::test_utils::test_allocator(),
        );
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![input]);
        let mut ts_result = paro_common::test_utils::test_vector(LogicalType::Timestamp);
        to_timestamp_from_seconds(&chunk, &MockState, &mut ts_result).unwrap();

        let ts_chunk = paro_common::test_utils::test_chunk_from_vectors(vec![ts_result]);
        let mut epoch_result = paro_common::test_utils::test_vector(LogicalType::BigInt);
        epoch_from_timestamp(&ts_chunk, &MockState, &mut epoch_result).unwrap();

        assert_eq!(epoch_result.get_i64(0), Some(original_seconds));
    }
}
