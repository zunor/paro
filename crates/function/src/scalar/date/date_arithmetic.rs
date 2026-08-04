// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Date and timestamp arithmetic.

use crate::scalar::cast::date_casts::{days_in_month, days_to_ymd, ymd_to_days};
use crate::{ExpressionState, ScalarFunction, ScalarFunctionSet};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

const MICROS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000;
type IntervalParts = (i32, i32, i64);

fn column<'a>(input: &'a Chunk, index: usize, name: &str) -> Result<&'a Vector> {
    input
        .column(index)
        .map(AsRef::as_ref)
        .ok_or_else(|| paro_error::internal(format!("Missing {name} column")))
}

fn invert_interval((months, days, micros): IntervalParts) -> Result<IntervalParts> {
    Ok((
        months
            .checked_neg()
            .ok_or_else(|| paro_error::out_of_range("INTERVAL months overflow"))?,
        days.checked_neg()
            .ok_or_else(|| paro_error::out_of_range("INTERVAL days overflow"))?,
        micros
            .checked_neg()
            .ok_or_else(|| paro_error::out_of_range("INTERVAL microseconds overflow"))?,
    ))
}

fn add_months_to_date(date: i32, months: i32) -> Result<i32> {
    if months == 0 || matches!(date, i32::MIN | i32::MAX) {
        return Ok(date);
    }

    let (year, month, day) = days_to_ymd(i64::from(date));
    let month_index = i64::from(year)
        .checked_mul(12)
        .and_then(|value| value.checked_add(i64::from(month) - 1))
        .and_then(|value| value.checked_add(i64::from(months)))
        .ok_or_else(|| paro_error::out_of_range("DATE month arithmetic overflow"))?;
    let new_year = i32::try_from(month_index.div_euclid(12))
        .map_err(|_| paro_error::out_of_range("DATE year out of range"))?;
    let new_month = u32::try_from(month_index.rem_euclid(12) + 1)
        .map_err(|_| paro_error::out_of_range("DATE month out of range"))?;
    let new_day = day.min(days_in_month(new_year, new_month));
    i32::try_from(ymd_to_days(new_year, new_month, new_day))
        .map_err(|_| paro_error::out_of_range("DATE out of range"))
}

fn add_interval_to_timestamp(timestamp: i64, interval: IntervalParts) -> Result<i64> {
    if matches!(timestamp, i64::MIN | i64::MAX) {
        return Ok(timestamp);
    }

    let (months, days, micros) = interval;
    let date = i32::try_from(timestamp.div_euclid(MICROS_PER_DAY))
        .map_err(|_| paro_error::out_of_range("TIMESTAMP date out of range"))?;
    let time = timestamp.rem_euclid(MICROS_PER_DAY);
    let date = add_months_to_date(date, months)?;
    let date = date
        .checked_add(days)
        .ok_or_else(|| paro_error::out_of_range("DATE day arithmetic overflow"))?;
    i64::from(date)
        .checked_mul(MICROS_PER_DAY)
        .and_then(|value| value.checked_add(time))
        .and_then(|value| value.checked_add(micros))
        .ok_or_else(|| paro_error::out_of_range("TIMESTAMP arithmetic overflow"))
}

fn add_interval_to_date(date: i32, interval: IntervalParts) -> Result<i64> {
    match date {
        i32::MAX => Ok(i64::MAX),
        i32::MIN => Ok(i64::MIN),
        _ => add_interval_to_timestamp(i64::from(date) * MICROS_PER_DAY, interval),
    }
}

fn execute_date_interval(
    input: &Chunk,
    result: &mut Vector,
    date_index: usize,
    interval_index: usize,
    subtract: bool,
) -> Result<()> {
    let date = column(input, date_index, "date")?;
    let interval = column(input, interval_index, "interval")?;
    let count = input.size();
    result.set_count(count);
    for row in 0..count {
        if date.is_null(row) || interval.is_null(row) {
            result.set_null(row, true);
            continue;
        }
        let date = date
            .get_i32(row)
            .ok_or_else(|| paro_error::internal("DATE vector has no physical INT32 value"))?;
        let interval = interval.get_interval(row).ok_or_else(|| {
            paro_error::internal("INTERVAL vector has no physical interval value")
        })?;
        let interval = if subtract {
            invert_interval(interval)?
        } else {
            interval
        };
        result.set_i64(row, add_interval_to_date(date, interval)?);
    }
    Ok(())
}

fn execute_timestamp_interval(
    input: &Chunk,
    result: &mut Vector,
    timestamp_index: usize,
    interval_index: usize,
    subtract: bool,
) -> Result<()> {
    let timestamp = column(input, timestamp_index, "timestamp")?;
    let interval = column(input, interval_index, "interval")?;
    let count = input.size();
    result.set_count(count);
    for row in 0..count {
        if timestamp.is_null(row) || interval.is_null(row) {
            result.set_null(row, true);
            continue;
        }
        let timestamp = timestamp
            .get_i64(row)
            .ok_or_else(|| paro_error::internal("TIMESTAMP vector has no physical INT64 value"))?;
        let interval = interval.get_interval(row).ok_or_else(|| {
            paro_error::internal("INTERVAL vector has no physical interval value")
        })?;
        let interval = if subtract {
            invert_interval(interval)?
        } else {
            interval
        };
        result.set_i64(row, add_interval_to_timestamp(timestamp, interval)?);
    }
    Ok(())
}

fn date_interval_add_impl(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_date_interval(input, result, 0, 1, false)
}

fn interval_date_add_impl(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_date_interval(input, result, 1, 0, false)
}

fn date_interval_sub_impl(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_date_interval(input, result, 0, 1, true)
}

fn timestamp_interval_add_impl(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_timestamp_interval(input, result, 0, 1, false)
}

fn interval_timestamp_add_impl(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_timestamp_interval(input, result, 1, 0, false)
}

fn timestamp_interval_sub_impl(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_timestamp_interval(input, result, 0, 1, true)
}

/// Add temporal overloads to the standard `+` and `-` operator sets.
pub fn register_temporal_arithmetic_functions(set: &mut ScalarFunctionSet) {
    match set.name.as_str() {
        "+" => {
            set.add_function(ScalarFunction::new(
                "+".to_string(),
                vec![LogicalType::Date, LogicalType::Interval],
                LogicalType::Timestamp,
                date_interval_add_impl,
            ));
            set.add_function(ScalarFunction::new(
                "+".to_string(),
                vec![LogicalType::Interval, LogicalType::Date],
                LogicalType::Timestamp,
                interval_date_add_impl,
            ));
            set.add_function(ScalarFunction::new(
                "+".to_string(),
                vec![LogicalType::Timestamp, LogicalType::Interval],
                LogicalType::Timestamp,
                timestamp_interval_add_impl,
            ));
            set.add_function(ScalarFunction::new(
                "+".to_string(),
                vec![LogicalType::Interval, LogicalType::Timestamp],
                LogicalType::Timestamp,
                interval_timestamp_add_impl,
            ));
        }
        "-" => {
            set.add_function(ScalarFunction::new(
                "-".to_string(),
                vec![LogicalType::Date, LogicalType::Interval],
                LogicalType::Timestamp,
                date_interval_sub_impl,
            ));
            set.add_function(ScalarFunction::new(
                "-".to_string(),
                vec![LogicalType::Timestamp, LogicalType::Interval],
                LogicalType::Timestamp,
                timestamp_interval_sub_impl,
            ));
        }
        _ => {}
    }
}

fn date_add_days_impl(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_date_days(input, result, false)
}

fn date_sub_days_impl(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    execute_date_days(input, result, true)
}

fn execute_date_days(input: &Chunk, result: &mut Vector, subtract: bool) -> Result<()> {
    let date = column(input, 0, "date")?;
    let days = column(input, 1, "days")?;
    let count = input.size();
    result.set_count(count);
    for row in 0..count {
        if date.is_null(row) || days.is_null(row) {
            result.set_null(row, true);
            continue;
        }
        let date = date
            .get_i32(row)
            .ok_or_else(|| paro_error::internal("DATE vector has no physical INT32 value"))?;
        if matches!(date, i32::MIN | i32::MAX) {
            result.set_i32(row, date);
            continue;
        }
        let days = days
            .get_i64(row)
            .ok_or_else(|| paro_error::internal("BIGINT vector has no physical INT64 value"))?;
        let days = if subtract {
            days.checked_neg()
                .ok_or_else(|| paro_error::out_of_range("day offset overflow"))?
        } else {
            days
        };
        let value = i64::from(date)
            .checked_add(days)
            .and_then(|value| i32::try_from(value).ok())
            .ok_or_else(|| paro_error::out_of_range("DATE arithmetic overflow"))?;
        result.set_i32(row, value);
    }
    Ok(())
}

pub fn get_date_add_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("date_add".to_string());
    set.add_function(ScalarFunction::new(
        "date_add".to_string(),
        vec![LogicalType::Date, LogicalType::BigInt],
        LogicalType::Date,
        date_add_days_impl,
    ));
    set
}

pub fn get_date_sub_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("date_sub".to_string());
    set.add_function(ScalarFunction::new(
        "date_sub".to_string(),
        vec![LogicalType::Date, LogicalType::BigInt],
        LogicalType::Date,
        date_sub_days_impl,
    ));
    set
}

fn datediff_date_impl(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let start = column(input, 0, "start date")?;
    let end = column(input, 1, "end date")?;
    let count = input.size();
    result.set_count(count);
    for row in 0..count {
        if start.is_null(row) || end.is_null(row) {
            result.set_null(row, true);
        } else {
            let start = start
                .get_i32(row)
                .ok_or_else(|| paro_error::internal("DATE vector has no physical INT32 value"))?;
            let end = end
                .get_i32(row)
                .ok_or_else(|| paro_error::internal("DATE vector has no physical INT32 value"))?;
            result.set_i64(row, i64::from(end) - i64::from(start));
        }
    }
    Ok(())
}

fn datediff_timestamp_impl(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let start = column(input, 0, "start timestamp")?;
    let end = column(input, 1, "end timestamp")?;
    let count = input.size();
    result.set_count(count);
    for row in 0..count {
        if start.is_null(row) || end.is_null(row) {
            result.set_null(row, true);
        } else {
            let start = start.get_i64(row).ok_or_else(|| {
                paro_error::internal("TIMESTAMP vector has no physical INT64 value")
            })?;
            let end = end.get_i64(row).ok_or_else(|| {
                paro_error::internal("TIMESTAMP vector has no physical INT64 value")
            })?;
            result.set_i64(row, (end - start) / MICROS_PER_DAY);
        }
    }
    Ok(())
}

pub fn get_date_diff_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("datediff".to_string());
    set.add_function(ScalarFunction::new(
        "datediff".to_string(),
        vec![LogicalType::Date, LogicalType::Date],
        LogicalType::BigInt,
        datediff_date_impl,
    ));
    set.add_function(ScalarFunction::new(
        "datediff".to_string(),
        vec![LogicalType::Timestamp, LogicalType::Timestamp],
        LogicalType::BigInt,
        datediff_timestamp_impl,
    ));
    set
}

fn age_impl(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    let end = column(input, 0, "end timestamp")?;
    let start = column(input, 1, "start timestamp")?;
    let count = input.size();
    result.set_count(count);
    for row in 0..count {
        if end.is_null(row) || start.is_null(row) {
            result.set_null(row, true);
        } else {
            let end = end.get_i64(row).ok_or_else(|| {
                paro_error::internal("TIMESTAMP vector has no physical INT64 value")
            })?;
            let start = start.get_i64(row).ok_or_else(|| {
                paro_error::internal("TIMESTAMP vector has no physical INT64 value")
            })?;
            result.set_i64(row, (end - start) / MICROS_PER_DAY);
        }
    }
    Ok(())
}

pub fn get_age_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("age".to_string());
    set.add_function(ScalarFunction::new(
        "age".to_string(),
        vec![LogicalType::Timestamp, LogicalType::Timestamp],
        LogicalType::BigInt,
        age_impl,
    ));
    set
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;
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

    fn date_vector(values: &[i32]) -> Vector {
        let mut vector = paro_common::test_utils::test_vector(LogicalType::Date);
        vector.set_count(values.len());
        for (row, value) in values.iter().copied().enumerate() {
            vector.set_i32(row, value);
        }
        vector
    }

    fn interval_vector(values: &[IntervalParts]) -> Vector {
        let mut vector = paro_common::test_utils::test_vector(LogicalType::Interval);
        vector.set_count(values.len());
        for (row, (months, days, micros)) in values.iter().copied().enumerate() {
            vector.set_value(row, &Value::Interval(months, days, micros));
        }
        vector
    }

    #[test]
    fn date_add_and_sub_use_physical_int32_dates() {
        let dates = date_vector(&[0, 10, 100]);
        let days = paro_common::test_utils::test_i64_vector(&[1, 5, -10]);
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![dates, days]);
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Date);
        date_add_days_impl(&chunk, &state, &mut result).unwrap();
        assert_eq!(result.get_i32(0), Some(1));
        assert_eq!(result.get_i32(1), Some(15));
        assert_eq!(result.get_i32(2), Some(90));
    }

    #[test]
    fn datediff_uses_physical_int32_dates() {
        let start = date_vector(&[0, 10, 100]);
        let end = date_vector(&[10, 20, 50]);
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![start, end]);
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::BigInt);
        datediff_date_impl(&chunk, &state, &mut result).unwrap();
        assert_eq!(result.get_i64(0), Some(10));
        assert_eq!(result.get_i64(1), Some(10));
        assert_eq!(result.get_i64(2), Some(-50));
    }

    #[test]
    fn date_interval_arithmetic_clamps_calendar_months() {
        let january_31 = i32::try_from(ymd_to_days(2024, 1, 31)).unwrap();
        let dates = date_vector(&[january_31]);
        let intervals = interval_vector(&[(1, 0, 0)]);
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![dates, intervals]);
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Timestamp);
        date_interval_add_impl(&chunk, &state, &mut result).unwrap();
        let february_29 = ymd_to_days(2024, 2, 29) * MICROS_PER_DAY;
        assert_eq!(result.get_i64(0), Some(february_29));
    }

    #[test]
    fn timestamp_interval_arithmetic_preserves_time_of_day() {
        let january_31 = ymd_to_days(2023, 1, 31) * MICROS_PER_DAY;
        let timestamps =
            paro_common::test_utils::test_i64_vector(&[january_31 + 12 * 60 * 60 * 1_000_000]);
        let intervals = interval_vector(&[(1, 1, 5)]);
        let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![timestamps, intervals]);
        let state = MockState;
        let mut result = paro_common::test_utils::test_vector(LogicalType::Timestamp);
        timestamp_interval_add_impl(&chunk, &state, &mut result).unwrap();
        let expected = ymd_to_days(2023, 3, 1) * MICROS_PER_DAY + 12 * 60 * 60 * 1_000_000 + 5;
        assert_eq!(result.get_i64(0), Some(expected));
    }

    #[test]
    fn standard_operators_bind_temporal_interval_overloads() {
        let mut subtract = ScalarFunctionSet::new("-".to_string());
        register_temporal_arithmetic_functions(&mut subtract);
        let (function, arguments) = subtract
            .bind(&[LogicalType::Date, LogicalType::Interval])
            .unwrap();
        assert_eq!(arguments, vec![LogicalType::Date, LogicalType::Interval]);
        assert_eq!(function.return_type, LogicalType::Timestamp);
    }
}
