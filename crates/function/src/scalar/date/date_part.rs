// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Date part extraction functions.
//!
//!
//!
//! ## Functions
//! - `extract(field FROM date/timestamp)` - Extract date/time component
//! - `date_part(field, date/timestamp)` - Same as extract
//! - `year(date)`, `month(date)`, `day(date)` - Shorthand extractors
//! - `hour(timestamp)`, `minute(timestamp)`, `second(timestamp)` - Time extractors
//! - `dayofweek(date)`, `dayofyear(date)`, `week(date)`, `quarter(date)`

use crate::{ExpressionState, ScalarFunction, ScalarFunctionSet};
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_storage::statistics::{BaseStatistics, NumericStats};

/// Microseconds per second
const MICROS_PER_SECOND: i64 = 1_000_000;
/// Microseconds per minute
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
/// Microseconds per hour
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
/// Microseconds per day
const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;

/// Date part specifier enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatePartSpecifier {
    Year,
    Month,
    Day,
    Decade,
    Century,
    Millennium,
    Microseconds,
    Milliseconds,
    Second,
    Minute,
    Hour,
    Epoch,
    DayOfWeek,
    IsoDayOfWeek,
    DayOfYear,
    Week,
    IsoYear,
    Quarter,
    YearWeek,
    Era,
    JulianDay,
}

impl DatePartSpecifier {
    /// Parse a date part specifier from a string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "year" | "years" | "y" => Some(Self::Year),
            "month" | "months" | "mon" => Some(Self::Month),
            "day" | "days" | "d" => Some(Self::Day),
            "decade" | "decades" => Some(Self::Decade),
            "century" | "centuries" => Some(Self::Century),
            "millennium" | "millennia" => Some(Self::Millennium),
            "microsecond" | "microseconds" | "us" => Some(Self::Microseconds),
            "millisecond" | "milliseconds" | "ms" => Some(Self::Milliseconds),
            "second" | "seconds" | "s" => Some(Self::Second),
            "minute" | "minutes" | "m" => Some(Self::Minute),
            "hour" | "hours" | "h" => Some(Self::Hour),
            "epoch" => Some(Self::Epoch),
            "dow" | "dayofweek" => Some(Self::DayOfWeek),
            "isodow" => Some(Self::IsoDayOfWeek),
            "doy" | "dayofyear" => Some(Self::DayOfYear),
            "week" | "weeks" | "w" => Some(Self::Week),
            "isoyear" => Some(Self::IsoYear),
            "quarter" | "quarters" | "q" => Some(Self::Quarter),
            "yearweek" => Some(Self::YearWeek),
            "era" => Some(Self::Era),
            "julian" | "julianday" => Some(Self::JulianDay),
            _ => None,
        }
    }
}

// ============================================================================
// Date Conversion Utilities
// ============================================================================

/// Convert days since Unix epoch to (year, month, day).
fn days_to_ymd(days: i64) -> (i32, u32, u32) {
    let z = days + 719468;
    let era = if z >= 0 {
        z / 146097
    } else {
        (z - 146096) / 146097
    };
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = (yoe as i64 + era * 400) as i32;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Convert microseconds since epoch to datetime components.
fn micros_to_datetime(micros: i64) -> (i32, u32, u32, u32, u32, u32, u32) {
    let (days, time_micros) = if micros >= 0 {
        (micros / MICROS_PER_DAY, micros % MICROS_PER_DAY)
    } else {
        let days = (micros - MICROS_PER_DAY + 1) / MICROS_PER_DAY;
        let time_micros = micros - days * MICROS_PER_DAY;
        (days, time_micros)
    };

    let (year, month, day) = days_to_ymd(days);
    let hours = (time_micros / MICROS_PER_HOUR) as u32;
    let remaining = time_micros % MICROS_PER_HOUR;
    let minutes = (remaining / MICROS_PER_MINUTE) as u32;
    let remaining = remaining % MICROS_PER_MINUTE;
    let seconds = (remaining / MICROS_PER_SECOND) as u32;
    let micros = (remaining % MICROS_PER_SECOND) as u32;

    (year, month, day, hours, minutes, seconds, micros)
}

/// Check if a year is a leap year.
#[inline]
fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Get the day of year (1-366).
fn day_of_year(year: i32, month: u32, day: u32) -> u32 {
    const DAYS_BEFORE_MONTH: [u32; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let mut doy = DAYS_BEFORE_MONTH[month as usize - 1] + day;
    if month > 2 && is_leap_year(year) {
        doy += 1;
    }
    doy
}

/// Get the ISO day of week (Monday = 1, Sunday = 7).
fn iso_day_of_week(days: i64) -> i64 {
    // Unix epoch (1970-01-01) was a Thursday (4)
    let dow = ((days % 7) + 4) % 7;
    if dow == 0 {
        7
    } else {
        dow
    }
}

/// Get the day of week (Sunday = 0, Saturday = 6).
fn day_of_week(days: i64) -> i64 {
    iso_day_of_week(days) % 7
}

/// Get the ISO week number (1-53).
fn iso_week_number(year: i32, month: u32, day: u32, days: i64) -> i64 {
    let doy = day_of_year(year, month, day) as i64;
    let dow = iso_day_of_week(days);
    // ISO week starts on Monday
    let week = (doy - dow + 10) / 7;
    if week < 1 {
        // Last week of previous year
        52
    } else if week > 52 {
        // Check if it's week 1 of next year
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if doy > days_in_year - 3 {
            1
        } else {
            week
        }
    } else {
        week
    }
}

/// Get the quarter (1-4).
fn quarter(month: u32) -> i64 {
    ((month - 1) / 3 + 1) as i64
}

// ============================================================================
// Extract from Date
// ============================================================================

/// Extract a date part from a date (days since epoch).
pub fn extract_from_date(days: i64, part: DatePartSpecifier) -> i64 {
    let (year, month, day) = days_to_ymd(days);

    match part {
        DatePartSpecifier::Year => year as i64,
        DatePartSpecifier::Month => month as i64,
        DatePartSpecifier::Day => day as i64,
        DatePartSpecifier::Decade => (year / 10) as i64,
        DatePartSpecifier::Century => {
            if year > 0 {
                ((year - 1) / 100 + 1) as i64
            } else {
                (year / 100 - 1) as i64
            }
        }
        DatePartSpecifier::Millennium => {
            if year > 0 {
                ((year - 1) / 1000 + 1) as i64
            } else {
                (year / 1000 - 1) as i64
            }
        }
        DatePartSpecifier::DayOfWeek => day_of_week(days),
        DatePartSpecifier::IsoDayOfWeek => iso_day_of_week(days),
        DatePartSpecifier::DayOfYear => day_of_year(year, month, day) as i64,
        DatePartSpecifier::Week => iso_week_number(year, month, day, days),
        DatePartSpecifier::Quarter => quarter(month),
        DatePartSpecifier::Epoch => days * 86400, // Seconds since epoch
        DatePartSpecifier::JulianDay => days + 2440588, // Julian day number
        DatePartSpecifier::Era => {
            if year > 0 {
                1
            } else {
                0
            }
        }
        DatePartSpecifier::IsoYear => year as i64, // Simplified
        DatePartSpecifier::YearWeek => {
            let week = iso_week_number(year, month, day, days);
            year as i64 * 100 + week
        }
        // Time parts are 0 for dates
        DatePartSpecifier::Hour
        | DatePartSpecifier::Minute
        | DatePartSpecifier::Second
        | DatePartSpecifier::Microseconds
        | DatePartSpecifier::Milliseconds => 0,
    }
}

// ============================================================================
// Extract from Timestamp
// ============================================================================

/// Extract a date part from a timestamp (microseconds since epoch).
pub fn extract_from_timestamp(micros: i64, part: DatePartSpecifier) -> i64 {
    let (year, month, day, hour, minute, second, us) = micros_to_datetime(micros);
    let days = micros / MICROS_PER_DAY;

    match part {
        DatePartSpecifier::Year => year as i64,
        DatePartSpecifier::Month => month as i64,
        DatePartSpecifier::Day => day as i64,
        DatePartSpecifier::Hour => hour as i64,
        DatePartSpecifier::Minute => minute as i64,
        DatePartSpecifier::Second => second as i64,
        DatePartSpecifier::Microseconds => (second as i64 * MICROS_PER_SECOND) + us as i64,
        DatePartSpecifier::Milliseconds => (second as i64 * 1000) + (us as i64 / 1000),
        DatePartSpecifier::Decade => (year / 10) as i64,
        DatePartSpecifier::Century => {
            if year > 0 {
                ((year - 1) / 100 + 1) as i64
            } else {
                (year / 100 - 1) as i64
            }
        }
        DatePartSpecifier::Millennium => {
            if year > 0 {
                ((year - 1) / 1000 + 1) as i64
            } else {
                (year / 1000 - 1) as i64
            }
        }
        DatePartSpecifier::DayOfWeek => day_of_week(days),
        DatePartSpecifier::IsoDayOfWeek => iso_day_of_week(days),
        DatePartSpecifier::DayOfYear => day_of_year(year, month, day) as i64,
        DatePartSpecifier::Week => iso_week_number(year, month, day, days),
        DatePartSpecifier::Quarter => quarter(month),
        DatePartSpecifier::Epoch => micros / MICROS_PER_SECOND,
        DatePartSpecifier::JulianDay => days + 2440588,
        DatePartSpecifier::Era => {
            if year > 0 {
                1
            } else {
                0
            }
        }
        DatePartSpecifier::IsoYear => year as i64,
        DatePartSpecifier::YearWeek => {
            let week = iso_week_number(year, month, day, days);
            year as i64 * 100 + week
        }
    }
}

/// Extract a date part from a time (microseconds since midnight).
pub fn extract_from_time(time_micros: i64, part: DatePartSpecifier) -> i64 {
    let hours = time_micros / MICROS_PER_HOUR;
    let remaining = time_micros % MICROS_PER_HOUR;
    let minutes = remaining / MICROS_PER_MINUTE;
    let remaining = remaining % MICROS_PER_MINUTE;
    let seconds = remaining / MICROS_PER_SECOND;
    let us = remaining % MICROS_PER_SECOND;

    match part {
        DatePartSpecifier::Hour => hours,
        DatePartSpecifier::Minute => minutes,
        DatePartSpecifier::Second => seconds,
        DatePartSpecifier::Microseconds => seconds * MICROS_PER_SECOND + us,
        DatePartSpecifier::Milliseconds => seconds * 1000 + us / 1000,
        DatePartSpecifier::Epoch => time_micros / MICROS_PER_SECOND,
        _ => 0, // Date parts are 0 for time
    }
}

// ============================================================================
// Macro for generating extract functions
// ============================================================================

macro_rules! define_extract_fn {
    ($name:ident, $part:expr, Date, $extract_fn:ident) => {
        fn $name(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
            let count = input.size();
            let src = input
                .column(0)
                .ok_or_else(|| paro_common::error::internal("Missing input column".to_string()))?;

            result.set_count(count);

            for i in 0..count {
                if src.is_null(i) {
                    result.validity_mut().set_null(i);
                } else {
                    let val = i64::from(src.get_i32(i).unwrap_or(0));
                    result.set_i64(i, $extract_fn(val, $part));
                }
            }

            Ok(())
        }
    };
    ($name:ident, $part:expr, $input_type:ident, $extract_fn:ident) => {
        fn $name(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
            let count = input.size();
            let src = input
                .column(0)
                .ok_or_else(|| paro_common::error::internal("Missing input column".to_string()))?;

            result.set_count(count);

            for i in 0..count {
                if src.is_null(i) {
                    result.validity_mut().set_null(i);
                } else {
                    let val = src.get_i64(i).unwrap_or(0);
                    result.set_i64(i, $extract_fn(val, $part));
                }
            }

            Ok(())
        }
    };
}

// ============================================================================
// Year extraction
// ============================================================================

fn year_from_date_statistics(inputs: &[&BaseStatistics]) -> Option<BaseStatistics> {
    let input = *inputs.first()?;
    let (Value::Date(minimum), Value::Date(maximum)) = (input.min_value()?, input.max_value()?)
    else {
        return None;
    };
    year_statistics(
        input,
        extract_from_date(i64::from(minimum), DatePartSpecifier::Year),
        extract_from_date(i64::from(maximum), DatePartSpecifier::Year),
    )
}

fn year_from_timestamp_statistics(inputs: &[&BaseStatistics]) -> Option<BaseStatistics> {
    let input = *inputs.first()?;
    let (Value::Timestamp(minimum), Value::Timestamp(maximum)) =
        (input.min_value()?, input.max_value()?)
    else {
        return None;
    };
    year_statistics(
        input,
        extract_from_timestamp(minimum, DatePartSpecifier::Year),
        extract_from_timestamp(maximum, DatePartSpecifier::Year),
    )
}

fn year_statistics(input: &BaseStatistics, minimum: i64, maximum: i64) -> Option<BaseStatistics> {
    if minimum > maximum {
        return None;
    }
    let mut output = NumericStats::create_empty(LogicalType::BigInt);
    NumericStats::update(&mut output, &Value::BigInt(minimum));
    NumericStats::update(&mut output, &Value::BigInt(maximum));
    if input.can_have_null() {
        output.set_has_null_fast();
    }
    let range = usize::try_from(maximum.checked_sub(minimum)?.checked_add(1)?).ok()?;
    output.set_distinct_count(input.get_distinct_count().min(range));
    Some(output)
}

define_extract_fn!(
    year_from_date,
    DatePartSpecifier::Year,
    Date,
    extract_from_date
);
define_extract_fn!(
    year_from_timestamp,
    DatePartSpecifier::Year,
    Timestamp,
    extract_from_timestamp
);

pub fn get_year_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("year".to_string());
    set.add_function(
        ScalarFunction::new(
            "year".to_string(),
            vec![LogicalType::Date],
            LogicalType::BigInt,
            year_from_date,
        )
        .with_statistics(year_from_date_statistics),
    );
    set.add_function(
        ScalarFunction::new(
            "year".to_string(),
            vec![LogicalType::Timestamp],
            LogicalType::BigInt,
            year_from_timestamp,
        )
        .with_statistics(year_from_timestamp_statistics),
    );
    set
}

// ============================================================================
// Month extraction
// ============================================================================

define_extract_fn!(
    month_from_date,
    DatePartSpecifier::Month,
    Date,
    extract_from_date
);
define_extract_fn!(
    month_from_timestamp,
    DatePartSpecifier::Month,
    Timestamp,
    extract_from_timestamp
);

pub fn get_month_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("month".to_string());
    set.add_function(ScalarFunction::new(
        "month".to_string(),
        vec![LogicalType::Date],
        LogicalType::BigInt,
        month_from_date,
    ));
    set.add_function(ScalarFunction::new(
        "month".to_string(),
        vec![LogicalType::Timestamp],
        LogicalType::BigInt,
        month_from_timestamp,
    ));
    set
}

// ============================================================================
// Day extraction
// ============================================================================

define_extract_fn!(
    day_from_date,
    DatePartSpecifier::Day,
    Date,
    extract_from_date
);
define_extract_fn!(
    day_from_timestamp,
    DatePartSpecifier::Day,
    Timestamp,
    extract_from_timestamp
);

pub fn get_day_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("day".to_string());
    set.add_function(ScalarFunction::new(
        "day".to_string(),
        vec![LogicalType::Date],
        LogicalType::BigInt,
        day_from_date,
    ));
    set.add_function(ScalarFunction::new(
        "day".to_string(),
        vec![LogicalType::Timestamp],
        LogicalType::BigInt,
        day_from_timestamp,
    ));
    set
}

// ============================================================================
// Hour extraction
// ============================================================================

define_extract_fn!(
    hour_from_timestamp,
    DatePartSpecifier::Hour,
    Timestamp,
    extract_from_timestamp
);
define_extract_fn!(
    hour_from_time,
    DatePartSpecifier::Hour,
    Time,
    extract_from_time
);

pub fn get_hour_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("hour".to_string());
    set.add_function(ScalarFunction::new(
        "hour".to_string(),
        vec![LogicalType::Timestamp],
        LogicalType::BigInt,
        hour_from_timestamp,
    ));
    set.add_function(ScalarFunction::new(
        "hour".to_string(),
        vec![LogicalType::Time],
        LogicalType::BigInt,
        hour_from_time,
    ));
    set
}

// ============================================================================
// Minute extraction
// ============================================================================

define_extract_fn!(
    minute_from_timestamp,
    DatePartSpecifier::Minute,
    Timestamp,
    extract_from_timestamp
);
define_extract_fn!(
    minute_from_time,
    DatePartSpecifier::Minute,
    Time,
    extract_from_time
);

pub fn get_minute_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("minute".to_string());
    set.add_function(ScalarFunction::new(
        "minute".to_string(),
        vec![LogicalType::Timestamp],
        LogicalType::BigInt,
        minute_from_timestamp,
    ));
    set.add_function(ScalarFunction::new(
        "minute".to_string(),
        vec![LogicalType::Time],
        LogicalType::BigInt,
        minute_from_time,
    ));
    set
}

// ============================================================================
// Second extraction
// ============================================================================

define_extract_fn!(
    second_from_timestamp,
    DatePartSpecifier::Second,
    Timestamp,
    extract_from_timestamp
);
define_extract_fn!(
    second_from_time,
    DatePartSpecifier::Second,
    Time,
    extract_from_time
);

pub fn get_second_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("second".to_string());
    set.add_function(ScalarFunction::new(
        "second".to_string(),
        vec![LogicalType::Timestamp],
        LogicalType::BigInt,
        second_from_timestamp,
    ));
    set.add_function(ScalarFunction::new(
        "second".to_string(),
        vec![LogicalType::Time],
        LogicalType::BigInt,
        second_from_time,
    ));
    set
}

// ============================================================================
// Day of week extraction
// ============================================================================

define_extract_fn!(
    dayofweek_from_date,
    DatePartSpecifier::DayOfWeek,
    Date,
    extract_from_date
);
define_extract_fn!(
    dayofweek_from_timestamp,
    DatePartSpecifier::DayOfWeek,
    Timestamp,
    extract_from_timestamp
);

pub fn get_dayofweek_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("dayofweek".to_string());
    set.add_function(ScalarFunction::new(
        "dayofweek".to_string(),
        vec![LogicalType::Date],
        LogicalType::BigInt,
        dayofweek_from_date,
    ));
    set.add_function(ScalarFunction::new(
        "dayofweek".to_string(),
        vec![LogicalType::Timestamp],
        LogicalType::BigInt,
        dayofweek_from_timestamp,
    ));
    set
}

// ============================================================================
// Day of year extraction
// ============================================================================

define_extract_fn!(
    dayofyear_from_date,
    DatePartSpecifier::DayOfYear,
    Date,
    extract_from_date
);
define_extract_fn!(
    dayofyear_from_timestamp,
    DatePartSpecifier::DayOfYear,
    Timestamp,
    extract_from_timestamp
);

pub fn get_dayofyear_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("dayofyear".to_string());
    set.add_function(ScalarFunction::new(
        "dayofyear".to_string(),
        vec![LogicalType::Date],
        LogicalType::BigInt,
        dayofyear_from_date,
    ));
    set.add_function(ScalarFunction::new(
        "dayofyear".to_string(),
        vec![LogicalType::Timestamp],
        LogicalType::BigInt,
        dayofyear_from_timestamp,
    ));
    set
}

// ============================================================================
// Week extraction
// ============================================================================

define_extract_fn!(
    week_from_date,
    DatePartSpecifier::Week,
    Date,
    extract_from_date
);
define_extract_fn!(
    week_from_timestamp,
    DatePartSpecifier::Week,
    Timestamp,
    extract_from_timestamp
);

pub fn get_week_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("week".to_string());
    set.add_function(ScalarFunction::new(
        "week".to_string(),
        vec![LogicalType::Date],
        LogicalType::BigInt,
        week_from_date,
    ));
    set.add_function(ScalarFunction::new(
        "week".to_string(),
        vec![LogicalType::Timestamp],
        LogicalType::BigInt,
        week_from_timestamp,
    ));
    set
}

// ============================================================================
// Quarter extraction
// ============================================================================

define_extract_fn!(
    quarter_from_date,
    DatePartSpecifier::Quarter,
    Date,
    extract_from_date
);
define_extract_fn!(
    quarter_from_timestamp,
    DatePartSpecifier::Quarter,
    Timestamp,
    extract_from_timestamp
);

pub fn get_quarter_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("quarter".to_string());
    set.add_function(ScalarFunction::new(
        "quarter".to_string(),
        vec![LogicalType::Date],
        LogicalType::BigInt,
        quarter_from_date,
    ));
    set.add_function(ScalarFunction::new(
        "quarter".to_string(),
        vec![LogicalType::Timestamp],
        LogicalType::BigInt,
        quarter_from_timestamp,
    ));
    set
}

// ============================================================================
// EXTRACT function (generic)
// ============================================================================

/// Get the `extract` function set.
/// Note: In SQL, EXTRACT is typically used as EXTRACT(field FROM source).
/// This is handled by the parser, which converts it to a function call.
pub fn get_extract_functions() -> ScalarFunctionSet {
    // For now, we provide the shorthand functions (year, month, day, etc.)
    // The full EXTRACT syntax requires parser support to convert
    // EXTRACT(YEAR FROM date) to year(date)
    ScalarFunctionSet::new("extract".to_string())
}

/// Get the `date_part` function set.
/// date_part(field, source) is equivalent to EXTRACT(field FROM source)
pub fn get_date_part_functions() -> ScalarFunctionSet {
    // Similar to extract, this requires string-based field selection
    // which would need FunctionData to store the field specifier
    ScalarFunctionSet::new("date_part".to_string())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_days_to_ymd() {
        // Unix epoch
        assert_eq!(days_to_ymd(0), (1970, 1, 1));
        // 2024-01-01
        let days_2024 = 19723; // Approximate
        let (y, m, d) = days_to_ymd(days_2024);
        assert_eq!(y, 2024);
        assert_eq!(m, 1);
        assert_eq!(d, 1);
    }

    #[test]
    fn test_extract_year() {
        // 2024-06-15 = 19889 days since epoch (approximate)
        let days = 19889_i64;
        let year = extract_from_date(days, DatePartSpecifier::Year);
        assert!(year >= 2024 && year <= 2025);
    }

    #[test]
    fn year_kernel_derives_a_bounded_output_domain() {
        let mut input = NumericStats::create_empty(LogicalType::Date);
        NumericStats::update(&mut input, &Value::Date(0));
        NumericStats::update(&mut input, &Value::Date(730));
        input.set_has_null_fast();

        let output = year_from_date_statistics(&[&input]).expect("year statistics");

        assert_eq!(output.min_value(), Some(Value::BigInt(1970)));
        assert_eq!(output.max_value(), Some(Value::BigInt(1972)));
        assert!(output.can_have_null());
        assert!(output.can_have_no_null());
    }

    #[test]
    fn test_extract_month() {
        let days = 0_i64; // 1970-01-01
        assert_eq!(extract_from_date(days, DatePartSpecifier::Month), 1);
    }

    #[test]
    fn test_extract_day() {
        let days = 0_i64; // 1970-01-01
        assert_eq!(extract_from_date(days, DatePartSpecifier::Day), 1);
    }

    #[test]
    fn test_extract_hour_from_timestamp() {
        // 12:30:45 on 1970-01-01
        let micros = 12 * MICROS_PER_HOUR + 30 * MICROS_PER_MINUTE + 45 * MICROS_PER_SECOND;
        assert_eq!(extract_from_timestamp(micros, DatePartSpecifier::Hour), 12);
        assert_eq!(
            extract_from_timestamp(micros, DatePartSpecifier::Minute),
            30
        );
        assert_eq!(
            extract_from_timestamp(micros, DatePartSpecifier::Second),
            45
        );
    }

    #[test]
    fn test_day_of_week() {
        // 1970-01-01 was a Thursday
        // Sunday = 0, Thursday = 4
        assert_eq!(day_of_week(0), 4);
    }

    #[test]
    fn test_iso_day_of_week() {
        // 1970-01-01 was a Thursday
        // Monday = 1, Thursday = 4
        assert_eq!(iso_day_of_week(0), 4);
    }

    #[test]
    fn test_quarter() {
        assert_eq!(quarter(1), 1);
        assert_eq!(quarter(3), 1);
        assert_eq!(quarter(4), 2);
        assert_eq!(quarter(6), 2);
        assert_eq!(quarter(7), 3);
        assert_eq!(quarter(9), 3);
        assert_eq!(quarter(10), 4);
        assert_eq!(quarter(12), 4);
    }

    #[test]
    fn test_day_of_year() {
        // January 1st
        assert_eq!(day_of_year(2024, 1, 1), 1);
        // February 1st (31 days in January)
        assert_eq!(day_of_year(2024, 2, 1), 32);
        // March 1st in leap year (31 + 29 = 60)
        assert_eq!(day_of_year(2024, 3, 1), 61);
        // March 1st in non-leap year (31 + 28 = 59)
        assert_eq!(day_of_year(2023, 3, 1), 60);
    }

    #[test]
    fn test_date_part_specifier_from_str() {
        assert_eq!(
            DatePartSpecifier::from_str("year"),
            Some(DatePartSpecifier::Year)
        );
        assert_eq!(
            DatePartSpecifier::from_str("MONTH"),
            Some(DatePartSpecifier::Month)
        );
        assert_eq!(
            DatePartSpecifier::from_str("Day"),
            Some(DatePartSpecifier::Day)
        );
        assert_eq!(DatePartSpecifier::from_str("invalid"), None);
    }
}
