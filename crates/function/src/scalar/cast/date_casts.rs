//! Date/Time/Timestamp casts.
//!
//!
//!
//! ## Dependencies Check
//! - Vector: ✅
//! - StringHeap: ✅
//!
//! ## Supported Conversions
//! - Date <-> VARCHAR (YYYY-MM-DD format)
//! - Time <-> VARCHAR (HH:MM:SS[.ffffff] format)
//! - Timestamp <-> VARCHAR (YYYY-MM-DD HH:MM:SS.ffffff format)
//! - TimestampTz <-> VARCHAR (YYYY-MM-DD HH:MM:SS.ffffff+00 format)
//! - Interval <-> VARCHAR (Verbose, ISO 8601, and Time formats)

use crate::scalar::cast::CastExecCtx;
use paro_common::error::{self as paro_error, Result};
use paro_common::vector::Vector;
use std::io::Write;

/// Days in each month (non-leap year)
const DAYS_IN_MONTH: [i32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

/// Check if a year is a leap year.
#[inline]
fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Convert days since Unix epoch to (year, month, day).
/// Returns (year, month, day) where month is 1-12 and day is 1-31.
fn days_to_ymd(days: i64) -> (i32, u32, u32) {
    // http://howardhinnant.github.io/date_algorithms.html

    // Shift epoch from 1970-01-01 to 0000-03-01 (makes leap year calculation easier)
    let z = days + 719468; // Days from 0000-03-01 to 1970-01-01

    // Handle negative values
    let era = if z >= 0 {
        z / 146097
    } else {
        (z - 146096) / 146097
    };
    let doe = (z - era * 146097) as u32; // day of era: 0..146096
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // year of era: 0..399
    let y = (yoe as i64 + era * 400) as i32;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year: 0..365
    let mp = (5 * doy + 2) / 153; // month of year adjusted for March start
    let d = doy - (153 * mp + 2) / 5 + 1; // day: 1..31
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // month: 1..12
    let y = if m <= 2 { y + 1 } else { y };

    (y, m, d)
}

/// Convert (year, month, day) to days since Unix epoch.
fn ymd_to_days(year: i32, month: u32, day: u32) -> i64 {
    // http://howardhinnant.github.io/date_algorithms.html

    // Validate inputs
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return 0;
    }

    // Adjust year and month (March is month 1 of the cycle)
    let y = if month <= 2 {
        year as i64 - 1
    } else {
        year as i64
    };
    let m = if month <= 2 { month + 9 } else { month - 3 };

    // Calculate era (400-year cycles)
    let era = if y >= 0 { y / 400 } else { (y - 399) / 400 };
    let yoe = (y - era * 400) as u32; // year of era: 0..399
    let doy = (153 * m + 2) / 5 + day - 1; // day of year: 0..365
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // day of era: 0..146096

    // Days since epoch (0000-03-01)
    let days_from_epoch_0 = era * 146097 + doe as i64;

    // Shift epoch from 0000-03-01 to 1970-01-01
    days_from_epoch_0 - 719468
}

/// Parse a date string in YYYY-MM-DD format.
fn parse_date(s: &str) -> Option<i64> {
    let s = s.trim();

    // Handle special values
    match s.to_lowercase().as_str() {
        "infinity" | "inf" => return Some(i64::MAX),
        "-infinity" | "-inf" => return Some(i64::MIN),
        _ => {}
    }

    // Parse YYYY-MM-DD format
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return None;
    }

    // Handle negative years (BC dates)
    let (year_str, is_negative) = if let Some(s) = s.strip_prefix('-') {
        // Re-parse for negative year
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() != 3 {
            return None;
        }
        (parts[0], true)
    } else {
        (parts[0], false)
    };

    let year: i32 = year_str.parse().ok()?;
    let year = if is_negative { -year } else { year };

    let month: u32 = if is_negative {
        let s = &s[1..];
        let parts: Vec<&str> = s.split('-').collect();
        parts[1].parse().ok()?
    } else {
        parts[1].parse().ok()?
    };

    let day: u32 = if is_negative {
        let s = &s[1..];
        let parts: Vec<&str> = s.split('-').collect();
        parts[2].parse().ok()?
    } else {
        parts[2].parse().ok()?
    };

    // Validate month and day
    if !(1..=12).contains(&month) {
        return None;
    }

    let max_day = if is_leap_year(year) && month == 2 {
        29
    } else {
        DAYS_IN_MONTH[month as usize - 1] as u32
    };

    if day < 1 || day > max_day {
        return None;
    }

    Some(ymd_to_days(year, month, day))
}

pub fn parse_date_text(s: &str) -> Option<i64> {
    parse_date(s)
}

/// Format a date (days since epoch) as YYYY-MM-DD string.
fn format_date(days: i64, buf: &mut [u8]) -> usize {
    // Handle special values
    if days == i64::MAX {
        let s = b"infinity";
        buf[..s.len()].copy_from_slice(s);
        return s.len();
    }
    if days == i64::MIN {
        let s = b"-infinity";
        buf[..s.len()].copy_from_slice(s);
        return s.len();
    }

    let (year, month, day) = days_to_ymd(days);

    // Format as YYYY-MM-DD
    let mut cursor = std::io::Cursor::new(&mut buf[..]);

    if year < 0 {
        // BC date
        write!(cursor, "{:05}-{:02}-{:02}", year, month, day).ok();
    } else {
        write!(cursor, "{:04}-{:02}-{:02}", year, month, day).ok();
    }

    cursor.position() as usize
}

/// Cast Date to VARCHAR.
pub fn date_to_varchar(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    result.set_count(count);
    let mut buf = [0u8; 32];

    for i in 0..count {
        if !input.is_null(i) {
            let days = unsafe { input.get_flat::<i64>(i) };
            let len = format_date(days, &mut buf);
            let s = std::str::from_utf8(&buf[..len]).unwrap();
            result.set_string(i, s);
        } else {
            result.set_null(i, true);
        }
    }

    Ok(true)
}

/// Cast VARCHAR to Date.
pub fn varchar_to_date(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let mut all_success = true;
    result.set_count(count);

    for i in 0..count {
        if !input.is_null(i) {
            if let Some(s) = input.get_string(i) {
                match parse_date(s) {
                    Some(days) => {
                        unsafe { result.set_flat::<i64>(i, days) };
                    }
                    None => {
                        if ctx.try_cast {
                            result.set_null(i, true);
                            all_success = false;
                        } else {
                            return Err(paro_error::invalid_value("DATE", s));
                        }
                    }
                }
            }
        } else {
            result.set_null(i, true);
        }
    }

    Ok(all_success)
}

// ============================================================================
// Timestamp <-> VARCHAR Conversions
// ============================================================================

/// Microseconds per second
const MICROS_PER_SECOND: i64 = 1_000_000;
/// Microseconds per minute
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
/// Microseconds per hour
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
/// Microseconds per day
const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;

/// Convert microseconds since epoch to (year, month, day, hour, minute, second, micros).
fn micros_to_datetime(micros: i64) -> (i32, u32, u32, u32, u32, u32, u32) {
    // Split into days and time-of-day
    let (days, time_micros) = if micros >= 0 {
        (micros / MICROS_PER_DAY, micros % MICROS_PER_DAY)
    } else {
        // Handle negative microseconds (before epoch)
        let days = (micros - MICROS_PER_DAY + 1) / MICROS_PER_DAY;
        let time_micros = micros - days * MICROS_PER_DAY;
        (days, time_micros)
    };

    // Get date components
    let (year, month, day) = days_to_ymd(days);

    // Get time components
    let hours = (time_micros / MICROS_PER_HOUR) as u32;
    let remaining = time_micros % MICROS_PER_HOUR;
    let minutes = (remaining / MICROS_PER_MINUTE) as u32;
    let remaining = remaining % MICROS_PER_MINUTE;
    let seconds = (remaining / MICROS_PER_SECOND) as u32;
    let micros = (remaining % MICROS_PER_SECOND) as u32;

    (year, month, day, hours, minutes, seconds, micros)
}

/// Convert (year, month, day, hour, minute, second, micros) to microseconds since epoch.
fn datetime_to_micros(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    micros: u32,
) -> i64 {
    let days = ymd_to_days(year, month, day);
    let time_micros = hour as i64 * MICROS_PER_HOUR
        + minute as i64 * MICROS_PER_MINUTE
        + second as i64 * MICROS_PER_SECOND
        + micros as i64;
    days * MICROS_PER_DAY + time_micros
}

/// Parse a timestamp string in various formats.
/// Supports: YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, YYYY-MM-DD HH:MM:SS.ffffff
fn parse_timestamp(s: &str) -> Option<i64> {
    let s = s.trim();

    // Handle special values
    match s.to_lowercase().as_str() {
        "infinity" | "inf" => return Some(i64::MAX),
        "-infinity" | "-inf" => return Some(i64::MIN),
        _ => {}
    }

    // Split date and time parts
    let parts: Vec<&str> = s.split([' ', 'T']).collect();

    // Parse date part
    let date_str = parts.first()?;
    let date_parts: Vec<&str> = date_str.split('-').collect();
    if date_parts.len() != 3 {
        return None;
    }

    let year: i32 = date_parts[0].parse().ok()?;
    let month: u32 = date_parts[1].parse().ok()?;
    let day: u32 = date_parts[2].parse().ok()?;

    // Validate date
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    let max_day = if is_leap_year(year) && month == 2 {
        29
    } else {
        DAYS_IN_MONTH[month as usize - 1] as u32
    };

    if day > max_day {
        return None;
    }

    // Parse time part (if present)
    let (hour, minute, second, micros) = if parts.len() > 1 {
        let time_str = parts[1];

        // Split time and fractional seconds
        let (time_main, frac) = if let Some(dot_pos) = time_str.find('.') {
            (&time_str[..dot_pos], Some(&time_str[dot_pos + 1..]))
        } else {
            (time_str, None)
        };

        let time_parts: Vec<&str> = time_main.split(':').collect();
        if time_parts.len() < 2 || time_parts.len() > 3 {
            return None;
        }

        let h: u32 = time_parts[0].parse().ok()?;
        let m: u32 = time_parts[1].parse().ok()?;
        let s: u32 = if time_parts.len() == 3 {
            time_parts[2].parse().ok()?
        } else {
            0
        };

        // Parse fractional seconds (microseconds)
        let us: u32 = if let Some(frac_str) = frac {
            // Pad or truncate to 6 digits
            let frac_str = if frac_str.len() > 6 {
                &frac_str[..6]
            } else {
                frac_str
            };
            let padded = format!("{:0<6}", frac_str);
            padded.parse().ok()?
        } else {
            0
        };

        // Validate time
        if h >= 24 || m >= 60 || s >= 60 {
            return None;
        }

        (h, m, s, us)
    } else {
        (0, 0, 0, 0)
    };

    Some(datetime_to_micros(
        year, month, day, hour, minute, second, micros,
    ))
}

pub fn parse_timestamp_text(s: &str) -> Option<i64> {
    parse_timestamp(s)
}

/// Parse a time-of-day string in HH:MM[:SS][.ffffff] format.
/// Returns microseconds since midnight.
fn parse_time_of_day(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    if s.starts_with('-') {
        return None;
    }

    let (time_main, frac) = if let Some(dot_pos) = s.find('.') {
        (&s[..dot_pos], Some(&s[dot_pos + 1..]))
    } else {
        (s, None)
    };

    let parts: Vec<&str> = time_main.split(':').collect();
    if parts.len() < 2 || parts.len() > 3 {
        return None;
    }

    let h: u32 = parts[0].parse().ok()?;
    let m: u32 = parts[1].parse().ok()?;
    let s: u32 = if parts.len() == 3 {
        parts[2].parse().ok()?
    } else {
        0
    };

    if h >= 24 || m >= 60 || s >= 60 {
        return None;
    }

    let us: u32 = if let Some(frac_str) = frac {
        let frac_str = if frac_str.len() > 6 {
            &frac_str[..6]
        } else {
            frac_str
        };
        let padded = format!("{:0<6}", frac_str);
        padded.parse().ok()?
    } else {
        0
    };

    Some(
        h as i64 * MICROS_PER_HOUR
            + m as i64 * MICROS_PER_MINUTE
            + s as i64 * MICROS_PER_SECOND
            + us as i64,
    )
}

pub fn parse_time_text(s: &str) -> Option<i64> {
    parse_time_of_day(s)
}

fn parse_tz_offset_minutes(tz: &str) -> Option<i32> {
    let tz = tz.trim();
    if tz.is_empty() {
        return Some(0);
    }
    if tz.eq_ignore_ascii_case("z") {
        return Some(0);
    }

    let sign = if tz.starts_with('+') {
        1i32
    } else if tz.starts_with('-') {
        -1i32
    } else {
        return None;
    };
    let rest = &tz[1..];

    let (hour_str, min_str) = if let Some(colon) = rest.find(':') {
        (&rest[..colon], &rest[colon + 1..])
    } else if rest.len() == 2 {
        (rest, "0")
    } else if rest.len() == 4 {
        (&rest[..2], &rest[2..])
    } else {
        return None;
    };

    let hours: i32 = hour_str.parse().ok()?;
    let minutes: i32 = min_str.parse().ok()?;
    if hours.abs() > 23 || minutes.abs() > 59 {
        return None;
    }

    Some(sign * (hours * 60 + minutes))
}

fn parse_time_with_tz(s: &str) -> Option<(i64, i32)> {
    let s = s.trim();
    if s.is_empty() {
        return Some((0, 0));
    }

    let mut tz_start = None;
    for (idx, ch) in s.char_indices() {
        if !(ch.is_ascii_digit() || ch == ':' || ch == '.') {
            tz_start = Some(idx);
            break;
        }
    }

    let (time_str, tz_str) = match tz_start {
        Some(idx) => (&s[..idx], s[idx..].trim()),
        None => (s, ""),
    };

    let time_micros = parse_time_of_day(time_str)?;
    let offset_minutes = parse_tz_offset_minutes(tz_str)?;

    Some((time_micros, offset_minutes))
}

/// Parse a timestamptz string with optional offset.
/// Supports: YYYY-MM-DD[ HH:MM[:SS][.ffffff]][Z|+HH[:MM]|-HH[:MM]]
fn parse_timestamptz(s: &str) -> Option<i64> {
    let s = s.trim();

    // Handle special values
    match s.to_lowercase().as_str() {
        "infinity" | "inf" => return Some(i64::MAX),
        "-infinity" | "-inf" => return Some(i64::MIN),
        _ => {}
    }

    let (date_str, time_str) = if let Some(pos) = s.find([' ', 'T']) {
        (&s[..pos], s[pos + 1..].trim())
    } else {
        (s, "")
    };

    let date_parts: Vec<&str> = date_str.split('-').collect();
    if date_parts.len() != 3 {
        return None;
    }

    let year: i32 = date_parts[0].parse().ok()?;
    let month: u32 = date_parts[1].parse().ok()?;
    let day: u32 = date_parts[2].parse().ok()?;

    // Validate date
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let max_day = if is_leap_year(year) && month == 2 {
        29
    } else {
        DAYS_IN_MONTH[month as usize - 1] as u32
    };
    if day > max_day {
        return None;
    }

    let (time_micros, offset_minutes) = parse_time_with_tz(time_str)?;
    let days = ymd_to_days(year, month, day);
    let local_micros = days * MICROS_PER_DAY + time_micros;

    Some(local_micros - offset_minutes as i64 * MICROS_PER_MINUTE)
}

pub fn parse_timestamptz_text(s: &str) -> Option<i64> {
    parse_timestamptz(s)
}

/// Format a time (microseconds since midnight) as HH:MM:SS[.ffffff] string.
fn format_time(micros: i64, buf: &mut [u8]) -> usize {
    let total_secs = micros / MICROS_PER_SECOND;
    let remaining = micros % MICROS_PER_SECOND;
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    let us = remaining.abs() as u32;

    let mut cursor = std::io::Cursor::new(&mut buf[..]);
    if us == 0 {
        write!(cursor, "{:02}:{:02}:{:02}", hours, minutes, seconds).ok();
    } else {
        write!(
            cursor,
            "{:02}:{:02}:{:02}.{:06}",
            hours, minutes, seconds, us
        )
        .ok();
    }

    cursor.position() as usize
}

/// Format a timestamptz (UTC micros) as YYYY-MM-DD HH:MM:SS[.ffffff]+00.
fn format_timestamptz(micros: i64, buf: &mut [u8]) -> usize {
    if micros == i64::MAX {
        let s = b"infinity";
        buf[..s.len()].copy_from_slice(s);
        return s.len();
    }
    if micros == i64::MIN {
        let s = b"-infinity";
        buf[..s.len()].copy_from_slice(s);
        return s.len();
    }

    let len = format_timestamp(micros, buf);
    let mut cursor = std::io::Cursor::new(&mut buf[len..]);
    write!(cursor, "+00").ok();
    len + cursor.position() as usize
}

/// Format a timestamp (microseconds since epoch) as YYYY-MM-DD HH:MM:SS.ffffff string.
fn format_timestamp(micros: i64, buf: &mut [u8]) -> usize {
    // Handle special values
    if micros == i64::MAX {
        let s = b"infinity";
        buf[..s.len()].copy_from_slice(s);
        return s.len();
    }
    if micros == i64::MIN {
        let s = b"-infinity";
        buf[..s.len()].copy_from_slice(s);
        return s.len();
    }

    let (year, month, day, hour, minute, second, us) = micros_to_datetime(micros);

    let mut cursor = std::io::Cursor::new(&mut buf[..]);

    if us == 0 {
        // No fractional seconds
        if year < 0 {
            write!(
                cursor,
                "{:05}-{:02}-{:02} {:02}:{:02}:{:02}",
                year, month, day, hour, minute, second
            )
            .ok();
        } else {
            write!(
                cursor,
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                year, month, day, hour, minute, second
            )
            .ok();
        }
    } else {
        // Include fractional seconds
        if year < 0 {
            write!(
                cursor,
                "{:05}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                year, month, day, hour, minute, second, us
            )
            .ok();
        } else {
            write!(
                cursor,
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                year, month, day, hour, minute, second, us
            )
            .ok();
        }
    }

    cursor.position() as usize
}

/// Cast Timestamp to VARCHAR.
pub fn timestamp_to_varchar(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    result.set_count(count);
    let mut buf = [0u8; 64];

    for i in 0..count {
        if !input.is_null(i) {
            let micros = unsafe { input.get_flat::<i64>(i) };
            let len = format_timestamp(micros, &mut buf);
            let s = std::str::from_utf8(&buf[..len]).unwrap();
            result.set_string(i, s);
        } else {
            result.set_null(i, true);
        }
    }

    Ok(true)
}

/// Cast VARCHAR to Timestamp.
pub fn varchar_to_timestamp(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let mut all_success = true;
    result.set_count(count);

    for i in 0..count {
        if !input.is_null(i) {
            if let Some(s) = input.get_string(i) {
                match parse_timestamp(s) {
                    Some(micros) => {
                        unsafe { result.set_flat::<i64>(i, micros) };
                    }
                    None => {
                        if ctx.try_cast {
                            result.set_null(i, true);
                            all_success = false;
                        } else {
                            return Err(paro_error::invalid_value("TIMESTAMP", s));
                        }
                    }
                }
            }
        } else {
            result.set_null(i, true);
        }
    }

    Ok(all_success)
}

// ============================================================================
// TimestampTz <-> VARCHAR Conversions
// ============================================================================

/// Cast TimestampTz to VARCHAR.
pub fn timestamp_tz_to_varchar(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    result.set_count(count);
    let mut buf = [0u8; 64];

    for i in 0..count {
        if !input.is_null(i) {
            let micros = unsafe { input.get_flat::<i64>(i) };
            let len = format_timestamptz(micros, &mut buf);
            let s = std::str::from_utf8(&buf[..len]).unwrap();
            result.set_string(i, s);
        } else {
            result.set_null(i, true);
        }
    }

    Ok(true)
}

/// Cast VARCHAR to TimestampTz.
pub fn varchar_to_timestamp_tz(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let mut all_success = true;
    result.set_count(count);

    for i in 0..count {
        if !input.is_null(i) {
            if let Some(s) = input.get_string(i) {
                match parse_timestamptz(s) {
                    Some(micros) => unsafe { result.set_flat::<i64>(i, micros) },
                    None => {
                        if ctx.try_cast {
                            result.set_null(i, true);
                            all_success = false;
                        } else {
                            return Err(paro_error::invalid_value("TIMESTAMPTZ", s));
                        }
                    }
                }
            }
        } else {
            result.set_null(i, true);
        }
    }

    Ok(all_success)
}

// ============================================================================
// Time <-> VARCHAR Conversions
// ============================================================================

/// Cast Time to VARCHAR.
pub fn time_to_varchar(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    result.set_count(count);
    let mut buf = [0u8; 32];

    for i in 0..count {
        if !input.is_null(i) {
            let micros = unsafe { input.get_flat::<i64>(i) };
            let len = format_time(micros, &mut buf);
            let s = std::str::from_utf8(&buf[..len]).unwrap();
            result.set_string(i, s);
        } else {
            result.set_null(i, true);
        }
    }

    Ok(true)
}

/// Cast VARCHAR to Time.
pub fn varchar_to_time(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let mut all_success = true;
    result.set_count(count);

    for i in 0..count {
        if !input.is_null(i) {
            if let Some(s) = input.get_string(i) {
                match parse_time_of_day(s) {
                    Some(micros) => unsafe { result.set_flat::<i64>(i, micros) },
                    None => {
                        if ctx.try_cast {
                            result.set_null(i, true);
                            all_success = false;
                        } else {
                            return Err(paro_error::invalid_value("TIME", s));
                        }
                    }
                }
            }
        } else {
            result.set_null(i, true);
        }
    }

    Ok(all_success)
}

// ============================================================================
// Interval <-> VARCHAR Conversions
// ============================================================================
/// Stored as 16 bytes: months (i32) + days (i32) + micros (i64)
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

impl Interval {
    pub const MONTHS_PER_YEAR: i32 = 12;
    pub const DAYS_PER_MONTH: i64 = 30; // For comparison purposes
    pub const MICROS_PER_SEC: i64 = 1_000_000;
    pub const MICROS_PER_MINUTE: i64 = 60 * Self::MICROS_PER_SEC;
    pub const MICROS_PER_HOUR: i64 = 60 * Self::MICROS_PER_MINUTE;
    pub const MICROS_PER_DAY: i64 = 24 * Self::MICROS_PER_HOUR;

    pub fn new(months: i32, days: i32, micros: i64) -> Self {
        Self {
            months,
            days,
            micros,
        }
    }

    /// Create an interval from years and months.
    pub fn from_years_months(years: i32, months: i32) -> Self {
        Self::new(years * Self::MONTHS_PER_YEAR + months, 0, 0)
    }

    /// Create an interval from days.
    pub fn from_days(days: i32) -> Self {
        Self::new(0, days, 0)
    }

    /// Create an interval from time components.
    pub fn from_time(hours: i64, minutes: i64, seconds: i64, micros: i64) -> Self {
        let total_micros = hours * Self::MICROS_PER_HOUR
            + minutes * Self::MICROS_PER_MINUTE
            + seconds * Self::MICROS_PER_SEC
            + micros;
        Self::new(0, 0, total_micros)
    }
}

/// Parse an interval string.
/// Supports formats like:
/// - "1 year 2 months 3 days"
/// - "1 year"
/// - "3 days 4 hours 5 minutes 6 seconds"
/// - "01:02:03" (time format: HH:MM:SS)
/// - "01:02:03.123456" (time format with microseconds)
/// - PostgreSQL ISO format: "P1Y2M3DT4H5M6S"
fn parse_interval(s: &str) -> Option<Interval> {
    let s = s.trim();

    if s.is_empty() {
        return None;
    }

    // Try ISO 8601 duration format: P[n]Y[n]M[n]DT[n]H[n]M[n]S
    if s.starts_with('P') || s.starts_with('p') {
        return parse_iso_interval(&s[1..]);
    }

    // Try time-only format: HH:MM:SS or HH:MM:SS.ffffff
    if s.contains(':') && !s.contains(' ') {
        return parse_time_interval(s);
    }

    // Parse verbose format: "1 year 2 months 3 days 4 hours 5 minutes 6 seconds"
    parse_verbose_interval(s)
}

pub fn parse_interval_text(s: &str) -> Option<Interval> {
    parse_interval(s)
}

/// Parse ISO 8601 duration format.
fn parse_iso_interval(s: &str) -> Option<Interval> {
    let mut months = 0i32;
    let mut days = 0i32;
    let mut micros = 0i64;

    let mut num_buf = String::new();
    let mut in_time = false;

    for c in s.chars() {
        match c {
            '0'..='9' | '.' | '-' => {
                num_buf.push(c);
            }
            'T' | 't' => {
                in_time = true;
            }
            'Y' | 'y' if !in_time => {
                let years: i32 = num_buf.parse().ok()?;
                months += years * 12;
                num_buf.clear();
            }
            'M' | 'm' if !in_time => {
                let m: i32 = num_buf.parse().ok()?;
                months += m;
                num_buf.clear();
            }
            'D' | 'd' => {
                let d: i32 = num_buf.parse().ok()?;
                days += d;
                num_buf.clear();
            }
            'H' | 'h' => {
                let h: f64 = num_buf.parse().ok()?;
                micros += (h * Interval::MICROS_PER_HOUR as f64) as i64;
                num_buf.clear();
            }
            'M' | 'm' if in_time => {
                let m: f64 = num_buf.parse().ok()?;
                micros += (m * Interval::MICROS_PER_MINUTE as f64) as i64;
                num_buf.clear();
            }
            'S' | 's' => {
                let sec: f64 = num_buf.parse().ok()?;
                micros += (sec * Interval::MICROS_PER_SEC as f64) as i64;
                num_buf.clear();
            }
            _ => {}
        }
    }

    Some(Interval::new(months, days, micros))
}

/// Parse time-only interval format: HH:MM:SS or HH:MM:SS.ffffff
fn parse_time_interval(s: &str) -> Option<Interval> {
    // Handle negative time
    let (s, negative) = if s.starts_with('-') {
        (&s[1..], true)
    } else {
        (s, false)
    };

    // Split time and fractional seconds
    let (time_main, frac) = if let Some(dot_pos) = s.find('.') {
        (&s[..dot_pos], Some(&s[dot_pos + 1..]))
    } else {
        (s, None)
    };

    let parts: Vec<&str> = time_main.split(':').collect();
    if parts.len() < 2 || parts.len() > 3 {
        return None;
    }

    let hours: i64 = parts[0].parse().ok()?;
    let minutes: i64 = parts[1].parse().ok()?;
    let seconds: i64 = if parts.len() == 3 {
        parts[2].parse().ok()?
    } else {
        0
    };

    let us: i64 = if let Some(frac_str) = frac {
        let frac_str = if frac_str.len() > 6 {
            &frac_str[..6]
        } else {
            frac_str
        };
        let padded = format!("{:0<6}", frac_str);
        padded.parse().ok()?
    } else {
        0
    };

    let mut micros = hours * Interval::MICROS_PER_HOUR
        + minutes * Interval::MICROS_PER_MINUTE
        + seconds * Interval::MICROS_PER_SEC
        + us;

    if negative {
        micros = -micros;
    }

    Some(Interval::new(0, 0, micros))
}

/// Parse verbose interval format like "1 year 2 months 3 days 4 hours"
fn parse_verbose_interval(s: &str) -> Option<Interval> {
    let mut months = 0i32;
    let mut days = 0i32;
    let mut micros = 0i64;

    let s = s.to_lowercase();
    let tokens: Vec<&str> = s.split_whitespace().collect();

    let mut i = 0;
    while i < tokens.len() {
        // Try to parse as number
        if let Ok(num) = tokens[i].parse::<f64>() {
            if i + 1 < tokens.len() {
                let unit = tokens[i + 1];
                match unit {
                    u if u.starts_with("year") => {
                        months += (num as i32) * 12;
                    }
                    u if u.starts_with("month") => {
                        months += num as i32;
                    }
                    u if u.starts_with("week") => {
                        days += (num as i32) * 7;
                    }
                    u if u.starts_with("day") => {
                        days += num as i32;
                    }
                    u if u.starts_with("hour") => {
                        micros += (num * Interval::MICROS_PER_HOUR as f64) as i64;
                    }
                    u if u.starts_with("minute") || u.starts_with("min") => {
                        micros += (num * Interval::MICROS_PER_MINUTE as f64) as i64;
                    }
                    u if u.starts_with("second") || u.starts_with("sec") => {
                        micros += (num * Interval::MICROS_PER_SEC as f64) as i64;
                    }
                    u if u.starts_with("millisecond") || u.starts_with("ms") => {
                        micros += (num * 1000.0) as i64;
                    }
                    u if u.starts_with("microsecond") || u.starts_with("us") => {
                        micros += num as i64;
                    }
                    _ => {
                        return None;
                    }
                }
                i += 2;
                continue;
            }
        }
        i += 1;
    }

    if months == 0 && days == 0 && micros == 0 && !tokens.is_empty() {
        // Failed to parse anything meaningful
        return None;
    }

    Some(Interval::new(months, days, micros))
}

/// Format an interval as a string.
fn format_interval(interval: &Interval, buf: &mut [u8]) -> usize {
    let mut cursor = std::io::Cursor::new(&mut buf[..]);
    let mut has_output = false;

    // Format months
    if interval.months != 0 {
        let years = interval.months / 12;
        let months = interval.months % 12;

        if years != 0 {
            if years == 1 || years == -1 {
                write!(cursor, "{} year", years).ok();
            } else {
                write!(cursor, "{} years", years).ok();
            }
            has_output = true;
        }

        if months != 0 {
            if has_output {
                write!(cursor, " ").ok();
            }
            if months == 1 || months == -1 {
                write!(cursor, "{} month", months).ok();
            } else {
                write!(cursor, "{} months", months).ok();
            }
            has_output = true;
        }
    }

    // Format days
    if interval.days != 0 {
        if has_output {
            write!(cursor, " ").ok();
        }
        if interval.days == 1 || interval.days == -1 {
            write!(cursor, "{} day", interval.days).ok();
        } else {
            write!(cursor, "{} days", interval.days).ok();
        }
        has_output = true;
    }

    // Format time component
    if interval.micros != 0 {
        if has_output {
            write!(cursor, " ").ok();
        }

        let mut remaining = interval.micros.abs();
        let hours = remaining / Interval::MICROS_PER_HOUR;
        remaining %= Interval::MICROS_PER_HOUR;
        let minutes = remaining / Interval::MICROS_PER_MINUTE;
        remaining %= Interval::MICROS_PER_MINUTE;
        let seconds = remaining / Interval::MICROS_PER_SEC;
        let us = remaining % Interval::MICROS_PER_SEC;

        if interval.micros < 0 {
            write!(cursor, "-").ok();
        }

        if us == 0 {
            write!(cursor, "{:02}:{:02}:{:02}", hours, minutes, seconds).ok();
        } else {
            write!(
                cursor,
                "{:02}:{:02}:{:02}.{:06}",
                hours, minutes, seconds, us
            )
            .ok();
        }
        has_output = true;
    }

    // Handle zero interval
    if !has_output {
        write!(cursor, "00:00:00").ok();
    }

    cursor.position() as usize
}

/// Cast Interval to VARCHAR.
pub fn interval_to_varchar(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    result.set_count(count);
    let mut buf = [0u8; 128];

    for i in 0..count {
        if !input.is_null(i) {
            let interval = unsafe { input.get_flat::<Interval>(i) };
            let len = format_interval(&interval, &mut buf);
            let s = std::str::from_utf8(&buf[..len]).unwrap();
            result.set_string(i, s);
        } else {
            result.set_null(i, true);
        }
    }

    Ok(true)
}

/// Cast VARCHAR to Interval.
pub fn varchar_to_interval(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let mut all_success = true;
    result.set_count(count);

    for i in 0..count {
        if !input.is_null(i) {
            if let Some(s) = input.get_string(i) {
                match parse_interval(s) {
                    Some(interval) => {
                        unsafe { result.set_flat::<Interval>(i, interval) };
                    }
                    None => {
                        if ctx.try_cast {
                            result.set_null(i, true);
                            all_success = false;
                        } else {
                            return Err(paro_error::invalid_value("INTERVAL", s));
                        }
                    }
                }
            }
        } else {
            result.set_null(i, true);
        }
    }

    Ok(all_success)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_days_to_ymd() {
        // Unix epoch
        assert_eq!(days_to_ymd(0), (1970, 1, 1));

        // Day after epoch
        assert_eq!(days_to_ymd(1), (1970, 1, 2));

        // End of January 1970
        assert_eq!(days_to_ymd(30), (1970, 1, 31));

        // February 1970
        assert_eq!(days_to_ymd(31), (1970, 2, 1));

        // 2000-01-01 (30 years from epoch, including leap years)
        // From 1970 to 2000: 30 years, with leap years: 1972, 1976, 1980, 1984, 1988, 1992, 1996 = 7 leap years
        // Days = 30 * 365 + 7 = 10957 (but 2000 is also a leap year)
        let days_2000 = ymd_to_days(2000, 1, 1);
        assert_eq!(days_to_ymd(days_2000), (2000, 1, 1));
    }

    #[test]
    fn test_ymd_to_days() {
        // Unix epoch
        assert_eq!(ymd_to_days(1970, 1, 1), 0);

        // Day before epoch
        assert_eq!(ymd_to_days(1969, 12, 31), -1);

        // Round trip
        let days = ymd_to_days(2024, 6, 15);
        let (y, m, d) = days_to_ymd(days);
        assert_eq!((y, m, d), (2024, 6, 15));
    }

    #[test]
    fn test_parse_date() {
        assert_eq!(parse_date("1970-01-01"), Some(0));
        assert_eq!(parse_date("2024-06-15"), Some(ymd_to_days(2024, 6, 15)));
        assert_eq!(parse_date("  2024-06-15  "), Some(ymd_to_days(2024, 6, 15)));

        // Invalid formats
        assert_eq!(parse_date("not-a-date"), None);
        assert_eq!(parse_date("2024-13-01"), None); // Invalid month
        assert_eq!(parse_date("2024-02-30"), None); // Invalid day

        // Special values
        assert_eq!(parse_date("infinity"), Some(i64::MAX));
        assert_eq!(parse_date("-infinity"), Some(i64::MIN));
    }

    #[test]
    fn test_format_date() {
        let mut buf = [0u8; 32];

        let len = format_date(0, &mut buf);
        assert_eq!(&buf[..len], b"1970-01-01");

        let days = ymd_to_days(2024, 6, 15);
        let len = format_date(days, &mut buf);
        assert_eq!(&buf[..len], b"2024-06-15");

        // Special values
        let len = format_date(i64::MAX, &mut buf);
        assert_eq!(&buf[..len], b"infinity");

        let len = format_date(i64::MIN, &mut buf);
        assert_eq!(&buf[..len], b"-infinity");
    }

    #[test]
    fn test_leap_year() {
        assert!(!is_leap_year(1900)); // Divisible by 100 but not 400
        assert!(is_leap_year(2000)); // Divisible by 400
        assert!(is_leap_year(2004)); // Divisible by 4
        assert!(!is_leap_year(2001)); // Not divisible by 4
    }

    // ========== Timestamp Tests ==========

    #[test]
    fn test_micros_to_datetime() {
        // Unix epoch
        assert_eq!(micros_to_datetime(0), (1970, 1, 1, 0, 0, 0, 0));

        // 1 second after epoch
        assert_eq!(
            micros_to_datetime(MICROS_PER_SECOND),
            (1970, 1, 1, 0, 0, 1, 0)
        );

        // 1 hour after epoch
        assert_eq!(
            micros_to_datetime(MICROS_PER_HOUR),
            (1970, 1, 1, 1, 0, 0, 0)
        );

        // 1 day after epoch
        assert_eq!(micros_to_datetime(MICROS_PER_DAY), (1970, 1, 2, 0, 0, 0, 0));

        // Specific timestamp with microseconds
        let micros = datetime_to_micros(2024, 6, 15, 14, 30, 45, 123456);
        assert_eq!(
            micros_to_datetime(micros),
            (2024, 6, 15, 14, 30, 45, 123456)
        );
    }

    #[test]
    fn test_datetime_to_micros() {
        // Unix epoch
        assert_eq!(datetime_to_micros(1970, 1, 1, 0, 0, 0, 0), 0);

        // 1 day before epoch
        assert_eq!(
            datetime_to_micros(1969, 12, 31, 0, 0, 0, 0),
            -MICROS_PER_DAY
        );

        // Round trip
        let micros = datetime_to_micros(2024, 6, 15, 14, 30, 45, 123456);
        let (y, mon, d, h, min, s, us) = micros_to_datetime(micros);
        assert_eq!(
            (y, mon, d, h, min, s, us),
            (2024, 6, 15, 14, 30, 45, 123456)
        );
    }

    #[test]
    fn test_parse_timestamp() {
        // Date only (defaults to midnight)
        assert_eq!(parse_timestamp("1970-01-01"), Some(0));

        // Date and time
        assert_eq!(parse_timestamp("1970-01-01 00:00:00"), Some(0));
        assert_eq!(
            parse_timestamp("1970-01-01 00:00:01"),
            Some(MICROS_PER_SECOND)
        );

        // With microseconds
        assert_eq!(parse_timestamp("1970-01-01 00:00:00.000001"), Some(1));
        assert_eq!(parse_timestamp("1970-01-01 00:00:00.123456"), Some(123456));

        // ISO format with T separator
        assert_eq!(parse_timestamp("1970-01-01T00:00:00"), Some(0));

        // Trimmed whitespace
        assert_eq!(
            parse_timestamp("  1970-01-01 12:30:00  "),
            Some(datetime_to_micros(1970, 1, 1, 12, 30, 0, 0))
        );

        // Invalid formats
        assert_eq!(parse_timestamp("not-a-timestamp"), None);
        assert_eq!(parse_timestamp("1970-01-01 25:00:00"), None); // Invalid hour
        assert_eq!(parse_timestamp("1970-01-01 00:60:00"), None); // Invalid minute

        // Special values
        assert_eq!(parse_timestamp("infinity"), Some(i64::MAX));
        assert_eq!(parse_timestamp("-infinity"), Some(i64::MIN));
    }

    #[test]
    fn test_format_timestamp() {
        let mut buf = [0u8; 64];

        // Unix epoch
        let len = format_timestamp(0, &mut buf);
        assert_eq!(&buf[..len], b"1970-01-01 00:00:00");

        // With time
        let micros = datetime_to_micros(2024, 6, 15, 14, 30, 45, 0);
        let len = format_timestamp(micros, &mut buf);
        assert_eq!(&buf[..len], b"2024-06-15 14:30:45");

        // With microseconds
        let micros = datetime_to_micros(2024, 6, 15, 14, 30, 45, 123456);
        let len = format_timestamp(micros, &mut buf);
        assert_eq!(&buf[..len], b"2024-06-15 14:30:45.123456");

        // Special values
        let len = format_timestamp(i64::MAX, &mut buf);
        assert_eq!(&buf[..len], b"infinity");

        let len = format_timestamp(i64::MIN, &mut buf);
        assert_eq!(&buf[..len], b"-infinity");
    }

    // ========== Interval Tests ==========

    #[test]
    fn test_interval_struct() {
        let interval = Interval::new(14, 5, 3_600_000_000); // 1 year 2 months, 5 days, 1 hour
        assert_eq!(interval.months, 14);
        assert_eq!(interval.days, 5);
        assert_eq!(interval.micros, 3_600_000_000);

        // Test constructors
        let from_ym = Interval::from_years_months(1, 2);
        assert_eq!(from_ym.months, 14);

        let from_days = Interval::from_days(10);
        assert_eq!(from_days.days, 10);

        let from_time = Interval::from_time(1, 30, 45, 0);
        assert_eq!(
            from_time.micros,
            Interval::MICROS_PER_HOUR
                + 30 * Interval::MICROS_PER_MINUTE
                + 45 * Interval::MICROS_PER_SEC
        );
    }

    #[test]
    fn test_parse_interval_verbose() {
        // Single components
        assert_eq!(parse_interval("1 year"), Some(Interval::new(12, 0, 0)));
        assert_eq!(parse_interval("2 months"), Some(Interval::new(2, 0, 0)));
        assert_eq!(parse_interval("3 days"), Some(Interval::new(0, 3, 0)));
        assert_eq!(
            parse_interval("4 hours"),
            Some(Interval::new(0, 0, 4 * Interval::MICROS_PER_HOUR))
        );
        assert_eq!(
            parse_interval("5 minutes"),
            Some(Interval::new(0, 0, 5 * Interval::MICROS_PER_MINUTE))
        );
        assert_eq!(
            parse_interval("6 seconds"),
            Some(Interval::new(0, 0, 6 * Interval::MICROS_PER_SEC))
        );

        // Mixed components
        assert_eq!(
            parse_interval("1 year 2 months"),
            Some(Interval::new(14, 0, 0))
        );
        assert_eq!(
            parse_interval("1 year 2 months 3 days"),
            Some(Interval::new(14, 3, 0))
        );

        // Invalid
        assert_eq!(parse_interval("invalid"), None);
        assert_eq!(parse_interval(""), None);
    }

    #[test]
    fn test_parse_interval_time_format() {
        // Time-only format
        assert_eq!(
            parse_interval("01:00:00"),
            Some(Interval::new(0, 0, Interval::MICROS_PER_HOUR))
        );
        assert_eq!(
            parse_interval("00:30:00"),
            Some(Interval::new(0, 0, 30 * Interval::MICROS_PER_MINUTE))
        );
        assert_eq!(
            parse_interval("00:00:45"),
            Some(Interval::new(0, 0, 45 * Interval::MICROS_PER_SEC))
        );

        // With microseconds
        assert_eq!(
            parse_interval("00:00:00.123456"),
            Some(Interval::new(0, 0, 123456))
        );

        // Negative time
        assert_eq!(
            parse_interval("-01:00:00"),
            Some(Interval::new(0, 0, -Interval::MICROS_PER_HOUR))
        );
    }

    #[test]
    fn test_parse_interval_iso() {
        // ISO 8601 duration format
        assert_eq!(parse_interval("P1Y"), Some(Interval::new(12, 0, 0)));
        assert_eq!(parse_interval("P2M"), Some(Interval::new(2, 0, 0)));
        assert_eq!(parse_interval("P3D"), Some(Interval::new(0, 3, 0)));
        assert_eq!(
            parse_interval("PT4H"),
            Some(Interval::new(0, 0, 4 * Interval::MICROS_PER_HOUR))
        );
        assert_eq!(
            parse_interval("PT5M"),
            Some(Interval::new(0, 0, 5 * Interval::MICROS_PER_MINUTE))
        );
        assert_eq!(
            parse_interval("PT6S"),
            Some(Interval::new(0, 0, 6 * Interval::MICROS_PER_SEC))
        );

        // Combined
        assert_eq!(
            parse_interval("P1Y2M3DT4H5M6S"),
            Some(Interval::new(
                14, // 1 year + 2 months
                3,
                4 * Interval::MICROS_PER_HOUR
                    + 5 * Interval::MICROS_PER_MINUTE
                    + 6 * Interval::MICROS_PER_SEC
            ))
        );
    }

    #[test]
    fn test_format_interval() {
        let mut buf = [0u8; 128];

        // Zero interval
        let interval = Interval::new(0, 0, 0);
        let len = format_interval(&interval, &mut buf);
        assert_eq!(&buf[..len], b"00:00:00");

        // Years only
        let interval = Interval::new(24, 0, 0); // 2 years
        let len = format_interval(&interval, &mut buf);
        assert_eq!(&buf[..len], b"2 years");

        // Year + months
        let interval = Interval::new(14, 0, 0); // 1 year 2 months
        let len = format_interval(&interval, &mut buf);
        assert_eq!(&buf[..len], b"1 year 2 months");

        // Days only
        let interval = Interval::new(0, 5, 0);
        let len = format_interval(&interval, &mut buf);
        assert_eq!(&buf[..len], b"5 days");

        // Time only
        let interval = Interval::new(0, 0, 3_661_000_000); // 1:01:01
        let len = format_interval(&interval, &mut buf);
        assert_eq!(&buf[..len], b"01:01:01");

        // Mixed
        let interval = Interval::new(14, 5, 3_600_000_000); // 1 year 2 months 5 days 01:00:00
        let len = format_interval(&interval, &mut buf);
        assert_eq!(&buf[..len], b"1 year 2 months 5 days 01:00:00");
    }
}
