// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL text-format encoding for scalar and nested values.

use std::fmt::{self, Write};

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::{
    format_date_days, format_decimal_i128, format_interval_parts, format_timestamp_micros,
    format_uuid, Value,
};
use paro_common::types::{LogicalType, StringView};
use paro_common::vector::{DecodedVectorRef, Vector};
use tokio_util::bytes::BytesMut;

/// A formatting target that writes directly into a PostgreSQL message buffer.
///
/// Rust's primitive `Display` implementations do not allocate when their output
/// target implements `fmt::Write`. Keeping this adapter here lets the protocol
/// path format fixed-width vectors without first constructing `Value` or
/// `String` objects.
struct MessageTextWriter<'a>(&'a mut BytesMut);

impl fmt::Write for MessageTextWriter<'_> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.0.extend_from_slice(value.as_bytes());
        Ok(())
    }
}

#[inline]
fn write_display(buffer: &mut BytesMut, value: impl fmt::Display) -> Result<()> {
    write!(MessageTextWriter(buffer), "{value}")
        .map_err(|_| paro_error::internal("failed to format PostgreSQL text value"))
}

#[inline]
fn append_timestamp_text(buffer: &mut BytesMut, micros: i64, with_timezone: bool) {
    buffer.extend_from_slice(format_timestamp_micros(micros).as_bytes());
    if with_timezone && !matches!(micros, i64::MIN | i64::MAX) {
        buffer.extend_from_slice(b"+00");
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct IntervalPhysical {
    months: i32,
    days: i32,
    micros: i64,
}

/// A vector decoded once for an entire PostgreSQL text-result batch.
///
/// Row encoding is necessarily row-major because every PostgreSQL `DataRow`
/// owns its field lengths. Retaining the decoded vector view here keeps that
/// wire constraint from repeating dictionary/constant dispatch for every
/// cell.
pub(crate) struct TextVectorEncoder<'a> {
    vector: &'a Vector,
    decoded: DecodedVectorRef<'a>,
    integer_buffer: itoa::Buffer,
    float_buffer: ryu::Buffer,
}

impl<'a> TextVectorEncoder<'a> {
    pub(crate) fn try_new(vector: &'a Vector, row_count: usize) -> Result<Self> {
        Ok(Self {
            vector,
            decoded: vector.try_decode_ref(row_count)?,
            integer_buffer: itoa::Buffer::new(),
            float_buffer: ryu::Buffer::new(),
        })
    }

    #[inline]
    pub(crate) fn is_null(&self, row_idx: usize) -> bool {
        !self.decoded.is_valid(row_idx)
    }

    /// Capacity hint for one complete vector. It deliberately uses cheap
    /// type-level upper estimates rather than making a second pass over
    /// variable-length values.
    pub(crate) fn encoded_bytes_hint(&self, row_count: usize) -> usize {
        let per_row = match self.vector.logical_type() {
            LogicalType::Boolean => 5,
            LogicalType::TinyInt => 4,
            LogicalType::SmallInt => 6,
            LogicalType::Integer | LogicalType::Date => 11,
            LogicalType::BigInt
            | LogicalType::UBigInt
            | LogicalType::Timestamp
            | LogicalType::TimestampTz
            | LogicalType::Time
            | LogicalType::IntegerLiteral(_) => 24,
            LogicalType::HugeInt | LogicalType::UHugeInt => 40,
            LogicalType::UTinyInt => 3,
            LogicalType::USmallInt => 5,
            LogicalType::UInteger => 10,
            LogicalType::Float | LogicalType::Double => 24,
            LogicalType::Decimal { precision, .. } => usize::from(*precision) + 2,
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::StringLiteral => 24,
            LogicalType::Blob => 16,
            LogicalType::Uuid => 36,
            LogicalType::Interval => 48,
            LogicalType::Array(_, _)
            | LogicalType::List(_)
            | LogicalType::Struct(_)
            | LogicalType::Null
            | LogicalType::Unknown => 32,
        };
        row_count.saturating_mul(per_row)
    }

    /// Append one known-non-null row in PostgreSQL text format.
    pub(crate) fn append_non_null(&mut self, buffer: &mut BytesMut, row_idx: usize) -> Result<()> {
        debug_assert!(!self.is_null(row_idx));
        match self.vector.logical_type() {
            LogicalType::Boolean => {
                let value = unsafe { self.fixed::<u8>(row_idx) } != 0;
                buffer.extend_from_slice(if value { b"true" } else { b"false" });
                Ok(())
            }
            LogicalType::TinyInt => {
                self.append_integer(buffer, unsafe { self.fixed::<i8>(row_idx) })
            }
            LogicalType::SmallInt => {
                self.append_integer(buffer, unsafe { self.fixed::<i16>(row_idx) })
            }
            LogicalType::Integer => {
                self.append_integer(buffer, unsafe { self.fixed::<i32>(row_idx) })
            }
            LogicalType::BigInt => {
                self.append_integer(buffer, unsafe { self.fixed::<i64>(row_idx) })
            }
            LogicalType::HugeInt => {
                self.append_integer(buffer, unsafe { self.fixed::<i128>(row_idx) })
            }
            LogicalType::UTinyInt => {
                self.append_integer(buffer, unsafe { self.fixed::<u8>(row_idx) })
            }
            LogicalType::USmallInt => {
                self.append_integer(buffer, unsafe { self.fixed::<u16>(row_idx) })
            }
            LogicalType::UInteger => {
                self.append_integer(buffer, unsafe { self.fixed::<u32>(row_idx) })
            }
            LogicalType::UBigInt => {
                self.append_integer(buffer, unsafe { self.fixed::<u64>(row_idx) })
            }
            LogicalType::UHugeInt => {
                self.append_integer(buffer, unsafe { self.fixed::<u128>(row_idx) })
            }
            LogicalType::Float => {
                let text = self
                    .float_buffer
                    .format(unsafe { self.fixed::<f32>(row_idx) });
                buffer.extend_from_slice(text.as_bytes());
                Ok(())
            }
            LogicalType::Double => {
                let text = self
                    .float_buffer
                    .format(unsafe { self.fixed::<f64>(row_idx) });
                buffer.extend_from_slice(text.as_bytes());
                Ok(())
            }
            LogicalType::Decimal { precision, scale } => {
                let value = if *precision <= 18 {
                    (unsafe { self.fixed::<i64>(row_idx) }) as i128
                } else {
                    unsafe { self.fixed::<i128>(row_idx) }
                };
                buffer.extend_from_slice(format_decimal_i128(value, *scale).as_bytes());
                Ok(())
            }
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::StringLiteral => {
                let value = unsafe { self.fixed::<StringView>(row_idx) };
                buffer.extend_from_slice(value.as_bytes());
                Ok(())
            }
            LogicalType::Blob => {
                let value = unsafe { self.fixed::<StringView>(row_idx) };
                buffer.extend_from_slice(b"BLOB[");
                self.append_integer(buffer, value.len())?;
                buffer.extend_from_slice(b"]");
                Ok(())
            }
            LogicalType::Uuid => {
                let value = unsafe { self.fixed::<u128>(row_idx) };
                buffer.extend_from_slice(format_uuid(value).as_bytes());
                Ok(())
            }
            LogicalType::Date => {
                let value = unsafe { self.fixed::<i32>(row_idx) };
                buffer.extend_from_slice(format_date_days(i64::from(value)).as_bytes());
                Ok(())
            }
            LogicalType::Timestamp => {
                let value = unsafe { self.fixed::<i64>(row_idx) };
                append_timestamp_text(buffer, value, false);
                Ok(())
            }
            LogicalType::TimestampTz => {
                let value = unsafe { self.fixed::<i64>(row_idx) };
                append_timestamp_text(buffer, value, true);
                Ok(())
            }
            LogicalType::Time => {
                let value = unsafe { self.fixed::<i64>(row_idx) };
                write_display(buffer, Value::Time(value))
            }
            LogicalType::Interval => {
                let value = unsafe { self.fixed::<IntervalPhysical>(row_idx) };
                buffer.extend_from_slice(
                    format_interval_parts(value.months, value.days, value.micros).as_bytes(),
                );
                Ok(())
            }
            LogicalType::IntegerLiteral(_) => {
                self.append_integer(buffer, unsafe { self.fixed::<i64>(row_idx) })
            }
            LogicalType::Array(_, _)
            | LogicalType::List(_)
            | LogicalType::Struct(_)
            | LogicalType::Null
            | LogicalType::Unknown => {
                buffer.extend_from_slice(
                    value_to_pg_text(&self.vector.get_value(row_idx)).as_bytes(),
                );
                Ok(())
            }
        }
    }

    #[inline]
    fn append_integer<T: itoa::Integer>(&mut self, buffer: &mut BytesMut, value: T) -> Result<()> {
        buffer.extend_from_slice(self.integer_buffer.format(value).as_bytes());
        Ok(())
    }

    /// # Safety
    /// The logical-type dispatch in [`Self::append_non_null`] must request the
    /// physical type represented by this vector, and `row_idx` must be in the
    /// decoded batch.
    #[inline]
    unsafe fn fixed<T: Copy>(&self, row_idx: usize) -> T {
        unsafe { self.decoded.get_value::<T>(row_idx) }
    }
}

pub(crate) fn value_to_pg_text(value: &Value) -> String {
    match value {
        Value::HugeInt(v) => v.to_string(),
        Value::UBigInt(v) => v.to_string(),
        Value::UHugeInt(v) => v.to_string(),
        Value::Varchar(s) => s.clone(),
        Value::List(values, _) | Value::Array(values, _, _) => format_pg_array(values),
        _ => value.to_string(),
    }
}

pub(crate) fn format_pg_array(values: &[Value]) -> String {
    let mut out = String::from("{");
    for (i, value) in values.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&format_pg_array_element(value));
    }
    out.push('}');
    out
}

fn format_pg_array_element(value: &Value) -> String {
    match value {
        Value::Null(_) => "NULL".to_string(),
        Value::Varchar(s) => format_pg_array_string(s),
        Value::List(values, _) | Value::Array(values, _, _) => format_pg_array(values),
        _ => value_to_pg_text(value),
    }
}

fn format_pg_array_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        if ch == '"' || ch == '\\' {
            out.push('\\');
        }
        out.push(ch);
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timestamp_text(micros: i64, with_timezone: bool) -> String {
        let mut buffer = BytesMut::new();
        append_timestamp_text(&mut buffer, micros, with_timezone);
        String::from_utf8(buffer.to_vec()).expect("timestamp text should be UTF-8")
    }

    #[test]
    fn timestamp_infinities_match_postgres_for_both_timestamp_types() {
        for with_timezone in [false, true] {
            assert_eq!(timestamp_text(i64::MIN, with_timezone), "-infinity");
            assert_eq!(timestamp_text(i64::MAX, with_timezone), "infinity");
        }
    }

    #[test]
    fn timestamp_with_time_zone_appends_utc_offset_only_to_finite_values() {
        assert_eq!(timestamp_text(0, false), "1970-01-01 00:00:00");
        assert_eq!(timestamp_text(0, true), "1970-01-01 00:00:00+00");
    }
}
