// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! SQL runtime value representation (`Value` and nested value helpers).

use std::cmp::Ordering;
use std::fmt;
use std::fmt::Write;
use std::mem::size_of;

use crate::error::{self as paro_error};
use crate::types::{ArrayType, LogicalType};

/// A single value of a specific logical type.
///
/// Nested types (List, Array, Struct) store their children in a `Vec<Value>`.
#[derive(Debug, Clone)]
pub enum Value {
    /// NULL value
    Null(LogicalType),
    /// Boolean value
    Boolean(bool),

    // Signed integers
    /// TinyInt value
    TinyInt(i8),
    /// SmallInt value
    SmallInt(i16),
    /// Integer value
    Integer(i32),
    /// BigInt value
    BigInt(i64),
    /// HugeInt value (i128)
    HugeInt(i128),

    // Unsigned integers
    /// UTinyInt value
    UTinyInt(u8),
    /// USmallInt value
    USmallInt(u16),
    /// UInteger value
    UInteger(u32),
    /// UBigInt value
    UBigInt(u64),
    /// UHugeInt value (u128)
    UHugeInt(u128),

    /// Float value
    Float(f32),
    /// Double value
    Double(f64),
    /// Decimal value stored as scaled integer (value, precision, scale)
    Decimal(i128, u8, u8),
    /// Varchar value (String)
    Varchar(String),
    /// Blob value (Byte array)
    Blob(Vec<u8>),
    /// UUID value (u128)
    Uuid(u128),

    // Temporal types
    /// Date value (days since 1970-01-01 epoch)
    Date(i32),
    /// Timestamp value (microseconds since 1970-01-01 epoch)
    Timestamp(i64),
    /// Timestamp with timezone (stored in UTC microseconds)
    TimestampTz(i64),
    /// Time value (microseconds since midnight)
    Time(i64),
    /// Interval value (months, days, microseconds)
    Interval(i32, i32, i64),

    /// List value (variable-length array)
    ///
    /// - First field: child values
    /// - Second field: element type
    List(Vec<Value>, LogicalType),

    /// Struct value with named fields
    ///
    /// - First field: child values (in field order)
    /// - Second field: field definitions (name + type)
    Struct(Vec<Value>, Vec<(String, LogicalType)>),

    /// Array value (fixed-size array)
    ///
    /// - First field: child values (must have exactly `size` elements)
    /// - Second field: element type
    /// - Third field: fixed size
    ///
    /// # Example
    /// ```ignore
    /// // FLOAT[3] array with values [1.0, 2.0, 3.0]
    /// Value::Array(vec![Value::Float(1.0), Value::Float(2.0), Value::Float(3.0)], LogicalType::Float, 3)
    /// ```
    Array(Vec<Value>, LogicalType, usize),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null(a), Value::Null(b)) => a == b,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::TinyInt(a), Value::TinyInt(b)) => a == b,
            (Value::SmallInt(a), Value::SmallInt(b)) => a == b,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::BigInt(a), Value::BigInt(b)) => a == b,
            (Value::HugeInt(a), Value::HugeInt(b)) => a == b,
            (Value::UTinyInt(a), Value::UTinyInt(b)) => a == b,
            (Value::USmallInt(a), Value::USmallInt(b)) => a == b,
            (Value::UInteger(a), Value::UInteger(b)) => a == b,
            (Value::UBigInt(a), Value::UBigInt(b)) => a == b,
            (Value::UHugeInt(a), Value::UHugeInt(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a.to_bits() == b.to_bits(),
            (Value::Double(a), Value::Double(b)) => a.to_bits() == b.to_bits(),
            (Value::Decimal(av, ap, ascale), Value::Decimal(bv, bp, bscale)) => {
                av == bv && ap == bp && ascale == bscale
            }
            (Value::Varchar(a), Value::Varchar(b)) => a == b,
            (Value::Blob(a), Value::Blob(b)) => a == b,
            (Value::Uuid(a), Value::Uuid(b)) => a == b,
            (Value::Date(a), Value::Date(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (Value::TimestampTz(a), Value::TimestampTz(b)) => a == b,
            (Value::Time(a), Value::Time(b)) => a == b,
            (Value::Interval(am, ad, au), Value::Interval(bm, bd, bu)) => {
                am == bm && ad == bd && au == bu
            }
            (Value::List(a, ta), Value::List(b, tb)) => a == b && ta == tb,
            (Value::Struct(a_vals, a_fields), Value::Struct(b_vals, b_fields)) => {
                a_vals == b_vals && a_fields == b_fields
            }
            (Value::Array(a, ta, sa), Value::Array(b, tb, sb)) => a == b && ta == tb && sa == sb,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            Value::Null(t) => t.hash(state),
            Value::Boolean(b) => b.hash(state),
            Value::TinyInt(v) => v.hash(state),
            Value::SmallInt(v) => v.hash(state),
            Value::Integer(v) => v.hash(state),
            Value::BigInt(v) => v.hash(state),
            Value::HugeInt(v) => v.hash(state),
            Value::UTinyInt(v) => v.hash(state),
            Value::USmallInt(v) => v.hash(state),
            Value::UInteger(v) => v.hash(state),
            Value::UBigInt(v) => v.hash(state),
            Value::UHugeInt(v) => v.hash(state),
            Value::Float(v) => v.to_bits().hash(state),
            Value::Double(v) => v.to_bits().hash(state),
            Value::Decimal(v, p, s) => {
                v.hash(state);
                p.hash(state);
                s.hash(state);
            }
            Value::Varchar(v) => v.hash(state),
            Value::Blob(v) => v.hash(state),
            Value::Uuid(v) => v.hash(state),
            Value::Date(v) => v.hash(state),
            Value::Timestamp(v) => v.hash(state),
            Value::TimestampTz(v) => v.hash(state),
            Value::Time(v) => v.hash(state),
            Value::Interval(m, d, u) => {
                m.hash(state);
                d.hash(state);
                u.hash(state);
            }
            Value::List(v, t) => {
                v.hash(state);
                t.hash(state);
            }
            Value::Struct(v, fields) => {
                v.hash(state);
                fields.hash(state);
            }
            Value::Array(v, t, s) => {
                v.hash(state);
                t.hash(state);
                s.hash(state);
            }
        }
    }
}

impl Value {
    /// Return the heap allocation owned by this value.
    pub fn allocation_size(&self) -> usize {
        match self {
            Value::Varchar(value) => value.capacity(),
            Value::Blob(value) => value.capacity(),
            Value::List(values, _) | Value::Array(values, _, _) => {
                values.capacity() * size_of::<Value>()
                    + values.iter().map(Value::allocation_size).sum::<usize>()
            }
            Value::Struct(values, fields) => {
                values.capacity() * size_of::<Value>()
                    + values.iter().map(Value::allocation_size).sum::<usize>()
                    + fields.capacity() * size_of::<(String, LogicalType)>()
                    + fields
                        .iter()
                        .map(|(name, _)| name.capacity())
                        .sum::<usize>()
            }
            _ => 0,
        }
    }

    /// Create a NULL value of specific type
    pub fn null(data_type: LogicalType) -> Self {
        Value::Null(data_type)
    }

    /// Create an Array value from a list of values.
    ///
    /// The array size is determined by the number of values provided.
    ///
    /// # Arguments
    /// * `child_type` - The element type of the array
    /// * `values` - The values to store in the array
    ///
    /// # Panics
    /// Panics if the array size exceeds `ArrayType::MAX_ARRAY_SIZE`.
    ///
    /// # Example
    /// ```ignore
    /// // Create FLOAT[3] array
    /// let arr = Value::array(LogicalType::Float, vec![
    ///     Value::Float(1.0),
    ///     Value::Float(2.0),
    ///     Value::Float(3.0),
    /// ]);
    /// ```
    pub fn array(child_type: LogicalType, values: Vec<Value>) -> Self {
        let size = values.len();
        if size > ArrayType::MAX_ARRAY_SIZE {
            panic!(
                "Array size {} exceeds maximum allowed size {}",
                size,
                ArrayType::MAX_ARRAY_SIZE
            );
        }
        // Cast all values to the child type before storing them.
        let casted_values: Vec<Value> = values
            .into_iter()
            .map(|v| v.cast(&child_type).unwrap_or(v))
            .collect();
        Value::Array(casted_values, child_type, size)
    }

    /// Create an Array value from a slice of f32 (convenience for AI embeddings).
    ///
    /// # Arguments
    /// * `values` - The f32 values to store in the array
    pub fn array_f32(values: &[f32]) -> Self {
        let values: Vec<Value> = values.iter().map(|v| Value::Float(*v)).collect();
        Value::array(LogicalType::Float, values)
    }

    /// Create a List value from a list of values.
    ///
    /// # Arguments
    /// * `child_type` - The element type of the list
    /// * `values` - The values to store in the list
    pub fn list(child_type: LogicalType, values: Vec<Value>) -> Self {
        // Cast all values to the child type before storing them.
        let casted_values: Vec<Value> = values
            .into_iter()
            .map(|v| v.cast(&child_type).unwrap_or(v))
            .collect();
        Value::List(casted_values, child_type)
    }

    /// Create a Struct value from a list of field definitions and values.
    ///
    /// # Arguments
    /// * `fields` - Struct fields (name + type) in order
    /// * `values` - Values for each field (same length as `fields`)
    pub fn struct_value(fields: Vec<(String, LogicalType)>, values: Vec<Value>) -> Self {
        if fields.len() != values.len() {
            panic!(
                "Struct value expects {} fields but got {} values",
                fields.len(),
                values.len()
            );
        }

        let casted_values: Vec<Value> = values
            .into_iter()
            .zip(fields.iter())
            .map(|(v, (_, ty))| v.cast(ty).unwrap_or(v))
            .collect();

        Value::Struct(casted_values, fields)
    }

    /// Get the logical type of this value
    pub fn logical_type(&self) -> LogicalType {
        match self {
            Value::Null(t) => t.clone(),
            Value::Boolean(_) => LogicalType::Boolean,
            Value::TinyInt(_) => LogicalType::TinyInt,
            Value::SmallInt(_) => LogicalType::SmallInt,
            Value::Integer(_) => LogicalType::Integer,
            Value::BigInt(_) => LogicalType::BigInt,
            Value::HugeInt(_) => LogicalType::HugeInt,
            Value::UTinyInt(_) => LogicalType::UTinyInt,
            Value::USmallInt(_) => LogicalType::USmallInt,
            Value::UInteger(_) => LogicalType::UInteger,
            Value::UBigInt(_) => LogicalType::UBigInt,
            Value::UHugeInt(_) => LogicalType::UHugeInt,
            Value::Float(_) => LogicalType::Float,
            Value::Double(_) => LogicalType::Double,
            Value::Decimal(_, precision, scale) => LogicalType::Decimal {
                precision: *precision,
                scale: *scale,
            },
            Value::Varchar(_) => LogicalType::Varchar,
            Value::Blob(_) => LogicalType::Blob,
            Value::Uuid(_) => LogicalType::Uuid,
            Value::Date(_) => LogicalType::Date,
            Value::Timestamp(_) => LogicalType::Timestamp,
            Value::TimestampTz(_) => LogicalType::TimestampTz,
            Value::Time(_) => LogicalType::Time,
            Value::Interval(_, _, _) => LogicalType::Interval,
            Value::List(_, elem_type) => LogicalType::List(Box::new(elem_type.clone())),
            Value::Struct(_, fields) => LogicalType::Struct(fields.clone()),
            Value::Array(_, elem_type, size) => {
                LogicalType::Array(Box::new(elem_type.clone()), *size)
            }
        }
    }

    /// Check if the value is NULL
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null(_))
    }

    /// Try to get the value as an i64.
    ///
    /// Used by `get_expression_return_type()` to extract integer constant values
    /// for creating `INTEGER_LITERAL` types.
    ///
    /// Returns `None` if:
    /// - The value is NULL
    /// - The value is not an integral type
    /// - The value cannot be represented as i64 (e.g., very large i128/u128)
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Null(_) => None,
            Value::TinyInt(v) => Some(*v as i64),
            Value::SmallInt(v) => Some(*v as i64),
            Value::Integer(v) => Some(*v as i64),
            Value::BigInt(v) => Some(*v),
            Value::HugeInt(v) => i64::try_from(*v).ok(),
            Value::UTinyInt(v) => Some(*v as i64),
            Value::USmallInt(v) => Some(*v as i64),
            Value::UInteger(v) => Some(*v as i64),
            Value::UBigInt(v) => i64::try_from(*v).ok(),
            Value::UHugeInt(v) => i64::try_from(*v).ok(),
            _ => None,
        }
    }

    /// Cast this value to another logical type.
    pub fn cast(&self, target_type: &LogicalType) -> crate::error::Result<Value> {
        if self.is_null() {
            return Ok(Value::Null(target_type.clone()));
        }
        if &self.logical_type() == target_type {
            return Ok(self.clone());
        }

        match (self, target_type) {
            // Numeric casts
            (Value::Integer(v), LogicalType::BigInt) => Ok(Value::BigInt(*v as i64)),
            (Value::Integer(v), LogicalType::Double) => Ok(Value::Double(*v as f64)),
            (Value::BigInt(v), LogicalType::Double) => Ok(Value::Double(*v as f64)),
            (Value::Float(v), LogicalType::Double) => Ok(Value::Double(*v as f64)),
            (Value::Timestamp(v), LogicalType::TimestampTz) => Ok(Value::TimestampTz(*v)),
            (Value::TimestampTz(v), LogicalType::Timestamp) => Ok(Value::Timestamp(*v)),

            // Boolean to numeric
            (Value::Boolean(v), LogicalType::Integer) => Ok(Value::Integer(if *v { 1 } else { 0 })),

            // From Varchar
            (Value::Varchar(s), LogicalType::Integer) => s
                .parse::<i32>()
                .map(Value::Integer)
                .map_err(|_| paro_error::invalid_value(LogicalType::Integer.to_string(), s)),
            (Value::Varchar(s), LogicalType::Double) => s
                .parse::<f64>()
                .map(Value::Double)
                .map_err(|_| paro_error::invalid_value(LogicalType::Double.to_string(), s)),
            (Value::Varchar(s), LogicalType::Uuid) => parse_uuid_str(s).map(Value::Uuid),

            // To Varchar
            (_, LogicalType::Varchar) => Ok(Value::Varchar(self.to_string())),

            // Struct -> Struct (cast children by position)
            (Value::Struct(values, _fields), LogicalType::Struct(target_fields)) => {
                if values.len() != target_fields.len() {
                    return Err(paro_error::not_implemented(format!(
                        "Cast struct with {} fields to {} fields not implemented",
                        values.len(),
                        target_fields.len()
                    )));
                }
                let mut casted = Vec::with_capacity(values.len());
                for (value, (_, target_ty)) in values.iter().zip(target_fields.iter()) {
                    casted.push(value.cast(target_ty)?);
                }
                Ok(Value::Struct(casted, target_fields.clone()))
            }

            _ => Err(paro_error::not_implemented(format!(
                "Cast value {} to {} not implemented",
                self, target_type
            ))),
        }
    }

    /// Create a Value from an Option<String>
    pub fn from_option_string(opt: Option<String>) -> Self {
        match opt {
            Some(s) => Value::Varchar(s),
            None => Value::Null(LogicalType::Varchar),
        }
    }

    /// Create a Value from an Option<u64> (mapped to BigInt)
    pub fn from_option_u64(opt: Option<u64>) -> Self {
        match opt {
            Some(v) => Value::BigInt(v as i64),
            None => Value::Null(LogicalType::BigInt),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::TinyInt(a), Value::TinyInt(b)) => a.partial_cmp(b),
            (Value::SmallInt(a), Value::SmallInt(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::BigInt(a), Value::BigInt(b)) => a.partial_cmp(b),
            (Value::HugeInt(a), Value::HugeInt(b)) => a.partial_cmp(b),
            (Value::UTinyInt(a), Value::UTinyInt(b)) => a.partial_cmp(b),
            (Value::USmallInt(a), Value::USmallInt(b)) => a.partial_cmp(b),
            (Value::UInteger(a), Value::UInteger(b)) => a.partial_cmp(b),
            (Value::UBigInt(a), Value::UBigInt(b)) => a.partial_cmp(b),
            (Value::UHugeInt(a), Value::UHugeInt(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Double(a), Value::Double(b)) => a.partial_cmp(b),
            (Value::Decimal(av, ap, ascale), Value::Decimal(bv, bp, bscale)) => {
                if ap == bp && ascale == bscale {
                    av.partial_cmp(bv)
                } else {
                    None
                }
            }
            (Value::Varchar(a), Value::Varchar(b)) => a.partial_cmp(b),
            (Value::Uuid(a), Value::Uuid(b)) => a.partial_cmp(b),
            (Value::Date(a), Value::Date(b)) => a.partial_cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.partial_cmp(b),
            (Value::TimestampTz(a), Value::TimestampTz(b)) => a.partial_cmp(b),
            (Value::Time(a), Value::Time(b)) => a.partial_cmp(b),
            (Value::List(a, _), Value::List(b, _)) => a.partial_cmp(b),
            (Value::Struct(a_vals, a_fields), Value::Struct(b_vals, b_fields)) => {
                if a_fields == b_fields {
                    a_vals.partial_cmp(b_vals)
                } else {
                    None
                }
            }
            (Value::Array(a, _, _), Value::Array(b, _, _)) => a.partial_cmp(b),
            // Handle explicit Null comparison logic if needed (Nulls first/last)
            // For now, strict type match and valid comparison
            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null(_) => write!(f, "NULL"),
            Value::Boolean(v) => write!(f, "{}", v),
            Value::TinyInt(v) => write!(f, "{}", v),
            Value::SmallInt(v) => write!(f, "{}", v),
            Value::Integer(v) => write!(f, "{}", v),
            Value::BigInt(v) => write!(f, "{}", v),
            Value::HugeInt(v) => write!(f, "{}", v),
            Value::UTinyInt(v) => write!(f, "{}", v),
            Value::USmallInt(v) => write!(f, "{}", v),
            Value::UInteger(v) => write!(f, "{}", v),
            Value::UBigInt(v) => write!(f, "{}", v),
            Value::UHugeInt(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v),
            Value::Double(v) => write!(f, "{}", v),
            Value::Decimal(value, _precision, scale) => {
                write!(f, "{}", format_decimal_i128(*value, *scale))
            }
            Value::Varchar(v) => write!(f, "'{}'", v),
            Value::Blob(v) => write!(f, "BLOB[{}]", v.len()),
            Value::Uuid(v) => write!(f, "{}", format_uuid(*v)),
            Value::Date(days) => write!(f, "{}", format_date_days(*days as i64)),
            Value::Timestamp(micros) => write!(f, "{}", format_timestamp_micros(*micros)),
            Value::TimestampTz(micros) => {
                if *micros == i64::MAX {
                    write!(f, "infinity")
                } else if *micros == i64::MIN {
                    write!(f, "-infinity")
                } else {
                    write!(f, "{}+00", format_timestamp_micros(*micros))
                }
            }
            Value::Time(micros) => {
                let total_secs = (*micros / 1_000_000) as u64;
                let remaining = (*micros % 1_000_000) as u64;
                let h = total_secs / 3600;
                let m = (total_secs % 3600) / 60;
                let s = total_secs % 60;
                if remaining > 0 {
                    write!(f, "{:02}:{:02}:{:02}.{:06}", h, m, s, remaining)
                } else {
                    write!(f, "{:02}:{:02}:{:02}", h, m, s)
                }
            }
            Value::Interval(months, days, micros) => {
                write!(f, "{}", format_interval_parts(*months, *days, *micros))
            }
            Value::List(values, _) => {
                write!(f, "[")?;
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, "]")
            }
            Value::Struct(values, fields) => {
                write!(f, "STRUCT(")?;
                for (i, (field, value)) in fields.iter().zip(values.iter()).enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", field.0, value)?;
                }
                write!(f, ")")
            }
            // Array uses the same display format as List: [v1, v2, v3]
            Value::Array(values, _, _) => {
                write!(f, "[")?;
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, "]")
            }
        }
    }
}

// ============================================================================
// Decimal formatting helpers
// ============================================================================

/// Format a scaled decimal stored as an i128.
///
/// The `value` is an integer with an implicit scale of `10^scale`.
pub fn format_decimal_i128(value: i128, scale: u8) -> String {
    let mut digits = value.to_string();
    let negative = digits.starts_with('-');
    if negative {
        digits.remove(0);
    }

    if scale == 0 {
        return if negative {
            format!("-{}", digits)
        } else {
            digits
        };
    }

    let scale = scale as usize;
    if digits.len() <= scale {
        let mut frac = String::with_capacity(scale);
        for _ in 0..(scale - digits.len()) {
            frac.push('0');
        }
        frac.push_str(&digits);
        if negative {
            format!("-0.{}", frac)
        } else {
            format!("0.{}", frac)
        }
    } else {
        let split = digits.len() - scale;
        let (int_part, frac_part) = digits.split_at(split);
        if negative {
            format!("-{}.{}", int_part, frac_part)
        } else {
            format!("{}.{}", int_part, frac_part)
        }
    }
}

// ============================================================================
// Temporal formatting helpers
// ============================================================================

/// Format date (days since epoch) as YYYY-MM-DD string.
///
/// Uses the Howard Hinnant civil-from-days algorithm to convert
/// days since 1970-01-01 to a year/month/day triple.
pub fn format_date_days(days: i64) -> String {
    if days == i64::MAX {
        return "infinity".to_string();
    }
    if days == i64::MIN {
        return "-infinity".to_string();
    }
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = (yoe as i64 + era * 400) as i32;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}", y, m, d)
}

/// Format timestamp (microseconds since epoch) as YYYY-MM-DD HH:MM:SS[.ffffff] string.
pub fn format_timestamp_micros(micros: i64) -> String {
    if micros == i64::MAX {
        return "infinity".to_string();
    }
    if micros == i64::MIN {
        return "-infinity".to_string();
    }
    let total_seconds = micros / 1_000_000;
    let remaining_micros = (micros % 1_000_000).unsigned_abs();
    let days = if total_seconds >= 0 {
        total_seconds / 86400
    } else {
        (total_seconds - 86399) / 86400
    };
    let day_seconds = (total_seconds - days * 86400) as u64;
    let hours = day_seconds / 3600;
    let minutes = (day_seconds % 3600) / 60;
    let seconds = day_seconds % 60;
    let date = format_date_days(days);
    if remaining_micros > 0 {
        format!(
            "{} {:02}:{:02}:{:02}.{:06}",
            date, hours, minutes, seconds, remaining_micros
        )
    } else {
        format!("{} {:02}:{:02}:{:02}", date, hours, minutes, seconds)
    }
}

/// Format interval (months, days, micros) in a PostgreSQL-compatible text form.
pub fn format_interval_parts(months: i32, days: i32, micros: i64) -> String {
    const MICROS_PER_SEC: i64 = 1_000_000;
    const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SEC;
    const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;

    let mut out = String::new();
    let mut has_output = false;

    if months != 0 {
        let years = months / 12;
        let rem_months = months % 12;
        if years != 0 {
            if years == 1 || years == -1 {
                let _ = write!(out, "{} year", years);
            } else {
                let _ = write!(out, "{} years", years);
            }
            has_output = true;
        }
        if rem_months != 0 {
            if has_output {
                out.push(' ');
            }
            if rem_months == 1 || rem_months == -1 {
                let _ = write!(out, "{} month", rem_months);
            } else {
                let _ = write!(out, "{} months", rem_months);
            }
            has_output = true;
        }
    }

    if days != 0 {
        if has_output {
            out.push(' ');
        }
        if days == 1 || days == -1 {
            let _ = write!(out, "{} day", days);
        } else {
            let _ = write!(out, "{} days", days);
        }
        has_output = true;
    }

    if micros != 0 {
        if has_output {
            out.push(' ');
        }

        let mut remaining = micros.abs();
        let hours = remaining / MICROS_PER_HOUR;
        remaining %= MICROS_PER_HOUR;
        let minutes = remaining / MICROS_PER_MINUTE;
        remaining %= MICROS_PER_MINUTE;
        let seconds = remaining / MICROS_PER_SEC;
        let us = remaining % MICROS_PER_SEC;

        if micros < 0 {
            out.push('-');
        }

        if us == 0 {
            let _ = write!(out, "{:02}:{:02}:{:02}", hours, minutes, seconds);
        } else {
            let _ = write!(out, "{:02}:{:02}:{:02}.{:06}", hours, minutes, seconds, us);
        }
        has_output = true;
    }

    if !has_output {
        out.push_str("00:00:00");
    }

    out
}

/// Format a UUID (u128) as a canonical lowercase string: 8-4-4-4-12.
pub fn format_uuid(value: u128) -> String {
    let bytes = value.to_be_bytes();
    let mut out = String::with_capacity(36);
    for (i, b) in bytes.iter().enumerate() {
        if i == 4 || i == 6 || i == 8 || i == 10 {
            out.push('-');
        }
        let _ = write!(out, "{:02x}", b);
    }
    out
}

/// Parse a UUID string (accepts hyphenated 8-4-4-4-12 form, case-insensitive).
pub fn parse_uuid_str(input: &str) -> crate::error::Result<u128> {
    let s = input.trim();
    let mut hex = [0u8; 32];
    let mut hex_len = 0usize;

    for ch in s.chars() {
        if ch == '-' {
            continue;
        }
        if ch.is_ascii_hexdigit() {
            if hex_len >= 32 {
                return Err(paro_error::invalid_value("UUID", s));
            }
            hex[hex_len] = ch.to_ascii_lowercase() as u8;
            hex_len += 1;
        } else {
            return Err(paro_error::invalid_value("UUID", s));
        }
    }

    if hex_len != 32 {
        return Err(paro_error::invalid_value("UUID", s));
    }

    let mut bytes = [0u8; 16];
    for i in 0..16 {
        let pair = std::str::from_utf8(&hex[i * 2..i * 2 + 2])
            .map_err(|_| paro_error::invalid_value("UUID", s))?;
        let byte =
            u8::from_str_radix(pair, 16).map_err(|_| paro_error::invalid_value("UUID", s))?;
        bytes[i] = byte;
    }

    Ok(u128::from_be_bytes(bytes))
}

// ============================================================================
// Type-specific Value Getters
// ============================================================================
// These structs provide type-safe access to nested value children.

/// Helper struct for Array value operations.
pub struct ArrayValue;

impl ArrayValue {
    /// Get the children (elements) of an Array value.
    ///
    /// # Panics
    /// Panics if the value is NULL or not an Array type.
    #[inline]
    pub fn get_children(value: &Value) -> &[Value] {
        match value {
            Value::Array(children, _, _) => children.as_slice(),
            Value::Null(_) => panic!("Calling ArrayValue::get_children on a NULL value"),
            _ => panic!(
                "ArrayValue::get_children called on non-Array value: {:?}",
                value.logical_type()
            ),
        }
    }

    /// Get the size of an Array value.
    ///
    /// # Panics
    /// Panics if the value is not an Array type.
    #[inline]
    pub fn get_size(value: &Value) -> usize {
        match value {
            Value::Array(_, _, size) => *size,
            _ => panic!(
                "ArrayValue::get_size called on non-Array value: {:?}",
                value.logical_type()
            ),
        }
    }

    /// Get the element type of an Array value.
    ///
    /// # Panics
    /// Panics if the value is not an Array type.
    #[inline]
    pub fn get_child_type(value: &Value) -> &LogicalType {
        match value {
            Value::Array(_, child_type, _) => child_type,
            _ => panic!(
                "ArrayValue::get_child_type called on non-Array value: {:?}",
                value.logical_type()
            ),
        }
    }
}

/// Helper struct for List value operations.
pub struct ListValue;

impl ListValue {
    /// Get the children (elements) of a List value.
    ///
    /// # Panics
    /// Panics if the value is NULL or not a List type.
    #[inline]
    pub fn get_children(value: &Value) -> &[Value] {
        match value {
            Value::List(children, _) => children.as_slice(),
            Value::Null(_) => panic!("Calling ListValue::get_children on a NULL value"),
            _ => panic!(
                "ListValue::get_children called on non-List value: {:?}",
                value.logical_type()
            ),
        }
    }

    /// Get the element type of a List value.
    ///
    /// # Panics
    /// Panics if the value is not a List type.
    #[inline]
    pub fn get_child_type(value: &Value) -> &LogicalType {
        match value {
            Value::List(_, child_type) => child_type,
            _ => panic!(
                "ListValue::get_child_type called on non-List value: {:?}",
                value.logical_type()
            ),
        }
    }
}

/// Helper struct for Struct value operations.
pub struct StructValue;

impl StructValue {
    /// Get the children (field values) of a Struct value.
    #[inline]
    pub fn get_children(value: &Value) -> &[Value] {
        match value {
            Value::Struct(children, _) => children.as_slice(),
            Value::Null(_) => panic!("Calling StructValue::get_children on a NULL value"),
            _ => panic!(
                "StructValue::get_children called on non-Struct value: {:?}",
                value.logical_type()
            ),
        }
    }

    /// Get the field definitions (name + type) of a Struct value.
    #[inline]
    pub fn get_fields(value: &Value) -> &[(String, LogicalType)] {
        match value {
            Value::Struct(_, fields) => fields.as_slice(),
            _ => panic!(
                "StructValue::get_fields called on non-Struct value: {:?}",
                value.logical_type()
            ),
        }
    }

    /// Get the field name at a specific index.
    #[inline]
    pub fn get_field_name(value: &Value, idx: usize) -> &str {
        match value {
            Value::Struct(_, fields) => fields[idx].0.as_str(),
            _ => panic!(
                "StructValue::get_field_name called on non-Struct value: {:?}",
                value.logical_type()
            ),
        }
    }

    /// Get the field type at a specific index.
    #[inline]
    pub fn get_field_type(value: &Value, idx: usize) -> &LogicalType {
        match value {
            Value::Struct(_, fields) => &fields[idx].1,
            _ => panic!(
                "StructValue::get_field_type called on non-Struct value: {:?}",
                value.logical_type()
            ),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LogicalType;

    #[test]
    fn test_array_value_creation() {
        let arr = Value::array(
            LogicalType::Float,
            vec![Value::Float(1.0), Value::Float(2.0), Value::Float(3.0)],
        );

        assert_eq!(
            arr.logical_type(),
            LogicalType::Array(Box::new(LogicalType::Float), 3)
        );
        assert!(!arr.is_null());
    }

    #[test]
    fn test_array_value_get_children() {
        let arr = Value::array(
            LogicalType::Integer,
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
        );

        let children = ArrayValue::get_children(&arr);
        assert_eq!(children.len(), 3);
        assert_eq!(children[0], Value::Integer(1));
        assert_eq!(children[1], Value::Integer(2));
        assert_eq!(children[2], Value::Integer(3));
    }

    #[test]
    fn test_array_value_get_size() {
        let arr = Value::array(
            LogicalType::Float,
            vec![Value::Float(1.0), Value::Float(2.0)],
        );

        assert_eq!(ArrayValue::get_size(&arr), 2);
    }

    #[test]
    fn test_array_value_get_child_type() {
        let arr = Value::array(
            LogicalType::Double,
            vec![Value::Double(1.0), Value::Double(2.0)],
        );

        assert_eq!(ArrayValue::get_child_type(&arr), &LogicalType::Double);
    }

    #[test]
    fn test_array_value_display() {
        let arr = Value::array(
            LogicalType::Integer,
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
        );

        assert_eq!(format!("{}", arr), "[1, 2, 3]");
    }

    #[test]
    fn test_array_value_equality() {
        let arr1 = Value::array(
            LogicalType::Integer,
            vec![Value::Integer(1), Value::Integer(2)],
        );
        let arr2 = Value::array(
            LogicalType::Integer,
            vec![Value::Integer(1), Value::Integer(2)],
        );
        let arr3 = Value::array(
            LogicalType::Integer,
            vec![Value::Integer(1), Value::Integer(3)],
        );

        assert_eq!(arr1, arr2);
        assert_ne!(arr1, arr3);
    }

    #[test]
    fn test_array_value_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let arr1 = Value::array(
            LogicalType::Integer,
            vec![Value::Integer(1), Value::Integer(2)],
        );
        let arr2 = Value::array(
            LogicalType::Integer,
            vec![Value::Integer(1), Value::Integer(2)],
        );

        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        arr1.hash(&mut hasher1);
        arr2.hash(&mut hasher2);

        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn test_list_value_creation() {
        let list = Value::list(
            LogicalType::Integer,
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
        );

        assert_eq!(
            list.logical_type(),
            LogicalType::List(Box::new(LogicalType::Integer))
        );
        assert!(!list.is_null());
    }

    #[test]
    fn test_list_value_get_children() {
        let list = Value::list(
            LogicalType::Integer,
            vec![Value::Integer(1), Value::Integer(2)],
        );

        let children = ListValue::get_children(&list);
        assert_eq!(children.len(), 2);
        assert_eq!(children[0], Value::Integer(1));
        assert_eq!(children[1], Value::Integer(2));
    }

    #[test]
    fn test_struct_value_creation() {
        let fields = vec![
            ("a".to_string(), LogicalType::Integer),
            ("b".to_string(), LogicalType::Varchar),
        ];
        let value = Value::struct_value(
            fields.clone(),
            vec![Value::Integer(7), Value::Varchar("x".to_string())],
        );

        assert_eq!(value.logical_type(), LogicalType::Struct(fields.clone()));
        let children = StructValue::get_children(&value);
        assert_eq!(children.len(), 2);
        assert_eq!(StructValue::get_field_name(&value, 0), "a");
        assert_eq!(
            StructValue::get_field_type(&value, 1),
            &LogicalType::Varchar
        );
    }

    #[test]
    fn test_empty_array() {
        let arr = Value::array(LogicalType::Integer, vec![]);
        assert_eq!(ArrayValue::get_size(&arr), 0);
        assert_eq!(ArrayValue::get_children(&arr).len(), 0);
    }

    #[test]
    fn test_nested_array_in_list() {
        // List of arrays: [[1, 2], [3, 4]]
        let arr1 = Value::array(
            LogicalType::Integer,
            vec![Value::Integer(1), Value::Integer(2)],
        );
        let arr2 = Value::array(
            LogicalType::Integer,
            vec![Value::Integer(3), Value::Integer(4)],
        );

        let list = Value::list(
            LogicalType::Array(Box::new(LogicalType::Integer), 2),
            vec![arr1, arr2],
        );

        let children = ListValue::get_children(&list);
        assert_eq!(children.len(), 2);
    }

    #[test]
    fn test_array_f32_helper() {
        let arr = Value::array_f32(&[1.0, 2.5, -3.0]);
        assert_eq!(
            arr.logical_type(),
            LogicalType::Array(Box::new(LogicalType::Float), 3)
        );
        assert_eq!(format!("{}", arr), "[1, 2.5, -3]");
    }

    #[test]
    fn test_array_value_as_i64() {
        let arr = Value::array_f32(&[1.0, 2.0]);
        assert_eq!(arr.as_i64(), None);
    }

    #[test]
    fn test_decimal_display() {
        let v1 = Value::Decimal(12345, 7, 2);
        let v2 = Value::Decimal(-5, 3, 2);
        let v3 = Value::Decimal(12, 4, 4);
        let v4 = Value::Decimal(42, 2, 0);

        assert_eq!(format!("{}", v1), "123.45");
        assert_eq!(format!("{}", v2), "-0.05");
        assert_eq!(format!("{}", v3), "0.0012");
        assert_eq!(format!("{}", v4), "42");
    }

    #[test]
    fn test_decimal_equality_and_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let a = Value::Decimal(12345, 7, 2);
        let b = Value::Decimal(12345, 7, 2);
        let c = Value::Decimal(12345, 8, 2);

        assert_eq!(a, b);
        assert_ne!(a, c);

        let mut ha = DefaultHasher::new();
        let mut hb = DefaultHasher::new();
        a.hash(&mut ha);
        b.hash(&mut hb);
        assert_eq!(ha.finish(), hb.finish());
    }
}
