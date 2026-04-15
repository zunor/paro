// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! ## Known Limitations
//! - Only implements core numeric cast rules (20 rules for MVP)
//! - No nested type casts (STRUCT, LIST, ARRAY)
//! - No decimal precision handling
//!
//! # Cost-Based Implicit Cast Rules
//!
//! This module implements cost-based implicit cast selection.
//! When multiple function overloads exist, the binder uses cast costs to select the best match.
//!
//! ## Cost Semantic
//! - `-1`: Implicit cast not allowed
//! - `0`: No-op (same type or UNKNOWN parameter)
//! - `1-150`: Cast cost (lower = higher priority)
//!
//! ## Design Principles
//! 1. **Target Type Cost**: Each type has a "preference" cost (e.g., BIGINT=101, VARCHAR=149)
//! 2. **Integer Literal Fitting**: `42` can fit into TINYINT (cost=12) or INTEGER (cost=11, preferred)
//! 3. **Implicit Cast Path**: Only safe, lossless casts are allowed implicitly

use crate::types::LogicalType;

/// Returns the preference cost for a target type.
/// Lower values indicate higher preference.
///
/// # Cost Table
/// - BIGINT: 101 (most preferred numeric)
/// - INTEGER: 102
/// - HUGEINT: 103
/// - DOUBLE: 104
/// - DECIMAL: 105
/// - TIMESTAMP variants: 119-123
/// - VARCHAR: 149 (least preferred, catchall)
/// - Nested types (LIST/STRUCT): 160
pub fn target_type_cost(ty: &LogicalType) -> i64 {
    match ty {
        // Numeric types (prefer wider integer types over narrower ones)
        LogicalType::BigInt => 101,
        LogicalType::Integer => 102,
        LogicalType::HugeInt => 103,
        LogicalType::Double => 104,
        LogicalType::Decimal { .. } => 105,

        // Float is less preferred (loss of precision)
        LogicalType::Float => 111,

        // Smaller integers have higher cost (less preferred)
        LogicalType::SmallInt => 112,
        LogicalType::TinyInt => 113,

        // Unsigned integers (similar cost to signed counterparts)
        LogicalType::UBigInt => 101,
        LogicalType::UInteger => 102,
        LogicalType::UHugeInt => 103,
        LogicalType::USmallInt => 112,
        LogicalType::UTinyInt => 113,

        // Temporal types
        LogicalType::TimestampTz => 119,
        LogicalType::Timestamp => 120,
        LogicalType::Date => 121,
        LogicalType::Time => 122,
        LogicalType::Interval => 123,

        // String is the universal fallback (highest cost)
        LogicalType::Varchar => 149,
        LogicalType::TsVector => 149,
        LogicalType::TsQuery => 149,
        LogicalType::Json => 149,
        LogicalType::Jsonb => 149,

        // Nested types
        LogicalType::List(_) | LogicalType::Struct(_) | LogicalType::Array(_, _) => 160,

        // Special types
        LogicalType::Boolean => 130,
        LogicalType::Blob => 140,

        // Default for unknown types
        _ => 110,
    }
}

/// Cost-based implicit cast rule engine.
///
/// This struct provides the core logic for determining if an implicit cast
/// from one type to another is allowed, and what the cost of that cast is.
pub struct CastRules;

impl CastRules {
    /// Returns the cost of performing an implicit cast from `from` to `to`.
    ///
    /// # Returns
    /// - `-1`: Implicit cast not possible
    /// - `0`: No-op (same type or UNKNOWN parameter)
    /// - `1-150`: Cast cost (lower is better)
    ///
    /// # Examples
    /// ```
    /// use paro_common::cast_rules::CastRules;
    /// use paro_common::types::LogicalType;
    ///
    /// // INTEGER -> BIGINT: allowed with cost 101
    /// assert_eq!(
    ///     CastRules::implicit_cast_cost(&LogicalType::Integer, &LogicalType::BigInt),
    ///     101
    /// );
    ///
    /// // VARCHAR -> INTEGER: not allowed implicitly
    /// assert_eq!(
    ///     CastRules::implicit_cast_cost(&LogicalType::Varchar, &LogicalType::Integer),
    ///     -1
    /// );
    /// ```
    pub fn implicit_cast_cost(from: &LogicalType, to: &LogicalType) -> i64 {
        // Fast path: same type
        if from == to {
            return 0;
        }

        // UNKNOWN target acts as a wildcard in function binding.
        if matches!(to, LogicalType::Unknown) {
            return 0;
        }

        // Special cases first
        match from {
            // UNKNOWN (parameter) can cast to anything for free
            LogicalType::Unknown => return 0,

            // NULL can cast to any type (with target type cost)
            LogicalType::Null => return target_type_cost(to),

            // Integer literal: check if it fits in target type
            LogicalType::IntegerLiteral(value) => {
                return Self::implicit_cast_integer_literal(*value, to);
            }

            // String literal: can cast to any type with low cost
            LogicalType::StringLiteral => {
                return if matches!(to, LogicalType::Varchar) {
                    1 // Prefer VARCHAR
                } else {
                    20 // Still acceptable for other types
                };
            }

            _ => {}
        }

        // String-like casts between VARCHAR/JSON/JSONB are allowed implicitly.
        if matches!(
            from,
            LogicalType::Varchar | LogicalType::Json | LogicalType::Jsonb
        ) {
            return match to {
                LogicalType::Varchar | LogicalType::Json | LogicalType::Jsonb => {
                    target_type_cost(to)
                }
                _ => -1,
            };
        }

        // Dispatch to type-specific cast rules
        match from {
            LogicalType::TinyInt => Self::implicit_cast_tinyint(to),
            LogicalType::SmallInt => Self::implicit_cast_smallint(to),
            LogicalType::Integer => Self::implicit_cast_integer(to),
            LogicalType::BigInt => Self::implicit_cast_bigint(to),
            LogicalType::HugeInt => Self::implicit_cast_hugeint(to),

            LogicalType::UTinyInt => Self::implicit_cast_utinyint(to),
            LogicalType::USmallInt => Self::implicit_cast_usmallint(to),
            LogicalType::UInteger => Self::implicit_cast_uinteger(to),
            LogicalType::UBigInt => Self::implicit_cast_ubigint(to),
            LogicalType::UHugeInt => Self::implicit_cast_uhugeint(to),

            LogicalType::Float => Self::implicit_cast_float(to),
            LogicalType::Double => Self::implicit_cast_double(to),

            LogicalType::Date => Self::implicit_cast_date(to),
            LogicalType::Timestamp => Self::implicit_cast_timestamp(to),
            LogicalType::TimestampTz => Self::implicit_cast_timestamptz(to),
            LogicalType::TsVector => Self::implicit_cast_tsvector(to),
            LogicalType::TsQuery => Self::implicit_cast_tsquery(to),

            LogicalType::Array(_, _) => Self::implicit_cast_array(from, to),
            LogicalType::List(_) => Self::implicit_cast_list(from, to),
            LogicalType::Struct(_) => Self::implicit_cast_struct(from, to),

            // Default: no implicit cast
            _ => -1,
        }
    }

    /// Check implicit cast for Arrays.
    fn implicit_cast_array(from: &LogicalType, to: &LogicalType) -> i64 {
        if from == to {
            return 0;
        }

        match (from, to) {
            (LogicalType::Array(from_child, _), LogicalType::List(to_child)) => {
                let child_cost = Self::implicit_cast_cost(from_child, to_child);
                if child_cost < 0 {
                    return -1;
                }
                if child_cost > 0 {
                    target_type_cost(to) + child_cost
                } else {
                    1
                }
            }
            (LogicalType::Array(from_child, from_size), LogicalType::Array(to_child, to_size)) => {
                // Sizes must match, or one must be generic (0)
                if *from_size != *to_size && *from_size != 0 && *to_size != 0 {
                    return -1;
                }

                // Child types must be implicitly castable
                let child_cost = Self::implicit_cast_cost(from_child, to_child);
                if child_cost < 0 {
                    return -1;
                }

                // If sizes don't match exactly (but are compatible), add a small cost
                let size_cost = if *from_size == *to_size { 0 } else { 1 };

                // If child types don't match exactly, add child cost
                if child_cost > 0 {
                    target_type_cost(to) + child_cost + size_cost
                } else {
                    size_cost
                }
            }
            _ => -1,
        }
    }

    /// Check implicit cast for Lists.
    fn implicit_cast_list(from: &LogicalType, to: &LogicalType) -> i64 {
        if from == to {
            return 0;
        }

        match (from, to) {
            (LogicalType::List(from_child), LogicalType::List(to_child)) => {
                let child_cost = Self::implicit_cast_cost(from_child, to_child);
                if child_cost < 0 {
                    return -1;
                }

                if child_cost > 0 {
                    target_type_cost(to) + child_cost
                } else {
                    0
                }
            }
            _ => -1,
        }
    }

    /// Check implicit cast for Structs (position-based).
    fn implicit_cast_struct(from: &LogicalType, to: &LogicalType) -> i64 {
        if from == to {
            return 0;
        }

        match (from, to) {
            (LogicalType::Struct(from_fields), LogicalType::Struct(to_fields)) => {
                if from_fields.len() != to_fields.len() {
                    return -1;
                }

                let mut child_cost_sum = 0;
                for ((_, from_ty), (_, to_ty)) in from_fields.iter().zip(to_fields.iter()) {
                    let child_cost = Self::implicit_cast_cost(from_ty, to_ty);
                    if child_cost < 0 {
                        return -1;
                    }
                    if child_cost > 0 {
                        child_cost_sum += child_cost;
                    }
                }

                if child_cost_sum > 0 {
                    target_type_cost(to) + child_cost_sum
                } else {
                    0
                }
            }
            _ => -1,
        }
    }

    /// Check if an integer literal fits in the target type.
    fn implicit_cast_integer_literal(value: i64, to: &LogicalType) -> i64 {
        match to {
            // Check if value fits in target range
            LogicalType::TinyInt if value >= i8::MIN as i64 && value <= i8::MAX as i64 => 12,
            LogicalType::SmallInt if value >= i16::MIN as i64 && value <= i16::MAX as i64 => 13,
            LogicalType::Integer if value >= i32::MIN as i64 && value <= i32::MAX as i64 => 11, // Prefer INTEGER
            LogicalType::BigInt => 12,
            LogicalType::HugeInt => 13,

            // Unsigned types (only if value is non-negative)
            LogicalType::UTinyInt if value >= 0 && value <= u8::MAX as i64 => 12,
            LogicalType::USmallInt if value >= 0 && value <= u16::MAX as i64 => 13,
            LogicalType::UInteger if value >= 0 && value <= u32::MAX as i64 => 11,
            LogicalType::UBigInt if value >= 0 => 12,
            LogicalType::UHugeInt if value >= 0 => 13,

            // Floating point types
            LogicalType::Float => 111,
            LogicalType::Double => 104,

            // Fallback: use the preferred type of the literal (INTEGER or BIGINT)
            _ => {
                let preferred = if value >= i32::MIN as i64 && value <= i32::MAX as i64 {
                    LogicalType::Integer
                } else {
                    LogicalType::BigInt
                };
                Self::implicit_cast_cost(&preferred, to)
            }
        }
    }

    // ========== Signed Integer Cast Rules ==========

    fn implicit_cast_tinyint(to: &LogicalType) -> i64 {
        match to {
            LogicalType::UTinyInt => target_type_cost(to),
            LogicalType::SmallInt => target_type_cost(to),
            LogicalType::Integer => target_type_cost(to),
            LogicalType::BigInt => target_type_cost(to),
            LogicalType::HugeInt => target_type_cost(to),
            LogicalType::Float => target_type_cost(to),
            LogicalType::Double => target_type_cost(to),
            LogicalType::Decimal { .. } => target_type_cost(to),
            _ => -1,
        }
    }

    fn implicit_cast_smallint(to: &LogicalType) -> i64 {
        match to {
            LogicalType::USmallInt => target_type_cost(to),
            LogicalType::Integer => target_type_cost(to),
            LogicalType::BigInt => target_type_cost(to),
            LogicalType::HugeInt => target_type_cost(to),
            LogicalType::Float => target_type_cost(to),
            LogicalType::Double => target_type_cost(to),
            LogicalType::Decimal { .. } => target_type_cost(to),
            _ => -1,
        }
    }

    fn implicit_cast_integer(to: &LogicalType) -> i64 {
        match to {
            LogicalType::UInteger => target_type_cost(to),
            LogicalType::BigInt => target_type_cost(to),
            LogicalType::HugeInt => target_type_cost(to),
            LogicalType::Float => target_type_cost(to),
            LogicalType::Double => target_type_cost(to),
            LogicalType::Decimal { .. } => target_type_cost(to),
            _ => -1,
        }
    }

    fn implicit_cast_bigint(to: &LogicalType) -> i64 {
        match to {
            LogicalType::UInteger => target_type_cost(to),
            LogicalType::UBigInt => target_type_cost(to),
            LogicalType::HugeInt => target_type_cost(to),
            LogicalType::Float => target_type_cost(to),
            LogicalType::Double => target_type_cost(to),
            LogicalType::Decimal { .. } => target_type_cost(to),
            _ => -1,
        }
    }

    fn implicit_cast_hugeint(to: &LogicalType) -> i64 {
        match to {
            LogicalType::Float => target_type_cost(to),
            LogicalType::Double => target_type_cost(to),
            LogicalType::Decimal { .. } => target_type_cost(to),
            _ => -1,
        }
    }

    // ========== Unsigned Integer Cast Rules ==========

    fn implicit_cast_utinyint(to: &LogicalType) -> i64 {
        match to {
            LogicalType::USmallInt => target_type_cost(to),
            LogicalType::UInteger => target_type_cost(to),
            LogicalType::UBigInt => target_type_cost(to),
            LogicalType::UHugeInt => target_type_cost(to),
            // Can promote to signed if range allows
            LogicalType::SmallInt => target_type_cost(to),
            LogicalType::Integer => target_type_cost(to),
            LogicalType::BigInt => target_type_cost(to),
            LogicalType::HugeInt => target_type_cost(to),
            LogicalType::Float => target_type_cost(to),
            LogicalType::Double => target_type_cost(to),
            LogicalType::Decimal { .. } => target_type_cost(to),
            _ => -1,
        }
    }

    fn implicit_cast_usmallint(to: &LogicalType) -> i64 {
        match to {
            LogicalType::UInteger => target_type_cost(to),
            LogicalType::UBigInt => target_type_cost(to),
            LogicalType::UHugeInt => target_type_cost(to),
            LogicalType::Integer => target_type_cost(to),
            LogicalType::BigInt => target_type_cost(to),
            LogicalType::HugeInt => target_type_cost(to),
            LogicalType::Float => target_type_cost(to),
            LogicalType::Double => target_type_cost(to),
            LogicalType::Decimal { .. } => target_type_cost(to),
            _ => -1,
        }
    }

    fn implicit_cast_uinteger(to: &LogicalType) -> i64 {
        match to {
            LogicalType::UBigInt => target_type_cost(to),
            LogicalType::UHugeInt => target_type_cost(to),
            LogicalType::BigInt => target_type_cost(to),
            LogicalType::HugeInt => target_type_cost(to),
            LogicalType::Float => target_type_cost(to),
            LogicalType::Double => target_type_cost(to),
            LogicalType::Decimal { .. } => target_type_cost(to),
            _ => -1,
        }
    }

    fn implicit_cast_ubigint(to: &LogicalType) -> i64 {
        match to {
            LogicalType::UHugeInt => target_type_cost(to),
            LogicalType::HugeInt => target_type_cost(to),
            LogicalType::Float => target_type_cost(to),
            LogicalType::Double => target_type_cost(to),
            LogicalType::Decimal { .. } => target_type_cost(to),
            _ => -1,
        }
    }

    fn implicit_cast_uhugeint(to: &LogicalType) -> i64 {
        match to {
            LogicalType::Float => target_type_cost(to),
            LogicalType::Double => target_type_cost(to),
            LogicalType::Decimal { .. } => target_type_cost(to),
            _ => -1,
        }
    }

    // ========== Floating Point Cast Rules ==========

    fn implicit_cast_float(to: &LogicalType) -> i64 {
        match to {
            LogicalType::Double => target_type_cost(to),
            _ => -1,
        }
    }

    fn implicit_cast_double(_to: &LogicalType) -> i64 {
        // DOUBLE cannot be implicitly cast to anything (potential precision loss)
        -1
    }

    // ========== Temporal Cast Rules ==========

    fn implicit_cast_date(to: &LogicalType) -> i64 {
        match to {
            LogicalType::Timestamp | LogicalType::TimestampTz => target_type_cost(to),
            _ => -1,
        }
    }

    fn implicit_cast_timestamp(to: &LogicalType) -> i64 {
        match to {
            LogicalType::TimestampTz => target_type_cost(to),
            _ => -1,
        }
    }

    fn implicit_cast_timestamptz(to: &LogicalType) -> i64 {
        match to {
            LogicalType::Timestamp => target_type_cost(to),
            _ => -1,
        }
    }

    fn implicit_cast_tsvector(_to: &LogicalType) -> i64 {
        // TSVECTOR should only be converted explicitly.
        -1
    }

    fn implicit_cast_tsquery(_to: &LogicalType) -> i64 {
        // TSQUERY should only be converted explicitly.
        -1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LogicalType;

    #[test]
    fn test_same_type_no_cost() {
        assert_eq!(
            CastRules::implicit_cast_cost(&LogicalType::Integer, &LogicalType::Integer),
            0
        );
    }

    #[test]
    fn test_integer_to_bigint() {
        assert_eq!(
            CastRules::implicit_cast_cost(&LogicalType::Integer, &LogicalType::BigInt),
            101 // TargetTypeCost(BIGINT)
        );
    }

    #[test]
    fn test_integer_to_double() {
        assert_eq!(
            CastRules::implicit_cast_cost(&LogicalType::Integer, &LogicalType::Double),
            104 // TargetTypeCost(DOUBLE)
        );
    }

    #[test]
    fn test_bigint_to_double() {
        assert_eq!(
            CastRules::implicit_cast_cost(&LogicalType::BigInt, &LogicalType::Double),
            104
        );
    }

    #[test]
    fn test_float_to_double() {
        assert_eq!(
            CastRules::implicit_cast_cost(&LogicalType::Float, &LogicalType::Double),
            104
        );
    }

    #[test]
    fn test_no_implicit_varchar_to_int() {
        assert_eq!(
            CastRules::implicit_cast_cost(&LogicalType::Varchar, &LogicalType::Integer),
            -1
        );
    }

    #[test]
    fn test_no_implicit_double_to_int() {
        assert_eq!(
            CastRules::implicit_cast_cost(&LogicalType::Double, &LogicalType::Integer),
            -1
        );
    }

    #[test]
    fn test_timestamp_tz_implicit_casts() {
        assert_eq!(
            CastRules::implicit_cast_cost(&LogicalType::Timestamp, &LogicalType::TimestampTz),
            target_type_cost(&LogicalType::TimestampTz)
        );
        assert_eq!(
            CastRules::implicit_cast_cost(&LogicalType::TimestampTz, &LogicalType::Timestamp),
            target_type_cost(&LogicalType::Timestamp)
        );
    }

    #[test]
    fn test_integer_literal_fits_tinyint() {
        let lit = LogicalType::IntegerLiteral(42);
        assert_eq!(
            CastRules::implicit_cast_cost(&lit, &LogicalType::TinyInt),
            12
        );
    }

    #[test]
    fn test_integer_literal_prefers_integer() {
        let lit = LogicalType::IntegerLiteral(1000);
        let cost_to_int = CastRules::implicit_cast_cost(&lit, &LogicalType::Integer);
        let cost_to_bigint = CastRules::implicit_cast_cost(&lit, &LogicalType::BigInt);
        assert_eq!(cost_to_int, 11);
        assert_eq!(cost_to_bigint, 12);
        assert!(cost_to_int < cost_to_bigint); // INTEGER is preferred
    }

    #[test]
    fn test_integer_literal_too_large_for_tinyint() {
        let lit = LogicalType::IntegerLiteral(200); // > i8::MAX
                                                    // Should fallback to INTEGER -> TINYINT, which is not allowed
        assert_eq!(
            CastRules::implicit_cast_cost(&lit, &LogicalType::TinyInt),
            -1
        );
    }

    #[test]
    fn test_string_literal_to_varchar() {
        assert_eq!(
            CastRules::implicit_cast_cost(&LogicalType::StringLiteral, &LogicalType::Varchar),
            1
        );
    }

    #[test]
    fn test_string_literal_to_integer() {
        assert_eq!(
            CastRules::implicit_cast_cost(&LogicalType::StringLiteral, &LogicalType::Integer),
            20
        );
    }

    #[test]
    fn test_null_to_any_type() {
        assert_eq!(
            CastRules::implicit_cast_cost(&LogicalType::Null, &LogicalType::Integer),
            102 // TargetTypeCost(INTEGER)
        );
    }

    #[test]
    fn test_unknown_to_any_type() {
        assert_eq!(
            CastRules::implicit_cast_cost(&LogicalType::Unknown, &LogicalType::Varchar),
            0 // Free cast
        );
    }

    #[test]
    fn test_any_to_unknown_type() {
        assert_eq!(
            CastRules::implicit_cast_cost(&LogicalType::Integer, &LogicalType::Unknown),
            0
        );
    }

    #[test]
    fn test_list_to_generic_list() {
        assert_eq!(
            CastRules::implicit_cast_cost(
                &LogicalType::List(Box::new(LogicalType::Integer)),
                &LogicalType::List(Box::new(LogicalType::Unknown))
            ),
            0
        );
    }

    #[test]
    fn test_list_to_list_child_mismatch() {
        assert_eq!(
            CastRules::implicit_cast_cost(
                &LogicalType::List(Box::new(LogicalType::Integer)),
                &LogicalType::List(Box::new(LogicalType::Varchar))
            ),
            -1
        );
    }

    #[test]
    fn test_unsigned_to_signed() {
        assert_eq!(
            CastRules::implicit_cast_cost(&LogicalType::UInteger, &LogicalType::BigInt),
            101 // Allowed
        );
    }

    #[test]
    fn test_date_to_timestamp() {
        assert_eq!(
            CastRules::implicit_cast_cost(&LogicalType::Date, &LogicalType::Timestamp),
            120 // TargetTypeCost(TIMESTAMP)
        );
    }

    #[test]
    fn test_no_implicit_varchar_to_ts_types() {
        assert_eq!(
            CastRules::implicit_cast_cost(&LogicalType::Varchar, &LogicalType::TsVector),
            -1
        );
        assert_eq!(
            CastRules::implicit_cast_cost(&LogicalType::Varchar, &LogicalType::TsQuery),
            -1
        );
    }

    #[test]
    fn test_no_implicit_ts_types_to_varchar() {
        assert_eq!(
            CastRules::implicit_cast_cost(&LogicalType::TsVector, &LogicalType::Varchar),
            -1
        );
        assert_eq!(
            CastRules::implicit_cast_cost(&LogicalType::TsQuery, &LogicalType::Varchar),
            -1
        );
    }
}
