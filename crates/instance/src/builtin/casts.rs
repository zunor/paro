//! Built-in Casts Registration
//!
//! ## Overview
//! This module handles the registration of all built-in type casts into the CastFunctionSet.
//!
//! ## Cast Categories
//! - Numeric casts: Between integer and floating point types
//! - String casts: To/from VARCHAR
//! - Boolean casts: To/from BOOLEAN
//! - Date casts: Between date/time types
//! - Literal casts: From NULL and literal types
//!
//! ## Usage
//! ```rust,ignore
//! // Casts are registered when creating CastFunctionSet
//! let mut cast_functions = CastFunctionSet::new();
//! BuiltinCasts::register_all(&mut cast_functions);
//! ```

use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_function::scalar::cast::array_casts;
use paro_function::scalar::cast::boolean_casts;
use paro_function::scalar::cast::date_casts;
use paro_function::scalar::cast::decimal_casts;
use paro_function::scalar::cast::numeric_casts;
use paro_function::scalar::cast::string_casts;
use paro_function::scalar::cast::struct_casts;
use paro_function::scalar::cast::{BindCastInput, BoundCastInfo, CastFunctionSet};

/// BuiltinCasts handles the registration of all built-in type conversions.
///
/// - Casts are registered to CastFunctionSet
/// - register_all is the single entry point for cast registration
pub struct BuiltinCasts;

impl BuiltinCasts {
    /// Register all default casts into the provided set.
    pub fn register_all(set: &mut CastFunctionSet) {
        Self::register_numeric_casts(set);
        Self::register_string_casts(set);
        Self::register_boolean_casts(set);
        Self::register_date_casts(set);
        Self::register_literal_casts(set);
        Self::register_decimal_casts(set);
        Self::register_array_casts(set);
        Self::register_struct_casts(set);
    }

    fn register_numeric_casts(set: &mut CastFunctionSet) {
        // --- Signed Integer -> Signed Integer ---
        set.register_cast(
            LogicalType::TinyInt,
            LogicalType::SmallInt,
            BoundCastInfo::fixed(numeric_casts::int8_to_int16),
        );
        set.register_cast(
            LogicalType::TinyInt,
            LogicalType::Integer,
            BoundCastInfo::fixed(numeric_casts::int8_to_int32),
        );
        set.register_cast(
            LogicalType::TinyInt,
            LogicalType::BigInt,
            BoundCastInfo::fixed(numeric_casts::int8_to_int64),
        );
        set.register_cast(
            LogicalType::TinyInt,
            LogicalType::HugeInt,
            BoundCastInfo::fixed(numeric_casts::int8_to_int128),
        );

        set.register_cast(
            LogicalType::SmallInt,
            LogicalType::TinyInt,
            BoundCastInfo::fixed(numeric_casts::int16_to_int8),
        );
        set.register_cast(
            LogicalType::SmallInt,
            LogicalType::Integer,
            BoundCastInfo::fixed(numeric_casts::int16_to_int32),
        );
        set.register_cast(
            LogicalType::SmallInt,
            LogicalType::BigInt,
            BoundCastInfo::fixed(numeric_casts::int16_to_int64),
        );
        set.register_cast(
            LogicalType::SmallInt,
            LogicalType::HugeInt,
            BoundCastInfo::fixed(numeric_casts::int16_to_int128),
        );

        set.register_cast(
            LogicalType::Integer,
            LogicalType::TinyInt,
            BoundCastInfo::fixed(numeric_casts::int32_to_int8),
        );
        set.register_cast(
            LogicalType::Integer,
            LogicalType::SmallInt,
            BoundCastInfo::fixed(numeric_casts::int32_to_int16),
        );
        set.register_cast(
            LogicalType::Integer,
            LogicalType::BigInt,
            BoundCastInfo::fixed(numeric_casts::int32_to_int64),
        );
        set.register_cast(
            LogicalType::Integer,
            LogicalType::HugeInt,
            BoundCastInfo::fixed(numeric_casts::int32_to_int128),
        );

        set.register_cast(
            LogicalType::BigInt,
            LogicalType::TinyInt,
            BoundCastInfo::fixed(numeric_casts::int64_to_int8),
        );
        set.register_cast(
            LogicalType::BigInt,
            LogicalType::SmallInt,
            BoundCastInfo::fixed(numeric_casts::int64_to_int16),
        );
        set.register_cast(
            LogicalType::BigInt,
            LogicalType::Integer,
            BoundCastInfo::fixed(numeric_casts::int64_to_int32),
        );
        set.register_cast(
            LogicalType::BigInt,
            LogicalType::HugeInt,
            BoundCastInfo::fixed(numeric_casts::int64_to_int128),
        );

        set.register_cast(
            LogicalType::HugeInt,
            LogicalType::TinyInt,
            BoundCastInfo::fixed(numeric_casts::int128_to_int8),
        );
        set.register_cast(
            LogicalType::HugeInt,
            LogicalType::SmallInt,
            BoundCastInfo::fixed(numeric_casts::int128_to_int16),
        );
        set.register_cast(
            LogicalType::HugeInt,
            LogicalType::Integer,
            BoundCastInfo::fixed(numeric_casts::int128_to_int32),
        );
        set.register_cast(
            LogicalType::HugeInt,
            LogicalType::BigInt,
            BoundCastInfo::fixed(numeric_casts::int128_to_int64),
        );

        // --- Unsigned Integer -> Unsigned Integer ---
        set.register_cast(
            LogicalType::UTinyInt,
            LogicalType::USmallInt,
            BoundCastInfo::fixed(numeric_casts::uint8_to_uint16),
        );
        set.register_cast(
            LogicalType::UTinyInt,
            LogicalType::UInteger,
            BoundCastInfo::fixed(numeric_casts::uint8_to_uint32),
        );
        set.register_cast(
            LogicalType::UTinyInt,
            LogicalType::UBigInt,
            BoundCastInfo::fixed(numeric_casts::uint8_to_uint64),
        );

        set.register_cast(
            LogicalType::USmallInt,
            LogicalType::UTinyInt,
            BoundCastInfo::fixed(numeric_casts::uint16_to_uint8),
        );
        set.register_cast(
            LogicalType::USmallInt,
            LogicalType::UInteger,
            BoundCastInfo::fixed(numeric_casts::uint16_to_uint32),
        );
        set.register_cast(
            LogicalType::USmallInt,
            LogicalType::UBigInt,
            BoundCastInfo::fixed(numeric_casts::uint16_to_uint64),
        );

        set.register_cast(
            LogicalType::UInteger,
            LogicalType::UTinyInt,
            BoundCastInfo::fixed(numeric_casts::uint32_to_uint8),
        );
        set.register_cast(
            LogicalType::UInteger,
            LogicalType::USmallInt,
            BoundCastInfo::fixed(numeric_casts::uint32_to_uint16),
        );
        set.register_cast(
            LogicalType::UInteger,
            LogicalType::UBigInt,
            BoundCastInfo::fixed(numeric_casts::uint32_to_uint64),
        );

        set.register_cast(
            LogicalType::UBigInt,
            LogicalType::UTinyInt,
            BoundCastInfo::fixed(numeric_casts::uint64_to_uint8),
        );
        set.register_cast(
            LogicalType::UBigInt,
            LogicalType::USmallInt,
            BoundCastInfo::fixed(numeric_casts::uint64_to_uint16),
        );
        set.register_cast(
            LogicalType::UBigInt,
            LogicalType::UInteger,
            BoundCastInfo::fixed(numeric_casts::uint64_to_uint32),
        );

        // --- Signed <-> Unsigned ---
        set.register_cast(
            LogicalType::TinyInt,
            LogicalType::UTinyInt,
            BoundCastInfo::fixed(numeric_casts::int8_to_uint8),
        );
        set.register_cast(
            LogicalType::UTinyInt,
            LogicalType::TinyInt,
            BoundCastInfo::fixed(numeric_casts::uint8_to_int8),
        );
        set.register_cast(
            LogicalType::SmallInt,
            LogicalType::USmallInt,
            BoundCastInfo::fixed(numeric_casts::int16_to_uint16),
        );
        set.register_cast(
            LogicalType::USmallInt,
            LogicalType::SmallInt,
            BoundCastInfo::fixed(numeric_casts::uint16_to_int16),
        );
        set.register_cast(
            LogicalType::Integer,
            LogicalType::UTinyInt,
            BoundCastInfo::fixed(numeric_casts::int32_to_uint8),
        );
        set.register_cast(
            LogicalType::Integer,
            LogicalType::USmallInt,
            BoundCastInfo::fixed(numeric_casts::int32_to_uint16),
        );
        set.register_cast(
            LogicalType::Integer,
            LogicalType::UInteger,
            BoundCastInfo::fixed(numeric_casts::int32_to_uint32),
        );
        set.register_cast(
            LogicalType::Integer,
            LogicalType::UBigInt,
            BoundCastInfo::fixed(numeric_casts::int32_to_uint64),
        );
        set.register_cast(
            LogicalType::Integer,
            LogicalType::UHugeInt,
            BoundCastInfo::fixed(numeric_casts::int32_to_uint128),
        );
        set.register_cast(
            LogicalType::BigInt,
            LogicalType::UTinyInt,
            BoundCastInfo::fixed(numeric_casts::int64_to_uint8),
        );
        set.register_cast(
            LogicalType::BigInt,
            LogicalType::USmallInt,
            BoundCastInfo::fixed(numeric_casts::int64_to_uint16),
        );
        set.register_cast(
            LogicalType::BigInt,
            LogicalType::UInteger,
            BoundCastInfo::fixed(numeric_casts::int64_to_uint32),
        );
        set.register_cast(
            LogicalType::BigInt,
            LogicalType::UHugeInt,
            BoundCastInfo::fixed(numeric_casts::int64_to_uint128),
        );
        set.register_cast(
            LogicalType::UInteger,
            LogicalType::Integer,
            BoundCastInfo::fixed(numeric_casts::uint32_to_int32),
        );
        set.register_cast(
            LogicalType::BigInt,
            LogicalType::UBigInt,
            BoundCastInfo::fixed(numeric_casts::int64_to_uint64),
        );
        set.register_cast(
            LogicalType::HugeInt,
            LogicalType::UTinyInt,
            BoundCastInfo::fixed(numeric_casts::int128_to_uint8),
        );
        set.register_cast(
            LogicalType::HugeInt,
            LogicalType::USmallInt,
            BoundCastInfo::fixed(numeric_casts::int128_to_uint16),
        );
        set.register_cast(
            LogicalType::HugeInt,
            LogicalType::UInteger,
            BoundCastInfo::fixed(numeric_casts::int128_to_uint32),
        );
        set.register_cast(
            LogicalType::HugeInt,
            LogicalType::UBigInt,
            BoundCastInfo::fixed(numeric_casts::int128_to_uint64),
        );
        set.register_cast(
            LogicalType::HugeInt,
            LogicalType::UHugeInt,
            BoundCastInfo::fixed(numeric_casts::int128_to_uint128),
        );
        set.register_cast(
            LogicalType::UBigInt,
            LogicalType::BigInt,
            BoundCastInfo::fixed(numeric_casts::uint64_to_int64),
        );
        set.register_cast(
            LogicalType::UHugeInt,
            LogicalType::HugeInt,
            BoundCastInfo::fixed(numeric_casts::uint128_to_int128),
        );

        // --- Integer -> Float ---
        set.register_cast(
            LogicalType::Integer,
            LogicalType::Float,
            BoundCastInfo::fixed(numeric_casts::int32_to_float),
        );
        set.register_cast(
            LogicalType::Integer,
            LogicalType::Double,
            BoundCastInfo::fixed(numeric_casts::int32_to_double),
        );
        set.register_cast(
            LogicalType::BigInt,
            LogicalType::Float,
            BoundCastInfo::fixed(numeric_casts::int64_to_float),
        );
        set.register_cast(
            LogicalType::BigInt,
            LogicalType::Double,
            BoundCastInfo::fixed(numeric_casts::int64_to_double),
        );

        // --- Float -> Integer ---
        set.register_cast(
            LogicalType::Float,
            LogicalType::Integer,
            BoundCastInfo::fixed(numeric_casts::float_to_int32),
        );
        set.register_cast(
            LogicalType::Float,
            LogicalType::BigInt,
            BoundCastInfo::fixed(numeric_casts::float_to_int64),
        );
        set.register_cast(
            LogicalType::Double,
            LogicalType::Integer,
            BoundCastInfo::fixed(numeric_casts::double_to_int32),
        );
        set.register_cast(
            LogicalType::Double,
            LogicalType::BigInt,
            BoundCastInfo::fixed(numeric_casts::double_to_int64),
        );

        // --- Float -> Float ---
        set.register_cast(
            LogicalType::Float,
            LogicalType::Double,
            BoundCastInfo::fixed(numeric_casts::float_to_double),
        );
        set.register_cast(
            LogicalType::Double,
            LogicalType::Float,
            BoundCastInfo::fixed(numeric_casts::double_to_float),
        );
    }

    fn register_string_casts(set: &mut CastFunctionSet) {
        // --- Numeric -> VARCHAR ---
        set.register_cast(
            LogicalType::TinyInt,
            LogicalType::Varchar,
            BoundCastInfo::varlen(string_casts::numeric_to_varchar_cast::<i8>),
        );
        set.register_cast(
            LogicalType::SmallInt,
            LogicalType::Varchar,
            BoundCastInfo::varlen(string_casts::numeric_to_varchar_cast::<i16>),
        );
        set.register_cast(
            LogicalType::Integer,
            LogicalType::Varchar,
            BoundCastInfo::varlen(string_casts::numeric_to_varchar_cast::<i32>),
        );
        set.register_cast(
            LogicalType::BigInt,
            LogicalType::Varchar,
            BoundCastInfo::varlen(string_casts::numeric_to_varchar_cast::<i64>),
        );
        set.register_cast(
            LogicalType::HugeInt,
            LogicalType::Varchar,
            BoundCastInfo::varlen(string_casts::numeric_to_varchar_cast::<i128>),
        );

        set.register_cast(
            LogicalType::UTinyInt,
            LogicalType::Varchar,
            BoundCastInfo::varlen(string_casts::numeric_to_varchar_cast::<u8>),
        );
        set.register_cast(
            LogicalType::USmallInt,
            LogicalType::Varchar,
            BoundCastInfo::varlen(string_casts::numeric_to_varchar_cast::<u16>),
        );
        set.register_cast(
            LogicalType::UInteger,
            LogicalType::Varchar,
            BoundCastInfo::varlen(string_casts::numeric_to_varchar_cast::<u32>),
        );
        set.register_cast(
            LogicalType::UBigInt,
            LogicalType::Varchar,
            BoundCastInfo::varlen(string_casts::numeric_to_varchar_cast::<u64>),
        );
        set.register_cast(
            LogicalType::UHugeInt,
            LogicalType::Varchar,
            BoundCastInfo::varlen(string_casts::numeric_to_varchar_cast::<u128>),
        );
        set.register_cast(
            LogicalType::Uuid,
            LogicalType::Varchar,
            BoundCastInfo::varlen(string_casts::uuid_to_varchar_cast),
        );

        set.register_cast(
            LogicalType::Float,
            LogicalType::Varchar,
            BoundCastInfo::varlen(string_casts::numeric_to_varchar_cast::<f32>),
        );
        set.register_cast(
            LogicalType::Double,
            LogicalType::Varchar,
            BoundCastInfo::varlen(string_casts::numeric_to_varchar_cast::<f64>),
        );

        // --- JSON/JSONB <-> VARCHAR (varchar -> json validates) ---
        set.register_cast(
            LogicalType::Varchar,
            LogicalType::Json,
            BoundCastInfo::varlen(string_casts::varchar_to_json_cast),
        );
        set.register_cast(
            LogicalType::Varchar,
            LogicalType::Jsonb,
            BoundCastInfo::varlen(string_casts::varchar_to_jsonb_cast),
        );
        set.register_cast(
            LogicalType::Json,
            LogicalType::Varchar,
            BoundCastInfo::identity(&LogicalType::Json, &LogicalType::Varchar),
        );
        set.register_cast(
            LogicalType::Jsonb,
            LogicalType::Varchar,
            BoundCastInfo::identity(&LogicalType::Jsonb, &LogicalType::Varchar),
        );
        set.register_cast(
            LogicalType::Json,
            LogicalType::Jsonb,
            BoundCastInfo::identity(&LogicalType::Json, &LogicalType::Jsonb),
        );
        set.register_cast(
            LogicalType::Jsonb,
            LogicalType::Json,
            BoundCastInfo::identity(&LogicalType::Jsonb, &LogicalType::Json),
        );

        // --- VARCHAR -> Numeric ---
        set.register_cast(
            LogicalType::Varchar,
            LogicalType::TinyInt,
            BoundCastInfo::varlen(string_casts::varchar_to_numeric_cast::<i8>),
        );
        set.register_cast(
            LogicalType::Varchar,
            LogicalType::SmallInt,
            BoundCastInfo::varlen(string_casts::varchar_to_numeric_cast::<i16>),
        );
        set.register_cast(
            LogicalType::Varchar,
            LogicalType::Integer,
            BoundCastInfo::varlen(string_casts::varchar_to_numeric_cast::<i32>),
        );
        set.register_cast(
            LogicalType::Varchar,
            LogicalType::BigInt,
            BoundCastInfo::varlen(string_casts::varchar_to_numeric_cast::<i64>),
        );
        set.register_cast(
            LogicalType::Varchar,
            LogicalType::HugeInt,
            BoundCastInfo::varlen(string_casts::varchar_to_numeric_cast::<i128>),
        );

        set.register_cast(
            LogicalType::Varchar,
            LogicalType::UTinyInt,
            BoundCastInfo::varlen(string_casts::varchar_to_numeric_cast::<u8>),
        );
        set.register_cast(
            LogicalType::Varchar,
            LogicalType::USmallInt,
            BoundCastInfo::varlen(string_casts::varchar_to_numeric_cast::<u16>),
        );
        set.register_cast(
            LogicalType::Varchar,
            LogicalType::UInteger,
            BoundCastInfo::varlen(string_casts::varchar_to_numeric_cast::<u32>),
        );
        set.register_cast(
            LogicalType::Varchar,
            LogicalType::UBigInt,
            BoundCastInfo::varlen(string_casts::varchar_to_numeric_cast::<u64>),
        );
        set.register_cast(
            LogicalType::Varchar,
            LogicalType::UHugeInt,
            BoundCastInfo::varlen(string_casts::varchar_to_numeric_cast::<u128>),
        );
        set.register_cast(
            LogicalType::Varchar,
            LogicalType::Uuid,
            BoundCastInfo::varlen(string_casts::varchar_to_uuid_cast),
        );

        set.register_cast(
            LogicalType::Varchar,
            LogicalType::Float,
            BoundCastInfo::varlen(string_casts::varchar_to_numeric_cast::<f32>),
        );
        set.register_cast(
            LogicalType::Varchar,
            LogicalType::Double,
            BoundCastInfo::varlen(string_casts::varchar_to_numeric_cast::<f64>),
        );

        // --- VARCHAR -> BLOB ---
        set.register_cast(
            LogicalType::Varchar,
            LogicalType::Blob,
            BoundCastInfo::identity(&LogicalType::Varchar, &LogicalType::Blob),
        );

        // --- Full-text types -> VARCHAR (explicit CAST only; no implicit cast rule) ---
        set.register_cast(
            LogicalType::TsVector,
            LogicalType::Varchar,
            BoundCastInfo::identity(&LogicalType::TsVector, &LogicalType::Varchar),
        );
        set.register_cast(
            LogicalType::TsQuery,
            LogicalType::Varchar,
            BoundCastInfo::identity(&LogicalType::TsQuery, &LogicalType::Varchar),
        );
    }

    fn register_boolean_casts(set: &mut CastFunctionSet) {
        // --- Boolean -> Numeric ---
        set.register_cast(
            LogicalType::Boolean,
            LogicalType::TinyInt,
            BoundCastInfo::fixed(boolean_casts::bool_to_int8),
        );
        set.register_cast(
            LogicalType::Boolean,
            LogicalType::Integer,
            BoundCastInfo::fixed(boolean_casts::bool_to_int32),
        );
        set.register_cast(
            LogicalType::Boolean,
            LogicalType::BigInt,
            BoundCastInfo::fixed(boolean_casts::bool_to_int64),
        );

        // --- Numeric -> Boolean ---
        set.register_cast(
            LogicalType::TinyInt,
            LogicalType::Boolean,
            BoundCastInfo::fixed(boolean_casts::int8_to_bool),
        );
        set.register_cast(
            LogicalType::Integer,
            LogicalType::Boolean,
            BoundCastInfo::fixed(boolean_casts::int32_to_bool),
        );
        set.register_cast(
            LogicalType::BigInt,
            LogicalType::Boolean,
            BoundCastInfo::fixed(boolean_casts::int64_to_bool),
        );
        set.register_cast(
            LogicalType::Float,
            LogicalType::Boolean,
            BoundCastInfo::fixed(boolean_casts::float_to_bool),
        );
        set.register_cast(
            LogicalType::Double,
            LogicalType::Boolean,
            BoundCastInfo::fixed(boolean_casts::double_to_bool),
        );

        // --- Boolean <-> VARCHAR ---
        set.register_cast(
            LogicalType::Boolean,
            LogicalType::Varchar,
            BoundCastInfo::varlen(boolean_casts::bool_to_varchar),
        );
        set.register_cast(
            LogicalType::Varchar,
            LogicalType::Boolean,
            BoundCastInfo::varlen(boolean_casts::varchar_to_bool),
        );
    }

    fn register_date_casts(set: &mut CastFunctionSet) {
        // --- Date <-> VARCHAR ---
        set.register_cast(
            LogicalType::Date,
            LogicalType::Varchar,
            BoundCastInfo::varlen(date_casts::date_to_varchar),
        );
        set.register_cast(
            LogicalType::Varchar,
            LogicalType::Date,
            BoundCastInfo::varlen(date_casts::varchar_to_date),
        );

        // --- Timestamp <-> VARCHAR ---
        set.register_cast(
            LogicalType::Timestamp,
            LogicalType::Varchar,
            BoundCastInfo::varlen(date_casts::timestamp_to_varchar),
        );
        set.register_cast(
            LogicalType::Varchar,
            LogicalType::Timestamp,
            BoundCastInfo::varlen(date_casts::varchar_to_timestamp),
        );

        // --- TimestampTz <-> VARCHAR ---
        set.register_cast(
            LogicalType::TimestampTz,
            LogicalType::Varchar,
            BoundCastInfo::varlen(date_casts::timestamp_tz_to_varchar),
        );
        set.register_cast(
            LogicalType::Varchar,
            LogicalType::TimestampTz,
            BoundCastInfo::varlen(date_casts::varchar_to_timestamp_tz),
        );

        // --- Timestamp <-> TimestampTz ---
        set.register_cast(
            LogicalType::Timestamp,
            LogicalType::TimestampTz,
            BoundCastInfo::identity(&LogicalType::Timestamp, &LogicalType::TimestampTz),
        );
        set.register_cast(
            LogicalType::TimestampTz,
            LogicalType::Timestamp,
            BoundCastInfo::identity(&LogicalType::TimestampTz, &LogicalType::Timestamp),
        );

        // --- Time <-> VARCHAR ---
        set.register_cast(
            LogicalType::Time,
            LogicalType::Varchar,
            BoundCastInfo::varlen(date_casts::time_to_varchar),
        );
        set.register_cast(
            LogicalType::Varchar,
            LogicalType::Time,
            BoundCastInfo::varlen(date_casts::varchar_to_time),
        );

        // --- Interval <-> VARCHAR ---
        set.register_cast(
            LogicalType::Interval,
            LogicalType::Varchar,
            BoundCastInfo::varlen(date_casts::interval_to_varchar),
        );
        set.register_cast(
            LogicalType::Varchar,
            LogicalType::Interval,
            BoundCastInfo::varlen(date_casts::varchar_to_interval),
        );
    }

    fn register_literal_casts(set: &mut CastFunctionSet) {
        set.register_bind_function(Self::bind_literal_casts);
    }

    fn register_decimal_casts(set: &mut CastFunctionSet) {
        set.register_bind_function(decimal_casts::bind_decimal_casts);
    }

    fn bind_literal_casts(
        input: &BindCastInput,
        source: &LogicalType,
        target: &LogicalType,
    ) -> Result<Option<BoundCastInfo>> {
        match source {
            LogicalType::IntegerLiteral(_) => {
                // IntegerLiteral physically stores data as BigInt (i64) in Value::BigInt
                // So we can use any cast that works for BigInt
                input
                    .get_cast_function(&LogicalType::BigInt, target)
                    .map(Some)
            }
            LogicalType::StringLiteral => {
                // StringLiteral physically stores data as Varchar
                input
                    .get_cast_function(&LogicalType::Varchar, target)
                    .map(Some)
            }
            LogicalType::Null => {
                // Null can be cast to any type
                Ok(Some(BoundCastInfo::null(target)))
            }
            _ => Ok(None),
        }
    }

    /// Register Array type casts.
    ///
    /// Includes:
    /// - VARCHAR -> Array (pgvector-style '[1,2,3]' literals)
    /// - Array -> VARCHAR
    /// - Array -> Array (child type conversion)
    ///
    fn register_array_casts(set: &mut CastFunctionSet) {
        // Register dynamic bind function for Array casts
        // This handles VARCHAR -> Array, Array -> VARCHAR, and Array -> Array
        set.register_bind_function(array_casts::bind_array_casts);
    }

    /// Register Struct type casts.
    fn register_struct_casts(set: &mut CastFunctionSet) {
        set.register_bind_function(struct_casts::bind_struct_casts);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_function::scalar::cast::CastDispatch;

    #[test]
    fn test_fulltext_to_varchar_cast_registered() {
        let mut cast_functions = CastFunctionSet::new();
        BuiltinCasts::register_all(&mut cast_functions);

        assert!(cast_functions
            .get_cast_function(&LogicalType::TsVector, &LogicalType::Varchar)
            .is_ok());
        assert!(cast_functions
            .get_cast_function(&LogicalType::TsQuery, &LogicalType::Varchar)
            .is_ok());
        assert!(cast_functions
            .get_cast_function(&LogicalType::Varchar, &LogicalType::TsVector)
            .is_err());
        assert!(cast_functions
            .get_cast_function(&LogicalType::Varchar, &LogicalType::TsQuery)
            .is_err());
    }

    #[test]
    fn fixed_width_casts_bind_to_fixed_dispatch() {
        let mut cast_functions = CastFunctionSet::new();
        BuiltinCasts::register_all(&mut cast_functions);

        assert!(matches!(
            cast_functions
                .get_cast_function(&LogicalType::Integer, &LogicalType::BigInt)
                .expect("integer -> bigint cast")
                .dispatch,
            CastDispatch::Fixed(_)
        ));
        assert!(matches!(
            cast_functions
                .get_cast_function(&LogicalType::Boolean, &LogicalType::BigInt)
                .expect("boolean -> bigint cast")
                .dispatch,
            CastDispatch::Fixed(_)
        ));
        assert!(matches!(
            cast_functions
                .get_cast_function(&LogicalType::Timestamp, &LogicalType::TimestampTz)
                .expect("timestamp -> timestamptz cast")
                .dispatch,
            CastDispatch::Fixed(_)
        ));
        assert!(matches!(
            cast_functions
                .get_cast_function(&LogicalType::Varchar, &LogicalType::Integer)
                .expect("varchar -> integer cast")
                .dispatch,
            CastDispatch::Varlen(_)
        ));
    }
}
