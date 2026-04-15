// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL OID (Object Identifier) mappings for LogicalType.
//!
//! This module provides mappings from Paro's LogicalType to PostgreSQL's
//! type OIDs, which are used in the pgwire protocol for RowDescription messages.
//!
//! # References
//! - Type OIDs follow the PostgreSQL pg_type convention
//! - pgwire protocol: RowDescription message format

use super::LogicalType;

/// Boolean type OID
pub const BOOLOID: u32 = 16;

/// Binary data (bytea) type OID
pub const BYTEAOID: u32 = 17;

/// UUID type OID
pub const UUIDOID: u32 = 2950;

/// 64-bit integer (int8/bigint) type OID
pub const INT8OID: u32 = 20;

/// 16-bit integer (int2/smallint) type OID
pub const INT2OID: u32 = 21;

/// 32-bit integer (int4/integer) type OID
pub const INT4OID: u32 = 23;

/// Text type OID (variable-length string, no limit)
pub const TEXTOID: u32 = 25;

/// Single-precision float (float4/real) type OID
pub const FLOAT4OID: u32 = 700;

/// Double-precision float (float8/double precision) type OID
pub const FLOAT8OID: u32 = 701;

/// Variable-length character string (varchar) type OID
pub const VARCHAROID: u32 = 1043;

/// Date type OID
pub const DATEOID: u32 = 1082;

/// Time (without timezone) type OID
pub const TIMEOID: u32 = 1083;

/// Timestamp (without timezone) type OID
pub const TIMESTAMPOID: u32 = 1114;

/// Timestamp with timezone type OID
pub const TIMESTAMPTZOID: u32 = 1184;

/// Interval type OID
pub const INTERVALOID: u32 = 1186;

/// Numeric/Decimal type OID
pub const NUMERICOID: u32 = 1700;

/// PostgreSQL `tsvector` type OID
pub const TSVECTOROID: u32 = 3614;

/// PostgreSQL `tsquery` type OID
pub const TSQUERYOID: u32 = 3615;

/// PostgreSQL JSON type OID
pub const JSONOID: u32 = 114;

/// PostgreSQL JSONB type OID
pub const JSONBOID: u32 = 3802;

/// PostgreSQL array type OIDs (subset)
pub const BOOLARRAYOID: u32 = 1000;
pub const BYTEAARRAYOID: u32 = 1001;
pub const INT2ARRAYOID: u32 = 1005;
pub const INT4ARRAYOID: u32 = 1007;
pub const INT8ARRAYOID: u32 = 1016;
pub const FLOAT4ARRAYOID: u32 = 1021;
pub const FLOAT8ARRAYOID: u32 = 1022;
pub const TEXTARRAYOID: u32 = 1009;
pub const VARCHARARRAYOID: u32 = 1015;
pub const DATEARRAYOID: u32 = 1182;
pub const TIMEARRAYOID: u32 = 1183;
pub const TIMESTAMPARRAYOID: u32 = 1115;
pub const TIMESTAMPTZARRAYOID: u32 = 1185;
pub const INTERVALARRAYOID: u32 = 1187;
pub const UUIDARRAYOID: u32 = 2951;
pub const JSONARRAYOID: u32 = 199;
pub const JSONBARRAYOID: u32 = 3807;
pub const NUMERICARRAYOID: u32 = 1231;

impl LogicalType {
    /// Returns the PostgreSQL OID (Object Identifier) for this type.
    ///
    /// This is used in the pgwire protocol to identify column types in
    /// RowDescription messages.
    ///
    /// # OID Reference
    ///
    /// Common type OIDs (PostgreSQL-compatible):
    /// - Boolean: 16 (BOOLOID)
    /// - Integers: 21 (INT2OID), 23 (INT4OID), 20 (INT8OID)
    /// - Floats: 700 (FLOAT4OID), 701 (FLOAT8OID)
    /// - Strings: 25 (TEXTOID), 1043 (VARCHAROID)
    /// - Temporal: 1082 (DATEOID), 1114 (TIMESTAMPOID), 1184 (TIMESTAMPTZOID), 1083 (TIMEOID), 1186 (INTERVALOID)
    /// - Binary: 17 (BYTEAOID)
    /// - Numeric: 1700 (NUMERICOID)
    ///
    /// # Examples
    ///
    /// ```
    /// use paro_common::types::LogicalType;
    ///
    /// assert_eq!(LogicalType::Integer.to_pg_oid(), 23);
    /// assert_eq!(LogicalType::BigInt.to_pg_oid(), 20);
    /// assert_eq!(LogicalType::Varchar.to_pg_oid(), 1043);
    /// ```
    pub fn to_pg_oid(&self) -> u32 {
        self.pg_descriptor().oid
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_boolean_oid() {
        assert_eq!(LogicalType::Boolean.to_pg_oid(), BOOLOID);
    }

    #[test]
    fn test_integer_oids() {
        assert_eq!(LogicalType::TinyInt.to_pg_oid(), INT2OID);
        assert_eq!(LogicalType::SmallInt.to_pg_oid(), INT2OID);
        assert_eq!(LogicalType::Integer.to_pg_oid(), INT4OID);
        assert_eq!(LogicalType::BigInt.to_pg_oid(), INT8OID);
        assert_eq!(LogicalType::HugeInt.to_pg_oid(), NUMERICOID);
    }

    #[test]
    fn test_unsigned_integer_oids() {
        assert_eq!(LogicalType::UTinyInt.to_pg_oid(), INT2OID);
        assert_eq!(LogicalType::USmallInt.to_pg_oid(), INT4OID);
        assert_eq!(LogicalType::UInteger.to_pg_oid(), INT8OID);
        assert_eq!(LogicalType::UBigInt.to_pg_oid(), NUMERICOID);
        assert_eq!(LogicalType::UHugeInt.to_pg_oid(), NUMERICOID);
    }

    #[test]
    fn test_float_oids() {
        assert_eq!(LogicalType::Float.to_pg_oid(), FLOAT4OID);
        assert_eq!(LogicalType::Double.to_pg_oid(), FLOAT8OID);
    }

    #[test]
    fn test_decimal_oid() {
        assert_eq!(
            LogicalType::Decimal {
                precision: 10,
                scale: 2
            }
            .to_pg_oid(),
            NUMERICOID
        );
    }

    #[test]
    fn test_string_oids() {
        assert_eq!(LogicalType::Varchar.to_pg_oid(), VARCHAROID);
        assert_eq!(
            LogicalType::VarcharCollation("NOCASE".to_string()).to_pg_oid(),
            VARCHAROID
        );
        assert_eq!(LogicalType::TsVector.to_pg_oid(), TSVECTOROID);
        assert_eq!(LogicalType::TsQuery.to_pg_oid(), TSQUERYOID);
    }

    #[test]
    fn test_json_oids() {
        assert_eq!(LogicalType::Json.to_pg_oid(), JSONOID);
        assert_eq!(LogicalType::Jsonb.to_pg_oid(), JSONBOID);
    }

    #[test]
    fn test_temporal_oids() {
        assert_eq!(LogicalType::Date.to_pg_oid(), DATEOID);
        assert_eq!(LogicalType::Timestamp.to_pg_oid(), TIMESTAMPOID);
        assert_eq!(LogicalType::TimestampTz.to_pg_oid(), TIMESTAMPTZOID);
        assert_eq!(LogicalType::Time.to_pg_oid(), TIMEOID);
        assert_eq!(LogicalType::Interval.to_pg_oid(), INTERVALOID);
    }

    #[test]
    fn test_blob_oid() {
        assert_eq!(LogicalType::Blob.to_pg_oid(), BYTEAOID);
    }

    #[test]
    fn test_uuid_oid() {
        assert_eq!(LogicalType::Uuid.to_pg_oid(), UUIDOID);
    }

    #[test]
    fn test_special_type_oids() {
        assert_eq!(LogicalType::Null.to_pg_oid(), TEXTOID);
        assert_eq!(LogicalType::IntegerLiteral(42).to_pg_oid(), INT4OID);
        assert_eq!(LogicalType::StringLiteral.to_pg_oid(), TEXTOID);
        assert_eq!(LogicalType::Unknown.to_pg_oid(), TEXTOID);
    }

    #[test]
    fn test_array_oids_follow_advertised_element_oids() {
        assert_eq!(LogicalType::embedding(1536).to_pg_oid(), FLOAT4ARRAYOID);
        assert_eq!(
            LogicalType::Array(Box::new(LogicalType::USmallInt), 4).to_pg_oid(),
            INT4ARRAYOID
        );
        assert_eq!(
            LogicalType::List(Box::new(LogicalType::UBigInt)).to_pg_oid(),
            NUMERICARRAYOID
        );
    }

    #[test]
    fn test_verification_standard() {
        assert_eq!(LogicalType::BigInt.to_pg_oid(), 20);
        assert_eq!(LogicalType::Integer.to_pg_oid(), 23);
        assert_eq!(LogicalType::Varchar.to_pg_oid(), 1043);
    }
}
