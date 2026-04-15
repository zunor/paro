// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::error::{self as paro_error, Result};

use super::pg_oid::{
    BOOLARRAYOID, BOOLOID, BYTEAARRAYOID, BYTEAOID, DATEARRAYOID, DATEOID, FLOAT4ARRAYOID,
    FLOAT4OID, FLOAT8ARRAYOID, FLOAT8OID, INT2ARRAYOID, INT2OID, INT4ARRAYOID, INT4OID,
    INT8ARRAYOID, INT8OID, INTERVALARRAYOID, INTERVALOID, JSONARRAYOID, JSONBARRAYOID, JSONBOID,
    JSONOID, NUMERICARRAYOID, NUMERICOID, TEXTARRAYOID, TEXTOID, TIMEARRAYOID, TIMEOID,
    TIMESTAMPARRAYOID, TIMESTAMPOID, TIMESTAMPTZARRAYOID, TIMESTAMPTZOID, TSQUERYOID, TSVECTOROID,
    UUIDARRAYOID, UUIDOID, VARCHARARRAYOID, VARCHAROID,
};
use super::LogicalType;

const NO_TYPE_MODIFIER: i32 = -1;
const VARIABLE_TYPE_SIZE: i16 = -1;
const VARHDRSZ: i32 = 4;

/// Complete PostgreSQL type metadata needed for RowDescription field descriptors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgTypeDescriptor {
    pub oid: u32,
    pub type_size: i16,
    pub type_modifier: i32,
}

impl LogicalType {
    /// Returns the PostgreSQL protocol type descriptor advertised for this logical type.
    pub fn pg_descriptor(&self) -> PgTypeDescriptor {
        let oid = match self {
            LogicalType::Boolean => BOOLOID,
            LogicalType::TinyInt | LogicalType::SmallInt => INT2OID,
            LogicalType::Integer => INT4OID,
            LogicalType::BigInt => INT8OID,
            LogicalType::HugeInt => NUMERICOID,
            LogicalType::UTinyInt => INT2OID,
            LogicalType::USmallInt => INT4OID,
            LogicalType::UInteger => INT8OID,
            LogicalType::UBigInt | LogicalType::UHugeInt => NUMERICOID,
            LogicalType::Float => FLOAT4OID,
            LogicalType::Double => FLOAT8OID,
            LogicalType::Decimal { .. } => NUMERICOID,
            LogicalType::Varchar | LogicalType::VarcharCollation(_) => VARCHAROID,
            LogicalType::TsVector => TSVECTOROID,
            LogicalType::TsQuery => TSQUERYOID,
            LogicalType::Json => JSONOID,
            LogicalType::Jsonb => JSONBOID,
            LogicalType::Date => DATEOID,
            LogicalType::Timestamp => TIMESTAMPOID,
            LogicalType::TimestampTz => TIMESTAMPTZOID,
            LogicalType::Time => TIMEOID,
            LogicalType::Interval => INTERVALOID,
            LogicalType::Blob => BYTEAOID,
            LogicalType::Uuid => UUIDOID,
            LogicalType::Null | LogicalType::StringLiteral | LogicalType::Unknown => TEXTOID,
            LogicalType::IntegerLiteral(_) => INT4OID,
            LogicalType::Array(elem, _) | LogicalType::List(elem) => array_oid_for_element(elem),
            LogicalType::Struct(_) => TEXTOID,
        };

        PgTypeDescriptor {
            oid,
            type_size: type_size_for_pg_oid(oid),
            type_modifier: type_modifier_for_logical_type(self),
        }
    }
}

pub fn logical_type_from_pg_oid(oid: u32) -> Result<Option<LogicalType>> {
    let ty = match oid {
        0 => return Ok(None),
        BOOLOID => LogicalType::Boolean,
        BYTEAOID => LogicalType::Blob,
        INT8OID => LogicalType::BigInt,
        INT2OID => LogicalType::SmallInt,
        INT4OID => LogicalType::Integer,
        TEXTOID | VARCHAROID => LogicalType::Varchar,
        FLOAT4OID => LogicalType::Float,
        FLOAT8OID => LogicalType::Double,
        DATEOID => LogicalType::Date,
        TIMEOID => LogicalType::Time,
        TIMESTAMPOID => LogicalType::Timestamp,
        TIMESTAMPTZOID => LogicalType::TimestampTz,
        INTERVALOID => LogicalType::Interval,
        UUIDOID => LogicalType::Uuid,
        NUMERICOID => LogicalType::Decimal {
            precision: 0,
            scale: 0,
        },
        JSONOID => LogicalType::Json,
        JSONBOID => LogicalType::Jsonb,
        BOOLARRAYOID => LogicalType::List(Box::new(LogicalType::Boolean)),
        BYTEAARRAYOID => LogicalType::List(Box::new(LogicalType::Blob)),
        INT2ARRAYOID => LogicalType::List(Box::new(LogicalType::SmallInt)),
        INT4ARRAYOID => LogicalType::List(Box::new(LogicalType::Integer)),
        INT8ARRAYOID => LogicalType::List(Box::new(LogicalType::BigInt)),
        FLOAT4ARRAYOID => LogicalType::List(Box::new(LogicalType::Float)),
        FLOAT8ARRAYOID => LogicalType::List(Box::new(LogicalType::Double)),
        TEXTARRAYOID | VARCHARARRAYOID => LogicalType::List(Box::new(LogicalType::Varchar)),
        DATEARRAYOID => LogicalType::List(Box::new(LogicalType::Date)),
        TIMEARRAYOID => LogicalType::List(Box::new(LogicalType::Time)),
        TIMESTAMPARRAYOID => LogicalType::List(Box::new(LogicalType::Timestamp)),
        TIMESTAMPTZARRAYOID => LogicalType::List(Box::new(LogicalType::TimestampTz)),
        INTERVALARRAYOID => LogicalType::List(Box::new(LogicalType::Interval)),
        UUIDARRAYOID => LogicalType::List(Box::new(LogicalType::Uuid)),
        JSONARRAYOID => LogicalType::List(Box::new(LogicalType::Json)),
        JSONBARRAYOID => LogicalType::List(Box::new(LogicalType::Jsonb)),
        NUMERICARRAYOID => LogicalType::List(Box::new(LogicalType::Decimal {
            precision: 0,
            scale: 0,
        })),
        other => {
            return Err(paro_error::not_implemented(format!(
                "protocol parameter type OID {other} is not supported yet",
            )))
        }
    };
    Ok(Some(ty))
}

pub(crate) fn array_oid_for_element(elem: &LogicalType) -> u32 {
    match elem.pg_descriptor().oid {
        BOOLOID => BOOLARRAYOID,
        BYTEAOID => BYTEAARRAYOID,
        INT2OID => INT2ARRAYOID,
        INT4OID => INT4ARRAYOID,
        INT8OID => INT8ARRAYOID,
        FLOAT4OID => FLOAT4ARRAYOID,
        FLOAT8OID => FLOAT8ARRAYOID,
        VARCHAROID => VARCHARARRAYOID,
        TEXTOID => TEXTARRAYOID,
        DATEOID => DATEARRAYOID,
        TIMEOID => TIMEARRAYOID,
        TIMESTAMPOID => TIMESTAMPARRAYOID,
        TIMESTAMPTZOID => TIMESTAMPTZARRAYOID,
        INTERVALOID => INTERVALARRAYOID,
        UUIDOID => UUIDARRAYOID,
        JSONOID => JSONARRAYOID,
        JSONBOID => JSONBARRAYOID,
        NUMERICOID => NUMERICARRAYOID,
        _ => TEXTARRAYOID,
    }
}

fn type_size_for_pg_oid(oid: u32) -> i16 {
    match oid {
        BOOLOID => 1,
        INT2OID => 2,
        INT4OID => 4,
        INT8OID => 8,
        FLOAT4OID => 4,
        FLOAT8OID => 8,
        DATEOID => 4,
        TIMEOID | TIMESTAMPOID | TIMESTAMPTZOID => 8,
        INTERVALOID | UUIDOID => 16,
        BYTEAOID | NUMERICOID | TEXTOID | VARCHAROID | TSVECTOROID | TSQUERYOID | JSONOID
        | JSONBOID | BOOLARRAYOID | BYTEAARRAYOID | INT2ARRAYOID | INT4ARRAYOID | INT8ARRAYOID
        | FLOAT4ARRAYOID | FLOAT8ARRAYOID | TEXTARRAYOID | VARCHARARRAYOID | DATEARRAYOID
        | TIMEARRAYOID | TIMESTAMPARRAYOID | TIMESTAMPTZARRAYOID | INTERVALARRAYOID
        | UUIDARRAYOID | JSONARRAYOID | JSONBARRAYOID | NUMERICARRAYOID => VARIABLE_TYPE_SIZE,
        _ => VARIABLE_TYPE_SIZE,
    }
}

fn type_modifier_for_logical_type(logical_type: &LogicalType) -> i32 {
    match logical_type {
        LogicalType::Decimal { precision: 0, .. } => NO_TYPE_MODIFIER,
        LogicalType::Decimal { precision, scale } => {
            ((i32::from(*precision) << 16) | i32::from(*scale)) + VARHDRSZ
        }
        _ => NO_TYPE_MODIFIER,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_descriptor_uses_advertised_wire_widths() {
        assert_eq!(
            LogicalType::TinyInt.pg_descriptor(),
            PgTypeDescriptor {
                oid: INT2OID,
                type_size: 2,
                type_modifier: -1,
            }
        );
        assert_eq!(
            LogicalType::USmallInt.pg_descriptor(),
            PgTypeDescriptor {
                oid: INT4OID,
                type_size: 4,
                type_modifier: -1,
            }
        );
        assert_eq!(
            LogicalType::UInteger.pg_descriptor(),
            PgTypeDescriptor {
                oid: INT8OID,
                type_size: 8,
                type_modifier: -1,
            }
        );
        assert_eq!(LogicalType::HugeInt.pg_descriptor().oid, NUMERICOID);
        assert_eq!(LogicalType::HugeInt.pg_descriptor().type_size, -1);
        assert_eq!(LogicalType::UBigInt.pg_descriptor().oid, NUMERICOID);
        assert_eq!(LogicalType::UHugeInt.pg_descriptor().oid, NUMERICOID);
    }

    #[test]
    fn decimal_descriptor_encodes_typmod() {
        assert_eq!(
            LogicalType::Decimal {
                precision: 10,
                scale: 2,
            }
            .pg_descriptor()
            .type_modifier,
            ((10_i32 << 16) | 2) + 4
        );
        assert_eq!(
            LogicalType::Decimal {
                precision: 0,
                scale: 0,
            }
            .pg_descriptor()
            .type_modifier,
            -1
        );
    }

    #[test]
    fn reverse_mapping_supports_numeric_json_and_arrays() {
        assert_eq!(
            logical_type_from_pg_oid(NUMERICOID).unwrap(),
            Some(LogicalType::Decimal {
                precision: 0,
                scale: 0,
            })
        );
        assert_eq!(
            logical_type_from_pg_oid(JSONOID).unwrap(),
            Some(LogicalType::Json)
        );
        assert_eq!(
            logical_type_from_pg_oid(JSONBOID).unwrap(),
            Some(LogicalType::Jsonb)
        );
        assert_eq!(
            logical_type_from_pg_oid(NUMERICARRAYOID).unwrap(),
            Some(LogicalType::List(Box::new(LogicalType::Decimal {
                precision: 0,
                scale: 0,
            })))
        );
    }

    #[test]
    fn advertised_oid_roundtrips_through_reverse_mapping() {
        let types = [
            LogicalType::Boolean,
            LogicalType::TinyInt,
            LogicalType::SmallInt,
            LogicalType::Integer,
            LogicalType::BigInt,
            LogicalType::HugeInt,
            LogicalType::UTinyInt,
            LogicalType::USmallInt,
            LogicalType::UInteger,
            LogicalType::UBigInt,
            LogicalType::UHugeInt,
            LogicalType::Float,
            LogicalType::Double,
            LogicalType::Decimal {
                precision: 12,
                scale: 3,
            },
            LogicalType::Varchar,
            LogicalType::Json,
            LogicalType::Jsonb,
            LogicalType::Date,
            LogicalType::Time,
            LogicalType::Timestamp,
            LogicalType::TimestampTz,
            LogicalType::Interval,
            LogicalType::Blob,
            LogicalType::Uuid,
            LogicalType::List(Box::new(LogicalType::Integer)),
            LogicalType::Array(Box::new(LogicalType::Float), 4),
            LogicalType::List(Box::new(LogicalType::UBigInt)),
        ];

        for logical_type in types {
            let oid = logical_type.to_pg_oid();
            let back = logical_type_from_pg_oid(oid).unwrap().unwrap();
            assert_eq!(
                back.to_pg_oid(),
                oid,
                "roundtrip OID mismatch for {logical_type:?}"
            );
        }
    }
}
