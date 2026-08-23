// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;

const PG_EPOCH_UNIX_DAYS: i32 = 10_957;
const PG_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;

pub fn is_binary_recv_supported(ty: &LogicalType) -> bool {
    paro_function::pg_binary::is_binary_recv_supported(ty)
}

pub fn is_binary_send_supported(ty: &LogicalType) -> bool {
    matches!(
        ty,
        LogicalType::Boolean
            | LogicalType::TinyInt
            | LogicalType::UTinyInt
            | LogicalType::SmallInt
            | LogicalType::Integer
            | LogicalType::USmallInt
            | LogicalType::BigInt
            | LogicalType::UInteger
            | LogicalType::Float
            | LogicalType::Double
            | LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::Json
            | LogicalType::Blob
            | LogicalType::Uuid
            | LogicalType::Date
            | LogicalType::Timestamp
            | LogicalType::TimestampTz
    )
}

pub fn decode_binary_param(bytes: &[u8], ty: &LogicalType) -> Result<Value> {
    paro_function::pg_binary::decode_binary_value(bytes, ty)
}

pub fn encode_binary_value(value: &Value, ty: &LogicalType) -> Result<Vec<u8>> {
    match ty {
        LogicalType::Boolean => match value {
            Value::Boolean(v) => Ok(vec![u8::from(*v)]),
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::TinyInt => match value {
            Value::TinyInt(v) => Ok(i16::from(*v).to_be_bytes().to_vec()),
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::UTinyInt => match value {
            Value::UTinyInt(v) => Ok(i16::from(*v).to_be_bytes().to_vec()),
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::SmallInt => match value {
            Value::SmallInt(v) => Ok(v.to_be_bytes().to_vec()),
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::Integer => match value {
            Value::Integer(v) => Ok(v.to_be_bytes().to_vec()),
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::USmallInt => match value {
            Value::USmallInt(v) => Ok(i32::from(*v).to_be_bytes().to_vec()),
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::BigInt => match value {
            Value::BigInt(v) => Ok(v.to_be_bytes().to_vec()),
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::UInteger => match value {
            Value::UInteger(v) => Ok(i64::from(*v).to_be_bytes().to_vec()),
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::Float => match value {
            Value::Float(v) => Ok(v.to_be_bytes().to_vec()),
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::Double => match value {
            Value::Double(v) => Ok(v.to_be_bytes().to_vec()),
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::Varchar | LogicalType::VarcharCollation(_) | LogicalType::Json => {
            match value {
                Value::Varchar(v) => Ok(v.as_bytes().to_vec()),
                _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
            }
        }
        LogicalType::Blob => match value {
            Value::Blob(v) => Ok(v.clone()),
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::Uuid => match value {
            Value::Uuid(v) => Ok(v.to_be_bytes().to_vec()),
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::Date => match value {
            Value::Date(days) => {
                let pg_days = days
                    .checked_sub(PG_EPOCH_UNIX_DAYS)
                    .ok_or_else(|| paro_error::invalid_value("date", days.to_string()))?;
                Ok(pg_days.to_be_bytes().to_vec())
            }
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::Timestamp => match value {
            Value::Timestamp(micros) => Ok(encode_pg_timestamp(*micros)?.to_be_bytes().to_vec()),
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::TimestampTz => match value {
            Value::TimestampTz(micros) => Ok(encode_pg_timestamp(*micros)?.to_be_bytes().to_vec()),
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        _ => Err(paro_error::not_implemented(format!(
            "binary result format not supported for type {ty}",
        ))),
    }
}

fn encode_pg_timestamp(micros: i64) -> Result<i64> {
    if matches!(micros, i64::MAX | i64::MIN) {
        return Ok(micros);
    }
    micros
        .checked_sub(PG_EPOCH_UNIX_MICROS)
        .ok_or_else(|| paro_error::invalid_value("timestamp", micros.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_timestamp_roundtrip_preserves_infinity() {
        assert_eq!(
            decode_binary_param(&i64::MAX.to_be_bytes(), &LogicalType::Timestamp).unwrap(),
            Value::Timestamp(i64::MAX)
        );
        assert_eq!(
            decode_binary_param(&i64::MIN.to_be_bytes(), &LogicalType::Timestamp).unwrap(),
            Value::Timestamp(i64::MIN)
        );
        assert_eq!(encode_pg_timestamp(i64::MAX).unwrap(), i64::MAX);
        assert_eq!(encode_pg_timestamp(i64::MIN).unwrap(), i64::MIN);
    }

    #[test]
    fn binary_unsigned_values_follow_advertised_widths() {
        let tiny = decode_binary_param(&1_i16.to_be_bytes(), &LogicalType::UTinyInt).unwrap();
        assert_eq!(tiny, Value::UTinyInt(1));

        let integer = decode_binary_param(&42_i64.to_be_bytes(), &LogicalType::UInteger).unwrap();
        assert_eq!(integer, Value::UInteger(42));

        assert_eq!(
            encode_binary_value(&Value::UTinyInt(7), &LogicalType::UTinyInt).unwrap(),
            i16::from(7_u8).to_be_bytes().to_vec()
        );
        assert_eq!(
            encode_binary_value(&Value::UInteger(9), &LogicalType::UInteger).unwrap(),
            i64::from(9_u32).to_be_bytes().to_vec()
        );
    }

    #[test]
    fn binary_float_array_decodes_postgres_wire_format() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1_i32.to_be_bytes());
        bytes.extend_from_slice(&0_i32.to_be_bytes());
        bytes.extend_from_slice(&LogicalType::Float.pg_descriptor().oid.to_be_bytes());
        bytes.extend_from_slice(&3_i32.to_be_bytes());
        bytes.extend_from_slice(&1_i32.to_be_bytes());
        for value in [1.25_f32, -2.5, 3.75] {
            bytes.extend_from_slice(&4_i32.to_be_bytes());
            bytes.extend_from_slice(&value.to_be_bytes());
        }

        assert_eq!(
            decode_binary_param(&bytes, &LogicalType::List(Box::new(LogicalType::Float))).unwrap(),
            Value::List(
                vec![Value::Float(1.25), Value::Float(-2.5), Value::Float(3.75)],
                LogicalType::Float,
            )
        );
    }

    #[test]
    fn binary_fixed_array_rejects_dimension_mismatch() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1_i32.to_be_bytes());
        bytes.extend_from_slice(&0_i32.to_be_bytes());
        bytes.extend_from_slice(&LogicalType::Float.pg_descriptor().oid.to_be_bytes());
        bytes.extend_from_slice(&2_i32.to_be_bytes());
        bytes.extend_from_slice(&1_i32.to_be_bytes());
        for value in [1.0_f32, 2.0] {
            bytes.extend_from_slice(&4_i32.to_be_bytes());
            bytes.extend_from_slice(&value.to_be_bytes());
        }

        assert!(
            decode_binary_param(&bytes, &LogicalType::Array(Box::new(LogicalType::Float), 3),)
                .is_err()
        );
    }
}
