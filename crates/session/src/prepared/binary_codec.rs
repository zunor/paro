// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;

const PG_EPOCH_UNIX_DAYS: i32 = 10_957;
const PG_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;

pub fn is_binary_recv_supported(ty: &LogicalType) -> bool {
    if matches!(
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
            | LogicalType::Json
            | LogicalType::Blob
            | LogicalType::Uuid
            | LogicalType::Date
            | LogicalType::Timestamp
            | LogicalType::TimestampTz
    ) {
        return true;
    }

    match ty {
        LogicalType::List(child) | LogicalType::Array(child, _) => {
            is_binary_array_element_recv_supported(child)
        }
        _ => false,
    }
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
    match ty {
        LogicalType::Boolean => {
            require_len(bytes, 1, ty)?;
            Ok(Value::Boolean(bytes[0] != 0))
        }
        LogicalType::TinyInt => {
            require_len(bytes, 2, ty)?;
            let value = i16::from_be_bytes(bytes.try_into().expect("checked length"));
            i8::try_from(value)
                .map(Value::TinyInt)
                .map_err(|_| paro_error::invalid_value("tinyint", value.to_string()))
        }
        LogicalType::UTinyInt => {
            require_len(bytes, 2, ty)?;
            let value = i16::from_be_bytes(bytes.try_into().expect("checked length"));
            u8::try_from(value)
                .map(Value::UTinyInt)
                .map_err(|_| paro_error::invalid_value("utinyint", value.to_string()))
        }
        LogicalType::SmallInt => {
            require_len(bytes, 2, ty)?;
            Ok(Value::SmallInt(i16::from_be_bytes(
                bytes.try_into().expect("checked length"),
            )))
        }
        LogicalType::Integer => {
            require_len(bytes, 4, ty)?;
            Ok(Value::Integer(i32::from_be_bytes(
                bytes.try_into().expect("checked length"),
            )))
        }
        LogicalType::USmallInt => {
            require_len(bytes, 4, ty)?;
            let value = i32::from_be_bytes(bytes.try_into().expect("checked length"));
            u16::try_from(value)
                .map(Value::USmallInt)
                .map_err(|_| paro_error::invalid_value("usmallint", value.to_string()))
        }
        LogicalType::BigInt => {
            require_len(bytes, 8, ty)?;
            Ok(Value::BigInt(i64::from_be_bytes(
                bytes.try_into().expect("checked length"),
            )))
        }
        LogicalType::UInteger => {
            require_len(bytes, 8, ty)?;
            let value = i64::from_be_bytes(bytes.try_into().expect("checked length"));
            u32::try_from(value)
                .map(Value::UInteger)
                .map_err(|_| paro_error::invalid_value("uinteger", value.to_string()))
        }
        LogicalType::Float => {
            require_len(bytes, 4, ty)?;
            Ok(Value::Float(f32::from_be_bytes(
                bytes.try_into().expect("checked length"),
            )))
        }
        LogicalType::Double => {
            require_len(bytes, 8, ty)?;
            Ok(Value::Double(f64::from_be_bytes(
                bytes.try_into().expect("checked length"),
            )))
        }
        LogicalType::Varchar | LogicalType::VarcharCollation(_) | LogicalType::Json => {
            Ok(Value::Varchar(String::from_utf8(bytes.to_vec()).map_err(
                |_| paro_error::invalid_value(ty.to_string(), "<binary utf8>"),
            )?))
        }
        LogicalType::Blob => Ok(Value::Blob(bytes.to_vec())),
        LogicalType::Uuid => {
            require_len(bytes, 16, ty)?;
            Ok(Value::Uuid(u128::from_be_bytes(
                bytes.try_into().expect("checked length"),
            )))
        }
        LogicalType::Date => {
            require_len(bytes, 4, ty)?;
            let pg_days = i32::from_be_bytes(bytes.try_into().expect("checked length"));
            let days = pg_days
                .checked_add(PG_EPOCH_UNIX_DAYS)
                .ok_or_else(|| paro_error::invalid_value("date", pg_days.to_string()))?;
            Ok(Value::Date(days))
        }
        LogicalType::Timestamp => {
            require_len(bytes, 8, ty)?;
            let pg_micros = i64::from_be_bytes(bytes.try_into().expect("checked length"));
            Ok(Value::Timestamp(decode_pg_timestamp(pg_micros)?))
        }
        LogicalType::TimestampTz => {
            require_len(bytes, 8, ty)?;
            let pg_micros = i64::from_be_bytes(bytes.try_into().expect("checked length"));
            Ok(Value::TimestampTz(decode_pg_timestamp(pg_micros)?))
        }
        LogicalType::List(child) => {
            let values = decode_binary_array(bytes, child, None)?;
            Ok(Value::List(values, child.as_ref().clone()))
        }
        LogicalType::Array(child, size) => {
            let values = decode_binary_array(bytes, child, Some(*size))?;
            Ok(Value::Array(values, child.as_ref().clone(), *size))
        }
        _ => Err(paro_error::not_implemented(format!(
            "binary parameter format not supported for type {ty}",
        ))),
    }
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

fn require_len(bytes: &[u8], expected: usize, ty: &LogicalType) -> Result<()> {
    if bytes.len() != expected {
        return Err(paro_error::protocol_violation(format!(
            "binary value for type {ty} expected {expected} bytes, got {}",
            bytes.len(),
        )));
    }
    Ok(())
}

fn is_binary_array_element_recv_supported(ty: &LogicalType) -> bool {
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
            | LogicalType::Json
            | LogicalType::Blob
            | LogicalType::Uuid
            | LogicalType::Date
            | LogicalType::Timestamp
            | LogicalType::TimestampTz
    )
}

fn decode_binary_array(
    bytes: &[u8],
    child_type: &LogicalType,
    fixed_size: Option<usize>,
) -> Result<Vec<Value>> {
    let mut input = BinaryInput::new(bytes);
    let dimensions = input.read_i32("array dimension count")?;
    let _has_null = input.read_i32("array null flag")?;
    let element_oid = input.read_u32("array element OID")?;
    let expected_oid = child_type.pg_descriptor().oid;
    if element_oid != expected_oid {
        return Err(paro_error::protocol_violation(format!(
            "binary array element OID {element_oid} does not match {child_type} OID {expected_oid}",
        )));
    }

    let length = match dimensions {
        0 => 0,
        1 => {
            let length = input.read_i32("array length")?;
            let _lower_bound = input.read_i32("array lower bound")?;
            usize::try_from(length).map_err(|_| {
                paro_error::protocol_violation(format!("invalid binary array length {length}"))
            })?
        }
        other => {
            return Err(paro_error::not_implemented(format!(
                "binary arrays with {other} dimensions are not supported",
            )))
        }
    };
    if let Some(expected) = fixed_size {
        if length != expected {
            return Err(paro_error::invalid_value(
                format!("ARRAY({expected})"),
                format!("binary array with {length} elements"),
            ));
        }
    }

    let mut values = Vec::with_capacity(length);
    for _ in 0..length {
        let element_len = input.read_i32("array element length")?;
        if element_len == -1 {
            values.push(Value::Null(child_type.clone()));
            continue;
        }
        let element_len = usize::try_from(element_len).map_err(|_| {
            paro_error::protocol_violation(format!(
                "invalid binary array element length {element_len}",
            ))
        })?;
        values.push(decode_binary_param(
            input.read_bytes(element_len, "array element")?,
            child_type,
        )?);
    }
    if !input.is_empty() {
        return Err(paro_error::protocol_violation(format!(
            "binary array has {} trailing bytes",
            input.remaining(),
        )));
    }
    Ok(values)
}

struct BinaryInput<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> BinaryInput<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_i32(&mut self, field: &str) -> Result<i32> {
        Ok(i32::from_be_bytes(
            self.read_bytes(4, field)?
                .try_into()
                .expect("checked length"),
        ))
    }

    fn read_u32(&mut self, field: &str) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.read_bytes(4, field)?
                .try_into()
                .expect("checked length"),
        ))
    }

    fn read_bytes(&mut self, length: usize, field: &str) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(length).ok_or_else(|| {
            paro_error::protocol_violation(format!("binary {field} length overflow"))
        })?;
        let value = self.bytes.get(self.offset..end).ok_or_else(|| {
            paro_error::protocol_violation(format!(
                "binary {field} is truncated at byte {}",
                self.offset,
            ))
        })?;
        self.offset = end;
        Ok(value)
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }

    fn is_empty(&self) -> bool {
        self.remaining() == 0
    }
}

fn decode_pg_timestamp(pg_micros: i64) -> Result<i64> {
    if matches!(pg_micros, i64::MAX | i64::MIN) {
        return Ok(pg_micros);
    }
    pg_micros
        .checked_add(PG_EPOCH_UNIX_MICROS)
        .ok_or_else(|| paro_error::invalid_value("timestamp", pg_micros.to_string()))
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
        assert_eq!(decode_pg_timestamp(i64::MAX).unwrap(), i64::MAX);
        assert_eq!(decode_pg_timestamp(i64::MIN).unwrap(), i64::MIN);
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
