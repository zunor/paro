// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Comparable primary-key encoding utilities.
//!
//! The encoded byte representation preserves the logical sort order under
//! lexicographic byte comparison.

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;

const FLOAT_SIGN_MASK: u32 = 0x8000_0000;
const DOUBLE_SIGN_MASK: u64 = 0x8000_0000_0000_0000;
const GROUP_SIZE: usize = 8;

/// Memcmp-comparable encoder/decoder for primary-key values.
#[derive(Debug, Default, Clone, Copy)]
pub struct ComparableEncoder;

impl ComparableEncoder {
    /// Encode a single value into a memcmp-comparable byte sequence.
    pub fn encode_value(
        value: &Value,
        logical_type: &LogicalType,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        if value.is_null() {
            return Err(paro_error::invalid_input(
                "Primary key column cannot be NULL",
            ));
        }

        match (value, logical_type) {
            (Value::Boolean(v), LogicalType::Boolean) => out.push(if *v { 1 } else { 0 }),
            (Value::TinyInt(v), LogicalType::TinyInt) => out.push((*v as u8) ^ 0x80),
            (Value::SmallInt(v), LogicalType::SmallInt) => {
                out.extend_from_slice(&((*v as u16) ^ 0x8000).to_be_bytes());
            }
            (Value::Integer(v), LogicalType::Integer) => {
                out.extend_from_slice(&((*v as u32) ^ 0x8000_0000).to_be_bytes());
            }
            (Value::BigInt(v), LogicalType::BigInt) => {
                out.extend_from_slice(&((*v as u64) ^ DOUBLE_SIGN_MASK).to_be_bytes());
            }
            (Value::HugeInt(v), LogicalType::HugeInt) => {
                out.extend_from_slice(&((*v as u128) ^ (1u128 << 127)).to_be_bytes());
            }
            (Value::UTinyInt(v), LogicalType::UTinyInt) => out.push(*v),
            (Value::USmallInt(v), LogicalType::USmallInt) => {
                out.extend_from_slice(&v.to_be_bytes())
            }
            (Value::UInteger(v), LogicalType::UInteger) => out.extend_from_slice(&v.to_be_bytes()),
            (Value::UBigInt(v), LogicalType::UBigInt) => out.extend_from_slice(&v.to_be_bytes()),
            (Value::UHugeInt(v), LogicalType::UHugeInt) => out.extend_from_slice(&v.to_be_bytes()),
            (Value::Uuid(v), LogicalType::Uuid) => out.extend_from_slice(&v.to_be_bytes()),
            (Value::Float(v), LogicalType::Float) => {
                out.extend_from_slice(&Self::encode_f32(*v).to_be_bytes());
            }
            (Value::Double(v), LogicalType::Double) => {
                out.extend_from_slice(&Self::encode_f64(*v).to_be_bytes());
            }
            (Value::Date(v), LogicalType::Date) => {
                out.extend_from_slice(&((*v as u32) ^ 0x8000_0000).to_be_bytes());
            }
            (Value::Timestamp(v), LogicalType::Timestamp)
            | (Value::TimestampTz(v), LogicalType::TimestampTz)
            | (Value::Time(v), LogicalType::Time) => {
                out.extend_from_slice(&((*v as u64) ^ DOUBLE_SIGN_MASK).to_be_bytes());
            }
            (
                Value::Decimal(v, precision, scale),
                LogicalType::Decimal {
                    precision: p,
                    scale: s,
                },
            ) => {
                if precision != p || scale != s {
                    return Err(paro_error::invalid_input(format!(
                        "Decimal precision/scale mismatch: value=({}, {}), type=({}, {})",
                        precision, scale, p, s
                    )));
                }
                Self::encode_decimal(*v, *p, out);
            }
            (Value::BigInt(v), LogicalType::Decimal { precision, .. }) => {
                Self::encode_decimal(*v as i128, *precision, out);
            }
            (Value::HugeInt(v), LogicalType::Decimal { precision, .. }) => {
                Self::encode_decimal(*v, *precision, out);
            }
            (
                Value::Varchar(s),
                LogicalType::Varchar
                | LogicalType::VarcharCollation(_)
                | LogicalType::TsVector
                | LogicalType::TsQuery
                | LogicalType::Json
                | LogicalType::Jsonb,
            ) => Self::encode_bytes(s.as_bytes(), out),
            (Value::Blob(b), LogicalType::Blob) => Self::encode_bytes(b, out),
            _ => {
                return Err(paro_error::invalid_input(format!(
                    "Unsupported primary key type/value combination: type={}, value={:?}",
                    logical_type, value
                )))
            }
        }

        Ok(())
    }

    /// Decode a single value from comparable bytes.
    pub fn decode_value(input: &mut &[u8], logical_type: &LogicalType) -> Result<Value> {
        match logical_type {
            LogicalType::Boolean => Ok(Value::Boolean(Self::read_u8(input)? != 0)),
            LogicalType::TinyInt => Ok(Value::TinyInt((Self::read_u8(input)? ^ 0x80) as i8)),
            LogicalType::SmallInt => {
                let raw = Self::read_u16(input)? ^ 0x8000;
                Ok(Value::SmallInt(raw as i16))
            }
            LogicalType::Integer => {
                let raw = Self::read_u32(input)? ^ 0x8000_0000;
                Ok(Value::Integer(raw as i32))
            }
            LogicalType::BigInt => {
                let raw = Self::read_u64(input)? ^ DOUBLE_SIGN_MASK;
                Ok(Value::BigInt(raw as i64))
            }
            LogicalType::HugeInt => {
                let raw = Self::read_u128(input)? ^ (1u128 << 127);
                Ok(Value::HugeInt(raw as i128))
            }
            LogicalType::UTinyInt => Ok(Value::UTinyInt(Self::read_u8(input)?)),
            LogicalType::USmallInt => Ok(Value::USmallInt(Self::read_u16(input)?)),
            LogicalType::UInteger => Ok(Value::UInteger(Self::read_u32(input)?)),
            LogicalType::UBigInt => Ok(Value::UBigInt(Self::read_u64(input)?)),
            LogicalType::UHugeInt => Ok(Value::UHugeInt(Self::read_u128(input)?)),
            LogicalType::Uuid => Ok(Value::Uuid(Self::read_u128(input)?)),
            LogicalType::Float => Ok(Value::Float(Self::decode_f32(Self::read_u32(input)?))),
            LogicalType::Double => Ok(Value::Double(Self::decode_f64(Self::read_u64(input)?))),
            LogicalType::Date => {
                let raw = Self::read_u32(input)? ^ 0x8000_0000;
                Ok(Value::Date(raw as i32))
            }
            LogicalType::Timestamp => {
                let raw = Self::read_u64(input)? ^ DOUBLE_SIGN_MASK;
                Ok(Value::Timestamp(raw as i64))
            }
            LogicalType::TimestampTz => {
                let raw = Self::read_u64(input)? ^ DOUBLE_SIGN_MASK;
                Ok(Value::TimestampTz(raw as i64))
            }
            LogicalType::Time => {
                let raw = Self::read_u64(input)? ^ DOUBLE_SIGN_MASK;
                Ok(Value::Time(raw as i64))
            }
            LogicalType::Decimal { precision, scale } => {
                let raw = if *precision <= 18 {
                    (Self::read_u64(input)? ^ DOUBLE_SIGN_MASK) as i64 as i128
                } else {
                    (Self::read_u128(input)? ^ (1u128 << 127)) as i128
                };
                Ok(Value::Decimal(raw, *precision, *scale))
            }
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb => {
                let bytes = Self::decode_bytes(input)?;
                let text = String::from_utf8(bytes).map_err(|e| {
                    paro_error::serialization_error(format!(
                        "decode comparable varchar as utf8: {}",
                        e
                    ))
                })?;
                Ok(Value::Varchar(text))
            }
            LogicalType::Blob => Ok(Value::Blob(Self::decode_bytes(input)?)),
            _ => Err(paro_error::invalid_input(format!(
                "Unsupported primary key type for comparable decode: {}",
                logical_type
            ))),
        }
    }

    fn encode_decimal(value: i128, precision: u8, out: &mut Vec<u8>) {
        if precision <= 18 {
            out.extend_from_slice(&((value as i64 as u64) ^ DOUBLE_SIGN_MASK).to_be_bytes());
        } else {
            out.extend_from_slice(&((value as u128) ^ (1u128 << 127)).to_be_bytes());
        }
    }

    fn encode_f32(value: f32) -> u32 {
        let bits = if value.is_nan() {
            f32::NAN.to_bits()
        } else if value == 0.0 {
            0.0f32.to_bits()
        } else {
            value.to_bits()
        };
        if bits & FLOAT_SIGN_MASK != 0 {
            !bits
        } else {
            bits ^ FLOAT_SIGN_MASK
        }
    }

    fn decode_f32(encoded: u32) -> f32 {
        let bits = if encoded & FLOAT_SIGN_MASK != 0 {
            encoded ^ FLOAT_SIGN_MASK
        } else {
            !encoded
        };
        f32::from_bits(bits)
    }

    fn encode_f64(value: f64) -> u64 {
        let bits = if value.is_nan() {
            f64::NAN.to_bits()
        } else if value == 0.0 {
            0.0f64.to_bits()
        } else {
            value.to_bits()
        };
        if bits & DOUBLE_SIGN_MASK != 0 {
            !bits
        } else {
            bits ^ DOUBLE_SIGN_MASK
        }
    }

    fn decode_f64(encoded: u64) -> f64 {
        let bits = if encoded & DOUBLE_SIGN_MASK != 0 {
            encoded ^ DOUBLE_SIGN_MASK
        } else {
            !encoded
        };
        f64::from_bits(bits)
    }

    fn encode_bytes(bytes: &[u8], out: &mut Vec<u8>) {
        if bytes.is_empty() {
            out.extend_from_slice(&[0u8; GROUP_SIZE]);
            out.push(0);
            return;
        }

        for chunk in bytes.chunks(GROUP_SIZE) {
            out.extend_from_slice(chunk);
            if chunk.len() < GROUP_SIZE {
                out.resize(out.len() + (GROUP_SIZE - chunk.len()), 0);
                out.push(chunk.len() as u8);
                return;
            }
            out.push(GROUP_SIZE as u8);
        }

        out.extend_from_slice(&[0u8; GROUP_SIZE]);
        out.push(0);
    }

    fn decode_bytes(input: &mut &[u8]) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        loop {
            if input.len() < GROUP_SIZE + 1 {
                return Err(paro_error::internal("comparable byte payload truncated"));
            }
            let (chunk, rest) = input.split_at(GROUP_SIZE);
            let marker = rest[0] as usize;
            if marker > GROUP_SIZE {
                return Err(paro_error::internal(format!(
                    "invalid comparable byte marker {}",
                    marker
                )));
            }
            out.extend_from_slice(&chunk[..marker]);
            *input = &rest[1..];
            if marker < GROUP_SIZE {
                return Ok(out);
            }
        }
    }

    fn read_u8(input: &mut &[u8]) -> Result<u8> {
        if input.is_empty() {
            return Err(paro_error::internal("comparable payload truncated"));
        }
        let value = input[0];
        *input = &input[1..];
        Ok(value)
    }

    fn read_u16(input: &mut &[u8]) -> Result<u16> {
        if input.len() < 2 {
            return Err(paro_error::internal("comparable payload truncated"));
        }
        let (bytes, rest) = input.split_at(2);
        *input = rest;
        Ok(u16::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn read_u32(input: &mut &[u8]) -> Result<u32> {
        if input.len() < 4 {
            return Err(paro_error::internal("comparable payload truncated"));
        }
        let (bytes, rest) = input.split_at(4);
        *input = rest;
        Ok(u32::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn read_u64(input: &mut &[u8]) -> Result<u64> {
        if input.len() < 8 {
            return Err(paro_error::internal("comparable payload truncated"));
        }
        let (bytes, rest) = input.split_at(8);
        *input = rest;
        Ok(u64::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn read_u128(input: &mut &[u8]) -> Result<u128> {
        if input.len() < 16 {
            return Err(paro_error::internal("comparable payload truncated"));
        }
        let (bytes, rest) = input.split_at(16);
        *input = rest;
        Ok(u128::from_be_bytes(bytes.try_into().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::ComparableEncoder;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;

    fn encode(value: Value, ty: LogicalType) -> Vec<u8> {
        let mut out = Vec::new();
        ComparableEncoder::encode_value(&value, &ty, &mut out).unwrap();
        out
    }

    fn assert_roundtrip(value: Value, ty: LogicalType) {
        let mut encoded = Vec::new();
        ComparableEncoder::encode_value(&value, &ty, &mut encoded).unwrap();
        let mut input = encoded.as_slice();
        let decoded = ComparableEncoder::decode_value(&mut input, &ty).unwrap();
        assert_eq!(input.len(), 0, "type {ty} left trailing bytes");
        assert_eq!(decoded, value, "type {ty} failed roundtrip");
    }

    fn assert_order_preserved(ty: LogicalType, values: &[Value]) {
        let sorted = values
            .iter()
            .cloned()
            .map(|value| encode(value, ty.clone()))
            .collect::<Vec<_>>();
        let mut encoded = sorted.clone();
        encoded.sort();
        assert_eq!(encoded, sorted, "type {ty} did not preserve logical order");
    }

    #[test]
    fn signed_integer_encoding_preserves_order() {
        let values = [
            Value::Integer(i32::MIN),
            Value::Integer(-10),
            Value::Integer(-1),
            Value::Integer(0),
            Value::Integer(1),
            Value::Integer(42),
            Value::Integer(i32::MAX),
        ];
        let mut encoded: Vec<_> = values
            .iter()
            .cloned()
            .map(|v| encode(v, LogicalType::Integer))
            .collect();
        let sorted = encoded.clone();
        encoded.sort();
        assert_eq!(encoded, sorted);
    }

    #[test]
    fn float_encoding_canonicalizes_negative_zero() {
        let neg_zero = encode(Value::Float(-0.0), LogicalType::Float);
        let pos_zero = encode(Value::Float(0.0), LogicalType::Float);
        assert_eq!(neg_zero, pos_zero);
    }

    #[test]
    fn varlen_encoding_preserves_lexicographic_order() {
        let values = [
            Value::Varchar("".to_string()),
            Value::Varchar("a".to_string()),
            Value::Varchar("aa".to_string()),
            Value::Varchar("b".to_string()),
            Value::Varchar("b\0".to_string()),
        ];
        let mut encoded: Vec<_> = values
            .iter()
            .cloned()
            .map(|v| encode(v, LogicalType::Varchar))
            .collect();
        let sorted = encoded.clone();
        encoded.sort();
        assert_eq!(encoded, sorted);
    }

    #[test]
    fn roundtrip_supported_values() {
        let cases = vec![
            (Value::Boolean(false), LogicalType::Boolean),
            (Value::Boolean(true), LogicalType::Boolean),
            (Value::TinyInt(-12), LogicalType::TinyInt),
            (Value::SmallInt(-1234), LogicalType::SmallInt),
            (Value::Integer(-567_890), LogicalType::Integer),
            (Value::BigInt(-42), LogicalType::BigInt),
            (Value::HugeInt(-(1i128 << 100)), LogicalType::HugeInt),
            (Value::UTinyInt(240), LogicalType::UTinyInt),
            (Value::USmallInt(60_000), LogicalType::USmallInt),
            (Value::UInteger(99), LogicalType::UInteger),
            (Value::UBigInt(1u64 << 50), LogicalType::UBigInt),
            (Value::UHugeInt(1u128 << 100), LogicalType::UHugeInt),
            (
                Value::Uuid(0x1234_5678_9abc_def0_1357_9bdf_2468_ace0),
                LogicalType::Uuid,
            ),
            (Value::Float(-12.5), LogicalType::Float),
            (Value::Double(12.5), LogicalType::Double),
            (Value::Date(-3), LogicalType::Date),
            (Value::Timestamp(-123456), LogicalType::Timestamp),
            (Value::TimestampTz(123456), LogicalType::TimestampTz),
            (Value::Time(-654321), LogicalType::Time),
            (
                Value::Decimal(-1234, 18, 2),
                LogicalType::Decimal {
                    precision: 18,
                    scale: 2,
                },
            ),
            (
                Value::Decimal(-(1i128 << 100), 38, 4),
                LogicalType::Decimal {
                    precision: 38,
                    scale: 4,
                },
            ),
            (Value::Varchar("paro".to_string()), LogicalType::Varchar),
            (
                Value::Varchar("nocase".to_string()),
                LogicalType::VarcharCollation("NOCASE".to_string()),
            ),
            (
                Value::Varchar("'a' & 'b'".to_string()),
                LogicalType::TsQuery,
            ),
            (
                Value::Varchar("'paro':1".to_string()),
                LogicalType::TsVector,
            ),
            (Value::Varchar("{\"k\":1}".to_string()), LogicalType::Json),
            (Value::Varchar("{\"k\":2}".to_string()), LogicalType::Jsonb),
            (Value::Blob(vec![0, 1, 2, 255]), LogicalType::Blob),
        ];

        for (value, ty) in cases {
            assert_roundtrip(value, ty);
        }
    }

    #[test]
    fn ordering_is_preserved_across_supported_types() {
        let cases = vec![
            (
                LogicalType::Boolean,
                vec![Value::Boolean(false), Value::Boolean(true)],
            ),
            (
                LogicalType::TinyInt,
                vec![
                    Value::TinyInt(i8::MIN),
                    Value::TinyInt(-1),
                    Value::TinyInt(0),
                    Value::TinyInt(i8::MAX),
                ],
            ),
            (
                LogicalType::SmallInt,
                vec![
                    Value::SmallInt(i16::MIN),
                    Value::SmallInt(-7),
                    Value::SmallInt(0),
                    Value::SmallInt(42),
                    Value::SmallInt(i16::MAX),
                ],
            ),
            (
                LogicalType::Integer,
                vec![
                    Value::Integer(i32::MIN),
                    Value::Integer(-10),
                    Value::Integer(-1),
                    Value::Integer(0),
                    Value::Integer(1),
                    Value::Integer(i32::MAX),
                ],
            ),
            (
                LogicalType::BigInt,
                vec![
                    Value::BigInt(i64::MIN),
                    Value::BigInt(-10),
                    Value::BigInt(-1),
                    Value::BigInt(0),
                    Value::BigInt(1),
                    Value::BigInt(i64::MAX),
                ],
            ),
            (
                LogicalType::HugeInt,
                vec![
                    Value::HugeInt(i128::MIN),
                    Value::HugeInt(-(1i128 << 96)),
                    Value::HugeInt(-1),
                    Value::HugeInt(0),
                    Value::HugeInt(1),
                    Value::HugeInt(i128::MAX),
                ],
            ),
            (
                LogicalType::UTinyInt,
                vec![
                    Value::UTinyInt(0),
                    Value::UTinyInt(7),
                    Value::UTinyInt(u8::MAX),
                ],
            ),
            (
                LogicalType::USmallInt,
                vec![
                    Value::USmallInt(0),
                    Value::USmallInt(7),
                    Value::USmallInt(u16::MAX),
                ],
            ),
            (
                LogicalType::UInteger,
                vec![
                    Value::UInteger(0),
                    Value::UInteger(7),
                    Value::UInteger(u32::MAX),
                ],
            ),
            (
                LogicalType::UBigInt,
                vec![
                    Value::UBigInt(0),
                    Value::UBigInt(7),
                    Value::UBigInt(u64::MAX),
                ],
            ),
            (
                LogicalType::UHugeInt,
                vec![
                    Value::UHugeInt(0),
                    Value::UHugeInt(7),
                    Value::UHugeInt(u128::MAX),
                ],
            ),
            (
                LogicalType::Uuid,
                vec![
                    Value::Uuid(0),
                    Value::Uuid(1),
                    Value::Uuid(0xffff),
                    Value::Uuid(u128::MAX),
                ],
            ),
            (
                LogicalType::Float,
                vec![
                    Value::Float(f32::NEG_INFINITY),
                    Value::Float(-10.5),
                    Value::Float(-0.0),
                    Value::Float(0.0),
                    Value::Float(10.5),
                    Value::Float(f32::INFINITY),
                ],
            ),
            (
                LogicalType::Double,
                vec![
                    Value::Double(f64::NEG_INFINITY),
                    Value::Double(-10.5),
                    Value::Double(-0.0),
                    Value::Double(0.0),
                    Value::Double(10.5),
                    Value::Double(f64::INFINITY),
                ],
            ),
            (
                LogicalType::Date,
                vec![
                    Value::Date(i32::MIN),
                    Value::Date(-7),
                    Value::Date(0),
                    Value::Date(7),
                    Value::Date(i32::MAX),
                ],
            ),
            (
                LogicalType::Timestamp,
                vec![
                    Value::Timestamp(i64::MIN),
                    Value::Timestamp(-7),
                    Value::Timestamp(0),
                    Value::Timestamp(7),
                    Value::Timestamp(i64::MAX),
                ],
            ),
            (
                LogicalType::TimestampTz,
                vec![
                    Value::TimestampTz(i64::MIN),
                    Value::TimestampTz(-7),
                    Value::TimestampTz(0),
                    Value::TimestampTz(7),
                    Value::TimestampTz(i64::MAX),
                ],
            ),
            (
                LogicalType::Time,
                vec![
                    Value::Time(i64::MIN),
                    Value::Time(-7),
                    Value::Time(0),
                    Value::Time(7),
                    Value::Time(i64::MAX),
                ],
            ),
            (
                LogicalType::Decimal {
                    precision: 18,
                    scale: 2,
                },
                vec![
                    Value::Decimal(i64::MIN as i128, 18, 2),
                    Value::Decimal(-12345, 18, 2),
                    Value::Decimal(0, 18, 2),
                    Value::Decimal(12345, 18, 2),
                    Value::Decimal(i64::MAX as i128, 18, 2),
                ],
            ),
            (
                LogicalType::Decimal {
                    precision: 38,
                    scale: 4,
                },
                vec![
                    Value::Decimal(i128::MIN, 38, 4),
                    Value::Decimal(-(1i128 << 100), 38, 4),
                    Value::Decimal(0, 38, 4),
                    Value::Decimal(1i128 << 100, 38, 4),
                    Value::Decimal(i128::MAX, 38, 4),
                ],
            ),
            (
                LogicalType::Varchar,
                vec![
                    Value::Varchar("".to_string()),
                    Value::Varchar("a".to_string()),
                    Value::Varchar("aa".to_string()),
                    Value::Varchar("b".to_string()),
                    Value::Varchar("b\0".to_string()),
                ],
            ),
            (
                LogicalType::Blob,
                vec![
                    Value::Blob(vec![]),
                    Value::Blob(vec![0]),
                    Value::Blob(vec![0, 1]),
                    Value::Blob(vec![0, 1, 0]),
                    Value::Blob(vec![1]),
                ],
            ),
        ];

        for (ty, values) in cases {
            assert_order_preserved(ty, &values);
        }
    }
}
