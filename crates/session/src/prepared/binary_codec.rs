// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use tokio_util::bytes::BufMut;

const PG_EPOCH_UNIX_DAYS: i32 = 10_957;
const PG_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;

pub fn is_binary_recv_supported(ty: &LogicalType) -> bool {
    paro_common::pg_binary::is_binary_recv_supported(ty)
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
            | LogicalType::HugeInt
            | LogicalType::UInteger
            | LogicalType::UBigInt
            | LogicalType::UHugeInt
            | LogicalType::Float
            | LogicalType::Double
            | LogicalType::Decimal { .. }
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
    paro_common::pg_binary::decode_binary_value(bytes, ty)
}

pub fn encode_binary_value(value: &Value, ty: &LogicalType) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();
    append_binary_value(&mut encoded, value, ty)?;
    Ok(encoded)
}

/// Append one non-null PostgreSQL binary field directly to an existing row
/// buffer. Keeping framing ownership with the caller avoids a heap allocation
/// for every scalar result cell.
pub fn append_binary_value(
    encoded: &mut impl BufMut,
    value: &Value,
    ty: &LogicalType,
) -> Result<()> {
    match ty {
        LogicalType::Boolean => match value {
            Value::Boolean(v) => {
                encoded.put_u8(u8::from(*v));
                Ok(())
            }
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::TinyInt => match value {
            Value::TinyInt(v) => {
                encoded.put_i16(i16::from(*v));
                Ok(())
            }
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::UTinyInt => match value {
            Value::UTinyInt(v) => {
                encoded.put_i16(i16::from(*v));
                Ok(())
            }
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::SmallInt => match value {
            Value::SmallInt(v) => {
                encoded.put_i16(*v);
                Ok(())
            }
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::Integer => match value {
            Value::Integer(v) => {
                encoded.put_i32(*v);
                Ok(())
            }
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::USmallInt => match value {
            Value::USmallInt(v) => {
                encoded.put_i32(i32::from(*v));
                Ok(())
            }
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::BigInt => match value {
            Value::BigInt(v) => {
                encoded.put_i64(*v);
                Ok(())
            }
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::HugeInt => match value {
            Value::HugeInt(v) => append_pg_numeric(encoded, v.unsigned_abs(), *v < 0, 0),
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::UInteger => match value {
            Value::UInteger(v) => {
                encoded.put_i64(i64::from(*v));
                Ok(())
            }
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::UBigInt => match value {
            Value::UBigInt(v) => append_pg_numeric(encoded, u128::from(*v), false, 0),
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::UHugeInt => match value {
            Value::UHugeInt(v) => append_pg_numeric(encoded, *v, false, 0),
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::Float => match value {
            Value::Float(v) => {
                encoded.put_f32(*v);
                Ok(())
            }
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::Double => match value {
            Value::Double(v) => {
                encoded.put_f64(*v);
                Ok(())
            }
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::Decimal { scale, .. } => match value {
            Value::Decimal(v, _, value_scale) if value_scale == scale => {
                append_pg_numeric(encoded, v.unsigned_abs(), *v < 0, *scale)
            }
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::Varchar | LogicalType::VarcharCollation(_) | LogicalType::Json => {
            match value {
                Value::Varchar(v) => {
                    encoded.put_slice(v.as_bytes());
                    Ok(())
                }
                _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
            }
        }
        LogicalType::Blob => match value {
            Value::Blob(v) => {
                encoded.put_slice(v);
                Ok(())
            }
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::Uuid => match value {
            Value::Uuid(v) => {
                encoded.put_u128(*v);
                Ok(())
            }
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::Date => match value {
            Value::Date(days) => {
                let pg_days = days
                    .checked_sub(PG_EPOCH_UNIX_DAYS)
                    .ok_or_else(|| paro_error::invalid_value("date", days.to_string()))?;
                encoded.put_i32(pg_days);
                Ok(())
            }
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::Timestamp => match value {
            Value::Timestamp(micros) => {
                encoded.put_i64(encode_pg_timestamp(*micros)?);
                Ok(())
            }
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        LogicalType::TimestampTz => match value {
            Value::TimestampTz(micros) => {
                encoded.put_i64(encode_pg_timestamp(*micros)?);
                Ok(())
            }
            _ => Err(paro_error::invalid_value(ty.to_string(), value.to_string())),
        },
        _ => Err(paro_error::not_implemented(format!(
            "binary result format not supported for type {ty}",
        ))),
    }
}

/// Encode an exact Paro integral or fixed-scale decimal as PostgreSQL's
/// base-10,000 `numeric` wire representation.
fn append_pg_numeric(
    encoded: &mut impl BufMut,
    magnitude: u128,
    negative: bool,
    scale: u8,
) -> Result<()> {
    const PG_NUMERIC_POS: u16 = 0x0000;
    const PG_NUMERIC_NEG: u16 = 0x4000;

    let display_scale = u16::from(scale);
    if magnitude == 0 {
        encoded.put_i16(0);
        encoded.put_i16(0);
        encoded.put_u16(PG_NUMERIC_POS);
        encoded.put_u16(display_scale);
        return Ok(());
    }

    // u128 has at most 39 decimal digits. One extra group is sufficient for
    // the right-padding needed to align a non-multiple-of-four scale.
    let mut groups = [0_u16; 11];
    let mut group_count = 0usize;
    let mut remaining = magnitude;
    while remaining != 0 {
        groups[group_count] = (remaining % 10_000) as u16;
        remaining /= 10_000;
        group_count += 1;
    }
    let leading_digits = match groups[group_count - 1] {
        0..=9 => 1,
        10..=99 => 2,
        100..=999 => 3,
        _ => 4,
    };
    let decimal_digits = (group_count - 1) * 4 + leading_digits;

    // Align the scale to PostgreSQL's base-10,000 digit boundary. Applying
    // the decimal shift group-by-group avoids overflowing u128 near its max.
    let padding = (4 - usize::from(scale) % 4) % 4;
    if padding != 0 {
        let factor = 10_u32.pow(padding as u32);
        let mut carry = 0_u32;
        for group in &mut groups[..group_count] {
            let shifted = u32::from(*group) * factor + carry;
            *group = (shifted % 10_000) as u16;
            carry = shifted / 10_000;
        }
        if carry != 0 {
            groups[group_count] = carry as u16;
            group_count += 1;
        }
    }

    let integer_digits = i32::try_from(decimal_digits).unwrap() - i32::from(scale);
    let integer_groups = (integer_digits + 3).div_euclid(4);
    let weight = i16::try_from(integer_groups - 1)
        .map_err(|_| paro_error::invalid_value("numeric", "weight out of range"))?;
    let first_fractional_group = groups[..group_count]
        .iter()
        .position(|group| *group != 0)
        .expect("non-zero magnitude has a non-zero PostgreSQL numeric digit");
    let digit_count = i16::try_from(group_count - first_fractional_group)
        .map_err(|_| paro_error::invalid_value("numeric", "digit count out of range"))?;
    encoded.put_i16(digit_count);
    encoded.put_i16(weight);
    encoded.put_u16(if negative {
        PG_NUMERIC_NEG
    } else {
        PG_NUMERIC_POS
    });
    encoded.put_u16(display_scale);
    for group in groups[first_fractional_group..group_count].iter().rev() {
        encoded.put_u16(*group);
    }
    Ok(())
}

#[cfg(test)]
fn encode_pg_numeric(magnitude: u128, negative: bool, scale: u8) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();
    append_pg_numeric(&mut encoded, magnitude, negative, scale)?;
    Ok(encoded)
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
    fn binary_numeric_uses_postgres_base_10000_format() {
        assert_eq!(
            encode_binary_value(
                &Value::Decimal(1_234_567, 9, 2),
                &LogicalType::Decimal {
                    precision: 9,
                    scale: 2,
                },
            )
            .unwrap(),
            [
                0, 3, // ndigits
                0, 1, // weight
                0, 0, // positive
                0, 2, // display scale
                0, 1, // 1
                9, 41, // 2345
                26, 44, // 6700
            ]
        );
        assert_eq!(
            encode_binary_value(
                &Value::Decimal(-12, 4, 4),
                &LogicalType::Decimal {
                    precision: 4,
                    scale: 4,
                },
            )
            .unwrap(),
            [
                0, 1, // ndigits
                255, 255, // weight = -1
                64, 0, // negative
                0, 4, // display scale
                0, 12,
            ]
        );
        assert_eq!(
            encode_binary_value(&Value::HugeInt(i128::MIN), &LogicalType::HugeInt).unwrap(),
            encode_pg_numeric(1_u128 << 127, true, 0).unwrap()
        );
        assert_eq!(
            encode_binary_value(
                &Value::Decimal(0, 8, 3),
                &LogicalType::Decimal {
                    precision: 8,
                    scale: 3,
                },
            )
            .unwrap(),
            [0, 0, 0, 0, 0, 0, 0, 3]
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
