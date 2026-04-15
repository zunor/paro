// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::codec::physical_layout;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;

pub(crate) fn decode_nested_element(logical_type: &LogicalType, bytes: &[u8]) -> Result<Value> {
    Ok(match logical_type {
        LogicalType::Boolean => Value::Boolean(read_array::<1>(bytes).map(|buf| buf[0] != 0)?),
        LogicalType::TinyInt => Value::TinyInt(read_array::<1>(bytes)?[0] as i8),
        LogicalType::UTinyInt => Value::UTinyInt(read_array::<1>(bytes)?[0]),
        LogicalType::SmallInt => Value::SmallInt(i16::from_le_bytes(read_array::<2>(bytes)?)),
        LogicalType::USmallInt => Value::USmallInt(u16::from_le_bytes(read_array::<2>(bytes)?)),
        LogicalType::Integer => Value::Integer(i32::from_le_bytes(read_array::<4>(bytes)?)),
        LogicalType::UInteger => Value::UInteger(u32::from_le_bytes(read_array::<4>(bytes)?)),
        LogicalType::BigInt => Value::BigInt(i64::from_le_bytes(read_array::<8>(bytes)?)),
        LogicalType::UBigInt => Value::UBigInt(u64::from_le_bytes(read_array::<8>(bytes)?)),
        LogicalType::HugeInt => Value::HugeInt(i128::from_le_bytes(read_array::<16>(bytes)?)),
        LogicalType::UHugeInt => Value::UHugeInt(u128::from_le_bytes(read_array::<16>(bytes)?)),
        LogicalType::Uuid => Value::Uuid(u128::from_le_bytes(read_array::<16>(bytes)?)),
        LogicalType::Float => Value::Float(f32::from_le_bytes(read_array::<4>(bytes)?)),
        LogicalType::Double => Value::Double(f64::from_le_bytes(read_array::<8>(bytes)?)),
        LogicalType::Date => Value::Date(i32::from_le_bytes(read_array::<4>(bytes)?)),
        LogicalType::Time => Value::Time(i64::from_le_bytes(read_array::<8>(bytes)?)),
        LogicalType::Timestamp => Value::Timestamp(i64::from_le_bytes(read_array::<8>(bytes)?)),
        LogicalType::TimestampTz => Value::TimestampTz(i64::from_le_bytes(read_array::<8>(bytes)?)),
        LogicalType::Interval => {
            let months = i32::from_le_bytes(read_exact(bytes, 0, 4)?);
            let days = i32::from_le_bytes(read_exact(bytes, 4, 4)?);
            let micros = i64::from_le_bytes(read_exact(bytes, 8, 8)?);
            Value::Interval(months, days, micros)
        }
        LogicalType::Decimal { precision, scale } => {
            let value = match physical_layout::decimal_storage_width(*precision) {
                8 => i64::from_le_bytes(read_array::<8>(bytes)?) as i128,
                16 => i128::from_le_bytes(read_array::<16>(bytes)?),
                other => unreachable!("unexpected decimal storage width {}", other),
            };
            Value::Decimal(value, *precision, *scale)
        }
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb => {
            let s = std::str::from_utf8(bytes)
                .map_err(|_| paro_error::data_corrupted("Nested payload invalid UTF-8"))?;
            Value::Varchar(s.to_string())
        }
        LogicalType::Blob => Value::Blob(bytes.to_vec()),
        LogicalType::Null => Value::Null(LogicalType::Null),
        other => {
            return Err(paro_error::not_supported(format!(
                "Nested payload decode does not support type {:?}",
                other
            )))
        }
    })
}

pub(crate) fn encode_nested_element(
    logical_type: &LogicalType,
    value: &Value,
    out: &mut Vec<u8>,
) -> Result<()> {
    match (logical_type, value) {
        (LogicalType::Boolean, Value::Boolean(v)) => out.push(*v as u8),
        (LogicalType::TinyInt, Value::TinyInt(v)) => out.push(*v as u8),
        (LogicalType::UTinyInt, Value::UTinyInt(v)) => out.push(*v),
        (LogicalType::SmallInt, Value::SmallInt(v)) => out.extend_from_slice(&v.to_le_bytes()),
        (LogicalType::USmallInt, Value::USmallInt(v)) => out.extend_from_slice(&v.to_le_bytes()),
        (LogicalType::Integer, Value::Integer(v)) => out.extend_from_slice(&v.to_le_bytes()),
        (LogicalType::UInteger, Value::UInteger(v)) => out.extend_from_slice(&v.to_le_bytes()),
        (LogicalType::BigInt, Value::BigInt(v)) => out.extend_from_slice(&v.to_le_bytes()),
        (LogicalType::UBigInt, Value::UBigInt(v)) => out.extend_from_slice(&v.to_le_bytes()),
        (LogicalType::HugeInt, Value::HugeInt(v)) => out.extend_from_slice(&v.to_le_bytes()),
        (LogicalType::UHugeInt, Value::UHugeInt(v)) => out.extend_from_slice(&v.to_le_bytes()),
        (LogicalType::Uuid, Value::Uuid(v)) => out.extend_from_slice(&v.to_le_bytes()),
        (LogicalType::Float, Value::Float(v)) => out.extend_from_slice(&v.to_le_bytes()),
        (LogicalType::Double, Value::Double(v)) => out.extend_from_slice(&v.to_le_bytes()),
        (LogicalType::Date, Value::Date(v)) => out.extend_from_slice(&v.to_le_bytes()),
        (LogicalType::Time, Value::Time(v))
        | (LogicalType::Timestamp, Value::Timestamp(v))
        | (LogicalType::TimestampTz, Value::TimestampTz(v)) => {
            out.extend_from_slice(&v.to_le_bytes())
        }
        (LogicalType::Interval, Value::Interval(months, days, micros)) => {
            out.extend_from_slice(&months.to_le_bytes());
            out.extend_from_slice(&days.to_le_bytes());
            out.extend_from_slice(&micros.to_le_bytes());
        }
        (LogicalType::Decimal { precision, .. }, Value::Decimal(v, _, _)) => {
            if physical_layout::decimal_storage_width(*precision) == std::mem::size_of::<i64>() {
                let narrow = i64::try_from(*v)
                    .map_err(|_| paro_error::invalid_input("Decimal value exceeds i64 range"))?;
                out.extend_from_slice(&narrow.to_le_bytes());
            } else {
                out.extend_from_slice(&v.to_le_bytes());
            }
        }
        (LogicalType::Decimal { precision, .. }, Value::BigInt(v)) => {
            if physical_layout::decimal_storage_width(*precision) == std::mem::size_of::<i64>() {
                out.extend_from_slice(&v.to_le_bytes());
            } else {
                out.extend_from_slice(&(*v as i128).to_le_bytes());
            }
        }
        (LogicalType::Decimal { .. }, Value::HugeInt(v)) => out.extend_from_slice(&v.to_le_bytes()),
        (
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb,
            Value::Varchar(s),
        ) => out.extend_from_slice(s.as_bytes()),
        (LogicalType::Blob, Value::Blob(b)) => out.extend_from_slice(b),
        (LogicalType::Null, Value::Null(_)) => out.extend(std::iter::repeat_n(
            0u8,
            physical_layout::fixed_row_width(logical_type)?,
        )),
        _ => {
            return Err(paro_error::invalid_input(format!(
                "Nested payload type mismatch: expected {}, got {:?}",
                logical_type, value
            )))
        }
    }
    Ok(())
}

pub(crate) fn encode_list_payload(child_type: &LogicalType, values: &[Value]) -> Result<Vec<u8>> {
    let count = values.len();
    let mut payload = Vec::new();
    payload.extend_from_slice(&(count as u32).to_le_bytes());

    let null_bytes_len = count.div_ceil(8);
    let mut nulls = vec![0u8; null_bytes_len];
    for (idx, value) in values.iter().enumerate() {
        if value.is_null() {
            nulls[idx / 8] |= 1 << (idx % 8);
        }
    }
    payload.extend_from_slice(&nulls);

    for (idx, value) in values.iter().enumerate() {
        let is_null = (nulls[idx / 8] >> (idx % 8)) & 1 == 1;
        if physical_layout::list_child_is_varlen(child_type) {
            let mut bytes = Vec::new();
            if !is_null {
                encode_nested_element(child_type, value, &mut bytes)?;
            }
            payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            payload.extend_from_slice(&bytes);
        } else if is_null {
            let size = physical_layout::list_child_fixed_size(child_type)?;
            payload.resize(payload.len() + size, 0);
        } else {
            encode_nested_element(child_type, value, &mut payload)?;
        }
    }

    Ok(payload)
}

pub(crate) fn encode_struct_payload(
    fields: &[(String, LogicalType)],
    values: &[Value],
) -> Result<Vec<u8>> {
    if fields.len() != values.len() {
        return Err(paro_error::invalid_input(format!(
            "Struct field count mismatch: expected {}, got {}",
            fields.len(),
            values.len()
        )));
    }

    let count = fields.len();
    let mut payload = Vec::new();
    payload.extend_from_slice(&(count as u32).to_le_bytes());

    let null_bytes_len = count.div_ceil(8);
    let mut nulls = vec![0u8; null_bytes_len];
    for (idx, value) in values.iter().enumerate() {
        if value.is_null() {
            nulls[idx / 8] |= 1 << (idx % 8);
        }
    }
    payload.extend_from_slice(&nulls);

    for (idx, value) in values.iter().enumerate() {
        let field_type = &fields[idx].1;
        let is_null = (nulls[idx / 8] >> (idx % 8)) & 1 == 1;
        if physical_layout::struct_field_is_varlen(field_type) {
            let mut bytes = Vec::new();
            if !is_null {
                encode_nested_element(field_type, value, &mut bytes)?;
            }
            payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            payload.extend_from_slice(&bytes);
        } else if is_null {
            let size = physical_layout::struct_field_fixed_size(field_type)?;
            payload.resize(payload.len() + size, 0);
        } else {
            encode_nested_element(field_type, value, &mut payload)?;
        }
    }

    Ok(payload)
}

fn read_array<const N: usize>(bytes: &[u8]) -> Result<[u8; N]> {
    bytes.try_into().map_err(|_| {
        paro_error::data_corrupted(format!("Expected {} bytes, got {}", N, bytes.len()))
    })
}

fn read_exact<const N: usize>(bytes: &[u8], offset: usize, len: usize) -> Result<[u8; N]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| paro_error::data_corrupted("Nested payload offset overflow"))?;
    bytes[offset..end]
        .try_into()
        .map_err(|_| paro_error::data_corrupted("Nested payload truncated"))
}
