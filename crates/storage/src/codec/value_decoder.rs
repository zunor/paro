use crate::codec::{nested_payload_codec, physical_layout, vector_decoder};
use bytes::Bytes;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;

pub(crate) fn decode_values(
    logical_type: &LogicalType,
    data: &Bytes,
    nulls: Option<&[u8]>,
    rows: usize,
) -> Result<Vec<Value>> {
    if rows == 0 {
        return Ok(Vec::new());
    }

    let mut values = match logical_type {
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb
        | LogicalType::Blob
        | LogicalType::List(_)
        | LogicalType::Struct(_) => decode_varlen(logical_type, data, rows)?,
        LogicalType::Array(inner, dim) if matches!(**inner, LogicalType::Float) => {
            let width = dim
                .checked_mul(std::mem::size_of::<f32>())
                .ok_or_else(|| paro_error::data_corrupted("Array row width overflow"))?;
            decode_fixed(data.as_ref(), rows, width, |slice| {
                vector_decoder::decode_payload_value(logical_type, slice)
            })?
        }
        LogicalType::Null => vec![Value::Null(LogicalType::Null); rows],
        _ => {
            let width = physical_layout::fixed_row_width(logical_type)?;
            decode_fixed(data.as_ref(), rows, width, |slice| {
                nested_payload_codec::decode_nested_element(logical_type, slice)
            })?
        }
    };

    if let Some(nulls) = nulls {
        if nulls.len() < rows {
            return Err(paro_error::data_corrupted(
                "Null map shorter than expected row count",
            ));
        }
        for idx in 0..rows {
            if nulls[idx] != 0 {
                values[idx] = Value::Null(logical_type.clone());
            }
        }
    }

    Ok(values)
}

fn decode_fixed<F>(bytes: &[u8], rows: usize, type_size: usize, mut f: F) -> Result<Vec<Value>>
where
    F: FnMut(&[u8]) -> Result<Value>,
{
    let expected = rows
        .checked_mul(type_size)
        .ok_or_else(|| paro_error::data_corrupted("Fixed-length decode overflow"))?;
    if bytes.len() < expected {
        return Err(paro_error::data_corrupted(
            "Fixed-length column data too small",
        ));
    }

    let mut values = Vec::with_capacity(rows);
    for i in 0..rows {
        let offset = i * type_size;
        values.push(f(&bytes[offset..offset + type_size])?);
    }
    Ok(values)
}

fn decode_varlen(logical_type: &LogicalType, data: &Bytes, rows: usize) -> Result<Vec<Value>> {
    vector_decoder::parse_varlen_values(data, rows)?
        .into_iter()
        .map(|payload| vector_decoder::decode_payload_value(logical_type, &payload))
        .collect()
}
