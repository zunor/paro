// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::codec::{column_decoder::decode_varlen_cell, vector_decoder};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

pub(crate) fn decode_cell_into_vector(
    logical_type: &LogicalType,
    bytes: &[u8],
    vector: &mut Vector,
    row_idx: usize,
) -> Result<()> {
    let payload = match logical_type {
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb
        | LogicalType::Blob
        | LogicalType::List(_)
        | LogicalType::Struct(_) => decode_varlen_cell(bytes)?,
        LogicalType::Array(inner, dim) if matches!(**inner, LogicalType::Float) => {
            let expected = dim
                .checked_mul(std::mem::size_of::<f32>())
                .ok_or_else(|| paro_error::data_corrupted("Array decode: width overflow"))?;
            if bytes.len() < expected {
                return Err(paro_error::data_corrupted(
                    "Array(Float) decode: insufficient bytes",
                ));
            }
            &bytes[..expected]
        }
        _ => bytes,
    };

    let value = vector_decoder::decode_payload_value(logical_type, payload)?;
    vector.set_value(row_idx, &value);
    Ok(())
}
