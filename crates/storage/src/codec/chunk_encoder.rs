// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Chunk → columnar storage encoding.

use crate::codec::{nested_payload_codec, physical_layout};
use crate::rowset::ColumnData;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;

pub(crate) fn encode_chunk(types: &[LogicalType], chunk: &Chunk) -> Result<Vec<ColumnData>> {
    if chunk.size() == 0 {
        return Ok(Vec::new());
    }

    let mut cols = Vec::with_capacity(chunk.column_count());
    for (i, ty) in types.iter().enumerate() {
        let v = chunk
            .column(i)
            .ok_or_else(|| paro_error::invalid_input("column missing"))?;
        let validity = v.validity();
        let count = chunk.size();

        // null bitmap (1 bit = null)
        let mut nulls = vec![0u8; count.div_ceil(8)];
        for row in 0..count {
            if !validity.is_valid(row) {
                nulls[row / 8] |= 1 << (row % 8);
            }
        }

        let mut data = Vec::new();
        match ty {
            LogicalType::Boolean => {
                for row in 0..count {
                    let val: bool = unsafe { v.get_flat(row) };
                    data.push(val as u8);
                }
            }
            LogicalType::TinyInt => {
                for row in 0..count {
                    let val: i8 = unsafe { v.get_flat(row) };
                    data.push(val as u8);
                }
            }
            LogicalType::SmallInt => {
                for row in 0..count {
                    let val: i16 = unsafe { v.get_flat(row) };
                    data.extend_from_slice(&val.to_le_bytes());
                }
            }
            LogicalType::Integer => {
                for row in 0..count {
                    let val: i32 = unsafe { v.get_flat(row) };
                    data.extend_from_slice(&val.to_le_bytes());
                }
            }
            LogicalType::BigInt => {
                for row in 0..count {
                    let val: i64 = unsafe { v.get_flat(row) };
                    data.extend_from_slice(&val.to_le_bytes());
                }
            }
            LogicalType::UTinyInt => {
                for row in 0..count {
                    let val: u8 = unsafe { v.get_flat(row) };
                    data.push(val);
                }
            }
            LogicalType::USmallInt => {
                for row in 0..count {
                    let val: u16 = unsafe { v.get_flat(row) };
                    data.extend_from_slice(&val.to_le_bytes());
                }
            }
            LogicalType::UInteger => {
                for row in 0..count {
                    let val: u32 = unsafe { v.get_flat(row) };
                    data.extend_from_slice(&val.to_le_bytes());
                }
            }
            LogicalType::UBigInt => {
                for row in 0..count {
                    let val: u64 = unsafe { v.get_flat(row) };
                    data.extend_from_slice(&val.to_le_bytes());
                }
            }
            LogicalType::HugeInt => {
                for row in 0..count {
                    let val: i128 = unsafe { v.get_flat(row) };
                    data.extend_from_slice(&val.to_le_bytes());
                }
            }
            LogicalType::UHugeInt => {
                for row in 0..count {
                    let val: u128 = unsafe { v.get_flat(row) };
                    data.extend_from_slice(&val.to_le_bytes());
                }
            }
            LogicalType::Uuid => {
                for row in 0..count {
                    let val: u128 = unsafe { v.get_flat(row) };
                    data.extend_from_slice(&val.to_le_bytes());
                }
            }
            LogicalType::Float => {
                for row in 0..count {
                    let val: f32 = unsafe { v.get_flat(row) };
                    data.extend_from_slice(&val.to_le_bytes());
                }
            }
            LogicalType::Double => {
                for row in 0..count {
                    let val: f64 = unsafe { v.get_flat(row) };
                    data.extend_from_slice(&val.to_le_bytes());
                }
            }
            LogicalType::Date => {
                for row in 0..count {
                    let val: i32 = unsafe { v.get_flat(row) };
                    data.extend_from_slice(&val.to_le_bytes());
                }
            }
            LogicalType::Time | LogicalType::Timestamp | LogicalType::TimestampTz => {
                for row in 0..count {
                    let val: i64 = unsafe { v.get_flat(row) };
                    data.extend_from_slice(&val.to_le_bytes());
                }
            }
            LogicalType::Interval => {
                for row in 0..count {
                    let val: i128 = unsafe { v.get_flat(row) };
                    data.extend_from_slice(&val.to_le_bytes());
                }
            }
            LogicalType::Decimal { precision, .. } => {
                let width = physical_layout::decimal_storage_width(*precision);
                for row in 0..count {
                    if !validity.is_valid(row) {
                        data.resize(data.len() + width, 0);
                        continue;
                    }

                    if width == std::mem::size_of::<i64>() {
                        let val = v.get_i64(row).ok_or_else(|| {
                            paro_error::internal(
                                "Failed to read Decimal(i64) value while encoding chunk",
                            )
                        })?;
                        data.extend_from_slice(&val.to_le_bytes());
                    } else {
                        let val = v.get_i128(row).ok_or_else(|| {
                            paro_error::internal(
                                "Failed to read Decimal(i128) value while encoding chunk",
                            )
                        })?;
                        data.extend_from_slice(&val.to_le_bytes());
                    }
                }
            }
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb => {
                for row in 0..count {
                    if !validity.is_valid(row) {
                        data.extend_from_slice(&0u32.to_le_bytes());
                        continue;
                    }
                    let s = v.get_string(row).ok_or_else(|| {
                        paro_error::internal("Failed to read string value in Tablet append")
                    })?;
                    data.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    data.extend_from_slice(s.as_bytes());
                }
            }
            LogicalType::Blob => {
                for row in 0..count {
                    if !validity.is_valid(row) {
                        data.extend_from_slice(&0u32.to_le_bytes());
                        continue;
                    }
                    let b = v.get_blob(row).ok_or_else(|| {
                        paro_error::internal("Failed to read blob value in Tablet append")
                    })?;
                    data.extend_from_slice(&(b.len() as u32).to_le_bytes());
                    data.extend_from_slice(b);
                }
            }
            LogicalType::List(child_type) => {
                for row in 0..count {
                    if !validity.is_valid(row) {
                        data.extend_from_slice(&0u32.to_le_bytes());
                        continue;
                    }

                    let row_value = v.get_value(row);
                    let values = match row_value {
                        Value::List(values, _) | Value::Array(values, _, _) => values,
                        Value::Null(_) => {
                            data.extend_from_slice(&0u32.to_le_bytes());
                            continue;
                        }
                        _ => {
                            return Err(paro_error::invalid_input(format!(
                                "List column {} expects list value, got {:?}",
                                i, row_value
                            )));
                        }
                    };

                    let payload = nested_payload_codec::encode_list_payload(child_type, &values)?;
                    data.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                    data.extend_from_slice(&payload);
                }
            }
            LogicalType::Struct(fields) => {
                for row in 0..count {
                    if !validity.is_valid(row) {
                        data.extend_from_slice(&0u32.to_le_bytes());
                        continue;
                    }

                    let row_value = v.get_value(row);
                    let values = match row_value {
                        Value::Struct(values, _) => values,
                        Value::Null(_) => {
                            data.extend_from_slice(&0u32.to_le_bytes());
                            continue;
                        }
                        _ => {
                            return Err(paro_error::invalid_input(format!(
                                "Struct column {} expects struct value, got {:?}",
                                i, row_value
                            )));
                        }
                    };

                    let payload = nested_payload_codec::encode_struct_payload(fields, &values)?;
                    data.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                    data.extend_from_slice(&payload);
                }
            }
            LogicalType::Array(inner, dim) if matches!(**inner, LogicalType::Float) => {
                let dim = *dim;
                for row in 0..count {
                    // Keep payload width stable for fixed-size vector columns.
                    if !validity.is_valid(row) {
                        data.resize(data.len() + dim * std::mem::size_of::<f32>(), 0);
                        continue;
                    }

                    let row_value = v.get_value(row);
                    let values = match row_value {
                        Value::Array(values, _, _) | Value::List(values, _) => values,
                        Value::Null(_) => {
                            data.resize(data.len() + dim * std::mem::size_of::<f32>(), 0);
                            continue;
                        }
                        _ => {
                            return Err(paro_error::invalid_input(format!(
                                "Vector column {} expects array value, got {:?}",
                                i, row_value
                            )));
                        }
                    };

                    if values.len() != dim {
                        return Err(paro_error::invalid_input(format!(
                            "Vector dimension mismatch for column {}: expected {}, got {}",
                            i,
                            dim,
                            values.len()
                        )));
                    }

                    for value in values {
                        let f = match value {
                            Value::Float(v) => v,
                            Value::Double(v) => v as f32,
                            Value::TinyInt(v) => v as f32,
                            Value::SmallInt(v) => v as f32,
                            Value::Integer(v) => v as f32,
                            Value::BigInt(v) => v as f32,
                            Value::UTinyInt(v) => v as f32,
                            Value::USmallInt(v) => v as f32,
                            Value::UInteger(v) => v as f32,
                            Value::UBigInt(v) => v as f32,
                            _ => {
                                return Err(paro_error::invalid_input(format!(
                                    "Vector element must be numeric, got {:?}",
                                    value
                                )));
                            }
                        };
                        data.extend_from_slice(&f.to_le_bytes());
                    }
                }
            }
            _ => {
                return Err(paro_error::not_supported(format!(
                    "Type {:?} not yet supported in Tablet append",
                    ty
                )));
            }
        }

        let col = ColumnData::with_nulls(data, nulls, count as u32);
        cols.push(col);
    }
    Ok(cols)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use paro_common::allocator::default_allocator;
    use paro_common::vector::Vector;
    use std::sync::Arc;

    #[test]
    fn decimal_precision_18_uses_narrow_storage_width() {
        let decimal_type = LogicalType::Decimal {
            precision: 18,
            scale: 2,
        };
        let mut vector = Vector::with_capacity_and_allocator(
            decimal_type.clone(),
            2,
            Arc::new(default_allocator()),
        );
        vector.set_value(0, &Value::Decimal(12_345, 18, 2));
        vector.set_value(1, &Value::Decimal(-678, 18, 2));
        vector.set_count(2);

        let chunk = Chunk::from_vectors(vec![vector]);
        let encoded = encode_chunk(std::slice::from_ref(&decimal_type), &chunk).unwrap();
        assert_eq!(encoded[0].data.len(), 2 * std::mem::size_of::<i64>());

        let decoded = crate::codec::vector_decoder::build_vector_from_bytes(
            &decimal_type,
            &Bytes::from(encoded[0].data.clone()),
            2,
            Arc::new(default_allocator()),
        )
        .unwrap();
        assert_eq!(decoded.get_i64(0), Some(12_345));
        assert_eq!(decoded.get_i64(1), Some(-678));
    }
}
