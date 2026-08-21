// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::ptr;

use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::StringView;
use paro_common::vector::Vector;

use super::RowHeapWriter;
use crate::row::RowLayout;

#[inline]
pub unsafe fn row_is_valid(row_ptr: *const u8, col_idx: usize) -> bool {
    let byte_idx = col_idx / 8;
    let bit_idx = col_idx % 8;
    // SAFETY: caller guarantees `row_ptr` points at a valid row for `col_idx`.
    (unsafe { ptr::read(row_ptr.add(byte_idx)) } & (1 << bit_idx)) != 0
}

#[inline]
pub unsafe fn clear_row_validity(row_ptr: *mut u8, col_idx: usize) {
    let byte_idx = col_idx / 8;
    let bit_idx = col_idx % 8;
    // SAFETY: caller guarantees `row_ptr` points at a valid row for `col_idx`.
    let byte_ptr = unsafe { row_ptr.add(byte_idx) };
    // SAFETY: caller guarantees `row_ptr` points at a valid row for `col_idx`.
    let current = unsafe { ptr::read(byte_ptr) };
    // SAFETY: caller guarantees `row_ptr` points at a valid row for `col_idx`.
    unsafe { ptr::write(byte_ptr, current & !(1 << bit_idx)) };
}

#[inline]
pub unsafe fn set_row_validity(row_ptr: *mut u8, col_idx: usize) {
    let byte_idx = col_idx / 8;
    let bit_idx = col_idx % 8;
    // SAFETY: caller guarantees `row_ptr` points at a valid row for `col_idx`.
    let byte_ptr = unsafe { row_ptr.add(byte_idx) };
    // SAFETY: caller guarantees `row_ptr` points at a valid row for `col_idx`.
    let current = unsafe { ptr::read(byte_ptr) };
    // SAFETY: caller guarantees `row_ptr` points at a valid row for `col_idx`.
    unsafe { ptr::write(byte_ptr, current | (1 << bit_idx)) };
}

pub unsafe fn read_row_value(layout: &RowLayout, row_ptr: *const u8, col_idx: usize) -> Value {
    let logical_type = layout.types()[col_idx].clone();
    if !layout.all_valid() && !unsafe { row_is_valid(row_ptr, col_idx) } {
        return Value::Null(logical_type);
    }

    let offset = layout.offsets()[col_idx];
    // SAFETY: caller guarantees `row_ptr` is valid for this layout.
    let data_ptr = unsafe { row_ptr.add(offset) };

    match &logical_type {
        paro_common::types::LogicalType::Boolean => {
            Value::Boolean(unsafe { ptr::read(data_ptr) != 0 })
        }
        paro_common::types::LogicalType::TinyInt => {
            Value::TinyInt(unsafe { ptr::read(data_ptr as *const i8) })
        }
        paro_common::types::LogicalType::UTinyInt => {
            Value::UTinyInt(unsafe { ptr::read(data_ptr) })
        }
        paro_common::types::LogicalType::SmallInt => {
            Value::SmallInt(unsafe { ptr::read_unaligned(data_ptr as *const i16) })
        }
        paro_common::types::LogicalType::USmallInt => {
            Value::USmallInt(unsafe { ptr::read_unaligned(data_ptr as *const u16) })
        }
        paro_common::types::LogicalType::Integer => {
            Value::Integer(unsafe { ptr::read_unaligned(data_ptr as *const i32) })
        }
        paro_common::types::LogicalType::UInteger => {
            Value::UInteger(unsafe { ptr::read_unaligned(data_ptr as *const u32) })
        }
        paro_common::types::LogicalType::BigInt => {
            Value::BigInt(unsafe { ptr::read_unaligned(data_ptr as *const i64) })
        }
        paro_common::types::LogicalType::UBigInt => {
            Value::UBigInt(unsafe { ptr::read_unaligned(data_ptr as *const u64) })
        }
        paro_common::types::LogicalType::Float => {
            Value::Float(unsafe { ptr::read_unaligned(data_ptr as *const f32) })
        }
        paro_common::types::LogicalType::Double => {
            Value::Double(unsafe { ptr::read_unaligned(data_ptr as *const f64) })
        }
        paro_common::types::LogicalType::HugeInt => {
            Value::HugeInt(unsafe { ptr::read_unaligned(data_ptr as *const i128) })
        }
        paro_common::types::LogicalType::UHugeInt => {
            Value::UHugeInt(unsafe { ptr::read_unaligned(data_ptr as *const u128) })
        }
        paro_common::types::LogicalType::Uuid => {
            Value::Uuid(unsafe { ptr::read_unaligned(data_ptr as *const u128) })
        }
        paro_common::types::LogicalType::Date => {
            Value::Date(unsafe { ptr::read_unaligned(data_ptr as *const i32) })
        }
        paro_common::types::LogicalType::Timestamp => {
            Value::Timestamp(unsafe { ptr::read_unaligned(data_ptr as *const i64) })
        }
        paro_common::types::LogicalType::TimestampTz => {
            Value::TimestampTz(unsafe { ptr::read_unaligned(data_ptr as *const i64) })
        }
        paro_common::types::LogicalType::Time => {
            Value::Time(unsafe { ptr::read_unaligned(data_ptr as *const i64) })
        }
        paro_common::types::LogicalType::Interval => Value::Interval(
            unsafe { ptr::read_unaligned(data_ptr as *const i32) },
            unsafe { ptr::read_unaligned(data_ptr.add(4) as *const i32) },
            unsafe { ptr::read_unaligned(data_ptr.add(8) as *const i64) },
        ),
        paro_common::types::LogicalType::Decimal { precision, scale } => {
            let value = if *precision <= 18 {
                unsafe { ptr::read_unaligned(data_ptr as *const i64) as i128 }
            } else {
                unsafe { ptr::read_unaligned(data_ptr as *const i128) }
            };
            Value::Decimal(value, *precision, *scale)
        }
        paro_common::types::LogicalType::Varchar
        | paro_common::types::LogicalType::VarcharCollation(_)
        | paro_common::types::LogicalType::TsVector
        | paro_common::types::LogicalType::TsQuery
        | paro_common::types::LogicalType::Json
        | paro_common::types::LogicalType::Jsonb
        | paro_common::types::LogicalType::StringLiteral => {
            // SAFETY: `data_ptr` addresses a live canonical row varlen cell.
            let value = unsafe { StringView::from_cell(data_ptr) };
            // This legacy scalar accessor cannot return a decoding error. Keep
            // it memory-safe for corrupted rows; fallible bulk gather paths
            // validate and report the malformed value instead.
            Value::Varchar(String::from_utf8_lossy(value.as_bytes()).into_owned())
        }
        paro_common::types::LogicalType::Blob => {
            // SAFETY: `data_ptr` addresses a live canonical row varlen cell.
            let value = unsafe { StringView::from_cell(data_ptr) };
            Value::Blob(value.as_bytes().to_vec())
        }
        paro_common::types::LogicalType::List(_)
        | paro_common::types::LogicalType::Array(_, _)
        | paro_common::types::LogicalType::Struct(_) => {
            let value_ptr = unsafe { ptr::read_unaligned(data_ptr as *const *const Value) };
            if value_ptr.is_null() {
                Value::Null(logical_type)
            } else {
                unsafe { (*value_ptr).clone() }
            }
        }
        _ => Value::Null(logical_type),
    }
}

pub unsafe fn write_row_value(
    layout: &RowLayout,
    row_ptr: *mut u8,
    col_idx: usize,
    value: &Value,
    heap: &mut impl RowHeapWriter,
) -> Result<()> {
    let offset = layout.offsets()[col_idx];
    // SAFETY: caller guarantees `row_ptr` is valid for this layout.
    let cell_ptr = unsafe { row_ptr.add(offset) };
    let cell_size = RowLayout::get_type_size(&layout.types()[col_idx]);

    if value.is_null() {
        // SAFETY: caller guarantees `cell_ptr` is a valid cell slot.
        unsafe {
            ptr::write_bytes(cell_ptr, 0, cell_size);
            clear_row_validity(row_ptr, col_idx);
        }
        return Ok(());
    }

    match value {
        Value::Boolean(v) => unsafe { ptr::write(cell_ptr, u8::from(*v)) },
        Value::TinyInt(v) => unsafe { ptr::write(cell_ptr as *mut i8, *v) },
        Value::UTinyInt(v) => unsafe { ptr::write(cell_ptr, *v) },
        Value::SmallInt(v) => unsafe { ptr::write_unaligned(cell_ptr as *mut i16, *v) },
        Value::USmallInt(v) => unsafe { ptr::write_unaligned(cell_ptr as *mut u16, *v) },
        Value::Integer(v) => unsafe { ptr::write_unaligned(cell_ptr as *mut i32, *v) },
        Value::UInteger(v) => unsafe { ptr::write_unaligned(cell_ptr as *mut u32, *v) },
        Value::BigInt(v) => unsafe { ptr::write_unaligned(cell_ptr as *mut i64, *v) },
        Value::UBigInt(v) => unsafe { ptr::write_unaligned(cell_ptr as *mut u64, *v) },
        Value::Float(v) => unsafe { ptr::write_unaligned(cell_ptr as *mut f32, *v) },
        Value::Double(v) => unsafe { ptr::write_unaligned(cell_ptr as *mut f64, *v) },
        Value::HugeInt(v) => unsafe { ptr::write_unaligned(cell_ptr as *mut i128, *v) },
        Value::UHugeInt(v) => unsafe { ptr::write_unaligned(cell_ptr as *mut u128, *v) },
        Value::Uuid(v) => unsafe { ptr::write_unaligned(cell_ptr as *mut u128, *v) },
        Value::Date(v) => unsafe { ptr::write_unaligned(cell_ptr as *mut i32, *v) },
        Value::Timestamp(v) | Value::TimestampTz(v) | Value::Time(v) => unsafe {
            ptr::write_unaligned(cell_ptr as *mut i64, *v)
        },
        Value::Interval(months, days, micros) => unsafe {
            ptr::write_unaligned(cell_ptr as *mut i32, *months);
            ptr::write_unaligned(cell_ptr.add(4) as *mut i32, *days);
            ptr::write_unaligned(cell_ptr.add(8) as *mut i64, *micros);
        },
        Value::Decimal(value, precision, _) => {
            if *precision <= 18 {
                unsafe { ptr::write_unaligned(cell_ptr as *mut i64, *value as i64) };
            } else {
                unsafe { ptr::write_unaligned(cell_ptr as *mut i128, *value) };
            }
        }
        Value::Varchar(v) => unsafe { write_varlen_bytes(cell_ptr, v.as_bytes(), heap)? },
        Value::Blob(v) => unsafe { write_varlen_bytes(cell_ptr, v.as_slice(), heap)? },
        Value::List(_, _) | Value::Array(_, _, _) | Value::Struct(_, _) => {
            let value_ptr = heap.store_value(value.clone())?;
            unsafe {
                ptr::write_bytes(cell_ptr, 0, cell_size);
                ptr::write_unaligned(cell_ptr as *mut *const Value, value_ptr);
            }
        }
        Value::Null(_) => unreachable!("NULL is handled before write_row_value dispatch"),
    }

    Ok(())
}

pub unsafe fn write_vector_value(
    layout: &RowLayout,
    row_ptr: *mut u8,
    col_idx: usize,
    vector: &Vector,
    row_idx: usize,
    heap: &mut impl RowHeapWriter,
) -> Result<()> {
    let offset = layout.offsets()[col_idx];
    // SAFETY: caller guarantees `row_ptr` is valid for this layout.
    let cell_ptr = unsafe { row_ptr.add(offset) };
    let cell_size = RowLayout::get_type_size(&layout.types()[col_idx]);

    if vector.is_null(row_idx) {
        unsafe {
            ptr::write_bytes(cell_ptr, 0, cell_size);
            clear_row_validity(row_ptr, col_idx);
        }
        return Ok(());
    }

    match &layout.types()[col_idx] {
        paro_common::types::LogicalType::Boolean => unsafe {
            ptr::write(
                cell_ptr,
                u8::from(vector.get_bool(row_idx).unwrap_or(false)),
            )
        },
        paro_common::types::LogicalType::TinyInt => unsafe {
            ptr::write(
                cell_ptr as *mut i8,
                vector.get_i8(row_idx).unwrap_or_default(),
            )
        },
        paro_common::types::LogicalType::UTinyInt => unsafe {
            ptr::write(cell_ptr, vector.get_u8(row_idx).unwrap_or_default())
        },
        paro_common::types::LogicalType::SmallInt => unsafe {
            ptr::write_unaligned(
                cell_ptr as *mut i16,
                vector.get_i16(row_idx).unwrap_or_default(),
            )
        },
        paro_common::types::LogicalType::USmallInt => unsafe {
            ptr::write_unaligned(
                cell_ptr as *mut u16,
                vector.get_u16(row_idx).unwrap_or_default(),
            )
        },
        paro_common::types::LogicalType::Integer => unsafe {
            ptr::write_unaligned(
                cell_ptr as *mut i32,
                vector.get_i32(row_idx).unwrap_or_default(),
            )
        },
        paro_common::types::LogicalType::UInteger => unsafe {
            ptr::write_unaligned(
                cell_ptr as *mut u32,
                vector.get_u32(row_idx).unwrap_or_default(),
            )
        },
        paro_common::types::LogicalType::BigInt => unsafe {
            ptr::write_unaligned(
                cell_ptr as *mut i64,
                vector.get_i64(row_idx).unwrap_or_default(),
            )
        },
        paro_common::types::LogicalType::UBigInt => unsafe {
            ptr::write_unaligned(
                cell_ptr as *mut u64,
                vector.get_u64(row_idx).unwrap_or_default(),
            )
        },
        paro_common::types::LogicalType::Float => unsafe {
            ptr::write_unaligned(
                cell_ptr as *mut f32,
                vector.get_f32(row_idx).unwrap_or_default(),
            )
        },
        paro_common::types::LogicalType::Double => unsafe {
            ptr::write_unaligned(
                cell_ptr as *mut f64,
                vector.get_f64(row_idx).unwrap_or_default(),
            )
        },
        paro_common::types::LogicalType::HugeInt => unsafe {
            ptr::write_unaligned(
                cell_ptr as *mut i128,
                vector.get_i128(row_idx).unwrap_or_default(),
            )
        },
        paro_common::types::LogicalType::UHugeInt => unsafe {
            ptr::write_unaligned(
                cell_ptr as *mut u128,
                vector.get_u128(row_idx).unwrap_or_default(),
            )
        },
        paro_common::types::LogicalType::Uuid => unsafe {
            ptr::write_unaligned(
                cell_ptr as *mut u128,
                vector.get_u128(row_idx).unwrap_or_default(),
            )
        },
        paro_common::types::LogicalType::Date => unsafe {
            ptr::write_unaligned(
                cell_ptr as *mut i32,
                vector.get_i32(row_idx).unwrap_or_default(),
            )
        },
        paro_common::types::LogicalType::Timestamp
        | paro_common::types::LogicalType::TimestampTz
        | paro_common::types::LogicalType::Time => unsafe {
            ptr::write_unaligned(
                cell_ptr as *mut i64,
                vector.get_i64(row_idx).unwrap_or_default(),
            )
        },
        paro_common::types::LogicalType::Interval => {
            let (months, days, micros) = vector.get_interval(row_idx).unwrap_or((0, 0, 0));
            unsafe {
                ptr::write_unaligned(cell_ptr as *mut i32, months);
                ptr::write_unaligned(cell_ptr.add(4) as *mut i32, days);
                ptr::write_unaligned(cell_ptr.add(8) as *mut i64, micros);
            }
        }
        paro_common::types::LogicalType::Decimal { precision, .. } => {
            if *precision <= 18 {
                unsafe {
                    ptr::write_unaligned(
                        cell_ptr as *mut i64,
                        vector.get_i64(row_idx).unwrap_or_default(),
                    )
                };
            } else {
                unsafe {
                    ptr::write_unaligned(
                        cell_ptr as *mut i128,
                        vector.get_i128(row_idx).unwrap_or_default(),
                    )
                };
            }
        }
        paro_common::types::LogicalType::Varchar
        | paro_common::types::LogicalType::VarcharCollation(_)
        | paro_common::types::LogicalType::TsVector
        | paro_common::types::LogicalType::TsQuery
        | paro_common::types::LogicalType::Json
        | paro_common::types::LogicalType::Jsonb
        | paro_common::types::LogicalType::StringLiteral => {
            let value = vector.get_string(row_idx).unwrap_or_default();
            unsafe { write_varlen_bytes(cell_ptr, value.as_bytes(), heap)? };
        }
        paro_common::types::LogicalType::Blob => {
            let value = vector.get_blob(row_idx).unwrap_or_default();
            unsafe { write_varlen_bytes(cell_ptr, value, heap)? };
        }
        paro_common::types::LogicalType::List(_)
        | paro_common::types::LogicalType::Array(_, _)
        | paro_common::types::LogicalType::Struct(_) => {
            let value_ptr = heap.store_value(vector.get_value(row_idx))?;
            unsafe {
                ptr::write_bytes(cell_ptr, 0, cell_size);
                ptr::write_unaligned(cell_ptr as *mut *const Value, value_ptr);
            }
        }
        _ => {
            let value = vector.get_value(row_idx);
            unsafe { write_row_value(layout, row_ptr, col_idx, &value, heap) }?;
        }
    }

    Ok(())
}

unsafe fn write_varlen_bytes(
    cell_ptr: *mut u8,
    bytes: &[u8],
    heap: &mut impl RowHeapWriter,
) -> Result<()> {
    let len = u32::try_from(bytes.len())
        .map_err(|_| paro_common::error::out_of_range("row varlen value exceeds u32 length"))?;
    let value = if let Some(value) = StringView::try_inline(bytes) {
        value
    } else {
        let heap_ptr = heap.store_bytes(bytes)?;
        // SAFETY: the row heap owns the initialized bytes for the row lifetime.
        unsafe { StringView::from_out_of_line(bytes, heap_ptr, len) }
    };
    // SAFETY: `cell_ptr` addresses a writable StringView-sized row cell.
    unsafe { value.write_cell(cell_ptr) };
    Ok(())
}
