// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Gather columnar chunks back out of the raw row backend.

use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::{LogicalType, StringView};
use paro_common::vector::{SelectionVector, Vector};

use super::{
    RawRowCollection, RawRowLayout, RawRowPinProperties, RawRowPinState, RawRowScanState,
    RawRowSegment,
};

/// Gather data from row storage to a Chunk.
///
/// This is the main entry point for reading row-based data back to columnar format.
///
/// # Arguments
/// * `collection` - The raw row collection to read from
/// * `row_locations` - Pointers to row storage locations
/// * `chunk` - The data chunk to fill
/// * `count` - Number of rows to gather
pub fn gather_chunk(
    collection: &RawRowCollection,
    row_locations: &[*const u8],
    chunk: &mut Chunk,
    count: usize,
) -> Result<()> {
    if count > chunk.capacity() {
        return Err(paro_error::out_of_range(format!(
            "raw-row gather exceeds output capacity: rows={count}, capacity={}",
            chunk.capacity()
        )));
    }
    let layout = collection.layout();

    // Gather each column
    for col_idx in 0..layout.column_count() {
        if let Some(vector) = chunk.column_mut(col_idx) {
            let offset = layout.get_offsets()[col_idx];
            gather_vector(layout, vector, col_idx, offset, row_locations, count)?;
        }
    }

    chunk.try_set_cardinality(count)?;
    Ok(())
}

/// Gather data with a selection vector.
///
/// # Arguments
/// * `collection` - The raw row collection to read from
/// * `row_locations` - Pointers to row storage locations
/// * `sel` - Selection vector specifying which output positions to fill
/// * `chunk` - The data chunk to fill
/// * `count` - Number of rows to gather
pub fn gather_chunk_with_sel(
    collection: &RawRowCollection,
    row_locations: &[*const u8],
    sel: &SelectionVector,
    chunk: &mut Chunk,
    count: usize,
) -> Result<()> {
    let layout = collection.layout();

    for col_idx in 0..layout.column_count() {
        if let Some(vector) = chunk.column_mut(col_idx) {
            let offset = layout.get_offsets()[col_idx];
            gather_vector_with_sel(layout, vector, col_idx, offset, row_locations, sel, count)?;
        }
    }

    chunk.try_set_cardinality(count)?;
    Ok(())
}

/// Gather a single vector from row storage.
fn gather_vector(
    layout: &RawRowLayout,
    vector: &mut Vector,
    col_idx: usize,
    offset: usize,
    row_locations: &[*const u8],
    count: usize,
) -> Result<()> {
    let logical_type = vector.logical_type().clone();

    match &logical_type {
        // Fixed-size numeric types
        LogicalType::Boolean => {
            gather_fixed::<u8>(layout, vector, col_idx, offset, row_locations, count)
        }
        LogicalType::TinyInt => {
            gather_fixed::<i8>(layout, vector, col_idx, offset, row_locations, count)
        }
        LogicalType::UTinyInt => {
            gather_fixed::<u8>(layout, vector, col_idx, offset, row_locations, count)
        }
        LogicalType::SmallInt => {
            gather_fixed::<i16>(layout, vector, col_idx, offset, row_locations, count)
        }
        LogicalType::USmallInt => {
            gather_fixed::<u16>(layout, vector, col_idx, offset, row_locations, count)
        }
        LogicalType::Integer => {
            gather_fixed::<i32>(layout, vector, col_idx, offset, row_locations, count)
        }
        LogicalType::UInteger => {
            gather_fixed::<u32>(layout, vector, col_idx, offset, row_locations, count)
        }
        LogicalType::BigInt => {
            gather_fixed::<i64>(layout, vector, col_idx, offset, row_locations, count)
        }
        LogicalType::UBigInt => {
            gather_fixed::<u64>(layout, vector, col_idx, offset, row_locations, count)
        }
        LogicalType::Float => {
            gather_fixed::<f32>(layout, vector, col_idx, offset, row_locations, count)
        }
        LogicalType::Double => {
            gather_fixed::<f64>(layout, vector, col_idx, offset, row_locations, count)
        }
        LogicalType::Date => {
            gather_fixed::<i32>(layout, vector, col_idx, offset, row_locations, count)
        }
        LogicalType::Time | LogicalType::TimestampTz => {
            gather_fixed::<i64>(layout, vector, col_idx, offset, row_locations, count)
        }
        LogicalType::Timestamp => {
            gather_fixed::<i64>(layout, vector, col_idx, offset, row_locations, count)
        }
        LogicalType::HugeInt => {
            gather_fixed::<i128>(layout, vector, col_idx, offset, row_locations, count)
        }
        LogicalType::UHugeInt => {
            gather_fixed::<u128>(layout, vector, col_idx, offset, row_locations, count)
        }
        LogicalType::Uuid => {
            gather_fixed::<u128>(layout, vector, col_idx, offset, row_locations, count)
        }
        LogicalType::Interval => {
            gather_fixed::<i128>(layout, vector, col_idx, offset, row_locations, count)
        }
        LogicalType::Decimal { precision, .. } => {
            if *precision <= 18 {
                gather_fixed::<i64>(layout, vector, col_idx, offset, row_locations, count)
            } else {
                gather_fixed::<i128>(layout, vector, col_idx, offset, row_locations, count)
            }
        }

        LogicalType::Null => {
            // Null type: just set count and mark all as null
            vector.try_set_len(count)?;
            for i in 0..count {
                vector.try_set_null(i, true)?;
            }
            Ok(())
        }

        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb
        | LogicalType::Blob => gather_string(layout, vector, col_idx, offset, row_locations, count),

        LogicalType::Array(_, _) => {
            array_row_gather(layout, vector, col_idx, offset, row_locations, count)
        }
        LogicalType::List(_) => {
            list_row_gather(layout, vector, col_idx, offset, row_locations, count)
        }
        LogicalType::Struct(fields) => gather_struct(
            layout,
            vector,
            col_idx,
            offset,
            row_locations,
            count,
            fields,
        ),
        _ => {
            // For unsupported types, set vector length first, then set all to null
            vector.try_set_len(count)?;
            for i in 0..count {
                vector.try_set_null(i, true)?;
            }
            Ok(())
        }
    }
}

/// Check if a row's column is valid (not NULL).
///
/// # Safety
/// row_ptr must point to a valid row in row storage.
#[inline]
pub unsafe fn is_row_valid(row_ptr: *const u8, col_idx: usize) -> bool {
    let entry_idx = col_idx / 8;
    let bit_idx = col_idx % 8;
    // SAFETY: row_ptr is valid and entry_idx is within validity bytes
    unsafe {
        let validity_byte = std::ptr::read(row_ptr.add(entry_idx));
        (validity_byte & (1 << bit_idx)) != 0
    }
}

/// Read a single value from row storage.
///
/// # Safety
/// row_ptr must point to a valid row in row storage.
pub unsafe fn read_value(
    row_ptr: *const u8,
    col_idx: usize,
    logical_type: &LogicalType,
    layout: &RawRowLayout,
    allocator: Arc<dyn Allocator>,
) -> Result<paro_common::runtime_value::Value> {
    use paro_common::runtime_value::Value;

    if !layout.all_valid() && unsafe { !is_row_valid(row_ptr, col_idx) } {
        return Ok(Value::Null(logical_type.clone()));
    }

    let offset = layout.get_offsets()[col_idx];
    let data_ptr = row_ptr.add(offset);

    match logical_type {
        LogicalType::Boolean => Ok(Value::Boolean(std::ptr::read(data_ptr) != 0)),
        LogicalType::TinyInt => Ok(Value::TinyInt(std::ptr::read(data_ptr as *const i8))),
        LogicalType::UTinyInt => Ok(Value::UTinyInt(std::ptr::read(data_ptr))),
        LogicalType::SmallInt => Ok(Value::SmallInt(std::ptr::read_unaligned(
            data_ptr as *const i16,
        ))),
        LogicalType::USmallInt => Ok(Value::USmallInt(std::ptr::read_unaligned(
            data_ptr as *const u16,
        ))),
        LogicalType::Integer => Ok(Value::Integer(std::ptr::read_unaligned(
            data_ptr as *const i32,
        ))),
        LogicalType::UInteger => Ok(Value::UInteger(std::ptr::read_unaligned(
            data_ptr as *const u32,
        ))),
        LogicalType::BigInt => Ok(Value::BigInt(std::ptr::read_unaligned(
            data_ptr as *const i64,
        ))),
        LogicalType::UBigInt => Ok(Value::UBigInt(std::ptr::read_unaligned(
            data_ptr as *const u64,
        ))),
        LogicalType::Float => Ok(Value::Float(std::ptr::read_unaligned(
            data_ptr as *const f32,
        ))),
        LogicalType::Double => Ok(Value::Double(std::ptr::read_unaligned(
            data_ptr as *const f64,
        ))),
        LogicalType::Decimal { precision, scale } => {
            let value = if *precision <= 18 {
                std::ptr::read_unaligned(data_ptr as *const i64) as i128
            } else {
                std::ptr::read_unaligned(data_ptr as *const i128)
            };
            Ok(Value::Decimal(value, *precision, *scale))
        }

        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb
        | LogicalType::Blob => {
            // SAFETY: `data_ptr` addresses a live canonical row varlen cell.
            let value = StringView::from_cell(data_ptr);
            if logical_type == &LogicalType::Blob {
                Ok(Value::Blob(value.as_bytes().to_vec()))
            } else {
                Ok(Value::Varchar(
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                ))
            }
        }

        LogicalType::Array(_, _) | LogicalType::List(_) => {
            let heap_ptr = std::ptr::read(data_ptr as *const *const u8);
            let mut vector = Vector::try_new(logical_type.clone(), 1, Arc::clone(&allocator))?;
            let _ = gather_collection_entry(logical_type, &mut vector, 0, heap_ptr)?;
            vector.try_set_count(1)?;
            Ok(vector.get_value(0))
        }

        LogicalType::Struct(fields) => {
            let struct_layout = RawRowLayout::struct_layout(fields);
            let mut values = Vec::with_capacity(fields.len());
            for (field_idx, (_name, field_type)) in fields.iter().enumerate() {
                let field_val = read_value(
                    data_ptr,
                    field_idx,
                    field_type,
                    &struct_layout,
                    Arc::clone(&allocator),
                )?;
                values.push(field_val);
            }
            Ok(Value::Struct(values, fields.clone()))
        }

        _ => Ok(Value::Null(logical_type.clone())),
    }
}

/// Gather fixed-size values from row storage.
fn gather_fixed<T: Copy + Default>(
    layout: &RawRowLayout,
    vector: &mut Vector,
    col_idx: usize,
    offset: usize,
    row_locations: &[*const u8],
    count: usize,
) -> Result<()> {
    let all_valid = layout.all_valid();

    // Dispatch to the optimal templated version
    if all_valid {
        gather_fixed_internal::<T, true>(layout, vector, col_idx, offset, row_locations, count)
    } else {
        gather_fixed_internal::<T, false>(layout, vector, col_idx, offset, row_locations, count)
    }
}

#[inline(always)]
fn gather_fixed_internal<T: Copy + Default, const ALL_VALID: bool>(
    _layout: &RawRowLayout,
    vector: &mut Vector,
    col_idx: usize,
    offset: usize,
    row_locations: &[*const u8],
    count: usize,
) -> Result<()> {
    // Set vector length first to ensure validity mask is properly sized
    vector.try_set_len(count)?;

    // Get data pointer for writing
    let data_ptr = unsafe { vector.flat_data_mut::<T>() };
    if data_ptr.is_null() {
        return Ok(());
    }

    for (i, &row_ptr) in row_locations.iter().enumerate().take(count) {
        let is_valid = if ALL_VALID {
            true
        } else {
            unsafe { is_row_valid(row_ptr, col_idx) }
        };

        if is_valid {
            // SAFETY: row_ptr + offset points to valid data
            // Use unaligned read because row data may not be aligned
            unsafe {
                let src = row_ptr.add(offset) as *const T;
                let value = std::ptr::read_unaligned(src);
                std::ptr::write(data_ptr.add(i), value);
            }
            vector.try_set_null(i, false)?;
        } else {
            // Write default value and mark as null
            unsafe {
                std::ptr::write(data_ptr.add(i), T::default());
            }
            vector.try_set_null(i, true)?;
        }
    }
    Ok(())
}

/// Gather string values from row storage.
///
/// String layout in row (16 bytes total):
/// - 4 bytes: length
/// - 4 bytes: prefix (first 4 chars or padding)
/// - 8 bytes: pointer to heap data (or inline data for short strings)
fn gather_string(
    layout: &RawRowLayout,
    vector: &mut Vector,
    col_idx: usize,
    offset: usize,
    row_locations: &[*const u8],
    count: usize,
) -> Result<()> {
    let all_valid = layout.all_valid();

    // Dispatch to the optimal templated version
    if all_valid {
        gather_string_internal::<true>(layout, vector, col_idx, offset, row_locations, count)
    } else {
        gather_string_internal::<false>(layout, vector, col_idx, offset, row_locations, count)
    }
}

/// Gather Struct values from row storage.
fn gather_struct(
    layout: &RawRowLayout,
    vector: &mut Vector,
    col_idx: usize,
    offset: usize,
    row_locations: &[*const u8],
    count: usize,
    fields: &[(String, LogicalType)],
) -> Result<()> {
    let struct_layout = RawRowLayout::struct_layout(fields);

    // Ensure vector length and parent validity.
    vector.try_set_len(count)?;
    if layout.all_valid() {
        for i in 0..count {
            vector.try_set_null(i, false)?;
        }
    } else {
        for (i, &row_ptr) in row_locations.iter().enumerate().take(count) {
            let valid = unsafe { is_row_valid(row_ptr, col_idx) };
            vector.try_set_null(i, !valid)?;
        }
    }

    let mut struct_row_locations = Vec::with_capacity(count);
    for row_ptr in row_locations.iter().take(count) {
        unsafe {
            struct_row_locations.push(row_ptr.add(offset));
        }
    }

    let Some(children) = vector.children_mut() else {
        return Ok(());
    };

    for (field_idx, _field) in fields.iter().enumerate() {
        if field_idx >= children.len() {
            continue;
        }
        let child = Vector::try_make_arc_mut(&mut children[field_idx])?;
        let child_offset = struct_layout.get_offsets()[field_idx];
        gather_vector(
            &struct_layout,
            child,
            field_idx,
            child_offset,
            &struct_row_locations,
            count,
        )?;
    }
    Ok(())
}

/// Gather Struct values with selection vector.
#[allow(clippy::too_many_arguments)]
fn gather_struct_with_sel(
    layout: &RawRowLayout,
    vector: &mut Vector,
    col_idx: usize,
    offset: usize,
    row_locations: &[*const u8],
    sel: &SelectionVector,
    count: usize,
    fields: &[(String, LogicalType)],
) -> Result<()> {
    let struct_layout = RawRowLayout::struct_layout(fields);

    vector.try_set_len(required_len_for_sel(sel, count))?;
    if layout.all_valid() {
        for i in 0..count {
            let dst_idx = sel.get(i);
            vector.try_set_null(dst_idx, false)?;
        }
    } else {
        for (i, &row_ptr) in row_locations.iter().enumerate().take(count) {
            let valid = unsafe { is_row_valid(row_ptr, col_idx) };
            let dst_idx = sel.get(i);
            vector.try_set_null(dst_idx, !valid)?;
        }
    }

    let mut struct_row_locations = Vec::with_capacity(count);
    for row_ptr in row_locations.iter().take(count) {
        unsafe {
            struct_row_locations.push(row_ptr.add(offset));
        }
    }

    let Some(children) = vector.children_mut() else {
        return Ok(());
    };

    for (field_idx, _field) in fields.iter().enumerate() {
        if field_idx >= children.len() {
            continue;
        }
        let child = Vector::try_make_arc_mut(&mut children[field_idx])?;
        let child_offset = struct_layout.get_offsets()[field_idx];
        gather_vector_with_sel(
            &struct_layout,
            child,
            field_idx,
            child_offset,
            &struct_row_locations,
            sel,
            count,
        )?;
    }
    Ok(())
}

#[inline(always)]
fn gather_string_internal<const ALL_VALID: bool>(
    _layout: &RawRowLayout,
    vector: &mut Vector,
    col_idx: usize,
    offset: usize,
    row_locations: &[*const u8],
    count: usize,
) -> Result<()> {
    let is_blob = vector.logical_type() == &LogicalType::Blob;

    // Set vector length first to ensure validity mask is properly sized
    vector.try_set_len(count)?;

    for (i, &row_ptr) in row_locations.iter().enumerate().take(count) {
        let is_valid = if ALL_VALID {
            true
        } else {
            unsafe { is_row_valid(row_ptr, col_idx) }
        };

        if is_valid {
            // SAFETY: `row_ptr + offset` addresses a live canonical row cell.
            let value = unsafe { StringView::from_cell(row_ptr.add(offset)) };
            if is_blob {
                vector.try_set_blob(i, value.as_bytes())?;
            } else if let Ok(text) = value.as_str() {
                vector.try_set_string(i, text)?;
            } else {
                vector.try_set_string(i, "")?;
            }
            vector.try_set_null(i, false)?;
        } else {
            // Write default value based on type
            if is_blob {
                vector.try_set_blob(i, &[])?;
            } else {
                vector.try_set_string(i, "")?;
            }
            vector.try_set_null(i, true)?;
        }
    }
    Ok(())
}

/// Gather a single vector with selection vector.
fn gather_vector_with_sel(
    layout: &RawRowLayout,
    vector: &mut Vector,
    col_idx: usize,
    offset: usize,
    row_locations: &[*const u8],
    sel: &SelectionVector,
    count: usize,
) -> Result<()> {
    let logical_type = vector.logical_type().clone();

    match &logical_type {
        LogicalType::Boolean => {
            gather_fixed_with_sel::<u8>(layout, vector, col_idx, offset, row_locations, sel, count)
        }
        LogicalType::TinyInt => {
            gather_fixed_with_sel::<i8>(layout, vector, col_idx, offset, row_locations, sel, count)
        }
        LogicalType::UTinyInt => {
            gather_fixed_with_sel::<u8>(layout, vector, col_idx, offset, row_locations, sel, count)
        }
        LogicalType::SmallInt => {
            gather_fixed_with_sel::<i16>(layout, vector, col_idx, offset, row_locations, sel, count)
        }
        LogicalType::USmallInt => {
            gather_fixed_with_sel::<u16>(layout, vector, col_idx, offset, row_locations, sel, count)
        }
        LogicalType::Integer => {
            gather_fixed_with_sel::<i32>(layout, vector, col_idx, offset, row_locations, sel, count)
        }
        LogicalType::UInteger => {
            gather_fixed_with_sel::<u32>(layout, vector, col_idx, offset, row_locations, sel, count)
        }
        LogicalType::BigInt => {
            gather_fixed_with_sel::<i64>(layout, vector, col_idx, offset, row_locations, sel, count)
        }
        LogicalType::UBigInt => {
            gather_fixed_with_sel::<u64>(layout, vector, col_idx, offset, row_locations, sel, count)
        }
        LogicalType::Float => {
            gather_fixed_with_sel::<f32>(layout, vector, col_idx, offset, row_locations, sel, count)
        }
        LogicalType::Double => {
            gather_fixed_with_sel::<f64>(layout, vector, col_idx, offset, row_locations, sel, count)
        }
        LogicalType::Date => {
            gather_fixed_with_sel::<i32>(layout, vector, col_idx, offset, row_locations, sel, count)
        }
        LogicalType::Time | LogicalType::TimestampTz => {
            gather_fixed_with_sel::<i64>(layout, vector, col_idx, offset, row_locations, sel, count)
        }
        LogicalType::Timestamp => {
            gather_fixed_with_sel::<i64>(layout, vector, col_idx, offset, row_locations, sel, count)
        }
        LogicalType::HugeInt => gather_fixed_with_sel::<i128>(
            layout,
            vector,
            col_idx,
            offset,
            row_locations,
            sel,
            count,
        ),
        LogicalType::UHugeInt => gather_fixed_with_sel::<u128>(
            layout,
            vector,
            col_idx,
            offset,
            row_locations,
            sel,
            count,
        ),
        LogicalType::Uuid => gather_fixed_with_sel::<u128>(
            layout,
            vector,
            col_idx,
            offset,
            row_locations,
            sel,
            count,
        ),
        LogicalType::Interval => gather_fixed_with_sel::<i128>(
            layout,
            vector,
            col_idx,
            offset,
            row_locations,
            sel,
            count,
        ),
        LogicalType::Decimal { precision, .. } => {
            if *precision <= 18 {
                gather_fixed_with_sel::<i64>(
                    layout,
                    vector,
                    col_idx,
                    offset,
                    row_locations,
                    sel,
                    count,
                )
            } else {
                gather_fixed_with_sel::<i128>(
                    layout,
                    vector,
                    col_idx,
                    offset,
                    row_locations,
                    sel,
                    count,
                )
            }
        }
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb
        | LogicalType::Blob => {
            gather_string_with_sel(layout, vector, col_idx, offset, row_locations, sel, count)
        }
        LogicalType::Array(_, _) => {
            array_row_gather_with_sel(layout, vector, col_idx, offset, row_locations, sel, count)
        }
        LogicalType::List(_) => {
            list_row_gather_with_sel(layout, vector, col_idx, offset, row_locations, sel, count)
        }
        LogicalType::Struct(fields) => gather_struct_with_sel(
            layout,
            vector,
            col_idx,
            offset,
            row_locations,
            sel,
            count,
            fields,
        ),
        _ => {
            // For unsupported types, set vector length first, then set all to null
            vector.try_set_len(required_len_for_sel(sel, count))?;
            for i in 0..count {
                let dst_idx = sel.get(i);
                vector.try_set_null(dst_idx, true)?;
            }
            Ok(())
        }
    }
}

/// Gather fixed-size values with selection vector.
fn gather_fixed_with_sel<T: Copy + Default>(
    layout: &RawRowLayout,
    vector: &mut Vector,
    col_idx: usize,
    offset: usize,
    row_locations: &[*const u8],
    sel: &SelectionVector,
    count: usize,
) -> Result<()> {
    let all_valid = layout.all_valid();

    // Set vector length first to ensure validity mask is properly sized
    vector.try_set_len(required_len_for_sel(sel, count))?;

    let data_ptr = unsafe { vector.flat_data_mut::<T>() };
    if data_ptr.is_null() {
        return Ok(());
    }

    for (i, &row_ptr) in row_locations.iter().enumerate().take(count) {
        let dst_idx = sel.get(i);
        let is_valid = all_valid || unsafe { is_row_valid(row_ptr, col_idx) };

        if is_valid {
            unsafe {
                let src = row_ptr.add(offset) as *const T;
                let value = std::ptr::read_unaligned(src);
                std::ptr::write(data_ptr.add(dst_idx), value);
            }
            vector.try_set_null(dst_idx, false)?;
        } else {
            unsafe {
                std::ptr::write(data_ptr.add(dst_idx), T::default());
            }
            vector.try_set_null(dst_idx, true)?;
        }
    }
    Ok(())
}

/// Gather string values with selection vector.
fn gather_string_with_sel(
    layout: &RawRowLayout,
    vector: &mut Vector,
    col_idx: usize,
    offset: usize,
    row_locations: &[*const u8],
    sel: &SelectionVector,
    count: usize,
) -> Result<()> {
    let all_valid = layout.all_valid();

    // Set vector length first to ensure validity mask is properly sized
    vector.try_set_len(required_len_for_sel(sel, count))?;

    for (i, &row_ptr) in row_locations.iter().enumerate().take(count) {
        let dst_idx = sel.get(i);
        let is_valid = all_valid || unsafe { is_row_valid(row_ptr, col_idx) };

        if is_valid {
            // SAFETY: `row_ptr + offset` addresses a live canonical row cell.
            let value = unsafe { StringView::from_cell(row_ptr.add(offset)) };
            if vector.logical_type() == &LogicalType::Blob {
                vector.try_set_blob(dst_idx, value.as_bytes())?;
            } else if let Ok(text) = value.as_str() {
                vector.try_set_string(dst_idx, text)?;
            } else {
                vector.try_set_string(dst_idx, "")?;
            }
            vector.try_set_null(dst_idx, false)?;
        } else {
            vector.try_set_null(dst_idx, true)?;
        }
    }
    Ok(())
}

#[inline]
unsafe fn collection_mask_is_valid(mask_ptr: *const u8, idx: usize) -> bool {
    let byte_idx = idx / 8;
    let bit_idx = idx % 8;
    (std::ptr::read(mask_ptr.add(byte_idx)) & (1 << bit_idx)) != 0
}

fn required_len_for_sel(sel: &SelectionVector, count: usize) -> usize {
    (0..count)
        .map(|idx| sel.get(idx).saturating_add(1))
        .max()
        .unwrap_or(0)
}

#[inline]
unsafe fn write_list_entry(vector: &mut Vector, idx: usize, offset: u32, length: u32) {
    let base = vector.flat_data_mut::<u8>();
    let ptr = base.add(idx * 8) as *mut u32;
    std::ptr::write_unaligned(ptr, offset);
    std::ptr::write_unaligned(ptr.add(1), length);
}

fn set_array_child_nulls(vector: &mut Vector, index: usize, array_size: usize) -> Result<()> {
    if let Some(child) = vector.child_mut() {
        let child_mut = Vector::try_make_arc_mut(child)?;
        let child_base = index.saturating_mul(array_size);
        let required = child_base.saturating_add(array_size);
        if child_mut.len() < required {
            child_mut.try_set_len(required)?;
        }
        for i in 0..array_size {
            child_mut.try_set_null(child_base + i, true)?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
unsafe fn gather_collection_payload(
    logical_type: &LogicalType,
    child_vector: &mut Vector,
    child_base: usize,
    expected_count: usize,
    source_count: usize,
    mask_ptr: *const u8,
    payload_ptr: *const u8,
) -> Result<*const u8> {
    if child_vector.len() < child_base.saturating_add(expected_count) {
        child_vector.try_set_len(child_base.saturating_add(expected_count))?;
    }

    if RawRowLayout::type_is_constant_size(logical_type) {
        let element_size = RawRowLayout::get_type_size(logical_type);
        let child_data = child_vector.flat_data_mut::<u8>();
        for elem_i in 0..expected_count {
            let child_idx = child_base + elem_i;
            let valid = elem_i < source_count && collection_mask_is_valid(mask_ptr, elem_i);

            if valid && !child_data.is_null() {
                let src_ptr = payload_ptr.add(elem_i * element_size);
                let dst_ptr = child_data.add(child_idx * element_size);
                std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, element_size);
                child_vector.try_set_null(child_idx, false)?;
            } else {
                child_vector.try_set_null(child_idx, true)?;
            }
        }
        return Ok(payload_ptr.add(source_count * element_size));
    }

    match logical_type {
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb
        | LogicalType::Blob => {
            let inline_data = payload_ptr as *const paro_common::types::StringView;
            let mut heap_bytes = 0usize;
            for elem_i in 0..expected_count {
                let child_idx = child_base + elem_i;
                let valid = elem_i < source_count && collection_mask_is_valid(mask_ptr, elem_i);
                if !valid {
                    child_vector.try_set_null(child_idx, true)?;
                    continue;
                }

                let string = std::ptr::read_unaligned(inline_data.add(elem_i));
                let bytes = string.as_bytes();
                if matches!(logical_type, LogicalType::Blob) {
                    child_vector.try_set_blob(child_idx, bytes)?;
                } else {
                    let value = String::from_utf8_lossy(bytes);
                    child_vector.try_set_string(child_idx, value.as_ref())?;
                }
                if !string.is_inlined() && !string.is_empty() && elem_i < source_count {
                    heap_bytes = heap_bytes.saturating_add(string.len());
                }
            }
            let inline_size = paro_common::types::StringView::SIZE;
            Ok(payload_ptr.add(source_count * inline_size + heap_bytes))
        }
        LogicalType::Struct(fields) => {
            for elem_i in 0..expected_count {
                let child_idx = child_base + elem_i;
                let valid = elem_i < source_count && collection_mask_is_valid(mask_ptr, elem_i);
                child_vector.try_set_null(child_idx, !valid)?;
            }

            let Some(children) = child_vector.children_mut() else {
                return Ok(payload_ptr);
            };

            let mut cursor = payload_ptr;
            let field_mask_size = RawRowLayout::validity_mask_size(source_count);
            for (field_idx, (_name, field_type)) in fields.iter().enumerate() {
                if field_idx >= children.len() {
                    continue;
                }
                let field_mask_ptr = cursor;
                let field_payload_ptr = cursor.add(field_mask_size);
                let child = Vector::try_make_arc_mut(&mut children[field_idx])?;
                cursor = gather_collection_payload(
                    field_type,
                    child,
                    child_base,
                    expected_count,
                    source_count,
                    field_mask_ptr,
                    field_payload_ptr,
                )?;
            }
            Ok(cursor)
        }
        LogicalType::List(_) | LogicalType::Array(_, _) => {
            let nested_ptrs = payload_ptr as *const *const u8;
            let pointer_size = std::mem::size_of::<usize>();
            let mut cursor = payload_ptr.add(source_count * pointer_size);
            for elem_i in 0..expected_count {
                let child_idx = child_base + elem_i;
                let valid = elem_i < source_count && collection_mask_is_valid(mask_ptr, elem_i);
                if !valid {
                    gather_collection_entry(
                        logical_type,
                        child_vector,
                        child_idx,
                        std::ptr::null(),
                    )?;
                    continue;
                }

                let nested_heap_ptr = std::ptr::read_unaligned(nested_ptrs.add(elem_i));
                let nested_end = gather_collection_entry(
                    logical_type,
                    child_vector,
                    child_idx,
                    nested_heap_ptr,
                )?;
                cursor = nested_end;
            }
            Ok(cursor)
        }
        _ => {
            for elem_i in 0..expected_count {
                child_vector.try_set_null(child_base + elem_i, true)?;
            }
            Ok(payload_ptr)
        }
    }
}

unsafe fn gather_collection_entry(
    logical_type: &LogicalType,
    vector: &mut Vector,
    index: usize,
    heap_ptr: *const u8,
) -> Result<*const u8> {
    if heap_ptr.is_null() {
        vector.try_set_null(index, true)?;
        if let LogicalType::Array(_, array_size) = logical_type {
            set_array_child_nulls(vector, index, *array_size)?;
        }
        return Ok(heap_ptr);
    }

    vector.try_set_null(index, false)?;
    let length = std::ptr::read_unaligned(heap_ptr as *const u64) as usize;
    if length == 0 {
        if let LogicalType::Array(_, array_size) = logical_type {
            set_array_child_nulls(vector, index, *array_size)?;
        }
        return Ok(heap_ptr.add(8));
    }

    let mask_ptr = heap_ptr.add(8);
    let payload_ptr = mask_ptr.add(RawRowLayout::validity_mask_size(length));

    match logical_type {
        LogicalType::Array(child_type, array_size) => {
            if let Some(child) = vector.child_mut() {
                let child_mut = Vector::try_make_arc_mut(child)?;
                let child_base = index.saturating_mul(*array_size);
                return gather_collection_payload(
                    child_type,
                    child_mut,
                    child_base,
                    *array_size,
                    length,
                    mask_ptr,
                    payload_ptr,
                );
            }
        }
        LogicalType::List(child_type) => {
            let mut child_base = 0usize;
            let mut end_ptr = payload_ptr;
            if let Some(child) = vector.child_mut() {
                let mut child_mut = Vector::try_make_arc_mut(child)?;
                child_base = child_mut.len();
                let required = child_base.saturating_add(length);
                if required > child_mut.logical_capacity() {
                    let new_capacity = required
                        .max(child_mut.logical_capacity().saturating_mul(2))
                        .max(1);
                    let mut new_child = Vector::try_new(
                        child_mut.logical_type().clone(),
                        new_capacity,
                        child_mut.allocator().clone(),
                    )?;
                    new_child.try_copy_range(0, child_mut, 0, child_base)?;
                    *child = Arc::new(new_child);
                    child_mut = Vector::try_make_arc_mut(child)?;
                }

                // Ensure validity mask can address appended range.
                child_mut.try_set_len(required)?;
                end_ptr = gather_collection_payload(
                    child_type,
                    child_mut,
                    child_base,
                    length,
                    length,
                    mask_ptr,
                    payload_ptr,
                )?;
            }

            if child_base > u32::MAX as usize || length > u32::MAX as usize {
                return Err(paro_error::internal(format!(
                    "List entry exceeds u32 range: offset={child_base}, length={length}"
                )));
            }
            unsafe {
                write_list_entry(vector, index, child_base as u32, length as u32);
            }
            return Ok(end_ptr);
        }
        _ => {}
    }

    Ok(payload_ptr)
}

fn gather_collection_internal(
    layout: &RawRowLayout,
    vector: &mut Vector,
    col_idx: usize,
    offset: usize,
    row_locations: &[*const u8],
    sel: Option<&SelectionVector>,
    count: usize,
) -> Result<()> {
    let required_len = sel.map(|s| required_len_for_sel(s, count)).unwrap_or(count);
    vector.try_set_len(required_len)?;
    let logical_type = vector.logical_type().clone();

    for (i, &row_ptr) in row_locations.iter().enumerate().take(count) {
        let dst_idx = sel.map(|s| s.get(i)).unwrap_or(i);
        let is_valid = layout.all_valid() || unsafe { is_row_valid(row_ptr, col_idx) };
        let heap_ptr = if is_valid {
            unsafe {
                let src = row_ptr.add(offset);
                std::ptr::read(src as *const *const u8)
            }
        } else {
            std::ptr::null()
        };

        unsafe {
            let _ = gather_collection_entry(&logical_type, vector, dst_idx, heap_ptr)?;
        }
    }
    Ok(())
}

fn array_row_gather(
    layout: &RawRowLayout,
    vector: &mut Vector,
    col_idx: usize,
    offset: usize,
    row_locations: &[*const u8],
    count: usize,
) -> Result<()> {
    gather_collection_internal(layout, vector, col_idx, offset, row_locations, None, count)
}

fn array_row_gather_with_sel(
    layout: &RawRowLayout,
    vector: &mut Vector,
    col_idx: usize,
    offset: usize,
    row_locations: &[*const u8],
    sel: &SelectionVector,
    count: usize,
) -> Result<()> {
    gather_collection_internal(
        layout,
        vector,
        col_idx,
        offset,
        row_locations,
        Some(sel),
        count,
    )
}

fn list_row_gather(
    layout: &RawRowLayout,
    vector: &mut Vector,
    col_idx: usize,
    offset: usize,
    row_locations: &[*const u8],
    count: usize,
) -> Result<()> {
    gather_collection_internal(layout, vector, col_idx, offset, row_locations, None, count)
}

fn list_row_gather_with_sel(
    layout: &RawRowLayout,
    vector: &mut Vector,
    col_idx: usize,
    offset: usize,
    row_locations: &[*const u8],
    sel: &SelectionVector,
    count: usize,
) -> Result<()> {
    gather_collection_internal(
        layout,
        vector,
        col_idx,
        offset,
        row_locations,
        Some(sel),
        count,
    )
}

/// Scan a chunk from a RawRowCollection.
///
/// This reads the next chunk of data from the collection into the provided Chunk.
///
/// # Arguments
/// * `collection` - The collection to scan from
/// * `state` - The scan state (tracks position)
/// * `chunk` - The output chunk to fill
///
/// # Returns
/// Number of rows scanned (0 if scan is complete)
pub fn scan_chunk(
    collection: &RawRowCollection,
    state: &mut RawRowScanState,
    chunk: &mut Chunk,
) -> Result<usize> {
    if collection.scan_complete(state) {
        chunk.try_set_cardinality(0)?;
        return Ok(0);
    }

    let (seg_idx, chunk_idx) = match (state.segment_index, state.chunk_index) {
        (Some(s), Some(c)) => (s, c),
        _ => {
            chunk.try_set_cardinality(0)?;
            return Ok(0);
        }
    };

    let segment = match collection.get_segment(seg_idx) {
        Some(s) => s,
        None => {
            chunk.try_set_cardinality(0)?;
            return Ok(0);
        }
    };

    let count =
        fetch_chunk_from_segment(collection, &mut state.pin_state, segment, chunk_idx, chunk)?;

    // Advance to next chunk
    collection.next_scan_index(state);

    Ok(count)
}

/// Fetch a specific chunk from a segment.
fn fetch_chunk_from_segment(
    collection: &RawRowCollection,
    pin_state: &mut RawRowPinState,
    segment: &RawRowSegment,
    chunk_idx: usize,
    chunk: &mut Chunk,
) -> Result<usize> {
    let layout = collection.layout();

    if chunk_idx >= segment.chunks.len() {
        chunk.try_set_cardinality(0)?;
        return Ok(0);
    }

    let row_chunk = &segment.chunks[chunk_idx];
    if row_chunk.count == 0 || row_chunk.part_indices.is_empty() {
        chunk.try_set_cardinality(0)?;
        return Ok(0);
    }

    // Collect row locations from all parts
    let mut row_locations: Vec<*const u8> = Vec::with_capacity(row_chunk.count);
    let row_width = layout.get_row_width();
    let allocator = segment.allocator();

    for part_idx in row_chunk.part_indices.start()..row_chunk.part_indices.end() {
        let part = &segment.chunk_parts[part_idx as usize];
        let block_ptr = match allocator.pin_chunk_part(pin_state, part) {
            Ok(p) => p,
            Err(_) => continue,
        };

        for row_in_part in 0..part.count {
            let offset = (row_in_part as usize) * row_width;
            // SAFETY: offset is within block bounds
            let row_ptr = unsafe { block_ptr.add(offset) };
            row_locations.push(row_ptr as *const u8);
        }
    }

    let count = row_locations.len();
    if count == 0 {
        chunk.try_set_cardinality(0)?;
        return Ok(0);
    }

    gather_chunk(collection, &row_locations, chunk, count)?;
    Ok(count)
}

/// Fetch a chunk by absolute index across all segments.
///
/// # Arguments
/// * `collection` - The collection to fetch from
/// * `chunk_index` - Absolute chunk index (0-based across all segments)
/// * `chunk` - The output chunk to fill
///
/// # Returns
/// Number of rows fetched (0 if index is out of bounds)
pub fn fetch_chunk(
    collection: &RawRowCollection,
    chunk_index: usize,
    chunk: &mut Chunk,
) -> Result<usize> {
    let mut remaining = chunk_index;

    for segment in (0..collection.segment_count()).map(|i| collection.get_segment(i).unwrap()) {
        if remaining < segment.chunk_count() {
            let mut pin_state = RawRowPinState::new(RawRowPinProperties::KeepEverythingPinned);
            return fetch_chunk_from_segment(collection, &mut pin_state, segment, remaining, chunk);
        }
        remaining -= segment.chunk_count();
    }

    chunk.try_set_cardinality(0)?;
    Ok(0)
}

/// Gather a single column from segments using row indices.
///
/// This function is used by SortedRun::scan() to gather data in sorted order.
///
/// # Arguments
/// * `segments` - The segments to gather from
/// * `layout` - The raw row layout
/// * `row_indices` - Indices of rows to gather (in original insertion order)
/// * `column_idx` - Index of the column to gather
/// * `output` - Output vector to write to
/// * `count` - Number of rows to gather
pub fn gather_column(
    segments: &[RawRowSegment],
    layout: &RawRowLayout,
    row_indices: &[usize],
    column_idx: usize,
    output: &mut Vector,
    count: usize,
) -> Result<()> {
    if count == 0 {
        output.try_set_count(0)?;
        return Ok(());
    }

    // Create a pin state that will keep blocks pinned for the entire gather operation
    // This is CRITICAL: without this, blocks get unpinned immediately after getting pointers,
    // causing the pointers to become invalid (blocks may be evicted or memory reused)
    let mut pin_state = RawRowPinState::new(RawRowPinProperties::KeepEverythingPinned);

    // Build row_locations from row_indices
    let mut row_locations: Vec<*const u8> = Vec::with_capacity(count);
    let row_width = layout.get_row_width();

    for &row_idx in row_indices.iter().take(count) {
        // Find which segment and local position this row is in
        let mut remaining = row_idx;
        let mut found = false;

        for segment in segments {
            let segment_row_count = segment.row_count();
            if remaining < segment_row_count {
                // This row is in this segment
                // Find the row pointer (reuse the same pin_state to keep blocks pinned)
                if let Some(row_ptr) =
                    find_row_in_segment(&mut pin_state, segment, remaining, row_width)
                {
                    row_locations.push(row_ptr);
                    found = true;
                    break;
                }
            }
            remaining -= segment_row_count;
        }

        if !found {
            return Err(paro_common::error::internal(format!(
                "Row index {} not found in segments",
                row_idx
            )));
        }
    }

    // Now gather the column using the row locations
    // The pin_state keeps blocks pinned until it goes out of scope at the end of this function
    let offset = layout.get_offsets()[column_idx];
    gather_vector(layout, output, column_idx, offset, &row_locations, count)?;

    // pin_state is dropped here, unpinning all blocks
    Ok(())
}

/// Gather a single column from explicit row pointers.
///
/// This is used when callers already hold row locations (e.g. payload pointers
/// embedded in sort keys) and want to reuse raw-row gather dispatch.
pub fn gather_column_from_row_locations(
    collection: &RawRowCollection,
    row_locations: &[*const u8],
    column_idx: usize,
    output: &mut Vector,
    count: usize,
) -> Result<()> {
    if count == 0 {
        output.try_set_count(0)?;
        return Ok(());
    }
    if column_idx >= collection.layout().column_count() {
        return Err(paro_common::error::internal(format!(
            "Column index {} out of range",
            column_idx
        )));
    }
    if row_locations.len() < count {
        return Err(paro_common::error::internal(format!(
            "Row location count {} smaller than requested gather count {}",
            row_locations.len(),
            count
        )));
    }

    let layout = collection.layout();
    let offset = layout.get_offsets()[column_idx];
    gather_vector(layout, output, column_idx, offset, row_locations, count)?;
    output.try_set_count(count)?;
    Ok(())
}

/// Find a row pointer in a segment by local row index.
fn find_row_in_segment(
    pin_state: &mut RawRowPinState,
    segment: &RawRowSegment,
    local_row_idx: usize,
    row_width: usize,
) -> Option<*const u8> {
    let mut remaining = local_row_idx;

    for chunk in &segment.chunks {
        if remaining < chunk.count {
            // This row is in this chunk
            // Find the part and offset within the chunk
            let mut chunk_offset = remaining;
            for part_idx in chunk.part_indices.start()..chunk.part_indices.end() {
                let part = &segment.chunk_parts[part_idx as usize];
                let part_count = part.count as usize;
                if chunk_offset < part_count {
                    // This row is in this part
                    let allocator = segment.allocator();
                    if let Ok(block_ptr) = allocator.pin_chunk_part(pin_state, part) {
                        let offset = chunk_offset * row_width;
                        let row_ptr = unsafe { block_ptr.add(offset) };
                        return Some(row_ptr as *const u8);
                    }
                }
                chunk_offset -= part_count;
            }
            // If we get here, something is wrong
            return None;
        }
        remaining -= chunk.count;
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{BufferPool, MemoryTag};
    use crate::row::raw::{
        append_chunk, RawRowAllocator, RawRowAppendState, RawRowChunkState, RawRowPinProperties,
        RawRowValidityType,
    };
    use crate::test_utils::*;
    use paro_common::allocator::{Allocator, DefaultAllocator};
    use paro_common::error::{self as paro_error, Result as ParoResult};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[derive(Debug)]
    struct ToggleAllocator {
        inner: DefaultAllocator,
        fail: AtomicBool,
    }

    impl ToggleAllocator {
        fn new() -> Self {
            Self {
                inner: DefaultAllocator::new(),
                fail: AtomicBool::new(false),
            }
        }

        fn set_fail(&self, fail: bool) {
            self.fail.store(fail, Ordering::SeqCst);
        }
    }

    impl Allocator for ToggleAllocator {
        fn allocate(&self, size: usize) -> ParoResult<*mut u8> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(paro_error::out_of_memory(format!(
                    "injected allocation failure: {size} bytes"
                )));
            }
            self.inner.allocate(size)
        }

        fn allocate_zeroed(&self, size: usize) -> ParoResult<*mut u8> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(paro_error::out_of_memory(format!(
                    "injected allocation failure: {size} bytes"
                )));
            }
            self.inner.allocate_zeroed(size)
        }

        fn free(&self, ptr: *mut u8, size: usize) {
            self.inner.free(ptr, size);
        }

        fn reallocate(
            &self,
            ptr: *mut u8,
            old_size: usize,
            new_size: usize,
        ) -> ParoResult<*mut u8> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(paro_error::out_of_memory(format!(
                    "injected allocation failure: {new_size} bytes"
                )));
            }
            self.inner.reallocate(ptr, old_size, new_size)
        }

        fn name(&self) -> &'static str {
            "ToggleAllocator"
        }
    }

    fn create_test_layout(types: Vec<LogicalType>) -> RawRowLayout {
        let mut layout = RawRowLayout::new();
        layout.initialize(types, RawRowValidityType::CanHaveNullValues);
        layout
    }

    fn create_test_chunk(types: &[LogicalType], count: usize) -> Chunk {
        let mut chunk = test_chunk_with_capacity(types, count);
        chunk.set_cardinality(count);
        for i in 0..chunk.column_count() {
            if let Some(v) = chunk.column_mut(i) {
                v.set_len(count);
            }
        }
        chunk
    }

    fn create_test_collection(types: Vec<LogicalType>) -> RawRowCollection {
        let pool = Arc::new(BufferPool::new(10 * 1024 * 1024));
        RawRowCollection::from_types(pool, types, MemoryTag::HashTable)
    }

    #[test]
    fn test_gather_fixed_integers() {
        let layout = create_test_layout(vec![LogicalType::Integer]);
        let row_width = layout.get_row_width();

        // Create row storage with known values
        let mut storage: Vec<Vec<u8>> = (0..3).map(|_| vec![0xFFu8; row_width]).collect();

        // Write values at correct offset
        let offset = layout.get_offsets()[0];
        for (i, val) in [42i32, -100, 999].iter().enumerate() {
            unsafe {
                let ptr = storage[i].as_mut_ptr().add(offset) as *mut i32;
                std::ptr::write_unaligned(ptr, *val);
            }
        }

        let row_locations: Vec<*const u8> = storage.iter().map(|b| b.as_ptr()).collect();

        // Create collection and output chunk
        let collection = create_test_collection(vec![LogicalType::Integer]);
        let mut chunk = create_test_chunk(&[LogicalType::Integer], 3);

        gather_chunk(&collection, &row_locations, &mut chunk, 3).unwrap();

        // Verify values
        if let Some(v) = chunk.column(0) {
            assert_eq!(v.get_i32(0), Some(42));
            assert_eq!(v.get_i32(1), Some(-100));
            assert_eq!(v.get_i32(2), Some(999));
        } else {
            panic!("Column 0 not found");
        }
    }

    #[test]
    fn test_gather_with_nulls() {
        let layout = create_test_layout(vec![LogicalType::Integer]);
        let row_width = layout.get_row_width();

        // Create row storage
        let mut storage: Vec<Vec<u8>> = (0..3).map(|_| vec![0xFFu8; row_width]).collect();

        // Write values
        let offset = layout.get_offsets()[0];
        for (i, val) in [42i32, 0, 999].iter().enumerate() {
            unsafe {
                let ptr = storage[i].as_mut_ptr().add(offset) as *mut i32;
                std::ptr::write_unaligned(ptr, *val);
            }
        }

        // Clear validity bit for row 1 (make it NULL)
        storage[1][0] &= !(1 << 0); // Clear bit 0

        let row_locations: Vec<*const u8> = storage.iter().map(|b| b.as_ptr()).collect();

        let collection = create_test_collection(vec![LogicalType::Integer]);
        let mut chunk = create_test_chunk(&[LogicalType::Integer], 3);

        gather_chunk(&collection, &row_locations, &mut chunk, 3).unwrap();

        if let Some(v) = chunk.column(0) {
            assert_eq!(v.get_i32(0), Some(42));
            assert!(!v.validity().is_valid(1)); // Should be NULL (not valid)
            assert_eq!(v.get_i32(2), Some(999));
        }
    }

    #[test]
    fn test_gather_multiple_columns() {
        let layout = create_test_layout(vec![LogicalType::Integer, LogicalType::BigInt]);
        let row_width = layout.get_row_width();

        let mut storage: Vec<Vec<u8>> = (0..2).map(|_| vec![0xFFu8; row_width]).collect();

        // Write values
        let off0 = layout.get_offsets()[0];
        let off1 = layout.get_offsets()[1];

        unsafe {
            std::ptr::write_unaligned(storage[0].as_mut_ptr().add(off0) as *mut i32, 100);
            std::ptr::write_unaligned(storage[1].as_mut_ptr().add(off0) as *mut i32, 200);
            std::ptr::write_unaligned(storage[0].as_mut_ptr().add(off1) as *mut i64, 1000);
            std::ptr::write_unaligned(storage[1].as_mut_ptr().add(off1) as *mut i64, 2000);
        }

        let row_locations: Vec<*const u8> = storage.iter().map(|b| b.as_ptr()).collect();

        let collection = create_test_collection(vec![LogicalType::Integer, LogicalType::BigInt]);
        let mut chunk = create_test_chunk(&[LogicalType::Integer, LogicalType::BigInt], 2);

        gather_chunk(&collection, &row_locations, &mut chunk, 2).unwrap();

        assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(100));
        assert_eq!(chunk.column(0).unwrap().get_i32(1), Some(200));
        assert_eq!(chunk.column(1).unwrap().get_i64(0), Some(1000));
        assert_eq!(chunk.column(1).unwrap().get_i64(1), Some(2000));
    }

    #[test]
    fn test_gather_string_view() {
        let layout = create_test_layout(vec![LogicalType::Varchar]);
        let row_width = layout.get_row_width();

        let mut storage: Vec<Vec<u8>> = (0..2).map(|_| vec![0xFFu8; row_width]).collect();

        let offset = layout.get_offsets()[0];

        // Write inline strings
        let strings = ["hello", "world"];
        for (i, s) in strings.iter().enumerate() {
            unsafe {
                let dst = storage[i].as_mut_ptr().add(offset);
                let value = StringView::try_inline(s.as_bytes()).unwrap();
                // SAFETY: `dst` is a writable varlen cell in the test row.
                value.write_cell(dst);
            }
        }

        let row_locations: Vec<*const u8> = storage.iter().map(|b| b.as_ptr()).collect();

        let collection = create_test_collection(vec![LogicalType::Varchar]);
        let mut chunk = create_test_chunk(&[LogicalType::Varchar], 2);

        gather_chunk(&collection, &row_locations, &mut chunk, 2).unwrap();

        assert_eq!(chunk.column(0).unwrap().get_string(0), Some("hello"));
        assert_eq!(chunk.column(0).unwrap().get_string(1), Some("world"));
    }

    #[test]
    fn test_gather_heap_string() {
        let layout = create_test_layout(vec![LogicalType::Varchar]);
        let row_width = layout.get_row_width();

        // Heap storage for long strings
        let long_string = "this is a very long string that exceeds inline limit";
        let heap_data: Vec<u8> = long_string.as_bytes().to_vec();

        let mut storage: Vec<Vec<u8>> = vec![vec![0xFFu8; row_width]];

        let offset = layout.get_offsets()[0];
        let len = heap_data.len();

        unsafe {
            let dst = storage[0].as_mut_ptr().add(offset);
            // SAFETY: `heap_data` remains alive through gather and owns all bytes.
            let value = StringView::from_raw_parts(heap_data.as_ptr(), len as u32);
            // SAFETY: `dst` is a writable varlen cell in the test row.
            value.write_cell(dst);
        }

        let row_locations: Vec<*const u8> = storage.iter().map(|b| b.as_ptr()).collect();

        let collection = create_test_collection(vec![LogicalType::Varchar]);
        let mut chunk = create_test_chunk(&[LogicalType::Varchar], 1);

        gather_chunk(&collection, &row_locations, &mut chunk, 1).unwrap();

        assert_eq!(chunk.column(0).unwrap().get_string(0), Some(long_string));
    }

    #[test]
    fn test_gather_heap_string_allocation_failure_returns_error() {
        let layout = create_test_layout(vec![LogicalType::Varchar]);
        let row_width = layout.get_row_width();
        let long_string = "this long string must allocate in the destination heap";
        let heap_data = long_string.as_bytes().to_vec();
        let mut storage = vec![vec![0xFFu8; row_width]];
        let offset = layout.get_offsets()[0];

        unsafe {
            let dst = storage[0].as_mut_ptr().add(offset);
            // SAFETY: `heap_data` remains alive through gather and owns all bytes.
            let value = StringView::from_raw_parts(heap_data.as_ptr(), heap_data.len() as u32);
            // SAFETY: `dst` is a writable varlen cell in the test row.
            value.write_cell(dst);
        }
        let row_locations: Vec<*const u8> = storage.iter().map(|row| row.as_ptr()).collect();

        let collection = create_test_collection(vec![LogicalType::Varchar]);
        let allocator = Arc::new(ToggleAllocator::new());
        let mut chunk =
            Chunk::try_initialize(&[LogicalType::Varchar], 1, allocator.clone()).unwrap();
        allocator.set_fail(true);

        let err = gather_chunk(&collection, &row_locations, &mut chunk, 1).unwrap_err();
        assert!(err.to_string().contains("injected allocation failure"));
    }

    #[test]
    fn test_gather_list_child_growth_failure_returns_error() {
        let list_type = LogicalType::List(Box::new(LogicalType::Integer));
        let layout = create_test_layout(vec![list_type.clone()]);
        let row_width = layout.get_row_width();
        let mut storage = vec![vec![0xFFu8; row_width]];

        let values: Vec<i32> = (0..16).collect();
        let mut list_heap = Vec::new();
        list_heap.extend_from_slice(&(values.len() as u64).to_ne_bytes());
        list_heap.extend_from_slice(&[0xFF, 0xFF]);
        for value in &values {
            list_heap.extend_from_slice(&value.to_ne_bytes());
        }

        let offset = layout.get_offsets()[0];
        unsafe {
            let dst = storage[0].as_mut_ptr().add(offset);
            std::ptr::write(dst as *mut *const u8, list_heap.as_ptr());
        }
        let row_locations: Vec<*const u8> = storage.iter().map(|row| row.as_ptr()).collect();

        let collection = create_test_collection(vec![list_type.clone()]);
        let allocator = Arc::new(ToggleAllocator::new());
        let mut chunk = Chunk::try_initialize(&[list_type], 1, allocator.clone()).unwrap();
        allocator.set_fail(true);

        let err = gather_chunk(&collection, &row_locations, &mut chunk, 1).unwrap_err();
        assert!(err.to_string().contains("injected allocation failure"));
    }

    #[test]
    fn test_gather_null_validity_failure_returns_error() {
        let row_locations: Vec<*const u8> = vec![std::ptr::null()];
        let collection = create_test_collection(vec![LogicalType::Null]);
        let allocator = Arc::new(ToggleAllocator::new());
        let mut chunk = Chunk::try_initialize(&[LogicalType::Null], 1, allocator.clone()).unwrap();
        allocator.set_fail(true);

        let err = gather_chunk(&collection, &row_locations, &mut chunk, 1).unwrap_err();
        assert!(err.to_string().contains("injected allocation failure"));
    }

    #[test]
    fn test_scatter_then_gather_roundtrip() {
        // This test verifies that scatter followed by gather produces the same data
        let types = vec![LogicalType::Integer, LogicalType::BigInt];
        let pool = Arc::new(BufferPool::new(10 * 1024 * 1024));

        let mut collection =
            RawRowCollection::from_types(pool.clone(), types.clone(), MemoryTag::HashTable);

        // Create input chunk with data
        let mut input_chunk = create_test_chunk(&types, 5);
        if let Some(v) = input_chunk.column_mut(0) {
            v.set_i32(0, 10);
            v.set_i32(1, 20);
            v.set_i32(2, 30);
            v.set_i32(3, 40);
            v.set_i32(4, 50);
        }
        if let Some(v) = input_chunk.column_mut(1) {
            v.set_i64(0, 100);
            v.set_i64(1, 200);
            v.set_i64(2, 300);
            v.set_i64(3, 400);
            v.set_i64(4, 500);
        }

        // Initialize append
        let mut append_state = RawRowAppendState::new();
        collection.initialize_append(&mut append_state, RawRowPinProperties::KeepEverythingPinned);

        // Get allocator and segment for append
        let layout = collection.layout_ptr();
        let allocator_arc = Arc::new(RawRowAllocator::new(pool, layout, MemoryTag::HashTable));
        let mut segment = RawRowSegment::new(allocator_arc);

        // Append data
        let mut pin_state = RawRowPinState::new(RawRowPinProperties::KeepEverythingPinned);
        let mut chunk_state = RawRowChunkState::new();
        let appended = append_chunk(
            &mut collection,
            &mut pin_state,
            &mut segment,
            &input_chunk,
            &mut chunk_state,
        )
        .unwrap();
        assert_eq!(appended, 5);

        // Now gather back
        let mut output_chunk = create_test_chunk(&types, 5);

        // Get row locations from segment
        let row_width = collection.layout().get_row_width();
        let mut row_locations: Vec<*const u8> = Vec::with_capacity(5);

        for chunk in &segment.chunks {
            for part_idx in chunk.part_indices.start()..chunk.part_indices.end() {
                let part = &segment.chunk_parts[part_idx as usize];
                if let Ok(block_ptr) = segment.allocator().pin_chunk_part(&mut pin_state, part) {
                    for row_in_part in 0..part.count {
                        let offset = (row_in_part as usize) * row_width;
                        let row_ptr: *mut u8 = unsafe { block_ptr.add(offset) };
                        row_locations.push(row_ptr as *const u8);
                    }
                }
            }
        }

        gather_chunk(&collection, &row_locations, &mut output_chunk, 5).unwrap();

        // Verify roundtrip
        for i in 0..5 {
            assert_eq!(
                output_chunk.column(0).unwrap().get_i32(i),
                input_chunk.column(0).unwrap().get_i32(i),
                "Mismatch at row {} col 0",
                i
            );
            assert_eq!(
                output_chunk.column(1).unwrap().get_i64(i),
                input_chunk.column(1).unwrap().get_i64(i),
                "Mismatch at row {} col 1",
                i
            );
        }
    }

    #[test]
    fn test_gather_with_selection_vector() {
        let layout = create_test_layout(vec![LogicalType::Integer]);
        let row_width = layout.get_row_width();

        // Create 3 rows with values [10, 20, 30]
        let mut storage: Vec<Vec<u8>> = (0..3).map(|_| vec![0xFFu8; row_width]).collect();
        let offset = layout.get_offsets()[0];

        for (i, val) in [10i32, 20, 30].iter().enumerate() {
            unsafe {
                let ptr = storage[i].as_mut_ptr().add(offset) as *mut i32;
                std::ptr::write_unaligned(ptr, *val);
            }
        }

        let row_locations: Vec<*const u8> = storage.iter().map(|b| b.as_ptr()).collect();

        // Selection: write to positions [2, 0, 1] instead of [0, 1, 2]
        let sel = test_selection(vec![2, 0, 1]);

        let collection = create_test_collection(vec![LogicalType::Integer]);
        let mut chunk = create_test_chunk(&[LogicalType::Integer], 3);

        gather_chunk_with_sel(&collection, &row_locations, &sel, &mut chunk, 3).unwrap();

        // Values should be at positions specified by selection
        // row_locations[0] (10) -> position 2
        // row_locations[1] (20) -> position 0
        // row_locations[2] (30) -> position 1
        assert_eq!(chunk.column(0).unwrap().get_i32(2), Some(10));
        assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(20));
        assert_eq!(chunk.column(0).unwrap().get_i32(1), Some(30));
    }

    #[test]
    fn test_is_row_valid() {
        // Test validity bit checking
        let mut row = vec![0xFFu8; 16]; // All valid initially

        // All bits set = all valid
        assert!(unsafe { is_row_valid(row.as_ptr(), 0) });
        assert!(unsafe { is_row_valid(row.as_ptr(), 7) });

        // Clear bit 3
        row[0] &= !(1 << 3);
        assert!(unsafe { is_row_valid(row.as_ptr(), 0) });
        assert!(!unsafe { is_row_valid(row.as_ptr(), 3) });
        assert!(unsafe { is_row_valid(row.as_ptr(), 7) });

        // Test second byte (columns 8-15)
        row[1] = 0x00; // All invalid
        assert!(!unsafe { is_row_valid(row.as_ptr(), 15) });
    }

    #[test]
    fn test_gather_null_type() {
        // Create 2 rows
        let row_locations: Vec<*const u8> = vec![std::ptr::null(); 2];

        let collection = create_test_collection(vec![LogicalType::Null]);
        let mut chunk = create_test_chunk(&[LogicalType::Null], 2);

        // This used to panic: row_idx 1 out of range for capacity 1
        gather_chunk(&collection, &row_locations, &mut chunk, 2).unwrap();

        assert_eq!(chunk.size(), 2);
        for i in 0..2 {
            assert!(chunk.column(0).unwrap().is_null(i));
        }
    }
}
