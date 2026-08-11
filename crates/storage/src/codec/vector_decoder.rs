// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::codec::{nested_payload_codec, physical_layout};
use crate::rowset::column::{ColumnBatch, StorageDictionaryBatch};
use crate::rowset::encoding::BinaryPlainPageDecoder;
use bytes::Bytes;
use paro_common::allocator::Allocator;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{DictionaryInfo, DictionarySource, SelectionVector, Vector};
use std::sync::Arc;

pub(crate) fn build_vector_from_bytes(
    logical_type: &LogicalType,
    data: &Bytes,
    rows: usize,
    allocator: Arc<dyn Allocator>,
) -> Result<Vector> {
    build_vector_from_bytes_with_utf8_validation(logical_type, data, rows, allocator, true)
}

fn build_vector_from_bytes_with_utf8_validation(
    logical_type: &LogicalType,
    data: &Bytes,
    rows: usize,
    allocator: Arc<dyn Allocator>,
    validate_utf8: bool,
) -> Result<Vector> {
    let mut vector = match logical_type {
        LogicalType::Boolean => build_bool_vector(data, rows, allocator),
        LogicalType::TinyInt => {
            build_fixed_vector::<i8>(LogicalType::TinyInt, data, rows, allocator)
        }
        LogicalType::UTinyInt => {
            build_fixed_vector::<u8>(LogicalType::UTinyInt, data, rows, allocator)
        }
        LogicalType::SmallInt => {
            build_fixed_vector::<i16>(LogicalType::SmallInt, data, rows, allocator)
        }
        LogicalType::USmallInt => {
            build_fixed_vector::<u16>(LogicalType::USmallInt, data, rows, allocator)
        }
        LogicalType::Integer => {
            build_fixed_vector::<i32>(LogicalType::Integer, data, rows, allocator)
        }
        LogicalType::UInteger => {
            build_fixed_vector::<u32>(LogicalType::UInteger, data, rows, allocator)
        }
        LogicalType::BigInt => {
            build_fixed_vector::<i64>(LogicalType::BigInt, data, rows, allocator)
        }
        LogicalType::UBigInt => {
            build_fixed_vector::<u64>(LogicalType::UBigInt, data, rows, allocator)
        }
        LogicalType::HugeInt => {
            build_fixed_vector::<i128>(LogicalType::HugeInt, data, rows, allocator)
        }
        LogicalType::UHugeInt => {
            build_fixed_vector::<u128>(LogicalType::UHugeInt, data, rows, allocator)
        }
        LogicalType::Uuid => build_fixed_vector::<u128>(LogicalType::Uuid, data, rows, allocator),
        LogicalType::Float => build_fixed_vector::<f32>(LogicalType::Float, data, rows, allocator),
        LogicalType::Double => {
            build_fixed_vector::<f64>(LogicalType::Double, data, rows, allocator)
        }
        LogicalType::Date => build_fixed_vector::<i32>(LogicalType::Date, data, rows, allocator),
        LogicalType::Time => build_fixed_vector::<i64>(LogicalType::Time, data, rows, allocator),
        LogicalType::Timestamp => {
            build_fixed_vector::<i64>(LogicalType::Timestamp, data, rows, allocator)
        }
        LogicalType::TimestampTz => {
            build_fixed_vector::<i64>(LogicalType::TimestampTz, data, rows, allocator)
        }
        LogicalType::Interval => {
            build_fixed_vector::<i128>(LogicalType::Interval, data, rows, allocator)
        }
        LogicalType::Decimal { precision, scale } => {
            build_decimal_vector(*precision, *scale, data, rows, allocator)
        }
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb => {
            build_varlen_vector(logical_type, data, rows, allocator, validate_utf8)
        }
        LogicalType::Blob => build_varlen_vector(logical_type, data, rows, allocator, false),
        LogicalType::List(child_type) => {
            build_list_vector(data, rows, child_type.as_ref(), allocator)
        }
        LogicalType::Struct(fields) => build_struct_vector(data, rows, fields, allocator),
        LogicalType::Array(inner, dim) if matches!(**inner, LogicalType::Float) => {
            build_float_array_vector(data, rows, *dim, allocator)
        }
        LogicalType::Null => Vector::try_constant_null(LogicalType::Null, rows, allocator),
        other => Err(paro_error::not_supported(format!(
            "Logical type {:?} not yet supported in vector decoder",
            other
        ))),
    }?;

    if matches!(logical_type, LogicalType::Null) {
        vector.try_set_count(rows)?;
    }

    Ok(vector)
}

pub(crate) fn storage_dictionary_provenance_id(
    rowset_id: u64,
    segment_id: u32,
    column_id: u32,
) -> u64 {
    fn mix(value: u64) -> u64 {
        let mut state = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
        state = (state ^ (state >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        state = (state ^ (state >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        state ^ (state >> 31)
    }

    mix(rowset_id) ^ mix(((segment_id as u64) << 32) | column_id as u64)
}

fn decode_storage_dictionary_batch(
    logical_type: &LogicalType,
    batch: &StorageDictionaryBatch,
    nulls: Option<&[u8]>,
    rows: usize,
    allocator: Arc<dyn Allocator>,
    provenance_id: Option<u64>,
    utf8_verified: bool,
) -> Result<Vector> {
    let mut dictionary_decoder = BinaryPlainPageDecoder::new(batch.dictionary.clone());
    dictionary_decoder.init()?;
    let dictionary_len = dictionary_decoder.count() as usize;
    let has_null_slot = nulls.is_some();
    let unique_len = dictionary_len + usize::from(has_null_slot);

    let mut child = Vector::try_new(logical_type.clone(), unique_len.max(1), allocator.clone())?;
    match logical_type {
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb => {
            for idx in 0..dictionary_len {
                let value = dictionary_decoder
                    .string_at(idx as u32)
                    .ok_or_else(|| paro_error::data_corrupted("dictionary entry missing"))?;
                if !utf8_verified {
                    std::str::from_utf8(&value).map_err(|_| {
                        paro_error::data_corrupted("dictionary entry is not valid UTF-8")
                    })?;
                }
                child.try_set_blob(idx, &value)?;
            }
        }
        LogicalType::Blob => {
            for idx in 0..dictionary_len {
                let value = dictionary_decoder
                    .string_at(idx as u32)
                    .ok_or_else(|| paro_error::data_corrupted("dictionary entry missing"))?;
                child.try_set_blob(idx, &value)?;
            }
        }
        other => {
            let width = physical_layout::fixed_row_width(other).map_err(|_| {
                paro_error::not_supported(format!(
                    "Storage dictionary decode not supported for {other:?}"
                ))
            })?;
            let mut raw = Vec::with_capacity(dictionary_len.saturating_mul(width));
            for idx in 0..dictionary_len {
                let value = dictionary_decoder
                    .string_at(idx as u32)
                    .ok_or_else(|| paro_error::data_corrupted("dictionary entry missing"))?;
                if value.len() != width {
                    return Err(paro_error::data_corrupted(format!(
                        "Dictionary value width {} does not match {other:?} physical width {width}",
                        value.len(),
                    )));
                }
                raw.extend_from_slice(&value);
            }
            let decoded = build_vector_from_bytes(
                other,
                &Bytes::from(raw),
                dictionary_len,
                allocator.clone(),
            )?;
            for idx in 0..dictionary_len {
                child.try_copy_at(idx, &decoded, idx)?;
            }
        }
    }
    if has_null_slot {
        child.try_set_null(dictionary_len, true)?;
    }
    child.try_set_count(unique_len)?;

    if batch.codes.len() % std::mem::size_of::<u32>() != 0 {
        return Err(paro_error::data_corrupted(
            "Storage dictionary codes are not aligned to u32",
        ));
    }
    if batch.codes.len() / std::mem::size_of::<u32>() != rows {
        return Err(paro_error::data_corrupted(
            "Storage dictionary code count does not match row count",
        ));
    }
    if let Some(nulls) = nulls {
        if nulls.len() < rows {
            return Err(paro_error::data_corrupted(
                "Null map shorter than expected row count",
            ));
        }
    }

    let null_index = dictionary_len as u32;
    #[cfg(target_endian = "little")]
    if nulls.is_none() {
        for row_idx in 0..rows {
            let code_offset = row_idx * std::mem::size_of::<u32>();
            let code = u32::from_le_bytes(
                batch.codes[code_offset..code_offset + std::mem::size_of::<u32>()]
                    .try_into()
                    .expect("u32-aligned storage dictionary codes"),
            );
            if code as usize >= dictionary_len {
                return Err(paro_error::data_corrupted(format!(
                    "storage dictionary code {} out of range {}",
                    code, dictionary_len
                )));
            }
        }
        let selection =
            SelectionVector::try_from_native_bytes(batch.codes.clone(), rows, allocator)?;
        // SAFETY: every storage code was checked against `dictionary_len` in
        // the loop immediately above.
        return unsafe {
            Vector::try_with_validated_dictionary(
                Arc::new(child),
                selection,
                DictionaryInfo {
                    unique_len,
                    provenance_id,
                    source: DictionarySource::Storage,
                },
            )
        };
    }

    let mut selection = Vec::with_capacity(rows);
    for row_idx in 0..rows {
        let code_offset = row_idx * std::mem::size_of::<u32>();
        let mut code = u32::from_le_bytes(
            batch.codes[code_offset..code_offset + std::mem::size_of::<u32>()]
                .try_into()
                .expect("u32-aligned storage dictionary codes"),
        );
        if nulls.is_some_and(|flags| flags[row_idx] != 0) {
            code = null_index;
        } else if code as usize >= dictionary_len {
            return Err(paro_error::data_corrupted(format!(
                "storage dictionary code {} out of range {}",
                code, dictionary_len
            )));
        }
        selection.push(code);
    }

    let selection = SelectionVector::try_from_indices(selection, allocator)?;
    // SAFETY: non-null codes were checked against `dictionary_len`; null rows
    // map to the appended null slot at `dictionary_len < unique_len`.
    unsafe {
        Vector::try_with_validated_dictionary(
            Arc::new(child),
            selection,
            DictionaryInfo {
                unique_len,
                provenance_id,
                source: DictionarySource::Storage,
            },
        )
    }
}

pub(crate) fn decode_column_batch(
    logical_type: &LogicalType,
    batch: &ColumnBatch,
    rows: usize,
    allocator: Arc<dyn Allocator>,
    storage_provenance_id: Option<u64>,
) -> Result<Vector> {
    if let Some(storage_dictionary) = &batch.storage_dictionary {
        return decode_storage_dictionary_batch(
            logical_type,
            storage_dictionary,
            batch.nulls.as_deref(),
            rows,
            allocator,
            storage_provenance_id,
            batch.has_verified_utf8(),
        );
    }

    let mut vector = build_vector_from_bytes_with_utf8_validation(
        logical_type,
        &batch.data,
        rows,
        allocator,
        !batch.has_verified_utf8(),
    )?;
    if !matches!(logical_type, LogicalType::Null) {
        if let Some(nulls) = batch.nulls.as_deref() {
            apply_nulls(&mut vector, nulls, rows)?;
        }
    }
    Ok(vector)
}

pub(crate) fn infer_batch_row_count(
    logical_type: &LogicalType,
    data: &Bytes,
    expected: usize,
) -> Result<usize> {
    if expected == 0 {
        return Ok(0);
    }

    match logical_type {
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::Blob
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb
        | LogicalType::List(_)
        | LogicalType::Struct(_) => count_varlen_values(data),
        _ => {
            let row_width = physical_layout::fixed_row_width(logical_type)?;
            if row_width == 0 || data.len() % row_width != 0 {
                return Err(paro_error::data_corrupted(
                    "Column data length is not aligned with fixed-width type",
                ));
            }
            Ok(data.len() / row_width)
        }
    }
}

pub(crate) fn apply_nulls(vector: &mut Vector, nulls: &[u8], rows: usize) -> Result<()> {
    if nulls.len() < rows {
        return Err(paro_error::data_corrupted(
            "Null map shorter than expected row count",
        ));
    }
    for (idx, &null_value) in nulls.iter().enumerate().take(rows) {
        if null_value != 0 {
            vector.try_set_null(idx, true)?;
        }
    }
    Ok(())
}

pub(crate) fn decode_payload_value(logical_type: &LogicalType, payload: &[u8]) -> Result<Value> {
    match logical_type {
        LogicalType::List(child_type) => Ok(Value::List(
            decode_list_payload(child_type, payload)?,
            child_type.as_ref().clone(),
        )),
        LogicalType::Struct(fields) => Ok(Value::Struct(
            decode_struct_payload(fields, payload)?,
            fields.clone(),
        )),
        LogicalType::Array(inner, dim) if matches!(**inner, LogicalType::Float) => {
            let expected = dim
                .checked_mul(std::mem::size_of::<f32>())
                .ok_or_else(|| paro_error::data_corrupted("Array decode: width overflow"))?;
            if payload.len() != expected {
                return Err(paro_error::data_corrupted(
                    "Array(Float) payload has unexpected width",
                ));
            }
            let values = payload
                .chunks_exact(4)
                .map(|chunk| {
                    Value::Float(f32::from_le_bytes(
                        chunk.try_into().expect("chunk size checked"),
                    ))
                })
                .collect();
            Ok(Value::Array(values, LogicalType::Float, *dim))
        }
        _ => nested_payload_codec::decode_nested_element(logical_type, payload),
    }
}

fn build_fixed_vector<T>(
    logical_type: LogicalType,
    data: &Bytes,
    rows: usize,
    allocator: Arc<dyn Allocator>,
) -> Result<Vector>
where
    T: Default + Copy + FromPrimitiveLe,
{
    let expected_bytes = rows
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| paro_error::data_corrupted("Column data length overflow"))?;
    if data.len() != expected_bytes {
        return Err(paro_error::data_corrupted(
            "Column data length does not match expected rows",
        ));
    }
    #[cfg(target_endian = "little")]
    {
        Vector::try_from_fixed_width_bytes(logical_type, rows, data.clone(), allocator)
    }
    #[cfg(target_endian = "big")]
    {
        let values = parse_primitive::<T>(data, rows)?;
        let mut vector = Vector::try_new(logical_type, rows, allocator)?;
        unsafe {
            std::ptr::copy_nonoverlapping(values.as_ptr(), vector.flat_data_mut::<T>(), rows);
        }
        vector.try_set_count(rows)?;
        Ok(vector)
    }
}

fn build_varlen_vector(
    logical_type: &LogicalType,
    data: &Bytes,
    rows: usize,
    allocator: Arc<dyn Allocator>,
    validate_utf8: bool,
) -> Result<Vector> {
    let mut vector = Vector::try_new(logical_type.clone(), rows, allocator)?;
    let (entries, _validity, heap) = vector.begin_varlen_write(rows);
    let mut offset = 0usize;
    for row_idx in 0..rows {
        let length_end = offset
            .checked_add(std::mem::size_of::<u32>())
            .ok_or_else(|| paro_error::data_corrupted("Variable-length column offset overflow"))?;
        let length_bytes = data.get(offset..length_end).ok_or_else(|| {
            paro_error::data_corrupted("Variable-length column length prefix is truncated")
        })?;
        let length = u32::from_le_bytes(
            length_bytes
                .try_into()
                .expect("varlen length slice was checked"),
        ) as usize;
        let value_end = length_end.checked_add(length).ok_or_else(|| {
            paro_error::data_corrupted("Variable-length column value offset overflow")
        })?;
        let value = data
            .get(length_end..value_end)
            .ok_or_else(|| paro_error::data_corrupted("Variable-length value exceeds buffer"))?;
        if validate_utf8 {
            std::str::from_utf8(value)
                .map_err(|_| paro_error::data_corrupted("Invalid UTF-8 in string column"))?;
        }
        let entry = heap.try_add_blob(value)?;
        // SAFETY: `begin_varlen_write(rows)` returns an `InlineString` array
        // with exactly `rows` writable entries and `row_idx < rows`.
        unsafe { entries.add(row_idx).write(entry) };
        offset = value_end;
    }
    if offset != data.len() {
        return Err(paro_error::data_corrupted(
            "Variable-length column contains trailing bytes",
        ));
    }
    Ok(vector)
}

fn build_bool_vector(data: &Bytes, rows: usize, allocator: Arc<dyn Allocator>) -> Result<Vector> {
    let values = parse_bool(data, rows)?;
    let mut vector = Vector::try_new(LogicalType::Boolean, rows, allocator)?;
    if rows > 0 {
        unsafe {
            std::ptr::copy_nonoverlapping(values.as_ptr(), vector.flat_data_mut::<bool>(), rows);
        }
    }
    vector.try_set_count(rows)?;
    Ok(vector)
}

fn build_decimal_vector(
    precision: u8,
    scale: u8,
    data: &Bytes,
    rows: usize,
    allocator: Arc<dyn Allocator>,
) -> Result<Vector> {
    let decimal_type = LogicalType::Decimal { precision, scale };
    match physical_layout::decimal_storage_width(precision) {
        8 => build_fixed_vector::<i64>(decimal_type, data, rows, allocator),
        16 => build_fixed_vector::<i128>(decimal_type, data, rows, allocator),
        other => Err(paro_error::data_corrupted(format!(
            "Unexpected decimal storage width {}",
            other
        ))),
    }
}

fn build_float_array_vector(
    data: &Bytes,
    rows: usize,
    dim: usize,
    allocator: Arc<dyn Allocator>,
) -> Result<Vector> {
    if dim == 0 {
        return Err(paro_error::invalid_input(
            "Array(Float, 0) is not supported in vector decoder",
        ));
    }
    let total = rows
        .checked_mul(dim)
        .ok_or_else(|| paro_error::data_corrupted("Array element count overflow"))?;
    let values = parse_primitive::<f32>(data, total)?;
    let child = Arc::new(Vector::try_from_f32(&values, allocator)?);
    let mut vector = Vector::try_from_array(LogicalType::Float, child, rows, dim)?;
    vector.try_set_count(rows)?;
    Ok(vector)
}

fn build_list_vector(
    data: &Bytes,
    rows: usize,
    child_type: &LogicalType,
    allocator: Arc<dyn Allocator>,
) -> Result<Vector> {
    let payloads = parse_varlen_values(data, rows)?;
    let mut offsets = Vec::with_capacity(rows);
    let mut lengths = Vec::with_capacity(rows);
    let mut flat_values: Vec<Value> = Vec::new();

    for payload in payloads {
        let values = decode_list_payload(child_type, &payload)?;
        offsets.push(flat_values.len());
        lengths.push(values.len());
        flat_values.extend(values);
    }

    let mut child_vec = Vector::try_new(
        child_type.clone(),
        flat_values.len().max(1),
        allocator.clone(),
    )?;
    for (idx, value) in flat_values.iter().enumerate() {
        child_vec.set_value(idx, value);
    }
    child_vec.try_set_count(flat_values.len())?;

    let mut list_vec = Vector::try_new(
        LogicalType::List(Box::new(child_type.clone())),
        rows,
        allocator,
    )?;
    list_vec.set_child(Arc::new(child_vec));
    list_vec.try_set_count(rows)?;

    let entries = unsafe { list_vec.flat_data_mut::<u8>() };
    for row in 0..rows {
        let entry_ptr = unsafe { entries.add(row * 8) as *mut u32 };
        unsafe {
            std::ptr::write_unaligned(entry_ptr, offsets[row] as u32);
            std::ptr::write_unaligned(entry_ptr.add(1), lengths[row] as u32);
        }
    }

    Ok(list_vec)
}

fn build_struct_vector(
    data: &Bytes,
    rows: usize,
    fields: &[(String, LogicalType)],
    allocator: Arc<dyn Allocator>,
) -> Result<Vector> {
    let payloads = parse_varlen_values(data, rows)?;
    let field_count = fields.len();
    let mut column_values: Vec<Vec<Value>> =
        (0..field_count).map(|_| Vec::with_capacity(rows)).collect();

    for payload in payloads {
        let values = decode_struct_payload(fields, &payload)?;
        if values.len() != field_count {
            return Err(paro_error::data_corrupted(
                "Struct payload field count mismatch",
            ));
        }
        for (idx, value) in values.into_iter().enumerate() {
            column_values[idx].push(value);
        }
    }

    let mut struct_vec = Vector::try_new(
        LogicalType::Struct(fields.to_vec()),
        rows,
        allocator.clone(),
    )?;
    struct_vec.try_set_count(rows)?;
    let children = struct_vec
        .children_mut()
        .ok_or_else(|| paro_error::internal("Struct vector missing children during decode"))?;

    if children.len() != field_count {
        return Err(paro_error::data_corrupted(
            "Struct vector child count mismatch",
        ));
    }

    for (idx, values) in column_values.into_iter().enumerate() {
        let child = Arc::make_mut(&mut children[idx]);
        for (row, value) in values.iter().enumerate() {
            child.set_value(row, value);
        }
        child.try_set_count(rows)?;
    }

    Ok(struct_vec)
}

pub(crate) fn decode_list_payload(child_type: &LogicalType, payload: &[u8]) -> Result<Vec<Value>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    if payload.len() < 4 {
        return Err(paro_error::data_corrupted(
            "List payload missing element count",
        ));
    }
    let count = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;
    let null_bytes_len = count.div_ceil(8);
    if payload.len() < 4 + null_bytes_len {
        return Err(paro_error::data_corrupted(
            "List payload missing null bitmap",
        ));
    }
    let nulls = &payload[4..4 + null_bytes_len];
    let mut offset = 4 + null_bytes_len;
    let mut values = Vec::with_capacity(count);

    for idx in 0..count {
        let is_null = (nulls[idx / 8] >> (idx % 8)) & 1 == 1;
        if physical_layout::list_child_is_varlen(child_type) {
            if offset + 4 > payload.len() {
                return Err(paro_error::data_corrupted(
                    "List payload missing varlen length",
                ));
            }
            let len = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            if offset + len > payload.len() {
                return Err(paro_error::data_corrupted(
                    "List payload varlen value exceeds buffer",
                ));
            }
            let bytes = &payload[offset..offset + len];
            offset += len;
            if is_null {
                values.push(Value::Null(child_type.clone()));
            } else {
                values.push(decode_payload_value(child_type, bytes)?);
            }
        } else {
            let size = physical_layout::list_child_fixed_size(child_type)?;
            if offset + size > payload.len() {
                return Err(paro_error::data_corrupted(
                    "List payload fixed-width value exceeds buffer",
                ));
            }
            let bytes = &payload[offset..offset + size];
            offset += size;
            if is_null {
                values.push(Value::Null(child_type.clone()));
            } else {
                values.push(decode_payload_value(child_type, bytes)?);
            }
        }
    }

    if offset != payload.len() {
        return Err(paro_error::data_corrupted(
            "List payload contains trailing bytes",
        ));
    }

    Ok(values)
}

pub(crate) fn decode_struct_payload(
    fields: &[(String, LogicalType)],
    payload: &[u8],
) -> Result<Vec<Value>> {
    if payload.is_empty() {
        return Ok(fields
            .iter()
            .map(|(_, ty)| Value::Null(ty.clone()))
            .collect());
    }
    if payload.len() < 4 {
        return Err(paro_error::data_corrupted(
            "Struct payload missing field count",
        ));
    }
    let count = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;
    if count != fields.len() {
        return Err(paro_error::data_corrupted(
            "Struct payload field count mismatch",
        ));
    }
    let null_bytes_len = count.div_ceil(8);
    if payload.len() < 4 + null_bytes_len {
        return Err(paro_error::data_corrupted(
            "Struct payload missing null bitmap",
        ));
    }
    let nulls = &payload[4..4 + null_bytes_len];
    let mut offset = 4 + null_bytes_len;
    let mut values = Vec::with_capacity(count);

    for idx in 0..count {
        let field_type = &fields[idx].1;
        let is_null = (nulls[idx / 8] >> (idx % 8)) & 1 == 1;
        if physical_layout::struct_field_is_varlen(field_type) {
            if offset + 4 > payload.len() {
                return Err(paro_error::data_corrupted(
                    "Struct payload missing varlen length",
                ));
            }
            let len = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            if offset + len > payload.len() {
                return Err(paro_error::data_corrupted(
                    "Struct payload varlen value exceeds buffer",
                ));
            }
            let bytes = &payload[offset..offset + len];
            offset += len;
            if is_null {
                values.push(Value::Null(field_type.clone()));
            } else {
                values.push(decode_payload_value(field_type, bytes)?);
            }
        } else {
            let size = physical_layout::struct_field_fixed_size(field_type)?;
            if offset + size > payload.len() {
                return Err(paro_error::data_corrupted(
                    "Struct payload fixed-width value exceeds buffer",
                ));
            }
            let bytes = &payload[offset..offset + size];
            offset += size;
            if is_null {
                values.push(Value::Null(field_type.clone()));
            } else {
                values.push(decode_payload_value(field_type, bytes)?);
            }
        }
    }

    if offset != payload.len() {
        return Err(paro_error::data_corrupted(
            "Struct payload contains trailing bytes",
        ));
    }

    Ok(values)
}

pub(crate) fn parse_varlen_values(data: &Bytes, rows: usize) -> Result<Vec<Vec<u8>>> {
    let mut offset = 0usize;
    let bytes = data.as_ref();
    let mut values = Vec::with_capacity(rows);
    for _ in 0..rows {
        if offset + 4 > bytes.len() {
            return Err(paro_error::data_corrupted("Invalid length prefix"));
        }
        let len = u32::from_le_bytes(
            bytes[offset..offset + 4]
                .try_into()
                .map_err(|_| paro_error::data_corrupted("Invalid length prefix"))?,
        ) as usize;
        offset += 4;
        if offset + len > bytes.len() {
            return Err(paro_error::data_corrupted(
                "Variable-length value exceeds buffer",
            ));
        }
        values.push(bytes[offset..offset + len].to_vec());
        offset += len;
    }
    if offset != bytes.len() {
        return Err(paro_error::data_corrupted(
            "Variable-length column contains trailing bytes",
        ));
    }
    Ok(values)
}

fn count_varlen_values(data: &Bytes) -> Result<usize> {
    let mut offset = 0usize;
    let bytes = data.as_ref();
    let mut count = 0usize;

    while offset < bytes.len() {
        if offset + 4 > bytes.len() {
            return Err(paro_error::data_corrupted("Invalid length prefix"));
        }
        let len = u32::from_le_bytes(
            bytes[offset..offset + 4]
                .try_into()
                .map_err(|_| paro_error::data_corrupted("Invalid length prefix"))?,
        ) as usize;
        offset += 4;
        if offset + len > bytes.len() {
            return Err(paro_error::data_corrupted(
                "Variable-length value out of bounds",
            ));
        }
        offset += len;
        count += 1;
    }

    Ok(count)
}

fn parse_primitive<T>(data: &Bytes, rows: usize) -> Result<Vec<T>>
where
    T: Default + Copy + FromPrimitiveLe,
{
    let elem_size = std::mem::size_of::<T>();
    if data.len() != rows * elem_size {
        return Err(paro_error::data_corrupted(
            "Column data length does not match expected rows",
        ));
    }

    let mut result = Vec::with_capacity(rows);
    let bytes = data.as_ref();
    for i in 0..rows {
        let start = i * elem_size;
        result.push(T::from_le_bytes(&bytes[start..start + elem_size]));
    }
    Ok(result)
}

fn parse_bool(data: &Bytes, rows: usize) -> Result<Vec<bool>> {
    if data.len() != rows {
        return Err(paro_error::data_corrupted(
            "Boolean column length does not match expected rows",
        ));
    }
    Ok(data.iter().map(|b| *b != 0).collect())
}

trait FromPrimitiveLe: Sized + Copy {
    fn from_le_bytes(bytes: &[u8]) -> Self;
}

impl FromPrimitiveLe for i64 {
    fn from_le_bytes(bytes: &[u8]) -> Self {
        let arr: [u8; 8] = bytes.try_into().expect("slice with incorrect length");
        i64::from_le_bytes(arr)
    }
}

impl FromPrimitiveLe for u64 {
    fn from_le_bytes(bytes: &[u8]) -> Self {
        let arr: [u8; 8] = bytes.try_into().expect("slice with incorrect length");
        u64::from_le_bytes(arr)
    }
}

impl FromPrimitiveLe for i32 {
    fn from_le_bytes(bytes: &[u8]) -> Self {
        let arr: [u8; 4] = bytes.try_into().expect("slice with incorrect length");
        i32::from_le_bytes(arr)
    }
}

impl FromPrimitiveLe for u32 {
    fn from_le_bytes(bytes: &[u8]) -> Self {
        let arr: [u8; 4] = bytes.try_into().expect("slice with incorrect length");
        u32::from_le_bytes(arr)
    }
}

impl FromPrimitiveLe for i16 {
    fn from_le_bytes(bytes: &[u8]) -> Self {
        let arr: [u8; 2] = bytes.try_into().expect("slice with incorrect length");
        i16::from_le_bytes(arr)
    }
}

impl FromPrimitiveLe for u16 {
    fn from_le_bytes(bytes: &[u8]) -> Self {
        let arr: [u8; 2] = bytes.try_into().expect("slice with incorrect length");
        u16::from_le_bytes(arr)
    }
}

impl FromPrimitiveLe for i8 {
    fn from_le_bytes(bytes: &[u8]) -> Self {
        bytes[0] as i8
    }
}

impl FromPrimitiveLe for u8 {
    fn from_le_bytes(bytes: &[u8]) -> Self {
        bytes[0]
    }
}

impl FromPrimitiveLe for i128 {
    fn from_le_bytes(bytes: &[u8]) -> Self {
        let arr: [u8; 16] = bytes.try_into().expect("slice with incorrect length");
        i128::from_le_bytes(arr)
    }
}

impl FromPrimitiveLe for u128 {
    fn from_le_bytes(bytes: &[u8]) -> Self {
        let arr: [u8; 16] = bytes.try_into().expect("slice with incorrect length");
        u128::from_le_bytes(arr)
    }
}

impl FromPrimitiveLe for f64 {
    fn from_le_bytes(bytes: &[u8]) -> Self {
        let arr: [u8; 8] = bytes.try_into().expect("slice with incorrect length");
        f64::from_le_bytes(arr)
    }
}

impl FromPrimitiveLe for f32 {
    fn from_le_bytes(bytes: &[u8]) -> Self {
        let arr: [u8; 4] = bytes.try_into().expect("slice with incorrect length");
        f32::from_le_bytes(arr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::allocator::default_allocator;

    fn encode_varlen(values: &[&[u8]]) -> Bytes {
        let mut encoded = Vec::new();
        for value in values {
            encoded.extend_from_slice(&(value.len() as u32).to_le_bytes());
            encoded.extend_from_slice(value);
        }
        Bytes::from(encoded)
    }

    #[test]
    fn varlen_vector_decodes_directly_from_canonical_rows() {
        let allocator: Arc<dyn Allocator> = Arc::new(default_allocator());
        let encoded = encode_varlen(&[b"short", b"a value longer than twelve bytes"]);

        let vector = build_vector_from_bytes(&LogicalType::Varchar, &encoded, 2, allocator)
            .expect("valid varlen column");

        assert_eq!(vector.get_string(0), Some("short"));
        assert_eq!(
            vector.get_string(1),
            Some("a value longer than twelve bytes")
        );
    }

    #[test]
    fn varchar_decoder_rejects_invalid_utf8_while_blob_preserves_it() {
        let allocator: Arc<dyn Allocator> = Arc::new(default_allocator());
        let encoded = encode_varlen(&[&[0xff, 0xfe]]);

        assert!(
            build_vector_from_bytes(&LogicalType::Varchar, &encoded, 1, allocator.clone()).is_err()
        );
        let blob = build_vector_from_bytes(&LogicalType::Blob, &encoded, 1, allocator)
            .expect("blob bytes need not be UTF-8");
        assert_eq!(blob.get_blob(0), Some([0xff, 0xfe].as_slice()));
    }
}
