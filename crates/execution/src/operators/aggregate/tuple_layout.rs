// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Tuple layout for grouped aggregate hash table rows.

use std::mem::size_of;
use std::str;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{AccountedVec, MemoryAccountingClass, MemoryAccountingContext};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use super::aggregate_object::AggregateObject;
use super::aggregate_state::AggregateStateLayout;

const MIN_ALIGNMENT: usize = 8;

/// Row-local reference to variable-length bytes in [`VarlenHeap`].
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VarlenRef {
    meta: u32,
    payload: [u8; 12],
}

impl VarlenRef {
    const INLINE_TAG: u32 = 1u32 << 31;
    const LEN_MASK: u32 = Self::INLINE_TAG - 1;
    const INLINE_CAPACITY: usize = 12;

    pub fn inline_capacity() -> usize {
        Self::INLINE_CAPACITY
    }

    pub fn from_inline(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > Self::INLINE_CAPACITY {
            return Err(paro_error::internal(format!(
                "Inline varlen bytes exceed capacity: len={} cap={}",
                bytes.len(),
                Self::INLINE_CAPACITY
            )));
        }
        let len = u32::try_from(bytes.len()).map_err(|_| {
            paro_error::internal(format!(
                "Inline varlen byte length overflow: len={}",
                bytes.len()
            ))
        })?;
        let mut payload = [0u8; Self::INLINE_CAPACITY];
        payload[..bytes.len()].copy_from_slice(bytes);
        Ok(Self {
            meta: Self::INLINE_TAG | len,
            payload,
        })
    }

    pub fn from_heap(offset: usize, len: usize) -> Result<Self> {
        let len_u32 = u32::try_from(len)
            .map_err(|_| paro_error::internal(format!("Varlen heap length overflow: len={len}")))?;
        if len_u32 > Self::LEN_MASK {
            return Err(paro_error::internal(format!(
                "Varlen heap length exceeds representable range: len={len}"
            )));
        }
        let offset_u64 = u64::try_from(offset).map_err(|_| {
            paro_error::internal(format!("Varlen heap offset overflow: offset={offset}"))
        })?;
        let mut payload = [0u8; Self::INLINE_CAPACITY];
        payload[..8].copy_from_slice(&offset_u64.to_le_bytes());
        Ok(Self {
            meta: len_u32,
            payload,
        })
    }

    pub fn is_inline(&self) -> bool {
        (self.meta & Self::INLINE_TAG) != 0
    }

    pub fn len(&self) -> usize {
        (self.meta & Self::LEN_MASK) as usize
    }

    pub fn inline_bytes(&self) -> Option<&[u8]> {
        if !self.is_inline() {
            return None;
        }
        Some(&self.payload[..self.len()])
    }

    pub fn heap_offset(&self) -> Option<usize> {
        if self.is_inline() {
            return None;
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&self.payload[..8]);
        usize::try_from(u64::from_le_bytes(bytes)).ok()
    }
}

/// Contiguous out-of-line storage for variable-length group keys.
#[derive(Debug)]
pub struct VarlenHeap {
    data: AccountedVec<u8>,
}

impl Default for VarlenHeap {
    fn default() -> Self {
        Self::new()
    }
}

impl VarlenHeap {
    pub fn new() -> Self {
        Self::new_with_memory(MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        ))
    }

    pub fn new_with_memory(memory: MemoryAccountingContext) -> Self {
        Self {
            data: memory
                .grant()
                .map(|grant| {
                    AccountedVec::new_with_accounting(
                        grant,
                        memory.tag(),
                        memory.accounting_class(),
                    )
                })
                .unwrap_or_else(|_| {
                    AccountedVec::new_with_accounting(
                        paro_common::memory::MemoryGrant::detached(usize::MAX / 4, memory.domain()),
                        memory.tag(),
                        memory.accounting_class(),
                    )
                }),
        }
    }

    pub fn reset(&mut self) {
        self.data.clear();
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn append(&mut self, bytes: &[u8]) -> Result<usize> {
        let offset = self.data.len();
        let len = bytes.len();
        let _ = offset.checked_add(len).ok_or_else(|| {
            paro_error::internal(format!(
                "VarlenHeap overflow when appending bytes: offset={offset}, len={len}"
            ))
        })?;
        self.data.try_extend_from_slice(bytes)?;
        Ok(offset)
    }

    pub fn get(&self, offset: usize, len: usize) -> Result<&[u8]> {
        let end = offset.checked_add(len).ok_or_else(|| {
            paro_error::internal(format!(
                "VarlenHeap reference overflow: offset={offset}, len={len}"
            ))
        })?;
        if end > self.data.len() {
            return Err(paro_error::internal(format!(
                "VarlenHeap reference out of bounds: end={end}, heap_size={}",
                self.data.len()
            )));
        }
        Ok(&self.data.as_slice()[offset..end])
    }
}

/// Aggregate hash table row layout:
///
/// `[validity bytes][group columns][aggregate states][hash]`
#[derive(Debug, Clone)]
pub struct TupleLayout {
    pub group_types: Vec<LogicalType>,
    pub group_offsets: Vec<usize>,
    pub agg_state_offset: usize,
    pub agg_offsets: Vec<usize>,
    pub hash_offset: usize,
    pub row_width: usize,
    validity_width: usize,
    varlen_groups: Vec<bool>,
}

impl TupleLayout {
    /// Build row layout from group key types and aggregate objects.
    pub fn build(
        group_types: &[LogicalType],
        aggregate_objects: &[AggregateObject],
    ) -> Result<Self> {
        let validity_width = validity_mask_size(group_types.len());
        let mut current_offset = validity_width;

        let mut group_offsets = Vec::with_capacity(group_types.len());
        let mut varlen_groups = Vec::with_capacity(group_types.len());
        for group_type in group_types {
            let width = group_storage_width(group_type)?;
            let alignment = group_alignment(group_type)?;
            current_offset = align_to(current_offset, alignment)?;
            group_offsets.push(current_offset);
            current_offset = current_offset.checked_add(width).ok_or_else(|| {
                paro_error::internal(format!(
                    "TupleLayout overflow building groups: offset={current_offset}, width={width}"
                ))
            })?;
            varlen_groups.push(is_varlen_group_type(group_type));
        }

        let agg_state_offset = align_to(current_offset, MIN_ALIGNMENT)?;
        let aggregate_state_layout = AggregateStateLayout::new(aggregate_objects)?;
        let agg_offsets = (0..aggregate_state_layout.aggregate_count())
            .map(|idx| aggregate_state_layout.state_offset(idx))
            .collect::<Vec<_>>();

        let after_aggs = agg_state_offset
            .checked_add(aggregate_state_layout.total_size())
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "TupleLayout aggregate section overflow: offset={agg_state_offset}, width={}",
                    aggregate_state_layout.total_size()
                ))
            })?;
        let hash_offset = align_to(after_aggs, MIN_ALIGNMENT)?;
        let row_width = align_to(hash_offset + size_of::<u64>(), MIN_ALIGNMENT)?;

        Ok(Self {
            group_types: group_types.to_vec(),
            group_offsets,
            agg_state_offset,
            agg_offsets,
            hash_offset,
            row_width,
            validity_width,
            varlen_groups,
        })
    }

    pub fn group_count(&self) -> usize {
        self.group_types.len()
    }

    pub fn validity_width(&self) -> usize {
        self.validity_width
    }

    /// Write group key values from `groups[row_idx]` into one row.
    ///
    /// - Fixed-width types are inlined into the row.
    /// - Variable-length types write a `(offset,len)` ref and append bytes into `varlen_heap`.
    pub fn scatter_groups(
        &self,
        row_ptr: *mut u8,
        groups: &Chunk,
        row_idx: usize,
        varlen_heap: &mut VarlenHeap,
    ) -> Result<()> {
        if groups.column_count() < self.group_count() {
            return Err(paro_error::internal(format!(
                "Insufficient group columns: required={}, actual={}",
                self.group_count(),
                groups.column_count()
            )));
        }
        if row_idx >= groups.size() {
            return Err(paro_error::internal(format!(
                "Group row index out of bounds: row_idx={row_idx}, rows={}",
                groups.size()
            )));
        }

        if self.validity_width > 0 {
            unsafe {
                std::ptr::write_bytes(row_ptr, 0, self.validity_width);
            }
        }

        for group_idx in 0..self.group_count() {
            let group_type = &self.group_types[group_idx];
            let group_column = groups.column(group_idx).ok_or_else(|| {
                paro_error::internal(format!("Group column not found at index {group_idx}"))
            })?;
            let is_valid = !group_column.is_null(row_idx);
            set_validity(row_ptr, group_idx, is_valid);

            let column_offset = self.group_offsets[group_idx];
            let target = unsafe { row_ptr.add(column_offset) };
            if !is_valid {
                unsafe {
                    std::ptr::write_bytes(target, 0, group_storage_width(group_type)?);
                }
                continue;
            }

            if self.varlen_groups[group_idx] {
                let bytes = varlen_bytes(group_column.as_ref(), row_idx, group_type)?;
                let varlen_ref = if bytes.len() <= VarlenRef::inline_capacity() {
                    VarlenRef::from_inline(bytes)?
                } else {
                    let offset = varlen_heap.append(bytes)?;
                    VarlenRef::from_heap(offset, bytes.len())?
                };
                unsafe {
                    std::ptr::write_unaligned(target as *mut VarlenRef, varlen_ref);
                }
            } else {
                write_fixed_group_value(target, group_column.as_ref(), row_idx, group_type)?;
            }
        }
        Ok(())
    }

    /// Deserialize one group value from a row.
    pub fn deserialize_group_value(
        &self,
        row_ptr: *const u8,
        group_idx: usize,
        varlen_heap: &VarlenHeap,
    ) -> Result<Value> {
        let group_type = self.group_types.get(group_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "Group index out of bounds in TupleLayout: idx={group_idx}, count={}",
                self.group_count()
            ))
        })?;
        if !row_is_valid(row_ptr, group_idx) {
            return Ok(Value::Null(group_type.clone()));
        }

        let value_ptr = unsafe { row_ptr.add(self.group_offsets[group_idx]) };
        if self.varlen_groups[group_idx] {
            let varlen_ref = unsafe { std::ptr::read_unaligned(value_ptr as *const VarlenRef) };
            let bytes = read_varlen_ref_bytes(&varlen_ref, varlen_heap)?;
            deserialize_varlen_value(group_type, bytes)
        } else {
            read_fixed_group_value(value_ptr, group_type)
        }
    }

    /// Deserialize full group key from one row.
    pub fn deserialize_group_key(
        &self,
        row_ptr: *const u8,
        varlen_heap: &VarlenHeap,
    ) -> Result<Vec<Value>> {
        let mut result = Vec::with_capacity(self.group_count());
        for idx in 0..self.group_count() {
            result.push(self.deserialize_group_value(row_ptr, idx, varlen_heap)?);
        }
        Ok(result)
    }

    /// Compare one serialized group key row with `groups[row_idx]`.
    pub fn compare_groups(
        &self,
        row_ptr: *const u8,
        groups: &Chunk,
        row_idx: usize,
        varlen_heap: &VarlenHeap,
    ) -> Result<bool> {
        if groups.column_count() < self.group_count() {
            return Err(paro_error::internal(format!(
                "Insufficient group columns for compare: required={}, actual={}",
                self.group_count(),
                groups.column_count()
            )));
        }
        if row_idx >= groups.size() {
            return Err(paro_error::internal(format!(
                "Group row index out of bounds in compare: row_idx={row_idx}, rows={}",
                groups.size()
            )));
        }

        for group_idx in 0..self.group_count() {
            let column = groups.column(group_idx).ok_or_else(|| {
                paro_error::internal(format!("Group column not found at index {group_idx}"))
            })?;
            let row_valid = row_is_valid(row_ptr, group_idx);
            let source_valid = !column.is_null(row_idx);
            if row_valid != source_valid {
                return Ok(false);
            }
            if !row_valid {
                continue;
            }

            let source_ptr = unsafe { row_ptr.add(self.group_offsets[group_idx]) };
            let group_type = &self.group_types[group_idx];
            if self.varlen_groups[group_idx] {
                let varlen_ref =
                    unsafe { std::ptr::read_unaligned(source_ptr as *const VarlenRef) };
                let left = read_varlen_ref_bytes(&varlen_ref, varlen_heap)?;
                let right = varlen_bytes(column.as_ref(), row_idx, group_type)?;
                if left != right {
                    return Ok(false);
                }
            } else if !fixed_group_value_equals(source_ptr, column.as_ref(), row_idx, group_type)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn store_hash(&self, row_ptr: *mut u8, hash: u64) {
        unsafe {
            std::ptr::write_unaligned(row_ptr.add(self.hash_offset) as *mut u64, hash);
        }
    }

    pub fn load_hash(&self, row_ptr: *const u8) -> u64 {
        unsafe { std::ptr::read_unaligned(row_ptr.add(self.hash_offset) as *const u64) }
    }
}

fn row_is_valid(row_ptr: *const u8, col_idx: usize) -> bool {
    let entry_idx = col_idx / 8;
    let bit_idx = col_idx % 8;
    unsafe { ((*row_ptr.add(entry_idx)) & (1u8 << bit_idx)) != 0 }
}

fn set_validity(row_ptr: *mut u8, col_idx: usize, valid: bool) {
    if !valid {
        return;
    }
    let entry_idx = col_idx / 8;
    let bit_idx = col_idx % 8;
    unsafe {
        let byte_ptr = row_ptr.add(entry_idx);
        *byte_ptr |= 1u8 << bit_idx;
    }
}

fn validity_mask_size(column_count: usize) -> usize {
    if column_count == 0 {
        0
    } else {
        column_count.div_ceil(8)
    }
}

fn is_varlen_group_type(logical_type: &LogicalType) -> bool {
    matches!(
        logical_type,
        LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::Blob
    )
}

fn group_storage_width(logical_type: &LogicalType) -> Result<usize> {
    if is_varlen_group_type(logical_type) {
        return Ok(size_of::<VarlenRef>());
    }
    let width = logical_type.physical_size();
    if width == 0 {
        return Err(paro_error::internal(format!(
            "Unsupported group key type in TupleLayout: {logical_type:?}"
        )));
    }
    Ok(width)
}

fn group_alignment(logical_type: &LogicalType) -> Result<usize> {
    if is_varlen_group_type(logical_type) {
        return Ok(MIN_ALIGNMENT);
    }
    let width = group_storage_width(logical_type)?;
    Ok(width.min(MIN_ALIGNMENT))
}

fn align_to(value: usize, alignment: usize) -> Result<usize> {
    if alignment == 0 || !alignment.is_power_of_two() {
        return Err(paro_error::internal(format!(
            "Invalid TupleLayout alignment: {alignment}"
        )));
    }
    let addend = alignment - 1;
    value
        .checked_add(addend)
        .map(|aligned| aligned & !addend)
        .ok_or_else(|| {
            paro_error::internal(format!(
                "TupleLayout alignment overflow: value={value}, alignment={alignment}"
            ))
        })
}

fn varlen_bytes<'a>(
    column: &'a Vector,
    row_idx: usize,
    logical_type: &LogicalType,
) -> Result<&'a [u8]> {
    match logical_type {
        LogicalType::Blob => column.get_blob(row_idx).ok_or_else(|| {
            paro_error::internal(format!("Expected non-null BLOB at row {row_idx}"))
        }),
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb => column
            .get_string(row_idx)
            .map(str::as_bytes)
            .ok_or_else(|| {
                paro_error::internal(format!("Expected non-null string at row {row_idx}"))
            }),
        _ => Err(paro_error::internal(format!(
            "TupleLayout varlen bytes requested for non-varlen type: {logical_type:?}"
        ))),
    }
}

fn deserialize_varlen_value(logical_type: &LogicalType, bytes: &[u8]) -> Result<Value> {
    match logical_type {
        LogicalType::Blob => Ok(Value::Blob(bytes.to_vec())),
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb => {
            let text = str::from_utf8(bytes).map_err(|e| paro_error::internal(e.to_string()))?;
            Ok(Value::Varchar(text.to_string()))
        }
        _ => Err(paro_error::internal(format!(
            "TupleLayout varlen deserialize requested for non-varlen type: {logical_type:?}"
        ))),
    }
}

fn read_varlen_ref_bytes<'a>(
    varlen_ref: &'a VarlenRef,
    varlen_heap: &'a VarlenHeap,
) -> Result<&'a [u8]> {
    if let Some(inline) = varlen_ref.inline_bytes() {
        return Ok(inline);
    }
    let offset = varlen_ref.heap_offset().ok_or_else(|| {
        paro_error::internal("Failed to decode varlen heap offset from VarlenRef".to_string())
    })?;
    varlen_heap.get(offset, varlen_ref.len())
}

fn write_fixed_group_value(
    target: *mut u8,
    column: &Vector,
    row_idx: usize,
    logical_type: &LogicalType,
) -> Result<()> {
    match logical_type {
        LogicalType::Boolean => {
            let value = column.get_bool(row_idx).ok_or_else(|| {
                paro_error::internal(format!("Expected non-null BOOLEAN at row {row_idx}"))
            })?;
            unsafe { std::ptr::write(target as *mut bool, value) };
            Ok(())
        }
        LogicalType::TinyInt => write_scalar(target, column.get_i8(row_idx), "TINYINT", row_idx),
        LogicalType::UTinyInt => write_scalar(target, column.get_u8(row_idx), "UTINYINT", row_idx),
        LogicalType::SmallInt => write_scalar(target, column.get_i16(row_idx), "SMALLINT", row_idx),
        LogicalType::USmallInt => {
            write_scalar(target, column.get_u16(row_idx), "USMALLINT", row_idx)
        }
        LogicalType::Integer | LogicalType::Date => {
            write_scalar(target, column.get_i32(row_idx), "INT32", row_idx)
        }
        LogicalType::UInteger => write_scalar(target, column.get_u32(row_idx), "UINTEGER", row_idx),
        LogicalType::BigInt
        | LogicalType::Timestamp
        | LogicalType::TimestampTz
        | LogicalType::Time => write_scalar(target, column.get_i64(row_idx), "INT64", row_idx),
        LogicalType::UBigInt => write_scalar(target, column.get_u64(row_idx), "UBIGINT", row_idx),
        LogicalType::HugeInt => write_scalar(target, column.get_i128(row_idx), "HUGEINT", row_idx),
        LogicalType::UHugeInt | LogicalType::Uuid => {
            write_scalar(target, column.get_u128(row_idx), "UHUGEINT/UUID", row_idx)
        }
        LogicalType::Float => write_scalar(target, column.get_f32(row_idx), "FLOAT", row_idx),
        LogicalType::Double => write_scalar(target, column.get_f64(row_idx), "DOUBLE", row_idx),
        LogicalType::Interval => {
            let (months, days, micros) = column.get_interval(row_idx).ok_or_else(|| {
                paro_error::internal(format!("Expected non-null INTERVAL at row {row_idx}"))
            })?;
            unsafe {
                std::ptr::write_unaligned(target as *mut i32, months);
                std::ptr::write_unaligned(target.add(4) as *mut i32, days);
                std::ptr::write_unaligned(target.add(8) as *mut i64, micros);
            }
            Ok(())
        }
        LogicalType::Decimal { precision, .. } => {
            if *precision <= 18 {
                write_scalar(target, column.get_i64(row_idx), "DECIMAL64", row_idx)
            } else {
                write_scalar(target, column.get_i128(row_idx), "DECIMAL128", row_idx)
            }
        }
        _ => Err(paro_error::internal(format!(
            "Unsupported fixed group type in TupleLayout scatter: {logical_type:?}"
        ))),
    }
}

fn fixed_group_value_equals(
    source: *const u8,
    column: &Vector,
    row_idx: usize,
    logical_type: &LogicalType,
) -> Result<bool> {
    match logical_type {
        LogicalType::Boolean => {
            Ok(column.get_bool(row_idx) == Some(unsafe { std::ptr::read(source as *const bool) }))
        }
        LogicalType::TinyInt => eq_scalar(source, column.get_i8(row_idx), "TINYINT", row_idx),
        LogicalType::UTinyInt => eq_scalar(source, column.get_u8(row_idx), "UTINYINT", row_idx),
        LogicalType::SmallInt => eq_scalar(source, column.get_i16(row_idx), "SMALLINT", row_idx),
        LogicalType::USmallInt => eq_scalar(source, column.get_u16(row_idx), "USMALLINT", row_idx),
        LogicalType::Integer | LogicalType::Date => {
            eq_scalar(source, column.get_i32(row_idx), "INT32", row_idx)
        }
        LogicalType::UInteger => eq_scalar(source, column.get_u32(row_idx), "UINTEGER", row_idx),
        LogicalType::BigInt
        | LogicalType::Timestamp
        | LogicalType::TimestampTz
        | LogicalType::Time => eq_scalar(source, column.get_i64(row_idx), "INT64", row_idx),
        LogicalType::UBigInt => eq_scalar(source, column.get_u64(row_idx), "UBIGINT", row_idx),
        LogicalType::HugeInt => eq_scalar(source, column.get_i128(row_idx), "HUGEINT", row_idx),
        LogicalType::UHugeInt | LogicalType::Uuid => {
            eq_scalar(source, column.get_u128(row_idx), "UHUGEINT/UUID", row_idx)
        }
        LogicalType::Float => {
            let left = unsafe { std::ptr::read_unaligned(source as *const f32) };
            let right = column.get_f32(row_idx).ok_or_else(|| {
                paro_error::internal(format!("Expected non-null FLOAT at row {row_idx}"))
            })?;
            Ok(left.to_bits() == right.to_bits())
        }
        LogicalType::Double => {
            let left = unsafe { std::ptr::read_unaligned(source as *const f64) };
            let right = column.get_f64(row_idx).ok_or_else(|| {
                paro_error::internal(format!("Expected non-null DOUBLE at row {row_idx}"))
            })?;
            Ok(left.to_bits() == right.to_bits())
        }
        LogicalType::Interval => {
            let months = unsafe { std::ptr::read_unaligned(source as *const i32) };
            let days = unsafe { std::ptr::read_unaligned(source.add(4) as *const i32) };
            let micros = unsafe { std::ptr::read_unaligned(source.add(8) as *const i64) };
            Ok(column.get_interval(row_idx) == Some((months, days, micros)))
        }
        LogicalType::Decimal { precision, .. } => {
            if *precision <= 18 {
                eq_scalar(source, column.get_i64(row_idx), "DECIMAL64", row_idx)
            } else {
                eq_scalar(source, column.get_i128(row_idx), "DECIMAL128", row_idx)
            }
        }
        _ => Err(paro_error::internal(format!(
            "Unsupported fixed group type in TupleLayout compare: {logical_type:?}"
        ))),
    }
}

fn read_fixed_group_value(source: *const u8, logical_type: &LogicalType) -> Result<Value> {
    match logical_type {
        LogicalType::Boolean => Ok(Value::Boolean(unsafe {
            std::ptr::read(source as *const bool)
        })),
        LogicalType::TinyInt => Ok(Value::TinyInt(unsafe {
            std::ptr::read(source as *const i8)
        })),
        LogicalType::UTinyInt => Ok(Value::UTinyInt(unsafe {
            std::ptr::read(source as *const u8)
        })),
        LogicalType::SmallInt => Ok(Value::SmallInt(unsafe {
            std::ptr::read_unaligned(source as *const i16)
        })),
        LogicalType::USmallInt => Ok(Value::USmallInt(unsafe {
            std::ptr::read_unaligned(source as *const u16)
        })),
        LogicalType::Integer => Ok(Value::Integer(unsafe {
            std::ptr::read_unaligned(source as *const i32)
        })),
        LogicalType::Date => Ok(Value::Date(unsafe {
            std::ptr::read_unaligned(source as *const i32)
        })),
        LogicalType::UInteger => Ok(Value::UInteger(unsafe {
            std::ptr::read_unaligned(source as *const u32)
        })),
        LogicalType::BigInt => Ok(Value::BigInt(unsafe {
            std::ptr::read_unaligned(source as *const i64)
        })),
        LogicalType::Timestamp => Ok(Value::Timestamp(unsafe {
            std::ptr::read_unaligned(source as *const i64)
        })),
        LogicalType::TimestampTz => Ok(Value::TimestampTz(unsafe {
            std::ptr::read_unaligned(source as *const i64)
        })),
        LogicalType::Time => Ok(Value::Time(unsafe {
            std::ptr::read_unaligned(source as *const i64)
        })),
        LogicalType::UBigInt => Ok(Value::UBigInt(unsafe {
            std::ptr::read_unaligned(source as *const u64)
        })),
        LogicalType::HugeInt => Ok(Value::HugeInt(unsafe {
            std::ptr::read_unaligned(source as *const i128)
        })),
        LogicalType::UHugeInt => Ok(Value::UHugeInt(unsafe {
            std::ptr::read_unaligned(source as *const u128)
        })),
        LogicalType::Uuid => Ok(Value::Uuid(unsafe {
            std::ptr::read_unaligned(source as *const u128)
        })),
        LogicalType::Float => Ok(Value::Float(unsafe {
            std::ptr::read_unaligned(source as *const f32)
        })),
        LogicalType::Double => Ok(Value::Double(unsafe {
            std::ptr::read_unaligned(source as *const f64)
        })),
        LogicalType::Interval => Ok(Value::Interval(
            unsafe { std::ptr::read_unaligned(source as *const i32) },
            unsafe { std::ptr::read_unaligned(source.add(4) as *const i32) },
            unsafe { std::ptr::read_unaligned(source.add(8) as *const i64) },
        )),
        LogicalType::Decimal { precision, scale } => {
            if *precision <= 18 {
                let value = unsafe { std::ptr::read_unaligned(source as *const i64) };
                Ok(Value::Decimal(value as i128, *precision, *scale))
            } else {
                let value = unsafe { std::ptr::read_unaligned(source as *const i128) };
                Ok(Value::Decimal(value, *precision, *scale))
            }
        }
        _ => Err(paro_error::internal(format!(
            "Unsupported fixed group type in TupleLayout deserialize: {logical_type:?}"
        ))),
    }
}

fn write_scalar<T: Copy>(
    target: *mut u8,
    value: Option<T>,
    type_name: &str,
    row_idx: usize,
) -> Result<()> {
    let value = value.ok_or_else(|| {
        paro_error::internal(format!(
            "Expected non-null {type_name} value at row {row_idx}"
        ))
    })?;
    unsafe {
        std::ptr::write_unaligned(target as *mut T, value);
    }
    Ok(())
}

fn eq_scalar<T: Copy + PartialEq>(
    source: *const u8,
    value: Option<T>,
    type_name: &str,
    row_idx: usize,
) -> Result<bool> {
    let value = value.ok_or_else(|| {
        paro_error::internal(format!(
            "Expected non-null {type_name} value at row {row_idx}"
        ))
    })?;
    let row_value = unsafe { std::ptr::read_unaligned(source as *const T) };
    Ok(row_value == value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_function::aggregate::{AggregateFunction, AggregateInputData};
    use paro_planner::expression::AggregateExpression;

    unsafe fn initialize(_state: *mut u8) {}
    unsafe fn update(
        _inputs: &[&Vector],
        _input_data: &AggregateInputData,
        _states: &Vector,
        _count: usize,
    ) {
    }
    unsafe fn combine(
        _source: &Vector,
        _target: &Vector,
        _input_data: &AggregateInputData,
        _count: usize,
    ) {
    }
    unsafe fn finalize(
        _states: &Vector,
        _input_data: &AggregateInputData,
        _result: &mut Vector,
        _count: usize,
    ) {
    }

    fn make_test_aggregate_object() -> AggregateObject {
        let function = AggregateFunction::new(
            "test".to_string(),
            vec![LogicalType::Integer],
            LogicalType::BigInt,
            8,
            initialize,
            update,
            combine,
            finalize,
            None,
            None,
        );
        let bound = AggregateExpression::new(function, vec![], LogicalType::BigInt);
        AggregateObject::from_bound(&bound).expect("aggregate object")
    }

    #[test]
    fn tuple_layout_builds_offsets() {
        let objects = vec![make_test_aggregate_object(), make_test_aggregate_object()];
        let layout = TupleLayout::build(
            &[
                LogicalType::Integer,
                LogicalType::Varchar,
                LogicalType::Boolean,
            ],
            &objects,
        )
        .expect("layout");

        assert_eq!(layout.group_count(), 3);
        assert_eq!(layout.agg_offsets.len(), 2);
        assert!(layout.agg_state_offset >= layout.group_offsets[2] + 1);
        assert!(layout.hash_offset >= layout.agg_state_offset + 16);
        assert!(layout.row_width >= layout.hash_offset + 8);
        assert_eq!(layout.row_width % 8, 0);
    }

    #[test]
    fn tuple_layout_scatter_varlen_and_compare() {
        let objects = vec![make_test_aggregate_object()];
        let layout = TupleLayout::build(&[LogicalType::Integer, LogicalType::Varchar], &objects)
            .expect("layout");

        let groups = Chunk::from_vectors(
            vec![
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[42],
                    paro_common::test_utils::test_allocator(),
                ),
                paro_common::test_utils::test_string_vector_with_allocator(
                    &["paro"],
                    paro_common::test_utils::test_allocator(),
                ),
            ],
            paro_common::test_utils::test_allocator(),
        );

        let mut row = vec![0u8; layout.row_width];
        let mut varlen_heap = VarlenHeap::new();
        layout
            .scatter_groups(row.as_mut_ptr(), &groups, 0, &mut varlen_heap)
            .expect("scatter");

        assert!(layout
            .compare_groups(row.as_ptr(), &groups, 0, &varlen_heap)
            .expect("compare"));
        assert!(varlen_heap.is_empty());

        let varlen_ref = unsafe {
            std::ptr::read_unaligned(row.as_ptr().add(layout.group_offsets[1]) as *const VarlenRef)
        };
        assert!(varlen_ref.is_inline());
        assert_eq!(
            read_varlen_ref_bytes(&varlen_ref, &varlen_heap).expect("varlen bytes"),
            b"paro"
        );

        let key = layout
            .deserialize_group_key(row.as_ptr(), &varlen_heap)
            .expect("deserialize");
        assert_eq!(key[0], Value::Integer(42));
        assert_eq!(key[1], Value::Varchar("paro".to_string()));
    }

    #[test]
    fn tuple_layout_scatter_heap_backed_varlen_and_compare() {
        let objects = vec![make_test_aggregate_object()];
        let layout = TupleLayout::build(&[LogicalType::Integer, LogicalType::Varchar], &objects)
            .expect("layout");

        let text = "paro-varlen-key";
        assert!(text.len() > VarlenRef::inline_capacity());
        let groups = Chunk::from_vectors(
            vec![
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[42],
                    paro_common::test_utils::test_allocator(),
                ),
                paro_common::test_utils::test_string_vector_with_allocator(
                    &[text],
                    paro_common::test_utils::test_allocator(),
                ),
            ],
            paro_common::test_utils::test_allocator(),
        );

        let mut row = vec![0u8; layout.row_width];
        let mut varlen_heap = VarlenHeap::new();
        layout
            .scatter_groups(row.as_mut_ptr(), &groups, 0, &mut varlen_heap)
            .expect("scatter");

        assert!(layout
            .compare_groups(row.as_ptr(), &groups, 0, &varlen_heap)
            .expect("compare"));
        assert_eq!(varlen_heap.len(), text.len());

        let varlen_ref = unsafe {
            std::ptr::read_unaligned(row.as_ptr().add(layout.group_offsets[1]) as *const VarlenRef)
        };
        assert!(!varlen_ref.is_inline());
        assert_eq!(
            read_varlen_ref_bytes(&varlen_ref, &varlen_heap).expect("varlen bytes"),
            text.as_bytes()
        );
    }

    #[test]
    fn tuple_layout_handles_null_groups() {
        let objects = vec![make_test_aggregate_object()];
        let layout = TupleLayout::build(&[LogicalType::Integer, LogicalType::Varchar], &objects)
            .expect("layout");

        let mut strings = paro_common::test_utils::test_string_vector_with_allocator(
            &["x", "y"],
            paro_common::test_utils::test_allocator(),
        );
        strings.set_null(0, true);
        let groups = Chunk::from_vectors(
            vec![
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[1, 2],
                    paro_common::test_utils::test_allocator(),
                ),
                strings,
            ],
            paro_common::test_utils::test_allocator(),
        );

        let mut row = vec![0u8; layout.row_width];
        let mut varlen_heap = VarlenHeap::new();
        layout
            .scatter_groups(row.as_mut_ptr(), &groups, 0, &mut varlen_heap)
            .expect("scatter");

        assert!(layout
            .compare_groups(row.as_ptr(), &groups, 0, &varlen_heap)
            .expect("compare null row"));
        assert!(!layout
            .compare_groups(row.as_ptr(), &groups, 1, &varlen_heap)
            .expect("compare different row"));

        let value = layout
            .deserialize_group_value(row.as_ptr(), 1, &varlen_heap)
            .expect("deserialize null");
        assert_eq!(value, Value::Null(LogicalType::Varchar));
    }

    #[test]
    fn tuple_layout_hash_roundtrip() {
        let layout = TupleLayout::build(&[LogicalType::Integer], &[]).expect("layout");
        let mut row = vec![0u8; layout.row_width];
        layout.store_hash(row.as_mut_ptr(), 0x1234_5678_9ABC_DEF0);
        assert_eq!(layout.load_hash(row.as_ptr()), 0x1234_5678_9ABC_DEF0);
    }
}
