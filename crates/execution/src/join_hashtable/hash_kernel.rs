// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Typed hash/equality kernels for hash join keys.
//!
//! The join hot path must not go through the standard library hash path.
//! This module keeps the fast primitive/string cases explicit and funnels the
//! remaining logical types through a single generic value-gather fallback.

use std::ptr;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector, VectorType};
use paro_planner::operator::join::JoinComparisonType;
use paro_storage::row::codec::unsafe_api;
use paro_storage::row::RowLayout;

use super::build_store::BuildRowLayout;

const HASH_SEED: u64 = 0xa076_1d64_78bd_642f;
const FIELD_SEED: u64 = 0xe703_7ed1_a0b4_28db;
const NULL_HASH: u64 = 0x9e37_79b9_7f4a_7c15;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinKeyColumn {
    Boolean,
    I8,
    I16,
    I32,
    I64,
    I128,
    U8,
    U16,
    U32,
    U64,
    U128,
    F32,
    F64,
    Varchar,
    Blob,
    /// Fallback for keys without a typed vector/row kernel.
    ///
    /// This path gathers `Value`s row-at-a-time, so it must remain the rare
    /// escape hatch for nested or currently unsupported logical types.
    ValueFallback {
        type_hash: u64,
    },
}

impl JoinKeyColumn {
    fn from_type(logical_type: &LogicalType) -> Self {
        match logical_type {
            LogicalType::Boolean => Self::Boolean,
            LogicalType::TinyInt => Self::I8,
            LogicalType::SmallInt => Self::I16,
            LogicalType::Integer | LogicalType::Date => Self::I32,
            LogicalType::BigInt
            | LogicalType::Timestamp
            | LogicalType::TimestampTz
            | LogicalType::Time => Self::I64,
            LogicalType::HugeInt | LogicalType::Interval => Self::I128,
            LogicalType::UTinyInt => Self::U8,
            LogicalType::USmallInt => Self::U16,
            LogicalType::UInteger => Self::U32,
            LogicalType::UBigInt => Self::U64,
            LogicalType::UHugeInt | LogicalType::Uuid => Self::U128,
            LogicalType::Float => Self::F32,
            LogicalType::Double => Self::F64,
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::StringLiteral => Self::Varchar,
            LogicalType::Blob => Self::Blob,
            LogicalType::Decimal { precision, .. } => {
                if *precision <= 18 {
                    Self::I64
                } else {
                    Self::I128
                }
            }
            _ => Self::ValueFallback {
                type_hash: hash_logical_type_signature(logical_type),
            },
        }
    }
}

/// Physical key layout chosen once when the hash table is created.
#[derive(Debug, Clone)]
pub(crate) struct JoinKeyLayout {
    columns: Box<[JoinKeyColumn]>,
}

impl JoinKeyLayout {
    pub(crate) fn new(key_types: &[LogicalType]) -> Self {
        Self {
            columns: key_types
                .iter()
                .map(JoinKeyColumn::from_type)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    #[cfg(test)]
    pub(crate) fn hash_key_at(&self, keys: &Chunk, row_idx: usize) -> u64 {
        let mut hash = HASH_SEED;
        for (col_idx, column_kind) in self.columns.iter().copied().enumerate() {
            let vector = keys
                .column(col_idx)
                .expect("hash join key column must exist");
            hash = combine_hash(hash, self.hash_vector_value(column_kind, vector, row_idx));
        }
        hash
    }

    pub(crate) fn hash_keys_into(&self, keys: &Chunk, hashes: &mut Vector) -> Result<()> {
        let count = keys.size();
        hashes.try_set_count(count)?;
        self.hash_rows_into(
            keys,
            None,
            count,
            &mut hashes.as_mut_slice::<u64>()[..count],
        );
        Ok(())
    }

    pub(crate) fn hash_selected_into(
        &self,
        keys: &Chunk,
        selection: &SelectionVector,
        count: usize,
        hashes: &mut [u64],
    ) {
        debug_assert!(hashes.len() >= count);
        self.hash_rows_into(keys, Some(selection), count, hashes);
    }

    fn hash_rows_into(
        &self,
        keys: &Chunk,
        selection: Option<&SelectionVector>,
        count: usize,
        hashes: &mut [u64],
    ) {
        debug_assert!(hashes.len() >= count);
        let hashes = &mut hashes[..count];
        hashes.fill(HASH_SEED);
        let selected_rows = selection.map(|selection| &selection.as_slice()[..count]);

        for (col_idx, column_kind) in self.columns.iter().copied().enumerate() {
            let vector = keys
                .column(col_idx)
                .expect("hash join key column must exist");
            apply_column_hash(column_kind, vector, selected_rows, count, hashes);
        }
    }

    pub(crate) fn keys_match_build_row(
        &self,
        keys: &Chunk,
        probe_row_idx: usize,
        build_layout: &BuildRowLayout,
        row_ptr: usize,
        comparisons: &[JoinComparisonType],
    ) -> bool {
        let row_ptr = row_ptr as *const u8;
        for (col_idx, column_kind) in self.columns.iter().copied().enumerate() {
            let vector = keys
                .column(col_idx)
                .expect("hash join key column must exist");
            let probe_null = vector.is_null(probe_row_idx);
            let build_null = row_value_is_null(build_layout.base(), row_ptr, col_idx);

            match comparisons[col_idx] {
                JoinComparisonType::Equal if probe_null || build_null => return false,
                JoinComparisonType::NotDistinctFrom if probe_null || build_null => {
                    if probe_null && build_null {
                        continue;
                    }
                    return false;
                }
                JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom => {}
                comparison => {
                    unreachable!("JoinHashTable key comparison must be hashable: {comparison:?}")
                }
            }

            if !self.typed_value_equals_build_row(
                column_kind,
                vector,
                probe_row_idx,
                build_layout,
                row_ptr,
                col_idx,
            ) {
                return false;
            }
        }
        true
    }

    #[cfg(test)]
    fn hash_vector_value(
        &self,
        column_kind: JoinKeyColumn,
        vector: &Vector,
        row_idx: usize,
    ) -> u64 {
        if vector.is_null(row_idx) {
            return NULL_HASH;
        }

        match column_kind {
            JoinKeyColumn::Boolean => hash_u64(u64::from(vector.get_bool(row_idx).unwrap())),
            JoinKeyColumn::I8 => hash_i64(vector.get_i8(row_idx).unwrap() as i64),
            JoinKeyColumn::I16 => hash_i64(vector.get_i16(row_idx).unwrap() as i64),
            JoinKeyColumn::I32 => hash_i64(vector.get_i32(row_idx).unwrap() as i64),
            JoinKeyColumn::I64 => hash_i64(vector.get_i64(row_idx).unwrap()),
            JoinKeyColumn::I128 => hash_u128(vector.get_i128(row_idx).unwrap() as u128),
            JoinKeyColumn::U8 => hash_u64(vector.get_u8(row_idx).unwrap() as u64),
            JoinKeyColumn::U16 => hash_u64(vector.get_u16(row_idx).unwrap() as u64),
            JoinKeyColumn::U32 => hash_u64(vector.get_u32(row_idx).unwrap() as u64),
            JoinKeyColumn::U64 => hash_u64(vector.get_u64(row_idx).unwrap()),
            JoinKeyColumn::U128 => hash_u128(vector.get_u128(row_idx).unwrap()),
            JoinKeyColumn::F32 => hash_u64(vector.get_f32(row_idx).unwrap().to_bits() as u64),
            JoinKeyColumn::F64 => hash_u64(vector.get_f64(row_idx).unwrap().to_bits()),
            JoinKeyColumn::Varchar => hash_bytes(vector.get_string(row_idx).unwrap().as_bytes()),
            JoinKeyColumn::Blob => hash_bytes(vector.get_blob(row_idx).unwrap()),
            JoinKeyColumn::ValueFallback { type_hash } => {
                combine_hash(type_hash, value_fallback_hash(&vector.get_value(row_idx)))
            }
        }
    }

    fn typed_value_equals_build_row(
        &self,
        column_kind: JoinKeyColumn,
        vector: &Vector,
        probe_row_idx: usize,
        build_layout: &BuildRowLayout,
        row_ptr: *const u8,
        col_idx: usize,
    ) -> bool {
        match column_kind {
            JoinKeyColumn::Boolean => {
                vector.get_bool(probe_row_idx).unwrap()
                    == read_row_bool(build_layout.base(), row_ptr, col_idx)
            }
            JoinKeyColumn::I8 => {
                vector.get_i8(probe_row_idx).unwrap()
                    == read_row_i8(build_layout.base(), row_ptr, col_idx)
            }
            JoinKeyColumn::I16 => {
                vector.get_i16(probe_row_idx).unwrap()
                    == read_row_i16(build_layout.base(), row_ptr, col_idx)
            }
            JoinKeyColumn::I32 => {
                vector.get_i32(probe_row_idx).unwrap()
                    == read_row_i32(build_layout.base(), row_ptr, col_idx)
            }
            JoinKeyColumn::I64 => {
                vector.get_i64(probe_row_idx).unwrap()
                    == read_row_i64(build_layout.base(), row_ptr, col_idx)
            }
            JoinKeyColumn::I128 => {
                vector.get_i128(probe_row_idx).unwrap()
                    == read_row_i128(build_layout.base(), row_ptr, col_idx)
            }
            JoinKeyColumn::U8 => {
                vector.get_u8(probe_row_idx).unwrap()
                    == read_row_u8(build_layout.base(), row_ptr, col_idx)
            }
            JoinKeyColumn::U16 => {
                vector.get_u16(probe_row_idx).unwrap()
                    == read_row_u16(build_layout.base(), row_ptr, col_idx)
            }
            JoinKeyColumn::U32 => {
                vector.get_u32(probe_row_idx).unwrap()
                    == read_row_u32(build_layout.base(), row_ptr, col_idx)
            }
            JoinKeyColumn::U64 => {
                vector.get_u64(probe_row_idx).unwrap()
                    == read_row_u64(build_layout.base(), row_ptr, col_idx)
            }
            JoinKeyColumn::U128 => {
                vector.get_u128(probe_row_idx).unwrap()
                    == read_row_u128(build_layout.base(), row_ptr, col_idx)
            }
            JoinKeyColumn::F32 => {
                vector.get_f32(probe_row_idx).unwrap().to_bits()
                    == read_row_u32(build_layout.base(), row_ptr, col_idx)
            }
            JoinKeyColumn::F64 => {
                vector.get_f64(probe_row_idx).unwrap().to_bits()
                    == read_row_u64(build_layout.base(), row_ptr, col_idx)
            }
            JoinKeyColumn::Varchar => {
                vector.get_string(probe_row_idx).unwrap().as_bytes()
                    == read_row_varlen_bytes(build_layout.base(), row_ptr, col_idx)
            }
            JoinKeyColumn::Blob => {
                vector.get_blob(probe_row_idx).unwrap()
                    == read_row_varlen_bytes(build_layout.base(), row_ptr, col_idx)
            }
            JoinKeyColumn::ValueFallback { .. } => {
                vector.get_value(probe_row_idx) == build_layout.read_value(row_ptr, col_idx)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn column_kinds_for_tests(&self) -> Vec<&'static str> {
        self.columns
            .iter()
            .map(|column| match column {
                JoinKeyColumn::Boolean => "bool",
                JoinKeyColumn::I8 => "i8",
                JoinKeyColumn::I16 => "i16",
                JoinKeyColumn::I32 => "i32",
                JoinKeyColumn::I64 => "i64",
                JoinKeyColumn::I128 => "i128",
                JoinKeyColumn::U8 => "u8",
                JoinKeyColumn::U16 => "u16",
                JoinKeyColumn::U32 => "u32",
                JoinKeyColumn::U64 => "u64",
                JoinKeyColumn::U128 => "u128",
                JoinKeyColumn::F32 => "f32",
                JoinKeyColumn::F64 => "f64",
                JoinKeyColumn::Varchar => "varchar",
                JoinKeyColumn::Blob => "blob",
                JoinKeyColumn::ValueFallback { .. } => "value_fallback",
            })
            .collect()
    }
}

fn apply_column_hash(
    column_kind: JoinKeyColumn,
    vector: &Vector,
    selected_rows: Option<&[u32]>,
    count: usize,
    hashes: &mut [u64],
) {
    match column_kind {
        JoinKeyColumn::Boolean => apply_fixed_hash_column::<bool>(
            vector,
            selected_rows,
            count,
            hashes,
            |value| hash_u64(u64::from(value)),
            |vector, row| vector.get_bool(row),
        ),
        JoinKeyColumn::I8 => apply_fixed_hash_column::<i8>(
            vector,
            selected_rows,
            count,
            hashes,
            |value| hash_i64(value as i64),
            |vector, row| vector.get_i8(row),
        ),
        JoinKeyColumn::I16 => apply_fixed_hash_column::<i16>(
            vector,
            selected_rows,
            count,
            hashes,
            |value| hash_i64(value as i64),
            |vector, row| vector.get_i16(row),
        ),
        JoinKeyColumn::I32 => apply_fixed_hash_column::<i32>(
            vector,
            selected_rows,
            count,
            hashes,
            |value| hash_i64(value as i64),
            |vector, row| vector.get_i32(row),
        ),
        JoinKeyColumn::I64 => apply_fixed_hash_column::<i64>(
            vector,
            selected_rows,
            count,
            hashes,
            hash_i64,
            |vector, row| vector.get_i64(row),
        ),
        JoinKeyColumn::I128 => apply_fixed_hash_column::<i128>(
            vector,
            selected_rows,
            count,
            hashes,
            |value| hash_u128(value as u128),
            |vector, row| vector.get_i128(row),
        ),
        JoinKeyColumn::U8 => apply_fixed_hash_column::<u8>(
            vector,
            selected_rows,
            count,
            hashes,
            |value| hash_u64(value as u64),
            |vector, row| vector.get_u8(row),
        ),
        JoinKeyColumn::U16 => apply_fixed_hash_column::<u16>(
            vector,
            selected_rows,
            count,
            hashes,
            |value| hash_u64(value as u64),
            |vector, row| vector.get_u16(row),
        ),
        JoinKeyColumn::U32 => apply_fixed_hash_column::<u32>(
            vector,
            selected_rows,
            count,
            hashes,
            |value| hash_u64(value as u64),
            |vector, row| vector.get_u32(row),
        ),
        JoinKeyColumn::U64 => apply_fixed_hash_column::<u64>(
            vector,
            selected_rows,
            count,
            hashes,
            hash_u64,
            |vector, row| vector.get_u64(row),
        ),
        JoinKeyColumn::U128 => apply_fixed_hash_column::<u128>(
            vector,
            selected_rows,
            count,
            hashes,
            hash_u128,
            |vector, row| vector.get_u128(row),
        ),
        JoinKeyColumn::F32 => apply_fixed_hash_column::<f32>(
            vector,
            selected_rows,
            count,
            hashes,
            |value| hash_u64(value.to_bits() as u64),
            |vector, row| vector.get_f32(row),
        ),
        JoinKeyColumn::F64 => apply_fixed_hash_column::<f64>(
            vector,
            selected_rows,
            count,
            hashes,
            |value| hash_u64(value.to_bits()),
            |vector, row| vector.get_f64(row),
        ),
        JoinKeyColumn::Varchar => {
            apply_varlen_hash_column(vector, selected_rows, count, hashes, |vector, row| {
                vector.get_string(row).map(str::as_bytes)
            })
        }
        JoinKeyColumn::Blob => {
            apply_varlen_hash_column(vector, selected_rows, count, hashes, |vector, row| {
                vector.get_blob(row)
            })
        }
        JoinKeyColumn::ValueFallback { type_hash } => {
            apply_value_fallback_hash_column(vector, selected_rows, count, hashes, type_hash)
        }
    }
}

fn apply_fixed_hash_column<T: Copy>(
    vector: &Vector,
    selected_rows: Option<&[u32]>,
    count: usize,
    hashes: &mut [u64],
    hash_value: impl Fn(T) -> u64,
    read_value: impl Fn(&Vector, usize) -> Option<T>,
) {
    match (selected_rows, vector.vector_type()) {
        (None, VectorType::Flat) => {
            let values = vector.as_slice::<T>();
            for row_idx in 0..count {
                let value_hash = if vector.is_null(row_idx) {
                    NULL_HASH
                } else {
                    hash_value(values[row_idx])
                };
                hashes[row_idx] = combine_hash(hashes[row_idx], value_hash);
            }
        }
        (Some(selection), VectorType::Flat) => {
            let values = vector.as_slice::<T>();
            for out_idx in 0..count {
                let row_idx = selection[out_idx] as usize;
                let value_hash = if vector.is_null(row_idx) {
                    NULL_HASH
                } else {
                    hash_value(values[row_idx])
                };
                hashes[out_idx] = combine_hash(hashes[out_idx], value_hash);
            }
        }
        (_, VectorType::Constant) => {
            let value_hash = if vector.is_null(0) {
                NULL_HASH
            } else {
                hash_value(vector.as_slice::<T>()[0])
            };
            for slot in hashes.iter_mut().take(count) {
                *slot = combine_hash(*slot, value_hash);
            }
        }
        (None, _) => {
            for row_idx in 0..count {
                let value_hash = if vector.is_null(row_idx) {
                    NULL_HASH
                } else {
                    hash_value(
                        read_value(vector, row_idx)
                            .expect("hash join key vector must contain the expected type"),
                    )
                };
                hashes[row_idx] = combine_hash(hashes[row_idx], value_hash);
            }
        }
        (Some(selection), _) => {
            for out_idx in 0..count {
                let row_idx = selection[out_idx] as usize;
                let value_hash = if vector.is_null(row_idx) {
                    NULL_HASH
                } else {
                    hash_value(
                        read_value(vector, row_idx)
                            .expect("hash join key vector must contain the expected type"),
                    )
                };
                hashes[out_idx] = combine_hash(hashes[out_idx], value_hash);
            }
        }
    }
}

fn apply_varlen_hash_column<'a>(
    vector: &'a Vector,
    selected_rows: Option<&[u32]>,
    count: usize,
    hashes: &mut [u64],
    read_value: impl Fn(&'a Vector, usize) -> Option<&'a [u8]>,
) {
    if vector.vector_type() == VectorType::Constant {
        let value_hash = if vector.is_null(0) {
            NULL_HASH
        } else {
            hash_bytes(read_value(vector, 0).unwrap_or_default())
        };
        for slot in hashes.iter_mut().take(count) {
            *slot = combine_hash(*slot, value_hash);
        }
        return;
    }

    for out_idx in 0..count {
        let row_idx = selected_rows.map_or(out_idx, |selection| selection[out_idx] as usize);
        let value_hash = if vector.is_null(row_idx) {
            NULL_HASH
        } else {
            hash_bytes(read_value(vector, row_idx).unwrap_or_default())
        };
        hashes[out_idx] = combine_hash(hashes[out_idx], value_hash);
    }
}

fn apply_value_fallback_hash_column(
    vector: &Vector,
    selected_rows: Option<&[u32]>,
    count: usize,
    hashes: &mut [u64],
    type_hash: u64,
) {
    for out_idx in 0..count {
        let row_idx = selected_rows.map_or(out_idx, |selection| selection[out_idx] as usize);
        let value_hash = if vector.is_null(row_idx) {
            NULL_HASH
        } else {
            combine_hash(type_hash, value_fallback_hash(&vector.get_value(row_idx)))
        };
        hashes[out_idx] = combine_hash(hashes[out_idx], value_hash);
    }
}

fn row_value_is_null(layout: &RowLayout, row_ptr: *const u8, col_idx: usize) -> bool {
    !layout.all_valid() && !unsafe { unsafe_api::row_is_valid(row_ptr, col_idx) }
}

#[inline]
fn row_cell_ptr(layout: &RowLayout, row_ptr: *const u8, col_idx: usize) -> *const u8 {
    unsafe { row_ptr.add(layout.offsets()[col_idx]) }
}

#[inline]
fn read_row_bool(layout: &RowLayout, row_ptr: *const u8, col_idx: usize) -> bool {
    unsafe { ptr::read(row_cell_ptr(layout, row_ptr, col_idx)) != 0 }
}

#[inline]
fn read_row_i8(layout: &RowLayout, row_ptr: *const u8, col_idx: usize) -> i8 {
    unsafe { ptr::read(row_cell_ptr(layout, row_ptr, col_idx) as *const i8) }
}

#[inline]
fn read_row_u8(layout: &RowLayout, row_ptr: *const u8, col_idx: usize) -> u8 {
    unsafe { ptr::read(row_cell_ptr(layout, row_ptr, col_idx)) }
}

macro_rules! read_row_unaligned {
    ($name:ident, $ty:ty) => {
        #[inline]
        fn $name(layout: &RowLayout, row_ptr: *const u8, col_idx: usize) -> $ty {
            unsafe { ptr::read_unaligned(row_cell_ptr(layout, row_ptr, col_idx) as *const $ty) }
        }
    };
}

read_row_unaligned!(read_row_i16, i16);
read_row_unaligned!(read_row_i32, i32);
read_row_unaligned!(read_row_i64, i64);
read_row_unaligned!(read_row_i128, i128);
read_row_unaligned!(read_row_u16, u16);
read_row_unaligned!(read_row_u32, u32);
read_row_unaligned!(read_row_u64, u64);
read_row_unaligned!(read_row_u128, u128);

fn read_row_varlen_bytes<'a>(layout: &RowLayout, row_ptr: *const u8, col_idx: usize) -> &'a [u8] {
    let cell_ptr = row_cell_ptr(layout, row_ptr, col_idx);
    let len = unsafe { ptr::read_unaligned(cell_ptr as *const u32) as usize };
    if len == 0 {
        &[]
    } else if len <= 12 {
        unsafe { std::slice::from_raw_parts(cell_ptr.add(4), len) }
    } else {
        let heap_ptr = unsafe { ptr::read_unaligned(cell_ptr.add(8) as *const *const u8) };
        unsafe { std::slice::from_raw_parts(heap_ptr, len) }
    }
}

pub(crate) fn value_fallback_hash(value: &Value) -> u64 {
    match value {
        Value::Null(_) => NULL_HASH,
        Value::Boolean(v) => hash_u64(u64::from(*v)),
        Value::TinyInt(v) => hash_i64(*v as i64),
        Value::SmallInt(v) => hash_i64(*v as i64),
        Value::Integer(v) => hash_i64(*v as i64),
        Value::BigInt(v) => hash_i64(*v),
        Value::HugeInt(v) => hash_u128(*v as u128),
        Value::UTinyInt(v) => hash_u64(*v as u64),
        Value::USmallInt(v) => hash_u64(*v as u64),
        Value::UInteger(v) => hash_u64(*v as u64),
        Value::UBigInt(v) => hash_u64(*v),
        Value::UHugeInt(v) => hash_u128(*v),
        Value::Float(v) => hash_u64(v.to_bits() as u64),
        Value::Double(v) => hash_u64(v.to_bits()),
        Value::Decimal(v, precision, scale) => combine_hash(
            hash_u128(*v as u128),
            combine_hash(hash_u64(*precision as u64), hash_u64(*scale as u64)),
        ),
        Value::Varchar(v) => hash_bytes(v.as_bytes()),
        Value::Blob(v) => hash_bytes(v),
        Value::Uuid(v) => hash_u128(*v),
        Value::Date(v) => hash_i64(*v as i64),
        Value::Timestamp(v) | Value::TimestampTz(v) | Value::Time(v) => hash_i64(*v),
        Value::Interval(months, days, micros) => combine_hash(
            hash_i64(*months as i64),
            combine_hash(hash_i64(*days as i64), hash_i64(*micros)),
        ),
        Value::List(values, logical_type) => hash_nested_values(values, logical_type),
        Value::Struct(values, fields) => {
            let mut hash = hash_u64(fields.len() as u64);
            for (name, logical_type) in fields {
                hash = combine_hash(hash, hash_bytes(name.as_bytes()));
                hash = combine_hash(hash, hash_logical_type_signature(logical_type));
            }
            for value in values {
                hash = combine_hash(hash, value_fallback_hash(value));
            }
            hash
        }
        Value::Array(values, logical_type, size) => combine_hash(
            hash_nested_values(values, logical_type),
            hash_u64(*size as u64),
        ),
    }
}

fn hash_nested_values(values: &[Value], logical_type: &LogicalType) -> u64 {
    let mut hash = combine_hash(
        hash_u64(values.len() as u64),
        hash_logical_type_signature(logical_type),
    );
    for value in values {
        hash = combine_hash(hash, value_fallback_hash(value));
    }
    hash
}

fn hash_logical_type_signature(logical_type: &LogicalType) -> u64 {
    let mut hash = hash_u64(logical_type.type_id() as u64);
    match logical_type {
        LogicalType::Decimal { precision, scale } => {
            hash = combine_hash(hash, hash_u64(*precision as u64));
            hash = combine_hash(hash, hash_u64(*scale as u64));
        }
        LogicalType::Array(child, size) => {
            hash = combine_hash(hash, hash_logical_type_signature(child));
            hash = combine_hash(hash, hash_u64(*size as u64));
        }
        LogicalType::List(child) => {
            hash = combine_hash(hash, hash_logical_type_signature(child));
        }
        LogicalType::Struct(fields) => {
            hash = combine_hash(hash, hash_u64(fields.len() as u64));
            for (name, field_type) in fields {
                hash = combine_hash(hash, hash_bytes(name.as_bytes()));
                hash = combine_hash(hash, hash_logical_type_signature(field_type));
            }
        }
        LogicalType::VarcharCollation(collation) => {
            hash = combine_hash(hash, hash_bytes(collation.as_bytes()));
        }
        LogicalType::IntegerLiteral(value) => {
            hash = combine_hash(hash, hash_i64(*value));
        }
        _ => {}
    }
    hash
}

#[inline]
fn hash_i64(value: i64) -> u64 {
    hash_u64(value as u64)
}

#[inline]
fn hash_u128(value: u128) -> u64 {
    combine_hash(hash_u64(value as u64), hash_u64((value >> 64) as u64))
}

#[inline]
fn hash_u64(value: u64) -> u64 {
    let left = value.wrapping_mul(0xa076_1d64_78bd_642f) ^ HASH_SEED;
    let right = value.rotate_left(31).wrapping_mul(0xe703_7ed1_a0b4_28db) ^ FIELD_SEED;
    mul_xor_mix(left, right)
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut hash = combine_hash(hash_u64(bytes.len() as u64), FIELD_SEED);
    let mut chunks = bytes.chunks_exact(8);
    for chunk in chunks.by_ref() {
        let word = u64::from_le_bytes(chunk.try_into().expect("chunk length is exactly 8"));
        hash = combine_hash(hash, hash_u64(word));
    }
    let remainder = chunks.remainder();
    if !remainder.is_empty() {
        let mut tail = [0u8; 8];
        tail[..remainder.len()].copy_from_slice(remainder);
        hash = combine_hash(hash, hash_u64(u64::from_le_bytes(tail)));
    }
    hash
}

#[inline]
fn combine_hash(left: u64, right: u64) -> u64 {
    mul_xor_mix(left ^ FIELD_SEED, right.wrapping_add(0x9e37_79b9_7f4a_7c15))
}

#[inline]
fn mul_xor_mix(left: u64, right: u64) -> u64 {
    let product = (left as u128).wrapping_mul(right as u128);
    ((product >> 64) as u64) ^ (product as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::chunk::Chunk;
    use paro_common::test_utils::{test_allocator, test_i32_vector_with_allocator, test_vector};
    use std::sync::Arc;

    #[test]
    fn join_key_layout_selects_typed_integer_kernel() {
        let layout = JoinKeyLayout::new(&[LogicalType::Integer, LogicalType::Varchar]);
        assert_eq!(layout.column_kinds_for_tests(), vec!["i32", "varchar"]);
    }

    #[test]
    fn typed_hash_kernel_hashes_equal_values_identically() {
        let allocator = test_allocator();
        let layout = JoinKeyLayout::new(&[LogicalType::Integer]);
        let left = Chunk::from_arc_vectors(
            vec![Arc::new(test_i32_vector_with_allocator(
                &[42],
                allocator.clone(),
            ))],
            allocator.clone(),
        );
        let right = Chunk::from_arc_vectors(
            vec![Arc::new(test_i32_vector_with_allocator(
                &[42],
                allocator.clone(),
            ))],
            allocator,
        );
        assert_eq!(layout.hash_key_at(&left, 0), layout.hash_key_at(&right, 0));
    }

    #[test]
    fn generic_hash_fallback_covers_nested_values() {
        let mut nested = test_vector(LogicalType::List(Box::new(LogicalType::Integer)));
        nested.set_value(
            0,
            &Value::List(
                vec![Value::Integer(1), Value::Integer(2)],
                LogicalType::Integer,
            ),
        );
        nested.set_count(1);
        let chunk = Chunk::from_arc_vectors(vec![Arc::new(nested)], test_allocator());
        let layout = JoinKeyLayout::new(&[LogicalType::List(Box::new(LogicalType::Integer))]);
        assert_ne!(layout.hash_key_at(&chunk, 0), 0);
    }

    #[test]
    fn hash_kernel_hashes_selected_rows_column_major() {
        let allocator = test_allocator();
        let layout = JoinKeyLayout::new(&[LogicalType::Integer]);
        let chunk = Chunk::from_arc_vectors(
            vec![Arc::new(test_i32_vector_with_allocator(
                &[7, 42, 99],
                allocator.clone(),
            ))],
            allocator.clone(),
        );
        let selection =
            SelectionVector::try_from_indices(vec![2, 1], allocator).expect("selection");
        let mut hashes = vec![0; 2];
        layout.hash_selected_into(&chunk, &selection, 2, &mut hashes);

        assert_eq!(hashes[0], layout.hash_key_at(&chunk, 2));
        assert_eq!(hashes[1], layout.hash_key_at(&chunk, 1));
    }
}
