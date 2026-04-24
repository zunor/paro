// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::cmp::Ordering;

use crate::chunk::Chunk;
use crate::error::{self as paro_error, Result};
use crate::types::LogicalType;
use crate::vector::Vector;

const NULL_FIRST_BYTE: u8 = 1;
const NULL_LAST_BYTE: u8 = 2;
const STRING_DELIMITER: u8 = 0;
const BLOB_ESCAPE_CHARACTER: u8 = 1;
const VARIABLE_INLINE_PREFIX_LEN: usize = 16;
const VARIABLE_SLOT_SIZE: usize = 32;
const TRANSFORM_BUFFER_LEN: usize = 256;

trait KeyWriter {
    fn push_byte(&mut self, byte: u8);

    fn extend_bytes(&mut self, bytes: &[u8]);

    fn repeat_byte(&mut self, byte: u8, count: usize) {
        for _ in 0..count {
            self.push_byte(byte);
        }
    }
}

impl KeyWriter for Vec<u8> {
    #[inline]
    fn push_byte(&mut self, byte: u8) {
        self.push(byte);
    }

    #[inline]
    fn extend_bytes(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }
}

struct SplitKeyWriter<'a> {
    inline_prefix: &'a mut [u8],
    overflow: &'a mut [u8],
    written: usize,
}

impl<'a> SplitKeyWriter<'a> {
    fn new(inline_prefix: &'a mut [u8], overflow: &'a mut [u8]) -> Self {
        Self {
            inline_prefix,
            overflow,
            written: 0,
        }
    }

    fn finish(self) {
        debug_assert_eq!(self.written, self.inline_prefix.len() + self.overflow.len());
    }
}

impl KeyWriter for SplitKeyWriter<'_> {
    #[inline]
    fn push_byte(&mut self, byte: u8) {
        let inline_len = self.inline_prefix.len();
        if self.written < inline_len {
            self.inline_prefix[self.written] = byte;
        } else {
            self.overflow[self.written - inline_len] = byte;
        }
        self.written += 1;
        debug_assert!(self.written <= inline_len + self.overflow.len());
    }

    #[inline]
    fn extend_bytes(&mut self, bytes: &[u8]) {
        let total_len = self.inline_prefix.len() + self.overflow.len();
        debug_assert!(self.written + bytes.len() <= total_len);

        let mut remaining = bytes;
        if self.written < self.inline_prefix.len() {
            let inline_offset = self.written;
            let inline_copy = (self.inline_prefix.len() - inline_offset).min(remaining.len());
            self.inline_prefix[inline_offset..inline_offset + inline_copy]
                .copy_from_slice(&remaining[..inline_copy]);
            self.written += inline_copy;
            remaining = &remaining[inline_copy..];
        }

        if !remaining.is_empty() {
            let overflow_offset = self.written - self.inline_prefix.len();
            self.overflow[overflow_offset..overflow_offset + remaining.len()]
                .copy_from_slice(remaining);
            self.written += remaining.len();
        }
    }
}

/// ORDER BY modifiers for a single column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderModifiers {
    pub ascending: bool,
    pub nulls_first: bool,
}

impl OrderModifiers {
    #[inline]
    pub const fn new(ascending: bool, nulls_first: bool) -> Self {
        Self {
            ascending,
            nulls_first,
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        let normalized = s.to_ascii_lowercase().replace('_', " ");
        let ascending = if normalized.starts_with("asc") {
            true
        } else if normalized.starts_with("desc") {
            false
        } else {
            return Err(paro_error::syntax(
                "create_sort_key modifier must start with either ASC or DESC",
            ));
        };

        let nulls_first = if normalized.ends_with("nulls first") {
            true
        } else if normalized.ends_with("nulls last") {
            false
        } else {
            return Err(paro_error::syntax(
                "create_sort_key modifier must end with either NULLS FIRST or NULLS LAST",
            ));
        };

        Ok(Self {
            ascending,
            nulls_first,
        })
    }
}

#[derive(Debug, Clone)]
struct SortKeyFieldEncoding {
    logical_type: LogicalType,
    modifiers: OrderModifiers,
}

/// Precomputed sort-key contract for a specific ORDER BY key list.
#[derive(Debug, Clone)]
pub struct SortKeyEncoding {
    fields: Vec<SortKeyFieldEncoding>,
    fixed_key_len: Option<usize>,
    slot_size: usize,
    inline_prefix_len: usize,
}

impl SortKeyEncoding {
    pub fn new(types: Vec<LogicalType>, modifiers: Vec<OrderModifiers>) -> Result<Self> {
        if types.len() != modifiers.len() {
            return Err(paro_error::internal(format!(
                "sort key type/modifier count mismatch: {} types, {} modifiers",
                types.len(),
                modifiers.len()
            )));
        }

        let mut fields = Vec::with_capacity(types.len());
        let mut total_len = 0usize;
        let mut all_fixed = true;

        for (logical_type, modifiers) in types.into_iter().zip(modifiers.into_iter()) {
            match fixed_value_len(&logical_type)? {
                Some(data_len) => total_len += 1 + data_len,
                None => all_fixed = false,
            }
            fields.push(SortKeyFieldEncoding {
                logical_type,
                modifiers,
            });
        }

        let fixed_key_len = if all_fixed && total_len <= VARIABLE_SLOT_SIZE {
            Some(total_len)
        } else {
            None
        };
        let slot_size = fixed_key_len.unwrap_or(VARIABLE_SLOT_SIZE);
        let inline_prefix_len = fixed_key_len.unwrap_or(VARIABLE_INLINE_PREFIX_LEN);

        Ok(Self {
            fields,
            fixed_key_len,
            slot_size,
            inline_prefix_len,
        })
    }

    #[inline]
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    #[inline]
    pub fn logical_types(&self) -> impl Iterator<Item = &LogicalType> {
        self.fields.iter().map(|field| &field.logical_type)
    }

    #[inline]
    pub fn modifiers(&self) -> impl Iterator<Item = OrderModifiers> + '_ {
        self.fields.iter().map(|field| field.modifiers)
    }

    #[inline]
    pub fn fixed_key_len(&self) -> Option<usize> {
        self.fixed_key_len
    }

    #[inline]
    pub fn is_variable(&self) -> bool {
        self.fixed_key_len.is_none()
    }

    #[inline]
    pub fn slot_size(&self) -> usize {
        self.slot_size
    }

    #[inline]
    pub fn inline_prefix_len(&self) -> usize {
        self.inline_prefix_len
    }

    pub fn encoded_len(&self, chunk: &Chunk, row_idx: usize, columns: &[usize]) -> Result<usize> {
        if let Some(fixed_len) = self.fixed_key_len {
            return Ok(fixed_len);
        }
        if columns.len() != self.fields.len() {
            return Err(paro_error::internal(format!(
                "sort key column count mismatch: {} columns, {} fields",
                columns.len(),
                self.fields.len()
            )));
        }

        let mut len = 0usize;
        for (&column_idx, field) in columns.iter().zip(self.fields.iter()) {
            let vector = chunk.column(column_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "sort key column {} out of bounds for chunk with {} columns",
                    column_idx,
                    chunk.column_count()
                ))
            })?;
            len += encoded_column_len(vector, row_idx, &field.logical_type)?;
        }
        Ok(len)
    }

    pub fn encode_row_into(
        &self,
        chunk: &Chunk,
        row_idx: usize,
        columns: &[usize],
        out: &mut Vec<u8>,
    ) -> Result<()> {
        out.clear();
        let reserve = self
            .fixed_key_len
            .unwrap_or_else(|| self.inline_prefix_len.max(64));
        out.reserve(reserve);
        self.encode_row_with_writer(chunk, row_idx, columns, out)
    }

    pub fn encode_row_into_parts(
        &self,
        chunk: &Chunk,
        row_idx: usize,
        columns: &[usize],
        inline_prefix: &mut [u8],
        overflow: &mut [u8],
    ) -> Result<()> {
        let total_len = if let Some(fixed_len) = self.fixed_key_len {
            fixed_len
        } else {
            self.encoded_len(chunk, row_idx, columns)?
        };
        let inline_len = total_len.min(self.inline_prefix_len);
        let overflow_len = total_len.saturating_sub(inline_len);

        if inline_prefix.len() != inline_len || overflow.len() != overflow_len {
            return Err(paro_error::internal(format!(
                "sort key output buffer mismatch: expected inline={}, overflow={}, got inline={}, overflow={}",
                inline_len,
                overflow_len,
                inline_prefix.len(),
                overflow.len()
            )));
        }

        let mut writer = SplitKeyWriter::new(inline_prefix, overflow);
        self.encode_row_with_writer(chunk, row_idx, columns, &mut writer)?;
        writer.finish();
        Ok(())
    }

    pub fn encode_row(&self, chunk: &Chunk, row_idx: usize, columns: &[usize]) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        self.encode_row_into(chunk, row_idx, columns, &mut out)?;
        Ok(out)
    }

    fn encode_row_with_writer<W: KeyWriter + ?Sized>(
        &self,
        chunk: &Chunk,
        row_idx: usize,
        columns: &[usize],
        out: &mut W,
    ) -> Result<()> {
        if columns.len() != self.fields.len() {
            return Err(paro_error::internal(format!(
                "sort key column count mismatch: {} columns, {} fields",
                columns.len(),
                self.fields.len()
            )));
        }

        for (&column_idx, field) in columns.iter().zip(self.fields.iter()) {
            let vector = chunk.column(column_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "sort key column {} out of bounds for chunk with {} columns",
                    column_idx,
                    chunk.column_count()
                ))
            })?;
            encode_field(vector, row_idx, field, out)?;
        }

        Ok(())
    }
}

#[inline]
pub fn compare_keys(left: &[u8], right: &[u8]) -> Ordering {
    left.cmp(right)
}

pub fn encode_column(
    vector: &Vector,
    row_idx: usize,
    modifiers: OrderModifiers,
    out: &mut Vec<u8>,
) -> Result<()> {
    encode_column_into(vector, row_idx, modifiers, out)
}

fn encode_column_into<W: KeyWriter + ?Sized>(
    vector: &Vector,
    row_idx: usize,
    modifiers: OrderModifiers,
    out: &mut W,
) -> Result<()> {
    let (null_byte, valid_byte) = if modifiers.nulls_first {
        (NULL_FIRST_BYTE, NULL_LAST_BYTE)
    } else {
        (NULL_LAST_BYTE, NULL_FIRST_BYTE)
    };

    if vector.is_null(row_idx) {
        out.push_byte(null_byte);
        return Ok(());
    }

    out.push_byte(valid_byte);
    encode_value_into(vector, row_idx, modifiers.ascending, out)
}

pub fn encode_value(
    vector: &Vector,
    row_idx: usize,
    ascending: bool,
    out: &mut Vec<u8>,
) -> Result<()> {
    encode_value_into(vector, row_idx, ascending, out)
}

fn encode_value_into<W: KeyWriter + ?Sized>(
    vector: &Vector,
    row_idx: usize,
    ascending: bool,
    out: &mut W,
) -> Result<()> {
    match vector.logical_type() {
        LogicalType::Boolean => {
            if let Some(value) = vector.get_bool(row_idx) {
                encode_bool(value, ascending, out);
            }
        }
        LogicalType::TinyInt => {
            if let Some(value) = vector.get_i8(row_idx) {
                encode_i8(value, ascending, out);
            }
        }
        LogicalType::SmallInt => {
            if let Some(value) = vector.get_i16(row_idx) {
                encode_i16(value, ascending, out);
            }
        }
        LogicalType::Integer => {
            if let Some(value) = vector.get_i32(row_idx) {
                encode_i32(value, ascending, out);
            }
        }
        LogicalType::BigInt => {
            if let Some(value) = vector.get_i64(row_idx) {
                encode_i64(value, ascending, out);
            }
        }
        LogicalType::HugeInt => {
            if let Some(value) = vector.get_i128(row_idx) {
                encode_hugeint(value, ascending, out);
            }
        }
        LogicalType::UTinyInt => {
            if let Some(value) = vector.get_u8(row_idx) {
                encode_u8(value, ascending, out);
            }
        }
        LogicalType::USmallInt => {
            if let Some(value) = vector.get_u16(row_idx) {
                encode_u16(value, ascending, out);
            }
        }
        LogicalType::UInteger => {
            if let Some(value) = vector.get_u32(row_idx) {
                encode_u32(value, ascending, out);
            }
        }
        LogicalType::UBigInt => {
            if let Some(value) = vector.get_u64(row_idx) {
                encode_u64(value, ascending, out);
            }
        }
        LogicalType::UHugeInt | LogicalType::Uuid => {
            if let Some(value) = vector.get_u128(row_idx) {
                encode_uhugeint(value, ascending, out);
            }
        }
        LogicalType::Float => {
            if let Some(value) = vector.get_f32(row_idx) {
                encode_f32(value, ascending, out);
            }
        }
        LogicalType::Double => {
            if let Some(value) = vector.get_f64(row_idx) {
                encode_f64(value, ascending, out);
            }
        }
        LogicalType::Date => {
            if let Some(value) = vector.get_i32(row_idx) {
                encode_i32(value, ascending, out);
            }
        }
        LogicalType::Time | LogicalType::Timestamp | LogicalType::TimestampTz => {
            if let Some(value) = vector.get_i64(row_idx) {
                encode_i64(value, ascending, out);
            }
        }
        LogicalType::Interval => {
            if let Some(interval) = vector.get_interval(row_idx) {
                encode_interval(interval.0, interval.1, interval.2, ascending, out);
            }
        }
        LogicalType::Decimal { precision, .. } => {
            if *precision <= 18 {
                if let Some(value) = vector.get_i64(row_idx) {
                    encode_i64(value, ascending, out);
                }
            } else if let Some(value) = vector.get_i128(row_idx) {
                encode_hugeint(value, ascending, out);
            }
        }
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb => {
            if let Some(value) = vector.get_string(row_idx) {
                encode_string(value, ascending, out);
            }
        }
        LogicalType::Blob => {
            if let Some(value) = vector.get_blob(row_idx) {
                encode_blob(value, ascending, out);
            }
        }
        _ => {
            return Err(paro_error::not_implemented(format!(
                "sort key encoding not implemented for type: {:?}",
                vector.logical_type()
            )));
        }
    }

    Ok(())
}

fn encode_field<W: KeyWriter + ?Sized>(
    vector: &Vector,
    row_idx: usize,
    field: &SortKeyFieldEncoding,
    out: &mut W,
) -> Result<()> {
    let (null_byte, valid_byte) = if field.modifiers.nulls_first {
        (NULL_FIRST_BYTE, NULL_LAST_BYTE)
    } else {
        (NULL_LAST_BYTE, NULL_FIRST_BYTE)
    };

    if vector.is_null(row_idx) {
        out.push_byte(null_byte);
        if let Some(value_len) = fixed_value_len(&field.logical_type)? {
            out.repeat_byte(0, value_len);
        }
        return Ok(());
    }

    out.push_byte(valid_byte);
    encode_value_into(vector, row_idx, field.modifiers.ascending, out)
}

pub fn encoded_column_len(
    vector: &Vector,
    row_idx: usize,
    logical_type: &LogicalType,
) -> Result<usize> {
    if vector.is_null(row_idx) {
        return Ok(1 + fixed_value_len(logical_type)?.unwrap_or(0));
    }

    let value_len = match logical_type {
        LogicalType::Boolean | LogicalType::TinyInt | LogicalType::UTinyInt => 1,
        LogicalType::SmallInt | LogicalType::USmallInt => 2,
        LogicalType::Integer | LogicalType::UInteger | LogicalType::Float | LogicalType::Date => 4,
        LogicalType::BigInt
        | LogicalType::UBigInt
        | LogicalType::Double
        | LogicalType::Time
        | LogicalType::Timestamp
        | LogicalType::TimestampTz => 8,
        LogicalType::HugeInt
        | LogicalType::UHugeInt
        | LogicalType::Uuid
        | LogicalType::Interval => 16,
        LogicalType::Decimal { precision, .. } => {
            if *precision <= 18 {
                8
            } else {
                16
            }
        }
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb => vector
            .get_string(row_idx)
            .map(|value| value.len() + 1)
            .unwrap_or(1),
        LogicalType::Blob => {
            let escaped_len = vector
                .get_blob(row_idx)
                .map(|value| {
                    value
                        .iter()
                        .map(|byte| usize::from(*byte <= 1) + 1)
                        .sum::<usize>()
                })
                .unwrap_or(0);
            escaped_len + 1
        }
        _ => {
            return Err(paro_error::not_implemented(format!(
                "sort key encoding not implemented for type: {:?}",
                logical_type
            )));
        }
    };

    Ok(1 + value_len)
}

fn fixed_value_len(logical_type: &LogicalType) -> Result<Option<usize>> {
    Ok(match logical_type {
        LogicalType::Boolean | LogicalType::TinyInt | LogicalType::UTinyInt => Some(1),
        LogicalType::SmallInt | LogicalType::USmallInt => Some(2),
        LogicalType::Integer | LogicalType::UInteger | LogicalType::Float | LogicalType::Date => {
            Some(4)
        }
        LogicalType::BigInt
        | LogicalType::UBigInt
        | LogicalType::Double
        | LogicalType::Time
        | LogicalType::Timestamp
        | LogicalType::TimestampTz => Some(8),
        LogicalType::HugeInt
        | LogicalType::UHugeInt
        | LogicalType::Uuid
        | LogicalType::Interval => Some(16),
        LogicalType::Decimal { precision, .. } => Some(if *precision <= 18 { 8 } else { 16 }),
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb
        | LogicalType::Blob => None,
        _ => {
            return Err(paro_error::not_implemented(format!(
                "sort key encoding not implemented for type: {:?}",
                logical_type
            )));
        }
    })
}

#[inline]
fn encode_bool<W: KeyWriter + ?Sized>(value: bool, ascending: bool, out: &mut W) {
    let byte = if value { 1u8 } else { 0u8 };
    out.push_byte(if ascending { byte } else { !byte });
}

#[inline]
fn encode_i8<W: KeyWriter + ?Sized>(value: i8, ascending: bool, out: &mut W) {
    let encoded = (value as u8) ^ 0x80;
    out.push_byte(if ascending { encoded } else { !encoded });
}

#[inline]
fn encode_i16<W: KeyWriter + ?Sized>(value: i16, ascending: bool, out: &mut W) {
    encode_ordered_bytes(&(value as u16 ^ 0x8000).to_be_bytes(), ascending, out);
}

#[inline]
fn encode_i32<W: KeyWriter + ?Sized>(value: i32, ascending: bool, out: &mut W) {
    encode_ordered_bytes(&(value as u32 ^ 0x8000_0000).to_be_bytes(), ascending, out);
}

#[inline]
fn encode_i64<W: KeyWriter + ?Sized>(value: i64, ascending: bool, out: &mut W) {
    encode_ordered_bytes(
        &(value as u64 ^ 0x8000_0000_0000_0000).to_be_bytes(),
        ascending,
        out,
    );
}

#[inline]
fn encode_hugeint<W: KeyWriter + ?Sized>(value: i128, ascending: bool, out: &mut W) {
    encode_i64((value >> 64) as i64, ascending, out);
    encode_u64(value as u64, ascending, out);
}

#[inline]
fn encode_u8<W: KeyWriter + ?Sized>(value: u8, ascending: bool, out: &mut W) {
    out.push_byte(if ascending { value } else { !value });
}

#[inline]
fn encode_u16<W: KeyWriter + ?Sized>(value: u16, ascending: bool, out: &mut W) {
    encode_ordered_bytes(&value.to_be_bytes(), ascending, out);
}

#[inline]
fn encode_u32<W: KeyWriter + ?Sized>(value: u32, ascending: bool, out: &mut W) {
    encode_ordered_bytes(&value.to_be_bytes(), ascending, out);
}

#[inline]
fn encode_u64<W: KeyWriter + ?Sized>(value: u64, ascending: bool, out: &mut W) {
    encode_ordered_bytes(&value.to_be_bytes(), ascending, out);
}

#[inline]
fn encode_uhugeint<W: KeyWriter + ?Sized>(value: u128, ascending: bool, out: &mut W) {
    encode_u64((value >> 64) as u64, ascending, out);
    encode_u64(value as u64, ascending, out);
}

fn encode_f32<W: KeyWriter + ?Sized>(value: f32, ascending: bool, out: &mut W) {
    let encoded = if value == 0.0 {
        0x8000_0000u32
    } else if value.is_nan() {
        u32::MAX
    } else if value > f32::MAX {
        u32::MAX - 1
    } else if value < -f32::MAX {
        0u32
    } else {
        let bits = value.to_bits();
        if (bits & 0x8000_0000) == 0 {
            bits | 0x8000_0000
        } else {
            !bits
        }
    };
    encode_ordered_bytes(&encoded.to_be_bytes(), ascending, out);
}

fn encode_f64<W: KeyWriter + ?Sized>(value: f64, ascending: bool, out: &mut W) {
    let encoded = if value == 0.0 {
        0x8000_0000_0000_0000u64
    } else if value.is_nan() {
        u64::MAX
    } else if value > f64::MAX {
        u64::MAX - 1
    } else if value < -f64::MAX {
        0u64
    } else {
        let bits = value.to_bits();
        if bits < 0x8000_0000_0000_0000 {
            bits + 0x8000_0000_0000_0000
        } else {
            !bits
        }
    };
    encode_ordered_bytes(&encoded.to_be_bytes(), ascending, out);
}

#[inline]
fn encode_interval<W: KeyWriter + ?Sized>(
    months: i32,
    days: i32,
    micros: i64,
    ascending: bool,
    out: &mut W,
) {
    encode_i32(months, ascending, out);
    encode_i32(days, ascending, out);
    encode_i64(micros, ascending, out);
}

fn encode_string<W: KeyWriter + ?Sized>(value: &str, ascending: bool, out: &mut W) {
    if ascending {
        extend_transformed_bytes(value.as_bytes(), out, |byte| byte.wrapping_add(1));
        out.push_byte(STRING_DELIMITER);
    } else {
        extend_transformed_bytes(value.as_bytes(), out, |byte| !byte.wrapping_add(1));
        out.push_byte(!STRING_DELIMITER);
    }
}

fn encode_blob<W: KeyWriter + ?Sized>(value: &[u8], ascending: bool, out: &mut W) {
    let mut scratch = [0u8; TRANSFORM_BUFFER_LEN];
    let mut written = 0usize;
    if ascending {
        for &byte in value {
            if byte <= 1 {
                if written + 2 > scratch.len() {
                    flush_transformed_bytes(out, &scratch, &mut written);
                }
                scratch[written] = BLOB_ESCAPE_CHARACTER;
                scratch[written + 1] = byte;
                written += 2;
            } else {
                if written == scratch.len() {
                    flush_transformed_bytes(out, &scratch, &mut written);
                }
                scratch[written] = byte;
                written += 1;
            }
        }
        flush_transformed_bytes(out, &scratch, &mut written);
        out.push_byte(STRING_DELIMITER);
    } else {
        for &byte in value {
            if byte <= 1 {
                if written + 2 > scratch.len() {
                    flush_transformed_bytes(out, &scratch, &mut written);
                }
                scratch[written] = !BLOB_ESCAPE_CHARACTER;
                scratch[written + 1] = !byte;
                written += 2;
            } else {
                if written == scratch.len() {
                    flush_transformed_bytes(out, &scratch, &mut written);
                }
                scratch[written] = !byte;
                written += 1;
            }
        }
        flush_transformed_bytes(out, &scratch, &mut written);
        out.push_byte(!STRING_DELIMITER);
    }
}

#[inline]
fn encode_ordered_bytes<W: KeyWriter + ?Sized>(bytes: &[u8], ascending: bool, out: &mut W) {
    if ascending {
        out.extend_bytes(bytes);
    } else {
        for &byte in bytes {
            out.push_byte(!byte);
        }
    }
}

fn extend_transformed_bytes<W, F>(bytes: &[u8], out: &mut W, mut transform: F)
where
    W: KeyWriter + ?Sized,
    F: FnMut(u8) -> u8,
{
    let mut scratch = [0u8; TRANSFORM_BUFFER_LEN];
    for chunk in bytes.chunks(TRANSFORM_BUFFER_LEN) {
        for (idx, &byte) in chunk.iter().enumerate() {
            scratch[idx] = transform(byte);
        }
        out.extend_bytes(&scratch[..chunk.len()]);
    }
}

#[inline]
fn flush_transformed_bytes<W: KeyWriter + ?Sized>(
    out: &mut W,
    scratch: &[u8; TRANSFORM_BUFFER_LEN],
    written: &mut usize,
) {
    if *written > 0 {
        out.extend_bytes(&scratch[..*written]);
        *written = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_value::Value;

    fn encode_single(vector: Vector, modifiers: OrderModifiers) -> Vec<Vec<u8>> {
        let count = vector.len();
        let chunk =
            crate::test_utils::test_chunk_from_arc_vectors(vec![std::sync::Arc::new(vector)]);
        let encoding = SortKeyEncoding::new(
            vec![chunk.column(0).unwrap().logical_type().clone()],
            vec![modifiers],
        )
        .unwrap();

        (0..count)
            .map(|row_idx| encoding.encode_row(&chunk, row_idx, &[0]).unwrap())
            .collect()
    }

    #[test]
    fn parses_order_modifiers() {
        assert_eq!(
            OrderModifiers::parse("ASC NULLS LAST").unwrap(),
            OrderModifiers::new(true, false)
        );
        assert_eq!(
            OrderModifiers::parse("desc_nulls_first").unwrap(),
            OrderModifiers::new(false, true)
        );
    }

    #[test]
    fn encodes_integers_and_strings() {
        let mut ints = crate::test_utils::test_vector(LogicalType::Integer);
        ints.set_i32(0, 1);
        ints.set_i32(1, 2);
        ints.set_i32(2, 3);
        ints.set_count(3);
        let int_keys = encode_single(ints, OrderModifiers::new(true, false));
        assert!(int_keys[0] < int_keys[1]);
        assert!(int_keys[1] < int_keys[2]);

        let mut strings = crate::test_utils::test_vector(LogicalType::Varchar);
        strings.set_string(0, "apple");
        strings.set_string(1, "banana");
        strings.set_string(2, "cherry");
        strings.set_count(3);
        let string_keys = encode_single(strings, OrderModifiers::new(true, false));
        assert!(string_keys[0] < string_keys[1]);
        assert!(string_keys[1] < string_keys[2]);
    }

    #[test]
    fn respects_descending_and_null_order() {
        let mut vec = crate::test_utils::test_vector(LogicalType::Integer);
        vec.set_i32(0, 1);
        vec.set_null(1, true);
        vec.set_i32(2, 3);
        vec.set_count(3);

        let desc = encode_single(vec.clone(), OrderModifiers::new(false, false));
        assert!(desc[0] > desc[2]);

        let nulls_first = encode_single(vec, OrderModifiers::new(true, true));
        assert!(nulls_first[1] < nulls_first[0]);
        assert!(nulls_first[0] < nulls_first[2]);
    }

    #[test]
    fn encodes_decimal_widths_correctly() {
        let small_ty = LogicalType::Decimal {
            precision: 6,
            scale: 2,
        };
        let mut small = crate::test_utils::test_vector(small_ty.clone());
        small.set_value(0, &Value::Decimal(-125, 6, 2));
        small.set_value(1, &Value::Decimal(250, 6, 2));
        small.set_count(2);

        let large_ty = LogicalType::Decimal {
            precision: 30,
            scale: 6,
        };
        let mut large = crate::test_utils::test_vector(large_ty.clone());
        large.set_value(0, &Value::Decimal(i128::MIN / 8, 30, 6));
        large.set_value(1, &Value::Decimal(i128::MAX / 8, 30, 6));
        large.set_count(2);

        let encoding = SortKeyEncoding::new(
            vec![small_ty, large_ty],
            vec![
                OrderModifiers::new(true, false),
                OrderModifiers::new(true, false),
            ],
        )
        .unwrap();
        assert_eq!(encoding.fixed_key_len(), Some(26));
        assert_eq!(encoding.slot_size(), 26);

        let chunk =
            crate::test_utils::test_chunk_from_arc_vectors(vec![small.into(), large.into()]);
        let left = encoding.encode_row(&chunk, 0, &[0, 1]).unwrap();
        let right = encoding.encode_row(&chunk, 1, &[0, 1]).unwrap();
        assert!(left < right);
    }

    #[test]
    fn computes_variable_lengths_for_blob_escape() {
        let mut vec = crate::test_utils::test_vector(LogicalType::Blob);
        vec.set_blob(0, &[0, 1, 2, 3]);
        vec.set_count(1);

        assert_eq!(
            encoded_column_len(&vec, 0, &LogicalType::Blob).unwrap(),
            1 + 7
        );
    }

    #[test]
    fn slot_size_tracks_fixed_and_variable_keys() {
        let fixed = SortKeyEncoding::new(
            vec![LogicalType::Integer, LogicalType::BigInt],
            vec![
                OrderModifiers::new(true, false),
                OrderModifiers::new(true, false),
            ],
        )
        .unwrap();
        assert_eq!(fixed.fixed_key_len(), Some(1 + 4 + 1 + 8));
        assert_eq!(fixed.slot_size(), 14);
        assert_eq!(fixed.inline_prefix_len(), 14);

        let variable = SortKeyEncoding::new(
            vec![LogicalType::Integer, LogicalType::Varchar],
            vec![
                OrderModifiers::new(true, false),
                OrderModifiers::new(true, false),
            ],
        )
        .unwrap();
        assert!(variable.fixed_key_len().is_none());
        assert_eq!(variable.slot_size(), VARIABLE_SLOT_SIZE);
        assert_eq!(variable.inline_prefix_len(), VARIABLE_INLINE_PREFIX_LEN);
    }

    #[test]
    fn fixed_width_keys_keep_null_field_padding() {
        let mut ints = crate::test_utils::test_vector(LogicalType::Integer);
        ints.set_null(0, true);
        ints.set_i32(1, 7);
        ints.set_count(2);

        let encoding = SortKeyEncoding::new(
            vec![LogicalType::Integer],
            vec![OrderModifiers::new(true, true)],
        )
        .unwrap();
        let chunk = crate::test_utils::test_chunk_from_arc_vectors(vec![ints.into()]);

        let null_key = encoding.encode_row(&chunk, 0, &[0]).unwrap();
        let value_key = encoding.encode_row(&chunk, 1, &[0]).unwrap();

        assert_eq!(encoding.fixed_key_len(), Some(5));
        assert_eq!(encoding.encoded_len(&chunk, 0, &[0]).unwrap(), 5);
        assert_eq!(null_key.len(), 5);
        assert_eq!(value_key.len(), 5);
        assert!(null_key < value_key);
    }

    #[test]
    fn variable_keys_account_for_fixed_width_null_padding() {
        let mut groups = crate::test_utils::test_vector(LogicalType::Integer);
        groups.set_i32(0, 1);
        groups.set_i32(1, 1);
        groups.set_count(2);

        let mut labels = crate::test_utils::test_vector(LogicalType::Varchar);
        labels.set_null(0, true);
        labels.set_string(1, "gamma");
        labels.set_count(2);

        let mut ids = crate::test_utils::test_vector(LogicalType::Integer);
        ids.set_i32(0, 2);
        ids.set_i32(1, 5);
        ids.set_count(2);

        let chunk = crate::test_utils::test_chunk_from_arc_vectors(vec![
            groups.into(),
            labels.into(),
            ids.into(),
        ]);
        let encoding = SortKeyEncoding::new(
            vec![
                LogicalType::Integer,
                LogicalType::Varchar,
                LogicalType::Integer,
            ],
            vec![
                OrderModifiers::new(true, false),
                OrderModifiers::new(false, true),
                OrderModifiers::new(true, false),
            ],
        )
        .unwrap();

        let expected = encoding.encode_row(&chunk, 0, &[0, 1, 2]).unwrap();
        assert_eq!(
            encoding.encoded_len(&chunk, 0, &[0, 1, 2]).unwrap(),
            expected.len()
        );

        let inline_len = expected.len().min(encoding.inline_prefix_len());
        let mut inline_prefix = vec![0u8; inline_len];
        let mut overflow = vec![0u8; expected.len() - inline_len];
        encoding
            .encode_row_into_parts(&chunk, 0, &[0, 1, 2], &mut inline_prefix, &mut overflow)
            .unwrap();

        let mut actual = inline_prefix;
        actual.extend_from_slice(&overflow);
        assert_eq!(actual, expected);
    }

    #[test]
    fn encode_row_into_parts_matches_row_bytes_for_variable_keys() {
        let mut ints = crate::test_utils::test_vector(LogicalType::Integer);
        ints.set_i32(0, 42);
        ints.set_count(1);

        let mut strings = crate::test_utils::test_vector(LogicalType::Varchar);
        strings.set_string(0, "split-writer-overflow");
        strings.set_count(1);

        let chunk =
            crate::test_utils::test_chunk_from_arc_vectors(vec![ints.into(), strings.into()]);
        let encoding = SortKeyEncoding::new(
            vec![LogicalType::Integer, LogicalType::Varchar],
            vec![
                OrderModifiers::new(true, false),
                OrderModifiers::new(true, false),
            ],
        )
        .unwrap();

        let expected = encoding.encode_row(&chunk, 0, &[0, 1]).unwrap();
        let inline_len = expected.len().min(encoding.inline_prefix_len());
        let mut inline_prefix = vec![0u8; inline_len];
        let mut overflow = vec![0u8; expected.len() - inline_len];

        encoding
            .encode_row_into_parts(&chunk, 0, &[0, 1], &mut inline_prefix, &mut overflow)
            .unwrap();

        let mut actual = inline_prefix;
        actual.extend_from_slice(&overflow);
        assert_eq!(actual, expected);
    }

    #[test]
    fn compare_keys_is_lexicographic() {
        assert_eq!(compare_keys(b"abc", b"abc"), Ordering::Equal);
        assert_eq!(compare_keys(b"abc", b"abd"), Ordering::Less);
        assert_eq!(compare_keys(b"abd", b"abc"), Ordering::Greater);
    }
}
