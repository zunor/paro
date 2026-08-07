// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Lossless physical codecs for aggregate group keys.
//!
//! Aggregate hash tables own materialized keys and can therefore use a
//! representation different from the SQL-visible value. Codecs are selected
//! from conservative planner statistics, applied once per input batch, and
//! reversed only when finalized groups leave the operator.

use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::{InlineString, LogicalType};
use paro_common::vector::{VarlenView, Vector};

use crate::physical::specs::{AggregateSpec, GroupKeyEncoding};

#[derive(Debug)]
pub(crate) struct GroupKeyEncoder {
    encodings: Box<[GroupKeyEncoding]>,
    logical_types: Box<[LogicalType]>,
    encoded: Chunk,
}

impl GroupKeyEncoder {
    pub(crate) fn try_new(
        spec: &AggregateSpec,
        capacity: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        let logical_types = logical_group_types(spec);
        let physical_types = physical_group_types(spec)?;
        Ok(Self {
            encodings: spec.group_key_encodings.clone(),
            logical_types: logical_types.into_boxed_slice(),
            encoded: Chunk::try_initialize(&physical_types, capacity.max(1), allocator)?,
        })
    }

    /// Encode group columns referenced from an aggregate payload.
    ///
    /// Identity columns remain zero-copy references. Packed columns reuse the
    /// encoder-owned vectors, so callers must consume the returned batch before
    /// invoking this method again.
    pub(crate) fn encode_payload<'a>(
        &'a mut self,
        payload: &Chunk,
        group_refs: &[usize],
    ) -> Result<&'a Chunk> {
        if group_refs.len() != self.encodings.len() {
            return Err(paro_error::internal(format!(
                "group key reference width mismatch: references={}, encodings={}",
                group_refs.len(),
                self.encodings.len()
            )));
        }
        if self.encoded.capacity() < payload.size() {
            self.encoded = Chunk::try_initialize(
                &self
                    .encodings
                    .iter()
                    .zip(self.logical_types.iter())
                    .map(|(encoding, logical)| physical_type(encoding, logical))
                    .collect::<Result<Vec<_>>>()?,
                payload.size(),
                payload.allocator().clone(),
            )?;
        }

        for (group_idx, ((encoding, logical_type), &payload_idx)) in self
            .encodings
            .iter()
            .zip(self.logical_types.iter())
            .zip(group_refs)
            .enumerate()
        {
            let source = payload.column(payload_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "aggregate group payload column not found: group_idx={group_idx}, payload_idx={payload_idx}"
                ))
            })?;
            if source.logical_type() != logical_type {
                return Err(paro_error::internal(format!(
                    "aggregate logical group type mismatch at index {group_idx}: expected={logical_type:?}, actual={:?}",
                    source.logical_type()
                )));
            }
            match encoding {
                GroupKeyEncoding::Identity => {
                    self.encoded.data[group_idx] = Arc::clone(source);
                }
                GroupKeyEncoding::PackedString {
                    physical_type,
                    max_length,
                } => {
                    encode_packed_strings(
                        source,
                        physical_type,
                        *max_length,
                        payload.size(),
                        &mut self.encoded.data[group_idx],
                    )?;
                }
                GroupKeyEncoding::OffsetInteger {
                    physical_type,
                    minimum,
                } => {
                    encode_offset_integers(
                        source,
                        logical_type,
                        physical_type,
                        *minimum,
                        payload.size(),
                        &mut self.encoded.data[group_idx],
                    )?;
                }
            }
        }
        self.encoded.try_set_cardinality(payload.size())?;
        Ok(&self.encoded)
    }
}

pub(crate) fn has_encoded_group_keys(spec: &AggregateSpec) -> bool {
    spec.group_key_encodings
        .iter()
        .any(|encoding| !matches!(encoding, GroupKeyEncoding::Identity))
}

pub(crate) fn logical_group_types(spec: &AggregateSpec) -> Vec<LogicalType> {
    spec.groups
        .iter()
        .map(|group| group.return_type())
        .collect()
}

pub(crate) fn physical_group_types(spec: &AggregateSpec) -> Result<Vec<LogicalType>> {
    let logical_types = logical_group_types(spec);
    if spec.group_key_encodings.len() != logical_types.len() {
        return Err(paro_error::internal(format!(
            "aggregate group-key encoding width mismatch: encodings={}, groups={}",
            spec.group_key_encodings.len(),
            logical_types.len()
        )));
    }
    spec.group_key_encodings
        .iter()
        .zip(logical_types.iter())
        .map(|(encoding, logical)| physical_type(encoding, logical))
        .collect()
}

pub(crate) fn decode_group_columns(
    spec: &AggregateSpec,
    source: &Chunk,
    target: &mut Chunk,
) -> Result<()> {
    let logical_types = logical_group_types(spec);
    if source.size() > target.capacity() {
        return Err(paro_error::internal(format!(
            "aggregate group decode output is too small: rows={}, capacity={}",
            source.size(),
            target.capacity()
        )));
    }
    if source.column_count() < logical_types.len() || target.column_count() < logical_types.len() {
        return Err(paro_error::internal(format!(
            "aggregate group decode width mismatch: groups={}, source={}, target={}",
            logical_types.len(),
            source.column_count(),
            target.column_count()
        )));
    }
    target.try_set_cardinality(source.size())?;
    for (group_idx, ((encoding, logical_type), source_column)) in spec
        .group_key_encodings
        .iter()
        .zip(logical_types.iter())
        .zip(source.data.iter())
        .enumerate()
    {
        let target_column = target.column(group_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "aggregate decoded group output column not found: index={group_idx}"
            ))
        })?;
        if target_column.logical_type() != logical_type {
            return Err(paro_error::internal(format!(
                "aggregate decoded group type mismatch at index {group_idx}: expected={logical_type:?}, actual={:?}",
                target_column.logical_type()
            )));
        }
        match encoding {
            GroupKeyEncoding::Identity => {
                target.data[group_idx] = Arc::clone(source_column);
            }
            GroupKeyEncoding::PackedString {
                physical_type,
                max_length,
            } => {
                let target_column = Vector::try_make_arc_mut(&mut target.data[group_idx])?;
                decode_packed_strings(
                    source_column,
                    physical_type,
                    *max_length,
                    source.size(),
                    target_column,
                )?;
            }
            GroupKeyEncoding::OffsetInteger {
                physical_type,
                minimum,
            } => {
                let target_column = Vector::try_make_arc_mut(&mut target.data[group_idx])?;
                decode_offset_integers(
                    source_column,
                    logical_type,
                    physical_type,
                    *minimum,
                    source.size(),
                    target_column,
                )?;
            }
        }
    }
    Ok(())
}

fn physical_type(encoding: &GroupKeyEncoding, logical_type: &LogicalType) -> Result<LogicalType> {
    match encoding {
        GroupKeyEncoding::Identity => Ok(logical_type.clone()),
        GroupKeyEncoding::PackedString {
            physical_type,
            max_length,
        } => {
            if logical_type != &LogicalType::Varchar {
                return Err(paro_error::internal(format!(
                    "packed string group key requires VARCHAR, got {logical_type:?}"
                )));
            }
            validate_packed_type(physical_type, *max_length)?;
            Ok(physical_type.clone())
        }
        GroupKeyEncoding::OffsetInteger {
            physical_type,
            minimum: _,
        } => {
            validate_integer_type(logical_type)?;
            validate_unsigned_physical_type(physical_type)?;
            if physical_type.physical_size() >= logical_type.physical_size() {
                return Err(paro_error::internal(format!(
                    "offset integer group key must reduce physical width: logical={logical_type:?}, physical={physical_type:?}"
                )));
            }
            Ok(physical_type.clone())
        }
    }
}

fn validate_integer_type(logical_type: &LogicalType) -> Result<()> {
    if matches!(
        logical_type,
        LogicalType::TinyInt
            | LogicalType::SmallInt
            | LogicalType::Integer
            | LogicalType::BigInt
            | LogicalType::HugeInt
            | LogicalType::UTinyInt
            | LogicalType::USmallInt
            | LogicalType::UInteger
            | LogicalType::UBigInt
            | LogicalType::UHugeInt
    ) {
        Ok(())
    } else {
        Err(paro_error::internal(format!(
            "offset integer group key requires an integer type, got {logical_type:?}"
        )))
    }
}

fn validate_unsigned_physical_type(physical_type: &LogicalType) -> Result<()> {
    if matches!(
        physical_type,
        LogicalType::UTinyInt
            | LogicalType::USmallInt
            | LogicalType::UInteger
            | LogicalType::UBigInt
            | LogicalType::UHugeInt
    ) {
        Ok(())
    } else {
        Err(paro_error::internal(format!(
            "offset integer group key requires an unsigned physical type, got {physical_type:?}"
        )))
    }
}

fn validate_packed_type(physical_type: &LogicalType, max_length: usize) -> Result<usize> {
    let width = match physical_type {
        LogicalType::UTinyInt => 1,
        LogicalType::USmallInt => 2,
        LogicalType::UInteger => 4,
        LogicalType::UBigInt => 8,
        LogicalType::UHugeInt => 16,
        other => {
            return Err(paro_error::internal(format!(
                "unsupported packed string physical type: {other:?}"
            )));
        }
    };
    if max_length >= width {
        return Err(paro_error::internal(format!(
            "packed string bound does not fit physical type: max_length={max_length}, width={width}"
        )));
    }
    Ok(width)
}

fn encode_offset_integers(
    source: &Vector,
    logical_type: &LogicalType,
    physical_type: &LogicalType,
    minimum: i128,
    count: usize,
    target: &mut Arc<Vector>,
) -> Result<()> {
    validate_integer_type(logical_type)?;
    validate_unsigned_physical_type(physical_type)?;
    if source.logical_type() != logical_type {
        return Err(paro_error::internal(format!(
            "offset integer source type mismatch: expected={logical_type:?}, actual={:?}",
            source.logical_type()
        )));
    }
    if target.logical_type() != physical_type || target.capacity() < count {
        *target = Arc::new(Vector::try_new(
            physical_type.clone(),
            count.max(1),
            source.allocator().clone(),
        )?);
    }
    let target = Vector::try_make_arc_mut(target)?;
    match physical_type {
        LogicalType::UTinyInt => {
            encode_offset_integer_source::<u8>(source, logical_type, minimum, count, target)
        }
        LogicalType::USmallInt => {
            encode_offset_integer_source::<u16>(source, logical_type, minimum, count, target)
        }
        LogicalType::UInteger => {
            encode_offset_integer_source::<u32>(source, logical_type, minimum, count, target)
        }
        LogicalType::UBigInt => {
            encode_offset_integer_source::<u64>(source, logical_type, minimum, count, target)
        }
        LogicalType::UHugeInt => {
            encode_offset_integer_source::<u128>(source, logical_type, minimum, count, target)
        }
        other => Err(paro_error::internal(format!(
            "unsupported offset integer physical type after validation: {other:?}"
        ))),
    }
}

fn encode_offset_integer_source<T: PackedWord>(
    source: &Vector,
    logical_type: &LogicalType,
    minimum: i128,
    count: usize,
    target: &mut Vector,
) -> Result<()> {
    match logical_type {
        LogicalType::TinyInt => encode_offset_rows::<i8, T>(source, minimum, count, target),
        LogicalType::SmallInt => encode_offset_rows::<i16, T>(source, minimum, count, target),
        LogicalType::Integer => encode_offset_rows::<i32, T>(source, minimum, count, target),
        LogicalType::BigInt => encode_offset_rows::<i64, T>(source, minimum, count, target),
        LogicalType::HugeInt => encode_offset_rows::<i128, T>(source, minimum, count, target),
        LogicalType::UTinyInt => encode_offset_rows::<u8, T>(source, minimum, count, target),
        LogicalType::USmallInt => encode_offset_rows::<u16, T>(source, minimum, count, target),
        LogicalType::UInteger => encode_offset_rows::<u32, T>(source, minimum, count, target),
        LogicalType::UBigInt => encode_offset_rows::<u64, T>(source, minimum, count, target),
        LogicalType::UHugeInt => encode_offset_rows::<u128, T>(source, minimum, count, target),
        other => Err(paro_error::internal(format!(
            "unsupported offset integer logical type after validation: {other:?}"
        ))),
    }
}

fn encode_offset_rows<S: IntegerValue, T: PackedWord>(
    source: &Vector,
    minimum: i128,
    count: usize,
    target: &mut Vector,
) -> Result<()> {
    let source = source.try_decode_ref(count)?;
    let source_data = source.get_data::<S>();
    target.try_set_count(count)?;
    target.validity_mut().try_set_all_valid(count)?;
    let output = unsafe { target.flat_data_mut::<T>() };
    let validity = target.validity_mut();
    for row_idx in 0..count {
        let source_idx = source.physical_index(row_idx);
        if !source.validity().is_valid(source_idx) {
            unsafe {
                *output.add(row_idx) = T::default();
            }
            validity.try_set_null(row_idx)?;
            continue;
        }
        let value = unsafe { *source_data.add(source_idx) }
            .to_i128()
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "guaranteed integer group-key bound violated at row {row_idx}: value is not representable as i128"
                ))
            })?;
        let offset = value
            .checked_sub(minimum)
            .and_then(|offset| u128::try_from(offset).ok())
            .and_then(T::from_u128)
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "guaranteed integer group-key bound violated at row {row_idx}: value={value}, minimum={minimum}, physical_width={}",
                    T::WIDTH
                ))
            })?;
        unsafe {
            *output.add(row_idx) = offset;
        }
    }
    Ok(())
}

fn decode_offset_integers(
    source: &Vector,
    logical_type: &LogicalType,
    physical_type: &LogicalType,
    minimum: i128,
    count: usize,
    target: &mut Vector,
) -> Result<()> {
    validate_integer_type(logical_type)?;
    validate_unsigned_physical_type(physical_type)?;
    if source.logical_type() != physical_type || target.logical_type() != logical_type {
        return Err(paro_error::internal(format!(
            "offset integer decode type mismatch: source={:?}, expected={physical_type:?}, target={:?}, logical={logical_type:?}",
            source.logical_type(),
            target.logical_type()
        )));
    }
    match physical_type {
        LogicalType::UTinyInt => {
            decode_offset_integer_target::<u8>(source, logical_type, minimum, count, target)
        }
        LogicalType::USmallInt => {
            decode_offset_integer_target::<u16>(source, logical_type, minimum, count, target)
        }
        LogicalType::UInteger => {
            decode_offset_integer_target::<u32>(source, logical_type, minimum, count, target)
        }
        LogicalType::UBigInt => {
            decode_offset_integer_target::<u64>(source, logical_type, minimum, count, target)
        }
        LogicalType::UHugeInt => {
            decode_offset_integer_target::<u128>(source, logical_type, minimum, count, target)
        }
        other => Err(paro_error::internal(format!(
            "unsupported offset integer physical type after validation: {other:?}"
        ))),
    }
}

fn decode_offset_integer_target<S: PackedWord>(
    source: &Vector,
    logical_type: &LogicalType,
    minimum: i128,
    count: usize,
    target: &mut Vector,
) -> Result<()> {
    match logical_type {
        LogicalType::TinyInt => decode_offset_rows::<S, i8>(source, minimum, count, target),
        LogicalType::SmallInt => decode_offset_rows::<S, i16>(source, minimum, count, target),
        LogicalType::Integer => decode_offset_rows::<S, i32>(source, minimum, count, target),
        LogicalType::BigInt => decode_offset_rows::<S, i64>(source, minimum, count, target),
        LogicalType::HugeInt => decode_offset_rows::<S, i128>(source, minimum, count, target),
        LogicalType::UTinyInt => decode_offset_rows::<S, u8>(source, minimum, count, target),
        LogicalType::USmallInt => decode_offset_rows::<S, u16>(source, minimum, count, target),
        LogicalType::UInteger => decode_offset_rows::<S, u32>(source, minimum, count, target),
        LogicalType::UBigInt => decode_offset_rows::<S, u64>(source, minimum, count, target),
        LogicalType::UHugeInt => decode_offset_rows::<S, u128>(source, minimum, count, target),
        other => Err(paro_error::internal(format!(
            "unsupported offset integer logical type after validation: {other:?}"
        ))),
    }
}

fn decode_offset_rows<S: PackedWord, T: IntegerValue>(
    source: &Vector,
    minimum: i128,
    count: usize,
    target: &mut Vector,
) -> Result<()> {
    let source = source.try_decode_ref(count)?;
    let source_data = source.get_data::<S>();
    target.try_set_count(count)?;
    target.validity_mut().try_set_all_valid(count)?;
    let output = unsafe { target.flat_data_mut::<T>() };
    let validity = target.validity_mut();
    for row_idx in 0..count {
        let source_idx = source.physical_index(row_idx);
        if !source.validity().is_valid(source_idx) {
            unsafe {
                *output.add(row_idx) = T::default();
            }
            validity.try_set_null(row_idx)?;
            continue;
        }
        let offset = unsafe { *source_data.add(source_idx) }.to_u128();
        let value = i128::try_from(offset)
            .ok()
            .and_then(|offset| minimum.checked_add(offset))
            .and_then(T::from_i128)
            .ok_or_else(|| {
                paro_error::data_corrupted(format!(
                    "invalid offset integer group key at row {row_idx}: offset={offset}, minimum={minimum}"
                ))
            })?;
        unsafe {
            *output.add(row_idx) = value;
        }
    }
    Ok(())
}

fn encode_packed_strings(
    source: &Vector,
    physical_type: &LogicalType,
    max_length: usize,
    count: usize,
    target: &mut Arc<Vector>,
) -> Result<()> {
    let width = validate_packed_type(physical_type, max_length)?;
    if source.logical_type() != &LogicalType::Varchar {
        return Err(paro_error::internal(format!(
            "packed string source must be VARCHAR, got {:?}",
            source.logical_type()
        )));
    }
    if target.logical_type() != physical_type || target.capacity() < count {
        *target = Arc::new(Vector::try_new(
            physical_type.clone(),
            count.max(1),
            source.allocator().clone(),
        )?);
    }
    let target = Vector::try_make_arc_mut(target)?;
    let view = source.try_to_varlen_view(count)?;
    match width {
        1 => encode_packed_rows::<u8>(&view, count, max_length, target)?,
        2 => encode_packed_rows::<u16>(&view, count, max_length, target)?,
        4 => encode_packed_rows::<u32>(&view, count, max_length, target)?,
        8 => encode_packed_rows::<u64>(&view, count, max_length, target)?,
        16 => encode_packed_rows::<u128>(&view, count, max_length, target)?,
        _ => {
            return Err(paro_error::internal(format!(
                "invalid packed string width: {width}"
            )));
        }
    }
    Ok(())
}

fn encode_packed_rows<T: PackedWord>(
    source: &VarlenView<'_>,
    count: usize,
    max_length: usize,
    target: &mut Vector,
) -> Result<()> {
    target.try_set_count(count)?;
    target.validity_mut().try_set_all_valid(count)?;
    let output = unsafe { target.flat_data_mut::<T>() };
    let validity = target.validity_mut();
    for row_idx in 0..count {
        if !source.is_valid(row_idx) {
            unsafe {
                *output.add(row_idx) = T::default();
            }
            validity.try_set_null(row_idx)?;
            continue;
        }
        let value = source.get_inline_string(row_idx);
        let bytes = value.as_bytes();
        if bytes.len() > max_length {
            return Err(paro_error::data_corrupted(format!(
                "group key exceeded its planned string bound: row={row_idx}, length={}, max_length={max_length}",
                bytes.len()
            )));
        }
        unsafe {
            *output.add(row_idx) = T::pack(bytes)?;
        }
    }
    Ok(())
}

fn decode_packed_strings(
    source: &Vector,
    physical_type: &LogicalType,
    max_length: usize,
    count: usize,
    target: &mut Vector,
) -> Result<()> {
    let width = validate_packed_type(physical_type, max_length)?;
    if source.logical_type() != physical_type || target.logical_type() != &LogicalType::Varchar {
        return Err(paro_error::internal(format!(
            "packed string decode type mismatch: source={:?}, expected={physical_type:?}, target={:?}",
            source.logical_type(),
            target.logical_type()
        )));
    }
    match width {
        1 => decode_packed_rows::<u8>(source, count, max_length, target)?,
        2 => decode_packed_rows::<u16>(source, count, max_length, target)?,
        4 => decode_packed_rows::<u32>(source, count, max_length, target)?,
        8 => decode_packed_rows::<u64>(source, count, max_length, target)?,
        16 => decode_packed_rows::<u128>(source, count, max_length, target)?,
        _ => {
            return Err(paro_error::internal(format!(
                "invalid packed string width: {width}"
            )));
        }
    }
    Ok(())
}

fn decode_packed_rows<T: PackedWord>(
    source: &Vector,
    count: usize,
    max_length: usize,
    target: &mut Vector,
) -> Result<()> {
    let source = source.try_decode_ref(count)?;
    let source_data = source.get_data::<T>();
    {
        let (entries, validity, heap) = target.begin_varlen_write(count);
        validity.try_set_all_valid(count)?;
        for row_idx in 0..count {
            let source_idx = source.physical_index(row_idx);
            if !source.validity().is_valid(source_idx) {
                unsafe {
                    *entries.add(row_idx) = InlineString::from_bytes(&[]);
                }
                validity.try_set_null(row_idx)?;
                continue;
            }
            let bytes = unsafe { (*source_data.add(source_idx)).unpack() };
            let length = bytes[T::WIDTH - 1] as usize;
            if length > max_length || length >= T::WIDTH {
                return Err(paro_error::data_corrupted(format!(
                    "invalid packed group key length: row={row_idx}, length={length}, max_length={max_length}, width={}",
                    T::WIDTH
                )));
            }
            let value = &bytes[..length];
            std::str::from_utf8(value).map_err(|error| {
                paro_error::data_corrupted(format!(
                    "packed VARCHAR group key is not UTF-8 at row {row_idx}: {error}"
                ))
            })?;
            unsafe {
                *entries.add(row_idx) = heap.try_add_blob(value)?;
            }
        }
    }
    target.try_set_count(count)
}

trait PackedWord: Copy + Default {
    const WIDTH: usize;

    fn from_bytes(bytes: [u8; 16]) -> Self;
    fn to_bytes(self) -> [u8; 16];
    fn from_u128(value: u128) -> Option<Self>;
    fn to_u128(self) -> u128;

    fn pack(value: &[u8]) -> Result<Self> {
        let length = u8::try_from(value.len()).map_err(|_| {
            paro_error::internal(format!(
                "packed string length exceeds byte metadata: {}",
                value.len()
            ))
        })?;
        debug_assert!(value.len() < Self::WIDTH);
        let mut bytes = [0u8; 16];
        bytes[..value.len()].copy_from_slice(value);
        bytes[Self::WIDTH - 1] = length;
        Ok(Self::from_bytes(bytes))
    }

    fn unpack(self) -> [u8; 16] {
        self.to_bytes()
    }
}

macro_rules! impl_packed_word {
    ($ty:ty, $width:expr) => {
        impl PackedWord for $ty {
            const WIDTH: usize = $width;

            fn from_bytes(bytes: [u8; 16]) -> Self {
                <$ty>::from_be_bytes(bytes[..$width].try_into().expect("fixed packed width"))
            }

            fn to_bytes(self) -> [u8; 16] {
                let mut bytes = [0u8; 16];
                bytes[..$width].copy_from_slice(&self.to_be_bytes());
                bytes
            }

            fn from_u128(value: u128) -> Option<Self> {
                <$ty>::try_from(value).ok()
            }

            fn to_u128(self) -> u128 {
                self as u128
            }
        }
    };
}

impl_packed_word!(u8, 1);
impl_packed_word!(u16, 2);
impl_packed_word!(u32, 4);
impl_packed_word!(u64, 8);
impl_packed_word!(u128, 16);

trait IntegerValue: Copy + Default {
    fn to_i128(self) -> Option<i128>;
    fn from_i128(value: i128) -> Option<Self>;
}

macro_rules! impl_signed_integer_value {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl IntegerValue for $ty {
                fn to_i128(self) -> Option<i128> {
                    Some(self as i128)
                }

                fn from_i128(value: i128) -> Option<Self> {
                    <$ty>::try_from(value).ok()
                }
            }
        )+
    };
}

macro_rules! impl_unsigned_integer_value {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl IntegerValue for $ty {
                fn to_i128(self) -> Option<i128> {
                    i128::try_from(self).ok()
                }

                fn from_i128(value: i128) -> Option<Self> {
                    <$ty>::try_from(value).ok()
                }
            }
        )+
    };
}

impl_signed_integer_value!(i8, i16, i32, i64, i128);
impl_unsigned_integer_value!(u8, u16, u32, u64, u128);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_common::chunk::Chunk;
    use paro_common::test_utils::{
        test_allocator, test_i32_vector_with_allocator, test_string_vector_with_allocator,
    };
    use paro_common::types::LogicalType;

    use super::{
        decode_offset_integers, decode_packed_strings, encode_offset_integers,
        encode_packed_strings,
    };

    #[test]
    fn packed_string_roundtrips_prefixes_and_embedded_zeroes() {
        let allocator = test_allocator();
        let source =
            test_string_vector_with_allocator(&["", "a", "a\0", "Brand#45"], allocator.clone());
        let mut encoded = Arc::new(
            paro_common::vector::Vector::try_new(LogicalType::UHugeInt, 4, allocator.clone())
                .unwrap(),
        );
        encode_packed_strings(&source, &LogicalType::UHugeInt, 8, 4, &mut encoded).unwrap();

        let mut output = Chunk::try_initialize(&[LogicalType::Varchar], 4, allocator).unwrap();
        decode_packed_strings(
            encoded.as_ref(),
            &LogicalType::UHugeInt,
            8,
            4,
            Arc::get_mut(&mut output.data[0]).unwrap(),
        )
        .unwrap();
        assert_eq!(output.column(0).unwrap().get_string(0), Some(""));
        assert_eq!(output.column(0).unwrap().get_string(1), Some("a"));
        assert_eq!(output.column(0).unwrap().get_string(2), Some("a\0"));
        assert_eq!(output.column(0).unwrap().get_string(3), Some("Brand#45"));
    }

    #[test]
    fn offset_integer_roundtrips_negative_domain_and_reused_validity() {
        let allocator = test_allocator();
        let mut source = test_i32_vector_with_allocator(&[-5, 250, 0, 42], allocator.clone());
        source.set_null(3, true);
        let mut encoded = Arc::new(
            paro_common::vector::Vector::try_new(LogicalType::UTinyInt, 4, allocator.clone())
                .unwrap(),
        );
        encode_offset_integers(
            &source,
            &LogicalType::Integer,
            &LogicalType::UTinyInt,
            -5,
            4,
            &mut encoded,
        )
        .unwrap();
        assert_eq!(encoded.get_u8(0), Some(0));
        assert_eq!(encoded.get_u8(1), Some(255));
        assert_eq!(encoded.get_u8(2), Some(5));
        assert!(encoded.is_null(3));

        let mut output =
            paro_common::vector::Vector::try_new(LogicalType::Integer, 4, allocator.clone())
                .unwrap();
        decode_offset_integers(
            encoded.as_ref(),
            &LogicalType::Integer,
            &LogicalType::UTinyInt,
            -5,
            4,
            &mut output,
        )
        .unwrap();
        assert_eq!(output.get_i32(0), Some(-5));
        assert_eq!(output.get_i32(1), Some(250));
        assert_eq!(output.get_i32(2), Some(0));
        assert!(output.is_null(3));

        let all_valid = test_i32_vector_with_allocator(&[1, 2, 3, 4], allocator);
        encode_offset_integers(
            &all_valid,
            &LogicalType::Integer,
            &LogicalType::UTinyInt,
            -5,
            4,
            &mut encoded,
        )
        .unwrap();
        assert_eq!(encoded.get_u8(3), Some(9));
    }
}
