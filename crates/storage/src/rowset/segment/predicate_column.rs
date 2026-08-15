// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical column representations used by compiled storage predicates.

use std::sync::Arc;

use bytes::Bytes;
use paro_common::allocator::Allocator;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use std::ops::Range;

use crate::codec::vector_decoder;
use crate::rowset::column::ColumnBatch;
use crate::rowset::encoding::BinaryPlainPageDecoder;
use crate::rowset::encoding::BinaryPlainPageSlice;
use crate::rowset::BatchRowOrdinal;

/// The least materialized representation accepted by every predicate that
/// references a column. A decoded consumer dominates typed consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PredicateColumnAccess {
    Unused,
    Typed { raw_width: Option<usize> },
    Decoded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PredicateColumnReuse {
    Fixed { width: usize },
    Varlen,
}

impl PredicateColumnAccess {
    pub(super) fn require_typed(&mut self, raw_width: Option<usize>) -> Result<()> {
        match *self {
            Self::Unused => *self = Self::Typed { raw_width },
            Self::Typed {
                raw_width: existing,
            } if existing != raw_width => {
                return Err(paro_error::internal(format!(
                    "Predicate column compiled with conflicting physical layouts {existing:?} and {raw_width:?}"
                )));
            }
            Self::Typed { .. } | Self::Decoded => {}
        }
        Ok(())
    }

    pub(super) fn require_decoded(&mut self) {
        *self = Self::Decoded;
    }

    pub(super) fn raw_width(self) -> Option<usize> {
        match self {
            Self::Typed {
                raw_width: Some(width),
            } => Some(width),
            Self::Unused | Self::Typed { raw_width: None } | Self::Decoded => None,
        }
    }
}

/// A validated view over a storage dictionary and its row codes.
///
/// Construction validates every non-null code and every dictionary entry.
/// Predicate kernels can therefore use unchecked fixed-width loads in their
/// hot loops without making memory safety depend on persisted data validity.
pub(super) struct StorageDictionaryPredicateBatch {
    encoded_dictionary: Bytes,
    dictionary: BinaryPlainPageDecoder,
    codes: Bytes,
    nulls: Option<Bytes>,
    rows: usize,
    utf8_verified: bool,
}

/// Validated row boundaries over a plain varlen column batch.
///
/// The storage batch remains in canonical `[u32 length][bytes]` form. Predicate
/// evaluation borrows values directly from it, and late materialization copies
/// only selected encoded rows into the output batch.
pub(super) struct RawVarlenPredicateBatch {
    source: RawVarlenSource,
    nulls: Option<Bytes>,
    utf8_verified: bool,
}

enum RawVarlenSource {
    LengthPrefixed { data: Bytes, row_ends: Box<[u32]> },
    BinaryPlain(BinaryPlainPageSlice),
}

impl RawVarlenPredicateBatch {
    fn try_new(batch: ColumnBatch, logical_type: &LogicalType, rows: usize) -> Result<Self> {
        if batch.storage_dictionary.is_some() {
            return Err(paro_error::internal(
                "plain varlen predicate received a storage dictionary",
            ));
        }
        if batch.nulls.as_ref().is_some_and(|nulls| nulls.len() < rows) {
            return Err(paro_error::data_corrupted(
                "Varlen predicate null map is shorter than the batch",
            ));
        }
        let utf8_type = match logical_type {
            LogicalType::Varchar => true,
            LogicalType::Blob => false,
            other => {
                return Err(paro_error::internal(format!(
                    "raw varlen predicate does not support {other:?}"
                )));
            }
        };
        let validate_utf8 = utf8_type && !batch.has_verified_utf8();

        if let Some(storage) = batch.storage_binary_plain {
            if storage.rows() != rows {
                return Err(paro_error::data_corrupted(
                    "BinaryPlain predicate batch row count mismatch",
                ));
            }
            if validate_utf8 {
                for row_idx in 0..rows {
                    let value = storage.row_value_ref(row_idx).ok_or_else(|| {
                        paro_error::data_corrupted("BinaryPlain predicate row is missing")
                    })?;
                    std::str::from_utf8(value).map_err(|_| {
                        paro_error::data_corrupted(
                            "BinaryPlain predicate VARCHAR is not valid UTF-8",
                        )
                    })?;
                }
            }
            return Ok(Self {
                source: RawVarlenSource::BinaryPlain(storage),
                nulls: batch.nulls,
                utf8_verified: utf8_type,
            });
        }

        let mut offset = 0usize;
        let mut row_ends = Vec::with_capacity(rows);
        for _ in 0..rows {
            let length_end = offset
                .checked_add(std::mem::size_of::<u32>())
                .ok_or_else(|| {
                    paro_error::data_corrupted("Varlen predicate length offset overflow")
                })?;
            let length_bytes = batch.data.get(offset..length_end).ok_or_else(|| {
                paro_error::data_corrupted("Varlen predicate length prefix is truncated")
            })?;
            let length = u32::from_le_bytes(
                length_bytes
                    .try_into()
                    .expect("varlen length slice was checked"),
            ) as usize;
            let value_end = length_end.checked_add(length).ok_or_else(|| {
                paro_error::data_corrupted("Varlen predicate value offset overflow")
            })?;
            let value = batch.data.get(length_end..value_end).ok_or_else(|| {
                paro_error::data_corrupted("Varlen predicate value exceeds the batch")
            })?;
            if validate_utf8 {
                std::str::from_utf8(value).map_err(|_| {
                    paro_error::data_corrupted("Varlen predicate VARCHAR is not valid UTF-8")
                })?;
            }
            row_ends.push(u32::try_from(value_end).map_err(|_| {
                paro_error::data_corrupted("Varlen predicate batch exceeds u32 offsets")
            })?);
            offset = value_end;
        }
        if offset != batch.data.len() {
            return Err(paro_error::data_corrupted(
                "Varlen predicate batch contains trailing bytes",
            ));
        }

        Ok(Self {
            source: RawVarlenSource::LengthPrefixed {
                data: batch.data,
                row_ends: row_ends.into_boxed_slice(),
            },
            nulls: batch.nulls,
            utf8_verified: utf8_type,
        })
    }

    #[inline]
    pub(super) fn is_null(&self, row_idx: usize) -> bool {
        self.nulls.as_ref().is_some_and(|nulls| nulls[row_idx] != 0)
    }

    #[inline]
    pub(super) fn row_value(&self, row_idx: usize) -> Option<&[u8]> {
        if self.is_null(row_idx) {
            return None;
        }
        self.stored_row_value(row_idx)
    }

    /// Return the encoded row bytes even when SQL validity marks the row NULL.
    /// Page-level searches still need the physical boundary to prevent a
    /// literal match from leaking into the following row.
    #[inline]
    pub(super) fn stored_row_value(&self, row_idx: usize) -> Option<&[u8]> {
        match &self.source {
            RawVarlenSource::LengthPrefixed { data, row_ends } => {
                if row_idx >= row_ends.len() {
                    return None;
                }
                let row_start = if row_idx == 0 {
                    0
                } else {
                    row_ends[row_idx - 1] as usize
                };
                let value_start = row_start + std::mem::size_of::<u32>();
                let row_end = row_ends[row_idx] as usize;
                Some(&data[value_start..row_end])
            }
            RawVarlenSource::BinaryPlain(storage) => storage.row_value_ref(row_idx),
        }
    }

    /// Return the contiguous value payload when the storage encoding keeps
    /// row bytes adjacent without length prefixes between them.
    pub(super) fn contiguous_payload(&self) -> Option<&[u8]> {
        match &self.source {
            RawVarlenSource::BinaryPlain(storage) => storage.payload_ref(),
            RawVarlenSource::LengthPrefixed { .. } => None,
        }
    }

    pub(super) fn contiguous_row_range(&self, row_idx: usize) -> Option<Range<usize>> {
        match &self.source {
            RawVarlenSource::BinaryPlain(storage) => storage.payload_row_range(row_idx),
            RawVarlenSource::LengthPrefixed { .. } => None,
        }
    }

    fn append_encoded_row(
        &self,
        row_idx: usize,
        values: &mut Vec<u8>,
        nulls: &mut Vec<u8>,
    ) -> Result<()> {
        if self.is_null(row_idx) {
            values.extend_from_slice(&0u32.to_le_bytes());
            nulls.push(1);
            return Ok(());
        }
        let value = self.row_value(row_idx).ok_or_else(|| {
            paro_error::data_corrupted("Reusable varlen predicate row is out of bounds")
        })?;
        values.extend_from_slice(&(value.len() as u32).to_le_bytes());
        values.extend_from_slice(value);
        nulls.push(0);
        Ok(())
    }
}

impl StorageDictionaryPredicateBatch {
    fn try_new(
        batch: ColumnBatch,
        logical_type: &LogicalType,
        raw_width: Option<usize>,
        rows: usize,
    ) -> Result<Self> {
        let source_utf8_verified = batch.has_verified_utf8();
        let storage = batch.storage_dictionary.ok_or_else(|| {
            paro_error::internal("Storage dictionary predicate batch has no dictionary")
        })?;
        let expected_codes = rows
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or_else(|| paro_error::data_corrupted("Dictionary code count overflow"))?;
        if storage.codes.len() != expected_codes {
            return Err(paro_error::data_corrupted(format!(
                "Storage dictionary code bytes {} do not match {rows} rows",
                storage.codes.len()
            )));
        }
        if batch.nulls.as_ref().is_some_and(|nulls| nulls.len() < rows) {
            return Err(paro_error::data_corrupted(
                "Storage dictionary null map is shorter than the batch",
            ));
        }

        let encoded_dictionary = storage.dictionary;
        let mut dictionary = BinaryPlainPageDecoder::new(encoded_dictionary.clone());
        dictionary.init()?;
        let dictionary_len = dictionary.count() as usize;
        for code in 0..dictionary_len {
            let value = dictionary
                .value_ref_at(code as u32)
                .ok_or_else(|| paro_error::data_corrupted("Storage dictionary entry is missing"))?;
            if let Some(width) = raw_width {
                if value.len() != width {
                    return Err(paro_error::data_corrupted(format!(
                        "Storage dictionary value width {} does not match column width {width}",
                        value.len()
                    )));
                }
            } else if matches!(logical_type, LogicalType::Varchar) && !source_utf8_verified {
                std::str::from_utf8(value).map_err(|_| {
                    paro_error::data_corrupted("Storage dictionary VARCHAR is not valid UTF-8")
                })?;
            }
        }

        let result = Self {
            encoded_dictionary,
            dictionary,
            codes: storage.codes,
            nulls: batch.nulls,
            rows,
            utf8_verified: matches!(logical_type, LogicalType::Varchar),
        };
        for row_idx in 0..rows {
            if !result.is_null(row_idx) && result.code_at(row_idx) as usize >= dictionary_len {
                return Err(paro_error::data_corrupted(format!(
                    "Storage dictionary code {} is out of range {dictionary_len}",
                    result.code_at(row_idx)
                )));
            }
        }
        Ok(result)
    }

    #[inline]
    pub(super) fn is_null(&self, row_idx: usize) -> bool {
        self.nulls.as_ref().is_some_and(|nulls| nulls[row_idx] != 0)
    }

    #[inline]
    pub(super) fn code_at(&self, row_idx: usize) -> u32 {
        debug_assert!(row_idx < self.rows);
        // SAFETY: construction verifies `codes.len() == rows * size_of::<u32>()`.
        u32::from_le(unsafe {
            self.codes
                .as_ptr()
                .add(row_idx * std::mem::size_of::<u32>())
                .cast::<u32>()
                .read_unaligned()
        })
    }

    pub(super) fn dictionary_len(&self) -> usize {
        self.dictionary.count() as usize
    }

    pub(super) fn encoded_dictionary(&self) -> &Bytes {
        &self.encoded_dictionary
    }

    pub(super) fn encoded_code(&self, row_idx: usize) -> &[u8] {
        let start = row_idx * std::mem::size_of::<u32>();
        &self.codes[start..start + std::mem::size_of::<u32>()]
    }

    pub(super) fn has_verified_utf8(&self) -> bool {
        self.utf8_verified
    }

    pub(super) fn dictionary_value(&self, code: usize) -> &[u8] {
        self.dictionary
            .value_ref_at(code as u32)
            .expect("validated dictionary code")
    }

    pub(super) fn row_value(&self, row_idx: usize) -> Option<&[u8]> {
        if self.is_null(row_idx) {
            return None;
        }
        self.dictionary.value_ref_at(self.code_at(row_idx))
    }

    pub(super) fn filter_codes(
        &self,
        code_matches: &[bool],
        selection: &mut Vec<BatchRowOrdinal>,
        seed: bool,
    ) {
        debug_assert_eq!(code_matches.len(), self.dictionary_len());
        let matches =
            |row_idx: usize| !self.is_null(row_idx) && code_matches[self.code_at(row_idx) as usize];
        if seed {
            selection.extend(
                (0..self.rows)
                    .filter(|row_idx| matches(*row_idx))
                    .map(BatchRowOrdinal::from_index),
            );
        } else {
            selection.retain(|row_idx| matches(row_idx.index()));
        }
    }
}

pub(super) enum PredicateColumnBatch {
    Raw(ColumnBatch),
    RawVarlen(RawVarlenPredicateBatch),
    StorageDictionary(StorageDictionaryPredicateBatch),
    Decoded(Vector),
}

impl PredicateColumnBatch {
    pub(super) fn storage_dictionary(&self) -> Option<&StorageDictionaryPredicateBatch> {
        match self {
            Self::StorageDictionary(batch) => Some(batch),
            Self::Raw(_) | Self::RawVarlen(_) | Self::Decoded(_) => None,
        }
    }

    pub(super) fn prepare(
        logical_type: &LogicalType,
        access: PredicateColumnAccess,
        batch: ColumnBatch,
        rows: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        if batch.storage_dictionary.is_some() {
            if let PredicateColumnAccess::Typed { raw_width } = access {
                return Ok(Self::StorageDictionary(
                    StorageDictionaryPredicateBatch::try_new(batch, logical_type, raw_width, rows)?,
                ));
            }
        } else if let PredicateColumnAccess::Typed { raw_width } = access {
            return match raw_width {
                Some(width) => {
                    validate_raw_batch(&batch, width, rows)?;
                    Ok(Self::Raw(batch))
                }
                None => Ok(Self::RawVarlen(RawVarlenPredicateBatch::try_new(
                    batch,
                    logical_type,
                    rows,
                )?)),
            };
        }

        Ok(Self::Decoded(vector_decoder::decode_column_batch(
            logical_type,
            &batch,
            rows,
            allocator,
            None,
        )?))
    }

    #[inline]
    pub(super) fn is_null(&self, row_idx: usize) -> bool {
        match self {
            Self::Raw(batch) => batch
                .nulls
                .as_ref()
                .is_some_and(|nulls| nulls[row_idx] != 0),
            Self::RawVarlen(batch) => batch.is_null(row_idx),
            Self::StorageDictionary(batch) => batch.is_null(row_idx),
            Self::Decoded(vector) => vector.is_null(row_idx),
        }
    }

    #[inline]
    pub(super) fn decoded(&self) -> Option<&Vector> {
        match self {
            Self::Raw(_) | Self::RawVarlen(_) | Self::StorageDictionary(_) => None,
            Self::Decoded(vector) => Some(vector),
        }
    }

    pub(super) fn raw_varlen(&self) -> Option<&RawVarlenPredicateBatch> {
        match self {
            Self::RawVarlen(batch) => Some(batch),
            Self::Raw(_) | Self::StorageDictionary(_) | Self::Decoded(_) => None,
        }
    }

    pub(super) fn append_reusable_rows(
        &self,
        encoding: PredicateColumnReuse,
        rows: &[BatchRowOrdinal],
        values: &mut Vec<u8>,
        nulls: &mut Vec<u8>,
        row_ends: &mut Vec<usize>,
    ) -> Result<bool> {
        let estimated_width = match encoding {
            PredicateColumnReuse::Fixed { width } => width,
            PredicateColumnReuse::Varlen => 16,
        };
        let additional_bytes = rows.len().checked_mul(estimated_width).ok_or_else(|| {
            paro_error::out_of_memory("Reusable predicate column capacity overflow")
        })?;
        // Selected rows become an owned output batch, so one allocation is
        // unavoidable. Reserve it up front instead of repeatedly growing and
        // copying the buffer in the per-row append loop.
        values.reserve(additional_bytes);
        nulls.reserve(rows.len());

        match encoding {
            PredicateColumnReuse::Fixed { width } => {
                self.append_reusable_fixed_rows(width, rows, values, nulls)
            }
            PredicateColumnReuse::Varlen => {
                row_ends.reserve(rows.len());
                self.append_reusable_varlen_rows(rows, values, nulls, row_ends)
            }
        }
    }

    pub(super) fn reusable_rows_have_verified_utf8(&self) -> bool {
        match self {
            Self::RawVarlen(batch) => batch.utf8_verified,
            Self::StorageDictionary(batch) => batch.utf8_verified,
            Self::Raw(_) | Self::Decoded(_) => false,
        }
    }

    fn append_reusable_fixed_rows(
        &self,
        width: usize,
        rows: &[BatchRowOrdinal],
        values: &mut Vec<u8>,
        nulls: &mut Vec<u8>,
    ) -> Result<bool> {
        match self {
            Self::Raw(batch) => {
                for &row_idx in rows {
                    let row_idx = row_idx.index();
                    let start = row_idx.checked_mul(width).ok_or_else(|| {
                        paro_error::data_corrupted("Predicate row offset overflow")
                    })?;
                    let end = start.checked_add(width).ok_or_else(|| {
                        paro_error::data_corrupted("Predicate row width overflow")
                    })?;
                    values.extend_from_slice(batch.data.get(start..end).ok_or_else(|| {
                        paro_error::data_corrupted("Predicate row exceeds the fixed-width batch")
                    })?);
                    nulls.push(batch.nulls.as_ref().map_or(0, |nulls| nulls[row_idx]));
                }
                Ok(true)
            }
            Self::StorageDictionary(batch) => {
                for &row_idx in rows {
                    let row_idx = row_idx.index();
                    if let Some(value) = batch.row_value(row_idx) {
                        if value.len() != width {
                            return Err(paro_error::data_corrupted(
                                "Predicate dictionary value has an invalid fixed width",
                            ));
                        }
                        values.extend_from_slice(value);
                        nulls.push(0);
                    } else {
                        let end = values.len().checked_add(width).ok_or_else(|| {
                            paro_error::data_corrupted("Reusable predicate buffer overflow")
                        })?;
                        values.resize(end, 0);
                        nulls.push(1);
                    }
                }
                Ok(true)
            }
            Self::RawVarlen(_) => Ok(false),
            Self::Decoded(_) => Ok(false),
        }
    }

    fn append_reusable_varlen_rows(
        &self,
        rows: &[BatchRowOrdinal],
        values: &mut Vec<u8>,
        nulls: &mut Vec<u8>,
        row_ends: &mut Vec<usize>,
    ) -> Result<bool> {
        match self {
            Self::RawVarlen(batch) => {
                for &row_idx in rows {
                    let row_idx = row_idx.index();
                    batch.append_encoded_row(row_idx, values, nulls)?;
                    row_ends.push(values.len());
                }
                Ok(true)
            }
            Self::StorageDictionary(batch) => {
                for &row_idx in rows {
                    let row_idx = row_idx.index();
                    append_varlen_value(batch.row_value(row_idx), values, nulls)?;
                    row_ends.push(values.len());
                }
                Ok(true)
            }
            Self::Decoded(vector) => {
                let view = vector.try_to_varlen_view(vector.len())?;
                for &row_idx in rows {
                    let row_idx = row_idx.index();
                    if row_idx >= vector.len() {
                        return Err(paro_error::data_corrupted(
                            "Reusable predicate row exceeds the decoded batch",
                        ));
                    }
                    if view.is_valid(row_idx) {
                        let value = view.get_inline_string(row_idx);
                        append_varlen_value(Some(value.as_bytes()), values, nulls)?;
                    } else {
                        append_varlen_value(None, values, nulls)?;
                    }
                    row_ends.push(values.len());
                }
                Ok(true)
            }
            Self::Raw(_) => Ok(false),
        }
    }

    #[inline]
    pub(super) unsafe fn fixed_value<T: Copy>(&self, row_idx: usize) -> T {
        match self {
            Self::Raw(batch) => unsafe {
                batch
                    .data
                    .as_ptr()
                    .add(row_idx * std::mem::size_of::<T>())
                    .cast::<T>()
                    .read_unaligned()
            },
            Self::StorageDictionary(batch) => {
                let value = batch
                    .row_value(row_idx)
                    .expect("fixed value requested for a non-null row");
                // SAFETY: construction validates every dictionary entry against
                // the compiled raw width, which is checked against size_of::<T>.
                unsafe { value.as_ptr().cast::<T>().read_unaligned() }
            }
            Self::RawVarlen(_) => unreachable!("fixed value requested from a varlen batch"),
            Self::Decoded(vector) => unsafe { vector.get_fixed::<T>(row_idx) },
        }
    }
}

fn append_varlen_value(
    value: Option<&[u8]>,
    values: &mut Vec<u8>,
    nulls: &mut Vec<u8>,
) -> Result<()> {
    let bytes = value.unwrap_or_default();
    let len = u32::try_from(bytes.len())
        .map_err(|_| paro_error::data_corrupted("Predicate varlen value exceeds u32 length"))?;
    values.extend_from_slice(&len.to_le_bytes());
    values.extend_from_slice(bytes);
    nulls.push(u8::from(value.is_none()));
    Ok(())
}

fn validate_raw_batch(batch: &ColumnBatch, width: usize, rows: usize) -> Result<()> {
    let expected = rows
        .checked_mul(width)
        .ok_or_else(|| paro_error::data_corrupted("Predicate batch width overflow"))?;
    if width == 0 || batch.data.len() != expected {
        return Err(paro_error::data_corrupted(
            "Fixed predicate batch has an invalid physical layout",
        ));
    }
    if batch.nulls.as_ref().is_some_and(|nulls| nulls.len() < rows) {
        return Err(paro_error::data_corrupted(
            "Predicate null map is shorter than the batch",
        ));
    }
    Ok(())
}
