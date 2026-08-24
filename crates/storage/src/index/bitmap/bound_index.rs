// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Bitmap Bound Index
//!
//! BoundIndex wrapper for BitmapIndexReader.

use bytes::Bytes;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use roaring::RoaringBitmap;

use crate::index::bound_index::BoundIndex;
use crate::index::predicate::{compare_bytes, value_to_bytes, Predicate};
use crate::index::predicate_result::PredicateResult;
use crate::index::{
    ColumnId, ExactOrdinalPosting, ExactRowSet, Index, IndexAppendInfo, IndexBufferInfo,
    IndexConstraintType, IndexStorageInfo, OrdinalRowSet,
};

use super::{BitmapIndexReader, BitmapIndexWriter};

#[derive(Debug)]
struct AcceptedOrdinals {
    words: Vec<u64>,
    len: usize,
}

impl AcceptedOrdinals {
    fn new(len: usize) -> Self {
        Self {
            words: vec![0; len.div_ceil(64)],
            len,
        }
    }

    fn insert(&mut self, ordinal: usize) {
        if ordinal < self.len {
            self.words[ordinal / 64] |= 1_u64 << (ordinal % 64);
        }
    }

    fn remove(&mut self, ordinal: usize) {
        if ordinal < self.len {
            self.words[ordinal / 64] &= !(1_u64 << (ordinal % 64));
        }
    }

    fn insert_range(&mut self, start: usize, end: usize) {
        let start = start.min(self.len);
        let end = end.min(self.len);
        if start >= end {
            return;
        }
        let first_word = start / 64;
        let last_word = (end - 1) / 64;
        for word_index in first_word..=last_word {
            let word_start = word_index * 64;
            let first_bit = start.saturating_sub(word_start).min(64);
            let last_bit = end.saturating_sub(word_start).min(64);
            let below_last = if last_bit == 64 {
                u64::MAX
            } else {
                (1_u64 << last_bit) - 1
            };
            self.words[word_index] |= below_last & (u64::MAX << first_bit);
        }
    }

    fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.words
            .iter()
            .copied()
            .enumerate()
            .flat_map(move |(word_index, mut word)| {
                std::iter::from_fn(move || {
                    if word == 0 {
                        return None;
                    }
                    let bit = word.trailing_zeros() as usize;
                    word &= word - 1;
                    let ordinal = word_index * 64 + bit;
                    (ordinal < self.len).then_some(ordinal)
                })
            })
    }

    fn into_words(self) -> Box<[u64]> {
        self.words.into_boxed_slice()
    }
}

/// Bound Bitmap index.
pub struct BitmapIndex {
    name: String,
    constraint_type: IndexConstraintType,
    column_ids: Vec<ColumnId>,
    logical_types: Vec<LogicalType>,
    reader: BitmapIndexReader,
    /// Physical bitmap ordinals sorted by SQL scalar semantics. The durable
    /// bitmap dictionary is byte-keyed for equality lookup; integer and other
    /// fixed-width encodings are not lexicographically ordered, so range
    /// predicates must use this typed access path rather than byte order.
    ordered_ordinals: Box<[usize]>,
    index_data: Bytes,
}

impl BitmapIndex {
    /// Index type name.
    pub const TYPE_NAME: &'static str = "BITMAP";

    /// Build from serialized bytes.
    pub fn from_bytes(
        name: impl Into<String>,
        constraint_type: IndexConstraintType,
        column_ids: Vec<ColumnId>,
        logical_types: Vec<LogicalType>,
        index_data: Bytes,
    ) -> Result<Self> {
        let reader = BitmapIndexReader::from_bytes(&index_data)?;
        let logical_type = logical_types.first().ok_or_else(|| {
            paro_error::invalid_input("bitmap index requires one logical column type")
        })?;
        let mut ordered_ordinals = (0..reader.num_values()).collect::<Vec<_>>();
        let sort_error = std::cell::RefCell::new(None);
        ordered_ordinals.sort_by(|&left, &right| {
            let left = reader
                .get_dict_value(left)
                .expect("bitmap dictionary ordinal validated");
            let right = reader
                .get_dict_value(right)
                .expect("bitmap dictionary ordinal validated");
            match compare_bytes(logical_type, left, right) {
                Ok(ordering) => ordering,
                Err(error) => {
                    let mut first_error = sort_error.borrow_mut();
                    if first_error.is_none() {
                        *first_error = Some(error);
                    }
                    std::cmp::Ordering::Equal
                }
            }
        });
        if let Some(error) = sort_error.into_inner() {
            return Err(error.context("sort bitmap dictionary by SQL scalar semantics"));
        }
        Ok(Self {
            name: name.into(),
            constraint_type,
            column_ids,
            logical_types,
            reader,
            ordered_ordinals: ordered_ordinals.into_boxed_slice(),
            index_data,
        })
    }

    /// Build from a writer.
    pub fn from_writer(
        name: impl Into<String>,
        constraint_type: IndexConstraintType,
        column_ids: Vec<ColumnId>,
        logical_types: Vec<LogicalType>,
        writer: &BitmapIndexWriter,
    ) -> Result<Self> {
        let index_data = writer.finish()?;
        Self::from_bytes(name, constraint_type, column_ids, logical_types, index_data)
    }

    fn logical_type(&self) -> Option<&LogicalType> {
        self.logical_types.first()
    }

    /// Durable cardinality covered by the posting dictionary. A segment
    /// loader must compare this with its footer before the index can be used
    /// as an exact predicate proof.
    pub fn indexed_row_count(&self) -> u64 {
        u64::from(self.reader.row_count())
    }

    fn storage_info_with_data(&self) -> IndexStorageInfo {
        let mut info = IndexStorageInfo::new(&self.name);
        if !self.index_data.is_empty() {
            info.buffers.push(vec![IndexBufferInfo {
                data: self.index_data.to_vec(),
                size: self.index_data.len(),
            }]);
        }
        info
    }

    fn seek_ordered_dictionary(&self, value: &[u8]) -> Option<(usize, bool)> {
        let logical_type = self.logical_type()?;
        let mut left = 0usize;
        let mut right = self.ordered_ordinals.len();
        while left < right {
            let middle = left + (right - left) / 2;
            let ordinal = self.ordered_ordinals[middle];
            let candidate = self.reader.get_dict_value(ordinal)?;
            match compare_bytes(logical_type, candidate, value).ok()? {
                std::cmp::Ordering::Less => left = middle + 1,
                std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => right = middle,
            }
        }
        let exact = self
            .ordered_ordinals
            .get(left)
            .and_then(|&ordinal| self.reader.get_dict_value(ordinal))
            .and_then(|candidate| compare_bytes(logical_type, candidate, value).ok())
            == Some(std::cmp::Ordering::Equal);
        Some((left, exact))
    }

    fn insert_ordered_range(&self, accepted: &mut AcceptedOrdinals, start: usize, end: usize) {
        for &ordinal in &self.ordered_ordinals
            [start.min(self.ordered_ordinals.len())..end.min(self.ordered_ordinals.len())]
        {
            accepted.insert(ordinal);
        }
    }

    fn matching_ordinals(&self, predicate: &Predicate) -> Option<(AcceptedOrdinals, bool)> {
        if self.column_ids.len() != 1 || predicate.index_column_id() != Some(self.column_ids[0]) {
            return None;
        }
        let logical_type = self.logical_type()?;
        let mut accepted = AcceptedOrdinals::new(self.reader.num_values());
        let mut accepts_null = false;

        match predicate {
            Predicate::Eq { value, .. } => {
                let bytes = value_to_bytes(value, logical_type).ok()?;
                let (ordinal, exact) = self.reader.seek_dictionary(&bytes);
                if exact {
                    accepted.insert(ordinal);
                }
            }
            Predicate::NotEq { value, .. } => {
                let bytes = value_to_bytes(value, logical_type).ok()?;
                accepted.insert_range(0, self.reader.num_values());
                let (ordinal, exact) = self.reader.seek_dictionary(&bytes);
                if exact {
                    accepted.remove(ordinal);
                }
            }
            Predicate::Lt { value, .. } => {
                let bytes = value_to_bytes(value, logical_type).ok()?;
                let (end, _) = self.seek_ordered_dictionary(&bytes)?;
                self.insert_ordered_range(&mut accepted, 0, end);
            }
            Predicate::Le { value, .. } => {
                let bytes = value_to_bytes(value, logical_type).ok()?;
                let (end, exact) = self.seek_ordered_dictionary(&bytes)?;
                self.insert_ordered_range(&mut accepted, 0, end + usize::from(exact));
            }
            Predicate::Gt { value, .. } => {
                let bytes = value_to_bytes(value, logical_type).ok()?;
                let (start, exact) = self.seek_ordered_dictionary(&bytes)?;
                self.insert_ordered_range(
                    &mut accepted,
                    start + usize::from(exact),
                    self.reader.num_values(),
                );
            }
            Predicate::Ge { value, .. } => {
                let bytes = value_to_bytes(value, logical_type).ok()?;
                let (start, _) = self.seek_ordered_dictionary(&bytes)?;
                self.insert_ordered_range(&mut accepted, start, self.reader.num_values());
            }
            Predicate::In { values, .. } => {
                for value in values {
                    let bytes = value_to_bytes(value, logical_type).ok()?;
                    let (ordinal, exact) = self.reader.seek_dictionary(&bytes);
                    if exact {
                        accepted.insert(ordinal);
                    }
                }
            }
            Predicate::Range { lower, upper, .. } => {
                let lower = value_to_bytes(lower, logical_type).ok()?;
                let upper = value_to_bytes(upper, logical_type).ok()?;
                let (start, _) = self.seek_ordered_dictionary(&lower)?;
                let (end, exact) = self.seek_ordered_dictionary(&upper)?;
                self.insert_ordered_range(&mut accepted, start, end + usize::from(exact));
            }
            Predicate::IsNull { .. } => accepts_null = true,
            Predicate::IsNotNull { .. } => {
                accepted.insert_range(0, self.reader.num_values());
            }
            Predicate::FixedIn { .. }
            | Predicate::StringPrefix { .. }
            | Predicate::StringPrefixIn { .. }
            | Predicate::StringLike { .. }
            | Predicate::ColumnComparison { .. } => return None,
        }
        Some((accepted, accepts_null))
    }

    fn compile_ordinal_row_set(&self, predicate: &Predicate) -> Option<Arc<dyn ExactRowSet>> {
        let row_ordinals = self.reader.row_ordinals()?;
        let (accepted, accepts_null) = self.matching_ordinals(predicate)?;
        let mut cardinality = if accepts_null {
            self.reader.null_cardinality()
        } else {
            0
        };
        let mut postings = Vec::new();
        for ordinal in accepted.iter() {
            cardinality = cardinality.saturating_add(self.reader.bitmap_cardinality(ordinal)?);
            postings.push(ExactOrdinalPosting::from_index(
                u16::try_from(ordinal).ok()?,
                self.reader.bitmap(ordinal)?,
                self.reader.bitmap_fingerprint(ordinal)?,
            ));
        }
        if accepts_null {
            if let Some(posting) = self.reader.null_bitmap() {
                postings.push(ExactOrdinalPosting::from_index(
                    u16::MAX,
                    posting,
                    self.reader.null_fingerprint(),
                ));
            }
        }
        Some(Arc::new(OrdinalRowSet::new(
            self.column_ids[0],
            row_ordinals,
            accepted.into_words(),
            accepts_null,
            cardinality,
            postings.into_boxed_slice(),
        )))
    }

    fn evaluate_eq(&self, value: &Value) -> PredicateResult {
        let logical_type = match self.logical_type() {
            Some(t) => t,
            None => return PredicateResult::Unknown,
        };
        let Ok(bytes) = value_to_bytes(value, logical_type) else {
            return PredicateResult::Unknown;
        };

        match self.reader.get_rows_for_value(&bytes) {
            Ok(bitmap) => {
                if bitmap.is_empty() {
                    PredicateResult::NoneMatch
                } else {
                    PredicateResult::Bitmap(bitmap)
                }
            }
            Err(_) => PredicateResult::Unknown,
        }
    }

    fn evaluate_in(&self, values: &[Value]) -> PredicateResult {
        let logical_type = match self.logical_type() {
            Some(t) => t,
            None => return PredicateResult::Unknown,
        };

        let mut result = RoaringBitmap::new();
        for value in values {
            let Ok(bytes) = value_to_bytes(value, logical_type) else {
                return PredicateResult::Unknown;
            };
            match self.reader.get_rows_for_value(&bytes) {
                Ok(bitmap) => result |= bitmap,
                Err(_) => return PredicateResult::Unknown,
            }
        }

        if result.is_empty() {
            PredicateResult::NoneMatch
        } else {
            PredicateResult::Bitmap(result)
        }
    }

    fn evaluate_range(&self, lower: &Value, upper: &Value) -> PredicateResult {
        let logical_type = match self.logical_type() {
            Some(t) => t,
            None => return PredicateResult::Unknown,
        };
        let Ok(lower_bytes) = value_to_bytes(lower, logical_type) else {
            return PredicateResult::Unknown;
        };
        let Ok(upper_bytes) = value_to_bytes(upper, logical_type) else {
            return PredicateResult::Unknown;
        };

        let mut result = RoaringBitmap::new();
        for ordinal in 0..self.reader.num_values() {
            let Some(value) = self.reader.get_dict_value(ordinal) else {
                continue;
            };
            let Ok(cmp_lower) = compare_bytes(logical_type, value.as_ref(), &lower_bytes) else {
                return PredicateResult::Unknown;
            };
            let Ok(cmp_upper) = compare_bytes(logical_type, value.as_ref(), &upper_bytes) else {
                return PredicateResult::Unknown;
            };
            if cmp_lower == std::cmp::Ordering::Less {
                continue;
            }
            if cmp_upper == std::cmp::Ordering::Greater {
                continue;
            }

            match self.reader.read_bitmap(ordinal) {
                Ok(bitmap) => result |= bitmap,
                Err(_) => return PredicateResult::Unknown,
            }
        }

        if result.is_empty() {
            PredicateResult::NoneMatch
        } else {
            PredicateResult::Bitmap(result)
        }
    }

    fn evaluate_is_null(&self) -> PredicateResult {
        if !self.reader.has_null() {
            return PredicateResult::NoneMatch;
        }
        match self.reader.read_null_bitmap() {
            Ok(bitmap) => {
                if bitmap.is_empty() {
                    PredicateResult::NoneMatch
                } else {
                    PredicateResult::Bitmap(bitmap)
                }
            }
            Err(_) => PredicateResult::Unknown,
        }
    }

    fn evaluate_is_not_null(&self) -> PredicateResult {
        if !self.reader.has_null() {
            return PredicateResult::AllMatch;
        }
        // Collect all non-null rows by union-ing all value bitmaps
        let mut result = RoaringBitmap::new();
        for ordinal in 0..self.reader.num_values() {
            match self.reader.read_bitmap(ordinal) {
                Ok(bitmap) => result |= bitmap,
                Err(_) => return PredicateResult::Unknown,
            }
        }
        if result.is_empty() {
            PredicateResult::NoneMatch
        } else {
            PredicateResult::Bitmap(result)
        }
    }

    fn evaluate_not_eq(&self, value: &Value) -> PredicateResult {
        let logical_type = match self.logical_type() {
            Some(t) => t,
            None => return PredicateResult::Unknown,
        };
        let Ok(bytes) = value_to_bytes(value, logical_type) else {
            return PredicateResult::Unknown;
        };

        // Collect all rows whose value != target
        let mut result = RoaringBitmap::new();
        for ordinal in 0..self.reader.num_values() {
            let Some(dict_value) = self.reader.get_dict_value(ordinal) else {
                continue;
            };
            if dict_value.as_ref() != bytes.as_slice() {
                match self.reader.read_bitmap(ordinal) {
                    Ok(bitmap) => result |= bitmap,
                    Err(_) => return PredicateResult::Unknown,
                }
            }
        }
        // Include null rows? No — NOT_EQ semantics: NULL != x is NULL (unknown), exclude nulls.
        if result.is_empty() {
            PredicateResult::NoneMatch
        } else {
            PredicateResult::Bitmap(result)
        }
    }

    /// Evaluate column < value (inclusive=false) or column <= value (inclusive=true).
    fn evaluate_lt(&self, value: &Value, inclusive: bool) -> PredicateResult {
        let logical_type = match self.logical_type() {
            Some(t) => t,
            None => return PredicateResult::Unknown,
        };
        let Ok(bytes) = value_to_bytes(value, logical_type) else {
            return PredicateResult::Unknown;
        };

        let mut result = RoaringBitmap::new();
        for ordinal in 0..self.reader.num_values() {
            let Some(dict_value) = self.reader.get_dict_value(ordinal) else {
                continue;
            };
            let Ok(cmp) = compare_bytes(logical_type, dict_value.as_ref(), &bytes) else {
                return PredicateResult::Unknown;
            };
            let matches = if inclusive {
                cmp != std::cmp::Ordering::Greater
            } else {
                cmp == std::cmp::Ordering::Less
            };
            if matches {
                match self.reader.read_bitmap(ordinal) {
                    Ok(bitmap) => result |= bitmap,
                    Err(_) => return PredicateResult::Unknown,
                }
            }
        }
        if result.is_empty() {
            PredicateResult::NoneMatch
        } else {
            PredicateResult::Bitmap(result)
        }
    }

    /// Evaluate column > value (inclusive=false) or column >= value (inclusive=true).
    fn evaluate_gt(&self, value: &Value, inclusive: bool) -> PredicateResult {
        let logical_type = match self.logical_type() {
            Some(t) => t,
            None => return PredicateResult::Unknown,
        };
        let Ok(bytes) = value_to_bytes(value, logical_type) else {
            return PredicateResult::Unknown;
        };

        let mut result = RoaringBitmap::new();
        for ordinal in 0..self.reader.num_values() {
            let Some(dict_value) = self.reader.get_dict_value(ordinal) else {
                continue;
            };
            let Ok(cmp) = compare_bytes(logical_type, dict_value.as_ref(), &bytes) else {
                return PredicateResult::Unknown;
            };
            let matches = if inclusive {
                cmp != std::cmp::Ordering::Less
            } else {
                cmp == std::cmp::Ordering::Greater
            };
            if matches {
                match self.reader.read_bitmap(ordinal) {
                    Ok(bitmap) => result |= bitmap,
                    Err(_) => return PredicateResult::Unknown,
                }
            }
        }
        if result.is_empty() {
            PredicateResult::NoneMatch
        } else {
            PredicateResult::Bitmap(result)
        }
    }

    /// Load from IndexStorageInfo buffers/options.
    pub fn from_storage_info(
        input: &crate::index::CreateIndexInput,
    ) -> Result<Arc<dyn BoundIndex>> {
        let storage = input
            .storage_info
            .ok_or_else(|| paro_error::invalid_input("BitmapIndex: missing storage info"))?;
        let data = storage
            .buffers
            .first()
            .and_then(|bufs| bufs.first())
            .ok_or_else(|| paro_error::data_corrupted("BitmapIndex: missing buffer"))?;
        let index_data = Bytes::copy_from_slice(&data.data);

        let index = BitmapIndex::from_bytes(
            input.name,
            input.constraint_type,
            input.column_ids.to_vec(),
            input.logical_types.to_vec(),
            index_data,
        )?;

        Ok(Arc::new(index))
    }
}

impl Index for BitmapIndex {
    fn column_ids(&self) -> &[ColumnId] {
        &self.column_ids
    }

    fn is_bound(&self) -> bool {
        true
    }

    fn index_type(&self) -> &str {
        Self::TYPE_NAME
    }

    fn index_name(&self) -> &str {
        &self.name
    }

    fn constraint_type(&self) -> IndexConstraintType {
        self.constraint_type
    }

    fn commit_drop(&mut self) -> Result<()> {
        Ok(())
    }
}

impl BoundIndex for BitmapIndex {
    fn physical_types(&self) -> &[LogicalType] {
        &self.logical_types
    }

    fn logical_types(&self) -> &[LogicalType] {
        &self.logical_types
    }

    fn append(&self, _chunk: &Chunk, _row_ids: &Vector) -> Result<()> {
        Err(paro_error::not_implemented("BitmapIndex::append"))
    }

    fn append_with_info(
        &self,
        chunk: &Chunk,
        row_ids: &Vector,
        _info: &IndexAppendInfo,
    ) -> Result<()> {
        self.append(chunk, row_ids)
    }

    fn delete(&self, _entries: &Chunk, _row_ids: &Vector) -> Result<usize> {
        Err(paro_error::not_implemented("BitmapIndex::delete"))
    }

    fn insert(&self, _chunk: &Chunk, _row_ids: &Vector) -> Result<()> {
        Err(paro_error::not_implemented("BitmapIndex::insert"))
    }

    fn merge_indexes(&self, _other: &dyn BoundIndex) -> Result<bool> {
        Err(paro_error::not_implemented("BitmapIndex::merge_indexes"))
    }

    fn vacuum(&self) {}

    fn get_in_memory_size(&self) -> usize {
        self.index_data
            .len()
            .saturating_add(self.reader.mem_usage())
    }

    fn serialize_to_disk(&self) -> Result<IndexStorageInfo> {
        Ok(self.storage_info_with_data())
    }

    fn evaluate_predicate(&self, predicate: &Predicate) -> PredicateResult {
        if self.column_ids.len() != 1 {
            return PredicateResult::Unknown;
        }
        if predicate.index_column_id() != Some(self.column_ids[0]) {
            return PredicateResult::Unknown;
        }

        match predicate {
            Predicate::Eq { value, .. } => self.evaluate_eq(value),
            Predicate::NotEq { value, .. } => self.evaluate_not_eq(value),
            Predicate::Lt { value, .. } => self.evaluate_lt(value, false),
            Predicate::Le { value, .. } => self.evaluate_lt(value, true),
            Predicate::Gt { value, .. } => self.evaluate_gt(value, false),
            Predicate::Ge { value, .. } => self.evaluate_gt(value, true),
            Predicate::In { values, .. } => self.evaluate_in(values),
            Predicate::Range { lower, upper, .. } => self.evaluate_range(lower, upper),
            Predicate::IsNull { .. } => self.evaluate_is_null(),
            Predicate::IsNotNull { .. } => self.evaluate_is_not_null(),
            Predicate::FixedIn { .. }
            | Predicate::StringPrefix { .. }
            | Predicate::StringPrefixIn { .. }
            | Predicate::StringLike { .. }
            | Predicate::ColumnComparison { .. } => PredicateResult::Unknown,
        }
    }

    fn compile_exact_row_set(&self, predicate: &Predicate) -> Option<Arc<dyn ExactRowSet>> {
        self.compile_ordinal_row_set(predicate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_index_eval() {
        let mut writer = BitmapIndexWriter::new();

        writer.add_value(b"apple"); // row 0
        writer.add_value(b"banana"); // row 1
        writer.add_nulls(1); // row 2
        writer.add_value(b"cherry"); // row 3
        writer.add_value(b"banana"); // row 4

        let index = BitmapIndex::from_writer(
            "bm",
            IndexConstraintType::None,
            vec![0],
            vec![LogicalType::Varchar],
            &writer,
        )
        .unwrap();

        let eq_pred = Predicate::Eq {
            column_id: 0,
            value: Value::Varchar("banana".to_string()),
        };
        match index.evaluate_predicate(&eq_pred) {
            PredicateResult::Bitmap(bitmap) => {
                assert!(bitmap.contains(1));
                assert!(bitmap.contains(4));
            }
            _ => panic!("expected bitmap"),
        }

        let range_pred = Predicate::Range {
            column_id: 0,
            lower: Value::Varchar("banana".to_string()),
            upper: Value::Varchar("cherry".to_string()),
        };
        match index.evaluate_predicate(&range_pred) {
            PredicateResult::Bitmap(bitmap) => {
                assert!(bitmap.contains(1));
                assert!(bitmap.contains(3));
                assert!(bitmap.contains(4));
            }
            _ => panic!("expected bitmap"),
        }

        let null_pred = Predicate::IsNull { column_id: 0 };
        match index.evaluate_predicate(&null_pred) {
            PredicateResult::Bitmap(bitmap) => {
                assert!(bitmap.contains(2));
            }
            _ => panic!("expected bitmap"),
        }
    }

    #[test]
    fn integer_range_compiles_exact_ordinal_membership() {
        let mut writer = BitmapIndexWriter::new();
        // Little-endian byte order differs from SQL integer order for both
        // negative values and values crossing a byte boundary.
        for value in [0_i32, 256, -1, 2, 1] {
            writer.add_value(&value.to_le_bytes());
        }
        writer.add_nulls(1);
        let index = BitmapIndex::from_writer(
            "bm",
            IndexConstraintType::None,
            vec![0],
            vec![LogicalType::Integer],
            &writer,
        )
        .unwrap();
        let row_set = index
            .compile_ordinal_row_set(&Predicate::Lt {
                column_id: 0,
                value: Value::Integer(2),
            })
            .expect("low-cardinality range should compile");

        assert_eq!(row_set.len(), 3);
        assert!(row_set.contains(0));
        assert!(row_set.contains(2));
        assert!(row_set.contains(4));
        assert!(!row_set.contains(1));
        assert!(!row_set.contains(3));
        assert!(!row_set.contains(5));
        assert_eq!(
            row_set.materialize(),
            RoaringBitmap::from_iter([0_u32, 2, 4])
        );
    }
}
