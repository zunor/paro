// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # ZoneMap Bound Index
//!
//! BoundIndex wrapper for ZoneMapIndexReader.

use bytes::Bytes;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::index::bound_index::BoundIndex;
use crate::index::predicate::{compare_bytes, value_to_bytes, Predicate};
use crate::index::predicate_result::{
    decode_page_ranges, encode_page_ranges, PageRange, PredicateResult,
};
use crate::index::{
    ColumnId, Index, IndexAppendInfo, IndexBufferInfo, IndexConstraintType, IndexStorageInfo,
};

use super::ZoneMapIndexReader;

/// Bound ZoneMap index.
pub struct ZoneMapIndex {
    name: String,
    constraint_type: IndexConstraintType,
    column_ids: Vec<ColumnId>,
    logical_types: Vec<LogicalType>,
    reader: ZoneMapIndexReader,
    index_data: Bytes,
    page_ranges: Vec<PageRange>,
}

impl ZoneMapIndex {
    /// Index type name.
    pub const TYPE_NAME: &'static str = "ZONEMAP";

    /// Build from serialized bytes and page ranges.
    pub fn from_bytes(
        name: impl Into<String>,
        constraint_type: IndexConstraintType,
        column_ids: Vec<ColumnId>,
        logical_types: Vec<LogicalType>,
        index_data: Bytes,
        page_ranges: Vec<PageRange>,
    ) -> Result<Self> {
        let reader = ZoneMapIndexReader::from_bytes(&index_data)?;
        Ok(Self {
            name: name.into(),
            constraint_type,
            column_ids,
            logical_types,
            reader,
            index_data,
            page_ranges,
        })
    }

    fn logical_type(&self) -> Option<&LogicalType> {
        self.logical_types.first()
    }

    fn page_ranges_or_default(&self) -> Vec<PageRange> {
        let num_pages = self.reader.num_pages();
        if self.page_ranges.len() == num_pages {
            return self.page_ranges.clone();
        }
        (0..num_pages)
            .map(|idx| PageRange::new(idx as u32, idx as u32 + 1))
            .collect()
    }

    fn storage_info_with_ranges(&self) -> IndexStorageInfo {
        let mut info = IndexStorageInfo::new(&self.name);
        if !self.index_data.is_empty() {
            info.buffers.push(vec![IndexBufferInfo {
                data: self.index_data.to_vec(),
                size: self.index_data.len(),
            }]);
        }
        if !self.page_ranges.is_empty() {
            info.options.insert(
                "page_ranges".to_string(),
                Value::Blob(encode_page_ranges(&self.page_ranges)),
            );
        }
        info
    }

    fn eval_eq(&self, value: &Value) -> PredicateResult {
        let logical_type = match self.logical_type() {
            Some(t) => t,
            None => return PredicateResult::Unknown,
        };
        let Ok(bytes) = value_to_bytes(value, logical_type) else {
            return PredicateResult::Unknown;
        };

        let cmp = |a: &[u8], b: &[u8]| {
            compare_bytes(logical_type, a, b).unwrap_or(std::cmp::Ordering::Equal)
        };

        if !self.reader.segment_may_contain_range(&bytes, &bytes, cmp) {
            return PredicateResult::NoneMatch;
        }

        let ranges = self.page_ranges_or_default();
        let mut valid = Vec::new();
        for (idx, range) in ranges.iter().enumerate() {
            if self.reader.page_may_contain_value(idx, &bytes, cmp) {
                valid.push(*range);
            }
        }

        if valid.is_empty() {
            PredicateResult::NoneMatch
        } else {
            PredicateResult::PageRanges(valid)
        }
    }

    fn eval_range(&self, lower: &Value, upper: &Value) -> PredicateResult {
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

        let cmp = |a: &[u8], b: &[u8]| {
            compare_bytes(logical_type, a, b).unwrap_or(std::cmp::Ordering::Equal)
        };

        if !self
            .reader
            .segment_may_contain_range(&lower_bytes, &upper_bytes, cmp)
        {
            return PredicateResult::NoneMatch;
        }

        if !self.reader.global_has_null
            && self
                .reader
                .global_min
                .as_ref()
                .zip(self.reader.global_max.as_ref())
                .is_some_and(|(segment_min, segment_max)| {
                    cmp(segment_min, &lower_bytes) != std::cmp::Ordering::Less
                        && cmp(segment_max, &upper_bytes) != std::cmp::Ordering::Greater
                })
        {
            // Range predicates are inclusive. Exact segment bounds plus an
            // all-valid guarantee therefore prove every row satisfies it; the
            // row-level evaluator may remove this conjunct entirely.
            return PredicateResult::AllMatch;
        }

        let ranges = self.page_ranges_or_default();
        let mut valid = Vec::new();
        for (idx, range) in ranges.iter().enumerate() {
            if self
                .reader
                .page_may_contain_range(idx, &lower_bytes, &upper_bytes, cmp)
            {
                valid.push(*range);
            }
        }

        if valid.is_empty() {
            PredicateResult::NoneMatch
        } else {
            PredicateResult::PageRanges(valid)
        }
    }

    fn eval_is_null(&self) -> PredicateResult {
        if !self.reader.global_has_null {
            return PredicateResult::NoneMatch;
        }

        let ranges = self.page_ranges_or_default();
        let mut valid = Vec::new();
        for (idx, range) in ranges.iter().enumerate() {
            if self.reader.page_has_null(idx) {
                valid.push(*range);
            }
        }

        if valid.is_empty() {
            PredicateResult::NoneMatch
        } else {
            PredicateResult::PageRanges(valid)
        }
    }

    fn eval_is_not_null(&self) -> PredicateResult {
        // A page may contain non-null values unless ALL its values are null.
        // Since ZoneMap only tracks has_null (not all_null), we conservatively
        // include all pages. Pages with has_null=false definitely have no nulls.
        // Pages with has_null=true may or may not have all nulls — keep them.
        PredicateResult::AllMatch
    }

    fn eval_in(&self, values: &[Value]) -> PredicateResult {
        let logical_type = match self.logical_type() {
            Some(t) => t,
            None => return PredicateResult::Unknown,
        };

        // Encode all values
        let mut encoded_values = Vec::with_capacity(values.len());
        for value in values {
            match value_to_bytes(value, logical_type) {
                Ok(bytes) => encoded_values.push(bytes),
                Err(_) => return PredicateResult::Unknown,
            }
        }

        let cmp = |a: &[u8], b: &[u8]| {
            compare_bytes(logical_type, a, b).unwrap_or(std::cmp::Ordering::Equal)
        };

        let ranges = self.page_ranges_or_default();
        let mut valid = Vec::new();
        for (idx, range) in ranges.iter().enumerate() {
            // A page matches if ANY of the In-list values falls within its [min, max].
            let page_hit = encoded_values
                .iter()
                .any(|bytes| self.reader.page_may_contain_value(idx, bytes, cmp));
            if page_hit {
                valid.push(*range);
            }
        }

        if valid.is_empty() {
            PredicateResult::NoneMatch
        } else {
            PredicateResult::PageRanges(valid)
        }
    }

    /// Evaluate column < value (inclusive=false) or column <= value (inclusive=true).
    ///
    /// A page matches if its min is < value (or <= value for inclusive).
    /// Equivalently, a page is excluded only if its min > value (or >= for strict).
    fn eval_comparison_lt(&self, value: &Value, inclusive: bool) -> PredicateResult {
        let logical_type = match self.logical_type() {
            Some(t) => t,
            None => return PredicateResult::Unknown,
        };
        let Ok(bytes) = value_to_bytes(value, logical_type) else {
            return PredicateResult::Unknown;
        };

        let cmp = |a: &[u8], b: &[u8]| {
            compare_bytes(logical_type, a, b).unwrap_or(std::cmp::Ordering::Equal)
        };

        let ranges = self.page_ranges_or_default();
        let mut valid = Vec::new();
        for (idx, range) in ranges.iter().enumerate() {
            // Page has values in [page_min, page_max].
            // Column < value: need page_min < value  (at least one row could be < value)
            // Column <= value: need page_min <= value
            if let Some((page_min, _page_max)) = self.reader.page_min_max(idx) {
                let ord = cmp(page_min, &bytes);
                let matches = if inclusive {
                    ord != std::cmp::Ordering::Greater
                } else {
                    ord == std::cmp::Ordering::Less
                };
                if matches {
                    valid.push(*range);
                }
            } else {
                // No min/max info, conservatively include
                valid.push(*range);
            }
        }

        if valid.is_empty() {
            PredicateResult::NoneMatch
        } else {
            PredicateResult::PageRanges(valid)
        }
    }

    /// Evaluate column > value (inclusive=false) or column >= value (inclusive=true).
    fn eval_comparison_gt(&self, value: &Value, inclusive: bool) -> PredicateResult {
        let logical_type = match self.logical_type() {
            Some(t) => t,
            None => return PredicateResult::Unknown,
        };
        let Ok(bytes) = value_to_bytes(value, logical_type) else {
            return PredicateResult::Unknown;
        };

        let cmp = |a: &[u8], b: &[u8]| {
            compare_bytes(logical_type, a, b).unwrap_or(std::cmp::Ordering::Equal)
        };

        let ranges = self.page_ranges_or_default();
        let mut valid = Vec::new();
        for (idx, range) in ranges.iter().enumerate() {
            // Column > value: need page_max > value
            // Column >= value: need page_max >= value
            if let Some((_page_min, page_max)) = self.reader.page_min_max(idx) {
                let ord = cmp(page_max, &bytes);
                let matches = if inclusive {
                    ord != std::cmp::Ordering::Less
                } else {
                    ord == std::cmp::Ordering::Greater
                };
                if matches {
                    valid.push(*range);
                }
            } else {
                valid.push(*range);
            }
        }

        if valid.is_empty() {
            PredicateResult::NoneMatch
        } else {
            PredicateResult::PageRanges(valid)
        }
    }

    /// Load from IndexStorageInfo buffers/options.
    pub fn from_storage_info(
        input: &crate::index::CreateIndexInput,
    ) -> Result<Arc<dyn BoundIndex>> {
        let storage = input
            .storage_info
            .ok_or_else(|| paro_error::invalid_input("ZoneMapIndex: missing storage info"))?;
        let data = storage
            .buffers
            .first()
            .and_then(|bufs| bufs.first())
            .ok_or_else(|| paro_error::data_corrupted("ZoneMapIndex: missing buffer"))?;
        let index_data = Bytes::copy_from_slice(&data.data);

        let page_ranges = storage
            .options
            .get("page_ranges")
            .and_then(|value| match value {
                Value::Blob(bytes) => decode_page_ranges(bytes).ok(),
                _ => None,
            })
            .unwrap_or_default();

        let index = ZoneMapIndex::from_bytes(
            input.name,
            input.constraint_type,
            input.column_ids.to_vec(),
            input.logical_types.to_vec(),
            index_data,
            page_ranges,
        )?;

        Ok(Arc::new(index))
    }
}

impl Index for ZoneMapIndex {
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

impl BoundIndex for ZoneMapIndex {
    fn physical_types(&self) -> &[LogicalType] {
        &self.logical_types
    }

    fn logical_types(&self) -> &[LogicalType] {
        &self.logical_types
    }

    fn append(&self, _chunk: &Chunk, _row_ids: &Vector) -> Result<()> {
        Err(paro_error::not_implemented("ZoneMapIndex::append"))
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
        Err(paro_error::not_implemented("ZoneMapIndex::delete"))
    }

    fn insert(&self, _chunk: &Chunk, _row_ids: &Vector) -> Result<()> {
        Err(paro_error::not_implemented("ZoneMapIndex::insert"))
    }

    fn merge_indexes(&self, _other: &dyn BoundIndex) -> Result<bool> {
        Err(paro_error::not_implemented("ZoneMapIndex::merge_indexes"))
    }

    fn vacuum(&self) {}

    fn get_in_memory_size(&self) -> usize {
        self.index_data.len() + self.page_ranges.len() * std::mem::size_of::<PageRange>()
    }

    fn serialize_to_disk(&self) -> Result<IndexStorageInfo> {
        Ok(self.storage_info_with_ranges())
    }

    fn evaluate_predicate(&self, predicate: &Predicate) -> PredicateResult {
        if self.column_ids.len() != 1 {
            return PredicateResult::Unknown;
        }
        if predicate.index_column_id() != Some(self.column_ids[0]) {
            return PredicateResult::Unknown;
        }

        match predicate {
            Predicate::Eq { value, .. } => self.eval_eq(value),
            Predicate::Range { lower, upper, .. } => self.eval_range(lower, upper),
            Predicate::IsNull { .. } => self.eval_is_null(),
            Predicate::In { values, .. } => self.eval_in(values),
            Predicate::IsNotNull { .. } => self.eval_is_not_null(),
            // Lt/Le/Gt/Ge: delegate to range with open bound
            Predicate::Lt { value, .. } | Predicate::Le { value, .. } => {
                // ZoneMap checks if page [min,max] overlaps with (-∞, value].
                // We check per-page: if page_min <= value (Lt: page_min < value).
                self.eval_comparison_lt(value, matches!(predicate, Predicate::Le { .. }))
            }
            Predicate::Gt { value, .. } | Predicate::Ge { value, .. } => {
                self.eval_comparison_gt(value, matches!(predicate, Predicate::Ge { .. }))
            }
            // NotEq: ZoneMap cannot precisely exclude a single value
            Predicate::NotEq { .. } => PredicateResult::Unknown,
            Predicate::FixedIn { .. }
            | Predicate::StringPrefix { .. }
            | Predicate::ColumnComparison { .. } => PredicateResult::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::zonemap::ZoneMapIndexWriter;

    #[test]
    fn test_zonemap_eval() {
        let mut writer = ZoneMapIndexWriter::new();
        writer.add(
            Bytes::from_static(&[10, 0, 0, 0]),
            Bytes::from_static(&[20, 0, 0, 0]),
            false,
        );
        writer.add(
            Bytes::from_static(&[30, 0, 0, 0]),
            Bytes::from_static(&[40, 0, 0, 0]),
            true,
        );
        let data = writer.finish();
        let ranges = vec![PageRange::new(0, 3), PageRange::new(3, 6)];

        let index = ZoneMapIndex::from_bytes(
            "zm",
            IndexConstraintType::None,
            vec![0],
            vec![LogicalType::Integer],
            data,
            ranges,
        )
        .unwrap();

        let predicate = Predicate::Eq {
            column_id: 0,
            value: Value::Integer(15),
        };
        let result = index.evaluate_predicate(&predicate);
        match result {
            PredicateResult::PageRanges(ranges) => {
                assert!(ranges.iter().any(|r| *r == PageRange::new(0, 3)));
            }
            _ => panic!("expected page ranges"),
        }
    }

    #[test]
    fn inclusive_range_covering_all_valid_segment_is_exact_all_match() {
        let mut writer = ZoneMapIndexWriter::new();
        writer.add(
            Bytes::copy_from_slice(&10_i32.to_le_bytes()),
            Bytes::copy_from_slice(&20_i32.to_le_bytes()),
            false,
        );
        writer.add(
            Bytes::copy_from_slice(&30_i32.to_le_bytes()),
            Bytes::copy_from_slice(&40_i32.to_le_bytes()),
            false,
        );
        let index = ZoneMapIndex::from_bytes(
            "zm",
            IndexConstraintType::None,
            vec![0],
            vec![LogicalType::Integer],
            writer.finish(),
            vec![PageRange::new(0, 3), PageRange::new(3, 6)],
        )
        .unwrap();

        assert!(matches!(
            index.evaluate_predicate(&Predicate::Range {
                column_id: 0,
                lower: Value::Integer(10),
                upper: Value::Integer(40),
            }),
            PredicateResult::AllMatch
        ));
    }
}
