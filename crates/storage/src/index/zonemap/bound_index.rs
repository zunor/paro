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

use crate::index::bound_index::{BoundIndex, IndexPredicateEvaluation};
use crate::index::predicate::{compare_bytes, value_to_bytes, Predicate};
use crate::index::predicate_result::{
    decode_page_ranges, encode_page_ranges, PageRange, PredicateResult,
};
use crate::index::{
    ColumnId, Index, IndexAppendInfo, IndexBufferInfo, IndexConstraintType, IndexStorageInfo,
};

use super::{ZoneMapEntry, ZoneMapIndexReader};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PagePredicateTruth {
    AlwaysTrue,
    MaybeTrue,
    AlwaysFalse,
}

enum EncodedPredicate {
    Eq(Vec<u8>),
    NotEq(Vec<u8>),
    Lt { value: Vec<u8>, inclusive: bool },
    Gt { value: Vec<u8>, inclusive: bool },
    Range { lower: Vec<u8>, upper: Vec<u8> },
    In(Vec<Vec<u8>>),
    IsNull,
    IsNotNull,
}

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

    fn page_ranges(&self) -> Option<&[PageRange]> {
        (self.page_ranges.len() == self.reader.num_pages()).then_some(&self.page_ranges)
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

    fn evaluate_zonemap(&self, predicate: &Predicate) -> IndexPredicateEvaluation {
        let (Some(logical_type), Some(ranges)) = (self.logical_type(), self.page_ranges()) else {
            return IndexPredicateEvaluation::candidates_only(PredicateResult::Unknown);
        };
        let Some(predicate) = encode_predicate(predicate, logical_type) else {
            return IndexPredicateEvaluation::candidates_only(PredicateResult::Unknown);
        };
        let mut candidates = Vec::new();
        let mut guaranteed = Vec::new();
        for (entry, range) in self.reader.entries().iter().zip(ranges) {
            let Some(truth) = classify_page(entry, &predicate, logical_type) else {
                return IndexPredicateEvaluation::candidates_only(PredicateResult::Unknown);
            };
            match truth {
                PagePredicateTruth::AlwaysTrue => {
                    candidates.push(*range);
                    guaranteed.push(*range);
                }
                PagePredicateTruth::MaybeTrue => candidates.push(*range),
                PagePredicateTruth::AlwaysFalse => {}
            }
        }
        IndexPredicateEvaluation::new(
            ranges_to_result(candidates, ranges.len()),
            ranges_to_result(guaranteed, ranges.len()),
        )
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

fn encode_predicate(predicate: &Predicate, ty: &LogicalType) -> Option<EncodedPredicate> {
    let encode = |value: &Value| value_to_bytes(value, ty).ok();
    match predicate {
        Predicate::Eq { value, .. } => Some(EncodedPredicate::Eq(encode(value)?)),
        Predicate::NotEq { value, .. } => Some(EncodedPredicate::NotEq(encode(value)?)),
        Predicate::Lt { value, .. } => Some(EncodedPredicate::Lt {
            value: encode(value)?,
            inclusive: false,
        }),
        Predicate::Le { value, .. } => Some(EncodedPredicate::Lt {
            value: encode(value)?,
            inclusive: true,
        }),
        Predicate::Gt { value, .. } => Some(EncodedPredicate::Gt {
            value: encode(value)?,
            inclusive: false,
        }),
        Predicate::Ge { value, .. } => Some(EncodedPredicate::Gt {
            value: encode(value)?,
            inclusive: true,
        }),
        Predicate::Range { lower, upper, .. } => Some(EncodedPredicate::Range {
            lower: encode(lower)?,
            upper: encode(upper)?,
        }),
        Predicate::In { values, .. } => Some(EncodedPredicate::In(
            values.iter().map(encode).collect::<Option<Vec<_>>>()?,
        )),
        Predicate::IsNull { .. } => Some(EncodedPredicate::IsNull),
        Predicate::IsNotNull { .. } => Some(EncodedPredicate::IsNotNull),
        Predicate::FixedIn { .. }
        | Predicate::StringPrefix { .. }
        | Predicate::ColumnComparison { .. } => None,
    }
}

fn classify_page(
    entry: &ZoneMapEntry,
    predicate: &EncodedPredicate,
    ty: &LogicalType,
) -> Option<PagePredicateTruth> {
    use std::cmp::Ordering;

    let compare = |left: &[u8], right: &[u8]| compare_bytes(ty, left, right).ok();
    let contains = |value: &[u8]| {
        Some(
            compare(value, &entry.min)? != Ordering::Less
                && compare(value, &entry.max)? != Ordering::Greater,
        )
    };
    let exact_all_valid = entry.bounds_exact && !entry.has_null;
    match predicate {
        EncodedPredicate::Eq(value) => {
            if !contains(value)? {
                return Some(PagePredicateTruth::AlwaysFalse);
            }
            if exact_all_valid
                && compare(&entry.min, value)? == Ordering::Equal
                && compare(&entry.max, value)? == Ordering::Equal
            {
                Some(PagePredicateTruth::AlwaysTrue)
            } else {
                Some(PagePredicateTruth::MaybeTrue)
            }
        }
        EncodedPredicate::NotEq(value) => {
            let constant_page = entry.bounds_exact
                && compare(&entry.min, value)? == Ordering::Equal
                && compare(&entry.max, value)? == Ordering::Equal;
            if constant_page {
                Some(PagePredicateTruth::AlwaysFalse)
            } else if exact_all_valid && !contains(value)? {
                Some(PagePredicateTruth::AlwaysTrue)
            } else {
                Some(PagePredicateTruth::MaybeTrue)
            }
        }
        EncodedPredicate::Lt { value, inclusive } => {
            let minimum_order = compare(&entry.min, value)?;
            let candidate = if *inclusive {
                minimum_order != Ordering::Greater
            } else {
                minimum_order == Ordering::Less
            };
            if !candidate {
                return Some(PagePredicateTruth::AlwaysFalse);
            }
            let maximum_order = compare(&entry.max, value)?;
            let proven = exact_all_valid
                && if *inclusive {
                    maximum_order != Ordering::Greater
                } else {
                    maximum_order == Ordering::Less
                };
            Some(if proven {
                PagePredicateTruth::AlwaysTrue
            } else {
                PagePredicateTruth::MaybeTrue
            })
        }
        EncodedPredicate::Gt { value, inclusive } => {
            let maximum_order = compare(&entry.max, value)?;
            let candidate = if *inclusive {
                maximum_order != Ordering::Less
            } else {
                maximum_order == Ordering::Greater
            };
            if !candidate {
                return Some(PagePredicateTruth::AlwaysFalse);
            }
            let minimum_order = compare(&entry.min, value)?;
            let proven = exact_all_valid
                && if *inclusive {
                    minimum_order != Ordering::Less
                } else {
                    minimum_order == Ordering::Greater
                };
            Some(if proven {
                PagePredicateTruth::AlwaysTrue
            } else {
                PagePredicateTruth::MaybeTrue
            })
        }
        EncodedPredicate::Range { lower, upper } => {
            if compare(&entry.max, lower)? == Ordering::Less
                || compare(&entry.min, upper)? == Ordering::Greater
            {
                return Some(PagePredicateTruth::AlwaysFalse);
            }
            let proven = exact_all_valid
                && compare(&entry.min, lower)? != Ordering::Less
                && compare(&entry.max, upper)? != Ordering::Greater;
            Some(if proven {
                PagePredicateTruth::AlwaysTrue
            } else {
                PagePredicateTruth::MaybeTrue
            })
        }
        EncodedPredicate::In(values) => {
            let mut page_may_match = false;
            for value in values {
                page_may_match |= contains(value)?;
            }
            if !page_may_match {
                return Some(PagePredicateTruth::AlwaysFalse);
            }
            let constant = exact_all_valid
                && compare(&entry.min, &entry.max)? == Ordering::Equal
                && values
                    .iter()
                    .any(|value| compare(&entry.min, value) == Some(Ordering::Equal));
            Some(if constant {
                PagePredicateTruth::AlwaysTrue
            } else {
                PagePredicateTruth::MaybeTrue
            })
        }
        EncodedPredicate::IsNull => Some(if entry.has_null {
            PagePredicateTruth::MaybeTrue
        } else {
            PagePredicateTruth::AlwaysFalse
        }),
        EncodedPredicate::IsNotNull => Some(if entry.has_null {
            PagePredicateTruth::MaybeTrue
        } else {
            PagePredicateTruth::AlwaysTrue
        }),
    }
}

fn ranges_to_result(ranges: Vec<PageRange>, page_count: usize) -> PredicateResult {
    if ranges.is_empty() {
        PredicateResult::NoneMatch
    } else if ranges.len() == page_count {
        PredicateResult::AllMatch
    } else {
        PredicateResult::PageRanges(ranges)
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
        self.evaluate_predicate_with_proof(predicate).candidates
    }

    fn evaluate_predicate_with_proof(&self, predicate: &Predicate) -> IndexPredicateEvaluation {
        if self.column_ids.len() != 1 || predicate.index_column_id() != Some(self.column_ids[0]) {
            return IndexPredicateEvaluation::candidates_only(PredicateResult::Unknown);
        }
        self.evaluate_zonemap(predicate)
    }

    fn provides_predicate_proof(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::zonemap::{BoundsPrecision, ZoneMapIndexWriter};

    fn i32_cmp(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
        i32::from_le_bytes(left.try_into().unwrap())
            .cmp(&i32::from_le_bytes(right.try_into().unwrap()))
    }

    #[test]
    fn test_zonemap_eval() {
        let mut writer = ZoneMapIndexWriter::new();
        writer.add(
            Bytes::from_static(&[10, 0, 0, 0]),
            Bytes::from_static(&[20, 0, 0, 0]),
            false,
            BoundsPrecision::Exact,
        );
        writer.add(
            Bytes::from_static(&[30, 0, 0, 0]),
            Bytes::from_static(&[40, 0, 0, 0]),
            true,
            BoundsPrecision::Exact,
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
            BoundsPrecision::Exact,
        );
        writer.add(
            Bytes::copy_from_slice(&30_i32.to_le_bytes()),
            Bytes::copy_from_slice(&40_i32.to_le_bytes()),
            false,
            BoundsPrecision::Exact,
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

    #[test]
    fn candidate_and_proof_sets_come_from_one_page_classification() {
        let mut writer = ZoneMapIndexWriter::new();
        writer.add_with_cmp(
            Bytes::copy_from_slice(&10_i32.to_le_bytes()),
            Bytes::copy_from_slice(&10_i32.to_le_bytes()),
            false,
            BoundsPrecision::Exact,
            i32_cmp,
        );
        writer.add_with_cmp(
            Bytes::copy_from_slice(&15_i32.to_le_bytes()),
            Bytes::copy_from_slice(&30_i32.to_le_bytes()),
            false,
            BoundsPrecision::Exact,
            i32_cmp,
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

        let outcome = index.evaluate_predicate_with_proof(&Predicate::Le {
            column_id: 0,
            value: Value::Integer(20),
        });
        assert!(matches!(outcome.candidates, PredicateResult::AllMatch));
        assert_eq!(
            outcome.guaranteed,
            PredicateResult::PageRanges(vec![PageRange::new(0, 3)])
        );
    }

    #[test]
    fn missing_page_layout_disables_pruning_and_proof() {
        let mut writer = ZoneMapIndexWriter::new();
        writer.add_with_cmp(
            Bytes::copy_from_slice(&10_i32.to_le_bytes()),
            Bytes::copy_from_slice(&20_i32.to_le_bytes()),
            false,
            BoundsPrecision::Exact,
            i32_cmp,
        );
        let index = ZoneMapIndex::from_bytes(
            "zm",
            IndexConstraintType::None,
            vec![0],
            vec![LogicalType::Integer],
            writer.finish(),
            Vec::new(),
        )
        .unwrap();
        let outcome = index.evaluate_predicate_with_proof(&Predicate::Eq {
            column_id: 0,
            value: Value::Integer(15),
        });
        assert!(matches!(outcome.candidates, PredicateResult::Unknown));
        assert!(matches!(outcome.guaranteed, PredicateResult::NoneMatch));
    }

    #[test]
    fn inexact_bounds_never_prove_value_predicates() {
        let mut writer = ZoneMapIndexWriter::new();
        writer.add_with_cmp(
            Bytes::copy_from_slice(&10_i32.to_le_bytes()),
            Bytes::copy_from_slice(&20_i32.to_le_bytes()),
            false,
            BoundsPrecision::Conservative,
            i32_cmp,
        );
        let index = ZoneMapIndex::from_bytes(
            "zm",
            IndexConstraintType::None,
            vec![0],
            vec![LogicalType::Integer],
            writer.finish(),
            vec![PageRange::new(0, 3)],
        )
        .unwrap();
        let outcome = index.evaluate_predicate_with_proof(&Predicate::Range {
            column_id: 0,
            lower: Value::Integer(10),
            upper: Value::Integer(20),
        });
        assert!(matches!(outcome.candidates, PredicateResult::AllMatch));
        assert!(matches!(outcome.guaranteed, PredicateResult::NoneMatch));
    }
}
