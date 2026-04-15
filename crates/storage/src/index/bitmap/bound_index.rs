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
    ColumnId, Index, IndexAppendInfo, IndexBufferInfo, IndexConstraintType, IndexStorageInfo,
};

use super::{BitmapIndexReader, BitmapIndexWriter};

/// Bound Bitmap index.
pub struct BitmapIndex {
    name: String,
    constraint_type: IndexConstraintType,
    column_ids: Vec<ColumnId>,
    logical_types: Vec<LogicalType>,
    reader: BitmapIndexReader,
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
        Ok(Self {
            name: name.into(),
            constraint_type,
            column_ids,
            logical_types,
            reader,
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
        self.index_data.len()
    }

    fn serialize_to_disk(&self) -> Result<IndexStorageInfo> {
        Ok(self.storage_info_with_data())
    }

    fn evaluate_predicate(&self, predicate: &Predicate) -> PredicateResult {
        if self.column_ids.len() != 1 {
            return PredicateResult::Unknown;
        }
        if predicate.column_id() != self.column_ids[0] {
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
        }
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
}
