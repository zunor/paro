//! # Bloom Filter Bound Index
//!
//! BoundIndex wrapper for BloomFilterIndexReader.

use bytes::Bytes;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::index::bound_index::BoundIndex;
use crate::index::predicate::{value_to_bytes, Predicate};
use crate::index::predicate_result::{
    decode_page_ranges, encode_page_ranges, PageRange, PredicateResult,
};
use crate::index::{
    ColumnId, Index, IndexAppendInfo, IndexBufferInfo, IndexConstraintType, IndexStorageInfo,
};

use super::{BloomFilterIndexReader, BloomFilterIndexWriter};

/// Bound Bloom Filter index.
pub struct BloomFilterIndex {
    name: String,
    constraint_type: IndexConstraintType,
    column_ids: Vec<ColumnId>,
    logical_types: Vec<LogicalType>,
    reader: BloomFilterIndexReader,
    index_data: Bytes,
    page_ranges: Vec<PageRange>,
}

impl BloomFilterIndex {
    /// Index type name.
    pub const TYPE_NAME: &'static str = "BLOOM";

    /// Build from serialized bytes and page ranges.
    pub fn from_bytes(
        name: impl Into<String>,
        constraint_type: IndexConstraintType,
        column_ids: Vec<ColumnId>,
        logical_types: Vec<LogicalType>,
        index_data: Bytes,
        page_ranges: Vec<PageRange>,
    ) -> Result<Self> {
        let reader = BloomFilterIndexReader::from_bytes(&index_data)?;
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

    /// Build a bound index directly from a writer.
    pub fn from_writer(
        name: impl Into<String>,
        constraint_type: IndexConstraintType,
        column_ids: Vec<ColumnId>,
        logical_types: Vec<LogicalType>,
        writer: &mut BloomFilterIndexWriter,
        page_ranges: Vec<PageRange>,
    ) -> Result<Self> {
        let index_data = writer.finish();
        Self::from_bytes(
            name,
            constraint_type,
            column_ids,
            logical_types,
            index_data,
            page_ranges,
        )
    }

    fn logical_type(&self) -> Option<&LogicalType> {
        self.logical_types.first()
    }

    fn page_ranges_or_default(&self) -> Vec<PageRange> {
        let num_pages = self.reader.num_filters();
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

    fn evaluate_eq(&self, value: &Value) -> PredicateResult {
        let logical_type = match self.logical_type() {
            Some(t) => t,
            None => return PredicateResult::Unknown,
        };
        let Ok(bytes) = value_to_bytes(value, logical_type) else {
            return PredicateResult::Unknown;
        };

        let ranges = self.page_ranges_or_default();
        let mut valid = Vec::new();
        for (idx, range) in ranges.iter().enumerate() {
            if idx >= self.reader.num_filters() {
                break;
            }
            match self.reader.page_may_contain(idx, &bytes) {
                Ok(true) => valid.push(*range),
                Ok(false) => {}
                Err(_) => return PredicateResult::Unknown,
            }
        }

        if valid.is_empty() {
            PredicateResult::NoneMatch
        } else {
            PredicateResult::PageRanges(valid)
        }
    }

    fn evaluate_in(&self, values: &[Value]) -> PredicateResult {
        let logical_type = match self.logical_type() {
            Some(t) => t,
            None => return PredicateResult::Unknown,
        };

        let mut encoded_values = Vec::with_capacity(values.len());
        for value in values {
            match value_to_bytes(value, logical_type) {
                Ok(bytes) => encoded_values.push(bytes),
                Err(_) => return PredicateResult::Unknown,
            }
        }

        let ranges = self.page_ranges_or_default();
        let mut valid = Vec::new();
        for (idx, range) in ranges.iter().enumerate() {
            if idx >= self.reader.num_filters() {
                break;
            }
            let mut page_hit = false;
            for bytes in &encoded_values {
                match self.reader.page_may_contain(idx, bytes) {
                    Ok(true) => {
                        page_hit = true;
                        break;
                    }
                    Ok(false) => {}
                    Err(_) => return PredicateResult::Unknown,
                }
            }
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

    /// Load from IndexStorageInfo buffers/options.
    pub fn from_storage_info(
        input: &crate::index::CreateIndexInput,
    ) -> Result<Arc<dyn BoundIndex>> {
        let storage = input
            .storage_info
            .ok_or_else(|| paro_error::invalid_input("BloomFilterIndex: missing storage info"))?;
        let data = storage
            .buffers
            .first()
            .and_then(|bufs| bufs.first())
            .ok_or_else(|| paro_error::data_corrupted("BloomFilterIndex: missing buffer"))?;
        let index_data = Bytes::copy_from_slice(&data.data);

        let page_ranges = storage
            .options
            .get("page_ranges")
            .and_then(|value| match value {
                Value::Blob(bytes) => decode_page_ranges(bytes).ok(),
                _ => None,
            })
            .unwrap_or_default();

        let index = BloomFilterIndex::from_bytes(
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

impl Index for BloomFilterIndex {
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

impl BoundIndex for BloomFilterIndex {
    fn physical_types(&self) -> &[LogicalType] {
        &self.logical_types
    }

    fn logical_types(&self) -> &[LogicalType] {
        &self.logical_types
    }

    fn append(&self, _chunk: &Chunk, _row_ids: &Vector) -> Result<()> {
        Err(paro_error::not_implemented("BloomFilterIndex::append"))
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
        Err(paro_error::not_implemented("BloomFilterIndex::delete"))
    }

    fn insert(&self, _chunk: &Chunk, _row_ids: &Vector) -> Result<()> {
        Err(paro_error::not_implemented("BloomFilterIndex::insert"))
    }

    fn merge_indexes(&self, _other: &dyn BoundIndex) -> Result<bool> {
        Err(paro_error::not_implemented(
            "BloomFilterIndex::merge_indexes",
        ))
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
        if predicate.column_id() != self.column_ids[0] {
            return PredicateResult::Unknown;
        }

        match predicate {
            Predicate::Eq { value, .. } => self.evaluate_eq(value),
            Predicate::In { values, .. } => self.evaluate_in(values),
            // Bloom filter can only check existence (Eq/In), not ordering or negation.
            _ => PredicateResult::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::bloom::BloomFilterOptions;

    #[test]
    fn test_bloom_filter_eval() {
        let opts = BloomFilterOptions::default().with_fpp(0.0001);
        let mut writer = BloomFilterIndexWriter::new(opts);

        // Page 0
        writer.add_value(b"apple");
        writer.add_value(b"banana");
        writer.flush();

        // Page 1
        writer.add_value(b"cherry");
        writer.flush();

        let ranges = vec![PageRange::new(0, 2), PageRange::new(2, 3)];
        let index = BloomFilterIndex::from_writer(
            "bf",
            IndexConstraintType::None,
            vec![0],
            vec![LogicalType::Varchar],
            &mut writer,
            ranges,
        )
        .unwrap();

        let predicate = Predicate::Eq {
            column_id: 0,
            value: Value::Varchar("banana".to_string()),
        };
        let result = index.evaluate_predicate(&predicate);
        match result {
            PredicateResult::PageRanges(ranges) => {
                assert!(ranges.iter().any(|r| *r == PageRange::new(0, 2)));
            }
            _ => panic!("expected page ranges"),
        }
    }
}
