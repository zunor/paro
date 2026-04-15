//! # Array Column Reader
//!
//! Reads array column data stored as three sub-columns: offsets, elements, and nulls.
//!
//! ## Architecture
//!
//! Array columns are stored as:
//! - Offsets column: stores the start offset of each array (u32)
//! - Elements column: stores all array elements contiguously
//! - Null column: stores null flags for each array (optional)
//!
//! ## Example
//!
//! For arrays: [[1, 2], [3], [], [4, 5, 6]]
//! - Offsets: [0, 2, 3, 3, 6]
//! - Elements: [1, 2, 3, 4, 5, 6]

use crate::rowset::encoding::FieldType;
use crate::rowset::page_reader::PageReader;
use bytes::Bytes;
use paro_common::error::{self as paro_error, Result};
use std::io::{Read, Seek};

use super::array_column_writer::ArrayColumnWriterMeta;
use super::column_iterator::ColumnIterator;
use super::column_reader::{ColumnReader, ColumnReaderMeta, ColumnReaderOptions};

/// Array column reader metadata.
#[derive(Debug, Clone)]
pub struct ArrayColumnReaderMeta {
    /// Column ID
    pub column_id: u32,
    /// Total number of arrays
    pub num_rows: u64,
    /// Element type
    pub element_type: FieldType,
    /// Offsets column metadata
    pub offsets_meta: ColumnReaderMeta,
    /// Elements column metadata
    pub elements_meta: ColumnReaderMeta,
    /// Null column metadata (if nullable)
    pub null_meta: Option<ColumnReaderMeta>,
}

impl ArrayColumnReaderMeta {
    /// Create from ArrayColumnWriterMeta.
    pub fn from_writer_meta(meta: &ArrayColumnWriterMeta, element_type: FieldType) -> Self {
        ArrayColumnReaderMeta {
            column_id: meta.column_id,
            num_rows: meta.num_rows,
            element_type,
            offsets_meta: ColumnReaderMeta::from_writer_meta(&meta.offsets_meta, FieldType::Int),
            elements_meta: ColumnReaderMeta::from_writer_meta(&meta.elements_meta, element_type),
            null_meta: meta
                .null_meta
                .as_ref()
                .map(|m| ColumnReaderMeta::from_writer_meta(m, FieldType::Boolean)),
        }
    }
}

/// Array column iterator.
pub struct ArrayColumnIterator<R: Read + Seek + Clone + Send + Sync + 'static> {
    /// Offsets iterator
    offsets_iter: Box<dyn ColumnIterator + Send + Sync>,
    /// Elements iterator
    elements_iter: Box<dyn ColumnIterator + Send + Sync>,
    /// Null iterator (optional)
    null_iter: Option<Box<dyn ColumnIterator + Send + Sync>>,
    /// Total number of arrays
    num_rows: u64,
    /// Current array ordinal
    current_ordinal: u64,
    /// Element type size (for fixed-width types)
    element_type_size: Option<usize>,
    /// Phantom data for R
    _phantom: std::marker::PhantomData<R>,
}

impl<R: Read + Seek + Clone + Send + Sync + 'static> ArrayColumnIterator<R> {
    /// Create a new array column iterator.
    pub fn new(
        meta: ArrayColumnReaderMeta,
        reader: R,
        opts: ColumnReaderOptions,
        page_reader: PageReader,
    ) -> Result<Self> {
        // Create offsets reader
        let mut offsets_reader = ColumnReader::create(
            meta.offsets_meta.clone(),
            reader.clone(),
            opts.clone(),
            page_reader.clone(),
            None,
            None,
        )?;
        let offsets_iter = offsets_reader.new_iterator()?;

        // Create elements reader
        let mut elements_reader = ColumnReader::create(
            meta.elements_meta.clone(),
            reader.clone(),
            opts.clone(),
            page_reader.clone(),
            None,
            None,
        )?;
        let elements_iter = elements_reader.new_iterator()?;

        // Create null reader if nullable
        let null_iter = if let Some(null_meta) = meta.null_meta {
            let mut null_reader =
                ColumnReader::create(null_meta, reader, opts, page_reader, None, None)?;
            Some(null_reader.new_iterator()?)
        } else {
            None
        };

        Ok(ArrayColumnIterator {
            offsets_iter,
            elements_iter,
            null_iter,
            num_rows: meta.num_rows,
            current_ordinal: 0,
            element_type_size: meta.element_type.size(),
            _phantom: std::marker::PhantomData,
        })
    }

    /// Seek to a specific array ordinal.
    pub fn seek_to_ordinal(&mut self, ordinal: u64) -> Result<()> {
        if ordinal > self.num_rows {
            return Err(paro_error::out_of_range(format!(
                "ordinal {} > num_rows {}",
                ordinal, self.num_rows
            )));
        }

        self.offsets_iter.seek_to_ordinal(ordinal)?;
        if let Some(ref mut null_iter) = self.null_iter {
            null_iter.seek_to_ordinal(ordinal)?;
        }

        self.current_ordinal = ordinal;
        Ok(())
    }

    /// Read the next batch of arrays.
    ///
    /// # Returns
    /// Vector of (is_null, elements_data) tuples
    pub fn next_batch(&mut self, n: usize) -> Result<Vec<ArrayValue>> {
        if self.current_ordinal >= self.num_rows {
            return Ok(Vec::new());
        }

        let to_read = std::cmp::min(n, (self.num_rows - self.current_ordinal) as usize);
        if to_read == 0 {
            return Ok(Vec::new());
        }

        // Read offsets (need n+1 to get array boundaries)
        let (offset_count, offset_batch) = self.offsets_iter.next_batch(to_read + 1)?;
        let offset_data = offset_batch.data;
        if offset_count < 2 {
            return Ok(Vec::new());
        }

        // Parse offsets
        let mut offsets = Vec::with_capacity(offset_count);
        for i in 0..offset_count {
            let offset = u32::from_le_bytes([
                offset_data[i * 4],
                offset_data[i * 4 + 1],
                offset_data[i * 4 + 2],
                offset_data[i * 4 + 3],
            ]);
            offsets.push(offset);
        }

        // Read null flags if present
        let null_flags = if let Some(ref mut null_iter) = self.null_iter {
            let (_null_count, null_batch) = null_iter.next_batch(to_read)?;
            Some(null_batch.data.to_vec())
        } else {
            None
        };

        // Calculate total elements to read
        let first_offset = offsets[0];
        let last_offset = offsets[offset_count - 1];
        let total_elements = (last_offset - first_offset) as usize;

        // Seek elements iterator to the first element
        self.elements_iter.seek_to_ordinal(first_offset as u64)?;

        // Read all elements
        let (_elem_count, elem_batch) = self.elements_iter.next_batch(total_elements)?;
        let elem_data = elem_batch.data;

        // Build result arrays
        let num_arrays = offset_count - 1;
        let mut result = Vec::with_capacity(num_arrays);

        for i in 0..num_arrays {
            let is_null = null_flags
                .as_ref()
                .map(|flags| flags.get(i).copied().unwrap_or(0) != 0)
                .unwrap_or(false);

            let start = (offsets[i] - first_offset) as usize;
            let end = (offsets[i + 1] - first_offset) as usize;

            let elements = if is_null || start >= end {
                Bytes::new()
            } else if let Some(type_size) = self.element_type_size {
                // Fixed-width elements
                let byte_start = start * type_size;
                let byte_end = end * type_size;
                if byte_end <= elem_data.len() {
                    elem_data.slice(byte_start..byte_end)
                } else {
                    Bytes::new()
                }
            } else {
                // Variable-width elements - need to handle differently
                // For now, return the raw data
                elem_data.slice(start..std::cmp::min(end, elem_data.len()))
            };

            result.push(ArrayValue {
                is_null,
                num_elements: (offsets[i + 1] - offsets[i]) as usize,
                elements,
            });
        }

        self.current_ordinal += num_arrays as u64;
        Ok(result)
    }

    /// Get the current array ordinal.
    pub fn current_ordinal(&self) -> u64 {
        self.current_ordinal
    }

    /// Get the total number of arrays.
    pub fn num_rows(&self) -> u64 {
        self.num_rows
    }

    /// Check if there are more arrays to read.
    pub fn has_remaining(&self) -> bool {
        self.current_ordinal < self.num_rows
    }
}

/// Represents a single array value.
#[derive(Debug, Clone)]
pub struct ArrayValue {
    /// Whether this array is null
    pub is_null: bool,
    /// Number of elements in the array
    pub num_elements: usize,
    /// Raw element data
    pub elements: Bytes,
}

impl ArrayValue {
    /// Check if the array is empty (not null, but has no elements).
    pub fn is_empty(&self) -> bool {
        !self.is_null && self.num_elements == 0
    }

    /// Get elements as i32 values (for Int arrays).
    pub fn as_i32_vec(&self) -> Vec<i32> {
        if self.is_null || self.elements.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(self.num_elements);
        for i in 0..self.num_elements {
            let offset = i * 4;
            if offset + 4 <= self.elements.len() {
                let value = i32::from_le_bytes([
                    self.elements[offset],
                    self.elements[offset + 1],
                    self.elements[offset + 2],
                    self.elements[offset + 3],
                ]);
                result.push(value);
            }
        }
        result
    }

    /// Get elements as i64 values (for BigInt arrays).
    pub fn as_i64_vec(&self) -> Vec<i64> {
        if self.is_null || self.elements.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(self.num_elements);
        for i in 0..self.num_elements {
            let offset = i * 8;
            if offset + 8 <= self.elements.len() {
                let value = i64::from_le_bytes([
                    self.elements[offset],
                    self.elements[offset + 1],
                    self.elements[offset + 2],
                    self.elements[offset + 3],
                    self.elements[offset + 4],
                    self.elements[offset + 5],
                    self.elements[offset + 6],
                    self.elements[offset + 7],
                ]);
                result.push(value);
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rowset::column::array_column_writer::ArrayColumnWriter;
    use crate::rowset::column::column_writer::DEFAULT_PAGE_SIZE;
    use crate::rowset::page::CompressionType;
    use std::io::Cursor;

    #[test]
    fn test_array_iterator_basic() {
        // Create test data
        let buffer = Cursor::new(Vec::new());
        let mut writer = ArrayColumnWriter::new(
            0,
            FieldType::Int,
            false,
            buffer,
            DEFAULT_PAGE_SIZE,
            CompressionType::None,
        )
        .unwrap();

        // Write arrays: [[1, 2], [3], [], [4, 5, 6]]
        let offsets: Vec<u32> = vec![0, 2, 3, 3, 6];
        let elements: Vec<i32> = vec![1, 2, 3, 4, 5, 6];
        let element_bytes: Vec<u8> = elements.iter().flat_map(|v| v.to_le_bytes()).collect();

        writer.append(&offsets, &element_bytes, 6, None, 4).unwrap();

        let meta = writer.finish().unwrap();
        assert_eq!(meta.num_rows, 4);
    }
}
