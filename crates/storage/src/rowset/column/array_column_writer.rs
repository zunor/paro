// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Array Column Writer
//!
//! Writes array column data as three sub-columns: offsets, elements, and nulls.
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

use super::column_writer::{
    ColumnWriter, ColumnWriterMeta, ColumnWriterOptions, DataWriter, ScalarColumnWriter,
};
use crate::rowset::encoding::FieldType;
use crate::rowset::page::{CompressionType, EncodingType};
use paro_common::error::{self as paro_error, Result};
// no std::io imports needed here

/// Array column writer metadata.
#[derive(Debug, Clone)]
pub struct ArrayColumnWriterMeta {
    /// Column ID
    pub column_id: u32,
    /// Total number of arrays written
    pub num_rows: u64,
    /// Offsets column metadata
    pub offsets_meta: ColumnWriterMeta,
    /// Elements column metadata
    pub elements_meta: ColumnWriterMeta,
    /// Null column metadata (if nullable)
    pub null_meta: Option<ColumnWriterMeta>,
}

/// Array column writer for nested array types.
///
/// Array columns are stored as three sub-columns:
/// - Offsets column: stores the start offset of each array (u32)
/// - Elements column: stores all array elements contiguously
/// - Null column: stores null flags for each array (optional)
pub struct ArrayColumnWriter<W: DataWriter + Clone + 'static> {
    /// Column ID
    column_id: u32,
    /// Offsets column writer (stores array start positions)
    offsets_writer: ScalarColumnWriter<W>,
    /// Elements column writer (stores all array elements)
    elements_writer: ScalarColumnWriter<W>,
    /// Null column writer (stores null flags for arrays)
    null_writer: Option<ScalarColumnWriter<W>>,
    /// Total arrays written
    num_rows: u64,
    /// Current element offset (for tracking array boundaries)
    current_element_offset: u64,
}

impl<W: DataWriter + Clone + 'static> ArrayColumnWriter<W> {
    /// Create a new array column writer.
    ///
    /// # Arguments
    /// * `column_id` - Column identifier
    /// * `element_type` - Type of array elements
    /// * `is_nullable` - Whether arrays can be null
    /// * `writer` - File writer (will be cloned for sub-columns)
    /// * `page_size` - Target page size
    /// * `compression` - Compression type
    pub fn new(
        column_id: u32,
        element_type: FieldType,
        is_nullable: bool,
        writer: W,
        page_size: usize,
        compression: CompressionType,
    ) -> Result<Self> {
        // Offsets column: stores u32 offsets, not nullable
        let offsets_opts = ColumnWriterOptions::new(FieldType::Int, column_id)
            .with_nullable(false)
            .with_page_size(page_size)
            .with_compression(compression)
            .with_encoding(EncodingType::BitShuffle);
        let offsets_writer = ScalarColumnWriter::new(offsets_opts, writer.clone())?;

        // Elements column: stores actual array elements
        let elements_opts = ColumnWriterOptions::new(element_type, column_id)
            .with_nullable(false) // Element nullability handled separately
            .with_page_size(page_size)
            .with_compression(compression);
        let elements_writer = ScalarColumnWriter::new(elements_opts, writer.clone())?;

        // Null column: stores null flags for arrays (if nullable)
        let null_writer = if is_nullable {
            let null_opts = ColumnWriterOptions::new(FieldType::Boolean, column_id)
                .with_nullable(false)
                .with_page_size(page_size)
                .with_compression(compression)
                .with_encoding(EncodingType::Rle);
            Some(ScalarColumnWriter::new(null_opts, writer)?)
        } else {
            None
        };

        Ok(ArrayColumnWriter {
            column_id,
            offsets_writer,
            elements_writer,
            null_writer,
            num_rows: 0,
            current_element_offset: 0,
        })
    }

    /// Append arrays to the column.
    ///
    /// # Arguments
    /// * `offsets` - Array of offsets (length = num_arrays + 1)
    /// * `elements` - Raw element data
    /// * `element_count` - Total number of elements
    /// * `null_flags` - Null bitmap for arrays (1 bit per array, 1 = null)
    /// * `num_arrays` - Number of arrays to append
    pub fn append(
        &mut self,
        offsets: &[u32],
        elements: &[u8],
        element_count: u32,
        null_flags: Option<&[u8]>,
        num_arrays: u32,
    ) -> Result<()> {
        if num_arrays == 0 {
            return Ok(());
        }

        // Validate offsets length
        if offsets.len() != (num_arrays + 1) as usize {
            return Err(paro_error::internal(format!(
                "ArrayColumnWriter: expected {} offsets, got {}",
                num_arrays + 1,
                offsets.len()
            )));
        }

        // Write offsets (adjusted by current_element_offset)
        let adjusted_offsets: Vec<u32> = offsets
            .iter()
            .map(|&o| (o as u64 + self.current_element_offset) as u32)
            .collect();
        let offsets_bytes: Vec<u8> = adjusted_offsets
            .iter()
            .flat_map(|o| o.to_le_bytes())
            .collect();

        // Write all offsets except the last one (which is the start of next batch)
        self.offsets_writer
            .append(&offsets_bytes[..offsets_bytes.len() - 4], None, num_arrays)?;

        // Write elements
        self.elements_writer.append(elements, None, element_count)?;

        // Write null flags if nullable
        if let Some(ref mut null_writer) = self.null_writer {
            if let Some(flags) = null_flags {
                // Convert bit flags to byte array (1 byte per array)
                let null_bytes: Vec<u8> = (0..num_arrays as usize)
                    .map(|i| {
                        let byte_idx = i / 8;
                        let bit_idx = i % 8;
                        if byte_idx < flags.len() && (flags[byte_idx] >> bit_idx) & 1 == 1 {
                            1u8
                        } else {
                            0u8
                        }
                    })
                    .collect();
                null_writer.append(&null_bytes, None, num_arrays)?;
            } else {
                // No nulls - write all zeros
                let null_bytes = vec![0u8; num_arrays as usize];
                null_writer.append(&null_bytes, None, num_arrays)?;
            }
        }

        self.num_rows += num_arrays as u64;
        self.current_element_offset += element_count as u64;

        Ok(())
    }

    /// Append a single array.
    ///
    /// # Arguments
    /// * `elements` - Raw element data for this array
    /// * `element_count` - Number of elements in this array
    /// * `is_null` - Whether this array is null
    pub fn append_array(
        &mut self,
        elements: &[u8],
        element_count: u32,
        is_null: bool,
    ) -> Result<()> {
        // Write offset
        let offset = self.current_element_offset as u32;
        let offset_bytes = offset.to_le_bytes();
        self.offsets_writer.append(&offset_bytes, None, 1)?;

        // Write elements (only if not null and has elements)
        if !is_null && element_count > 0 {
            self.elements_writer.append(elements, None, element_count)?;
            self.current_element_offset += element_count as u64;
        }

        // Write null flag
        if let Some(ref mut null_writer) = self.null_writer {
            let null_byte = if is_null { 1u8 } else { 0u8 };
            null_writer.append(&[null_byte], None, 1)?;
        }

        self.num_rows += 1;

        Ok(())
    }

    /// Flush current pages.
    pub fn finish_current_page(&mut self) -> Result<()> {
        self.offsets_writer.finish_current_page()?;
        self.elements_writer.finish_current_page()?;
        if let Some(ref mut null_writer) = self.null_writer {
            null_writer.finish_current_page()?;
        }
        Ok(())
    }

    /// Finish writing and return metadata.
    pub fn finish(&mut self) -> Result<ArrayColumnWriterMeta> {
        // Write final offset (total element count)
        let final_offset = self.current_element_offset as u32;
        let final_offset_bytes = final_offset.to_le_bytes();
        self.offsets_writer.append(&final_offset_bytes, None, 1)?;

        let offsets_meta = self.offsets_writer.finish()?;
        let elements_meta = self.elements_writer.finish()?;
        let null_meta = if let Some(ref mut null_writer) = self.null_writer {
            Some(null_writer.finish()?)
        } else {
            None
        };

        Ok(ArrayColumnWriterMeta {
            column_id: self.column_id,
            num_rows: self.num_rows,
            offsets_meta,
            elements_meta,
            null_meta,
        })
    }

    /// Get the number of arrays written.
    pub fn num_rows(&self) -> u64 {
        self.num_rows
    }

    /// Get the total number of elements written.
    pub fn num_elements(&self) -> u64 {
        self.current_element_offset
    }
}

#[cfg(test)]
mod tests {
    use super::super::column_writer::DEFAULT_PAGE_SIZE;
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_array_column_writer_basic() {
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
        // Offsets: [0, 2, 3, 3, 6]
        // Elements: [1, 2, 3, 4, 5, 6]
        let offsets: Vec<u32> = vec![0, 2, 3, 3, 6];
        let elements: Vec<i32> = vec![1, 2, 3, 4, 5, 6];
        let element_bytes: Vec<u8> = elements.iter().flat_map(|v| v.to_le_bytes()).collect();

        writer.append(&offsets, &element_bytes, 6, None, 4).unwrap();

        assert_eq!(writer.num_rows(), 4);
        assert_eq!(writer.num_elements(), 6);

        let meta = writer.finish().unwrap();
        assert_eq!(meta.num_rows, 4);
    }

    #[test]
    fn test_array_column_writer_with_nulls() {
        let buffer = Cursor::new(Vec::new());
        let mut writer = ArrayColumnWriter::new(
            0,
            FieldType::Int,
            true, // nullable
            buffer,
            DEFAULT_PAGE_SIZE,
            CompressionType::Lz4,
        )
        .unwrap();

        // Write arrays: [[1, 2], NULL, [3, 4]]
        // Offsets: [0, 2, 2, 4]
        // Elements: [1, 2, 3, 4]
        // Null flags: [0, 1, 0] -> 0b010 = 2
        let offsets: Vec<u32> = vec![0, 2, 2, 4];
        let elements: Vec<i32> = vec![1, 2, 3, 4];
        let element_bytes: Vec<u8> = elements.iter().flat_map(|v| v.to_le_bytes()).collect();
        let null_flags: Vec<u8> = vec![0b00000010];

        writer
            .append(&offsets, &element_bytes, 4, Some(&null_flags), 3)
            .unwrap();

        assert_eq!(writer.num_rows(), 3);

        let meta = writer.finish().unwrap();
        assert_eq!(meta.num_rows, 3);
        assert!(meta.null_meta.is_some());
    }

    #[test]
    fn test_array_column_writer_single_arrays() {
        let buffer = Cursor::new(Vec::new());
        let mut writer = ArrayColumnWriter::new(
            0,
            FieldType::Int,
            true,
            buffer,
            DEFAULT_PAGE_SIZE,
            CompressionType::None,
        )
        .unwrap();

        // Write arrays one by one
        // Array 1: [1, 2, 3]
        let arr1: Vec<i32> = vec![1, 2, 3];
        let arr1_bytes: Vec<u8> = arr1.iter().flat_map(|v| v.to_le_bytes()).collect();
        writer.append_array(&arr1_bytes, 3, false).unwrap();

        // Array 2: NULL
        writer.append_array(&[], 0, true).unwrap();

        // Array 3: [4, 5]
        let arr3: Vec<i32> = vec![4, 5];
        let arr3_bytes: Vec<u8> = arr3.iter().flat_map(|v| v.to_le_bytes()).collect();
        writer.append_array(&arr3_bytes, 2, false).unwrap();

        // Array 4: [] (empty, not null)
        writer.append_array(&[], 0, false).unwrap();

        assert_eq!(writer.num_rows(), 4);
        assert_eq!(writer.num_elements(), 5);

        let meta = writer.finish().unwrap();
        assert_eq!(meta.num_rows, 4);
    }

    #[test]
    fn test_array_column_writer_empty() {
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

        assert_eq!(writer.num_rows(), 0);
        assert_eq!(writer.num_elements(), 0);

        let meta = writer.finish().unwrap();
        assert_eq!(meta.num_rows, 0);
    }

    #[test]
    fn test_array_column_writer_varchar_elements() {
        let buffer = Cursor::new(Vec::new());
        let mut writer = ArrayColumnWriter::new(
            0,
            FieldType::Varchar,
            false,
            buffer,
            DEFAULT_PAGE_SIZE,
            CompressionType::None,
        )
        .unwrap();

        // Write arrays of strings: [["hello", "world"], ["foo"]]
        // Elements as length-prefixed strings
        let mut element_bytes = Vec::new();
        for s in &["hello", "world", "foo"] {
            element_bytes.extend_from_slice(&(s.len() as u32).to_le_bytes());
            element_bytes.extend_from_slice(s.as_bytes());
        }

        let offsets: Vec<u32> = vec![0, 2, 3];
        writer.append(&offsets, &element_bytes, 3, None, 2).unwrap();

        assert_eq!(writer.num_rows(), 2);
        assert_eq!(writer.num_elements(), 3);

        let meta = writer.finish().unwrap();
        assert_eq!(meta.num_rows, 2);
    }
}
