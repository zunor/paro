// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Column Reader
//!
//! Reads column data with predicate pushdown and row ordinal positioning.
//!
//! ## Architecture
//!
//! ```text
//! ColumnReader
//!   ├── OrdinalIndexReader (row ordinal → page mapping)
//!   ├── ZoneMapIndexReader (page-level min/max filtering)
//!   └── ColumnIterator (sequential/random access)
//! ```
//!
//! ## Usage
//!
//! ```ignore
//! let reader = ColumnReader::create(meta, file, opts, page_reader, None, None)?;
//! let mut iter = reader.new_iterator()?;
//!
//! // Seek to row 1000
//! iter.seek_to_ordinal(1000)?;
//!
//! // Read next 100 rows
//! let (count, data) = iter.next_batch(100)?;
//! ```

use crate::buffer::Prefetcher;
use crate::rowset::encoding::{BinaryPlainPageDecoder, FieldType};
use crate::rowset::page::{CompressionType, EncodingType, PagePointer, PageReadOptions};
use crate::rowset::page_reader::PageReader;
use bytes::{Buf, Bytes};
use paro_common::error::{self as paro_error, Result};
use std::io::{Read, Seek};
use std::path::PathBuf;
use std::sync::Arc;

use super::column_iterator::{ColumnIterator, ScalarColumnIterator};
use super::column_writer::ColumnWriterMeta;

/// Column reader options.
#[derive(Debug, Clone)]
pub struct ColumnReaderOptions {
    /// Whether to verify checksums
    pub verify_checksum: bool,
    /// Compression type used
    pub compression: CompressionType,
}

impl Default for ColumnReaderOptions {
    fn default() -> Self {
        ColumnReaderOptions {
            verify_checksum: true,
            compression: CompressionType::Lz4,
        }
    }
}

impl ColumnReaderOptions {
    pub fn with_verify_checksum(mut self, verify: bool) -> Self {
        self.verify_checksum = verify;
        self
    }

    pub fn with_compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }
}

/// Ordinal index entry: maps first_ordinal to page pointer.
#[derive(Debug, Clone)]
pub struct OrdinalIndexEntry {
    pub first_ordinal: u64,
    pub page_pointer: PagePointer,
}

/// Ordinal index reader - maps row ordinals to page pointers.
#[derive(Debug, Clone)]
pub struct OrdinalIndexReader {
    /// Index entries sorted by first_ordinal
    entries: Vec<OrdinalIndexEntry>,
    /// Total number of rows
    num_rows: u64,
}

impl OrdinalIndexReader {
    /// Create a new ordinal index reader.
    pub fn new(entries: Vec<OrdinalIndexEntry>, num_rows: u64) -> Self {
        OrdinalIndexReader { entries, num_rows }
    }

    /// Create from serialized index data.
    pub fn from_bytes(data: &Bytes) -> Result<Self> {
        if data.len() < 4 {
            return Err(paro_error::data_corrupted(
                "OrdinalIndexReader: data too small",
            ));
        }

        let mut buf = data.as_ref();
        let num_entries = buf.get_u32_le() as usize;

        let mut entries = Vec::with_capacity(num_entries);
        for _ in 0..num_entries {
            if buf.remaining() < 12 {
                return Err(paro_error::data_corrupted(
                    "OrdinalIndexReader: truncated entry",
                ));
            }
            let first_ordinal = buf.get_u64_le();
            let page_pointer = PagePointer::decode_fixed(&mut buf)?;
            entries.push(OrdinalIndexEntry {
                first_ordinal,
                page_pointer,
            });
        }

        // Calculate total rows from last entry
        let num_rows = if entries.is_empty() {
            0
        } else {
            // We don't know the exact count from index alone,
            // it will be set from column metadata
            0
        };

        Ok(OrdinalIndexReader { entries, num_rows })
    }

    /// Set total number of rows (from column metadata).
    pub fn set_num_rows(&mut self, num_rows: u64) {
        self.num_rows = num_rows;
    }

    /// Get the number of pages.
    pub fn num_pages(&self) -> usize {
        self.entries.len()
    }

    /// Get total number of rows.
    pub fn num_rows(&self) -> u64 {
        self.num_rows
    }

    /// Find the page containing the given ordinal using binary search.
    ///
    /// Returns the index of the page that contains the ordinal,
    /// or None if ordinal is out of range.
    pub fn seek_at_or_before(&self, ordinal: u64) -> Option<usize> {
        if self.entries.is_empty() || ordinal >= self.num_rows {
            return None;
        }

        // Binary search for the page containing this ordinal
        let mut left = 0;
        let mut right = self.entries.len();

        while left < right {
            let mid = left + (right - left) / 2;
            if self.entries[mid].first_ordinal <= ordinal {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        // left is now the first entry with first_ordinal > ordinal
        // So the page we want is at left - 1
        if left > 0 {
            Some(left - 1)
        } else {
            Some(0)
        }
    }

    /// Get page entry by index.
    pub fn get_page(&self, idx: usize) -> Option<&OrdinalIndexEntry> {
        self.entries.get(idx)
    }

    /// Get all page entries.
    pub fn entries(&self) -> &[OrdinalIndexEntry] {
        &self.entries
    }
}

/// ZoneMap entry for a single page.
#[derive(Debug, Clone)]
pub struct ZoneMapEntry {
    pub min: Bytes,
    pub max: Bytes,
    pub has_null: bool,
}

/// ZoneMap index reader - stores min/max/has_null per page.
#[derive(Debug, Clone)]
pub struct ZoneMapIndexReader {
    /// Global min value
    pub global_min: Option<Bytes>,
    /// Global max value
    pub global_max: Option<Bytes>,
    /// Global has_null flag
    pub global_has_null: bool,
    /// Per-page zone maps
    entries: Vec<ZoneMapEntry>,
}

impl ZoneMapIndexReader {
    /// Create from serialized index data.
    pub fn from_bytes(data: &Bytes) -> Result<Self> {
        let mut buf = data.as_ref();

        // Read global zone map
        let global_min = Self::read_zonemap_value(&mut buf)?;
        let global_max = Self::read_zonemap_value(&mut buf)?;

        if buf.remaining() < 1 {
            return Err(paro_error::data_corrupted(
                "ZoneMapIndexReader: missing global has_null",
            ));
        }
        let global_has_null = buf.get_u8() != 0;

        // Read per-page zone maps
        if buf.remaining() < 4 {
            return Err(paro_error::data_corrupted(
                "ZoneMapIndexReader: missing entry count",
            ));
        }
        let num_entries = buf.get_u32_le() as usize;

        let mut entries = Vec::with_capacity(num_entries);
        for _ in 0..num_entries {
            let min = Self::read_zonemap_value(&mut buf)?
                .ok_or_else(|| paro_error::data_corrupted("ZoneMapIndexReader: missing min"))?;
            let max = Self::read_zonemap_value(&mut buf)?
                .ok_or_else(|| paro_error::data_corrupted("ZoneMapIndexReader: missing max"))?;

            if buf.remaining() < 1 {
                return Err(paro_error::data_corrupted(
                    "ZoneMapIndexReader: missing has_null",
                ));
            }
            let has_null = buf.get_u8() != 0;

            entries.push(ZoneMapEntry { min, max, has_null });
        }

        Ok(ZoneMapIndexReader {
            global_min,
            global_max,
            global_has_null,
            entries,
        })
    }

    fn read_zonemap_value(buf: &mut &[u8]) -> Result<Option<Bytes>> {
        if buf.remaining() < 4 {
            return Err(paro_error::data_corrupted(
                "ZoneMapIndexReader: missing value length",
            ));
        }
        let len_plus_one = buf.get_u32_le() as usize;
        if len_plus_one == 0 {
            return Ok(None);
        }
        let len = len_plus_one - 1;
        if buf.remaining() < len {
            return Err(paro_error::data_corrupted(
                "ZoneMapIndexReader: truncated value",
            ));
        }
        let value = Bytes::copy_from_slice(&buf[..len]);
        buf.advance(len);
        Ok(Some(value))
    }

    /// Get the number of pages.
    pub fn num_pages(&self) -> usize {
        self.entries.len()
    }

    /// Get zone map for a page.
    pub fn get_page(&self, idx: usize) -> Option<&ZoneMapEntry> {
        self.entries.get(idx)
    }

    /// Check if a page might contain values in the given range.
    ///
    /// Returns true if the page might contain matching values,
    /// false if it can be safely skipped.
    pub fn page_may_contain_range<F>(&self, page_idx: usize, min: &[u8], max: &[u8], cmp: F) -> bool
    where
        F: Fn(&[u8], &[u8]) -> std::cmp::Ordering,
    {
        if let Some(entry) = self.entries.get(page_idx) {
            // Page can be skipped if:
            // - page.max < query.min (all values too small)
            // - page.min > query.max (all values too large)
            let page_max_lt_min = cmp(&entry.max, min) == std::cmp::Ordering::Less;
            let page_min_gt_max = cmp(&entry.min, max) == std::cmp::Ordering::Greater;

            !page_max_lt_min && !page_min_gt_max
        } else {
            true // Unknown page, don't skip
        }
    }

    /// Check if a page might contain a specific value.
    pub fn page_may_contain_value<F>(&self, page_idx: usize, value: &[u8], cmp: F) -> bool
    where
        F: Fn(&[u8], &[u8]) -> std::cmp::Ordering,
    {
        self.page_may_contain_range(page_idx, value, value, cmp)
    }

    /// Check if a page has null values.
    pub fn page_has_null(&self, page_idx: usize) -> bool {
        self.entries.get(page_idx).is_some_and(|e| e.has_null)
    }
}

/// Column reader metadata (subset of ColumnWriterMeta needed for reading).
#[derive(Debug, Clone)]
pub struct ColumnReaderMeta {
    /// Column ID
    pub column_id: u32,
    /// Total number of rows
    pub num_rows: u64,
    /// Encoding type
    pub encoding: EncodingType,
    /// Compression type
    pub compression: CompressionType,
    /// Field type
    pub field_type: FieldType,
    /// Data pages pointer (first page)
    pub data_page_pointer: PagePointer,
    /// Ordinal index pointer
    pub ordinal_index_pointer: PagePointer,
    /// ZoneMap index pointer
    pub zonemap_index_pointer: PagePointer,
    /// Dictionary page pointer (if dictionary encoding)
    pub dict_page_pointer: Option<PagePointer>,
    /// Whether the column is nullable
    pub is_nullable: bool,
    /// Exact column NULL count when persisted by the segment format.
    pub null_count: Option<u64>,
    /// Type size for fixed-width types
    pub type_size: Option<usize>,
}

impl ColumnReaderMeta {
    /// Create from ColumnWriterMeta.
    pub fn from_writer_meta(meta: &ColumnWriterMeta, field_type: FieldType) -> Self {
        ColumnReaderMeta {
            column_id: meta.column_id,
            num_rows: meta.num_rows,
            encoding: meta.encoding,
            compression: meta.compression,
            field_type,
            data_page_pointer: meta.data_page_pointer,
            ordinal_index_pointer: meta.ordinal_index_pointer,
            zonemap_index_pointer: meta.zonemap_index_pointer,
            dict_page_pointer: meta.dict_page_pointer,
            is_nullable: true, // Default to nullable
            null_count: Some(meta.null_count),
            type_size: field_type.size(),
        }
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.is_nullable = nullable;
        self
    }
}

/// Column reader for reading column data.
pub struct ColumnReader<R: Read + Seek> {
    /// Reader metadata
    meta: ColumnReaderMeta,
    /// File reader
    reader: R,
    /// Reader options
    opts: ColumnReaderOptions,
    /// Page reader with cache integration
    page_reader: PageReader,
    /// Prefetcher for background page loads
    prefetcher: Option<Arc<Prefetcher>>,
    /// File path for prefetch tasks
    file_path: Option<PathBuf>,
    /// Ordinal index (loaded lazily)
    ordinal_index: Option<Arc<OrdinalIndexReader>>,
    /// ZoneMap index (loaded lazily)
    zonemap_index: Option<Arc<ZoneMapIndexReader>>,
    /// Parsed dictionary page (loaded lazily, for dict encoding).
    ///
    /// The offset table is validated once here and the immutable decoder is
    /// cloned into iterators. Data pages must not re-parse a column-global
    /// dictionary for every page or scan morsel.
    dictionary: Option<Arc<BinaryPlainPageDecoder>>,
}

impl<R: Read + Seek> ColumnReader<R> {
    /// Create a new column reader.
    pub fn create(
        meta: ColumnReaderMeta,
        reader: R,
        opts: ColumnReaderOptions,
        page_reader: PageReader,
        prefetcher: Option<Arc<Prefetcher>>,
        file_path: Option<PathBuf>,
    ) -> Result<Self> {
        Ok(ColumnReader {
            meta,
            reader,
            opts,
            page_reader,
            prefetcher,
            file_path,
            ordinal_index: None,
            zonemap_index: None,
            dictionary: None,
        })
    }

    /// Create with default options.
    pub fn new(meta: ColumnReaderMeta, reader: R, page_reader: PageReader) -> Result<Self> {
        Self::create(
            meta,
            reader,
            ColumnReaderOptions::default(),
            page_reader,
            None,
            None,
        )
    }

    /// Load the ordinal index.
    fn load_ordinal_index(&mut self) -> Result<()> {
        if self.ordinal_index.is_some() {
            return Ok(());
        }

        let opts = PageReadOptions::new(self.meta.ordinal_index_pointer)
            .with_verify_checksum(self.opts.verify_checksum)
            .with_codec(self.opts.compression);

        let (body, _footer, _) = self.page_reader.read_page(&mut self.reader, &opts)?;

        let mut index = OrdinalIndexReader::from_bytes(&body)?;
        index.set_num_rows(self.meta.num_rows);
        self.ordinal_index = Some(Arc::new(index));

        Ok(())
    }

    /// Load the zonemap index.
    fn load_zonemap_index(&mut self) -> Result<()> {
        if self.zonemap_index.is_some() {
            return Ok(());
        }

        let opts = PageReadOptions::new(self.meta.zonemap_index_pointer)
            .with_verify_checksum(self.opts.verify_checksum)
            .with_codec(self.opts.compression);

        let (body, _footer, _) = self.page_reader.read_page(&mut self.reader, &opts)?;

        self.zonemap_index = Some(Arc::new(ZoneMapIndexReader::from_bytes(&body)?));

        Ok(())
    }

    /// Load the dictionary page (for dictionary encoding).
    fn load_dictionary(&mut self) -> Result<()> {
        if self.dictionary.is_some() {
            return Ok(());
        }

        if let Some(dict_ptr) = self.meta.dict_page_pointer {
            let opts = PageReadOptions::new(dict_ptr)
                .with_verify_checksum(self.opts.verify_checksum)
                .with_codec(self.opts.compression);

            let (body, _footer, _) = self.page_reader.read_page(&mut self.reader, &opts)?;
            let mut dictionary = BinaryPlainPageDecoder::new(body);
            dictionary.init()?;
            self.dictionary = Some(Arc::new(dictionary));
        }

        Ok(())
    }

    /// Get the column metadata.
    pub fn meta(&self) -> &ColumnReaderMeta {
        &self.meta
    }

    /// Get the number of rows.
    pub fn num_rows(&self) -> u64 {
        self.meta.num_rows
    }

    /// Get the ordinal index (loads if needed).
    pub fn ordinal_index(&mut self) -> Result<&OrdinalIndexReader> {
        self.load_ordinal_index()?;
        Ok(self.ordinal_index.as_ref().unwrap())
    }

    /// Get the zonemap index (loads if needed).
    pub fn zonemap_index(&mut self) -> Result<&ZoneMapIndexReader> {
        self.load_zonemap_index()?;
        Ok(self.zonemap_index.as_ref().unwrap())
    }
}

impl<R: Read + Seek + Clone + Send + Sync + 'static> ColumnReader<R> {
    /// Create a new iterator for this column.
    pub fn new_iterator(&mut self) -> Result<Box<dyn ColumnIterator + Send + Sync>> {
        // Load indexes
        self.load_ordinal_index()?;
        self.load_zonemap_index()?;
        self.load_dictionary()?;

        let ordinal_index = self.ordinal_index.clone().unwrap();
        let zonemap_index = self.zonemap_index.clone();
        let dictionary = self.dictionary.clone();

        let iter = ScalarColumnIterator::new(
            self.meta.clone(),
            self.reader.clone(),
            self.opts.clone(),
            self.page_reader.clone(),
            self.prefetcher.clone(),
            self.file_path.clone(),
            ordinal_index,
            zonemap_index,
            dictionary,
        )?;

        Ok(Box::new(iter))
    }

    /// Create a shared reader that can be used to create multiple iterators.
    pub fn into_shared(self) -> Arc<SharedColumnReader<R>> {
        Arc::new(SharedColumnReader {
            meta: self.meta,
            opts: self.opts,
            page_reader: self.page_reader,
            ordinal_index: self.ordinal_index,
            zonemap_index: self.zonemap_index,
            dictionary: self.dictionary,
            _phantom: std::marker::PhantomData,
        })
    }
}

/// Shared column reader that can create multiple iterators.
pub struct SharedColumnReader<R: Read + Seek> {
    meta: ColumnReaderMeta,
    opts: ColumnReaderOptions,
    page_reader: PageReader,
    ordinal_index: Option<Arc<OrdinalIndexReader>>,
    zonemap_index: Option<Arc<ZoneMapIndexReader>>,
    dictionary: Option<Arc<BinaryPlainPageDecoder>>,
    _phantom: std::marker::PhantomData<R>,
}

impl<R: Read + Seek + Clone + Send + Sync + 'static> SharedColumnReader<R> {
    /// Create a new iterator using the provided reader.
    pub fn new_iterator(
        &self,
        reader: R,
        prefetcher: Option<Arc<Prefetcher>>,
        file_path: Option<PathBuf>,
    ) -> Result<Box<dyn ColumnIterator + Send + Sync>> {
        let ordinal_index = self
            .ordinal_index
            .clone()
            .ok_or_else(|| paro_error::internal("SharedColumnReader: ordinal index not loaded"))?;

        let iter = ScalarColumnIterator::new(
            self.meta.clone(),
            reader,
            self.opts.clone(),
            self.page_reader.clone(),
            prefetcher,
            file_path,
            ordinal_index,
            self.zonemap_index.clone(),
            self.dictionary.clone(),
        )?;

        Ok(Box::new(iter))
    }

    /// Get the column metadata.
    pub fn meta(&self) -> &ColumnReaderMeta {
        &self.meta
    }

    /// Get the number of rows.
    pub fn num_rows(&self) -> u64 {
        self.meta.num_rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ordinal_index_seek() {
        // Create test entries
        let entries = vec![
            OrdinalIndexEntry {
                first_ordinal: 0,
                page_pointer: PagePointer::new(100, 1000),
            },
            OrdinalIndexEntry {
                first_ordinal: 1000,
                page_pointer: PagePointer::new(1100, 1000),
            },
            OrdinalIndexEntry {
                first_ordinal: 2000,
                page_pointer: PagePointer::new(2100, 1000),
            },
        ];

        let mut index = OrdinalIndexReader {
            entries,
            num_rows: 3000,
        };
        index.set_num_rows(3000);

        // Test seeking
        assert_eq!(index.seek_at_or_before(0), Some(0));
        assert_eq!(index.seek_at_or_before(500), Some(0));
        assert_eq!(index.seek_at_or_before(999), Some(0));
        assert_eq!(index.seek_at_or_before(1000), Some(1));
        assert_eq!(index.seek_at_or_before(1500), Some(1));
        assert_eq!(index.seek_at_or_before(2000), Some(2));
        assert_eq!(index.seek_at_or_before(2999), Some(2));
        assert_eq!(index.seek_at_or_before(3000), None); // Out of range
    }

    #[test]
    fn test_zonemap_filtering() {
        let entries = vec![
            ZoneMapEntry {
                min: Bytes::from_static(&[10, 0, 0, 0]),
                max: Bytes::from_static(&[20, 0, 0, 0]),
                has_null: false,
            },
            ZoneMapEntry {
                min: Bytes::from_static(&[30, 0, 0, 0]),
                max: Bytes::from_static(&[40, 0, 0, 0]),
                has_null: true,
            },
        ];

        let index = ZoneMapIndexReader {
            global_min: Some(Bytes::from_static(&[10, 0, 0, 0])),
            global_max: Some(Bytes::from_static(&[40, 0, 0, 0])),
            global_has_null: true,
            entries,
        };

        let cmp = |a: &[u8], b: &[u8]| {
            let va = i32::from_le_bytes([a[0], a[1], a[2], a[3]]);
            let vb = i32::from_le_bytes([b[0], b[1], b[2], b[3]]);
            va.cmp(&vb)
        };

        // Page 0: [10, 20]
        // Query: 15 - should match
        assert!(index.page_may_contain_value(0, &[15, 0, 0, 0], cmp));
        // Query: 5 - should not match (too small)
        assert!(!index.page_may_contain_value(0, &[5, 0, 0, 0], cmp));
        // Query: 25 - should not match (too large)
        assert!(!index.page_may_contain_value(0, &[25, 0, 0, 0], cmp));

        // Page 1: [30, 40]
        // Query: 35 - should match
        assert!(index.page_may_contain_value(1, &[35, 0, 0, 0], cmp));
        // Query: 25 - should not match
        assert!(!index.page_may_contain_value(1, &[25, 0, 0, 0], cmp));

        // Check has_null
        assert!(!index.page_has_null(0));
        assert!(index.page_has_null(1));
    }
}
