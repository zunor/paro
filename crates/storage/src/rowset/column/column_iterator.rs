// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Column Iterator
//!
//! Iterates over column values with support for seeking and batch reading.
//!
//! ## Features
//!
//! - `seek_to_ordinal()`: Position to a specific row
//! - `next_batch()`: Read multiple values efficiently
//! - `read_by_rowids()`: Random access by row IDs
//! - ZoneMap filtering for predicate pushdown

use crate::buffer::{PrefetchItem, Prefetcher};
use crate::rowset::encoding::{
    BinaryDictPageDecoder, BinaryPlainPageDecoder, BitShufflePageDecoder, PlainPageDecoder,
    RlePageDecoder,
};
use crate::rowset::page::{EncodingType, NullEncoding, PageFooter, PageReadOptions};
use crate::rowset::page_reader::PageReader;
use bytes::Bytes;
use paro_common::error::{self as paro_error, Result};
use std::io::{Read, Seek};
use std::path::PathBuf;
use std::sync::Arc;

use super::column_reader::{
    ColumnReaderMeta, ColumnReaderOptions, OrdinalIndexReader, ZoneMapIndexReader,
};

/// Column batch returned by iterators.
#[derive(Debug, Clone)]
pub struct ColumnBatch {
    /// Raw data bytes (decoded at page level, still encoded per column type)
    pub data: Bytes,
    /// Per-row null flags (1 byte per value, 1 = NULL)
    pub nulls: Option<Bytes>,
    /// Optional storage dictionary payload for storage-aware dictionary execution.
    pub storage_dictionary: Option<StorageDictionaryBatch>,
}

#[derive(Debug, Clone)]
pub struct StorageDictionaryBatch {
    pub dictionary: Bytes,
    pub codes: Bytes,
}

impl ColumnBatch {
    pub fn new(data: Bytes, nulls: Option<Bytes>) -> Self {
        Self {
            data,
            nulls,
            storage_dictionary: None,
        }
    }

    pub fn with_storage_dictionary(dictionary: Bytes, codes: Bytes, nulls: Option<Bytes>) -> Self {
        Self {
            data: codes.clone(),
            nulls,
            storage_dictionary: Some(StorageDictionaryBatch { dictionary, codes }),
        }
    }

    pub fn empty() -> Self {
        Self {
            data: Bytes::new(),
            nulls: None,
            storage_dictionary: None,
        }
    }

    pub fn varlen_row(&self, row_idx: usize) -> Result<Option<Bytes>> {
        if let Some(nulls) = self.nulls.as_ref() {
            let is_null = *nulls.get(row_idx).ok_or_else(|| {
                paro_error::out_of_range(format!("null row {} out of range", row_idx))
            })? != 0;
            if is_null {
                return Ok(None);
            }
        }

        if let Some(storage_dictionary) = &self.storage_dictionary {
            let code_offset = row_idx
                .checked_mul(std::mem::size_of::<u32>())
                .ok_or_else(|| paro_error::data_corrupted("storage dictionary row overflow"))?;
            let code_end = code_offset
                .checked_add(std::mem::size_of::<u32>())
                .ok_or_else(|| paro_error::data_corrupted("storage dictionary code overflow"))?;
            if code_end > storage_dictionary.codes.len() {
                return Err(paro_error::out_of_range(format!(
                    "storage dictionary row {} out of range",
                    row_idx
                )));
            }

            let code = u32::from_le_bytes(
                storage_dictionary.codes[code_offset..code_end]
                    .try_into()
                    .expect("u32 code slice"),
            );
            let mut decoder = BinaryPlainPageDecoder::new(storage_dictionary.dictionary.clone());
            decoder.init()?;
            return decoder.string_at(code).map(Some).ok_or_else(|| {
                paro_error::data_corrupted(format!("storage dictionary code {} out of range", code))
            });
        }

        let mut offset = 0usize;
        let mut current_row = 0usize;
        while offset < self.data.len() {
            let len_end = offset
                .checked_add(std::mem::size_of::<u32>())
                .ok_or_else(|| paro_error::data_corrupted("varlen row length overflow"))?;
            if len_end > self.data.len() {
                return Err(paro_error::data_corrupted(
                    "varlen row length prefix truncated",
                ));
            }

            let len = u32::from_le_bytes(
                self.data[offset..len_end]
                    .try_into()
                    .expect("u32 length prefix"),
            ) as usize;
            offset = len_end;

            let value_end = offset
                .checked_add(len)
                .ok_or_else(|| paro_error::data_corrupted("varlen row value overflow"))?;
            if value_end > self.data.len() {
                return Err(paro_error::data_corrupted("varlen row extends past batch"));
            }

            if current_row == row_idx {
                return Ok(Some(self.data.slice(offset..value_end)));
            }

            offset = value_end;
            current_row += 1;
        }

        Err(paro_error::out_of_range(format!(
            "varlen row {} out of range",
            row_idx
        )))
    }
}

/// Trait for column iterators.
pub trait ColumnIterator: Send + Sync {
    /// Seek to a specific row ordinal.
    fn seek_to_ordinal(&mut self, ordinal: u64) -> Result<()>;

    /// Read the next batch of values.
    ///
    /// # Arguments
    /// * `n` - Maximum number of values to read
    ///
    /// # Returns
    /// Tuple of (values_read, batch)
    fn next_batch(&mut self, n: usize) -> Result<(usize, ColumnBatch)>;

    /// Read values by row IDs.
    ///
    /// # Arguments
    /// * `rowids` - Array of row ordinals to read
    ///
    /// # Returns
    /// Data for the requested rows
    fn read_by_rowids(&mut self, rowids: &[u64]) -> Result<ColumnBatch>;

    /// Get the current row ordinal.
    fn current_ordinal(&self) -> u64;

    /// Get the total number of rows.
    fn num_rows(&self) -> u64;

    /// Check if there are more rows to read.
    fn has_remaining(&self) -> bool {
        self.current_ordinal() < self.num_rows()
    }
}

/// Page decoder wrapper for different encoding types.
enum PageDecoderImpl {
    Plain(PlainPageDecoder),
    BitShuffle(BitShufflePageDecoder),
    Rle(RlePageDecoder<u8>),
    BinaryPlain(BinaryPlainPageDecoder),
    BinaryDict(BinaryDictPageDecoder),
}

impl PageDecoderImpl {
    fn init(&mut self) -> Result<()> {
        match self {
            PageDecoderImpl::Plain(d) => d.init(),
            PageDecoderImpl::BitShuffle(d) => d.init(),
            PageDecoderImpl::Rle(d) => d.init(),
            PageDecoderImpl::BinaryPlain(d) => d.init(),
            PageDecoderImpl::BinaryDict(d) => d.init(),
        }
    }

    fn seek_to_position(&mut self, pos: u32) -> Result<()> {
        match self {
            PageDecoderImpl::Plain(d) => d.seek_to_position(pos),
            PageDecoderImpl::BitShuffle(d) => d.seek_to_position(pos),
            PageDecoderImpl::Rle(d) => d.seek_to_position(pos),
            PageDecoderImpl::BinaryPlain(d) => d.seek_to_position(pos),
            PageDecoderImpl::BinaryDict(d) => d.seek_to_position(pos),
        }
    }

    fn next_batch(&mut self, n: usize) -> Result<(usize, Bytes)> {
        match self {
            PageDecoderImpl::Plain(d) => d.next_batch(n),
            PageDecoderImpl::BitShuffle(d) => d.next_batch(n),
            PageDecoderImpl::Rle(d) => {
                let values = d.next_batch(n)?;
                let bytes = Bytes::from(values);
                Ok((bytes.len(), bytes))
            }
            PageDecoderImpl::BinaryPlain(d) => {
                let strings = d.next_batch(n)?;
                // Concatenate strings with length prefixes
                let mut result = Vec::new();
                for s in &strings {
                    result.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    result.extend_from_slice(s);
                }
                Ok((strings.len(), Bytes::from(result)))
            }
            PageDecoderImpl::BinaryDict(d) => {
                let strings = d.next_batch(n)?;
                // Concatenate strings with length prefixes
                let mut result = Vec::new();
                for s in &strings {
                    result.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    result.extend_from_slice(s);
                }
                Ok((strings.len(), Bytes::from(result)))
            }
        }
    }

    fn count(&self) -> u32 {
        match self {
            PageDecoderImpl::Plain(d) => d.count(),
            PageDecoderImpl::BitShuffle(d) => d.count(),
            PageDecoderImpl::Rle(d) => d.count(),
            PageDecoderImpl::BinaryPlain(d) => d.count(),
            PageDecoderImpl::BinaryDict(d) => d.count(),
        }
    }

    fn current_index(&self) -> u32 {
        match self {
            PageDecoderImpl::Plain(d) => d.current_index(),
            PageDecoderImpl::BitShuffle(d) => d.current_index(),
            PageDecoderImpl::Rle(d) => d.current_index(),
            PageDecoderImpl::BinaryPlain(d) => d.current_index(),
            PageDecoderImpl::BinaryDict(d) => d.current_index(),
        }
    }
}

/// Scalar column iterator for non-nested types.
pub struct ScalarColumnIterator<R: Read + Seek> {
    /// Column metadata
    meta: ColumnReaderMeta,
    /// File reader
    reader: R,
    /// Reader options
    opts: ColumnReaderOptions,
    /// Page reader with cache integration
    page_reader: PageReader,
    /// Optional prefetcher
    prefetcher: Option<Arc<Prefetcher>>,
    /// File path for prefetch tasks
    file_path: Option<PathBuf>,
    /// Ordinal index
    ordinal_index: OrdinalIndexReader,
    /// ZoneMap index (optional)
    zonemap_index: Option<ZoneMapIndexReader>,
    /// Dictionary data (for dict encoding)
    dict_data: Option<Bytes>,
    /// Current page index
    current_page_idx: Option<usize>,
    /// Current page decoder
    current_decoder: Option<PageDecoderImpl>,
    /// Current page null decoder (optional)
    current_null_decoder: Option<PageDecoderImpl>,
    /// Current page first ordinal
    current_page_first_ordinal: u64,
    /// Current row ordinal
    current_ordinal: u64,
}

impl<R: Read + Seek> ScalarColumnIterator<R> {
    /// Create a new scalar column iterator.
    pub fn new(
        meta: ColumnReaderMeta,
        reader: R,
        opts: ColumnReaderOptions,
        page_reader: PageReader,
        prefetcher: Option<Arc<Prefetcher>>,
        file_path: Option<PathBuf>,
        ordinal_index: OrdinalIndexReader,
        zonemap_index: Option<ZoneMapIndexReader>,
        dict_data: Option<Bytes>,
    ) -> Result<Self> {
        Ok(ScalarColumnIterator {
            meta,
            reader,
            opts,
            page_reader,
            prefetcher,
            file_path,
            ordinal_index,
            zonemap_index,
            dict_data,
            current_page_idx: None,
            current_decoder: None,
            current_null_decoder: None,
            current_page_first_ordinal: 0,
            current_ordinal: 0,
        })
    }

    /// Load a page by index.
    fn load_page(&mut self, page_idx: usize) -> Result<()> {
        let entry = self.ordinal_index.get_page(page_idx).ok_or_else(|| {
            paro_error::out_of_range(format!("page index {} out of range", page_idx))
        })?;

        // Copy values we need before mutable borrow
        let page_pointer = entry.page_pointer;
        let first_ordinal = entry.first_ordinal;

        let page_opts = PageReadOptions::new(page_pointer)
            .with_verify_checksum(self.opts.verify_checksum)
            .with_codec(self.opts.compression);

        let page_key = self.page_reader.page_key(page_pointer);
        if let Some(prefetcher) = &self.prefetcher {
            prefetcher.record_consume(&page_key);
        }

        let (body, footer, _) = self.page_reader.read_page(&mut self.reader, &page_opts)?;

        // Verify it's a data page
        let data_footer = match &footer {
            PageFooter::Data(df) => df,
            _ => {
                return Err(paro_error::data_corrupted(
                    "Expected data page, got different type",
                ))
            }
        };

        let (data_body, null_body) = Self::split_page_body(body, data_footer.nullmap_size)?;

        // Create decoder based on encoding type
        let decoder = self.create_decoder(data_body)?;

        let null_decoder = if self.meta.is_nullable {
            if let Some(null_body) = null_body {
                Some(self.create_null_decoder(null_body, data_footer.null_encoding)?)
            } else {
                None
            }
        } else {
            None
        };

        self.current_page_idx = Some(page_idx);
        self.current_decoder = Some(decoder);
        self.current_null_decoder = null_decoder;
        self.current_page_first_ordinal = first_ordinal;

        if let (Some(prefetcher), Some(file_path)) = (&self.prefetcher, &self.file_path) {
            let window = prefetcher.options().window_pages;
            if window > 0 {
                let mut items = Vec::new();
                let start_idx = page_idx + 1;
                let end_idx = start_idx + window;
                for idx in start_idx..end_idx {
                    if let Some(entry) = self.ordinal_index.get_page(idx) {
                        let pointer = entry.page_pointer;
                        let key = self.page_reader.page_key(pointer);
                        items.push(PrefetchItem {
                            key,
                            offset: pointer.offset,
                            size: pointer.size,
                        });
                    } else {
                        break;
                    }
                }
                if !items.is_empty() {
                    prefetcher.prefetch_window(file_path, items);
                }
            }
        }

        Ok(())
    }

    /// Create a decoder for the given page data.
    fn create_decoder(&mut self, data: Bytes) -> Result<PageDecoderImpl> {
        let mut decoder = match self.meta.encoding {
            EncodingType::Plain => {
                if self.meta.field_type == crate::rowset::encoding::FieldType::Vector {
                    let type_size = self.meta.type_size.ok_or_else(|| {
                        paro_error::internal("Vector plain encoding requires type size")
                    })?;
                    PageDecoderImpl::Plain(PlainPageDecoder::new(data, type_size))
                } else if self.meta.field_type.is_variable_length() {
                    PageDecoderImpl::BinaryPlain(BinaryPlainPageDecoder::new(data))
                } else {
                    let type_size = self
                        .meta
                        .type_size
                        .ok_or_else(|| paro_error::internal("Plain encoding requires type size"))?;
                    PageDecoderImpl::Plain(PlainPageDecoder::new(data, type_size))
                }
            }
            EncodingType::BitShuffle => {
                PageDecoderImpl::BitShuffle(BitShufflePageDecoder::new(data))
            }
            EncodingType::Rle => PageDecoderImpl::Rle(RlePageDecoder::new(data, 1)),
            EncodingType::Dict => {
                let mut decoder = BinaryDictPageDecoder::new(data);
                if let Some(ref dict_data) = self.dict_data {
                    decoder.set_dict_decoder(dict_data.clone())?;
                }
                PageDecoderImpl::BinaryDict(decoder)
            }
            _ => {
                return Err(paro_error::not_supported(format!(
                    "Encoding {:?} not supported for reading",
                    self.meta.encoding
                )))
            }
        };

        decoder.init()?;
        Ok(decoder)
    }

    /// Split data page body into data and null map (if present).
    fn split_page_body(body: Bytes, nullmap_size: u32) -> Result<(Bytes, Option<Bytes>)> {
        if nullmap_size == 0 {
            return Ok((body, None));
        }
        let null_bytes = nullmap_size as usize;
        if null_bytes > body.len() {
            return Err(paro_error::data_corrupted(
                "Null map size exceeds page body length",
            ));
        }
        let data_len = body.len() - null_bytes;
        let data_body = body.slice(0..data_len);
        let null_body = body.slice(data_len..);
        Ok((data_body, Some(null_body)))
    }

    /// Create a null decoder for the given null map bytes.
    fn create_null_decoder(&self, data: Bytes, encoding: NullEncoding) -> Result<PageDecoderImpl> {
        let mut decoder = match encoding {
            NullEncoding::BitShuffle => {
                PageDecoderImpl::BitShuffle(BitShufflePageDecoder::new(data))
            }
            NullEncoding::Rle => PageDecoderImpl::Rle(RlePageDecoder::new(data, 1)),
            NullEncoding::Lz4 => {
                return Err(paro_error::not_supported("Null LZ4 encoding not supported"))
            }
        };
        decoder.init()?;
        Ok(decoder)
    }

    /// Get the number of rows in the current page.
    fn current_page_num_rows(&self) -> u64 {
        if let Some(page_idx) = self.current_page_idx {
            // Get next page's first ordinal or total rows
            if let Some(next_entry) = self.ordinal_index.get_page(page_idx + 1) {
                next_entry.first_ordinal - self.current_page_first_ordinal
            } else {
                self.meta.num_rows - self.current_page_first_ordinal
            }
        } else {
            0
        }
    }

    /// Check if we need to load the next page.
    fn need_next_page(&self) -> bool {
        if self.current_decoder.is_none() {
            return true;
        }

        let page_end = self.current_page_first_ordinal + self.current_page_num_rows();
        self.current_ordinal >= page_end
    }

    /// Load the next page if needed.
    fn ensure_page_loaded(&mut self) -> Result<bool> {
        if !self.need_next_page() {
            return Ok(true);
        }

        // Find the page containing current_ordinal
        if let Some(page_idx) = self.ordinal_index.seek_at_or_before(self.current_ordinal) {
            self.load_page(page_idx)?;

            // Seek within the page
            let offset_in_page = (self.current_ordinal - self.current_page_first_ordinal) as u32;
            if let Some(ref mut decoder) = self.current_decoder {
                decoder.seek_to_position(offset_in_page)?;
            }
            if let Some(ref mut null_decoder) = self.current_null_decoder {
                null_decoder.seek_to_position(offset_in_page)?;
            }

            Ok(true)
        } else {
            Ok(false) // No more pages
        }
    }

    fn seek_internal(&mut self, ordinal: u64) -> Result<()> {
        if ordinal > self.meta.num_rows {
            return Err(paro_error::out_of_range(format!(
                "ordinal {} > num_rows {}",
                ordinal, self.meta.num_rows
            )));
        }

        self.current_ordinal = ordinal;

        if self.current_page_idx.is_some() {
            let page_end = self.current_page_first_ordinal + self.current_page_num_rows();
            if ordinal >= self.current_page_first_ordinal && ordinal < page_end {
                let offset_in_page = (ordinal - self.current_page_first_ordinal) as u32;
                if let Some(ref mut decoder) = self.current_decoder {
                    decoder.seek_to_position(offset_in_page)?;
                }
                if let Some(ref mut null_decoder) = self.current_null_decoder {
                    null_decoder.seek_to_position(offset_in_page)?;
                }
                return Ok(());
            }
        }

        self.current_page_idx = None;
        self.current_decoder = None;
        self.current_null_decoder = None;
        Ok(())
    }

    fn try_next_storage_dictionary_batch(
        &mut self,
        n: usize,
    ) -> Result<Option<(usize, ColumnBatch)>> {
        if self.current_ordinal >= self.meta.num_rows || n == 0 {
            return Ok(None);
        }
        if !self.ensure_page_loaded()? {
            return Ok(None);
        }

        let Some(dictionary) = self.dict_data.clone() else {
            return Ok(None);
        };

        let (count, codes) = match self.current_decoder.as_mut() {
            Some(PageDecoderImpl::BinaryDict(decoder)) if decoder.is_dict_encoded() => {
                let page_remaining =
                    decoder.count().saturating_sub(decoder.current_index()) as usize;
                if page_remaining == 0 || n > page_remaining {
                    return Ok(None);
                }
                decoder.next_dict_codes(n)?
            }
            _ => return Ok(None),
        };

        if count == 0 {
            return Ok(None);
        }

        let nulls = if let Some(ref mut null_decoder) = self.current_null_decoder {
            let (null_count, null_bytes) = null_decoder.next_batch(count)?;
            if null_count != count {
                return Err(paro_error::data_corrupted(
                    "Null map count mismatch with dictionary page",
                ));
            }
            Some(null_bytes)
        } else if self.meta.is_nullable {
            Some(Bytes::from(vec![0u8; count]))
        } else {
            None
        };

        self.current_ordinal += count as u64;
        Ok(Some((
            count,
            ColumnBatch::with_storage_dictionary(dictionary, codes, nulls),
        )))
    }

    fn try_read_varlen_storage_dictionary_by_rowids(
        &mut self,
        sorted_rowids: &[(usize, u64)],
        total_rows: usize,
    ) -> Result<Option<ColumnBatch>> {
        let Some(dictionary) = self.dict_data.clone() else {
            return Ok(None);
        };

        let mut codes = vec![0u32; total_rows];
        let mut nulls_out = if self.meta.is_nullable {
            Some(vec![0u8; total_rows])
        } else {
            None
        };

        for &(orig_idx, rowid) in sorted_rowids {
            self.seek_internal(rowid)?;
            if !self.ensure_page_loaded()? {
                return Err(paro_error::out_of_range(format!(
                    "rowid {} not found",
                    rowid
                )));
            }

            let (count, code_bytes) = match self.current_decoder.as_mut() {
                Some(PageDecoderImpl::BinaryDict(decoder)) if decoder.is_dict_encoded() => {
                    decoder.next_dict_codes(1)?
                }
                _ => return Ok(None),
            };

            if count == 0 || code_bytes.len() < std::mem::size_of::<u32>() {
                return Err(paro_error::out_of_range(format!(
                    "rowid {} not found",
                    rowid
                )));
            }

            codes[orig_idx] =
                u32::from_le_bytes(code_bytes[..4].try_into().expect("u32 code slice"));

            if let Some(ref mut nulls) = nulls_out {
                let is_null = if let Some(ref mut null_decoder) = self.current_null_decoder {
                    let (null_count, null_bytes) = null_decoder.next_batch(1)?;
                    if null_count != 1 {
                        return Err(paro_error::data_corrupted(
                            "Null map count mismatch with dictionary row lookup",
                        ));
                    }
                    null_bytes.first().copied().unwrap_or(0)
                } else {
                    0
                };
                nulls[orig_idx] = is_null;
            }

            self.current_ordinal += 1;
        }

        let mut encoded_codes = Vec::with_capacity(total_rows * std::mem::size_of::<u32>());
        for code in codes {
            encoded_codes.extend_from_slice(&code.to_le_bytes());
        }

        Ok(Some(ColumnBatch::with_storage_dictionary(
            dictionary,
            Bytes::from(encoded_codes),
            nulls_out.map(Bytes::from),
        )))
    }
}

impl<R: Read + Seek + Send + Sync> ColumnIterator for ScalarColumnIterator<R> {
    fn seek_to_ordinal(&mut self, ordinal: u64) -> Result<()> {
        self.seek_internal(ordinal)
    }

    fn next_batch(&mut self, n: usize) -> Result<(usize, ColumnBatch)> {
        if self.current_ordinal >= self.meta.num_rows {
            return Ok((0, ColumnBatch::empty()));
        }

        if let Some(storage_batch) = self.try_next_storage_dictionary_batch(n)? {
            return Ok(storage_batch);
        }

        // Ensure we have a page loaded
        if !self.ensure_page_loaded()? {
            return Ok((0, ColumnBatch::empty()));
        }

        let mut total_read = 0;
        let mut result_data = Vec::new();
        let mut result_nulls: Option<Vec<u8>> = if self.meta.is_nullable {
            Some(Vec::new())
        } else {
            None
        };

        while total_read < n && self.current_ordinal < self.meta.num_rows {
            // Ensure page is loaded
            if !self.ensure_page_loaded()? {
                break;
            }

            let decoder = self.current_decoder.as_mut().unwrap();

            // Calculate how many to read from this page
            let page_remaining = decoder.count() - decoder.current_index();
            let to_read = std::cmp::min(n - total_read, page_remaining as usize);

            if to_read == 0 {
                // Move to next page
                self.current_page_idx = None;
                self.current_decoder = None;
                self.current_null_decoder = None;
                continue;
            }

            let (count, data) = decoder.next_batch(to_read)?;
            if count == 0 {
                break;
            }

            if let Some(ref mut nulls_out) = result_nulls {
                if let Some(ref mut null_decoder) = self.current_null_decoder {
                    let (null_count, null_bytes) = null_decoder.next_batch(to_read)?;
                    if null_count != count {
                        return Err(paro_error::data_corrupted(
                            "Null map count mismatch with data page",
                        ));
                    }
                    nulls_out.extend_from_slice(&null_bytes);
                } else {
                    nulls_out.resize(nulls_out.len() + count, 0);
                }
            }

            result_data.extend_from_slice(&data);
            total_read += count;
            self.current_ordinal += count as u64;
        }

        Ok((
            total_read,
            ColumnBatch::new(Bytes::from(result_data), result_nulls.map(Bytes::from)),
        ))
    }

    fn read_by_rowids(&mut self, rowids: &[u64]) -> Result<ColumnBatch> {
        if rowids.is_empty() {
            return Ok(ColumnBatch::empty());
        }

        if let Some(type_size) = self.meta.type_size {
            let mut result = Vec::with_capacity(rowids.len() * type_size);
            let mut result_nulls: Option<Vec<u8>> = if self.meta.is_nullable {
                Some(vec![0u8; rowids.len()])
            } else {
                None
            };

            // Sort rowids for sequential access
            let mut sorted_rowids: Vec<(usize, u64)> = rowids.iter().copied().enumerate().collect();
            sorted_rowids.sort_by_key(|&(_, rowid)| rowid);

            // Read each rowid
            for &(orig_idx, rowid) in &sorted_rowids {
                self.seek_to_ordinal(rowid)?;
                let (count, batch) = self.next_batch(1)?;
                if count == 0 {
                    return Err(paro_error::out_of_range(format!(
                        "rowid {} not found",
                        rowid
                    )));
                }
                result.extend_from_slice(&batch.data);
                if let Some(ref mut nulls_out) = result_nulls {
                    let is_null = batch
                        .nulls
                        .as_ref()
                        .and_then(|b| b.first())
                        .copied()
                        .unwrap_or(0);
                    nulls_out[orig_idx] = is_null;
                }
            }

            // Reorder results to match original rowid order
            let mut final_result = vec![0u8; rowids.len() * type_size];
            for (i, &(orig_idx, _)) in sorted_rowids.iter().enumerate() {
                let src_start = i * type_size;
                let dst_start = orig_idx * type_size;
                final_result[dst_start..dst_start + type_size]
                    .copy_from_slice(&result[src_start..src_start + type_size]);
            }

            Ok(ColumnBatch::new(
                Bytes::from(final_result),
                result_nulls.map(Bytes::from),
            ))
        } else {
            // Variable-length types: read per row and keep length-prefixed bytes.
            let mut sorted_rowids: Vec<(usize, u64)> = rowids.iter().copied().enumerate().collect();
            sorted_rowids.sort_by_key(|&(_, rowid)| rowid);

            if let Some(batch) =
                self.try_read_varlen_storage_dictionary_by_rowids(&sorted_rowids, rowids.len())?
            {
                return Ok(batch);
            }

            let mut values: Vec<Vec<u8>> = vec![Vec::new(); rowids.len()];
            let mut nulls_out: Option<Vec<u8>> = if self.meta.is_nullable {
                Some(vec![0u8; rowids.len()])
            } else {
                None
            };
            for &(orig_idx, rowid) in &sorted_rowids {
                self.seek_to_ordinal(rowid)?;
                let (count, batch) = self.next_batch(1)?;
                if count == 0 {
                    return Err(paro_error::out_of_range(format!(
                        "rowid {} not found",
                        rowid
                    )));
                }
                if let Some(ref mut nulls) = nulls_out {
                    let is_null = batch
                        .nulls
                        .as_ref()
                        .and_then(|b| b.first())
                        .copied()
                        .unwrap_or(0);
                    nulls[orig_idx] = is_null;
                }
                values[orig_idx] = batch.data.to_vec();
            }

            let total_len: usize = values.iter().map(|v| v.len()).sum();
            let mut result = Vec::with_capacity(total_len);
            for v in values {
                result.extend_from_slice(&v);
            }
            Ok(ColumnBatch::new(
                Bytes::from(result),
                nulls_out.map(Bytes::from),
            ))
        }
    }

    fn current_ordinal(&self) -> u64 {
        self.current_ordinal
    }

    fn num_rows(&self) -> u64 {
        self.meta.num_rows
    }
}

/// Iterator with ZoneMap filtering support.
pub struct FilteredColumnIterator<R: Read + Seek> {
    /// Inner iterator
    inner: ScalarColumnIterator<R>,
    /// Pages to skip (based on ZoneMap filtering)
    skip_pages: Vec<bool>,
}

impl<R: Read + Seek> FilteredColumnIterator<R> {
    /// Create a new filtered iterator.
    pub fn new(inner: ScalarColumnIterator<R>) -> Self {
        let num_pages = inner.ordinal_index.num_pages();
        FilteredColumnIterator {
            inner,
            skip_pages: vec![false; num_pages],
        }
    }

    /// Apply a range filter using ZoneMap.
    ///
    /// Pages that don't overlap with [min, max] will be skipped.
    pub fn apply_range_filter<F>(&mut self, min: &[u8], max: &[u8], cmp: F)
    where
        F: Fn(&[u8], &[u8]) -> std::cmp::Ordering,
    {
        if let Some(ref zonemap) = self.inner.zonemap_index {
            for i in 0..self.skip_pages.len() {
                if !zonemap.page_may_contain_range(i, min, max, &cmp) {
                    self.skip_pages[i] = true;
                }
            }
        }
    }

    /// Apply an equality filter using ZoneMap.
    pub fn apply_eq_filter<F>(&mut self, value: &[u8], cmp: F)
    where
        F: Fn(&[u8], &[u8]) -> std::cmp::Ordering,
    {
        self.apply_range_filter(value, value, cmp);
    }

    /// Check if a page should be skipped.
    pub fn should_skip_page(&self, page_idx: usize) -> bool {
        self.skip_pages.get(page_idx).copied().unwrap_or(false)
    }

    /// Get the number of pages that will be read (not skipped).
    pub fn num_pages_to_read(&self) -> usize {
        self.skip_pages.iter().filter(|&&skip| !skip).count()
    }
}

impl<R: Read + Seek + Send + Sync> ColumnIterator for FilteredColumnIterator<R> {
    fn seek_to_ordinal(&mut self, ordinal: u64) -> Result<()> {
        self.inner.seek_to_ordinal(ordinal)
    }

    fn next_batch(&mut self, n: usize) -> Result<(usize, ColumnBatch)> {
        // Skip filtered pages
        while let Some(page_idx) = self.inner.current_page_idx {
            if self.should_skip_page(page_idx) {
                // Skip to next page
                if let Some(next_entry) = self.inner.ordinal_index.get_page(page_idx + 1) {
                    self.inner.current_ordinal = next_entry.first_ordinal;
                    self.inner.current_page_idx = None;
                    self.inner.current_decoder = None;
                    self.inner.current_null_decoder = None;
                } else {
                    // No more pages
                    self.inner.current_ordinal = self.inner.meta.num_rows;
                    break;
                }
            } else {
                break;
            }
        }

        self.inner.next_batch(n)
    }

    fn read_by_rowids(&mut self, rowids: &[u64]) -> Result<ColumnBatch> {
        self.inner.read_by_rowids(rowids)
    }

    fn current_ordinal(&self) -> u64 {
        self.inner.current_ordinal()
    }

    fn num_rows(&self) -> u64 {
        self.inner.num_rows()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::vector_decoder;
    use crate::rowset::column::column_reader::OrdinalIndexEntry;
    use crate::rowset::column::column_writer::{
        ColumnWriter, ColumnWriterOptions, ScalarColumnWriter,
    };
    use crate::rowset::column::{ColumnReader, ColumnReaderOptions};
    use crate::rowset::encoding::FieldType;
    use crate::rowset::page::CompressionType;
    use crate::rowset::page::EncodingType;
    use crate::rowset::page_reader::{PageReaderContext, PageReaderOptions};
    use paro_common::allocator::default_allocator;
    use paro_common::types::LogicalType;
    use paro_common::vector::DictionarySource;
    use std::io::Cursor;
    use std::sync::Arc;

    fn create_page_reader() -> PageReader {
        PageReader::new(
            PageReaderContext::new(0, 0, 0, 0),
            None,
            PageReaderOptions::default(),
        )
    }

    fn create_test_column() -> (Cursor<Vec<u8>>, ColumnReaderMeta, OrdinalIndexReader) {
        let opts = ColumnWriterOptions::new(FieldType::Int, 0)
            .with_nullable(false)
            .with_compression(CompressionType::None)
            .with_page_size(1024); // Small pages for testing

        let buffer = Cursor::new(Vec::new());
        let mut writer = ScalarColumnWriter::new(opts, buffer).unwrap();

        // Write 100 i32 values
        let values: Vec<i32> = (0..100).collect();
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        writer.append(&bytes, None, 100).unwrap();

        let meta = writer.finish().unwrap();

        // Get the buffer
        let buffer = Cursor::new(writer.into_inner().into_inner());

        // Create reader meta
        let reader_meta = ColumnReaderMeta::from_writer_meta(&meta, FieldType::Int);

        // Create ordinal index (simplified for test)
        let ordinal_index = OrdinalIndexReader::new(
            vec![OrdinalIndexEntry {
                first_ordinal: 0,
                page_pointer: meta.data_page_pointer,
            }],
            100,
        );

        (buffer, reader_meta, ordinal_index)
    }

    fn create_dict_varchar_iterator(nullable: bool) -> Box<dyn ColumnIterator + Send + Sync> {
        let opts = ColumnWriterOptions::new(FieldType::Varchar, 0)
            .with_nullable(nullable)
            .with_encoding(EncodingType::Dict)
            .with_compression(CompressionType::None)
            .with_page_size(1024);
        let buffer = Cursor::new(Vec::new());
        let mut writer = ScalarColumnWriter::new(opts, buffer).unwrap();

        let strings = ["apple", "banana", "apple", "cherry"];
        let mut data = Vec::new();
        for value in &strings {
            data.extend_from_slice(&(value.len() as u32).to_le_bytes());
            data.extend_from_slice(value.as_bytes());
        }
        let nulls = nullable.then(|| vec![0u8, 1, 0, 0]);
        writer
            .append(
                &data,
                nulls.as_deref(),
                strings.len().try_into().expect("string count fits in u32"),
            )
            .unwrap();

        let meta = writer.finish().unwrap();
        let buffer = Cursor::new(writer.into_inner().into_inner());
        let reader_meta =
            ColumnReaderMeta::from_writer_meta(&meta, FieldType::Varchar).with_nullable(nullable);
        let mut reader = ColumnReader::create(
            reader_meta,
            buffer,
            ColumnReaderOptions::default(),
            create_page_reader(),
            None,
            None,
        )
        .unwrap();
        reader.new_iterator().unwrap()
    }

    fn encode_varlen_strings(strings: &[&str]) -> Vec<u8> {
        let mut data = Vec::new();
        for value in strings {
            data.extend_from_slice(&(value.len() as u32).to_le_bytes());
            data.extend_from_slice(value.as_bytes());
        }
        data
    }

    #[test]
    fn test_scalar_iterator_basic() {
        let (buffer, meta, ordinal_index) = create_test_column();

        let mut iter = ScalarColumnIterator::new(
            meta,
            buffer,
            ColumnReaderOptions::default(),
            create_page_reader(),
            None,
            None,
            ordinal_index,
            None,
            None,
        )
        .unwrap();

        // Read all values
        let (count, batch) = iter.next_batch(100).unwrap();
        assert_eq!(count, 100);
        assert_eq!(batch.data.len(), 400); // 100 * 4 bytes

        // Verify values
        for i in 0..100 {
            let offset = i * 4;
            let value = i32::from_le_bytes([
                batch.data[offset],
                batch.data[offset + 1],
                batch.data[offset + 2],
                batch.data[offset + 3],
            ]);
            assert_eq!(value, i as i32);
        }
    }

    #[test]
    fn test_scalar_iterator_seek() {
        let (buffer, meta, ordinal_index) = create_test_column();

        let mut iter = ScalarColumnIterator::new(
            meta,
            buffer,
            ColumnReaderOptions::default(),
            create_page_reader(),
            None,
            None,
            ordinal_index,
            None,
            None,
        )
        .unwrap();

        // Seek to position 50
        iter.seek_to_ordinal(50).unwrap();
        assert_eq!(iter.current_ordinal(), 50);

        // Read 10 values
        let (count, batch) = iter.next_batch(10).unwrap();
        assert_eq!(count, 10);

        // First value should be 50
        let first =
            i32::from_le_bytes([batch.data[0], batch.data[1], batch.data[2], batch.data[3]]);
        assert_eq!(first, 50);
    }

    #[test]
    fn test_scalar_iterator_read_by_rowids() {
        let (buffer, meta, ordinal_index) = create_test_column();

        let mut iter = ScalarColumnIterator::new(
            meta,
            buffer,
            ColumnReaderOptions::default(),
            create_page_reader(),
            None,
            None,
            ordinal_index,
            None,
            None,
        )
        .unwrap();

        // Read specific rows: 10, 50, 90
        let rowids = vec![10u64, 50, 90];
        let batch = iter.read_by_rowids(&rowids).unwrap();

        assert_eq!(batch.data.len(), 12); // 3 * 4 bytes

        // Verify values
        let v0 = i32::from_le_bytes([batch.data[0], batch.data[1], batch.data[2], batch.data[3]]);
        let v1 = i32::from_le_bytes([batch.data[4], batch.data[5], batch.data[6], batch.data[7]]);
        let v2 = i32::from_le_bytes([batch.data[8], batch.data[9], batch.data[10], batch.data[11]]);

        assert_eq!(v0, 10);
        assert_eq!(v1, 50);
        assert_eq!(v2, 90);
    }

    #[test]
    fn test_scalar_iterator_partial_reads() {
        let (buffer, meta, ordinal_index) = create_test_column();

        let mut iter = ScalarColumnIterator::new(
            meta,
            buffer,
            ColumnReaderOptions::default(),
            create_page_reader(),
            None,
            None,
            ordinal_index,
            None,
            None,
        )
        .unwrap();

        // Read in batches of 30
        let (count1, _batch1) = iter.next_batch(30).unwrap();
        assert_eq!(count1, 30);
        assert_eq!(iter.current_ordinal(), 30);

        let (count2, _batch2) = iter.next_batch(30).unwrap();
        assert_eq!(count2, 30);
        assert_eq!(iter.current_ordinal(), 60);

        let (count3, _batch3) = iter.next_batch(30).unwrap();
        assert_eq!(count3, 30);
        assert_eq!(iter.current_ordinal(), 90);

        // Only 10 remaining
        let (count4, _batch4) = iter.next_batch(30).unwrap();
        assert_eq!(count4, 10);
        assert_eq!(iter.current_ordinal(), 100);

        // No more data
        let (count5, _) = iter.next_batch(30).unwrap();
        assert_eq!(count5, 0);
    }

    #[test]
    fn test_varchar_iterator_multiple_pages_preserves_input_offset() {
        let opts = ColumnWriterOptions::new(FieldType::Varchar, 0)
            .with_nullable(false)
            .with_encoding(EncodingType::Plain)
            .with_compression(CompressionType::None)
            .with_page_size(32);
        let buffer = Cursor::new(Vec::new());
        let mut writer = ScalarColumnWriter::new(opts, buffer).unwrap();

        let strings = [
            "page00-aa",
            "page00-bb",
            "page00-cc",
            "page01-dd",
            "page01-ee",
            "page01-ff",
            "page02-gg",
        ];
        let data = encode_varlen_strings(&strings);
        writer.append(&data, None, strings.len() as u32).unwrap();

        let meta = writer.finish().unwrap();
        let buffer = Cursor::new(writer.into_inner().into_inner());
        let reader_meta = ColumnReaderMeta::from_writer_meta(&meta, FieldType::Varchar);
        let mut reader = ColumnReader::create(
            reader_meta,
            buffer,
            ColumnReaderOptions::default(),
            create_page_reader(),
            None,
            None,
        )
        .unwrap();
        let mut iter = reader.new_iterator().unwrap();

        let (count, batch) = iter.next_batch(strings.len()).unwrap();
        assert_eq!(count, strings.len());
        for (idx, expected) in strings.iter().enumerate() {
            let value = batch.varlen_row(idx).unwrap().unwrap();
            assert_eq!(value.as_ref(), expected.as_bytes());
        }
    }

    #[test]
    fn test_varchar_iterator_emits_storage_dictionary_batch() {
        let mut iter = create_dict_varchar_iterator(false);

        let (count, batch) = iter.next_batch(4).unwrap();
        assert_eq!(count, 4);
        let storage_dictionary = batch
            .storage_dictionary
            .as_ref()
            .expect("dictionary batch should keep storage dictionary payload");
        assert!(!storage_dictionary.dictionary.is_empty());
        assert_eq!(
            storage_dictionary.codes.len(),
            4 * std::mem::size_of::<u32>()
        );

        let mut decoded_batch = batch.clone();
        decoded_batch.nulls = Some(Bytes::from(vec![0, 1, 0, 0]));
        let vector = vector_decoder::decode_column_batch(
            &LogicalType::Varchar,
            &decoded_batch,
            4,
            Arc::new(default_allocator()),
            Some(77),
        )
        .unwrap();

        assert_eq!(vector.get_string(0), Some("apple"));
        assert!(vector.is_null(1));
        assert_eq!(vector.get_string(2), Some("apple"));
        assert_eq!(vector.get_string(3), Some("cherry"));
        let info = vector
            .dictionary_info()
            .expect("decoded storage dictionary should keep provenance");
        assert_eq!(info.provenance_id, Some(77));
        assert_eq!(info.source, DictionarySource::Storage);
    }

    #[test]
    fn test_varchar_iterator_read_by_rowids_emits_storage_dictionary_batch() {
        let mut iter = create_dict_varchar_iterator(false);

        let batch = iter.read_by_rowids(&[2, 1, 3]).unwrap();
        let storage_dictionary = batch
            .storage_dictionary
            .as_ref()
            .expect("rowid lookup should keep storage dictionary payload");
        assert!(!storage_dictionary.dictionary.is_empty());

        let mut decoded_batch = batch.clone();
        decoded_batch.nulls = Some(Bytes::from(vec![0, 1, 0]));
        let vector = vector_decoder::decode_column_batch(
            &LogicalType::Varchar,
            &decoded_batch,
            3,
            Arc::new(default_allocator()),
            Some(88),
        )
        .unwrap();

        assert_eq!(vector.get_string(0), Some("apple"));
        assert!(vector.is_null(1));
        assert_eq!(vector.get_string(2), Some("cherry"));
        let info = vector
            .dictionary_info()
            .expect("decoded rowid storage dictionary should keep provenance");
        assert_eq!(info.provenance_id, Some(88));
        assert_eq!(info.source, DictionarySource::Storage);
    }
}
