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
use crate::metrics::storage_metrics;
use crate::rowset::encoding::{
    BinaryDictPageDecoder, BinaryPlainPageDecoder, BitShufflePageDecoder, PlainPageDecoder,
    RlePageDecoder,
};
use crate::rowset::page::{
    EncodingType, NullEncoding, PageFooter, PageReadOptions, CURRENT_DATA_PAGE_FORMAT_VERSION,
};
use crate::rowset::page_reader::{DecodedPageAccess, PageReader};
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
    /// Per-row null flags (1 byte per value, 1 = NULL). `None` is the
    /// canonical all-valid representation, including for nullable columns.
    pub nulls: Option<Bytes>,
    /// Optional storage dictionary payload for storage-aware dictionary execution.
    pub storage_dictionary: Option<StorageDictionaryBatch>,
    /// Page-local span seeks used by read_by_rowids() to assemble this batch.
    pub page_run_seeks: usize,
    /// Semantic guarantees established before this batch reached execution.
    integrity: ColumnBatchIntegrity,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum ColumnBatchIntegrity {
    #[default]
    Unverified,
    VerifiedUtf8,
}

#[derive(Debug, Clone)]
pub struct StorageDictionaryBatch {
    pub dictionary: Bytes,
    pub codes: Bytes,
}

#[derive(Debug, Clone, Copy)]
struct RowIdPageRun {
    run_start: usize,
    run_end: usize,
    span_start: u64,
    span_end: u64,
    span_len: usize,
}

trait RowIdSequence {
    fn len(&self) -> usize;
    fn pair(&self, index: usize) -> (usize, u64);
}

impl RowIdSequence for [(usize, u64)] {
    fn len(&self) -> usize {
        <[(usize, u64)]>::len(self)
    }

    #[inline]
    fn pair(&self, index: usize) -> (usize, u64) {
        self[index]
    }
}

/// Strictly increasing physical row IDs validated once for a multi-column
/// gather. The private representation prevents callers from bypassing the
/// ordering contract.
pub struct OrderedRowIds<'a>(&'a [u32]);

impl<'a> OrderedRowIds<'a> {
    pub fn try_new(rowids: &'a [u32]) -> Result<Self> {
        if rowids.windows(2).any(|window| window[0] >= window[1]) {
            return Err(paro_error::invalid_input(
                "ordered row IDs must be strictly increasing",
            ));
        }
        Ok(Self(rowids))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl RowIdSequence for OrderedRowIds<'_> {
    fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    fn pair(&self, index: usize) -> (usize, u64) {
        (index, u64::from(self.0[index]))
    }
}

impl ColumnBatch {
    pub fn new(data: Bytes, nulls: Option<Bytes>) -> Self {
        Self {
            data,
            nulls,
            storage_dictionary: None,
            page_run_seeks: 0,
            integrity: ColumnBatchIntegrity::Unverified,
        }
    }

    pub fn with_storage_dictionary(dictionary: Bytes, codes: Bytes, nulls: Option<Bytes>) -> Self {
        Self {
            data: codes.clone(),
            nulls,
            storage_dictionary: Some(StorageDictionaryBatch { dictionary, codes }),
            page_run_seeks: 0,
            integrity: ColumnBatchIntegrity::Unverified,
        }
    }

    pub fn with_page_run_seeks(mut self, page_run_seeks: usize) -> Self {
        self.page_run_seeks = page_run_seeks;
        self
    }

    pub(crate) fn with_verified_utf8(mut self) -> Self {
        self.integrity = ColumnBatchIntegrity::VerifiedUtf8;
        self
    }

    pub(crate) fn has_verified_utf8(&self) -> bool {
        self.integrity == ColumnBatchIntegrity::VerifiedUtf8
    }

    pub fn empty() -> Self {
        Self {
            data: Bytes::new(),
            nulls: None,
            storage_dictionary: None,
            page_run_seeks: 0,
            integrity: ColumnBatchIntegrity::Unverified,
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

    /// Read row IDs already ordered by ascending physical ordinal.
    ///
    /// Scan selection builders establish this order once for the whole
    /// projected batch, so column readers can avoid independently sorting the
    /// same row IDs for every projected column.
    fn read_by_ordered_rowids(&mut self, rowids: &OrderedRowIds<'_>) -> Result<ColumnBatch>;

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
    Dictionary {
        decoder: BinaryDictPageDecoder,
        value_width: Option<usize>,
    },
}

impl PageDecoderImpl {
    fn init(&mut self) -> Result<()> {
        match self {
            PageDecoderImpl::Plain(d) => d.init(),
            PageDecoderImpl::BitShuffle(d) => d.init(),
            PageDecoderImpl::Rle(d) => d.init(),
            PageDecoderImpl::BinaryPlain(d) => d.init(),
            PageDecoderImpl::Dictionary { decoder, .. } => decoder.init(),
        }
    }

    fn seek_to_position(&mut self, pos: u32) -> Result<()> {
        match self {
            PageDecoderImpl::Plain(d) => d.seek_to_position(pos),
            PageDecoderImpl::BitShuffle(d) => d.seek_to_position(pos),
            PageDecoderImpl::Rle(d) => d.seek_to_position(pos),
            PageDecoderImpl::BinaryPlain(d) => d.seek_to_position(pos),
            PageDecoderImpl::Dictionary { decoder, .. } => decoder.seek_to_position(pos),
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
            PageDecoderImpl::Dictionary {
                decoder,
                value_width,
            } => {
                let values = decoder.next_batch(n)?;
                let mut result = Vec::new();
                for value in &values {
                    if let Some(width) = value_width {
                        if value.len() != *width {
                            return Err(paro_error::data_corrupted(format!(
                                "Dictionary value width {} does not match column width {width}",
                                value.len(),
                            )));
                        }
                    } else {
                        result.extend_from_slice(&(value.len() as u32).to_le_bytes());
                    }
                    result.extend_from_slice(value);
                }
                Ok((values.len(), Bytes::from(result)))
            }
        }
    }

    fn count(&self) -> u32 {
        match self {
            PageDecoderImpl::Plain(d) => d.count(),
            PageDecoderImpl::BitShuffle(d) => d.count(),
            PageDecoderImpl::Rle(d) => d.count(),
            PageDecoderImpl::BinaryPlain(d) => d.count(),
            PageDecoderImpl::Dictionary { decoder, .. } => decoder.count(),
        }
    }

    fn current_index(&self) -> u32 {
        match self {
            PageDecoderImpl::Plain(d) => d.current_index(),
            PageDecoderImpl::BitShuffle(d) => d.current_index(),
            PageDecoderImpl::Rle(d) => d.current_index(),
            PageDecoderImpl::BinaryPlain(d) => d.current_index(),
            PageDecoderImpl::Dictionary { decoder, .. } => decoder.current_index(),
        }
    }

    fn decoded_cache_decoder_mut(&mut self) -> Option<&mut BitShufflePageDecoder> {
        match self {
            PageDecoderImpl::BitShuffle(decoder) => Some(decoder),
            PageDecoderImpl::Dictionary { decoder, .. } => decoder.code_decoder_mut(),
            _ => None,
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
    /// Physical pointer for decoded-cache publication.
    current_page_pointer: Option<crate::rowset::page::PagePointer>,
    /// Current row ordinal
    current_ordinal: u64,
    /// Whether the current page carries the checksummed v3 UTF-8 invariant.
    current_page_utf8_verified: bool,
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
            current_page_pointer: None,
            current_ordinal: 0,
            current_page_utf8_verified: false,
        })
    }

    #[inline]
    fn column_may_have_nulls(&self) -> bool {
        self.meta.is_nullable && self.meta.null_count != Some(0)
    }

    /// Load a page by index.
    fn load_page(&mut self, page_idx: usize) -> Result<()> {
        let entry = self.ordinal_index.get_page(page_idx).ok_or_else(|| {
            paro_error::out_of_range(format!("page index {} out of range", page_idx))
        })?;

        // Copy values we need before mutable borrow
        let page_pointer = entry.page_pointer;
        let first_ordinal = entry.first_ordinal;
        let page_end_ordinal = self.page_end_ordinal(page_idx);
        let page_num_rows = page_end_ordinal
            .checked_sub(first_ordinal)
            .ok_or_else(|| paro_error::data_corrupted("Page ordinal range is inverted"))?;
        let page_num_rows = u32::try_from(page_num_rows).map_err(|_| {
            paro_error::data_corrupted(format!(
                "Page row count {page_num_rows} exceeds the decoder format"
            ))
        })?;

        let page_opts = PageReadOptions::new(page_pointer)
            .with_verify_checksum(self.opts.verify_checksum)
            .with_codec(self.opts.compression);

        let page_key = self.page_reader.page_key(page_pointer);
        if let Some(prefetcher) = &self.prefetcher {
            prefetcher.record_consume(&page_key);
        }

        if self.meta.encoding == EncodingType::BitShuffle
            && (!self.meta.is_nullable || self.meta.null_count == Some(0))
        {
            if let Some(decoded) = self.page_reader.lookup_decoded(page_pointer) {
                let type_size = self.meta.type_size.ok_or_else(|| {
                    paro_error::internal("BitShuffle encoding requires type size")
                })?;
                let decoder = PageDecoderImpl::BitShuffle(
                    BitShufflePageDecoder::from_decoded_data(page_num_rows, type_size, decoded)?,
                );
                self.install_page(
                    page_idx,
                    first_ordinal,
                    page_pointer,
                    decoder,
                    None,
                    false,
                    false,
                );
                return Ok(());
            }
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
        let utf8_verified = self.opts.verify_checksum
            && self.meta.field_type.requires_valid_utf8()
            && data_footer.format_version == CURRENT_DATA_PAGE_FORMAT_VERSION;
        // Create decoder based on encoding type
        let decoder = self.create_decoder(data_body, page_num_rows, page_pointer)?;

        let null_decoder = if self.meta.is_nullable {
            if let Some(null_body) = null_body {
                Some(self.create_null_decoder(
                    null_body,
                    data_footer.null_encoding,
                    page_num_rows,
                )?)
            } else {
                None
            }
        } else {
            None
        };

        self.install_page(
            page_idx,
            first_ordinal,
            page_pointer,
            decoder,
            null_decoder,
            true,
            utf8_verified,
        );
        Ok(())
    }

    fn install_page(
        &mut self,
        page_idx: usize,
        first_ordinal: u64,
        page_pointer: crate::rowset::page::PagePointer,
        decoder: PageDecoderImpl,
        null_decoder: Option<PageDecoderImpl>,
        schedule_prefetch: bool,
        utf8_verified: bool,
    ) {
        self.current_page_idx = Some(page_idx);
        self.current_decoder = Some(decoder);
        self.current_null_decoder = null_decoder;
        self.current_page_first_ordinal = first_ordinal;
        self.current_page_pointer = Some(page_pointer);
        self.current_page_utf8_verified = utf8_verified;
        if schedule_prefetch {
            let (Some(prefetcher), Some(file_path)) = (&self.prefetcher, &self.file_path) else {
                return;
            };
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
    }

    /// Create a decoder for the given page data.
    fn create_decoder(
        &mut self,
        data: Bytes,
        expected_num_elements: u32,
        page_pointer: crate::rowset::page::PagePointer,
    ) -> Result<PageDecoderImpl> {
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
                let type_size = self.meta.type_size.ok_or_else(|| {
                    paro_error::internal("BitShuffle encoding requires type size")
                })?;
                let cached = self.page_reader.lookup_decoded(page_pointer);
                let decoder = if let Some(decoded) = cached {
                    BitShufflePageDecoder::with_decoded_data(
                        data,
                        expected_num_elements,
                        type_size,
                        decoded,
                    )
                } else {
                    BitShufflePageDecoder::new(data, expected_num_elements, type_size)
                };
                PageDecoderImpl::BitShuffle(decoder)
            }
            EncodingType::Rle => PageDecoderImpl::Rle(RlePageDecoder::new(data, 1)),
            EncodingType::Dict => {
                let cached = self.page_reader.lookup_decoded(page_pointer);
                let mut decoder = if let Some(decoded_codes) = cached {
                    BinaryDictPageDecoder::with_decoded_codes(
                        data,
                        expected_num_elements,
                        decoded_codes,
                    )
                } else {
                    BinaryDictPageDecoder::new(data, expected_num_elements)
                };
                if let Some(ref dict_data) = self.dict_data {
                    decoder.set_dict_decoder(dict_data.clone())?;
                }
                PageDecoderImpl::Dictionary {
                    decoder,
                    value_width: self.meta.type_size,
                }
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
    fn create_null_decoder(
        &self,
        data: Bytes,
        encoding: NullEncoding,
        expected_num_elements: u32,
    ) -> Result<PageDecoderImpl> {
        let mut decoder =
            match encoding {
                NullEncoding::BitShuffle => PageDecoderImpl::BitShuffle(
                    BitShufflePageDecoder::new(data, expected_num_elements, 1),
                ),
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
            self.page_end_ordinal(page_idx) - self.current_page_first_ordinal
        } else {
            0
        }
    }

    #[inline]
    fn attach_current_page_integrity(&self, batch: ColumnBatch) -> ColumnBatch {
        if self.current_page_utf8_verified {
            batch.with_verified_utf8()
        } else {
            batch
        }
    }

    fn page_end_ordinal(&self, page_idx: usize) -> u64 {
        if let Some(next_entry) = self.ordinal_index.get_page(page_idx + 1) {
            next_entry.first_ordinal
        } else {
            self.meta.num_rows
        }
    }

    fn page_index_for_rowid(&self, rowid: u64) -> Result<usize> {
        if rowid >= self.meta.num_rows {
            return Err(paro_error::out_of_range(format!(
                "rowid {} not found",
                rowid
            )));
        }

        let Some(page_idx) = self.ordinal_index.seek_at_or_before(rowid) else {
            return Err(paro_error::out_of_range(format!(
                "rowid {} not found",
                rowid
            )));
        };
        let Some(entry) = self.ordinal_index.get_page(page_idx) else {
            return Err(paro_error::out_of_range(format!(
                "page index {} out of range",
                page_idx
            )));
        };
        if rowid < entry.first_ordinal || rowid >= self.page_end_ordinal(page_idx) {
            return Err(paro_error::out_of_range(format!(
                "rowid {} not found",
                rowid
            )));
        }
        Ok(page_idx)
    }

    fn next_rowid_page_run<S: RowIdSequence + ?Sized>(
        &self,
        rowids: &S,
        idx: &mut usize,
    ) -> Result<RowIdPageRun> {
        let page_idx = self.page_index_for_rowid(rowids.pair(*idx).1)?;
        let page_end = self.page_end_ordinal(page_idx);
        let run_start = *idx;
        *idx += 1;
        while *idx < rowids.len() && rowids.pair(*idx).1 < page_end {
            *idx += 1;
        }

        let span_start = rowids.pair(run_start).1;
        let span_end = rowids.pair(*idx - 1).1;
        let span_len = usize::try_from(span_end - span_start + 1)
            .map_err(|_| paro_error::data_corrupted("rowid span overflow"))?;

        Ok(RowIdPageRun {
            run_start,
            run_end: *idx,
            span_start,
            span_end,
            span_len,
        })
    }

    fn plain_varlen_row_ranges(
        batch: &ColumnBatch,
        row_count: usize,
    ) -> Result<Vec<(usize, usize)>> {
        if batch.storage_dictionary.is_some() {
            return Err(paro_error::internal(
                "plain varlen row extraction received storage dictionary batch",
            ));
        }

        let mut offset = 0usize;
        let mut ranges = Vec::with_capacity(row_count);
        while ranges.len() < row_count {
            let len_start = offset;
            let len_end = offset
                .checked_add(std::mem::size_of::<u32>())
                .ok_or_else(|| paro_error::data_corrupted("varlen row length overflow"))?;
            if len_end > batch.data.len() {
                return Err(paro_error::data_corrupted(
                    "varlen row length prefix truncated",
                ));
            }

            let len = u32::from_le_bytes(
                batch.data[len_start..len_end]
                    .try_into()
                    .expect("u32 length prefix"),
            ) as usize;
            let value_end = len_end
                .checked_add(len)
                .ok_or_else(|| paro_error::data_corrupted("varlen row value overflow"))?;
            if value_end > batch.data.len() {
                return Err(paro_error::data_corrupted("varlen row extends past batch"));
            }

            ranges.push((len_start, value_end));
            offset = value_end;
        }

        Ok(ranges)
    }

    fn next_batch_within_current_page(&mut self, n: usize) -> Result<(usize, ColumnBatch)> {
        if n == 0 || !self.ensure_page_loaded(DecodedPageAccess::Sequential)? {
            return Ok((0, ColumnBatch::empty()));
        }

        let decoder = self
            .current_decoder
            .as_mut()
            .ok_or_else(|| paro_error::internal("column page decoder not loaded"))?;
        let page_remaining = decoder.count().saturating_sub(decoder.current_index()) as usize;
        let to_read = n.min(page_remaining);
        if to_read == 0 {
            return Ok((0, ColumnBatch::empty()));
        }

        let (count, data) = decoder.next_batch(to_read)?;
        let nulls = if let Some(ref mut null_decoder) = self.current_null_decoder {
            let (null_count, null_bytes) = null_decoder.next_batch(count)?;
            if null_count != count {
                return Err(paro_error::data_corrupted(
                    "Null map count mismatch with data page",
                ));
            }
            Some(null_bytes)
        } else {
            None
        };

        self.current_ordinal += count as u64;
        Ok((
            count,
            self.attach_current_page_integrity(ColumnBatch::new(data, nulls)),
        ))
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
    fn ensure_page_loaded(&mut self, access: DecodedPageAccess) -> Result<bool> {
        if self.need_next_page() {
            // Find the page containing current_ordinal
            let Some(page_idx) = self.ordinal_index.seek_at_or_before(self.current_ordinal) else {
                return Ok(false);
            };
            self.load_page(page_idx)?;

            // Seek within the page
            let offset_in_page = (self.current_ordinal - self.current_page_first_ordinal) as u32;
            if let Some(ref mut decoder) = self.current_decoder {
                decoder.seek_to_position(offset_in_page)?;
            }
            if let Some(ref mut null_decoder) = self.current_null_decoder {
                null_decoder.seek_to_position(offset_in_page)?;
            }
        }

        self.prepare_page_access(access)?;
        Ok(true)
    }

    fn prepare_page_access(&mut self, access: DecodedPageAccess) -> Result<()> {
        let Some(decoder) = self
            .current_decoder
            .as_mut()
            .and_then(PageDecoderImpl::decoded_cache_decoder_mut)
        else {
            return Ok(());
        };
        if decoder.is_materialized() {
            return Ok(());
        }

        let page_pointer = self.current_page_pointer.ok_or_else(|| {
            paro_error::internal("loaded BitShuffle page is missing its physical pointer")
        })?;
        if !self
            .page_reader
            .should_materialize_decoded(page_pointer, access)
        {
            return Ok(());
        }
        let decoded_size = decoder.decoded_size()?;
        if let Some(decoded) =
            self.page_reader
                .cache_decoded_with(page_pointer, decoded_size, |destination| {
                    decoder.materialize_into(destination)
                })?
        {
            decoder.install_decoded(decoded)?;
        } else if access == DecodedPageAccess::Sequential {
            decoder.materialize_all()?;
        }
        Ok(())
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
        self.current_page_pointer = None;
        self.current_page_utf8_verified = false;
        Ok(())
    }

    fn try_next_storage_dictionary_batch(
        &mut self,
        n: usize,
    ) -> Result<Option<(usize, ColumnBatch)>> {
        if self.current_ordinal >= self.meta.num_rows || n == 0 {
            return Ok(None);
        }
        if !self.ensure_page_loaded(DecodedPageAccess::Sequential)? {
            return Ok(None);
        }

        let Some(dictionary) = self.dict_data.clone() else {
            return Ok(None);
        };

        let (count, codes) = match self.current_decoder.as_mut() {
            Some(PageDecoderImpl::Dictionary { decoder, .. }) if decoder.is_dict_encoded() => {
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
        } else {
            None
        };

        self.current_ordinal += count as u64;
        Ok(Some((
            count,
            self.attach_current_page_integrity(ColumnBatch::with_storage_dictionary(
                dictionary, codes, nulls,
            )),
        )))
    }

    fn try_read_varlen_storage_dictionary_by_rowids<S: RowIdSequence + ?Sized>(
        &mut self,
        rowids: &S,
        total_rows: usize,
    ) -> Result<Option<ColumnBatch>> {
        let Some(dictionary) = self.dict_data.clone() else {
            return Ok(None);
        };

        let mut codes = vec![0u32; total_rows];
        let mut nulls_out = if self.column_may_have_nulls() {
            Some(vec![0u8; total_rows])
        } else {
            None
        };
        let mut page_run_seeks = 0usize;
        let mut utf8_verified = true;

        let mut idx = 0usize;
        while idx < rowids.len() {
            let row_run = self.next_rowid_page_run(rowids, &mut idx)?;

            self.seek_internal(row_run.span_start)?;
            page_run_seeks += 1;
            if !self.ensure_page_loaded(DecodedPageAccess::SparseGather)? {
                return Err(paro_error::out_of_range(format!(
                    "rowid {} not found",
                    row_run.span_start
                )));
            }
            utf8_verified &= self.current_page_utf8_verified;

            let (count, code_bytes) = match self.current_decoder.as_mut() {
                Some(PageDecoderImpl::Dictionary { decoder, .. }) if decoder.is_dict_encoded() => {
                    decoder.next_dict_codes(row_run.span_len)?
                }
                _ => {
                    storage_metrics().add_column_read_by_rowids_page_run_seeks(page_run_seeks);
                    return Ok(None);
                }
            };

            if count != row_run.span_len
                || code_bytes.len()
                    < row_run
                        .span_len
                        .checked_mul(std::mem::size_of::<u32>())
                        .ok_or_else(|| {
                            paro_error::data_corrupted("dictionary code bytes overflow")
                        })?
            {
                return Err(paro_error::out_of_range(format!(
                    "rowid {} not found",
                    row_run.span_end
                )));
            }

            let span_nulls = if let Some(ref mut null_decoder) = self.current_null_decoder {
                let (null_count, null_bytes) = null_decoder.next_batch(row_run.span_len)?;
                if null_count != row_run.span_len {
                    return Err(paro_error::data_corrupted(
                        "Null map count mismatch with dictionary row lookup",
                    ));
                }
                Some(null_bytes)
            } else {
                None
            };

            for run_idx in row_run.run_start..row_run.run_end {
                let (orig_idx, rowid) = rowids.pair(run_idx);
                let row_idx = usize::try_from(rowid - row_run.span_start)
                    .map_err(|_| paro_error::data_corrupted("dictionary row offset overflow"))?;
                let code_offset = row_idx
                    .checked_mul(std::mem::size_of::<u32>())
                    .ok_or_else(|| paro_error::data_corrupted("dictionary code offset overflow"))?;
                let code_end = code_offset
                    .checked_add(std::mem::size_of::<u32>())
                    .ok_or_else(|| paro_error::data_corrupted("dictionary code end overflow"))?;
                if code_end > code_bytes.len() {
                    return Err(paro_error::out_of_range(format!(
                        "dictionary row {} out of range",
                        row_idx
                    )));
                }
                codes[orig_idx] = u32::from_le_bytes(
                    code_bytes[code_offset..code_end]
                        .try_into()
                        .expect("u32 code slice"),
                );

                if let Some(ref mut nulls) = nulls_out {
                    let is_null = span_nulls
                        .as_ref()
                        .and_then(|b| b.get(row_idx))
                        .copied()
                        .unwrap_or(0);
                    nulls[orig_idx] = is_null;
                }
            }

            self.current_ordinal = row_run
                .span_end
                .checked_add(1)
                .ok_or_else(|| paro_error::data_corrupted("dictionary row ordinal overflow"))?;
        }
        storage_metrics().add_column_read_by_rowids_page_run_seeks(page_run_seeks);

        let mut encoded_codes = Vec::with_capacity(total_rows * std::mem::size_of::<u32>());
        for code in codes {
            encoded_codes.extend_from_slice(&code.to_le_bytes());
        }

        let batch = ColumnBatch::with_storage_dictionary(
            dictionary,
            Bytes::from(encoded_codes),
            nulls_out.map(Bytes::from),
        )
        .with_page_run_seeks(page_run_seeks);
        Ok(Some(if utf8_verified {
            batch.with_verified_utf8()
        } else {
            batch
        }))
    }

    fn read_plain_varlen_by_rowids<S: RowIdSequence + ?Sized>(
        &mut self,
        rowids: &S,
        total_rows: usize,
    ) -> Result<ColumnBatch> {
        let mut values: Vec<Vec<u8>> = vec![Vec::new(); total_rows];
        let mut nulls_out: Option<Vec<u8>> = if self.column_may_have_nulls() {
            Some(vec![0u8; total_rows])
        } else {
            None
        };

        let mut idx = 0usize;
        let mut page_run_seeks = 0usize;
        let mut utf8_verified = true;
        while idx < rowids.len() {
            let row_run = self.next_rowid_page_run(rowids, &mut idx)?;

            self.seek_internal(row_run.span_start)?;
            page_run_seeks += 1;
            let (count, batch) = self.next_batch_within_current_page(row_run.span_len)?;
            utf8_verified &= batch.has_verified_utf8();
            if count != row_run.span_len {
                return Err(paro_error::out_of_range(format!(
                    "rowid {} not found",
                    row_run.span_end
                )));
            }
            let row_ranges = Self::plain_varlen_row_ranges(&batch, count)?;

            for run_idx in row_run.run_start..row_run.run_end {
                let (orig_idx, rowid) = rowids.pair(run_idx);
                let row_idx = usize::try_from(rowid - row_run.span_start)
                    .map_err(|_| paro_error::data_corrupted("varlen row offset overflow"))?;
                let Some(&(row_start, row_end)) = row_ranges.get(row_idx) else {
                    return Err(paro_error::out_of_range(format!(
                        "varlen row {} out of range",
                        row_idx
                    )));
                };
                values[orig_idx].extend_from_slice(&batch.data[row_start..row_end]);
                if let Some(ref mut nulls) = nulls_out {
                    let is_null = batch
                        .nulls
                        .as_ref()
                        .and_then(|b| b.get(row_idx))
                        .copied()
                        .unwrap_or(0);
                    nulls[orig_idx] = is_null;
                }
            }
        }
        storage_metrics().add_column_read_by_rowids_page_run_seeks(page_run_seeks);

        let total_len: usize = values.iter().map(|v| v.len()).sum();
        let mut result = Vec::with_capacity(total_len);
        for v in values {
            result.extend_from_slice(&v);
        }
        let batch = ColumnBatch::new(Bytes::from(result), nulls_out.map(Bytes::from))
            .with_page_run_seeks(page_run_seeks);
        Ok(if utf8_verified {
            batch.with_verified_utf8()
        } else {
            batch
        })
    }

    fn read_fixed_width_by_rowids<S: RowIdSequence + ?Sized>(
        &mut self,
        rowids: &S,
        total_rows: usize,
        type_size: usize,
    ) -> Result<ColumnBatch> {
        let data_len = total_rows
            .checked_mul(type_size)
            .ok_or_else(|| paro_error::data_corrupted("fixed-width result size overflow"))?;
        let mut result = vec![0u8; data_len];
        let mut result_nulls: Option<Vec<u8>> = if self.column_may_have_nulls() {
            Some(vec![0u8; total_rows])
        } else {
            None
        };

        let mut idx = 0usize;
        let mut page_run_seeks = 0usize;
        while idx < rowids.len() {
            let row_run = self.next_rowid_page_run(rowids, &mut idx)?;

            self.seek_internal(row_run.span_start)?;
            page_run_seeks += 1;
            if !self.ensure_page_loaded(DecodedPageAccess::SparseGather)? {
                return Err(paro_error::out_of_range(format!(
                    "rowid {} not found",
                    row_run.span_start
                )));
            }

            if matches!(self.current_decoder, Some(PageDecoderImpl::BitShuffle(_))) {
                let span_nulls = if let Some(ref mut null_decoder) = self.current_null_decoder {
                    let (null_count, null_bytes) = null_decoder.next_batch(row_run.span_len)?;
                    if null_count != row_run.span_len {
                        return Err(paro_error::data_corrupted(
                            "Null map count mismatch with fixed-width row lookup",
                        ));
                    }
                    Some(null_bytes)
                } else {
                    None
                };
                let page_start = self.current_page_first_ordinal;
                let decoder = match self.current_decoder.as_mut() {
                    Some(PageDecoderImpl::BitShuffle(decoder)) => decoder,
                    _ => unreachable!("BitShuffle decoder checked above"),
                };

                let page_span_end = u32::try_from(row_run.span_end - page_start).map_err(|_| {
                    paro_error::data_corrupted("BitShuffle page row offset overflow")
                })?;
                if page_span_end >= decoder.count() {
                    return Err(paro_error::data_corrupted(format!(
                        "BitShuffle row span exceeds page: end={page_span_end}, count={}",
                        decoder.count()
                    )));
                }
                // SAFETY: `run` belongs to the page selected by
                // `next_rowid_page_run`; `page_span_end < decoder.count()`
                // proves every sorted source index is in range. `orig_idx`
                // came from enumerating the `total_rows` request that sized
                // `result`, so every destination slot is in range.
                unsafe {
                    decoder.gather_values_at_validated(
                        (row_run.run_start..row_run.run_end).map(|run_idx| {
                            let (orig_idx, rowid) = rowids.pair(run_idx);
                            ((rowid - page_start) as u32, orig_idx)
                        }),
                        &mut result,
                    )?;
                }

                if let Some(ref mut nulls_out) = result_nulls {
                    for run_idx in row_run.run_start..row_run.run_end {
                        let (orig_idx, rowid) = rowids.pair(run_idx);
                        let span_idx =
                            usize::try_from(rowid - row_run.span_start).map_err(|_| {
                                paro_error::data_corrupted("fixed-width null offset overflow")
                            })?;
                        nulls_out[orig_idx] = span_nulls
                            .as_ref()
                            .and_then(|nulls| nulls.get(span_idx))
                            .copied()
                            .unwrap_or(0);
                    }
                }

                let next_ordinal = row_run.span_end.checked_add(1).ok_or_else(|| {
                    paro_error::data_corrupted("fixed-width row ordinal overflow")
                })?;
                decoder.seek_to_position(u32::try_from(next_ordinal - page_start).map_err(
                    |_| paro_error::data_corrupted("BitShuffle page cursor overflow"),
                )?)?;
                self.current_ordinal = next_ordinal;
                continue;
            }

            let (count, batch) = self.next_batch_within_current_page(row_run.span_len)?;
            if count != row_run.span_len {
                return Err(paro_error::out_of_range(format!(
                    "rowid {} not found",
                    row_run.span_end
                )));
            }

            for run_idx in row_run.run_start..row_run.run_end {
                let (orig_idx, rowid) = rowids.pair(run_idx);
                let row_idx = usize::try_from(rowid - row_run.span_start)
                    .map_err(|_| paro_error::data_corrupted("fixed-width row offset overflow"))?;
                let src_start = row_idx
                    .checked_mul(type_size)
                    .ok_or_else(|| paro_error::data_corrupted("fixed-width source overflow"))?;
                let src_end = src_start
                    .checked_add(type_size)
                    .ok_or_else(|| paro_error::data_corrupted("fixed-width source end overflow"))?;
                let dst_start = orig_idx.checked_mul(type_size).ok_or_else(|| {
                    paro_error::data_corrupted("fixed-width destination overflow")
                })?;
                let dst_end = dst_start.checked_add(type_size).ok_or_else(|| {
                    paro_error::data_corrupted("fixed-width destination end overflow")
                })?;
                if src_end > batch.data.len() || dst_end > result.len() {
                    return Err(paro_error::out_of_range(format!(
                        "fixed-width row {} out of range",
                        row_idx
                    )));
                }

                result[dst_start..dst_end].copy_from_slice(&batch.data[src_start..src_end]);
                if let Some(ref mut nulls_out) = result_nulls {
                    let is_null = batch
                        .nulls
                        .as_ref()
                        .and_then(|b| b.get(row_idx))
                        .copied()
                        .unwrap_or(0);
                    nulls_out[orig_idx] = is_null;
                }
            }
        }
        storage_metrics().add_column_read_by_rowids_page_run_seeks(page_run_seeks);

        Ok(
            ColumnBatch::new(Bytes::from(result), result_nulls.map(Bytes::from))
                .with_page_run_seeks(page_run_seeks),
        )
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
        if !self.ensure_page_loaded(DecodedPageAccess::Sequential)? {
            return Ok((0, ColumnBatch::empty()));
        }

        let page_remaining = self
            .current_decoder
            .as_ref()
            .map(|decoder| decoder.count().saturating_sub(decoder.current_index()) as usize)
            .unwrap_or(0);
        if n <= page_remaining {
            return self.next_batch_within_current_page(n);
        }

        let mut total_read = 0;
        let mut result_data = Vec::new();
        let mut result_nulls: Option<Vec<u8>> = if self.column_may_have_nulls() {
            Some(Vec::new())
        } else {
            None
        };
        let mut utf8_verified = true;

        while total_read < n && self.current_ordinal < self.meta.num_rows {
            // Ensure page is loaded
            if !self.ensure_page_loaded(DecodedPageAccess::Sequential)? {
                break;
            }
            utf8_verified &= self.current_page_utf8_verified;

            let decoder = self.current_decoder.as_mut().unwrap();

            // Calculate how many to read from this page
            let page_remaining = decoder.count() - decoder.current_index();
            let to_read = std::cmp::min(n - total_read, page_remaining as usize);

            if to_read == 0 {
                // Move to next page
                self.current_page_idx = None;
                self.current_decoder = None;
                self.current_null_decoder = None;
                self.current_page_pointer = None;
                self.current_page_utf8_verified = false;
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

        let batch = ColumnBatch::new(Bytes::from(result_data), result_nulls.map(Bytes::from));
        Ok((
            total_read,
            if utf8_verified {
                batch.with_verified_utf8()
            } else {
                batch
            },
        ))
    }

    fn read_by_rowids(&mut self, rowids: &[u64]) -> Result<ColumnBatch> {
        if rowids.is_empty() {
            return Ok(ColumnBatch::empty());
        }

        if let Some(type_size) = self.meta.type_size {
            let mut sorted_rowids: Vec<(usize, u64)> = rowids.iter().copied().enumerate().collect();
            sorted_rowids.sort_by_key(|&(_, rowid)| rowid);
            self.read_fixed_width_by_rowids(sorted_rowids.as_slice(), rowids.len(), type_size)
        } else {
            // Variable-length types keep their length-prefixed bytes after page-local span reads.
            let mut sorted_rowids: Vec<(usize, u64)> = rowids.iter().copied().enumerate().collect();
            sorted_rowids.sort_by_key(|&(_, rowid)| rowid);

            if let Some(batch) = self.try_read_varlen_storage_dictionary_by_rowids(
                sorted_rowids.as_slice(),
                rowids.len(),
            )? {
                return Ok(batch);
            }

            self.read_plain_varlen_by_rowids(sorted_rowids.as_slice(), rowids.len())
        }
    }

    fn read_by_ordered_rowids(&mut self, rowids: &OrderedRowIds<'_>) -> Result<ColumnBatch> {
        if rowids.is_empty() {
            return Ok(ColumnBatch::empty());
        }
        if let Some(type_size) = self.meta.type_size {
            self.read_fixed_width_by_rowids(rowids, rowids.len(), type_size)
        } else {
            if let Some(batch) =
                self.try_read_varlen_storage_dictionary_by_rowids(rowids, rowids.len())?
            {
                return Ok(batch);
            }
            self.read_plain_varlen_by_rowids(rowids, rowids.len())
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
                    self.inner.current_page_pointer = None;
                    self.inner.current_page_utf8_verified = false;
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

    fn read_by_ordered_rowids(&mut self, rowids: &OrderedRowIds<'_>) -> Result<ColumnBatch> {
        self.inner.read_by_ordered_rowids(rowids)
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
    use crate::buffer::{BufferPool, PageCache};
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

    #[test]
    fn ordered_rowids_require_strict_order() {
        assert!(OrderedRowIds::try_new(&[1, 2, 4]).is_ok());
        assert!(OrderedRowIds::try_new(&[]).is_ok());
        assert!(OrderedRowIds::try_new(&[1, 1]).is_err());
        assert!(OrderedRowIds::try_new(&[2, 1]).is_err());
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

    fn create_fixed_i32_iterator(
        nullable: bool,
        page_size: usize,
        row_count: usize,
    ) -> Box<dyn ColumnIterator + Send + Sync> {
        let opts = ColumnWriterOptions::new(FieldType::Int, 0)
            .with_nullable(nullable)
            .with_compression(CompressionType::None)
            .with_page_size(page_size);
        let buffer = Cursor::new(Vec::new());
        let mut writer = ScalarColumnWriter::new(opts, buffer).unwrap();

        let values: Vec<i32> = (0..row_count as i32).collect();
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        let nulls = nullable.then(|| {
            let mut flags = vec![0u8; row_count.div_ceil(8)];
            if row_count > 7 {
                flags[0] |= 1 << 7;
            }
            flags
        });
        writer
            .append(
                &bytes,
                nulls.as_deref(),
                row_count.try_into().expect("row count fits in u32"),
            )
            .unwrap();

        let meta = writer.finish().unwrap();
        let buffer = Cursor::new(writer.into_inner().into_inner());
        let reader_meta =
            ColumnReaderMeta::from_writer_meta(&meta, FieldType::Int).with_nullable(nullable);
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
        let nulls = nullable.then(|| vec![0b0000_0010]);
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

    fn create_dict_i32_iterator() -> Box<dyn ColumnIterator + Send + Sync> {
        let opts = ColumnWriterOptions::new(FieldType::Int, 0)
            .with_nullable(false)
            .with_encoding(EncodingType::Dict)
            .with_compression(CompressionType::None)
            .with_page_size(1024);
        let buffer = Cursor::new(Vec::new());
        let mut writer = ScalarColumnWriter::new(opts, buffer).unwrap();

        let values = [7_i32, 11, 7, 13, 11, 7];
        let data = values
            .into_iter()
            .flat_map(i32::to_le_bytes)
            .collect::<Vec<_>>();
        writer.append(&data, None, values.len() as u32).unwrap();

        let meta = writer.finish().unwrap();
        assert_eq!(meta.num_rows, values.len() as u64);
        let buffer = Cursor::new(writer.into_inner().into_inner());
        let reader_meta = ColumnReaderMeta::from_writer_meta(&meta, FieldType::Int);
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

    fn create_plain_varchar_iterator(nullable: bool) -> Box<dyn ColumnIterator + Send + Sync> {
        create_plain_varchar_iterator_with_checksum(nullable, true)
    }

    fn create_plain_varchar_iterator_with_checksum(
        nullable: bool,
        verify_checksum: bool,
    ) -> Box<dyn ColumnIterator + Send + Sync> {
        let opts = ColumnWriterOptions::new(FieldType::Varchar, 0)
            .with_nullable(nullable)
            .with_encoding(EncodingType::Plain)
            .with_compression(CompressionType::None)
            .with_page_size(4096);
        let buffer = Cursor::new(Vec::new());
        let mut writer = ScalarColumnWriter::new(opts, buffer).unwrap();

        let strings = ["zero", "one", "two", "three", "four"];
        let data = encode_varlen_strings(&strings);
        let nulls = nullable.then(|| vec![0b0000_0100]);
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
            ColumnReaderOptions::default().with_verify_checksum(verify_checksum),
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
    fn decoded_cache_hit_bypasses_physical_page_for_all_valid_column() {
        let opts = ColumnWriterOptions::new(FieldType::Int, 0)
            .with_nullable(true)
            .with_encoding(EncodingType::BitShuffle)
            .with_compression(CompressionType::None);
        let mut writer = ScalarColumnWriter::new(opts, Cursor::new(Vec::new())).unwrap();
        let values: Vec<i32> = (0..17).collect();
        let bytes = values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect::<Vec<_>>();
        writer.append(&bytes, None, values.len() as u32).unwrap();
        let meta = writer.finish().unwrap();
        assert_eq!(meta.null_count, 0);
        let physical_page = writer.into_inner().into_inner();

        let reader_meta = ColumnReaderMeta::from_writer_meta(&meta, FieldType::Int);
        let ordinal_index = OrdinalIndexReader::new(
            vec![OrdinalIndexEntry {
                first_ordinal: 0,
                page_pointer: meta.data_page_pointer,
            }],
            values.len() as u64,
        );
        let page_cache = Arc::new(PageCache::new(BufferPool::new_arc(1024 * 1024)));
        let page_reader = PageReader::new(
            PageReaderContext::new(1, 2, 3, 4),
            Some(page_cache),
            PageReaderOptions {
                cache_decoded: true,
                ..PageReaderOptions::default()
            },
        );
        let reader_options = ColumnReaderOptions::default().with_compression(CompressionType::None);

        let mut first = ScalarColumnIterator::new(
            reader_meta.clone(),
            Cursor::new(physical_page),
            reader_options.clone(),
            page_reader.clone(),
            None,
            None,
            ordinal_index.clone(),
            None,
            None,
        )
        .unwrap();
        assert_eq!(first.next_batch(values.len()).unwrap().0, values.len());

        // An empty reader proves the second iterator reaches the complete
        // logical cache entry before attempting physical page I/O.
        let mut cached = ScalarColumnIterator::new(
            reader_meta,
            Cursor::new(Vec::new()),
            reader_options,
            page_reader,
            None,
            None,
            ordinal_index,
            None,
            None,
        )
        .unwrap();
        let (count, batch) = cached.next_batch(values.len()).unwrap();
        assert_eq!(count, values.len());
        for (row, expected) in values.into_iter().enumerate() {
            let start = row * std::mem::size_of::<i32>();
            assert_eq!(
                i32::from_le_bytes(batch.data[start..start + 4].try_into().unwrap()),
                expected
            );
        }
        assert!(batch.nulls.is_none());
    }

    #[test]
    fn sparse_gather_uses_probation_before_decoded_cache_promotion() {
        let opts = ColumnWriterOptions::new(FieldType::Int, 0)
            .with_nullable(false)
            .with_encoding(EncodingType::BitShuffle)
            .with_compression(CompressionType::None);
        let mut writer = ScalarColumnWriter::new(opts, Cursor::new(Vec::new())).unwrap();
        let values: Vec<i32> = (0..128).collect();
        let bytes = values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect::<Vec<_>>();
        writer.append(&bytes, None, values.len() as u32).unwrap();
        let meta = writer.finish().unwrap();
        let physical_page = writer.into_inner().into_inner();
        let reader_meta = ColumnReaderMeta::from_writer_meta(&meta, FieldType::Int);
        let ordinal_index = OrdinalIndexReader::new(
            vec![OrdinalIndexEntry {
                first_ordinal: 0,
                page_pointer: meta.data_page_pointer,
            }],
            values.len() as u64,
        );
        let page_cache = Arc::new(PageCache::new(BufferPool::new_arc(1024 * 1024)));
        let page_reader = PageReader::new(
            PageReaderContext::new(1, 2, 3, 4),
            Some(page_cache.clone()),
            PageReaderOptions {
                cache_decoded: true,
                ..PageReaderOptions::default()
            },
        );
        let reader_options = ColumnReaderOptions::default().with_compression(CompressionType::None);

        let mut gather = ScalarColumnIterator::new(
            reader_meta.clone(),
            Cursor::new(physical_page.clone()),
            reader_options.clone(),
            page_reader.clone(),
            None,
            None,
            ordinal_index.clone(),
            None,
            None,
        )
        .unwrap();
        let gathered = gather.read_by_rowids(&[3, 97]).unwrap();
        assert_eq!(
            gathered
                .data
                .chunks_exact(4)
                .map(|bytes| i32::from_le_bytes(bytes.try_into().unwrap()))
                .collect::<Vec<_>>(),
            [3, 97]
        );
        assert_eq!(page_cache.stats().decoded_entries, 0);

        let mut repeated_gather = ScalarColumnIterator::new(
            reader_meta.clone(),
            Cursor::new(physical_page.clone()),
            reader_options.clone(),
            page_reader.clone(),
            None,
            None,
            ordinal_index.clone(),
            None,
            None,
        )
        .unwrap();
        let repeated = repeated_gather.read_by_rowids(&[3, 97]).unwrap();
        assert_eq!(repeated.data, gathered.data);
        assert_eq!(page_cache.stats().decoded_entries, 1);

        let mut scan = ScalarColumnIterator::new(
            reader_meta,
            Cursor::new(physical_page),
            reader_options,
            page_reader,
            None,
            None,
            ordinal_index,
            None,
            None,
        )
        .unwrap();
        assert_eq!(scan.next_batch(values.len()).unwrap().0, values.len());
        assert_eq!(page_cache.stats().decoded_entries, 1);
    }

    #[test]
    fn dictionary_code_stream_is_retained_in_decoded_cache() {
        let opts = ColumnWriterOptions::new(FieldType::Varchar, 0)
            .with_nullable(false)
            .with_encoding(EncodingType::Dict)
            .with_compression(CompressionType::None)
            .with_page_size(16 * 1024);
        let mut writer = ScalarColumnWriter::new(opts, Cursor::new(Vec::new())).unwrap();
        let values = (0..4096)
            .map(|row| if row % 3 == 0 { "alpha" } else { "beta" })
            .collect::<Vec<_>>();
        writer
            .append(&encode_varlen_strings(&values), None, values.len() as u32)
            .unwrap();
        let meta = writer.finish().unwrap();
        let physical_page = writer.into_inner().into_inner();
        let reader_meta = ColumnReaderMeta::from_writer_meta(&meta, FieldType::Varchar);
        let page_cache = Arc::new(PageCache::new(BufferPool::new_arc(4 * 1024 * 1024)));
        let page_reader = PageReader::new(
            PageReaderContext::new(1, 2, 3, 4),
            Some(page_cache.clone()),
            PageReaderOptions {
                cache_decoded: true,
                ..PageReaderOptions::default()
            },
        );

        let mut expected_codes = None;
        for _ in 0..2 {
            let mut reader = ColumnReader::create(
                reader_meta.clone(),
                Cursor::new(physical_page.clone()),
                ColumnReaderOptions::default(),
                page_reader.clone(),
                None,
                None,
            )
            .unwrap();
            let mut iterator = reader.new_iterator().unwrap();
            let (count, batch) = iterator.next_batch(values.len()).unwrap();
            assert_eq!(count, values.len());
            let codes = batch
                .storage_dictionary
                .expect("dictionary scan should retain storage codes")
                .codes;
            if let Some(expected) = &expected_codes {
                assert_eq!(&codes, expected);
            } else {
                expected_codes = Some(codes);
            }
            assert_eq!(page_cache.stats().decoded_entries, 1);
        }
    }

    #[test]
    fn fixed_width_dictionary_roundtrips_as_a_typed_dictionary_batch() {
        let mut iter = create_dict_i32_iterator();
        let (count, batch) = iter.next_batch(6).unwrap();
        assert_eq!(count, 6);
        assert!(batch.storage_dictionary.is_some());

        let decoded = vector_decoder::decode_column_batch(
            &LogicalType::Integer,
            &batch,
            count,
            Arc::new(default_allocator()),
            None,
        )
        .unwrap();
        let expected = [7_i32, 11, 7, 13, 11, 7];
        for (row, expected) in expected.into_iter().enumerate() {
            assert_eq!(unsafe { decoded.get_fixed::<i32>(row) }, expected);
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
    fn test_scalar_iterator_read_by_rowids_preserves_fixed_order_duplicates_and_nulls() {
        let mut iter = create_fixed_i32_iterator(true, 32, 24);

        let batch = iter.read_by_rowids(&[18, 3, 18, 7]).unwrap();
        assert_eq!(batch.data.len(), 4 * std::mem::size_of::<i32>());

        let values = batch
            .data
            .chunks_exact(std::mem::size_of::<i32>())
            .map(|bytes| i32::from_le_bytes(bytes.try_into().expect("i32 bytes")))
            .collect::<Vec<_>>();
        assert_eq!(values, vec![18, 3, 18, 7]);
        assert_eq!(batch.nulls.as_deref(), Some(&[0, 0, 0, 1][..]));
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
        assert!(batch.has_verified_utf8());
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
    fn utf8_integrity_requires_a_checksum_verified_current_page() {
        let mut verified = create_plain_varchar_iterator_with_checksum(false, true);
        let (_, verified_batch) = verified.next_batch(5).unwrap();
        assert!(verified_batch.has_verified_utf8());

        let mut unverified = create_plain_varchar_iterator_with_checksum(false, false);
        let (_, unverified_batch) = unverified.next_batch(5).unwrap();
        assert!(!unverified_batch.has_verified_utf8());
    }

    #[test]
    fn test_dict_varchar_iterator_crosses_page_and_input_batch_boundaries() {
        let opts = ColumnWriterOptions::new(FieldType::Varchar, 0)
            .with_nullable(false)
            .with_encoding(EncodingType::Dict)
            .with_compression(CompressionType::None)
            .with_page_size(16 * 1024);
        let buffer = Cursor::new(Vec::new());
        let mut writer = ScalarColumnWriter::new(opts, buffer).unwrap();

        let first = vec!["N"; 4096];
        let second = vec!["R"; 1909];
        writer
            .append(&encode_varlen_strings(&first), None, first.len() as u32)
            .unwrap();
        writer
            .append(&encode_varlen_strings(&second), None, second.len() as u32)
            .unwrap();

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

        let (first_count, first_batch) = iter.next_batch(4096).unwrap();
        assert_eq!(first_count, 4096);
        for row in 0..first_count {
            assert_eq!(
                first_batch.varlen_row(row).unwrap().as_deref(),
                Some(b"N".as_slice())
            );
        }

        let (second_count, second_batch) = iter.next_batch(4096).unwrap();
        assert_eq!(second_count, 1909);
        for row in 0..second_count {
            assert_eq!(
                second_batch.varlen_row(row).unwrap().as_deref(),
                Some(b"R".as_slice())
            );
        }
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

    #[test]
    fn test_varchar_iterator_read_by_rowids_storage_dictionary_preserves_duplicates_and_nulls() {
        let mut iter = create_dict_varchar_iterator(true);

        let batch = iter.read_by_rowids(&[2, 1, 2, 3]).unwrap();
        assert!(batch.storage_dictionary.is_some());

        let vector = vector_decoder::decode_column_batch(
            &LogicalType::Varchar,
            &batch,
            4,
            Arc::new(default_allocator()),
            Some(89),
        )
        .unwrap();

        assert_eq!(vector.get_string(0), Some("apple"));
        assert!(vector.is_null(1));
        assert_eq!(vector.get_string(2), Some("apple"));
        assert_eq!(vector.get_string(3), Some("cherry"));
        let info = vector
            .dictionary_info()
            .expect("decoded rowid storage dictionary should keep provenance");
        assert_eq!(info.provenance_id, Some(89));
        assert_eq!(info.source, DictionarySource::Storage);
    }

    #[test]
    fn test_varchar_iterator_read_by_rowids_preserves_plain_order_duplicates_and_nulls() {
        let mut iter = create_plain_varchar_iterator(true);

        let batch = iter.read_by_rowids(&[3, 1, 3, 2]).unwrap();

        assert_eq!(batch.varlen_row(0).unwrap().unwrap().as_ref(), b"three");
        assert_eq!(batch.varlen_row(1).unwrap().unwrap().as_ref(), b"one");
        assert_eq!(batch.varlen_row(2).unwrap().unwrap().as_ref(), b"three");
        assert!(batch.varlen_row(3).unwrap().is_none());
    }
}
