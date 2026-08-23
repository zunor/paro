// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Column Writer
//!
//! Writes column data with automatic encoding selection, page building, and index generation.
//!
//! ## Architecture
//!
//! ```text
//! ColumnWriter
//!   ├── PageBuilder (encoding-specific)
//!   ├── NullMapBuilder (BitShuffle for v2 format)
//!   ├── OrdinalIndexWriter
//!   └── ZoneMapIndexWriter
//! ```
//!
//! ## Usage
//!
//! ```ignore
//! let opts = ColumnWriterOptions::new(FieldType::Int, 0);
//! let mut writer = ColumnWriter::create(opts, &mut file)?;
//!
//! // Append data
//! writer.append(&values, &null_flags)?;
//!
//! // Finish and get metadata
//! let meta = writer.finish()?;
//! ```

use crate::codec::physical_layout::fixed_row_width;
use crate::index::hnsw::{
    HnswBuildContract, HnswBuildExecutionPolicy, HnswBuildStopCheck, HnswBuilder,
    InMemoryVectorStorage, VectorStorage,
};
use crate::index::{BitmapIndexWriter, BloomFilterIndexWriter, BloomFilterOptions};
use crate::rowset::encoding::{
    get_encoding_registry, BinaryDictPageBuilder, BinaryPlainPageBuilder, BitShufflePageBuilder,
    FieldType, PlainPageBuilder, RlePageBuilder,
};
use crate::rowset::page::{
    CompressionType, DataPageFooter, EncodingType, IndexPageFooter, IndexPageType, NullEncoding,
    PageFooter, PageIO, PagePointer, CURRENT_DATA_PAGE_FORMAT_VERSION,
};
use crate::statistics::{BaseStatistics, ColumnStatistics, HnswIndexStatistics};
use bytes::{BufMut, Bytes, BytesMut};
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use std::io::{Seek, Write};
use std::sync::Arc;

/// Default page size (256KB).
pub const DEFAULT_PAGE_SIZE: usize = 256 * 1024;

/// Default minimum space saving for compression.
pub const DEFAULT_MIN_SPACE_SAVING: f64 = 0.1;

/// Trait for writers that can provide their buffered data.
pub trait DataWriter: Write + Seek + Send {
    fn get_data(&self) -> Bytes;
}

impl DataWriter for std::io::Cursor<Vec<u8>> {
    fn get_data(&self) -> Bytes {
        Bytes::copy_from_slice(self.get_ref())
    }
}

impl DataWriter for std::fs::File {
    fn get_data(&self) -> Bytes {
        Bytes::new()
    }
}

/// Column writer options.
#[derive(Debug, Clone)]
pub struct ColumnWriterOptions {
    /// Field type for encoding selection
    pub field_type: FieldType,
    /// Column ID
    pub column_id: u32,
    /// Logical type for statistics (optional)
    pub logical_type: Option<LogicalType>,
    /// Whether the column is nullable
    pub is_nullable: bool,
    /// Target page size in bytes
    pub page_size: usize,
    /// Compression type
    pub compression: CompressionType,
    /// Minimum space saving ratio for compression
    pub min_space_saving: f64,
    /// Encoding type (Default = auto-select)
    pub encoding: EncodingType,
    /// Data-page format version.
    pub format_version: u32,
    /// Whether to build a bloom filter index
    pub build_bloom_filter: bool,
    /// Whether to build a bitmap index
    pub build_bitmap_index: bool,
    /// Fixed length for types like Vector
    pub fixed_len: usize,
    /// Whether to build an HNSW index (for Vector type)
    pub build_hnsw: bool,
    /// Immutable HNSW physical build contract.
    pub hnsw_build_contract: Option<HnswBuildContract>,
    /// Optional cooperative stop-check for HNSW build.
    pub hnsw_stop_check: Option<HnswBuildStopCheck>,
}

impl ColumnWriterOptions {
    /// Create new options with default settings.
    pub fn new(field_type: FieldType, column_id: u32) -> Self {
        ColumnWriterOptions {
            field_type,
            column_id,
            logical_type: None,
            is_nullable: true,
            page_size: DEFAULT_PAGE_SIZE,
            compression: CompressionType::Lz4,
            min_space_saving: DEFAULT_MIN_SPACE_SAVING,
            encoding: EncodingType::Default,
            format_version: CURRENT_DATA_PAGE_FORMAT_VERSION,
            build_bloom_filter: false,
            build_bitmap_index: false,
            fixed_len: 0,
            build_hnsw: false,
            hnsw_build_contract: None,
            hnsw_stop_check: None,
        }
    }

    pub fn with_logical_type(mut self, logical_type: LogicalType) -> Self {
        self.logical_type = Some(logical_type);
        self
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.is_nullable = nullable;
        self
    }

    pub fn with_page_size(mut self, size: usize) -> Self {
        self.page_size = size;
        self
    }

    pub fn with_compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }

    pub fn with_encoding(mut self, encoding: EncodingType) -> Self {
        self.encoding = encoding;
        self
    }

    pub fn with_bloom_filter(mut self, build: bool) -> Self {
        self.build_bloom_filter = build;
        self
    }

    pub fn with_bitmap_index(mut self, build: bool) -> Self {
        self.build_bitmap_index = build;
        self
    }

    pub fn with_fixed_len(mut self, len: usize) -> Self {
        self.fixed_len = len;
        self
    }

    pub fn with_hnsw(mut self, build: bool, build_contract: Option<HnswBuildContract>) -> Self {
        self.build_hnsw = build;
        self.hnsw_build_contract = build_contract;
        self
    }

    pub fn with_hnsw_stop_check(mut self, stop_check: Option<HnswBuildStopCheck>) -> Self {
        self.hnsw_stop_check = stop_check;
        self
    }
}

/// Ordinal index writer - maps row ordinals to page pointers.
#[derive(Debug, Default)]
pub struct OrdinalIndexWriter {
    /// (first_ordinal, page_pointer) pairs
    entries: Vec<(u64, PagePointer)>,
}

impl OrdinalIndexWriter {
    pub fn new() -> Self {
        OrdinalIndexWriter {
            entries: Vec::new(),
        }
    }

    /// Add an entry for a data page.
    pub fn add(&mut self, first_ordinal: u64, page_pointer: PagePointer) {
        self.entries.push((first_ordinal, page_pointer));
    }

    /// Finish and serialize the index, relocating pointers by base_offset.
    pub fn finish_relocated(&self, base_offset: u64) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.entries.len() * 20 + 4);
        buf.put_u32_le(self.entries.len() as u32);
        for &(ordinal, ptr) in &self.entries {
            buf.put_u64_le(ordinal);
            let relocated_ptr = PagePointer::new(base_offset + ptr.offset, ptr.size);
            relocated_ptr.encode_fixed(&mut buf);
        }
        buf.freeze()
    }

    /// Finish and serialize the index.
    pub fn finish(&self) -> Bytes {
        self.finish_relocated(0)
    }

    /// Get the number of entries.
    pub fn num_entries(&self) -> usize {
        self.entries.len()
    }
}

/// ZoneMap index writer - stores min/max/has_null per page.
#[derive(Debug)]
pub struct ZoneMapIndexWriter {
    /// Per-page zone maps: (min, max, has_null)
    entries: Vec<ZoneMapEntry>,
    /// Global min value
    global_min: Option<Bytes>,
    /// Global max value
    global_max: Option<Bytes>,
    /// Global has_null flag
    global_has_null: bool,
}

#[derive(Debug, Clone)]
struct ZoneMapEntry {
    min: Bytes,
    max: Bytes,
    has_null: bool,
}

impl ZoneMapIndexWriter {
    pub fn new() -> Self {
        ZoneMapIndexWriter {
            entries: Vec::new(),
            global_min: None,
            global_max: None,
            global_has_null: false,
        }
    }

    /// Add a zone map entry for a page.
    pub fn add(&mut self, min: Bytes, max: Bytes, has_null: bool) {
        // Update global stats
        if self.global_min.is_none() || min < *self.global_min.as_ref().unwrap() {
            self.global_min = Some(min.clone());
        }
        if self.global_max.is_none() || max > *self.global_max.as_ref().unwrap() {
            self.global_max = Some(max.clone());
        }
        if has_null {
            self.global_has_null = true;
        }

        self.entries.push(ZoneMapEntry { min, max, has_null });
    }

    /// Finish and serialize the index.
    pub fn finish(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Write global zone map
        Self::write_zonemap_value(&mut buf, self.global_min.as_ref());
        Self::write_zonemap_value(&mut buf, self.global_max.as_ref());
        buf.put_u8(if self.global_has_null { 1 } else { 0 });

        // Write per-page zone maps
        buf.put_u32_le(self.entries.len() as u32);
        for entry in &self.entries {
            Self::write_zonemap_value(&mut buf, Some(&entry.min));
            Self::write_zonemap_value(&mut buf, Some(&entry.max));
            buf.put_u8(if entry.has_null { 1 } else { 0 });
        }

        buf.freeze()
    }

    fn write_zonemap_value(buf: &mut BytesMut, value: Option<&Bytes>) {
        match value {
            Some(v) => {
                buf.put_u32_le(v.len() as u32 + 1);
                buf.extend_from_slice(v);
            }
            None => {
                buf.put_u32_le(0);
            }
        }
    }

    /// Get global min value.
    pub fn global_min(&self) -> Option<&Bytes> {
        self.global_min.as_ref()
    }

    /// Get global max value.
    pub fn global_max(&self) -> Option<&Bytes> {
        self.global_max.as_ref()
    }

    /// Check if any page has null values.
    pub fn has_null(&self) -> bool {
        self.global_has_null
    }
}

impl Default for ZoneMapIndexWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Column writer metadata returned after finish().
#[derive(Debug, Clone)]
pub struct ColumnWriterMeta {
    /// Column ID
    pub column_id: u32,
    /// Total number of rows written
    pub num_rows: u64,
    /// Encoding type used
    pub encoding: EncodingType,
    /// Compression type used
    pub compression: CompressionType,
    /// Data pages pointer (first page)
    pub data_page_pointer: PagePointer,
    /// Ordinal index pointer
    pub ordinal_index_pointer: PagePointer,
    /// ZoneMap index pointer
    pub zonemap_index_pointer: PagePointer,
    /// Dictionary page pointer (if dictionary encoding)
    pub dict_page_pointer: Option<PagePointer>,
    /// Bloom filter index pointer (optional)
    pub bloom_filter_pointer: Option<PagePointer>,
    /// Bitmap index pointer (optional)
    pub bitmap_index_pointer: Option<PagePointer>,
    /// HNSW index pointer (optional)
    pub hnsw_index_pointer: Option<PagePointer>,
    /// HNSW summary persisted in the segment footer.
    pub hnsw_index_statistics: Option<HnswIndexStatistics>,
    /// Total data size in bytes
    pub data_size: u64,
    /// Total index size in bytes
    pub index_size: u64,
    /// Total memory footprint (data + index) in bytes
    pub total_mem_footprint: u64,
    /// Column statistics collected during write
    pub column_stats: ColumnStatistics,
    /// Number of NULL values
    pub null_count: u64,
}

/// Trait for column writers.
pub trait ColumnWriter: Send {
    /// Append values to the column.
    ///
    /// # Arguments
    /// * `data` - Raw value bytes
    /// * `null_flags` - Null bitmap (1 bit per value, 1 = null)
    /// * `count` - Number of values
    fn append(&mut self, data: &[u8], null_flags: Option<&[u8]>, count: u32) -> Result<()>;

    /// Flush the current page if it has data.
    fn finish_current_page(&mut self) -> Result<()>;

    /// Finish writing and return metadata.
    fn finish(&mut self) -> Result<ColumnWriterMeta>;

    /// Finish writing with a base offset for relocating internal page pointers.
    ///
    /// Default implementation falls back to `finish()` when relocation is not needed.
    fn finish_with_base_offset(&mut self, _base_offset: u64) -> Result<ColumnWriterMeta> {
        self.finish()
    }

    /// Get the number of rows written so far.
    fn num_rows(&self) -> u64;

    /// Get the buffered data bytes (if in-memory).
    fn get_data(&self) -> Bytes;
}

/// Page builder wrapper for different encoding types.
enum PageBuilderImpl {
    Plain(PlainPageBuilder),
    BitShuffle(BitShufflePageBuilder),
    RleBool(RlePageBuilder<u8>),
    BinaryPlain(BinaryPlainPageBuilder),
    BinaryDict(BinaryDictPageBuilder),
}

impl PageBuilderImpl {
    fn is_page_full(&self) -> bool {
        match self {
            PageBuilderImpl::Plain(b) => b.is_page_full(),
            PageBuilderImpl::BitShuffle(b) => b.is_page_full(),
            PageBuilderImpl::RleBool(b) => b.is_page_full(),
            PageBuilderImpl::BinaryPlain(b) => b.is_page_full(),
            PageBuilderImpl::BinaryDict(b) => b.is_page_full(),
        }
    }

    fn count(&self) -> u32 {
        match self {
            PageBuilderImpl::Plain(b) => b.count(),
            PageBuilderImpl::BitShuffle(b) => b.count(),
            PageBuilderImpl::RleBool(b) => b.count(),
            PageBuilderImpl::BinaryPlain(b) => b.count(),
            PageBuilderImpl::BinaryDict(b) => b.count(),
        }
    }

    fn get_first_value(&self) -> Option<Bytes> {
        match self {
            PageBuilderImpl::Plain(b) => b.get_first_value(),
            PageBuilderImpl::BitShuffle(b) => b.get_first_value(),
            PageBuilderImpl::RleBool(b) => {
                b.get_first_value().map(|v| Bytes::copy_from_slice(&[v]))
            }
            PageBuilderImpl::BinaryPlain(b) => b.get_first_value(),
            PageBuilderImpl::BinaryDict(b) => b.get_first_value(),
        }
    }

    fn get_last_value(&self) -> Option<Bytes> {
        match self {
            PageBuilderImpl::Plain(b) => b.get_last_value(),
            PageBuilderImpl::BitShuffle(b) => b.get_last_value(),
            PageBuilderImpl::RleBool(b) => b.get_last_value().map(|v| Bytes::copy_from_slice(&[v])),
            PageBuilderImpl::BinaryPlain(b) => b.get_last_value(),
            PageBuilderImpl::BinaryDict(b) => b.get_last_value(),
        }
    }

    fn finish(&mut self) -> Result<Bytes> {
        match self {
            PageBuilderImpl::Plain(b) => b.finish(),
            PageBuilderImpl::BitShuffle(b) => b.finish(),
            PageBuilderImpl::RleBool(b) => b.finish(),
            PageBuilderImpl::BinaryPlain(b) => b.finish(),
            PageBuilderImpl::BinaryDict(b) => b.finish(),
        }
    }

    fn reset(&mut self) {
        match self {
            PageBuilderImpl::Plain(b) => b.reset(),
            PageBuilderImpl::BitShuffle(b) => b.reset(),
            PageBuilderImpl::RleBool(b) => b.reset(),
            PageBuilderImpl::BinaryPlain(b) => b.reset(),
            PageBuilderImpl::BinaryDict(b) => b.reset(),
        }
    }

    fn get_dictionary_page(&mut self) -> Option<Bytes> {
        match self {
            PageBuilderImpl::BinaryDict(b) => b.get_dictionary_page(),
            _ => None,
        }
    }
}

fn field_type_to_logical_type(field_type: FieldType) -> LogicalType {
    match field_type {
        FieldType::Boolean => LogicalType::Boolean,
        FieldType::TinyInt => LogicalType::TinyInt,
        FieldType::SmallInt => LogicalType::SmallInt,
        FieldType::Int => LogicalType::Integer,
        FieldType::BigInt => LogicalType::BigInt,
        FieldType::LargeInt => LogicalType::HugeInt,
        FieldType::Float => LogicalType::Float,
        FieldType::Double => LogicalType::Double,
        FieldType::Char | FieldType::Varchar | FieldType::Json => LogicalType::Varchar,
        FieldType::Date => LogicalType::Date,
        FieldType::DateTime => LogicalType::Timestamp,
        FieldType::Decimal => LogicalType::Decimal {
            precision: 0,
            scale: 0,
        },
        FieldType::Binary => LogicalType::Blob,
        FieldType::Vector => LogicalType::Array(Box::new(LogicalType::Float), 0),
    }
}

fn fixed_type_size(opts: &ColumnWriterOptions) -> Option<usize> {
    if opts.field_type == FieldType::Vector {
        return Some(opts.fixed_len);
    }

    opts.logical_type
        .as_ref()
        .and_then(|logical_type| fixed_row_width(logical_type).ok())
        .or_else(|| opts.field_type.size())
}

fn normalize_stats_logical_type(logical_type: LogicalType) -> LogicalType {
    match logical_type {
        LogicalType::VarcharCollation(_) => LogicalType::Varchar,
        // Use i128 for decimal statistics until scale-aware stats are supported.
        LogicalType::Decimal { .. } => LogicalType::HugeInt,
        other => other,
    }
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    paro_common::hash::hash_bytes(bytes)
}

fn decode_signed_wide_integer(bytes: &[u8], label: &str) -> Result<i128> {
    match bytes.len() {
        8..=15 => Ok(i64::from_le_bytes(bytes[..8].try_into().unwrap()) as i128),
        16.. => Ok(i128::from_le_bytes(bytes[..16].try_into().unwrap())),
        _ => Err(paro_error::type_mismatch(format!(
            "{label}: insufficient bytes"
        ))),
    }
}

fn decode_unsigned_wide_integer(bytes: &[u8], label: &str) -> Result<u128> {
    match bytes.len() {
        8..=15 => Ok(u64::from_le_bytes(bytes[..8].try_into().unwrap()) as u128),
        16.. => Ok(u128::from_le_bytes(bytes[..16].try_into().unwrap())),
        _ => Err(paro_error::type_mismatch(format!(
            "{label}: insufficient bytes"
        ))),
    }
}

/// Scalar column writer for non-nested types.
pub struct ScalarColumnWriter<W: DataWriter> {
    /// Writer options
    opts: ColumnWriterOptions,
    /// File writer
    writer: W,
    /// Page builder
    page_builder: PageBuilderImpl,
    /// Null map builder (BitShuffle for v2)
    null_builder: Option<BitShufflePageBuilder>,
    /// Ordinal index writer
    ordinal_index: OrdinalIndexWriter,
    /// ZoneMap index writer
    zonemap_index: ZoneMapIndexWriter,
    /// Bloom filter index writer (optional)
    bloom_filter_index: Option<BloomFilterIndexWriter>,
    /// Bitmap index writer (optional)
    bitmap_index: Option<BitmapIndexWriter>,
    /// Current page first ordinal
    first_ordinal: u64,
    /// Total rows written
    num_rows: u64,
    /// Current page has null
    page_has_null: bool,
    /// Data pages written
    data_pages: Vec<PagePointer>,
    /// Dictionary page pointer
    dict_page_pointer: Option<PagePointer>,
    /// Encoding type used
    encoding: EncodingType,
    /// Compression codec
    codec: Option<Box<dyn crate::rowset::page::BlockCompressionCodec>>,
    /// First data page pointer
    first_data_page: Option<PagePointer>,
    /// Total data size
    data_size: u64,
    /// HNSW vector storage (in-memory during building)
    hnsw_storage: Option<InMemoryVectorStorage>,
    /// Column statistics collected during write
    column_stats: ColumnStatistics,
    /// Number of NULL values
    null_count: u64,
    /// Logical type used for statistics
    stats_logical_type: LogicalType,
}

impl<W: DataWriter> ScalarColumnWriter<W> {
    /// Create a new scalar column writer.
    pub fn new(opts: ColumnWriterOptions, writer: W) -> Result<Self> {
        let registry = get_encoding_registry();
        let encoding = if opts.encoding == EncodingType::Default {
            registry.get_default_encoding(opts.field_type, false)
        } else {
            opts.encoding
        };

        let build_bloom_filter = opts.build_bloom_filter;
        let build_bitmap_index = opts.build_bitmap_index;

        let page_builder = Self::create_page_builder(&opts, encoding)?;
        let null_builder = if opts.is_nullable {
            Some(BitShufflePageBuilder::new(1, opts.page_size))
        } else {
            None
        };

        let codec: Option<Box<dyn crate::rowset::page::BlockCompressionCodec>> =
            match opts.compression {
                CompressionType::None => None,
                CompressionType::Lz4 => Some(Box::new(crate::rowset::page::Lz4Codec)),
                CompressionType::Zstd => Some(Box::new(crate::rowset::page::ZstdCodec::default())),
            };

        let hnsw_storage = if opts.field_type == FieldType::Vector && opts.build_hnsw {
            let dim = opts.fixed_len / 4; // Assuming f32
            Some(InMemoryVectorStorage::empty(dim))
        } else {
            None
        };

        let stats_logical_type = normalize_stats_logical_type(
            opts.logical_type
                .clone()
                .unwrap_or_else(|| field_type_to_logical_type(opts.field_type)),
        );
        let column_stats =
            ColumnStatistics::new(BaseStatistics::create_empty(stats_logical_type.clone()));

        Ok(ScalarColumnWriter {
            opts,
            writer,
            page_builder,
            null_builder,
            ordinal_index: OrdinalIndexWriter::new(),
            zonemap_index: ZoneMapIndexWriter::new(),
            bloom_filter_index: if build_bloom_filter {
                Some(BloomFilterIndexWriter::new(BloomFilterOptions::default()))
            } else {
                None
            },
            bitmap_index: if build_bitmap_index {
                Some(BitmapIndexWriter::new())
            } else {
                None
            },
            first_ordinal: 0,
            num_rows: 0,
            page_has_null: false,
            data_pages: Vec::new(),
            dict_page_pointer: None,
            encoding,
            codec,
            first_data_page: None,
            data_size: 0,
            hnsw_storage,
            column_stats,
            null_count: 0,
            stats_logical_type,
        })
    }

    fn create_page_builder(
        opts: &ColumnWriterOptions,
        encoding: EncodingType,
    ) -> Result<PageBuilderImpl> {
        let type_size = fixed_type_size(opts).unwrap_or(0);

        match encoding {
            EncodingType::Plain => {
                if opts.field_type == FieldType::Vector {
                    Ok(PageBuilderImpl::Plain(PlainPageBuilder::new(
                        opts.fixed_len,
                        opts.page_size,
                    )))
                } else if opts.field_type.is_variable_length() {
                    Ok(PageBuilderImpl::BinaryPlain(BinaryPlainPageBuilder::new(
                        opts.page_size,
                    )))
                } else {
                    Ok(PageBuilderImpl::Plain(PlainPageBuilder::new(
                        type_size,
                        opts.page_size,
                    )))
                }
            }
            EncodingType::BitShuffle => {
                if type_size == 0 {
                    return Err(paro_error::not_supported(
                        "BitShuffle encoding requires fixed-width type",
                    ));
                }
                Ok(PageBuilderImpl::BitShuffle(BitShufflePageBuilder::new(
                    type_size,
                    opts.page_size,
                )))
            }
            EncodingType::Rle => {
                // RLE is primarily for boolean types (1 byte)
                Ok(PageBuilderImpl::RleBool(RlePageBuilder::new(
                    1,
                    opts.page_size,
                )))
            }
            EncodingType::Dict => Ok(PageBuilderImpl::BinaryDict(BinaryDictPageBuilder::new(
                opts.page_size,
            ))),
            _ => Err(paro_error::not_supported(format!(
                "Encoding {:?} not yet implemented",
                encoding
            ))),
        }
    }
}

impl<W: DataWriter> ScalarColumnWriter<W> {
    /// Add fixed-width values to the page builder.
    fn add_fixed_values(&mut self, data: &[u8], count: u32) -> u32 {
        let fixed_width = fixed_type_size(&self.opts);
        match &mut self.page_builder {
            PageBuilderImpl::Plain(b) => b.add(data, count),
            PageBuilderImpl::BitShuffle(b) => b.add(data, count),
            PageBuilderImpl::RleBool(b) => {
                // Convert bytes to u8 slice for RLE
                let values: Vec<u8> = data.iter().take(count as usize).copied().collect();
                b.add(&values)
            }
            PageBuilderImpl::BinaryDict(builder) => {
                let Some(width) = fixed_width else {
                    return 0;
                };
                let available = (data.len() / width).min(count as usize);
                let mut added = 0usize;
                for value in data.chunks_exact(width).take(available) {
                    if !builder.add_slice(value) {
                        break;
                    }
                    added += 1;
                }
                added as u32
            }
            _ => 0,
        }
    }

    fn is_null_at(flags: Option<&[u8]>, row_idx: usize) -> bool {
        let Some(flags) = flags else {
            return false;
        };
        let byte_idx = row_idx / 8;
        let bit_idx = row_idx % 8;
        if byte_idx >= flags.len() {
            return false;
        }
        (flags[byte_idx] >> bit_idx) & 1 == 1
    }

    fn update_index_value(&mut self, value: &[u8], is_null: bool) {
        if let Some(ref mut bitmap) = self.bitmap_index {
            if is_null {
                bitmap.add_nulls(1);
            } else {
                bitmap.add_value(value);
            }
        }
        if let Some(ref mut bloom) = self.bloom_filter_index {
            if is_null {
                bloom.add_nulls(1);
            } else {
                bloom.add_value(value);
            }
        }
    }

    fn update_secondary_indexes_fixed(
        &mut self,
        data: &[u8],
        null_flags: Option<&[u8]>,
        input_row_offset: usize,
        count: u32,
        type_size: usize,
    ) {
        if self.bloom_filter_index.is_none() && self.bitmap_index.is_none() {
            return;
        }
        let total_bytes = count as usize * type_size;
        if data.len() < total_bytes {
            return;
        }
        for i in 0..count as usize {
            let row_idx = input_row_offset + i;
            let is_null = Self::is_null_at(null_flags, row_idx);
            let offset = i * type_size;
            let value = &data[offset..offset + type_size];
            self.update_index_value(value, is_null);
        }
    }

    fn decode_value(logical_type: &LogicalType, bytes: &[u8]) -> Result<Option<Value>> {
        let value = match logical_type {
            LogicalType::Boolean => {
                let v = bytes.first().copied().unwrap_or(0) != 0;
                Value::Boolean(v)
            }
            LogicalType::TinyInt => Value::TinyInt(bytes.first().copied().unwrap_or(0) as i8),
            LogicalType::SmallInt => {
                if bytes.len() < 2 {
                    return Err(paro_error::type_mismatch("SmallInt: insufficient bytes"));
                }
                Value::SmallInt(i16::from_le_bytes(bytes[..2].try_into().unwrap()))
            }
            LogicalType::Integer => {
                if bytes.len() < 4 {
                    return Err(paro_error::type_mismatch("Integer: insufficient bytes"));
                }
                Value::Integer(i32::from_le_bytes(bytes[..4].try_into().unwrap()))
            }
            LogicalType::BigInt => {
                if bytes.len() < 8 {
                    return Err(paro_error::type_mismatch("BigInt: insufficient bytes"));
                }
                Value::BigInt(i64::from_le_bytes(bytes[..8].try_into().unwrap()))
            }
            LogicalType::HugeInt => Value::HugeInt(decode_signed_wide_integer(bytes, "HugeInt")?),
            LogicalType::UTinyInt => Value::UTinyInt(bytes.first().copied().unwrap_or(0)),
            LogicalType::USmallInt => {
                if bytes.len() < 2 {
                    return Err(paro_error::type_mismatch("USmallInt: insufficient bytes"));
                }
                Value::USmallInt(u16::from_le_bytes(bytes[..2].try_into().unwrap()))
            }
            LogicalType::UInteger => {
                if bytes.len() < 4 {
                    return Err(paro_error::type_mismatch("UInteger: insufficient bytes"));
                }
                Value::UInteger(u32::from_le_bytes(bytes[..4].try_into().unwrap()))
            }
            LogicalType::UBigInt => {
                if bytes.len() < 8 {
                    return Err(paro_error::type_mismatch("UBigInt: insufficient bytes"));
                }
                Value::UBigInt(u64::from_le_bytes(bytes[..8].try_into().unwrap()))
            }
            LogicalType::UHugeInt => {
                Value::UHugeInt(decode_unsigned_wide_integer(bytes, "UHugeInt")?)
            }
            LogicalType::Float => {
                if bytes.len() < 4 {
                    return Err(paro_error::type_mismatch("Float: insufficient bytes"));
                }
                Value::Float(f32::from_le_bytes(bytes[..4].try_into().unwrap()))
            }
            LogicalType::Double => {
                if bytes.len() < 8 {
                    return Err(paro_error::type_mismatch("Double: insufficient bytes"));
                }
                Value::Double(f64::from_le_bytes(bytes[..8].try_into().unwrap()))
            }
            LogicalType::Date => {
                if bytes.len() < 4 {
                    return Err(paro_error::type_mismatch("Date: insufficient bytes"));
                }
                Value::Date(i32::from_le_bytes(bytes[..4].try_into().unwrap()))
            }
            LogicalType::Timestamp => {
                if bytes.len() < 8 {
                    return Err(paro_error::type_mismatch("Timestamp: insufficient bytes"));
                }
                Value::Timestamp(i64::from_le_bytes(bytes[..8].try_into().unwrap()))
            }
            LogicalType::TimestampTz => {
                if bytes.len() < 8 {
                    return Err(paro_error::type_mismatch("TimestampTz: insufficient bytes"));
                }
                Value::TimestampTz(i64::from_le_bytes(bytes[..8].try_into().unwrap()))
            }
            LogicalType::Time => {
                if bytes.len() < 8 {
                    return Err(paro_error::type_mismatch("Time: insufficient bytes"));
                }
                Value::Time(i64::from_le_bytes(bytes[..8].try_into().unwrap()))
            }
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb => Value::Varchar(String::from_utf8_lossy(bytes).to_string()),
            // Unsupported types for min/max stats
            LogicalType::Blob
            | LogicalType::Interval
            | LogicalType::Uuid
            | LogicalType::Null
            | LogicalType::Unknown
            | LogicalType::List(_)
            | LogicalType::Struct(_)
            | LogicalType::Array(_, _)
            | LogicalType::IntegerLiteral(_)
            | LogicalType::StringLiteral => {
                return Ok(None);
            }
            LogicalType::Decimal { .. } => {
                // Decimal stats are tracked as i128 (HugeInt) after normalization.
                Value::HugeInt(decode_signed_wide_integer(bytes, "Decimal")?)
            }
        };

        Ok(Some(value))
    }

    fn update_statistics_value(
        &mut self,
        value_bytes: &[u8],
        is_null: bool,
        hashes: &mut Vec<u64>,
    ) -> Result<()> {
        if is_null {
            self.null_count += 1;
            self.column_stats.statistics_mut().set_has_null_fast();
            return Ok(());
        }

        if let Some(value) = Self::decode_value(&self.stats_logical_type, value_bytes)? {
            self.column_stats.statistics_mut().observe_value(&value);
        } else {
            self.column_stats.statistics_mut().set_has_no_null_fast();
        }

        if self.column_stats.has_distinct_stats() {
            hashes.push(hash_bytes(value_bytes));
        }

        Ok(())
    }

    fn flush_distinct_hashes(&mut self, hashes: Vec<u64>) {
        if hashes.is_empty() {
            return;
        }
        self.column_stats
            .update_distinct_statistics(&hashes, hashes.len());
    }

    fn update_statistics_fixed(
        &mut self,
        data: &[u8],
        null_flags: Option<&[u8]>,
        input_row_offset: usize,
        count: u32,
        type_size: usize,
    ) -> Result<()> {
        if count == 0 {
            return Ok(());
        }

        let mut hashes = if self.column_stats.has_distinct_stats() {
            Vec::with_capacity(count as usize)
        } else {
            Vec::new()
        };

        for i in 0..count as usize {
            let row_idx = input_row_offset + i;
            let is_null = Self::is_null_at(null_flags, row_idx);
            let offset = i * type_size;
            if offset + type_size > data.len() {
                break;
            }
            let value_bytes = &data[offset..offset + type_size];
            self.update_statistics_value(value_bytes, is_null, &mut hashes)?;
        }

        self.flush_distinct_hashes(hashes);
        Ok(())
    }

    /// Write the current page to file.
    fn write_data_page(&mut self) -> Result<()> {
        if self.page_builder.count() == 0 {
            return Ok(());
        }

        let page_data = self.page_builder.finish()?;
        let num_values = self.page_builder.count() as u64;

        // Build null map if needed
        let mut null_data: Option<Bytes> = None;
        let nullmap_size = if let Some(ref mut null_builder) = self.null_builder {
            if self.page_has_null && null_builder.count() > 0 {
                let data = null_builder.finish()?;
                let size = data.len() as u32;
                null_data = Some(data);
                size
            } else {
                0
            }
        } else {
            0
        };

        let page_body = if let Some(null_data) = null_data {
            let mut combined =
                BytesMut::with_capacity(page_data.len().saturating_add(null_data.len()));
            combined.extend_from_slice(&page_data);
            combined.extend_from_slice(&null_data);
            combined.freeze()
        } else {
            page_data
        };

        // Create page footer
        let footer = PageFooter::Data(DataPageFooter {
            first_ordinal: self.first_ordinal,
            num_values,
            nullmap_size,
            corresponding_element_ordinal: None,
            format_version: self.opts.format_version,
            null_encoding: NullEncoding::BitShuffle,
        });

        // Write page with compression
        let codec_ref = self.codec.as_deref();
        let ptr = PageIO::compress_and_write_page(
            codec_ref,
            self.opts.min_space_saving,
            &mut self.writer,
            &page_body,
            &footer,
        )?;

        // Update indexes
        self.ordinal_index.add(self.first_ordinal, ptr);

        // Update zone map
        if let (Some(min), Some(max)) = (
            self.page_builder.get_first_value(),
            self.page_builder.get_last_value(),
        ) {
            self.zonemap_index.add(min, max, self.page_has_null);
        }

        if let Some(ref mut bloom) = self.bloom_filter_index {
            bloom.flush();
        }

        // Track data pages
        if self.first_data_page.is_none() {
            self.first_data_page = Some(ptr);
        }
        self.data_pages.push(ptr);
        self.data_size += ptr.size as u64;

        // Update state for next page
        self.first_ordinal += num_values;
        self.page_has_null = false;
        self.page_builder.reset();
        if let Some(ref mut null_builder) = self.null_builder {
            null_builder.reset();
        }

        Ok(())
    }

    /// Write ordinal index to file.
    fn write_ordinal_index(&mut self, base_offset: u64) -> Result<PagePointer> {
        let index_data = self.ordinal_index.finish_relocated(base_offset);
        let footer = PageFooter::Index(crate::rowset::page::IndexPageFooter {
            num_entries: self.ordinal_index.num_entries() as u32,
            page_type: crate::rowset::page::IndexPageType::Leaf,
        });

        let codec_ref = self.codec.as_deref();
        PageIO::compress_and_write_page(
            codec_ref,
            self.opts.min_space_saving,
            &mut self.writer,
            &index_data,
            &footer,
        )
    }

    /// Write zone map index to file.
    fn write_zone_map(&mut self) -> Result<PagePointer> {
        let index_data = self.zonemap_index.finish();
        let footer = PageFooter::Index(crate::rowset::page::IndexPageFooter {
            num_entries: self.zonemap_index.entries.len() as u32,
            page_type: crate::rowset::page::IndexPageType::Leaf,
        });

        let codec_ref = self.codec.as_deref();
        PageIO::compress_and_write_page(
            codec_ref,
            self.opts.min_space_saving,
            &mut self.writer,
            &index_data,
            &footer,
        )
    }

    /// Write bloom filter index to file (optional).
    fn write_bloom_filter_index(&mut self) -> Result<Option<PagePointer>> {
        let Some(ref mut bloom) = self.bloom_filter_index else {
            return Ok(None);
        };
        let index_data = bloom.finish();
        let footer = PageFooter::Index(IndexPageFooter {
            num_entries: bloom.num_filters() as u32,
            page_type: IndexPageType::Leaf,
        });
        let codec_ref = self.codec.as_deref();
        let ptr = PageIO::compress_and_write_page(
            codec_ref,
            self.opts.min_space_saving,
            &mut self.writer,
            &index_data,
            &footer,
        )?;
        Ok(Some(ptr))
    }

    /// Write bitmap index to file (optional).
    fn write_bitmap_index(&mut self) -> Result<Option<PagePointer>> {
        let Some(ref mut bitmap) = self.bitmap_index else {
            return Ok(None);
        };
        let index_data = bitmap.finish()?;
        let footer = PageFooter::Index(IndexPageFooter {
            num_entries: bitmap.num_values() as u32,
            page_type: IndexPageType::Leaf,
        });
        let codec_ref = self.codec.as_deref();
        let ptr = PageIO::compress_and_write_page(
            codec_ref,
            self.opts.min_space_saving,
            &mut self.writer,
            &index_data,
            &footer,
        )?;
        Ok(Some(ptr))
    }

    /// Write dictionary page if using dictionary encoding.
    fn write_dictionary_page(&mut self) -> Result<Option<PagePointer>> {
        if let Some(dict_data) = self.page_builder.get_dictionary_page() {
            let footer = PageFooter::Dict(crate::rowset::page::DictPageFooter {
                encoding: EncodingType::Plain.to_u8(),
            });

            let codec_ref = self.codec.as_deref();
            let ptr = PageIO::compress_and_write_page(
                codec_ref,
                self.opts.min_space_saving,
                &mut self.writer,
                &dict_data,
                &footer,
            )?;
            Ok(Some(ptr))
        } else {
            Ok(None)
        }
    }
}

impl<W: DataWriter + 'static> ScalarColumnWriter<W> {
    fn append_internal(
        &mut self,
        data: &[u8],
        null_flags: Option<&[u8]>,
        count: u32,
    ) -> Result<()> {
        if count == 0 {
            return Ok(());
        }

        let type_size = fixed_type_size(&self.opts);
        let mut offset = 0usize;
        let mut row_offset = 0usize;
        let mut remaining = count;

        while remaining > 0 {
            // Check if page is full
            if self.page_builder.is_page_full() {
                self.write_data_page()?;
            }

            let input_row_offset = row_offset;
            // Add values based on type
            let added = if let Some(ts) = type_size {
                // Fixed-width type
                let bytes_per_value = ts;
                let available_bytes = data.len() - offset;
                let max_values = (available_bytes / bytes_per_value) as u32;
                let to_add = std::cmp::min(remaining, max_values);

                // Base column pages, statistics, and predicate indexes retain
                // the SQL value exactly as supplied. Metric preprocessing is
                // an HNSW artifact concern and must never rewrite table data.
                let added = self.add_fixed_values(&data[offset..], to_add);
                let data_slice_len = added as usize * bytes_per_value;
                if data_slice_len > 0 && offset + data_slice_len <= data.len() {
                    let slice = &data[offset..offset + data_slice_len];
                    self.update_secondary_indexes_fixed(
                        slice,
                        null_flags,
                        input_row_offset,
                        added,
                        bytes_per_value,
                    );
                    self.update_statistics_fixed(
                        slice,
                        null_flags,
                        input_row_offset,
                        added,
                        bytes_per_value,
                    )?;

                    // Collect vectors for HNSW building
                    if let Some(ref mut storage) = self.hnsw_storage {
                        let dim = storage.vector_dim();
                        for i in 0..added as usize {
                            let start = i * bytes_per_value;
                            let vec_bytes = &slice[start..start + bytes_per_value];
                            let vec_f32 = unsafe {
                                std::slice::from_raw_parts(vec_bytes.as_ptr() as *const f32, dim)
                            };
                            storage.append(vec_f32);
                        }
                    }
                }
                offset += added as usize * bytes_per_value;
                added
            } else {
                // Variable-length type
                let (added, consumed) = self.add_variable_length_data(
                    &data[offset..],
                    remaining,
                    null_flags,
                    input_row_offset,
                )?;
                offset += consumed;
                added
            };

            if added == 0 {
                if self.page_builder.count() > 0 {
                    self.write_data_page()?;
                    continue;
                }
                break;
            }

            // Process null flags
            if let Some(flags) = null_flags {
                self.process_null_flags(flags, input_row_offset, added as usize);
            }

            self.num_rows += added as u64;
            row_offset += added as usize;
            remaining -= added;
        }

        Ok(())
    }

    fn finish_internal_with_base_offset(&mut self, base_offset: u64) -> Result<ColumnWriterMeta> {
        let meta_base_offset = 0;
        // Flush any remaining data
        self.write_data_page()?;

        // Write dictionary page if applicable
        self.dict_page_pointer = self.write_dictionary_page()?;

        // Write indexes
        let ordinal_index_pointer = self.write_ordinal_index(base_offset)?;
        let zonemap_index_pointer = self.write_zone_map()?;
        let bloom_filter_pointer = self.write_bloom_filter_index()?;
        let bitmap_index_pointer = self.write_bitmap_index()?;
        let hnsw = self.write_hnsw_index()?;
        let hnsw_index_pointer = hnsw.as_ref().map(|(pointer, _)| *pointer);
        let hnsw_index_statistics = hnsw.map(|(_, statistics)| statistics);

        let dict_size = self.dict_page_pointer.map(|p| p.size as u64).unwrap_or(0);
        let mut index_size =
            ordinal_index_pointer.size as u64 + zonemap_index_pointer.size as u64 + dict_size;
        if let Some(ptr) = bloom_filter_pointer {
            index_size += ptr.size as u64;
        }
        if let Some(ptr) = bitmap_index_pointer {
            index_size += ptr.size as u64;
        }
        if let Some(ptr) = hnsw_index_pointer {
            index_size += ptr.size as u64;
        }
        let total_mem_footprint = self.data_size + index_size;

        Ok(ColumnWriterMeta {
            column_id: self.opts.column_id,
            num_rows: self.num_rows,
            encoding: self.encoding,
            compression: self.opts.compression,
            data_page_pointer: self
                .first_data_page
                .map(|p| PagePointer::new(meta_base_offset + p.offset, p.size))
                .unwrap_or_default(),
            ordinal_index_pointer: PagePointer::new(
                meta_base_offset + ordinal_index_pointer.offset,
                ordinal_index_pointer.size,
            ),
            zonemap_index_pointer: PagePointer::new(
                meta_base_offset + zonemap_index_pointer.offset,
                zonemap_index_pointer.size,
            ),
            dict_page_pointer: self
                .dict_page_pointer
                .map(|p| PagePointer::new(meta_base_offset + p.offset, p.size)),
            bloom_filter_pointer: bloom_filter_pointer
                .map(|p| PagePointer::new(meta_base_offset + p.offset, p.size)),
            bitmap_index_pointer: bitmap_index_pointer
                .map(|p| PagePointer::new(meta_base_offset + p.offset, p.size)),
            hnsw_index_pointer: hnsw_index_pointer
                .map(|p| PagePointer::new(meta_base_offset + p.offset, p.size)),
            hnsw_index_statistics,
            data_size: self.data_size,
            index_size,
            total_mem_footprint,
            column_stats: self.column_stats.clone(),
            null_count: self.null_count,
        })
    }
}

impl<W: DataWriter + 'static> ColumnWriter for ScalarColumnWriter<W> {
    fn append(&mut self, data: &[u8], null_flags: Option<&[u8]>, count: u32) -> Result<()> {
        self.append_internal(data, null_flags, count)
    }

    fn finish_current_page(&mut self) -> Result<()> {
        self.write_data_page()
    }

    fn finish(&mut self) -> Result<ColumnWriterMeta> {
        self.finish_internal_with_base_offset(0)
    }

    fn finish_with_base_offset(&mut self, base_offset: u64) -> Result<ColumnWriterMeta> {
        self.finish_internal_with_base_offset(base_offset)
    }

    fn num_rows(&self) -> u64 {
        self.num_rows
    }

    fn get_data(&self) -> Bytes {
        self.writer.get_data()
    }
}

impl<W: DataWriter> ScalarColumnWriter<W> {
    /// Process null flags and add to null builder.
    fn process_null_flags(&mut self, flags: &[u8], input_row_offset: usize, count: usize) {
        if let Some(ref mut null_builder) = self.null_builder {
            for i in 0..count {
                let byte_idx = (input_row_offset + i) / 8;
                let bit_idx = (input_row_offset + i) % 8;
                let is_null = if byte_idx < flags.len() {
                    (flags[byte_idx] >> bit_idx) & 1 == 1
                } else {
                    false
                };

                if is_null {
                    self.page_has_null = true;
                }

                // Add null flag as single byte
                let null_byte = if is_null { 1u8 } else { 0u8 };
                null_builder.add_one(&[null_byte]);
            }
        }
    }

    /// Add variable-length data (length-prefixed format).
    fn add_variable_length_data(
        &mut self,
        data: &[u8],
        max_count: u32,
        null_flags: Option<&[u8]>,
        input_row_offset: usize,
    ) -> Result<(u32, usize)> {
        let mut offset = 0usize;
        let mut added = 0u32;
        let mut hashes = if self.column_stats.has_distinct_stats() {
            Vec::with_capacity(max_count as usize)
        } else {
            Vec::new()
        };

        while added < max_count && offset + 4 <= data.len() {
            let value_start = offset + 4;
            // Read length prefix
            let len = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;

            let value_end = value_start.checked_add(len).ok_or_else(|| {
                paro_error::invalid_input("Variable-length value offset overflow")
            })?;
            if value_end > data.len() {
                break;
            }

            let value = &data[value_start..value_end];
            if self.opts.field_type.requires_valid_utf8() {
                std::str::from_utf8(value).map_err(|_| {
                    paro_error::invalid_input(format!(
                        "Invalid UTF-8 for {:?} column {}",
                        self.opts.field_type, self.opts.column_id
                    ))
                })?;
            }
            let success = match &mut self.page_builder {
                PageBuilderImpl::BinaryPlain(b) => b.add_slice(value),
                PageBuilderImpl::BinaryDict(b) => b.add_slice(value),
                _ => false,
            };

            if !success {
                break;
            }

            let row_idx = input_row_offset + added as usize;
            let is_null = Self::is_null_at(null_flags, row_idx);
            self.update_index_value(value, is_null);
            self.update_statistics_value(value, is_null, &mut hashes)?;

            offset = value_end;
            added += 1;
        }

        self.flush_distinct_hashes(hashes);
        Ok((added, offset))
    }

    /// Write HNSW index to file (optional).
    fn write_hnsw_index(&mut self) -> Result<Option<(PagePointer, HnswIndexStatistics)>> {
        let Some(storage) = self.hnsw_storage.take() else {
            return Ok(None);
        };

        if storage.num_vectors() == 0 {
            return Ok(None);
        }

        let build_contract = self.opts.hnsw_build_contract.ok_or_else(|| {
            paro_error::internal("HNSW vector storage is missing its build contract")
        })?;
        let mut hnsw_builder =
            HnswBuilder::new().with_execution_policy(HnswBuildExecutionPolicy::parallel());
        if let Some(stop_check) = self.opts.hnsw_stop_check.clone() {
            hnsw_builder = hnsw_builder.with_stop_check(stop_check);
        }
        let index = hnsw_builder.build(Arc::new(storage), build_contract)?;

        let statistics = HnswIndexStatistics::collect(&index);
        let index_data = index.serialize()?;
        let footer = PageFooter::Index(IndexPageFooter {
            num_entries: index.graph.links.num_points() as u32,
            page_type: IndexPageType::Leaf,
        });

        let codec_ref = self.codec.as_deref();
        let ptr = PageIO::compress_and_write_page(
            codec_ref,
            self.opts.min_space_saving,
            &mut self.writer,
            &index_data,
            &footer,
        )?;

        Ok(Some((ptr, statistics)))
    }

    /// Consume the writer and return the underlying writer.
    pub fn into_inner(self) -> W {
        self.writer
    }
}

/// Factory function to create column writers.
impl ScalarColumnWriter<std::io::Cursor<Vec<u8>>> {
    /// Create a column writer that writes to an in-memory buffer.
    pub fn create_in_memory(opts: ColumnWriterOptions) -> Result<Self> {
        Self::new(opts, std::io::Cursor::new(Vec::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn first_data_footer(null_flags: Option<&[u8]>) -> DataPageFooter {
        let opts = ColumnWriterOptions::new(FieldType::Int, 0)
            .with_nullable(true)
            .with_compression(CompressionType::None);
        let mut writer = ScalarColumnWriter::new(opts, Cursor::new(Vec::new())).unwrap();
        let values: Vec<u8> = (0_i32..8).flat_map(i32::to_le_bytes).collect();
        writer.append(&values, null_flags, 8).unwrap();
        let meta = writer.finish().unwrap();
        let data = writer.get_data();
        let start = meta.data_page_pointer.offset as usize;
        let end = start + meta.data_page_pointer.size as usize;
        let (footer, _, _) = PageIO::parse_page_footer(&data[start..end], true).unwrap();
        match footer {
            PageFooter::Data(footer) => footer,
            _ => panic!("expected data page footer"),
        }
    }

    #[test]
    fn test_column_writer_options() {
        let opts = ColumnWriterOptions::new(FieldType::Int, 1)
            .with_nullable(false)
            .with_compression(CompressionType::Zstd)
            .with_page_size(128 * 1024);

        assert_eq!(opts.column_id, 1);
        assert!(!opts.is_nullable);
        assert_eq!(opts.compression, CompressionType::Zstd);
        assert_eq!(opts.page_size, 128 * 1024);
    }

    #[test]
    fn test_ordinal_index_writer() {
        let mut writer = OrdinalIndexWriter::new();
        writer.add(0, PagePointer::new(100, 1000));
        writer.add(1000, PagePointer::new(1100, 1000));
        writer.add(2000, PagePointer::new(2100, 1000));
        assert_eq!(writer.num_entries(), 3);

        let data = writer.finish();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_zonemap_index_writer() {
        let mut writer = ZoneMapIndexWriter::new();

        writer.add(
            Bytes::from_static(&[1, 0, 0, 0]),
            Bytes::from_static(&[10, 0, 0, 0]),
            false,
        );
        writer.add(
            Bytes::from_static(&[5, 0, 0, 0]),
            Bytes::from_static(&[20, 0, 0, 0]),
            true,
        );

        let data = writer.finish();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_scalar_column_writer_int() {
        let opts = ColumnWriterOptions::new(FieldType::Int, 0)
            .with_nullable(false)
            .with_compression(CompressionType::None);

        let buffer = Cursor::new(Vec::new());
        let mut writer = ScalarColumnWriter::new(opts, buffer).unwrap();

        // Write some i32 values
        let values: Vec<i32> = (0..100).collect();
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        writer.append(&bytes, None, 100).unwrap();
        assert_eq!(writer.num_rows(), 100);

        let meta = writer.finish().unwrap();
        assert_eq!(meta.num_rows, 100);
        assert_eq!(meta.column_id, 0);
    }

    #[test]
    fn temporal_column_statistics_preserve_logical_value_types() {
        let opts = ColumnWriterOptions::new(FieldType::Date, 0)
            .with_nullable(false)
            .with_compression(CompressionType::None);
        let mut writer = ScalarColumnWriter::new(opts, Cursor::new(Vec::new())).unwrap();
        let values = [8_035_i32, 10_591_i32];
        let bytes = values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect::<Vec<_>>();

        writer.append(&bytes, None, values.len() as u32).unwrap();
        let meta = writer.finish().unwrap();

        assert_eq!(
            meta.column_stats.statistics().min_value(),
            Some(Value::Date(8_035))
        );
        assert_eq!(
            meta.column_stats.statistics().max_value(),
            Some(Value::Date(10_591))
        );
    }

    #[test]
    fn test_scalar_column_writer_with_nulls() {
        let opts = ColumnWriterOptions::new(FieldType::Int, 0)
            .with_nullable(true)
            .with_compression(CompressionType::Lz4);

        let buffer = Cursor::new(Vec::new());
        let mut writer = ScalarColumnWriter::new(opts, buffer).unwrap();

        // Write values with some nulls
        let values: Vec<i32> = (0..10).collect();
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        // Null flags: positions 2, 5, 8 are null
        let null_flags: Vec<u8> = vec![0b00100100, 0b00000001];

        writer.append(&bytes, Some(&null_flags), 10).unwrap();

        let meta = writer.finish().unwrap();
        assert_eq!(meta.num_rows, 10);
    }

    #[test]
    fn nullable_page_omits_all_valid_null_map() {
        let footer = first_data_footer(Some(&[0]));
        assert_eq!(footer.nullmap_size, 0);
    }

    #[test]
    fn nullable_page_preserves_non_empty_null_map() {
        let footer = first_data_footer(Some(&[0b0000_0100]));
        assert!(footer.nullmap_size > 0);
    }

    #[test]
    fn test_scalar_column_writer_multiple_pages() {
        let opts = ColumnWriterOptions::new(FieldType::Int, 0)
            .with_nullable(false)
            .with_page_size(1024) // Small page size to force multiple pages
            .with_compression(CompressionType::None);

        let buffer = Cursor::new(Vec::new());
        let mut writer = ScalarColumnWriter::new(opts, buffer).unwrap();

        // Write enough values to span multiple pages
        let values: Vec<i32> = (0..1000).collect();
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        writer.append(&bytes, None, 1000).unwrap();

        let meta = writer.finish().unwrap();
        assert_eq!(meta.num_rows, 1000);
        assert!(meta.data_size > 0);
    }

    #[test]
    fn test_scalar_column_writer_varchar() {
        let opts = ColumnWriterOptions::new(FieldType::Varchar, 0)
            .with_nullable(false)
            .with_encoding(EncodingType::Dict);

        let buffer = Cursor::new(Vec::new());
        let mut writer = ScalarColumnWriter::new(opts, buffer).unwrap();

        // Write length-prefixed strings
        let strings = ["hello", "world", "hello", "test"];
        let mut data = Vec::new();
        for s in &strings {
            data.extend_from_slice(&(s.len() as u32).to_le_bytes());
            data.extend_from_slice(s.as_bytes());
        }

        writer.append(&data, None, 4).unwrap();

        let meta = writer.finish().unwrap();
        assert_eq!(meta.num_rows, 4);
    }

    #[test]
    fn utf8_storage_types_reject_invalid_bytes_before_encoding() {
        let encoded = [2, 0, 0, 0, 0xff, 0xfe];
        for field_type in [FieldType::Char, FieldType::Varchar, FieldType::Json] {
            let opts = ColumnWriterOptions::new(field_type, 7)
                .with_nullable(false)
                .with_encoding(EncodingType::Plain);
            let mut writer = ScalarColumnWriter::new(opts, Cursor::new(Vec::new())).unwrap();

            assert!(writer.append(&encoded, None, 1).is_err());
            assert_eq!(writer.num_rows(), 0);
        }
    }
}
