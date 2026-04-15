// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Segment Writer
//!
//! Writes Segment files with column data, indexes, and footer.
//!
//! ## Key Design
//!
//! - Writes column data using ColumnWriter for each column
//! - Builds ordinal and zonemap indexes per column
//! - Writes short key index for prefix-based lookup
//! - Finalizes with SegmentFooter containing all metadata
//!
//! ## File Layout
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │ Column 0 Data Pages                     │
//! │ Column 0 Ordinal Index Page             │
//! │ Column 0 ZoneMap Index Page             │
//! ├─────────────────────────────────────────┤
//! │ Column 1 Data Pages                     │
//! │ ...                                     │
//! ├─────────────────────────────────────────┤
//! │ Short Key Index Page (optional)         │
//! ├─────────────────────────────────────────┤
//! │ Segment Footer                          │
//! │ Footer Size (4 bytes)                   │
//! └─────────────────────────────────────────┘
//! ```

use super::segment::{Segment, SegmentOptions};
use super::segment_format::{ColumnMeta, SegmentFooter};
use crate::index::hnsw::{DistanceMetric, HnswBuildStopCheck, HnswConfig};
use crate::index::short_key::{ShortKeyFooter, ShortKeyIndexBuilder};
use crate::rowset::column::{ColumnWriter, ColumnWriterOptions, ScalarColumnWriter};
use crate::rowset::encoding::FieldType;
use crate::rowset::page::{CompressionType, PagePointer};
use crate::rowset::segment_statistics::{ColumnSegmentStatistics, SegmentStatistics};
use crate::rowset::RowsetId;
use crate::tablet::{ColumnId, TabletColumn, TabletSchemaRef};
use bytes::{Bytes, BytesMut};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, Write};
use std::path::{Path, PathBuf};

/// Options for SegmentWriter
#[derive(Debug, Clone)]
pub struct SegmentWriterOptions {
    /// Segment ID within the rowset
    pub segment_id: u32,
    /// Tablet ID for cache isolation
    pub tablet_id: u64,
    /// Rowset ID for cache isolation
    pub rowset_id: RowsetId,
    /// Rowset generation for cache isolation
    pub rowset_gen: u64,
    /// Target page size in bytes
    pub page_size: usize,
    /// Compression type
    pub compression: CompressionType,
    /// Whether to build short key index
    pub build_short_key_index: bool,
    /// Number of short key columns
    pub num_short_key_columns: usize,
    /// Columns to build bloom filter index
    pub bloom_filter_columns: Vec<ColumnId>,
    /// Columns to build bitmap index
    pub bitmap_index_columns: Vec<ColumnId>,
    /// Columns to build HNSW index
    pub hnsw_index_columns: Vec<ColumnId>,
    /// Whether to build HNSW index pages for configured HNSW columns.
    pub build_hnsw_indexes: bool,
    /// Optional HNSW config
    pub hnsw_config: Option<HnswConfig>,
    /// Optional HNSW distance metric
    pub hnsw_distance: Option<DistanceMetric>,
    /// Optional cooperative stop-check for HNSW build.
    pub hnsw_stop_check: Option<HnswBuildStopCheck>,
}

impl Default for SegmentWriterOptions {
    fn default() -> Self {
        Self {
            segment_id: 0,
            tablet_id: 0,
            rowset_id: 0,
            rowset_gen: 0,
            page_size: 256 * 1024, // 256KB
            compression: CompressionType::Lz4,
            build_short_key_index: true,
            num_short_key_columns: 3,
            bloom_filter_columns: Vec::new(),
            bitmap_index_columns: Vec::new(),
            hnsw_index_columns: Vec::new(),
            build_hnsw_indexes: true,
            hnsw_config: None,
            hnsw_distance: None,
            hnsw_stop_check: None,
        }
    }
}

impl SegmentWriterOptions {
    /// Create new options with segment ID
    pub fn new(segment_id: u32) -> Self {
        Self {
            segment_id,
            ..Default::default()
        }
    }

    /// Set rowset context for cache isolation.
    pub fn with_rowset_context(
        mut self,
        tablet_id: u64,
        rowset_id: RowsetId,
        rowset_gen: u64,
    ) -> Self {
        self.tablet_id = tablet_id;
        self.rowset_id = rowset_id;
        self.rowset_gen = rowset_gen;
        self
    }

    /// Set page size
    pub fn with_page_size(mut self, size: usize) -> Self {
        self.page_size = size;
        self
    }

    /// Set compression type
    pub fn with_compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }

    /// Set whether to build short key index
    pub fn with_short_key_index(mut self, build: bool) -> Self {
        self.build_short_key_index = build;
        self
    }

    /// Set number of short key columns
    pub fn with_num_short_key_columns(mut self, num: usize) -> Self {
        self.num_short_key_columns = num;
        self
    }

    /// Set columns to build bloom filter index
    pub fn with_bloom_filter_columns(mut self, columns: Vec<ColumnId>) -> Self {
        self.bloom_filter_columns = columns;
        self
    }

    /// Set columns to build bitmap index
    pub fn with_bitmap_index_columns(mut self, columns: Vec<ColumnId>) -> Self {
        self.bitmap_index_columns = columns;
        self
    }

    /// Set columns to build HNSW index
    pub fn with_hnsw_index_columns(mut self, columns: Vec<ColumnId>) -> Self {
        self.hnsw_index_columns = columns;
        self
    }

    /// Enable/disable building HNSW index pages during segment write.
    pub fn with_build_hnsw_indexes(mut self, build: bool) -> Self {
        self.build_hnsw_indexes = build;
        self
    }

    /// Set HNSW config
    pub fn with_hnsw_config(mut self, config: HnswConfig) -> Self {
        self.hnsw_config = Some(config);
        self
    }

    /// Set HNSW distance metric
    pub fn with_hnsw_distance(mut self, distance: DistanceMetric) -> Self {
        self.hnsw_distance = Some(distance);
        self
    }

    /// Set cooperative stop-check for HNSW build.
    pub fn with_hnsw_stop_check(mut self, stop_check: HnswBuildStopCheck) -> Self {
        self.hnsw_stop_check = Some(stop_check);
        self
    }
}

/// Column data to be written
pub struct ColumnData {
    /// Raw data bytes
    pub data: Bytes,
    /// Null flags (1 bit per value, 1 = null)
    pub null_flags: Option<Bytes>,
    /// Number of values
    pub num_values: u32,
}

impl ColumnData {
    /// Create new column data
    pub fn new(data: impl Into<Bytes>, num_values: u32) -> Self {
        Self {
            data: data.into(),
            null_flags: None,
            num_values,
        }
    }

    /// Create column data with null flags
    pub fn with_nulls(
        data: impl Into<Bytes>,
        null_flags: impl Into<Bytes>,
        num_values: u32,
    ) -> Self {
        Self {
            data: data.into(),
            null_flags: Some(null_flags.into()),
            num_values,
        }
    }
}

/// Internal state for a column being written
struct ColumnWriterState {
    /// Column ID
    column_id: ColumnId,
    /// Column writer
    writer: Box<dyn ColumnWriter>,
    /// Field type
    field_type: FieldType,
    /// Whether nullable
    is_nullable: bool,
}

/// SegmentWriter writes a complete Segment file
///
/// ## Usage
///
/// ```ignore
/// let opts = SegmentWriterOptions::new(0);
/// let mut writer = SegmentWriter::create(schema, "/path/to/segment.dat", opts)?;
///
/// // Append data chunk by chunk
/// writer.append_chunk(&column_data_vec)?;
///
/// // Finalize and get the segment
/// let segment = writer.finalize()?;
/// ```
pub struct SegmentWriter {
    /// Segment ID
    segment_id: u32,
    /// Tablet ID
    tablet_id: u64,
    /// Rowset ID
    rowset_id: RowsetId,
    /// Rowset generation
    rowset_gen: u64,
    /// Tablet schema
    schema: TabletSchemaRef,
    /// Output file path
    file_path: PathBuf,
    /// Writer options
    options: SegmentWriterOptions,
    /// Column writers (one per column)
    column_writers: Vec<ColumnWriterState>,
    /// Number of rows written
    num_rows: u64,
    /// Short key index entries (first N key columns concatenated)
    short_key_entries: Vec<ShortKeyEntry>,
    /// Whether the writer has been finalized
    finalized: bool,
    /// Buffered file writer
    file_writer: BufWriter<File>,
    /// Accumulated column metadata for vertical writing
    partial_column_metas: Vec<ColumnMeta>,
    /// Accumulated column statistics for vertical writing
    partial_column_stats: Vec<ColumnSegmentStatistics>,
    /// Whether this segment has sort keys
    has_key: bool,
}

/// Short key index entry
#[derive(Debug, Clone)]
struct ShortKeyEntry {
    /// Concatenated short key bytes
    key: Bytes,
}

impl SegmentWriter {
    /// Create a new SegmentWriter
    ///
    /// # Arguments
    /// * `schema` - Tablet schema defining columns
    /// * `file_path` - Output file path
    /// * `options` - Writer options
    pub fn create(
        schema: TabletSchemaRef,
        file_path: impl AsRef<Path>,
        options: SegmentWriterOptions,
    ) -> Result<Self> {
        let file_path = file_path.as_ref().to_path_buf();

        // Create parent directory if needed
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                paro_error::io_error(format!("Failed to create directory {:?}: {}", parent, e))
            })?;
        }

        // Open file for writing
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&file_path)
            .map_err(|e| {
                paro_error::io_error(format!(
                    "Failed to create segment file {:?}: {}",
                    file_path, e
                ))
            })?;

        let file_writer = BufWriter::new(file);

        Ok(Self {
            segment_id: options.segment_id,
            tablet_id: options.tablet_id,
            rowset_id: options.rowset_id,
            rowset_gen: options.rowset_gen,
            schema,
            file_path,
            options,
            column_writers: Vec::new(),
            num_rows: 0,
            short_key_entries: Vec::new(),
            finalized: false,
            file_writer,
            partial_column_metas: Vec::new(),
            partial_column_stats: Vec::new(),
            has_key: true,
        })
    }

    /// Initialize column writers for a subset of columns (vertical compaction)
    pub fn init_vertical(&mut self, column_ids: Vec<ColumnId>, has_key: bool) -> Result<()> {
        self.column_writers.clear();
        self.has_key = has_key;

        for cid in column_ids {
            let col = self.schema.column_by_id(cid).ok_or_else(|| {
                paro_error::invalid_input(format!("Column ID {} not found in schema", cid))
            })?;

            let field_type = Self::logical_type_to_field_type(&col.logical_type);
            let is_hnsw_col = self.options.hnsw_index_columns.contains(&col.id);
            let mut col_opts = ColumnWriterOptions::new(field_type, col.id)
                .with_logical_type(col.logical_type.clone())
                .with_nullable(col.is_nullable)
                .with_page_size(self.options.page_size)
                .with_compression(self.options.compression)
                .with_bloom_filter(self.options.bloom_filter_columns.contains(&col.id))
                .with_bitmap_index(self.options.bitmap_index_columns.contains(&col.id))
                .with_hnsw(
                    self.options.build_hnsw_indexes && is_hnsw_col,
                    is_hnsw_col,
                    self.options.hnsw_config,
                    self.options.hnsw_distance,
                )
                .with_hnsw_stop_check(self.options.hnsw_stop_check.clone());

            if field_type == FieldType::Vector {
                if let LogicalType::Array(_, dim) = &col.logical_type {
                    col_opts = col_opts
                        .with_fixed_len(dim * 4)
                        .with_compression(CompressionType::None)
                        .with_page_size(1024 * 1024 * 1024); // 1GB page for vectors to keep them contiguous
                }
            }

            let writer = ScalarColumnWriter::create_in_memory(col_opts)?;

            self.column_writers.push(ColumnWriterState {
                column_id: col.id,
                writer: Box::new(writer),
                field_type,
                is_nullable: col.is_nullable,
            });
        }

        Ok(())
    }

    /// Initialize column writers (lazy initialization)
    fn init_column_writers(&mut self) -> Result<()> {
        if !self.column_writers.is_empty() || !self.partial_column_metas.is_empty() {
            return Ok(());
        }

        let all_col_ids: Vec<ColumnId> = self.schema.columns().iter().map(|c| c.id).collect();
        self.init_vertical(all_col_ids, true)
    }

    /// Convert LogicalType to FieldType
    fn logical_type_to_field_type(logical_type: &LogicalType) -> FieldType {
        match logical_type {
            LogicalType::Boolean => FieldType::Boolean,
            LogicalType::TinyInt => FieldType::TinyInt,
            LogicalType::SmallInt => FieldType::SmallInt,
            LogicalType::Integer => FieldType::Int,
            LogicalType::BigInt => FieldType::BigInt,
            LogicalType::HugeInt => FieldType::LargeInt,
            LogicalType::Uuid => FieldType::LargeInt,
            LogicalType::UTinyInt => FieldType::TinyInt,
            LogicalType::USmallInt => FieldType::SmallInt,
            LogicalType::UInteger => FieldType::Int,
            LogicalType::UBigInt => FieldType::BigInt,
            LogicalType::UHugeInt => FieldType::LargeInt,
            LogicalType::Float => FieldType::Float,
            LogicalType::Double => FieldType::Double,
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery => FieldType::Varchar,
            LogicalType::Json | LogicalType::Jsonb => FieldType::Json,
            LogicalType::Date => FieldType::Date,
            LogicalType::Timestamp | LogicalType::TimestampTz | LogicalType::Time => {
                FieldType::DateTime
            }
            LogicalType::Decimal { .. } => FieldType::Decimal,
            LogicalType::Blob => FieldType::Binary,
            LogicalType::Interval => FieldType::LargeInt,
            LogicalType::Array(inner, _) if matches!(**inner, LogicalType::Float) => {
                FieldType::Vector
            }
            LogicalType::List(_) => FieldType::Binary,
            _ => FieldType::Binary, // Default fallback
        }
    }

    /// Append a chunk of data for all columns
    ///
    /// # Arguments
    /// * `columns` - Vector of column data, one per schema column
    ///
    /// # Returns
    /// Number of rows appended
    pub fn append_chunk(&mut self, columns: &[ColumnData]) -> Result<u64> {
        if self.finalized {
            return Err(paro_error::internal("SegmentWriter already finalized"));
        }

        self.init_column_writers()?;

        if columns.len() != self.column_writers.len() {
            return Err(paro_error::invalid_input(format!(
                "Expected {} columns, got {}",
                self.column_writers.len(),
                columns.len()
            )));
        }

        // Verify all columns have the same number of values
        let num_values = columns.first().map(|c| c.num_values).unwrap_or(0);
        for (i, col) in columns.iter().enumerate() {
            if col.num_values != num_values {
                return Err(paro_error::invalid_input(format!(
                    "Column {} has {} values, expected {}",
                    i, col.num_values, num_values
                )));
            }
        }

        if num_values == 0 {
            return Ok(0);
        }

        // Build short key entries before writing (only if we have key columns in this pass)
        if self.options.build_short_key_index && self.has_key {
            // Check if any of the columns in this pass are key columns
            let has_short_key_cols = self.column_writers.iter().any(|cw| {
                self.schema
                    .column_by_id(cw.column_id)
                    .map(|c| c.is_key)
                    .unwrap_or(false)
            });

            if has_short_key_cols {
                self.build_short_key_entries(columns, num_values)?;
            }
        }

        // Append data to each column writer
        for (i, col_data) in columns.iter().enumerate() {
            let writer_state = &mut self.column_writers[i];
            writer_state.writer.append(
                &col_data.data,
                col_data.null_flags.as_deref(),
                col_data.num_values,
            )?;
        }

        // Only update segment-level num_rows if this is the first column group (or matching)
        if self.partial_column_metas.is_empty() {
            self.num_rows += num_values as u64;
        }

        Ok(num_values as u64)
    }

    /// Build short key index entries from the chunk
    fn build_short_key_entries(&mut self, columns: &[ColumnData], _num_values: u32) -> Result<()> {
        let num_short_key_cols = self
            .options
            .num_short_key_columns
            .min(self.schema.num_key_columns());
        if num_short_key_cols == 0 {
            return Ok(());
        }

        // For simplicity, we add one entry per chunk (first row of chunk)
        // In production, we'd add entries at regular intervals
        let mut key_buf = BytesMut::new();

        for (col, col_data) in self
            .schema
            .columns()
            .iter()
            .zip(columns.iter())
            .take(num_short_key_cols)
        {
            // Extract first value from column data
            let first_value = self.extract_first_value(col, &col_data.data)?;
            key_buf.extend_from_slice(&first_value);
        }

        self.short_key_entries.push(ShortKeyEntry {
            key: key_buf.freeze(),
        });

        Ok(())
    }

    /// Extract the first value from column data
    fn extract_first_value(&self, col: &TabletColumn, data: &Bytes) -> Result<Bytes> {
        let type_size = col.type_size();
        if type_size > 0 && data.len() >= type_size {
            Ok(data.slice(0..type_size))
        } else if data.len() >= 4 {
            // Variable length: read length prefix
            let len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
            if data.len() >= 4 + len {
                Ok(data.slice(4..4 + len))
            } else {
                Ok(Bytes::new())
            }
        } else {
            Ok(Bytes::new())
        }
    }

    /// Finalize the segment and return the Segment instance
    ///
    /// This writes all column data, indexes, and footer to the file.
    fn get_current_offset(&mut self) -> Result<u64> {
        self.file_writer
            .stream_position()
            .map_err(|e| paro_error::io_error(format!("Failed to get file position: {}", e)))
    }

    fn do_finalize_columns(&mut self) -> Result<()> {
        // Write column data to file
        for i in 0..self.column_writers.len() {
            // Ensure 8-byte alignment for column data (required for mmap vector access)
            let current_pos = self
                .file_writer
                .stream_position()
                .map_err(|e| paro_error::io_error(e.to_string()))?;
            if current_pos % 8 != 0 {
                let padding = 8 - (current_pos % 8);
                self.file_writer
                    .write_all(&vec![0u8; padding as usize])
                    .map_err(|e| paro_error::io_error(e.to_string()))?;
            }

            let current_offset = self
                .file_writer
                .stream_position()
                .map_err(|e| paro_error::io_error(e.to_string()))?;
            let writer_state = &mut self.column_writers[i];
            let column_id = writer_state.column_id;
            let field_type = writer_state.field_type;
            let is_nullable = writer_state.is_nullable;
            let writer_meta = writer_state
                .writer
                .finish_with_base_offset(current_offset)?;

            // Write column data to file
            let col_data = writer_state.writer.get_data();

            self.file_writer.write_all(&col_data).map_err(|e| {
                paro_error::io_error(format!("Failed to write column {} data: {}", column_id, e))
            })?;

            // Create ColumnMeta with updated offsets
            let col_meta = ColumnMeta {
                column_id,
                num_rows: writer_meta.num_rows,
                encoding: writer_meta.encoding,
                compression: writer_meta.compression,
                data_page_pointer: PagePointer::new(
                    current_offset + writer_meta.data_page_pointer.offset,
                    writer_meta.data_page_pointer.size,
                ),
                ordinal_index_pointer: PagePointer::new(
                    current_offset + writer_meta.ordinal_index_pointer.offset,
                    writer_meta.ordinal_index_pointer.size,
                ),
                zonemap_index_pointer: PagePointer::new(
                    current_offset + writer_meta.zonemap_index_pointer.offset,
                    writer_meta.zonemap_index_pointer.size,
                ),
                dict_page_pointer: writer_meta
                    .dict_page_pointer
                    .map(|p| PagePointer::new(current_offset + p.offset, p.size)),
                bloom_filter_pointer: writer_meta
                    .bloom_filter_pointer
                    .map(|p| PagePointer::new(current_offset + p.offset, p.size)),
                bitmap_index_pointer: writer_meta
                    .bitmap_index_pointer
                    .map(|p| PagePointer::new(current_offset + p.offset, p.size)),
                hnsw_index_pointer: writer_meta
                    .hnsw_index_pointer
                    .map(|p| PagePointer::new(current_offset + p.offset, p.size)),
                sparse_index_pointer: None,
                fulltext_index_pointer: None,
                field_type,
                is_nullable,
                total_mem_footprint: writer_meta.total_mem_footprint,
                column_stats: Some(writer_meta.column_stats.clone()),
                null_count: Some(writer_meta.null_count),
            };

            self.partial_column_metas.push(col_meta);
            self.partial_column_stats.push(ColumnSegmentStatistics::new(
                column_id,
                writer_meta.column_stats.clone(),
                writer_meta.null_count,
                writer_meta.num_rows,
            ));
        }

        self.column_writers.clear();
        Ok(())
    }

    /// Finalize columns data and index (used for vertical compaction)
    pub fn finalize_columns(&mut self) -> Result<()> {
        self.do_finalize_columns()
    }

    /// Finalize the segment and return the Segment instance (used for horizontal compaction)
    pub fn finalize(mut self) -> Result<Segment> {
        if self.finalized {
            return Err(paro_error::internal("SegmentWriter already finalized"));
        }

        self.init_column_writers()?;
        self.do_finalize_columns()?;
        self.finalize_footer()
    }

    /// Finalize segment footer and return Segment (used for vertical compaction)
    pub fn finalize_footer(mut self) -> Result<Segment> {
        if self.finalized {
            return Err(paro_error::internal("SegmentWriter already finalized"));
        }
        self.finalized = true;

        // Write short key index if enabled
        let (short_key_index_pointer, short_key_index_footer) =
            if self.options.build_short_key_index && !self.short_key_entries.is_empty() {
                let (index_data, footer) = self.serialize_short_key_index();
                let offset = self.get_current_offset()?;
                self.file_writer.write_all(&index_data).map_err(|e| {
                    paro_error::io_error(format!("Failed to write short key index: {}", e))
                })?;
                (
                    Some(PagePointer::new(offset, index_data.len() as u32)),
                    Some(footer),
                )
            } else {
                (None, None)
            };

        // Create and write footer
        let mut footer = SegmentFooter::new(self.num_rows, self.partial_column_metas.clone());
        footer.short_key_index_pointer = short_key_index_pointer;
        footer.short_key_index_footer = short_key_index_footer;

        let footer_bytes = footer.serialize();
        self.file_writer
            .write_all(&footer_bytes)
            .map_err(|e| paro_error::io_error(format!("Failed to write segment footer: {}", e)))?;

        // Write footer size (including the size field itself)
        let footer_size = (footer_bytes.len() + 4) as u32;
        self.file_writer
            .write_all(&footer_size.to_le_bytes())
            .map_err(|e| {
                paro_error::io_error(format!("Failed to write segment footer size: {}", e))
            })?;

        self.file_writer
            .flush()
            .map_err(|e| paro_error::io_error(format!("Failed to flush segment file: {}", e)))?;

        // Re-open from disk so secondary indexes (HNSW/Bitmap/Bloom/...) are eagerly available
        // to readers immediately after commit.
        let mut segment = Segment::open(
            self.segment_id,
            &self.file_path,
            self.schema.clone(),
            SegmentOptions::default().with_compression(self.options.compression),
            self.tablet_id,
            self.rowset_id,
            self.rowset_gen,
        )?;

        let mut stats = SegmentStatistics::new(self.num_rows);
        for col_stats in self.partial_column_stats {
            stats.add_column(col_stats);
        }
        segment.set_statistics(stats);

        Ok(segment)
    }

    /// Serialize short key index to bytes
    fn serialize_short_key_index(&self) -> (Vec<u8>, ShortKeyFooter) {
        let mut builder = ShortKeyIndexBuilder::new(self.segment_id, 1);
        for entry in &self.short_key_entries {
            builder
                .add_item(&entry.key)
                .expect("short key builder cannot fail");
        }
        let (body, footer) = builder
            .finalize(self.num_rows as u32)
            .expect("short key builder cannot fail");
        (body.to_vec(), footer)
    }

    /// Get the number of rows written so far
    pub fn num_rows(&self) -> u64 {
        self.num_rows
    }

    /// Get the segment ID
    pub fn segment_id(&self) -> u32 {
        self.segment_id
    }

    /// Get the file path
    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    /// Check if the writer has been finalized
    pub fn is_finalized(&self) -> bool {
        self.finalized
    }
}

/// Builder for creating SegmentWriter with fluent API
pub struct SegmentWriterBuilder {
    schema: TabletSchemaRef,
    file_path: PathBuf,
    options: SegmentWriterOptions,
}

impl SegmentWriterBuilder {
    /// Create a new builder
    pub fn new(schema: TabletSchemaRef, file_path: impl AsRef<Path>) -> Self {
        Self {
            schema,
            file_path: file_path.as_ref().to_path_buf(),
            options: SegmentWriterOptions::default(),
        }
    }

    /// Set segment ID
    pub fn segment_id(mut self, id: u32) -> Self {
        self.options.segment_id = id;
        self
    }

    /// Set page size
    pub fn page_size(mut self, size: usize) -> Self {
        self.options.page_size = size;
        self
    }

    /// Set compression type
    pub fn compression(mut self, compression: CompressionType) -> Self {
        self.options.compression = compression;
        self
    }

    /// Set whether to build short key index
    pub fn short_key_index(mut self, build: bool) -> Self {
        self.options.build_short_key_index = build;
        self
    }

    /// Set columns to build bloom filter index
    pub fn bloom_filter_columns(mut self, columns: Vec<ColumnId>) -> Self {
        self.options.bloom_filter_columns = columns;
        self
    }

    /// Set columns to build bitmap index
    pub fn bitmap_index_columns(mut self, columns: Vec<ColumnId>) -> Self {
        self.options.bitmap_index_columns = columns;
        self
    }

    /// Build the SegmentWriter
    pub fn build(self) -> Result<SegmentWriter> {
        SegmentWriter::create(self.schema, self.file_path, self.options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tablet::tablet_schema::{KeysType, TabletColumn, TabletSchema};
    use tempfile::TempDir;

    fn create_simple_schema() -> TabletSchemaRef {
        let columns = vec![
            TabletColumn::new(0, "col0", LogicalType::Integer),
            TabletColumn::new(1, "col1", LogicalType::Integer),
        ];
        std::sync::Arc::new(TabletSchema::new(1, columns, KeysType::DuplicateKeys).unwrap())
    }

    #[test]
    fn test_segment_writer_options() {
        let opts = SegmentWriterOptions::new(5)
            .with_page_size(128 * 1024)
            .with_compression(CompressionType::Zstd)
            .with_short_key_index(false)
            .with_num_short_key_columns(2)
            .with_bloom_filter_columns(vec![1, 2])
            .with_bitmap_index_columns(vec![3]);

        assert_eq!(opts.segment_id, 5);
        assert_eq!(opts.page_size, 128 * 1024);
        assert_eq!(opts.compression, CompressionType::Zstd);
        assert!(!opts.build_short_key_index);
        assert_eq!(opts.num_short_key_columns, 2);
        assert_eq!(opts.bloom_filter_columns, vec![1, 2]);
        assert_eq!(opts.bitmap_index_columns, vec![3]);
    }

    #[test]
    fn test_column_data_new() {
        let data = vec![1u8, 2, 3, 4];
        let col_data = ColumnData::new(data.clone(), 1);

        assert_eq!(col_data.data.as_ref(), &data[..]);
        assert!(col_data.null_flags.is_none());
        assert_eq!(col_data.num_values, 1);
    }

    #[test]
    fn test_column_data_with_nulls() {
        let data = vec![1u8, 2, 3, 4];
        let nulls = vec![0b00000001u8];
        let col_data = ColumnData::with_nulls(data.clone(), nulls.clone(), 1);

        assert_eq!(col_data.data.as_ref(), &data[..]);
        assert!(col_data.null_flags.is_some());
        assert_eq!(col_data.null_flags.as_ref().unwrap().as_ref(), &nulls[..]);
        assert_eq!(col_data.num_values, 1);
    }

    #[test]
    fn test_segment_writer_create() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_segment.dat");
        let schema = create_simple_schema();
        let opts = SegmentWriterOptions::new(0);

        let writer = SegmentWriter::create(schema, &file_path, opts).unwrap();

        assert_eq!(writer.segment_id(), 0);
        assert_eq!(writer.num_rows(), 0);
        assert!(!writer.is_finalized());
        assert_eq!(writer.file_path(), file_path);
    }

    #[test]
    fn test_segment_writer_builder() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_segment.dat");
        let schema = create_simple_schema();

        let writer = SegmentWriterBuilder::new(schema, &file_path)
            .segment_id(3)
            .page_size(64 * 1024)
            .compression(CompressionType::None)
            .short_key_index(false)
            .build()
            .unwrap();

        assert_eq!(writer.segment_id(), 3);
    }

    #[test]
    fn test_segment_writer_empty_finalize() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_segment.dat");
        let schema = create_simple_schema();
        let opts = SegmentWriterOptions::new(0).with_short_key_index(false);

        let writer = SegmentWriter::create(schema, &file_path, opts).unwrap();
        let segment = writer.finalize().unwrap();

        assert_eq!(segment.segment_id(), 0);
        assert_eq!(segment.num_rows(), 0);
        assert_eq!(segment.num_columns(), 2);
    }

    #[test]
    fn test_segment_writer_append_and_finalize() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_segment.dat");
        let schema = create_simple_schema();
        let opts = SegmentWriterOptions::new(0)
            .with_short_key_index(false)
            .with_compression(CompressionType::None);

        let mut writer = SegmentWriter::create(schema, &file_path, opts).unwrap();

        // Create test data: 10 rows of two i32 columns
        let col0_data: Vec<u8> = (0i32..10).flat_map(|v| v.to_le_bytes()).collect();
        let col1_data: Vec<u8> = (100i32..110).flat_map(|v| v.to_le_bytes()).collect();

        let columns = vec![
            ColumnData::new(col0_data, 10),
            ColumnData::new(col1_data, 10),
        ];

        let rows_added = writer.append_chunk(&columns).unwrap();
        assert_eq!(rows_added, 10);
        assert_eq!(writer.num_rows(), 10);

        let segment = writer.finalize().unwrap();
        assert_eq!(segment.num_rows(), 10);
        assert_eq!(segment.num_columns(), 2);
    }

    #[test]
    fn test_segment_writer_multiple_chunks() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_segment.dat");
        let schema = create_simple_schema();
        let opts = SegmentWriterOptions::new(0)
            .with_short_key_index(false)
            .with_compression(CompressionType::None);

        let mut writer = SegmentWriter::create(schema, &file_path, opts).unwrap();

        // Append multiple chunks
        for batch in 0..5 {
            let col0_data: Vec<u8> = (0i32..100)
                .flat_map(|v| (v + batch * 100).to_le_bytes())
                .collect();
            let col1_data: Vec<u8> = (0i32..100)
                .flat_map(|v| (v + batch * 1000).to_le_bytes())
                .collect();

            let columns = vec![
                ColumnData::new(col0_data, 100),
                ColumnData::new(col1_data, 100),
            ];

            writer.append_chunk(&columns).unwrap();
        }

        assert_eq!(writer.num_rows(), 500);

        let segment = writer.finalize().unwrap();
        assert_eq!(segment.num_rows(), 500);
    }

    #[test]
    fn test_segment_writer_column_count_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_segment.dat");
        let schema = create_simple_schema(); // 2 columns
        let opts = SegmentWriterOptions::new(0).with_short_key_index(false);

        let mut writer = SegmentWriter::create(schema, &file_path, opts).unwrap();

        // Try to append only 1 column (should fail)
        let col0_data: Vec<u8> = (0i32..10).flat_map(|v| v.to_le_bytes()).collect();
        let columns = vec![ColumnData::new(col0_data, 10)];

        let result = writer.append_chunk(&columns);
        assert!(result.is_err());
    }

    #[test]
    fn test_segment_writer_row_count_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_segment.dat");
        let schema = create_simple_schema();
        let opts = SegmentWriterOptions::new(0).with_short_key_index(false);

        let mut writer = SegmentWriter::create(schema, &file_path, opts).unwrap();

        // Columns with different row counts
        let col0_data: Vec<u8> = (0i32..10).flat_map(|v| v.to_le_bytes()).collect();
        let col1_data: Vec<u8> = (0i32..5).flat_map(|v| v.to_le_bytes()).collect();

        let columns = vec![
            ColumnData::new(col0_data, 10),
            ColumnData::new(col1_data, 5), // Different count!
        ];

        let result = writer.append_chunk(&columns);
        assert!(result.is_err());
    }

    #[test]
    fn test_segment_writer_double_finalize() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_segment.dat");
        let schema = create_simple_schema();
        let opts = SegmentWriterOptions::new(0).with_short_key_index(false);

        let writer = SegmentWriter::create(schema, &file_path, opts).unwrap();
        let _segment = writer.finalize().unwrap();

        // Can't finalize twice because finalize consumes self
        // This is enforced by Rust's ownership system
    }

    #[test]
    fn test_logical_type_to_field_type() {
        assert_eq!(
            SegmentWriter::logical_type_to_field_type(&LogicalType::Boolean),
            FieldType::Boolean
        );
        assert_eq!(
            SegmentWriter::logical_type_to_field_type(&LogicalType::Integer),
            FieldType::Int
        );
        assert_eq!(
            SegmentWriter::logical_type_to_field_type(&LogicalType::BigInt),
            FieldType::BigInt
        );
        assert_eq!(
            SegmentWriter::logical_type_to_field_type(&LogicalType::Varchar),
            FieldType::Varchar
        );
        assert_eq!(
            SegmentWriter::logical_type_to_field_type(&LogicalType::Double),
            FieldType::Double
        );
    }

    #[test]
    fn test_segment_writer_with_nulls() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_segment.dat");

        // Create schema with nullable columns
        let columns = vec![
            TabletColumn::new(0, "col0", LogicalType::Integer).with_nullable(true),
            TabletColumn::new(1, "col1", LogicalType::Integer).with_nullable(true),
        ];
        let schema =
            std::sync::Arc::new(TabletSchema::new(1, columns, KeysType::DuplicateKeys).unwrap());

        let opts = SegmentWriterOptions::new(0)
            .with_short_key_index(false)
            .with_compression(CompressionType::None);

        let mut writer = SegmentWriter::create(schema, &file_path, opts).unwrap();

        // Create data with nulls
        let col0_data: Vec<u8> = (0i32..8).flat_map(|v| v.to_le_bytes()).collect();
        let col1_data: Vec<u8> = (100i32..108).flat_map(|v| v.to_le_bytes()).collect();

        // Null flags: positions 2 and 5 are null
        let null_flags = vec![0b00100100u8];

        let columns = vec![
            ColumnData::with_nulls(col0_data, null_flags.clone(), 8),
            ColumnData::with_nulls(col1_data, null_flags, 8),
        ];

        writer.append_chunk(&columns).unwrap();
        let segment = writer.finalize().unwrap();

        assert_eq!(segment.num_rows(), 8);
    }

    #[test]
    fn test_segment_writer_vertical() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_segment_vertical.dat");
        let schema = create_simple_schema();
        let opts = SegmentWriterOptions::new(0)
            .with_short_key_index(true)
            .with_compression(CompressionType::None);

        let mut writer = SegmentWriter::create(schema.clone(), &file_path, opts).unwrap();

        // 1. Write first column group
        writer.init_vertical(vec![0], true).unwrap();
        let col0_data: Vec<u8> = (0i32..10).flat_map(|v| v.to_le_bytes()).collect();
        let columns0 = vec![ColumnData::new(col0_data, 10)];
        writer.append_chunk(&columns0).unwrap();
        writer.finalize_columns().unwrap();

        // 2. Write second column group
        writer.init_vertical(vec![1], false).unwrap();
        let col1_data: Vec<u8> = (100i32..110).flat_map(|v| v.to_le_bytes()).collect();
        let columns1 = vec![ColumnData::new(col1_data, 10)];
        writer.append_chunk(&columns1).unwrap();
        writer.finalize_columns().unwrap();

        // 3. Finalize footer
        let segment = writer.finalize_footer().unwrap();

        assert_eq!(segment.num_rows(), 10);
        assert_eq!(segment.num_columns(), 2);
    }
}
