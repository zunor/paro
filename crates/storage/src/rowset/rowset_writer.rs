// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # RowsetWriter
//!
//! Rowset writer that coordinates multiple Segment writes.
//!
//! ## Key Design
//!
//! - Coordinates writing data across multiple Segments
//! - Automatically creates new Segments when size threshold is reached
//! - Tracks statistics (rows, data size, index size) across all Segments
//! - Produces a complete Rowset with all Segments on finalization
//!
//! ## Usage
//!
//! ```ignore
//! let context = RowsetWriterContext::new(schema, tablet_id, version, rowset_path);
//! let mut writer = RowsetWriter::create(context)?;
//!
//! // Add data chunks
//! writer.add_chunk(&column_data_vec)?;
//!
//! // Optionally flush current segment
//! writer.flush_segment()?;
//!
//! // Build the final Rowset
//! let rowset = writer.build()?;
//! ```

use super::rowset::{Rowset, RowsetSharedPtr};
use super::rowset_meta::{generate_rowset_id, RowsetId, RowsetMeta, RowsetState, SegmentsOverlap};
use super::segment::{ColumnData, Segment, SegmentWriter, SegmentWriterOptions};
use crate::tablet::{ColumnId, TabletSchemaRef, Version};
use paro_common::error::{self as paro_error, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Default segment size threshold (256 MB)
const DEFAULT_SEGMENT_SIZE_THRESHOLD: u64 = 256 * 1024 * 1024;

/// Default maximum rows per segment (1 million)
const DEFAULT_MAX_ROWS_PER_SEGMENT: u64 = 1_000_000;

/// Context for creating a RowsetWriter
#[derive(Debug, Clone)]
pub struct RowsetWriterContext {
    /// Rowset ID (auto-generated if not specified)
    pub rowset_id: RowsetId,
    /// Tablet ID
    pub tablet_id: u64,
    /// Version for this rowset
    pub version: Version,
    /// Tablet schema
    pub schema: TabletSchemaRef,
    /// Rowset data directory path
    pub rowset_path: PathBuf,
    /// Segment size threshold in bytes
    pub segment_size_threshold: u64,
    /// Maximum rows per segment
    pub max_rows_per_segment: u64,
    /// Compression type for segments
    pub compression: super::page::CompressionType,
    /// Whether to build short key index
    pub build_short_key_index: bool,
    /// Number of short key columns
    pub num_short_key_columns: usize,
    /// Whether to build HNSW index pages during segment write.
    pub build_hnsw_indexes: bool,
    /// Optional subset of columns to write for partial-row rowsets.
    pub write_column_ids: Option<Vec<ColumnId>>,
}

impl RowsetWriterContext {
    /// Create a new RowsetWriterContext
    pub fn new(
        schema: TabletSchemaRef,
        tablet_id: u64,
        version: Version,
        rowset_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            rowset_id: generate_rowset_id(),
            tablet_id,
            version,
            schema,
            rowset_path: rowset_path.into(),
            segment_size_threshold: DEFAULT_SEGMENT_SIZE_THRESHOLD,
            max_rows_per_segment: DEFAULT_MAX_ROWS_PER_SEGMENT,
            compression: super::page::CompressionType::Lz4,
            build_short_key_index: true,
            num_short_key_columns: 3,
            build_hnsw_indexes: true,
            write_column_ids: None,
        }
    }

    /// Set rowset ID
    pub fn with_rowset_id(mut self, id: RowsetId) -> Self {
        self.rowset_id = id;
        self
    }

    /// Set segment size threshold
    pub fn with_segment_size_threshold(mut self, threshold: u64) -> Self {
        self.segment_size_threshold = threshold;
        self
    }

    /// Set maximum rows per segment
    pub fn with_max_rows_per_segment(mut self, max_rows: u64) -> Self {
        self.max_rows_per_segment = max_rows;
        self
    }

    /// Set compression type
    pub fn with_compression(mut self, compression: super::page::CompressionType) -> Self {
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

    /// Set whether to build HNSW index pages while writing segments.
    pub fn with_build_hnsw_indexes(mut self, build: bool) -> Self {
        self.build_hnsw_indexes = build;
        self
    }

    pub fn with_write_column_ids(mut self, column_ids: Vec<ColumnId>) -> Self {
        self.write_column_ids = Some(column_ids);
        self
    }
}

/// Statistics for a completed segment
#[derive(Debug, Clone, Default)]
struct SegmentStats {
    /// Number of rows
    num_rows: u64,
    /// Data size in bytes
    data_size: u64,
    /// Index size in bytes
    index_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowsetWriterSavepoint {
    completed_segments_len: usize,
    total_rows: u64,
    total_data_size: u64,
    total_index_size: u64,
    current_segment_id: u32,
}

/// RowsetWriter coordinates writing data across multiple Segments
///
/// ## Lifecycle
///
/// 1. Create with `RowsetWriter::create(context)`
/// 2. Add data with `add_chunk()`
/// 3. Optionally flush segments with `flush_segment()`
/// 4. Finalize with `build()` to get the Rowset
///
/// ## Automatic Segment Management
///
/// The writer automatically creates new Segments when:
/// - Current segment exceeds size threshold
/// - Current segment exceeds row count threshold
///
pub struct RowsetWriter {
    /// Writer context
    context: RowsetWriterContext,
    /// Rowset metadata (updated during writing)
    rowset_meta: RowsetMeta,
    /// Current segment writer (None if no active segment)
    current_segment_writer: Option<SegmentWriter>,
    /// Completed segments
    completed_segments: Vec<Segment>,
    /// Statistics for completed segments
    segment_stats: Vec<SegmentStats>,
    /// Total rows written across all segments
    total_rows: u64,
    /// Total data size across all segments
    total_data_size: u64,
    /// Total index size across all segments
    total_index_size: u64,
    /// Current segment ID
    current_segment_id: u32,
    /// Whether the writer has been finalized
    finalized: bool,
}

impl RowsetWriter {
    /// Create a new RowsetWriter
    ///
    /// # Arguments
    /// * `context` - Writer context with configuration
    ///
    /// # Returns
    /// A new RowsetWriter instance
    pub fn create(context: RowsetWriterContext) -> Result<Self> {
        // Create rowset directory
        std::fs::create_dir_all(&context.rowset_path).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to create rowset directory {:?}: {}",
                context.rowset_path, e
            ))
        })?;

        // Initialize rowset metadata
        let rowset_meta = RowsetMeta::new(context.rowset_id, context.tablet_id, context.version);

        Ok(Self {
            context,
            rowset_meta,
            current_segment_writer: None,
            completed_segments: Vec::new(),
            segment_stats: Vec::new(),
            total_rows: 0,
            total_data_size: 0,
            total_index_size: 0,
            current_segment_id: 0,
            finalized: false,
        })
    }

    /// Get the rowset ID
    pub fn rowset_id(&self) -> RowsetId {
        self.context.rowset_id
    }

    /// Get the tablet ID
    pub fn tablet_id(&self) -> u64 {
        self.context.tablet_id
    }

    /// Get the version
    pub fn version(&self) -> Version {
        self.context.version
    }

    /// Get total rows written
    pub fn num_rows(&self) -> u64 {
        self.total_rows + self.current_segment_rows()
    }

    /// Get number of segments (completed + current)
    pub fn num_segments(&self) -> u32 {
        let current = if self.current_segment_writer.is_some() {
            1
        } else {
            0
        };
        self.completed_segments.len() as u32 + current
    }

    /// Get total data size
    pub fn total_data_size(&self) -> u64 {
        self.total_data_size
    }

    /// Get total index size
    pub fn total_index_size(&self) -> u64 {
        self.total_index_size
    }

    /// Check if the writer has been finalized
    pub fn is_finalized(&self) -> bool {
        self.finalized
    }

    /// Get rows in current segment
    fn current_segment_rows(&self) -> u64 {
        self.current_segment_writer
            .as_ref()
            .map(|w| w.num_rows())
            .unwrap_or(0)
    }

    /// Check if current segment should be flushed
    fn should_flush_segment(&self) -> bool {
        if let Some(writer) = &self.current_segment_writer {
            let rows = writer.num_rows();
            // Check row count threshold
            if rows >= self.context.max_rows_per_segment {
                return true;
            }
            // Note: Size threshold check would require tracking estimated size
            // For now, we rely on row count
        }
        false
    }

    /// Create a new segment writer
    fn create_segment_writer(&mut self) -> Result<()> {
        let segment_id = self.current_segment_id;
        let segment_path = self.segment_path(segment_id);
        let rowset_gen = self.rowset_meta.rowset_gen();

        let mut options = SegmentWriterOptions::new(segment_id)
            .with_rowset_context(self.context.tablet_id, self.context.rowset_id, rowset_gen)
            .with_compression(self.context.compression)
            .with_short_key_index(self.context.build_short_key_index)
            .with_num_short_key_columns(self.context.num_short_key_columns);

        // Collect HNSW columns from schema
        let mut hnsw_cols = Vec::new();
        for col in self.context.schema.columns() {
            if col.index_hnsw {
                hnsw_cols.push(col.id);
            }
        }
        if !hnsw_cols.is_empty() {
            options = options
                .with_hnsw_index_columns(hnsw_cols)
                .with_build_hnsw_indexes(self.context.build_hnsw_indexes);
            // Use config/distance from the first column for now
            if let Some(col) = self.context.schema.columns().iter().find(|c| c.index_hnsw) {
                use crate::index::hnsw::{DistanceMetric, HnswConfig};
                options = options
                    .with_hnsw_config(HnswConfig::new(col.hnsw_m, col.hnsw_ef_construct))
                    .with_hnsw_distance(DistanceMetric::from_u8(col.hnsw_distance));
            }
        }

        let mut writer = SegmentWriter::create(self.context.schema.clone(), segment_path, options)?;
        if let Some(column_ids) = &self.context.write_column_ids {
            writer.init_vertical(column_ids.clone(), true)?;
        }
        self.current_segment_writer = Some(writer);

        Ok(())
    }

    /// Get segment file path
    fn segment_path(&self, segment_id: u32) -> PathBuf {
        self.context.rowset_path.join(format!("{}.dat", segment_id))
    }

    /// Add a chunk of data
    ///
    /// The data is written to the current segment. If the segment exceeds
    /// thresholds, it is automatically flushed and a new segment is created.
    ///
    /// # Arguments
    /// * `columns` - Vector of column data, one per schema column
    ///
    /// # Returns
    /// Number of rows added
    pub fn add_chunk(&mut self, columns: &[ColumnData]) -> Result<u64> {
        if self.finalized {
            return Err(paro_error::internal("RowsetWriter already finalized"));
        }

        // Create segment writer if needed
        if self.current_segment_writer.is_none() {
            self.create_segment_writer()?;
        }

        // Add data to current segment
        let writer = self.current_segment_writer.as_mut().unwrap();
        let rows_added = writer.append_chunk(columns)?;

        // Check if we should flush
        if self.should_flush_segment() {
            self.flush_segment()?;
        }

        Ok(rows_added)
    }

    /// Flush the current segment
    ///
    /// Finalizes the current segment and prepares for a new one.
    /// This is called automatically when thresholds are exceeded,
    /// but can also be called manually.
    pub fn flush_segment(&mut self) -> Result<()> {
        if self.finalized {
            return Err(paro_error::internal("RowsetWriter already finalized"));
        }

        if let Some(writer) = self.current_segment_writer.take() {
            let num_rows = writer.num_rows();

            // Skip empty segments
            if num_rows == 0 {
                return Ok(());
            }

            // Finalize the segment
            let segment = writer.finalize()?;

            // Collect statistics
            let stats = SegmentStats {
                num_rows,
                data_size: segment.data_size(),
                index_size: segment.index_size(),
            };

            self.total_rows += stats.num_rows;
            self.total_data_size += stats.data_size;
            self.total_index_size += stats.index_size;

            self.completed_segments.push(segment);
            self.segment_stats.push(stats);

            // Increment segment ID for next segment
            self.current_segment_id += 1;
        }

        Ok(())
    }

    pub fn mark_savepoint(&mut self) -> Result<RowsetWriterSavepoint> {
        self.flush_segment()?;
        Ok(RowsetWriterSavepoint {
            completed_segments_len: self.completed_segments.len(),
            total_rows: self.total_rows,
            total_data_size: self.total_data_size,
            total_index_size: self.total_index_size,
            current_segment_id: self.current_segment_id,
        })
    }

    pub fn rollback_to_savepoint(&mut self, mark: &RowsetWriterSavepoint) -> Result<()> {
        if self.finalized {
            return Err(paro_error::internal("RowsetWriter already finalized"));
        }

        self.current_segment_writer.take();
        self.remove_segment_outputs_from(mark.current_segment_id)?;
        self.completed_segments
            .truncate(mark.completed_segments_len);
        self.segment_stats.truncate(mark.completed_segments_len);
        self.total_rows = mark.total_rows;
        self.total_data_size = mark.total_data_size;
        self.total_index_size = mark.total_index_size;
        self.current_segment_id = mark.current_segment_id;
        Ok(())
    }

    /// Build the final Rowset
    ///
    /// Finalizes any remaining segment and creates the Rowset with all segments.
    ///
    /// # Returns
    /// The completed Rowset
    pub fn build(mut self) -> Result<Rowset> {
        if self.finalized {
            return Err(paro_error::internal("RowsetWriter already finalized"));
        }

        // Flush any remaining segment
        self.flush_segment()?;
        self.finalized = true;

        // Update rowset metadata
        self.rowset_meta.set_num_rows(self.total_rows);
        self.rowset_meta
            .set_num_segments(self.completed_segments.len() as u32);
        self.rowset_meta
            .set_disk_sizes(self.total_data_size, self.total_index_size);
        self.rowset_meta.set_rowset_state(RowsetState::Committed);
        self.rowset_meta
            .set_rowset_path(self.context.rowset_path.to_string_lossy().to_string());

        // Determine segments overlap
        // For now, assume non-overlapping if we have proper ordering
        let overlap = if self.completed_segments.len() <= 1 {
            SegmentsOverlap::NonOverlapping
        } else {
            SegmentsOverlap::Unknown
        };
        self.rowset_meta.set_segments_overlap(overlap);

        // Create the Rowset
        let rowset = Rowset::create_with_segments(
            self.context.schema.clone(),
            self.rowset_meta,
            &self.context.rowset_path,
            self.completed_segments.into_iter().map(Arc::new).collect(),
        )?;

        Ok(rowset)
    }

    /// Build and return a shared pointer to the Rowset
    pub fn build_shared(self) -> Result<RowsetSharedPtr> {
        Ok(Arc::new(self.build()?))
    }

    /// Get the rowset path
    pub fn rowset_path(&self) -> &Path {
        &self.context.rowset_path
    }

    fn remove_segment_outputs_from(&self, first_segment_id: u32) -> Result<()> {
        if !self.context.rowset_path.exists() {
            return Ok(());
        }

        for entry in fs::read_dir(&self.context.rowset_path).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to read rowset directory {:?}: {}",
                self.context.rowset_path, e
            ))
        })? {
            let entry = entry
                .map_err(|e| paro_error::io_error(format!("Failed to read rowset entry: {}", e)))?;
            let path = entry.path();
            let Some(segment_id) = Self::segment_artifact_id(&path) else {
                continue;
            };
            if segment_id < first_segment_id {
                continue;
            }

            let file_type = entry.file_type().map_err(|e| {
                paro_error::io_error(format!(
                    "Failed to inspect rowset artifact {:?}: {}",
                    path, e
                ))
            })?;
            if file_type.is_dir() {
                fs::remove_dir_all(&path).map_err(|e| {
                    paro_error::io_error(format!(
                        "Failed to remove rowset artifact directory {:?}: {}",
                        path, e
                    ))
                })?;
            } else {
                fs::remove_file(&path).map_err(|e| {
                    paro_error::io_error(format!(
                        "Failed to remove rowset artifact {:?}: {}",
                        path, e
                    ))
                })?;
            }
        }

        Ok(())
    }

    fn segment_artifact_id(path: &Path) -> Option<u32> {
        let file_name = path.file_name()?.to_str()?;
        let prefix = file_name.split('.').next()?;
        prefix.parse().ok()
    }
}

/// Builder for creating RowsetWriter with fluent API
pub struct RowsetWriterBuilder {
    context: RowsetWriterContext,
}

impl RowsetWriterBuilder {
    /// Create a new builder
    pub fn new(
        schema: TabletSchemaRef,
        tablet_id: u64,
        version: Version,
        rowset_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            context: RowsetWriterContext::new(schema, tablet_id, version, rowset_path),
        }
    }

    /// Set rowset ID
    pub fn rowset_id(mut self, id: RowsetId) -> Self {
        self.context.rowset_id = id;
        self
    }

    /// Set segment size threshold
    pub fn segment_size_threshold(mut self, threshold: u64) -> Self {
        self.context.segment_size_threshold = threshold;
        self
    }

    /// Set maximum rows per segment
    pub fn max_rows_per_segment(mut self, max_rows: u64) -> Self {
        self.context.max_rows_per_segment = max_rows;
        self
    }

    /// Set compression type
    pub fn compression(mut self, compression: super::page::CompressionType) -> Self {
        self.context.compression = compression;
        self
    }

    /// Set whether to build short key index
    pub fn short_key_index(mut self, build: bool) -> Self {
        self.context.build_short_key_index = build;
        self
    }

    /// Set number of short key columns
    pub fn num_short_key_columns(mut self, num: usize) -> Self {
        self.context.num_short_key_columns = num;
        self
    }

    /// Set whether to build HNSW index pages while writing segments.
    pub fn build_hnsw_indexes(mut self, build: bool) -> Self {
        self.context.build_hnsw_indexes = build;
        self
    }

    pub fn write_column_ids(mut self, column_ids: Vec<ColumnId>) -> Self {
        self.context.write_column_ids = Some(column_ids);
        self
    }

    /// Build the RowsetWriter
    pub fn build(self) -> Result<RowsetWriter> {
        RowsetWriter::create(self.context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rowset::page::CompressionType;
    use crate::tablet::tablet_schema::{KeysType, TabletColumn, TabletSchema};
    use paro_common::types::LogicalType;
    use tempfile::TempDir;

    fn create_test_schema() -> TabletSchemaRef {
        let columns = vec![
            TabletColumn::key(0, "id", LogicalType::BigInt),
            TabletColumn::new(1, "name", LogicalType::Varchar),
            TabletColumn::new(2, "value", LogicalType::Integer),
        ];
        Arc::new(TabletSchema::new(1, columns, KeysType::PrimaryKeys).unwrap())
    }

    fn create_simple_schema() -> TabletSchemaRef {
        let columns = vec![
            TabletColumn::new(0, "col0", LogicalType::Integer),
            TabletColumn::new(1, "col1", LogicalType::Integer),
        ];
        Arc::new(TabletSchema::new(1, columns, KeysType::DuplicateKeys).unwrap())
    }

    #[test]
    fn test_rowset_writer_context() {
        let schema = create_test_schema();
        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), "/tmp/rowset")
            .with_segment_size_threshold(128 * 1024 * 1024)
            .with_max_rows_per_segment(500_000)
            .with_compression(CompressionType::Zstd)
            .with_short_key_index(false);

        assert_eq!(context.tablet_id, 100);
        assert_eq!(context.segment_size_threshold, 128 * 1024 * 1024);
        assert_eq!(context.max_rows_per_segment, 500_000);
        assert_eq!(context.compression, CompressionType::Zstd);
        assert!(!context.build_short_key_index);
    }

    #[test]
    fn test_rowset_writer_create() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path);
        let writer = RowsetWriter::create(context).unwrap();

        assert_eq!(writer.tablet_id(), 100);
        assert_eq!(writer.num_rows(), 0);
        assert_eq!(writer.num_segments(), 0);
        assert!(!writer.is_finalized());
    }

    #[test]
    fn test_rowset_writer_builder() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        let writer = RowsetWriterBuilder::new(schema, 100, Version::singleton(0), &rowset_path)
            .rowset_id(42)
            .max_rows_per_segment(100)
            .compression(CompressionType::None)
            .short_key_index(false)
            .build()
            .unwrap();

        assert_eq!(writer.rowset_id(), 42);
    }

    #[test]
    fn test_rowset_writer_empty_build() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_short_key_index(false);
        let writer = RowsetWriter::create(context).unwrap();

        let rowset = writer.build().unwrap();

        assert_eq!(rowset.num_rows(), 0);
        assert_eq!(rowset.num_segments(), 0);
        assert_eq!(rowset.rowset_state(), RowsetState::Committed);
    }

    #[test]
    fn test_rowset_writer_add_chunk() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_short_key_index(false)
            .with_compression(CompressionType::None);
        let mut writer = RowsetWriter::create(context).unwrap();

        // Create test data: 10 rows of two i32 columns
        let col0_data: Vec<u8> = (0i32..10).flat_map(|v| v.to_le_bytes()).collect();
        let col1_data: Vec<u8> = (100i32..110).flat_map(|v| v.to_le_bytes()).collect();

        let columns = vec![
            ColumnData::new(col0_data, 10),
            ColumnData::new(col1_data, 10),
        ];

        let rows_added = writer.add_chunk(&columns).unwrap();
        assert_eq!(rows_added, 10);
        assert_eq!(writer.num_rows(), 10);
        assert_eq!(writer.num_segments(), 1); // One active segment

        let rowset = writer.build().unwrap();
        assert_eq!(rowset.num_rows(), 10);
        assert_eq!(rowset.num_segments(), 1);
    }

    #[test]
    fn test_rowset_writer_multiple_chunks() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_short_key_index(false)
            .with_compression(CompressionType::None);
        let mut writer = RowsetWriter::create(context).unwrap();

        // Add multiple chunks
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

            writer.add_chunk(&columns).unwrap();
        }

        assert_eq!(writer.num_rows(), 500);

        let rowset = writer.build().unwrap();
        assert_eq!(rowset.num_rows(), 500);
    }

    #[test]
    fn test_rowset_writer_auto_flush() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        // Set low threshold to trigger auto-flush
        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_short_key_index(false)
            .with_compression(CompressionType::None)
            .with_max_rows_per_segment(50); // Very low threshold
        let mut writer = RowsetWriter::create(context).unwrap();

        // Add multiple small chunks to trigger auto-flush
        for _ in 0..4 {
            let col0_data: Vec<u8> = (0i32..30).flat_map(|v| v.to_le_bytes()).collect();
            let col1_data: Vec<u8> = (0i32..30).flat_map(|v| v.to_le_bytes()).collect();

            let columns = vec![
                ColumnData::new(col0_data, 30),
                ColumnData::new(col1_data, 30),
            ];

            writer.add_chunk(&columns).unwrap();
        }

        // Should have created multiple segments due to auto-flush
        let rowset = writer.build().unwrap();
        assert_eq!(rowset.num_rows(), 120);
        // At least 2 segments due to 50 row threshold (120 rows / 50 = 2.4)
        assert!(
            rowset.num_segments() >= 2,
            "Expected at least 2 segments, got {}",
            rowset.num_segments()
        );
    }

    #[test]
    fn test_rowset_writer_manual_flush() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_short_key_index(false)
            .with_compression(CompressionType::None);
        let mut writer = RowsetWriter::create(context).unwrap();

        // Add first chunk
        let col0_data: Vec<u8> = (0i32..50).flat_map(|v| v.to_le_bytes()).collect();
        let col1_data: Vec<u8> = (0i32..50).flat_map(|v| v.to_le_bytes()).collect();
        let columns = vec![
            ColumnData::new(col0_data, 50),
            ColumnData::new(col1_data, 50),
        ];
        writer.add_chunk(&columns).unwrap();

        // Manual flush
        writer.flush_segment().unwrap();

        // Add second chunk
        let col0_data: Vec<u8> = (50i32..100).flat_map(|v| v.to_le_bytes()).collect();
        let col1_data: Vec<u8> = (50i32..100).flat_map(|v| v.to_le_bytes()).collect();
        let columns = vec![
            ColumnData::new(col0_data, 50),
            ColumnData::new(col1_data, 50),
        ];
        writer.add_chunk(&columns).unwrap();

        let rowset = writer.build().unwrap();
        assert_eq!(rowset.num_rows(), 100);
        assert_eq!(rowset.num_segments(), 2);
    }

    #[test]
    fn test_rowset_writer_savepoint_discards_new_segment_outputs() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_savepoint");
        let schema = create_simple_schema();

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_short_key_index(false)
            .with_compression(CompressionType::None);
        let mut writer = RowsetWriter::create(context).unwrap();

        let first_columns = vec![
            ColumnData::new(
                (0i32..10).flat_map(|v| v.to_le_bytes()).collect::<Vec<_>>(),
                10,
            ),
            ColumnData::new(
                (100i32..110)
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<_>>(),
                10,
            ),
        ];
        writer.add_chunk(&first_columns).unwrap();

        let mark = writer.mark_savepoint().unwrap();
        assert!(rowset_path.join("0.dat").exists());

        let second_columns = vec![
            ColumnData::new(
                (10i32..15)
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<_>>(),
                5,
            ),
            ColumnData::new(
                (110i32..115)
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<_>>(),
                5,
            ),
        ];
        writer.add_chunk(&second_columns).unwrap();
        assert!(rowset_path.join("1.dat").exists());
        assert_eq!(writer.num_rows(), 15);

        writer.rollback_to_savepoint(&mark).unwrap();

        assert_eq!(writer.num_rows(), 10);
        assert_eq!(writer.num_segments(), 1);
        assert!(rowset_path.join("0.dat").exists());
        assert!(!rowset_path.join("1.dat").exists());

        let third_columns = vec![
            ColumnData::new(
                (20i32..24)
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<_>>(),
                4,
            ),
            ColumnData::new(
                (120i32..124)
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<_>>(),
                4,
            ),
        ];
        writer.add_chunk(&third_columns).unwrap();

        let rowset = writer.build().unwrap();
        assert_eq!(rowset.num_rows(), 14);
        assert_eq!(rowset.num_segments(), 2);
    }

    #[test]
    fn test_rowset_writer_version() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        // Test with range version
        let context = RowsetWriterContext::new(schema, 100, Version::new(5, 10), &rowset_path)
            .with_short_key_index(false);
        let writer = RowsetWriter::create(context).unwrap();

        assert_eq!(writer.version().start, 5);
        assert_eq!(writer.version().end, 10);

        let rowset = writer.build().unwrap();
        assert_eq!(rowset.start_version(), 5);
        assert_eq!(rowset.end_version(), 10);
    }

    #[test]
    fn test_rowset_writer_statistics() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_short_key_index(false)
            .with_compression(CompressionType::None);
        let mut writer = RowsetWriter::create(context).unwrap();

        // Add data
        let col0_data: Vec<u8> = (0i32..100).flat_map(|v| v.to_le_bytes()).collect();
        let col1_data: Vec<u8> = (0i32..100).flat_map(|v| v.to_le_bytes()).collect();
        let columns = vec![
            ColumnData::new(col0_data, 100),
            ColumnData::new(col1_data, 100),
        ];
        writer.add_chunk(&columns).unwrap();

        let rowset = writer.build().unwrap();

        // Check that statistics are populated
        assert_eq!(rowset.num_rows(), 100);
        assert!(rowset.total_disk_size() > 0);
    }

    #[test]
    fn test_rowset_writer_double_build_fails() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_short_key_index(false);
        let writer = RowsetWriter::create(context).unwrap();

        // First build succeeds
        let _rowset = writer.build().unwrap();

        // Can't build twice because build() consumes self
        // This is enforced by Rust's ownership system
    }

    #[test]
    fn test_rowset_writer_path() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_simple_schema();

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_short_key_index(false);
        let writer = RowsetWriter::create(context).unwrap();

        assert_eq!(writer.rowset_path(), rowset_path);
    }

    #[test]
    fn test_rowset_writer_with_primary_key_schema() {
        let temp_dir = TempDir::new().unwrap();
        let rowset_path = temp_dir.path().join("rowset_1");
        let schema = create_test_schema(); // Has primary key

        let context = RowsetWriterContext::new(schema, 100, Version::singleton(0), &rowset_path)
            .with_short_key_index(false)
            .with_compression(CompressionType::None);
        let mut writer = RowsetWriter::create(context).unwrap();

        // Create test data for 3 columns
        let col0_data: Vec<u8> = (0i64..10).flat_map(|v| v.to_le_bytes()).collect(); // BigInt
        let col1_data: Vec<u8> = vec![0u8; 40]; // Varchar placeholder
        let col2_data: Vec<u8> = (0i32..10).flat_map(|v| v.to_le_bytes()).collect(); // Integer

        let columns = vec![
            ColumnData::new(col0_data, 10),
            ColumnData::new(col1_data, 10),
            ColumnData::new(col2_data, 10),
        ];

        writer.add_chunk(&columns).unwrap();

        let rowset = writer.build().unwrap();
        assert_eq!(rowset.num_rows(), 10);
    }
}
