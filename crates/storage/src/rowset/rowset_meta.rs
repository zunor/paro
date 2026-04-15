// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # RowsetMeta
//!
//! Rowset metadata for persistence and management.
//!
//! ## Key Design
//!
//! - Stores all metadata needed to identify and manage a Rowset
//! - Includes version information for MVCC
//! - Tracks size statistics for compaction decisions
//! - Supports serialization for persistence

use crate::tablet::Version;
use paro_common::error::{self as paro_error, Result};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Unique identifier for a Rowset
pub type RowsetId = u64;

/// Rowset state enumeration.
///
/// Tracks the lifecycle state of a Rowset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RowsetState {
    /// Rowset is being prepared (not yet committed)
    Prepared = 0,

    /// Rowset is committed but not yet visible
    Committed = 1,

    /// Rowset is visible for reads
    Visible = 2,

    /// Rowset is being deleted
    Deleting = 3,

    /// Rowset has been deleted
    Deleted = 4,
}

impl Default for RowsetState {
    fn default() -> Self {
        RowsetState::Prepared
    }
}

impl std::fmt::Display for RowsetState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RowsetState::Prepared => write!(f, "PREPARED"),
            RowsetState::Committed => write!(f, "COMMITTED"),
            RowsetState::Visible => write!(f, "VISIBLE"),
            RowsetState::Deleting => write!(f, "DELETING"),
            RowsetState::Deleted => write!(f, "DELETED"),
        }
    }
}

impl TryFrom<u8> for RowsetState {
    type Error = paro_common::error::ParoError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(RowsetState::Prepared),
            1 => Ok(RowsetState::Committed),
            2 => Ok(RowsetState::Visible),
            3 => Ok(RowsetState::Deleting),
            4 => Ok(RowsetState::Deleted),
            _ => Err(paro_error::invalid_input(format!(
                "Invalid RowsetState value: {}",
                value
            ))),
        }
    }
}

/// Segments overlap type
///
/// Indicates whether segments within a rowset have overlapping key ranges.
/// Tracks whether rowsets overlap in the version graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum SegmentsOverlap {
    /// Segments are non-overlapping (sorted, no key overlap)
    NonOverlapping = 0,

    /// Segments may have overlapping key ranges
    Overlapping = 1,

    /// Overlap status is unknown
    Unknown = 2,
}

impl Default for SegmentsOverlap {
    fn default() -> Self {
        SegmentsOverlap::Unknown
    }
}

impl std::fmt::Display for SegmentsOverlap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SegmentsOverlap::NonOverlapping => write!(f, "NON_OVERLAPPING"),
            SegmentsOverlap::Overlapping => write!(f, "OVERLAPPING"),
            SegmentsOverlap::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

impl TryFrom<u8> for SegmentsOverlap {
    type Error = paro_common::error::ParoError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(SegmentsOverlap::NonOverlapping),
            1 => Ok(SegmentsOverlap::Overlapping),
            2 => Ok(SegmentsOverlap::Unknown),
            _ => Err(paro_error::invalid_input(format!(
                "Invalid SegmentsOverlap value: {}",
                value
            ))),
        }
    }
}

/// Global rowset ID generator
static NEXT_ROWSET_ID: AtomicU64 = AtomicU64::new(1);

/// Generate a new unique rowset ID
pub fn generate_rowset_id() -> RowsetId {
    NEXT_ROWSET_ID.fetch_add(1, Ordering::SeqCst)
}

/// Set the next rowset ID (for recovery)
pub fn set_next_rowset_id(id: RowsetId) {
    NEXT_ROWSET_ID.store(id, Ordering::SeqCst);
}

/// Get current timestamp in seconds since UNIX epoch
fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// RowsetMeta contains all metadata for a Rowset.
///
/// This structure stores the essential information needed to identify,
/// locate, and manage a Rowset. It is persisted as part of TabletMeta.
#[derive(Debug, Clone)]
pub struct RowsetMeta {
    /// Unique rowset identifier
    rowset_id: RowsetId,

    /// Parent tablet ID
    tablet_id: u64,

    /// Version range [start, end]
    /// For singleton versions, start == end
    version: Version,

    /// Total number of rows in the rowset
    num_rows: u64,

    /// Number of segments in the rowset
    num_segments: u32,

    /// Total disk size in bytes (data + index)
    total_disk_size: u64,

    /// Data disk size in bytes (excluding index)
    data_disk_size: u64,

    /// Index disk size in bytes
    index_disk_size: u64,

    /// Creation timestamp (seconds since UNIX epoch)
    creation_time: i64,

    /// Modification timestamp (seconds since UNIX epoch)
    modification_time: i64,

    /// Current rowset state
    rowset_state: RowsetState,

    /// Segments overlap type
    segments_overlap: SegmentsOverlap,

    /// Number of delete vectors (for primary key model)
    num_delete_vectors: u32,

    /// Total number of deleted rows
    num_deleted_rows: u64,

    /// Rowset path relative to tablet data directory
    rowset_path: String,

    /// Schema hash (for schema versioning)
    schema_hash: u32,

    /// Referenced schema ID (for global schema deduplication)
    schema_id: u64,

    /// Whether this rowset is from compaction
    is_compaction_output: bool,

    /// Source rowset IDs (if this is a compaction output)
    source_rowset_ids: Vec<RowsetId>,
}

impl RowsetMeta {
    /// Create a new RowsetMeta with default values
    ///
    /// # Arguments
    /// * `rowset_id` - Unique rowset identifier
    /// * `tablet_id` - Parent tablet identifier
    /// * `version` - Version range for this rowset
    pub fn new(rowset_id: RowsetId, tablet_id: u64, version: Version) -> Self {
        let now = current_timestamp();
        Self {
            rowset_id,
            tablet_id,
            version,
            num_rows: 0,
            num_segments: 0,
            total_disk_size: 0,
            data_disk_size: 0,
            index_disk_size: 0,
            creation_time: now,
            modification_time: now,
            rowset_state: RowsetState::Prepared,
            segments_overlap: SegmentsOverlap::Unknown,
            num_delete_vectors: 0,
            num_deleted_rows: 0,
            rowset_path: String::new(),
            schema_hash: 0,
            schema_id: 0,
            is_compaction_output: false,
            source_rowset_ids: Vec::new(),
        }
    }

    /// Create a new RowsetMeta with auto-generated ID
    pub fn create(tablet_id: u64, version: Version) -> Self {
        Self::new(generate_rowset_id(), tablet_id, version)
    }

    /// Create a singleton version rowset meta
    pub fn singleton(rowset_id: RowsetId, tablet_id: u64, version: i64) -> Self {
        Self::new(rowset_id, tablet_id, Version::singleton(version))
    }

    // ==================== Getters ====================

    /// Get rowset ID
    pub fn rowset_id(&self) -> RowsetId {
        self.rowset_id
    }

    /// Get tablet ID
    pub fn tablet_id(&self) -> u64 {
        self.tablet_id
    }

    /// Get version range
    pub fn version(&self) -> Version {
        self.version
    }

    /// Get start version
    pub fn start_version(&self) -> i64 {
        self.version.start
    }

    /// Get end version
    pub fn end_version(&self) -> i64 {
        self.version.end
    }

    /// Get rowset generation for cache isolation.
    pub fn rowset_gen(&self) -> u64 {
        if self.version.end < 0 {
            0
        } else {
            self.version.end as u64
        }
    }

    /// Get number of rows
    pub fn num_rows(&self) -> u64 {
        self.num_rows
    }

    /// Get number of segments
    pub fn num_segments(&self) -> u32 {
        self.num_segments
    }

    /// Get total disk size
    pub fn total_disk_size(&self) -> u64 {
        self.total_disk_size
    }

    /// Get data disk size
    pub fn data_disk_size(&self) -> u64 {
        self.data_disk_size
    }

    /// Get index disk size
    pub fn index_disk_size(&self) -> u64 {
        self.index_disk_size
    }

    /// Get creation time
    pub fn creation_time(&self) -> i64 {
        self.creation_time
    }

    /// Get modification time
    pub fn modification_time(&self) -> i64 {
        self.modification_time
    }

    /// Get rowset state
    pub fn rowset_state(&self) -> RowsetState {
        self.rowset_state
    }

    /// Get segments overlap type
    pub fn segments_overlap(&self) -> SegmentsOverlap {
        self.segments_overlap
    }

    /// Get number of delete vectors
    pub fn num_delete_vectors(&self) -> u32 {
        self.num_delete_vectors
    }

    /// Get number of deleted rows
    pub fn num_deleted_rows(&self) -> u64 {
        self.num_deleted_rows
    }

    /// Get rowset path
    pub fn rowset_path(&self) -> &str {
        &self.rowset_path
    }

    /// Get schema hash
    pub fn schema_hash(&self) -> u32 {
        self.schema_hash
    }

    /// Get schema ID reference
    pub fn schema_id(&self) -> u64 {
        self.schema_id
    }

    /// Check if this is a compaction output
    pub fn is_compaction_output(&self) -> bool {
        self.is_compaction_output
    }

    /// Get source rowset IDs
    pub fn source_rowset_ids(&self) -> &[RowsetId] {
        &self.source_rowset_ids
    }

    // ==================== Setters ====================

    /// Set number of rows
    pub fn set_num_rows(&mut self, num_rows: u64) {
        self.num_rows = num_rows;
        self.modification_time = current_timestamp();
    }

    /// Set number of segments
    pub fn set_num_segments(&mut self, num_segments: u32) {
        self.num_segments = num_segments;
        self.modification_time = current_timestamp();
    }

    /// Set disk sizes
    pub fn set_disk_sizes(&mut self, data_size: u64, index_size: u64) {
        self.data_disk_size = data_size;
        self.index_disk_size = index_size;
        self.total_disk_size = data_size + index_size;
        self.modification_time = current_timestamp();
    }

    /// Set total disk size directly
    pub fn set_total_disk_size(&mut self, size: u64) {
        self.total_disk_size = size;
        self.modification_time = current_timestamp();
    }

    /// Set version range
    pub fn set_version(&mut self, version: Version) {
        self.version = version;
        self.modification_time = current_timestamp();
    }

    /// Set rowset state
    pub fn set_rowset_state(&mut self, state: RowsetState) {
        self.rowset_state = state;
        self.modification_time = current_timestamp();
    }

    /// Set segments overlap type
    pub fn set_segments_overlap(&mut self, overlap: SegmentsOverlap) {
        self.segments_overlap = overlap;
    }

    /// Set delete vector info
    pub fn set_delete_info(&mut self, num_vectors: u32, num_deleted: u64) {
        self.num_delete_vectors = num_vectors;
        self.num_deleted_rows = num_deleted;
        self.modification_time = current_timestamp();
    }

    /// Set rowset path
    pub fn set_rowset_path(&mut self, path: impl Into<String>) {
        self.rowset_path = path.into();
    }

    /// Set schema hash
    pub fn set_schema_hash(&mut self, hash: u32) {
        self.schema_hash = hash;
    }

    /// Set schema ID reference
    pub fn set_schema_id(&mut self, schema_id: u64) {
        self.schema_id = schema_id;
    }

    /// Mark as compaction output
    pub fn set_compaction_output(&mut self, source_ids: Vec<RowsetId>) {
        self.is_compaction_output = true;
        self.source_rowset_ids = source_ids;
    }

    // ==================== Utility Methods ====================

    /// Check if this is a singleton version (start == end)
    pub fn is_singleton_delta(&self) -> bool {
        self.version.is_singleton()
    }

    /// Check if this rowset is empty
    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    /// Check if this rowset is visible
    pub fn is_visible(&self) -> bool {
        self.rowset_state == RowsetState::Visible
    }

    /// Check if this rowset can be read
    pub fn is_readable(&self) -> bool {
        matches!(
            self.rowset_state,
            RowsetState::Committed | RowsetState::Visible
        )
    }

    /// Get effective row count (total - deleted)
    pub fn effective_rows(&self) -> u64 {
        self.num_rows.saturating_sub(self.num_deleted_rows)
    }

    /// Get average row size in bytes
    pub fn avg_row_size(&self) -> u64 {
        if self.num_rows == 0 {
            0
        } else {
            self.data_disk_size / self.num_rows
        }
    }

    /// Get compaction score for this rowset
    ///
    /// Higher score means higher priority for compaction.
    /// Factors considered:
    /// - Number of segments (more segments = higher score)
    /// - Delete ratio (more deletes = higher score)
    /// - Size (smaller rowsets are preferred for compaction)
    ///
    /// # Returns
    /// Compaction score (0.0 - 100.0)
    pub fn get_compaction_score(&self) -> f64 {
        // Base score from segment count
        let segment_score = (self.num_segments as f64).min(10.0) * 5.0;

        // Delete ratio score (0-30 points)
        let delete_ratio = if self.num_rows > 0 {
            self.num_deleted_rows as f64 / self.num_rows as f64
        } else {
            0.0
        };
        let delete_score = delete_ratio * 30.0;

        // Size score (smaller is better for compaction, 0-20 points)
        // Normalize to 256MB as reference
        let size_mb = self.total_disk_size as f64 / (256.0 * 1024.0 * 1024.0);
        let size_score = (1.0 - size_mb.min(1.0)) * 20.0;

        segment_score + delete_score + size_score
    }

    /// Check if this rowset should be compacted
    ///
    /// Returns true if:
    /// - Has multiple segments
    /// - Has significant delete ratio (> 10%)
    /// - Is small enough to benefit from compaction
    pub fn needs_compaction(&self) -> bool {
        // Multiple segments
        if self.num_segments > 1 {
            return true;
        }

        // High delete ratio
        if self.num_rows > 0 {
            let delete_ratio = self.num_deleted_rows as f64 / self.num_rows as f64;
            if delete_ratio > 0.1 {
                return true;
            }
        }

        false
    }

    // ==================== Serialization ====================

    /// Serialize RowsetMeta to bytes
    ///
    /// Binary format (little-endian):
    /// - rowset_id: u64
    /// - tablet_id: u64
    /// - version.start: i64
    /// - version.end: i64
    /// - num_rows: u64
    /// - num_segments: u32
    /// - total_disk_size: u64
    /// - data_disk_size: u64
    /// - index_disk_size: u64
    /// - creation_time: i64
    /// - modification_time: i64
    /// - rowset_state: u8
    /// - segments_overlap: u8
    /// - num_delete_vectors: u32
    /// - num_deleted_rows: u64
    /// - schema_hash: u32
    /// - is_compaction_output: u8
    /// - rowset_path_len: u32
    /// - rowset_path: [u8]
    /// - source_rowset_ids_len: u32
    /// - source_rowset_ids: [u64]
    /// - schema_id: u64 (optional trailing field for backward compatibility)
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut data = Vec::with_capacity(256);

        // Fixed fields
        data.extend_from_slice(&self.rowset_id.to_le_bytes());
        data.extend_from_slice(&self.tablet_id.to_le_bytes());
        data.extend_from_slice(&self.version.start.to_le_bytes());
        data.extend_from_slice(&self.version.end.to_le_bytes());
        data.extend_from_slice(&self.num_rows.to_le_bytes());
        data.extend_from_slice(&self.num_segments.to_le_bytes());
        data.extend_from_slice(&self.total_disk_size.to_le_bytes());
        data.extend_from_slice(&self.data_disk_size.to_le_bytes());
        data.extend_from_slice(&self.index_disk_size.to_le_bytes());
        data.extend_from_slice(&self.creation_time.to_le_bytes());
        data.extend_from_slice(&self.modification_time.to_le_bytes());
        data.push(self.rowset_state as u8);
        data.push(self.segments_overlap as u8);
        data.extend_from_slice(&self.num_delete_vectors.to_le_bytes());
        data.extend_from_slice(&self.num_deleted_rows.to_le_bytes());
        data.extend_from_slice(&self.schema_hash.to_le_bytes());
        data.push(self.is_compaction_output as u8);

        // Rowset path (variable length)
        let path_bytes = self.rowset_path.as_bytes();
        data.extend_from_slice(&(path_bytes.len() as u32).to_le_bytes());
        data.extend_from_slice(path_bytes);

        // Source rowset IDs (variable length)
        data.extend_from_slice(&(self.source_rowset_ids.len() as u32).to_le_bytes());
        for id in &self.source_rowset_ids {
            data.extend_from_slice(&id.to_le_bytes());
        }

        // Optional trailing schema_id for schema-map reference.
        data.extend_from_slice(&self.schema_id.to_le_bytes());

        Ok(data)
    }

    /// Deserialize RowsetMeta from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        // Minimum size check (fixed fields)
        const MIN_SIZE: usize =
            8 + 8 + 8 + 8 + 8 + 4 + 8 + 8 + 8 + 8 + 8 + 1 + 1 + 4 + 8 + 4 + 1 + 4 + 4;
        if data.len() < MIN_SIZE {
            return Err(paro_error::internal(format!(
                "RowsetMeta: data too short ({} < {})",
                data.len(),
                MIN_SIZE
            )));
        }

        let mut offset = 0;

        // Helper macros for reading
        macro_rules! read_u64 {
            () => {{
                let val = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                val
            }};
        }

        macro_rules! read_i64 {
            () => {{
                let val = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                val
            }};
        }

        macro_rules! read_u32 {
            () => {{
                let val = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                offset += 4;
                val
            }};
        }

        macro_rules! read_u8 {
            () => {{
                let val = data[offset];
                offset += 1;
                val
            }};
        }

        // Read fixed fields
        let rowset_id = read_u64!();
        let tablet_id = read_u64!();
        let version_start = read_i64!();
        let version_end = read_i64!();
        let num_rows = read_u64!();
        let num_segments = read_u32!();
        let total_disk_size = read_u64!();
        let data_disk_size = read_u64!();
        let index_disk_size = read_u64!();
        let creation_time = read_i64!();
        let modification_time = read_i64!();
        let rowset_state = RowsetState::try_from(read_u8!())?;
        let segments_overlap = SegmentsOverlap::try_from(read_u8!())?;
        let num_delete_vectors = read_u32!();
        let num_deleted_rows = read_u64!();
        let schema_hash = read_u32!();
        let is_compaction_output = read_u8!() != 0;

        // Read rowset path
        let path_len = read_u32!() as usize;
        if offset + path_len > data.len() {
            return Err(paro_error::internal("RowsetMeta: truncated path"));
        }
        let rowset_path = String::from_utf8_lossy(&data[offset..offset + path_len]).to_string();
        offset += path_len;

        // Read source rowset IDs
        if offset + 4 > data.len() {
            return Err(paro_error::internal(
                "RowsetMeta: truncated source_ids length",
            ));
        }
        let source_ids_len = read_u32!() as usize;
        let mut source_rowset_ids = Vec::with_capacity(source_ids_len);
        for _ in 0..source_ids_len {
            if offset + 8 > data.len() {
                return Err(paro_error::internal("RowsetMeta: truncated source_ids"));
            }
            source_rowset_ids.push(read_u64!());
        }

        // Old rowset_meta payloads do not contain schema_id.
        let schema_id = if offset + 8 <= data.len() {
            u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap())
        } else {
            schema_hash as u64
        };

        Ok(Self {
            rowset_id,
            tablet_id,
            version: Version::new(version_start, version_end),
            num_rows,
            num_segments,
            total_disk_size,
            data_disk_size,
            index_disk_size,
            creation_time,
            modification_time,
            rowset_state,
            segments_overlap,
            num_delete_vectors,
            num_deleted_rows,
            rowset_path,
            schema_hash,
            schema_id,
            is_compaction_output,
            source_rowset_ids,
        })
    }
}

impl std::fmt::Display for RowsetMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Rowset[id={}, version={}, rows={}, segments={}, size={}B, state={}]",
            self.rowset_id,
            self.version,
            self.num_rows,
            self.num_segments,
            self.total_disk_size,
            self.rowset_state
        )
    }
}

/// Builder for RowsetMeta
///
/// Provides a fluent API for constructing RowsetMeta instances.
#[derive(Debug)]
pub struct RowsetMetaBuilder {
    meta: RowsetMeta,
}

impl RowsetMetaBuilder {
    /// Create a new builder
    pub fn new(tablet_id: u64, version: Version) -> Self {
        Self {
            meta: RowsetMeta::create(tablet_id, version),
        }
    }

    /// Create a builder with specific rowset ID
    pub fn with_id(rowset_id: RowsetId, tablet_id: u64, version: Version) -> Self {
        Self {
            meta: RowsetMeta::new(rowset_id, tablet_id, version),
        }
    }

    /// Set number of rows
    pub fn num_rows(mut self, rows: u64) -> Self {
        self.meta.num_rows = rows;
        self
    }

    /// Set number of segments
    pub fn num_segments(mut self, segments: u32) -> Self {
        self.meta.num_segments = segments;
        self
    }

    /// Set disk sizes
    pub fn disk_sizes(mut self, data: u64, index: u64) -> Self {
        self.meta.data_disk_size = data;
        self.meta.index_disk_size = index;
        self.meta.total_disk_size = data + index;
        self
    }

    /// Set rowset state
    pub fn state(mut self, state: RowsetState) -> Self {
        self.meta.rowset_state = state;
        self
    }

    /// Set segments overlap
    pub fn segments_overlap(mut self, overlap: SegmentsOverlap) -> Self {
        self.meta.segments_overlap = overlap;
        self
    }

    /// Set rowset path
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.meta.rowset_path = path.into();
        self
    }

    /// Set schema hash
    pub fn schema_hash(mut self, hash: u32) -> Self {
        self.meta.schema_hash = hash;
        self
    }

    /// Set schema ID reference
    pub fn schema_id(mut self, schema_id: u64) -> Self {
        self.meta.schema_id = schema_id;
        self
    }

    /// Mark as compaction output
    pub fn compaction_output(mut self, source_ids: Vec<RowsetId>) -> Self {
        self.meta.is_compaction_output = true;
        self.meta.source_rowset_ids = source_ids;
        self
    }

    /// Build the RowsetMeta
    pub fn build(self) -> RowsetMeta {
        self.meta
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rowset_meta_new() {
        let meta = RowsetMeta::new(1, 100, Version::singleton(0));

        assert_eq!(meta.rowset_id(), 1);
        assert_eq!(meta.tablet_id(), 100);
        assert_eq!(meta.start_version(), 0);
        assert_eq!(meta.end_version(), 0);
        assert!(meta.is_singleton_delta());
        assert_eq!(meta.rowset_state(), RowsetState::Prepared);
        assert_eq!(meta.schema_id(), 0);
    }

    #[test]
    fn test_rowset_meta_create() {
        let meta1 = RowsetMeta::create(100, Version::singleton(0));
        let meta2 = RowsetMeta::create(100, Version::singleton(1));

        // Auto-generated IDs should be different
        assert_ne!(meta1.rowset_id(), meta2.rowset_id());
    }

    #[test]
    fn test_rowset_meta_setters() {
        let mut meta = RowsetMeta::new(1, 100, Version::new(0, 5));

        meta.set_num_rows(1000);
        meta.set_num_segments(3);
        meta.set_disk_sizes(1024 * 1024, 64 * 1024);
        meta.set_rowset_state(RowsetState::Visible);
        meta.set_segments_overlap(SegmentsOverlap::NonOverlapping);

        assert_eq!(meta.num_rows(), 1000);
        assert_eq!(meta.num_segments(), 3);
        assert_eq!(meta.data_disk_size(), 1024 * 1024);
        assert_eq!(meta.index_disk_size(), 64 * 1024);
        assert_eq!(meta.total_disk_size(), 1024 * 1024 + 64 * 1024);
        assert!(meta.is_visible());
        assert_eq!(meta.segments_overlap(), SegmentsOverlap::NonOverlapping);
    }

    #[test]
    fn test_rowset_meta_effective_rows() {
        let mut meta = RowsetMeta::new(1, 100, Version::singleton(0));
        meta.set_num_rows(1000);
        meta.set_delete_info(1, 100);

        assert_eq!(meta.effective_rows(), 900);
    }

    #[test]
    fn test_rowset_meta_compaction_score() {
        // Empty rowset
        let meta1 = RowsetMeta::new(1, 100, Version::singleton(0));
        let score1 = meta1.get_compaction_score();

        // Rowset with multiple segments
        let mut meta2 = RowsetMeta::new(2, 100, Version::singleton(1));
        meta2.set_num_segments(5);
        let score2 = meta2.get_compaction_score();

        // Rowset with high delete ratio
        let mut meta3 = RowsetMeta::new(3, 100, Version::singleton(2));
        meta3.set_num_rows(1000);
        meta3.set_delete_info(1, 500);
        let score3 = meta3.get_compaction_score();

        // More segments = higher score
        assert!(score2 > score1);
        // High delete ratio = higher score
        assert!(score3 > score1);
    }

    #[test]
    fn test_rowset_meta_needs_compaction() {
        // Single segment, no deletes
        let meta1 = RowsetMeta::new(1, 100, Version::singleton(0));
        assert!(!meta1.needs_compaction());

        // Multiple segments
        let mut meta2 = RowsetMeta::new(2, 100, Version::singleton(1));
        meta2.set_num_segments(3);
        assert!(meta2.needs_compaction());

        // High delete ratio
        let mut meta3 = RowsetMeta::new(3, 100, Version::singleton(2));
        meta3.set_num_rows(1000);
        meta3.set_delete_info(1, 200); // 20% deleted
        assert!(meta3.needs_compaction());
    }

    #[test]
    fn test_rowset_meta_serialize_deserialize() {
        let mut meta = RowsetMeta::new(1, 100, Version::new(0, 5));
        meta.set_num_rows(1000);
        meta.set_num_segments(3);
        meta.set_disk_sizes(1024 * 1024, 64 * 1024);
        meta.set_rowset_state(RowsetState::Visible);
        meta.set_segments_overlap(SegmentsOverlap::NonOverlapping);
        meta.set_rowset_path("/data/rowset_1");
        meta.set_schema_hash(12345);
        meta.set_schema_id(67890);
        meta.set_compaction_output(vec![10, 11, 12]);

        let bytes = meta.serialize().unwrap();
        let restored = RowsetMeta::deserialize(&bytes).unwrap();

        assert_eq!(restored.rowset_id(), 1);
        assert_eq!(restored.tablet_id(), 100);
        assert_eq!(restored.start_version(), 0);
        assert_eq!(restored.end_version(), 5);
        assert_eq!(restored.num_rows(), 1000);
        assert_eq!(restored.num_segments(), 3);
        assert_eq!(restored.data_disk_size(), 1024 * 1024);
        assert_eq!(restored.index_disk_size(), 64 * 1024);
        assert_eq!(restored.rowset_state(), RowsetState::Visible);
        assert_eq!(restored.segments_overlap(), SegmentsOverlap::NonOverlapping);
        assert_eq!(restored.rowset_path(), "/data/rowset_1");
        assert_eq!(restored.schema_hash(), 12345);
        assert_eq!(restored.schema_id(), 67890);
        assert!(restored.is_compaction_output());
        assert_eq!(restored.source_rowset_ids(), &[10, 11, 12]);
    }

    #[test]
    fn test_rowset_meta_deserialize_legacy_without_schema_id() {
        let mut meta = RowsetMeta::new(1, 100, Version::singleton(0));
        meta.set_schema_hash(54321);

        let bytes = meta.serialize().unwrap();
        let legacy_bytes = &bytes[..bytes.len() - 8];
        let restored = RowsetMeta::deserialize(legacy_bytes).unwrap();

        assert_eq!(restored.schema_hash(), 54321);
        assert_eq!(restored.schema_id(), 54321);
    }

    #[test]
    fn test_rowset_meta_builder() {
        let meta = RowsetMetaBuilder::new(100, Version::singleton(0))
            .num_rows(1000)
            .num_segments(2)
            .disk_sizes(1024, 128)
            .state(RowsetState::Visible)
            .path("/data/rowset")
            .schema_id(77)
            .build();

        assert_eq!(meta.tablet_id(), 100);
        assert_eq!(meta.num_rows(), 1000);
        assert_eq!(meta.num_segments(), 2);
        assert_eq!(meta.total_disk_size(), 1024 + 128);
        assert!(meta.is_visible());
        assert_eq!(meta.rowset_path(), "/data/rowset");
        assert_eq!(meta.schema_id(), 77);
    }

    #[test]
    fn test_rowset_state_display() {
        assert_eq!(format!("{}", RowsetState::Prepared), "PREPARED");
        assert_eq!(format!("{}", RowsetState::Visible), "VISIBLE");
    }

    #[test]
    fn test_segments_overlap_display() {
        assert_eq!(
            format!("{}", SegmentsOverlap::NonOverlapping),
            "NON_OVERLAPPING"
        );
        assert_eq!(format!("{}", SegmentsOverlap::Overlapping), "OVERLAPPING");
    }

    #[test]
    fn test_rowset_meta_display() {
        let mut meta = RowsetMeta::new(1, 100, Version::new(0, 5));
        meta.set_num_rows(1000);
        meta.set_num_segments(3);
        meta.set_total_disk_size(1024);
        meta.set_rowset_state(RowsetState::Visible);

        let display = format!("{}", meta);
        assert!(display.contains("id=1"));
        assert!(display.contains("rows=1000"));
        assert!(display.contains("segments=3"));
        assert!(display.contains("VISIBLE"));
    }

    #[test]
    fn test_rowset_state_try_from() {
        assert_eq!(RowsetState::try_from(0).unwrap(), RowsetState::Prepared);
        assert_eq!(RowsetState::try_from(2).unwrap(), RowsetState::Visible);
        assert!(RowsetState::try_from(99).is_err());
    }

    #[test]
    fn test_segments_overlap_try_from() {
        assert_eq!(
            SegmentsOverlap::try_from(0).unwrap(),
            SegmentsOverlap::NonOverlapping
        );
        assert_eq!(
            SegmentsOverlap::try_from(1).unwrap(),
            SegmentsOverlap::Overlapping
        );
        assert!(SegmentsOverlap::try_from(99).is_err());
    }

    #[test]
    fn test_avg_row_size() {
        let mut meta = RowsetMeta::new(1, 100, Version::singleton(0));

        // Empty rowset
        assert_eq!(meta.avg_row_size(), 0);

        // With data
        meta.set_num_rows(100);
        meta.set_disk_sizes(1000, 0);
        assert_eq!(meta.avg_row_size(), 10);
    }
}
