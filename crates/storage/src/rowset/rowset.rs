// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Rowset
//!
//! Core Rowset structure managing Segment collection.
//!
//! ## Key Design
//!
//! - Rowset is a versioned collection of Segments
//! - Immutable once created (append-only)
//! - Reference counted for safe concurrent access
//! - State machine for lifecycle management (Prepared → Committed → Visible)
//!
//! ## Architecture
//!
//! ```text
//! Rowset
//! ├── RowsetMeta                    # Metadata (version, stats, etc.)
//! ├── TabletSchema                  # Schema reference
//! ├── Segments[]                    # Immutable segment files
//! │   ├── Segment 0
//! │   │   ├── ColumnReader[]
//! │   │   └── Indexes
//! │   └── Segment 1
//! │       └── ...
//! ├── State Machine                 # Lifecycle state
//! └── Reference Count               # For safe concurrent access
//! ```

use super::rowset_meta::{RowsetId, RowsetMeta, RowsetState, SegmentsOverlap};
use super::rowset_statistics::RowsetStatistics;
use super::segment::{Segment, SegmentIterator, SegmentOptions, SegmentSharedPtr};
use crate::index::{
    hnsw::{HnswSearchPolicy, ScoredPoint, SearchParams},
    PredicateTree,
};
use crate::primary_key::DeleteVector;
use crate::statistics::DeleteStatistics;
use crate::tablet::{ColumnId, TabletSchemaRef, Version};
use paro_common::error::{self as paro_error, Result};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Rowset state machine for lifecycle management
///
/// State transitions:
/// - Prepared → Committed (after flush)
/// - Committed → Visible (after publish)
/// - Visible → Deleting (during compaction cleanup)
/// - Deleting → Deleted (after file removal)
#[derive(Debug)]
struct RowsetStateMachine {
    state: RwLock<RowsetState>,
}

impl RowsetStateMachine {
    fn new(initial_state: RowsetState) -> Self {
        Self {
            state: RwLock::new(initial_state),
        }
    }

    fn state(&self) -> RowsetState {
        *self.state.read().unwrap()
    }

    fn transition_to(&self, new_state: RowsetState) -> Result<()> {
        let mut state = self.state.write().unwrap();
        let current = *state;

        // Validate state transition
        let valid = match (current, new_state) {
            (RowsetState::Prepared, RowsetState::Committed) => true,
            (RowsetState::Committed, RowsetState::Visible) => true,
            (RowsetState::Visible, RowsetState::Deleting) => true,
            (RowsetState::Deleting, RowsetState::Deleted) => true,
            // Allow same state (idempotent)
            (s1, s2) if s1 == s2 => true,
            _ => false,
        };

        if !valid {
            return Err(paro_error::invalid_input(format!(
                "Invalid state transition: {} → {}",
                current, new_state
            )));
        }

        *state = new_state;
        Ok(())
    }
}

/// Rowset is a versioned collection of Segments
///
/// A Rowset represents a batch of data written in a single transaction or
/// compaction operation. It contains one or more Segments, each of which
/// is an immutable columnar file.
///
/// ## Lifecycle
///
/// 1. Created by RowsetWriter during flush
/// 2. Transitions: Prepared → Committed → Visible
/// 3. Becomes invisible after compaction (replaced by merged rowset)
/// 4. Deleted after all readers release references
///
/// ## Thread Safety
///
/// - Rowset is immutable after creation (segments are append-only)
/// - Reference counting ensures safe concurrent access
/// - State machine is protected by RwLock
///
#[derive(Debug)]
pub struct Rowset {
    /// Schema reference (shared with Tablet)
    schema: TabletSchemaRef,

    /// Rowset data directory path
    rowset_path: PathBuf,

    /// Rowset metadata
    rowset_meta: RwLock<RowsetMeta>,

    /// Segment collection (ordered by segment_id)
    segments: RwLock<Vec<SegmentSharedPtr>>,

    /// State machine for lifecycle management
    state_machine: RowsetStateMachine,

    /// Reference count by readers
    /// Rowset cannot be deleted while refs > 0
    refs_by_reader: AtomicU64,

    /// Whether segments are loaded into memory
    segments_loaded: RwLock<bool>,

    /// Cached rowset statistics (lazy)
    statistics_cache: RwLock<Option<RowsetStatistics>>,
}

impl Rowset {
    /// Create a new Rowset
    ///
    /// This is the factory method for creating Rowsets. The rowset starts
    /// in Prepared state and must be committed before becoming visible.
    ///
    /// # Arguments
    /// * `schema` - Tablet schema reference
    /// * `rowset_meta` - Rowset metadata
    /// * `rowset_path` - Path to rowset data directory
    ///
    /// # Returns
    /// A new Rowset instance in Prepared state
    pub fn create(
        schema: TabletSchemaRef,
        rowset_meta: RowsetMeta,
        rowset_path: impl Into<PathBuf>,
    ) -> Result<Self> {
        let rowset_path = rowset_path.into();
        let initial_state = rowset_meta.rowset_state();

        Ok(Self {
            schema,
            rowset_path,
            rowset_meta: RwLock::new(rowset_meta),
            segments: RwLock::new(Vec::new()),
            state_machine: RowsetStateMachine::new(initial_state),
            refs_by_reader: AtomicU64::new(0),
            segments_loaded: RwLock::new(false),
            statistics_cache: RwLock::new(None),
        })
    }

    /// Create a Rowset with segments
    ///
    /// # Arguments
    /// * `schema` - Tablet schema reference
    /// * `rowset_meta` - Rowset metadata
    /// * `rowset_path` - Path to rowset data directory
    /// * `segments` - Pre-created segments
    pub fn create_with_segments(
        schema: TabletSchemaRef,
        rowset_meta: RowsetMeta,
        rowset_path: impl Into<PathBuf>,
        segments: Vec<SegmentSharedPtr>,
    ) -> Result<Self> {
        let rowset_path = rowset_path.into();
        let initial_state = rowset_meta.rowset_state();

        Ok(Self {
            schema,
            rowset_path,
            rowset_meta: RwLock::new(rowset_meta),
            segments: RwLock::new(segments),
            state_machine: RowsetStateMachine::new(initial_state),
            refs_by_reader: AtomicU64::new(0),
            segments_loaded: RwLock::new(true),
            statistics_cache: RwLock::new(None),
        })
    }

    // ==================== Getters ====================

    /// Get rowset ID
    pub fn rowset_id(&self) -> RowsetId {
        self.rowset_meta.read().unwrap().rowset_id()
    }

    /// Get tablet ID
    pub fn tablet_id(&self) -> u64 {
        self.rowset_meta.read().unwrap().tablet_id()
    }

    /// Get version range
    pub fn version(&self) -> Version {
        self.rowset_meta.read().unwrap().version()
    }

    /// Get start version
    pub fn start_version(&self) -> i64 {
        self.rowset_meta.read().unwrap().start_version()
    }

    /// Get end version
    pub fn end_version(&self) -> i64 {
        self.rowset_meta.read().unwrap().end_version()
    }

    /// Get rowset generation for cache isolation.
    pub fn rowset_gen(&self) -> u64 {
        self.rowset_meta.read().unwrap().rowset_gen()
    }

    /// Get number of rows
    pub fn num_rows(&self) -> u64 {
        self.rowset_meta.read().unwrap().num_rows()
    }

    /// Get number of segments
    pub fn num_segments(&self) -> u32 {
        self.segments.read().unwrap().len() as u32
    }

    /// Get total disk size
    pub fn total_disk_size(&self) -> u64 {
        self.rowset_meta.read().unwrap().total_disk_size()
    }

    /// Get data disk size
    pub fn data_disk_size(&self) -> u64 {
        self.rowset_meta.read().unwrap().data_disk_size()
    }

    /// Get index disk size
    pub fn index_disk_size(&self) -> u64 {
        self.rowset_meta.read().unwrap().index_disk_size()
    }

    /// Update delete vector statistics.
    pub fn add_delete_stats(&self, num_vectors: u32, num_deleted: u64) {
        let mut meta = self.rowset_meta.write().unwrap();
        let prev_vectors = meta.num_delete_vectors();
        let prev_deleted = meta.num_deleted_rows();
        let new_deleted = prev_deleted + num_deleted;
        meta.set_delete_info(prev_vectors + num_vectors, new_deleted);
        drop(meta);

        let mut cache = self.statistics_cache.write().unwrap();
        if let Some(stats) = cache.as_mut() {
            stats.set_delete_stats(DeleteStatistics::from_counts(stats.num_rows, new_deleted));
        }
    }

    /// Replace delete vector statistics with exact counts.
    pub fn set_delete_stats(&self, num_vectors: u32, num_deleted: u64) {
        let mut meta = self.rowset_meta.write().unwrap();
        meta.set_delete_info(num_vectors, num_deleted);
        drop(meta);

        let mut cache = self.statistics_cache.write().unwrap();
        if let Some(stats) = cache.as_mut() {
            stats.set_delete_stats(DeleteStatistics::from_counts(stats.num_rows, num_deleted));
        }
    }

    /// Invalidate delete-vector cache for a specific segment if loaded.
    pub fn invalidate_delete_vector_cache(&self, segment_id: u32) {
        if let Some(segment) = self.get_segment(segment_id) {
            segment.invalidate_delete_vector_cache();
        }
    }

    /// Get rowset state
    pub fn rowset_state(&self) -> RowsetState {
        self.state_machine.state()
    }

    /// Get segments overlap type
    pub fn segments_overlap(&self) -> SegmentsOverlap {
        self.rowset_meta.read().unwrap().segments_overlap()
    }

    /// Get schema reference
    pub fn schema(&self) -> &TabletSchemaRef {
        &self.schema
    }

    /// Get rowset path
    pub fn rowset_path(&self) -> &Path {
        &self.rowset_path
    }

    /// Get rowset metadata (clone)
    pub fn rowset_meta(&self) -> RowsetMeta {
        self.rowset_meta.read().unwrap().clone()
    }

    /// Update the rowset version (used during commit/publish).
    pub fn set_version(&self, version: Version) {
        let mut meta = self.rowset_meta.write().unwrap();
        meta.set_version(version);
    }

    /// Mark this rowset as compaction output and record source rowset IDs.
    pub fn mark_compaction_output(&self, source_ids: Vec<RowsetId>) {
        let mut meta = self.rowset_meta.write().unwrap();
        meta.set_compaction_output(source_ids);
    }

    /// Check if rowset is empty
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    /// Check if rowset is visible
    pub fn is_visible(&self) -> bool {
        self.rowset_state() == RowsetState::Visible
    }

    /// Check if rowset can be read
    pub fn is_readable(&self) -> bool {
        matches!(
            self.rowset_state(),
            RowsetState::Committed | RowsetState::Visible
        )
    }

    /// Check if this is a singleton version (start == end)
    pub fn is_singleton_delta(&self) -> bool {
        self.rowset_meta.read().unwrap().is_singleton_delta()
    }

    /// Get current reference count
    pub fn ref_count(&self) -> u64 {
        self.refs_by_reader.load(Ordering::Acquire)
    }

    // ==================== Segment Management ====================

    /// Load segments from disk
    ///
    /// This loads segment metadata and prepares them for reading.
    /// Actual column data is loaded lazily when accessed.
    pub fn load(&self) -> Result<()> {
        if *self.segments_loaded.read().unwrap() {
            return Ok(());
        }
        let mut loaded = self.segments_loaded.write().unwrap();
        self.load_segments_locked(&mut loaded)
    }

    fn load_segments_locked(&self, loaded: &mut bool) -> Result<()> {
        if *loaded {
            return Ok(());
        }

        let meta = self.rowset_meta.read().unwrap();
        let num_segments = meta.num_segments();
        let rowset_id = meta.rowset_id();
        let tablet_id = meta.tablet_id();
        let rowset_gen = meta.rowset_gen();
        let options = SegmentOptions::default();

        let mut segments = self.segments.write().unwrap();
        segments.clear();

        for seg_id in 0..num_segments {
            let segment_path = self.segment_path(seg_id);
            let segment = Segment::open(
                seg_id,
                segment_path,
                self.schema.clone(),
                options.clone(),
                tablet_id,
                rowset_id,
                rowset_gen,
            )?;
            segments.push(Arc::new(segment));
        }

        *loaded = true;
        Ok(())
    }

    /// Open an immutable segment view configured with the requested runtime
    /// resources.
    ///
    /// The rowset owns one structural view. Non-default views share its
    /// immutable footer, indexes and file descriptor, but own their PageReader
    /// and column-reader caches. They belong to the caller (normally a read
    /// lease) and are deliberately not retained by the rowset.
    pub fn open_segment_view(&self, options: SegmentOptions) -> Result<Vec<SegmentSharedPtr>> {
        self.load()?;
        if options.runtime_equivalent(&SegmentOptions::default()) {
            return Ok(self.segments.read().unwrap().clone());
        }
        Ok(self
            .segments
            .read()
            .unwrap()
            .iter()
            .map(|segment| Arc::new(segment.runtime_view(options.clone())))
            .collect())
    }

    /// Reload segments (force reload from disk)
    pub fn reload(&self) -> Result<()> {
        let mut loaded = self.segments_loaded.write().unwrap();
        {
            let segments = self.segments.read().unwrap();
            for segment in segments.iter() {
                segment.invalidate_delete_vector_cache();
            }
        }
        *loaded = false;
        self.invalidate_statistics();
        self.load_segments_locked(&mut loaded)
    }

    /// Get segment by ID
    pub fn get_segment(&self, segment_id: u32) -> Option<SegmentSharedPtr> {
        let segments = self.segments.read().unwrap();
        segments.get(segment_id as usize).cloned()
    }

    /// Get all segments
    pub fn segments(&self) -> Vec<SegmentSharedPtr> {
        self.segments.read().unwrap().clone()
    }

    /// Perform a vector search on this rowset.
    pub fn vector_search(
        &self,
        column_id: ColumnId,
        query: &[f32],
        top_k: usize,
        params: &SearchParams,
        policy: &HnswSearchPolicy,
        predicate_tree: Option<&PredicateTree>,
    ) -> Result<Vec<ScoredPoint>> {
        self.load()?;
        let segments = self.segments.read().unwrap();

        let mut all_results = Vec::new();
        let mut current_row_offset = 0;

        for segment in segments.iter() {
            let seg_results =
                segment.vector_search(column_id, query, top_k, params, policy, predicate_tree)?;
            for mut p in seg_results {
                p.idx += current_row_offset as u32;
                all_results.push(p);
            }
            current_row_offset += segment.num_rows();
        }

        // Sort by score (higher is better)
        all_results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        all_results.truncate(top_k);

        Ok(all_results)
    }

    /// Add a segment to the rowset
    ///
    /// This is used during rowset creation by RowsetWriter.
    /// Segments can only be added while rowset is in Prepared state.
    pub fn add_segment(&self, segment: SegmentSharedPtr) -> Result<()> {
        if self.rowset_state() != RowsetState::Prepared {
            return Err(paro_error::invalid_input(
                "Cannot add segment to non-prepared rowset",
            ));
        }

        let mut segments = self.segments.write().unwrap();
        segments.push(segment);

        // Update metadata
        let mut meta = self.rowset_meta.write().unwrap();
        meta.set_num_segments(segments.len() as u32);

        self.invalidate_statistics();

        Ok(())
    }

    /// Get segment file path
    fn segment_path(&self, segment_id: u32) -> PathBuf {
        self.rowset_path.join(format!("{}.dat", segment_id))
    }

    // ==================== Iterator Creation ====================

    /// Create a new iterator for reading this rowset
    ///
    /// The iterator merges data from all segments in order.
    ///
    /// # Returns
    /// A RowsetIterator for reading data
    pub fn new_iterator(&self) -> Result<RowsetIterator<'_>> {
        if !self.is_readable() {
            return Err(paro_error::invalid_input(format!(
                "Rowset {} is not readable (state: {})",
                self.rowset_id(),
                self.rowset_state()
            )));
        }

        // Ensure segments are loaded
        self.load()?;

        // Acquire reference
        self.acquire();

        Ok(RowsetIterator::new(self))
    }

    /// Get segment iterators for parallel reading
    ///
    /// Returns one iterator per segment for parallel processing.
    /// Useful for vectorized execution.
    pub fn get_segment_iterators(&self) -> Result<Vec<SegmentIterator>> {
        if !self.is_readable() {
            return Err(paro_error::invalid_input(format!(
                "Rowset {} is not readable",
                self.rowset_id()
            )));
        }

        self.load()?;

        let segments = self.segments.read().unwrap();
        let mut iterators = Vec::with_capacity(segments.len());

        for segment in segments.iter() {
            iterators.push(segment.new_iterator()?);
        }

        Ok(iterators)
    }

    // ==================== State Management ====================

    /// Make rowset visible for reads
    ///
    /// Transitions from Committed to Visible state.
    /// After this, the rowset can be read by queries.
    pub fn make_visible(&self) -> Result<()> {
        // First transition to Committed if in Prepared state
        if self.rowset_state() == RowsetState::Prepared {
            self.state_machine.transition_to(RowsetState::Committed)?;
        }

        // Then transition to Visible
        self.state_machine.transition_to(RowsetState::Visible)?;

        // Update metadata
        let mut meta = self.rowset_meta.write().unwrap();
        meta.set_rowset_state(RowsetState::Visible);

        Ok(())
    }

    /// Mark rowset for deletion
    ///
    /// Transitions to Deleting state. The rowset will be deleted
    /// once all readers release their references.
    pub fn mark_deleting(&self) -> Result<()> {
        self.state_machine.transition_to(RowsetState::Deleting)?;

        let mut meta = self.rowset_meta.write().unwrap();
        meta.set_rowset_state(RowsetState::Deleting);

        Ok(())
    }

    // ==================== Reference Counting ====================

    /// Acquire a reference to this rowset
    ///
    /// Increments the reference count. The rowset cannot be deleted
    /// while references are held.
    pub fn acquire(&self) {
        self.refs_by_reader.fetch_add(1, Ordering::AcqRel);
    }

    /// Release a reference to this rowset
    ///
    /// Decrements the reference count. When count reaches 0 and
    /// rowset is in Deleting state, it can be safely removed.
    pub fn release(&self) {
        let prev = self.refs_by_reader.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev > 0, "Reference count underflow");
    }

    /// Check if rowset can be safely deleted
    ///
    /// Returns true if rowset is in Deleting state and has no references.
    pub fn can_delete(&self) -> bool {
        self.rowset_state() == RowsetState::Deleting && self.ref_count() == 0
    }

    // ==================== Cleanup ====================

    /// Close the rowset and release resources
    ///
    /// This unloads segments from memory but does not delete files.
    pub fn close(&self) -> Result<()> {
        let mut loaded = self.segments_loaded.write().unwrap();
        let mut segments = self.segments.write().unwrap();
        segments.clear();
        *loaded = false;

        Ok(())
    }

    /// Remove rowset files from disk
    ///
    /// This deletes all segment files and the rowset directory.
    /// Should only be called when can_delete() returns true.
    pub fn remove(&self) -> Result<()> {
        if !self.can_delete() {
            return Err(paro_error::invalid_input(format!(
                "Cannot remove rowset {}: state={}, refs={}",
                self.rowset_id(),
                self.rowset_state(),
                self.ref_count()
            )));
        }

        // Close first
        self.close()?;

        // Delete segment files
        let meta = self.rowset_meta.read().unwrap();
        for seg_id in 0..meta.num_segments() {
            let segment_path = self.segment_path(seg_id);
            if segment_path.exists() {
                std::fs::remove_file(&segment_path).map_err(|e| {
                    paro_error::io_error(format!(
                        "Failed to remove segment file {:?}: {}",
                        segment_path, e
                    ))
                })?;
            }
        }

        // Delete rowset directory if empty
        if self.rowset_path.exists() {
            let _ = std::fs::remove_dir(&self.rowset_path);
        }

        // Update state
        self.state_machine.transition_to(RowsetState::Deleted)?;

        Ok(())
    }

    // ==================== Statistics ====================

    /// Update rowset statistics after writing
    pub fn update_stats(&self, num_rows: u64, data_size: u64, index_size: u64) {
        let mut meta = self.rowset_meta.write().unwrap();
        meta.set_num_rows(num_rows);
        meta.set_disk_sizes(data_size, index_size);
    }

    /// Get aggregated rowset statistics.
    pub fn statistics(&self) -> Result<RowsetStatistics> {
        {
            let cache = self.statistics_cache.read().unwrap();
            if let Some(stats) = cache.as_ref() {
                return Ok(stats.clone());
            }
        }

        self.load()?;
        let segments = self.segments.read().unwrap();
        let mut stats = RowsetStatistics::from_segments(&segments);
        let deleted_rows = self.rowset_meta.read().unwrap().num_deleted_rows();
        stats.set_delete_stats(DeleteStatistics::from_counts(stats.num_rows, deleted_rows));

        let mut cache = self.statistics_cache.write().unwrap();
        *cache = Some(stats.clone());
        Ok(stats)
    }

    pub(crate) fn set_statistics_cache(&self, stats: RowsetStatistics) {
        let mut cache = self.statistics_cache.write().unwrap();
        *cache = Some(stats);
    }

    fn invalidate_statistics(&self) {
        let mut cache = self.statistics_cache.write().unwrap();
        *cache = None;
    }

    /// Set segments overlap type
    pub fn set_segments_overlap(&self, overlap: SegmentsOverlap) {
        let mut meta = self.rowset_meta.write().unwrap();
        meta.set_segments_overlap(overlap);
    }

    /// Get compaction score
    pub fn get_compaction_score(&self) -> f64 {
        self.rowset_meta.read().unwrap().get_compaction_score()
    }

    /// Check if rowset needs compaction
    pub fn needs_compaction(&self) -> bool {
        self.rowset_meta.read().unwrap().needs_compaction()
    }
}

impl std::fmt::Display for Rowset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Rowset[id={}, version={}, rows={}, segments={}, state={}]",
            self.rowset_id(),
            self.version(),
            self.num_rows(),
            self.num_segments(),
            self.rowset_state()
        )
    }
}

/// Shared pointer to Rowset (thread-safe)
pub type RowsetSharedPtr = Arc<Rowset>;

/// Rowset iterator for reading data
///
/// The iterator merges data from all segments in the rowset.
#[derive(Debug)]
pub struct RowsetIterator<'a> {
    /// Reference to the rowset
    rowset: &'a Rowset,
    /// Current segment index
    current_segment: usize,
    /// Current row within segment
    current_row: u64,
    /// Total rows read
    rows_read: u64,
    /// Current segment iterator (lazy)
    current_iter: Option<SegmentIterator>,
}

impl<'a> RowsetIterator<'a> {
    fn new(rowset: &'a Rowset) -> Self {
        Self {
            rowset,
            current_segment: 0,
            current_row: 0,
            rows_read: 0,
            current_iter: None,
        }
    }

    /// Check if there are more rows to read
    pub fn has_next(&self) -> bool {
        self.rows_read < self.rowset.num_rows()
            && self.current_segment < self.rowset.num_segments() as usize
    }

    /// Get the next batch of rows
    ///
    pub fn next_batch(
        &mut self,
        batch_size: usize,
    ) -> Result<Option<(usize, Vec<(ColumnId, crate::rowset::column::ColumnBatch)>)>> {
        if !self.has_next() {
            return Ok(None);
        }

        loop {
            let segments = self.rowset.segments.read().unwrap();
            if self.current_segment >= segments.len() {
                return Ok(None);
            }

            if self.current_iter.is_none() {
                let segment = segments[self.current_segment].clone();
                let col_ids: Vec<ColumnId> = segment
                    .footer()
                    .column_metas
                    .iter()
                    .map(|m| m.column_id)
                    .collect();
                let dv = DeleteVector::load_from_dir_at_version(
                    self.rowset.rowset_path(),
                    self.current_segment as u32,
                    self.rowset.end_version(),
                )?;
                let iter = if let Some(dv) = dv {
                    SegmentIterator::new_with_delete_vector(&segment, col_ids, Some(dv))?
                } else {
                    SegmentIterator::new_with_delete_vector(&segment, col_ids, None)?
                };
                self.current_iter = Some(iter);
            }

            let iter = self.current_iter.as_mut().unwrap();

            if !iter.has_next() {
                self.current_segment += 1;
                self.current_row = 0;
                self.current_iter = None;
                continue;
            }

            let (rowids, batch) = iter.next_batch(batch_size)?;
            let rows_read = rowids.len();
            if rows_read == 0 || batch.is_empty() {
                self.current_segment += 1;
                self.current_row = 0;
                self.current_iter = None;
                continue;
            }

            self.current_row = iter.current_ordinal();
            self.rows_read += rows_read as u64;

            return Ok(Some((rows_read, batch)));
        }
    }

    /// Seek to a specific row ordinal
    pub fn seek_to_ordinal(&mut self, ordinal: u64) -> Result<()> {
        if ordinal >= self.rowset.num_rows() {
            return Err(paro_error::invalid_input(format!(
                "Ordinal {} out of range (max: {})",
                ordinal,
                self.rowset.num_rows()
            )));
        }

        // Find the segment containing this ordinal
        let segments = self.rowset.segments();
        let mut cumulative_rows = 0u64;

        for (idx, segment) in segments.iter().enumerate() {
            let seg_rows = segment.num_rows();

            if cumulative_rows + seg_rows > ordinal {
                self.current_segment = idx;
                self.current_row = ordinal - cumulative_rows;
                self.rows_read = ordinal;
                let mut iter = segment.new_iterator()?;
                iter.seek_to_ordinal(self.current_row)?;
                self.current_iter = Some(iter);
                return Ok(());
            }

            cumulative_rows += seg_rows;
        }

        Err(paro_error::internal("Failed to seek to ordinal"))
    }

    /// Get current row ordinal
    pub fn current_ordinal(&self) -> u64 {
        self.rows_read
    }

    /// Get total number of rows
    pub fn num_rows(&self) -> u64 {
        self.rowset.num_rows()
    }
}

impl<'a> Drop for RowsetIterator<'a> {
    fn drop(&mut self) {
        // Release reference when iterator is dropped
        self.rowset.release();
    }
}

/// Builder for creating Rowsets
///
/// Provides a fluent API for constructing Rowset instances.
#[derive(Debug)]
pub struct RowsetBuilder {
    schema: Option<TabletSchemaRef>,
    rowset_meta: Option<RowsetMeta>,
    rowset_path: Option<PathBuf>,
    segments: Vec<SegmentSharedPtr>,
}

impl RowsetBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            schema: None,
            rowset_meta: None,
            rowset_path: None,
            segments: Vec::new(),
        }
    }

    /// Set schema
    pub fn schema(mut self, schema: TabletSchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set rowset metadata
    pub fn rowset_meta(mut self, meta: RowsetMeta) -> Self {
        self.rowset_meta = Some(meta);
        self
    }

    /// Set rowset path
    pub fn rowset_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.rowset_path = Some(path.into());
        self
    }

    /// Add a segment
    pub fn add_segment(mut self, segment: SegmentSharedPtr) -> Self {
        self.segments.push(segment);
        self
    }

    /// Build the Rowset
    pub fn build(self) -> Result<Rowset> {
        let schema = self
            .schema
            .ok_or_else(|| paro_error::invalid_input("RowsetBuilder: schema is required"))?;

        let rowset_meta = self
            .rowset_meta
            .ok_or_else(|| paro_error::invalid_input("RowsetBuilder: rowset_meta is required"))?;

        let rowset_path = self
            .rowset_path
            .ok_or_else(|| paro_error::invalid_input("RowsetBuilder: rowset_path is required"))?;

        if self.segments.is_empty() {
            Rowset::create(schema, rowset_meta, rowset_path)
        } else {
            Rowset::create_with_segments(schema, rowset_meta, rowset_path, self.segments)
        }
    }
}

impl Default for RowsetBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{BufferPool, PageCache};
    use crate::rowset::encoding::FieldType;
    use crate::rowset::rowset_meta::RowsetMetaBuilder;
    use crate::rowset::segment::{
        ColumnData, ColumnMeta, SegmentFooter, SegmentWriter, SegmentWriterOptions,
    };
    use crate::rowset::CompressionType;
    use crate::tablet::tablet_schema::{KeysType, TabletColumn, TabletSchema};
    use paro_common::types::LogicalType;

    fn create_test_schema() -> TabletSchemaRef {
        let columns = vec![
            TabletColumn::key(0, "id", LogicalType::BigInt),
            TabletColumn::new(1, "name", LogicalType::Varchar),
            TabletColumn::new(2, "value", LogicalType::Integer),
        ];
        Arc::new(TabletSchema::new(1, columns, KeysType::PrimaryKeys).unwrap())
    }

    fn create_test_rowset_meta(rowset_id: RowsetId, tablet_id: u64, version: i64) -> RowsetMeta {
        RowsetMetaBuilder::with_id(rowset_id, tablet_id, Version::singleton(version))
            .num_rows(1000)
            .num_segments(2)
            .disk_sizes(1024 * 1024, 64 * 1024)
            .state(RowsetState::Prepared)
            .build()
    }

    fn create_test_segment(
        schema: &TabletSchemaRef,
        segment_id: u32,
        num_rows: u64,
    ) -> SegmentSharedPtr {
        let column_metas = vec![ColumnMeta::new(0, FieldType::BigInt)];
        let footer = SegmentFooter::new(num_rows, column_metas);
        Arc::new(Segment::from_footer(
            segment_id,
            format!("/tmp/segment_{}.dat", segment_id),
            schema.clone(),
            footer,
            SegmentOptions::default(),
            0,
            0,
            0,
        ))
    }

    fn create_segment_with_path(
        schema: &TabletSchemaRef,
        segment_id: u32,
        num_rows: u64,
        path: impl Into<PathBuf>,
    ) -> SegmentSharedPtr {
        let path = path.into();
        let opts = SegmentWriterOptions::new(segment_id)
            .with_short_key_index(false)
            .with_compression(CompressionType::None);
        let mut writer = SegmentWriter::create(schema.clone(), &path, opts).unwrap();

        let num_rows_u32 = num_rows as u32;
        // key column (id)
        let col0_data: Vec<u8> = (0..num_rows as i64).flat_map(|v| v.to_le_bytes()).collect();
        // varchar column: store empty strings (length prefix 0)
        let col1_data: Vec<u8> = (0..num_rows_u32).flat_map(|_| 0u32.to_le_bytes()).collect();
        // value column: simple incremental i32
        let col2_data: Vec<u8> = (0..num_rows_u32)
            .flat_map(|v| (v as i32).to_le_bytes())
            .collect();

        let columns = vec![
            ColumnData::new(col0_data, num_rows_u32),
            ColumnData::new(col1_data, num_rows_u32),
            ColumnData::new(col2_data, num_rows_u32),
        ];
        writer.append_chunk(&columns).unwrap();
        writer.finalize().unwrap();

        Arc::new(
            Segment::open(
                segment_id,
                &path,
                schema.clone(),
                SegmentOptions::default().with_verify_checksum(false),
                0,
                0,
                0,
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_rowset_create() {
        let schema = create_test_schema();
        let meta = create_test_rowset_meta(1, 100, 0);
        let rowset = Rowset::create(schema, meta, "/tmp/rowset_1").unwrap();

        assert_eq!(rowset.rowset_id(), 1);
        assert_eq!(rowset.tablet_id(), 100);
        assert_eq!(rowset.start_version(), 0);
        assert_eq!(rowset.end_version(), 0);
        assert_eq!(rowset.num_rows(), 1000);
        assert_eq!(rowset.rowset_state(), RowsetState::Prepared);
        assert!(!rowset.is_visible());
        assert!(!rowset.is_readable());
    }

    #[test]
    fn test_rowset_lifecycle() {
        let schema = create_test_schema();
        let meta = create_test_rowset_meta(1, 100, 0);
        let rowset = Rowset::create(schema, meta, "/tmp/rowset_1").unwrap();

        // Initial state: Prepared
        assert_eq!(rowset.rowset_state(), RowsetState::Prepared);
        assert!(!rowset.is_readable());

        // Transition to Visible
        rowset.make_visible().unwrap();
        assert_eq!(rowset.rowset_state(), RowsetState::Visible);
        assert!(rowset.is_visible());
        assert!(rowset.is_readable());

        // Mark for deletion
        rowset.mark_deleting().unwrap();
        assert_eq!(rowset.rowset_state(), RowsetState::Deleting);
        assert!(!rowset.is_visible());
        assert!(!rowset.is_readable());
    }

    #[test]
    fn test_rowset_reference_counting() {
        let schema = create_test_schema();
        let meta = create_test_rowset_meta(1, 100, 0);
        let rowset = Rowset::create(schema, meta, "/tmp/rowset_1").unwrap();

        assert_eq!(rowset.ref_count(), 0);

        rowset.acquire();
        assert_eq!(rowset.ref_count(), 1);

        rowset.acquire();
        assert_eq!(rowset.ref_count(), 2);

        rowset.release();
        assert_eq!(rowset.ref_count(), 1);

        rowset.release();
        assert_eq!(rowset.ref_count(), 0);
    }

    #[test]
    fn test_rowset_can_delete() {
        let schema = create_test_schema();
        let meta = create_test_rowset_meta(1, 100, 0);
        let rowset = Rowset::create(schema, meta, "/tmp/rowset_1").unwrap();

        // Cannot delete in Prepared state
        assert!(!rowset.can_delete());

        // Make visible first
        rowset.make_visible().unwrap();
        assert!(!rowset.can_delete());

        // Mark for deletion
        rowset.mark_deleting().unwrap();
        assert!(rowset.can_delete());

        // Acquire reference - cannot delete
        rowset.acquire();
        assert!(!rowset.can_delete());

        // Release reference - can delete again
        rowset.release();
        assert!(rowset.can_delete());
    }

    #[test]
    fn test_rowset_add_segment() {
        let schema = create_test_schema();
        let meta = RowsetMetaBuilder::with_id(1, 100, Version::singleton(0))
            .state(RowsetState::Prepared)
            .build();
        let rowset = Rowset::create(schema.clone(), meta, "/tmp/rowset_1").unwrap();

        assert_eq!(rowset.num_segments(), 0);

        // Add segments
        let seg1 = create_test_segment(&schema, 0, 1000);
        let seg2 = create_test_segment(&schema, 1, 1000);

        rowset.add_segment(seg1).unwrap();
        assert_eq!(rowset.num_segments(), 1);

        rowset.add_segment(seg2).unwrap();
        assert_eq!(rowset.num_segments(), 2);

        // Cannot add segment after making visible
        rowset.make_visible().unwrap();
        let seg3 = create_test_segment(&schema, 2, 1000);
        assert!(rowset.add_segment(seg3).is_err());
    }

    #[test]
    fn test_rowset_with_segments() {
        let schema = create_test_schema();
        let meta = RowsetMetaBuilder::with_id(1, 100, Version::singleton(0))
            .num_rows(2000)
            .state(RowsetState::Committed)
            .build();

        let segments = vec![
            create_test_segment(&schema, 0, 1000),
            create_test_segment(&schema, 1, 1000),
        ];

        let rowset = Rowset::create_with_segments(schema, meta, "/tmp/rowset_1", segments).unwrap();

        assert_eq!(rowset.num_segments(), 2);
        assert_eq!(rowset.rowset_state(), RowsetState::Committed);
        assert!(rowset.is_readable());
    }

    #[test]
    fn nondefault_segment_views_are_owned_by_the_read_lease() {
        let schema = create_test_schema();
        let tmp = tempfile::tempdir().unwrap();
        let rowset_dir = tmp.path().join("rowset");
        std::fs::create_dir_all(&rowset_dir).unwrap();
        create_segment_with_path(&schema, 0, 3, rowset_dir.join("0.dat"));

        let meta = RowsetMetaBuilder::with_id(1, 100, Version::singleton(0))
            .num_rows(3)
            .num_segments(1)
            .state(RowsetState::Visible)
            .build();
        let rowset = Rowset::create(schema, meta, rowset_dir).unwrap();
        rowset.load().unwrap();
        let original = rowset.get_segment(0).unwrap();

        let cache = Arc::new(PageCache::new(BufferPool::new_arc(1024 * 1024)));
        let first_options = SegmentOptions::default()
            .with_verify_checksum(false)
            .with_page_cache(cache.clone())
            .with_cache_decoded(true);
        let reopened_segments = rowset.open_segment_view(first_options.clone()).unwrap();

        let reopened = reopened_segments[0].clone();
        let reopened_weak = Arc::downgrade(&reopened);
        assert!(!Arc::ptr_eq(&original, &reopened));
        assert!(original.shares_structural_state_with(&reopened));
        assert!(original.uses_page_cache(None));
        assert!(reopened.uses_page_cache(Some(&cache)));
        let independently_opened = rowset.open_segment_view(first_options).unwrap();
        assert!(!Arc::ptr_eq(&reopened, &independently_opened[0]));

        let second_cache = Arc::new(PageCache::new(BufferPool::new_arc(1024 * 1024)));
        let second_options = SegmentOptions::default()
            .with_page_cache(second_cache)
            .with_cache_decoded(true);
        let second_view = rowset.open_segment_view(second_options).unwrap();
        assert!(!Arc::ptr_eq(&reopened, &second_view[0]));

        assert!(Arc::ptr_eq(&original, &rowset.get_segment(0).unwrap()));
        drop(reopened);
        drop(reopened_segments);
        assert!(
            reopened_weak.upgrade().is_none(),
            "the rowset must not retain a query-owned segment view"
        );
    }

    #[test]
    fn test_rowset_iterator() {
        let schema = create_test_schema();
        let meta = RowsetMetaBuilder::with_id(1, 100, Version::singleton(0))
            .num_rows(100)
            .num_segments(1)
            .state(RowsetState::Visible)
            .build();

        let segment = create_test_segment(&schema, 0, 100);
        let rowset =
            Rowset::create_with_segments(schema, meta, "/tmp/rowset_1", vec![segment]).unwrap();

        // Create iterator
        let iter = rowset.new_iterator().unwrap();
        assert!(iter.has_next());
        assert_eq!(iter.num_rows(), 100);
        assert_eq!(iter.current_ordinal(), 0);

        // Reference count should be incremented
        assert_eq!(rowset.ref_count(), 1);

        // Drop iterator - reference count should decrement
        drop(iter);
        assert_eq!(rowset.ref_count(), 0);
    }

    #[test]
    fn test_rowset_iterator_not_readable() {
        let schema = create_test_schema();
        let meta = RowsetMetaBuilder::with_id(1, 100, Version::singleton(0))
            .state(RowsetState::Prepared)
            .build();

        let rowset = Rowset::create(schema, meta, "/tmp/rowset_1").unwrap();

        // Cannot create iterator for non-readable rowset
        assert!(rowset.new_iterator().is_err());
    }

    #[test]
    fn test_rowset_iterator_applies_delete_vector() {
        let schema = create_test_schema();
        let tmp = tempfile::tempdir().unwrap();
        let rowset_dir = tmp.path().join("rowset");
        std::fs::create_dir_all(&rowset_dir).unwrap();

        let segment_path = rowset_dir.join("0.dat");
        let segment = create_segment_with_path(&schema, 0, 3, &segment_path);

        // Write delete vector marking middle row
        let mut dv = DeleteVector::new();
        dv.mark_deleted(crate::rowset::SegmentRowId::from_raw(1));
        dv.save_to_dir(&rowset_dir, 0).unwrap();

        let meta = RowsetMetaBuilder::with_id(1, 100, Version::singleton(0))
            .num_rows(3)
            .num_segments(1)
            .state(RowsetState::Visible)
            .build();

        let rowset =
            Rowset::create_with_segments(schema, meta, &rowset_dir, vec![segment]).unwrap();
        let mut iter = rowset.new_iterator().unwrap();
        let (rows, batch) = iter.next_batch(10).unwrap().unwrap();
        // Only 2 rows should remain, each i64 -> 8 bytes
        assert_eq!(batch[0].1.data.len(), 16);
        assert_eq!(rows, 2);
    }

    #[test]
    fn test_rowset_state_machine_invalid_transition() {
        let schema = create_test_schema();
        let meta = RowsetMetaBuilder::with_id(1, 100, Version::singleton(0))
            .state(RowsetState::Prepared)
            .build();

        let rowset = Rowset::create(schema, meta, "/tmp/rowset_1").unwrap();

        // Cannot transition directly to Deleting from Prepared
        assert!(rowset.mark_deleting().is_err());
    }

    #[test]
    fn test_rowset_update_stats() {
        let schema = create_test_schema();
        let meta = RowsetMetaBuilder::with_id(1, 100, Version::singleton(0)).build();
        let rowset = Rowset::create(schema, meta, "/tmp/rowset_1").unwrap();

        rowset.update_stats(5000, 2 * 1024 * 1024, 128 * 1024);

        assert_eq!(rowset.num_rows(), 5000);
        assert_eq!(rowset.data_disk_size(), 2 * 1024 * 1024);
        assert_eq!(rowset.index_disk_size(), 128 * 1024);
        assert_eq!(rowset.total_disk_size(), 2 * 1024 * 1024 + 128 * 1024);
    }

    #[test]
    fn test_rowset_builder() {
        let schema = create_test_schema();
        let meta = create_test_rowset_meta(1, 100, 0);

        let rowset = RowsetBuilder::new()
            .schema(schema)
            .rowset_meta(meta)
            .rowset_path("/tmp/rowset_1")
            .build()
            .unwrap();

        assert_eq!(rowset.rowset_id(), 1);
        assert_eq!(rowset.tablet_id(), 100);
    }

    #[test]
    fn test_rowset_builder_missing_fields() {
        // Missing schema
        let result = RowsetBuilder::new()
            .rowset_meta(create_test_rowset_meta(1, 100, 0))
            .rowset_path("/tmp/rowset_1")
            .build();
        assert!(result.is_err());

        // Missing meta
        let result = RowsetBuilder::new()
            .schema(create_test_schema())
            .rowset_path("/tmp/rowset_1")
            .build();
        assert!(result.is_err());

        // Missing path
        let result = RowsetBuilder::new()
            .schema(create_test_schema())
            .rowset_meta(create_test_rowset_meta(1, 100, 0))
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_rowset_display() {
        let schema = create_test_schema();
        let meta = RowsetMetaBuilder::with_id(1, 100, Version::new(0, 5))
            .num_rows(1000)
            .num_segments(3)
            .state(RowsetState::Visible)
            .build();

        let rowset = Rowset::create(schema, meta, "/tmp/rowset_1").unwrap();
        rowset.make_visible().unwrap();

        let display = format!("{}", rowset);
        assert!(display.contains("id=1"));
        assert!(display.contains("rows=1000"));
        assert!(display.contains("VISIBLE"));
    }

    #[test]
    fn test_rowset_compaction_score() {
        let schema = create_test_schema();
        let meta = RowsetMetaBuilder::with_id(1, 100, Version::singleton(0))
            .num_rows(1000)
            .num_segments(5)
            .build();

        let rowset = Rowset::create(schema, meta, "/tmp/rowset_1").unwrap();

        // Should have a positive compaction score due to multiple segments
        let score = rowset.get_compaction_score();
        assert!(score > 0.0);
    }

    #[test]
    fn test_rowset_needs_compaction() {
        let schema = create_test_schema();

        // Single segment - no compaction needed
        let meta1 = RowsetMetaBuilder::with_id(1, 100, Version::singleton(0))
            .num_segments(1)
            .build();
        let rowset1 = Rowset::create(schema.clone(), meta1, "/tmp/rowset_1").unwrap();
        assert!(!rowset1.needs_compaction());

        // Multiple segments - compaction needed
        let meta2 = RowsetMetaBuilder::with_id(2, 100, Version::singleton(1))
            .num_segments(3)
            .build();
        let rowset2 = Rowset::create(schema, meta2, "/tmp/rowset_2").unwrap();
        assert!(rowset2.needs_compaction());
    }

    #[test]
    fn test_rowset_close() {
        let schema = create_test_schema();
        let meta = RowsetMetaBuilder::with_id(1, 100, Version::singleton(0))
            .num_segments(2)
            .build();

        let seg1 = create_test_segment(&schema, 0, 1000);
        let seg2 = create_test_segment(&schema, 1, 1000);

        let rowset =
            Rowset::create_with_segments(schema, meta, "/tmp/rowset_1", vec![seg1, seg2]).unwrap();

        assert_eq!(rowset.num_segments(), 2);

        rowset.close().unwrap();

        // Segments should be cleared
        assert_eq!(rowset.segments().len(), 0);
    }

    #[test]
    fn test_rowset_is_singleton_delta() {
        let schema = create_test_schema();

        // Singleton version
        let meta1 = RowsetMetaBuilder::with_id(1, 100, Version::singleton(5)).build();
        let rowset1 = Rowset::create(schema.clone(), meta1, "/tmp/rowset_1").unwrap();
        assert!(rowset1.is_singleton_delta());

        // Range version
        let meta2 = RowsetMetaBuilder::with_id(2, 100, Version::new(0, 5)).build();
        let rowset2 = Rowset::create(schema, meta2, "/tmp/rowset_2").unwrap();
        assert!(!rowset2.is_singleton_delta());
    }
}
