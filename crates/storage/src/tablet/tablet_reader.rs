//! # TabletReader
//!
//! Cross-Rowset merge reader for Tablet data.
//!
//! ## Key Design
//!
//! - Reads data from multiple Rowsets with version-based visibility
//! - Merges data from overlapping Rowsets (for compacted versions)
//! - Supports column projection and predicate pushdown
//! - Returns Chunks for pipeline execution

pub use super::tablet_reader_params::{ColumnProjection, TabletReaderBuilder, TabletReaderParams};
use super::tablet_runtime::{TabletReadGuard, TabletRef};
use super::tablet_schema::TabletSchemaRef;
use crate::primary_key::DeleteVector;
use crate::rowset::segment::SegmentIterator;
use crate::rowset::RowsetSharedPtr;
use crate::tablet::ColumnId;
use paro_common::allocator::{default_allocator, Allocator};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use std::sync::Arc;

use crate::index::collect_predicate_columns;

/// Reader state for iterating through Rowsets
#[derive(Debug)]
struct ReaderState {
    /// Current rowset index
    current_rowset_idx: usize,

    /// Total rows read
    rows_read: u64,

    /// Whether reading is complete
    is_finished: bool,
}

/// Cursor for iterating through a single Rowset
#[derive(Debug)]
struct RowsetCursor {
    /// Rowset being read
    rowset: RowsetSharedPtr,
    /// Segment iterators for this rowset
    segment_iters: Vec<SegmentIterator>,
    /// Current segment index
    current_seg_idx: usize,
}

impl RowsetCursor {
    fn new(
        rowset: RowsetSharedPtr,
        projection: &[ColumnId],
        params: &TabletReaderParams,
    ) -> Result<Self> {
        // Ensure segments are loaded and protect rowset from deletion during read
        if let Some(opts) = &params.segment_options {
            rowset.load_with_options(opts.clone())?;
        } else {
            rowset.load()?;
        }
        rowset.acquire();

        let segments = rowset.segments();
        let mut segment_iters = Vec::with_capacity(segments.len());
        for seg in segments {
            if let Some(target_seg_id) = params.segment_id {
                if seg.segment_id() != target_seg_id {
                    continue;
                }
            }

            let col_ids: Vec<ColumnId> = if params.projection.is_none() && projection.is_empty() {
                seg.footer()
                    .column_metas
                    .iter()
                    .map(|m| m.column_id)
                    .collect()
            } else {
                projection
                    .iter()
                    .copied()
                    .filter(|cid| seg.get_column_meta(*cid).is_some())
                    .collect()
            };
            let delete_vector = DeleteVector::load_from_dir_at_version(
                rowset.rowset_path(),
                seg.segment_id(),
                params.version,
            )?;

            let use_late_materialize = params.late_materialize && params.predicate_tree.is_some();
            let predicate_columns = if use_late_materialize {
                if let Some(cols) = &params.predicate_columns {
                    cols.clone()
                } else {
                    collect_predicate_columns(params.predicate_tree.as_ref().unwrap())
                }
            } else {
                Vec::new()
            };

            let iter = if use_late_materialize && !predicate_columns.is_empty() {
                SegmentIterator::new_with_delete_vector_predicate_and_prefetcher_late_materialize(
                    &seg,
                    col_ids,
                    predicate_columns,
                    delete_vector,
                    params.predicate_tree.clone(),
                    params.prefetcher.clone(),
                )?
            } else {
                SegmentIterator::new_with_delete_vector_predicate_and_prefetcher(
                    &seg,
                    col_ids,
                    delete_vector,
                    params.predicate_tree.clone(),
                    params.prefetcher.clone(),
                )?
            };
            if iter.num_columns() == 0 && !params.emit_row_id {
                continue;
            };
            segment_iters.push(iter);
        }

        Ok(Self {
            rowset,
            segment_iters,
            current_seg_idx: 0,
        })
    }

    fn next_iter(&mut self) -> Option<&mut SegmentIterator> {
        self.segment_iters.get_mut(self.current_seg_idx)
    }

    fn advance_segment(&mut self) {
        self.current_seg_idx += 1;
    }

    fn is_finished(&self) -> bool {
        self.current_seg_idx >= self.segment_iters.len()
    }
}

impl Drop for RowsetCursor {
    fn drop(&mut self) {
        // Balance the acquire() in new()
        self.rowset.release();
    }
}

impl ReaderState {
    fn new() -> Self {
        Self {
            current_rowset_idx: 0,
            rows_read: 0,
            is_finished: false,
        }
    }
}

/// TabletReader reads data from a Tablet with version-based MVCC
///
/// ## Usage
///
/// ```ignore
/// let reader = TabletReader::new(tablet, params)?;
/// reader.prepare()?;
/// while let Some(chunk) = reader.get_next_chunk()? {
///     // Process chunk
/// }
/// reader.close();
/// ```
///
pub struct TabletReader {
    /// Reference to the tablet being read
    pub(super) tablet: TabletRef,

    /// Reader parameters
    pub(super) params: TabletReaderParams,

    /// Schema for the tablet
    pub(super) schema: TabletSchemaRef,

    /// Rowsets to read (captured at prepare time)
    pub(super) rowsets: Vec<RowsetSharedPtr>,

    /// Column types for output
    pub(super) output_types: Vec<LogicalType>,

    /// Column types for deduped read columns
    pub(super) read_types: Vec<LogicalType>,

    /// Column projection for reading (column IDs in read order)
    pub(super) projection: Vec<ColumnId>,

    /// Output column mapping (output idx -> read idx)
    pub(super) output_to_read: Vec<usize>,

    /// Reader state
    state: ReaderState,

    /// Current rowset cursor (constructed lazily)
    current_cursor: Option<RowsetCursor>,

    /// Whether the reader has been prepared
    pub(super) is_prepared: bool,

    /// Allocator used to materialize output vectors/chunks.
    pub(super) allocator: Arc<dyn Allocator>,

    /// Snapshot guard pinned for the full reader lifetime.
    snapshot_guard: Option<TabletReadGuard>,
}

impl std::fmt::Debug for TabletReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TabletReader")
            .field("params", &self.params)
            .field("schema", &self.schema)
            .field("rowsets", &self.rowsets.len())
            .field("output_types", &self.output_types)
            .field("read_types", &self.read_types)
            .field("projection", &self.projection)
            .field("output_to_read", &self.output_to_read)
            .field("state", &self.state)
            .field("current_cursor", &self.current_cursor)
            .field("is_prepared", &self.is_prepared)
            .field("allocator", &self.allocator.name())
            .field("snapshot_guard", &self.snapshot_guard)
            .finish()
    }
}

impl TabletReader {
    /// Create a new TabletReader
    ///
    /// # Arguments
    /// * `tablet` - The tablet to read from
    /// * `params` - Reader parameters
    ///
    /// # Returns
    /// A new TabletReader, or error if schema is not available
    pub fn new(tablet: TabletRef, params: TabletReaderParams) -> Result<Self> {
        Self::new_with_allocator(tablet, params, Arc::new(default_allocator()))
    }

    /// Create a new TabletReader with explicit allocator for chunk/vector materialization.
    pub fn new_with_allocator(
        tablet: TabletRef,
        params: TabletReaderParams,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        let schema = tablet
            .schema()
            .ok_or_else(|| paro_error::internal("Tablet schema not available"))?;

        let total_columns = schema.columns().len();
        let output_columns = if let Some(proj) = &params.projection {
            proj.output_columns().to_vec()
        } else if let Some(cols) = &params.columns {
            if cols.is_empty() {
                (0..total_columns).collect()
            } else {
                cols.clone()
            }
        } else {
            (0..total_columns).collect()
        };

        let column_projection = if let Some(proj) = &params.projection {
            proj.clone()
        } else {
            ColumnProjection::new(output_columns.clone())
        };

        // Determine output types based on output columns
        let mut output_types = output_columns
            .iter()
            .map(|&idx| {
                schema
                    .column(idx)
                    .map(|c| c.logical_type.clone())
                    .ok_or_else(|| paro_error::invalid_input("Column index out of range"))
            })
            .collect::<Result<Vec<_>>>()?;
        if params.emit_row_id {
            output_types.push(LogicalType::BigInt);
        }

        // Determine read types based on deduped read columns
        let read_types = column_projection
            .read_columns()
            .iter()
            .map(|&idx| {
                schema
                    .column(idx)
                    .map(|c| c.logical_type.clone())
                    .ok_or_else(|| paro_error::invalid_input("Column index out of range"))
            })
            .collect::<Result<Vec<_>>>()?;

        // Column projection in column-id order (deduped read columns)
        let projection = column_projection
            .read_columns()
            .iter()
            .map(|&idx| {
                schema
                    .column(idx)
                    .map(|c| c.id)
                    .ok_or_else(|| paro_error::invalid_input("Column index out of range"))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            tablet,
            params,
            schema,
            rowsets: Vec::new(),
            output_types,
            read_types,
            projection,
            output_to_read: column_projection.output_to_read().to_vec(),
            state: ReaderState::new(),
            current_cursor: None,
            is_prepared: false,
            allocator,
            snapshot_guard: None,
        })
    }

    /// Prepare the reader for reading
    ///
    /// This captures the consistent set of Rowsets based on the version parameter.
    /// Must be called before `get_next_chunk()`.
    pub fn prepare(&mut self) -> Result<()> {
        if self.is_prepared {
            return Ok(());
        }

        let snapshot_guard = TabletReadGuard::pin(&self.tablet, self.params.version);

        // Capture consistent rowsets at the specified version
        self.rowsets = self
            .tablet
            .capture_consistent_rowsets(self.params.version)?;

        // Sort rowsets by version for ordered reading
        self.rowsets.sort_by_key(|a| a.version());

        self.snapshot_guard = Some(snapshot_guard);
        self.is_prepared = true;
        Ok(())
    }

    /// Prepare the reader with a specific list of Rowsets.
    pub fn prepare_with_rowsets(&mut self, rowsets: Vec<RowsetSharedPtr>) -> Result<()> {
        if self.snapshot_guard.is_none() {
            self.snapshot_guard = Some(TabletReadGuard::pin(&self.tablet, self.params.version));
        }
        self.rowsets = rowsets;
        // Keep rowsets sorted by version for consistency with standard prepare
        self.rowsets.sort_by_key(|a| a.version());
        self.is_prepared = true;
        Ok(())
    }

    /// Get the next chunk of data
    ///
    /// # Returns
    /// * `Ok(Some(chunk))` - Next chunk of data
    /// * `Ok(None)` - No more data
    /// * `Err(e)` - Error occurred
    ///
    /// The reader advances rowset-by-rowset and segment-by-segment until it
    /// can materialize the next visible chunk for the requested snapshot.
    pub fn get_next_chunk(&mut self) -> Result<Option<Chunk>> {
        if !self.is_prepared {
            return Err(paro_error::internal("TabletReader not prepared"));
        }

        if self.state.is_finished {
            return Ok(None);
        }

        loop {
            // Create cursor for current rowset if needed
            if self.current_cursor.is_none() {
                if self.state.current_rowset_idx >= self.rowsets.len() {
                    self.state.is_finished = true;
                    return Ok(None);
                }

                let rowset = self.rowsets[self.state.current_rowset_idx].clone();
                let cursor = RowsetCursor::new(rowset, &self.projection, &self.params)?;
                self.current_cursor = Some(cursor);
            }

            // Fetch next batch from current rowset/segment
            let (rowids, batch_v, segment_finished, rowset_id, segment_id) = {
                let cursor = self.current_cursor.as_mut().unwrap();
                let rowset_id = cursor.rowset.rowset_id();

                if cursor.is_finished() {
                    (Vec::new(), Vec::new(), true, rowset_id, 0)
                } else {
                    let iter = cursor.next_iter().expect("segment iterator must exist");
                    let segment_id = iter.segment_id();
                    let (rowids, batch) = iter.next_batch(self.params.batch_size)?;
                    let finished = !iter.has_next();
                    (rowids, batch, finished, rowset_id, segment_id)
                }
            };

            let rows_read = rowids.len();
            let batch = batch_v;

            // If no more segments in this rowset, advance to next rowset
            if rows_read == 0 && batch.is_empty() && segment_finished {
                self.state.current_rowset_idx += 1;
                self.current_cursor = None;
                continue;
            }

            // Infer row count (verifies against expected)
            let rows = self.infer_row_count(&batch, rows_read)?;

            if rows == 0 {
                // Empty batch – advance segment and continue
                if let Some(cursor) = self.current_cursor.as_mut() {
                    cursor.advance_segment();
                    if cursor.is_finished() {
                        self.state.current_rowset_idx += 1;
                        self.current_cursor = None;
                    }
                }
                continue;
            }

            let chunk = self.build_chunk(&batch, rows, &rowids, rowset_id, segment_id)?;

            self.state.rows_read += rows as u64;

            if segment_finished {
                if let Some(cursor) = self.current_cursor.as_mut() {
                    cursor.advance_segment();
                    if cursor.is_finished() {
                        self.state.current_rowset_idx += 1;
                        self.current_cursor = None;
                    }
                }
            }

            return Ok(Some(chunk));
        }
    }

    /// Close the reader and release resources
    pub fn close(&mut self) {
        self.rowsets.clear();
        self.state.is_finished = true;
        self.current_cursor = None;
        self.snapshot_guard = None;
    }

    // ==================== Getters ====================

    /// Get the schema
    pub fn schema(&self) -> &TabletSchemaRef {
        &self.schema
    }

    /// Get output column types
    pub fn output_types(&self) -> &[LogicalType] {
        &self.output_types
    }

    /// Get number of rowsets to read
    pub fn num_rowsets(&self) -> usize {
        self.rowsets.len()
    }

    /// Get total rows read so far
    pub fn rows_read(&self) -> u64 {
        self.state.rows_read
    }

    /// Check if reading is finished
    pub fn is_finished(&self) -> bool {
        self.state.is_finished
    }

    /// Get the version being read
    pub fn version(&self) -> i64 {
        self.params.version
    }
}

impl Drop for TabletReader {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
#[path = "tablet_reader_tests.rs"]
mod tests;
