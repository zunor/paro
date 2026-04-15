// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::table_handle::TableHandle;
use crate::rowset::segment::{SegmentOptions, SegmentSharedPtr};
use crate::rowset::RowsetSharedPtr;
use crate::tablet::tablet_reader::{TabletReader, TabletReaderParams};
use crate::tablet::TabletReadGuard;
use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use std::sync::Arc;

impl TableHandle {
    /// Create a TabletReader with the given parameters.
    pub fn create_reader(&self, params: TabletReaderParams) -> Result<TabletReader> {
        TabletReader::new(self.tablet(), params)
    }

    /// Create a TabletReader with explicit allocator for chunk/vector materialization.
    pub fn create_reader_with_allocator(
        &self,
        params: TabletReaderParams,
        allocator: Arc<dyn Allocator>,
    ) -> Result<TabletReader> {
        TabletReader::new_with_allocator(self.tablet(), params, allocator)
    }

    /// Collect all segments visible at the given version.
    pub fn collect_segments(
        &self,
        version: i64,
    ) -> Result<Vec<(RowsetSharedPtr, SegmentSharedPtr)>> {
        let _snapshot = TabletReadGuard::pin(&self.tablet(), version);
        let rowsets = self.tablet().capture_consistent_rowsets(version)?;
        let mut segments = Vec::new();
        for rowset in rowsets {
            rowset.load()?;
            for segment in rowset.segments() {
                segments.push((rowset.clone(), segment));
            }
        }
        Ok(segments)
    }

    /// Count all visible segments at the given version.
    pub fn visible_segment_count(&self, version: i64) -> Result<usize> {
        let _snapshot = TabletReadGuard::pin(&self.tablet(), version);
        let rowsets = self.tablet().capture_consistent_rowsets(version)?;
        let mut total = 0usize;
        for rowset in rowsets {
            rowset.load()?;
            total += rowset.segments().len();
        }
        Ok(total)
    }

    /// Collect segments with custom segment options (page cache, etc.).
    pub fn collect_segments_with_options(
        &self,
        version: i64,
        options: SegmentOptions,
    ) -> Result<Vec<(RowsetSharedPtr, SegmentSharedPtr)>> {
        let _snapshot = TabletReadGuard::pin(&self.tablet(), version);
        let rowsets = self.tablet().capture_consistent_rowsets(version)?;
        let mut segments = Vec::new();
        for rowset in rowsets {
            rowset.load_with_options(options.clone())?;
            for segment in rowset.segments() {
                segments.push((rowset.clone(), segment));
            }
        }
        Ok(segments)
    }

    /// Full table scan into a vector of Chunks (visible at max version).
    pub fn scan_chunks(&self) -> Result<Vec<Chunk>> {
        let params = TabletReaderParams::with_version(self.max_version());
        let mut reader = self.create_reader(params)?;
        reader.prepare()?;

        let mut out = Vec::new();
        while let Some(chunk) = reader.get_next_chunk()? {
            out.push(chunk);
        }
        Ok(out)
    }

    /// Legacy scan signature: fills `result` with the first chunk if any.
    pub fn scan_legacy(&self, result: &mut Chunk) -> Result<()> {
        let mut chunks = self.scan_chunks()?;
        if let Some(chunk) = chunks.pop() {
            *result = chunk;
        }
        Ok(())
    }

    /// Total rows (sum of rowset metas).
    pub fn total_rows(&self) -> usize {
        self.tablet()
            .statistics()
            .map(|stats| stats.num_rows as usize)
            .unwrap_or(0)
    }

    /// Rowset count.
    pub fn rowset_count(&self) -> usize {
        self.tablet().num_rowsets()
    }
}
