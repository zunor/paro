// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_function::table::{GlobalTableFunctionState, LocalTableFunctionState};
use paro_storage::index::{ColumnId, PredicateTree};
use paro_storage::rowset::{RowsetSharedPtr, SegmentSharedPtr};
use paro_storage::table::table_handle::TableHandle;
use paro_storage::table::StorageSnapshot;
use paro_storage::tablet::{ColumnProjection, TabletReader};
use paro_storage::transaction::overlay_reader::OverlayDeleteVectorMap;

use crate::physical::specs::RowsetScanMaterialization;

use super::table_function::TableFunctionBindDataWrapper;

/// Enough decoded/predicate work to amortize one scan task and its reader.
/// Morsels may remain smaller for stealing; this threshold only bounds useful
/// concurrent consumers of their shared queue.
pub(crate) const ROWSET_PARALLEL_WORK_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug)]
pub struct RowsetSourceGlobal {
    pub table_index: usize,
    pub table: Arc<TableHandle>,
    pub storage_snapshot: Arc<StorageSnapshot>,
    pub segments: Box<[(RowsetSharedPtr, SegmentSharedPtr)]>,
    pub morsels: Box<[RowsetScanMorsel]>,
    pub row_work_bytes: u64,
    pub next_morsel: AtomicUsize,
    pub column_projection: ColumnProjection,
    pub overlay_delete_vectors: Option<Arc<OverlayDeleteVectorMap>>,
    pub prepared_predicate: Option<PreparedRowsetPredicate>,
}

impl RowsetSourceGlobal {
    /// Return the number of workers that can consume at least one physical
    /// vector of scan input. Morsels remain the stealing and reader-reopen
    /// boundary, but tiny multi-segment tables must not manufacture useful
    /// parallelism merely because they have several storage fragments.
    pub(crate) fn parallel_work_count(&self) -> usize {
        useful_rowset_scan_workers(&self.morsels, self.row_work_bytes)
    }
}

fn useful_rowset_scan_workers(morsels: &[RowsetScanMorsel], row_work_bytes: u64) -> usize {
    let physical_rows = morsels.iter().fold(0u64, |rows, morsel| {
        rows.saturating_add(morsel.end_ordinal.saturating_sub(morsel.start_ordinal))
    });
    let physical_work = physical_rows.saturating_mul(row_work_bytes.max(1));
    let useful_workers =
        usize::try_from(physical_work.div_ceil(ROWSET_PARALLEL_WORK_BYTES)).unwrap_or(usize::MAX);
    morsels.len().min(useful_workers)
}

/// Execution-bound predicate and its matching initial access mode.
///
/// These fields are prepared together after all build-dependent predicates
/// are published, then shared immutably by every scan worker.
#[derive(Debug)]
pub struct PreparedRowsetPredicate {
    pub tree: PredicateTree,
    pub columns: Box<[ColumnId]>,
    pub materialization: RowsetScanMaterialization,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowsetScanMorsel {
    pub segment_idx: usize,
    pub start_ordinal: u64,
    pub end_ordinal: u64,
}

#[derive(Debug, Default)]
pub struct RowsetSourceLocal {
    pub reader: Option<TabletReader>,
}

#[derive(Debug)]
pub struct ValuesSourceGlobal {
    pub row_count: usize,
}

pub type ValuesSourceLocal = super::expression_rows::ExpressionRowsSourceLocal;

#[derive(Debug)]
pub struct ChunkSourceGlobal {
    pub chunks: Arc<[Chunk]>,
}

#[derive(Debug, Default)]
pub struct ChunkSourceLocal {
    pub next_chunk: usize,
    pub assigned_chunk_end: Option<usize>,
}

impl ChunkSourceLocal {
    pub fn assign_chunk_range(&mut self, start: usize, end: usize) {
        debug_assert!(start < end);
        self.next_chunk = start;
        self.assigned_chunk_end = Some(end);
    }
}

#[derive(Debug)]
pub struct ExpressionSourceGlobal {
    pub row_count: usize,
}

pub type ExpressionSourceLocal = super::expression_rows::ExpressionRowsSourceLocal;

pub struct TableFunctionSourceGlobal {
    pub bind_data: Arc<TableFunctionBindDataWrapper>,
    pub global_state: Option<Box<dyn GlobalTableFunctionState>>,
    pub max_threads: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rowset_scan_parallelism_is_bounded_by_physical_work() {
        let tiny_fragments = [
            RowsetScanMorsel {
                segment_idx: 0,
                start_ordinal: 0,
                end_ordinal: 7,
            },
            RowsetScanMorsel {
                segment_idx: 1,
                start_ordinal: 0,
                end_ordinal: 8,
            },
        ];
        assert_eq!(useful_rowset_scan_workers(&tiny_fragments, 8), 1);

        let useful_fragments = [
            RowsetScanMorsel {
                segment_idx: 0,
                start_ordinal: 0,
                end_ordinal: ROWSET_PARALLEL_WORK_BYTES,
            },
            RowsetScanMorsel {
                segment_idx: 1,
                start_ordinal: 0,
                end_ordinal: ROWSET_PARALLEL_WORK_BYTES,
            },
        ];
        assert_eq!(useful_rowset_scan_workers(&useful_fragments, 1), 2);
        assert_eq!(useful_rowset_scan_workers(&[], 8), 0);
    }
}

impl fmt::Debug for TableFunctionSourceGlobal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableFunctionSourceGlobal")
            .field("function", &self.bind_data.function.name)
            .field("has_global_state", &self.global_state.is_some())
            .field("max_threads", &self.max_threads)
            .finish()
    }
}

impl TableFunctionSourceGlobal {
    #[inline(always)]
    pub fn global_state(&self) -> Option<&dyn GlobalTableFunctionState> {
        self.global_state.as_ref().map(|state| state.as_ref())
    }
}

pub struct TableFunctionSourceLocal {
    pub local_state: Option<Box<dyn LocalTableFunctionState>>,
    pub finished: bool,
    pub ordinality_counter: i64,
}

impl fmt::Debug for TableFunctionSourceLocal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableFunctionSourceLocal")
            .field("has_local_state", &self.local_state.is_some())
            .field("finished", &self.finished)
            .field("ordinality_counter", &self.ordinality_counter)
            .finish()
    }
}

impl Default for TableFunctionSourceLocal {
    fn default() -> Self {
        Self {
            local_state: None,
            finished: false,
            ordinality_counter: 1,
        }
    }
}

impl TableFunctionSourceLocal {
    #[inline(always)]
    pub fn local_state_mut(&mut self) -> Option<&mut dyn LocalTableFunctionState> {
        self.local_state
            .as_mut()
            .map(|state| state.as_mut() as &mut dyn LocalTableFunctionState)
    }

    #[inline]
    pub fn advance_ordinality(&mut self, count: usize) -> i64 {
        let start = self.ordinality_counter;
        self.ordinality_counter = self.ordinality_counter.saturating_add(count as i64);
        start
    }
}

#[derive(Debug, Default)]
pub struct EmptySourceGlobal;

#[derive(Debug, Default)]
pub struct EmptySourceLocal {
    pub emitted: bool,
}
