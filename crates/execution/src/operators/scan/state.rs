// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::vector::Vector;
use paro_function::table::{GlobalTableFunctionState, LocalTableFunctionState};
use paro_storage::index::{ColumnId, PredicateTree};
use paro_storage::rowset::{RowsetSharedPtr, SegmentSharedPtr};
use paro_storage::table::table_handle::TableHandle;
use paro_storage::table::StorageSnapshot;
use paro_storage::tablet::{ColumnProjection, TabletReader};
use paro_storage::transaction::overlay_reader::OverlayDeleteVectorMap;

use super::table_function::TableFunctionBindDataWrapper;

#[derive(Debug)]
pub struct RowsetSourceGlobal {
    pub table_index: usize,
    pub table: Arc<TableHandle>,
    pub storage_snapshot: StorageSnapshot,
    pub segments: Box<[(RowsetSharedPtr, SegmentSharedPtr)]>,
    pub next_segment: AtomicUsize,
    pub column_projection: ColumnProjection,
    pub overlay_delete_vectors: Option<Arc<OverlayDeleteVectorMap>>,
    pub predicate: Option<PredicateTree>,
    pub predicate_columns: Box<[ColumnId]>,
}

#[derive(Debug, Default)]
pub struct RowsetSourceLocal {
    pub next_morsel: usize,
    pub assigned_segment: Option<usize>,
    pub assigned_segment_consumed: bool,
    pub reader: Option<TabletReader>,
}

impl RowsetSourceLocal {
    pub fn assign_segment_morsel(&mut self, segment_idx: usize) {
        self.assigned_segment = Some(segment_idx);
        self.assigned_segment_consumed = false;
        self.next_morsel = segment_idx;
    }
}

#[derive(Debug)]
pub struct ValuesSourceGlobal {
    pub row_count: usize,
}

#[derive(Debug, Default)]
pub struct ValuesSourceLocal {
    pub cursor: usize,
    pub scalar_scratch: Vec<Vector>,
}

#[derive(Debug)]
pub struct ChunkSourceGlobal {
    pub chunks: Arc<[Chunk]>,
    pub next_chunk: AtomicUsize,
}

#[derive(Debug, Default)]
pub struct ChunkSourceLocal {
    pub assigned_chunk: Option<usize>,
    pub assigned_chunk_consumed: bool,
}

impl ChunkSourceLocal {
    pub fn assign_chunk_morsel(&mut self, chunk_idx: usize) {
        self.assigned_chunk = Some(chunk_idx);
        self.assigned_chunk_consumed = false;
    }
}

#[derive(Debug)]
pub struct ExpressionSourceGlobal {
    pub row_count: usize,
}

#[derive(Debug, Default)]
pub struct ExpressionSourceLocal {
    pub cursor: usize,
    pub scalar_scratch: Vec<Vector>,
}

pub struct TableFunctionSourceGlobal {
    pub bind_data: Arc<TableFunctionBindDataWrapper>,
    pub global_state: Option<Box<dyn GlobalTableFunctionState>>,
    pub max_threads: usize,
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
