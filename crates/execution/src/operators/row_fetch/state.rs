// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::vector::Vector;
use paro_storage::table::table_handle::TableHandle;
use paro_storage::table::StorageSnapshot;
use paro_storage::tablet::TabletRowIdReader;

use crate::expression_executor::executor::ExpressionExecutor;

#[derive(Debug)]
pub struct RowFetchTableState {
    pub table_name: String,
    pub rowid_col_idx: usize,
    pub storage: Arc<TableHandle>,
    /// Captured at local initialization but materialized only by a worker that
    /// receives an actual row-id batch. Scheduler locals that never run must
    /// not pay snapshot-lineage construction or retain its rowset directory.
    pub storage_snapshot: Arc<StorageSnapshot>,
    pub reader: Option<TabletRowIdReader>,
    pub rowids: Vec<u64>,
    pub column_ids: Box<[u32]>,
}

#[derive(Debug)]
pub struct RowFetchTransformLocal {
    pub table_fetches: Box<[RowFetchTableState]>,
    pub direct_project_columns: Option<Box<[usize]>>,
    pub project_executor: Option<ExpressionExecutor>,
    /// Reused only to avoid rebuilding the small Arc directory on every poll.
    pub combined_columns: Vec<Arc<Vector>>,
}
