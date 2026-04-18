// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Transaction State Management
//!
//! Coordinates undo tracking, staged writes, commit, rollback, and cleanup.

use crate::index::fulltext::tokenizer::TokenizerKind;
use crate::rowset::RowsetSharedPtr;
use crate::table::index_runtime::IndexRuntime;
use crate::tablet::{
    build_delete_patch_from_primary_keys, build_delete_patch_from_row_refs,
    capture_prepare_snapshot, materialize_delete_patch, PhysicalRowRef, PrepareSnapshot,
    PrimaryIndexUpdate, TabletRef, TabletState,
};
use crate::transaction::undo_buffer::{ActiveTransactionState, UndoBuffer};
use crate::write::{DeltaWriter, DeltaWriterSavepoint};
use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::durability::PrepareToken;
use paro_common::effect::{
    ArtifactRef, GraphDmlTableDelta as GraphDmlHookDelta, PostCommitHookDescriptor,
    StorageCommitOp, TabletApplyOp, TabletMutation, VersionSpan,
};
use paro_common::error::{self as paro_error, Result};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

/// Pending operations staged by a transaction (rowsets + primary deletes).
#[derive(Debug)]
pub enum PendingOperation {
    Rowset(PendingRowset),
    PrimaryDelete(PendingPrimaryDelete),
    RowIdDelete(PendingRowIdDelete),
}

/// Pending rowset + primary key update metadata.
#[derive(Debug)]
pub struct PendingRowset {
    tablet: TabletRef,
    rowset: RowsetSharedPtr,
    primary_update: Option<PrimaryIndexUpdate>,
    rowset_path: PathBuf,
    art_columns: Vec<u32>,
    fulltext_columns: Vec<(u32, String)>,
}

/// Pending primary key delete (no rowset).
#[derive(Debug)]
pub struct PendingPrimaryDelete {
    tablet: TabletRef,
    keys: Vec<Vec<u8>>,
}

/// Pending row-id delete (no rowset).
#[derive(Debug)]
pub struct PendingRowIdDelete {
    tablet: TabletRef,
    locations: Vec<PhysicalRowRef>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GraphTableDmlDelta {
    pub inserted: u64,
    pub deleted: u64,
    pub updated: u64,
    pub updated_columns: BTreeSet<u32>,
}

#[derive(Debug, Clone, Default)]
pub struct StorageSavepointMark {
    pub pending_ops_len: usize,
    pub pending_dml_tables: BTreeSet<u64>,
    pub pending_art_columns: HashMap<u64, BTreeSet<u32>>,
    pub pending_fulltext_columns: HashMap<u64, HashMap<u32, String>>,
    pub pending_graph_dml: HashMap<u64, GraphTableDmlDelta>,
    pub writer_marks: HashMap<u64, DeltaWriterSavepoint>,
}

#[derive(Debug, Clone)]
pub struct PreparedStorageCommit {
    pub storage_ops: Vec<StorageCommitOp>,
    pub post_commit_hooks: Vec<PostCommitHookDescriptor>,
    pub tablets: Vec<PreparedTabletCommit>,
}

impl PreparedStorageCommit {
    pub fn is_empty(&self) -> bool {
        self.storage_ops.is_empty() && self.post_commit_hooks.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct PreparedTabletCommit {
    pub tablet: TabletRef,
    pub token: PrepareToken,
}

#[derive(Debug, Default)]
struct PreparedStorageState {
    rowsets: Vec<PendingRowset>,
    primary_deletes: Vec<PendingPrimaryDelete>,
    row_id_deletes: Vec<PendingRowIdDelete>,
    delete_patch_artifacts: Vec<PathBuf>,
}

/// A transaction context representing an active database transaction.
///
/// - Paro uses `AtomicBool` for `is_read_only` and `awaiting_cleanup`
/// - No transaction-local storage (data writes go through Tablet/Rowset)
#[derive(Debug)]
pub struct Transaction {
    /// The transaction ID (unique identifier)
    pub id: u64,

    /// The start timestamp of the transaction (used for MVCC visibility)
    pub start_time: u64,

    /// The commit ID assigned when transaction commits successfully
    /// Zero means not yet committed
    pub commit_id: Mutex<u64>,

    /// The undo buffer for storing rollback information
    pub undo_buffer: Mutex<UndoBuffer>,

    /// Whether this transaction is read-only
    /// Read-only transactions cannot trigger checkpoints
    is_read_only: AtomicBool,

    /// Catalog version at transaction start
    /// Used to detect catalog changes during transaction
    catalog_version: AtomicU64,

    /// Whether this transaction is awaiting cleanup
    /// Set after commit/rollback, before cleanup is performed
    awaiting_cleanup: AtomicBool,

    /// Monotonic counter tracking transactional mutations for savepoint guards.
    mutation_generation: AtomicU64,

    /// State tracking for active transactions.
    active_transaction_state: AtomicU8,

    /// Pending operations staged by this transaction (rowsets + deletes).
    pending_ops: Mutex<Vec<PendingOperation>>,

    /// Tables touched by DML in this transaction for mixed admission checks.
    pending_dml_tables: Mutex<BTreeSet<u64>>,

    /// Transaction-scoped writers for per-tablet MemTable reuse.
    pending_writers: Mutex<HashMap<u64, DeltaWriter>>,

    /// Per-tablet full-text indexed columns and config that need runtime index
    /// build when pending writers are finalized into rowsets.
    pending_fulltext_columns: Mutex<HashMap<u64, HashMap<u32, String>>>,

    /// Per-tablet ART indexed columns that need runtime index build when
    /// pending writers are finalized into rowsets.
    pending_art_columns: Mutex<HashMap<u64, BTreeSet<u32>>>,

    /// Per-table graph-relevant DML summary for commit-time maintenance hooks.
    pending_graph_dml: Mutex<HashMap<u64, GraphTableDmlDelta>>,

    /// Stable participant state materialized during prepare_commit().
    prepared_storage_state: Mutex<Option<PreparedStorageState>>,
}

impl Transaction {
    /// Create a new transaction with the given ID and start time.
    ///
    /// # Arguments
    /// * `id` - Unique transaction identifier
    /// * `start_time` - Transaction start timestamp for MVCC
    ///
    /// ```cpp
    ///     transaction_t start_time, transaction_t transaction_id, idx_t catalog_version_p)
    /// ```
    pub fn new(id: u64, start_time: u64) -> Self {
        Self::with_catalog_version(id, start_time, 0)
    }

    /// Create a new transaction with explicit catalog version.
    ///
    /// # Arguments
    /// * `id` - Unique transaction identifier
    /// * `start_time` - Transaction start timestamp for MVCC
    /// * `catalog_version` - Current catalog version at transaction start
    pub fn with_catalog_version(id: u64, start_time: u64, catalog_version: u64) -> Self {
        Self {
            id,
            start_time,
            commit_id: Mutex::new(0),
            undo_buffer: Mutex::new(UndoBuffer::new()),
            is_read_only: AtomicBool::new(true), // Start as read-only
            catalog_version: AtomicU64::new(catalog_version),
            awaiting_cleanup: AtomicBool::new(false),
            mutation_generation: AtomicU64::new(0),
            active_transaction_state: AtomicU8::new(ActiveTransactionState::Unset as u8),
            pending_ops: Mutex::new(Vec::new()),
            pending_dml_tables: Mutex::new(BTreeSet::new()),
            pending_writers: Mutex::new(HashMap::new()),
            pending_fulltext_columns: Mutex::new(HashMap::new()),
            pending_art_columns: Mutex::new(HashMap::new()),
            pending_graph_dml: Mutex::new(HashMap::new()),
            prepared_storage_state: Mutex::new(None),
        }
    }

    /// Returns whether this transaction is read-only.
    ///
    /// A transaction starts as read-only and becomes read-write when it
    /// makes modifications (INSERT, UPDATE, DELETE, DDL).
    pub fn is_read_only(&self) -> bool {
        self.is_read_only.load(Ordering::Acquire)
    }

    /// Promote this transaction from read-only to read-write.
    ///
    /// Called when the transaction first makes a modification.
    /// This is a one-way transition (cannot go back to read-only).
    pub fn set_read_write(&self) {
        self.is_read_only.store(false, Ordering::Release);
    }

    fn bump_mutation_generation(&self) {
        self.mutation_generation.fetch_add(1, Ordering::AcqRel);
    }

    pub fn mutation_generation(&self) -> u64 {
        self.mutation_generation.load(Ordering::Acquire)
    }

    pub fn mark_savepoint(&self) -> Result<StorageSavepointMark> {
        let pending_ops_len = self
            .pending_ops
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock pending ops: {}", e)))?
            .len();
        let pending_dml_tables = self
            .pending_dml_tables
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock pending dml tables: {}", e)))?
            .clone();
        let pending_art_columns = self
            .pending_art_columns
            .lock()
            .map_err(|e| {
                paro_error::internal(format!("failed to lock pending art columns: {}", e))
            })?
            .clone();
        let pending_fulltext_columns = self
            .pending_fulltext_columns
            .lock()
            .map_err(|e| {
                paro_error::internal(format!("failed to lock pending fulltext columns: {}", e))
            })?
            .clone();
        let pending_graph_dml = self
            .pending_graph_dml
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock pending graph dml: {}", e)))?
            .clone();
        let mut writers = self
            .pending_writers
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock pending writers: {}", e)))?;
        let mut writer_marks = HashMap::with_capacity(writers.len());
        for (tablet_id, writer) in writers.iter_mut() {
            writer_marks.insert(*tablet_id, writer.mark_savepoint()?);
        }

        Ok(StorageSavepointMark {
            pending_ops_len,
            pending_dml_tables,
            pending_art_columns,
            pending_fulltext_columns,
            pending_graph_dml,
            writer_marks,
        })
    }

    pub fn rollback_to_savepoint(&self, mark: &StorageSavepointMark) -> Result<()> {
        let cancelled_writers = {
            let mut writers = self.pending_writers.lock().map_err(|e| {
                paro_error::internal(format!("failed to lock pending writers: {}", e))
            })?;
            let writer_ids: Vec<u64> = writers.keys().copied().collect();
            let mut cancelled = Vec::new();
            for tablet_id in &writer_ids {
                if let Some(writer_mark) = mark.writer_marks.get(tablet_id) {
                    let writer = writers
                        .get_mut(tablet_id)
                        .ok_or_else(|| paro_error::internal("failed to get pending writer"))?;
                    writer.rollback_to_savepoint(writer_mark)?;
                }
            }
            for tablet_id in writer_ids {
                if mark.writer_marks.contains_key(&tablet_id) {
                    continue;
                }
                if let Some(writer) = writers.remove(&tablet_id) {
                    cancelled.push(writer);
                }
            }
            cancelled
        };
        for writer in cancelled_writers {
            writer.cancel()?;
        }

        let mut pending = self
            .pending_ops
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock pending ops: {}", e)))?;
        let tail = if mark.pending_ops_len >= pending.len() {
            Vec::new()
        } else {
            pending.split_off(mark.pending_ops_len)
        };
        drop(pending);

        for op in tail.into_iter().rev() {
            self.rollback_pending_op(op);
        }

        *self.pending_dml_tables.lock().map_err(|e| {
            paro_error::internal(format!("failed to lock pending dml tables: {}", e))
        })? = mark.pending_dml_tables.clone();
        *self.pending_art_columns.lock().map_err(|e| {
            paro_error::internal(format!("failed to lock pending art columns: {}", e))
        })? = mark.pending_art_columns.clone();
        *self.pending_fulltext_columns.lock().map_err(|e| {
            paro_error::internal(format!("failed to lock pending fulltext columns: {}", e))
        })? = mark.pending_fulltext_columns.clone();
        *self.pending_graph_dml.lock().map_err(|e| {
            paro_error::internal(format!("failed to lock pending graph dml: {}", e))
        })? = mark.pending_graph_dml.clone();

        Ok(())
    }

    /// Returns the current catalog version tracked by this transaction.
    pub fn catalog_version(&self) -> u64 {
        self.catalog_version.load(Ordering::Acquire)
    }

    /// Update the catalog version (called when catalog changes are made).
    pub fn set_catalog_version(&self, version: u64) {
        self.catalog_version.store(version, Ordering::Release);
    }

    /// Returns whether this transaction is awaiting cleanup.
    pub fn is_awaiting_cleanup(&self) -> bool {
        self.awaiting_cleanup.load(Ordering::Acquire)
    }

    /// Mark this transaction as awaiting cleanup.
    pub fn set_awaiting_cleanup(&self, awaiting: bool) {
        self.awaiting_cleanup.store(awaiting, Ordering::Release);
    }

    /// Returns the active transaction state.
    pub fn active_transaction_state(&self) -> ActiveTransactionState {
        ActiveTransactionState::try_from(
            self.active_transaction_state.load(Ordering::Acquire) as u32
        )
        .unwrap_or(ActiveTransactionState::Unset)
    }

    /// Set the active transaction state.
    pub fn set_active_transaction_state(&self, state: ActiveTransactionState) {
        self.active_transaction_state
            .store(state as u8, Ordering::Release);
    }

    /// Returns whether this transaction has made any changes.
    ///
    /// ```cpp
    ///     return undo_buffer.ChangesMade();
    /// }
    /// ```
    pub fn changes_made(&self) -> bool {
        // Check undo buffer
        let undo_changes = match self.undo_buffer.lock() {
            Ok(buffer) => buffer.changes_made(),
            Err(_) => true, // If lock is poisoned, assume changes were made (safe default)
        };

        let pending_changes = match self.pending_ops.lock() {
            Ok(ops) => !ops.is_empty(),
            Err(_) => true,
        };

        let writer_changes = match self.pending_writers.lock() {
            Ok(writers) => !writers.is_empty(),
            Err(_) => true,
        };

        undo_changes || pending_changes || writer_changes
    }

    pub fn record_dml_table(&self, table_oid: u64) -> Result<()> {
        let mut tables = self.pending_dml_tables.lock().map_err(|e| {
            paro_error::internal(format!("failed to lock pending dml tables: {}", e))
        })?;
        tables.insert(table_oid);
        Ok(())
    }

    pub fn has_dml_on_table(&self, table_oid: u64) -> bool {
        self.pending_dml_tables
            .lock()
            .map(|tables| tables.contains(&table_oid))
            .unwrap_or(true)
    }

    pub fn has_dml_on_any_table<I>(&self, table_oids: I) -> bool
    where
        I: IntoIterator<Item = u64>,
    {
        let Ok(tables) = self.pending_dml_tables.lock() else {
            return true;
        };
        table_oids
            .into_iter()
            .any(|table_oid| tables.contains(&table_oid))
    }

    fn undo_changes_made(&self) -> bool {
        match self.undo_buffer.lock() {
            Ok(buffer) => buffer.changes_made(),
            Err(_) => true,
        }
    }

    /// Stage a pending rowset for commit (transactional visibility).
    pub fn add_pending_rowset(
        &self,
        tablet: TabletRef,
        rowset: RowsetSharedPtr,
        primary_update: Option<PrimaryIndexUpdate>,
        art_columns: Vec<u32>,
        fulltext_columns: Vec<(u32, String)>,
    ) -> Result<()> {
        let tablet_id = tablet.tablet_id();
        let mut ops = self
            .pending_ops
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock pending ops: {}", e)))?;

        if ops.iter().any(
            |op| matches!(op, PendingOperation::Rowset(p) if p.tablet.tablet_id() == tablet_id),
        ) {
            return Err(paro_error::not_supported(format!(
                "multiple rowsets for tablet {} in one transaction are not supported",
                tablet_id
            )));
        }

        if self
            .pending_writers
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock pending writers: {}", e)))?
            .contains_key(&tablet_id)
        {
            return Err(paro_error::not_supported(format!(
                "pending memtable writer exists for tablet {}",
                tablet_id
            )));
        }

        ops.push(PendingOperation::Rowset(PendingRowset {
            tablet,
            rowset: rowset.clone(),
            primary_update,
            rowset_path: rowset.rowset_path().to_path_buf(),
            art_columns,
            fulltext_columns,
        }));
        self.bump_mutation_generation();
        self.set_read_write();
        Ok(())
    }

    /// Stage primary-key deletes for commit (transactional visibility).
    pub fn add_pending_primary_delete(&self, tablet: TabletRef, keys: Vec<Vec<u8>>) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        tablet.acquire_primary_delete_intents(self.id, &keys)?;

        let mut ops = self.pending_ops.lock().map_err(|e| {
            tablet.release_primary_delete_intents(self.id, &keys);
            paro_error::internal(format!("failed to lock pending ops: {}", e))
        })?;
        ops.push(PendingOperation::PrimaryDelete(PendingPrimaryDelete {
            tablet,
            keys,
        }));
        self.bump_mutation_generation();
        self.set_read_write();
        Ok(())
    }

    /// Stage row-id deletes for commit (transactional visibility).
    pub fn add_pending_row_id_delete(
        &self,
        tablet: TabletRef,
        locations: Vec<PhysicalRowRef>,
    ) -> Result<()> {
        if locations.is_empty() {
            return Ok(());
        }
        tablet.acquire_row_id_delete_intents(self.id, &locations)?;

        let mut ops = self.pending_ops.lock().map_err(|e| {
            tablet.release_row_id_delete_intents(self.id, &locations);
            paro_error::internal(format!("failed to lock pending ops: {}", e))
        })?;
        ops.push(PendingOperation::RowIdDelete(PendingRowIdDelete {
            tablet,
            locations,
        }));
        self.bump_mutation_generation();
        self.set_read_write();
        Ok(())
    }

    /// Append a chunk using a transaction-scoped DeltaWriter (MemTable reuse).
    pub fn append_to_tablet(&self, tablet: TabletRef, chunk: &Chunk) -> Result<()> {
        if chunk.size() == 0 {
            return Ok(());
        }
        self.with_tablet_writer(tablet, chunk.allocator().clone(), |writer| {
            writer.write_chunk(chunk)
        })?;
        Ok(())
    }

    /// Register declared full-text columns for a tablet.
    ///
    /// These columns will be used to build runtime full-text indexes on
    /// transaction-private rowsets before commit visibility.
    pub fn register_pending_fulltext_columns(
        &self,
        tablet_id: u64,
        columns: Vec<(u32, String)>,
    ) -> Result<()> {
        if columns.is_empty() {
            return Ok(());
        }

        let mut pending = self.pending_fulltext_columns.lock().map_err(|e| {
            paro_error::internal(format!("failed to lock pending fulltext columns: {}", e))
        })?;
        let entry = pending.entry(tablet_id).or_default();
        for (column_id, config) in columns {
            let normalized = TokenizerKind::from_config(&config)?
                .config_name()
                .to_string();
            if let Some(existing) = entry.get(&column_id) {
                if !existing.eq_ignore_ascii_case(&normalized) {
                    return Err(paro_error::invalid_input(format!(
                        "conflicting full-text config for column {}: '{}' vs '{}'",
                        column_id, existing, normalized
                    )));
                }
                continue;
            }
            entry.insert(column_id, normalized);
        }
        Ok(())
    }

    /// Register declared ART columns for a tablet.
    ///
    /// These columns will be used to build runtime ART indexes on
    /// transaction-private rowsets before commit visibility.
    pub fn register_pending_art_columns(&self, tablet_id: u64, columns: Vec<u32>) -> Result<()> {
        if columns.is_empty() {
            return Ok(());
        }

        let mut pending = self.pending_art_columns.lock().map_err(|e| {
            paro_error::internal(format!("failed to lock pending art columns: {}", e))
        })?;
        pending.entry(tablet_id).or_default().extend(columns);
        Ok(())
    }

    fn take_pending_art_columns(&self, tablet_id: u64) -> Result<Vec<u32>> {
        let mut pending = self.pending_art_columns.lock().map_err(|e| {
            paro_error::internal(format!("failed to lock pending art columns: {}", e))
        })?;
        Ok(pending
            .remove(&tablet_id)
            .unwrap_or_default()
            .into_iter()
            .collect())
    }

    fn take_pending_fulltext_columns(&self, tablet_id: u64) -> Result<Vec<(u32, String)>> {
        let mut pending = self.pending_fulltext_columns.lock().map_err(|e| {
            paro_error::internal(format!("failed to lock pending fulltext columns: {}", e))
        })?;
        let mut columns: Vec<(u32, String)> = pending
            .remove(&tablet_id)
            .unwrap_or_default()
            .into_iter()
            .collect();
        columns.sort_unstable_by_key(|(column_id, _)| *column_id);
        Ok(columns)
    }

    fn with_tablet_writer<F, R>(
        &self,
        tablet: TabletRef,
        allocator: Arc<dyn Allocator>,
        f: F,
    ) -> Result<R>
    where
        F: FnOnce(&mut DeltaWriter) -> Result<R>,
    {
        let tablet_id = tablet.tablet_id();
        {
            let ops = self
                .pending_ops
                .lock()
                .map_err(|e| paro_error::internal(format!("failed to lock pending ops: {}", e)))?;
            if ops.iter().any(
                |op| matches!(op, PendingOperation::Rowset(p) if p.tablet.tablet_id() == tablet_id),
            ) {
                return Err(paro_error::not_supported(format!(
                    "pending rowset already exists for tablet {}",
                    tablet_id
                )));
            }
        }

        let mut writers = self
            .pending_writers
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock pending writers: {}", e)))?;
        if let std::collections::hash_map::Entry::Vacant(e) = writers.entry(tablet_id) {
            let writer = DeltaWriter::open_with_allocator(tablet.clone(), self.id, allocator)?;
            e.insert(writer);
        }
        let writer = writers
            .get_mut(&tablet_id)
            .ok_or_else(|| paro_error::internal("failed to get pending writer"))?;
        let result = f(writer)?;
        self.bump_mutation_generation();
        self.set_read_write();
        Ok(result)
    }

    fn materialize_pending_writers(&self) -> Result<()> {
        let pending = {
            let mut writers = self.pending_writers.lock().map_err(|e| {
                paro_error::internal(format!("failed to lock pending writers: {}", e))
            })?;
            std::mem::take(&mut *writers)
        };

        for (tablet_id, writer) in pending {
            let (tablet, rowset, primary_update) = writer.finalize_for_transaction()?;
            let art_columns = self.take_pending_art_columns(tablet_id)?;
            let fulltext_columns = self.take_pending_fulltext_columns(tablet_id)?;

            if tablet.state() == TabletState::Shutdown {
                let _ = std::fs::remove_dir_all(rowset.rowset_path());
                continue;
            }

            if !fulltext_columns.is_empty() {
                if let Err(err) = IndexRuntime::build_runtime_fulltext_indexes_for_rowset(
                    &rowset,
                    &fulltext_columns,
                ) {
                    let _ = std::fs::remove_dir_all(rowset.rowset_path());
                    return Err(err);
                }
            }
            if !art_columns.is_empty() {
                if let Err(err) =
                    IndexRuntime::build_runtime_art_indexes_for_rowset(&rowset, &art_columns)
                {
                    tracing::warn!(
                        error = %err,
                        tablet_id,
                        "ART index backfill failed for transaction rowset; queries will fallback to scan"
                    );
                }
            }

            if let Err(err) = self.add_pending_rowset(
                tablet,
                rowset.clone(),
                primary_update,
                art_columns,
                fulltext_columns,
            ) {
                let _ = std::fs::remove_dir_all(rowset.rowset_path());
                return Err(err);
            }
        }
        Ok(())
    }

    fn rollback_pending_writers(&self) {
        let pending = match self.pending_writers.lock() {
            Ok(mut writers) => std::mem::take(&mut *writers),
            Err(_) => return,
        };
        for (_tablet_id, writer) in pending {
            let _ = writer.cancel();
        }
        if let Ok(mut pending) = self.pending_art_columns.lock() {
            pending.clear();
        }
        if let Ok(mut pending) = self.pending_fulltext_columns.lock() {
            pending.clear();
        }
    }

    fn rollback_pending_op(&self, op: PendingOperation) {
        match op {
            PendingOperation::Rowset(rowset) => {
                let _ = std::fs::remove_dir_all(&rowset.rowset_path);
            }
            PendingOperation::PrimaryDelete(delete) => {
                delete
                    .tablet
                    .release_primary_delete_intents(self.id, &delete.keys);
            }
            PendingOperation::RowIdDelete(delete) => {
                delete
                    .tablet
                    .release_row_id_delete_intents(self.id, &delete.locations);
            }
        }
    }

    fn rollback_prepared_storage_state(&self) {
        let prepared = match self.prepared_storage_state.lock() {
            Ok(mut state) => state.take(),
            Err(_) => None,
        };
        let Some(prepared) = prepared else {
            return;
        };
        for rowset in prepared.rowsets.into_iter().rev() {
            let _ = std::fs::remove_dir_all(&rowset.rowset_path);
        }
        for delete in prepared.primary_deletes {
            delete
                .tablet
                .release_primary_delete_intents(self.id, &delete.keys);
        }
        for delete in prepared.row_id_deletes {
            delete
                .tablet
                .release_row_id_delete_intents(self.id, &delete.locations);
        }
        Self::cleanup_delete_patch_artifacts(&prepared.delete_patch_artifacts);
    }

    pub fn record_graph_insert(&self, table_oid: u64, rows: usize) {
        if rows == 0 {
            return;
        }
        if let Ok(mut pending) = self.pending_graph_dml.lock() {
            let entry = pending.entry(table_oid).or_default();
            entry.inserted = entry.inserted.saturating_add(rows as u64);
        }
    }

    pub fn record_graph_delete(&self, table_oid: u64, rows: usize) {
        if rows == 0 {
            return;
        }
        if let Ok(mut pending) = self.pending_graph_dml.lock() {
            let entry = pending.entry(table_oid).or_default();
            entry.deleted = entry.deleted.saturating_add(rows as u64);
        }
    }

    pub fn record_graph_update(&self, table_oid: u64, rows: usize, updated_columns: &[u32]) {
        if rows == 0 {
            return;
        }
        if let Ok(mut pending) = self.pending_graph_dml.lock() {
            let entry = pending.entry(table_oid).or_default();
            entry.updated = entry.updated.saturating_add(rows as u64);
            entry
                .updated_columns
                .extend(updated_columns.iter().copied());
        }
    }

    pub fn take_graph_dml_deltas(&self) -> Result<HashMap<u64, GraphTableDmlDelta>> {
        let mut pending = self.pending_graph_dml.lock().map_err(|e| {
            paro_error::internal(format!("failed to lock pending graph dml: {}", e))
        })?;
        Ok(std::mem::take(&mut *pending))
    }

    fn split_pending_operations(
        &self,
        pending: Vec<PendingOperation>,
    ) -> (
        Vec<PendingPrimaryDelete>,
        Vec<PendingRowIdDelete>,
        Vec<PendingRowset>,
    ) {
        let mut primary_deletes = Vec::new();
        let mut row_id_deletes = Vec::new();
        let mut rowsets = Vec::new();

        for op in pending {
            match op {
                PendingOperation::PrimaryDelete(pending) => primary_deletes.push(pending),
                PendingOperation::RowIdDelete(pending) => row_id_deletes.push(pending),
                PendingOperation::Rowset(pending) => rowsets.push(pending),
            }
        }

        (primary_deletes, row_id_deletes, rowsets)
    }

    fn apply_materialized_writes(
        &self,
        commit_id: u64,
        primary_deletes: &[PendingPrimaryDelete],
        row_id_deletes: &[PendingRowIdDelete],
        rowsets: Vec<PendingRowset>,
    ) -> Result<()> {
        let commit_version = i64::try_from(commit_id)
            .map_err(|_| paro_error::invalid_input("commit_id exceeds supported version range"))?;

        // Apply deletes first, then inserts/rowset commits.
        (|| {
            for pending in primary_deletes {
                pending.tablet.apply_primary_delete(pending.keys.clone())?;
            }

            for pending in row_id_deletes {
                if pending.locations.is_empty() {
                    continue;
                }
                pending
                    .tablet
                    .apply_row_id_delete_refs_at_version(&pending.locations, commit_version)?;
            }

            for pending in rowsets {
                let PendingRowset {
                    tablet,
                    rowset,
                    primary_update,
                    art_columns,
                    fulltext_columns,
                    ..
                } = pending;

                if let Some(update) = primary_update {
                    tablet.publish_rowset_with_index(commit_version, rowset.clone(), update)?;
                } else {
                    tablet.rowset_commit(commit_version, rowset.clone())?;
                }

                let published = tablet
                    .find_rowset_by_id(rowset.rowset_id())
                    .ok_or_else(|| {
                        paro_error::internal(format!(
                            "published rowset {} missing from tablet {}",
                            rowset.rowset_id(),
                            tablet.tablet_id()
                        ))
                    })?;
                Self::restore_runtime_indexes_on_published_rowset(
                    tablet.tablet_id(),
                    &rowset,
                    &published,
                    &art_columns,
                    &fulltext_columns,
                )?;
            }

            Ok(())
        })()
    }

    fn restore_runtime_indexes_on_published_rowset(
        tablet_id: u64,
        staged: &RowsetSharedPtr,
        published: &RowsetSharedPtr,
        art_columns: &[u32],
        fulltext_columns: &[(u32, String)],
    ) -> Result<()> {
        if art_columns.is_empty() && fulltext_columns.is_empty() {
            return Ok(());
        }

        let staged_segments = staged.segments();
        let published_segments = published.segments();
        let segments_match = staged_segments.len() == published_segments.len()
            && staged_segments
                .iter()
                .zip(published_segments.iter())
                .all(|(source, target)| source.segment_id() == target.segment_id());

        let mut rebuild_fulltext = !fulltext_columns.is_empty() && !segments_match;
        let mut rebuild_art = !art_columns.is_empty() && !segments_match;

        if segments_match {
            for (source, target) in staged_segments.iter().zip(published_segments.iter()) {
                for (column_id, _) in fulltext_columns {
                    if let Some(index) = source.fulltext_index(*column_id) {
                        target.register_runtime_fulltext_index(*column_id, index);
                    } else {
                        rebuild_fulltext = true;
                    }
                }
                for &column_id in art_columns {
                    if let Some(index) = source.art_index(column_id) {
                        target.register_runtime_art_index(column_id, index);
                    } else {
                        rebuild_art = true;
                    }
                }
            }
        }

        if rebuild_fulltext {
            IndexRuntime::build_runtime_fulltext_indexes_for_rowset(published, fulltext_columns)?;
        }
        if rebuild_art {
            if let Err(err) =
                IndexRuntime::build_runtime_art_indexes_for_rowset(published, art_columns)
            {
                tracing::warn!(
                    error = %err,
                    tablet_id,
                    rowset_id = published.rowset_id(),
                    "ART index backfill failed for published transaction rowset; queries will fallback to scan"
                );
            }
        }
        Ok(())
    }

    fn rowset_replaced_locations(
        primary_update: &Option<PrimaryIndexUpdate>,
    ) -> Vec<PhysicalRowRef> {
        let Some(primary_update) = primary_update.as_ref() else {
            return Vec::new();
        };

        let mut locations = primary_update
            .pending_delete_vectors
            .iter()
            .flat_map(|(&(rowset_id, segment_id), delete_vector)| {
                delete_vector
                    .iter()
                    .map(move |row_offset| PhysicalRowRef::new(rowset_id, segment_id, row_offset))
            })
            .collect::<Vec<_>>();
        locations.sort_unstable_by_key(|location| {
            (location.rowset_id, location.segment_id, location.row_offset)
        });
        locations
    }

    fn capture_prepare_snapshots(
        primary_deletes: &[PendingPrimaryDelete],
        row_id_deletes: &[PendingRowIdDelete],
        rowsets: &[PendingRowset],
    ) -> Result<BTreeMap<u64, (TabletRef, PrepareSnapshot)>> {
        let mut lookup_keys_by_tablet: BTreeMap<u64, (TabletRef, Vec<Vec<u8>>)> = BTreeMap::new();
        for pending in primary_deletes {
            let entry = lookup_keys_by_tablet
                .entry(pending.tablet.tablet_id())
                .or_insert_with(|| (pending.tablet.clone(), Vec::new()));
            entry.1.extend(pending.keys.iter().cloned());
        }
        for pending in row_id_deletes {
            lookup_keys_by_tablet
                .entry(pending.tablet.tablet_id())
                .or_insert_with(|| (pending.tablet.clone(), Vec::new()));
        }
        for pending in rowsets {
            lookup_keys_by_tablet
                .entry(pending.tablet.tablet_id())
                .or_insert_with(|| (pending.tablet.clone(), Vec::new()));
        }

        lookup_keys_by_tablet
            .into_iter()
            .map(|(tablet_id, (tablet, lookup_keys))| {
                let snapshot = capture_prepare_snapshot(tablet.as_ref(), &lookup_keys)?;
                Ok((tablet_id, (tablet, snapshot)))
            })
            .collect()
    }

    fn rebuild_prepared_commit(
        &self,
        prepared_state: &mut PreparedStorageState,
        post_commit_hooks: Vec<PostCommitHookDescriptor>,
    ) -> Result<PreparedStorageCommit> {
        Self::cleanup_delete_patch_artifacts(&prepared_state.delete_patch_artifacts);
        prepared_state.delete_patch_artifacts.clear();

        let prepare_snapshots = Self::capture_prepare_snapshots(
            &prepared_state.primary_deletes,
            &prepared_state.row_id_deletes,
            &prepared_state.rowsets,
        )?;
        let mut mutations_by_tablet: BTreeMap<u64, Vec<TabletMutation>> = BTreeMap::new();
        let mut prepared_tablets = BTreeMap::new();
        let mut delete_patch_ordinal = 0usize;

        let storage_ops = (|| -> Result<Vec<StorageCommitOp>> {
            for pending in &prepared_state.primary_deletes {
                let tablet_id = pending.tablet.tablet_id();
                let (_, snapshot) = prepare_snapshots.get(&tablet_id).ok_or_else(|| {
                    paro_error::internal(format!(
                        "prepare snapshot missing for tablet {} primary delete",
                        tablet_id
                    ))
                })?;
                if let Some(patch) = build_delete_patch_from_primary_keys(snapshot, &pending.keys)?
                {
                    let materialized = materialize_delete_patch(
                        pending.tablet.as_ref(),
                        self.id,
                        delete_patch_ordinal,
                        patch,
                    )?;
                    delete_patch_ordinal = delete_patch_ordinal.saturating_add(1);
                    if let Some(path) = materialized.artifact_path.clone() {
                        prepared_state.delete_patch_artifacts.push(path);
                    }
                    prepared_tablets
                        .entry(tablet_id)
                        .or_insert_with(|| PreparedTabletCommit {
                            tablet: pending.tablet.clone(),
                            token: snapshot.prepare_token(),
                        });
                    mutations_by_tablet.entry(tablet_id).or_default().push(
                        TabletMutation::ApplyDeletePatch {
                            patch: materialized.patch_ref,
                            deleted_row_count: materialized.deleted_row_count,
                        },
                    );
                }
            }

            for pending in &prepared_state.row_id_deletes {
                let tablet_id = pending.tablet.tablet_id();
                let (_, snapshot) = prepare_snapshots.get(&tablet_id).ok_or_else(|| {
                    paro_error::internal(format!(
                        "prepare snapshot missing for tablet {} row-id delete",
                        tablet_id
                    ))
                })?;
                if let Some(patch) = build_delete_patch_from_row_refs(snapshot, &pending.locations)?
                {
                    let materialized = materialize_delete_patch(
                        pending.tablet.as_ref(),
                        self.id,
                        delete_patch_ordinal,
                        patch,
                    )?;
                    delete_patch_ordinal = delete_patch_ordinal.saturating_add(1);
                    if let Some(path) = materialized.artifact_path.clone() {
                        prepared_state.delete_patch_artifacts.push(path);
                    }
                    prepared_tablets
                        .entry(tablet_id)
                        .or_insert_with(|| PreparedTabletCommit {
                            tablet: pending.tablet.clone(),
                            token: snapshot.prepare_token(),
                        });
                    mutations_by_tablet.entry(tablet_id).or_default().push(
                        TabletMutation::ApplyDeletePatch {
                            patch: materialized.patch_ref,
                            deleted_row_count: materialized.deleted_row_count,
                        },
                    );
                }
            }

            for pending in &prepared_state.rowsets {
                let tablet_id = pending.tablet.tablet_id();
                let (_, snapshot) = prepare_snapshots.get(&tablet_id).ok_or_else(|| {
                    paro_error::internal(format!(
                        "prepare snapshot missing for tablet {} rowset publish",
                        tablet_id
                    ))
                })?;
                prepared_tablets
                    .entry(tablet_id)
                    .or_insert_with(|| PreparedTabletCommit {
                        tablet: pending.tablet.clone(),
                        token: snapshot.prepare_token(),
                    });

                if let Some(patch) = build_delete_patch_from_row_refs(
                    snapshot,
                    &Self::rowset_replaced_locations(&pending.primary_update),
                )? {
                    let materialized = materialize_delete_patch(
                        pending.tablet.as_ref(),
                        self.id,
                        delete_patch_ordinal,
                        patch,
                    )?;
                    delete_patch_ordinal = delete_patch_ordinal.saturating_add(1);
                    if let Some(path) = materialized.artifact_path.clone() {
                        prepared_state.delete_patch_artifacts.push(path);
                    }
                    mutations_by_tablet.entry(tablet_id).or_default().push(
                        TabletMutation::ApplyDeletePatch {
                            patch: materialized.patch_ref,
                            deleted_row_count: materialized.deleted_row_count,
                        },
                    );
                }

                mutations_by_tablet.entry(tablet_id).or_default().push(
                    TabletMutation::PublishRowset {
                        rowset_id: pending.rowset.rowset_id(),
                        version_span: VersionSpan {
                            start: pending.rowset.start_version(),
                            end: pending.rowset.end_version(),
                        },
                        rowset_ref: ArtifactRef::from_tablet_path(
                            pending.tablet.data_dir(),
                            &pending.rowset_path,
                        )?,
                    },
                );
            }

            Ok(mutations_by_tablet
                .into_iter()
                .filter_map(|(tablet_id, mutations)| {
                    if mutations.is_empty() {
                        None
                    } else {
                        Some(StorageCommitOp::Tablet(TabletApplyOp {
                            tablet_id,
                            mutations,
                        }))
                    }
                })
                .collect::<Vec<_>>())
        })();

        let storage_ops = match storage_ops {
            Ok(storage_ops) => storage_ops,
            Err(err) => {
                Self::cleanup_delete_patch_artifacts(&prepared_state.delete_patch_artifacts);
                prepared_state.delete_patch_artifacts.clear();
                return Err(err);
            }
        };

        Ok(PreparedStorageCommit {
            storage_ops,
            post_commit_hooks,
            tablets: prepared_tablets.into_values().collect(),
        })
    }

    pub fn prepare_commit(&self) -> Result<PreparedStorageCommit> {
        self.materialize_pending_writers()?;

        let pending = {
            let mut ops = self
                .pending_ops
                .lock()
                .map_err(|e| paro_error::internal(format!("failed to lock pending ops: {}", e)))?;
            std::mem::take(&mut *ops)
        };
        let (primary_deletes, row_id_deletes, rowsets) = self.split_pending_operations(pending);
        let graph_deltas = self.take_graph_dml_deltas()?;
        let post_commit_hooks = if graph_deltas.is_empty() {
            Vec::new()
        } else {
            vec![PostCommitHookDescriptor::GraphDmlMaintenance {
                deltas: graph_deltas
                    .into_iter()
                    .map(|(table_oid, delta)| {
                        GraphDmlHookDelta::from_parts(
                            table_oid,
                            delta.inserted,
                            delta.deleted,
                            delta.updated,
                            &delta.updated_columns,
                        )
                    })
                    .collect(),
            }]
        };
        let mut prepared_state = PreparedStorageState {
            rowsets,
            primary_deletes,
            row_id_deletes,
            delete_patch_artifacts: Vec::new(),
        };
        let prepared_commit =
            self.rebuild_prepared_commit(&mut prepared_state, post_commit_hooks)?;

        *self.prepared_storage_state.lock().map_err(|e| {
            paro_error::internal(format!("failed to lock prepared storage state: {}", e))
        })? = Some(prepared_state);

        Ok(prepared_commit)
    }

    pub fn reprepare_commit(
        &self,
        post_commit_hooks: &[PostCommitHookDescriptor],
    ) -> Result<PreparedStorageCommit> {
        let mut prepared_state = self.prepared_storage_state.lock().map_err(|e| {
            paro_error::internal(format!("failed to lock prepared storage state: {}", e))
        })?;
        let prepared_state = prepared_state.as_mut().ok_or_else(|| {
            paro_error::internal("prepared storage state missing during reprepare")
        })?;
        self.rebuild_prepared_commit(prepared_state, post_commit_hooks.to_vec())
    }

    fn apply_pending_writes(&self, commit_id: u64) -> Result<()> {
        let pending = {
            let mut ops = self
                .pending_ops
                .lock()
                .map_err(|e| paro_error::internal(format!("failed to lock pending ops: {}", e)))?;
            std::mem::take(&mut *ops)
        };
        let (primary_deletes, row_id_deletes, rowsets) = self.split_pending_operations(pending);
        let result =
            self.apply_materialized_writes(commit_id, &primary_deletes, &row_id_deletes, rowsets);

        for pending in &primary_deletes {
            pending
                .tablet
                .release_primary_delete_intents(self.id, &pending.keys);
        }
        for pending in &row_id_deletes {
            pending
                .tablet
                .release_row_id_delete_intents(self.id, &pending.locations);
        }

        result
    }

    fn commit_prepared_storage(&self, commit_id: u64) -> Result<()> {
        let prepared = self
            .prepared_storage_state
            .lock()
            .map_err(|e| {
                paro_error::internal(format!("failed to lock prepared storage state: {}", e))
            })?
            .take();
        let Some(prepared) = prepared else {
            self.materialize_pending_writers()?;
            return self.apply_pending_writes(commit_id);
        };

        let result = self.apply_materialized_writes(
            commit_id,
            &prepared.primary_deletes,
            &prepared.row_id_deletes,
            prepared.rowsets,
        );

        for pending in &prepared.primary_deletes {
            pending
                .tablet
                .release_primary_delete_intents(self.id, &pending.keys);
        }
        for pending in &prepared.row_id_deletes {
            pending
                .tablet
                .release_row_id_delete_intents(self.id, &pending.locations);
        }
        Self::cleanup_delete_patch_artifacts(&prepared.delete_patch_artifacts);

        result
    }

    fn release_prepared_storage_state(&self) -> Result<()> {
        let prepared = self
            .prepared_storage_state
            .lock()
            .map_err(|e| {
                paro_error::internal(format!("failed to lock prepared storage state: {}", e))
            })?
            .take();
        let Some(prepared) = prepared else {
            return Ok(());
        };

        for pending in &prepared.primary_deletes {
            pending
                .tablet
                .release_primary_delete_intents(self.id, &pending.keys);
        }
        for pending in &prepared.row_id_deletes {
            pending
                .tablet
                .release_row_id_delete_intents(self.id, &pending.locations);
        }
        Self::cleanup_delete_patch_artifacts(&prepared.delete_patch_artifacts);

        Ok(())
    }

    fn cleanup_delete_patch_artifacts(paths: &[PathBuf]) {
        for path in paths.iter().rev() {
            let _ = std::fs::remove_file(path);
            if let Some(parent) = path.parent() {
                let _ = std::fs::remove_dir(parent);
                if let Some(grandparent) = parent.parent() {
                    let _ = std::fs::remove_dir(grandparent);
                }
            }
        }
    }

    fn rollback_pending_writes(&self) {
        let pending = match self.pending_ops.lock() {
            Ok(mut ops) => std::mem::take(&mut *ops),
            Err(_) => return,
        };

        for op in pending {
            self.rollback_pending_op(op);
        }

        if let Ok(mut pending) = self.pending_art_columns.lock() {
            pending.clear();
        }
        if let Ok(mut pending) = self.pending_fulltext_columns.lock() {
            pending.clear();
        }
        if let Ok(mut pending) = self.pending_dml_tables.lock() {
            pending.clear();
        }
        if let Ok(mut pending) = self.pending_graph_dml.lock() {
            pending.clear();
        }
        self.rollback_prepared_storage_state();
    }

    /// Commit the transaction with the given commit ID.
    ///
    /// ```cpp
    ///                                   unique_ptr<StorageCommitState> commit_state) noexcept {
    ///     this->commit_id = commit_info.commit_id;
    ///     if (!ChangesMade()) { return ErrorData(); }
    ///     storage->Commit(commit_state.get());
    ///     undo_buffer.Commit(iterator_state, commit_info);
    ///     return ErrorData();
    /// }
    /// ```
    ///
    /// # Arguments
    /// * `commit_id` - The commit timestamp to assign
    ///
    /// # Returns
    /// * `Ok(())` - Transaction committed successfully
    /// * `Err(paro_error::transaction_aborted)` - Failed to acquire lock (poisoned mutex)
    ///
    /// # Note
    /// This method handles the transaction-level commit. Data changes are already
    /// persisted via Tablet/Rowset; the undo buffer only tracks catalog-level edits.
    pub fn commit(&self, commit_id: u64) -> Result<()> {
        self.commit_prepared_storage(commit_id)?;
        self.finish_commit_state(commit_id)
    }

    pub fn finalize_commit_after_apply(&self, commit_id: u64) -> Result<()> {
        self.release_prepared_storage_state()?;
        self.finish_commit_state(commit_id)
    }

    fn finish_commit_state(&self, commit_id: u64) -> Result<()> {
        if let Ok(mut pending) = self.pending_art_columns.lock() {
            pending.clear();
        }
        if let Ok(mut pending) = self.pending_fulltext_columns.lock() {
            pending.clear();
        }
        if let Ok(mut pending) = self.pending_dml_tables.lock() {
            pending.clear();
        }

        // Set commit_id
        {
            let mut cid = self
                .commit_id
                .lock()
                .map_err(|e| paro_error::internal(format!("failed to acquire lock: {}", e)))?;
            *cid = commit_id;
        }

        // If no undo changes were made, we're done (fast path)
        if !self.undo_changes_made() {
            self.set_awaiting_cleanup(true);
            return Ok(());
        }

        // Commit the undo buffer
        {
            let buffer = self
                .undo_buffer
                .lock()
                .map_err(|e| paro_error::internal(format!("failed to acquire lock: {}", e)))?;
            // Commit the undo buffer with the commit_id
            // This iterates entries and finalizes changes
            buffer.commit(self, commit_id);
        }

        // Mark as awaiting cleanup after successful commit
        self.set_awaiting_cleanup(true);
        Ok(())
    }

    /// Rollback the transaction, undoing all changes.
    ///
    /// ```cpp
    ///     try {
    ///         storage->Rollback();
    ///         undo_buffer.Rollback();
    ///         return ErrorData();
    ///     } catch (std::exception &ex) {
    ///         return ErrorData(ex);
    ///     }
    /// }
    /// ```
    ///
    /// # Returns
    /// * `Ok(())` - Transaction rolled back successfully
    /// * `Err(paro_error::transaction_aborted)` - Failed to acquire lock (poisoned mutex)
    pub fn rollback(&self) -> Result<()> {
        self.rollback_pending_writers();
        self.rollback_pending_writes();

        // Rollback the undo buffer
        {
            let mut buffer = self
                .undo_buffer
                .lock()
                .map_err(|e| paro_error::internal(format!("failed to acquire lock: {}", e)))?;
            buffer.rollback(self);
        }

        // Mark as awaiting cleanup after rollback
        self.set_awaiting_cleanup(true);
        if let Ok(mut pending) = self.pending_graph_dml.lock() {
            pending.clear();
        }
        Ok(())
    }

    /// Get the commit ID (0 if not yet committed).
    pub fn get_commit_id(&self) -> u64 {
        match self.commit_id.lock() {
            Ok(cid) => *cid,
            Err(_) => 0,
        }
    }

    /// Get the visible version for MVCC reads.
    ///
    /// `start_time` tracks the next commit id that was not yet visible when the
    /// transaction started, so readers must use the previous committed version.
    #[inline]
    pub fn visible_version(&self) -> u64 {
        self.start_time.saturating_sub(1)
    }

    /// Get the commit version for MVCC writes (0 if not yet committed).
    #[inline]
    pub fn commit_version(&self) -> u64 {
        self.get_commit_id()
    }

    /// Perform cleanup on this transaction.
    ///
    /// Called after commit/rollback when the transaction is no longer needed
    /// by any active transaction. This releases resources held by the undo buffer.
    ///
    /// ```cpp
    ///     undo_buffer.Cleanup(lowest_start_time);
    /// }
    /// ```
    ///
    /// # Arguments
    /// * `lowest_start_time` - The lowest start time among active transactions.
    ///   Entries older than this can be safely cleaned up.
    pub fn cleanup(&self, lowest_start_time: u64) {
        if let Ok(mut buffer) = self.undo_buffer.lock() {
            buffer.cleanup(lowest_start_time, self.active_transaction_state());
        }
    }

    // ==================== Undo Buffer Push Methods ====================
    // These methods record undo information for rollback and automatically
    // promote the transaction from read-only to read-write.

    /// Push an append (insert) operation.
    ///
    /// Called when inserting rows into a table. Records the row range
    /// Legacy MVCC path; undo entries are no longer recorded.
    ///
    /// ```cpp
    /// ```
    ///
    /// # Arguments
    /// * `table_id` - The OID of the table being modified
    /// * `start_row` - The starting row ID of the inserted rows
    /// * `row_count` - The number of rows inserted
    pub fn push_append(&self, _table_id: u64, _start_row: u64, _row_count: u64) {
        // Legacy MVCC path removed; row-level undo is not tracked anymore.
        self.bump_mutation_generation();
        self.set_read_write();
    }

    /// Push a delete operation.
    ///
    /// Called when deleting rows from a table. Records the deleted row IDs
    /// Legacy MVCC path; undo entries are no longer recorded.
    ///
    /// ```cpp
    ///     idx_t vector_idx, row_t rows[], idx_t count, idx_t base_row)
    /// ```
    ///
    /// # Arguments
    /// * `table_id` - The OID of the table being modified
    /// * `row_ids` - The row IDs being deleted
    pub fn push_delete(&self, _table_id: u64, _row_ids: &[u64]) {
        // Legacy MVCC path removed; row-level undo is not tracked anymore.
        self.bump_mutation_generation();
        self.set_read_write();
    }

    /// Push an update operation.
    ///
    /// Called when updating rows in a table. Records the old values
    /// Legacy MVCC path; undo entries are no longer recorded.
    ///
    /// ```cpp
    ///     table_t table_id, idx_t entries, row_t rows[])
    /// ```
    ///
    /// # Arguments
    /// * `table_id` - The OID of the table being modified
    /// * `row_ids` - The row IDs being updated
    pub fn push_update(&self, _table_id: u64, _row_ids: &[u64]) {
        // Legacy MVCC path removed; row-level undo is not tracked anymore.
        self.bump_mutation_generation();
        self.set_read_write();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primary_key::{PrimaryKeySerializer, RowID};
    use crate::table::table_factory::TableFactory;
    use crate::table::table_handle::TableHandle;
    use crate::tablet::tablet_reader::TabletReaderParams;
    use crate::tablet::KeysType;
    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;

    fn create_table(types: &[LogicalType]) -> TableHandle {
        TableFactory::default().create_table(types).unwrap()
    }

    fn create_table_with_keys(types: &[LogicalType], keys_type: KeysType) -> TableHandle {
        TableFactory::default()
            .create_table_with_keys(types, keys_type)
            .unwrap()
    }

    fn collect_rows_i32_pair(table: &TableHandle) -> Vec<(i32, i32)> {
        let mut rows = Vec::new();
        for chunk in table.scan_chunks().expect("scan chunks") {
            let id_col = chunk.column(0).expect("id column");
            let value_col = chunk.column(1).expect("value column");
            for row in 0..chunk.size() {
                rows.push((
                    id_col.get_i32(row).expect("id as i32"),
                    value_col.get_i32(row).expect("value as i32"),
                ));
            }
        }
        rows.sort_unstable_by_key(|(id, _)| *id);
        rows
    }

    fn collect_rows_i32(table: &TableHandle) -> Vec<i32> {
        let mut rows = Vec::new();
        for chunk in table.scan_chunks().expect("scan chunks") {
            let col = chunk.column(0).expect("column 0");
            for row in 0..chunk.size() {
                rows.push(col.get_i32(row).expect("column 0 as i32"));
            }
        }
        rows.sort_unstable();
        rows
    }

    fn primary_key_bytes(table: &TableHandle, key: i32) -> Vec<u8> {
        let tablet = table.tablet();
        let schema = tablet.schema().expect("tablet schema");
        let serializer = PrimaryKeySerializer::from_schema_ref(&schema).expect("pk serializer");
        let key_chunk = Chunk::from_vectors(vec![Vector::from_i32(&[key])]);
        serializer
            .encode_row(&key_chunk, 0)
            .expect("encode primary key")
    }

    fn row_id_location_by_value(table: &TableHandle, target: i32) -> PhysicalRowRef {
        let mut reader = table
            .create_reader(
                TabletReaderParams::with_version(table.max_version()).with_emit_row_id(true),
            )
            .expect("create reader");
        reader.prepare().expect("prepare reader");

        while let Some(chunk) = reader.get_next_chunk().expect("read chunk") {
            let values = chunk.column(0).expect("value column");
            let row_ids = chunk
                .column(chunk.column_count() - 1)
                .expect("row id column");
            for row in 0..chunk.size() {
                if values.get_i32(row) == Some(target) {
                    let row_id =
                        RowID::from_raw(row_ids.get_i64(row).expect("row_id as i64") as u64);
                    let location = table.tablet().decode_row_id(row_id).expect("decode row id");
                    return location;
                }
            }
        }

        panic!("target row {target} not found");
    }

    // ==================== Happy Path Tests ====================

    #[test]
    fn test_transaction_new_defaults() {
        let txn = Transaction::new(1, 100);

        assert_eq!(txn.id, 1);
        assert_eq!(txn.start_time, 100);
        assert_eq!(txn.get_commit_id(), 0);
        assert!(txn.is_read_only());
        assert_eq!(txn.catalog_version(), 0);
        assert!(!txn.is_awaiting_cleanup());
        assert!(!txn.changes_made());
    }

    #[test]
    fn test_transaction_with_catalog_version() {
        let txn = Transaction::with_catalog_version(2, 200, 42);

        assert_eq!(txn.id, 2);
        assert_eq!(txn.start_time, 200);
        assert_eq!(txn.catalog_version(), 42);
        assert!(txn.is_read_only());
    }

    #[test]
    fn test_transaction_read_write_promotion() {
        let txn = Transaction::new(1, 100);

        // Initially read-only
        assert!(txn.is_read_only());

        // Promote to read-write
        txn.set_read_write();
        assert!(!txn.is_read_only());

        // Cannot go back to read-only (one-way transition)
        // This is by design - no set_read_only method
    }

    #[test]
    fn test_transaction_changes_made_with_undo_entries() {
        let txn = Transaction::new(1, 100);

        // No changes initially
        assert!(!txn.changes_made());

        // Add an undo entry directly to the undo buffer.
        {
            let mut buffer = txn.undo_buffer.lock().expect("lock undo buffer");
            buffer.push_insert(1, 0, 1);
        }

        // Now changes_made should return true
        assert!(txn.changes_made());
    }

    #[test]
    fn test_transaction_commit_sets_commit_id() {
        let txn = Transaction::new(1, 100);

        assert_eq!(txn.get_commit_id(), 0);
        assert!(!txn.is_awaiting_cleanup());

        txn.commit(500).expect("commit should succeed");

        assert_eq!(txn.get_commit_id(), 500);
        assert!(txn.is_awaiting_cleanup());
    }

    #[test]
    fn test_transaction_rollback_clears_undo_buffer() {
        let txn = Transaction::new(1, 100);

        // Add some undo entries via the undo buffer.
        {
            let mut buffer = txn.undo_buffer.lock().expect("lock undo buffer");
            buffer.push_insert(1, 0, 1);
        }

        assert!(txn.changes_made());

        // Rollback
        txn.rollback().expect("rollback should succeed");

        // Undo buffer should be cleared
        assert!(!txn.changes_made());
        assert!(txn.is_awaiting_cleanup());
    }

    #[test]
    fn test_transaction_catalog_version_update() {
        let txn = Transaction::new(1, 100);

        assert_eq!(txn.catalog_version(), 0);

        txn.set_catalog_version(10);
        assert_eq!(txn.catalog_version(), 10);

        txn.set_catalog_version(20);
        assert_eq!(txn.catalog_version(), 20);
    }

    #[test]
    fn test_transaction_savepoint_restores_writer_backed_dml() {
        let table = create_table_with_keys(
            &[LogicalType::Integer, LogicalType::Integer],
            KeysType::PrimaryKeys,
        );
        let tablet = table.tablet();
        let txn = Transaction::new(9201, 9201);

        txn.append_to_tablet(
            tablet.clone(),
            &Chunk::from_vectors(vec![Vector::from_i32(&[1, 2]), Vector::from_i32(&[10, 20])]),
        )
        .expect("append first batch");
        let mark = txn.mark_savepoint().expect("mark savepoint");
        txn.append_to_tablet(
            tablet,
            &Chunk::from_vectors(vec![Vector::from_i32(&[3, 4]), Vector::from_i32(&[30, 40])]),
        )
        .expect("append second batch");

        txn.rollback_to_savepoint(&mark)
            .expect("rollback to savepoint");
        txn.commit(9202).expect("commit txn");

        assert_eq!(collect_rows_i32_pair(&table), vec![(1, 10), (2, 20)]);
    }

    #[test]
    fn test_pending_primary_delete_commit_and_rollback_release_intents() {
        let table = create_table_with_keys(
            &[LogicalType::Integer, LogicalType::Integer],
            KeysType::PrimaryKeys,
        );
        table
            .append(&Chunk::from_vectors(vec![
                Vector::from_i32(&[1, 2]),
                Vector::from_i32(&[10, 20]),
            ]))
            .expect("append rows");
        let tablet = table.tablet();
        let key_bytes = primary_key_bytes(&table, 1);

        let txn1 = Transaction::new(9101, 9101);
        txn1.add_pending_primary_delete(tablet.clone(), vec![key_bytes.clone()])
            .expect("stage primary delete");

        // Pending ops are invisible before commit.
        assert_eq!(collect_rows_i32_pair(&table), vec![(1, 10), (2, 20)]);

        let txn2 = Transaction::new(9102, 9102);
        let conflict = txn2
            .add_pending_primary_delete(tablet.clone(), vec![key_bytes.clone()])
            .expect_err("should conflict with txn1 intent");
        assert!(
            conflict.to_string().contains("write-write conflict"),
            "expected write-write conflict, got: {conflict}"
        );

        txn1.rollback().expect("rollback txn1");
        assert_eq!(collect_rows_i32_pair(&table), vec![(1, 10), (2, 20)]);

        // Rollback releases intents; txn2 can stage and commit the delete.
        txn2.add_pending_primary_delete(tablet, vec![key_bytes])
            .expect("stage primary delete after rollback");
        txn2.commit(9202).expect("commit txn2");
        assert_eq!(collect_rows_i32_pair(&table), vec![(2, 20)]);
    }

    #[test]
    fn test_primary_delete_reprepare_refreshes_stale_prepare_token_after_rowset_epoch_changes() {
        let table = create_table_with_keys(
            &[LogicalType::Integer, LogicalType::Integer],
            KeysType::PrimaryKeys,
        );
        table
            .append(&Chunk::from_vectors(vec![
                Vector::from_i32(&[1, 2, 3]),
                Vector::from_i32(&[10, 20, 30]),
            ]))
            .expect("append rows");

        let tablet = table.tablet();
        let txn = Transaction::new(9_401, 9_401);
        txn.add_pending_primary_delete(tablet.clone(), vec![primary_key_bytes(&table, 2)])
            .expect("stage primary delete");

        let prepared = txn.prepare_commit().expect("prepare commit");
        assert_eq!(prepared.tablets.len(), 1);
        let original_token = prepared.tablets[0].token;

        let StorageCommitOp::Tablet(original_apply) = prepared
            .storage_ops
            .first()
            .expect("prepared storage op")
            .clone();
        let TabletMutation::ApplyDeletePatch {
            patch: original_patch,
            deleted_row_count,
        } = original_apply
            .mutations
            .into_iter()
            .next()
            .expect("delete mutation")
        else {
            panic!("expected delete patch mutation");
        };
        assert_eq!(deleted_row_count, 1);
        assert_eq!(
            original_patch
                .decode_row_refs_for_tablet(tablet.data_dir())
                .unwrap()
                .len(),
            1
        );

        tablet.bump_rowset_epoch();
        let stale = tablet
            .validate_prepare_token(&original_token)
            .expect_err("rowset epoch bump should stale original token");
        assert!(
            stale.to_string().contains("rowset_epoch"),
            "expected stale token error, got: {stale}"
        );

        let reprepared = txn
            .reprepare_commit(&prepared.post_commit_hooks)
            .expect("reprepare commit after stale token");
        assert_eq!(reprepared.tablets.len(), 1);
        let refreshed_token = reprepared.tablets[0].token;
        assert!(refreshed_token.rowset_epoch > original_token.rowset_epoch);
        tablet
            .validate_prepare_token(&refreshed_token)
            .expect("refreshed token should validate");

        let StorageCommitOp::Tablet(reprepared_apply) = reprepared
            .storage_ops
            .first()
            .expect("reprepared storage op")
            .clone();
        let TabletMutation::ApplyDeletePatch {
            patch: refreshed_patch,
            deleted_row_count,
        } = reprepared_apply
            .mutations
            .into_iter()
            .next()
            .expect("refreshed delete mutation")
        else {
            panic!("expected refreshed delete patch mutation");
        };
        assert_eq!(deleted_row_count, 1);
        assert_eq!(
            refreshed_patch
                .decode_row_refs_for_tablet(tablet.data_dir())
                .unwrap()
                .len(),
            1
        );

        txn.commit(9_402).expect("commit refreshed delete patch");
        assert_eq!(collect_rows_i32_pair(&table), vec![(1, 10), (3, 30)]);
    }

    #[test]
    fn test_pending_row_id_delete_commit_and_rollback_release_intents() {
        let table = create_table(&[LogicalType::Integer]);
        table
            .append(&Chunk::from_vectors(vec![Vector::from_i32(&[1, 2, 3])]))
            .expect("append rows");
        let tablet = table.tablet();
        let location = row_id_location_by_value(&table, 2);

        let txn1 = Transaction::new(9301, 9301);
        txn1.add_pending_row_id_delete(tablet.clone(), vec![location])
            .expect("stage row-id delete");

        // Pending ops are invisible before commit.
        assert_eq!(collect_rows_i32(&table), vec![1, 2, 3]);

        let txn2 = Transaction::new(9302, 9302);
        let conflict = txn2
            .add_pending_row_id_delete(tablet.clone(), vec![location])
            .expect_err("should conflict with txn1 intent");
        assert!(
            conflict.to_string().contains("write-write conflict"),
            "expected write-write conflict, got: {conflict}"
        );

        txn1.rollback().expect("rollback txn1");
        assert_eq!(collect_rows_i32(&table), vec![1, 2, 3]);

        // Rollback releases intents; txn2 can stage and commit the delete.
        txn2.add_pending_row_id_delete(tablet, vec![location])
            .expect("stage row-id delete after rollback");
        txn2.commit(9402).expect("commit txn2");
        assert_eq!(collect_rows_i32(&table), vec![1, 3]);
    }

    #[test]
    fn test_expired_delete_intents_are_pruned_on_reacquire() {
        let table = create_table_with_keys(
            &[LogicalType::Integer, LogicalType::Integer],
            KeysType::PrimaryKeys,
        );
        table
            .append(&Chunk::from_vectors(vec![
                Vector::from_i32(&[1, 2]),
                Vector::from_i32(&[10, 20]),
            ]))
            .expect("append rows");
        let tablet = table.tablet();
        let key_bytes = primary_key_bytes(&table, 1);

        let txn1 = Transaction::new(9501, 9501);
        txn1.add_pending_primary_delete(tablet.clone(), vec![key_bytes.clone()])
            .expect("stage primary delete");
        tablet.expire_delete_intents_for_test();

        let txn2 = Transaction::new(9502, 9502);
        txn2.add_pending_primary_delete(tablet.clone(), vec![key_bytes.clone()])
            .expect("expired intent should be pruned");

        let row_location = row_id_location_by_value(&table, 2);
        let txn3 = Transaction::new(9503, 9503);
        txn3.add_pending_row_id_delete(tablet.clone(), vec![row_location])
            .expect("stage row-id delete");
        tablet.expire_delete_intents_for_test();

        let txn4 = Transaction::new(9504, 9504);
        txn4.add_pending_row_id_delete(tablet, vec![row_location])
            .expect("expired row-id intent should be pruned");
    }

    // ==================== Push Methods Tests ====================

    #[test]
    fn test_push_append_promotes_to_read_write() {
        let txn = Transaction::new(1, 100);

        assert!(txn.is_read_only());

        txn.push_append(42, 100, 3);

        assert!(!txn.is_read_only());
        assert!(!txn.changes_made());
    }

    #[test]
    fn test_push_append_records_inserts() {
        let txn = Transaction::new(1, 100);

        assert!(txn.is_read_only());

        // Insert 3 rows starting at row 100
        txn.push_append(42, 100, 3);

        assert!(!txn.is_read_only());
        assert!(!txn.changes_made());

        // Legacy DML undo is disabled; no entries should be recorded.
        let buffer = txn.undo_buffer.lock().expect("lock undo_buffer");
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_push_delete_records_deletions() {
        let txn = Transaction::new(1, 100);

        assert!(txn.is_read_only());

        // Delete rows 10, 20, 30
        txn.push_delete(42, &[10, 20, 30]);

        assert!(!txn.is_read_only());
        assert!(!txn.changes_made());

        // Legacy DML undo is disabled; no entries should be recorded.
        let buffer = txn.undo_buffer.lock().expect("lock undo_buffer");
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_push_update_records_updates() {
        let txn = Transaction::new(1, 100);

        assert!(txn.is_read_only());

        // Update rows 5, 6
        txn.push_update(42, &[5, 6]);

        assert!(!txn.is_read_only());
        assert!(!txn.changes_made());

        // Legacy DML undo is disabled; no entries should be recorded.
        let buffer = txn.undo_buffer.lock().expect("lock undo_buffer");
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_multiple_push_operations() {
        let txn = Transaction::new(1, 100);

        // Mix of operations
        txn.push_append(1, 0, 2);
        txn.push_delete(1, &[100]);
        txn.push_update(1, &[50, 51, 52]);

        assert!(!txn.is_read_only());
        assert!(!txn.changes_made());

        // Legacy DML paths do not record undo entries.
        let buffer = txn.undo_buffer.lock().expect("lock undo_buffer");
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_push_then_rollback_clears_all() {
        let txn = Transaction::new(1, 100);

        txn.push_append(1, 0, 5);
        txn.push_delete(1, &[10, 11]);

        assert!(!txn.changes_made());

        txn.rollback().expect("rollback should succeed");

        assert!(!txn.changes_made());
        assert!(txn.is_awaiting_cleanup());
    }

    // ==================== Error/Edge Case Tests ====================

    #[test]
    fn test_transaction_multiple_commits_overwrites() {
        // Edge case: calling commit multiple times
        // This shouldn't happen in practice, but we should handle it gracefully
        let txn = Transaction::new(1, 100);

        txn.commit(500).expect("first commit should succeed");
        assert_eq!(txn.get_commit_id(), 500);

        // Second commit overwrites (not ideal, but safe)
        txn.commit(600).expect("second commit should succeed");
        assert_eq!(txn.get_commit_id(), 600);
    }

    #[test]
    fn test_transaction_rollback_after_commit() {
        // Edge case: rollback after commit
        // This shouldn't happen in practice, but should not panic
        let txn = Transaction::new(1, 100);

        // Add entry and commit
        {
            let mut buffer = txn.undo_buffer.lock().expect("lock undo buffer");
            buffer.push_insert(1, 0, 1);
        }
        txn.commit(500).expect("commit should succeed");

        // Rollback after commit (unusual but should not panic)
        txn.rollback().expect("rollback should succeed");

        // Undo buffer is cleared, but commit_id remains
        assert!(!txn.changes_made());
        assert_eq!(txn.get_commit_id(), 500);
    }

    #[test]
    fn test_push_empty_operations() {
        // Edge case: push with empty data
        let txn = Transaction::new(1, 100);

        // Push append with 0 rows - still creates an entry (with count=0)
        txn.push_append(1, 0, 0);
        // Push delete with empty slice - no entry created (early return)
        txn.push_delete(1, &[]);
        // Push update with empty slice - no entry created (early return)
        txn.push_update(1, &[]);

        // Transaction should be promoted to read-write (push_append was called)
        assert!(!txn.is_read_only());
        // Zero-length delete/update inputs do not create undo entries.
        let buffer = txn.undo_buffer.lock().expect("lock undo_buffer");
        assert_eq!(buffer.len(), 0);
    }

    // Commit and rollback result tests

    #[test]
    fn test_commit_returns_ok_on_success() {
        let txn = Transaction::new(1, 100);

        // Add some changes
        {
            let mut buffer = txn.undo_buffer.lock().expect("lock undo buffer");
            buffer.push_insert(1, 0, 3);
        }

        assert!(txn.changes_made());

        // Commit should return Ok
        let result = txn.commit(500);
        assert!(result.is_ok());
        assert_eq!(txn.get_commit_id(), 500);
        assert!(txn.is_awaiting_cleanup());
    }

    #[test]
    fn test_commit_no_changes_fast_path() {
        // Test that commit with no changes still succeeds (fast path)
        let txn = Transaction::new(1, 100);

        assert!(!txn.changes_made());

        let result = txn.commit(500);
        assert!(result.is_ok());
        assert_eq!(txn.get_commit_id(), 500);
        assert!(txn.is_awaiting_cleanup());
    }

    #[test]
    fn test_rollback_returns_ok_on_success() {
        let txn = Transaction::new(1, 100);

        // Add some changes
        {
            let mut buffer = txn.undo_buffer.lock().expect("lock undo buffer");
            buffer.push_insert(1, 0, 1);
        }

        assert!(txn.changes_made());

        // Rollback should return Ok
        let result = txn.rollback();
        assert!(result.is_ok());
        assert!(!txn.changes_made());
        assert!(txn.is_awaiting_cleanup());
    }

    #[test]
    fn test_rollback_empty_buffer_returns_ok() {
        // Rollback on empty buffer should still succeed
        let txn = Transaction::new(1, 100);

        assert!(!txn.changes_made());

        let result = txn.rollback();
        assert!(result.is_ok());
        assert!(txn.is_awaiting_cleanup());
    }

    #[test]
    fn test_commit_then_rollback_sequence() {
        // Test the sequence: make changes -> commit -> rollback (edge case)
        let txn = Transaction::new(1, 100);

        {
            let mut buffer = txn.undo_buffer.lock().expect("lock undo buffer");
            buffer.push_insert(1, 0, 1);
        }
        assert!(txn.changes_made());

        // Commit
        let commit_result = txn.commit(500);
        assert!(commit_result.is_ok());
        assert_eq!(txn.get_commit_id(), 500);

        // Rollback after commit (clears buffer but keeps commit_id)
        let rollback_result = txn.rollback();
        assert!(rollback_result.is_ok());
        assert!(!txn.changes_made());
        assert_eq!(txn.get_commit_id(), 500); // commit_id preserved
    }

    // LocalStorage integration removed.
}
