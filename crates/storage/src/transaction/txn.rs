// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Transaction State Management
//!
//! Coordinates undo tracking, staged writes, commit, rollback, and cleanup.

use crate::search::write_path::SearchWriteContext;
use crate::tablet::{PhysicalRowRef, PrimaryIndexUpdate, TabletRef};
use crate::transaction::undo_buffer::{ActiveTransactionState, UndoBuffer};
use crate::transaction::write_buffer::{
    GraphTableDmlDelta, PendingMutation, PendingPrimaryDelete, PendingRowIdDelete, PendingRowset,
    PreparedStorageState, StorageTxnState, TxnWriteBuffer, TxnWriteBufferMark,
};
use paro_common::chunk::Chunk;
use paro_common::effect::{
    ArtifactRef, DeletePatchEncoding, DeletePatchGroup, DeletePatchInline, DeletePatchRef,
    DeletePatchSegment, GraphDmlTableDelta as GraphDmlHookDelta, PostCommitHookDescriptor,
    PreparedDataOp, RowsetLocator, StorageCommitOp, TabletApplyOp, TabletMutation, VersionSpan,
};
use paro_common::error::{self as paro_error, Result};
use paro_transaction::{
    ActiveRwTxnHandle, ActiveTxnHandle, CommandId, CommitTs, DatabaseId, FrozenLockSet,
    LockAcquireError, LockMode, LockNamespace, LockRequest, LockResource, ParticipantDescriptor,
    ParticipantId, ParticipantKind, ParticipantStateRef, ReadTrackerHandle,
    ReadTrackerSavepointMark, ReadTs, ShardedLockManager, TableId, TxnId, TxnLockSet,
    TxnResourceKey, WriterId,
};
use std::collections::{BTreeMap, HashMap};
use std::path::Component;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Default)]
pub struct StorageSavepointMark {
    pub write_buffer_mark: TxnWriteBufferMark,
    pub pending_lock_sets_len: usize,
    pub command_id_mark: CommandId,
    pub read_dependency_mark: ReadTrackerSavepointMark,
    pub read_coarsening_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedStorageCommit {
    pub data_ops: Vec<PreparedDataOp>,
    pub storage_ops: Vec<StorageCommitOp>,
    pub post_commit_hooks: Vec<PostCommitHookDescriptor>,
}

enum ActiveRegistryBinding {
    ReadOnly(ActiveTxnHandle),
    ReadWrite(ActiveRwTxnHandle),
}

impl std::fmt::Debug for ActiveRegistryBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReadOnly(_) => f.write_str("ActiveRegistryBinding::ReadOnly"),
            Self::ReadWrite(_) => f.write_str("ActiveRegistryBinding::ReadWrite"),
        }
    }
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

    /// Storage-owned pending write buffer and overlay state.
    write_buffer: Arc<TxnWriteBuffer>,

    /// Opaque participant state shared with TransactionView.
    storage_state: Arc<StorageTxnState>,

    /// Lock sets backing staged delete/update operations.
    pending_lock_sets: Mutex<Vec<TxnLockSet>>,

    /// Database-scoped lock namespace for table/object locks.
    lock_namespace: LockNamespace,

    /// Database-scoped lock manager for DDL/DML admission.
    lock_manager: Arc<ShardedLockManager>,

    /// Slot handle in the global active transaction registry.
    active_registry_binding: Mutex<Option<ActiveRegistryBinding>>,
}

impl Transaction {
    /// Create a new transaction with the given ID and start time.
    ///
    /// # Arguments
    /// * `id` - Unique transaction identifier
    /// * `start_time` - Transaction start timestamp for MVCC
    ///
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
        Self::with_catalog_version_and_locks(
            id,
            start_time,
            catalog_version,
            Arc::new(ShardedLockManager::default()),
            LockNamespace::single_tenant(DatabaseId::new(0)),
        )
    }

    pub fn with_catalog_version_and_locks(
        id: u64,
        start_time: u64,
        catalog_version: u64,
        lock_manager: Arc<ShardedLockManager>,
        lock_namespace: LockNamespace,
    ) -> Self {
        let storage_state = Arc::new(StorageTxnState::new(lock_namespace.database_id));
        let write_buffer = storage_state.write_buffer();
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
            write_buffer,
            storage_state,
            pending_lock_sets: Mutex::new(Vec::new()),
            lock_namespace,
            lock_manager,
            active_registry_binding: Mutex::new(None),
        }
    }

    #[inline]
    pub fn txn_id(&self) -> TxnId {
        TxnId::new(self.id)
    }

    #[inline]
    pub fn writer_id(&self) -> WriterId {
        self.txn_id().as_writer_id()
    }

    #[inline]
    pub fn read_ts(&self) -> ReadTs {
        ReadTs::new(self.start_time)
    }

    #[inline]
    pub fn lock_namespace(&self) -> LockNamespace {
        self.lock_namespace
    }

    #[inline]
    pub fn lock_manager(&self) -> Arc<ShardedLockManager> {
        Arc::clone(&self.lock_manager)
    }

    #[inline]
    pub fn storage_participant_state(&self) -> ParticipantStateRef {
        self.storage_state.clone()
    }

    #[inline]
    pub fn write_buffer(&self) -> Arc<TxnWriteBuffer> {
        Arc::clone(&self.write_buffer)
    }

    #[inline]
    pub fn write_buffer_memory_usage_bytes(&self) -> u64 {
        self.write_buffer.memory_usage_bytes()
    }

    #[inline]
    pub fn write_buffer_mutation_count(&self) -> u64 {
        self.write_buffer.mutation_count()
    }

    #[inline]
    pub fn set_write_buffer_memory_budget_bytes(&self, bytes: u64) {
        self.write_buffer.set_memory_budget_bytes(bytes);
    }

    #[inline]
    pub fn publish_command_boundary(&self, command_id: CommandId) {
        self.write_buffer.publish_command_boundary(command_id);
    }

    #[inline]
    pub fn freeze_write_buffer(&self, command_id: CommandId) {
        self.write_buffer.freeze(command_id);
    }

    pub fn acquire_lock_requests(
        &self,
        requests: impl IntoIterator<Item = LockRequest>,
    ) -> Result<()> {
        let lock_set = self
            .lock_manager
            .lock_many(self.txn_id(), requests)
            .map_err(|err| self.lock_acquire_error(err))?;
        self.hold_lock_set(lock_set)
    }

    pub fn hold_lock_set(&self, lock_set: TxnLockSet) -> Result<()> {
        let mut lock_sets = self
            .pending_lock_sets
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock pending lock sets: {e}")))?;
        lock_sets.push(lock_set);
        Ok(())
    }

    pub fn held_lock_count(&self) -> usize {
        self.pending_lock_sets
            .lock()
            .map(|lock_sets| lock_sets.iter().map(TxnLockSet::len).sum())
            .unwrap_or(0)
    }

    pub fn frozen_lock_set(&self) -> FrozenLockSet {
        let locks = self
            .pending_lock_sets
            .lock()
            .map(|lock_sets| {
                lock_sets
                    .iter()
                    .flat_map(TxnLockSet::lock_requests)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        FrozenLockSet::from_locks(locks)
    }

    fn table_resource(&self, table_id: u64) -> LockResource {
        LockResource::Table {
            namespace: self.lock_namespace,
            table_id: TableId::new(table_id),
        }
    }

    fn has_table_write_intent(&self, table_id: u64) -> Result<bool> {
        let resource = self.table_resource(table_id);
        let lock_sets = self
            .pending_lock_sets
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock pending lock sets: {e}")))?;
        Ok(lock_sets.iter().any(|lock_set| {
            lock_set.mode_for(&resource).is_some_and(|mode| {
                matches!(
                    mode,
                    LockMode::IX | LockMode::X | LockMode::SchemaModification
                )
            })
        }))
    }

    fn acquire_table_write_intent_lock(&self, table_id: u64) -> Result<Option<TxnLockSet>> {
        if self.has_table_write_intent(table_id)? {
            return Ok(None);
        }
        let request = LockRequest::new(self.table_resource(table_id), LockMode::IX);
        self.lock_manager
            .lock_many(self.txn_id(), [request])
            .map(Some)
            .map_err(|err| self.lock_acquire_error(err))
    }

    fn hold_lock_sets(
        &self,
        lock_sets: impl IntoIterator<Item = Option<TxnLockSet>>,
    ) -> Result<()> {
        let mut pending_lock_sets = self
            .pending_lock_sets
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock pending lock sets: {e}")))?;
        pending_lock_sets.extend(lock_sets.into_iter().flatten());
        Ok(())
    }

    pub fn participant_descriptors(&self) -> Vec<ParticipantDescriptor> {
        if !self.has_pending_storage_work() {
            return Vec::new();
        }
        vec![ParticipantDescriptor::new(
            ParticipantId::new(1),
            ParticipantKind::Storage,
            TxnResourceKey::database(ParticipantKind::Storage, self.lock_namespace.database_id),
        )]
    }

    pub fn has_pending_storage_work(&self) -> bool {
        self.write_buffer.has_pending_storage_work() || self.held_lock_count() > 0
    }

    fn lock_acquire_error(&self, err: LockAcquireError) -> paro_error::ParoError {
        match err {
            LockAcquireError::WouldWait { blockers } => paro_error::serialization_failure(format!(
                "transaction {} blocked by conflicting locks held by {:?}",
                self.txn_id(),
                blockers
            )),
            LockAcquireError::WouldWound { victims } => paro_error::serialization_failure(format!(
                "transaction {} would wound conflicting lock owners {:?}",
                self.txn_id(),
                victims
            )),
            LockAcquireError::WouldWoundAndWait { victims, blockers } => {
                paro_error::serialization_failure(format!(
                    "transaction {} would wound conflicting lock owners {:?} and wait for {:?}",
                    self.txn_id(),
                    victims,
                    blockers
                ))
            }
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
        let _ = self.promote_to_read_write();
    }

    pub fn promote_to_read_write(&self) -> Result<()> {
        let Ok(mut binding) = self.active_registry_binding.lock() else {
            return Err(paro_error::internal("active registry binding poisoned"));
        };
        let mut handle = match binding.take() {
            Some(ActiveRegistryBinding::ReadOnly(handle)) => handle,
            None => {
                self.is_read_only.store(false, Ordering::Release);
                return Ok(());
            }
            existing => {
                if matches!(existing, Some(ActiveRegistryBinding::ReadWrite(_))) {
                    self.is_read_only.store(false, Ordering::Release);
                }
                *binding = existing;
                return Ok(());
            }
        };

        match handle.promote() {
            Ok(handle) => {
                self.is_read_only.store(false, Ordering::Release);
                *binding = Some(ActiveRegistryBinding::ReadWrite(handle));
                Ok(())
            }
            Err(error) => {
                *binding = Some(ActiveRegistryBinding::ReadOnly(handle));
                Err(paro_error::internal(format!(
                    "failed to promote active transaction: {error}"
                )))
            }
        }
    }

    pub fn bind_active_registry_handle(&self, handle: ActiveTxnHandle) -> Result<()> {
        let mut binding = self.active_registry_binding.lock().map_err(|e| {
            paro_error::internal(format!("failed to lock active registry binding: {e}"))
        })?;
        *binding = Some(ActiveRegistryBinding::ReadOnly(handle));
        Ok(())
    }

    pub fn release_active_registry_handle(&self) {
        let Ok(mut binding) = self.active_registry_binding.lock() else {
            return;
        };
        let Some(binding) = binding.take() else {
            return;
        };
        match binding {
            ActiveRegistryBinding::ReadOnly(handle) => {
                let _ = handle.release();
            }
            ActiveRegistryBinding::ReadWrite(handle) => {
                let _ = handle.release();
            }
        }
    }

    fn bump_mutation_generation(&self) {
        self.mutation_generation.fetch_add(1, Ordering::AcqRel);
    }

    pub fn mutation_generation(&self) -> u64 {
        self.mutation_generation.load(Ordering::Acquire)
    }

    pub fn mark_savepoint(&self) -> Result<StorageSavepointMark> {
        self.mark_savepoint_with_read_tracker(CommandId::new(0), &ReadTrackerHandle::noop())
    }

    pub fn mark_savepoint_with_read_tracker(
        &self,
        command_id_mark: CommandId,
        read_tracker: &ReadTrackerHandle,
    ) -> Result<StorageSavepointMark> {
        let write_buffer_mark = self.write_buffer.mark_savepoint()?;
        let pending_lock_sets_len = self
            .pending_lock_sets
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock pending lock sets: {e}")))?
            .len();
        let read_dependency_mark = read_tracker.mark_savepoint();
        let read_coarsening_epoch = read_dependency_mark.coarsening_epoch();

        Ok(StorageSavepointMark {
            write_buffer_mark,
            pending_lock_sets_len,
            command_id_mark,
            read_dependency_mark,
            read_coarsening_epoch,
        })
    }

    pub fn rollback_to_savepoint(&self, mark: &StorageSavepointMark) -> Result<()> {
        self.write_buffer
            .rollback_to_savepoint(&mark.write_buffer_mark)?;

        let lock_tail = {
            let mut lock_sets = self.pending_lock_sets.lock().map_err(|e| {
                paro_error::internal(format!("failed to lock pending lock sets: {e}"))
            })?;
            if mark.pending_lock_sets_len >= lock_sets.len() {
                Vec::new()
            } else {
                lock_sets.split_off(mark.pending_lock_sets_len)
            }
        };

        drop(lock_tail);

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
    pub fn changes_made(&self) -> bool {
        // Check undo buffer
        let undo_changes = match self.undo_buffer.lock() {
            Ok(buffer) => buffer.changes_made(),
            Err(_) => true, // If lock is poisoned, assume changes were made (safe default)
        };

        undo_changes || self.write_buffer.has_pending_storage_work()
    }

    pub fn record_dml_table(&self, table_oid: u64) -> Result<()> {
        self.write_buffer.record_dml_table(table_oid)
    }

    pub fn has_dml_on_table(&self, table_oid: u64) -> bool {
        self.write_buffer.has_dml_on_table(table_oid)
    }

    pub fn has_dml_on_any_table<I>(&self, table_oids: I) -> bool
    where
        I: IntoIterator<Item = u64>,
    {
        self.write_buffer.has_dml_on_any_table(table_oids)
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
        command_id: CommandId,
        tablet: TabletRef,
        rowset: crate::rowset::RowsetSharedPtr,
        primary_update: Option<PrimaryIndexUpdate>,
    ) -> Result<()> {
        let table_lock_set = self.acquire_table_write_intent_lock(tablet.table_id())?;
        let lock_set = primary_update.as_ref().and_then(|update| {
            let keys = update
                .written
                .iter()
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            (!keys.is_empty()).then(|| tablet.acquire_primary_key_write_locks(self.txn_id(), &keys))
        });
        let lock_set = lock_set.transpose()?;
        tablet.ensure_rowset_rssids(&rowset);
        self.write_buffer
            .add_rowset(self.id, command_id, tablet, rowset, primary_update)?;
        self.hold_lock_sets([table_lock_set, lock_set])?;
        self.bump_mutation_generation();
        self.set_read_write();
        Ok(())
    }

    /// Stage primary-key deletes for commit (transactional visibility).
    pub fn add_pending_primary_delete(
        &self,
        command_id: CommandId,
        tablet: TabletRef,
        keys: Vec<Vec<u8>>,
        locations: Vec<PhysicalRowRef>,
    ) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let table_lock_set = self.acquire_table_write_intent_lock(tablet.table_id())?;
        let lock_set = tablet.acquire_primary_delete_locks(self.txn_id(), &keys)?;

        self.write_buffer
            .add_primary_delete(self.id, command_id, tablet, keys, locations)?;
        self.hold_lock_sets([table_lock_set, Some(lock_set)])?;
        self.bump_mutation_generation();
        self.set_read_write();
        Ok(())
    }

    /// Stage row-id deletes for commit (transactional visibility).
    pub fn add_pending_row_id_delete(
        &self,
        command_id: CommandId,
        tablet: TabletRef,
        locations: Vec<PhysicalRowRef>,
    ) -> Result<()> {
        if locations.is_empty() {
            return Ok(());
        }
        let table_lock_set = self.acquire_table_write_intent_lock(tablet.table_id())?;
        let lock_set = tablet.acquire_row_id_delete_locks(self.txn_id(), &locations)?;

        self.write_buffer
            .add_row_id_delete(self.id, command_id, tablet, locations)?;
        self.hold_lock_sets([table_lock_set, Some(lock_set)])?;
        self.bump_mutation_generation();
        self.set_read_write();
        Ok(())
    }

    /// Append a chunk using a transaction-scoped DeltaWriter (MemTable reuse).
    pub(crate) fn append_to_tablet(
        &self,
        command_id: CommandId,
        tablet: TabletRef,
        chunk: &Chunk,
        search_write_context: SearchWriteContext,
    ) -> Result<()> {
        if chunk.size() == 0 {
            return Ok(());
        }
        let table_lock_set = self.acquire_table_write_intent_lock(tablet.table_id())?;
        let primary_key_lock_set = if let Some(schema) = tablet.schema() {
            if schema.keys_type() == crate::tablet::KeysType::PrimaryKeys {
                let serializer =
                    crate::primary_key::PrimaryKeySerializer::from_schema_ref(&schema)?;
                let keys = serializer.encode_chunk(chunk)?;
                if keys.is_empty() {
                    None
                } else {
                    Some(tablet.acquire_primary_key_write_locks(self.txn_id(), &keys)?)
                }
            } else {
                None
            }
        } else {
            None
        };
        self.with_tablet_writer(
            command_id,
            tablet,
            chunk.allocator().clone(),
            search_write_context,
            chunk.get_allocation_size() as u64,
            |writer| writer.write_chunk(chunk),
        )?;
        self.hold_lock_sets([table_lock_set, primary_key_lock_set])?;
        Ok(())
    }

    /// Register declared ART columns for a tablet.
    ///
    /// These columns will be used to build runtime ART indexes on
    /// transaction-private rowsets before commit visibility.
    pub fn register_pending_art_columns(&self, tablet_id: u64, columns: Vec<u32>) -> Result<()> {
        self.write_buffer.register_art_columns(tablet_id, columns)
    }

    fn with_tablet_writer<F, R>(
        &self,
        command_id: CommandId,
        tablet: TabletRef,
        allocator: Arc<dyn paro_common::allocator::Allocator>,
        search_write_context: SearchWriteContext,
        estimated_new_bytes: u64,
        f: F,
    ) -> Result<R>
    where
        F: FnOnce(&mut crate::write::DeltaWriter) -> Result<R>,
    {
        let result = self.write_buffer.with_tablet_writer(
            self.id,
            command_id,
            self.read_ts(),
            tablet,
            allocator,
            search_write_context,
            estimated_new_bytes,
            f,
        )?;
        self.bump_mutation_generation();
        self.set_read_write();
        Ok(result)
    }

    pub fn record_graph_insert(&self, table_oid: u64, rows: usize) {
        self.write_buffer.record_graph_insert(table_oid, rows);
    }

    pub fn record_graph_delete(&self, table_oid: u64, rows: usize) {
        self.write_buffer.record_graph_delete(table_oid, rows);
    }

    pub fn record_graph_update(&self, table_oid: u64, rows: usize, updated_columns: &[u32]) {
        self.write_buffer
            .record_graph_update(table_oid, rows, updated_columns);
    }

    pub fn take_graph_dml_deltas(&self) -> Result<HashMap<u64, GraphTableDmlDelta>> {
        self.write_buffer.take_graph_dml_deltas()
    }

    fn path_components(path: &std::path::Path) -> Vec<String> {
        path.components()
            .filter_map(|component| match component {
                Component::Normal(value) => Some(value.to_string_lossy().to_string()),
                Component::RootDir => Some("/".to_string()),
                _ => None,
            })
            .collect()
    }

    fn split_pending_operations(
        &self,
        pending: Vec<PendingMutation>,
    ) -> Result<(
        Vec<PendingPrimaryDelete>,
        Vec<PendingRowIdDelete>,
        Vec<PendingRowset>,
    )> {
        let mut primary_deletes = Vec::new();
        let mut overlay_primary_deletes = Vec::new();
        let mut row_id_deletes = Vec::new();
        let mut rowsets = Vec::new();

        for op in pending {
            match op {
                PendingMutation::PrimaryDelete(pending) if pending.durable => {
                    primary_deletes.push(pending)
                }
                PendingMutation::PrimaryDelete(pending) => overlay_primary_deletes.push(pending),
                PendingMutation::RowIdDelete(pending) => row_id_deletes.push(pending),
                PendingMutation::Rowset(pending) => rowsets.push(pending),
            }
        }

        Self::apply_overlay_primary_deletes_to_rowsets(&overlay_primary_deletes, &mut rowsets)?;
        for pending in &overlay_primary_deletes {
            pending.abandon_spill();
        }
        rowsets = Self::discard_noop_pending_primary_rowsets(rowsets)?;

        Ok((primary_deletes, row_id_deletes, rowsets))
    }

    fn apply_overlay_primary_deletes_to_rowsets(
        primary_deletes: &[PendingPrimaryDelete],
        rowsets: &mut [PendingRowset],
    ) -> Result<()> {
        for pending_delete in primary_deletes {
            let locations = pending_delete.overlay_locations()?;
            if pending_delete.keys.len() != locations.len() {
                return Err(paro_error::internal(format!(
                    "overlay primary delete key/location count mismatch: {} keys vs {} locations",
                    pending_delete.keys.len(),
                    locations.len()
                )));
            }

            for location in locations {
                let Some(pending_rowset) = rowsets.iter_mut().find(|rowset| {
                    rowset.tablet.tablet_id() == pending_delete.tablet.tablet_id()
                        && rowset.rowset.rowset_id() == location.rowset_id
                }) else {
                    return Err(paro_error::internal(format!(
                        "overlay primary delete references missing pending rowset {}",
                        location.rowset_id
                    )));
                };
                let Some(primary_update) = pending_rowset.primary_update.as_mut() else {
                    return Err(paro_error::internal(format!(
                        "overlay primary delete references non-primary pending rowset {}",
                        location.rowset_id
                    )));
                };
                primary_update
                    .pending_delete_vectors
                    .entry(location.segment_key())
                    .or_insert_with(crate::primary_key::DeleteVector::new)
                    .mark_deleted(location.row_offset);
            }
        }
        Ok(())
    }

    fn discard_noop_pending_primary_rowsets(
        rowsets: Vec<PendingRowset>,
    ) -> Result<Vec<PendingRowset>> {
        let mut publish = Vec::with_capacity(rowsets.len());
        for pending in rowsets {
            if Self::pending_primary_rowset_is_noop(&pending)? {
                Self::discard_pending_rowset(pending);
            } else {
                publish.push(pending);
            }
        }
        Ok(publish)
    }

    fn pending_primary_rowset_is_noop(pending: &PendingRowset) -> Result<bool> {
        let Some(primary_update) = pending.primary_update.as_ref() else {
            return Ok(false);
        };
        if primary_update
            .written
            .iter()
            .any(|(_, previous)| previous.is_some())
        {
            return Ok(false);
        }
        let row_ids = pending.tablet.row_ids_for_rowset(&pending.rowset)?;
        if row_ids.len() != primary_update.written.len() {
            return Err(paro_error::internal(format!(
                "row id count {} does not match written primary keys {}",
                row_ids.len(),
                primary_update.written.len()
            )));
        }
        if row_ids.is_empty() {
            return Ok(true);
        }
        for row_id in row_ids {
            let location = pending.tablet.decode_row_id(row_id)?;
            let deleted = primary_update
                .pending_delete_vectors
                .get(&location.segment_key())
                .is_some_and(|delete_vector| delete_vector.is_deleted(location.row_offset));
            if !deleted {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn discard_pending_rowset(pending: PendingRowset) {
        if let Some(artifact) = &pending.spilled_artifact {
            artifact.abandon_and_remove();
        } else {
            let _ = std::fs::remove_dir_all(&pending.rowset_path);
        }
    }

    fn take_pending_lock_sets(&self) -> Vec<TxnLockSet> {
        match self.pending_lock_sets.lock() {
            Ok(mut lock_sets) => std::mem::take(&mut *lock_sets),
            Err(_) => Vec::new(),
        }
    }

    pub fn release_transaction_locks(&self) {
        drop(self.take_pending_lock_sets());
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
        let advance_delete_publish_version = rowsets.is_empty();
        self.validate_primary_key_commit(&rowsets, commit_id)?;

        // Apply deletes first, then inserts/rowset commits.
        (|| {
            for pending in primary_deletes {
                if advance_delete_publish_version {
                    pending.tablet.apply_primary_delete_at_version(
                        pending.keys.clone(),
                        CommitTs::new(commit_id),
                    )?;
                } else {
                    pending
                        .tablet
                        .apply_primary_delete_at_version_without_publish_advance(
                            pending.keys.clone(),
                            CommitTs::new(commit_id),
                        )?;
                }
            }

            for pending in row_id_deletes {
                let locations = pending.locations_for_commit()?;
                if locations.is_empty() {
                    continue;
                }
                if advance_delete_publish_version {
                    pending
                        .tablet
                        .apply_row_id_delete_refs(&locations, CommitTs::new(commit_id))?;
                } else {
                    pending
                        .tablet
                        .apply_row_id_delete_refs_at_version_without_publish_advance(
                            &locations,
                            commit_version,
                        )?;
                }
                pending.mark_spill_committed();
            }

            for pending in rowsets {
                if let Some(update) = pending.primary_update {
                    pending.tablet.publish_rowset_with_index(
                        commit_version,
                        pending.rowset.clone(),
                        update,
                    )?;
                } else {
                    pending
                        .tablet
                        .rowset_commit(commit_version, pending.rowset.clone())?;
                }
                if let Some(artifact) = &pending.spilled_artifact {
                    artifact.mark_committed_descriptor_written();
                }
            }

            for pending in primary_deletes {
                pending.mark_spill_committed();
            }

            Ok(())
        })()
    }

    fn validate_primary_key_commit(&self, rowsets: &[PendingRowset], commit_id: u64) -> Result<()> {
        let read_ts = self.read_ts().into_raw();
        for pending in rowsets {
            let Some(primary_update) = pending.primary_update.as_ref() else {
                continue;
            };
            let keys = primary_update
                .written
                .iter()
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            pending
                .tablet
                .validate_primary_key_no_write_in_range(&keys, read_ts, commit_id)?;
        }
        Ok(())
    }

    fn inline_delete_patch(locations: &[PhysicalRowRef]) -> DeletePatchInline {
        let mut by_segment: BTreeMap<(u64, u32), Vec<u32>> = BTreeMap::new();
        for location in locations {
            by_segment
                .entry(location.segment_key())
                .or_default()
                .push(location.row_offset);
        }

        let mut by_rowset: BTreeMap<u64, Vec<DeletePatchSegment>> = BTreeMap::new();
        for ((rowset_id, segment_id), mut offsets) in by_segment {
            offsets.sort_unstable();
            offsets.dedup();
            let mut previous = 0u32;
            let mut first = true;
            let mut deltas = Vec::with_capacity(offsets.len());
            for offset in offsets {
                let delta = if first {
                    first = false;
                    offset
                } else {
                    offset.saturating_sub(previous)
                };
                previous = offset;
                deltas.push(delta);
            }
            by_rowset
                .entry(rowset_id)
                .or_default()
                .push(DeletePatchSegment {
                    segment_id,
                    row_offsets_delta: deltas,
                });
        }

        let row_count = by_rowset
            .values()
            .flat_map(|segments| segments.iter())
            .map(|segment| segment.row_offsets_delta.len())
            .sum::<usize>()
            .min(u32::MAX as usize) as u32;
        DeletePatchInline {
            encoding: DeletePatchEncoding::GroupedRowOffsetDeltaV1,
            row_count,
            groups: by_rowset
                .into_iter()
                .map(|(rowset_id, segments)| DeletePatchGroup {
                    rowset_id,
                    segments,
                })
                .collect(),
        }
    }

    fn push_storage_mutation(
        storage_mutations: &mut BTreeMap<u64, Vec<TabletMutation>>,
        tablet_id: u64,
        mutation: TabletMutation,
    ) {
        storage_mutations
            .entry(tablet_id)
            .or_default()
            .push(mutation);
    }

    pub fn prepare_commit(&self) -> Result<PreparedStorageCommit> {
        self.write_buffer.materialize_writers()?;

        let pending = self.write_buffer.take_mutations()?;
        let (primary_deletes, row_id_deletes, rowsets) = self.split_pending_operations(pending)?;

        let mut data_ops = Vec::new();
        let mut storage_mutations: BTreeMap<u64, Vec<TabletMutation>> = BTreeMap::new();
        for pending in &rowsets {
            let tablet_id = pending.tablet.tablet_id();
            let rowset_id = pending.rowset.rowset_id();
            let locator = RowsetLocator {
                tablet_id,
                rowset_id,
                path_components: Self::path_components(&pending.rowset_path),
            };
            data_ops.push(PreparedDataOp::RowsetCommit {
                locator,
                start_version: pending.rowset.start_version(),
                end_version: pending.rowset.end_version(),
            });
        }
        for pending in &primary_deletes {
            let tablet_id = pending.tablet.tablet_id();
            data_ops.push(PreparedDataOp::PrimaryDelete {
                tablet_id,
                keys: pending.keys.clone(),
            });
            Self::push_storage_mutation(
                &mut storage_mutations,
                tablet_id,
                TabletMutation::ApplyPrimaryDelete {
                    keys: pending.keys.clone(),
                },
            );
        }
        for pending in &row_id_deletes {
            let locations = pending.locations_for_commit()?;
            let tablet_id = pending.tablet.tablet_id();
            data_ops.push(PreparedDataOp::RowIdDelete {
                tablet_id,
                locations: locations.iter().copied().map(Into::into).collect(),
            });
            if !locations.is_empty() {
                Self::push_storage_mutation(
                    &mut storage_mutations,
                    tablet_id,
                    TabletMutation::ApplyDeletePatch {
                        patch: DeletePatchRef::Inline(Self::inline_delete_patch(&locations)),
                        deleted_row_count: u32::try_from(locations.len()).map_err(|_| {
                            paro_error::invalid_input("row-id delete patch exceeds u32 row count")
                        })?,
                    },
                );
            }
        }
        for pending in &rowsets {
            let tablet_id = pending.tablet.tablet_id();
            Self::push_storage_mutation(
                &mut storage_mutations,
                tablet_id,
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
        let storage_ops = storage_mutations
            .into_iter()
            .map(|(tablet_id, mutations)| {
                StorageCommitOp::Tablet(TabletApplyOp {
                    tablet_id,
                    mutations,
                })
            })
            .collect();

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

        self.write_buffer.set_prepared(PreparedStorageState {
            rowsets,
            primary_deletes,
            row_id_deletes,
        })?;

        Ok(PreparedStorageCommit {
            data_ops,
            storage_ops,
            post_commit_hooks,
        })
    }

    fn apply_pending_writes(&self, commit_id: u64) -> Result<()> {
        let pending = self.write_buffer.take_mutations()?;
        let (primary_deletes, row_id_deletes, rowsets) = self.split_pending_operations(pending)?;
        self.apply_materialized_writes(commit_id, &primary_deletes, &row_id_deletes, rowsets)
    }

    fn commit_prepared_storage(&self, commit_id: u64) -> Result<()> {
        let prepared = self.write_buffer.take_prepared()?;
        let Some(prepared) = prepared else {
            self.write_buffer.materialize_writers()?;
            return self.apply_pending_writes(commit_id);
        };

        self.apply_materialized_writes(
            commit_id,
            &prepared.primary_deletes,
            &prepared.row_id_deletes,
            prepared.rowsets,
        )
    }

    fn rollback_pending_writes(&self) {
        let lock_sets = self.take_pending_lock_sets();
        self.write_buffer.rollback_mutations();
        drop(lock_sets);
        self.write_buffer.rollback_prepared();
    }

    pub fn abort_prepared_storage(&self) {
        self.write_buffer.rollback_prepared();
    }

    pub fn rollback_prepared_storage_only(&self) {
        self.abort_prepared_storage();
    }

    pub fn apply_prepared_storage_for_commit(&self, commit_id: u64) -> Result<()> {
        self.commit_prepared_storage(commit_id)?;
        self.write_buffer.clear_after_commit();
        Ok(())
    }

    pub fn finalize_applied_commit(&self, commit_id: u64) -> Result<()> {
        {
            let mut cid = self
                .commit_id
                .lock()
                .map_err(|e| paro_error::internal(format!("failed to acquire lock: {}", e)))?;
            *cid = commit_id;
        }

        if self.undo_changes_made() {
            let buffer = self
                .undo_buffer
                .lock()
                .map_err(|e| paro_error::internal(format!("failed to acquire lock: {}", e)))?;
            buffer.commit(self, commit_id);
        }

        self.set_awaiting_cleanup(true);
        Ok(())
    }

    /// Rollback the transaction, undoing all changes.
    ///
    ///
    /// # Returns
    /// * `Ok(())` - Transaction rolled back successfully
    /// * `Err(paro_error::transaction_aborted)` - Failed to acquire lock (poisoned mutex)
    pub fn rollback(&self) -> Result<()> {
        self.write_buffer.rollback_writers();
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
        Ok(())
    }

    /// Get the commit ID (0 if not yet committed).
    pub fn get_commit_id(&self) -> u64 {
        match self.commit_id.lock() {
            Ok(cid) => *cid,
            Err(_) => 0,
        }
    }

    #[inline]
    pub fn commit_ts(&self) -> CommitTs {
        CommitTs::new(self.get_commit_id())
    }

    /// Get the visible version for MVCC reads.
    ///
    /// `start_time` tracks the next commit id that was not yet visible when the
    /// transaction started, so readers must use the previous committed version.
    #[inline]
    pub fn visible_version(&self) -> u64 {
        self.start_time.saturating_sub(1)
    }

    #[inline]
    pub fn visible_commit_ts(&self) -> CommitTs {
        self.read_ts().visible_before_start()
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
    use crate::test_utils::*;
    use crate::transaction::overlay_reader::TxnOverlayReader;
    use crate::transaction::spill::TxnSpillAdmission;
    use paro_common::types::LogicalType;
    use paro_transaction::{IsolationLevel, ParticipantStateSet, ReadSnapshot, TransactionView};
    use std::collections::BTreeSet;
    use std::sync::Barrier;
    use std::thread;

    static SPILL_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct SpillTestGuard {
        _guard: std::sync::MutexGuard<'static, ()>,
    }

    impl Drop for SpillTestGuard {
        fn drop(&mut self) {
            TxnSpillAdmission::global().reset_for_tests();
        }
    }

    fn reset_spill_for_test() -> SpillTestGuard {
        let guard = SPILL_TEST_LOCK.lock().expect("lock spill test guard");
        TxnSpillAdmission::global().reset_for_tests();
        SpillTestGuard { _guard: guard }
    }

    fn create_table(types: &[LogicalType]) -> TableHandle {
        TableFactory::default().create_table(types).unwrap()
    }

    fn create_table_with_keys(types: &[LogicalType], keys_type: KeysType) -> TableHandle {
        TableFactory::default()
            .create_table_with_keys(types, keys_type)
            .unwrap()
    }

    fn commit_transaction(txn: &Transaction, commit_id: u64) -> Result<()> {
        let apply_result = txn.apply_prepared_storage_for_commit(commit_id);
        txn.release_transaction_locks();
        apply_result?;
        txn.finalize_applied_commit(commit_id)
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
        let key_chunk = test_chunk_from_vectors(vec![test_i32_vector(&[key])]);
        serializer
            .encode_row(&key_chunk, 0)
            .expect("encode primary key")
    }

    fn row_id_location_by_value(table: &TableHandle, target: i32) -> PhysicalRowRef {
        row_id_locations_by_values(table, &[target])
            .pop()
            .expect("target row should exist")
    }

    fn row_id_locations_by_values(table: &TableHandle, targets: &[i32]) -> Vec<PhysicalRowRef> {
        let targets = targets.iter().copied().collect::<BTreeSet<_>>();
        let mut reader = table
            .create_reader(
                TabletReaderParams::with_version(table.max_version()).with_emit_row_id(true),
            )
            .expect("create reader");
        reader.prepare().expect("prepare reader");

        let mut locations = Vec::new();
        while let Some(chunk) = reader.get_next_chunk().expect("read chunk") {
            let values = chunk.column(0).expect("value column");
            let row_ids = chunk
                .column(chunk.column_count() - 1)
                .expect("row id column");
            for row in 0..chunk.size() {
                if values
                    .get_i32(row)
                    .is_some_and(|value| targets.contains(&value))
                {
                    let row_id =
                        RowID::from_raw(row_ids.get_i64(row).expect("row_id as i64") as u64);
                    let location = table.tablet().decode_row_id(row_id).expect("decode row id");
                    locations.push(location);
                }
            }
        }

        assert_eq!(locations.len(), targets.len(), "not all target rows found");
        locations
    }

    fn view_for_command(txn: &Transaction, command_id: u32) -> TransactionView {
        TransactionView::new(
            txn.writer_id(),
            txn.read_ts(),
            ReadSnapshot::without_lease(ReadTs::new(txn.visible_commit_ts().into_raw())),
            IsolationLevel::Snapshot,
            CommandId::new(command_id),
            paro_transaction::ReadTrackerHandle::noop(),
            ParticipantStateSet::from_vec(vec![txn.storage_participant_state()]),
        )
    }

    fn collect_rows_i32_with_view(table: &TableHandle, view: &TransactionView) -> Vec<i32> {
        let snapshot = table
            .storage_snapshot(view.read_ts(), view.read_snapshot().lease())
            .expect("capture storage snapshot");
        let overlay =
            TxnOverlayReader::for_tablet(&table.tablet(), view).expect("build txn overlay");
        let mut rowsets = snapshot.rowsets().expect("materialize snapshot rowsets");
        if let Some(overlay) = &overlay {
            rowsets.extend(overlay.all_rowsets());
        }
        let mut params = TabletReaderParams::with_version(snapshot.visible_version());
        if let Some(delete_vectors) = overlay.as_ref().and_then(TxnOverlayReader::delete_vectors) {
            params = params.with_overlay_delete_vectors(delete_vectors);
        }
        let mut reader = table.create_reader(params).expect("create reader");
        reader
            .prepare_with_pinned_rowsets(rowsets)
            .expect("prepare reader");

        let mut rows = Vec::new();
        while let Some(chunk) = reader.get_next_chunk().expect("read chunk") {
            let col = chunk.column(0).expect("column 0");
            for row in 0..chunk.size() {
                rows.push(col.get_i32(row).expect("column 0 as i32"));
            }
        }
        rows.sort_unstable();
        rows
    }

    fn collect_rows_i32_pair_with_view(
        table: &TableHandle,
        view: &TransactionView,
    ) -> Vec<(i32, i32)> {
        let snapshot = table
            .storage_snapshot(view.read_ts(), view.read_snapshot().lease())
            .expect("capture storage snapshot");
        let overlay =
            TxnOverlayReader::for_tablet(&table.tablet(), view).expect("build txn overlay");
        let mut rowsets = snapshot.rowsets().expect("materialize snapshot rowsets");
        if let Some(overlay) = &overlay {
            rowsets.extend(overlay.all_rowsets());
        }
        let mut params = TabletReaderParams::with_version(snapshot.visible_version());
        if let Some(delete_vectors) = overlay.as_ref().and_then(TxnOverlayReader::delete_vectors) {
            params = params.with_overlay_delete_vectors(delete_vectors);
        }
        let mut reader = table.create_reader(params).expect("create reader");
        reader
            .prepare_with_pinned_rowsets(rowsets)
            .expect("prepare reader");

        let mut rows = Vec::new();
        while let Some(chunk) = reader.get_next_chunk().expect("read chunk") {
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

    fn txn_start_after_table(table: &TableHandle) -> u64 {
        table.max_version().max(0) as u64 + 1
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

        commit_transaction(&txn, 500).expect("commit should succeed");

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
            CommandId::new(0),
            tablet.clone(),
            &test_chunk_from_vectors(vec![test_i32_vector(&[1, 2]), test_i32_vector(&[10, 20])]),
            SearchWriteContext::default(),
        )
        .expect("append first batch");
        let mark = txn.mark_savepoint().expect("mark savepoint");
        txn.append_to_tablet(
            CommandId::new(1),
            tablet,
            &test_chunk_from_vectors(vec![test_i32_vector(&[3, 4]), test_i32_vector(&[30, 40])]),
            SearchWriteContext::default(),
        )
        .expect("append second batch");

        txn.rollback_to_savepoint(&mark)
            .expect("rollback to savepoint");
        commit_transaction(&txn, 9202).expect("commit txn");

        assert_eq!(collect_rows_i32_pair(&table), vec![(1, 10), (2, 20)]);
    }

    #[test]
    fn test_storage_participant_state_is_resolved_from_transaction_view() {
        let txn = Transaction::new(9001, 9001);
        let states = ParticipantStateSet::from_vec(vec![txn.storage_participant_state()]);
        let view = TransactionView::new(
            txn.writer_id(),
            txn.read_ts(),
            ReadSnapshot::without_lease(ReadTs::new(txn.visible_commit_ts().into_raw())),
            IsolationLevel::Snapshot,
            CommandId::new(0),
            paro_transaction::ReadTrackerHandle::noop(),
            states,
        );

        let write_buffer = StorageTxnState::write_buffer_from_view(&view)
            .expect("storage write buffer should be available from participant states");
        assert_eq!(write_buffer.memory_usage_bytes(), 0);
        assert_eq!(write_buffer.mutation_count(), 0);
    }

    #[test]
    fn test_txn_overlay_reader_exposes_prior_command_rowsets_only() {
        let table = create_table(&[LogicalType::Integer]);
        let txn = Arc::new(Transaction::new(9601, txn_start_after_table(&table)));
        let view0 = view_for_command(&txn, 0);

        table
            .append_with_transaction(
                &view0,
                &test_chunk_from_vectors(vec![test_i32_vector(&[1, 2, 3])]),
                txn.clone(),
            )
            .expect("stage append");

        assert_eq!(
            collect_rows_i32_with_view(&table, &view0),
            Vec::<i32>::new()
        );

        txn.publish_command_boundary(CommandId::new(1));
        let view1 = view_for_command(&txn, 1);
        assert_eq!(collect_rows_i32_with_view(&table, &view1), vec![1, 2, 3]);
    }

    #[test]
    fn test_txn_overlay_materialize_keeps_already_admitted_writer() {
        let table = create_table(&[LogicalType::Integer]);
        let txn = Arc::new(Transaction::new(9604, txn_start_after_table(&table)));
        let view0 = view_for_command(&txn, 0);

        table
            .append_with_transaction(
                &view0,
                &test_chunk_from_vectors(vec![test_i32_vector(&[7, 8])]),
                txn.clone(),
            )
            .expect("stage append before budget shrink");

        txn.set_write_buffer_memory_budget_bytes(1);
        txn.publish_command_boundary(CommandId::new(1));
        let view1 = view_for_command(&txn, 1);

        assert_eq!(collect_rows_i32_with_view(&table, &view1), vec![7, 8]);
        assert!(txn.write_buffer_memory_usage_bytes() > txn.write_buffer().memory_budget_bytes());
    }

    #[test]
    fn test_parallel_overlay_reader_uses_immutable_published_epoch() {
        let table = Arc::new(create_table(&[LogicalType::Integer]));
        let txn = Arc::new(Transaction::new(96041, txn_start_after_table(&table)));
        let view0 = view_for_command(&txn, 0);

        table
            .append_with_transaction(
                &view0,
                &test_chunk_from_vectors(vec![test_i32_vector(&[21, 22, 23])]),
                txn.clone(),
            )
            .expect("stage append");
        txn.publish_command_boundary(CommandId::new(1));
        let view1 = Arc::new(view_for_command(&txn, 1));

        let barrier = Arc::new(Barrier::new(8));
        let mut workers = Vec::new();
        for _ in 0..8 {
            let table = Arc::clone(&table);
            let view = Arc::clone(&view1);
            let barrier = Arc::clone(&barrier);
            workers.push(thread::spawn(move || {
                barrier.wait();
                collect_rows_i32_with_view(&table, &view)
            }));
        }

        for worker in workers {
            assert_eq!(worker.join().expect("worker"), vec![21, 22, 23]);
        }
    }

    #[test]
    fn test_txn_overlay_reader_reads_spilled_rowset() {
        let _spill_guard = reset_spill_for_test();
        let table = create_table(&[LogicalType::Integer]);
        let txn = Arc::new(Transaction::new(9605, txn_start_after_table(&table)));
        let view0 = view_for_command(&txn, 0);

        table
            .append_with_transaction(
                &view0,
                &test_chunk_from_vectors(vec![test_i32_vector(&[11, 12, 13, 14])]),
                txn.clone(),
            )
            .expect("stage append");

        txn.set_write_buffer_memory_budget_bytes(1);
        txn.publish_command_boundary(CommandId::new(1));
        let view1 = view_for_command(&txn, 1);
        let overlay = TxnOverlayReader::for_tablet(&table.tablet(), &view1)
            .expect("build spilled overlay")
            .expect("overlay should exist");

        assert_eq!(overlay.rowsets().len(), 0);
        assert_eq!(overlay.spilled_artifacts().len(), 1);
        assert_eq!(
            collect_rows_i32_with_view(&table, &view1),
            vec![11, 12, 13, 14]
        );
    }

    #[test]
    fn test_txn_overlay_reader_applies_spilled_row_id_delete() {
        let _spill_guard = reset_spill_for_test();
        let table = create_table(&[LogicalType::Integer]);
        let values = (0..64).collect::<Vec<_>>();
        table
            .append(&test_chunk_from_vectors(vec![test_i32_vector(&values)]))
            .expect("append rows");
        let txn = Transaction::new(9606, table.max_version() as u64 + 1);
        txn.set_write_buffer_memory_budget_bytes(800);
        let deleted = (0..48).collect::<Vec<_>>();
        let locations = row_id_locations_by_values(&table, &deleted);

        txn.add_pending_row_id_delete(CommandId::new(0), table.tablet(), locations)
            .expect("stage spilled row-id delete");

        txn.publish_command_boundary(CommandId::new(1));
        let view1 = view_for_command(&txn, 1);
        assert_eq!(
            collect_rows_i32_with_view(&table, &view1),
            (48..64).collect::<Vec<_>>()
        );

        commit_transaction(&txn, 9607).expect("commit spilled row-id delete");
        assert_eq!(collect_rows_i32(&table), (48..64).collect::<Vec<_>>());
    }

    #[test]
    fn test_txn_spill_device_pressure_rejects_before_staging_delete() {
        let _spill_guard = reset_spill_for_test();
        TxnSpillAdmission::global().set_device_pressure_for_tests(true);
        let table = create_table(&[LogicalType::Integer]);
        let values = (0..64).collect::<Vec<_>>();
        table
            .append(&test_chunk_from_vectors(vec![test_i32_vector(&values)]))
            .expect("append rows");
        let txn = Transaction::new(9608, table.max_version() as u64 + 1);
        txn.set_write_buffer_memory_budget_bytes(800);
        let locations = row_id_locations_by_values(&table, &(0..48).collect::<Vec<_>>());

        let err = txn
            .add_pending_row_id_delete(CommandId::new(0), table.tablet(), locations)
            .expect_err("device pressure should reject spill");

        assert!(err.to_string().contains("device pressure"));
        assert_eq!(txn.write_buffer_mutation_count(), 0);
        assert!(
            !table.tablet().data_dir().join("txn_staging").exists(),
            "device-pressure rejection must happen before staging files are created"
        );
        TxnSpillAdmission::global().reset_for_tests();
    }

    #[test]
    fn test_txn_overlay_reader_applies_row_id_delete_by_command() {
        let table = create_table(&[LogicalType::Integer]);
        table
            .append(&test_chunk_from_vectors(vec![test_i32_vector(&[1, 2, 3])]))
            .expect("append rows");
        let txn = Transaction::new(9602, table.max_version() as u64 + 1);
        let location = row_id_location_by_value(&table, 2);

        txn.add_pending_row_id_delete(CommandId::new(0), table.tablet(), vec![location])
            .expect("stage row-id delete");

        let view0 = view_for_command(&txn, 0);
        assert_eq!(collect_rows_i32_with_view(&table, &view0), vec![1, 2, 3]);

        txn.publish_command_boundary(CommandId::new(1));
        let view1 = view_for_command(&txn, 1);
        assert_eq!(collect_rows_i32_with_view(&table, &view1), vec![1, 3]);
    }

    #[test]
    fn test_txn_overlay_reader_applies_primary_delete_by_command() {
        let table = create_table_with_keys(
            &[LogicalType::Integer, LogicalType::Integer],
            KeysType::PrimaryKeys,
        );
        table
            .append(&test_chunk_from_vectors(vec![
                test_i32_vector(&[1, 2]),
                test_i32_vector(&[10, 20]),
            ]))
            .expect("append rows");
        let txn = Transaction::new(9603, table.max_version() as u64 + 1);

        txn.add_pending_primary_delete(
            CommandId::new(0),
            table.tablet(),
            vec![primary_key_bytes(&table, 1)],
            vec![row_id_location_by_value(&table, 1)],
        )
        .expect("stage primary delete");

        let view0 = view_for_command(&txn, 0);
        assert_eq!(
            collect_rows_i32_pair_with_view(&table, &view0),
            vec![(1, 10), (2, 20)]
        );

        txn.publish_command_boundary(CommandId::new(1));
        let view1 = view_for_command(&txn, 1);
        assert_eq!(
            collect_rows_i32_pair_with_view(&table, &view1),
            vec![(2, 20)]
        );
    }

    #[test]
    fn test_write_buffer_labels_pending_delete_with_command_id() {
        let table = create_table_with_keys(
            &[LogicalType::Integer, LogicalType::Integer],
            KeysType::PrimaryKeys,
        );
        table
            .append(&test_chunk_from_vectors(vec![
                test_i32_vector(&[1]),
                test_i32_vector(&[10]),
            ]))
            .expect("append row");
        let txn = Transaction::new(9002, 9002);

        txn.add_pending_primary_delete(
            CommandId::new(7),
            table.tablet(),
            vec![primary_key_bytes(&table, 1)],
            vec![row_id_location_by_value(&table, 1)],
        )
        .expect("stage primary delete");

        let mutations = txn.write_buffer().take_mutations().expect("take mutations");
        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].command_id(), CommandId::new(7));
    }

    #[test]
    fn test_write_buffer_budget_fails_fast_before_staging_delete() {
        let _spill_guard = reset_spill_for_test();
        TxnSpillAdmission::global().set_device_pressure_for_tests(true);
        let table = create_table_with_keys(
            &[LogicalType::Integer, LogicalType::Integer],
            KeysType::PrimaryKeys,
        );
        table
            .append(&test_chunk_from_vectors(vec![
                test_i32_vector(&[1]),
                test_i32_vector(&[10]),
            ]))
            .expect("append row");
        let txn = Transaction::new(9003, 9003);
        txn.set_write_buffer_memory_budget_bytes(64);

        let err = txn
            .add_pending_primary_delete(
                CommandId::new(0),
                table.tablet(),
                vec![primary_key_bytes(&table, 1)],
                vec![row_id_location_by_value(&table, 1)],
            )
            .expect_err("delete should exceed tiny write-buffer budget");

        assert!(
            err.to_string()
                .contains("transaction write buffer budget exceeded"),
            "unexpected error: {err}"
        );
        assert_eq!(txn.write_buffer_mutation_count(), 0);
        assert_eq!(txn.write_buffer_memory_usage_bytes(), 0);
        assert!(
            !table.tablet().data_dir().join("txn_staging").exists(),
            "budget rejection must happen before staging files are created"
        );
    }

    #[test]
    fn test_prepare_commit_builds_durable_storage_ops() {
        let table = create_table_with_keys(
            &[LogicalType::Integer, LogicalType::Integer],
            KeysType::PrimaryKeys,
        );
        table
            .append(&test_chunk_from_vectors(vec![
                test_i32_vector(&[1, 2, 3]),
                test_i32_vector(&[10, 20, 30]),
            ]))
            .expect("append rows");
        let txn = Arc::new(Transaction::new(9004, table.max_version() as u64 + 1));
        let tablet = table.tablet();

        txn.add_pending_primary_delete(
            CommandId::new(0),
            tablet.clone(),
            vec![primary_key_bytes(&table, 1)],
            vec![row_id_location_by_value(&table, 1)],
        )
        .expect("stage primary delete");
        txn.add_pending_row_id_delete(
            CommandId::new(0),
            tablet.clone(),
            vec![row_id_location_by_value(&table, 2)],
        )
        .expect("stage row-id delete");
        let view = view_for_command(&txn, 0);
        table
            .append_with_transaction(
                &view,
                &test_chunk_from_vectors(vec![test_i32_vector(&[4]), test_i32_vector(&[40])]),
                Arc::clone(&txn),
            )
            .expect("stage rowset");

        let prepared = txn.prepare_commit().expect("prepare commit");
        assert_eq!(prepared.data_ops.len(), 3);
        assert_eq!(prepared.storage_ops.len(), 1);
        let StorageCommitOp::Tablet(tablet_op) = &prepared.storage_ops[0];
        assert_eq!(tablet_op.tablet_id, tablet.tablet_id());
        assert!(matches!(
            &tablet_op.mutations[0],
            TabletMutation::ApplyPrimaryDelete { keys } if keys.len() == 1
        ));
        assert!(matches!(
            &tablet_op.mutations[1],
            TabletMutation::ApplyDeletePatch {
                patch: DeletePatchRef::Inline(patch),
                deleted_row_count: 1,
            } if patch.row_count == 1
        ));
        assert!(matches!(
            &tablet_op.mutations[2],
            TabletMutation::PublishRowset { rowset_id, .. } if *rowset_id > 0
        ));
    }

    #[test]
    fn test_transaction_publish_does_not_write_tablet_wal() {
        let table = create_table_with_keys(
            &[LogicalType::Integer, LogicalType::Integer],
            KeysType::PrimaryKeys,
        );
        let wal_path = table.tablet().data_dir().join("tablet.wal");
        let insert_txn = Arc::new(Transaction::new(9701, 0));
        let insert_view = view_for_command(&insert_txn, 0);

        table
            .append_with_transaction(
                &insert_view,
                &test_chunk_from_vectors(vec![
                    test_i32_vector(&[1, 2]),
                    test_i32_vector(&[10, 20]),
                ]),
                Arc::clone(&insert_txn),
            )
            .expect("stage transactional insert");
        commit_transaction(&insert_txn, 9702).expect("commit insert");

        assert!(
            std::fs::metadata(&wal_path)
                .map(|meta| meta.len() == 0)
                .unwrap_or(true),
            "transactional rowset publish must not write tablet WAL"
        );

        let delete_txn = Transaction::new(9703, 9702);
        delete_txn
            .add_pending_primary_delete(
                CommandId::new(0),
                table.tablet(),
                vec![primary_key_bytes(&table, 1)],
                vec![row_id_location_by_value(&table, 1)],
            )
            .expect("stage primary delete");
        delete_txn
            .add_pending_row_id_delete(
                CommandId::new(0),
                table.tablet(),
                vec![row_id_location_by_value(&table, 2)],
            )
            .expect("stage row-id delete");
        commit_transaction(&delete_txn, 9704).expect("commit deletes");

        assert!(
            std::fs::metadata(&wal_path)
                .map(|meta| meta.len() == 0)
                .unwrap_or(true),
            "transactional delete publish must not write tablet WAL"
        );
    }

    #[test]
    fn test_pending_primary_delete_commit_and_rollback_release_locks() {
        let table = create_table_with_keys(
            &[LogicalType::Integer, LogicalType::Integer],
            KeysType::PrimaryKeys,
        );
        table
            .append(&test_chunk_from_vectors(vec![
                test_i32_vector(&[1, 2]),
                test_i32_vector(&[10, 20]),
            ]))
            .expect("append rows");
        let tablet = table.tablet();
        let key_bytes = primary_key_bytes(&table, 1);
        let location = row_id_location_by_value(&table, 1);

        let txn1 = Transaction::new(9101, 9101);
        txn1.add_pending_primary_delete(
            CommandId::new(0),
            tablet.clone(),
            vec![key_bytes.clone()],
            vec![location],
        )
        .expect("stage primary delete");
        assert!(tablet.has_pending_delete_locks());

        // Pending ops are invisible before commit.
        assert_eq!(collect_rows_i32_pair(&table), vec![(1, 10), (2, 20)]);

        let txn2 = Transaction::new(9102, 9102);
        let conflict = txn2
            .add_pending_primary_delete(
                CommandId::new(0),
                tablet.clone(),
                vec![key_bytes.clone()],
                vec![location],
            )
            .expect_err("should conflict with txn1 lock");
        assert!(
            conflict.to_string().contains("write-write conflict"),
            "expected write-write conflict, got: {conflict}"
        );

        txn1.rollback().expect("rollback txn1");
        assert_eq!(collect_rows_i32_pair(&table), vec![(1, 10), (2, 20)]);

        // Rollback releases locks; txn2 can stage and commit the delete.
        txn2.add_pending_primary_delete(CommandId::new(0), tablet, vec![key_bytes], vec![location])
            .expect("stage primary delete after rollback");
        commit_transaction(&txn2, 9202).expect("commit txn2");
        assert_eq!(collect_rows_i32_pair(&table), vec![(2, 20)]);
    }

    #[test]
    fn test_pending_row_id_delete_commit_and_rollback_release_locks() {
        let table = create_table(&[LogicalType::Integer]);
        table
            .append(&test_chunk_from_vectors(vec![test_i32_vector(&[1, 2, 3])]))
            .expect("append rows");
        let tablet = table.tablet();
        let location = row_id_location_by_value(&table, 2);

        let txn1 = Transaction::new(9301, 9301);
        txn1.add_pending_row_id_delete(CommandId::new(0), tablet.clone(), vec![location])
            .expect("stage row-id delete");
        assert!(tablet.has_pending_delete_locks());

        // Pending ops are invisible before commit.
        assert_eq!(collect_rows_i32(&table), vec![1, 2, 3]);

        let txn2 = Transaction::new(9302, 9302);
        let conflict = txn2
            .add_pending_row_id_delete(CommandId::new(0), tablet.clone(), vec![location])
            .expect_err("should conflict with txn1 lock");
        assert!(
            conflict.to_string().contains("write-write conflict"),
            "expected write-write conflict, got: {conflict}"
        );

        txn1.rollback().expect("rollback txn1");
        assert_eq!(collect_rows_i32(&table), vec![1, 2, 3]);

        // Rollback releases locks; txn2 can stage and commit the delete.
        txn2.add_pending_row_id_delete(CommandId::new(0), tablet, vec![location])
            .expect("stage row-id delete after rollback");
        commit_transaction(&txn2, 9402).expect("commit txn2");
        assert_eq!(collect_rows_i32(&table), vec![1, 3]);
    }

    #[test]
    fn test_delete_locks_stay_until_owner_finishes() {
        let table = create_table_with_keys(
            &[LogicalType::Integer, LogicalType::Integer],
            KeysType::PrimaryKeys,
        );
        table
            .append(&test_chunk_from_vectors(vec![
                test_i32_vector(&[1, 2]),
                test_i32_vector(&[10, 20]),
            ]))
            .expect("append rows");
        let tablet = table.tablet();
        let key_bytes = primary_key_bytes(&table, 1);
        let location = row_id_location_by_value(&table, 1);

        let txn1 = Transaction::new(9501, 9501);
        txn1.add_pending_primary_delete(
            CommandId::new(0),
            tablet.clone(),
            vec![key_bytes.clone()],
            vec![location],
        )
        .expect("stage primary delete");

        let txn2 = Transaction::new(9502, 9502);
        txn2.add_pending_primary_delete(
            CommandId::new(0),
            tablet.clone(),
            vec![key_bytes.clone()],
            vec![location],
        )
        .expect_err("lock should stay until owner finishes");
        txn1.rollback().expect("rollback primary owner");
        txn2.add_pending_primary_delete(
            CommandId::new(0),
            tablet.clone(),
            vec![key_bytes],
            vec![location],
        )
        .expect("lock should be released after rollback");
        txn2.rollback().expect("rollback primary successor");

        let row_location = row_id_location_by_value(&table, 2);
        let txn3 = Transaction::new(9503, 9503);
        txn3.add_pending_row_id_delete(CommandId::new(0), tablet.clone(), vec![row_location])
            .expect("stage row-id delete");

        let txn4 = Transaction::new(9504, 9504);
        txn4.add_pending_row_id_delete(CommandId::new(0), tablet.clone(), vec![row_location])
            .expect_err("row-id lock should stay until owner finishes");
        txn3.rollback().expect("rollback row-id owner");
        txn4.add_pending_row_id_delete(CommandId::new(0), tablet, vec![row_location])
            .expect("row-id lock should be released after rollback");
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

        commit_transaction(&txn, 500).expect("first commit should succeed");
        assert_eq!(txn.get_commit_id(), 500);

        // Second commit overwrites (not ideal, but safe)
        commit_transaction(&txn, 600).expect("second commit should succeed");
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
        commit_transaction(&txn, 500).expect("commit should succeed");

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
        let result = commit_transaction(&txn, 500);
        assert!(result.is_ok());
        assert_eq!(txn.get_commit_id(), 500);
        assert!(txn.is_awaiting_cleanup());
    }

    #[test]
    fn test_commit_no_changes_fast_path() {
        // Test that commit with no changes still succeeds (fast path)
        let txn = Transaction::new(1, 100);

        assert!(!txn.changes_made());

        let result = commit_transaction(&txn, 500);
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
        let commit_result = commit_transaction(&txn, 500);
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
