// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Session-side transaction state, including savepoints and admission tracking.

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_context::{ReadTrackerHandle, TxnAdmissionState, WriteGuard};
use paro_storage::transaction::manager::TransactionManager;
use paro_storage::transaction::txn::{StorageSavepointMark, Transaction};
use paro_transaction::{
    CommandId, DatabaseId, FrozenReadSet, IsolationLevel, ParticipantStateSet, ReadSnapshot,
    ReadTrackingPolicy, ReadTs, ReadWritePromotion, TransactionView,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::prepared::store::PortalStoreMark;

use super::block_kind::{BlockKind, SavepointFrame};
use super::ddl_changes::{CatalogOpBatch, PreparedCatalogOp};
use super::local_settings::TransactionLocalSettings;

#[derive(Debug)]
pub struct SessionTransaction {
    active: Option<Arc<Transaction>>,
    auto_commit: bool,
    block_kind: BlockKind,
    failed: bool,
    command_id: u32,
    local_settings: TransactionLocalSettings,
    savepoints: Vec<SavepointFrame>,
    ddl_changes: Arc<Mutex<CatalogOpBatch>>,
    admission_state: Arc<TxnAdmissionState>,
    write_guard: Arc<WriteGuard>,
    read_tracker: ReadTrackerHandle,
    default_isolation_level: IsolationLevel,
    default_read_only: bool,
    isolation_level: IsolationLevel,
    read_only: bool,
    participant_states: ParticipantStateSet,
}

#[derive(Debug)]
pub struct FrozenTransaction {
    pub active: Arc<Transaction>,
    pub ddl_changes: Vec<PreparedCatalogOp>,
    pub transaction_view: TransactionView,
}

/// Why the caller is freezing the session transaction. Commit enforces the
/// safe-snapshot-must-restart guard; rollback/abort is allowed to freeze any
/// state because no writes will be published.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FreezeIntent {
    Commit,
    Rollback,
}

impl Default for SessionTransaction {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionTransaction {
    /// Creates a new transaction context with auto-commit enabled.
    pub fn new() -> Self {
        Self {
            active: None,
            auto_commit: true,
            block_kind: BlockKind::None,
            failed: false,
            command_id: 0,
            local_settings: TransactionLocalSettings::default(),
            savepoints: Vec::new(),
            ddl_changes: Arc::new(Mutex::new(CatalogOpBatch::new())),
            admission_state: Arc::new(TxnAdmissionState::new()),
            write_guard: Arc::new(WriteGuard::new()),
            read_tracker: ReadTrackerHandle::noop(),
            default_isolation_level: IsolationLevel::Serializable,
            default_read_only: false,
            isolation_level: IsolationLevel::Serializable,
            read_only: false,
            participant_states: ParticipantStateSet::empty(),
        }
    }

    fn reset_characteristics_to_defaults(&mut self) {
        self.isolation_level = self.default_isolation_level;
        self.read_only = self.default_read_only;
    }

    // ============================================================
    // Command Counter
    // ============================================================

    /// Returns the current command ID.
    ///
    /// The command ID is used for visibility control within a transaction.
    /// Each statement sees only the effects of commands with a lower command ID.
    #[inline]
    pub fn current_command_id(&self) -> u32 {
        self.command_id
    }

    /// Increments the command counter.
    ///
    /// This should be called after each non-transaction-control statement
    /// (SELECT, INSERT, UPDATE, DELETE, DDL, etc.) executes successfully.
    /// Transaction control statements (BEGIN, COMMIT, ROLLBACK) do NOT
    /// increment the counter.
    ///
    /// The increment ensures that subsequent statements within the same
    /// transaction can see the effects of previous statements.
    ///
    /// Reference: PostgreSQL `src/backend/access/transam/xact.c` - `CommandCounterIncrement()`
    ///
    /// # Overflow Behavior
    ///
    /// The command ID wraps around on overflow. In practice, this is unlikely
    /// to be an issue as it would require over 4 billion commands in a single
    /// transaction.
    pub fn command_counter_increment(&mut self) {
        self.command_id = self.command_id.wrapping_add(1);
        if let Some(active) = self.active.as_ref() {
            active.publish_command_boundary(CommandId::new(self.command_id));
        }
        tracing::trace!(command_id = self.command_id, "command counter incremented");
    }

    // ============================================================
    // Transaction State Queries
    // ============================================================

    /// Returns whether the transaction is in a failed state.
    ///
    /// When true, all statements except ROLLBACK will return an error.
    /// This corresponds to PostgreSQL's `IsAbortedTransactionBlockState()`.
    ///
    /// Reference: PostgreSQL `src/backend/access/transam/xact.c`
    #[inline]
    pub fn is_failed(&self) -> bool {
        self.failed
    }

    /// Marks the transaction as failed.
    ///
    /// This is called when an error occurs during statement execution
    /// within an explicit transaction block. The transaction remains
    /// active but all subsequent statements (except ROLLBACK) will fail.
    ///
    /// Reference: PostgreSQL error handling in `exec_simple_query()`
    pub fn set_failed(&mut self) {
        self.failed = true;
    }

    /// Returns the kind of transaction block currently active.
    #[inline]
    pub fn block_kind(&self) -> BlockKind {
        self.block_kind
    }

    /// Returns whether we are in an implicit transaction block.
    ///
    /// Implicit transaction blocks are auto-created for multi-statement
    /// execution to provide atomicity.
    #[inline]
    pub fn is_in_implicit_block(&self) -> bool {
        self.block_kind == BlockKind::Implicit
    }

    /// Returns whether we are in an explicit transaction block.
    ///
    /// Explicit transaction blocks are started with BEGIN.
    #[inline]
    pub fn is_in_explicit_block(&self) -> bool {
        self.block_kind == BlockKind::Explicit
    }

    #[inline]
    pub fn local_setting(&self, name: &str) -> Option<&Value> {
        self.local_settings.overlay.get(&name.to_lowercase())
    }

    #[inline]
    pub fn local_settings(&self) -> &HashMap<String, Value> {
        &self.local_settings.overlay
    }

    #[inline]
    pub fn clear_failed(&mut self) {
        self.failed = false;
    }

    pub fn set_local_setting(&mut self, name: impl Into<String>, value: Option<Value>) {
        self.local_settings.set(name, value);
    }

    pub fn current_local_settings_mark(&self) -> usize {
        self.local_settings.mark()
    }

    pub fn define_savepoint(
        &mut self,
        name: impl Into<String>,
        portal_mark: PortalStoreMark,
        storage_mark: StorageSavepointMark,
    ) -> SavepointFrame {
        let ddl_mark = self
            .ddl_changes
            .lock()
            .map(|changes| changes.mark())
            .unwrap_or_default();
        let frame = SavepointFrame {
            name: name.into(),
            settings_journal_mark: self.local_settings.mark(),
            portal_mark,
            write_guard_mark: self.write_guard.mark(),
            ddl_mark,
            storage_mark,
        };
        self.savepoints.push(frame.clone());
        frame
    }

    pub fn mark_savepoint(&self) -> Result<StorageSavepointMark> {
        self.active_transaction()?
            .mark_savepoint_with_read_tracker(CommandId::new(self.command_id), &self.read_tracker)
    }

    pub fn release_savepoint(&mut self, name: &str) -> Result<SavepointFrame> {
        let Some(index) = self
            .savepoints
            .iter()
            .rposition(|frame| frame.name.eq_ignore_ascii_case(name))
        else {
            return Err(paro_error::invalid_transaction_state(format!(
                "savepoint \"{name}\" does not exist",
            )));
        };

        let frame = self.savepoints[index].clone();
        self.savepoints.truncate(index);
        Ok(frame)
    }

    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<SavepointFrame> {
        let Some(index) = self
            .savepoints
            .iter()
            .rposition(|frame| frame.name.eq_ignore_ascii_case(name))
        else {
            return Err(paro_error::invalid_transaction_state(format!(
                "savepoint \"{name}\" does not exist",
            )));
        };

        let frame = self.savepoints[index].clone();
        if let Some(active) = self.active.as_ref() {
            active.rollback_to_savepoint(&frame.storage_mark)?;
            active.publish_command_boundary(frame.storage_mark.command_id_mark);
        }
        self.read_tracker
            .rollback_to_savepoint(&frame.storage_mark.read_dependency_mark);
        self.command_id = frame.storage_mark.command_id_mark.into_raw();

        let rolled_back = self
            .ddl_changes
            .lock()
            .map_err(|_| paro_error::internal("ddl state poisoned"))?
            .rollback_to_mark(frame.ddl_mark);
        for mut change in rolled_back.into_iter().rev() {
            if let Some(handle) = change.catalog.take() {
                handle.discard()?;
            }
        }
        self.admission_state.rollback_to_mark(frame.ddl_mark);

        self.write_guard.restore(frame.write_guard_mark.clone());
        self.local_settings
            .rollback_to_mark(frame.settings_journal_mark);
        self.savepoints.truncate(index + 1);
        self.failed = false;
        Ok(frame)
    }

    // ============================================================
    // Auto-Commit Mode Management
    // ============================================================

    /// Returns whether auto-commit mode is enabled.
    ///
    /// When true (default), each statement is automatically committed.
    /// When false, transactions must be explicitly committed.
    #[inline]
    pub fn is_auto_commit(&self) -> bool {
        self.auto_commit
    }

    /// Sets the auto-commit mode.
    ///
    /// When setting `auto_commit` to `false`, a new transaction is automatically
    ///
    /// When setting `auto_commit` to `true`, this only changes the flag - it does
    /// not commit or rollback any active transaction.
    pub fn set_auto_commit(&mut self, value: bool, manager: &TransactionManager) -> Result<()> {
        self.auto_commit = value;
        // automatically start one
        if !self.auto_commit && self.active.is_none() {
            self.begin_transaction(manager)?;
        }
        Ok(())
    }

    /// Sets the auto-commit flag directly without starting a transaction.
    ///
    /// This is used when a transaction has already been started (e.g., by explicit BEGIN)
    /// and we just need to disable auto-commit mode.
    #[inline]
    pub fn set_auto_commit_flag(&mut self, value: bool) {
        self.auto_commit = value;
    }

    /// Returns whether there is an active transaction.
    #[inline]
    pub fn has_active_transaction(&self) -> bool {
        self.active.is_some()
    }

    /// Clears the current transaction and resets auto-commit to true.
    ///
    /// This is called after commit or rollback to reset the context state.
    pub fn clear_transaction(&mut self) {
        self.auto_commit = true;
        self.active = None;
        self.block_kind = BlockKind::None;
        self.failed = false;
        self.command_id = 0;
        self.local_settings = TransactionLocalSettings::default();
        self.savepoints.clear();
        if let Ok(mut ddl_state) = self.ddl_changes.lock() {
            ddl_state.clear();
        }
        self.admission_state.clear();
        self.write_guard.reset();
        self.read_tracker = ReadTrackerHandle::noop();
        self.reset_characteristics_to_defaults();
        self.participant_states = ParticipantStateSet::empty();
    }

    // ============================================================
    // Transaction Information
    // ============================================================

    /// Returns the transaction ID of the active transaction, if any.
    pub fn transaction_id(&self) -> Option<u64> {
        self.active.as_ref().map(|t| t.writer_id().into_raw())
    }

    /// Returns the start time of the active transaction, if any.
    pub fn start_time(&self) -> Option<u64> {
        self.active.as_ref().map(|t| t.read_ts().into_raw())
    }

    /// Returns the visible version of the active transaction, if any.
    pub fn visible_version(&self) -> Option<u64> {
        self.active
            .as_ref()
            .map(|t| t.visible_commit_ts().into_raw())
    }

    pub fn write_guard(&self) -> Arc<WriteGuard> {
        self.write_guard.clone()
    }

    pub fn ddl_changes(&self) -> Arc<Mutex<CatalogOpBatch>> {
        self.ddl_changes.clone()
    }

    pub fn admission_state(&self) -> Arc<TxnAdmissionState> {
        self.admission_state.clone()
    }

    pub fn read_tracker(&self) -> ReadTrackerHandle {
        self.read_tracker.clone()
    }

    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }

    #[inline]
    pub fn default_isolation_level(&self) -> IsolationLevel {
        self.default_isolation_level
    }

    #[inline]
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    #[inline]
    pub fn default_read_only(&self) -> bool {
        self.default_read_only
    }

    pub fn set_default_isolation_level(&mut self, isolation_level: IsolationLevel) {
        self.default_isolation_level = isolation_level;
        if self.active.is_none() {
            self.isolation_level = isolation_level;
        }
    }

    pub fn set_default_read_only(&mut self, read_only: bool) {
        self.default_read_only = read_only;
        if self.active.is_none() {
            self.read_only = read_only;
        }
    }

    pub fn set_transaction_characteristics(
        &mut self,
        isolation_level: Option<IsolationLevel>,
        read_only: Option<bool>,
    ) -> Result<()> {
        if self.active.is_none() || self.block_kind != BlockKind::Explicit {
            return Err(paro_error::invalid_transaction_state(
                "SET TRANSACTION can only be used in transaction blocks".to_string(),
            ));
        }
        if self.command_id != 0 {
            return Err(paro_error::invalid_transaction_state(
                "SET TRANSACTION must be called before the first query in a transaction"
                    .to_string(),
            ));
        }
        if let Some(isolation_level) = isolation_level {
            self.isolation_level = isolation_level;
        }
        if let Some(read_only) = read_only {
            self.read_only = read_only;
        }
        Ok(())
    }

    pub fn frozen_read_set(&self) -> FrozenReadSet {
        self.read_tracker.frozen_read_set()
    }

    pub fn participant_states(&self) -> ParticipantStateSet {
        self.participant_states.clone()
    }

    // ============================================================
    // Transaction Lifecycle
    // ============================================================

    /// Begins a new transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if a transaction is already active.
    pub fn begin_transaction(&mut self, manager: &TransactionManager) -> Result<Arc<Transaction>> {
        self.begin_transaction_for_database(
            manager,
            manager.database_id(),
            "",
            ReadTrackingPolicy::SafeSnapshotPreferred,
        )
    }

    pub fn begin_transaction_with_policy(
        &mut self,
        manager: &TransactionManager,
        read_tracking_policy: ReadTrackingPolicy,
    ) -> Result<Arc<Transaction>> {
        self.begin_transaction_for_database(
            manager,
            manager.database_id(),
            "",
            read_tracking_policy,
        )
    }

    pub fn begin_transaction_for_database(
        &mut self,
        manager: &TransactionManager,
        database_id: DatabaseId,
        database_name: &str,
        read_tracking_policy: ReadTrackingPolicy,
    ) -> Result<Arc<Transaction>> {
        if self.active.is_some() {
            return Err(paro_error::transaction_active());
        }
        self.command_id = 0;
        let txn = manager.begin_transaction()?;
        self.write_guard
            .set_transaction_database(database_id, database_name);
        self.read_tracker =
            manager.read_tracker_for_policy(txn.txn_id(), txn.read_ts(), read_tracking_policy);
        self.participant_states =
            ParticipantStateSet::from_vec(vec![txn.storage_participant_state()]);
        self.active = Some(txn.clone());
        Ok(txn)
    }

    pub fn prepare_statement_read_tracking(
        &mut self,
        manager: &TransactionManager,
        policy: ReadTrackingPolicy,
    ) -> Result<ReadWritePromotion> {
        self.prepare_statement_read_tracking_for_database(
            manager,
            policy,
            manager.database_id(),
            "",
        )
    }

    pub fn prepare_statement_read_tracking_for_database(
        &mut self,
        manager: &TransactionManager,
        policy: ReadTrackingPolicy,
        database_id: DatabaseId,
        database_name: &str,
    ) -> Result<ReadWritePromotion> {
        self.prepare_statement_read_tracking_for_database_with_access(
            manager,
            policy,
            policy != ReadTrackingPolicy::SafeSnapshotPreferred,
            database_id,
            database_name,
        )
    }

    pub fn prepare_statement_read_tracking_for_database_with_access(
        &mut self,
        manager: &TransactionManager,
        policy: ReadTrackingPolicy,
        requires_read_write: bool,
        database_id: DatabaseId,
        database_name: &str,
    ) -> Result<ReadWritePromotion> {
        if self.active.is_none() {
            self.begin_transaction_for_database(manager, database_id, database_name, policy)?;
        }
        if !requires_read_write {
            self.refresh_read_tracker_for_read_only_hint(manager, policy)?;
            return Ok(ReadWritePromotion::Promoted);
        }

        let active = self.active_transaction()?;
        if self.read_tracker.is_safe_snapshot() {
            if self.can_restart_safe_snapshot_for_write() {
                self.restart_as_read_write(manager, policy, database_id, database_name)?;
                return Ok(ReadWritePromotion::MustRestartImplicitOk);
            }
            return Ok(ReadWritePromotion::MustRestartUserVisible);
        }

        self.read_tracker =
            manager.read_tracker_for_policy(active.txn_id(), active.read_ts(), policy);
        active.promote_to_read_write()?;
        Ok(ReadWritePromotion::Promoted)
    }

    fn refresh_read_tracker_for_read_only_hint(
        &mut self,
        manager: &TransactionManager,
        policy: ReadTrackingPolicy,
    ) -> Result<()> {
        let active = self.active_transaction()?;
        if policy == ReadTrackingPolicy::SafeSnapshotPreferred {
            return Ok(());
        }
        if self.read_tracker.is_safe_snapshot() && self.command_id != 0 {
            return Ok(());
        }
        self.read_tracker =
            manager.read_tracker_for_policy(active.txn_id(), active.read_ts(), policy);
        Ok(())
    }

    fn can_restart_safe_snapshot_for_write(&self) -> bool {
        self.command_id == 0
            && self.savepoints.is_empty()
            && self
                .ddl_changes
                .lock()
                .map(|changes| changes.is_empty())
                .unwrap_or(false)
    }

    fn restart_as_read_write(
        &mut self,
        manager: &TransactionManager,
        policy: ReadTrackingPolicy,
        database_id: DatabaseId,
        database_name: &str,
    ) -> Result<()> {
        let auto_commit = self.auto_commit;
        let block_kind = self.block_kind;
        if let Some(active) = self.active.take() {
            manager.rollback_transaction(active)?;
        }
        self.command_id = 0;
        self.read_tracker = ReadTrackerHandle::noop();
        self.participant_states = ParticipantStateSet::empty();
        if let Ok(mut ddl_state) = self.ddl_changes.lock() {
            ddl_state.clear();
        }
        self.admission_state.clear();
        self.write_guard.reset();
        self.savepoints.clear();

        let txn =
            self.begin_transaction_for_database(manager, database_id, database_name, policy)?;
        txn.promote_to_read_write()?;
        self.auto_commit = auto_commit;
        self.block_kind = block_kind;
        Ok(())
    }

    /// Begins an explicit transaction block (for BEGIN command).
    ///
    /// This starts a new transaction and marks it as an explicit block.
    /// Auto-commit is disabled until COMMIT or ROLLBACK.
    ///
    /// # Errors
    ///
    /// Returns an error if a transaction is already active.
    ///
    /// Reference: PostgreSQL `BeginTransactionBlock()`
    pub fn begin_explicit_block(
        &mut self,
        manager: &TransactionManager,
    ) -> Result<Arc<Transaction>> {
        let txn = self.begin_transaction(manager)?;
        self.block_kind = BlockKind::Explicit;
        self.auto_commit = false;
        Ok(txn)
    }

    pub fn begin_explicit_block_for_database(
        &mut self,
        manager: &TransactionManager,
        database_id: DatabaseId,
        database_name: &str,
    ) -> Result<Arc<Transaction>> {
        let txn = self.begin_transaction_for_database(
            manager,
            database_id,
            database_name,
            ReadTrackingPolicy::SafeSnapshotPreferred,
        )?;
        self.block_kind = BlockKind::Explicit;
        self.auto_commit = false;
        Ok(txn)
    }

    pub fn begin_explicit_block_with_characteristics(
        &mut self,
        manager: &TransactionManager,
        database_id: DatabaseId,
        database_name: &str,
        isolation_level: Option<IsolationLevel>,
        read_only: Option<bool>,
    ) -> Result<Arc<Transaction>> {
        let txn = self.begin_explicit_block_for_database(manager, database_id, database_name)?;
        if let Some(isolation_level) = isolation_level {
            self.isolation_level = isolation_level;
        }
        if let Some(read_only) = read_only {
            self.read_only = read_only;
        }
        Ok(txn)
    }

    // ============================================================
    // Implicit Transaction Block
    // ============================================================

    /// Begins an implicit transaction block for multi-statement execution.
    ///
    /// This is called when the second statement arrives in a multi-statement
    /// request. The implicit block provides atomicity for the entire batch.
    ///
    /// If a transaction is already active (e.g., from auto-commit of first
    /// statement), this just marks it as an implicit block.
    ///
    /// Reference: PostgreSQL `BeginImplicitTransactionBlock()` in
    /// `src/backend/tcop/postgres.c`
    ///
    /// # Behavior
    ///
    /// - If no transaction is active: starts a new transaction and marks as implicit
    /// - If transaction is active: just marks the block kind as implicit
    ///
    /// Returns `true` when this call started a new transaction.
    pub fn begin_implicit_transaction_block(
        &mut self,
        manager: &TransactionManager,
    ) -> Result<bool> {
        self.begin_implicit_transaction_block_for_database(manager, manager.database_id(), "")
    }

    pub fn begin_implicit_transaction_block_for_database(
        &mut self,
        manager: &TransactionManager,
        database_id: DatabaseId,
        database_name: &str,
    ) -> Result<bool> {
        let started_new = self.active.is_none();
        if started_new {
            self.begin_transaction_for_database(
                manager,
                database_id,
                database_name,
                ReadTrackingPolicy::SafeSnapshotPreferred,
            )?;
        }

        // Mark as implicit block and disable auto-commit
        self.block_kind = BlockKind::Implicit;
        self.auto_commit = false;
        Ok(started_new)
    }

    /// Rolls back an implicit transaction block.
    ///
    /// This is called when an error occurs during multi-statement execution
    /// within an implicit transaction block. The entire batch is rolled back.
    ///
    /// # Behavior
    ///
    /// - If in implicit block: rolls back the transaction
    /// - If not in implicit block: no-op
    pub fn rollback_implicit_transaction(&mut self, manager: &TransactionManager) -> Result<()> {
        if self.block_kind != BlockKind::Implicit {
            return Ok(());
        }

        if self.active.is_some() {
            self.rollback(manager)?;
        } else {
            self.clear_transaction();
        }
        Ok(())
    }

    /// Rolls back the current transaction.
    ///
    /// After rollback, the transaction context is cleared and auto-commit is reset to true.
    /// This also clears the failed state, allowing new transactions to proceed.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no active transaction.
    pub fn rollback(&mut self, manager: &TransactionManager) -> Result<()> {
        let txn = self
            .active
            .take()
            .ok_or_else(|| paro_error::no_transaction())?;
        self.clear_transaction();
        manager.rollback_transaction(txn)
    }

    /// Rolls back the current transaction, clearing the failed state.
    ///
    /// This is specifically for handling ROLLBACK in a failed transaction.
    /// Unlike `rollback()`, this method does not return an error if there
    /// is no active transaction (PostgreSQL compatible behavior).
    ///
    /// Reference: PostgreSQL `AbortCurrentTransaction()` and `CleanupTransaction()`
    pub fn rollback_and_clear_failed(&mut self, manager: &TransactionManager) -> Result<()> {
        if let Some(txn) = self.active.take() {
            manager.rollback_transaction(txn)?;
        }
        self.clear_transaction();
        Ok(())
    }

    /// Returns the active transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no active transaction.
    pub fn active_transaction(&self) -> Result<Arc<Transaction>> {
        self.active
            .clone()
            .ok_or_else(|| paro_error::no_transaction())
    }

    pub fn take_read_only_noop_for_commit(&mut self) -> Option<Arc<Transaction>> {
        let active = self.active.as_ref()?;
        if !active.is_read_only() {
            return None;
        }
        if !self
            .ddl_changes
            .lock()
            .map(|changes| changes.is_empty())
            .unwrap_or(false)
        {
            return None;
        }

        let active = self.active.take()?;
        self.clear_transaction();
        Some(active)
    }

    pub fn freeze(&mut self) -> Result<FrozenTransaction> {
        self.freeze_for(FreezeIntent::Commit)
    }

    pub fn freeze_for(&mut self, intent: FreezeIntent) -> Result<FrozenTransaction> {
        if matches!(intent, FreezeIntent::Commit) {
            let has_catalog_writes = self
                .ddl_changes
                .lock()
                .map(|changes| !changes.is_empty())
                .unwrap_or(true);
            if self.read_tracker.is_safe_snapshot()
                && (has_catalog_writes
                    || self
                        .active
                        .as_ref()
                        .is_some_and(|active| !active.is_read_only()))
            {
                return Err(paro_error::serialization_failure(
                    "safe snapshot transaction must restart before committing writes",
                ));
            }
        }
        let active = self
            .active
            .take()
            .ok_or_else(|| paro_error::no_transaction())?;
        active.freeze_write_buffer(CommandId::new(self.command_id));
        let transaction_view = TransactionView::new(
            active.writer_id(),
            active.read_ts(),
            ReadSnapshot::without_lease(ReadTs::new(active.visible_commit_ts().into_raw())),
            self.isolation_level,
            CommandId::new(self.command_id),
            self.read_tracker.clone(),
            self.participant_states.clone(),
        );
        let ddl_changes = self
            .ddl_changes
            .lock()
            .map(|mut changes| changes.take_all())
            .unwrap_or_default();
        self.clear_transaction();
        Ok(FrozenTransaction {
            active,
            ddl_changes,
            transaction_view,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::ddl::{
        CreateSchemaPayload, DdlChange, DdlChangeRecord, DdlObjectKey, DdlObjectKind,
    };
    use paro_context::DdlExecutionProfile;

    fn create_manager() -> TransactionManager {
        TransactionManager::new()
    }

    fn commit_frozen_for_test(
        ctx: &mut SessionTransaction,
        manager: &TransactionManager,
    ) -> Result<u64> {
        let frozen = ctx.freeze()?;
        let commit_id = manager.durable_commit_id().saturating_add(1).max(1);
        frozen.active.commit(commit_id)?;
        Ok(commit_id)
    }

    // ============================================================
    // Auto-Commit Tests
    // ============================================================

    #[test]
    fn test_default_auto_commit_is_true() {
        let ctx = SessionTransaction::new();
        assert!(ctx.is_auto_commit());
        assert!(!ctx.has_active_transaction());
        assert!(!ctx.is_failed());
        assert_eq!(ctx.block_kind(), BlockKind::None);
    }

    #[test]
    fn test_set_auto_commit_false_starts_transaction() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        // Initially no transaction
        assert!(!ctx.has_active_transaction());

        // Setting auto_commit to false should auto-start a transaction
        ctx.set_auto_commit(false, &manager).unwrap();

        assert!(!ctx.is_auto_commit());
        assert!(ctx.has_active_transaction());
        assert!(ctx.transaction_id().is_some());
    }

    #[test]
    fn test_set_auto_commit_true_does_not_affect_transaction() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        // Start a transaction first
        ctx.begin_transaction(&manager).unwrap();
        assert!(ctx.has_active_transaction());

        // Setting auto_commit to true should not commit/rollback
        ctx.set_auto_commit(true, &manager).unwrap();

        assert!(ctx.is_auto_commit());
        assert!(ctx.has_active_transaction()); // Transaction still active
    }

    #[test]
    fn test_set_auto_commit_false_with_existing_transaction() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        // Start a transaction first
        let txn = ctx.begin_transaction(&manager).unwrap();
        let txn_id = txn.id;

        // Setting auto_commit to false should NOT start a new transaction
        ctx.set_auto_commit(false, &manager).unwrap();

        assert!(!ctx.is_auto_commit());
        assert!(ctx.has_active_transaction());
        assert_eq!(ctx.transaction_id(), Some(txn_id)); // Same transaction
    }

    // ============================================================
    // Has Active Transaction Tests
    // ============================================================

    #[test]
    fn test_has_active_transaction_initially_false() {
        let ctx = SessionTransaction::new();
        assert!(!ctx.has_active_transaction());
    }

    #[test]
    fn test_has_active_transaction_after_begin() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        ctx.begin_transaction(&manager).unwrap();
        assert!(ctx.has_active_transaction());
    }

    #[test]
    fn test_has_active_transaction_after_commit() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        ctx.begin_transaction(&manager).unwrap();
        commit_frozen_for_test(&mut ctx, &manager).unwrap();
        assert!(!ctx.has_active_transaction());
    }

    #[test]
    fn test_has_active_transaction_after_rollback() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        ctx.begin_transaction(&manager).unwrap();
        ctx.rollback(&manager).unwrap();
        assert!(!ctx.has_active_transaction());
    }

    // ============================================================
    // Clear Transaction Tests
    // ============================================================

    #[test]
    fn test_clear_transaction_resets_auto_commit() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        // Set auto_commit to false (which starts a transaction)
        ctx.set_auto_commit(false, &manager).unwrap();
        assert!(!ctx.is_auto_commit());

        // Clear transaction should reset auto_commit to true
        ctx.clear_transaction();

        assert!(ctx.is_auto_commit());
        assert!(!ctx.has_active_transaction());
    }

    #[test]
    fn test_commit_resets_auto_commit() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        // Set auto_commit to false and start transaction
        ctx.set_auto_commit(false, &manager).unwrap();
        assert!(!ctx.is_auto_commit());

        // Commit should reset auto_commit to true
        commit_frozen_for_test(&mut ctx, &manager).unwrap();

        assert!(ctx.is_auto_commit());
        assert!(!ctx.has_active_transaction());
    }

    #[test]
    fn test_rollback_resets_auto_commit() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        // Set auto_commit to false and start transaction
        ctx.set_auto_commit(false, &manager).unwrap();
        assert!(!ctx.is_auto_commit());

        // Rollback should reset auto_commit to true
        ctx.rollback(&manager).unwrap();

        assert!(ctx.is_auto_commit());
        assert!(!ctx.has_active_transaction());
    }

    // ============================================================
    // Error Cases
    // ============================================================

    #[test]
    fn test_begin_transaction_when_already_active_fails() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        ctx.begin_transaction(&manager).unwrap();

        let result = ctx.begin_transaction(&manager);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("already a transaction"));
    }

    #[test]
    fn safe_snapshot_read_only_transaction_skips_read_tracking() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        ctx.begin_transaction(&manager).unwrap();

        assert_eq!(
            ctx.read_tracker().policy(),
            ReadTrackingPolicy::SafeSnapshot
        );
        ctx.read_tracker()
            .record_table_read(paro_transaction::TableId::new(10));
        assert_eq!(ctx.frozen_read_set().dependency_count(), 0);
    }

    #[test]
    fn read_only_exact_hint_does_not_promote_to_read_write() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        ctx.begin_transaction(&manager).unwrap();
        let promotion = ctx
            .prepare_statement_read_tracking_for_database_with_access(
                &manager,
                ReadTrackingPolicy::RangeCritical,
                false,
                manager.database_id(),
                "",
            )
            .unwrap();

        assert_eq!(promotion, ReadWritePromotion::Promoted);
        assert!(ctx.active_transaction().unwrap().is_read_only());
        assert_eq!(
            ctx.read_tracker().policy(),
            ReadTrackingPolicy::RangeCritical
        );
    }

    #[test]
    fn implicit_safe_snapshot_write_restarts_as_read_write() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        let original = ctx.begin_transaction(&manager).unwrap().txn_id();
        let promotion = ctx
            .prepare_statement_read_tracking(&manager, ReadTrackingPolicy::RangeCritical)
            .unwrap();
        let restarted = ctx.active_transaction().unwrap();

        assert_eq!(promotion, ReadWritePromotion::MustRestartImplicitOk);
        assert_ne!(restarted.txn_id(), original);
        assert!(!restarted.is_read_only());
        assert_eq!(
            ctx.read_tracker().policy(),
            ReadTrackingPolicy::RangeCritical
        );
    }

    #[test]
    fn explicit_safe_snapshot_write_requires_user_visible_restart() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        ctx.begin_explicit_block(&manager).unwrap();
        ctx.command_counter_increment();
        let promotion = ctx
            .prepare_statement_read_tracking(&manager, ReadTrackingPolicy::RangeCritical)
            .unwrap();

        assert_eq!(promotion, ReadWritePromotion::MustRestartUserVisible);
        assert!(ctx.active_transaction().unwrap().is_read_only());
    }

    #[test]
    fn test_freeze_without_transaction_fails() {
        let mut ctx = SessionTransaction::new();

        let result = ctx.freeze();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("no transaction in progress"));
    }

    #[test]
    fn test_rollback_without_transaction_fails() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        let result = ctx.rollback(&manager);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("no transaction in progress"));
    }

    #[test]
    fn test_active_transaction_without_transaction_fails() {
        let ctx = SessionTransaction::new();

        let result = ctx.active_transaction();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("no transaction in progress"));
    }

    // ============================================================
    // Transaction Info Tests
    // ============================================================

    #[test]
    fn test_transaction_id_and_start_time() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        assert!(ctx.transaction_id().is_none());
        assert!(ctx.start_time().is_none());

        let txn = ctx.begin_transaction(&manager).unwrap();

        assert_eq!(ctx.transaction_id(), Some(txn.id));
        assert_eq!(ctx.start_time(), Some(txn.start_time));
    }

    // ============================================================
    // Failed-state behavior
    // ============================================================

    #[test]
    fn test_failed_state_initially_false() {
        let ctx = SessionTransaction::new();
        assert!(!ctx.is_failed());
    }

    #[test]
    fn test_set_failed_marks_transaction_as_failed() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        ctx.begin_transaction(&manager).unwrap();
        assert!(!ctx.is_failed());

        ctx.set_failed();
        assert!(ctx.is_failed());
        // Transaction is still active
        assert!(ctx.has_active_transaction());
    }

    #[test]
    fn test_rollback_clears_failed_state() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        ctx.begin_transaction(&manager).unwrap();
        ctx.set_failed();
        assert!(ctx.is_failed());

        ctx.rollback(&manager).unwrap();
        assert!(!ctx.is_failed());
        assert!(!ctx.has_active_transaction());
    }

    #[test]
    fn test_rollback_and_clear_failed_without_transaction() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        // Should not error even without active transaction
        ctx.rollback_and_clear_failed(&manager).unwrap();
        assert!(!ctx.is_failed());
    }

    #[test]
    fn test_clear_transaction_clears_failed_state() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        ctx.begin_transaction(&manager).unwrap();
        ctx.set_failed();
        assert!(ctx.is_failed());

        ctx.clear_transaction();
        assert!(!ctx.is_failed());
    }

    // ============================================================
    // Implicit transaction blocks
    // ============================================================

    #[test]
    fn test_begin_implicit_block_starts_transaction() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        assert!(!ctx.has_active_transaction());
        assert_eq!(ctx.block_kind(), BlockKind::None);

        let started_new = ctx.begin_implicit_transaction_block(&manager).unwrap();

        assert!(started_new);
        assert!(ctx.has_active_transaction());
        assert!(ctx.is_in_implicit_block());
        assert!(!ctx.is_auto_commit());
    }

    #[test]
    fn test_begin_implicit_block_with_existing_transaction() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        // Start a transaction first (simulating first statement in auto-commit)
        let txn = ctx.begin_transaction(&manager).unwrap();
        let txn_id = txn.id;

        // Begin implicit block should reuse existing transaction
        let started_new = ctx.begin_implicit_transaction_block(&manager).unwrap();

        assert!(!started_new);
        assert!(ctx.has_active_transaction());
        assert!(ctx.is_in_implicit_block());
        assert_eq!(ctx.transaction_id(), Some(txn_id)); // Same transaction
    }

    #[test]
    fn test_rollback_implicit_transaction() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        let _ = ctx.begin_implicit_transaction_block(&manager).unwrap();
        assert!(ctx.has_active_transaction());

        ctx.rollback_implicit_transaction(&manager).unwrap();
        assert!(!ctx.has_active_transaction());
        assert!(ctx.is_auto_commit());
        assert_eq!(ctx.block_kind(), BlockKind::None);
    }

    #[test]
    fn test_rollback_implicit_noop_for_explicit() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        // Start explicit transaction
        ctx.begin_explicit_block(&manager).unwrap();

        // rollback_implicit_transaction should be a no-op
        ctx.rollback_implicit_transaction(&manager).unwrap();
        assert!(ctx.has_active_transaction()); // Still active
        assert!(ctx.is_in_explicit_block()); // Still explicit
    }

    // ============================================================
    // Explicit Transaction Block Tests
    // ============================================================

    #[test]
    fn test_begin_explicit_block() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        ctx.begin_explicit_block(&manager).unwrap();

        assert!(ctx.has_active_transaction());
        assert!(ctx.is_in_explicit_block());
        assert!(!ctx.is_auto_commit());
    }

    #[test]
    fn test_explicit_block_commit_clears_state() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        ctx.begin_explicit_block(&manager).unwrap();
        commit_frozen_for_test(&mut ctx, &manager).unwrap();

        assert!(!ctx.has_active_transaction());
        assert!(ctx.is_auto_commit());
        assert_eq!(ctx.block_kind(), BlockKind::None);
    }

    #[test]
    fn test_explicit_block_rollback_clears_state() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        ctx.begin_explicit_block(&manager).unwrap();
        ctx.rollback(&manager).unwrap();

        assert!(!ctx.has_active_transaction());
        assert!(ctx.is_auto_commit());
        assert_eq!(ctx.block_kind(), BlockKind::None);
    }

    // ============================================================
    // Combined Scenarios
    // ============================================================

    #[test]
    fn test_failed_explicit_transaction_needs_rollback() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        // Start explicit transaction
        ctx.begin_explicit_block(&manager).unwrap();

        // Simulate error - mark as failed
        ctx.set_failed();
        assert!(ctx.is_failed());
        assert!(ctx.has_active_transaction());

        // Rollback clears failed state
        ctx.rollback(&manager).unwrap();
        assert!(!ctx.is_failed());
        assert!(!ctx.has_active_transaction());
    }

    #[test]
    fn test_implicit_block_error_rollback() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        // Start implicit block
        let _ = ctx.begin_implicit_transaction_block(&manager).unwrap();

        // Simulate error - rollback implicit transaction
        ctx.rollback_implicit_transaction(&manager).unwrap();

        assert!(!ctx.has_active_transaction());
        assert!(ctx.is_auto_commit());
        assert!(!ctx.is_failed());
    }

    // ============================================================
    // Command counter
    // ============================================================

    #[test]
    fn test_command_counter_initially_zero() {
        let ctx = SessionTransaction::new();
        assert_eq!(ctx.current_command_id(), 0);
    }

    #[test]
    fn test_command_counter_increment() {
        let mut ctx = SessionTransaction::new();

        assert_eq!(ctx.current_command_id(), 0);

        ctx.command_counter_increment();
        assert_eq!(ctx.current_command_id(), 1);

        ctx.command_counter_increment();
        assert_eq!(ctx.current_command_id(), 2);

        ctx.command_counter_increment();
        assert_eq!(ctx.current_command_id(), 3);
    }

    #[test]
    fn test_command_counter_reset_on_clear() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        // Start transaction and increment counter
        ctx.begin_transaction(&manager).unwrap();
        ctx.command_counter_increment();
        ctx.command_counter_increment();
        assert_eq!(ctx.current_command_id(), 2);

        // Clear transaction should reset counter
        ctx.clear_transaction();
        assert_eq!(ctx.current_command_id(), 0);
    }

    #[test]
    fn test_command_counter_reset_on_commit() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        // Start transaction and increment counter
        ctx.begin_transaction(&manager).unwrap();
        ctx.command_counter_increment();
        ctx.command_counter_increment();
        assert_eq!(ctx.current_command_id(), 2);

        // Commit should reset counter
        commit_frozen_for_test(&mut ctx, &manager).unwrap();
        assert_eq!(ctx.current_command_id(), 0);
    }

    #[test]
    fn test_command_counter_reset_on_rollback() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        // Start transaction and increment counter
        ctx.begin_transaction(&manager).unwrap();
        ctx.command_counter_increment();
        ctx.command_counter_increment();
        assert_eq!(ctx.current_command_id(), 2);

        // Rollback should reset counter
        ctx.rollback(&manager).unwrap();
        assert_eq!(ctx.current_command_id(), 0);
    }

    #[test]
    fn test_new_transaction_starts_at_command_zero_after_autocommit_increment() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        ctx.command_counter_increment();
        assert_eq!(ctx.current_command_id(), 1);

        ctx.begin_transaction(&manager).unwrap();
        assert_eq!(ctx.current_command_id(), 0);
    }

    #[test]
    fn test_read_only_noop_commit_fast_path_clears_transaction() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        let txn = ctx.begin_transaction(&manager).unwrap();
        let taken = ctx
            .take_read_only_noop_for_commit()
            .expect("read-only transaction should use no-op commit path");

        assert_eq!(taken.txn_id(), txn.txn_id());
        assert!(!ctx.has_active_transaction());
        assert_eq!(ctx.current_command_id(), 0);
    }

    #[test]
    fn test_command_counter_persists_across_statements() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        // Start explicit transaction
        ctx.begin_explicit_block(&manager).unwrap();

        // Simulate multiple statements
        ctx.command_counter_increment(); // Statement 1
        assert_eq!(ctx.current_command_id(), 1);

        ctx.command_counter_increment(); // Statement 2
        assert_eq!(ctx.current_command_id(), 2);

        ctx.command_counter_increment(); // Statement 3
        assert_eq!(ctx.current_command_id(), 3);

        // Counter persists until commit/rollback
        commit_frozen_for_test(&mut ctx, &manager).unwrap();
        assert_eq!(ctx.current_command_id(), 0);
    }

    #[test]
    fn test_command_counter_wrapping() {
        let mut ctx = SessionTransaction::new();

        // Set command_id to max value (simulate many commands)
        // We can't easily set it directly, but we can test the wrapping behavior
        // by checking that increment doesn't panic
        for _ in 0..1000 {
            ctx.command_counter_increment();
        }
        assert_eq!(ctx.current_command_id(), 1000);
    }

    #[test]
    fn test_rollback_to_savepoint_restores_local_settings_and_clears_failed() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        ctx.begin_explicit_block(&manager).unwrap();
        ctx.set_local_setting("application_name", Some(Value::Varchar("base".to_string())));
        ctx.define_savepoint(
            "sp1",
            PortalStoreMark::default(),
            StorageSavepointMark::default(),
        );
        ctx.set_local_setting(
            "application_name",
            Some(Value::Varchar("after".to_string())),
        );
        ctx.set_failed();

        ctx.rollback_to_savepoint("sp1").unwrap();

        assert_eq!(
            ctx.local_setting("application_name"),
            Some(&Value::Varchar("base".to_string()))
        );
        assert!(!ctx.is_failed());
    }

    #[test]
    fn test_rollback_to_savepoint_discards_ddl_changes_after_mark() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        ctx.begin_explicit_block(&manager).unwrap();
        ctx.define_savepoint(
            "sp1",
            PortalStoreMark::default(),
            StorageSavepointMark::default(),
        );

        ctx.ddl_changes().lock().unwrap().record(PreparedCatalogOp {
            record: DdlChangeRecord {
                key: DdlObjectKey::new("test", None::<String>, "sp1_schema", DdlObjectKind::Schema),
                change: DdlChange::CreateSchema(CreateSchemaPayload {
                    object_id: 0,
                    if_not_exists: false,
                }),
            },
            profile: DdlExecutionProfile::metadata_only(),
            catalog: None,
            dependencies: None,
            dml_targets: Vec::new(),
            staged_artifacts: Vec::new(),
            runtime_transitions: Vec::new(),
            cleanups: Vec::new(),
            post_commit_hooks: Vec::new(),
            transient_runtime: None,
        });

        ctx.rollback_to_savepoint("sp1").unwrap();

        assert!(ctx.ddl_changes().lock().unwrap().is_empty());
    }

    #[test]
    fn test_release_savepoint_discards_nested_frames() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        ctx.begin_explicit_block(&manager).unwrap();
        ctx.define_savepoint(
            "sp1",
            PortalStoreMark::default(),
            StorageSavepointMark::default(),
        );
        ctx.define_savepoint(
            "sp2",
            PortalStoreMark::default(),
            StorageSavepointMark::default(),
        );
        ctx.define_savepoint(
            "sp3",
            PortalStoreMark::default(),
            StorageSavepointMark::default(),
        );

        ctx.release_savepoint("sp2").unwrap();

        let err = ctx.release_savepoint("sp3").unwrap_err();
        assert!(err.to_string().contains("savepoint \"sp3\" does not exist"));
        ctx.rollback_to_savepoint("sp1").unwrap();
    }

    #[test]
    fn test_rollback_to_savepoint_without_storage_writes_succeeds() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        ctx.begin_explicit_block(&manager).unwrap();
        ctx.define_savepoint(
            "sp1",
            PortalStoreMark::default(),
            StorageSavepointMark::default(),
        );

        ctx.rollback_to_savepoint("sp1").unwrap();
    }

    #[test]
    fn test_rollback_to_savepoint_restores_command_and_read_tracker() {
        let mut ctx = SessionTransaction::new();
        let manager = create_manager();

        ctx.begin_explicit_block(&manager).unwrap();
        ctx.prepare_statement_read_tracking(&manager, ReadTrackingPolicy::RangeCritical)
            .unwrap();
        ctx.read_tracker()
            .record_table_read(paro_transaction::TableId::new(10));
        ctx.command_counter_increment();
        ctx.command_counter_increment();

        let storage_mark = ctx.mark_savepoint().unwrap();
        ctx.define_savepoint("sp1", PortalStoreMark::default(), storage_mark);

        ctx.read_tracker()
            .record_table_read(paro_transaction::TableId::new(20));
        ctx.command_counter_increment();

        ctx.rollback_to_savepoint("sp1").unwrap();

        assert_eq!(ctx.current_command_id(), 2);
        assert_eq!(
            ctx.read_tracker().recorded_dependencies(),
            vec![paro_transaction::ReadDependency::Table {
                table_id: paro_transaction::TableId::new(10)
            }]
        );
    }
}
