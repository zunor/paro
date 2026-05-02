// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Transaction manager state and cleanup orchestration.

use crate::transaction::txn::Transaction;
use paro_common::error::{self as paro_error, Result};
use paro_transaction::{
    ActiveTxnRegistry, CleanupBackpressureSnapshot, CommitBackpressureController,
    CommitBackpressureOptions, CommitBackpressureSnapshot, CommitFenceRejectReason,
    CommitFinalFence, CommitFinalizeReservation, CommitFinalizeReservationFactory,
    CommitFinalizeReservationInput, CommitFrontier, CommitPlan, CommitSequencer,
    CommitSequencerMetrics, CommitSequencerOptions, CommitSequencingPlan, CommitTs,
    CommittedTxnSummary, CommittedTxnSummaryIndex, ConflictWrite, DatabaseId, FrozenReadSet,
    IsolationLevel, LockNamespace, LockResource, ReadDependencyIndex, ReadTrackerHandle,
    ReadTrackingPolicy, ReadTs, RetentionLeaseKind, RetentionRegistry, ShardedLockManager,
    SsiValidationOutcome, SsiValidator, SummaryReservation, TxnId, WriteConflictIndex,
    WriteConflictReservation,
};
pub use paro_transaction::{MAX_TRANSACTION_ID, TRANSACTION_ID_START};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

const RECOVERY_ADMISSION_OPEN: u8 = 0;
const RECOVERY_ADMISSION_BLOCKED: u8 = 1;

/// Collects transactions awaiting cleanup.
///
/// This ensures we can clean up after releasing the transaction lock.
/// All transactions in a cleanup info share the same `lowest_start_time`.
pub struct CleanupInfo {
    /// All transactions in this cleanup info share the same lowest_start_time.
    /// This is the minimum start_time among remaining active transactions
    /// at the time this cleanup info was created.
    pub lowest_start_time: u64,

    /// Transactions awaiting cleanup.
    pub transactions: Vec<Arc<Transaction>>,
}

impl std::fmt::Debug for CleanupInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CleanupInfo")
            .field("lowest_start_time", &self.lowest_start_time)
            .field("transactions", &self.transactions.len())
            .finish()
    }
}

impl CleanupInfo {
    /// Create a new empty cleanup info.
    pub fn new(lowest_start_time: u64) -> Self {
        Self {
            lowest_start_time,
            transactions: Vec::new(),
        }
    }

    /// Perform cleanup on all transactions in this info.
    pub fn cleanup(&self) {
        for transaction in &self.transactions {
            if transaction.is_awaiting_cleanup() {
                transaction.cleanup(self.lowest_start_time);
            }
        }
    }

    /// Check if there are transactions to clean up.
    #[inline]
    pub fn should_schedule(&self) -> bool {
        !self.transactions.is_empty()
    }

    /// Add a transaction to this cleanup info.
    pub fn add_transaction(&mut self, transaction: Arc<Transaction>) {
        self.transactions.push(transaction);
    }
}

/// Point-in-time registry metrics exposed by the transaction manager.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionManagerMetricsSnapshot {
    pub txn_begin_count: u64,
    pub txn_begin_latency_us_total: u64,
    pub txn_begin_latency_us_peak: u64,
    pub txn_commit_count: u64,
    pub txn_commit_latency_us_total: u64,
    pub txn_commit_latency_us_peak: u64,
    pub txn_commit_prepare_latency_us_total: u64,
    pub txn_commit_prepare_latency_us_peak: u64,
    pub txn_commit_validate_latency_us_total: u64,
    pub txn_commit_validate_latency_us_peak: u64,
    pub txn_commit_durable_latency_us_total: u64,
    pub txn_commit_durable_latency_us_peak: u64,
    pub txn_commit_required_publish_wait_us_total: u64,
    pub txn_commit_required_publish_wait_us_peak: u64,
    pub txn_commit_publish_latency_us_total: u64,
    pub txn_commit_publish_latency_us_peak: u64,
    pub txn_commit_ack_mode_last: u64,
    pub write_conflict_index_size: u64,
    pub write_conflict_index_fine_entries: u64,
    pub write_conflict_index_fine_summary_entries: u64,
    pub write_conflict_index_coarse_entries: u64,
    pub lock_wait_count: u64,
    pub lock_wait_duration_us: u64,
    pub lock_wound_wait_abort_count: u64,
    pub lock_deadlock_abort_count: u64,
    pub read_snapshot_lease_count: u64,
    pub active_txn_count: u64,
    pub active_rw_txn_count: u64,
    pub oldest_active_rw_lag_ms: u64,
    pub retention_watermark_lag_ms: u64,
    pub active_registry_epoch: u64,
    pub retention_registry_epoch: u64,
    pub ssi_validation_abort_count: u64,
    pub ssi_abort_due_to_exact_dependency: u64,
    pub ssi_abort_due_to_coarse_scan_marker: u64,
    pub read_tracker_record_count: u64,
    pub read_tracker_coarsened_count: u64,
    pub read_tracking_hint_count: u64,
    pub read_tracking_policy_escalation_count: u64,
    pub read_tracking_point_critical_count: u64,
    pub read_tracking_range_critical_count: u64,
    pub read_tracking_analytical_scan_count: u64,
    pub read_tracking_safe_snapshot_preferred_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionLatencyStage {
    CommitTotal,
    CommitPrepare,
    CommitValidate,
    CommitDurable,
    CommitRequiredPublishWait,
    CommitPublish,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryAdmissionState {
    Open,
    Blocked,
}

impl RecoveryAdmissionState {
    fn from_raw(raw: u8) -> Self {
        match raw {
            RECOVERY_ADMISSION_BLOCKED => Self::Blocked,
            _ => Self::Open,
        }
    }
}

/// Manages transactions within the database.
#[derive(Debug)]
pub struct TransactionManager {
    /// The current transaction ID for new transactions.
    /// Starts at TRANSACTION_ID_START (very high) to distinguish from timestamps.
    current_transaction_id: AtomicU64,

    /// Commit-id visibility frontier shared with the commit runtime.
    commit_frontier: Arc<CommitFrontier>,

    /// Single commit timestamp owner for this database.
    commit_sequencer: Arc<CommitSequencer>,

    /// Commit publish-lag backpressure shared by session prepare and publish hooks.
    commit_backpressure: Arc<CommitBackpressureController>,

    /// Sharded active transaction registry for hot lifecycle paths.
    active_registry: ActiveTxnRegistry,

    /// Typed retention lease registry for cross-subsystem GC pins.
    retention_registry: RetentionRegistry,

    /// Database-scoped pessimistic lock manager for transactional writes.
    lock_manager: Arc<ShardedLockManager>,

    /// Durable write set index used by SI/SSI conflict checks.
    write_conflict_index: WriteConflictIndex,

    /// Serializable read dependency storage for active read-write transactions.
    read_dependency_index: Arc<ReadDependencyIndex>,

    /// Committed transaction summaries retained for SSI validation.
    committed_txn_summaries: CommittedTxnSummaryIndex,

    /// Namespace used for database-scoped table/object lock resources.
    lock_namespace: LockNamespace,

    /// Startup recovery gate. While blocked, runtime cannot admit new txns.
    recovery_admission: AtomicU8,

    /// Serializable abort counters split by exact vs analytical coarse marker.
    ssi_abort_due_to_exact_dependency: AtomicU64,
    ssi_abort_due_to_coarse_scan_marker: AtomicU64,

    /// Statement-level read tracking policy selection counters.
    read_tracking_hint_count: AtomicU64,
    read_tracking_policy_escalation_count: AtomicU64,
    read_tracking_point_critical_count: AtomicU64,
    read_tracking_range_critical_count: AtomicU64,
    read_tracking_analytical_scan_count: AtomicU64,
    read_tracking_safe_snapshot_preferred_count: AtomicU64,

    /// Lightweight transaction latency counters exposed through system metrics.
    txn_begin_count: AtomicU64,
    txn_begin_latency_us_total: AtomicU64,
    txn_begin_latency_us_peak: AtomicU64,
    txn_commit_count: AtomicU64,
    txn_commit_latency_us_total: AtomicU64,
    txn_commit_latency_us_peak: AtomicU64,
    txn_commit_prepare_latency_us_total: AtomicU64,
    txn_commit_prepare_latency_us_peak: AtomicU64,
    txn_commit_validate_latency_us_total: AtomicU64,
    txn_commit_validate_latency_us_peak: AtomicU64,
    txn_commit_durable_latency_us_total: AtomicU64,
    txn_commit_durable_latency_us_peak: AtomicU64,
    txn_commit_required_publish_wait_us_total: AtomicU64,
    txn_commit_required_publish_wait_us_peak: AtomicU64,
    txn_commit_publish_latency_us_total: AtomicU64,
    txn_commit_publish_latency_us_peak: AtomicU64,
    txn_commit_ack_mode_last: AtomicU64,

    /// List of recently committed transactions, pending cleanup.
    /// Transactions are moved here after commit and removed during GC.
    recently_committed_transactions: RwLock<Vec<Arc<Transaction>>>,

    /// Lock for cleanup operations. Only one cleanup can be active at any time.
    cleanup_lock: Mutex<()>,

    /// Lock for cleanup queue modifications.
    cleanup_queue_lock: Mutex<()>,

    /// Queue of cleanup infos. Cleanups must happen in-order.
    cleanup_queue: Mutex<VecDeque<CleanupInfo>>,
}

impl TransactionManager {
    /// Create a new transaction manager.
    pub fn new() -> Self {
        Self::new_for_database(DatabaseId::new(0))
    }

    pub fn new_for_database_id(database_id: u64) -> Self {
        Self::new_for_database(DatabaseId::new(database_id))
    }

    pub fn new_for_database(database_id: DatabaseId) -> Self {
        let lock_namespace = LockNamespace::single_tenant(database_id);
        Self {
            // Transaction ID starts very high to distinguish from timestamps
            current_transaction_id: AtomicU64::new(TRANSACTION_ID_START),
            commit_frontier: Arc::new(CommitFrontier::new()),
            commit_sequencer: Arc::new(CommitSequencer::new(
                CommitTs::new(1),
                CommitSequencerOptions::default(),
            )),
            commit_backpressure: Arc::new(CommitBackpressureController::new(
                CommitBackpressureOptions::default(),
            )),
            active_registry: ActiveTxnRegistry::default(),
            retention_registry: RetentionRegistry::default(),
            lock_manager: Arc::new(ShardedLockManager::default()),
            write_conflict_index: WriteConflictIndex::default(),
            read_dependency_index: Arc::new(ReadDependencyIndex::default()),
            committed_txn_summaries: CommittedTxnSummaryIndex::default(),
            lock_namespace,
            recovery_admission: AtomicU8::new(RECOVERY_ADMISSION_OPEN),
            ssi_abort_due_to_exact_dependency: AtomicU64::new(0),
            ssi_abort_due_to_coarse_scan_marker: AtomicU64::new(0),
            read_tracking_hint_count: AtomicU64::new(0),
            read_tracking_policy_escalation_count: AtomicU64::new(0),
            read_tracking_point_critical_count: AtomicU64::new(0),
            read_tracking_range_critical_count: AtomicU64::new(0),
            read_tracking_analytical_scan_count: AtomicU64::new(0),
            read_tracking_safe_snapshot_preferred_count: AtomicU64::new(0),
            txn_begin_count: AtomicU64::new(0),
            txn_begin_latency_us_total: AtomicU64::new(0),
            txn_begin_latency_us_peak: AtomicU64::new(0),
            txn_commit_count: AtomicU64::new(0),
            txn_commit_latency_us_total: AtomicU64::new(0),
            txn_commit_latency_us_peak: AtomicU64::new(0),
            txn_commit_prepare_latency_us_total: AtomicU64::new(0),
            txn_commit_prepare_latency_us_peak: AtomicU64::new(0),
            txn_commit_validate_latency_us_total: AtomicU64::new(0),
            txn_commit_validate_latency_us_peak: AtomicU64::new(0),
            txn_commit_durable_latency_us_total: AtomicU64::new(0),
            txn_commit_durable_latency_us_peak: AtomicU64::new(0),
            txn_commit_required_publish_wait_us_total: AtomicU64::new(0),
            txn_commit_required_publish_wait_us_peak: AtomicU64::new(0),
            txn_commit_publish_latency_us_total: AtomicU64::new(0),
            txn_commit_publish_latency_us_peak: AtomicU64::new(0),
            txn_commit_ack_mode_last: AtomicU64::new(0),
            recently_committed_transactions: RwLock::new(Vec::new()),
            cleanup_lock: Mutex::new(()),
            cleanup_queue_lock: Mutex::new(()),
            cleanup_queue: Mutex::new(VecDeque::new()),
        }
    }

    #[inline]
    pub const fn database_id(&self) -> DatabaseId {
        self.lock_namespace.database_id
    }

    /// Begin a new transaction.
    ///
    /// The active registry owns hot-path lifecycle tracking. Read snapshots use
    /// the shared commit frontier owned by the commit runtime.
    pub fn begin_transaction(&self) -> Result<Arc<Transaction>> {
        let started_at = Instant::now();
        self.ensure_recovery_admission_open()?;
        let start_time = self
            .commit_frontier
            .published_commit_id()
            .into_raw()
            .saturating_add(1);
        let id = self.current_transaction_id.fetch_add(1, Ordering::SeqCst);

        let txn = Arc::new(Transaction::with_catalog_version_and_locks(
            id,
            start_time,
            0,
            Arc::clone(&self.lock_manager),
            self.lock_namespace,
        ));
        let handle = self
            .active_registry
            .register(txn.txn_id(), txn.read_ts(), ReadTs::new(start_time))
            .map_err(|e| paro_error::internal(format!("failed to register active txn: {e}")))?;
        txn.bind_active_registry_handle(handle)?;
        self.record_begin_latency(started_at.elapsed());

        Ok(txn)
    }

    pub fn record_begin_latency(&self, duration: Duration) {
        observe_counted_latency(
            &self.txn_begin_count,
            &self.txn_begin_latency_us_total,
            &self.txn_begin_latency_us_peak,
            duration,
        );
    }

    pub fn record_commit_latency(&self, stage: TransactionLatencyStage, duration: Duration) {
        match stage {
            TransactionLatencyStage::CommitTotal => observe_counted_latency(
                &self.txn_commit_count,
                &self.txn_commit_latency_us_total,
                &self.txn_commit_latency_us_peak,
                duration,
            ),
            TransactionLatencyStage::CommitPrepare => observe_latency(
                &self.txn_commit_prepare_latency_us_total,
                &self.txn_commit_prepare_latency_us_peak,
                duration,
            ),
            TransactionLatencyStage::CommitValidate => observe_latency(
                &self.txn_commit_validate_latency_us_total,
                &self.txn_commit_validate_latency_us_peak,
                duration,
            ),
            TransactionLatencyStage::CommitDurable => observe_latency(
                &self.txn_commit_durable_latency_us_total,
                &self.txn_commit_durable_latency_us_peak,
                duration,
            ),
            TransactionLatencyStage::CommitRequiredPublishWait => observe_latency(
                &self.txn_commit_required_publish_wait_us_total,
                &self.txn_commit_required_publish_wait_us_peak,
                duration,
            ),
            TransactionLatencyStage::CommitPublish => observe_latency(
                &self.txn_commit_publish_latency_us_total,
                &self.txn_commit_publish_latency_us_peak,
                duration,
            ),
        }
    }

    pub fn record_commit_ack_mode(&self, mode: paro_transaction::CommitAckPolicy) {
        let encoded = match mode {
            paro_transaction::CommitAckPolicy::RequiredPublished => 0,
            paro_transaction::CommitAckPolicy::DurableOnlyAsync => 1,
        };
        self.txn_commit_ack_mode_last
            .store(encoded, Ordering::Release);
    }

    pub fn record_read_tracking_selection(
        &self,
        policy: ReadTrackingPolicy,
        had_user_hint: bool,
        escalated: bool,
    ) {
        if had_user_hint {
            self.read_tracking_hint_count.fetch_add(1, Ordering::AcqRel);
        }
        if escalated {
            self.read_tracking_policy_escalation_count
                .fetch_add(1, Ordering::AcqRel);
        }
        match policy {
            ReadTrackingPolicy::PointCritical => {
                self.read_tracking_point_critical_count
                    .fetch_add(1, Ordering::AcqRel);
            }
            ReadTrackingPolicy::RangeCritical => {
                self.read_tracking_range_critical_count
                    .fetch_add(1, Ordering::AcqRel);
            }
            ReadTrackingPolicy::AnalyticalScan => {
                self.read_tracking_analytical_scan_count
                    .fetch_add(1, Ordering::AcqRel);
            }
            ReadTrackingPolicy::SafeSnapshotPreferred | ReadTrackingPolicy::SafeSnapshot => {
                self.read_tracking_safe_snapshot_preferred_count
                    .fetch_add(1, Ordering::AcqRel);
            }
            ReadTrackingPolicy::Noop
            | ReadTrackingPolicy::Record
            | ReadTrackingPolicy::Serializable => {}
        }
    }

    #[cfg(test)]
    fn commit_transaction(&self, transaction: Arc<Transaction>) -> Result<u64> {
        let commit_id = self.commit_sequencer.next_commit_ts();
        self.commit_sequencer.sync_next_commit_ts_with(commit_id);
        self.publish_prepared_transaction_at(transaction, commit_id.into_raw())?;
        Ok(commit_id.into_raw())
    }

    #[cfg(test)]
    fn publish_prepared_transaction_at(
        &self,
        transaction: Arc<Transaction>,
        commit_id: u64,
    ) -> Result<()> {
        self.release_pre_publish_lifecycle(&transaction);
        transaction.release_transaction_locks();
        transaction.apply_prepared_storage_for_commit(commit_id)?;
        transaction.finalize_applied_commit(commit_id)?;
        let commit_ts = CommitTs::new(commit_id);
        self.commit_frontier.sync_commit_ids(commit_ts, commit_ts);
        self.enqueue_finalized_transaction_cleanup(&transaction, transaction.changes_made());
        Ok(())
    }

    pub fn complete_read_only_transaction(&self, transaction: Arc<Transaction>) -> Result<()> {
        let cleanup_info = self.finish_transaction(&transaction, false);
        if cleanup_info.should_schedule() {
            self.schedule_cleanup(cleanup_info);
        }
        self.process_cleanup();
        Ok(())
    }

    pub fn register_committed_write_set(
        &self,
        commit_ts: CommitTs,
        write_set: &[LockResource],
    ) -> Result<()> {
        self.register_committed_transaction_summary(
            commit_ts,
            TxnId::new(0),
            ReadTs::new(commit_ts.into_raw()),
            write_set,
            &FrozenReadSet::empty(),
        )
    }

    pub fn register_committed_transaction_summary(
        &self,
        commit_ts: CommitTs,
        txn_id: TxnId,
        read_ts: ReadTs,
        write_set: &[LockResource],
        read_set: &FrozenReadSet,
    ) -> Result<()> {
        self.register_committed_transaction_summary_inner(
            commit_ts, txn_id, read_ts, write_set, read_set, true,
        )
    }

    fn register_committed_transaction_summary_inner(
        &self,
        commit_ts: CommitTs,
        txn_id: TxnId,
        read_ts: ReadTs,
        write_set: &[LockResource],
        read_set: &FrozenReadSet,
        advance_durable_frontier: bool,
    ) -> Result<()> {
        if write_set.is_empty() {
            self.committed_txn_summaries
                .register_commit(CommittedTxnSummary::new(
                    txn_id,
                    read_ts,
                    commit_ts,
                    std::iter::empty(),
                    read_set,
                ))
                .map_err(|error| {
                    paro_error::internal(format!(
                        "failed to register committed transaction summary at {}: {:?}",
                        commit_ts, error
                    ))
                })?;
            if advance_durable_frontier {
                self.commit_frontier.sync_durable_commit_id(commit_ts);
            }
            return Ok(());
        }
        self.write_conflict_index
            .register_commit(commit_ts, write_set.iter().cloned().map(ConflictWrite::new))
            .map_err(|error| {
                paro_error::internal(format!(
                    "failed to register durable write conflict set at {}: {:?}",
                    commit_ts, error
                ))
            })?;
        self.committed_txn_summaries
            .register_commit(CommittedTxnSummary::new(
                txn_id,
                read_ts,
                commit_ts,
                write_set.iter().cloned(),
                read_set,
            ))
            .map_err(|error| {
                paro_error::internal(format!(
                    "failed to register committed transaction summary at {}: {:?}",
                    commit_ts, error
                ))
            })?;
        if advance_durable_frontier {
            self.commit_frontier.sync_durable_commit_id(commit_ts);
        }
        Ok(())
    }

    pub fn validate_serializable_commit(
        &self,
        plan: &CommitPlan,
        write_set: &[LockResource],
    ) -> Result<SsiValidationOutcome> {
        if plan.isolation != IsolationLevel::Serializable {
            return Ok(SsiValidationOutcome::snapshot(
                self.read_dependency_index.state_epoch(),
            ));
        }
        SsiValidator::new(&self.read_dependency_index, &self.committed_txn_summaries)
            .validate_commit(plan, write_set)
            .map_err(|error| {
                if error.coarse_scan_marker_conflict() {
                    self.ssi_abort_due_to_coarse_scan_marker
                        .fetch_add(1, Ordering::AcqRel);
                } else {
                    self.ssi_abort_due_to_exact_dependency
                        .fetch_add(1, Ordering::AcqRel);
                }
                paro_error::serialization_failure(format!(
                    "serializable validation failed: {:?}",
                    error
                ))
            })
    }

    pub fn ssi_final_fence_reason(
        &self,
        plan: &CommitSequencingPlan,
    ) -> Option<CommitFenceRejectReason> {
        if plan.plan.isolation != IsolationLevel::Serializable {
            return None;
        }
        let state = self.read_dependency_index.ssi_state(plan.plan.txn_id);
        let current_epoch = state.ssi_state_epoch;
        if current_epoch <= plan.validation_epoch {
            return None;
        }
        if state.coarse_scan_marker_conflict {
            self.ssi_abort_due_to_coarse_scan_marker
                .fetch_add(1, Ordering::AcqRel);
        } else {
            self.ssi_abort_due_to_exact_dependency
                .fetch_add(1, Ordering::AcqRel);
        }
        Some(CommitFenceRejectReason::SsiStateEpochAdvanced {
            validation_epoch: plan.validation_epoch,
            current_epoch,
        })
    }

    pub fn advance_conflict_horizon(&self, published_ts: CommitTs) -> CommitTs {
        let horizon = self
            .write_conflict_index
            .advance_horizon_with_confirmed_active_rw(published_ts, &self.active_registry);
        self.committed_txn_summaries.advance_horizon(horizon);
        horizon
    }

    pub fn block_recovery_admission(&self) {
        self.recovery_admission
            .store(RECOVERY_ADMISSION_BLOCKED, Ordering::Release);
    }

    pub fn complete_recovery_admission(&self, durable_commit_id: u64) {
        self.sync_commit_id_with(durable_commit_id);
        self.recovery_admission
            .store(RECOVERY_ADMISSION_OPEN, Ordering::Release);
    }

    pub fn recovery_admission_state(&self) -> RecoveryAdmissionState {
        RecoveryAdmissionState::from_raw(self.recovery_admission.load(Ordering::Acquire))
    }

    fn ensure_recovery_admission_open(&self) -> Result<()> {
        if self.recovery_admission_state() == RecoveryAdmissionState::Open {
            return Ok(());
        }
        Err(paro_error::cannot_connect_now()
            .detail("database is publishing recovered committed records"))
    }

    pub fn rollback_transaction(&self, transaction: Arc<Transaction>) -> Result<()> {
        // Execute rollback logic in the undo buffer
        transaction.rollback()?;

        let store_transaction = transaction.changes_made();
        let cleanup_info = self.finish_transaction(&transaction, store_transaction);

        // Schedule cleanup if needed
        if cleanup_info.should_schedule() {
            self.schedule_cleanup(cleanup_info);
        }

        // Process any pending cleanups
        self.process_cleanup();

        Ok(())
    }

    /// Finish a transaction lifecycle and create cleanup work.
    fn finish_transaction(
        &self,
        transaction: &Arc<Transaction>,
        store_transaction: bool,
    ) -> CleanupInfo {
        self.release_pre_publish_lifecycle(transaction);
        self.build_cleanup_info(transaction, store_transaction)
    }

    pub(crate) fn release_pre_publish_lifecycle(&self, transaction: &Arc<Transaction>) {
        transaction.release_active_registry_handle();
        self.read_dependency_index
            .release_transaction(transaction.txn_id());
    }

    pub(crate) fn enqueue_finalized_transaction_cleanup(
        &self,
        transaction: &Arc<Transaction>,
        store_transaction: bool,
    ) {
        let cleanup_info = self.build_cleanup_info(transaction, store_transaction);
        if cleanup_info.should_schedule() {
            self.schedule_cleanup(cleanup_info);
        }
        self.process_cleanup();
    }

    fn build_cleanup_info(
        &self,
        transaction: &Arc<Transaction>,
        store_transaction: bool,
    ) -> CleanupInfo {
        let lowest_start_time = self.lowest_active_start();
        let mut cleanup_info = CleanupInfo::new(lowest_start_time);
        let commit_id = *transaction.commit_id.lock().unwrap();

        if store_transaction {
            if commit_id != 0 {
                let mut committed = self.recently_committed_transactions.write().unwrap();
                committed.push(transaction.clone());
            } else {
                cleanup_info.add_transaction(transaction.clone());
            }
        } else if transaction.changes_made() {
            transaction.set_awaiting_cleanup(true);
            cleanup_info.add_transaction(transaction.clone());
        }

        self.move_committed_to_cleanup(&mut cleanup_info, lowest_start_time);

        cleanup_info
    }

    fn move_committed_to_cleanup(&self, cleanup_info: &mut CleanupInfo, lowest_start_time: u64) {
        let mut committed = self.recently_committed_transactions.write().unwrap();

        // Find transactions that can be cleaned up
        // (commit_id < lowest_start_time means no active transaction needs their old data)
        let mut to_cleanup = Vec::new();
        committed.retain(|t| {
            let commit_id = *t.commit_id.lock().unwrap();
            if commit_id < lowest_start_time {
                t.set_awaiting_cleanup(true);
                to_cleanup.push(t.clone());
                false // Remove from recently_committed
            } else {
                true // Keep in recently_committed
            }
        });

        // Add to cleanup info
        for txn in to_cleanup {
            cleanup_info.add_transaction(txn);
        }
    }

    fn schedule_cleanup(&self, cleanup_info: CleanupInfo) {
        let _queue_lock = self.cleanup_queue_lock.lock().unwrap();
        let mut queue = self.cleanup_queue.lock().unwrap();
        queue.push_back(cleanup_info);
    }

    fn process_cleanup(&self) {
        let _cleanup_lock = self.cleanup_lock.lock().unwrap();

        // Get the next cleanup info from the queue
        let cleanup_info = {
            let _queue_lock = self.cleanup_queue_lock.lock().unwrap();
            let mut queue = self.cleanup_queue.lock().unwrap();
            queue.pop_front()
        };

        // Process the cleanup if we got one
        if let Some(info) = cleanup_info {
            info.cleanup();
        }
    }

    #[inline]
    pub fn lowest_active_id(&self) -> u64 {
        let watermarks = self.active_registry.watermarks();
        if watermarks.active_count == 0 {
            TRANSACTION_ID_START
        } else {
            watermarks.oldest_active_txn_id.into_raw()
        }
    }

    #[inline]
    pub fn lowest_active_start(&self) -> u64 {
        let watermarks = self.active_registry.watermarks();
        if watermarks.active_count == 0 {
            MAX_TRANSACTION_ID
        } else {
            watermarks.oldest_active_start_ts.into_raw()
        }
    }

    #[inline]
    pub fn last_commit(&self) -> u64 {
        self.published_commit_id()
    }

    #[inline]
    pub fn durable_commit_id(&self) -> u64 {
        self.commit_frontier.durable_commit_id().into_raw()
    }

    #[inline]
    pub fn published_commit_id(&self) -> u64 {
        self.commit_frontier.published_commit_id().into_raw()
    }

    pub fn wait_for_published_commit_id_at_least(&self, floor: u64) -> Result<()> {
        self.commit_frontier
            .wait_for_published_at_least(CommitTs::new(floor))
            .map_err(|error| paro_error::internal(error.to_string()))
    }

    #[inline]
    pub fn commit_frontier(&self) -> Arc<CommitFrontier> {
        Arc::clone(&self.commit_frontier)
    }

    #[inline]
    pub fn commit_sequencer(&self) -> Arc<CommitSequencer> {
        Arc::clone(&self.commit_sequencer)
    }

    #[inline]
    pub fn commit_sequencer_metrics(&self) -> CommitSequencerMetrics {
        self.commit_sequencer.metrics_snapshot()
    }

    #[inline]
    pub fn commit_backpressure_controller(&self) -> Arc<CommitBackpressureController> {
        Arc::clone(&self.commit_backpressure)
    }

    #[inline]
    pub fn commit_backpressure_snapshot(&self) -> CommitBackpressureSnapshot {
        self.commit_backpressure.snapshot()
    }

    #[inline]
    pub fn sync_commit_backpressure_frontiers(&self, durable_ts: CommitTs, published_ts: CommitTs) {
        self.commit_backpressure
            .sync_frontiers(durable_ts, published_ts);
    }

    #[inline]
    pub fn active_registry(&self) -> &ActiveTxnRegistry {
        &self.active_registry
    }

    #[inline]
    pub fn retention_registry(&self) -> &RetentionRegistry {
        &self.retention_registry
    }

    #[inline]
    pub fn lock_manager(&self) -> Arc<ShardedLockManager> {
        Arc::clone(&self.lock_manager)
    }

    #[inline]
    pub fn write_conflict_index(&self) -> &WriteConflictIndex {
        &self.write_conflict_index
    }

    #[inline]
    pub fn read_dependency_index(&self) -> &Arc<ReadDependencyIndex> {
        &self.read_dependency_index
    }

    #[inline]
    pub fn committed_txn_summaries(&self) -> &CommittedTxnSummaryIndex {
        &self.committed_txn_summaries
    }

    pub fn commit_finalize_reservation_factory(
        self: &Arc<Self>,
    ) -> CommitFinalizeReservationFactory {
        let manager = Arc::clone(self);
        Arc::new(
            move |commit_ts: CommitTs, input: &CommitFinalizeReservationInput| {
                let registration_manager = Arc::clone(&manager);
                let input = input.clone();
                CommitFinalizeReservation::new(
                    WriteConflictReservation::default(),
                    SummaryReservation::default(),
                    move || {
                        registration_manager
                            .register_committed_transaction_summary_inner(
                                commit_ts,
                                input.txn_id,
                                input.read_ts,
                                &input.write_set,
                                &input.frozen_read_set,
                                false,
                            )
                            .expect("commit finalize reservation registration must be infallible");
                    },
                    || {},
                )
            },
        )
    }

    pub fn commit_final_fence(self: &Arc<Self>) -> CommitFinalFence {
        let manager = Arc::clone(self);
        Arc::new(move |plan, _in_flight| manager.ssi_final_fence_reason(plan))
    }

    pub fn cleanup_backpressure_snapshot(&self) -> CleanupBackpressureSnapshot {
        CleanupBackpressureSnapshot {
            depth: self.pending_cleanup_count(),
            bytes: 0,
            reserved_slots_available: usize::MAX,
        }
    }

    #[inline]
    pub fn serializable_read_tracker(&self, txn_id: TxnId, read_ts: ReadTs) -> ReadTrackerHandle {
        ReadTrackerHandle::serializable(Arc::clone(&self.read_dependency_index), txn_id, read_ts)
    }

    #[inline]
    pub fn serializable_read_tracker_with_policy(
        &self,
        txn_id: TxnId,
        read_ts: ReadTs,
        policy: ReadTrackingPolicy,
    ) -> ReadTrackerHandle {
        ReadTrackerHandle::serializable_with_policy(
            Arc::clone(&self.read_dependency_index),
            txn_id,
            read_ts,
            policy,
        )
    }

    #[inline]
    pub fn is_safe_snapshot(&self, read_ts: ReadTs) -> bool {
        read_ts < self.active_registry.watermarks().oldest_active_rw_start_ts
    }

    pub fn read_tracker_for_policy(
        &self,
        txn_id: TxnId,
        read_ts: ReadTs,
        policy: ReadTrackingPolicy,
    ) -> ReadTrackerHandle {
        match policy {
            ReadTrackingPolicy::Noop => ReadTrackerHandle::noop(),
            ReadTrackingPolicy::Record => ReadTrackerHandle::recording(),
            ReadTrackingPolicy::SafeSnapshot => ReadTrackerHandle::safe_snapshot(),
            ReadTrackingPolicy::SafeSnapshotPreferred if self.is_safe_snapshot(read_ts) => {
                ReadTrackerHandle::safe_snapshot()
            }
            ReadTrackingPolicy::SafeSnapshotPreferred => self
                .serializable_read_tracker_with_policy(
                    txn_id,
                    read_ts,
                    ReadTrackingPolicy::RangeCritical,
                ),
            other => self.serializable_read_tracker_with_policy(txn_id, read_ts, other),
        }
    }

    #[inline]
    pub fn lock_namespace(&self) -> LockNamespace {
        self.lock_namespace
    }

    pub fn registry_metrics_snapshot(&self) -> TransactionManagerMetricsSnapshot {
        let published_commit_id = self.published_commit_id();
        let active = self.active_registry.watermarks();
        let retention = self.retention_registry.watermarks();
        let read_snapshot_lease_count = retention.lease_count(RetentionLeaseKind::ReadSnapshot);
        let conflict = self.write_conflict_index.stats();
        let lock = self.lock_manager.stats();
        let read_dependency = self.read_dependency_index.stats();
        let ssi_abort_due_to_exact_dependency = self
            .ssi_abort_due_to_exact_dependency
            .load(Ordering::Acquire);
        let ssi_abort_due_to_coarse_scan_marker = self
            .ssi_abort_due_to_coarse_scan_marker
            .load(Ordering::Acquire);

        TransactionManagerMetricsSnapshot {
            txn_begin_count: self.txn_begin_count.load(Ordering::Acquire),
            txn_begin_latency_us_total: self.txn_begin_latency_us_total.load(Ordering::Acquire),
            txn_begin_latency_us_peak: self.txn_begin_latency_us_peak.load(Ordering::Acquire),
            txn_commit_count: self.txn_commit_count.load(Ordering::Acquire),
            txn_commit_latency_us_total: self.txn_commit_latency_us_total.load(Ordering::Acquire),
            txn_commit_latency_us_peak: self.txn_commit_latency_us_peak.load(Ordering::Acquire),
            txn_commit_prepare_latency_us_total: self
                .txn_commit_prepare_latency_us_total
                .load(Ordering::Acquire),
            txn_commit_prepare_latency_us_peak: self
                .txn_commit_prepare_latency_us_peak
                .load(Ordering::Acquire),
            txn_commit_validate_latency_us_total: self
                .txn_commit_validate_latency_us_total
                .load(Ordering::Acquire),
            txn_commit_validate_latency_us_peak: self
                .txn_commit_validate_latency_us_peak
                .load(Ordering::Acquire),
            txn_commit_durable_latency_us_total: self
                .txn_commit_durable_latency_us_total
                .load(Ordering::Acquire),
            txn_commit_durable_latency_us_peak: self
                .txn_commit_durable_latency_us_peak
                .load(Ordering::Acquire),
            txn_commit_required_publish_wait_us_total: self
                .txn_commit_required_publish_wait_us_total
                .load(Ordering::Acquire),
            txn_commit_required_publish_wait_us_peak: self
                .txn_commit_required_publish_wait_us_peak
                .load(Ordering::Acquire),
            txn_commit_publish_latency_us_total: self
                .txn_commit_publish_latency_us_total
                .load(Ordering::Acquire),
            txn_commit_publish_latency_us_peak: self
                .txn_commit_publish_latency_us_peak
                .load(Ordering::Acquire),
            txn_commit_ack_mode_last: self.txn_commit_ack_mode_last.load(Ordering::Acquire),
            write_conflict_index_size: conflict.entry_count as u64,
            write_conflict_index_fine_entries: conflict.fine_entry_count as u64,
            write_conflict_index_fine_summary_entries: conflict.fine_summary_entry_count as u64,
            write_conflict_index_coarse_entries: conflict.coarse_entry_count as u64,
            lock_wait_count: lock.lock_wait_count,
            lock_wait_duration_us: lock.lock_wait_duration_us,
            lock_wound_wait_abort_count: lock.lock_wound_wait_abort_count,
            lock_deadlock_abort_count: lock.lock_deadlock_abort_count,
            read_snapshot_lease_count,
            active_txn_count: active.active_count,
            active_rw_txn_count: active.active_rw_count,
            oldest_active_rw_lag_ms: lag_from_watermark(
                published_commit_id,
                active.oldest_active_rw_read_ts.into_raw(),
                active.active_rw_count,
            ),
            retention_watermark_lag_ms: lag_from_watermark(
                published_commit_id,
                retention.oldest_read_ts.into_raw(),
                read_snapshot_lease_count,
            ),
            active_registry_epoch: active.epoch,
            retention_registry_epoch: retention.epoch,
            ssi_validation_abort_count: ssi_abort_due_to_exact_dependency
                .saturating_add(ssi_abort_due_to_coarse_scan_marker),
            ssi_abort_due_to_exact_dependency,
            ssi_abort_due_to_coarse_scan_marker,
            read_tracker_record_count: read_dependency.record_count,
            read_tracker_coarsened_count: read_dependency.coarsen_count,
            read_tracking_hint_count: self.read_tracking_hint_count.load(Ordering::Acquire),
            read_tracking_policy_escalation_count: self
                .read_tracking_policy_escalation_count
                .load(Ordering::Acquire),
            read_tracking_point_critical_count: self
                .read_tracking_point_critical_count
                .load(Ordering::Acquire),
            read_tracking_range_critical_count: self
                .read_tracking_range_critical_count
                .load(Ordering::Acquire),
            read_tracking_analytical_scan_count: self
                .read_tracking_analytical_scan_count
                .load(Ordering::Acquire),
            read_tracking_safe_snapshot_preferred_count: self
                .read_tracking_safe_snapshot_preferred_count
                .load(Ordering::Acquire),
        }
    }

    /// Align the global commit clock with an externally observed committed version.
    ///
    /// This is used to ensure `commit_id` stays monotonic and does not overlap with
    /// persisted Tablet versions loaded from disk or recovery.
    pub fn sync_commit_id_with(&self, min_committed_version: u64) {
        let commit_ts = CommitTs::new(min_committed_version);
        self.commit_sequencer.sync_next_commit_ts_with(commit_ts);
        self.commit_frontier.sync_commit_ids(commit_ts, commit_ts);
    }

    /// Get the minimum start time among all active transactions.
    /// Transactions older than this are safe to clean up if they are committed.
    pub fn get_min_active_start_time(&self) -> u64 {
        let watermarks = self.active_registry.watermarks();
        if watermarks.active_count == 0 {
            self.published_commit_id().saturating_add(1)
        } else {
            watermarks.oldest_active_start_ts.into_raw()
        }
    }

    /// Check if there are other active transactions besides the given one.
    ///
    /// This is not a hot-path operation; when only one transaction is active it
    /// scans slots to distinguish "self" from "another txn".
    pub fn has_other_transactions(&self, transaction_id: u64) -> bool {
        let watermarks = self.active_registry.watermarks();
        watermarks.active_count > 1
            || (watermarks.active_count == 1
                && !self
                    .active_registry
                    .contains_transaction(TxnId::new(transaction_id)))
    }

    /// Get the number of active transactions.
    pub fn active_transaction_count(&self) -> usize {
        self.active_registry.watermarks().active_count as usize
    }

    /// Get the number of recently committed transactions pending cleanup.
    pub fn committed_transaction_count(&self) -> usize {
        self.recently_committed_transactions.read().unwrap().len()
    }

    /// Perform garbage collection on committed transactions.
    ///
    /// Transactions with commit_id < lowest_active_start can be cleaned up
    /// because no active transaction needs to see their old data.
    pub fn cleanup(&self) {
        let lowest_start = self.lowest_active_start();

        // Create cleanup info for eligible transactions
        let mut cleanup_info = CleanupInfo::new(lowest_start);
        self.move_committed_to_cleanup(&mut cleanup_info, lowest_start);

        // Schedule and process cleanup
        if cleanup_info.should_schedule() {
            self.schedule_cleanup(cleanup_info);
        }
        self.process_cleanup();
    }

    /// Get the number of pending cleanups in the queue.
    pub fn pending_cleanup_count(&self) -> usize {
        let _queue_lock = self.cleanup_queue_lock.lock().unwrap();
        let queue = self.cleanup_queue.lock().unwrap();
        queue.len()
    }

    /// Force process all pending cleanups.
    /// This is useful for testing or shutdown scenarios.
    pub fn flush_cleanups(&self) {
        loop {
            let _cleanup_lock = self.cleanup_lock.lock().unwrap();

            let cleanup_info = {
                let _queue_lock = self.cleanup_queue_lock.lock().unwrap();
                let mut queue = self.cleanup_queue.lock().unwrap();
                queue.pop_front()
            };

            match cleanup_info {
                Some(info) => info.cleanup(),
                None => break,
            }
        }
    }
}

fn lag_from_watermark(published_commit_id: u64, watermark: u64, holder_count: u64) -> u64 {
    if holder_count == 0 || watermark == MAX_TRANSACTION_ID {
        0
    } else {
        published_commit_id.saturating_sub(watermark)
    }
}

fn observe_counted_latency(
    count: &AtomicU64,
    total: &AtomicU64,
    peak: &AtomicU64,
    duration: Duration,
) {
    count.fetch_add(1, Ordering::Relaxed);
    observe_latency(total, peak, duration);
}

fn observe_latency(total: &AtomicU64, peak: &AtomicU64, duration: Duration) {
    let micros = duration_micros(duration);
    total.fetch_add(micros, Ordering::Relaxed);
    let mut current = peak.load(Ordering::Relaxed);
    while micros > current {
        match peak.compare_exchange_weak(current, micros, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

fn duration_micros(duration: Duration) -> u64 {
    duration.as_micros().min(u64::MAX as u128) as u64
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mark_transaction_changed(txn: &Transaction) {
        let mut buffer = txn.undo_buffer.lock().expect("lock undo buffer");
        buffer.push_insert(1, 0, 1);
    }

    fn table_resource(tm: &TransactionManager, table_id: u64) -> LockResource {
        LockResource::Table {
            namespace: tm.lock_namespace(),
            table_id: paro_transaction::TableId::new(table_id),
        }
    }

    // ==================== Basic Transaction Tests ====================

    #[test]
    fn test_begin_transaction() {
        let tm = TransactionManager::new();
        let t1 = tm.begin_transaction().expect("failed to start t1");
        let t2 = tm.begin_transaction().expect("failed to start t2");

        // Transaction IDs start at TRANSACTION_ID_START
        assert_eq!(t1.id, TRANSACTION_ID_START);
        assert_eq!(t2.id, TRANSACTION_ID_START + 1);

        // Start times reuse the same snapshot until a commit advances last_commit.
        assert_eq!(t1.start_time, 1);
        assert_eq!(t2.start_time, 1);

        // lowest_active_start should be t1's start_time
        assert_eq!(tm.lowest_active_start(), 1);
        assert_eq!(tm.lowest_active_id(), TRANSACTION_ID_START);
    }

    #[test]
    fn test_commit_rollback() {
        let tm = TransactionManager::new();
        let t1 = tm.begin_transaction().unwrap();
        let t2 = tm.begin_transaction().unwrap();

        // Both active transactions share the same snapshot.
        assert_eq!(tm.lowest_active_start(), 1);

        // Commit t1
        let commit_id = tm.commit_transaction(t1).unwrap();
        assert!(commit_id > 0);
        assert_eq!(tm.last_commit(), commit_id);

        // After t1 commits, t2 still holds the original snapshot.
        assert_eq!(tm.lowest_active_start(), 1);

        // Rollback t2
        tm.rollback_transaction(t2).unwrap();

        // After t2 rollback, no active transactions
        // lowest_active_start should be MAX (no active transactions)
        assert_eq!(tm.lowest_active_start(), MAX_TRANSACTION_ID);
        assert_eq!(tm.lowest_active_id(), TRANSACTION_ID_START);
    }

    #[test]
    fn committed_summary_registration_tracks_read_and_write_sets() {
        let tm = TransactionManager::new_for_database_id(7);
        let read_set =
            FrozenReadSet::from_dependencies(vec![paro_transaction::ReadDependency::Row {
                table_id: paro_transaction::TableId::new(10),
                row_id: 42,
            }]);
        tm.register_committed_transaction_summary(
            CommitTs::new(5),
            TxnId::new(100),
            ReadTs::new(3),
            &[table_resource(&tm, 10)],
            &read_set,
        )
        .unwrap();

        let stats = tm.committed_txn_summaries().stats();
        assert_eq!(stats.summary_count, 1);
        assert_eq!(stats.write_dependency_count, 1);
        assert_eq!(stats.read_dependency_count, 1);
        assert!(tm.write_conflict_index().has_conflict(
            ReadTs::new(4),
            [ConflictWrite::new(table_resource(&tm, 10))]
        ));

        assert_eq!(
            tm.advance_conflict_horizon(CommitTs::new(5)),
            CommitTs::new(5)
        );
        assert_eq!(tm.committed_txn_summaries().stats().summary_count, 0);
        assert_eq!(tm.write_conflict_index().stats().entry_count, 0);
    }

    #[test]
    fn ssi_final_fence_rejects_epoch_advanced_after_validation() {
        let tm = TransactionManager::new_for_database_id(7);
        let txn_id = TxnId::new(100);
        let read_ts = ReadTs::new(5);
        let tracker = tm.serializable_read_tracker(txn_id, read_ts);
        tracker.record_table_read(paro_transaction::TableId::new(10));
        let lock = paro_transaction::LockRequest::new(
            table_resource(&tm, 11),
            paro_transaction::LockMode::X,
        );
        let view = paro_transaction::TransactionView::new(
            paro_transaction::WriterId::new(100),
            read_ts,
            paro_transaction::ReadSnapshot::without_lease(read_ts),
            IsolationLevel::Serializable,
            paro_transaction::CommandId::new(0),
            tracker,
            paro_transaction::ParticipantStateSet::empty(),
        );
        let request = paro_transaction::CommitRequest::new(
            DatabaseId::new(7),
            txn_id,
            view,
            paro_transaction::CommitAckPolicy::RequiredPublished,
            paro_transaction::FrozenLockSet::from_locks(vec![lock]),
            Vec::new(),
        );
        let mut sequencing_plan = CommitSequencingPlan::from_commit_plan(request.commit_plan());
        let outcome = tm
            .validate_serializable_commit(&sequencing_plan.plan, &sequencing_plan.write_set)
            .unwrap();
        sequencing_plan = sequencing_plan
            .with_validation_epoch(outcome.validation_epoch)
            .with_ssi_effect_epoch(outcome.ssi_effect_epoch);

        assert!(tm.ssi_final_fence_reason(&sequencing_plan).is_none());

        tm.read_dependency_index().mark_txn_conflict_out(txn_id);
        assert!(matches!(
            tm.ssi_final_fence_reason(&sequencing_plan),
            Some(CommitFenceRejectReason::SsiStateEpochAdvanced {
                validation_epoch,
                current_epoch,
            }) if validation_epoch == outcome.validation_epoch
                && current_epoch > validation_epoch
        ));
    }

    #[test]
    fn test_cleanup() {
        let tm = TransactionManager::new();

        // 1. Start t1, t2
        let t1 = tm.begin_transaction().unwrap();
        let t2 = tm.begin_transaction().unwrap();

        assert_eq!(t1.start_time, 1);
        assert_eq!(t2.start_time, 1);

        // Make t1 have changes so it gets stored in recently_committed
        mark_transaction_changed(&t1);

        // 2. Commit t1
        let commit_id = tm.commit_transaction(t1).unwrap();

        // At this point, lowest_active_start is still the shared snapshot (1).
        // t1's commit_id is 1. Since 1 >= 1, t1 should NOT be cleaned up yet.
        // Note: commit_transaction already processes one cleanup, but t1 stays
        // in recently_committed because its commit_id >= lowest_active_start
        assert_eq!(tm.committed_transaction_count(), 1);

        // 3. Start t3
        let t3 = tm.begin_transaction().unwrap();

        // Make t3 have changes
        mark_transaction_changed(&t3);

        // 4. Commit t3
        tm.commit_transaction(t3).unwrap();

        // t3's commit_id is 2. lowest_active_start is still 1 (t2 is still active).
        // Both committed transactions have commit_id >= 1, so neither should be cleaned up.
        assert_eq!(tm.committed_transaction_count(), 2);

        // Verify last_commit was updated
        assert!(tm.last_commit() > commit_id);

        // 5. Commit t2 - now all transactions are done
        tm.commit_transaction(t2).unwrap();

        // After t2 commits, lowest_active_start becomes MAX, so all committed transactions
        // should be eligible for cleanup.
        assert_eq!(tm.lowest_active_start(), MAX_TRANSACTION_ID);

        // Call cleanup to process remaining committed transactions
        tm.cleanup();
        assert_eq!(tm.committed_transaction_count(), 0);
    }

    #[test]
    fn test_lowest_active_tracking_empty() {
        let tm = TransactionManager::new();

        // No active transactions
        assert_eq!(tm.lowest_active_id(), TRANSACTION_ID_START);
        assert_eq!(tm.lowest_active_start(), MAX_TRANSACTION_ID);
        assert_eq!(tm.last_commit(), 0);
    }

    #[test]
    fn test_lowest_active_tracking_single_transaction() {
        let tm = TransactionManager::new();

        let t1 = tm.begin_transaction().unwrap();

        // Single transaction sets lowest values.
        assert_eq!(tm.lowest_active_start(), t1.start_time);
        assert_eq!(tm.lowest_active_id(), t1.id);

        // Commit t1
        tm.commit_transaction(t1).unwrap();

        // No active transactions - reset to initial values
        assert_eq!(tm.lowest_active_start(), MAX_TRANSACTION_ID);
        assert_eq!(tm.lowest_active_id(), TRANSACTION_ID_START);
    }

    #[test]
    fn test_lowest_active_tracking_multiple_transactions() {
        let tm = TransactionManager::new();

        let t1 = tm.begin_transaction().unwrap();
        let t2 = tm.begin_transaction().unwrap();
        let t3 = tm.begin_transaction().unwrap();

        // All three transactions share the same snapshot, so the first one wins the tie.
        assert_eq!(tm.lowest_active_start(), t1.start_time);
        assert_eq!(tm.lowest_active_id(), t1.id);

        // Commit t1 - lifecycle counts update immediately while watermarks
        // remain conservative until a registry refresh raises them.
        tm.commit_transaction(t1).unwrap();
        assert_eq!(tm.active_transaction_count(), 2);
        assert_eq!(tm.lowest_active_start(), t2.start_time);
        assert!(tm.lowest_active_id() <= t2.id);

        // Rollback t2 - count is exact; lowest id can still be stale-low.
        tm.rollback_transaction(t2).unwrap();
        assert_eq!(tm.active_transaction_count(), 1);
        assert_eq!(tm.lowest_active_start(), t3.start_time);
        assert!(tm.lowest_active_id() <= t3.id);

        // Commit t3 - no active transactions
        tm.commit_transaction(t3).unwrap();
        assert_eq!(tm.lowest_active_start(), MAX_TRANSACTION_ID);
        assert_eq!(tm.lowest_active_id(), TRANSACTION_ID_START);
    }

    #[test]
    fn test_last_commit_tracking() {
        let tm = TransactionManager::new();

        assert_eq!(tm.last_commit(), 0);

        let t1 = tm.begin_transaction().unwrap();
        let commit1 = tm.commit_transaction(t1).unwrap();
        assert_eq!(tm.last_commit(), commit1);

        let t2 = tm.begin_transaction().unwrap();
        let commit2 = tm.commit_transaction(t2).unwrap();
        assert_eq!(tm.last_commit(), commit2);
        assert!(commit2 > commit1);
    }

    #[test]
    fn test_sync_commit_id_with() {
        let tm = TransactionManager::new();

        tm.sync_commit_id_with(10);

        let txn = tm.begin_transaction().unwrap();
        assert!(txn.start_time >= 11);

        let commit_id = tm.commit_transaction(txn).unwrap();
        assert!(commit_id >= 11);
        assert!(tm.last_commit() >= 10);
    }

    #[test]
    fn sequencer_sync_does_not_skip_next_commit_id() {
        let tm = TransactionManager::new();
        tm.sync_commit_id_with(10);

        let txn = tm.begin_transaction().unwrap();
        let commit_id = tm.commit_transaction(txn).unwrap();
        assert_eq!(commit_id, 11);
    }

    #[test]
    fn test_durable_commit_does_not_advance_new_snapshot_frontier() {
        let tm = TransactionManager::new();

        tm.commit_frontier.sync_durable_commit_id(CommitTs::new(7));

        assert_eq!(tm.durable_commit_id(), 7);
        assert_eq!(tm.published_commit_id(), 0);

        let txn = tm.begin_transaction().unwrap();
        assert_eq!(txn.start_time, 1);
    }

    #[test]
    fn test_recovery_admission_blocks_transactions_until_durable_prefix_is_published() {
        let tm = TransactionManager::new();

        tm.commit_frontier.sync_durable_commit_id(CommitTs::new(7));
        tm.block_recovery_admission();
        assert_eq!(
            tm.recovery_admission_state(),
            RecoveryAdmissionState::Blocked
        );
        assert!(tm.begin_transaction().is_err());
        assert_eq!(tm.published_commit_id(), 0);

        tm.complete_recovery_admission(tm.durable_commit_id());
        assert_eq!(tm.recovery_admission_state(), RecoveryAdmissionState::Open);
        assert_eq!(tm.durable_commit_id(), 7);
        assert_eq!(tm.published_commit_id(), 7);
        assert_eq!(tm.begin_transaction().unwrap().start_time, 8);
    }

    #[test]
    fn test_has_other_transactions() {
        let tm = TransactionManager::new();

        let t1 = tm.begin_transaction().unwrap();
        assert!(!tm.has_other_transactions(t1.id));

        let t2 = tm.begin_transaction().unwrap();
        assert!(tm.has_other_transactions(t1.id));
        assert!(tm.has_other_transactions(t2.id));

        tm.commit_transaction(t1).unwrap();
        assert!(!tm.has_other_transactions(t2.id));
    }

    #[test]
    fn test_active_transaction_count() {
        let tm = TransactionManager::new();

        assert_eq!(tm.active_transaction_count(), 0);

        let t1 = tm.begin_transaction().unwrap();
        assert_eq!(tm.active_transaction_count(), 1);

        let t2 = tm.begin_transaction().unwrap();
        assert_eq!(tm.active_transaction_count(), 2);

        tm.commit_transaction(t1).unwrap();
        assert_eq!(tm.active_transaction_count(), 1);

        tm.rollback_transaction(t2).unwrap();
        assert_eq!(tm.active_transaction_count(), 0);
    }

    #[test]
    fn test_registry_metrics_track_active_read_write_transactions() {
        let tm = TransactionManager::new();
        let txn = tm.begin_transaction().unwrap();

        let metrics = tm.registry_metrics_snapshot();
        assert_eq!(metrics.active_txn_count, 1);
        assert_eq!(metrics.active_rw_txn_count, 0);

        txn.set_read_write();
        let metrics = tm.registry_metrics_snapshot();
        assert_eq!(metrics.active_txn_count, 1);
        assert_eq!(metrics.active_rw_txn_count, 1);
        assert_eq!(metrics.oldest_active_rw_lag_ms, 0);

        tm.commit_transaction(txn).unwrap();
        let metrics = tm.registry_metrics_snapshot();
        assert_eq!(metrics.active_txn_count, 0);
        assert_eq!(metrics.active_rw_txn_count, 0);
    }

    #[test]
    fn test_read_write_promotion_is_idempotent_for_registry_binding() {
        let tm = TransactionManager::new();
        let txn = tm.begin_transaction().unwrap();

        txn.set_read_write();
        txn.set_read_write();

        let metrics = tm.registry_metrics_snapshot();
        assert_eq!(metrics.active_txn_count, 1);
        assert_eq!(metrics.active_rw_txn_count, 1);

        tm.rollback_transaction(txn).unwrap();
        let metrics = tm.registry_metrics_snapshot();
        assert_eq!(metrics.active_txn_count, 0);
        assert_eq!(metrics.active_rw_txn_count, 0);
    }

    #[test]
    fn test_registry_metrics_track_read_snapshot_leases() {
        let tm = TransactionManager::new();
        tm.sync_commit_id_with(10);
        let lease = tm
            .retention_registry()
            .lease_read_snapshot(ReadTs::new(4))
            .unwrap();

        let metrics = tm.registry_metrics_snapshot();
        assert_eq!(metrics.read_snapshot_lease_count, 1);
        assert_eq!(metrics.retention_watermark_lag_ms, 6);

        drop(lease);
        let metrics = tm.registry_metrics_snapshot();
        assert_eq!(metrics.read_snapshot_lease_count, 0);
        assert_eq!(metrics.retention_watermark_lag_ms, 0);
    }

    #[test]
    fn test_registry_metrics_track_read_tracking_policy_selection() {
        let tm = TransactionManager::new();

        tm.record_read_tracking_selection(ReadTrackingPolicy::RangeCritical, true, true);
        tm.record_read_tracking_selection(ReadTrackingPolicy::AnalyticalScan, false, false);

        let metrics = tm.registry_metrics_snapshot();
        assert_eq!(metrics.read_tracking_hint_count, 1);
        assert_eq!(metrics.read_tracking_policy_escalation_count, 1);
        assert_eq!(metrics.read_tracking_range_critical_count, 1);
        assert_eq!(metrics.read_tracking_analytical_scan_count, 1);
        assert_eq!(metrics.read_tracking_point_critical_count, 0);
    }

    #[test]
    fn safe_snapshot_preferred_falls_back_to_exact_tracker_when_unsafe() {
        let tm = TransactionManager::new();
        let _active_rw = tm
            .active_registry()
            .register_read_write(TxnId::new(10), ReadTs::new(1), ReadTs::new(1))
            .unwrap();

        let tracker = tm.read_tracker_for_policy(
            TxnId::new(11),
            ReadTs::new(2),
            ReadTrackingPolicy::SafeSnapshotPreferred,
        );

        assert_eq!(tracker.policy(), ReadTrackingPolicy::RangeCritical);
    }

    #[test]
    fn test_registry_metrics_track_lock_rejections() {
        let tm = TransactionManager::new();
        let lock_manager = tm.lock_manager();
        let resource = LockResource::Table {
            namespace: tm.lock_namespace(),
            table_id: paro_transaction::TableId::new(42),
        };
        let _owner = lock_manager
            .lock_one(
                TxnId::new(10),
                resource.clone(),
                paro_transaction::LockMode::X,
            )
            .unwrap();

        let _ = lock_manager
            .lock_one(
                TxnId::new(11),
                resource.clone(),
                paro_transaction::LockMode::X,
            )
            .unwrap_err();
        let metrics = tm.registry_metrics_snapshot();
        assert_eq!(metrics.lock_wait_count, 1);
        assert_eq!(metrics.lock_wound_wait_abort_count, 0);

        let _ = lock_manager
            .lock_one(TxnId::new(9), resource, paro_transaction::LockMode::X)
            .unwrap_err();
        let metrics = tm.registry_metrics_snapshot();
        assert_eq!(metrics.lock_wait_count, 1);
        assert_eq!(metrics.lock_wound_wait_abort_count, 1);
        assert_eq!(metrics.lock_deadlock_abort_count, 0);
    }

    #[test]
    fn test_get_min_active_start_time_compatibility() {
        let tm = TransactionManager::new();

        // When no active transactions, returns the next snapshot upper bound.
        let min1 = tm.get_min_active_start_time();
        assert_eq!(min1, 1);

        let t1 = tm.begin_transaction().unwrap();
        assert_eq!(tm.get_min_active_start_time(), t1.start_time);

        let _t2 = tm.begin_transaction().unwrap();
        // Still t1's start_time (the minimum)
        assert_eq!(tm.get_min_active_start_time(), t1.start_time);

        // Commit all - should return the next snapshot upper bound again.
        tm.commit_transaction(t1).unwrap();
        tm.commit_transaction(_t2).unwrap();
        assert_eq!(
            tm.get_min_active_start_time(),
            tm.last_commit().saturating_add(1)
        );
    }

    // ==================== Error/Edge Case Tests ====================

    #[test]
    fn test_rollback_does_not_update_last_commit() {
        let tm = TransactionManager::new();

        let t1 = tm.begin_transaction().unwrap();
        let commit1 = tm.commit_transaction(t1).unwrap();

        let t2 = tm.begin_transaction().unwrap();
        tm.rollback_transaction(t2).unwrap();

        // last_commit should still be commit1
        assert_eq!(tm.last_commit(), commit1);
    }

    #[test]
    fn test_committed_transaction_count() {
        let tm = TransactionManager::new();

        assert_eq!(tm.committed_transaction_count(), 0);

        // Start a "blocker" transaction to prevent immediate cleanup
        let blocker = tm.begin_transaction().unwrap();

        let t1 = tm.begin_transaction().unwrap();
        // Make t1 have changes so it gets stored in recently_committed
        mark_transaction_changed(&t1);
        tm.commit_transaction(t1).unwrap();
        // t1 stays in recently_committed because blocker is still active
        assert_eq!(tm.committed_transaction_count(), 1);

        let t2 = tm.begin_transaction().unwrap();
        // Make t2 have changes
        mark_transaction_changed(&t2);
        tm.commit_transaction(t2).unwrap();
        // t2 also stays in recently_committed
        assert_eq!(tm.committed_transaction_count(), 2);

        // Commit the blocker - now all transactions can be cleaned up
        tm.commit_transaction(blocker).unwrap();

        // After blocker commits, lowest_active_start becomes MAX_TRANSACTION_ID
        // which is much larger than any commit_id, so cleanup should remove them
        tm.cleanup();
        assert_eq!(tm.committed_transaction_count(), 0);
    }

    #[test]
    fn test_commit_ids_follow_commit_order_under_shared_snapshots() {
        let tm = TransactionManager::new();

        let t1 = tm.begin_transaction().unwrap();
        let t2 = tm.begin_transaction().unwrap();
        assert_eq!(t1.start_time, t2.start_time);

        let commit2 = tm.commit_transaction(t2).unwrap();
        let commit1 = tm.commit_transaction(t1).unwrap();

        assert_eq!(commit2, 1);
        assert_eq!(commit1, 2);
        assert_eq!(tm.last_commit(), 2);
        assert_eq!(tm.get_min_active_start_time(), 3);
    }

    #[test]
    fn test_cleanup_info_new() {
        let info = CleanupInfo::new(100);
        assert_eq!(info.lowest_start_time, 100);
        assert!(info.transactions.is_empty());
        assert!(!info.should_schedule());
    }

    #[test]
    fn test_cleanup_info_add_transaction() {
        let mut info = CleanupInfo::new(100);
        let txn = Arc::new(Transaction::new(1, 50));

        info.add_transaction(txn);

        assert_eq!(info.transactions.len(), 1);
        assert!(info.should_schedule());
    }

    #[test]
    fn test_cleanup_info_cleanup_calls_transaction_cleanup() {
        let mut info = CleanupInfo::new(100);
        let txn = Arc::new(Transaction::new(1, 50));
        txn.set_awaiting_cleanup(true);
        mark_transaction_changed(&txn); // Add some entries

        info.add_transaction(txn.clone());

        // Before cleanup, transaction has changes
        assert!(txn.changes_made());

        // Cleanup should call transaction.cleanup()
        info.cleanup();

        // After cleanup, undo buffer should be cleared
        assert!(!txn.changes_made());
    }

    #[test]
    fn test_cleanup_info_skips_non_awaiting_transactions() {
        let mut info = CleanupInfo::new(100);
        let txn = Arc::new(Transaction::new(1, 50));
        // NOT marked as awaiting_cleanup
        mark_transaction_changed(&txn);

        info.add_transaction(txn.clone());

        // Cleanup should skip this transaction
        info.cleanup();

        // Transaction still has changes (not cleaned up)
        assert!(txn.changes_made());
    }

    #[test]
    fn test_pending_cleanup_count() {
        let tm = TransactionManager::new();

        assert_eq!(tm.pending_cleanup_count(), 0);

        // The cleanup queue is processed immediately during commit/rollback,
        // so pending_cleanup_count should typically be 0 after operations
        let t1 = tm.begin_transaction().unwrap();
        tm.commit_transaction(t1).unwrap();

        // Cleanup was processed during commit
        assert_eq!(tm.pending_cleanup_count(), 0);
    }

    #[test]
    fn test_flush_cleanups_processes_all() {
        let tm = TransactionManager::new();

        // Start multiple transactions
        let t1 = tm.begin_transaction().unwrap();
        let t2 = tm.begin_transaction().unwrap();
        let t3 = tm.begin_transaction().unwrap();

        // Make changes
        mark_transaction_changed(&t1);
        mark_transaction_changed(&t2);
        mark_transaction_changed(&t3);

        // Commit all
        tm.commit_transaction(t1).unwrap();
        tm.commit_transaction(t2).unwrap();
        tm.commit_transaction(t3).unwrap();

        // Flush any remaining cleanups
        tm.flush_cleanups();

        // All cleanups should be processed
        assert_eq!(tm.pending_cleanup_count(), 0);
    }

    #[test]
    fn test_finish_transaction_creates_cleanup_info() {
        let tm = TransactionManager::new();

        // Start a blocker to prevent immediate cleanup
        let blocker = tm.begin_transaction().unwrap();

        let t1 = tm.begin_transaction().unwrap();
        mark_transaction_changed(&t1);

        // Commit t1 - it should go to recently_committed
        tm.commit_transaction(t1).unwrap();
        assert_eq!(tm.committed_transaction_count(), 1);

        // Rollback blocker - t1 should now be eligible for cleanup
        tm.rollback_transaction(blocker).unwrap();

        // After blocker is gone, cleanup should process t1
        tm.cleanup();
        assert_eq!(tm.committed_transaction_count(), 0);
    }

    #[test]
    fn test_rollback_schedules_cleanup_for_changed_transaction() {
        let tm = TransactionManager::new();

        let t1 = tm.begin_transaction().unwrap();
        mark_transaction_changed(&t1);

        assert!(t1.changes_made());

        // Rollback should schedule cleanup
        tm.rollback_transaction(t1).unwrap();

        // Cleanup should have been processed
        assert_eq!(tm.pending_cleanup_count(), 0);
    }

    #[test]
    fn test_cleanup_order_preserved() {
        // Test that cleanups happen in order (important for catalog consistency)
        let tm = TransactionManager::new();

        // Start a blocker
        let blocker = tm.begin_transaction().unwrap();

        // Create transactions in order
        let t1 = tm.begin_transaction().unwrap();
        let t2 = tm.begin_transaction().unwrap();
        let t3 = tm.begin_transaction().unwrap();

        // Make changes
        mark_transaction_changed(&t1);
        mark_transaction_changed(&t2);
        mark_transaction_changed(&t3);

        // Commit in order
        tm.commit_transaction(t1).unwrap();
        tm.commit_transaction(t2).unwrap();
        tm.commit_transaction(t3).unwrap();

        // All should be in recently_committed
        assert_eq!(tm.committed_transaction_count(), 3);

        // Commit blocker to allow cleanup
        tm.commit_transaction(blocker).unwrap();

        // Cleanup should process all
        tm.cleanup();
        assert_eq!(tm.committed_transaction_count(), 0);
    }

    #[test]
    fn test_transaction_cleanup_method() {
        let txn = Transaction::new(1, 100);
        mark_transaction_changed(&txn);

        assert!(txn.changes_made());

        // Call cleanup directly
        txn.cleanup(50);

        // Undo buffer should be cleared
        assert!(!txn.changes_made());
    }
}
