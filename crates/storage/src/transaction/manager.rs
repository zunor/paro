// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Transaction manager state and cleanup orchestration.

use crate::transaction::txn::Transaction;
use paro_common::error::Result;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// Starting value for transaction IDs (high value to distinguish from timestamps).
///
/// ```cpp
/// current_transaction_id = TRANSACTION_ID_START;
/// ```
pub const TRANSACTION_ID_START: u64 = 4611686018427387904; // 2^62

/// Maximum transaction ID (used as sentinel for "no transaction").
///
/// ```cpp
/// lowest_active_start = MAX_TRANSACTION_ID;
/// ```
pub const MAX_TRANSACTION_ID: u64 = u64::MAX;

/// Collects transactions awaiting cleanup.
///
/// This ensures we can clean up after releasing the transaction lock.
/// All transactions in a cleanup info share the same `lowest_start_time`.
///
/// ```cpp
/// struct DuckCleanupInfo {
///     transaction_t lowest_start_time;
///     void Cleanup() noexcept;
///     bool ScheduleCleanup() noexcept;
/// };
/// ```
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
    ///
    /// ```cpp
    /// void DuckCleanupInfo::Cleanup() noexcept {
    ///     for (auto &transaction : transactions) {
    ///         if (transaction->awaiting_cleanup) {
    ///             transaction->Cleanup(lowest_start_time);
    ///         }
    ///     }
    /// }
    /// ```
    pub fn cleanup(&self) {
        for transaction in &self.transactions {
            if transaction.is_awaiting_cleanup() {
                transaction.cleanup(self.lowest_start_time);
            }
        }
    }

    /// Check if there are transactions to clean up.
    ///
    /// ```cpp
    /// bool DuckCleanupInfo::ScheduleCleanup() noexcept {
    ///     return !transactions.empty();
    /// }
    /// ```
    #[inline]
    pub fn should_schedule(&self) -> bool {
        !self.transactions.is_empty()
    }

    /// Add a transaction to this cleanup info.
    pub fn add_transaction(&mut self, transaction: Arc<Transaction>) {
        self.transactions.push(transaction);
    }
}

/// Manages transactions within the database.
///
/// ```cpp
///     transaction_t next_commit_id;
///     transaction_t current_transaction_id;
///     atomic<transaction_t> lowest_active_id;
///     atomic<transaction_t> lowest_active_start;
///     atomic<transaction_t> last_commit;
/// };
/// ```
#[derive(Debug)]
pub struct TransactionManager {
    /// The next commit version to allocate inside the commit barrier.
    next_commit_id: AtomicU64,

    /// The current transaction ID for new transactions.
    /// Starts at TRANSACTION_ID_START (very high) to distinguish from timestamps.
    current_transaction_id: AtomicU64,

    /// The lowest active transaction ID among all active transactions.
    /// Used for determining which transactions can see which data.
    ///
    /// ```cpp
    /// atomic<transaction_t> lowest_active_id;
    /// ```
    lowest_active_id: AtomicU64,

    /// The lowest active start timestamp among all active transactions.
    /// Used for garbage collection decisions.
    ///
    /// ```cpp
    /// atomic<transaction_t> lowest_active_start;
    /// ```
    lowest_active_start: AtomicU64,

    /// The last commit timestamp.
    /// Updated after each successful commit.
    ///
    /// ```cpp
    /// atomic<transaction_t> last_commit;
    /// ```
    last_commit: AtomicU64,

    /// Serializes commit-version allocation with durable commit publication.
    commit_barrier: Mutex<()>,

    /// List of active transactions.
    active_transactions: RwLock<Vec<Arc<Transaction>>>,

    /// List of recently committed transactions, pending cleanup.
    /// Transactions are moved here after commit and removed during GC.
    recently_committed_transactions: RwLock<Vec<Arc<Transaction>>>,

    /// Lock for cleanup operations. Only one cleanup can be active at any time.
    ///
    /// ```cpp
    /// mutex cleanup_lock;
    /// ```
    cleanup_lock: Mutex<()>,

    /// Lock for cleanup queue modifications.
    ///
    /// ```cpp
    /// mutex cleanup_queue_lock;
    /// ```
    cleanup_queue_lock: Mutex<()>,

    /// Queue of cleanup infos. Cleanups must happen in-order.
    ///
    /// E.g., if one transaction drops a table, and another creates a table,
    /// inverting the cleanup order can result in catalog errors.
    ///
    /// ```cpp
    /// queue<unique_ptr<DuckCleanupInfo>> cleanup_queue;
    /// ```
    cleanup_queue: Mutex<VecDeque<CleanupInfo>>,
}

impl TransactionManager {
    /// Create a new transaction manager.
    ///
    /// ```cpp
    ///     next_commit_id = 1;
    ///     current_transaction_id = TRANSACTION_ID_START;
    ///     lowest_active_id = TRANSACTION_ID_START;
    ///     lowest_active_start = MAX_TRANSACTION_ID;
    /// }
    /// ```
    pub fn new() -> Self {
        Self {
            next_commit_id: AtomicU64::new(1),
            // Transaction ID starts very high to distinguish from timestamps
            current_transaction_id: AtomicU64::new(TRANSACTION_ID_START),
            // Initially no active transactions
            lowest_active_id: AtomicU64::new(TRANSACTION_ID_START),
            // MAX means no active transactions
            lowest_active_start: AtomicU64::new(MAX_TRANSACTION_ID),
            // No commits yet
            last_commit: AtomicU64::new(0),
            commit_barrier: Mutex::new(()),
            active_transactions: RwLock::new(Vec::new()),
            recently_committed_transactions: RwLock::new(Vec::new()),
            cleanup_lock: Mutex::new(()),
            cleanup_queue_lock: Mutex::new(()),
            cleanup_queue: Mutex::new(VecDeque::new()),
        }
    }

    /// Begin a new transaction.
    ///
    /// ```cpp
    ///     transaction_t start_time = last_commit + 1;
    ///     transaction_t transaction_id = current_transaction_id++;
    ///     if (active_transactions.empty()) {
    ///         lowest_active_start = start_time;
    ///         lowest_active_id = transaction_id;
    ///     }
    ///     active_transactions.push_back(std::move(transaction));
    /// }
    /// ```
    pub fn begin_transaction(&self) -> Result<Arc<Transaction>> {
        let start_time = self.last_commit.load(Ordering::SeqCst).saturating_add(1);
        let id = self.current_transaction_id.fetch_add(1, Ordering::SeqCst);

        let txn = Arc::new(Transaction::new(id, start_time));

        let mut active = self.active_transactions.write().unwrap();

        // Update lowest_active_* if this is the first active transaction
        if active.is_empty() {
            self.lowest_active_start.store(start_time, Ordering::SeqCst);
            self.lowest_active_id.store(id, Ordering::SeqCst);
        }

        active.push(txn.clone());

        Ok(txn)
    }

    /// Commit a transaction.
    ///
    /// ```cpp
    ///     CommitInfo info;
    ///     info.commit_id = GetCommitTimestamp();
    ///     error = transaction.Commit(db, info, ...);
    ///     last_commit = info.commit_id;
    ///     auto cleanup_info = RemoveTransaction(transaction, store_transaction);
    ///     if (cleanup_info->ScheduleCleanup()) {
    ///         lock_guard<mutex> q_lock(cleanup_queue_lock);
    ///         cleanup_queue.emplace(std::move(cleanup_info));
    ///     }
    /// }
    /// ```
    pub fn commit_transaction(&self, transaction: Arc<Transaction>) -> Result<u64> {
        let _barrier = self.enter_commit_barrier();
        let commit_id = self.allocate_commit_id();
        self.commit_transaction_with_commit_id(transaction, commit_id)?;
        Ok(commit_id)
    }

    pub fn enter_commit_barrier(&self) -> std::sync::MutexGuard<'_, ()> {
        self.commit_barrier.lock().unwrap()
    }

    pub fn allocate_commit_id(&self) -> u64 {
        self.next_commit_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn commit_transaction_with_commit_id(
        &self,
        transaction: Arc<Transaction>,
        commit_id: u64,
    ) -> Result<()> {
        transaction.commit(commit_id)?;
        self.last_commit.store(commit_id, Ordering::SeqCst);

        let store_transaction = transaction.changes_made();
        let cleanup_info = self.remove_transaction(&transaction, store_transaction);
        if cleanup_info.should_schedule() {
            self.schedule_cleanup(cleanup_info);
        }
        self.process_cleanup();
        Ok(())
    }

    /// Rollback a transaction.
    ///
    /// ```cpp
    ///     error = transaction.Rollback();
    ///     auto cleanup_info = RemoveTransaction(transaction);
    ///     if (cleanup_info->ScheduleCleanup()) {
    ///         lock_guard<mutex> q_lock(cleanup_queue_lock);
    ///         cleanup_queue.emplace(std::move(cleanup_info));
    ///     }
    /// }
    /// ```
    pub fn rollback_transaction(&self, transaction: Arc<Transaction>) -> Result<()> {
        // Execute rollback logic in the undo buffer
        transaction.rollback()?;

        // Remove from active transactions - always store if changes were made
        let store_transaction = transaction.changes_made();
        let cleanup_info = self.remove_transaction(&transaction, store_transaction);

        // Schedule cleanup if needed
        if cleanup_info.should_schedule() {
            self.schedule_cleanup(cleanup_info);
        }

        // Process any pending cleanups
        self.process_cleanup();

        Ok(())
    }

    /// Remove a transaction from the active list and create cleanup info.
    ///
    /// ```cpp
    ///     auto cleanup_info = make_uniq<DuckCleanupInfo>();
    ///     // Find transaction and compute lowest values
    ///     auto lowest_start_time = TRANSACTION_ID_START;
    ///     auto lowest_transaction_id = MAX_TRANSACTION_ID;
    ///     for (idx_t i = 0; i < active_transactions.size(); i++) {
    ///         if (active_transactions[i].get() == &transaction) continue;
    ///         lowest_start_time = MinValue(lowest_start_time, active_transactions[i]->start_time);
    ///         lowest_transaction_id = MinValue(lowest_transaction_id, active_transactions[i]->transaction_id);
    ///     }
    ///     lowest_active_start = lowest_start_time;
    ///     lowest_active_id = lowest_transaction_id;
    ///     // Handle transaction storage
    ///     if (store_transaction) {
    ///         if (transaction.commit_id != 0) {
    ///             recently_committed_transactions.push_back(std::move(current_transaction));
    ///         } else {
    ///             cleanup_info->transactions.push_back(std::move(current_transaction));
    ///         }
    ///     } else if (transaction.ChangesMade()) {
    ///         current_transaction->awaiting_cleanup = true;
    ///         cleanup_info->transactions.push_back(std::move(current_transaction));
    ///     }
    ///     cleanup_info->lowest_start_time = lowest_start_time;
    ///     // Move eligible recently_committed to cleanup
    ///     ...
    ///     return cleanup_info;
    /// }
    /// ```
    fn remove_transaction(
        &self,
        transaction: &Arc<Transaction>,
        store_transaction: bool,
    ) -> CleanupInfo {
        let mut active = self.active_transactions.write().unwrap();

        // Find and remove the transaction, while computing new lowest values
        let mut lowest_start_time = MAX_TRANSACTION_ID;
        let mut lowest_transaction_id = MAX_TRANSACTION_ID;
        let mut removed_transaction: Option<Arc<Transaction>> = None;

        active.retain(|t| {
            if Arc::ptr_eq(t, transaction) {
                removed_transaction = Some(t.clone());
                false // Remove this transaction
            } else {
                // Track minimum values among remaining transactions
                if t.start_time < lowest_start_time {
                    lowest_start_time = t.start_time;
                }
                if t.id < lowest_transaction_id {
                    lowest_transaction_id = t.id;
                }
                true // Keep this transaction
            }
        });

        // All remaining active transactions have been checked
        if active.is_empty() {
            lowest_start_time = MAX_TRANSACTION_ID;
            lowest_transaction_id = TRANSACTION_ID_START;
        }

        // Update atomic tracking variables
        self.lowest_active_start
            .store(lowest_start_time, Ordering::SeqCst);
        self.lowest_active_id
            .store(lowest_transaction_id, Ordering::SeqCst);

        // Create cleanup info
        let mut cleanup_info = CleanupInfo::new(lowest_start_time);

        // Handle the removed transaction
        if let Some(txn) = removed_transaction {
            let commit_id = *txn.commit_id.lock().unwrap();

            if store_transaction {
                if commit_id != 0 {
                    // Transaction was committed - add to recently_committed
                    let mut committed = self.recently_committed_transactions.write().unwrap();
                    committed.push(txn.clone());
                } else {
                    // Transaction was aborted - schedule for cleanup
                    cleanup_info.add_transaction(txn.clone());
                }
            } else if txn.changes_made() {
                // Not storing but has changes - schedule for cleanup
                txn.set_awaiting_cleanup(true);
                cleanup_info.add_transaction(txn.clone());
            }
        }

        // Move eligible recently_committed transactions to cleanup
        self.move_committed_to_cleanup(&mut cleanup_info, lowest_start_time);

        cleanup_info
    }

    /// Move recently committed transactions that are safe to clean up.
    ///
    /// ```cpp
    /// for (; i < recently_committed_transactions.size(); i++) {
    ///     if (recently_committed_transactions[i]->commit_id >= lowest_start_time) {
    ///         break;
    ///     }
    ///     recently_committed_transactions[i]->awaiting_cleanup = true;
    ///     cleanup_info->transactions.push_back(std::move(recently_committed_transactions[i]));
    /// }
    /// ```
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

    /// Schedule a cleanup info for processing.
    ///
    /// ```cpp
    /// if (cleanup_info->ScheduleCleanup()) {
    ///     lock_guard<mutex> q_lock(cleanup_queue_lock);
    ///     cleanup_queue.emplace(std::move(cleanup_info));
    /// }
    /// ```
    fn schedule_cleanup(&self, cleanup_info: CleanupInfo) {
        let _queue_lock = self.cleanup_queue_lock.lock().unwrap();
        let mut queue = self.cleanup_queue.lock().unwrap();
        queue.push_back(cleanup_info);
    }

    /// Process pending cleanups from the queue.
    ///
    /// ```cpp
    /// {
    ///     lock_guard<mutex> c_lock(cleanup_lock);
    ///     unique_ptr<DuckCleanupInfo> top_cleanup_info;
    ///     {
    ///         lock_guard<mutex> q_lock(cleanup_queue_lock);
    ///         if (!cleanup_queue.empty()) {
    ///             top_cleanup_info = std::move(cleanup_queue.front());
    ///             cleanup_queue.pop();
    ///         }
    ///     }
    ///     if (top_cleanup_info) {
    ///         top_cleanup_info->Cleanup();
    ///     }
    /// }
    /// ```
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

    /// Remove a transaction from the active list and update tracking variables.
    /// This is a simplified version that doesn't create cleanup info.
    ///
    /// # Deprecated
    /// Use `remove_transaction()` instead for proper cleanup handling.
    #[allow(dead_code)]
    fn remove_transaction_internal(&self, transaction_id: u64) {
        let mut active = self.active_transactions.write().unwrap();

        // Find and remove the transaction, while computing new lowest values
        let mut lowest_start_time = TRANSACTION_ID_START;
        let mut lowest_transaction_id = MAX_TRANSACTION_ID;

        active.retain(|t| {
            if t.id == transaction_id {
                false // Remove this transaction
            } else {
                // Track minimum values among remaining transactions
                if t.start_time < lowest_start_time {
                    lowest_start_time = t.start_time;
                }
                if t.id < lowest_transaction_id {
                    lowest_transaction_id = t.id;
                }
                true // Keep this transaction
            }
        });

        // Update atomic tracking variables
        self.lowest_active_start
            .store(lowest_start_time, Ordering::SeqCst);
        self.lowest_active_id
            .store(lowest_transaction_id, Ordering::SeqCst);
    }

    /// Get the lowest active transaction ID.
    ///
    /// ```cpp
    /// transaction_t LowestActiveId() const {
    ///     return lowest_active_id;
    /// }
    /// ```
    #[inline]
    pub fn lowest_active_id(&self) -> u64 {
        self.lowest_active_id.load(Ordering::SeqCst)
    }

    /// Get the lowest active start timestamp.
    /// Used for garbage collection decisions.
    ///
    /// ```cpp
    /// transaction_t LowestActiveStart() const {
    ///     return lowest_active_start;
    /// }
    /// ```
    #[inline]
    pub fn lowest_active_start(&self) -> u64 {
        self.lowest_active_start.load(Ordering::SeqCst)
    }

    /// Get the last commit timestamp.
    ///
    /// ```cpp
    /// transaction_t GetLastCommit() const {
    ///     return last_commit;
    /// }
    /// ```
    #[inline]
    pub fn last_commit(&self) -> u64 {
        self.last_commit.load(Ordering::SeqCst)
    }

    /// Align the global commit clock with an externally observed committed version.
    ///
    /// This is used to ensure `commit_id` stays monotonic and does not overlap with
    /// persisted Tablet versions loaded from disk or recovery.
    pub fn sync_commit_id_with(&self, min_committed_version: u64) {
        let next = min_committed_version.saturating_add(1);
        Self::bump_atomic_min(&self.next_commit_id, next);
        Self::bump_atomic_min(&self.last_commit, min_committed_version);
    }

    /// Get the minimum start time among all active transactions.
    /// This is an alias for `lowest_active_start()` for backward compatibility.
    ///
    /// Transactions older than this are safe to clean up if they are committed.
    pub fn get_min_active_start_time(&self) -> u64 {
        let lowest = self.lowest_active_start.load(Ordering::SeqCst);
        if lowest == MAX_TRANSACTION_ID {
            self.last_commit.load(Ordering::SeqCst).saturating_add(1)
        } else {
            lowest
        }
    }

    /// Check if there are other active transactions besides the given one.
    ///
    /// ```cpp
    ///     for (auto &active_transaction : active_transactions) {
    ///         if (!RefersToSameObject(*active_transaction, transaction)) {
    ///             return true;
    ///         }
    ///     }
    ///     return false;
    /// }
    /// ```
    pub fn has_other_transactions(&self, transaction_id: u64) -> bool {
        let active = self.active_transactions.read().unwrap();
        for t in active.iter() {
            if t.id != transaction_id {
                return true;
            }
        }
        false
    }

    /// Get the number of active transactions.
    pub fn active_transaction_count(&self) -> usize {
        self.active_transactions.read().unwrap().len()
    }

    /// Get the number of recently committed transactions pending cleanup.
    pub fn committed_transaction_count(&self) -> usize {
        self.recently_committed_transactions.read().unwrap().len()
    }

    fn bump_atomic_min(atomic: &AtomicU64, min_value: u64) -> u64 {
        loop {
            let current = atomic.load(Ordering::SeqCst);
            if current >= min_value {
                return current;
            }
            if atomic
                .compare_exchange(current, min_value, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return min_value;
            }
        }
    }

    /// Perform garbage collection on committed transactions.
    ///
    /// Transactions with commit_id < lowest_active_start can be cleaned up
    /// because no active transaction needs to see their old data.
    pub fn cleanup(&self) {
        let lowest_start = self.lowest_active_start.load(Ordering::SeqCst);

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

        // Commit t1 - now t2 should be lowest by transaction id.
        tm.commit_transaction(t1).unwrap();
        assert_eq!(tm.lowest_active_start(), t2.start_time);
        assert_eq!(tm.lowest_active_id(), t2.id);

        // Rollback t2 - now t3 should be lowest by transaction id.
        tm.rollback_transaction(t2).unwrap();
        assert_eq!(tm.lowest_active_start(), t3.start_time);
        assert_eq!(tm.lowest_active_id(), t3.id);

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
    fn test_remove_transaction_creates_cleanup_info() {
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
