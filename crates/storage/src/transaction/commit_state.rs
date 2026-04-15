use crate::transaction::txn::Transaction;
use crate::transaction::undo_buffer::{
    ActiveTransactionState, CommitMode, UndoAppendInfo, UndoDeleteInfo, UndoEntry, UndoFlags,
    UndoPayload, UndoUpdateInfo,
};

/// Index removal type for commit and cleanup operations.
///
/// ```cpp
/// enum class IndexRemovalType {
///     MAIN_INDEX,           // Remove from main index, insert into deleted_rows_in_use
///     MAIN_INDEX_ONLY,      // Remove from main index only
///     REVERT_MAIN_INDEX,    // Revert MAIN_INDEX, append to main and remove from deleted_rows_in_use
///     REVERT_MAIN_INDEX_ONLY, // Revert MAIN_INDEX_ONLY, append to main index
///     DELETED_ROWS_IN_USE   // Remove from deleted_rows_in_use (used in cleanup)
/// };
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexRemovalType {
    /// Remove from main index, insert into deleted_rows_in_use
    MainIndex,
    /// No other transactions, don't need to track deleted rows in deleted_rows_in_use
    MainIndexOnly,
    /// Revert appends, other transactions exist (append to main, remove from deleted_rows_in_use)
    RevertMainIndex,
    /// Revert appends, no other transactions (append to main index only)
    RevertMainIndexOnly,
    /// Remove from deleted_rows_in_use (used during cleanup)
    DeletedRowsInUse,
}

/// IndexDataRemover handles index cleanup during commit.
///
/// ```cpp
/// struct IndexDataRemover {
///     QueryContext context;
///     IndexRemovalType removal_type;
///     Chunk chunk;
///     reference_map_t<table_t, shared_ptr<TableInfo>> verify_indexes;
/// };
/// ```
///
/// In Paro, this is a simplified version that tracks pending index deletions.
/// Full index integration will be added when index support is implemented.
#[derive(Debug)]
pub struct IndexDataRemover {
    /// The removal type determines how deleted rows are tracked
    pub removal_type: IndexRemovalType,
    /// Pending row deletions grouped by table_id
    pending_deletes: Vec<(u64, Vec<u64>)>, // (table_id, row_ids)
}

impl IndexDataRemover {
    /// Create a new IndexDataRemover.
    ///
    /// ```cpp
    ///                                    IndexRemovalType removal_type)
    /// ```
    pub fn new(removal_type: IndexRemovalType) -> Self {
        Self {
            removal_type,
            pending_deletes: Vec::new(),
        }
    }

    /// Push a delete operation for index cleanup.
    ///
    /// ```cpp
    /// void IndexDataRemover::PushDelete(DeleteInfo &info) {
    ///     if (!version_table.HasIndexes()) { return; }
    ///     // collect row_numbers and flush
    /// }
    /// ```
    pub fn push_delete(&mut self, table_id: u64, info: &UndoDeleteInfo) {
        // Collect row IDs for index removal
        let row_ids: Vec<u64> = if info.is_consecutive {
            (0..info.count).map(|i| info.base_row + i).collect()
        } else {
            info.row_ids.clone()
        };

        if !row_ids.is_empty() {
            self.pending_deletes.push((table_id, row_ids));
        }
    }

    /// Flush pending deletes to indexes.
    ///
    /// ```cpp
    /// void IndexDataRemover::Flush(table_t table_id, row_t *row_numbers, idx_t count)
    /// ```
    ///
    /// Note: Actual index removal will be implemented when index support is added.
    pub fn flush(&mut self) {
        // TODO: When index support is added, iterate pending_deletes and
        // call table.RemoveFromIndexes() for each table
        self.pending_deletes.clear();
    }

    /// Verify index integrity (debug mode).
    ///
    /// ```cpp
    /// void IndexDataRemover::Verify() {
    ///     for (auto &table : verify_indexes) {
    ///         table.second->VerifyIndexBuffers();
    ///     }
    /// }
    /// ```
    pub fn verify(&self) {
        // TODO: Implement index verification when index support is added
        #[cfg(debug_assertions)]
        {
            // In debug mode, we could verify index consistency here
        }
    }

    #[cfg(test)]
    pub(crate) fn pending_delete_count(&self) -> usize {
        self.pending_deletes.len()
    }
}

/// CommitState handles the commitment of undo entries.
///
/// ```cpp
/// class CommitState {
/// public:
///                          ActiveTransactionState transaction_state, CommitMode commit_mode);
///     void CommitEntry(UndoFlags type, data_ptr_t data);
///     void RevertCommit(UndoFlags type, data_ptr_t data);
///     void Flush();
///     void Verify();
/// private:
///     void CommitEntryDrop(CatalogEntry &entry, data_ptr_t extra_data);
///     void CommitDelete(DeleteInfo &info);
///     transaction_t commit_id;
///     IndexDataRemover index_data_remover;
/// };
/// ```
///
pub struct CommitState<'a> {
    /// Reference to the transaction being committed
    pub transaction: &'a Transaction,
    /// The commit ID (timestamp) for this commit
    pub commit_id: u64,
    /// State of other active transactions (affects index removal strategy)
    pub active_transaction_state: ActiveTransactionState,
    /// Whether we're committing or reverting a partial commit
    pub commit_mode: CommitMode,
    /// Handles index cleanup during commit
    index_data_remover: IndexDataRemover,
}

impl<'a> CommitState<'a> {
    /// Create a new CommitState.
    ///
    /// ```cpp
    ///                          ActiveTransactionState transaction_state, CommitMode commit_mode)
    ///     : transaction(transaction_p), commit_id(commit_id),
    ///       index_data_remover(transaction, *transaction.context.lock(),
    ///                          GetIndexRemovalType(transaction_state, commit_mode)) {}
    /// ```
    pub fn new(
        transaction: &'a Transaction,
        commit_id: u64,
        active_transaction_state: ActiveTransactionState,
        commit_mode: CommitMode,
    ) -> Self {
        let removal_type = Self::get_index_removal_type(active_transaction_state, commit_mode);
        Self {
            transaction,
            commit_id,
            active_transaction_state,
            commit_mode,
            index_data_remover: IndexDataRemover::new(removal_type),
        }
    }

    /// Determine the index removal type based on transaction state and commit mode.
    ///
    /// ```cpp
    /// IndexRemovalType CommitState::GetIndexRemovalType(ActiveTransactionState transaction_state,
    ///                                                   CommitMode commit_mode) {
    ///     if (commit_mode == CommitMode::COMMIT) {
    ///         if (transaction_state == ActiveTransactionState::NO_OTHER_TRANSACTIONS) {
    ///             return IndexRemovalType::MAIN_INDEX_ONLY;
    ///         }
    ///         return IndexRemovalType::MAIN_INDEX;
    ///     }
    ///     // revert the appends to the indexes
    ///     if (transaction_state == ActiveTransactionState::NO_OTHER_TRANSACTIONS) {
    ///         return IndexRemovalType::REVERT_MAIN_INDEX_ONLY;
    ///     }
    ///     return IndexRemovalType::REVERT_MAIN_INDEX;
    /// }
    /// ```
    pub fn get_index_removal_type(
        transaction_state: ActiveTransactionState,
        commit_mode: CommitMode,
    ) -> IndexRemovalType {
        match commit_mode {
            CommitMode::Commit => {
                if transaction_state == ActiveTransactionState::NoOtherTransactions {
                    IndexRemovalType::MainIndexOnly
                } else {
                    IndexRemovalType::MainIndex
                }
            }
            CommitMode::RevertCommit => {
                if transaction_state == ActiveTransactionState::NoOtherTransactions {
                    IndexRemovalType::RevertMainIndexOnly
                } else {
                    IndexRemovalType::RevertMainIndex
                }
            }
        }
    }

    /// Commit an entry from the undo buffer (raw pointer API).
    ///
    /// ```cpp
    /// void CommitState::CommitEntry(UndoFlags type, data_ptr_t data) {
    ///     switch (type) {
    ///     case UndoFlags::CATALOG_ENTRY: { ... }
    ///     case UndoFlags::INSERT_TUPLE: { ... }
    ///     case UndoFlags::DELETE_TUPLE: { ... }
    ///     case UndoFlags::UPDATE_TUPLE: { ... }
    ///     }
    /// }
    /// ```
    pub fn commit_entry(&mut self, flags: UndoFlags, data: *const u8) {
        if self.commit_mode == CommitMode::RevertCommit {
            self.revert_commit(flags, data);
            return;
        }

        match flags {
            UndoFlags::InsertTuple => {
                // auto info = reinterpret_cast<AppendInfo *>(data);
                // info->table->CommitAppend(commit_id, info->start_row, info->count);
                //
                // In Paro, this would update the version info for the inserted rows
                // to mark them as committed with commit_id.
            }
            UndoFlags::DeleteTuple => {
                // This marks deleted rows as permanently deleted and updates indexes.
                //
                // In Paro, we would:
                // 1. Call version_info.commit_delete(vector_idx, commit_id, info)
                // 2. Call index_data_remover.push_delete(info)
            }
            UndoFlags::UpdateTuple => {
                // This finalizes the update by setting the version number.
            }
            UndoFlags::SequenceValue | UndoFlags::DatabaseAttach | UndoFlags::EmptyEntry => {
                // No action needed for these types
            }
        }
        let _ = data; // Suppress unused warning until full implementation
    }

    /// Commit a high-level UndoEntry.
    ///
    /// This is the preferred API in Paro, using type-safe UndoEntry structures
    /// instead of raw pointers.
    pub fn commit_high_level_entry(&mut self, entry: &UndoEntry) {
        if self.commit_mode == CommitMode::RevertCommit {
            self.revert_high_level_commit(entry);
            return;
        }

        match &entry.payload {
            UndoPayload::Insert(info) => {
                self.commit_insert(info);
            }
            UndoPayload::Delete(info) => {
                self.commit_delete(info);
            }
            UndoPayload::Update(info) => {
                self.commit_update(info);
            }
            UndoPayload::Sequence(_) | UndoPayload::DatabaseAttach { .. } | UndoPayload::Empty => {
                // No action needed
            }
        }
    }

    /// Commit an insert (append) operation.
    ///
    /// ```cpp
    /// case UndoFlags::INSERT_TUPLE: {
    ///     auto info = reinterpret_cast<AppendInfo *>(data);
    ///     if (!info->table->IsMainTable()) {
    ///         throw TransactionException("...");
    ///     }
    ///     info->table->CommitAppend(commit_id, info->start_row, info->count);
    /// }
    /// ```
    fn commit_insert(&mut self, info: &UndoAppendInfo) {
        // Mark the inserted rows as committed by updating their version info
        // to use commit_id instead of transaction_id.
        //
        // This will be called by the storage layer:
        // table.commit_append(commit_id, info.start_row, info.count)
        let _ = info; // Will be used when storage integration is complete
    }

    /// Commit a delete operation.
    ///
    /// ```cpp
    /// void CommitState::CommitDelete(DeleteInfo &info) {
    ///     // mark the tuples as committed
    ///     info.version_info->CommitDelete(info.vector_idx, commit_id, info);
    ///     // delete from indexes
    ///     index_data_remover.PushDelete(info);
    /// }
    /// ```
    fn commit_delete(&mut self, info: &UndoDeleteInfo) {
        // 1. Mark deleted rows as permanently deleted with commit_id
        // This will be called by the storage layer:
        // version_info.commit_delete(vector_idx, commit_id, info)

        // 2. Queue index removal
        self.index_data_remover.push_delete(info.table_id, info);
    }

    /// Commit an update operation.
    ///
    /// ```cpp
    /// case UndoFlags::UPDATE_TUPLE: {
    ///     auto info = reinterpret_cast<UpdateInfo *>(data);
    ///     if (!info->table->IsMainTable()) {
    ///         throw TransactionException("...");
    ///     }
    ///     info->version_number = commit_id;
    /// }
    /// ```
    fn commit_update(&mut self, info: &UndoUpdateInfo) {
        // Set the version number of the update to commit_id,
        // making it visible to other transactions.
        let _ = info; // Will be used when storage integration is complete
    }

    /// Revert a commit for a raw entry (used when commit fails partway).
    ///
    /// ```cpp
    /// void CommitState::RevertCommit(UndoFlags type, data_ptr_t data) {
    ///     transaction_t transaction_id = commit_id;
    ///     switch (type) {
    ///     case UndoFlags::CATALOG_ENTRY: { ... }
    ///     case UndoFlags::INSERT_TUPLE: { ... }
    ///     case UndoFlags::DELETE_TUPLE: { ... }
    ///     case UndoFlags::UPDATE_TUPLE: { ... }
    ///     }
    /// }
    /// ```
    pub fn revert_commit(&mut self, flags: UndoFlags, data: *const u8) {
        let _transaction_id = self.commit_id;
        match flags {
            UndoFlags::InsertTuple => {
                // Revert the append
            }
            UndoFlags::DeleteTuple => {
                // Revert delete commit (write transaction_id back)
            }
            UndoFlags::UpdateTuple => {
                // Revert update version number
            }
            _ => {}
        }
        let _ = data;
    }

    /// Revert a commit for a high-level entry.
    pub fn revert_high_level_commit(&mut self, entry: &UndoEntry) {
        match &entry.payload {
            UndoPayload::Insert(_info) => {
                // Revert append
            }
            UndoPayload::Delete(info) => {
                // Revert delete commit - queue for index update
                self.index_data_remover.push_delete(info.table_id, info);
            }
            UndoPayload::Update(_info) => {
                // Revert update version number
            }
            _ => {}
        }
    }

    /// Flush any pending operations.
    ///
    /// Called at the end of commit to flush any buffered index operations.
    pub fn flush(&mut self) {
        self.index_data_remover.flush();
    }

    /// Verify commit integrity (debug mode).
    ///
    /// ```cpp
    /// void CommitState::Verify() {
    ///     index_data_remover.Verify();
    /// }
    /// ```
    pub fn verify(&self) {
        self.index_data_remover.verify();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_removal_type_commit_no_other_transactions() {
        let removal_type = CommitState::get_index_removal_type(
            ActiveTransactionState::NoOtherTransactions,
            CommitMode::Commit,
        );
        assert_eq!(removal_type, IndexRemovalType::MainIndexOnly);
    }

    #[test]
    fn test_index_removal_type_commit_with_other_transactions() {
        let removal_type = CommitState::get_index_removal_type(
            ActiveTransactionState::OtherTransactions,
            CommitMode::Commit,
        );
        assert_eq!(removal_type, IndexRemovalType::MainIndex);
    }

    #[test]
    fn test_index_removal_type_revert_no_other_transactions() {
        let removal_type = CommitState::get_index_removal_type(
            ActiveTransactionState::NoOtherTransactions,
            CommitMode::RevertCommit,
        );
        assert_eq!(removal_type, IndexRemovalType::RevertMainIndexOnly);
    }

    #[test]
    fn test_index_removal_type_revert_with_other_transactions() {
        let removal_type = CommitState::get_index_removal_type(
            ActiveTransactionState::OtherTransactions,
            CommitMode::RevertCommit,
        );
        assert_eq!(removal_type, IndexRemovalType::RevertMainIndex);
    }

    #[test]
    fn test_index_data_remover_push_delete_consecutive() {
        let mut remover = IndexDataRemover::new(IndexRemovalType::MainIndex);
        let info = UndoDeleteInfo {
            table_id: 1,
            base_row: 100,
            count: 3,
            is_consecutive: true,
            row_ids: vec![],
        };

        remover.push_delete(1, &info);
        assert_eq!(remover.pending_deletes.len(), 1);
        assert_eq!(remover.pending_deletes[0].0, 1);
        assert_eq!(remover.pending_deletes[0].1, vec![100, 101, 102]);
    }

    #[test]
    fn test_index_data_remover_push_delete_non_consecutive() {
        let mut remover = IndexDataRemover::new(IndexRemovalType::MainIndex);
        let info = UndoDeleteInfo {
            table_id: 2,
            base_row: 0,
            count: 3,
            is_consecutive: false,
            row_ids: vec![10, 20, 30],
        };

        remover.push_delete(2, &info);
        assert_eq!(remover.pending_deletes.len(), 1);
        assert_eq!(remover.pending_deletes[0].0, 2);
        assert_eq!(remover.pending_deletes[0].1, vec![10, 20, 30]);
    }

    #[test]
    fn test_index_data_remover_flush_clears_pending() {
        let mut remover = IndexDataRemover::new(IndexRemovalType::MainIndex);
        let info = UndoDeleteInfo {
            table_id: 1,
            base_row: 0,
            count: 1,
            is_consecutive: true,
            row_ids: vec![],
        };

        remover.push_delete(1, &info);
        assert!(!remover.pending_deletes.is_empty());

        remover.flush();
        assert!(remover.pending_deletes.is_empty());
    }

    #[test]
    fn test_commit_state_creation() {
        let txn = Transaction::new(1, 100);
        let state = CommitState::new(
            &txn,
            500,
            ActiveTransactionState::NoOtherTransactions,
            CommitMode::Commit,
        );

        assert_eq!(state.commit_id, 500);
        assert_eq!(
            state.active_transaction_state,
            ActiveTransactionState::NoOtherTransactions
        );
        assert_eq!(state.commit_mode, CommitMode::Commit);
        assert_eq!(
            state.index_data_remover.removal_type,
            IndexRemovalType::MainIndexOnly
        );
    }
}
