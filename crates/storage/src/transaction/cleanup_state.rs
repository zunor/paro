// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Cleanup helpers for committed undo-buffer entries.

use crate::transaction::commit_state::{IndexDataRemover, IndexRemovalType};
use crate::transaction::undo_buffer::{
    ActiveTransactionState, UndoAppendInfo, UndoDeleteInfo, UndoEntry, UndoFlags, UndoPayload,
    UndoUpdateInfo,
};

#[derive(Debug)]
pub struct CleanupState {
    /// The lowest active transaction ID whose state still needs to be preserved.
    pub lowest_active_transaction: u64,
    /// Whether other transactions were still active when cleanup started.
    pub transaction_state: ActiveTransactionState,
    /// Deferred index cleanup collected while walking undo entries.
    index_data_remover: IndexDataRemover,
}

impl CleanupState {
    pub fn new(lowest_active_transaction: u64, transaction_state: ActiveTransactionState) -> Self {
        Self {
            lowest_active_transaction,
            transaction_state,
            index_data_remover: IndexDataRemover::new(IndexRemovalType::DeletedRowsInUse),
        }
    }

    /// Cleanup entry points used by the raw-pointer path in `UndoBuffer`.
    pub fn cleanup_entry(&mut self, flags: UndoFlags, _data: *const u8) {
        match flags {
            UndoFlags::InsertTuple
            | UndoFlags::DeleteTuple
            | UndoFlags::UpdateTuple
            | UndoFlags::SequenceValue
            | UndoFlags::DatabaseAttach
            | UndoFlags::EmptyEntry => {}
        }
    }

    pub fn cleanup_high_level_entry(&mut self, entry: &UndoEntry) {
        match &entry.payload {
            UndoPayload::Insert(info) => self.cleanup_insert(info),
            UndoPayload::Delete(info) => self.cleanup_delete(info),
            UndoPayload::Update(info) => self.cleanup_update(info),
            UndoPayload::Sequence(_) | UndoPayload::DatabaseAttach { .. } | UndoPayload::Empty => {}
        }
    }

    fn cleanup_insert(&mut self, _info: &UndoAppendInfo) {}

    fn cleanup_delete(&mut self, info: &UndoDeleteInfo) {
        if self.transaction_state == ActiveTransactionState::NoOtherTransactions {
            return;
        }

        self.index_data_remover.push_delete(info.table_id, info);
    }

    fn cleanup_update(&mut self, _info: &UndoUpdateInfo) {}

    pub fn flush(&mut self) {
        self.index_data_remover.flush();
    }

    pub fn lowest_active_transaction(&self) -> u64 {
        self.lowest_active_transaction
    }

    pub fn transaction_state(&self) -> ActiveTransactionState {
        self.transaction_state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cleanup_state_creation() {
        let state = CleanupState::new(100, ActiveTransactionState::NoOtherTransactions);

        assert_eq!(state.lowest_active_transaction(), 100);
        assert_eq!(
            state.transaction_state(),
            ActiveTransactionState::NoOtherTransactions
        );
        assert_eq!(
            state.index_data_remover.removal_type,
            IndexRemovalType::DeletedRowsInUse
        );
    }

    #[test]
    fn test_cleanup_high_level_noops() {
        let mut state = CleanupState::new(100, ActiveTransactionState::NoOtherTransactions);

        state.cleanup_entry(UndoFlags::EmptyEntry, std::ptr::null());
        state.cleanup_high_level_entry(&UndoEntry {
            flags: UndoFlags::EmptyEntry,
            payload: UndoPayload::Empty,
        });
        state.cleanup_high_level_entry(&UndoEntry {
            flags: UndoFlags::DatabaseAttach,
            payload: UndoPayload::DatabaseAttach {
                schema: "public".into(),
                database: "users".into(),
            },
        });
        state.cleanup_high_level_entry(&UndoEntry::insert(42, 100, 5));
        state.cleanup_high_level_entry(&UndoEntry::update(42, 1, vec![5, 6, 7]));
    }

    #[test]
    fn test_cleanup_delete_no_other_transactions() {
        let mut state = CleanupState::new(100, ActiveTransactionState::NoOtherTransactions);
        let info = UndoDeleteInfo {
            table_id: 1,
            base_row: 0,
            count: 5,
            is_consecutive: true,
            row_ids: vec![],
        };

        state.cleanup_delete(&info);
        assert_eq!(state.index_data_remover.pending_delete_count(), 0);
    }

    #[test]
    fn test_cleanup_delete_with_other_transactions_tracks_pending_rows() {
        let mut state = CleanupState::new(100, ActiveTransactionState::OtherTransactions);
        let info = UndoDeleteInfo {
            table_id: 1,
            base_row: 0,
            count: 3,
            is_consecutive: true,
            row_ids: vec![],
        };

        state.cleanup_delete(&info);
        assert_eq!(state.index_data_remover.pending_delete_count(), 1);

        state.cleanup_high_level_entry(&UndoEntry::delete_consecutive(42, 100, 3));
        assert_eq!(state.index_data_remover.pending_delete_count(), 2);

        state.flush();
        assert_eq!(state.index_data_remover.pending_delete_count(), 0);
    }
}
