//! Rollback helpers for undo-buffer entries.

use crate::transaction::txn::Transaction;
use crate::transaction::undo_buffer::{
    UndoAppendInfo, UndoDeleteInfo, UndoEntry, UndoFlags, UndoPayload, UndoSequenceValueInfo,
    UndoUpdateInfo,
};

pub struct RollbackState<'a> {
    /// Reference to the transaction being rolled back.
    pub transaction: &'a Transaction,
}

impl<'a> RollbackState<'a> {
    pub fn new(transaction: &'a Transaction) -> Self {
        Self { transaction }
    }

    /// Rollback entry points used by the raw-pointer path in `UndoBuffer`.
    pub fn rollback_entry(&mut self, flags: UndoFlags, _data: *const u8) {
        match flags {
            UndoFlags::InsertTuple
            | UndoFlags::DeleteTuple
            | UndoFlags::UpdateTuple
            | UndoFlags::DatabaseAttach
            | UndoFlags::SequenceValue
            | UndoFlags::EmptyEntry => {}
        }
    }

    pub fn rollback_high_level_entry(&mut self, entry: &UndoEntry) {
        match &entry.payload {
            UndoPayload::Insert(info) => self.rollback_insert(info),
            UndoPayload::Delete(info) => self.rollback_delete(info),
            UndoPayload::Update(info) => self.rollback_update(info),
            UndoPayload::Sequence(info) => self.rollback_sequence(info),
            UndoPayload::DatabaseAttach { schema, database } => {
                self.rollback_database_attach(schema, database)
            }
            UndoPayload::Empty => {}
        }
    }

    fn rollback_insert(&mut self, _info: &UndoAppendInfo) {}

    fn rollback_delete(&mut self, _info: &UndoDeleteInfo) {}

    fn rollback_update(&mut self, _info: &UndoUpdateInfo) {}

    fn rollback_sequence(&mut self, _info: &UndoSequenceValueInfo) {}

    fn rollback_database_attach(&mut self, _schema: &str, _database: &str) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rollback_state_creation() {
        let txn = Transaction::new(1, 100);
        let state = RollbackState::new(&txn);
        assert_eq!(state.transaction.id, 1);
        assert_eq!(state.transaction.start_time, 100);
    }

    #[test]
    fn test_rollback_entry_dispatch_is_a_noop_for_placeholder_paths() {
        let txn = Transaction::new(1, 100);
        let mut state = RollbackState::new(&txn);

        state.rollback_entry(UndoFlags::EmptyEntry, std::ptr::null());
        state.rollback_high_level_entry(&UndoEntry {
            flags: UndoFlags::EmptyEntry,
            payload: UndoPayload::Empty,
        });
        state.rollback_high_level_entry(&UndoEntry {
            flags: UndoFlags::DatabaseAttach,
            payload: UndoPayload::DatabaseAttach {
                schema: "public".into(),
                database: "users".into(),
            },
        });
        state.rollback_high_level_entry(&UndoEntry::insert(42, 100, 5));
        state.rollback_high_level_entry(&UndoEntry::delete_consecutive(42, 100, 3));
        state.rollback_high_level_entry(&UndoEntry::update(42, 1, vec![5, 6, 7]));
        state.rollback_high_level_entry(&UndoEntry::sequence_value(10, 5, 42));
    }
}
