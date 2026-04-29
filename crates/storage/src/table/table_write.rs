// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::table_handle::{InsertOnConflictAction, TableHandle};
use crate::mutation::{self, MutationTarget};
use crate::transaction::txn::Transaction;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_transaction::TransactionView;
use std::sync::Arc;

impl TableHandle {
    /// Append a chunk through the storage-local direct path.
    ///
    /// This is a low-level construction/test helper. SQL/frontend DML must use
    /// `append_with_transaction` with an active transaction so commit sequencing,
    /// durable journal append, and publish are all coordinated.
    pub fn append(&self, chunk: &Chunk) -> Result<()> {
        mutation::writer::append(self, chunk)
    }

    /// Append a chunk via DeltaWriter and register it with a transaction.
    pub fn append_with_transaction(
        &self,
        view: &TransactionView,
        chunk: &Chunk,
        txn: Arc<Transaction>,
    ) -> Result<()> {
        mutation::writer::append_with_transaction(
            view,
            self,
            chunk,
            MutationTarget::Transaction(txn),
        )
    }

    pub fn append_partial_with_transaction(
        &self,
        view: &TransactionView,
        chunk: &Chunk,
        partial_column_indices: Vec<usize>,
        base_row_ids: &[u64],
        txn: Arc<Transaction>,
    ) -> Result<()> {
        mutation::writer::append_partial_with_transaction(
            view,
            self,
            chunk,
            partial_column_indices,
            base_row_ids,
            MutationTarget::Transaction(txn),
        )
    }

    pub fn insert_on_conflict(
        &self,
        view: &TransactionView,
        chunk: &Chunk,
        action: &InsertOnConflictAction,
        txn: Arc<Transaction>,
    ) -> Result<usize> {
        mutation::upsert::insert_on_conflict(
            view,
            self,
            chunk,
            action,
            MutationTarget::Transaction(txn),
        )
    }

    pub fn insert_on_conflict_direct(
        &self,
        view: &TransactionView,
        chunk: &Chunk,
        action: &InsertOnConflictAction,
    ) -> Result<usize> {
        mutation::upsert::insert_on_conflict(view, self, chunk, action, MutationTarget::Direct)
    }

    /// Delete rows by encoded row IDs and stage/persist delete vectors.
    ///
    pub fn delete(
        &self,
        view: &TransactionView,
        row_ids: &[u64],
        txn: Arc<Transaction>,
    ) -> Result<usize> {
        mutation::deleter::delete(view, self, row_ids, MutationTarget::Transaction(txn))
    }

    pub fn delete_direct(&self, view: &TransactionView, row_ids: &[u64]) -> Result<usize> {
        mutation::deleter::delete(view, self, row_ids, MutationTarget::Direct)
    }

    /// Delete all visible rows from the table.
    ///
    /// PRIMARY_KEYS tables use primary-index snapshot + primary-delete path.
    /// DUPLICATE_KEYS tables expand visible row locations and persist delete vectors.
    pub fn delete_all(&self, view: &TransactionView, txn: Arc<Transaction>) -> Result<usize> {
        mutation::deleter::delete_all(view, self, MutationTarget::Transaction(txn))
    }

    pub fn delete_all_direct(&self, view: &TransactionView) -> Result<usize> {
        mutation::deleter::delete_all(view, self, MutationTarget::Direct)
    }

    /// Delete by primary key Chunk (PRIMARY_KEYS only). Returns rows removed.
    pub fn delete_by_primary_keys(
        &self,
        view: &TransactionView,
        keys: &Chunk,
        txn: Arc<Transaction>,
    ) -> Result<usize> {
        mutation::deleter::delete_by_primary_keys(
            view,
            self,
            keys,
            MutationTarget::Transaction(txn),
        )
    }

    pub fn delete_by_primary_keys_direct(
        &self,
        view: &TransactionView,
        keys: &Chunk,
    ) -> Result<usize> {
        mutation::deleter::delete_by_primary_keys(view, self, keys, MutationTarget::Direct)
    }

    /// Update rows via row-id lookup + delete/insert semantics.
    pub fn update(
        &self,
        view: &TransactionView,
        row_ids: &[u64],
        column_ids: &[usize],
        values: &[Vec<paro_common::runtime_value::Value>],
        txn: Arc<Transaction>,
    ) -> Result<usize> {
        mutation::updater::update(
            view,
            self,
            row_ids,
            column_ids,
            values,
            MutationTarget::Transaction(txn),
        )
    }

    pub fn update_direct(
        &self,
        view: &TransactionView,
        row_ids: &[u64],
        column_ids: &[usize],
        values: &[Vec<paro_common::runtime_value::Value>],
    ) -> Result<usize> {
        mutation::updater::update(
            view,
            self,
            row_ids,
            column_ids,
            values,
            MutationTarget::Direct,
        )
    }
}
