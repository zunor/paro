// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::table_handle::{InsertOnConflictAction, TableHandle};
use crate::mutation;
use crate::transaction::txn::Transaction;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use std::sync::Arc;

impl TableHandle {
    /// Append a chunk via DeltaWriter.
    pub fn append(&self, chunk: &Chunk) -> Result<()> {
        mutation::writer::append(self, chunk)
    }

    /// Append a chunk via DeltaWriter, optionally registering with a transaction.
    pub fn append_with_transaction(
        &self,
        chunk: &Chunk,
        txn: Option<Arc<Transaction>>,
    ) -> Result<()> {
        mutation::writer::append_with_transaction(self, chunk, txn)
    }

    pub fn append_partial_with_transaction(
        &self,
        chunk: &Chunk,
        partial_column_indices: Vec<usize>,
        base_row_ids: &[u64],
        txn: Option<Arc<Transaction>>,
    ) -> Result<()> {
        mutation::writer::append_partial_with_transaction(
            self,
            chunk,
            partial_column_indices,
            base_row_ids,
            txn,
        )
    }

    pub fn insert_on_conflict(
        &self,
        chunk: &Chunk,
        action: &InsertOnConflictAction,
        txn: Option<Arc<Transaction>>,
    ) -> Result<usize> {
        mutation::upsert::insert_on_conflict(self, chunk, action, txn)
    }

    /// Delete rows by encoded row IDs and persist as delete vectors + WAL.
    ///
    /// With an active transaction, deletes are staged and applied on commit.
    pub fn delete(&self, row_ids: &[u64], txn: Option<Arc<Transaction>>) -> Result<usize> {
        mutation::deleter::delete(self, row_ids, txn)
    }

    /// Delete all visible rows from the table.
    ///
    /// PRIMARY_KEYS tables use primary-index snapshot + primary-delete path.
    /// DUPLICATE_KEYS tables expand visible row locations and persist a RowIdDelete WAL entry.
    pub fn delete_all(&self, txn: Option<Arc<Transaction>>) -> Result<usize> {
        mutation::deleter::delete_all(self, txn)
    }

    /// Delete by primary key Chunk (PRIMARY_KEYS only). Returns rows removed.
    pub fn delete_by_primary_keys(
        &self,
        keys: &Chunk,
        txn: Option<Arc<Transaction>>,
    ) -> Result<usize> {
        mutation::deleter::delete_by_primary_keys(self, keys, txn)
    }

    /// Update rows via row-id lookup + delete/insert semantics.
    pub fn update(
        &self,
        row_ids: &[u64],
        column_ids: &[usize],
        values: &[Vec<paro_common::runtime_value::Value>],
        txn: Option<Arc<Transaction>>,
    ) -> Result<usize> {
        mutation::updater::update(self, row_ids, column_ids, values, txn)
    }
}
