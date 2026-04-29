// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! WAL write session handle.
//!
//! Database catalog and mixed transactions commit through the unified `Txn*` journal on
//! `WalWriter`; this type is a thin wrapper for tests and call sites that share a writer.

use crate::wal::wal_entry::WalEntry;
use crate::wal::wal_type::WalType;
use crate::wal::wal_writer::WalWriter;
use paro_common::error::Result;
use std::sync::Arc;

/// WAL write state bound to one [`WalWriter`].
pub struct WalWriteState {
    wal: Arc<WalWriter>,
}

impl WalWriteState {
    pub fn new(wal: Arc<WalWriter>) -> Self {
        Self { wal }
    }

    pub fn flush(&self) -> Result<()> {
        self.wal.flush()
    }

    pub fn write_rowset_commit(
        &self,
        tablet_id: u64,
        rowset_id: u64,
        start_version: i64,
        end_version: i64,
        rowset_path: &str,
    ) -> Result<()> {
        let entry = WalEntry::RowsetCommit {
            tablet_id,
            rowset_id,
            start_version,
            end_version,
            rowset_path: rowset_path.to_string(),
        };
        self.wal
            .write_entry(WalType::RowsetCommit, &entry.serialize_data())
    }

    pub fn wal(&self) -> &Arc<WalWriter> {
        &self.wal
    }
}
