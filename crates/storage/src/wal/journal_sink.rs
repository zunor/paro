// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Storage-backed journal sink that appends framed records into the active WAL stream.

use crate::wal::wal_type::WalType;
use crate::wal::write_ahead_log::WriteAheadLog;
use paro_common::error::Result;
use paro_journal::JournalSink;
use std::sync::Arc;

pub struct WalJournalSink {
    wal: Arc<WriteAheadLog>,
}

impl WalJournalSink {
    pub fn new(wal: Arc<WriteAheadLog>) -> Self {
        Self { wal }
    }
}

impl JournalSink for WalJournalSink {
    fn append_batch(&self, frames: &[Vec<u8>]) -> Result<()> {
        if frames.is_empty() {
            return Ok(());
        }

        let write_state = self.wal.begin_write();
        let writer = write_state.wal();
        for frame in frames {
            writer.write_entry(WalType::JournalRecord, frame)?;
        }
        write_state.flush()
    }
}
