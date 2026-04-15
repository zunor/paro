// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Write-ahead logging, replay state tracking, and crash recovery.
//!
//! The WAL layer owns record encoding, buffered I/O, checkpoint handoff, and
//! replay/truncation during recovery.

mod checksum;
pub mod recovery;
pub mod replay_state;
pub mod txn_record;
pub mod wal_entry;
pub mod wal_reader;
pub mod wal_type;
pub mod wal_write_state;
pub mod wal_writer;
pub mod write_ahead_log;

/// Test-only WAL builders (also used by `paro-instance` integration tests).
#[doc(hidden)]
pub mod test_support;
