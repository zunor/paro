// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

// Version information for a row or group of rows.

use std::sync::atomic::{AtomicU64, Ordering};

/// Version information for a row or group of rows.
///
/// Uses AtomicU64 to allow safe concurrent visibility checks and conflict detection.
#[derive(Debug)]
pub struct VersionInfo {
    /// The transaction ID that inserted the row
    pub insertion_id: AtomicU64,
    /// The transaction ID that deleted the row (0 if not deleted)
    pub deletion_id: AtomicU64,
}

impl VersionInfo {
    pub fn new(insertion_id: u64) -> Self {
        Self {
            insertion_id: AtomicU64::new(insertion_id),
            deletion_id: AtomicU64::new(0),
        }
    }

    /// Check if a row is visible to a given transaction
    pub fn is_visible(&self, transaction_id: u64, start_time: u64) -> bool {
        let ins_id = self.insertion_id.load(Ordering::Relaxed);
        let del_id = self.deletion_id.load(Ordering::Relaxed);

        // Simple MVCC visibility rules:
        // 1. If insertion_id == transaction_id, it's visible (own insert)
        // 2. If insertion_id < start_time, it was committed before this txn started -> visible

        let inserted = ins_id == transaction_id || ins_id < start_time;
        if !inserted {
            return false;
        }

        if del_id == 0 {
            return true;
        }

        // If deleted by ourselves, it's not visible
        if del_id == transaction_id {
            return false;
        }

        // If deleted by someone else, it's visible only if they committed AFTER we started
        // (meaning at our start time, the row was still there)
        if del_id > start_time {
            return true;
        }

        false
    }

    /// Check for write-write conflicts
    pub fn check_conflict(&self, transaction_id: u64) -> bool {
        let del_id = self.deletion_id.load(Ordering::Relaxed);
        // Conflict if modified by another ACTIVE transaction
        // (If deletion_id is set and not by us, it's a conflict)
        if del_id != 0 && del_id != transaction_id {
            return true;
        }
        false
    }

    /// Try to mark the row as deleted by the given transaction.
    /// Returns true if successful, false if a conflict occurred.
    pub fn try_delete(&self, transaction_id: u64) -> bool {
        // Atomic compare and exchange: if deletion_id is 0, set it to transaction_id.
        // If it's already set to something else, someone else beat us to it (or it's already deleted).
        match self.deletion_id.compare_exchange(
            0,
            transaction_id,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => true,                             // Successfully marked as deleted
            Err(current) => current == transaction_id, // Already marked as deleted by us is fine
        }
    }
}

impl PartialEq for VersionInfo {
    fn eq(&self, other: &Self) -> bool {
        self.insertion_id.load(Ordering::Relaxed) == other.insertion_id.load(Ordering::Relaxed)
            && self.deletion_id.load(Ordering::Relaxed) == other.deletion_id.load(Ordering::Relaxed)
    }
}
