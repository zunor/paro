// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Version information for a row or group of rows.

use std::hint::spin_loop;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct RowVersionHeader {
    generation: AtomicU64,
    insertion_id: AtomicU64,
    deletion_id: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RowVersionSnapshot {
    insertion_id: u64,
    deletion_id: u64,
}

impl RowVersionHeader {
    pub fn new(insertion_id: u64) -> Self {
        Self {
            generation: AtomicU64::new(0),
            insertion_id: AtomicU64::new(insertion_id),
            deletion_id: AtomicU64::new(0),
        }
    }

    #[inline]
    fn snapshot(&self) -> RowVersionSnapshot {
        loop {
            let before = self.generation.load(Ordering::Acquire);
            if before & 1 != 0 {
                spin_loop();
                continue;
            }

            let insertion_id = self.insertion_id.load(Ordering::Acquire);
            let deletion_id = self.deletion_id.load(Ordering::Acquire);
            let after = self.generation.load(Ordering::Acquire);

            if before == after {
                return RowVersionSnapshot {
                    insertion_id,
                    deletion_id,
                };
            }

            spin_loop();
        }
    }

    #[inline]
    fn try_delete(&self, transaction_id: u64) -> bool {
        loop {
            let generation = self.generation.load(Ordering::Acquire);
            if generation & 1 != 0 {
                spin_loop();
                continue;
            }

            let current = self.deletion_id.load(Ordering::Acquire);
            if current != 0 {
                return current == transaction_id;
            }

            if self
                .generation
                .compare_exchange(
                    generation,
                    generation.wrapping_add(1),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                spin_loop();
                continue;
            }

            self.deletion_id.store(transaction_id, Ordering::Release);
            self.generation
                .store(generation.wrapping_add(2), Ordering::Release);
            return true;
        }
    }
}

/// Version information for a row or group of rows.
///
/// Readers take an acquire snapshot guarded by a generation word, so the
/// insertion/deletion pair is not assembled from unrelated relaxed moments.
#[derive(Debug)]
pub struct VersionInfo {
    header: RowVersionHeader,
}

impl VersionInfo {
    pub fn new(insertion_id: u64) -> Self {
        Self {
            header: RowVersionHeader::new(insertion_id),
        }
    }

    /// Check if a row is visible to a given transaction
    pub fn is_visible(&self, transaction_id: u64, start_time: u64) -> bool {
        let snapshot = self.header.snapshot();
        let ins_id = snapshot.insertion_id;
        let del_id = snapshot.deletion_id;

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
        let del_id = self.header.snapshot().deletion_id;
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
        self.header.try_delete(transaction_id)
    }
}

impl PartialEq for VersionInfo {
    fn eq(&self, other: &Self) -> bool {
        self.header.snapshot() == other.header.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::VersionInfo;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn visibility_follows_insert_and_delete_timestamps() {
        let version = VersionInfo::new(10);

        assert!(version.is_visible(99, 11));
        assert!(!version.is_visible(99, 10));
        assert!(version.try_delete(12));
        assert!(!version.is_visible(99, 13));
        assert!(version.is_visible(99, 11));
        assert!(!version.is_visible(12, 13));
    }

    #[test]
    fn concurrent_visibility_reads_do_not_observe_torn_header() {
        let version = Arc::new(VersionInfo::new(1));
        let mut readers = Vec::new();

        for _ in 0..4 {
            let version = version.clone();
            readers.push(thread::spawn(move || {
                for _ in 0..50_000 {
                    let _ = version.is_visible(0, 2);
                    let _ = version.check_conflict(0);
                }
            }));
        }

        assert!(version.try_delete(3));
        assert!(version.check_conflict(0));
        assert!(!version.is_visible(0, 4));

        for reader in readers {
            reader.join().expect("reader thread should not panic");
        }
    }
}
