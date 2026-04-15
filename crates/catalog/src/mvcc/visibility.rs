// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_storage::transaction::manager::TRANSACTION_ID_START;

pub fn is_permanent(timestamp: u64) -> bool {
    timestamp == 0
}

pub fn is_provisional(timestamp: u64) -> bool {
    timestamp >= TRANSACTION_ID_START
}

pub fn is_committed(timestamp: u64) -> bool {
    timestamp < TRANSACTION_ID_START
}

pub fn is_visible(timestamp: u64, writer_id: Option<u64>, start_time: u64) -> bool {
    if is_permanent(timestamp) {
        return true;
    }

    if is_provisional(timestamp) {
        return writer_id.is_some_and(|id| id == timestamp);
    }

    timestamp < start_time
}

pub fn has_conflict(timestamp: u64, writer_id: Option<u64>, start_time: u64) -> bool {
    if is_permanent(timestamp) {
        return false;
    }

    if is_provisional(timestamp) {
        return writer_id != Some(timestamp);
    }

    timestamp >= start_time
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provisional_visibility_and_conflict_follow_writer_identity() {
        let writer_id = Some(TRANSACTION_ID_START + 7);

        assert!(is_visible(TRANSACTION_ID_START + 7, writer_id, 100));
        assert!(!has_conflict(TRANSACTION_ID_START + 7, writer_id, 100));

        assert!(!is_visible(TRANSACTION_ID_START + 8, writer_id, 100));
        assert!(has_conflict(TRANSACTION_ID_START + 8, writer_id, 100));
    }

    #[test]
    fn committed_boundary_is_strictly_before_snapshot_start() {
        let snapshot_start = 100;

        assert!(is_visible(99, None, snapshot_start));
        assert!(!has_conflict(99, None, snapshot_start));

        assert!(!is_visible(100, None, snapshot_start));
        assert!(has_conflict(100, None, snapshot_start));
    }

    #[test]
    fn read_only_snapshot_never_sees_provisional_versions() {
        assert!(!is_visible(TRANSACTION_ID_START + 3, None, 100));
        assert!(has_conflict(TRANSACTION_ID_START + 3, None, 100));
    }

    #[test]
    fn permanent_versions_are_always_visible_and_never_conflicting() {
        assert!(is_visible(0, None, 1));
        assert!(is_visible(0, Some(TRANSACTION_ID_START + 1), 1));
        assert!(!has_conflict(0, None, 1));
        assert!(!has_conflict(0, Some(TRANSACTION_ID_START + 1), 1));
    }
}
