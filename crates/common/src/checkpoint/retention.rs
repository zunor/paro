// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

/// Unified retention floor used by journal pruning, backup pinning, and
/// checkpoint-driven GC.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetentionFloor {
    pub checkpoint_lsn: u64,
    pub manual_keep_from_lsn: Option<u64>,
    pub backup_floor_lsn: Option<u64>,
    pub replication_floor_lsn: Option<u64>,
    pub pitr_floor_lsn: Option<u64>,
}

impl RetentionFloor {
    /// Earliest logical LSN that must remain replayable after retention.
    pub fn effective_replay_from_lsn(&self) -> u64 {
        let mut replay_from_lsn = self.checkpoint_lsn.saturating_add(1).max(1);
        for floor in [
            self.manual_keep_from_lsn,
            self.backup_floor_lsn,
            self.replication_floor_lsn,
            self.pitr_floor_lsn,
        ]
        .into_iter()
        .flatten()
        {
            replay_from_lsn = replay_from_lsn.min(floor.max(1));
        }
        replay_from_lsn
    }

    pub fn keeps_history_before_checkpoint_tail(&self) -> bool {
        self.effective_replay_from_lsn() < self.checkpoint_lsn.saturating_add(1).max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::RetentionFloor;

    #[test]
    fn retention_floor_defaults_to_checkpoint_tail() {
        let floor = RetentionFloor {
            checkpoint_lsn: 41,
            ..RetentionFloor::default()
        };
        assert_eq!(floor.effective_replay_from_lsn(), 42);
        assert!(!floor.keeps_history_before_checkpoint_tail());
    }

    #[test]
    fn retention_floor_honors_oldest_external_pin() {
        let floor = RetentionFloor {
            checkpoint_lsn: 100,
            manual_keep_from_lsn: Some(80),
            backup_floor_lsn: Some(50),
            replication_floor_lsn: Some(60),
            pitr_floor_lsn: None,
        };
        assert_eq!(floor.effective_replay_from_lsn(), 50);
        assert!(floor.keeps_history_before_checkpoint_tail());
    }
}
