// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::checkpoint::{CheckpointFrontier, RecoverySummary};

/// Stable durable-prefix checkpoint cut captured by the coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointCut {
    pub target_lsn: u64,
    pub issued_at_micros: u64,
}

/// Internal freeze contract shared by checkpoint writers.
///
/// Every writer must derive its frozen state from the same `CheckpointView`.
/// `catalog_snapshot_ts` pins catalog MVCC at the exact published prefix that
/// satisfied `cut.target_lsn`; future route-registry and tablet writers must
/// clone/freeze from this view rather than re-reading live runtime state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointView {
    pub cut: CheckpointCut,
    pub frontier: CheckpointFrontier,
    pub bootstrap: RecoverySummary,
    pub catalog_snapshot_ts: u64,
}

impl CheckpointView {
    pub fn catalog_snapshot_ts_for(bootstrap: &RecoverySummary) -> u64 {
        bootstrap.max_catalog_commit_id.saturating_add(1).max(1)
    }

    pub fn new(
        cut: CheckpointCut,
        frontier: CheckpointFrontier,
        bootstrap: RecoverySummary,
        catalog_snapshot_ts: u64,
    ) -> anyhow::Result<Self> {
        if frontier.checkpoint_lsn != cut.target_lsn {
            anyhow::bail!(
                "checkpoint frontier lsn {} does not match cut {}",
                frontier.checkpoint_lsn,
                cut.target_lsn
            );
        }
        if bootstrap.max_lsn != cut.target_lsn {
            anyhow::bail!(
                "checkpoint bootstrap max_lsn {} does not match cut {}",
                bootstrap.max_lsn,
                cut.target_lsn
            );
        }
        if frontier.checkpoint_commit_id != bootstrap.max_commit_id {
            anyhow::bail!(
                "checkpoint frontier commit_id {} does not match bootstrap {}",
                frontier.checkpoint_commit_id,
                bootstrap.max_commit_id
            );
        }
        if frontier.checkpoint_maintenance_id != bootstrap.max_maintenance_id {
            anyhow::bail!(
                "checkpoint frontier maintenance_id {} does not match bootstrap {}",
                frontier.checkpoint_maintenance_id,
                bootstrap.max_maintenance_id
            );
        }
        let expected_catalog_snapshot_ts = Self::catalog_snapshot_ts_for(&bootstrap);
        if catalog_snapshot_ts != expected_catalog_snapshot_ts {
            anyhow::bail!(
                "checkpoint catalog snapshot ts {} does not match bootstrap-derived {}",
                catalog_snapshot_ts,
                expected_catalog_snapshot_ts
            );
        }

        Ok(Self {
            cut,
            frontier,
            bootstrap,
            catalog_snapshot_ts,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_view_rejects_misaligned_frontier() {
        let cut = CheckpointCut {
            target_lsn: 17,
            issued_at_micros: 99,
        };
        let err = CheckpointView::new(
            cut,
            CheckpointFrontier {
                checkpoint_lsn: 16,
                checkpoint_commit_id: 7,
                checkpoint_maintenance_id: 8,
            },
            RecoverySummary {
                max_lsn: 17,
                max_commit_id: 7,
                max_maintenance_id: 8,
                max_catalog_commit_id: 9,
                max_seen_object_id: 10,
            },
            10,
        )
        .expect_err("frontier mismatch should fail");
        assert!(err.to_string().contains("frontier"));
    }
}
