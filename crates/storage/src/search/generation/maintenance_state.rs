// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Manifest-visible maintenance state derivation.

use crate::search::capability::SearchIndexDefinition;
use crate::search::stats::{
    BuildWatermarks, CatchUpBacklogTier, GenerationMaintenanceState, GenerationRecoveryState,
    MaintenancePriority,
};
use crate::search::tail::{provider_tail_exact_merge_policy, TailPendingSet};

pub(crate) fn build_maintenance_state(
    definition: &SearchIndexDefinition,
    visible_version: i64,
    build_epoch: u64,
    indexed_rows: u64,
    tail_pending: &TailPendingSet,
    tombstone_rows: u64,
    previous_build_epoch: Option<u64>,
    mut superseded_build_epochs: Vec<u64>,
) -> GenerationMaintenanceState {
    let pending_rows = tail_pending.coverage_rows();
    let pending_rowsets = tail_pending.coverage_rowsets();
    let tail_policy = provider_tail_exact_merge_policy(definition.kind);
    let backlog_tier = if pending_rows <= tail_policy.soft_row_limit {
        CatchUpBacklogTier::Healthy
    } else if pending_rows <= tail_policy.hard_row_limit {
        CatchUpBacklogTier::Elevated
    } else {
        CatchUpBacklogTier::Degraded
    };
    let priority = if pending_rows == 0 {
        MaintenancePriority::Idle
    } else if pending_rows <= tail_policy.soft_row_limit {
        MaintenancePriority::Opportunistic
    } else if pending_rows <= tail_policy.hard_row_limit {
        MaintenancePriority::Elevated
    } else {
        MaintenancePriority::Critical
    };
    let (rowset_rate_limit, row_rate_limit) = catch_up_limits(definition.kind, priority);
    if let Some(previous_build_epoch) = previous_build_epoch.filter(|epoch| *epoch != build_epoch) {
        if !superseded_build_epochs.contains(&previous_build_epoch) {
            superseded_build_epochs.push(previous_build_epoch);
        }
    }
    if superseded_build_epochs.len() > 8 {
        let keep_from = superseded_build_epochs.len().saturating_sub(8);
        superseded_build_epochs = superseded_build_epochs.split_off(keep_from);
    }
    let tombstone_ratio_millis = if tombstone_rows == 0 {
        0
    } else {
        let denominator = indexed_rows.saturating_add(tombstone_rows).max(1);
        ((tombstone_rows as u128)
            .saturating_mul(1000)
            .saturating_div(denominator as u128))
        .min(u32::MAX as u128) as u32
    };

    GenerationMaintenanceState {
        build_watermarks: BuildWatermarks {
            snapshot_version: visible_version,
            replay_watermark: visible_version,
            cutover_watermark: visible_version,
        },
        recovery: GenerationRecoveryState {
            catch_up_build_epoch: (pending_rows > 0).then_some(build_epoch),
            superseded_build_epochs,
            tail_pending_rowsets: pending_rowsets,
            tail_pending_rows: pending_rows,
            backlog_tier,
            priority,
            rowset_rate_limit,
            row_rate_limit,
        },
        tombstone_rows,
        tombstone_ratio_millis,
    }
}

fn catch_up_limits(
    kind: crate::search::capability::SearchIndexKind,
    priority: MaintenancePriority,
) -> (usize, u64) {
    let tail_policy = provider_tail_exact_merge_policy(kind);
    let row_rate_limit = match priority {
        MaintenancePriority::Idle => 0,
        MaintenancePriority::Opportunistic => tail_policy.soft_row_limit.max(1),
        MaintenancePriority::Elevated | MaintenancePriority::Critical => {
            tail_policy.hard_row_limit.max(1)
        }
    };
    // Rows and bytes define provider build cost. Rowset count is only a
    // structural guard, so derive its ceiling from the row budget instead of
    // shrinking it as urgency rises. The old 8/4/2 policy turned a critical
    // backlog of small ingest rowsets into hundreds of tiny HNSW graphs and
    // manifest publications—the opposite of catch-up.
    let rowset_rate_limit = usize::try_from(row_rate_limit).unwrap_or(usize::MAX);
    (rowset_rate_limit, row_rate_limit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::capability::SearchIndexKind;

    #[test]
    fn urgent_catch_up_coalesces_every_rowset_that_fits_the_row_budget() {
        for priority in [
            MaintenancePriority::Opportunistic,
            MaintenancePriority::Elevated,
            MaintenancePriority::Critical,
        ] {
            let (rowsets, rows) = catch_up_limits(SearchIndexKind::Hnsw, priority);
            assert_eq!(rowsets, usize::try_from(rows).unwrap());
        }
        let (_, elevated_rows) =
            catch_up_limits(SearchIndexKind::Hnsw, MaintenancePriority::Elevated);
        let (_, critical_rows) =
            catch_up_limits(SearchIndexKind::Hnsw, MaintenancePriority::Critical);
        assert_eq!(critical_rows, elevated_rows);
    }
}
