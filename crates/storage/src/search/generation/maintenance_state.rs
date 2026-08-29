// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Manifest-visible maintenance state derivation.

use crate::search::capability::SearchIndexDefinition;
use crate::search::stats::{
    BuildWatermarks, CatchUpBacklogTier, GenerationMaintenanceState, GenerationRecoveryState,
    MaintenancePriority,
};
use paro_common::error::Result;

use crate::search::tail::{provider_tail_exact_merge_policy, TailPendingSet};
use crate::search::{HnswProviderConfig, SearchIndexKind};

#[derive(Debug, Clone, Copy)]
struct ProviderMaintenanceWatermarks {
    target_rows: u64,
    max_pending_rows: u64,
}

fn provider_maintenance_watermarks(
    definition: &SearchIndexDefinition,
    hnsw_provider: Option<&HnswProviderConfig>,
) -> Result<ProviderMaintenanceWatermarks> {
    if definition.kind == SearchIndexKind::Hnsw {
        let config = hnsw_provider.ok_or_else(|| {
            paro_common::error::internal(
                "HNSW maintenance state requires the registry-decoded provider contract",
            )
        })?;
        return Ok(ProviderMaintenanceWatermarks {
            target_rows: config.maintenance_target_rows(),
            max_pending_rows: config.maintenance.max_pending_rows(config.dimension),
        });
    }
    let exact_tail = provider_tail_exact_merge_policy(definition.kind);
    Ok(ProviderMaintenanceWatermarks {
        target_rows: exact_tail.soft_row_limit.max(1),
        max_pending_rows: exact_tail.hard_row_limit.max(1),
    })
}

pub(crate) fn build_maintenance_state(
    definition: &SearchIndexDefinition,
    hnsw_provider: Option<&HnswProviderConfig>,
    visible_version: i64,
    build_epoch: u64,
    indexed_rows: u64,
    tail_pending: &TailPendingSet,
    tombstone_rows: u64,
    previous_build_epoch: Option<u64>,
    mut superseded_build_epochs: Vec<u64>,
) -> Result<GenerationMaintenanceState> {
    let pending_rows = tail_pending.coverage_rows();
    let pending_rowsets = tail_pending.coverage_rowsets();
    let maintenance = provider_maintenance_watermarks(definition, hnsw_provider)?;
    let backlog_tier = if pending_rows <= maintenance.target_rows {
        CatchUpBacklogTier::Healthy
    } else if pending_rows <= maintenance.max_pending_rows {
        CatchUpBacklogTier::Elevated
    } else {
        CatchUpBacklogTier::Degraded
    };
    let priority = if pending_rows == 0 {
        MaintenancePriority::Idle
    } else if pending_rows <= maintenance.target_rows {
        MaintenancePriority::Opportunistic
    } else if pending_rows <= maintenance.max_pending_rows {
        MaintenancePriority::Elevated
    } else {
        MaintenancePriority::Critical
    };
    let (rowset_rate_limit, row_rate_limit) = catch_up_limits(maintenance, priority);
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

    Ok(GenerationMaintenanceState {
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
    })
}

fn catch_up_limits(
    maintenance: ProviderMaintenanceWatermarks,
    priority: MaintenancePriority,
) -> (usize, u64) {
    let row_rate_limit = match priority {
        MaintenancePriority::Idle => 0,
        MaintenancePriority::Opportunistic
        | MaintenancePriority::Elevated
        | MaintenancePriority::Critical => maintenance.target_rows.max(1),
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

    #[test]
    fn urgent_catch_up_coalesces_every_rowset_that_fits_the_row_budget() {
        let maintenance = ProviderMaintenanceWatermarks {
            target_rows: 524_288,
            max_pending_rows: 2_097_152,
        };
        for priority in [
            MaintenancePriority::Opportunistic,
            MaintenancePriority::Elevated,
            MaintenancePriority::Critical,
        ] {
            let (rowsets, rows) = catch_up_limits(maintenance, priority);
            assert_eq!(rowsets, usize::try_from(rows).unwrap());
        }
        let (_, elevated_rows) = catch_up_limits(maintenance, MaintenancePriority::Elevated);
        let (_, critical_rows) = catch_up_limits(maintenance, MaintenancePriority::Critical);
        assert_eq!(critical_rows, elevated_rows);
        assert_eq!(critical_rows, 524_288);
    }
}
