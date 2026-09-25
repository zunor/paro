// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Architectural stage timings. Observation never selects a planning strategy.

use paro_context::{OptimizerDiagnostic, OptimizerMetricUnit, SessionDiagnostics};
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OptimizerComponent {
    SemanticNormalization,
    RegionOptimization,
    PhysicalSelection,
    PhysicalExtraction,
}

impl OptimizerComponent {
    pub const ALL: [Self; 4] = [
        Self::SemanticNormalization,
        Self::RegionOptimization,
        Self::PhysicalSelection,
        Self::PhysicalExtraction,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::SemanticNormalization => "semantic_normalization",
            Self::RegionOptimization => "region_optimization",
            Self::PhysicalSelection => "physical_selection",
            Self::PhysicalExtraction => "physical_extraction",
        }
    }

    pub const fn kind(self) -> &'static str {
        match self {
            Self::SemanticNormalization => "frontend",
            Self::RegionOptimization | Self::PhysicalSelection => "planning",
            Self::PhysicalExtraction => "extraction",
        }
    }
}

#[derive(Debug, Default)]
pub struct OptimizerProfiler {
    entries: BTreeMap<OptimizerComponent, (Duration, u64)>,
}

#[derive(Debug, Clone)]
pub struct OptimizerProfileSnapshotEntry {
    pub component: OptimizerComponent,
    pub last_elapsed: Duration,
    pub invocation_count: u64,
}

#[derive(Debug, Clone, Default)]
pub struct OptimizerProfileSnapshot {
    pub entries: Vec<OptimizerProfileSnapshotEntry>,
}

impl OptimizerProfiler {
    pub fn record(&mut self, component: OptimizerComponent, elapsed: Duration) {
        let entry = self.entries.entry(component).or_default();
        entry.0 = elapsed;
        entry.1 = entry.1.saturating_add(1);
    }

    pub fn into_snapshot(self) -> OptimizerProfileSnapshot {
        OptimizerProfileSnapshot {
            entries: OptimizerComponent::ALL
                .into_iter()
                .map(|component| {
                    let (last_elapsed, invocation_count) =
                        self.entries.get(&component).copied().unwrap_or_default();
                    OptimizerProfileSnapshotEntry {
                        component,
                        last_elapsed,
                        invocation_count,
                    }
                })
                .collect(),
        }
    }
}

pub fn publish_optimizer_profile_snapshot(
    diagnostics: &SessionDiagnostics,
    snapshot: OptimizerProfileSnapshot,
) {
    diagnostics.publish_optimizer(
        snapshot
            .entries
            .into_iter()
            .map(|entry| OptimizerDiagnostic {
                name: entry.component.name().to_string(),
                kind: entry.component.kind().to_string(),
                last_elapsed_us: entry.last_elapsed.as_micros().min(i64::MAX as u128) as i64,
                metric_value: entry.invocation_count.min(i64::MAX as u64) as i64,
                metric_unit: OptimizerMetricUnit::Invocations,
                invocation_count: entry.invocation_count.min(i64::MAX as u64) as i64,
            })
            .collect(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_records_invocations_without_inventing_work_in_absent_stages() {
        let mut report = OptimizerProfiler::default();
        report.record(
            OptimizerComponent::RegionOptimization,
            Duration::from_micros(7),
        );
        report.record(
            OptimizerComponent::RegionOptimization,
            Duration::from_micros(9),
        );
        let snapshot = report.into_snapshot();
        assert_eq!(snapshot.entries.len(), OptimizerComponent::ALL.len());
        for entry in snapshot.entries {
            if entry.component == OptimizerComponent::RegionOptimization {
                assert_eq!(entry.invocation_count, 2);
                assert_eq!(entry.last_elapsed, Duration::from_micros(9));
            } else {
                assert_eq!(entry.invocation_count, 0);
                assert_eq!(entry.last_elapsed, Duration::ZERO);
            }
        }
    }
}
