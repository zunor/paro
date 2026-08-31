//! Query-generation optimizer diagnostics. Components describe architectural
//! phases, never user-toggleable ordered passes.

use std::collections::BTreeMap;
use std::sync::{LazyLock, RwLock};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OptimizerComponent {
    SemanticNormalization,
    QueryIrConstruction,
    DirectPhysicalSearch,
    MemoExploration,
    PhysicalExtraction,
    WinnerVerification,
}

impl OptimizerComponent {
    pub const ALL: [Self; 6] = [
        Self::SemanticNormalization,
        Self::QueryIrConstruction,
        Self::DirectPhysicalSearch,
        Self::MemoExploration,
        Self::PhysicalExtraction,
        Self::WinnerVerification,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::SemanticNormalization => "semantic_normalization",
            Self::QueryIrConstruction => "query_ir_construction",
            Self::DirectPhysicalSearch => "direct_physical_search",
            Self::MemoExploration => "memo_exploration",
            Self::PhysicalExtraction => "physical_extraction",
            Self::WinnerVerification => "winner_verification",
        }
    }

    pub const fn kind(self) -> &'static str {
        match self {
            Self::SemanticNormalization | Self::QueryIrConstruction => "frontend",
            Self::DirectPhysicalSearch | Self::MemoExploration => "search",
            Self::PhysicalExtraction => "extraction",
            Self::WinnerVerification => "verification",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct OptimizerTimingEntry {
    pub last_elapsed: Duration,
    pub invocation_count: u64,
}

#[derive(Debug, Default)]
pub struct OptimizerProfiler {
    entries: BTreeMap<OptimizerComponent, OptimizerTimingEntry>,
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

static LAST_PROFILE_SNAPSHOT: LazyLock<RwLock<OptimizerProfileSnapshot>> =
    LazyLock::new(|| RwLock::new(OptimizerProfileSnapshot::default()));

impl OptimizerProfiler {
    pub fn record(&mut self, component: impl Into<OptimizerComponent>, elapsed: Duration) {
        let component = component.into();
        let entry = self.entries.entry(component).or_default();
        entry.last_elapsed = elapsed;
        entry.invocation_count = entry.invocation_count.saturating_add(1);
    }

    pub fn snapshot(&self) -> OptimizerProfileSnapshot {
        OptimizerProfileSnapshot {
            entries: OptimizerComponent::ALL
                .into_iter()
                .map(|component| {
                    let entry = self.entries.get(&component).cloned().unwrap_or_default();
                    OptimizerProfileSnapshotEntry {
                        component,
                        last_elapsed: entry.last_elapsed,
                        invocation_count: entry.invocation_count,
                    }
                })
                .collect(),
        }
    }
}

pub fn publish_optimizer_profile_snapshot(snapshot: OptimizerProfileSnapshot) {
    *LAST_PROFILE_SNAPSHOT.write().unwrap() = snapshot;
}

pub fn latest_optimizer_profile_snapshot() -> OptimizerProfileSnapshot {
    LAST_PROFILE_SNAPSHOT.read().unwrap().clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_has_stable_architectural_components_not_pass_toggles() {
        let mut profiler = OptimizerProfiler::default();
        profiler.record(
            OptimizerComponent::MemoExploration,
            Duration::from_micros(7),
        );
        let snapshot = profiler.snapshot();
        assert_eq!(snapshot.entries.len(), OptimizerComponent::ALL.len());
        let memo = snapshot
            .entries
            .iter()
            .find(|entry| entry.component == OptimizerComponent::MemoExploration)
            .unwrap();
        assert_eq!(memo.invocation_count, 1);
        assert_eq!(memo.last_elapsed, Duration::from_micros(7));
    }
}
