// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Query-generation optimizer diagnostics. Components describe architectural
//! phases, never user-toggleable ordered passes.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::cascades::RuleId;
use paro_context::{OptimizerDiagnostic, SessionDiagnostics};

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
    rule_insertions: BTreeMap<RuleId, u64>,
    counters: BTreeMap<String, u64>,
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
    pub rule_insertions: BTreeMap<RuleId, u64>,
    pub counters: BTreeMap<String, u64>,
}

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
            rule_insertions: self.rule_insertions.clone(),
            counters: self.counters.clone(),
        }
    }

    pub fn record_rule_insertions(&mut self, insertions: BTreeMap<RuleId, u64>) {
        self.rule_insertions = insertions;
    }

    pub fn record_search_summary(&mut self, summary: &crate::cascades::SearchSummary) {
        self.counters
            .insert("memo_group_count".to_string(), summary.groups);
        self.counters.insert(
            "memo_logical_expression_count".to_string(),
            summary.logical_expressions,
        );
        self.counters.insert(
            "memo_physical_expression_count".to_string(),
            summary.physical_expressions,
        );
        for (dimension, count) in &summary.exhaustion_events {
            self.counters.insert(
                format!("budget_exhaustion_{}", dimension.stable_name()),
                *count,
            );
        }
    }
}

pub fn publish_optimizer_profile_snapshot(
    diagnostics: &SessionDiagnostics,
    snapshot: OptimizerProfileSnapshot,
) {
    let mut entries = snapshot
        .entries
        .into_iter()
        .map(|entry| OptimizerDiagnostic {
            name: entry.component.name().to_string(),
            kind: entry.component.kind().to_string(),
            last_elapsed_us: entry.last_elapsed.as_micros().min(i64::MAX as u128) as i64,
            invocation_count: entry.invocation_count.min(i64::MAX as u64) as i64,
        })
        .collect::<Vec<_>>();
    entries.extend(snapshot.rule_insertions.into_iter().map(|(rule, count)| {
        OptimizerDiagnostic {
            name: crate::cascades::rules::transformation_rule_name(rule)
                .map(str::to_string)
                .unwrap_or_else(|| format!("unknown_rule_{}", rule.0)),
            kind: "transformation_rule".to_string(),
            last_elapsed_us: 0,
            invocation_count: count.min(i64::MAX as u64) as i64,
        }
    }));
    entries.extend(
        snapshot
            .counters
            .into_iter()
            .map(|(name, count)| OptimizerDiagnostic {
                name,
                kind: "search_counter".to_string(),
                last_elapsed_us: 0,
                invocation_count: count.min(i64::MAX as u64) as i64,
            }),
    );
    diagnostics.publish_optimizer(entries);
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
