// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Query-generation optimizer diagnostics. Components describe architectural
//! phases, never user-toggleable ordered passes.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::cascades::RuleId;
use paro_context::{OptimizerDiagnostic, OptimizerMetricUnit, SessionDiagnostics};

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
    rule_attempts: BTreeMap<RuleId, u64>,
    rule_insertions: BTreeMap<RuleId, u64>,
    rule_elapsed: BTreeMap<RuleId, Duration>,
    rule_allocated_bytes: BTreeMap<RuleId, u64>,
    rule_budget_exhaustions: BTreeMap<RuleId, u64>,
    component_allocated_bytes: BTreeMap<OptimizerComponent, u64>,
    counters: BTreeMap<String, u64>,
    physical_search: crate::cascades::memo::PhysicalSearchProfile,
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
    pub rule_attempts: BTreeMap<RuleId, u64>,
    pub rule_insertions: BTreeMap<RuleId, u64>,
    pub rule_elapsed: BTreeMap<RuleId, Duration>,
    pub rule_allocated_bytes: BTreeMap<RuleId, u64>,
    pub rule_budget_exhaustions: BTreeMap<RuleId, u64>,
    pub component_allocated_bytes: BTreeMap<OptimizerComponent, u64>,
    pub counters: BTreeMap<String, u64>,
    pub physical_search: crate::cascades::memo::PhysicalSearchProfile,
}

impl OptimizerProfiler {
    pub fn record(&mut self, component: impl Into<OptimizerComponent>, elapsed: Duration) {
        let component = component.into();
        let entry = self.entries.entry(component).or_default();
        entry.last_elapsed = elapsed;
        entry.invocation_count = entry.invocation_count.saturating_add(1);
    }

    pub fn record_component_allocation(
        &mut self,
        component: OptimizerComponent,
        allocated_bytes: u64,
    ) {
        self.component_allocated_bytes
            .insert(component, allocated_bytes);
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
            rule_elapsed: self.rule_elapsed.clone(),
            rule_allocated_bytes: self.rule_allocated_bytes.clone(),
            rule_budget_exhaustions: self.rule_budget_exhaustions.clone(),
            component_allocated_bytes: self.component_allocated_bytes.clone(),
            rule_attempts: self.rule_attempts.clone(),
            counters: self.counters.clone(),
            physical_search: self.physical_search.clone(),
        }
    }

    pub fn record_rule_insertions(&mut self, insertions: BTreeMap<RuleId, u64>) {
        self.rule_insertions = insertions;
    }

    pub fn record_rule_attempts(&mut self, attempts: BTreeMap<RuleId, u64>) {
        self.rule_attempts = attempts;
    }

    pub fn record_rule_elapsed(&mut self, elapsed: BTreeMap<RuleId, Duration>) {
        self.rule_elapsed = elapsed;
    }

    pub fn record_rule_allocated_bytes(&mut self, allocated: BTreeMap<RuleId, u64>) {
        self.rule_allocated_bytes = allocated;
    }

    pub fn record_rule_budget_exhaustions(&mut self, exhausted: BTreeMap<RuleId, u64>) {
        self.rule_budget_exhaustions = exhausted;
    }

    pub fn record_search_summary(&mut self, summary: &crate::cascades::SearchSummary) {
        self.physical_search = summary.physical_search.clone();
        self.counters.insert(
            "search_complete".to_string(),
            u64::from(summary.is_complete()),
        );
        self.counters.insert(
            "search_rule_failure_count".to_string(),
            summary
                .obligations
                .iter()
                .filter(|obligation| {
                    matches!(
                        obligation.reason,
                        crate::cascades::budget::SearchIncompleteReason::RuleFailure { .. }
                    )
                })
                .count() as u64,
        );
        self.counters.insert(
            "search_deadline_reached".to_string(),
            u64::from(summary.obligations.iter().any(|obligation| {
                obligation.reason == crate::cascades::budget::SearchIncompleteReason::Deadline
            })),
        );
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
        for (name, count) in &summary.work_counters {
            self.counters.insert((*name).to_string(), *count);
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
            metric_value: entry.invocation_count.min(i64::MAX as u64) as i64,
            metric_unit: OptimizerMetricUnit::Invocations,
            invocation_count: entry.invocation_count.min(i64::MAX as u64) as i64,
        })
        .collect::<Vec<_>>();
    entries.extend(
        snapshot
            .component_allocated_bytes
            .into_iter()
            .map(|(component, bytes)| OptimizerDiagnostic {
                name: format!("allocation_bytes_{}", component.name()),
                kind: "allocation".to_string(),
                last_elapsed_us: 0,
                metric_value: bytes.min(i64::MAX as u64) as i64,
                metric_unit: OptimizerMetricUnit::Bytes,
                invocation_count: 0,
            }),
    );
    let rule_elapsed = snapshot.rule_elapsed;
    entries.extend(
        snapshot
            .rule_allocated_bytes
            .into_iter()
            .map(|(rule, bytes)| OptimizerDiagnostic {
                name: crate::cascades::rules::transformation_rule_name(rule)
                    .map(|name| format!("allocation_bytes_{name}"))
                    .unwrap_or_else(|| format!("allocation_bytes_unknown_rule_{}", rule.0)),
                kind: "search_counter".to_string(),
                last_elapsed_us: 0,
                metric_value: bytes.min(i64::MAX as u64) as i64,
                metric_unit: OptimizerMetricUnit::Bytes,
                invocation_count: 0,
            }),
    );
    entries.extend(
        snapshot
            .rule_budget_exhaustions
            .into_iter()
            .map(|(rule, count)| OptimizerDiagnostic {
                name: crate::cascades::rules::transformation_rule_name(rule)
                    .map(|name| format!("budget_exhaustion_{name}"))
                    .unwrap_or_else(|| format!("budget_exhaustion_unknown_rule_{}", rule.0)),
                kind: "search_counter".to_string(),
                last_elapsed_us: 0,
                metric_value: count.min(i64::MAX as u64) as i64,
                metric_unit: OptimizerMetricUnit::Count,
                invocation_count: 0,
            }),
    );
    entries.extend(snapshot.rule_insertions.into_iter().map(|(rule, count)| {
        OptimizerDiagnostic {
            name: crate::cascades::rules::transformation_rule_name(rule)
                .map(str::to_string)
                .unwrap_or_else(|| format!("unknown_rule_{}", rule.0)),
            kind: "transformation_rule".to_string(),
            last_elapsed_us: rule_elapsed
                .get(&rule)
                .copied()
                .unwrap_or_default()
                .as_micros()
                .min(i64::MAX as u128) as i64,
            metric_value: count.min(i64::MAX as u64) as i64,
            metric_unit: OptimizerMetricUnit::Insertions,
            invocation_count: 0,
        }
    }));
    entries.extend(snapshot.rule_attempts.into_iter().map(|(rule, count)| {
        OptimizerDiagnostic {
            name: crate::cascades::rules::transformation_rule_name(rule)
                .map(str::to_string)
                .unwrap_or_else(|| format!("unknown_rule_{}", rule.0)),
            kind: "transformation_rule_attempt".to_string(),
            last_elapsed_us: rule_elapsed
                .get(&rule)
                .copied()
                .unwrap_or_default()
                .as_micros()
                .min(i64::MAX as u128) as i64,
            metric_value: count.min(i64::MAX as u64) as i64,
            metric_unit: OptimizerMetricUnit::Attempts,
            invocation_count: 0,
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
                metric_value: count.min(i64::MAX as u64) as i64,
                metric_unit: OptimizerMetricUnit::Count,
                invocation_count: 0,
            }),
    );
    entries.extend(physical_search_diagnostics(&snapshot.physical_search));
    diagnostics.publish_optimizer(entries);
}

fn physical_search_diagnostics(
    profile: &crate::cascades::memo::PhysicalSearchProfile,
) -> Vec<OptimizerDiagnostic> {
    let mut entries = Vec::new();
    let mut push = |name: String, kind: &str, value: u64, unit| {
        entries.push(OptimizerDiagnostic {
            name,
            kind: kind.to_string(),
            last_elapsed_us: 0,
            metric_value: value.min(i64::MAX as u64) as i64,
            metric_unit: unit,
            invocation_count: 0,
        })
    };
    push(
        "memo_group_merge_count".into(),
        "search_counter",
        profile.group_merges,
        OptimizerMetricUnit::Count,
    );
    push(
        "physical_goal_count".into(),
        "search_counter",
        profile.frontiers.len() as u64,
        OptimizerMetricUnit::Count,
    );
    let mut by_group = BTreeMap::<_, (u64, usize)>::new();
    let mut histogram = BTreeMap::<usize, u64>::new();
    for frontier in &profile.frontiers {
        let group = by_group.entry(frontier.group).or_default();
        group.0 += 1;
        group.1 = group.1.max(frontier.candidates);
        *histogram.entry(frontier.candidates).or_default() += 1;
        let goal = frontier.goal;
        let prefix = format!(
            "group_{}_required_{}_rows_{}_objective_{}_grant_{}_context_{}_sources_{}",
            frontier.group.0,
            goal.required.0,
            goal.row_goal.stable_tag(),
            goal.objective.stable_tag(),
            goal.grant.stable_tag(),
            goal.context.0,
            frontier.demanded_sources
        );
        for (metric, value) in [
            ("size", frontier.candidates as u64),
            ("high_water", frontier.high_water as u64),
            ("proposals", frontier.proposals),
            ("truncations", frontier.truncations),
        ] {
            push(
                format!("{prefix}_{metric}"),
                "search_frontier",
                value,
                OptimizerMetricUnit::Count,
            );
        }
    }
    for (size, count) in histogram {
        push(
            format!("physical_frontiers_size_{size}"),
            "search_counter",
            count,
            OptimizerMetricUnit::Count,
        );
    }
    for group in &profile.groups {
        let (goals, maximum) = by_group.get(&group.group).copied().unwrap_or_default();
        for (metric, value) in [
            ("goals", goals),
            ("max_frontier", maximum as u64),
            ("proposals", group.proposals),
            ("archived", group.archived_candidates),
        ] {
            push(
                format!("group_{}_{}", group.group.0, metric),
                "search_group",
                value,
                OptimizerMetricUnit::Count,
            );
        }
        push(
            format!("group_{}_source_payload_bytes", group.group.0),
            "search_group",
            group.source_payload_bytes,
            OptimizerMetricUnit::Bytes,
        );
    }
    push(
        "winner_source_payload_bytes".into(),
        "search_bytes",
        profile
            .groups
            .iter()
            .map(|group| group.source_payload_bytes)
            .sum(),
        OptimizerMetricUnit::Bytes,
    );
    entries
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn physical_search_attribution_distinguishes_goals_frontiers_and_byte_payloads() {
        use crate::cascades::memo::{
            GrantGoalKey, OptimizationGoal, PhysicalFrontierProfile, PhysicalGroupProfile,
            PhysicalSearchProfile, RowGoal,
        };
        use crate::cascades::{
            AdmissibleGrantSetId, GroupId, OptimizationContextId, PropertySetId,
        };
        let group = GroupId(7);
        let goal = OptimizationGoal {
            required: PropertySetId(0),
            row_goal: RowGoal::All,
            objective: crate::physical::ObjectiveProfile::Latency,
            grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
            context: OptimizationContextId(0),
        };
        let profile = PhysicalSearchProfile {
            group_merges: 2,
            groups: Box::new([PhysicalGroupProfile {
                group,
                proposals: 50,
                archived_candidates: 13,
                source_payload_bytes: 4096,
            }]),
            frontiers: Box::new([
                PhysicalFrontierProfile {
                    group,
                    goal,
                    candidates: 1,
                    demanded_sources: 0,
                    proposals: 20,
                    truncations: 0,
                    high_water: 1,
                },
                PhysicalFrontierProfile {
                    group,
                    goal: OptimizationGoal {
                        context: OptimizationContextId(1),
                        ..goal
                    },
                    candidates: 8,
                    demanded_sources: 3,
                    proposals: 30,
                    truncations: 5,
                    high_water: 9,
                },
            ]),
        };
        let entries = physical_search_diagnostics(&profile);
        let find = |name: &str| entries.iter().find(|entry| entry.name == name).unwrap();
        assert_eq!(find("physical_goal_count").metric_value, 2);
        assert_eq!(find("group_7_goals").metric_value, 2);
        assert_eq!(find("group_7_max_frontier").metric_value, 8);
        assert_eq!(find("physical_frontiers_size_8").metric_value, 1);
        assert_eq!(
            find("winner_source_payload_bytes").metric_unit,
            OptimizerMetricUnit::Bytes
        );
        assert_eq!(find("winner_source_payload_bytes").metric_value, 4096);
        assert!(entries.iter().any(|entry| entry.kind == "search_frontier"
            && entry.name.ends_with("sources_3_truncations")
            && entry.metric_value == 5));
        assert!(entries.iter().all(|entry| entry.invocation_count == 0));
    }

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

    #[test]
    fn search_completion_is_published_as_an_explicit_counter() {
        let mut profiler = OptimizerProfiler::default();
        profiler.record_search_summary(&crate::cascades::SearchSummary {
            groups: 1,
            logical_expressions: 1,
            physical_expressions: 1,
            exhaustion_events: [(crate::cascades::budget::BudgetDimension::Group, 1)]
                .into_iter()
                .collect(),
            work_counters: [("transformation_binding_count", 7)].into_iter().collect(),
            obligations: Box::new([]),
            physical_search: Default::default(),
        });
        let snapshot = profiler.snapshot();
        assert_eq!(snapshot.counters.get("search_complete"), Some(&0));
        assert_eq!(snapshot.counters.get("budget_exhaustion_group"), Some(&1));
        assert_eq!(
            snapshot.counters.get("transformation_binding_count"),
            Some(&7)
        );
    }

    #[test]
    fn rule_elapsed_is_preserved_independently_of_attempt_and_insertion_counts() {
        let rule = RuleId(91);
        let mut profiler = OptimizerProfiler::default();
        profiler.record_rule_attempts(BTreeMap::from([(rule, 7)]));
        profiler.record_rule_insertions(BTreeMap::from([(rule, 2)]));
        profiler.record_rule_elapsed(BTreeMap::from([(rule, Duration::from_micros(37))]));
        profiler.record_rule_allocated_bytes(BTreeMap::from([(rule, 8192)]));
        profiler.record_rule_budget_exhaustions(BTreeMap::from([(rule, 3)]));

        let snapshot = profiler.snapshot();
        assert_eq!(snapshot.rule_attempts.get(&rule), Some(&7));
        assert_eq!(snapshot.rule_insertions.get(&rule), Some(&2));
        assert_eq!(
            snapshot.rule_elapsed.get(&rule),
            Some(&Duration::from_micros(37))
        );
        assert_eq!(snapshot.rule_allocated_bytes.get(&rule), Some(&8192));
        assert_eq!(snapshot.rule_budget_exhaustions.get(&rule), Some(&3));
    }

    #[test]
    fn component_allocation_is_published_as_a_typed_metric() {
        let mut profiler = OptimizerProfiler::default();
        profiler.record_component_allocation(OptimizerComponent::MemoExploration, 4096);
        assert_eq!(
            profiler
                .snapshot()
                .component_allocated_bytes
                .get(&OptimizerComponent::MemoExploration),
            Some(&4096)
        );
    }

    #[test]
    fn non_timing_metric_never_reuses_invocation_field() {
        let diagnostics = SessionDiagnostics::default();
        let mut profiler = OptimizerProfiler::default();
        profiler.record_rule_insertions(BTreeMap::from([(RuleId(7), 3)]));
        publish_optimizer_profile_snapshot(&diagnostics, profiler.snapshot());
        let row = diagnostics
            .optimizer_snapshot()
            .into_iter()
            .find(|row| row.name == "unknown_rule_7")
            .expect("rule insertion metric");
        assert_eq!(row.metric_unit, OptimizerMetricUnit::Insertions);
        assert_eq!(row.metric_value, 3);
        assert_eq!(row.invocation_count, 0);
    }
}
