// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Stable rule and implementation registries used by Direct and Memo search.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use paro_common::error::{self as paro_error, Result};

use super::cost::SearchCost;
use super::ids::{
    Fingerprint, GroupId, ImplementationId, LogicalExprId, LogicalPayloadId, PhysicalPayloadId,
    RuleId, StableFingerprintBuilder,
};
use super::memo::{
    EquivalenceProof, LogicalExpr, LogicalExprKey, Memo, OptimizationGoal, PhysicalExprKey,
};
use super::properties::ProvidedProperties;
use super::region::RegionCandidateContract;

pub const EXPENSIVE_PREDICATE_PLACEMENT_RULE: RuleId = RuleId(10_006);
pub const CTE_INLINE_RULE: RuleId = RuleId(10_007);
pub const CTE_DEMAND_PUSHDOWN_RULE: RuleId = RuleId(10_008);
pub const AGGREGATE_POST_REDUCTION_RULE: RuleId = RuleId(10_009);
pub const MARK_JOIN_TO_SEMI_RULE: RuleId = RuleId(10_010);
pub const JOIN_ELIMINATION_RULE: RuleId = RuleId(10_011);
pub const AGGREGATE_JOIN_PREAGGREGATION_RULE: RuleId = RuleId(10_012);
pub const AGGREGATE_JOIN_SUBSUMPTION_RULE: RuleId = RuleId(10_013);
pub const AGGREGATE_NON_NULL_INPUT_RULE: RuleId = RuleId(10_014);
pub const AGGREGATE_DIMENSION_DEFERRAL_RULE: RuleId = RuleId(10_015);
pub const AGGREGATE_INPUT_MATERIALIZATION_RULE: RuleId = RuleId(10_016);
pub const LIMIT_PUSHDOWN_RULE: RuleId = RuleId(10_017);
pub const LATE_PAYLOAD_FETCH_RULE: RuleId = RuleId(10_018);
pub const SCALAR_AGGREGATE_WINDOW_RULE: RuleId = RuleId(10_019);
pub const JOIN_REGION_ENUMERATION_RULE: RuleId = RuleId(10_021);
pub const TOP_N_INTRODUCTION_RULE: RuleId = RuleId(10_022);
pub const CTE_FILTER_PUSHDOWN_RULE: RuleId = RuleId(10_023);
pub const CTE_PARTITIONED_MATERIALIZATION_RULE: RuleId = RuleId(10_024);

const TRANSFORMATION_RULE_NAMES: &[(RuleId, &str)] = &[
    (
        EXPENSIVE_PREDICATE_PLACEMENT_RULE,
        "expensive_predicate_placement",
    ),
    (CTE_INLINE_RULE, "cte_inline"),
    (CTE_DEMAND_PUSHDOWN_RULE, "cte_demand_pushdown"),
    (CTE_FILTER_PUSHDOWN_RULE, "cte_filter_pushdown"),
    (
        CTE_PARTITIONED_MATERIALIZATION_RULE,
        "cte_partitioned_materialization",
    ),
    (AGGREGATE_POST_REDUCTION_RULE, "aggregate_post_reduction"),
    (MARK_JOIN_TO_SEMI_RULE, "mark_join_to_semi"),
    (JOIN_ELIMINATION_RULE, "join_elimination"),
    (
        AGGREGATE_JOIN_PREAGGREGATION_RULE,
        "aggregate_join_preaggregation",
    ),
    (
        AGGREGATE_JOIN_SUBSUMPTION_RULE,
        "aggregate_join_subsumption",
    ),
    (AGGREGATE_NON_NULL_INPUT_RULE, "aggregate_non_null_input"),
    (
        AGGREGATE_DIMENSION_DEFERRAL_RULE,
        "aggregate_dimension_deferral",
    ),
    (
        AGGREGATE_INPUT_MATERIALIZATION_RULE,
        "aggregate_input_materialization",
    ),
    (LIMIT_PUSHDOWN_RULE, "limit_pushdown"),
    (LATE_PAYLOAD_FETCH_RULE, "late_payload_fetch"),
    (SCALAR_AGGREGATE_WINDOW_RULE, "scalar_aggregate_window"),
    (JOIN_REGION_ENUMERATION_RULE, "join_region_enumeration"),
    (TOP_N_INTRODUCTION_RULE, "top_n_introduction"),
];

pub fn transformation_rule_name(id: RuleId) -> Option<&'static str> {
    TRANSFORMATION_RULE_NAMES
        .iter()
        .find_map(|(candidate, name)| (*candidate == id).then_some(*name))
}

pub fn transformation_rule_id(name: &str) -> Option<RuleId> {
    TRANSFORMATION_RULE_NAMES
        .iter()
        .find_map(|(id, candidate)| candidate.eq_ignore_ascii_case(name.trim()).then_some(*id))
}

pub fn validate_transformation_rule_names(names: &str) -> Result<()> {
    for name in names
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
    {
        if transformation_rule_id(name).is_none() {
            return Err(paro_error::invalid_input(format!(
                "unknown optimizer transformation rule '{name}'"
            )));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RulePromise {
    pub priority: u16,
}

impl RulePromise {
    pub const HIGH: Self = Self { priority: 100 };
    pub const NORMAL: Self = Self { priority: 1_000 };
    pub const LOW: Self = Self { priority: 10_000 };
}

#[derive(Debug, Clone)]
pub struct EquivalentExpression {
    pub target_group: GroupId,
    pub key: LogicalExprKey,
    pub payload: LogicalPayloadId,
    pub logical_properties: super::memo::LogicalProperties,
    pub cardinality: super::memo::GroupCardinality,
    pub proof: EquivalenceProof,
}

pub struct RuleContext<'a> {
    pub memo: &'a Memo,
    pub group: GroupId,
}

type TransformationRollback = Box<dyn FnOnce() -> Result<()> + 'static>;

pub struct TransformContext<'a> {
    memo: &'a mut Memo,
    group: GroupId,
    memo_savepoint: Option<super::memo::TransformationSavepoint>,
    sidecar_rollbacks: Vec<TransformationRollback>,
}

impl<'a> TransformContext<'a> {
    pub(crate) fn new(memo: &'a mut Memo, group: GroupId) -> Self {
        Self {
            memo,
            group,
            memo_savepoint: None,
            sidecar_rollbacks: Vec::new(),
        }
    }

    pub fn memo(&self) -> &Memo {
        self.memo
    }

    pub fn group(&self) -> GroupId {
        self.group
    }

    /// Obtain the bounded transformation writer. The Memo snapshot is created
    /// only at the first write, so failed shape checks do not clone the region
    /// forest. Transformations may append groups/expressions and update region
    /// facets, but must not mutate physical search state.
    pub fn memo_mut(&mut self) -> &mut Memo {
        if self.memo_savepoint.is_none() {
            self.memo_savepoint = Some(self.memo.transformation_savepoint());
        }
        self.memo
    }

    /// Enlist optimizer-owned side state in the same attempt as Memo writes.
    /// The action is registered immediately before the first side-state write
    /// and runs in reverse registration order on every rejected output path.
    pub fn enlist_rollback(&mut self, rollback: impl FnOnce() -> Result<()> + 'static) {
        self.sidecar_rollbacks.push(Box::new(rollback));
    }

    /// Mutate Memo and one optimizer-owned sidecar under a single attempt.
    /// The sidecar rollback is registered before either state can be written,
    /// and the mutation closure receives the write guard directly so it never
    /// needs to re-enter the same lock.
    pub fn with_sidecar_transaction<S, Savepoint, Output>(
        &mut self,
        sidecar: Arc<RwLock<S>>,
        savepoint: impl FnOnce(&S) -> Savepoint,
        rollback: impl FnOnce(&mut S, Savepoint) -> Result<()> + Send + 'static,
        mutate: impl FnOnce(&mut Memo, &mut S) -> Result<Output>,
    ) -> Result<Output>
    where
        S: Send + Sync + 'static,
        Savepoint: Send + 'static,
    {
        let mut state = sidecar
            .write()
            .map_err(|_| paro_error::internal("transformation sidecar state poisoned"))?;
        let checkpoint = savepoint(&state);
        let rollback_state = sidecar.clone();
        self.enlist_rollback(move || {
            let mut state = rollback_state.write().map_err(|_| {
                paro_error::internal("transformation sidecar state poisoned during rollback")
            })?;
            rollback(&mut state, checkpoint)
        });
        if self.memo_savepoint.is_none() {
            self.memo_savepoint = Some(self.memo.transformation_savepoint());
        }
        mutate(self.memo, &mut state)
    }

    pub(crate) fn rollback(mut self) -> Result<()> {
        let mut failures = Vec::new();
        while let Some(rollback) = self.sidecar_rollbacks.pop() {
            if let Err(error) = rollback() {
                failures.push(format!("sidecar rollback failed: {error}"));
            }
        }
        if let Some(savepoint) = self.memo_savepoint.take() {
            if let Err(error) = self.memo.rollback_transformation(savepoint) {
                failures.push(format!("Memo rollback failed: {error}"));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(paro_error::internal(format!(
                "transformation transaction rollback left inconsistent state: {}",
                failures.join("; ")
            )))
        }
    }

    pub(crate) fn commit(mut self) -> Result<Box<[GroupId]>> {
        let Some(savepoint) = self.memo_savepoint.take() else {
            return Ok(Box::new([]));
        };
        self.memo.appended_groups_since(&savepoint)
    }
}

pub trait TransformationRule: Send + Sync {
    fn id(&self) -> RuleId;

    /// Maximum number of alternatives one firing may publish. Local rewrite
    /// rules keep the default of one. A bounded whole-region owner may expose
    /// a deterministic frontier, but the engine reserves every possible root
    /// expression before the rule mutates Memo or sidecar state.
    fn output_bound(&self, _ctx: &RuleContext<'_>) -> usize {
        1
    }

    fn promise(&self, _expr: &LogicalExpr, _ctx: &RuleContext<'_>) -> RulePromise {
        RulePromise::NORMAL
    }

    fn matches(&self, expr: &LogicalExpr, ctx: &RuleContext<'_>) -> bool;

    fn apply(
        &self,
        expr: LogicalExprId,
        ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GrantDependencyDescriptor {
    Invariant,
    Sensitive,
}

/// Query-local identity of one base row source. Binder table indexes are
/// unique across aliases, so self joins remain distinct while equivalent Memo
/// expressions retain the same source identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct WorkSourceId(pub usize);

/// A disjoint portion of a winner's work proven to belong to one base source.
/// The contained cost is work-only: memory and external-resource contracts
/// remain on the complete winner and are never weakened by selectivity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceFilterWork {
    pub expected_retained_ppm: u32,
    /// Cost of evaluating this predicate against the unfiltered source. Joint
    /// composition orders and scales these costs by preceding predicates.
    pub full_apply_cost: SearchCost,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceWork {
    pub source: WorkSourceId,
    /// Base-source access work after every selected runtime filter.
    pub cost: SearchCost,
    /// Runtime predicates already attached to this source.
    pub filters: Box<[SourceFilterWork]>,
    /// Jointly ordered evaluation work currently present in the winner cost.
    pub filter_apply_cost: SearchCost,
}

#[derive(Debug, Clone)]
pub struct ChildGoalAlternative {
    pub children: Box<[(GroupId, OptimizationGoal)]>,
}

/// Physical lifecycle used when composing a candidate with its children.
/// The mask names child pipelines whose peak can overlap operator-owned state;
/// total work and critical path still follow the dependency order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostComposition {
    /// Children exist only to carry a schema and are never scheduled.
    LocalOnly,
    /// A base scan establishes one source-work lane. Ancestors propagate this
    /// lane without guessing which of their own local work is source-driven.
    Source {
        source: WorkSourceId,
    },
    Sequential,
    RetainedState {
        overlapping_children: u64,
    },
    /// A region-owned side input reduces work inside one child boundary.
    /// Resource proofs remain unscaled; the integer ratios affect only
    /// estimated/risk work and are recorded in the JointCostProof.
    SidewaysFilter {
        overlapping_children: u64,
        filtered_child: u8,
        source: WorkSourceId,
        expected_retained_ppm: u32,
    },
}

impl CostComposition {
    pub(crate) fn overlapping_children(self) -> u64 {
        match self {
            Self::LocalOnly | Self::Source { .. } | Self::Sequential => 0,
            Self::RetainedState {
                overlapping_children,
            }
            | Self::SidewaysFilter {
                overlapping_children,
                ..
            } => overlapping_children,
        }
    }

    pub(crate) fn sideways_filter(self) -> Option<(usize, WorkSourceId, u32)> {
        match self {
            Self::SidewaysFilter {
                filtered_child,
                source,
                expected_retained_ppm,
                ..
            } => Some((usize::from(filtered_child), source, expected_retained_ppm)),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalCandidate {
    pub key: PhysicalExprKey,
    pub payload: PhysicalPayloadId,
    pub provided: ProvidedProperties,
    pub child_goals: Box<[(GroupId, OptimizationGoal)]>,
    pub local_cost: SearchCost,
    /// Work already included in `local_cost` for evaluating this candidate's
    /// runtime predicate against an otherwise unfiltered source.
    pub source_filter_apply_cost: Option<SearchCost>,
    pub cost_composition: CostComposition,
    /// Whether operator-owned retained state can yield memory to spill after
    /// child winner peaks are known.
    pub spillable: bool,
    pub enforcer_cost_input: super::engine::EnforcerCostInput,
    pub physical_fingerprint: Fingerprint,
    pub region: Option<RegionCandidateContract>,
    /// Every logical expression must have at least one mandatory implementation
    /// path. Optional candidates may be cut by the search ledger.
    pub mandatory: bool,
}

impl PhysicalCandidate {
    pub fn stable_event(&self, goal: OptimizationGoal) -> Fingerprint {
        let mut builder = StableFingerprintBuilder::default();
        builder.write_fingerprint(self.key.stable_fingerprint());
        builder.write_fingerprint(self.physical_fingerprint);
        builder.write_u64(goal.required.0 as u64);
        builder.write_u64(goal.row_goal.stable_tag());
        builder.write_u64(goal.objective.0 as u64);
        builder.write_u64(goal.grant.stable_tag());
        builder.write_u64(goal.context.0 as u64);
        if let Some(region) = &self.region {
            builder.write_u64(region.region.0 as u64);
            for facet in &region.facets {
                builder.write_fingerprint(*facet);
            }
            for artifact in &region.artifacts {
                builder.write_fingerprint(artifact.fingerprint);
                builder.write_u64(artifact.kind as u64);
            }
            for dependency in &region.artifact_dependencies {
                builder.write_fingerprint(dependency.artifact);
                let (producer_kind, producer_value) = dependency.producer.stable_tag();
                builder.write_u64(producer_kind);
                builder.write_u64(producer_value);
                let (consumer_kind, consumer_value) = dependency.consumer.stable_tag();
                builder.write_u64(consumer_kind);
                builder.write_u64(consumer_value);
                builder.write_u64(dependency.kind as u64);
            }
        }
        builder.finish()
    }
}

pub struct ImplementationContext<'a> {
    pub memo: &'a Memo,
    pub group: GroupId,
}

pub trait PhysicalImplementation: Send + Sync {
    fn id(&self) -> ImplementationId;

    fn grant_dependency(&self) -> GrantDependencyDescriptor {
        GrantDependencyDescriptor::Invariant
    }

    /// Conservative declaration used before per-class goal creation. An
    /// implementation may specialize this by logical operator, but returning
    /// `Invariant` is a correctness promise for every expression it can
    /// implement, not merely a costing hint.
    fn grant_dependency_for(
        &self,
        _expr: &LogicalExpr,
        _ctx: &ImplementationContext<'_>,
    ) -> GrantDependencyDescriptor {
        self.grant_dependency()
    }

    fn promise(&self, _expr: &LogicalExpr, _goal: OptimizationGoal) -> RulePromise {
        RulePromise::NORMAL
    }

    fn matches(
        &self,
        expr: &LogicalExpr,
        goal: OptimizationGoal,
        ctx: &ImplementationContext<'_>,
    ) -> bool;

    fn candidates(
        &self,
        expr: LogicalExprId,
        goal: OptimizationGoal,
        ctx: &ImplementationContext<'_>,
    ) -> Result<Box<[PhysicalCandidate]>>;
}

#[derive(Default)]
pub struct ImplementationRegistry {
    transformations: BTreeMap<RuleId, Box<dyn TransformationRule>>,
    implementations: BTreeMap<ImplementationId, Box<dyn PhysicalImplementation>>,
}

impl std::fmt::Debug for ImplementationRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImplementationRegistry")
            .field("transformation_ids", &self.transformations.keys())
            .field("implementation_ids", &self.implementations.keys())
            .finish()
    }
}

impl ImplementationRegistry {
    pub fn register_transformation(
        &mut self,
        rule: impl TransformationRule + 'static,
    ) -> Result<()> {
        let id = rule.id();
        if self.transformations.insert(id, Box::new(rule)).is_some() {
            return Err(paro_error::internal(
                "duplicate stable transformation RuleId",
            ));
        }
        Ok(())
    }

    pub fn register_implementation(
        &mut self,
        implementation: impl PhysicalImplementation + 'static,
    ) -> Result<()> {
        let id = implementation.id();
        if self
            .implementations
            .insert(id, Box::new(implementation))
            .is_some()
        {
            return Err(paro_error::internal(
                "duplicate stable physical ImplementationId",
            ));
        }
        Ok(())
    }

    pub fn transformations(&self) -> impl ExactSizeIterator<Item = &dyn TransformationRule> {
        self.transformations.values().map(Box::as_ref)
    }

    pub fn implementations(&self) -> impl ExactSizeIterator<Item = &dyn PhysicalImplementation> {
        self.implementations.values().map(Box::as_ref)
    }

    pub fn transformation(&self, id: RuleId) -> Option<&dyn TransformationRule> {
        self.transformations.get(&id).map(Box::as_ref)
    }

    pub fn implementation(&self, id: ImplementationId) -> Option<&dyn PhysicalImplementation> {
        self.implementations.get(&id).map(Box::as_ref)
    }

    pub(crate) fn implementation_entries(
        &self,
    ) -> impl ExactSizeIterator<Item = (ImplementationId, &dyn PhysicalImplementation)> {
        self.implementations
            .iter()
            .map(|(id, implementation)| (*id, implementation.as_ref()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transformation_rule_names_are_stable_and_bijective() {
        for &(id, name) in TRANSFORMATION_RULE_NAMES {
            assert_eq!(transformation_rule_name(id), Some(name));
            assert_eq!(transformation_rule_id(name), Some(id));
        }
        assert_eq!(
            transformation_rule_id("JOIN_ELIMINATION"),
            Some(JOIN_ELIMINATION_RULE)
        );
        assert!(transformation_rule_id("unknown").is_none());
    }
}
