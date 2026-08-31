// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Stable rule and implementation registries used by Direct and Memo search.

use std::collections::BTreeMap;

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
pub const CTE_FILTER_PUSHDOWN_RULE: RuleId = RuleId(10_008);
pub const AGGREGATE_POST_REDUCTION_RULE: RuleId = RuleId(10_009);
pub const JOIN_ELIMINATION_RULE: RuleId = RuleId(10_011);
pub const AGGREGATE_JOIN_PREAGGREGATION_RULE: RuleId = RuleId(10_012);
pub const AGGREGATE_JOIN_SUBSUMPTION_RULE: RuleId = RuleId(10_013);
pub const AGGREGATE_NON_NULL_INPUT_RULE: RuleId = RuleId(10_014);
pub const AGGREGATE_DIMENSION_DEFERRAL_RULE: RuleId = RuleId(10_015);
pub const AGGREGATE_INPUT_MATERIALIZATION_RULE: RuleId = RuleId(10_016);
pub const LIMIT_PUSHDOWN_RULE: RuleId = RuleId(10_017);
pub const LATE_PAYLOAD_FETCH_RULE: RuleId = RuleId(10_018);
pub const SCALAR_AGGREGATE_WINDOW_RULE: RuleId = RuleId(10_019);

const TRANSFORMATION_RULE_NAMES: &[(RuleId, &str)] = &[
    (
        EXPENSIVE_PREDICATE_PLACEMENT_RULE,
        "expensive_predicate_placement",
    ),
    (CTE_INLINE_RULE, "cte_inline"),
    (CTE_FILTER_PUSHDOWN_RULE, "cte_filter_pushdown"),
    (AGGREGATE_POST_REDUCTION_RULE, "aggregate_post_reduction"),
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
    pub proof: EquivalenceProof,
}

pub struct RuleContext<'a> {
    pub memo: &'a Memo,
    pub group: GroupId,
}

pub struct TransformContext<'a> {
    /// Transformations may append child groups/expressions and update region
    /// facets, but must not mutate pre-existing group membership or physical
    /// search state. The engine relies on that write-set contract for its
    /// bounded incremental rollback savepoint.
    pub memo: &'a mut Memo,
    pub group: GroupId,
}

pub trait TransformationRule: Send + Sync {
    fn id(&self) -> RuleId;

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

#[derive(Debug, Clone)]
pub struct ChildGoalAlternative {
    pub children: Box<[(GroupId, OptimizationGoal)]>,
}

/// Physical lifecycle used when composing a candidate with its children.
/// The mask names child pipelines whose peak can overlap operator-owned state;
/// total work and critical path still follow the dependency order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostComposition {
    Sequential,
    RetainedState { overlapping_children: u64 },
}

#[derive(Debug, Clone)]
pub struct PhysicalCandidate {
    pub key: PhysicalExprKey,
    pub payload: PhysicalPayloadId,
    pub provided: ProvidedProperties,
    pub child_goals: Box<[(GroupId, OptimizationGoal)]>,
    pub local_cost: SearchCost,
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
