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

pub trait TransformationRule: Send + Sync {
    fn id(&self) -> RuleId;

    fn promise(&self, _expr: &LogicalExpr, _ctx: &RuleContext<'_>) -> RulePromise {
        RulePromise::NORMAL
    }

    fn matches(&self, expr: &LogicalExpr, ctx: &RuleContext<'_>) -> bool;

    fn apply(
        &self,
        expr: LogicalExprId,
        ctx: &RuleContext<'_>,
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
