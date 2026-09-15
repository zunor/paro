// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Construction of optimizer Query IR and Memo groups from bound plans.

mod boundary;
mod domain_transfer;
mod quality_domain;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::physical::{ObjectiveProfile, ResourceGrantClass, SpillPolicy};
use paro_catalog::entry::CatalogEntry;
use paro_common::error::{self as paro_error, Result};
use paro_common::logging::targets;
use paro_planner::binder::context::BindContext;
use paro_planner::binder::deep_copy::duplicate_plan_preserving_indices;
use paro_planner::binder::ir::OrderByNode;
use paro_planner::binder::Binder;
use paro_planner::expression::Expression;
use paro_planner::operator::join::{AntiJoinMode, Join, JoinComparisonType, JoinType};
use paro_planner::operator::{ColumnBinding, LogicalOperator, LogicalOperatorType};
use paro_planner::plan::{CardinalityEstimate, NodeStats, OwnedLogicalPlan};
use paro_storage::statistics::ColumnStatistics;
use tracing::debug;

use crate::aggregate::{
    dimension_deferral, dimension_sharing, input_materialization,
    join_subsumption, late_payload, post_reduction,
};
use crate::context::SharedColumnStatistics;
use crate::filter::pushdown::FilterPushdown;
use crate::join::elimination::JoinElimination;
use crate::statistics::gathering::StatisticsGathering;
use crate::statistics::propagator::StatisticsPropagator;
use crate::subquery::scalar_aggregate_window;

use super::budget::{BudgetDimension, SearchBudget};
use super::calibration::{
    LocalOperatorWork, MachineCalibrationBundle, ParallelWorkProfile, OP_HASH_KEY_BYTE_BLOCK,
    OP_RUNTIME_FILTER_APPLY_ROW, OP_RUNTIME_FILTER_BUILD_ROW, OP_TUPLE_BYTE_BLOCK,
};
use super::column::{ColumnCatalog, ColumnOrigin, ColumnVisibility, GroupSchema};
use super::cost::ResourceDimension;
use super::cost::{CompactRange, ScoreSummary, SearchCost};
use super::engine::{
    selected_proof_rule_ids, CascadesEngine, PricedIncumbent, SearchMode, SearchStopReason,
    SeedPlan,
};
use super::ids::{
    AdmissibleGrantSetId, BaseRelationId, CandidateId, ColumnId, Fingerprint, GroupId,
    ImplementationId, LogicalExprId, LogicalPayloadId, OpClassId, OptimizationContextId,
    PhysicalExprId, PhysicalPayloadId, PropertySetId, QualityPolicyId, ResourceGrantClassId,
    RuleId, ScalarExprId, SnapshotId, StableFingerprintBuilder,
};
use super::memo::{
    CardinalityEnvelope, CardinalityRecipeKind, ChildWinnerRef, CteReferenceDomain,
    EquivalenceProof, FrozenCandidate, GrantGoalKey, GroupCardinality, GroupColumnDomain,
    LogicalExprKey, LogicalProperties, Memo, OptimizationContext, OptimizationGoal,
    PhysicalExprKey, RowGoal,
};
use super::properties::{
    MutationSafetyRequirement, NullOrder, OrderingKey, OrderingRequirement, OrderingScope,
    PartitioningRequirement, ProvidedMaterialization, ProvidedMutationSafety, ProvidedOrdering,
    ProvidedPartitioning, ProvidedProperties, ProvidedReplayability, ProvidedRepresentation,
    ReplayabilityRequirement, RepresentationRequirement, RequiredOrdering, RequiredProperties,
    ResultGuarantee, SortDirection,
};
use super::quality::{
    AggregateRegionWitness, BundleCapability, BundleFact, NativeQualityEvidence,
    NativeQualityShape, QualityEvidenceProvider, QualityPolicyStatus,
};
use super::region::{
    FacetCriticality, RegionArtifactDependencyContract, RegionArtifactKind, RegionBoundaryEndpoint,
    RegionCandidateContract, RegionDependencyKind, RegionFacet, RegionFacetKind, RegionForest,
    RegionOwnedArtifact,
};
use super::rules::{
    CostComposition, EquivalentExpression, GrantDependencyDescriptor, ImplementationContext,
    ImplementationRegistry, PatternBinding, PatternBindingSet, PatternEnumerationCompletion,
    PatternOperand, PatternRead, PhysicalCandidate, PhysicalImplementation, QualityDependency,
    ReadScope,
    RootDispatch, RuleContext, RulePromise, SidewaysFilterSource, TaskSupplyContract,
    TransformContext, TransformationBudgetClass, TransformationPreflight, TransformationRule,
    WorkSourceId,
    AGGREGATE_DIMENSION_DEFERRAL_RULE, AGGREGATE_DIMENSION_SHARING_RULE,
    AGGREGATE_INPUT_MATERIALIZATION_RULE, AGGREGATE_JOIN_PREAGGREGATION_RULE,
    AGGREGATE_JOIN_SUBSUMPTION_RULE, AGGREGATE_NON_NULL_INPUT_RULE, AGGREGATE_POST_REDUCTION_RULE,
    CTE_DEMAND_PUSHDOWN_RULE, CTE_FILTER_PUSHDOWN_RULE, CTE_INLINE_RULE,
    CTE_PARTITIONED_MATERIALIZATION_RULE, JOIN_ELIMINATION_RULE, JOIN_REGION_ENUMERATION_RULE,
    KEY_DOMAIN_TRANSFER_RULE, LATE_PAYLOAD_FETCH_RULE, LIMIT_PUSHDOWN_RULE, MARK_JOIN_TO_SEMI_RULE,
    PREDICATE_TRANSFER_RULE, SCALAR_AGGREGATE_WINDOW_RULE, TOP_N_INTRODUCTION_RULE,
};
use super::scalar::ScalarArena;
use super::scalar_lowering::{
    encode_routine_identity, expression_fingerprint, intern_operator_scalars,
    logical_type_fingerprint, BindingCatalog,
};
use crate::physical::{
    ExtractedEnforcerContract, ExtractedEnforcerContracts, ExtractedPhysicalEnforcer,
    PhysicalImplementationFlavor, WinnerPhysicalContract, WinnerPhysicalContracts,
};

mod contracts;
mod costing;
#[cfg(test)]
#[path = "domain_oracle_tests.rs"]
mod domain_oracle_tests;
mod extraction;
mod identity;
mod implementation;
mod predicate_order;
mod scalar_facts;
mod semantic_plan;
mod state;
mod transformation;

use contracts::*;
use costing::*;
use extraction::*;
use identity::*;
use state::*;

const PLANNER_BASELINE_IMPLEMENTATION: ImplementationId = ImplementationId(1);
const PLANNER_PERFECT_HASH_AGGREGATE: ImplementationId = ImplementationId(2);
const PLANNER_SORT_RANGE_JOIN: ImplementationId = ImplementationId(3);
const PLANNER_CLASSIC_IE_JOIN: ImplementationId = ImplementationId(4);
const PLANNER_SEARCH_PROVIDER: ImplementationId = ImplementationId(5);
pub(super) const PLANNER_HASH_JOIN_RUNTIME_FILTER: ImplementationId = ImplementationId(6);
const PLANNER_PARTITION_AGGREGATE_WINDOW: ImplementationId = ImplementationId(7);
const PLANNER_SINGLETON_AGGREGATE_PROJECTION: ImplementationId = ImplementationId(8);
const PLANNER_EXTERNAL_CROSS_PRODUCT: ImplementationId = ImplementationId(9);
const PLANNER_HASH_JOIN_BUILD_LEFT: ImplementationId = ImplementationId(10);
pub(super) const PLANNER_HASH_JOIN_BUILD_LEFT_RUNTIME_FILTER: ImplementationId =
    ImplementationId(11);
const COST_OPTIMIZED_SEARCH_POLICY: QualityPolicyId = QualityPolicyId(1);

/// Planner-owned producer for the same-Memo quality handoff. It inspects the
/// exact frozen DAG and planner payload arena; it never creates alternatives
/// or imports an owned logical tree into the search Memo.
#[derive(Debug)]
struct PlannerQualityEvidenceProvider {
    state: Arc<RwLock<PlannerTransformState>>,
}

/// Return only proof-bearing rule identities for the expression selected by a
/// frozen candidate.  `LogicalExpr::applied_rules` is intentionally absent:
/// it records that a rule reached the apply gate at some point in the Memo,
/// not that its output is present in this candidate.
fn selected_rule_proofs(
    logical: &crate::cascades::memo::LogicalExpr,
    origin_rule: Option<RuleId>,
) -> BTreeSet<RuleId> {
    selected_proof_rule_ids(logical)
        .iter()
        .copied()
        .filter(|rule| {
            // A staged payload carries one origin rule.  Requiring its proof
            // prevents an apply attempt, empty result, or budget-rejected
            // binding from becoming quality evidence.  Specialized region
            // roots have no single payload origin, so their explicit Memo
            // proof is the witness.
            origin_rule.is_none_or(|origin| origin == *rule)
        })
        .collect()
}

/// Combine proof-bearing identities from the exact Memo expression and the
/// payload-local lineage retained while a native shell copied an inner Memo
/// node. Neither source consults `applied_rules`; the latter is only an audit
/// of work that reached an apply gate.
fn selected_payload_rule_proofs(
    logical: &crate::cascades::memo::LogicalExpr,
    metadata: &PlannerOperatorMetadata,
) -> BTreeSet<RuleId> {
    let mut rules = selected_rule_proofs(logical, None);
    rules.extend(
        metadata
            .selected_proofs
            .iter()
            .filter_map(|proof| match proof {
                EquivalenceProof::Transformation { rule, .. }
                | EquivalenceProof::TransformationDescendant { rule }
                | EquivalenceProof::SpecializedEnumerator { rule, .. } => Some(*rule),
                EquivalenceProof::Initial | EquivalenceProof::Normalization { .. } => None,
            }),
    );
    rules
}

fn selected_physical_contract_is_exact(
    logical: &crate::cascades::memo::LogicalExpr,
    physical: &crate::cascades::memo::PhysicalExpr,
    metadata: &PlannerOperatorMetadata,
    payload: &PlannerPhysicalPayload,
) -> bool {
    let Ok(flavor) =
        selected_implementation_flavor(physical.key.implementation, metadata.implementations)
    else {
        return false;
    };
    let implementation_supported =
        flavor == metadata.implementations.baseline || metadata.implementations.supports(flavor);
    if !implementation_supported || physical.key.children != logical.key.children {
        return false;
    }
    let logical_owner_matches = match &payload.template {
        PlannerPhysicalTemplate::Logical(owner)
        | PlannerPhysicalTemplate::OrderedFilter { logical: owner, .. } => {
            *owner == logical.payload
        }
        // An executable template is only produced for the native search
        // implementation.  Its separate search metadata below binds the
        // exact payload/fingerprint to this physical choice.
        PlannerPhysicalTemplate::Executable(_) => {
            physical.key.implementation == PLANNER_SEARCH_PROVIDER
        }
    };
    if !logical_owner_matches {
        return false;
    }
    if physical.key.implementation == PLANNER_SEARCH_PROVIDER {
        return metadata.search.as_ref().is_some_and(|search| {
            search.payload == physical.payload
                && search.payload_fingerprint == physical.key.payload_fingerprint
        });
    }
    true
}

fn frozen_choice_fingerprint(frozen: &FrozenCandidate) -> Fingerprint {
    let mut choice = StableFingerprintBuilder::default();
    choice.write_bytes(b"paro.quality.frozen-choice.v1");
    choice.write_u64(frozen.reference.group.0 as u64);
    choice.write_u64(frozen.reference.candidate.index() as u64);
    choice.write_u64(frozen.reference.goal.required.0 as u64);
    choice.write_u64(frozen.reference.goal.grant.stable_tag());
    choice.write_u64(frozen.reference.goal.row_goal.stable_tag());
    choice.write_u64(frozen.reference.goal.objective.stable_tag());
    choice.write_u64(frozen.reference.goal.context.0 as u64);
    choice.write_fingerprint(frozen.logical.key.stable_fingerprint());
    choice.write_fingerprint(frozen.physical.key.stable_fingerprint());
    choice.write_fingerprint(frozen.winner.physical_fingerprint);
    choice.write_u64(frozen.logical.payload.0 as u64);
    choice.write_u64(frozen.physical.payload.0 as u64);
    choice.write_u64(frozen.winner.children.len() as u64);
    for child in frozen.winner.children.iter() {
        choice.write_u64(child.group.0 as u64);
        choice.write_u64(child.candidate.index() as u64);
        choice.write_u64(child.goal.required.0 as u64);
        choice.write_u64(child.goal.grant.stable_tag());
        choice.write_u64(child.goal.context.0 as u64);
    }
    choice.finish()
}

fn collect_frozen_choices(
    frozen: &FrozenCandidate,
    choices: &mut Vec<Fingerprint>,
    visited: &mut BTreeSet<CandidateId>,
) {
    if !visited.insert(frozen.reference.candidate) {
        return;
    }
    choices.push(frozen_choice_fingerprint(frozen));
    for child in frozen.children.iter() {
        collect_frozen_choices(child, choices, visited);
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct SelectedAggregateRegionShape {
    aggregates: u32,
    joins: u32,
    decomposed: bool,
}

fn aggregate_merge_contract_matches<OuterChild, PartialChild>(
    outer: &paro_planner::operator::Aggregate<OuterChild>,
    partial: &paro_planner::operator::Aggregate<PartialChild>,
) -> bool {
    if outer.post_reduction.is_some()
        || outer.aggregates.is_empty()
        || !outer.has_plain_grouping_domain()
        || partial.post_reduction.is_some()
        || partial.aggregates.is_empty()
        || !partial.has_plain_grouping_domain()
    {
        return false;
    }
    outer.aggregates.iter().all(|expression| {
        let Expression::Aggregate(merge) = expression else {
            return false;
        };
        if merge.aggr_type != paro_planner::expression::AggregateType::NonDistinct
            || merge.filter.is_some()
            || !merge.order_bys.is_empty()
            || merge.children.len() != 1
            || merge.function.arguments.len() != 1
            || merge.function.arguments[0] != merge.children[0].return_type()
            || merge.function.return_type != merge.return_type
        {
            return false;
        }
        let Expression::ColumnRef(column) = &merge.children[0] else {
            return false;
        };
        if column.depth != 0
            || column.binding.table_index != partial.aggregate_index
            || column.binding.column_index >= partial.aggregates.len()
        {
            return false;
        }
        let Expression::Aggregate(source) = &partial.aggregates[column.binding.column_index] else {
            return false;
        };
        source
            .function
            .partial_merge_function()
            .is_some_and(|expected| expected.execution_semantics_equal(&merge.function))
    })
}

fn selected_aggregate_region_shape(
    frozen: &FrozenCandidate,
    state: &PlannerTransformState,
    visited: &mut BTreeSet<CandidateId>,
) -> SelectedAggregateRegionShape {
    if !visited.insert(frozen.reference.candidate) {
        return SelectedAggregateRegionShape::default();
    }
    let operator = state
        .payloads
        .logical
        .get(frozen.logical.payload.index())
        .map(|payload| &payload.semantic_template.operator);
    let mut shape = SelectedAggregateRegionShape::default();
    if matches!(operator, Some(LogicalOperator::Aggregate(_))) {
        shape.aggregates = 1;
        if frozen.children.len() == 1 {
            let join = &frozen.children[0];
            if let Some(LogicalOperator::Join(Join::Comparison(join_operator))) = state
                .payloads
                .logical
                .get(join.logical.payload.index())
                .map(|payload| &payload.semantic_template.operator)
            {
                if !join_operator.conditions.is_empty() && join.children.len() == 2 {
                    let outer = match operator {
                        Some(LogicalOperator::Aggregate(outer)) => outer,
                        _ => unreachable!("aggregate operator disappeared during inspection"),
                    };
                    shape.decomposed = join.children.iter().any(|partial| {
                        matches!(
                            state
                                .payloads
                                .logical
                                .get(partial.logical.payload.index())
                                .map(|payload| &payload.semantic_template.operator),
                            Some(LogicalOperator::Aggregate(partial))
                                if aggregate_merge_contract_matches(outer, partial)
                        )
                    });
                }
            }
        }
    }
    if matches!(operator, Some(LogicalOperator::Join(_))) {
        shape.joins = 1;
    }
    for child in frozen.children.iter() {
        let child_shape = selected_aggregate_region_shape(child, state, visited);
        shape.aggregates = shape.aggregates.saturating_add(child_shape.aggregates);
        shape.joins = shape.joins.saturating_add(child_shape.joins);
        shape.decomposed |= child_shape.decomposed;
    }
    shape
}

fn collect_region_fact_fingerprint(
    memo: &Memo,
    root: &FrozenCandidate,
    arm: &FrozenCandidate,
    goal: OptimizationGoal,
) -> Option<Fingerprint> {
    let mut facts = BTreeMap::new();
    let mut visited = BTreeSet::new();
    fn visit(
        memo: &Memo,
        frozen: &FrozenCandidate,
        facts: &mut BTreeMap<GroupId, (Fingerprint, Fingerprint)>,
        visited: &mut BTreeSet<CandidateId>,
    ) -> bool {
        if !visited.insert(frozen.reference.candidate) {
            return true;
        }
        let group = memo.canonical_group(frozen.reference.group);
        let Some(group_ref) = memo.group(group) else {
            return false;
        };
        facts.insert(
            group,
            (
                group_ref.logical_fact_fingerprint(),
                group_ref.statistics_snapshot_fingerprint(),
            ),
        );
        frozen
            .children
            .iter()
            .all(|child| visit(memo, child, facts, visited))
    }
    if !visit(memo, root, &mut facts, &mut visited) || !visit(memo, arm, &mut facts, &mut visited) {
        return None;
    }
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_bytes(b"paro.quality.aggregate-region-facts.v1");
    fingerprint.write_u64(goal.required.0 as u64);
    fingerprint.write_u64(goal.grant.stable_tag());
    fingerprint.write_u64(goal.row_goal.stable_tag());
    fingerprint.write_u64(goal.objective.stable_tag());
    fingerprint.write_u64(goal.context.0 as u64);
    fingerprint.write_u64(facts.len() as u64);
    for (group, (logical, statistics)) in facts {
        fingerprint.write_u64(group.0 as u64);
        fingerprint.write_fingerprint(logical);
        fingerprint.write_fingerprint(statistics);
    }
    Some(fingerprint.finish())
}

fn selected_subtree_contains_union(
    frozen: &FrozenCandidate,
    state: &PlannerTransformState,
    visited: &mut BTreeSet<CandidateId>,
) -> bool {
    if !visited.insert(frozen.reference.candidate) {
        return false;
    }
    let is_union = state
        .payloads
        .logical
        .get(frozen.logical.payload.index())
        .is_some_and(|payload| {
            matches!(
                &payload.semantic_template.operator,
                LogicalOperator::SetOperation(setop)
                    if setop.setop_type == paro_planner::operator::SetOpType::Union
                        && setop.setop_all
            )
        });
    is_union
        || frozen
            .children
            .iter()
            .any(|child| selected_subtree_contains_union(child, state, visited))
}

fn selected_aggregate_region_witnesses(
    memo: &Memo,
    state: &PlannerTransformState,
    root: &FrozenCandidate,
    goal: OptimizationGoal,
) -> Option<Vec<AggregateRegionWitness>> {
    let mut witnesses = Vec::new();
    let mut path = Vec::new();
    fn visit_union(
        memo: &Memo,
        state: &PlannerTransformState,
        union: &FrozenCandidate,
        root_candidate: CandidateId,
        goal: OptimizationGoal,
        path: &mut Vec<u32>,
        witnesses: &mut Vec<AggregateRegionWitness>,
    ) -> Option<()> {
        let operator = state
            .payloads
            .logical
            .get(union.logical.payload.index())
            .map(|payload| &payload.semantic_template.operator);
        if let Some(LogicalOperator::SetOperation(setop)) = operator {
            if setop.setop_type == paro_planner::operator::SetOpType::Union && setop.setop_all {
                for (index, arm) in union.children.iter().enumerate() {
                    path.push(index as u32);
                    let mut shape_visited = BTreeSet::new();
                    let shape = selected_aggregate_region_shape(arm, state, &mut shape_visited);
                    let mut union_visited = BTreeSet::new();
                    if shape.aggregates > 0
                        && shape.joins > 0
                        && !selected_subtree_contains_union(arm, state, &mut union_visited)
                    {
                        let mut choices = Vec::new();
                        let mut choice_visited = BTreeSet::new();
                        collect_frozen_choices(arm, &mut choices, &mut choice_visited);
                        let fact_fingerprint =
                            collect_region_fact_fingerprint(memo, union, arm, goal)?;
                        let mut region = StableFingerprintBuilder::default();
                        region.write_bytes(b"paro.quality.aggregate-region.v2");
                        region.write_u64(root_candidate.index() as u64);
                        region.write_u64(arm.reference.candidate.index() as u64);
                        region.write_fingerprint(frozen_choice_fingerprint(union));
                        region.write_u64(path.len() as u64);
                        for component in path.iter().copied() {
                            region.write_u64(component as u64);
                        }
                        region.write_fingerprint(fact_fingerprint);
                        for choice in choices.iter().copied() {
                            region.write_fingerprint(choice);
                        }
                        witnesses.push(AggregateRegionWitness {
                            region: region.finish(),
                            candidate: root_candidate,
                            anchor: arm.reference.candidate,
                            fact_fingerprint,
                            choices: choices.into_boxed_slice(),
                            covered: shape.decomposed,
                        });
                    }
                    visit_union(memo, state, arm, root_candidate, goal, path, witnesses)?;
                    path.pop();
                }
                return Some(());
            }
        }
        for child in union.children.iter() {
            visit_union(memo, state, child, root_candidate, goal, path, witnesses)?;
        }
        Some(())
    }
    visit_union(
        memo,
        state,
        root,
        root.reference.candidate,
        goal,
        &mut path,
        &mut witnesses,
    )?;
    if witnesses.is_empty() {
        let mut shape_visited = BTreeSet::new();
        let shape = selected_aggregate_region_shape(root, state, &mut shape_visited);
        if shape.aggregates > 0 {
            let mut choices = Vec::new();
            let mut choice_visited = BTreeSet::new();
            collect_frozen_choices(root, &mut choices, &mut choice_visited);
            let fact_fingerprint = collect_region_fact_fingerprint(memo, root, root, goal)?;
            let mut region = StableFingerprintBuilder::default();
            region.write_bytes(b"paro.quality.aggregate-region.root.v1");
            region.write_u64(root.reference.candidate.index() as u64);
            region.write_fingerprint(fact_fingerprint);
            for choice in choices.iter().copied() {
                region.write_fingerprint(choice);
            }
            witnesses.push(AggregateRegionWitness {
                region: region.finish(),
                candidate: root.reference.candidate,
                anchor: root.reference.candidate,
                fact_fingerprint,
                choices: choices.into_boxed_slice(),
                covered: shape.decomposed,
            });
        }
    }
    Some(witnesses)
}

impl QualityEvidenceProvider for PlannerQualityEvidenceProvider {
    fn evidence(
        &self,
        memo: &Memo,
        reference: ChildWinnerRef,
        frozen: &FrozenCandidate,
        goal: OptimizationGoal,
    ) -> Result<Option<NativeQualityEvidence>> {
        let _partition = crate::work_partition::enter(crate::work_partition::Bucket::QualityEvidence);
        let state = self.state.read().expect("planner transform state poisoned");
        let Some(required) = memo.required(goal.required) else {
            return Ok(None);
        };
        let mut capabilities = BTreeSet::new();
        let mut facts = BTreeSet::new();
        let mut choices = Vec::new();
        let mut rules = BTreeSet::new();
        let mut shape = NativeQualityShape::default();
        let mut cte_producers = BTreeSet::new();
        let mut cte_consumers = BTreeSet::new();
        let mut cte_producer_witnesses = BTreeSet::new();
        let mut has_filter = false;
        let mut has_get = false;
        let mut has_join = false;
        let mut has_join_region = false;
        let mut has_aggregate = false;
        let mut has_cte_consumer = false;
        let mut has_cte_producer = false;
        let mut has_ordering = false;
        let mut has_graph = false;
        let mut has_dependent = false;
        let mut exact_contract = true;
        let mut visited = BTreeSet::new();

        fn visit(
            frozen: &FrozenCandidate,
            state: &PlannerTransformState,
            choices: &mut Vec<Fingerprint>,
            rules: &mut BTreeSet<RuleId>,
            shape: &mut NativeQualityShape,
            cte_producers: &mut BTreeSet<usize>,
            cte_consumers: &mut BTreeSet<usize>,
            cte_producer_witnesses: &mut BTreeSet<usize>,
            has_filter: &mut bool,
            has_get: &mut bool,
            has_join: &mut bool,
            has_join_region: &mut bool,
            has_aggregate: &mut bool,
            has_cte_consumer: &mut bool,
            has_cte_producer: &mut bool,
            has_ordering: &mut bool,
            has_graph: &mut bool,
            has_dependent: &mut bool,
            exact_contract: &mut bool,
            visited: &mut BTreeSet<CandidateId>,
        ) -> Result<()> {
            if !visited.insert(frozen.reference.candidate) {
                return Ok(());
            }
            shape.nodes = shape.nodes.saturating_add(1);
            if frozen.physical.id != frozen.winner.expression
                || frozen.logical.id != frozen.physical.key.logical
                || frozen.physical.key.children.len() != frozen.logical.key.children.len()
                || frozen
                    .physical
                    .key
                    .children
                    .iter()
                    .zip(frozen.logical.key.children.iter())
                    .any(|(physical_child, logical_child)| *physical_child != *logical_child)
                || frozen.children.len() != frozen.winner.children.len()
                || frozen
                    .children
                    .iter()
                    .zip(frozen.winner.children.iter())
                    .any(|(child, reference)| child.reference != *reference)
            {
                *exact_contract = false;
                return Ok(());
            }
            let Some(metadata) = state.metadata.get(&frozen.logical.payload) else {
                *exact_contract = false;
                return Ok(());
            };
            let Some(physical_payload) = state.payloads.get_physical(frozen.physical.payload)
            else {
                *exact_contract = false;
                return Ok(());
            };
            if !selected_physical_contract_is_exact(
                &frozen.logical,
                &frozen.physical,
                metadata,
                &physical_payload,
            ) {
                *exact_contract = false;
                return Ok(());
            }
            choices.push(frozen_choice_fingerprint(frozen));
            let selected_rules = selected_payload_rule_proofs(&frozen.logical, metadata);
            if metadata.origin_rule.is_some() && selected_rules.is_empty() {
                // The sidecar says this payload came from a rule, but the
                // selected Memo expression has no corresponding equivalence
                // proof.  Fail closed instead of trusting origin metadata.
                *exact_contract = false;
                return Ok(());
            }
            rules.extend(selected_rules.iter().copied());
            *exact_contract &= frozen.winner.provided.result_guarantee == ResultGuarantee::Exact
                && metadata.provided.result_guarantee == ResultGuarantee::Exact;
            match metadata.operator_type {
                LogicalOperatorType::Filter | LogicalOperatorType::FullTextFilterScan => {
                    *has_filter = true
                }
                LogicalOperatorType::Get
                | LogicalOperatorType::SearchScan
                | LogicalOperatorType::TableFunctionGet => *has_get = true,
                LogicalOperatorType::ComparisonJoin
                | LogicalOperatorType::AnyJoin
                | LogicalOperatorType::CrossProduct => {
                    *has_join = true;
                    *has_join_region |= frozen.winner.joint_cost_proof.is_some();
                    shape.joins = shape.joins.saturating_add(1);
                    if frozen.winner.joint_cost_proof.is_some() {
                        shape.join_region_witness_nodes =
                            shape.join_region_witness_nodes.saturating_add(1);
                    }
                }
                LogicalOperatorType::Aggregate => {
                    *has_aggregate = true;
                    shape.aggregates = shape.aggregates.saturating_add(1);
                }
                LogicalOperatorType::CTERef => {
                    *has_cte_consumer = true;
                    if let LogicalOperator::CTERef(reference) = &state.payloads.logical
                        [frozen.logical.payload.index()]
                    .semantic_template
                    .operator
                    {
                        cte_consumers.insert(reference.cte_index);
                    }
                }
                LogicalOperatorType::MaterializedCTE | LogicalOperatorType::RecursiveCTE => {
                    *has_cte_producer = true;
                    let cte_index = match &state.payloads.logical[frozen.logical.payload.index()]
                        .semantic_template
                        .operator
                    {
                        LogicalOperator::MaterializedCTE(cte) => Some(cte.cte_index),
                        LogicalOperator::RecursiveCTE(cte) => Some(cte.cte_index),
                        _ => None,
                    };
                    if let Some(cte_index) = cte_index {
                        cte_producers.insert(cte_index);
                        if selected_rules.iter().any(|rule| {
                            matches!(
                                *rule,
                                CTE_DEMAND_PUSHDOWN_RULE
                                    | CTE_FILTER_PUSHDOWN_RULE
                                    | CTE_PARTITIONED_MATERIALIZATION_RULE
                            )
                        }) {
                            cte_producer_witnesses.insert(cte_index);
                        }
                    }
                }
                LogicalOperatorType::Order | LogicalOperatorType::TopN => *has_ordering = true,
                LogicalOperatorType::DependentJoin => *has_dependent = true,
                LogicalOperatorType::GraphMatch
                | LogicalOperatorType::GraphScan
                | LogicalOperatorType::GraphExpand
                | LogicalOperatorType::CreatePropertyGraph => *has_graph = true,
                _ => {}
            }
            if matches!(
                frozen.physical.key.implementation,
                PLANNER_HASH_JOIN_RUNTIME_FILTER | PLANNER_HASH_JOIN_BUILD_LEFT_RUNTIME_FILTER
            ) {
                shape.runtime_filter_joins = shape.runtime_filter_joins.saturating_add(1);
            }
            for child in frozen.children.iter() {
                visit(
                    child,
                    state,
                    choices,
                    rules,
                    shape,
                    cte_producers,
                    cte_consumers,
                    cte_producer_witnesses,
                    has_filter,
                    has_get,
                    has_join,
                    has_join_region,
                    has_aggregate,
                    has_cte_consumer,
                    has_cte_producer,
                    has_ordering,
                    has_graph,
                    has_dependent,
                    exact_contract,
                    visited,
                )?;
            }
            Ok(())
        }

        visit(
            frozen,
            &state,
            &mut choices,
            &mut rules,
            &mut shape,
            &mut cte_producers,
            &mut cte_consumers,
            &mut cte_producer_witnesses,
            &mut has_filter,
            &mut has_get,
            &mut has_join,
            &mut has_join_region,
            &mut has_aggregate,
            &mut has_cte_consumer,
            &mut has_cte_producer,
            &mut has_ordering,
            &mut has_graph,
            &mut has_dependent,
            &mut exact_contract,
            &mut visited,
        )?;
        if !exact_contract || choices.is_empty() {
            return Ok(None);
        }

        let has_any = |candidates: &[RuleId]| candidates.iter().any(|rule| rules.contains(rule));
        if frozen.winner.provided.satisfies(required) {
            facts.insert(BundleFact::OutputDemand);
        }
        let Some(pending_domain_transfers) = quality_domain::pending_transfers(frozen, &state)
        else {
            return Ok(None);
        };
        if pending_domain_transfers.is_empty()
            && has_filter
            && has_get
            && has_any(&[
                PREDICATE_TRANSFER_RULE,
                KEY_DOMAIN_TRANSFER_RULE,
                CTE_FILTER_PUSHDOWN_RULE,
            ])
        {
            facts.insert(BundleFact::PredicateDomain);
        }
        if has_join && has_join_region {
            facts.insert(BundleFact::JoinRegion);
        }
        let Some(aggregate_regions) =
            selected_aggregate_region_witnesses(memo, &state, frozen, goal)
        else {
            return Ok(None);
        };
        shape.aggregate_witness_nodes = aggregate_regions
            .iter()
            .filter(|witness| witness.covered)
            .count() as u32;
        if has_aggregate
            && !aggregate_regions.is_empty()
            && aggregate_regions.iter().all(|witness| witness.covered)
        {
            facts.insert(BundleFact::AggregateDecomposition);
        }
        // A CTE bundle is complete only when every selected producer domain
        // that has a selected consumer has its own proof-bearing restriction.
        // This prevents one branch/consumer's transform from certifying a
        // different branch that merely shares the same CTE index.
        if has_cte_consumer
            && has_cte_producer
            && !cte_consumers.is_empty()
            && cte_consumers.is_subset(&cte_producers)
            && cte_producers.is_subset(&cte_producer_witnesses)
        {
            facts.insert(BundleFact::CteConsumerDemand);
        }
        if exact_contract && (has_aggregate || has_join) {
            facts.insert(BundleFact::NullSemantics);
        }
        if exact_contract {
            facts.insert(BundleFact::ProviderCapability);
        }
        if matches!(
            frozen.winner.provided.ordering,
            ProvidedOrdering::Ordered { .. }
        ) {
            facts.insert(BundleFact::OrderingDemand);
        }

        if has_filter && has_get {
            capabilities.insert(BundleCapability::ScanPredicate);
        }
        if has_join {
            capabilities.insert(BundleCapability::SmallJoin);
        }
        if has_aggregate && has_cte_consumer && has_cte_producer {
            capabilities.insert(BundleCapability::SharedAggregate);
        }
        if has_dependent {
            capabilities.insert(BundleCapability::CorrelatedSubquery);
        }
        if has_ordering {
            capabilities.insert(BundleCapability::Ordering);
        }
        if has_graph {
            capabilities.insert(BundleCapability::GraphProvider);
        }

        let mut region = StableFingerprintBuilder::default();
        region.write_bytes(b"paro.quality.native-region.v1");
        region.write_u64(reference.group.0 as u64);
        region.write_u64(reference.candidate.index() as u64);
        region.write_fingerprint(frozen.winner.physical_fingerprint);
        for choice in &choices {
            region.write_fingerprint(*choice);
        }
        let region = region.finish();
        let mut proof = StableFingerprintBuilder::default();
        proof.write_bytes(b"paro.quality.native-evidence.v1");
        proof.write_fingerprint(region);
        proof.write_u64(rules.len() as u64);
        for rule in &rules {
            proof.write_u64(rule.0 as u64);
        }
        proof.write_u64(capabilities.len() as u64);
        proof.write_u64(facts.len() as u64);
        proof.write_u64(aggregate_regions.len() as u64);
        for witness in &aggregate_regions {
            proof.write_fingerprint(witness.region);
            proof.write_fingerprint(witness.fact_fingerprint);
            proof.write_u64(u64::from(witness.covered));
        }
        Ok(Some(NativeQualityEvidence {
            pending_domain_transfers,
            capabilities,
            facts,
            region,
            applicability_proof: proof.finish(),
            choices: choices.into_boxed_slice(),
            aggregate_regions: aggregate_regions.into_boxed_slice(),
            selected_rules: rules.into_iter().collect(),
            shape,
        }))
    }
}

fn record_frozen_candidate_trace(
    trace: &paro_context::StatementTrace,
    memo: &Memo,
    state: &PlannerTransformState,
    prefix: &str,
    frozen: &FrozenCandidate,
) -> Result<()> {
    let mut choices = Vec::new();
    let mut reads = BTreeSet::new();
    let mut visited = BTreeSet::new();
    fn visit(
        memo: &Memo,
        state: &PlannerTransformState,
        frozen: &FrozenCandidate,
        choices: &mut Vec<(
            ChildWinnerRef,
            LogicalExprId,
            PhysicalExprId,
            Fingerprint,
            u32,
            u32,
            u64,
            u32,
            Box<[ChildWinnerRef]>,
            Box<[RuleId]>,
            Box<[RuleId]>,
            Box<[EquivalenceProof]>,
            Option<RuleId>,
            Box<[EquivalenceProof]>,
        )>,
        reads: &mut BTreeSet<PatternRead>,
        visited: &mut BTreeSet<CandidateId>,
    ) -> Result<()> {
        if !visited.insert(frozen.reference.candidate) {
            return Ok(());
        }
        let (origin_rule, payload_selected_proofs) = memo
            .logical_expr(frozen.logical.id)
            .and_then(|logical| state.metadata.get(&logical.payload))
            .map(|metadata| (metadata.origin_rule, metadata.selected_proofs.clone()))
            .unwrap_or((None, Box::new([])));
        choices.push((
            frozen.reference,
            frozen.logical.id,
            frozen.physical.id,
            frozen.winner.physical_fingerprint,
            frozen.logical.payload.0,
            frozen.physical.payload.0,
            frozen.logical.operator_tag.unwrap_or(u64::MAX),
            frozen.physical.key.implementation.0,
            frozen.winner.children.clone(),
            frozen.logical.applied_rules.iter().copied().collect(),
            selected_proof_rule_ids(&frozen.logical),
            frozen.logical.proofs.iter().cloned().collect(),
            origin_rule,
            payload_selected_proofs,
        ));
        reads.insert(PatternRead::facts_from_group(memo, frozen.reference.group)?);
        for child in frozen.children.iter() {
            visit(memo, state, child, choices, reads, visited)?;
        }
        Ok(())
    }
    visit(memo, state, frozen, &mut choices, &mut reads, &mut visited)?;
    trace.record_value(
        "optimizer",
        &format!("{prefix}.choice_count"),
        choices.len() as u64,
    );
    for (
        index,
        (
            reference,
            logical,
            physical,
            physical_fingerprint,
            logical_payload,
            physical_payload,
            operator_tag,
            physical_implementation,
            children,
            rules,
            selected_rules,
            proofs,
            origin_rule,
            payload_selected_proofs,
        ),
    ) in choices.iter().enumerate()
    {
        let choice_prefix = format!("{prefix}.choice_{index}");
        trace.record_value(
            "optimizer",
            &format!("{choice_prefix}.group"),
            reference.group.0 as u64,
        );
        trace.record_value(
            "optimizer",
            &format!("{choice_prefix}.candidate"),
            reference.candidate.index() as u64,
        );
        trace.record_value(
            "optimizer",
            &format!("{choice_prefix}.goal_required"),
            reference.goal.required.0 as u64,
        );
        trace.record_value(
            "optimizer",
            &format!("{choice_prefix}.goal_grant"),
            reference.goal.grant.stable_tag(),
        );
        trace.record_value(
            "optimizer",
            &format!("{choice_prefix}.goal_context"),
            reference.goal.context.0 as u64,
        );
        trace.record_value(
            "optimizer",
            &format!("{choice_prefix}.logical"),
            logical.index() as u64,
        );
        trace.record_value(
            "optimizer",
            &format!("{choice_prefix}.physical"),
            physical.index() as u64,
        );
        trace.record_value(
            "optimizer",
            &format!("{choice_prefix}.logical_payload"),
            *logical_payload as u64,
        );
        trace.record_value(
            "optimizer",
            &format!("{choice_prefix}.physical_payload"),
            *physical_payload as u64,
        );
        trace.record_value(
            "optimizer",
            &format!("{choice_prefix}.operator_tag"),
            *operator_tag,
        );
        trace.record_value(
            "optimizer",
            &format!("{choice_prefix}.physical_implementation"),
            u64::from(*physical_implementation),
        );
        trace.record_value(
            "optimizer",
            &format!("{choice_prefix}.physical_fingerprint_lo"),
            physical_fingerprint.0 as u64,
        );
        trace.record_value(
            "optimizer",
            &format!("{choice_prefix}.physical_fingerprint_hi"),
            (physical_fingerprint.0 >> 64) as u64,
        );
        trace.record_value(
            "optimizer",
            &format!("{choice_prefix}.child_count"),
            children.len() as u64,
        );
        for (child_index, child) in children.iter().enumerate() {
            let child_prefix = format!("{choice_prefix}.child_{child_index}");
            trace.record_value(
                "optimizer",
                &format!("{child_prefix}.group"),
                child.group.0 as u64,
            );
            trace.record_value(
                "optimizer",
                &format!("{child_prefix}.candidate"),
                child.candidate.index() as u64,
            );
            trace.record_value(
                "optimizer",
                &format!("{child_prefix}.goal_required"),
                child.goal.required.0 as u64,
            );
            trace.record_value(
                "optimizer",
                &format!("{child_prefix}.goal_grant"),
                child.goal.grant.stable_tag(),
            );
            trace.record_value(
                "optimizer",
                &format!("{child_prefix}.goal_context"),
                child.goal.context.0 as u64,
            );
        }
        trace.record_value(
            "optimizer",
            &format!("{choice_prefix}.rule_count"),
            rules.len() as u64,
        );
        for (rule_index, rule) in rules.iter().enumerate() {
            trace.record_value(
                "optimizer",
                &format!("{choice_prefix}.rule_{rule_index}"),
                rule.0 as u64,
            );
        }
        trace.record_value(
            "optimizer",
            &format!("{choice_prefix}.selected_rule_count"),
            selected_rules.len() as u64,
        );
        for (rule_index, rule) in selected_rules.iter().enumerate() {
            trace.record_value(
                "optimizer",
                &format!("{choice_prefix}.selected_rule_{rule_index}"),
                rule.0 as u64,
            );
        }
        trace.record_value(
            "optimizer",
            &format!("{choice_prefix}.proof_count"),
            proofs.len() as u64,
        );
        trace.record_value(
            "optimizer",
            &format!("{choice_prefix}.origin_rule"),
            origin_rule.map_or(u64::MAX, |rule| rule.0 as u64),
        );
        trace.record_value(
            "optimizer",
            &format!("{choice_prefix}.payload_selected_proof_count"),
            payload_selected_proofs.len() as u64,
        );
        for (proof_index, proof) in payload_selected_proofs.iter().enumerate() {
            let rule = match proof {
                EquivalenceProof::Transformation { rule, .. }
                | EquivalenceProof::TransformationDescendant { rule }
                | EquivalenceProof::SpecializedEnumerator { rule, .. }
                | EquivalenceProof::Normalization { rule } => *rule,
                EquivalenceProof::Initial => continue,
            };
            trace.record_value(
                "optimizer",
                &format!("{choice_prefix}.payload_selected_proof_{proof_index}"),
                rule.0 as u64,
            );
        }
        for (proof_index, proof) in proofs.iter().enumerate() {
            let proof_prefix = format!("{choice_prefix}.proof_{proof_index}");
            let (kind, rule, source, premise_or_region) = match proof {
                EquivalenceProof::Initial => (0_u64, None, None, None),
                EquivalenceProof::TransformationDescendant { rule } => (1, Some(*rule), None, None),
                EquivalenceProof::Normalization { rule } => (2, Some(*rule), None, None),
                EquivalenceProof::Transformation {
                    rule,
                    source,
                    premise,
                } => (3, Some(*rule), Some(*source), Some(*premise)),
                EquivalenceProof::SpecializedEnumerator { rule, region } => {
                    (4, Some(*rule), None, Some(*region))
                }
            };
            trace.record_value("optimizer", &format!("{proof_prefix}.kind"), kind);
            trace.record_value(
                "optimizer",
                &format!("{proof_prefix}.rule"),
                rule.map_or(u64::MAX, |rule| rule.0 as u64),
            );
            trace.record_value(
                "optimizer",
                &format!("{proof_prefix}.source"),
                source.map_or(u64::MAX, |source| source.index() as u64),
            );
            if let Some(value) = premise_or_region {
                trace.record_value(
                    "optimizer",
                    &format!("{proof_prefix}.fingerprint_lo"),
                    value.0 as u64,
                );
                trace.record_value(
                    "optimizer",
                    &format!("{proof_prefix}.fingerprint_hi"),
                    (value.0 >> 64) as u64,
                );
            }
        }
    }
    trace.record_value(
        "optimizer",
        &format!("{prefix}.fact_read_count"),
        reads.len() as u64,
    );
    for (index, read) in reads.iter().enumerate() {
        let read_prefix = format!("{prefix}.fact_read_{index}");
        trace.record_value(
            "optimizer",
            &format!("{read_prefix}.group"),
            read.group.0 as u64,
        );
        trace.record_value(
            "optimizer",
            &format!("{read_prefix}.scope"),
            u64::from(read.scope.bits()),
        );
        trace.record_value(
            "optimizer",
            &format!("{read_prefix}.logical_frontier_present"),
            u64::from(read.logical_frontier_revision.is_some()),
        );
        trace.record_value(
            "optimizer",
            &format!("{read_prefix}.physical_frontier_present"),
            u64::from(read.physical_frontier_revision.is_some()),
        );
        trace.record_value(
            "optimizer",
            &format!("{read_prefix}.logical_fact_lo"),
            read.logical_fact_fingerprint.0 as u64,
        );
        trace.record_value(
            "optimizer",
            &format!("{read_prefix}.logical_fact_hi"),
            (read.logical_fact_fingerprint.0 >> 64) as u64,
        );
        trace.record_value(
            "optimizer",
            &format!("{read_prefix}.statistics_lo"),
            read.statistics_snapshot_fingerprint.0 as u64,
        );
        trace.record_value(
            "optimizer",
            &format!("{read_prefix}.statistics_hi"),
            (read.statistics_snapshot_fingerprint.0 >> 64) as u64,
        );
    }
    Ok(())
}

struct SearchStagingRequest<'a> {
    plan: OwnedLogicalPlan,
    expected_output_bindings: &'a [ColumnBinding],
    expected_output_types: &'a [paro_common::types::LogicalType],
    output_columns: &'a [ColumnId],
    materialized_columns: &'a BTreeSet<ColumnId>,
    binding_ids: &'a BindingCatalog,
    operator_fingerprint: Fingerprint,
    output_rows_hard_upper: Option<u64>,
    column_stats: &'a HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    scan_access_cost: paro_storage::rowset::scan_cost::ScanAccessCostModel,
}

fn stage_search_implementation(
    request: SearchStagingRequest<'_>,
    payloads: &mut PlannerPayloadArena,
) -> Result<PlannerSearchImplementationMetadata> {
    let SearchStagingRequest {
        mut plan,
        expected_output_bindings,
        expected_output_types,
        output_columns,
        materialized_columns,
        binding_ids,
        operator_fingerprint,
        output_rows_hard_upper,
        column_stats,
        scan_access_cost,
    } = request;
    if plan.get_column_bindings() != expected_output_bindings
        || plan.types() != expected_output_types
    {
        return Err(paro_error::internal(
            "physical search candidate changed its logical output contract",
        ));
    }
    let payload_fingerprint = search_payload_fingerprint(operator_fingerprint, &plan.operator);
    let provided = ProvidedProperties {
        ordering: derive_provided_ordering(&plan.operator, output_columns, None, binding_ids),
        partitioning: ProvidedPartitioning::Singleton,
        materialization: ProvidedMaterialization {
            values: materialized_columns.clone(),
            locators: BTreeMap::new(),
        },
        mutation_safety: ProvidedMutationSafety::NotApplicable,
        representation: ProvidedRepresentation::Flat,
        replayability: ProvidedReplayability::OnePass,
        result_guarantee: provided_result_guarantee(&plan.operator),
    };
    let local_cost =
        planner_operator_cost(&plan, 0, output_rows_hard_upper, &[], scan_access_cost)?;
    let cost_facts = planner_cost_facts(&plan, column_stats, binding_ids, scan_access_cost)?;
    plan.stats = NodeStats::default();
    let payload = payloads.push_physical(PlannerPhysicalTemplate::Executable(Box::new(plan)));
    Ok(PlannerSearchImplementationMetadata {
        payload,
        payload_fingerprint,
        provided,
        local_cost,
        cost_facts,
    })
}

/// Runtime-filter dependency direction declared by a physical implementation.
///
/// Candidate construction and winner verification share this implementation
/// metadata, while the verifier still resolves and validates the endpoints
/// independently against the winning physical children.
pub(super) const fn runtime_filter_dependency_boundary(
    implementation: ImplementationId,
) -> Option<(RegionBoundaryEndpoint, RegionBoundaryEndpoint)> {
    if implementation.0 == PLANNER_HASH_JOIN_RUNTIME_FILTER.0 {
        Some((
            RegionBoundaryEndpoint::Input(1),
            RegionBoundaryEndpoint::Input(0),
        ))
    } else if implementation.0 == PLANNER_HASH_JOIN_BUILD_LEFT_RUNTIME_FILTER.0 {
        Some((
            RegionBoundaryEndpoint::Input(0),
            RegionBoundaryEndpoint::Input(1),
        ))
    } else {
        None
    }
}
pub const SEARCH_REGION_ENUMERATOR_RULE: super::ids::RuleId = super::ids::RuleId(10_002);
pub const GRAPH_REGION_ENUMERATOR_RULE: super::ids::RuleId = super::ids::RuleId(10_003);

#[derive(Debug)]
pub struct LogicalAlternative {
    pub plan: OwnedLogicalPlan,
    pub source: AlternativeOrigin,
    /// Immutable estimator input owned by this alternative. Search providers
    /// must never observe statistics left behind by a different candidate.
    pub column_stats: SharedColumnStatistics,
}

#[derive(Debug, Clone, Copy)]
pub enum AlternativeOrigin {
    Baseline,
    Specialized { rule: super::ids::RuleId },
}

#[derive(Debug, Clone)]
pub struct ResultPresentation {
    pub columns: Box<[ColumnId]>,
    pub names: Box<[String]>,
}

#[derive(Debug)]
pub struct OptimizationInput {
    pub memo: Memo,
    pub root: GroupId,
    pub root_goal: OptimizationGoal,
    pub mode: SearchMode,
    pub presentation: ResultPresentation,
    planner_state: Arc<RwLock<PlannerTransformState>>,
    bind_context: BindContext,
    calibration: Arc<MachineCalibrationBundle>,
    force_spill: bool,
    /// `None` follows the process experiment setting; `Some(false)` is used
    /// by the strong-seed source Memo so A/B/C/D compare the same seed.
    certified_group_pruning: Option<bool>,
    strong_incumbent_plans: Vec<SeedPlan>,
    prepriced_strong_incumbents: Vec<PricedIncumbent>,
    export_strong_incumbent: bool,
}

impl OptimizationInput {
    pub fn with_calibration(mut self, calibration: Arc<MachineCalibrationBundle>) -> Self {
        self.calibration = calibration;
        self
    }

    pub fn with_force_spill(mut self, force_spill: bool) -> Self {
        self.force_spill = force_spill;
        self
    }

    /// Enable the proof-backed group-pruning experiment. The flag is kept on
    /// the input rather than coupled to tracing so normal C1 can remain
    /// measurement-clean. Benchmark/diagnostic callers may also use the
    /// `PARO_CERTIFIED_GROUP_PRUNING=1` process setting for an isolated run.
    pub fn with_certified_group_pruning(mut self, enabled: bool) -> Self {
        self.certified_group_pruning = Some(enabled);
        self
    }

    /// Install an independently exported, immutable plan in the new Memo.
    /// The plan is re-priced against this input's grant/statistics/calibration
    /// context before it can become a proof upper bound.
    pub fn with_strong_incumbent_plan(mut self, plan: SeedPlan) -> Self {
        self.strong_incumbent_plans.push(plan);
        self
    }

    /// Install a SeedPlan that was re-priced in an independent, non-search
    /// Memo. The destination search receives only the immutable priced
    /// witness; its source logical shell is never added to this Memo unless
    /// the caller explicitly requests logical injection.
    pub(crate) fn with_prepriced_strong_incumbent(mut self, incumbent: PricedIncumbent) -> Self {
        self.prepriced_strong_incumbents.push(incumbent);
        self
    }

    /// Export verified winner DAGs for a diagnostic caller that will install
    /// them in a separately constructed Memo. This is deliberately opt-in:
    /// freezing an additional tree is setup work and must never enter normal
    /// C1 merely because a tracing or pruning switch is present.
    pub fn with_strong_incumbent_export(mut self, enabled: bool) -> Self {
        self.export_strong_incumbent = enabled;
        self
    }

    /// Re-price fixed seed DAGs in a separate Memo without running logical or
    /// physical search. This is the isolation path for the strong-incumbent
    /// experiment: the temporary Memo may contain the source logical shells
    /// needed to resolve the selected DAG, but no such shell is published to
    /// the destination search Memo.
    pub(crate) fn reprice_seed_plans_in_isolated_memo(
        mut self,
        grant_classes: &[ResourceGrantClass],
        plans: &[SeedPlan],
    ) -> Result<Vec<PricedIncumbent>> {
        if plans.is_empty() {
            return Ok(Vec::new());
        }
        self.memo.set_calibration(self.calibration.clone());
        let grant_classes = Arc::new(
            grant_classes
                .iter()
                .map(|class| (class.id, *class))
                .collect::<BTreeMap<_, _>>(),
        );
        let mut registry = ImplementationRegistry::default();
        implementation::register_implementations(
            &mut registry,
            self.planner_state.clone(),
            grant_classes.clone(),
            self.calibration.clone(),
            self.force_spill,
        )?;
        let mut engine = CascadesEngine::new(self.memo, registry);
        engine.prime_grant_context(grant_classes.values().copied())?;
        engine.reprice_strong_incumbents_for_grants(
            self.root,
            self.root_goal,
            AdmissibleGrantSetId(0),
            grant_classes.values().copied(),
            plans,
        )
    }

    /// Strengthen the query root for a self-reading mutation. The mandatory
    /// physical baseline then owns the correctness barrier; statement lowering
    /// is not allowed to infer or insert one after winner selection.
    pub fn require_stable_mutation_input(
        mut self,
        targets: BTreeSet<BaseRelationId>,
        snapshot: SnapshotId,
    ) -> Result<Self> {
        if targets.is_empty() {
            return Ok(self);
        }
        let mut required = self
            .memo
            .required(self.root_goal.required)
            .cloned()
            .ok_or_else(|| paro_error::internal("root mutation goal lost its property set"))?;
        required.mutation_safety =
            MutationSafetyRequirement::StableReadBeforeWrite { targets, snapshot };
        self.root_goal.required = self.memo.intern_required(required)?;
        Ok(self)
    }

    pub fn optimize(mut self, grant_classes: &[ResourceGrantClass]) -> Result<OptimizationOutput> {
        let preparation_partition = crate::work_partition::enter(crate::work_partition::Bucket::Pre);
        if grant_classes.is_empty() {
            return Err(paro_error::internal(
                "planner optimization requires at least one resource grant class",
            ));
        }
        let mode = self.mode;
        self.memo.set_calibration(self.calibration.clone());
        let grant_classes = Arc::new(
            grant_classes
                .iter()
                .map(|class| (class.id, *class))
                .collect::<BTreeMap<_, _>>(),
        );
        let mut registry = ImplementationRegistry::default();
        if self
            .planner_state
            .read()
            .expect("planner transform state poisoned")
            .binder
            .is_some()
        {
            transformation::register_transformations(&mut registry, self.planner_state.clone())?;
        }
        implementation::register_implementations(
            &mut registry,
            self.planner_state.clone(),
            grant_classes.clone(),
            self.calibration.clone(),
            self.force_spill,
        )?;
        let mut engine = CascadesEngine::new(self.memo, registry);
        let quality_handoff = std::env::var_os("PARO_QUALITY_POLICY_HANDOFF")
            .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
        if quality_handoff {
            engine.set_quality_policy_handoff_enabled(true);
            engine.set_quality_evidence_provider(Arc::new(PlannerQualityEvidenceProvider {
                state: self.planner_state.clone(),
            }));
        }
        engine.prime_grant_context(grant_classes.values().copied())?;
        let strong_incumbent_plans = self.strong_incumbent_plans.clone();
        let prepriced_strong_incumbents = std::mem::take(&mut self.prepriced_strong_incumbents);
        let first_seed_identity = prepriced_strong_incumbents
            .first()
            .map(|incumbent| incumbent.plan().plan_identity())
            .or_else(|| {
                strong_incumbent_plans
                    .first()
                    .map(|plan| plan.plan_identity())
            });
        if !strong_incumbent_plans.is_empty() || !prepriced_strong_incumbents.is_empty() {
            let state = self
                .planner_state
                .read()
                .expect("planner transform state poisoned");
            if let Some(trace) = state
                .session
                .as_ref()
                .and_then(|context| context.statement_trace())
            {
                if let Some(root_group) = engine.memo().group(self.root) {
                    trace.record_value(
                        "optimizer",
                        "strong_incumbent_target_root_logical_count",
                        root_group.logical_exprs().len() as u64,
                    );
                    for (ordinal, logical_id) in root_group.logical_exprs().iter().enumerate() {
                        let Some(logical) = engine.memo().logical_expr(*logical_id) else {
                            continue;
                        };
                        let Some(metadata) = state.metadata.get(&logical.payload) else {
                            continue;
                        };
                        trace.record_value(
                            "optimizer",
                            &format!("strong_incumbent_target_root_{ordinal}_logical_id"),
                            logical_id.index() as u64,
                        );
                        trace.record_value(
                            "optimizer",
                            &format!("strong_incumbent_target_root_{ordinal}_output_width"),
                            metadata.output_columns.len() as u64,
                        );
                        for (column_ordinal, column) in metadata.output_columns.iter().enumerate() {
                            trace.record_value(
                                "optimizer",
                                &format!(
                                    "strong_incumbent_target_root_{ordinal}_output_column_{column_ordinal}"
                                ),
                                column.0 as u64,
                            );
                        }
                    }
                }
                if let Some(required) = engine.memo().required(self.root_goal.required) {
                    for (ordinal, column) in required.materialization.values.iter().enumerate() {
                        trace.record_value(
                            "optimizer",
                            &format!("strong_incumbent_target_required_column_{ordinal}"),
                            column.0 as u64,
                        );
                    }
                }
            }
        }
        let seed_reprice_started = Instant::now();
        let seed_reprice_count = if prepriced_strong_incumbents.is_empty() {
            engine.install_strong_incumbent_for_grants(
                self.root,
                self.root_goal,
                AdmissibleGrantSetId(0),
                grant_classes.values().copied(),
                &strong_incumbent_plans,
            )?
        } else {
            let mut installed = 0_u64;
            for incumbent in prepriced_strong_incumbents {
                let target_goal = OptimizationGoal {
                    grant: incumbent.goal().grant,
                    ..self.root_goal
                };
                let incumbent =
                    engine.rebind_priced_incumbent_to_current_facts(incumbent, target_goal)?;
                engine.install_priced_incumbent(incumbent)?;
                installed = installed.saturating_add(1);
            }
            installed
        };
        let seed_reprice_us = (seed_reprice_count > 0)
            .then(|| u64::try_from(seed_reprice_started.elapsed().as_micros()).unwrap_or(u64::MAX));
        engine.set_rule_work_profile_enabled(paro_context::StatementTrace::enabled());
        let env_certified_group_pruning = std::env::var_os("PARO_CERTIFIED_GROUP_PRUNING")
            .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
        engine.set_certified_group_pruning_enabled(
            self.certified_group_pruning
                .unwrap_or(env_certified_group_pruning),
        );
        let disable_protected_incumbent = std::env::var_os("PARO_DISABLE_PROTECTED_INCUMBENT")
            .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
        engine.set_protected_incumbent_enabled(!disable_protected_incumbent);
        let statement_context = self.planner_state.read().unwrap().session.clone();
        if let Some(session) = &statement_context {
            engine
                .memo_mut()
                .set_cancellation(session.cancellation.clone())?;
        }
        // Selection and the cache key both consume this statement's frozen
        // availability. Never sample live resources again inside optimization.
        let expected_class = statement_context.as_ref().and_then(|session| {
            session
                .compile_resources
                .expected_grant(
                    session.limits.max_memory,
                    session.limits.max_threads,
                    engine.memo().budget().max_grant_classes,
                )
                .map(|class| ResourceGrantClassId::new(class.index))
        });
        drop(preparation_partition);
        let grant_optimization = if statement_context.is_some() {
            engine.optimize_for_expected_grant(
                self.root,
                self.root_goal,
                AdmissibleGrantSetId(0),
                grant_classes.values().copied(),
                self.mode,
                expected_class,
            )?
        } else {
            // Standalone Memo callers have no compilation resource snapshot.
            // Preserve their explicit eager contract rather than inventing one.
            engine.optimize_for_grants(
                self.root,
                self.root_goal,
                AdmissibleGrantSetId(0),
                grant_classes.values().copied(),
                self.mode,
            )?
        };
        let _finish_partition = crate::work_partition::enter(crate::work_partition::Bucket::Finish);
        engine.note_search_return();
        let export_strong_incumbents = self.export_strong_incumbent
            || std::env::var_os("PARO_EXPORT_STRONG_INCUMBENT")
                .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
        let strong_incumbent_export_started = Instant::now();
        let strong_incumbent_plans = if export_strong_incumbents {
            grant_optimization
                .winners
                .iter()
                .filter_map(|winner| engine.export_seed_plan(self.root, winner.goal).transpose())
                .collect::<Result<Vec<_>>>()?
                .into_boxed_slice()
        } else {
            Box::new([])
        };
        let strong_incumbent_export_us = export_strong_incumbents.then(|| {
            u64::try_from(strong_incumbent_export_started.elapsed().as_micros()).unwrap_or(u64::MAX)
        });
        let strong_incumbent_logical_plans = if export_strong_incumbents {
            let state = self
                .planner_state
                .read()
                .expect("planner transform state poisoned");
            strong_incumbent_plans
                .iter()
                .map(|seed| materialize_seed_logical_plan(&state, &self.bind_context, seed))
                .collect::<Result<Vec<_>>>()?
                .into_boxed_slice()
        } else {
            Box::new([])
        };
        let stop = grant_optimization.stop;
        let grant_search = grant_optimization.grant_search.clone();
        let mut selected_winners = grant_optimization.winners.into_vec();
        for safe in grant_optimization.safe_winners {
            if !selected_winners.iter().any(|winner| {
                winner.class == safe.class && Arc::ptr_eq(&winner.frozen, &safe.frozen)
            }) {
                selected_winners.push(safe);
            }
        }
        let extraction_started = Instant::now();
        let mut variants = Vec::with_capacity(selected_winners.len());
        debug!(target: targets::OPTIMIZER, groups = engine.memo().group_count(), attempts = ?engine.rule_attempts(), insertions = ?engine.effective_rule_insertions(), "completed Memo search work");
        for (winner_index, grant_winner) in selected_winners.into_iter().enumerate() {
            let winner = &grant_winner.winner;
            if let Some(trace) = statement_context
                .as_ref()
                .and_then(|context| context.statement_trace())
            {
                let planner_state = self
                    .planner_state
                    .read()
                    .expect("planner transform state poisoned");
                record_frozen_candidate_trace(
                    &trace,
                    engine.memo(),
                    &planner_state,
                    &format!("final_winner_{winner_index}"),
                    &grant_winner.frozen,
                )?;
            }
            if winner.provided.result_guarantee != ResultGuarantee::Exact {
                let required = engine
                    .memo()
                    .required(grant_winner.goal.required)
                    .ok_or_else(|| {
                        paro_error::internal("root winner lost its required properties")
                    })?;
                if !winner
                    .provided
                    .result_guarantee
                    .satisfies(required.result_guarantee)
                {
                    return Err(paro_error::internal(
                        "Memo builder selected a search guarantee weaker than the root contract",
                    ));
                }
            }
            let extracted = {
                let planner_state = self.planner_state.read().unwrap();
                verify_frozen_candidate_payloads(&grant_winner.frozen, &planner_state)?;
                extract_frozen_planner_tree(
                    engine.memo(),
                    &planner_state,
                    &self.bind_context,
                    self.root,
                    grant_winner.goal,
                    grant_winner.frozen.clone(),
                    mode,
                )?
            };
            let PresentedWinnerTree {
                plan,
                contracts,
                enforcers,
                physical_fingerprint,
                cost,
            } = enforce_result_presentation(
                extracted,
                &self.presentation,
                &self.bind_context,
                self.calibration.as_ref(),
                winner.physical_fingerprint,
                winner.cost,
            )?;
            variants.push(OptimizedVariant {
                class: grant_winner.class,
                plan,
                contracts: Arc::new(contracts),
                enforcers: Arc::new(enforcers),
                write_contracts: Arc::new(std::collections::HashMap::new()),
                physical_fingerprint,
                cost,
            });
        }
        let handoff_extraction_us =
            u64::try_from(extraction_started.elapsed().as_micros()).unwrap_or(u64::MAX);
        let rule_insertions = engine.effective_rule_insertions().clone();
        let rule_attempts = engine.rule_attempts().clone();
        let rule_elapsed = engine.rule_elapsed().clone();
        let rule_allocated_bytes = engine.rule_allocated_bytes().clone();
        let rule_budget_exhaustions = engine.rule_budget_exhaustions().clone();
        let rule_work_profile = engine.rule_work_profile().clone();
        let mut search_milestones = engine.search_milestones().clone();
        if !matches!(stop.reason, SearchStopReason::Complete) {
            search_milestones.handoff_extraction_us = Some(handoff_extraction_us);
        }
        if let Some(trace) = statement_context
            .as_ref()
            .and_then(|context| context.statement_trace())
        {
            let stop_event = match stop.reason {
                SearchStopReason::Complete => "search_complete",
                SearchStopReason::SearchIncomplete => "search_incomplete",
                SearchStopReason::Deadline => "search_stop_deadline",
                SearchStopReason::BudgetLimited => "search_stop_budget_limited",
                SearchStopReason::RuleFailure => "search_stop_rule_failure",
                SearchStopReason::QualityPolicySatisfied => "quality_policy_satisfied",
            };
            trace.record_event("optimizer", stop_event);
            if stop.budget_limited {
                trace.record_event("optimizer", "search_budget_limited");
            }
            for (name, value) in [
                ("search_deadline_us", stop.configured_deadline_us),
                ("search_actual_stop_us", stop.actual_stop_us),
                (
                    "search_stop_profile_us",
                    search_milestones.search_stop_profile_us,
                ),
                (
                    "search_return_profile_us",
                    search_milestones.search_return_profile_us,
                ),
                (
                    "search_timeout_tail_profile_us",
                    search_milestones.timeout_tail_profile_us,
                ),
                (
                    "search_freeze_elapsed_us",
                    Some(search_milestones.freeze_elapsed_us),
                ),
                (
                    "search_frozen_candidate_count",
                    Some(search_milestones.frozen_candidate_count),
                ),
                (
                    "search_handoff_extraction_us",
                    search_milestones.handoff_extraction_us,
                ),
                (
                    "quality_policy_satisfied_us",
                    search_milestones.quality_policy_satisfied_us,
                ),
            ] {
                if let Some(value) = value {
                    trace.record_value("optimizer", name, value);
                }
            }
            if let Some(candidate) = search_milestones.quality_policy_candidate {
                trace.record_value(
                    "optimizer",
                    "quality_policy_candidate",
                    candidate.index() as u64,
                );
            }
            let quality_evaluation = engine.quality_last_evaluation();
            trace.record_value(
                "optimizer",
                "quality_last_completed_bundle_count",
                quality_evaluation.completed,
            );
            trace.record_value(
                "optimizer",
                "quality_last_not_applicable_bundle_count",
                quality_evaluation.not_applicable,
            );
            trace.record_value(
                "optimizer",
                "quality_last_missing_bundle_count",
                quality_evaluation.missing_evidence,
            );
            trace.record_value(
                "optimizer",
                "quality_last_missing_fact_count",
                quality_evaluation.missing_facts,
            );
            for (index, bundle) in quality_evaluation.missing_bundles.iter().enumerate() {
                trace.record_value(
                    "optimizer",
                    &format!("quality_last_missing_bundle_{index}"),
                    bundle.0 as u64,
                );
            }
            for (index, fact) in quality_evaluation.missing_fact_kinds.iter().enumerate() {
                trace.record_value(
                    "optimizer",
                    &format!("quality_last_missing_fact_{index}"),
                    fact.stable_tag(),
                );
            }
        }
        let mut work_counters = engine.search_work_counters();
        {
            let state = self
                .planner_state
                .read()
                .expect("planner transform state poisoned");
            work_counters.insert("settlement_local_hit_count", state.settlement_cache.hits);
            work_counters.insert("settlement_local_miss_count", state.settlement_cache.misses);
            work_counters.insert(
                "settlement_input_column_cache_hits",
                state.settlement_cache.input_column_cache_hits,
            );
            work_counters.insert(
                "settlement_input_column_cache_misses",
                state.settlement_cache.input_column_cache_misses,
            );
            work_counters.insert(
                "settlement_invalidation_visit_count",
                state.settlement_cache.invalidation_visits,
            );
            work_counters.insert(
                "native_relation_fact_cache_hits",
                state.settlement_cache.native_relation_hits,
            );
            work_counters.insert(
                "native_relation_fact_cache_misses",
                state.settlement_cache.native_relation_misses,
            );
            work_counters.insert(
                "native_relation_fact_evaluations",
                state.settlement_cache.native_relation_fact_evaluations,
            );
            work_counters.insert(
                "native_relation_owned_assembly_skips",
                state.settlement_cache.native_relation_owned_assembly_skips,
            );
            work_counters.insert(
                "native_relation_fact_cache_entries",
                state.settlement_cache.native_relation_entry_count(),
            );
            work_counters.insert(
                "native_relation_fact_cache_rollbacks",
                state.settlement_cache.native_relation_invalidations,
            );
            work_counters.extend(state.payloads.schedule_counters());
            let join_region_cache = state
                .join_region_cache
                .lock()
                .expect("join-region cache poisoned");
            work_counters.insert("join_region_cache_hits", join_region_cache.hits);
            work_counters.insert("join_region_cache_builds", join_region_cache.builds);
            let witness_cache = state
                .witness_cache
                .lock()
                .expect("pattern-witness cache poisoned");
            work_counters.insert(
                "pattern_witness_cache_entries",
                witness_cache.entries.len() as u64,
            );
            work_counters.insert("pattern_witness_cache_hits", witness_cache.hits);
            work_counters.insert(
                "pattern_witness_cache_invalidations",
                witness_cache.invalidations,
            );
            work_counters.insert(
                "pattern_witness_root_dispatch_skips",
                witness_cache.root_dispatch_skips,
            );
            let (hits, misses) = state.scalars.bound_import_counts();
            work_counters.insert("memo_scalar_import_hits", hits);
            work_counters.insert("memo_scalar_import_misses", misses);
            // Settlement and staging now borrow the same session scalar
            // arena. Keep one authoritative counter; separate "settlement"
            // totals would imply a namespace that no longer exists.
            work_counters.insert("planner_scalar_import_hits", hits);
            work_counters.insert("planner_scalar_import_misses", misses);
        }
        let search_summary = SearchSummary {
            groups: u64::try_from(engine.memo().canonical_group_count()).unwrap_or(u64::MAX),
            logical_expressions: u64::try_from(engine.memo().logical_expr_count())
                .unwrap_or(u64::MAX),
            physical_expressions: u64::try_from(engine.memo().physical_expr_count())
                .unwrap_or(u64::MAX),
            exhaustion_events: engine.memo().exhaustion_counts(),
            obligations: engine.memo().search_obligations(),
            work_counters,
            physical_search: engine.memo().physical_search_profile(),
        };
        Ok(OptimizationOutput {
            grant_search,
            variants: variants.into_boxed_slice(),
            rule_attempts,
            rule_insertions,
            rule_elapsed,
            rule_allocated_bytes,
            rule_budget_exhaustions,
            rule_work_profile,
            search_milestones,
            search_summary,
            search_stop: stop,
            quality_policy_status: engine.quality_policy_status(),
            strong_incumbent_plans,
            strong_incumbent_logical_plans,
            strong_incumbent_reprice_us: seed_reprice_us,
            strong_incumbent_reprice_count: seed_reprice_count,
            strong_incumbent_plan_identity: first_seed_identity,
            strong_incumbent_export_us,
        })
    }
}

#[derive(Debug)]
pub struct OptimizationOutput {
    pub grant_search: Option<crate::physical::GrantSearchCoverage>,
    pub variants: Box<[OptimizedVariant]>,
    /// Rule applications that passed structural matching and budget admission.
    /// Comparing this with `rule_insertions` measures pre-match precision.
    pub rule_attempts: BTreeMap<RuleId, u64>,
    /// Logical expressions actually inserted into an equivalence group by
    /// each transformation. Matching, scheduling, and duplicate replay do not
    /// count as an effect.
    pub rule_insertions: BTreeMap<RuleId, u64>,
    /// Binding construction plus rule application time, aggregated by rule.
    pub rule_elapsed: BTreeMap<RuleId, std::time::Duration>,
    pub rule_allocated_bytes: BTreeMap<RuleId, u64>,
    pub rule_budget_exhaustions: BTreeMap<RuleId, u64>,
    pub rule_work_profile: BTreeMap<RuleId, super::engine::RuleWorkProfile>,
    pub search_milestones: super::engine::SearchMilestones,
    pub search_summary: SearchSummary,
    /// Search stop is explicit so a policy handoff is not mistaken for
    /// ProofComplete or a deadline/budget stop.
    pub search_stop: super::engine::SearchStop,
    pub quality_policy_status: QualityPolicyStatus,
    /// Exported only when `PARO_EXPORT_STRONG_INCUMBENT=1`; diagnostic/setup
    /// callers can feed these immutable seeds to a fresh Memo.
    pub strong_incumbent_plans: Box<[SeedPlan]>,
    /// Logical shells for the selected seed DAGs. These are a transport
    /// artifact only: the destination builds fresh physical metadata and
    /// re-prices the immutable SeedPlan against its own context.
    pub strong_incumbent_logical_plans: Box<[OwnedLogicalPlan]>,
    /// Destination import/re-pricing time, kept separate from target search
    /// so diagnostic reports can explain the cost of making a source plan a
    /// valid upper bound. `None` means no plan was supplied.
    pub strong_incumbent_reprice_us: Option<u64>,
    pub strong_incumbent_reprice_count: u64,
    pub strong_incumbent_plan_identity: Option<Fingerprint>,
    pub strong_incumbent_export_us: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchSummary {
    pub groups: u64,
    pub logical_expressions: u64,
    pub physical_expressions: u64,
    pub exhaustion_events: BTreeMap<BudgetDimension, u64>,
    pub obligations: Box<[super::budget::SearchObligation]>,
    pub work_counters: BTreeMap<&'static str, u64>,
    pub physical_search: super::memo::PhysicalSearchProfile,
}

impl SearchSummary {
    /// Whether optional search reached every configured frontier. Mandatory
    /// normalization and baseline implementations remain valid when false,
    /// but the selected winner is explicitly incomplete. Advisory rule
    /// failures and budget omissions are distinguished in the obligations.
    pub fn is_complete(&self) -> bool {
        self.obligations.is_empty() && self.exhaustion_events.is_empty()
    }
}

#[derive(Debug)]
pub struct OptimizedVariant {
    pub class: super::ids::ResourceGrantClassId,
    pub plan: OwnedLogicalPlan,
    pub(crate) contracts: WinnerPhysicalContracts,
    pub(crate) enforcers: ExtractedEnforcerContracts,
    pub(crate) write_contracts: crate::physical::StatementWriteContracts,
    pub physical_fingerprint: Fingerprint,
    pub cost: SearchCost,
}

#[derive(Debug, Clone)]
struct BuildState {
    group: GroupId,
    logical: super::ids::LogicalExprId,
    columns: Box<[ColumnId]>,
    region_scope: PlannerRegionScope,
}

fn attach_group_column_domains(
    properties: &mut LogicalProperties,
    output_bindings: &[ColumnBinding],
    output_columns: &[ColumnId],
    column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    schema: &GroupSchema,
) -> Result<()> {
    for (&binding, &column) in output_bindings.iter().zip(output_columns) {
        let declared = schema
            .columns()
            .iter()
            .find(|entry| entry.id == column)
            .ok_or_else(|| paro_error::internal("column fact has no declared schema"))?;
        let statistics = column_stats.get(&binding).filter(|statistics| {
            let matches = statistics.get_type() == &declared.logical_type;
            if !matches {
                tracing::debug!(target: "paro::optimizer", ?binding, ?column,
                    expected = ?declared.logical_type, actual = ?statistics.get_type(),
                    "discarded ill-typed scalar statistics at Memo publication");
            }
            matches
        });
        if let Some(statistics) = statistics {
            properties.column_values.insert(
                column,
                paro_planner::operator::bound_reference::BoundColumnValues::from_column(
                    statistics,
                )?,
            );
        }
        let evidence = statistics.map(|statistics| statistics.distinct_evidence());
        let evidence = evidence.unwrap_or_default();
        let Some(mut domain) = GroupColumnDomain::from_evidence(
            evidence,
            // CardinalityEstimate::max is an uncertain observation, not a
            // semantic row bound. Using it to cap NDV made a three-valued
            // column collapse to one whenever a sibling alternative had a
            // one-row estimate. Only the group's explicit maximum proof may
            // narrow a distinct domain here.
            properties.maximum_cardinality,
        ) else {
            continue;
        };
        // A maximum-cardinality proof is stronger than an HLL point but must
        // not change its ranking point. Keep it as a separate upper bound so
        // costing and semantic admission consume the right evidence channel.
        domain.guaranteed_upper = domain
            .guaranteed_upper
            .into_iter()
            .chain(properties.maximum_cardinality)
            .min();
        properties
            .column_domains
            .entry(column)
            .and_modify(|current| *current = current.canonical_with(domain))
            .or_insert(domain);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PendingPlannerRegionFacets {
    required: Option<Fingerprint>,
    runtime_filter: Option<Fingerprint>,
}

pub struct MemoBuilder;

fn cte_reference_domain(
    reference: &paro_planner::operator::CTERef,
    columns: &[ColumnId],
) -> Result<CteReferenceDomain> {
    if reference.definition_columns.len() != columns.len() {
        return Err(paro_error::internal(
            "CTE reference column correspondence has inconsistent arity",
        ));
    }
    let mut mapping = BTreeMap::new();
    for (definition, column) in reference.definition_columns.iter().zip(columns) {
        if mapping.insert(*definition, *column).is_some() {
            return Err(paro_error::internal(
                "CTE reference repeats a definition column",
            ));
        }
    }
    Ok(CteReferenceDomain {
        cte_index: reference.cte_index,
        columns: mapping,
    })
}

fn cte_producer_columns<Child>(
    cte: &paro_planner::operator::MaterializedCTE<Child>,
    producer: GroupId,
    memo: &Memo,
    bindings: &BindingCatalog,
) -> Result<BTreeMap<paro_planner::operator::cte::CteColumnId, ColumnId>> {
    let schema = &memo
        .group(producer)
        .ok_or_else(|| paro_error::internal("CTE producer group is missing"))?
        .schema;
    let mut mapping = BTreeMap::new();
    for column in &cte.output_columns {
        let ty = cte
            .column_types
            .get(column.definition.0)
            .ok_or_else(|| paro_error::internal("CTE definition column has no declared type"))?;
        let Some(id) = bindings.get(column.binding.table_index, column.binding.column_index, ty)
        else {
            continue;
        };
        // A pruned producer may no longer expose this definition column. Its
        // absence contributes no evidence, never the next positional column.
        if schema.contains(*id) && mapping.insert(column.definition, *id).is_some() {
            return Err(paro_error::internal(
                "CTE producer repeats a definition column",
            ));
        }
    }
    Ok(mapping)
}

impl MemoBuilder {
    pub fn build(
        plan: OwnedLogicalPlan,
        bind_context: BindContext,
        budget: SearchBudget,
    ) -> Result<OptimizationInput> {
        Self::build_inner(
            vec![LogicalAlternative {
                plan,
                source: AlternativeOrigin::Baseline,
                column_stats: Arc::new(HashMap::new()),
            }],
            bind_context,
            budget,
            None,
            None,
        )
    }

    /// Seed one root group with independently derived equivalent plans. The
    /// mandatory baseline is always the first entry; every optional entry
    /// names its derivation rule, and only Memo winner selection chooses one.
    pub fn build_alternatives(
        alternatives: Vec<LogicalAlternative>,
        bind_context: BindContext,
        budget: SearchBudget,
    ) -> Result<OptimizationInput> {
        Self::build_inner(alternatives, bind_context, budget, None, None)
    }

    pub(crate) fn build_with_search(
        alternatives: Vec<LogicalAlternative>,
        binder: &Binder,
        budget: SearchBudget,
        search_context: &crate::context::OptimizationContext,
    ) -> Result<OptimizationInput> {
        Self::build_inner(
            alternatives,
            binder.bind_context.clone(),
            budget,
            Some(search_context),
            Some(binder.clone()),
        )
    }

    fn build_inner(
        alternatives: Vec<LogicalAlternative>,
        bind_context: BindContext,
        budget: SearchBudget,
        search_context: Option<&crate::context::OptimizationContext>,
        planner_binder: Option<Binder>,
    ) -> Result<OptimizationInput> {
        if alternatives.is_empty() {
            return Err(paro_error::internal(
                "Memo builder requires a mandatory baseline plan",
            ));
        }
        if !matches!(alternatives[0].source, AlternativeOrigin::Baseline)
            || alternatives
                .iter()
                .skip(1)
                .any(|alternative| matches!(alternative.source, AlternativeOrigin::Baseline))
        {
            return Err(paro_error::internal(
                "planner alternatives require exactly one leading mandatory baseline",
            ));
        }
        let root_result_guarantee = required_result_guarantee(&alternatives[0].plan);
        let mut memo = Memo::new(budget);
        let mut columns = ColumnCatalog::default();
        let mut scalars = ScalarArena::default();
        let mut binding_ids = BindingCatalog::default();
        let mut payloads = PlannerPayloadArena::default();
        let mut metadata = BTreeMap::new();
        let mut region_facets = Vec::<RegionFacet>::new();
        let mut pending_region_facets =
            BTreeMap::<LogicalPayloadId, PendingPlannerRegionFacets>::new();
        let mut expression_groups =
            BTreeMap::<LogicalExprKey, Vec<(GroupId, super::ids::LogicalExprId)>>::new();
        let rowset_scan_pushdown = search_context
            .map(|context| context.session.limits.rowset_scan_pushdown)
            .unwrap_or(true);
        let scan_access_cost = search_context
            .map(|context| context.cost_model.scan_access)
            .unwrap_or_default();
        let mut has_contextual_shape = false;

        let mut roots: Vec<(AlternativeOrigin, OwnedLogicalPlan, BuildState)> =
            Vec::with_capacity(alternatives.len());
        for alternative in alternatives {
            let source = alternative.source;
            let candidate_stats = alternative.column_stats.clone();
            let candidate_context = search_context
                .map(|context| context.fork_for_candidate(alternative.column_stats.clone()));
            let (root_plan, root_state) = alternative.plan.try_fold_post_order(
                |plan, child_states: Vec<BuildState>| -> Result<(OwnedLogicalPlan, BuildState)> {
                    has_contextual_shape |= is_contextual_operator(&plan.operator);
                    let output_bindings = plan.get_column_bindings();
                    let output_types = plan.types();
                    let output_names = plan.output_names();
                    if output_bindings.len() != output_types.len() {
                        return Err(paro_error::internal(
                            "bound plan output binding/type arity mismatch",
                        ));
                    }
                    let mut output_columns = Vec::with_capacity(output_bindings.len());
                    for (index, (binding, logical_type)) in output_bindings
                        .iter()
                        .copied()
                        .zip(output_types.into_iter())
                        .enumerate()
                    {
                        let type_domain = logical_type_fingerprint(&logical_type);
                        let id = if let Some(id) = binding_ids
                            .get(binding.table_index, binding.column_index, &logical_type)
                            .copied()
                        {
                            let desc = columns.get(id).ok_or_else(|| {
                                paro_error::internal(
                                    "column binding map references missing ColumnId",
                                )
                            })?;
                            debug_assert_eq!(desc.logical_type, logical_type);
                            id
                        } else {
                            let origin = ColumnOrigin::Derived {
                                key: typed_binding_fingerprint(binding, type_domain),
                            };
                            let id = columns.intern(
                                logical_type.clone(),
                                true,
                                origin,
                                ColumnVisibility::Visible,
                                output_names.get(index).cloned(),
                            )?;
                            binding_ids.insert(
                                binding.table_index,
                                binding.column_index,
                                &logical_type,
                                id,
                            )?;
                            id
                        };
                        output_columns.push(id);
                    }
                    let unique_columns: BTreeSet<_> = output_columns.iter().copied().collect();
                    let schema = GroupSchema::new(
                        unique_columns
                            .iter()
                            .map(|id| columns.get(*id).unwrap().clone()),
                    )?;
                    let child_maximum_cardinalities = child_states
                        .iter()
                        .map(|state| {
                            memo.group(state.group)
                                .and_then(|group| group.logical_properties.maximum_cardinality)
                        })
                        .collect::<Vec<_>>();
                    let mut logical_properties =
                        derive_logical_properties(&plan.operator, &child_maximum_cardinalities);
                    attach_group_column_domains(
                        &mut logical_properties,
                        &output_bindings,
                        &output_columns,
                        candidate_stats.as_ref(),
                        &schema,
                    )?;
                    if let LogicalOperator::CTERef(reference) = &plan.operator {
                        logical_properties.cte_references.insert(cte_reference_domain(reference, &output_columns)?);
                    }
                    if let LogicalOperator::MaterializedCTE(cte) = &plan.operator {
                        if let Some(producer) = child_states.first() {
                            memo.register_cte_producer(
                                cte.cte_index,
                                producer.group,
                                cte_producer_columns(cte, producer.group, &memo, &binding_ids)?,
                            )?;
                        }
                    }
                    let output_rows_hard_upper = logical_properties.maximum_cardinality;
                    // Capture binding semantics before Query IR interning
                    // replaces operator expressions with positional arena
                    // references. The Memo key owns the interned scalars;
                    // rule payloads never do.
                    // Clone only the current operator shell. Duplicating `plan`
                    // here used to recopy the complete subtree at every
                    // post-order node, turning Memo construction into O(N²).
                    let (shell, detached_children) = paro_planner::plan::arena::LogicalPlanNode::detach(plan);
                    let semantic_template = semantic_plan::canonical_template(shell.clone());
                    let plan = shell.assemble(detached_children)?;
                    let search_candidate = match candidate_context.as_ref() {
                        Some(search_context)
                            if matches!(
                                &plan.operator,
                                LogicalOperator::TopN(_) | LogicalOperator::Filter(_)
                            ) =>
                        {
                            crate::search::optimizer::SearchOptimizer::new()
                                .physical_candidate_for_root(&plan, search_context)?
                        }
                        _ => None,
                    };
                    // Scalar interning borrows the child layouts; do not copy
                    // every `ColumnId` array while constructing each Memo
                    // node in the initial post-order walk.
                    let child_columns = child_states
                        .iter()
                        .map(|state| state.columns.as_ref())
                        .collect::<Vec<_>>();
                    let scalar_roots = intern_operator_scalars(
                        &plan.operator,
                        &output_columns,
                        &child_columns,
                        &mut binding_ids,
                        &mut columns,
                        &mut scalars,
                    )?;
                    let (operator_fingerprint, operator_encoding) =
                        query_operator_identity(&plan.operator, &scalar_roots, &scalars)?;
                    let key = LogicalExprKey {
                        operator: operator_fingerprint,
                        scalars: scalar_roots,
                        children: child_states
                            .iter()
                            .map(|state| state.group)
                            .collect::<Vec<_>>()
                            .into_boxed_slice(),
                    };
                    // Equal relational keys at different tree occurrences do
                    // not imply equal region paths. Keep the occurrences
                    // separate here; transformations may reuse them later
                    // only after their OptimizationContext is known.
                    let cardinality = derive_group_cardinality(
                        &plan.operator,
                        &key.children,
                        &plan.stats,
                        stable_cardinality_recipe(operator_fingerprint, &operator_encoding),
                    );
                    let group = memo.create_group(schema, logical_properties, cardinality);
                    let (payload, baseline_payload) =
                        payloads.push_logical(PlannerLogicalPayload {
                            scalar_facts: scalar_facts::NativeScalarFacts::derive(
                                &semantic_template.operator,
                                &key.scalars,
                                &scalars,
                                &binding_ids,
                                &columns,
                                || memo.control().checkpoint(),
                            )?.ok_or_else(|| paro_error::internal("initial scalar evidence requires an incumbent phase"))?,
                            semantic_template,
                            operator_encoding: operator_encoding.clone(),
                            column_stats: candidate_stats.clone(),
                        });
                    let logical = memo.insert_logical_with_operator_encoding_and_tag(
                        group,
                        key.clone(),
                        payload,
                        EquivalenceProof::Initial,
                        operator_encoding,
                        operator_tag(plan.operator.op_type()),
                    )?;
                    let search = search_candidate
                        .map(|search_plan| {
                            stage_search_implementation(
                                SearchStagingRequest {
                                    plan: search_plan,
                                    expected_output_bindings: &output_bindings,
                                    expected_output_types: &plan.types(),
                                    output_columns: &output_columns,
                                    materialized_columns: &unique_columns,
                                    binding_ids: &binding_ids,
                                    operator_fingerprint,
                                    output_rows_hard_upper,
                                    column_stats: candidate_stats.as_ref(),
                                    scan_access_cost,
                                },
                                &mut payloads,
                            )
                        })
                        .transpose()?;
                    let implementations = planner_implementation_set(&plan, rowset_scan_pushdown);
                    let region_scope = PlannerRegionScope::new(
                        group,
                        child_states.iter().map(|child| child.region_scope.clone()),
                    );
                    let mut pending = PendingPlannerRegionFacets::default();
                    if let Some(kind) = required_region_kind(&plan.operator) {
                        let (scope, overflow) = region_scope.materialize_bounded(
                            &memo,
                            memo.budget().max_mandatory_region_groups as usize,
                        );
                        if overflow {
                            return Err(paro_error::internal(
                                "required planning-region closure exceeds query complexity ceiling",
                            ));
                        }
                        let facet = planner_region_facet(
                            kind,
                            FacetCriticality::Required,
                            key.stable_fingerprint(),
                            operator_fingerprint,
                            scope,
                        );
                        pending.required = Some(facet.fingerprint);
                        region_facets.push(facet);
                    }
                    if implementations.hash_join_runtime_filter
                        || implementations.hash_join_build_left_runtime_filter
                    {
                        // The forest owns the auxiliary capability at its
                        // logical join group. The selected candidate's
                        // immediate probe/build span is replayed separately by
                        // WinnerVerifier; pre-unioning every mutually
                        // exclusive join-order boundary here would collapse
                        // the decomposition before a winner exists.
                        let scope = std::iter::once(group).collect();
                        let facet = planner_region_facet(
                            RegionFacetKind::RuntimeFilter,
                            FacetCriticality::Optional,
                            key.stable_fingerprint(),
                            operator_fingerprint,
                            scope,
                        );
                        pending.runtime_filter = Some(facet.fingerprint);
                        region_facets.push(facet);
                    }
                    if pending != PendingPlannerRegionFacets::default() {
                        pending_region_facets.insert(payload, pending);
                    }
                    if matches!(plan.operator, LogicalOperator::Join(Join::Comparison(_))) {
                        let (join_type, probe_operator, conditions) = match &plan.operator {
                            LogicalOperator::Join(Join::Comparison(join)) => (
                                join.join_type,
                                join.left.operator.op_type(),
                                Some(&join.conditions),
                            ),
                            _ => unreachable!(),
                        };
                        debug!(
                            target: targets::OPTIMIZER,
                            logical_expression = logical.index(),
                            baseline = ?implementations.baseline,
                            ?join_type,
                            runtime_filter_candidate = implementations.hash_join_runtime_filter,
                            build_left_runtime_filter_candidate = implementations.hash_join_build_left_runtime_filter,
                            probe_operator = ?probe_operator,
                            conditions = ?conditions,
                            "registered physical join implementation set"
                        );
                    }
                    let operator_metadata = PlannerOperatorMetadata {
                        origin_rule: None,
                        selected_proofs: Box::new([]),
                        operator_type: plan.operator.op_type(),
                        operator_fingerprint,
                        provided: ProvidedProperties {
                            ordering: derive_provided_ordering(
                                &plan.operator,
                                &output_columns,
                                child_states.first().map(|state| state.columns.as_ref()),
                                &binding_ids,
                            ),
                            partitioning: ProvidedPartitioning::Singleton,
                            materialization: ProvidedMaterialization {
                                values: unique_columns,
                                locators: BTreeMap::new(),
                            },
                            mutation_safety: ProvidedMutationSafety::NotApplicable,
                            representation: ProvidedRepresentation::Flat,
                            replayability: ProvidedReplayability::OnePass,
                            result_guarantee: provided_result_guarantee(&plan.operator),
                        },
                        local_cost: planner_operator_cost(
                            &plan,
                            child_states.len(),
                            output_rows_hard_upper,
                            &child_maximum_cardinalities,
                            scan_access_cost,
                        )?,
                        implementations,
                        grant_dependency: planner_grant_dependency(&plan.operator),
                        spillable: planner_operator_spillable(&plan.operator),
                        cost_facts: planner_cost_facts(
                            &plan,
                            candidate_stats.as_ref(),
                            &binding_ids,
                            scan_access_cost,
                        )?,
                        output_columns: output_columns.clone().into_boxed_slice(),
                        child_layouts: plan
                            .children()
                            .into_iter()
                            .map(|child| Arc::new(child.output_layout()))
                            .collect::<Vec<_>>()
                            .into_boxed_slice(),
                        child_required: intern_child_requirements(
                            &mut memo,
                            child_states.iter().map(|state| state.columns.as_ref()),
                        )?,
                        child_row_goals: child_row_goals(&plan.operator, child_states.len()),
                        search,
                        input_context: OptimizationContextId::INVALID,
                        child_context: OptimizationContextId::INVALID,
                        required_region_facet: None,
                        runtime_filter_region_facet: None,
                        structural_retained_children: planner_structural_retained_children(
                            &plan.operator,
                        ),
                        baseline_payload,
                    };
                    if metadata.insert(payload, operator_metadata).is_some() {
                        return Err(paro_error::internal(
                            "planner payload metadata was assigned more than once",
                        ));
                    }
                    Ok((
                        plan,
                        BuildState {
                            group,
                            logical,
                            columns: output_columns.into_boxed_slice(),
                            region_scope,
                        },
                    ))
                },
            )?;
            match source {
                AlternativeOrigin::Baseline => {}
                AlternativeOrigin::Specialized { rule } => {
                    let region = memo
                        .logical_expr(root_state.logical)
                        .ok_or_else(|| {
                            paro_error::internal("specialized root expression disappeared")
                        })?
                        .key
                        .stable_fingerprint();
                    memo.add_equivalence_proof(
                        root_state.logical,
                        EquivalenceProof::SpecializedEnumerator { rule, region },
                    )?;
                }
            }
            roots.push((source, root_plan, root_state));
        }

        // Context belongs to an expression path, not to its semantic group.
        // Bind it before merging equivalent roots, while every initial tree
        // occurrence still has an unambiguous required-region membership.
        let required_facets = region_facets
            .iter()
            .filter(|facet| facet.criticality == FacetCriticality::Required)
            .cloned()
            .collect::<Vec<_>>();
        let group_expressions = memo
            .groups()
            .map(|group| (group.id, group.logical_exprs().to_vec()))
            .collect::<Vec<_>>();
        for (group, expressions) in group_expressions {
            let child_facets = required_facets
                .iter()
                .filter(|facet| facet.scope.contains(&group))
                .map(|facet| facet.fingerprint)
                .collect::<BTreeSet<_>>();
            for logical in expressions {
                let payload = memo
                    .logical_expr(logical)
                    .ok_or_else(|| paro_error::internal("initial expression disappeared"))?
                    .payload;
                let own_facet = pending_region_facets
                    .get(&payload)
                    .and_then(|pending| pending.required);
                let mut input_facets = child_facets.clone();
                if let Some(own_facet) = own_facet {
                    input_facets.remove(&own_facet);
                }
                let input_context =
                    memo.intern_optimization_context(OptimizationContext::new(input_facets))?;
                let child_context = memo.intern_optimization_context(OptimizationContext::new(
                    child_facets.iter().copied(),
                ))?;
                let operator = metadata.get_mut(&payload).ok_or_else(|| {
                    paro_error::internal("initial context lost operator metadata")
                })?;
                operator.input_context = input_context;
                operator.child_context = child_context;
            }
        }
        memo.freeze_optimization_contexts()?;

        let (_, root_plan, mut root_state) = roots.remove(0);
        for (_, _, alternative) in roots {
            root_state.group = memo.merge_groups(root_state.group, alternative.group)?;
        }
        root_state.group = memo.canonical_group(root_state.group);

        for facet in &mut region_facets {
            facet.scope = facet
                .scope
                .iter()
                .map(|group| memo.canonical_group(*group))
                .collect();
        }
        let regions = RegionForest::normalize(
            region_facets,
            usize::from(memo.budget().max_composite_region_groups),
            memo.budget().max_mandatory_region_groups as usize,
        )?;
        let dropped_optional: BTreeSet<_> =
            regions.dropped_optional_facets.iter().copied().collect();
        for (payload, pending) in pending_region_facets {
            let operator = metadata.get_mut(&payload).ok_or_else(|| {
                paro_error::internal("planning-region binding lost operator metadata")
            })?;
            operator.required_region_facet = pending.required;
            operator.runtime_filter_region_facet = pending
                .runtime_filter
                .filter(|facet| !dropped_optional.contains(facet));
            if operator.runtime_filter_region_facet.is_none() {
                operator.implementations.hash_join_runtime_filter = false;
                operator.implementations.hash_join_build_left_runtime_filter = false;
            }
        }
        memo.set_regions(regions);

        // The staging reuse index is valid only after contexts are bound and
        // root groups have reached their canonical identities.
        expression_groups.clear();
        let indexed_expressions = memo
            .groups()
            .flat_map(|group| {
                group
                    .logical_exprs()
                    .iter()
                    .copied()
                    .map(move |logical| (group.id, logical))
            })
            .collect::<Vec<_>>();
        for (group, logical) in indexed_expressions {
            let key = memo
                .logical_expr(logical)
                .ok_or_else(|| paro_error::internal("context index lost logical expression"))?
                .key
                .clone();
            expression_groups
                .entry(key)
                .or_default()
                .push((group, logical));
        }

        let root_provided = memo
            .group(root_state.group)
            .and_then(|group| group.logical_exprs().first())
            .and_then(|expr| memo.logical_expr(*expr))
            .and_then(|expr| metadata.get(&expr.payload))
            .map(|metadata| metadata.provided.clone())
            .ok_or_else(|| paro_error::internal("root Memo group has no baseline properties"))?;
        let root_required = memo.intern_required(RequiredProperties {
            ordering: match root_provided.ordering {
                ProvidedOrdering::Unordered => OrderingRequirement::Any,
                ProvidedOrdering::Ordered { keys, scope } => {
                    OrderingRequirement::Ordered(RequiredOrdering { keys, scope })
                }
            },
            partitioning: PartitioningRequirement::Singleton,
            materialization: super::properties::MaterializationRequirement {
                values: root_state.columns.iter().copied().collect(),
                locators: BTreeMap::new(),
            },
            mutation_safety: MutationSafetyRequirement::None,
            representation: RepresentationRequirement::Flat,
            replayability: ReplayabilityRequirement::Any,
            result_guarantee: root_result_guarantee,
        })?;
        let root_context = memo
            .logical_expr(root_state.logical)
            .and_then(|logical| metadata.get(&logical.payload))
            .map(|metadata| metadata.input_context)
            .ok_or_else(|| paro_error::internal("root expression has no optimization context"))?;
        let root_goal = OptimizationGoal {
            required: root_required,
            row_goal: RowGoal::All,
            objective: ObjectiveProfile::Latency,
            grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
            context: root_context,
        };
        let presentation = ResultPresentation {
            columns: root_state.columns,
            names: root_plan.output_names().into_boxed_slice(),
        };
        let requires_memo = has_contextual_shape
            || planner_binder.is_some()
            || memo.groups().any(|group| group.logical_exprs().len() > 1);
        let planner_state = Arc::new(RwLock::new(PlannerTransformState {
            staging_arena: paro_planner::plan::arena::LogicalPlanArena::default(),
            columns,
            scalars,
            binding_ids,
            payloads,
            metadata,
            expression_groups,
            expression_group_insertions: Vec::new(),
            metadata_runtime_filter_changes: Vec::new(),
            enumerated_join_regions: BTreeSet::new(),
            join_region_cache: std::sync::Mutex::new(JoinRegionCache::default()),
            witness_cache: std::sync::Mutex::new(PatternWitnessCache::default()),
            boundary_cache: std::sync::Mutex::new(boundary::BoundaryFactCache::default()),
            join_region_insertions: Vec::new(),
            cte_restrictions: Vec::new(),
            cte_partition_labels: Default::default(),
            cte_bindings: Vec::new(),
            cte_binding_index: BTreeMap::new(),
            settlement_cache: Default::default(),
            binder: planner_binder,
            bind_context: bind_context.clone(),
            session: search_context.map(|context| context.session.clone()),
            cost_model: search_context
                .map(|context| context.cost_model.clone())
                .unwrap_or_default(),
            verify_enabled: search_context.is_some_and(|context| context.verify_enabled),
            rowset_scan_pushdown,
            scan_access_cost,
        }));
        Ok(OptimizationInput {
            memo,
            root: root_state.group,
            root_goal,
            mode: if requires_memo {
                SearchMode::Memo
            } else {
                SearchMode::Direct
            },
            presentation,
            planner_state,
            bind_context,
            calibration: Arc::new(MachineCalibrationBundle::default()),
            force_spill: false,
            certified_group_pruning: None,
            strong_incumbent_plans: Vec::new(),
            prepriced_strong_incumbents: Vec::new(),
            export_strong_incumbent: false,
        })
    }
}

fn intern_child_requirements<'a>(
    memo: &mut Memo,
    children: impl IntoIterator<Item = &'a [ColumnId]>,
) -> Result<Box<[PropertySetId]>> {
    children
        .into_iter()
        .map(|columns| {
            memo.intern_required(RequiredProperties {
                materialization: super::properties::MaterializationRequirement {
                    values: columns.iter().copied().collect(),
                    locators: BTreeMap::new(),
                },
                representation: RepresentationRequirement::Flat,
                ..RequiredProperties::default()
            })
        })
        .collect::<Result<Vec<_>>>()
        .map(Vec::into_boxed_slice)
}

fn child_row_goals<Child>(
    operator: &LogicalOperator<Child>,
    child_count: usize,
) -> Box<[PlannerChildRowGoal]> {
    let policy = match operator {
        LogicalOperator::Projection(_)
        | LogicalOperator::RowFetch(_)
        | LogicalOperator::ExternalProject(_) => PlannerChildRowGoal::Parent,
        _ => PlannerChildRowGoal::All,
    };
    vec![policy; child_count].into_boxed_slice()
}

#[cfg(test)]
mod tests;
