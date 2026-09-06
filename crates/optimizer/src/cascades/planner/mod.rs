// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Construction of optimizer Query IR and Memo groups from bound plans.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, RwLock};

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
use paro_planner::plan::{CardinalityEstimate, LogicalPlan, NodeStats};
use paro_storage::statistics::ColumnStatistics;
use tracing::debug;

use crate::aggregate::{
    dimension_deferral, dimension_sharing, input_materialization, join_preaggregation,
    join_subsumption, late_payload, non_null_inputs, post_reduction, singleton_groups,
};
use crate::column::lifetime::ColumnLifetimeAnalyzer;
use crate::column::remove_unused::RemoveUnusedColumns;
use crate::context::SharedColumnStatistics;
use crate::cte::inlining::CTEInlining;
use crate::cte::{demand_pushdown::CTEDemandPusher, filter_pusher::CTEFilterPusher};
use crate::expression::normalize_scalar_expressions;
use crate::filter::pushdown::FilterPushdown;
use crate::filter::reorder::ReorderFilter;
use crate::join::elimination::JoinElimination;
use crate::join::mixed_predicates::JoinPredicateNormalizer;
use crate::limit::pushdown::LimitPushdown;
use crate::limit::topn::TopNOptimizer;
use crate::statistics::gathering::StatisticsGathering;
use crate::statistics::propagator::StatisticsPropagator;
use crate::subquery::empty_result::EmptyResultPullup;
use crate::subquery::scalar_aggregate_window;
use crate::verify::verify_logical_plan;

use super::budget::{BudgetDimension, SearchBudget};
use super::calibration::{
    LocalOperatorWork, MachineCalibrationBundle, ParallelWorkProfile, OP_HASH_KEY_BYTE_BLOCK,
    OP_RUNTIME_FILTER_APPLY_ROW, OP_RUNTIME_FILTER_BUILD_ROW, OP_TUPLE_BYTE_BLOCK,
};
use super::column::{ColumnCatalog, ColumnOrigin, ColumnVisibility, GroupSchema};
use super::cost::ResourceDimension;
use super::cost::{CompactRange, ScoreSummary, SearchCost};
use super::engine::{CascadesEngine, SearchMode};
use super::ids::{
    AdmissibleGrantSetId, BaseRelationId, ColumnId, Fingerprint, GroupId, ImplementationId,
    LogicalExprId, LogicalPayloadId, OpClassId, OptimizationContextId, PhysicalPayloadId,
    PropertySetId, QualityPolicyId, RuleId, ScalarExprId, SnapshotId, StableFingerprintBuilder,
};
use super::memo::{
    CardinalityEnvelope, CardinalityRecipeKind, ChildWinnerRef, CteReferenceDomain,
    EquivalenceProof, GrantGoalKey, GroupCardinality, GroupColumnDomain, LogicalExprKey,
    LogicalProperties, Memo, OptimizationContext, OptimizationGoal, PhysicalExprKey, RowGoal,
};
use super::properties::{
    MutationSafetyRequirement, NullOrder, OrderingKey, OrderingRequirement, OrderingScope,
    PartitioningRequirement, ProvidedMaterialization, ProvidedMutationSafety, ProvidedOrdering,
    ProvidedPartitioning, ProvidedProperties, ProvidedReplayability, ProvidedRepresentation,
    ReplayabilityRequirement, RepresentationRequirement, RequiredOrdering, RequiredProperties,
    ResultGuarantee, SortDirection,
};
use super::region::{
    FacetCriticality, RegionArtifactDependencyContract, RegionArtifactKind, RegionBoundaryEndpoint,
    RegionCandidateContract, RegionDependencyKind, RegionFacet, RegionFacetKind, RegionForest,
    RegionOwnedArtifact,
};
use super::rules::{
    CostComposition, EquivalentExpression, GrantDependencyDescriptor, ImplementationContext,
    ImplementationRegistry, PatternBinding, PatternBindingSet, PatternEnumerationCompletion,
    PatternOperand, PatternRead, PhysicalCandidate, PhysicalImplementation, RuleContext,
    RulePromise, SidewaysFilterSource, TaskSupplyContract, TransformContext,
    TransformationBudgetClass, TransformationRule, WorkSourceId, AGGREGATE_DIMENSION_DEFERRAL_RULE,
    AGGREGATE_DIMENSION_SHARING_RULE, AGGREGATE_INPUT_MATERIALIZATION_RULE,
    AGGREGATE_JOIN_PREAGGREGATION_RULE, AGGREGATE_JOIN_SUBSUMPTION_RULE,
    AGGREGATE_NON_NULL_INPUT_RULE, AGGREGATE_POST_REDUCTION_RULE, CTE_DEMAND_PUSHDOWN_RULE,
    CTE_FILTER_PUSHDOWN_RULE, CTE_INLINE_RULE, CTE_PARTITIONED_MATERIALIZATION_RULE,
    EXPENSIVE_PREDICATE_PLACEMENT_RULE, JOIN_ELIMINATION_RULE, JOIN_REGION_ENUMERATION_RULE,
    LATE_PAYLOAD_FETCH_RULE, LIMIT_PUSHDOWN_RULE, MARK_JOIN_TO_SEMI_RULE,
    SCALAR_AGGREGATE_WINDOW_RULE, TOP_N_INTRODUCTION_RULE,
};
use super::scalar::ScalarArena;
use super::scalar_lowering::{
    encode_routine_identity, intern_operator_scalars, logical_type_fingerprint, BindingCatalog,
};
use crate::physical::{
    ExtractedEnforcerContract, ExtractedEnforcerContracts, ExtractedPhysicalEnforcer,
    PhysicalImplementationFlavor, WinnerPhysicalContract, WinnerPhysicalContracts,
};

mod contracts;
mod costing;
mod extraction;
mod identity;
mod implementation;
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

struct SearchStagingRequest<'a> {
    plan: LogicalPlan,
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
    pub plan: LogicalPlan,
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
        let grant_optimization = engine.optimize_for_grants(
            self.root,
            self.root_goal,
            AdmissibleGrantSetId(0),
            grant_classes.keys().copied(),
            self.mode,
        )?;
        let mut variants = Vec::with_capacity(grant_optimization.winners.len());
        for grant_winner in grant_optimization.winners {
            let winner = &grant_winner.winner;
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
                extract_planner_tree(
                    engine.memo(),
                    &planner_state,
                    &self.bind_context,
                    self.root,
                    grant_winner.goal,
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
        let rule_insertions = engine.effective_rule_insertions().clone();
        let rule_attempts = engine.rule_attempts().clone();
        let search_summary = SearchSummary {
            groups: u64::try_from(engine.memo().canonical_group_count()).unwrap_or(u64::MAX),
            logical_expressions: u64::try_from(engine.memo().logical_expr_count())
                .unwrap_or(u64::MAX),
            physical_expressions: u64::try_from(engine.memo().physical_expr_count())
                .unwrap_or(u64::MAX),
            exhaustion_events: engine.memo().exhaustion_counts(),
        };
        Ok(OptimizationOutput {
            variants: variants.into_boxed_slice(),
            rule_attempts,
            rule_insertions,
            search_summary,
        })
    }
}

#[derive(Debug)]
pub struct OptimizationOutput {
    pub variants: Box<[OptimizedVariant]>,
    /// Rule applications that passed structural matching and budget admission.
    /// Comparing this with `rule_insertions` measures pre-match precision.
    pub rule_attempts: BTreeMap<RuleId, u64>,
    /// Logical expressions actually inserted into an equivalence group by
    /// each transformation. Matching, scheduling, and duplicate replay do not
    /// count as an effect.
    pub rule_insertions: BTreeMap<RuleId, u64>,
    pub search_summary: SearchSummary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchSummary {
    pub groups: u64,
    pub logical_expressions: u64,
    pub physical_expressions: u64,
    pub exhaustion_events: BTreeMap<BudgetDimension, u64>,
}

impl SearchSummary {
    /// Whether optional search reached every configured frontier. Mandatory
    /// normalization and baseline implementations remain valid when false,
    /// but the selected winner is explicitly a budget-limited result.
    pub fn is_complete(&self) -> bool {
        self.exhaustion_events.is_empty()
    }
}

#[derive(Debug)]
pub struct OptimizedVariant {
    pub class: super::ids::ResourceGrantClassId,
    pub plan: LogicalPlan,
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
    estimated_cardinality: Option<CardinalityEstimate>,
) {
    for (&binding, &column) in output_bindings.iter().zip(output_columns) {
        let statistics = column_stats.get(&binding);
        let expected = statistics
            .map(|statistics| statistics.get_distinct_count() as u64)
            .filter(|distinct| *distinct > 0)
            .map(|distinct| {
                estimated_cardinality
                    .map(|rows| rows.expected)
                    .map_or(distinct, |rows| distinct.min(rows))
            });
        let guaranteed_upper = statistics
            .and_then(|statistics| statistics.guaranteed_distinct_upper())
            .into_iter()
            .chain(properties.maximum_cardinality)
            .min();
        let Some(domain) = GroupColumnDomain::new(expected, guaranteed_upper) else {
            continue;
        };
        properties
            .column_domains
            .entry(column)
            .and_modify(|current| *current = current.canonical_with(domain))
            .or_insert(domain);
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PendingPlannerRegionFacets {
    required: Option<Fingerprint>,
    runtime_filter: Option<Fingerprint>,
}

pub struct MemoBuilder;

impl MemoBuilder {
    pub fn build(
        plan: LogicalPlan,
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
        let bind_shared = bind_context.shared().clone();
        let rowset_scan_pushdown = search_context
            .map(|context| context.session.limits.rowset_scan_pushdown)
            .unwrap_or(true);
        let scan_access_cost = search_context
            .map(|context| context.cost_model.scan_access)
            .unwrap_or_default();
        let mut has_contextual_shape = false;

        let mut roots: Vec<(AlternativeOrigin, LogicalPlan, BuildState)> =
            Vec::with_capacity(alternatives.len());
        for alternative in alternatives {
            let source = alternative.source;
            let candidate_stats = alternative.column_stats.clone();
            let candidate_context = search_context
                .map(|context| context.fork_for_candidate(alternative.column_stats.clone()));
            let (root_plan, root_state) = alternative.plan.try_fold_post_order(
                |plan, child_states: Vec<BuildState>| -> Result<(LogicalPlan, BuildState)> {
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
                        plan.stats.estimated_cardinality,
                    );
                    if let LogicalOperator::CTERef(reference) = &plan.operator {
                        logical_properties.cte_references.insert(CteReferenceDomain {
                            cte_index: reference.cte_index,
                            columns: output_columns.clone().into_boxed_slice(),
                        });
                    }
                    if let LogicalOperator::MaterializedCTE(cte) = &plan.operator {
                        if let Some(producer) = child_states.first() {
                            memo.register_cte_producer(
                                cte.cte_index,
                                producer.group,
                                producer.columns.clone(),
                            );
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
                    let mut detached_children = Vec::with_capacity(child_states.len());
                    let shell = plan.try_map_children(|child| {
                        detached_children.push(child);
                        Ok::<_, paro_common::error::ParoError>(LogicalPlan::synthetic(
                            LogicalOperator::DummyScan,
                        ))
                    })?;
                    let semantic_template = semantic_plan::detach_template(
                        duplicate_plan_preserving_indices(&shell, bind_shared.as_ref()),
                    );
                    let mut detached_children = detached_children.into_iter();
                    let mut plan = shell.try_map_children(|_| {
                        detached_children
                            .next()
                            .ok_or_else(|| paro_error::internal("Memo shell lost a detached child"))
                    })?;
                    if detached_children.next().is_some() {
                        return Err(paro_error::internal(
                            "Memo shell retained excess detached children",
                        ));
                    }
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
                    let scalar_roots = intern_operator_scalars(
                        &mut plan.operator,
                        &output_columns,
                        &child_states
                            .iter()
                            .map(|state| state.columns.clone())
                            .collect::<Vec<_>>(),
                        &mut binding_ids,
                        &mut columns,
                        &mut scalars,
                    )?;
                    let (operator_fingerprint, operator_encoding) =
                        query_operator_identity(&plan, &scalar_roots, &scalars)?;
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
                        key.stable_fingerprint(),
                    );
                    let group = memo.create_group(schema, logical_properties, cardinality);
                    let (payload, baseline_payload) =
                        payloads.push_logical(PlannerLogicalPayload {
                            semantic_template,
                            operator_encoding: operator_encoding.clone(),
                            column_stats: candidate_stats.clone(),
                        });
                    let logical = memo.insert_logical_with_operator_encoding(
                        group,
                        key.clone(),
                        payload,
                        EquivalenceProof::Initial,
                        operator_encoding,
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
                            .map(|child| PlannerBindingLayout {
                                bindings: child.get_column_bindings().into_boxed_slice(),
                                types: child.types().into_boxed_slice(),
                            })
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
            columns,
            scalars,
            binding_ids,
            payloads,
            metadata,
            expression_groups,
            expression_group_insertions: Vec::new(),
            metadata_runtime_filter_changes: Vec::new(),
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

fn child_row_goals(operator: &LogicalOperator, child_count: usize) -> Box<[PlannerChildRowGoal]> {
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
