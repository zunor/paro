// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Construction of optimizer Query IR and Memo groups from bound plans.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, RwLock};

use crate::physical::{ResourceGrantClass, SpillPolicy};
use paro_catalog::entry::CatalogEntry;
use paro_common::error::{self as paro_error, Result};
use paro_common::logging::targets;
use paro_planner::binder::context::BindContext;
use paro_planner::binder::deep_copy::duplicate_plan_preserving_indices;
use paro_planner::binder::ir::{CTEMaterialize, OrderByNode};
use paro_planner::binder::Binder;
use paro_planner::expression::{Expression, ReferenceExpression};
use paro_planner::operator::join::{AntiJoinMode, Join, JoinComparisonType, JoinType};
use paro_planner::operator::{ColumnBinding, LogicalOperator, LogicalOperatorType};
use paro_planner::plan::{LogicalPlan, NodeStats};
use paro_storage::statistics::ColumnStatistics;
use tracing::debug;

use crate::aggregate::{
    dimension_deferral, input_materialization, join_preaggregation, join_subsumption, late_payload,
    non_null_inputs, post_reduction, singleton_groups,
};
use crate::column::lifetime::ColumnLifetimeAnalyzer;
use crate::column::remove_unused::RemoveUnusedColumns;
use crate::cte::filter_pusher::CTEFilterPusher;
use crate::cte::inlining::CTEInlining;
use crate::filter::pullup::FilterPullup;
use crate::filter::pushdown::FilterPushdown;
use crate::filter::reorder::ReorderFilter;
use crate::join::elimination::JoinElimination;
use crate::join::mixed_predicates::JoinPredicateNormalizer;
use crate::limit::pushdown::LimitPushdown;
use crate::statistics::gathering::StatisticsGathering;
use crate::statistics::propagator::StatisticsPropagator;
use crate::subquery::scalar_aggregate_window;
use crate::verify::verify_logical_plan;

use super::budget::{BudgetDimension, SearchBudget};
use super::calibration::{
    LocalOperatorWork, MachineCalibrationBundle, OP_RUNTIME_FILTER_APPLY_ROW,
    OP_RUNTIME_FILTER_BUILD_ROW, OP_TUPLE_BYTE_BLOCK,
};
use super::column::{ColumnCatalog, ColumnOrigin, ColumnVisibility, GroupSchema};
use super::cost::ResourceDimension;
use super::cost::{CompactRange, ScoreSummary, SearchCost};
use super::engine::{CascadesEngine, SearchMode};
use super::ids::{
    AdmissibleGrantSetId, BaseRelationId, ColumnId, Fingerprint, GroupId, ImplementationId,
    LogicalExprId, LogicalPayloadId, ObjectiveProfileId, OpClassId, OptimizationContextId,
    PhysicalPayloadId, PropertySetId, QualityPolicyId, RuleId, ScalarExprId, SnapshotId,
    StableFingerprintBuilder,
};
use super::memo::{
    CardinalityAuthority, CardinalityEnvelope, EquivalenceProof, GrantGoalKey, GroupCardinality,
    LogicalExprKey, LogicalProperties, Memo, OptimizationGoal, PhysicalExprKey, RowGoal,
};
use super::properties::{
    MutationSafetyRequirement, NullOrder, OrderingKey, OrderingRequirement, OrderingScope,
    PartitioningRequirement, ProvidedMaterialization, ProvidedMutationSafety, ProvidedOrdering,
    ProvidedPartitioning, ProvidedProperties, ProvidedReplayability, ProvidedRepresentation,
    ReplayabilityRequirement, RepresentationRequirement, RequiredOrdering, RequiredProperties,
    ResultGuarantee, SortDirection,
};
use super::region::{
    FacetCriticality, RegionArtifactKind, RegionCandidateContract, RegionFacet, RegionFacetKind,
    RegionForest, RegionOwnedArtifact,
};
use super::rules::{
    CostComposition, EquivalentExpression, GrantDependencyDescriptor, ImplementationContext,
    ImplementationRegistry, PhysicalCandidate, PhysicalImplementation, RuleContext, RulePromise,
    TransformContext, TransformationRule, AGGREGATE_DIMENSION_DEFERRAL_RULE,
    AGGREGATE_INPUT_MATERIALIZATION_RULE, AGGREGATE_JOIN_PREAGGREGATION_RULE,
    AGGREGATE_JOIN_SUBSUMPTION_RULE, AGGREGATE_NON_NULL_INPUT_RULE, AGGREGATE_POST_REDUCTION_RULE,
    CTE_FILTER_PUSHDOWN_RULE, CTE_INLINE_RULE, EXPENSIVE_PREDICATE_PLACEMENT_RULE,
    JOIN_ELIMINATION_RULE, LATE_PAYLOAD_FETCH_RULE, LIMIT_PUSHDOWN_RULE, MARK_JOIN_TO_SEMI_RULE,
    SCALAR_AGGREGATE_WINDOW_RULE,
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
const PLANNER_HASH_JOIN_RUNTIME_FILTER: ImplementationId = ImplementationId(6);
const PLANNER_PARTITION_AGGREGATE_WINDOW: ImplementationId = ImplementationId(7);
const PLANNER_SINGLETON_AGGREGATE_PROJECTION: ImplementationId = ImplementationId(8);
const COST_OPTIMIZED_SEARCH_POLICY: QualityPolicyId = QualityPolicyId(1);
pub const SEARCH_REGION_ENUMERATOR_RULE: super::ids::RuleId = super::ids::RuleId(10_002);
pub const GRAPH_REGION_ENUMERATOR_RULE: super::ids::RuleId = super::ids::RuleId(10_003);

#[derive(Debug)]
pub struct LogicalAlternative {
    pub plan: LogicalPlan,
    pub source: AlternativeOrigin,
    /// Immutable estimator input owned by this alternative. Search providers
    /// must never observe statistics left behind by a different candidate.
    pub column_stats: Arc<HashMap<ColumnBinding, Arc<ColumnStatistics>>>,
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
    baseline_child_required: PropertySetId,
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
            self.baseline_child_required,
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
            let (plan, contracts, enforcers) = {
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
            variants.push(OptimizedVariant {
                class: grant_winner.class,
                plan,
                contracts: Arc::new(contracts),
                enforcers: Arc::new(enforcers),
                write_contracts: Arc::new(std::collections::HashMap::new()),
                physical_fingerprint: winner.physical_fingerprint,
                cost: winner.cost,
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
    subtree_groups: BTreeSet<GroupId>,
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
            let candidate_context = search_context.map(|context| {
                context.fork_for_candidate(Arc::unwrap_or_clone(alternative.column_stats))
            });
            let (root_plan, root_state) = alternative.plan.try_fold_post_order(
                |mut plan, child_states: Vec<BuildState>| -> Result<(LogicalPlan, BuildState)> {
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
                        let key = (binding.table_index, binding.column_index, type_domain);
                        let id = if let Some(id) = binding_ids.get(&key).copied() {
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
                                logical_type,
                                true,
                                origin,
                                ColumnVisibility::Visible,
                                output_names.get(index).cloned(),
                            )?;
                            binding_ids.insert(key, id)?;
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
                    let logical_properties =
                        derive_logical_properties(&plan.operator, &child_maximum_cardinalities);
                    let output_rows_hard_upper = logical_properties.maximum_cardinality;
                    // Capture binding semantics before Query IR interning
                    // replaces operator expressions with positional arena
                    // references. The Memo key owns the interned scalars;
                    // rule payloads never do.
                    let semantic_template = semantic_plan::detach_template(
                        duplicate_plan_preserving_indices(&plan, bind_shared.as_ref()),
                    )
                    .map_children(|_| LogicalPlan::synthetic(LogicalOperator::DummyScan));
                    let search_candidate = if let Some(search_context) = &candidate_context {
                        crate::search::optimizer::SearchOptimizer::new()
                            .physical_candidate_for_root(
                                duplicate_plan_preserving_indices(&plan, bind_shared.as_ref()),
                                search_context,
                            )?
                    } else {
                        None
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
                    let operator_fingerprint =
                        query_operator_fingerprint(&plan, &scalar_roots, &scalars)?;
                    let key = LogicalExprKey {
                        operator: operator_fingerprint,
                        scalars: scalar_roots,
                        children: child_states
                            .iter()
                            .map(|state| state.group)
                            .collect::<Vec<_>>()
                            .into_boxed_slice(),
                    };
                    let reusable = expression_groups.get(&key).and_then(|candidates| {
                        candidates.iter().copied().find(|(group, _)| {
                            memo.group(*group).is_some_and(|existing| {
                                existing.schema == schema
                                    && existing
                                        .logical_properties
                                        .same_contract(&logical_properties)
                            })
                        })
                    });
                    if let Some((group, logical)) = reusable {
                        memo.group_mut(group)
                            .expect("reusable group was validated")
                            .logical_properties
                            .merge_equivalent_facts(&logical_properties);
                        let mut subtree_groups = BTreeSet::from([group]);
                        for child in &child_states {
                            subtree_groups.extend(child.subtree_groups.iter().copied());
                        }
                        return Ok((
                            plan,
                            BuildState {
                                group,
                                logical,
                                columns: output_columns.into_boxed_slice(),
                                subtree_groups,
                            },
                        ));
                    }
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
                            column_stats: candidate_stats.clone(),
                        });
                    let logical = memo.insert_logical(
                        group,
                        key.clone(),
                        payload,
                        EquivalenceProof::Initial,
                    )?;
                    let search = if let Some(mut search_plan) = search_candidate {
                        if search_plan.get_column_bindings() != output_bindings
                            || search_plan.types() != plan.types()
                        {
                            return Err(paro_error::internal(
                                "physical search candidate changed its logical output contract",
                            ));
                        }
                        let search_fingerprint =
                            search_payload_fingerprint(operator_fingerprint, &search_plan.operator);
                        let provided = ProvidedProperties {
                            ordering: derive_provided_ordering(
                                &search_plan.operator,
                                &output_columns,
                                None,
                                &binding_ids,
                            ),
                            partitioning: ProvidedPartitioning::Singleton,
                            materialization: ProvidedMaterialization {
                                values: unique_columns.clone(),
                                locators: BTreeMap::new(),
                            },
                            mutation_safety: ProvidedMutationSafety::NotApplicable,
                            representation: ProvidedRepresentation::Flat,
                            replayability: ProvidedReplayability::OnePass,
                            result_guarantee: provided_result_guarantee(&search_plan.operator),
                        };
                        let local_cost = planner_operator_cost(
                            &search_plan,
                            0,
                            output_rows_hard_upper,
                            &child_maximum_cardinalities,
                            scan_access_cost,
                        )?;
                        let cost_facts = planner_cost_facts(&search_plan, scan_access_cost)?;
                        search_plan.stats = NodeStats::default();
                        let payload = payloads.push_physical(PlannerPhysicalTemplate::Executable(
                            Box::new(search_plan),
                        ));
                        Some(PlannerSearchImplementationMetadata {
                            payload,
                            payload_fingerprint: search_fingerprint,
                            provided,
                            local_cost,
                            cost_facts,
                        })
                    } else {
                        None
                    };
                    let implementations = planner_implementation_set(&plan, rowset_scan_pushdown);
                    let mut subtree_groups = BTreeSet::from([group]);
                    for child in &child_states {
                        subtree_groups.extend(child.subtree_groups.iter().copied());
                    }
                    let mut pending = PendingPlannerRegionFacets::default();
                    if let Some(kind) = required_region_kind(&plan.operator) {
                        let facet = planner_region_facet(
                            kind,
                            FacetCriticality::Required,
                            logical,
                            operator_fingerprint,
                            subtree_groups.clone(),
                        );
                        pending.required = Some(facet.fingerprint);
                        region_facets.push(facet);
                    }
                    if implementations.hash_join_runtime_filter {
                        let facet = planner_region_facet(
                            RegionFacetKind::RuntimeFilter,
                            FacetCriticality::Optional,
                            logical,
                            operator_fingerprint,
                            subtree_groups.clone(),
                        );
                        pending.runtime_filter = Some(facet.fingerprint);
                        region_facets.push(facet);
                    }
                    if pending != PendingPlannerRegionFacets::default() {
                        pending_region_facets.insert(payload, pending);
                    }
                    if matches!(plan.operator, LogicalOperator::Join(Join::Comparison(_))) {
                        let (probe_operator, conditions) = match &plan.operator {
                            LogicalOperator::Join(Join::Comparison(join)) => {
                                (join.left.operator.op_type(), Some(&join.conditions))
                            }
                            _ => unreachable!(),
                        };
                        debug!(
                            target: targets::OPTIMIZER,
                            logical_expression = logical.index(),
                            baseline = ?implementations.baseline,
                            runtime_filter_candidate = implementations.hash_join_runtime_filter,
                            probe_operator = ?probe_operator,
                            conditions = ?conditions,
                            "registered physical join implementation set"
                        );
                    }
                    let operator_metadata = PlannerOperatorMetadata {
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
                        cost_facts: planner_cost_facts(&plan, scan_access_cost)?,
                        output_columns: output_columns.clone().into_boxed_slice(),
                        search,
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
                    expression_groups
                        .entry(key)
                        .or_default()
                        .push((group, logical));
                    Ok((
                        plan,
                        BuildState {
                            group,
                            logical,
                            columns: output_columns.into_boxed_slice(),
                            subtree_groups,
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
            }
        }
        memo.set_regions(regions);

        let default_required = memo.intern_required(RequiredProperties::default())?;
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
        let root_goal = OptimizationGoal {
            required: root_required,
            row_goal: RowGoal::All,
            objective: ObjectiveProfileId(0),
            grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
            context: OptimizationContextId(0),
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
            baseline_child_required: default_required,
            bind_context,
            calibration: Arc::new(MachineCalibrationBundle::default()),
            force_spill: false,
        })
    }
}

type WinnerContractMap =
    std::collections::HashMap<paro_planner::plan::PlanNodeId, WinnerPhysicalContract>;
type WinnerEnforcerMap =
    std::collections::HashMap<paro_planner::plan::PlanNodeId, Box<[ExtractedEnforcerContract]>>;
type ExtractedWinnerTree = (LogicalPlan, WinnerContractMap, WinnerEnforcerMap);

#[cfg(test)]
mod tests;
