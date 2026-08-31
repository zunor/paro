// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Construction of optimizer Query IR and Memo groups from bound plans.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::physical::{ResourceGrantClass, SpillPolicy};
use paro_catalog::entry::CatalogEntry;
use paro_common::error::{self as paro_error, Result};
use paro_common::logging::targets;
use paro_planner::binder::context::BindContext;
use paro_planner::binder::deep_copy::duplicate_plan_preserving_indices;
use paro_planner::binder::ir::OrderByNode;
use paro_planner::expression::{Expression, ReferenceExpression};
use paro_planner::operator::join::{AntiJoinMode, Join, JoinComparisonType, JoinType};
use paro_planner::operator::{ColumnBinding, LogicalOperator, LogicalOperatorType};
use paro_planner::plan::{LogicalPlan, NodeStats};
use tracing::debug;

use super::budget::SearchBudget;
use super::calibration::{
    LocalOperatorWork, MachineCalibrationBundle, OP_RUNTIME_FILTER_APPLY_ROW,
    OP_RUNTIME_FILTER_BUILD_ROW,
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
    EquivalenceProof, GrantGoalKey, LogicalExprKey, LogicalProperties, Memo, OptimizationGoal,
    PhysicalExprKey, RowGoal,
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
    TransformationRule,
};
use super::scalar::ScalarArena;
use super::scalar_lowering::{
    encode_routine_identity, intern_operator_scalars, logical_type_fingerprint,
};
use crate::physical::{
    ExtractedEnforcerContract, ExtractedEnforcerContracts, ExtractedPhysicalEnforcer,
    PhysicalImplementationFlavor, WinnerPhysicalContract, WinnerPhysicalContracts,
};

const PLANNER_BASELINE_IMPLEMENTATION: ImplementationId = ImplementationId(1);
const PLANNER_PERFECT_HASH_AGGREGATE: ImplementationId = ImplementationId(2);
const PLANNER_SORT_RANGE_JOIN: ImplementationId = ImplementationId(3);
const PLANNER_CLASSIC_IE_JOIN: ImplementationId = ImplementationId(4);
const PLANNER_SEARCH_PROVIDER: ImplementationId = ImplementationId(5);
const PLANNER_HASH_JOIN_RUNTIME_FILTER: ImplementationId = ImplementationId(6);
const PLANNER_PARTITION_AGGREGATE_WINDOW: ImplementationId = ImplementationId(7);
const PLANNER_SINGLETON_AGGREGATE_PROJECTION: ImplementationId = ImplementationId(8);
const COST_OPTIMIZED_SEARCH_POLICY: QualityPolicyId = QualityPolicyId(1);
pub const JOIN_REGION_ENUMERATOR_RULE: super::ids::RuleId = super::ids::RuleId(10_001);
pub const SEARCH_REGION_ENUMERATOR_RULE: super::ids::RuleId = super::ids::RuleId(10_002);
pub const GRAPH_REGION_ENUMERATOR_RULE: super::ids::RuleId = super::ids::RuleId(10_003);

#[derive(Debug)]
pub struct LogicalAlternative {
    pub plan: LogicalPlan,
    pub source: AlternativeOrigin,
}

#[derive(Debug, Clone, Copy)]
pub enum AlternativeOrigin {
    Baseline,
    Transformation { rule: super::ids::RuleId },
    Specialized { rule: super::ids::RuleId },
}

#[derive(Debug, Clone)]
struct PreparedEquivalent {
    key: LogicalExprKey,
    payload: LogicalPayloadId,
}

#[derive(Debug)]
struct PreparedTransformationRule {
    id: RuleId,
    outputs: BTreeMap<LogicalExprId, Box<[PreparedEquivalent]>>,
}

impl TransformationRule for PreparedTransformationRule {
    fn id(&self) -> RuleId {
        self.id
    }

    fn promise(&self, _expr: &super::memo::LogicalExpr, _ctx: &RuleContext<'_>) -> RulePromise {
        RulePromise::HIGH
    }

    fn matches(&self, expr: &super::memo::LogicalExpr, _ctx: &RuleContext<'_>) -> bool {
        self.outputs.contains_key(&expr.id)
    }

    fn apply(
        &self,
        expr: LogicalExprId,
        ctx: &RuleContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        let source = ctx
            .memo
            .logical_expr(expr)
            .ok_or_else(|| paro_error::internal("prepared rule lost its source expression"))?;
        let premise = source.key.stable_fingerprint();
        Ok(self
            .outputs
            .get(&expr)
            .into_iter()
            .flatten()
            .map(|output| EquivalentExpression {
                target_group: ctx.group,
                key: output.key.clone(),
                payload: output.payload,
                proof: EquivalenceProof::Transformation {
                    rule: self.id,
                    source: expr,
                    premise,
                },
            })
            .collect::<Vec<_>>()
            .into_boxed_slice())
    }
}

#[derive(Debug, Clone)]
pub struct ResultPresentation {
    pub columns: Box<[ColumnId]>,
    pub names: Box<[String]>,
}

#[derive(Debug)]
pub struct PlannerLogicalPayload {
    pub skeleton: LogicalPlan,
    pub output_estimate: Option<paro_planner::plan::CardinalityEstimate>,
}

#[derive(Debug, Default)]
pub struct PlannerPayloadArena {
    logical: Vec<PlannerLogicalPayload>,
}

impl PlannerPayloadArena {
    fn push(&mut self, payload: PlannerLogicalPayload) -> LogicalPayloadId {
        let id = LogicalPayloadId::new(self.logical.len());
        self.logical.push(payload);
        id
    }

    fn get_physical(&self, id: PhysicalPayloadId) -> Option<&PlannerLogicalPayload> {
        self.logical.get(id.index())
    }
}

#[derive(Debug, Clone)]
struct PlannerOperatorMetadata {
    operator_type: LogicalOperatorType,
    operator_fingerprint: Fingerprint,
    provided: ProvidedProperties,
    local_cost: SearchCost,
    implementations: PlannerImplementationSet,
    grant_dependency: GrantDependencyDescriptor,
    spillable: bool,
    cost_facts: PlannerCostFacts,
    output_columns: Box<[ColumnId]>,
    search: Option<PlannerSearchImplementationMetadata>,
    required_region_facet: Option<Fingerprint>,
    runtime_filter_region_facet: Option<Fingerprint>,
    structural_retained_children: u64,
}

#[derive(Debug, Clone)]
struct PlannerSearchImplementationMetadata {
    payload: PhysicalPayloadId,
    payload_fingerprint: Fingerprint,
    provided: ProvidedProperties,
    local_cost: SearchCost,
    cost_facts: PlannerCostFacts,
}

#[derive(Debug, Clone)]
struct PlannerCostFacts {
    output_rows: CompactRange,
    child_rows: Box<[CompactRange]>,
    output_row_width: u64,
    perfect_hash_slots: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct PlannerImplementationSet {
    baseline: PhysicalImplementationFlavor,
    perfect_hash_aggregate: bool,
    sort_range_join: bool,
    classic_ie_join: bool,
    hash_join_runtime_filter: bool,
    partition_aggregate_window: bool,
    singleton_aggregate_projection: bool,
}

impl PlannerImplementationSet {
    const STRUCTURAL: Self = Self {
        baseline: PhysicalImplementationFlavor::Structural,
        perfect_hash_aggregate: false,
        sort_range_join: false,
        classic_ie_join: false,
        hash_join_runtime_filter: false,
        partition_aggregate_window: false,
        singleton_aggregate_projection: false,
    };

    fn supports(self, flavor: PhysicalImplementationFlavor) -> bool {
        match flavor {
            PhysicalImplementationFlavor::PerfectHashAggregate => self.perfect_hash_aggregate,
            PhysicalImplementationFlavor::SortRangeJoin => self.sort_range_join,
            PhysicalImplementationFlavor::ClassicIeJoin => self.classic_ie_join,
            PhysicalImplementationFlavor::HashJoinRuntimeFilter => self.hash_join_runtime_filter,
            PhysicalImplementationFlavor::PartitionAggregateWindow => {
                self.partition_aggregate_window
            }
            PhysicalImplementationFlavor::SingletonAggregateProjection => {
                self.singleton_aggregate_projection
            }
            _ => false,
        }
    }
}

#[derive(Debug)]
pub struct OptimizationInput {
    pub memo: Memo,
    pub columns: ColumnCatalog,
    pub scalars: ScalarArena,
    pub root: GroupId,
    pub root_goal: OptimizationGoal,
    pub mode: SearchMode,
    pub presentation: ResultPresentation,
    payloads: PlannerPayloadArena,
    metadata: Arc<BTreeMap<LogicalPayloadId, PlannerOperatorMetadata>>,
    transformations: Vec<PreparedTransformationRule>,
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
        for transformation in self.transformations {
            registry.register_transformation(transformation)?;
        }
        registry.register_implementation(PlannerBaselineImplementation {
            metadata: self.metadata.clone(),
            child_required: self.baseline_child_required,
            grant_classes: grant_classes.clone(),
            calibration: self.calibration.clone(),
            force_spill: self.force_spill,
        })?;
        for (id, flavor) in [
            (
                PLANNER_PERFECT_HASH_AGGREGATE,
                PhysicalImplementationFlavor::PerfectHashAggregate,
            ),
            (
                PLANNER_SORT_RANGE_JOIN,
                PhysicalImplementationFlavor::SortRangeJoin,
            ),
            (
                PLANNER_CLASSIC_IE_JOIN,
                PhysicalImplementationFlavor::ClassicIeJoin,
            ),
            (
                PLANNER_HASH_JOIN_RUNTIME_FILTER,
                PhysicalImplementationFlavor::HashJoinRuntimeFilter,
            ),
            (
                PLANNER_PARTITION_AGGREGATE_WINDOW,
                PhysicalImplementationFlavor::PartitionAggregateWindow,
            ),
            (
                PLANNER_SINGLETON_AGGREGATE_PROJECTION,
                PhysicalImplementationFlavor::SingletonAggregateProjection,
            ),
        ] {
            registry.register_implementation(AlternativeImplementation {
                id,
                flavor,
                metadata: self.metadata.clone(),
                child_required: self.baseline_child_required,
                grant_classes: grant_classes.clone(),
                calibration: self.calibration.clone(),
                force_spill: self.force_spill,
            })?;
        }
        registry.register_implementation(PlannerSearchImplementation {
            metadata: self.metadata.clone(),
        })?;
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
            let (plan, contracts, enforcers) = extract_planner_tree(
                engine.memo(),
                &self.payloads,
                &self.metadata,
                &self.bind_context,
                self.root,
                grant_winner.goal,
                mode,
            )?;
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
        let mut rule_firings = BTreeMap::<RuleId, u64>::new();
        let mut counted_expressions = BTreeSet::new();
        for group in engine.memo().groups() {
            for expression in group.logical_exprs() {
                if !counted_expressions.insert(*expression) {
                    continue;
                }
                let expression = engine.memo().logical_expr(*expression).ok_or_else(|| {
                    paro_error::internal("Memo group references a missing logical expression")
                })?;
                for rule in &expression.applied_rules {
                    *rule_firings.entry(*rule).or_default() += 1;
                }
            }
        }
        Ok(OptimizationOutput {
            variants: variants.into_boxed_slice(),
            rule_firings,
        })
    }
}

#[derive(Debug)]
pub struct OptimizationOutput {
    pub variants: Box<[OptimizedVariant]>,
    pub rule_firings: BTreeMap<RuleId, u64>,
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
            }],
            bind_context,
            budget,
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
        Self::build_inner(alternatives, bind_context, budget, None)
    }

    pub(crate) fn build_with_search(
        alternatives: Vec<LogicalAlternative>,
        bind_context: BindContext,
        budget: SearchBudget,
        search_context: &crate::context::OptimizationContext,
    ) -> Result<OptimizationInput> {
        Self::build_inner(alternatives, bind_context, budget, Some(search_context))
    }

    fn build_inner(
        alternatives: Vec<LogicalAlternative>,
        bind_context: BindContext,
        budget: SearchBudget,
        search_context: Option<&crate::context::OptimizationContext>,
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
        let mut binding_ids = BTreeMap::<(usize, usize, Fingerprint), ColumnId>::new();
        let mut payloads = PlannerPayloadArena::default();
        let mut metadata = BTreeMap::new();
        let mut region_facets = Vec::<RegionFacet>::new();
        let mut pending_region_facets =
            BTreeMap::<LogicalPayloadId, PendingPlannerRegionFacets>::new();
        let mut prepared_transformations =
            BTreeMap::<RuleId, BTreeMap<LogicalExprId, Vec<PreparedEquivalent>>>::new();
        let mut expression_groups =
            BTreeMap::<LogicalExprKey, Vec<(GroupId, super::ids::LogicalExprId)>>::new();
        let bind_shared = bind_context.shared().clone();
        let rowset_scan_pushdown = search_context
            .map(|context| context.session.limits.rowset_scan_pushdown)
            .unwrap_or(true);
        let mut has_contextual_shape = false;

        let mut roots: Vec<(AlternativeOrigin, LogicalPlan, BuildState)> =
            Vec::with_capacity(alternatives.len());
        for alternative in alternatives {
            let source = alternative.source;
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
                            binding_ids.insert(key, id);
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
                                    && existing.logical_properties == logical_properties
                            })
                        })
                    });
                    if let Some((group, logical)) = reusable {
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
                    let search_candidate = if let Some(search_context) = search_context {
                        crate::search::optimizer::SearchOptimizer::new()
                            .physical_candidate_for_root(
                                duplicate_plan_preserving_indices(&plan, bind_shared.as_ref()),
                                search_context,
                            )?
                    } else {
                        None
                    };
                    let group = memo.create_group(schema, logical_properties);
                    let output_estimate = plan.stats.estimated_cardinality;
                    let mut skeleton =
                        duplicate_plan_preserving_indices(&plan, bind_shared.as_ref())
                            .map_children(|_| LogicalPlan::synthetic(LogicalOperator::DummyScan));
                    skeleton.stats = NodeStats::default();
                    let payload = payloads.push(PlannerLogicalPayload {
                        skeleton,
                        output_estimate,
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
                        let local_cost = planner_operator_cost(&search_plan, 0)?;
                        let cost_facts = planner_cost_facts(&search_plan)?;
                        search_plan.stats = NodeStats::default();
                        let payload = PhysicalPayloadId(
                            payloads
                                .push(PlannerLogicalPayload {
                                    skeleton: search_plan,
                                    output_estimate,
                                })
                                .0,
                        );
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
                        local_cost: planner_operator_cost(&plan, child_states.len())?,
                        implementations,
                        grant_dependency: planner_grant_dependency(&plan.operator),
                        spillable: planner_operator_spillable(&plan.operator),
                        cost_facts: planner_cost_facts(&plan)?,
                        output_columns: output_columns.clone().into_boxed_slice(),
                        search,
                        required_region_facet: None,
                        runtime_filter_region_facet: None,
                        structural_retained_children: planner_structural_retained_children(
                            &plan.operator,
                        ),
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
                AlternativeOrigin::Transformation { rule } => {
                    let source = roots
                        .first()
                        .map(|(_, _, state)| state.logical)
                        .ok_or_else(|| {
                            paro_error::internal(
                                "transformation alternative has no baseline source",
                            )
                        })?;
                    let source_group = memo
                        .logical_owner(source)
                        .and_then(|group| memo.group(group))
                        .ok_or_else(|| {
                            paro_error::internal("transformation baseline group disappeared")
                        })?;
                    let output_group = memo.group(root_state.group).ok_or_else(|| {
                        paro_error::internal("prepared transformation group disappeared")
                    })?;
                    if source_group.schema != output_group.schema
                        || source_group.logical_properties != output_group.logical_properties
                    {
                        return Err(paro_error::internal(
                            "transformation changed its Memo group output contract",
                        ));
                    }
                    let output = memo.logical_expr(root_state.logical).ok_or_else(|| {
                        paro_error::internal("prepared transformation expression disappeared")
                    })?;
                    prepared_transformations
                        .entry(rule)
                        .or_default()
                        .entry(source)
                        .or_default()
                        .push(PreparedEquivalent {
                            key: output.key.clone(),
                            payload: output.payload,
                        });
                }
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
        for (origin, _, alternative) in roots {
            if !matches!(origin, AlternativeOrigin::Transformation { .. }) {
                root_state.group = memo.merge_groups(root_state.group, alternative.group)?;
            }
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
        let transformations = prepared_transformations
            .into_iter()
            .map(|(id, outputs)| PreparedTransformationRule {
                id,
                outputs: outputs
                    .into_iter()
                    .map(|(source, outputs)| (source, outputs.into_boxed_slice()))
                    .collect(),
            })
            .collect::<Vec<_>>();
        let requires_memo = has_contextual_shape
            || !transformations.is_empty()
            || memo.groups().any(|group| group.logical_exprs().len() > 1);
        Ok(OptimizationInput {
            memo,
            columns,
            scalars,
            root: root_state.group,
            root_goal,
            mode: if requires_memo {
                SearchMode::Memo
            } else {
                SearchMode::Direct
            },
            presentation,
            payloads,
            metadata: Arc::new(metadata),
            transformations,
            baseline_child_required: default_required,
            bind_context,
            calibration: Arc::new(MachineCalibrationBundle::default()),
            force_spill: false,
        })
    }
}

#[derive(Debug)]
struct PlannerBaselineImplementation {
    metadata: Arc<BTreeMap<LogicalPayloadId, PlannerOperatorMetadata>>,
    child_required: PropertySetId,
    grant_classes: Arc<BTreeMap<super::ids::ResourceGrantClassId, ResourceGrantClass>>,
    calibration: Arc<MachineCalibrationBundle>,
    force_spill: bool,
}

impl PhysicalImplementation for PlannerBaselineImplementation {
    fn id(&self) -> ImplementationId {
        PLANNER_BASELINE_IMPLEMENTATION
    }

    fn grant_dependency_for(
        &self,
        expr: &super::memo::LogicalExpr,
        _ctx: &ImplementationContext<'_>,
    ) -> GrantDependencyDescriptor {
        self.metadata
            .get(&expr.payload)
            .map(|metadata| metadata.grant_dependency)
            .unwrap_or(GrantDependencyDescriptor::Sensitive)
    }

    fn matches(
        &self,
        expr: &super::memo::LogicalExpr,
        _goal: OptimizationGoal,
        _ctx: &ImplementationContext<'_>,
    ) -> bool {
        self.metadata.contains_key(&expr.payload)
    }

    fn candidates(
        &self,
        expr: super::ids::LogicalExprId,
        goal: OptimizationGoal,
        ctx: &ImplementationContext<'_>,
    ) -> Result<Box<[PhysicalCandidate]>> {
        let logical = ctx
            .memo
            .logical_expr(expr)
            .ok_or_else(|| paro_error::internal("baseline implementation lost logical expr"))?;
        let metadata = self
            .metadata
            .get(&logical.payload)
            .ok_or_else(|| paro_error::internal("baseline implementation lost metadata"))?;
        let children = logical.key.children.clone();
        let child_goals = children
            .iter()
            .copied()
            .map(|child| {
                (
                    child,
                    OptimizationGoal {
                        required: self.child_required,
                        row_goal: RowGoal::All,
                        ..goal
                    },
                )
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let mut fingerprint = StableFingerprintBuilder::default();
        fingerprint.write_u64(self.id().0 as u64);
        fingerprint.write_fingerprint(metadata.operator_fingerprint);
        append_grant_fingerprint(&mut fingerprint, metadata.grant_dependency, goal.grant);
        fingerprint.write_u64(
            (self.force_spill
                && implementation_spillable(metadata, metadata.implementations.baseline))
                as u64,
        );
        let local_cost = implementation_cost(
            metadata,
            metadata.implementations.baseline,
            self.calibration.as_ref(),
        )?;
        let spillable = implementation_spillable(metadata, metadata.implementations.baseline);
        let estimated_peak_memory = local_cost.peak_memory_upper;
        let Some(local_cost) = cost_for_grant(
            local_cost,
            metadata.grant_dependency,
            spillable,
            goal.grant,
            &self.grant_classes,
            self.force_spill,
        )?
        else {
            debug!(
                target: targets::OPTIMIZER,
                logical_expression = expr.index(),
                payload = logical.payload.0,
                operator = ?metadata.operator_type,
                implementation = ?metadata.implementations.baseline,
                estimated_peak_memory,
                spillable,
                grant = ?goal.grant,
                "mandatory baseline is infeasible for the resource grant"
            );
            return Ok(Box::new([]));
        };
        Ok(vec![PhysicalCandidate {
            key: PhysicalExprKey {
                implementation: self.id(),
                logical: expr,
                children,
                payload_fingerprint: metadata.operator_fingerprint,
            },
            payload: PhysicalPayloadId(logical.payload.0),
            provided: metadata.provided.clone(),
            child_goals,
            local_cost,
            cost_composition: planner_cost_composition(metadata, metadata.implementations.baseline),
            spillable,
            enforcer_cost_input: planner_enforcer_cost_input(
                metadata,
                goal.grant,
                &self.grant_classes,
            )?,
            physical_fingerprint: fingerprint.finish(),
            region: planner_region_contract(ctx.memo, metadata.required_region_facet, None)?,
            mandatory: true,
        }]
        .into_boxed_slice())
    }
}

#[derive(Debug)]
struct AlternativeImplementation {
    id: ImplementationId,
    flavor: PhysicalImplementationFlavor,
    metadata: Arc<BTreeMap<LogicalPayloadId, PlannerOperatorMetadata>>,
    child_required: PropertySetId,
    grant_classes: Arc<BTreeMap<super::ids::ResourceGrantClassId, ResourceGrantClass>>,
    calibration: Arc<MachineCalibrationBundle>,
    force_spill: bool,
}

impl PhysicalImplementation for AlternativeImplementation {
    fn id(&self) -> ImplementationId {
        self.id
    }

    fn grant_dependency_for(
        &self,
        expr: &super::memo::LogicalExpr,
        _ctx: &ImplementationContext<'_>,
    ) -> GrantDependencyDescriptor {
        if self
            .metadata
            .get(&expr.payload)
            .is_some_and(|metadata| metadata.implementations.supports(self.flavor))
        {
            GrantDependencyDescriptor::Sensitive
        } else {
            GrantDependencyDescriptor::Invariant
        }
    }

    fn matches(
        &self,
        expr: &super::memo::LogicalExpr,
        _goal: OptimizationGoal,
        _ctx: &ImplementationContext<'_>,
    ) -> bool {
        self.metadata
            .get(&expr.payload)
            .is_some_and(|metadata| metadata.implementations.supports(self.flavor))
    }

    fn candidates(
        &self,
        expr: super::ids::LogicalExprId,
        goal: OptimizationGoal,
        ctx: &ImplementationContext<'_>,
    ) -> Result<Box<[PhysicalCandidate]>> {
        let logical = ctx
            .memo
            .logical_expr(expr)
            .ok_or_else(|| paro_error::internal("physical implementation lost logical expr"))?;
        let metadata = self
            .metadata
            .get(&logical.payload)
            .ok_or_else(|| paro_error::internal("physical implementation lost metadata"))?;
        if !metadata.implementations.supports(self.flavor) {
            return Ok(Box::new([]));
        }
        let children = logical.key.children.clone();
        let child_goals = children
            .iter()
            .copied()
            .map(|child| {
                (
                    child,
                    OptimizationGoal {
                        required: self.child_required,
                        row_goal: RowGoal::All,
                        ..goal
                    },
                )
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let mut fingerprint = StableFingerprintBuilder::default();
        fingerprint.write_u64(self.id.0 as u64);
        fingerprint.write_fingerprint(metadata.operator_fingerprint);
        append_grant_fingerprint(&mut fingerprint, metadata.grant_dependency, goal.grant);
        fingerprint.write_u64(
            (self.force_spill && implementation_spillable(metadata, self.flavor)) as u64,
        );
        let implementation_cost =
            implementation_cost(metadata, self.flavor, self.calibration.as_ref())?;
        let Some(local_cost) = cost_for_grant(
            implementation_cost,
            GrantDependencyDescriptor::Sensitive,
            implementation_spillable(metadata, self.flavor),
            goal.grant,
            &self.grant_classes,
            self.force_spill,
        )?
        else {
            return Ok(Box::new([]));
        };
        Ok(vec![PhysicalCandidate {
            key: PhysicalExprKey {
                implementation: self.id,
                logical: expr,
                children,
                payload_fingerprint: metadata.operator_fingerprint,
            },
            payload: PhysicalPayloadId(logical.payload.0),
            provided: metadata.provided.clone(),
            child_goals,
            local_cost,
            cost_composition: planner_cost_composition(metadata, self.flavor),
            spillable: implementation_spillable(metadata, self.flavor),
            enforcer_cost_input: planner_enforcer_cost_input(
                metadata,
                goal.grant,
                &self.grant_classes,
            )?,
            physical_fingerprint: fingerprint.finish(),
            region: if self.flavor == PhysicalImplementationFlavor::HashJoinRuntimeFilter {
                planner_region_contract(
                    ctx.memo,
                    metadata.runtime_filter_region_facet,
                    Some(RegionArtifactKind::RuntimeFilter),
                )?
            } else {
                planner_region_contract(ctx.memo, metadata.required_region_facet, None)?
            },
            mandatory: false,
        }]
        .into_boxed_slice())
    }
}

#[derive(Debug)]
struct PlannerSearchImplementation {
    metadata: Arc<BTreeMap<LogicalPayloadId, PlannerOperatorMetadata>>,
}

impl PhysicalImplementation for PlannerSearchImplementation {
    fn id(&self) -> ImplementationId {
        PLANNER_SEARCH_PROVIDER
    }

    fn grant_dependency_for(
        &self,
        _expr: &super::memo::LogicalExpr,
        _ctx: &ImplementationContext<'_>,
    ) -> GrantDependencyDescriptor {
        GrantDependencyDescriptor::Invariant
    }

    fn matches(
        &self,
        expr: &super::memo::LogicalExpr,
        _goal: OptimizationGoal,
        _ctx: &ImplementationContext<'_>,
    ) -> bool {
        self.metadata
            .get(&expr.payload)
            .is_some_and(|metadata| metadata.search.is_some())
    }

    fn candidates(
        &self,
        expr: super::ids::LogicalExprId,
        _goal: OptimizationGoal,
        ctx: &ImplementationContext<'_>,
    ) -> Result<Box<[PhysicalCandidate]>> {
        let logical = ctx
            .memo
            .logical_expr(expr)
            .ok_or_else(|| paro_error::internal("search implementation lost logical expr"))?;
        let Some(search) = self
            .metadata
            .get(&logical.payload)
            .and_then(|metadata| metadata.search.as_ref())
        else {
            return Ok(Box::new([]));
        };
        let mut fingerprint = StableFingerprintBuilder::default();
        fingerprint.write_u64(self.id().0 as u64);
        fingerprint.write_fingerprint(search.payload_fingerprint);
        Ok(vec![PhysicalCandidate {
            key: PhysicalExprKey {
                implementation: self.id(),
                logical: expr,
                children: Box::new([]),
                payload_fingerprint: search.payload_fingerprint,
            },
            payload: search.payload,
            provided: search.provided.clone(),
            child_goals: Box::new([]),
            local_cost: search.local_cost,
            cost_composition: CostComposition::Sequential,
            spillable: false,
            enforcer_cost_input: super::engine::EnforcerCostInput::unbounded(
                search.cost_facts.output_rows,
                search.cost_facts.output_row_width,
            ),
            physical_fingerprint: fingerprint.finish(),
            region: planner_region_contract(ctx.memo, None, None)?,
            mandatory: false,
        }]
        .into_boxed_slice())
    }
}

type WinnerContractMap =
    std::collections::HashMap<paro_planner::plan::PlanNodeId, WinnerPhysicalContract>;
type WinnerEnforcerMap =
    std::collections::HashMap<paro_planner::plan::PlanNodeId, Box<[ExtractedEnforcerContract]>>;
type ExtractedWinnerTree = (LogicalPlan, WinnerContractMap, WinnerEnforcerMap);

fn extract_planner_tree(
    memo: &Memo,
    payloads: &PlannerPayloadArena,
    metadata: &BTreeMap<LogicalPayloadId, PlannerOperatorMetadata>,
    bind_context: &BindContext,
    root: GroupId,
    goal: OptimizationGoal,
    mode: SearchMode,
) -> Result<ExtractedWinnerTree> {
    #[derive(Debug)]
    struct BuildTask {
        payload: PhysicalPayloadId,
        child_count: usize,
        output_columns: Box<[ColumnId]>,
        enforcers: Box<[super::enforcer::EnforcerStep]>,
        enforcer_cost_input: super::engine::EnforcerCostInput,
        base_contract: WinnerPhysicalContract,
        final_contract: WinnerPhysicalContract,
    }

    #[derive(Debug)]
    enum Task {
        Visit(GroupId, OptimizationGoal),
        Build(Box<BuildTask>),
    }

    let mut tasks = vec![Task::Visit(root, goal)];
    let mut plans = Vec::new();
    let mut contracts = std::collections::HashMap::new();
    let mut extracted_enforcers = std::collections::HashMap::new();
    while let Some(task) = tasks.pop() {
        match task {
            Task::Visit(group, goal) => {
                let winner = memo
                    .group(group)
                    .and_then(|group| group.winner(goal))
                    .ok_or_else(|| paro_error::internal("extraction found no group winner"))?;
                let physical = memo.physical_expr(winner.expression).ok_or_else(|| {
                    paro_error::internal("winner physical expression disappeared")
                })?;
                let logical = memo.logical_expr(physical.key.logical).ok_or_else(|| {
                    paro_error::internal("winner extraction lost logical expression")
                })?;
                let operator_metadata = metadata.get(&logical.payload).ok_or_else(|| {
                    paro_error::internal("winner extraction lost implementation metadata")
                })?;
                let implementation = selected_implementation_flavor(
                    physical.key.implementation,
                    operator_metadata.implementations,
                )?;
                if matches!(
                    implementation,
                    PhysicalImplementationFlavor::HashJoin
                        | PhysicalImplementationFlavor::HashJoinRuntimeFilter
                        | PhysicalImplementationFlavor::NestedLoopJoin
                        | PhysicalImplementationFlavor::SortRangeJoin
                        | PhysicalImplementationFlavor::ClassicIeJoin
                ) {
                    debug!(
                        target: targets::OPTIMIZER,
                        group = group.index(),
                        logical_expression = physical.key.logical.index(),
                        implementation = ?implementation,
                        winner_score = winner.cost.score.risk_adjusted,
                        "extracted physical join winner"
                    );
                }
                let required = memo.required(goal.required).cloned().ok_or_else(|| {
                    paro_error::internal("winner extraction lost required properties")
                })?;
                let grant = match goal.grant {
                    GrantGoalKey::Invariant(set) => {
                        crate::physical::properties::PhysicalGrantContract::Invariant(set)
                    }
                    GrantGoalKey::Class(class) => {
                        crate::physical::properties::PhysicalGrantContract::Class(class)
                    }
                };
                let origin = if let Some(proof) = &winner.joint_cost_proof {
                    let region = memo.regions().node(proof.region).ok_or_else(|| {
                        paro_error::internal("winner extraction lost its planning region")
                    })?;
                    crate::physical::properties::PlanOrigin::SpecializedRegion(
                        region.stable_fingerprint(),
                    )
                } else {
                    match mode {
                        SearchMode::Direct => crate::physical::properties::PlanOrigin::Direct,
                        SearchMode::Memo => crate::physical::properties::PlanOrigin::Memo,
                    }
                };
                let (region_owner, owned_artifacts) = extracted_region_ownership(memo, winner)?;
                let mut child_costs = Vec::with_capacity(winner.child_goals.len());
                for (child, child_goal) in &winner.child_goals {
                    let child_cost = memo
                        .group(*child)
                        .and_then(|group| group.winner(*child_goal))
                        .ok_or_else(|| {
                            paro_error::internal("winner extraction lost a child winner")
                        })?
                        .cost;
                    child_costs.push(child_cost);
                }
                let base_cost = super::engine::compose_candidate_cost(
                    winner.local_cost,
                    &child_costs,
                    winner.cost_composition,
                )?;
                let base_contract = WinnerPhysicalContract {
                    required: RequiredProperties {
                        result_guarantee: physical.provided.result_guarantee,
                        ..RequiredProperties::default()
                    },
                    provided: physical.provided.clone(),
                    cost: base_cost,
                    grant,
                    origin,
                    goal_fingerprint: physical.key.stable_fingerprint(),
                    physical_fingerprint: physical.key.stable_fingerprint(),
                    implementation,
                    region_owner,
                    owned_artifacts: owned_artifacts.clone(),
                };
                let final_contract = WinnerPhysicalContract {
                    required,
                    provided: winner.provided.clone(),
                    cost: winner.cost,
                    grant,
                    origin,
                    goal_fingerprint: optimization_goal_fingerprint(goal),
                    physical_fingerprint: winner.physical_fingerprint,
                    implementation,
                    region_owner,
                    owned_artifacts,
                };
                tasks.push(Task::Build(Box::new(BuildTask {
                    payload: physical.payload,
                    child_count: winner.child_goals.len(),
                    output_columns: operator_metadata.output_columns.clone(),
                    enforcers: winner.enforcers.clone(),
                    enforcer_cost_input: winner.enforcer_cost_input,
                    base_contract,
                    final_contract,
                })));
                for (child, child_goal) in winner.child_goals.iter().rev() {
                    tasks.push(Task::Visit(*child, *child_goal));
                }
            }
            Task::Build(task) => {
                let BuildTask {
                    payload,
                    child_count,
                    output_columns,
                    enforcers,
                    enforcer_cost_input,
                    base_contract,
                    final_contract,
                } = *task;
                if plans.len() < child_count {
                    return Err(paro_error::internal(
                        "physical extraction child stack underflow",
                    ));
                }
                let children = plans.split_off(plans.len() - child_count);
                let payload = payloads
                    .get_physical(payload)
                    .ok_or_else(|| paro_error::internal("unknown planner physical payload"))?;
                let mut children = children.into_iter();
                let mut plan = duplicate_plan_preserving_indices(
                    &payload.skeleton,
                    bind_context.shared().as_ref(),
                )
                .try_map_children(|_| {
                    children.next().ok_or_else(|| {
                        paro_error::internal("physical extraction lost a child plan")
                    })
                })?;
                if children.next().is_some() {
                    return Err(paro_error::internal(
                        "physical extraction produced excess child plans",
                    ));
                }
                plan.stats.estimated_cardinality = payload.output_estimate;
                contracts.insert(plan.id, base_contract.clone());
                let mut provided = base_contract.provided;
                let mut cumulative_cost = base_contract.cost;
                let enforcer_cost = super::engine::enforcer_cost(
                    enforcers.as_ref(),
                    enforcer_cost_input,
                    memo.calibration(),
                )?
                .ok_or_else(|| {
                    paro_error::internal("extracted enforcer exceeds its verified resource grant")
                })?;
                let expected_final_cost = cumulative_cost.sequential(enforcer_cost)?;
                if !enforcers.is_empty() && expected_final_cost != final_contract.cost {
                    return Err(paro_error::internal(
                        "extracted enforcer chain cost disagrees with the verified winner",
                    ));
                }
                let mut physical_enforcers = Vec::with_capacity(enforcers.len());
                for (index, enforcer) in enforcers.iter().enumerate() {
                    let is_final = index + 1 == enforcers.len();
                    provided = enforcer.apply(provided, &final_contract.required)?;
                    let single_cost = super::engine::enforcer_cost(
                        std::slice::from_ref(enforcer),
                        enforcer_cost_input,
                        memo.calibration(),
                    )?
                    .ok_or_else(|| {
                        paro_error::internal(
                            "extracted enforcer step exceeds its verified resource grant",
                        )
                    })?;
                    cumulative_cost = cumulative_cost.sequential(single_cost)?;
                    let mut contract = final_contract.clone();
                    contract.required = if is_final {
                        final_contract.required.clone()
                    } else {
                        RequiredProperties {
                            result_guarantee: provided.result_guarantee,
                            ..RequiredProperties::default()
                        }
                    };
                    contract.provided = provided.clone();
                    contract.cost = if is_final {
                        final_contract.cost
                    } else {
                        cumulative_cost
                    };
                    contract.origin = crate::physical::properties::PlanOrigin::Enforcer(
                        enforcer.stable_fingerprint(),
                    );
                    contract.implementation = PhysicalImplementationFlavor::Structural;
                    contract.region_owner = None;
                    contract.owned_artifacts = Box::new([]);
                    physical_enforcers.push(ExtractedEnforcerContract {
                        enforcer: extract_physical_enforcer(
                            &plan,
                            enforcer,
                            &output_columns,
                            &final_contract.required,
                        )?,
                        contract,
                    });
                }
                if enforcers.is_empty() {
                    contracts.insert(plan.id, final_contract);
                } else if extracted_enforcers
                    .insert(plan.id, physical_enforcers.into_boxed_slice())
                    .is_some()
                {
                    return Err(paro_error::internal(
                        "physical extraction assigned two enforcer chains to one plan node",
                    ));
                }
                plans.push(plan);
            }
        }
    }
    if plans.len() != 1 {
        return Err(paro_error::internal(
            "physical extraction did not produce exactly one root",
        ));
    }
    Ok((plans.pop().unwrap(), contracts, extracted_enforcers))
}

fn extract_physical_enforcer(
    child: &LogicalPlan,
    enforcer: &super::enforcer::EnforcerStep,
    output_columns: &[ColumnId],
    required: &RequiredProperties,
) -> Result<ExtractedPhysicalEnforcer> {
    match enforcer {
        super::enforcer::EnforcerStep::Sort(ordering) => {
            if ordering.scope != OrderingScope::Global {
                return Err(paro_error::not_implemented(
                    "partition-local sort requires an exchange-aware physical ABI",
                ));
            }
            let child_types = child.types();
            let mut orders = Vec::with_capacity(ordering.keys.len());
            for key in &ordering.keys {
                if key.collation.is_some() {
                    return Err(paro_error::not_implemented(
                        "collation-aware sort enforcer is not executable yet",
                    ));
                }
                let index = output_columns
                    .iter()
                    .position(|column| *column == key.column)
                    .ok_or_else(|| {
                        paro_error::internal(
                            "sort enforcer key is absent from the extracted row layout",
                        )
                    })?;
                let logical_type = child_types.get(index).cloned().ok_or_else(|| {
                    paro_error::internal(
                        "sort enforcer key position exceeds the extracted row layout",
                    )
                })?;
                orders.push(OrderByNode {
                    expression: Expression::Reference(ReferenceExpression::new(
                        index,
                        logical_type,
                    )),
                    ascending: key.direction == SortDirection::Asc,
                    nulls_first: key.nulls == NullOrder::First,
                });
            }
            Ok(ExtractedPhysicalEnforcer::Sort {
                orders: orders.into_boxed_slice(),
            })
        }
        super::enforcer::EnforcerStep::MutationInputSpool { barrier } => {
            let MutationSafetyRequirement::StableReadBeforeWrite { targets, snapshot } =
                &required.mutation_safety
            else {
                return Err(paro_error::internal(
                    "mutation input spool has no stable-read requirement",
                ));
            };
            Ok(ExtractedPhysicalEnforcer::MutationInputSpool {
                barrier: *barrier,
                targets: targets.clone(),
                snapshot: *snapshot,
            })
        }
        _ => Err(paro_error::not_implemented(format!(
            "physical extraction has no executable ABI for enforcer {enforcer:?}"
        ))),
    }
}

fn extracted_region_ownership(
    memo: &Memo,
    winner: &super::memo::Winner,
) -> Result<(
    Option<Fingerprint>,
    Box<[crate::physical::OwnedAuxiliaryArtifact]>,
)> {
    let Some(proof) = &winner.joint_cost_proof else {
        return Ok((None, Box::new([])));
    };
    let region = memo
        .regions()
        .node(proof.region)
        .ok_or_else(|| paro_error::internal("winner region ownership disappeared"))?;
    let artifacts = proof
        .owned_artifacts
        .iter()
        .map(|artifact| crate::physical::OwnedAuxiliaryArtifact {
            fingerprint: artifact.fingerprint,
            kind: match artifact.kind {
                RegionArtifactKind::RuntimeFilter => {
                    crate::physical::AuxiliaryArtifactKind::RuntimeFilter
                }
                RegionArtifactKind::SharedSpool => {
                    crate::physical::AuxiliaryArtifactKind::SharedSpool
                }
                RegionArtifactKind::WorkTable => crate::physical::AuxiliaryArtifactKind::WorkTable,
                RegionArtifactKind::ExactRowset => {
                    crate::physical::AuxiliaryArtifactKind::ExactRowset
                }
            },
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Ok((Some(region.stable_fingerprint()), artifacts))
}

fn optimization_goal_fingerprint(goal: OptimizationGoal) -> Fingerprint {
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_u64(goal.required.0 as u64);
    fingerprint.write_u64(goal.row_goal.stable_tag());
    fingerprint.write_u64(goal.objective.0 as u64);
    fingerprint.write_u64(goal.grant.stable_tag());
    fingerprint.write_u64(goal.context.0 as u64);
    fingerprint.finish()
}

fn derive_provided_ordering(
    operator: &LogicalOperator,
    output_columns: &[ColumnId],
    child_columns: Option<&[ColumnId]>,
    binding_ids: &BTreeMap<(usize, usize, Fingerprint), ColumnId>,
) -> ProvidedOrdering {
    if let LogicalOperator::SearchScan(search) = operator {
        return output_columns
            .get(search.score_projection_index)
            .copied()
            .map(|column| ProvidedOrdering::Ordered {
                keys: vec![OrderingKey {
                    column,
                    direction: if search.order_ascending {
                        SortDirection::Asc
                    } else {
                        SortDirection::Desc
                    },
                    nulls: if search.order_ascending {
                        NullOrder::Last
                    } else {
                        NullOrder::First
                    },
                    collation: None,
                }]
                .into_boxed_slice(),
                scope: OrderingScope::Global,
            })
            .unwrap_or(ProvidedOrdering::Unordered);
    }
    let orders: &[OrderByNode] = match operator {
        LogicalOperator::Order(order) => &order.orders,
        LogicalOperator::TopN(topn) => &topn.orders,
        _ => return ProvidedOrdering::Unordered,
    };
    let keys = orders
        .iter()
        .map(|order| {
            let column = match &order.expression {
                Expression::ColumnRef(column) if column.depth == 0 => binding_ids
                    .get(&(
                        column.binding.table_index,
                        column.binding.column_index,
                        logical_type_fingerprint(&column.return_type),
                    ))
                    .copied(),
                Expression::Reference(reference) => child_columns
                    .and_then(|columns| columns.get(reference.index))
                    .copied(),
                _ => None,
            }?;
            Some(OrderingKey {
                column,
                direction: if order.ascending {
                    SortDirection::Asc
                } else {
                    SortDirection::Desc
                },
                nulls: if order.nulls_first {
                    NullOrder::First
                } else {
                    NullOrder::Last
                },
                collation: None,
            })
        })
        .collect::<Option<Vec<_>>>();
    match keys {
        Some(keys) if !keys.is_empty() => ProvidedOrdering::Ordered {
            keys: keys.into_boxed_slice(),
            scope: OrderingScope::Global,
        },
        _ => ProvidedOrdering::Unordered,
    }
}

fn derive_logical_properties(
    operator: &LogicalOperator,
    child_maximum_cardinalities: &[Option<u64>],
) -> LogicalProperties {
    // Group properties describe the output relation, never a particular
    // expression's relationship to its children.  Only publish bounds that
    // survive substitution by an equivalent expression.
    let unary_bound = || child_maximum_cardinalities.first().copied().flatten();
    let maximum_cardinality = match operator {
        LogicalOperator::DummyScan => Some(1),
        LogicalOperator::EmptyResult(_) => Some(0),
        LogicalOperator::Projection(_)
        | LogicalOperator::Filter(_)
        | LogicalOperator::RowFetch(_)
        | LogicalOperator::ExternalProject(_)
        | LogicalOperator::Limit(_)
        | LogicalOperator::Order(_)
        | LogicalOperator::TopN(_)
        | LogicalOperator::Distinct(_)
        | LogicalOperator::Window(_) => unary_bound(),
        _ => None,
    };
    LogicalProperties {
        maximum_cardinality,
        ..Default::default()
    }
}

fn planner_grant_dependency(operator: &LogicalOperator) -> GrantDependencyDescriptor {
    if matches!(
        operator,
        LogicalOperator::Aggregate(_)
            | LogicalOperator::Distinct(_)
            | LogicalOperator::Order(_)
            | LogicalOperator::TopN(_)
            | LogicalOperator::Window(_)
            | LogicalOperator::MaterializedCTE(_)
            | LogicalOperator::RecursiveCTE(_)
            | LogicalOperator::Join(Join::Comparison(_))
            | LogicalOperator::Join(Join::Cross(_))
    ) {
        GrantDependencyDescriptor::Sensitive
    } else {
        GrantDependencyDescriptor::Invariant
    }
}

fn planner_operator_spillable(operator: &LogicalOperator) -> bool {
    match operator {
        LogicalOperator::Aggregate(aggregate) => {
            !aggregate.groups.is_empty()
                && aggregate.grouping_sets.is_empty()
                && aggregate.grouping_functions.is_empty()
                && aggregate.aggregates.iter().all(|expression| {
                    matches!(
                        expression,
                        Expression::Aggregate(aggregate)
                            if !aggregate.is_distinct() && aggregate.order_bys.is_empty()
                    )
                })
        }
        LogicalOperator::Distinct(_) | LogicalOperator::Order(_) | LogicalOperator::Window(_) => {
            true
        }
        LogicalOperator::Join(Join::Comparison(join)) => {
            crate::physical::extraction::helpers::supports_external_hash_join_type(join.join_type)
        }
        _ => false,
    }
}

fn implementation_spillable(
    metadata: &PlannerOperatorMetadata,
    flavor: PhysicalImplementationFlavor,
) -> bool {
    match flavor {
        PhysicalImplementationFlavor::HashJoin
        | PhysicalImplementationFlavor::HashJoinRuntimeFilter
        | PhysicalImplementationFlavor::HashAggregate
        | PhysicalImplementationFlavor::PartitionAggregateWindow => metadata.spillable,
        PhysicalImplementationFlavor::Structural => metadata.spillable,
        PhysicalImplementationFlavor::NestedLoopJoin
        | PhysicalImplementationFlavor::PerfectHashAggregate
        | PhysicalImplementationFlavor::SingletonAggregateProjection
        | PhysicalImplementationFlavor::Window
        | PhysicalImplementationFlavor::SortRangeJoin
        | PhysicalImplementationFlavor::ClassicIeJoin
        | PhysicalImplementationFlavor::SearchProvider => false,
    }
}

fn planner_structural_retained_children(operator: &LogicalOperator) -> u64 {
    match operator {
        LogicalOperator::Order(_)
        | LogicalOperator::TopN(_)
        | LogicalOperator::Distinct(_)
        | LogicalOperator::Window(_) => 0b1,
        LogicalOperator::MaterializedCTE(_)
        | LogicalOperator::RecursiveCTE(_)
        | LogicalOperator::DependentJoin(_)
        | LogicalOperator::Join(_) => 0b11,
        _ => 0,
    }
}

fn planner_cost_composition(
    metadata: &PlannerOperatorMetadata,
    flavor: PhysicalImplementationFlavor,
) -> CostComposition {
    let overlapping_children = match flavor {
        PhysicalImplementationFlavor::HashJoin
        | PhysicalImplementationFlavor::HashJoinRuntimeFilter => 0b11,
        PhysicalImplementationFlavor::HashAggregate
        | PhysicalImplementationFlavor::PerfectHashAggregate
        | PhysicalImplementationFlavor::Window
        | PhysicalImplementationFlavor::PartitionAggregateWindow => 0b1,
        PhysicalImplementationFlavor::SingletonAggregateProjection => 0,
        PhysicalImplementationFlavor::Structural => metadata.structural_retained_children,
        PhysicalImplementationFlavor::NestedLoopJoin
        | PhysicalImplementationFlavor::SortRangeJoin
        | PhysicalImplementationFlavor::ClassicIeJoin
        | PhysicalImplementationFlavor::SearchProvider => 0,
    };
    if overlapping_children == 0 {
        CostComposition::Sequential
    } else {
        CostComposition::RetainedState {
            overlapping_children,
        }
    }
}

fn append_grant_fingerprint(
    fingerprint: &mut StableFingerprintBuilder,
    dependency: GrantDependencyDescriptor,
    grant: GrantGoalKey,
) {
    if dependency == GrantDependencyDescriptor::Sensitive {
        fingerprint.write_u64(grant.stable_tag());
    }
}

fn cost_for_grant(
    mut cost: SearchCost,
    dependency: GrantDependencyDescriptor,
    spillable: bool,
    grant: GrantGoalKey,
    classes: &BTreeMap<super::ids::ResourceGrantClassId, ResourceGrantClass>,
    force_spill: bool,
) -> Result<Option<SearchCost>> {
    if dependency == GrantDependencyDescriptor::Invariant {
        return Ok(Some(cost));
    }
    let GrantGoalKey::Class(class_id) = grant else {
        return Err(paro_error::internal(
            "grant-sensitive implementation received an invariant goal",
        ));
    };
    let class = classes.get(&class_id).ok_or_else(|| {
        paro_error::internal("physical implementation references an unknown grant class")
    })?;
    if force_spill && spillable {
        if class.spill_policy == SpillPolicy::Forbidden {
            return Ok(None);
        }
        let spilled = cost.peak_memory_upper.max(1);
        cost.peak_memory_upper = cost.peak_memory_upper.min(class.hard_memory_bytes);
        add_spill_cost(&mut cost, spilled)?;
        return Ok(Some(cost));
    }
    if cost.peak_memory_upper <= class.hard_memory_bytes {
        return Ok(Some(cost));
    }
    if !spillable || class.spill_policy == SpillPolicy::Forbidden {
        return Ok(None);
    }

    let spilled = cost
        .peak_memory_upper
        .saturating_sub(class.hard_memory_bytes);
    cost.peak_memory_upper = class.hard_memory_bytes;
    add_spill_cost(&mut cost, spilled)?;
    Ok(Some(cost))
}

fn planner_enforcer_cost_input(
    metadata: &PlannerOperatorMetadata,
    grant: GrantGoalKey,
    classes: &BTreeMap<super::ids::ResourceGrantClassId, ResourceGrantClass>,
) -> Result<super::engine::EnforcerCostInput> {
    let mut input = super::engine::EnforcerCostInput::unbounded(
        metadata.cost_facts.output_rows,
        metadata.cost_facts.output_row_width,
    );
    if let GrantGoalKey::Class(class) = grant {
        let class = classes.get(&class).ok_or_else(|| {
            paro_error::internal("enforcer costing references an unknown grant class")
        })?;
        input.hard_memory_bytes = class.hard_memory_bytes;
        input.spill_policy = class.spill_policy;
    }
    Ok(input)
}

fn add_spill_cost(cost: &mut SearchCost, spilled: u64) -> Result<()> {
    cost.spill_bytes_expected = cost.spill_bytes_expected.saturating_add(spilled);
    let io_work = (spilled as f64 / 4096.0).max(1.0);
    let spill_range = CompactRange::new(io_work, io_work * 2.0, io_work * 6.0)?;
    cost.score.range = cost.score.range.checked_add(spill_range)?;
    cost.score.risk_adjusted += io_work * 3.0;
    cost.critical_path = cost.critical_path.checked_add(spill_range)?;
    cost.resources_expected[ResourceDimension::SequentialIo as usize] += io_work * 2.0;
    cost.resources_risk_upper[ResourceDimension::SequentialIo as usize] += io_work * 6.0;
    cost.validate()?;
    Ok(())
}

fn provided_result_guarantee(operator: &LogicalOperator) -> ResultGuarantee {
    match operator {
        LogicalOperator::SearchScan(scan)
            if scan.request.intents.iter().any(|intent| {
                matches!(
                    intent,
                    paro_storage::search::SearchIntent::Hnsw(hnsw)
                        if hnsw.options.objective
                            == paro_storage::index::hnsw::HnswSearchObjective::CostOptimized
                )
            }) =>
        {
            ResultGuarantee::ApproximateAllowed(COST_OPTIMIZED_SEARCH_POLICY)
        }
        _ => ResultGuarantee::Exact,
    }
}

fn search_payload_fingerprint(
    logical_fingerprint: Fingerprint,
    operator: &LogicalOperator,
) -> Fingerprint {
    fn write_candidate(
        fingerprint: &mut StableFingerprintBuilder,
        candidate: &paro_planner::operator::SearchCandidate,
    ) {
        fingerprint.write_u64(candidate.token.definition_id);
        fingerprint.write_u64(candidate.token.generation_id);
        fingerprint.write_u64(candidate.token.root_version);
    }

    fn write_decision(
        fingerprint: &mut StableFingerprintBuilder,
        decision: &paro_planner::operator::SearchDecision,
    ) {
        match decision {
            paro_planner::operator::SearchDecision::IndexScan { candidate, .. } => {
                fingerprint.write_u64(0);
                write_candidate(fingerprint, candidate);
            }
            paro_planner::operator::SearchDecision::Adaptive {
                candidates,
                sequential,
            } => {
                fingerprint.write_u64(1);
                fingerprint.write_u64(sequential.table_id);
                fingerprint.write_u64(candidates.len() as u64);
                for candidate in candidates {
                    write_candidate(fingerprint, candidate);
                }
            }
        }
    }

    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_bytes(b"paro.physical.search-provider.v1");
    fingerprint.write_fingerprint(logical_fingerprint);
    match operator {
        LogicalOperator::SearchScan(search) => {
            fingerprint.write_u64(0);
            encode_search_request(&mut fingerprint, &search.request);
            fingerprint.write_u64(search.score_projection_index as u64);
            fingerprint.write_u64(search.order_ascending as u64);
            fingerprint.write_u64(search.limit as u64);
            write_decision(&mut fingerprint, &search.decision);
        }
        LogicalOperator::FullTextFilterScan(search) => {
            fingerprint.write_u64(1);
            encode_search_request(&mut fingerprint, &search.request);
            encode_projection_map(&mut fingerprint, &search.projection_map);
            write_decision(&mut fingerprint, &search.decision);
        }
        _ => fingerprint.write_u64(u64::MAX),
    }
    fingerprint.finish()
}

/// Bind-time query options form the root semantic contract. With Exact as the
/// API default, seeing CostOptimized here necessarily represents an explicit
/// opt-in and may therefore admit the matching approximate provider policy.
fn required_result_guarantee(plan: &LogicalPlan) -> ResultGuarantee {
    match &plan.operator {
        LogicalOperator::TopN(topn)
            if topn.hnsw_options.objective
                == paro_storage::index::hnsw::HnswSearchObjective::CostOptimized =>
        {
            ResultGuarantee::ApproximateAllowed(COST_OPTIMIZED_SEARCH_POLICY)
        }
        LogicalOperator::SearchScan(_) => provided_result_guarantee(&plan.operator),
        _ => plan
            .children()
            .into_iter()
            .map(required_result_guarantee)
            .find(|guarantee| matches!(guarantee, ResultGuarantee::ApproximateAllowed(_)))
            .unwrap_or(ResultGuarantee::Exact),
    }
}

fn planner_implementation_set(
    plan: &LogicalPlan,
    rowset_scan_pushdown: bool,
) -> PlannerImplementationSet {
    match &plan.operator {
        LogicalOperator::Aggregate(aggregate) => PlannerImplementationSet {
            baseline: PhysicalImplementationFlavor::HashAggregate,
            perfect_hash_aggregate:
                crate::physical::extraction::helpers::can_use_perfect_hash_aggregate(
                    aggregate,
                    &aggregate.groups,
                    &aggregate.aggregates,
                )
                .is_some(),
            singleton_aggregate_projection:
                crate::physical::extraction::aggregate::supports_singleton_aggregate_projection(
                    aggregate,
                ),
            ..PlannerImplementationSet::STRUCTURAL
        },
        LogicalOperator::Join(Join::Comparison(join)) => {
            if !join.duplicate_eliminated_columns.is_empty() || join.delim_flipped {
                return PlannerImplementationSet::STRUCTURAL;
            }
            let has_hash_key = join.conditions.iter().any(|condition| {
                matches!(
                    condition.comparison,
                    JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
                )
            });
            let mark_has_residual = join.join_type == JoinType::Mark
                && join.conditions.iter().any(|condition| {
                    !matches!(
                        condition.comparison,
                        JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
                    )
                });
            let supports_hash_type = matches!(
                join.join_type,
                JoinType::Left
                    | JoinType::Right
                    | JoinType::Inner
                    | JoinType::Outer
                    | JoinType::Semi
                    | JoinType::Anti
                    | JoinType::Mark
                    | JoinType::Single
                    | JoinType::RightSemi
                    | JoinType::RightAnti
            );
            let baseline = if has_hash_key && !mark_has_residual && supports_hash_type {
                PhysicalImplementationFlavor::HashJoin
            } else if join.anti_join_mode == AntiJoinMode::NullAware {
                // The extractor reports the precise semantic capability error;
                // keep structural lowering for this malformed/non-hashable
                // shape rather than advertising NLJ as null-aware.
                PhysicalImplementationFlavor::Structural
            } else {
                PhysicalImplementationFlavor::NestedLoopJoin
            };
            PlannerImplementationSet {
                baseline,
                hash_join_runtime_filter: baseline == PhysicalImplementationFlavor::HashJoin
                    && supports_runtime_filter_auxiliary(join, rowset_scan_pushdown),
                sort_range_join:
                    crate::physical::extraction::inequality_join_gate::is_sort_range_join_candidate(
                        join,
                        plan.stats.estimated_cardinality,
                    ),
                classic_ie_join:
                    crate::physical::extraction::inequality_join_gate::is_classic_ie_join_candidate(
                        join,
                        plan.stats.estimated_cardinality,
                    ),
                ..PlannerImplementationSet::STRUCTURAL
            }
        }
        LogicalOperator::Window(window) => PlannerImplementationSet {
            baseline: PhysicalImplementationFlavor::Window,
            partition_aggregate_window:
                crate::physical::extraction::misc::supports_partition_aggregate_window(window),
            ..PlannerImplementationSet::STRUCTURAL
        },
        _ => PlannerImplementationSet::STRUCTURAL,
    }
}

/// Current AuxiliaryPlanRegion support is intentionally narrow: the probe
/// must be a direct base rowset so extraction can name one unambiguous
/// consumer. Broader lineage remains a future implementation registration,
/// never an execution-time inference.
fn supports_runtime_filter_auxiliary(
    join: &paro_planner::operator::ComparisonJoin,
    rowset_scan_pushdown: bool,
) -> bool {
    if !matches!(
        join.join_type,
        JoinType::Inner | JoinType::Semi | JoinType::RightSemi | JoinType::RightAnti
    ) {
        return false;
    }
    let (get, filter_projection) = match &join.left.operator {
        LogicalOperator::Get(get) => (get, None),
        LogicalOperator::Filter(filter) if rowset_scan_pushdown => {
            let LogicalOperator::Get(get) = &filter.child.operator else {
                return false;
            };
            let filter_is_fully_pushable = [
                filter.expressions.as_slice(),
                get.runtime_filter_expressions.as_slice(),
            ]
            .into_iter()
            .all(|expressions| {
                crate::physical::extraction::predicate_builder::build_predicate_tree(
                    expressions,
                    get,
                )
                .is_ok_and(|(_, residual)| residual.is_empty())
            });
            if !filter_is_fully_pushable {
                return false;
            }
            (
                get,
                Some(filter.projection_map.to_indices(filter.child.types().len())),
            )
        }
        _ => return false,
    };
    if get.table.is_none() {
        return false;
    }
    join.conditions.iter().any(|condition| {
        if condition.comparison != JoinComparisonType::Equal {
            return false;
        }
        match &condition.left {
            Expression::ColumnRef(column) => {
                column.depth == 0 && column.binding.table_index == get.table_index
            }
            // Join ordering and slot binding may already have converted the
            // direct Get output into its local row position. Accept only a
            // stored column, never a derived prefix or virtual row id.
            Expression::Reference(reference) => {
                let get_index = filter_projection
                    .as_ref()
                    .and_then(|projection| projection.get(reference.index))
                    .copied()
                    .unwrap_or(reference.index);
                get_index < get.returned_types.len() && get.stored_column(get_index).is_some()
            }
            _ => false,
        }
    })
}

fn selected_implementation_flavor(
    id: ImplementationId,
    implementations: PlannerImplementationSet,
) -> Result<PhysicalImplementationFlavor> {
    match id {
        PLANNER_BASELINE_IMPLEMENTATION => Ok(implementations.baseline),
        PLANNER_PERFECT_HASH_AGGREGATE => Ok(PhysicalImplementationFlavor::PerfectHashAggregate),
        PLANNER_SORT_RANGE_JOIN => Ok(PhysicalImplementationFlavor::SortRangeJoin),
        PLANNER_CLASSIC_IE_JOIN => Ok(PhysicalImplementationFlavor::ClassicIeJoin),
        PLANNER_SEARCH_PROVIDER => Ok(PhysicalImplementationFlavor::SearchProvider),
        PLANNER_HASH_JOIN_RUNTIME_FILTER => Ok(PhysicalImplementationFlavor::HashJoinRuntimeFilter),
        PLANNER_PARTITION_AGGREGATE_WINDOW => {
            Ok(PhysicalImplementationFlavor::PartitionAggregateWindow)
        }
        PLANNER_SINGLETON_AGGREGATE_PROJECTION => {
            Ok(PhysicalImplementationFlavor::SingletonAggregateProjection)
        }
        _ => Err(paro_error::internal(
            "winner references an implementation unavailable for its logical expression",
        )),
    }
}

fn planner_region_contract(
    memo: &Memo,
    facet: Option<Fingerprint>,
    artifact_kind: Option<RegionArtifactKind>,
) -> Result<Option<RegionCandidateContract>> {
    let Some(facet) = facet else {
        if artifact_kind.is_some() {
            return Err(paro_error::internal(
                "region artifact has no owning planning facet",
            ));
        }
        return Ok(None);
    };
    let region = memo.regions().region_for_facet(facet).ok_or_else(|| {
        paro_error::internal("physical implementation references an unowned planning facet")
    })?;
    let artifacts = artifact_kind
        .map(|kind| {
            vec![RegionOwnedArtifact {
                fingerprint: facet,
                kind,
            }]
            .into_boxed_slice()
        })
        .unwrap_or_default();
    Ok(Some(RegionCandidateContract {
        region,
        facets: vec![facet].into_boxed_slice(),
        artifacts,
    }))
}

const OP_HASH_BUILD_ROW: OpClassId = OpClassId(1);
const OP_HASH_PROBE_ROW: OpClassId = OpClassId(2);
const OP_NESTED_LOOP_PAIR: OpClassId = OpClassId(3);
const OP_SORT_COMPARE: OpClassId = OpClassId(4);
const OP_RANGE_JOIN_ROW: OpClassId = OpClassId(5);
const OP_IE_JOIN_ROW: OpClassId = OpClassId(6);
const OP_HASH_AGGREGATE_ROW: OpClassId = OpClassId(7);
const OP_HASH_AGGREGATE_GROUP: OpClassId = OpClassId(8);
const OP_PERFECT_AGGREGATE_ROW: OpClassId = OpClassId(9);
const OP_PERFECT_AGGREGATE_SLOT: OpClassId = OpClassId(10);
// 11 and 12 are the stable runtime-filter classes in `calibration`; keep the
// planner-local namespace disjoint so calibration cannot silently price an
// unrelated operator with runtime-filter coefficients.
const OP_WINDOW_ROW: OpClassId = OpClassId(13);
const OP_PARTITION_AGGREGATE_WINDOW_ROW: OpClassId = OpClassId(14);
const OP_SINGLETON_AGGREGATE_PROJECT_ROW: OpClassId = OpClassId(15);

fn planner_cost_facts(plan: &LogicalPlan) -> Result<PlannerCostFacts> {
    let output_rows = cardinality_work_range(plan.stats.estimated_cardinality)?;
    let child_rows = plan
        .children()
        .into_iter()
        .map(|child| cardinality_work_range(child.stats.estimated_cardinality))
        .collect::<Result<Vec<_>>>()?
        .into_boxed_slice();
    let output_row_width = plan
        .types()
        .iter()
        .map(|logical_type| logical_type.type_size().max(1) as u64)
        .sum::<u64>()
        .saturating_add(32);
    let perfect_hash_slots = match &plan.operator {
        LogicalOperator::Aggregate(aggregate) => {
            crate::physical::extraction::helpers::can_use_perfect_hash_aggregate(
                aggregate,
                &aggregate.groups,
                &aggregate.aggregates,
            )
            .and_then(|info| {
                info.group_cardinalities
                    .into_iter()
                    .try_fold(1u64, |slots, cardinality| {
                        slots.checked_mul(u64::try_from(cardinality).ok()?)
                    })
            })
        }
        _ => None,
    };
    Ok(PlannerCostFacts {
        output_rows,
        child_rows,
        output_row_width,
        perfect_hash_slots,
    })
}

fn cardinality_work_range(
    estimate: Option<paro_planner::plan::CardinalityEstimate>,
) -> Result<CompactRange> {
    match estimate {
        Some(estimate) => CompactRange::new(
            estimate.min as f64,
            estimate.expected as f64,
            estimate.max as f64,
        ),
        None => CompactRange::new(0.0, 1.0, 4.0),
    }
}

fn multiply_work(left: CompactRange, right: CompactRange) -> Result<CompactRange> {
    CompactRange::new(
        left.lower * right.lower,
        left.expected * right.expected,
        left.upper * right.upper,
    )
}

fn sort_work(rows: CompactRange) -> Result<CompactRange> {
    let comparisons = |value: f64| {
        if value <= 1.0 {
            value
        } else {
            value * value.log2()
        }
    };
    CompactRange::new(
        comparisons(rows.lower),
        comparisons(rows.expected),
        comparisons(rows.upper),
    )
}

fn implementation_cost(
    metadata: &PlannerOperatorMetadata,
    flavor: PhysicalImplementationFlavor,
    calibration: &MachineCalibrationBundle,
) -> Result<SearchCost> {
    let facts = &metadata.cost_facts;
    let mut work = LocalOperatorWork::default();
    let mut peak_memory_upper = metadata.local_cost.peak_memory_upper;
    match flavor {
        PhysicalImplementationFlavor::Structural => return Ok(metadata.local_cost),
        PhysicalImplementationFlavor::SearchProvider => {
            return Err(paro_error::internal(
                "search provider cost must come from its physical payload",
            ));
        }
        PhysicalImplementationFlavor::HashAggregate => {
            let input = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(facts.output_rows);
            work.add(OP_HASH_AGGREGATE_ROW, input)?;
            work.add(OP_HASH_AGGREGATE_GROUP, facts.output_rows)?;
        }
        PhysicalImplementationFlavor::PerfectHashAggregate => {
            let input = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(facts.output_rows);
            let slots = facts.perfect_hash_slots.ok_or_else(|| {
                paro_error::internal("perfect-hash candidate lost its proven key domain")
            })?;
            work.add(OP_PERFECT_AGGREGATE_ROW, input)?;
            work.add(
                OP_PERFECT_AGGREGATE_SLOT,
                CompactRange::point(slots as f64)?,
            )?;
            peak_memory_upper = slots.saturating_mul(facts.output_row_width.saturating_add(16));
        }
        PhysicalImplementationFlavor::SingletonAggregateProjection => {
            let input = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(facts.output_rows);
            work.add(OP_SINGLETON_AGGREGATE_PROJECT_ROW, input)?;
            peak_memory_upper = 0;
        }
        PhysicalImplementationFlavor::Window => {
            let input = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(facts.output_rows);
            work.add(OP_SORT_COMPARE, sort_work(input)?)?;
            work.add(OP_WINDOW_ROW, input)?;
            peak_memory_upper = (input.upper as u64).saturating_mul(facts.output_row_width.max(32));
        }
        PhysicalImplementationFlavor::PartitionAggregateWindow => {
            let input = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(facts.output_rows);
            work.add(OP_PARTITION_AGGREGATE_WINDOW_ROW, input)?;
            peak_memory_upper = (input.upper as u64).saturating_mul(facts.output_row_width.max(32));
        }
        PhysicalImplementationFlavor::HashJoin
        | PhysicalImplementationFlavor::HashJoinRuntimeFilter => {
            let left = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(CompactRange::ZERO);
            let right = facts
                .child_rows
                .get(1)
                .copied()
                .unwrap_or(CompactRange::ZERO);
            work.add(OP_HASH_BUILD_ROW, right)?;
            let probe = if flavor == PhysicalImplementationFlavor::HashJoinRuntimeFilter {
                work.add(OP_RUNTIME_FILTER_BUILD_ROW, right)?;
                work.add(OP_RUNTIME_FILTER_APPLY_ROW, left)?;
                runtime_filtered_probe_work(left, right)?
            } else {
                left
            };
            work.add(OP_HASH_PROBE_ROW, probe.checked_add(facts.output_rows)?)?;
            peak_memory_upper = (right.upper as u64)
                .saturating_mul(facts.output_row_width.saturating_div(2).max(32));
            if flavor == PhysicalImplementationFlavor::HashJoinRuntimeFilter {
                // The execution policy freezes at most a bounded exact domain
                // before degrading to min/max. Charge the upper envelope here
                // so the auxiliary artifact participates in grant admission.
                peak_memory_upper = peak_memory_upper
                    .saturating_add((right.upper as u64).min(65_536).saturating_mul(24));
            }
        }
        PhysicalImplementationFlavor::NestedLoopJoin => {
            let left = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(CompactRange::ZERO);
            let right = facts
                .child_rows
                .get(1)
                .copied()
                .unwrap_or(CompactRange::ZERO);
            work.add(OP_NESTED_LOOP_PAIR, multiply_work(left, right)?)?;
            work.add(OP_RANGE_JOIN_ROW, facts.output_rows)?;
            peak_memory_upper = 0;
        }
        PhysicalImplementationFlavor::SortRangeJoin
        | PhysicalImplementationFlavor::ClassicIeJoin => {
            let left = facts
                .child_rows
                .first()
                .copied()
                .unwrap_or(CompactRange::ZERO);
            let right = facts
                .child_rows
                .get(1)
                .copied()
                .unwrap_or(CompactRange::ZERO);
            work.add(
                OP_SORT_COMPARE,
                sort_work(left)?.checked_add(sort_work(right)?)?,
            )?;
            work.add(
                if flavor == PhysicalImplementationFlavor::SortRangeJoin {
                    OP_RANGE_JOIN_ROW
                } else {
                    OP_IE_JOIN_ROW
                },
                left.checked_add(right)?.checked_add(facts.output_rows)?,
            )?;
            peak_memory_upper =
                ((left.upper + right.upper) as u64).saturating_mul(facts.output_row_width.max(32));
        }
    }
    let mut cost = calibration.fold(&work)?;
    cost.peak_memory_upper = peak_memory_upper;
    cost.validate()?;
    Ok(cost)
}

fn runtime_filtered_probe_work(probe: CompactRange, build: CompactRange) -> Result<CompactRange> {
    let retained = |probe_rows: f64, build_rows: f64| {
        if probe_rows <= 0.0 {
            return 0.0;
        }
        let ratio = (build_rows / probe_rows).clamp(0.0, 1.0);
        // Without a joint histogram the expected benefit is deliberately
        // damped. The upper bound retains the no-benefit fallback.
        probe_rows * ratio.sqrt().clamp(0.1, 1.0)
    };
    let expected = retained(probe.expected, build.expected);
    CompactRange::new(0.0, expected, probe.upper.max(expected))
}

fn is_contextual_operator(operator: &LogicalOperator) -> bool {
    matches!(
        operator,
        LogicalOperator::Join(_)
            | LogicalOperator::DependentJoin(_)
            | LogicalOperator::Aggregate(_)
            | LogicalOperator::MaterializedCTE(_)
            | LogicalOperator::RecursiveCTE(_)
            | LogicalOperator::GraphMatch(_)
            | LogicalOperator::GraphExpand(_)
            | LogicalOperator::SearchScan(_)
            | LogicalOperator::FullTextFilterScan(_)
    )
}

fn required_region_kind(operator: &LogicalOperator) -> Option<RegionFacetKind> {
    match operator {
        LogicalOperator::DependentJoin(_) => Some(RegionFacetKind::Parameterization),
        LogicalOperator::MaterializedCTE(_) => Some(RegionFacetKind::Sharing),
        LogicalOperator::RecursiveCTE(_) => Some(RegionFacetKind::Recursion),
        _ => None,
    }
}

fn planner_region_facet(
    kind: RegionFacetKind,
    criticality: FacetCriticality,
    logical: LogicalExprId,
    operator: Fingerprint,
    scope: BTreeSet<GroupId>,
) -> RegionFacet {
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_bytes(b"paro.planning-region.facet.v1");
    fingerprint.write_u64(kind as u64);
    fingerprint.write_u64(criticality as u64);
    fingerprint.write_u64(logical.0 as u64);
    fingerprint.write_fingerprint(operator);
    RegionFacet {
        fingerprint: fingerprint.finish(),
        kind,
        criticality,
        priority: match criticality {
            FacetCriticality::Required => 100 + kind as u16,
            FacetCriticality::Optional => 1_000 + kind as u16,
        },
        scope,
    }
}

fn binding_fingerprint(binding: ColumnBinding) -> Fingerprint {
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_u64(binding.table_index as u64);
    fingerprint.write_u64(binding.column_index as u64);
    fingerprint.finish()
}

fn typed_binding_fingerprint(binding: ColumnBinding, type_domain: Fingerprint) -> Fingerprint {
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_fingerprint(binding_fingerprint(binding));
    fingerprint.write_fingerprint(type_domain);
    fingerprint.finish()
}

fn query_operator_fingerprint(
    plan: &LogicalPlan,
    scalar_roots: &[ScalarExprId],
    scalars: &ScalarArena,
) -> Result<Fingerprint> {
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_u64(operator_tag(plan.operator.op_type()));
    fingerprint.write_u64(scalar_roots.len() as u64);
    for root in scalar_roots {
        let scalar = scalars
            .get(*root)
            .ok_or_else(|| paro_error::internal("operator references an unknown scalar root"))?;
        fingerprint.write_fingerprint(scalar.fingerprint);
    }
    for ty in plan.types() {
        super::scalar::encode_logical_type(&mut fingerprint, &ty);
    }
    match &plan.operator {
        LogicalOperator::Get(get) => encode_get(&mut fingerprint, get),
        LogicalOperator::Filter(filter) => {
            encode_projection_map(&mut fingerprint, &filter.projection_map)
        }
        LogicalOperator::Projection(projection) => {
            fingerprint.write_u64(projection.visible_count as u64);
            encode_optional_string(&mut fingerprint, projection.visible_qualifier.as_deref());
        }
        LogicalOperator::RowFetch(fetch) => {
            fingerprint.write_u64(fetch.sources.len() as u64);
            for source in &fetch.sources {
                fingerprint.write_u64(source.table.object_id().raw());
                encode_usizes(&mut fingerprint, &source.needed_columns);
            }
        }
        LogicalOperator::ExternalProject(project) => {
            fingerprint.write_u64(project.expressions.len() as u64);
            for expression in &project.expressions {
                encode_external_call(&mut fingerprint, &expression.routine_meta);
            }
        }
        LogicalOperator::ExternalTable(table) => {
            encode_external_call(&mut fingerprint, &table.call);
            fingerprint.write_u64(table.lateral as u64);
            fingerprint.write_u64(table.parameterized as u64);
        }
        LogicalOperator::Limit(limit) => {
            encode_hnsw_options(&mut fingerprint, limit.hnsw_options);
        }
        LogicalOperator::Order(order) => {
            encode_projection_map(&mut fingerprint, &order.projection_map);
            encode_orders(&mut fingerprint, &order.orders);
        }
        LogicalOperator::TopN(topn) => {
            fingerprint.write_u64(topn.limit as u64);
            fingerprint.write_u64(topn.offset as u64);
            encode_orders(&mut fingerprint, &topn.orders);
            encode_hnsw_options(&mut fingerprint, topn.hnsw_options);
        }
        LogicalOperator::ExpressionGet(values) => {
            fingerprint.write_u64(values.expressions.len() as u64);
            for row in &values.expressions {
                fingerprint.write_u64(row.len() as u64);
            }
        }
        LogicalOperator::Join(join) => match join {
            Join::Comparison(join) => {
                fingerprint.write_u64(0);
                fingerprint.write_u64(join.join_type as u64);
                fingerprint.write_u64(join.anti_join_mode as u64);
                fingerprint.write_u64(join.delim_flipped as u64);
                encode_optional_usize(&mut fingerprint, join.mark_index);
                match join.mark_semantics {
                    paro_planner::operator::MarkJoinSemantics::NotMark => fingerprint.write_u64(0),
                    paro_planner::operator::MarkJoinSemantics::TwoValued => {
                        fingerprint.write_u64(1)
                    }
                    paro_planner::operator::MarkJoinSemantics::ThreeValuedFrom(index) => {
                        fingerprint.write_u64(2);
                        fingerprint.write_u64(index as u64);
                    }
                }
                encode_projection_map(&mut fingerprint, &join.left_projection_map);
                encode_projection_map(&mut fingerprint, &join.right_projection_map);
            }
            Join::Any(join) => {
                fingerprint.write_u64(1);
                fingerprint.write_u64(join.join_type as u64);
                encode_optional_usize(&mut fingerprint, join.mark_index);
                encode_projection_map(&mut fingerprint, &join.left_projection_map);
                encode_projection_map(&mut fingerprint, &join.right_projection_map);
            }
            Join::Cross(_) => fingerprint.write_u64(2),
        },
        LogicalOperator::DelimGet(_) => {}
        LogicalOperator::DependentJoin(join) => encode_dependent_join(&mut fingerprint, join),
        LogicalOperator::SetOperation(set) => {
            fingerprint.write_u64(set.setop_type as u64);
            fingerprint.write_u64(set.setop_all as u64);
            fingerprint.write_u64(set.allow_out_of_order as u64);
        }
        LogicalOperator::Distinct(distinct) => {
            fingerprint.write_u64(distinct.distinct_type as u64);
            match &distinct.order_by {
                None => fingerprint.write_u64(u64::MAX),
                Some(orders) => encode_orders(&mut fingerprint, orders),
            }
        }
        LogicalOperator::Window(window) => {
            fingerprint.write_u64(window.expressions.len() as u64);
        }
        LogicalOperator::EmptyResult(_) => {}
        LogicalOperator::Aggregate(aggregate) => {
            fingerprint.write_u64(aggregate.groups.len() as u64);
            fingerprint.write_u64(aggregate.aggregates.len() as u64);
            fingerprint.write_u64(aggregate.grouping_sets.len() as u64);
            for grouping_set in &aggregate.grouping_sets {
                encode_usizes(&mut fingerprint, &grouping_set.expressions);
            }
            fingerprint.write_u64(aggregate.grouping_functions.len() as u64);
            for grouping in &aggregate.grouping_functions {
                encode_usizes(&mut fingerprint, grouping);
            }
            fingerprint.write_u64(aggregate.group_dependencies.len() as u64);
            for dependency in &aggregate.group_dependencies {
                encode_usizes(&mut fingerprint, &dependency.determinants);
                encode_usizes(&mut fingerprint, &dependency.dependents);
            }
            fingerprint.write_u64(match aggregate.group_input_multiplicity {
                paro_planner::operator::GroupInputMultiplicity::Arbitrary => 0,
                paro_planner::operator::GroupInputMultiplicity::AtMostOne(_) => 1,
            });
            fingerprint.write_u64(aggregate.post_reduction.is_some() as u64);
        }
        LogicalOperator::MaterializedCTE(cte) => {
            fingerprint.write_u64(cte.cte_index as u64);
            fingerprint.write_u64(match cte.materialized {
                paro_planner::binder::ir::CTEMaterialize::Default => 0,
                paro_planner::binder::ir::CTEMaterialize::Materialized => 1,
                paro_planner::binder::ir::CTEMaterialize::NotMaterialized => 2,
            });
            fingerprint.write_u64(cte.ref_count as u64);
        }
        LogicalOperator::RecursiveCTE(cte) => {
            fingerprint.write_u64(cte.cte_index as u64);
            fingerprint.write_u64(cte.union_all as u64);
        }
        LogicalOperator::CTERef(cte) => {
            fingerprint.write_u64(cte.cte_index as u64);
        }
        LogicalOperator::TableFunctionGet(function) => {
            encode_table_function(&mut fingerprint, function);
        }
        LogicalOperator::SearchScan(search) => {
            encode_get(&mut fingerprint, &search.get);
            encode_search_request(&mut fingerprint, &search.request);
            fingerprint.write_u64(search.score_projection_index as u64);
            fingerprint.write_u64(search.order_ascending as u64);
            fingerprint.write_u64(search.limit as u64);
        }
        LogicalOperator::FullTextFilterScan(search) => {
            encode_get(&mut fingerprint, &search.get);
            encode_search_request(&mut fingerprint, &search.request);
            encode_projection_map(&mut fingerprint, &search.projection_map);
        }
        LogicalOperator::GraphMatch(graph) => {
            fingerprint.write_u64(graph.graph_entry.object_id().raw());
            fingerprint.write_u64(graph.table_index as u64);
            fingerprint.write_bytes(graph.relation_alias.as_bytes());
            encode_graph_pattern(&mut fingerprint, &graph.bound_pattern);
            fingerprint.write_u64(graph.columns.len() as u64);
            for column in &graph.columns {
                fingerprint.write_bytes(column.alias.as_bytes());
                super::scalar::encode_logical_type(&mut fingerprint, &column.logical_type);
            }
            fingerprint.write_u64(match graph.path_mode.as_ref() {
                None => 0,
                Some(paro_parser::ast::PathMode::AnyShortest) => 1,
                Some(paro_parser::ast::PathMode::AllShortest) => 2,
                Some(paro_parser::ast::PathMode::Any) => 3,
                Some(paro_parser::ast::PathMode::All) => 4,
            });
            fingerprint.write_u64(graph.has_path_functions as u64);
        }
        LogicalOperator::GraphScan(scan) => {
            // A graph variable's table index is part of the carrier ABI. Two
            // scans of the same vertex relation are not interchangeable when
            // downstream expands address their local-id slots by variable.
            fingerprint.write_u64(scan.table_index as u64);
            fingerprint.write_u64(scan.output_table_index as u64);
            fingerprint.write_bytes(scan.schema_name.as_bytes());
            fingerprint.write_bytes(scan.graph_name.as_bytes());
            fingerprint.write_bytes(scan.label.as_bytes());
            fingerprint.write_u64(scan.vertex_info.table_oid);
            fingerprint.write_bytes(scan.vertex_info.table_name.as_bytes());
            encode_u32s(&mut fingerprint, &scan.vertex_info.key_column_ids);
            encode_u32s(&mut fingerprint, &scan.vertex_info.property_column_ids);
        }
        LogicalOperator::GraphExpand(expand) => {
            fingerprint.write_u64(expand.source_table_index as u64);
            fingerprint.write_u64(expand.edge_table_index as u64);
            fingerprint.write_u64(expand.target_table_index as u64);
            fingerprint.write_u64(expand.output_table_index as u64);
            fingerprint.write_u64(expand.edge_info.table_oid);
            fingerprint.write_bytes(expand.edge_info.table_name.as_bytes());
            fingerprint.write_bytes(expand.edge_info.label.as_bytes());
            fingerprint.write_bytes(expand.source_label.as_bytes());
            fingerprint.write_bytes(expand.target_label.as_bytes());
            fingerprint.write_u64(expand.source_table_oid);
            fingerprint.write_u64(expand.target_table_oid);
            fingerprint.write_bytes(expand.target_table_name.as_bytes());
            fingerprint.write_u64(match expand.direction {
                paro_planner::operator::ExpandDirection::Forward => 0,
                paro_planner::operator::ExpandDirection::Backward => 1,
                paro_planner::operator::ExpandDirection::Both => 2,
            });
            fingerprint.write_bytes(
                expand
                    .quantifier
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default()
                    .as_bytes(),
            );
            fingerprint.write_bytes(
                expand
                    .path_mode
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default()
                    .as_bytes(),
            );
            fingerprint.write_u64(expand.has_path_functions as u64);
            encode_u32s(&mut fingerprint, &expand.edge_info.key_column_ids);
            encode_u32s(&mut fingerprint, &expand.edge_info.source_key_column_ids);
            fingerprint.write_bytes(expand.edge_info.source_vertex_table.as_bytes());
            encode_u32s(&mut fingerprint, &expand.edge_info.source_ref_column_ids);
            encode_u32s(
                &mut fingerprint,
                &expand.edge_info.destination_key_column_ids,
            );
            fingerprint.write_bytes(expand.edge_info.destination_vertex_table.as_bytes());
            encode_u32s(
                &mut fingerprint,
                &expand.edge_info.destination_ref_column_ids,
            );
            encode_u32s(&mut fingerprint, &expand.edge_info.property_column_ids);
        }
        LogicalOperator::DummyScan => {}
        LogicalOperator::Insert(_)
        | LogicalOperator::Delete(_)
        | LogicalOperator::Update(_)
        | LogicalOperator::CopyTo(_)
        | LogicalOperator::Explain(_)
        | LogicalOperator::Alter(_)
        | LogicalOperator::CreateTable(_)
        | LogicalOperator::CreateRoutine(_)
        | LogicalOperator::CreateSequence(_)
        | LogicalOperator::CreateSchema(_)
        | LogicalOperator::CreateIndex(_)
        | LogicalOperator::CreateView(_)
        | LogicalOperator::Drop(_)
        | LogicalOperator::CreatePropertyGraph(_)
        | LogicalOperator::DropPropertyGraph(_)
        | LogicalOperator::RefreshPropertyGraph(_) => {
            return Err(paro_error::internal(
                "statement operator crossed the statement/query boundary into Memo",
            ));
        }
    }
    Ok(fingerprint.finish())
}

fn encode_graph_pattern(
    fingerprint: &mut StableFingerprintBuilder,
    pattern: &paro_planner::binder::ir::BoundGraphPattern,
) {
    use paro_planner::binder::bind::graph::BoundPatternElement;

    fingerprint.write_u64(pattern.elements.len() as u64);
    for element in &pattern.elements {
        match element {
            BoundPatternElement::Vertex(vertex) => {
                fingerprint.write_u64(0);
                fingerprint.write_bytes(vertex.variable_name.as_bytes());
                fingerprint.write_u64(vertex.table_index as u64);
                encode_vertex_table(fingerprint, &vertex.vertex_table_info);
                encode_column_bindings(fingerprint, &vertex.column_bindings);
                encode_strings(fingerprint, &vertex.column_names);
                fingerprint.write_u64(vertex.filter.is_some() as u64);
            }
            BoundPatternElement::Edge(edge) => {
                fingerprint.write_u64(1);
                fingerprint.write_bytes(edge.variable_name.as_bytes());
                fingerprint.write_u64(edge.table_index as u64);
                encode_edge_table(fingerprint, &edge.edge_table_info);
                encode_column_bindings(fingerprint, &edge.column_bindings);
                encode_strings(fingerprint, &edge.column_names);
                fingerprint.write_u64(match edge.direction {
                    paro_parser::ast::EdgeDirection::Right => 0,
                    paro_parser::ast::EdgeDirection::Left => 1,
                    paro_parser::ast::EdgeDirection::Undirected => 2,
                    paro_parser::ast::EdgeDirection::LeftRight => 3,
                });
                match &edge.quantifier {
                    None => fingerprint.write_u64(0),
                    Some(paro_parser::ast::PathQuantifier::Plus) => fingerprint.write_u64(1),
                    Some(paro_parser::ast::PathQuantifier::Star) => fingerprint.write_u64(2),
                    Some(paro_parser::ast::PathQuantifier::Bounded { lower, upper }) => {
                        fingerprint.write_u64(3);
                        fingerprint.write_u64(*lower);
                        match upper {
                            None => fingerprint.write_u64(0),
                            Some(upper) => {
                                fingerprint.write_u64(1);
                                fingerprint.write_u64(*upper);
                            }
                        }
                    }
                }
                fingerprint.write_u64(edge.filter.is_some() as u64);
                fingerprint.write_bytes(edge.source_variable.as_bytes());
                fingerprint.write_bytes(edge.destination_variable.as_bytes());
            }
        }
    }
}

fn encode_vertex_table(
    fingerprint: &mut StableFingerprintBuilder,
    table: &paro_catalog::entry::VertexTableInfo,
) {
    fingerprint.write_bytes(table.table_name.as_bytes());
    fingerprint.write_u64(table.table_oid);
    encode_u32s(fingerprint, &table.key_column_ids);
    fingerprint.write_bytes(table.label.as_bytes());
    encode_u32s(fingerprint, &table.property_column_ids);
}

fn encode_edge_table(
    fingerprint: &mut StableFingerprintBuilder,
    table: &paro_catalog::entry::EdgeTableInfo,
) {
    fingerprint.write_bytes(table.table_name.as_bytes());
    fingerprint.write_u64(table.table_oid);
    encode_u32s(fingerprint, &table.key_column_ids);
    encode_u32s(fingerprint, &table.source_key_column_ids);
    fingerprint.write_bytes(table.source_vertex_table.as_bytes());
    encode_u32s(fingerprint, &table.source_ref_column_ids);
    encode_u32s(fingerprint, &table.destination_key_column_ids);
    fingerprint.write_bytes(table.destination_vertex_table.as_bytes());
    encode_u32s(fingerprint, &table.destination_ref_column_ids);
    fingerprint.write_bytes(table.label.as_bytes());
    encode_u32s(fingerprint, &table.property_column_ids);
}

fn encode_column_bindings(fingerprint: &mut StableFingerprintBuilder, bindings: &[ColumnBinding]) {
    fingerprint.write_u64(bindings.len() as u64);
    for binding in bindings {
        fingerprint.write_u64(binding.table_index as u64);
        fingerprint.write_u64(binding.column_index as u64);
    }
}

fn encode_strings(fingerprint: &mut StableFingerprintBuilder, values: &[String]) {
    fingerprint.write_u64(values.len() as u64);
    for value in values {
        fingerprint.write_bytes(value.as_bytes());
    }
}

fn encode_get(fingerprint: &mut StableFingerprintBuilder, get: &paro_planner::operator::Get) {
    // The same catalog object may occur more than once in a query. The bound
    // table index is part of the carrier ABI and distinguishes those aliases.
    fingerprint.write_u64(get.table_index as u64);
    fingerprint.write_u64(
        get.table
            .as_ref()
            .map(|table| table.object_id().raw())
            .unwrap_or(0),
    );
    fingerprint.write_u64(get.column_sources.len() as u64);
    for source in &get.column_sources {
        match source {
            paro_planner::operator::GetColumnSource::Stored { column_id } => {
                fingerprint.write_u64(0);
                fingerprint.write_u64(*column_id as u64);
            }
            paro_planner::operator::GetColumnSource::MatchedUtf8Prefix {
                source_column,
                byte_width,
            } => {
                fingerprint.write_u64(1);
                fingerprint.write_u64(*source_column as u64);
                fingerprint.write_u64(*byte_width as u64);
            }
            paro_planner::operator::GetColumnSource::VirtualRowId => {
                fingerprint.write_u64(2);
            }
        }
    }
    fingerprint.write_u64(get.column_types.len() as u64);
    for ty in &get.column_types {
        super::scalar::encode_logical_type(fingerprint, ty);
    }
    match &get.scan_order {
        None => fingerprint.write_u64(u64::MAX),
        Some(order) => {
            fingerprint.write_u64(order.column_idx as u64);
            fingerprint.write_u64(order.order_by as u64);
            fingerprint.write_u64(order.order_type as u64);
            fingerprint.write_u64(order.column_type as u64);
            encode_optional_usize(fingerprint, order.row_limit);
            fingerprint.write_u64(order.row_offset as u64);
        }
    }
}

fn encode_external_call(
    fingerprint: &mut StableFingerprintBuilder,
    call: &paro_external::routine::bound::BoundRoutineCallMeta,
) {
    use paro_external::routine::boundary::PlacementClass;
    use paro_external::routine::spec::{
        RoutineNullPolicy, RoutineSideEffects, RoutineStability, RowSemantics,
    };

    encode_routine_identity(fingerprint, &call.identity);
    fingerprint.write_u64(match call.boundary.placement {
        PlacementClass::Native => 0,
        PlacementClass::External => 1,
    });
    fingerprint.write_u64(call.boundary.may_block as u64);
    fingerprint.write_u64(match call.boundary.row_semantics {
        RowSemantics::RowPreserving => 0,
        RowSemantics::RelationExpanding => 1,
        RowSemantics::Aggregate => 2,
        RowSemantics::Window => 3,
    });
    fingerprint.write_u64(match call.semantics.stability {
        RoutineStability::Immutable => 0,
        RoutineStability::Stable => 1,
        RoutineStability::Volatile => 2,
    });
    fingerprint.write_u64(match call.semantics.null_policy {
        RoutineNullPolicy::Strict => 0,
        RoutineNullPolicy::CalledOnNullInput => 1,
    });
    fingerprint.write_u64(match call.semantics.side_effects {
        RoutineSideEffects::None => 0,
        RoutineSideEffects::HasSideEffects => 1,
    });
    fingerprint.write_u64(match call.semantics.row_semantics {
        RowSemantics::RowPreserving => 0,
        RowSemantics::RelationExpanding => 1,
        RowSemantics::Aggregate => 2,
        RowSemantics::Window => 3,
    });
    fingerprint.write_u64(call.semantics.may_block as u64);
}

fn encode_dependent_join(
    fingerprint: &mut StableFingerprintBuilder,
    join: &paro_planner::operator::DependentJoin,
) {
    use paro_planner::operator::{DependentJoinKind, MarkSubqueryKind};

    fingerprint.write_u64(join.correlated_columns.len() as u64);
    for correlation in &join.correlated_columns {
        fingerprint.write_u64(correlation.table_index as u64);
        fingerprint.write_u64(correlation.column_index as u64);
        super::scalar::encode_logical_type(fingerprint, &correlation.return_type);
        fingerprint.write_u64(correlation.depth as u64);
    }
    match &join.kind {
        DependentJoinKind::Scalar => fingerprint.write_u64(0),
        DependentJoinKind::Mark {
            mark_index,
            subquery,
        } => {
            fingerprint.write_u64(1);
            fingerprint.write_u64(*mark_index as u64);
            match subquery {
                MarkSubqueryKind::Exists => fingerprint.write_u64(0),
                MarkSubqueryKind::NotExists => fingerprint.write_u64(1),
                MarkSubqueryKind::Any(payload) | MarkSubqueryKind::All(payload) => {
                    fingerprint.write_u64(if matches!(subquery, MarkSubqueryKind::Any(_)) {
                        2
                    } else {
                        3
                    });
                    fingerprint.write_u64(payload.comparison_type as u64);
                    fingerprint.write_u64(payload.child_types.len() as u64);
                    for ty in &payload.child_types {
                        super::scalar::encode_logical_type(fingerprint, ty);
                    }
                    fingerprint.write_u64(payload.child_targets.len() as u64);
                    for ty in &payload.child_targets {
                        super::scalar::encode_logical_type(fingerprint, ty);
                    }
                }
            }
        }
        DependentJoinKind::Lateral {
            join_type,
            join_condition,
        } => {
            fingerprint.write_u64(2);
            fingerprint.write_u64(*join_type as u64);
            fingerprint.write_u64(join_condition.is_some() as u64);
        }
    }
}

fn encode_table_function(
    fingerprint: &mut StableFingerprintBuilder,
    function: &paro_planner::operator::TableFunctionGet,
) {
    fingerprint.write_bytes(function.function.name.as_bytes());
    fingerprint.write_u64(function.function.arguments.len() as u64);
    for ty in &function.function.arguments {
        super::scalar::encode_logical_type(fingerprint, ty);
    }
    fingerprint.write_u64(function.function.projection_pushdown as u64);
    fingerprint.write_u64(function.function.filter_pushdown as u64);
    match &function.function.varargs {
        None => fingerprint.write_u64(0),
        Some(ty) => {
            fingerprint.write_u64(1);
            super::scalar::encode_logical_type(fingerprint, ty);
        }
    }
    fingerprint.write_u64(function.function.named_parameters.len() as u64);
    for (name, ty) in &function.function.named_parameters {
        fingerprint.write_bytes(name.as_bytes());
        super::scalar::encode_logical_type(fingerprint, ty);
    }
    match &function.projection_ids {
        None => fingerprint.write_u64(u64::MAX),
        Some(projection) => encode_usizes(fingerprint, projection),
    }
    fingerprint.write_u64(function.input_table_types.len() as u64);
    for ty in &function.input_table_types {
        super::scalar::encode_logical_type(fingerprint, ty);
    }
    fingerprint.write_u64(function.with_ordinality as u64);
    match &function.bind_data {
        None => fingerprint.write_u64(0),
        Some(bind_data) => {
            fingerprint.write_u64(1);
            encode_optional_usize(fingerprint, bind_data.as_ref().cardinality());
        }
    }
}

fn encode_hnsw_options(
    fingerprint: &mut StableFingerprintBuilder,
    options: paro_storage::index::hnsw::HnswQueryOptions,
) {
    encode_optional_usize(fingerprint, options.ef);
    encode_optional_usize(fingerprint, options.rerank_window);
    fingerprint.write_u64(match options.objective {
        paro_storage::index::hnsw::HnswSearchObjective::CostOptimized => 0,
        paro_storage::index::hnsw::HnswSearchObjective::Exact => 1,
    });
}

fn encode_search_request(
    fingerprint: &mut StableFingerprintBuilder,
    request: &paro_storage::search::NormalizedSearchRequest,
) {
    use paro_storage::search::{DenseVectorQuery, FusionStrategy, SearchIntent, SearchRequestMode};

    fingerprint.write_u64(request.table_id);
    match request.mode {
        SearchRequestMode::TopK { limit } => {
            fingerprint.write_u64(0);
            fingerprint.write_u64(limit as u64);
        }
        SearchRequestMode::Filter => fingerprint.write_u64(1),
    }
    match &request.predicate {
        None => fingerprint.write_u64(0),
        Some(predicate) => {
            fingerprint.write_u64(1);
            encode_predicate_tree(fingerprint, predicate);
        }
    }
    encode_u32s(fingerprint, &request.projections.columns);
    fingerprint.write_u64(request.projections.include_score as u64);
    fingerprint.write_u64(request.intents.len() as u64);
    for intent in &request.intents {
        match intent {
            SearchIntent::Hnsw(intent) => {
                fingerprint.write_u64(0);
                fingerprint.write_u64(intent.column_id as u64);
                match &intent.query {
                    DenseVectorQuery::Literal(values) => {
                        fingerprint.write_u64(0);
                        fingerprint.write_u64(values.len() as u64);
                        for value in values {
                            fingerprint.write_u64(value.to_bits() as u64);
                        }
                    }
                    DenseVectorQuery::RuntimeParameter { slot, dimension } => {
                        fingerprint.write_u64(1);
                        fingerprint.write_u64(slot.index.index() as u64);
                        super::scalar::encode_logical_type(fingerprint, &slot.ty);
                        fingerprint.write_u64(*dimension as u64);
                    }
                }
                fingerprint.write_u64(intent.distance as u64);
                encode_hnsw_options(fingerprint, intent.options);
            }
            SearchIntent::Sparse(intent) => {
                fingerprint.write_u64(1);
                fingerprint.write_u64(intent.column_id as u64);
                encode_u32s(fingerprint, &intent.query_vector.dims);
                fingerprint.write_u64(intent.query_vector.weights.len() as u64);
                for value in &intent.query_vector.weights {
                    fingerprint.write_u64(value.to_bits() as u64);
                }
            }
            SearchIntent::FullText(intent) => {
                fingerprint.write_u64(2);
                fingerprint.write_u64(intent.column_id as u64);
                fingerprint.write_bytes(intent.query.as_bytes());
                fingerprint.write_u64(intent.query_kind as u64);
                fingerprint.write_u64(intent.query_stats.term_count as u64);
                fingerprint.write_u64(intent.query_stats.positive_term_count as u64);
                fingerprint.write_u64(intent.query_stats.phrase_count as u64);
                fingerprint.write_u64(intent.query_stats.proximity_count as u64);
                fingerprint.write_u64(intent.query_stats.prefix_count as u64);
                fingerprint.write_u64(intent.query_stats.not_count as u64);
                fingerprint.write_u64(intent.query_stats.or_branch_count as u64);
                fingerprint.write_bytes(intent.config.as_bytes());
                fingerprint.write_u64(intent.score_mode as u64);
            }
        }
    }
    match &request.fusion {
        None => fingerprint.write_u64(0),
        Some(FusionStrategy::ReciprocalRankFusion {
            window_size,
            rank_constant,
        }) => {
            fingerprint.write_u64(1);
            fingerprint.write_u64(*window_size as u64);
            fingerprint.write_u64(*rank_constant as u64);
        }
        Some(FusionStrategy::WeightedBlend { weights }) => {
            fingerprint.write_u64(2);
            fingerprint.write_u64(weights.len() as u64);
            for weight in weights {
                fingerprint.write_u64(weight.to_bits() as u64);
            }
        }
    }
}

fn encode_predicate_tree(
    fingerprint: &mut StableFingerprintBuilder,
    tree: &paro_storage::index::PredicateTree,
) {
    use paro_storage::index::PredicateTree;

    match tree {
        PredicateTree::Leaf(predicate) => {
            fingerprint.write_u64(0);
            encode_predicate(fingerprint, predicate);
        }
        PredicateTree::And(children) => {
            fingerprint.write_u64(1);
            fingerprint.write_u64(children.len() as u64);
            for child in children {
                encode_predicate_tree(fingerprint, child);
            }
        }
        PredicateTree::Or(children) => {
            fingerprint.write_u64(2);
            fingerprint.write_u64(children.len() as u64);
            for child in children {
                encode_predicate_tree(fingerprint, child);
            }
        }
    }
}

fn encode_predicate(
    fingerprint: &mut StableFingerprintBuilder,
    predicate: &paro_storage::index::Predicate,
) {
    use paro_storage::index::{FixedMembershipWidth, Predicate};

    macro_rules! scalar_predicate {
        ($tag:expr, $column_id:expr, $value:expr) => {{
            fingerprint.write_u64($tag);
            fingerprint.write_u64(u64::from(*$column_id));
            super::scalar_lowering::encode_value(fingerprint, $value);
        }};
    }

    match predicate {
        Predicate::Eq { column_id, value } => scalar_predicate!(0, column_id, value),
        Predicate::NotEq { column_id, value } => scalar_predicate!(1, column_id, value),
        Predicate::Lt { column_id, value } => scalar_predicate!(2, column_id, value),
        Predicate::Le { column_id, value } => scalar_predicate!(3, column_id, value),
        Predicate::Gt { column_id, value } => scalar_predicate!(4, column_id, value),
        Predicate::Ge { column_id, value } => scalar_predicate!(5, column_id, value),
        Predicate::In { column_id, values } => {
            fingerprint.write_u64(6);
            fingerprint.write_u64(u64::from(*column_id));
            fingerprint.write_u64(values.len() as u64);
            for value in values {
                super::scalar_lowering::encode_value(fingerprint, value);
            }
        }
        Predicate::FixedIn { column_id, values } => {
            fingerprint.write_u64(7);
            fingerprint.write_u64(u64::from(*column_id));
            fingerprint.write_u64(values.len() as u64);
            let width = values.visit_canonical_values(|value| {
                fingerprint.write_bytes(&value.to_le_bytes());
            });
            fingerprint.write_u64(match width {
                FixedMembershipWidth::I32 => 0,
                FixedMembershipWidth::I64 => 1,
                FixedMembershipWidth::I128 => 2,
            });
        }
        Predicate::Range {
            column_id,
            lower,
            upper,
        } => {
            fingerprint.write_u64(8);
            fingerprint.write_u64(u64::from(*column_id));
            super::scalar_lowering::encode_value(fingerprint, lower);
            super::scalar_lowering::encode_value(fingerprint, upper);
        }
        Predicate::IsNull { column_id } => {
            fingerprint.write_u64(9);
            fingerprint.write_u64(u64::from(*column_id));
        }
        Predicate::IsNotNull { column_id } => {
            fingerprint.write_u64(10);
            fingerprint.write_u64(u64::from(*column_id));
        }
        Predicate::StringPrefix {
            column_id,
            prefix,
            negated,
        } => {
            fingerprint.write_u64(11);
            fingerprint.write_u64(u64::from(*column_id));
            fingerprint.write_bytes(prefix.as_bytes());
            fingerprint.write_u64(*negated as u64);
        }
        Predicate::StringPrefixIn {
            column_id,
            prefixes,
        } => {
            fingerprint.write_u64(12);
            fingerprint.write_u64(u64::from(*column_id));
            fingerprint.write_u64(prefixes.len() as u64);
            for prefix in prefixes {
                fingerprint.write_bytes(prefix.as_bytes());
            }
        }
        Predicate::StringLike {
            column_id,
            pattern,
            negated,
        } => {
            fingerprint.write_u64(13);
            fingerprint.write_u64(u64::from(*column_id));
            fingerprint.write_bytes(pattern.as_bytes());
            fingerprint.write_u64(*negated as u64);
        }
        Predicate::ColumnComparison {
            left_column_id,
            right_column_id,
            comparison,
        } => {
            fingerprint.write_u64(14);
            fingerprint.write_u64(u64::from(*left_column_id));
            fingerprint.write_u64(u64::from(*right_column_id));
            fingerprint.write_u64(match comparison {
                paro_storage::index::PredicateComparison::Equal => 0,
                paro_storage::index::PredicateComparison::NotEqual => 1,
                paro_storage::index::PredicateComparison::LessThan => 2,
                paro_storage::index::PredicateComparison::LessThanOrEqual => 3,
                paro_storage::index::PredicateComparison::GreaterThan => 4,
                paro_storage::index::PredicateComparison::GreaterThanOrEqual => 5,
            });
        }
    }
}

fn encode_optional_usize(fingerprint: &mut StableFingerprintBuilder, value: Option<usize>) {
    match value {
        None => fingerprint.write_u64(0),
        Some(value) => {
            fingerprint.write_u64(1);
            fingerprint.write_u64(value as u64);
        }
    }
}

fn encode_optional_string(fingerprint: &mut StableFingerprintBuilder, value: Option<&str>) {
    match value {
        None => fingerprint.write_u64(0),
        Some(value) => {
            fingerprint.write_u64(1);
            fingerprint.write_bytes(value.as_bytes());
        }
    }
}

fn encode_usizes(fingerprint: &mut StableFingerprintBuilder, values: &[usize]) {
    fingerprint.write_u64(values.len() as u64);
    for value in values {
        fingerprint.write_u64(*value as u64);
    }
}

fn encode_u32s(fingerprint: &mut StableFingerprintBuilder, values: &[u32]) {
    fingerprint.write_u64(values.len() as u64);
    for value in values {
        fingerprint.write_u64(u64::from(*value));
    }
}

fn encode_projection_map(
    fingerprint: &mut StableFingerprintBuilder,
    projection: &paro_planner::operator::ProjectionMap,
) {
    match projection.as_columns() {
        None => fingerprint.write_u64(u64::MAX),
        Some(columns) => {
            fingerprint.write_u64(columns.len() as u64);
            for column in columns {
                fingerprint.write_u64(*column as u64);
            }
        }
    }
}

fn encode_orders(fingerprint: &mut StableFingerprintBuilder, orders: &[OrderByNode]) {
    fingerprint.write_u64(orders.len() as u64);
    for order in orders {
        fingerprint.write_u64(order.ascending as u64);
        fingerprint.write_u64(order.nulls_first as u64);
    }
}

fn operator_tag(operator: LogicalOperatorType) -> u64 {
    match operator {
        LogicalOperatorType::Get => 0,
        LogicalOperatorType::Filter => 1,
        LogicalOperatorType::Projection => 2,
        LogicalOperatorType::RowFetch => 3,
        LogicalOperatorType::ExternalProject => 4,
        LogicalOperatorType::ExternalTable => 5,
        LogicalOperatorType::Limit => 6,
        LogicalOperatorType::Order => 7,
        LogicalOperatorType::TopN => 8,
        LogicalOperatorType::Alter => 9,
        LogicalOperatorType::CreateTable => 10,
        LogicalOperatorType::CreateRoutine => 11,
        LogicalOperatorType::CreateSequence => 12,
        LogicalOperatorType::CreateSchema => 13,
        LogicalOperatorType::CreateIndex => 14,
        LogicalOperatorType::Drop => 15,
        LogicalOperatorType::Insert => 16,
        LogicalOperatorType::Delete => 17,
        LogicalOperatorType::Update => 18,
        LogicalOperatorType::LogicalCopy => 19,
        LogicalOperatorType::Explain => 20,
        LogicalOperatorType::EmptyResult => 21,
        LogicalOperatorType::Aggregate => 22,
        LogicalOperatorType::ComparisonJoin => 23,
        LogicalOperatorType::AnyJoin => 24,
        LogicalOperatorType::CrossProduct => 25,
        LogicalOperatorType::DelimGet => 26,
        LogicalOperatorType::DependentJoin => 27,
        LogicalOperatorType::LogicalUnion => 28,
        LogicalOperatorType::LogicalIntersect => 29,
        LogicalOperatorType::LogicalExcept => 30,
        LogicalOperatorType::Distinct => 31,
        LogicalOperatorType::Window => 32,
        LogicalOperatorType::MaterializedCTE => 33,
        LogicalOperatorType::RecursiveCTE => 34,
        LogicalOperatorType::CTERef => 35,
        LogicalOperatorType::TableFunctionGet => 36,
        LogicalOperatorType::SearchScan => 37,
        LogicalOperatorType::FullTextFilterScan => 38,
        LogicalOperatorType::CreateView => 39,
        LogicalOperatorType::CreatePropertyGraph => 40,
        LogicalOperatorType::DropPropertyGraph => 41,
        LogicalOperatorType::RefreshPropertyGraph => 42,
        LogicalOperatorType::GraphMatch => 43,
        LogicalOperatorType::GraphScan => 44,
        LogicalOperatorType::GraphExpand => 45,
    }
}

fn planner_operator_cost(plan: &LogicalPlan, child_count: usize) -> Result<SearchCost> {
    match &plan.operator {
        LogicalOperator::SearchScan(scan) => return search_decision_cost(&scan.decision),
        LogicalOperator::FullTextFilterScan(scan) => return search_decision_cost(&scan.decision),
        LogicalOperator::ExternalProject(project) => {
            return external_operator_cost(project.cost, plan.stats.estimated_cardinality)
        }
        LogicalOperator::ExternalTable(table) => {
            return external_operator_cost(table.cost, plan.stats.estimated_cardinality)
        }
        _ => {}
    }
    let expected_rows = plan
        .stats
        .estimated_cardinality
        .map(|cardinality| cardinality.expected as f64)
        .unwrap_or(1.0)
        .max(1.0);
    let expected = expected_rows + child_count as f64;
    let upper = plan
        .stats
        .estimated_cardinality
        .map(|cardinality| cardinality.max as f64)
        .unwrap_or(expected * 4.0)
        .max(expected);
    let mut cost = SearchCost {
        score: ScoreSummary {
            range: CompactRange::new(1.0, expected, upper)?,
            risk_adjusted: expected + (upper - expected) * 0.5,
        },
        critical_path: CompactRange::new(1.0, expected, upper)?,
        ..SearchCost::ZERO
    };
    if planner_grant_dependency(&plan.operator) == GrantDependencyDescriptor::Sensitive {
        // Retained memory belongs to operator state, not to the number of rows
        // the operator happens to emit.  A cross product materializes only its
        // right build input; charging the Cartesian output here can reject a
        // tiny build side by many orders of magnitude under a hard grant.
        let resident_plan = match &plan.operator {
            LogicalOperator::Join(Join::Cross(cross)) => cross.right.as_ref(),
            _ => plan,
        };
        let row_width = resident_plan
            .types()
            .iter()
            .map(|logical_type| logical_type.type_size().max(1) as u64)
            .sum::<u64>()
            .saturating_add(32);
        let resident_rows = resident_plan
            .stats
            .estimated_cardinality
            .map(|cardinality| cardinality.max.max(cardinality.expected))
            .unwrap_or(1);
        cost.peak_memory_upper = resident_rows.saturating_mul(row_width);
        let resident_expected_rows = resident_plan
            .stats
            .estimated_cardinality
            .map(|cardinality| cardinality.expected as f64)
            .unwrap_or(1.0);
        cost.resources_expected[ResourceDimension::MemoryWrite as usize] =
            resident_expected_rows * row_width as f64;
        cost.resources_risk_upper[ResourceDimension::MemoryWrite as usize] =
            cost.peak_memory_upper as f64;
    }
    cost.validate()?;
    Ok(cost)
}

fn search_decision_cost(decision: &paro_planner::operator::SearchDecision) -> Result<SearchCost> {
    fn candidate_score(candidate: &paro_planner::operator::SearchCandidate) -> Option<f64> {
        candidate
            .estimated_cost()
            .map(|estimate| estimate.score)
            .filter(|score| score.is_finite() && *score >= 0.0)
    }

    let (expected, upper) = match decision {
        paro_planner::operator::SearchDecision::IndexScan { candidate, .. } => {
            let expected = candidate_score(candidate).unwrap_or(1.0).max(1.0);
            (expected, expected * 2.0)
        }
        paro_planner::operator::SearchDecision::Adaptive {
            candidates,
            sequential,
        } => {
            let index = candidates
                .iter()
                .filter_map(candidate_score)
                .min_by(f64::total_cmp)
                .unwrap_or(1.0)
                .max(1.0);
            let sequential = sequential
                .estimated_cost
                .map(|estimate| estimate.score)
                .filter(|score| score.is_finite() && *score >= 0.0)
                .unwrap_or(index)
                .max(1.0);
            // Observation has a bounded cost; the upper envelope must retain
            // the slower arm because admission cannot assume which one wins.
            (
                index.min(sequential) + 1.0,
                index.max(sequential) * 2.0 + 1.0,
            )
        }
    };
    let lower = (expected * 0.5).min(expected);
    let range = CompactRange::new(lower, expected, upper.max(expected))?;
    let mut cost = SearchCost {
        score: ScoreSummary {
            range,
            risk_adjusted: expected + (range.upper - expected) * 0.5,
        },
        critical_path: range,
        ..SearchCost::ZERO
    };
    cost.resources_expected[ResourceDimension::RandomIo as usize] = expected;
    cost.resources_risk_upper[ResourceDimension::RandomIo as usize] = range.upper;
    cost.validate()?;
    Ok(cost)
}

fn external_operator_cost(
    estimate: paro_planner::operator::external_project::ExternalCostEstimate,
    cardinality: Option<paro_planner::plan::CardinalityEstimate>,
) -> Result<SearchCost> {
    let rows = cardinality.unwrap_or(paro_planner::plan::CardinalityEstimate {
        min: 0,
        expected: 100,
        max: 1_000_000,
    });
    let lower = estimate.startup_cost
        + estimate.per_row_cost * rows.min as f64
        + estimate.bytes_cost * rows.min as f64;
    let expected = estimate.startup_cost
        + estimate.per_row_cost * rows.expected as f64
        + estimate.bytes_cost * rows.expected as f64
        + estimate.queue_risk;
    let upper = estimate.startup_cost
        + estimate.per_row_cost * rows.max as f64
        + estimate.bytes_cost * rows.max as f64
        + estimate.queue_risk * 4.0;
    let range = CompactRange::new(lower.min(expected), expected, upper.max(expected))?;
    let mut cost = SearchCost {
        score: ScoreSummary {
            range,
            risk_adjusted: expected + (range.upper - expected) * 0.5,
        },
        critical_path: range,
        external_workers: super::ids::ExternalWorkerRequirementSetId(1),
        external_worker_slots_upper: 1,
        ..SearchCost::ZERO
    };
    cost.resources_expected[ResourceDimension::Cpu as usize] =
        estimate.per_row_cost * rows.expected as f64;
    cost.resources_risk_upper[ResourceDimension::Cpu as usize] =
        estimate.per_row_cost * rows.max as f64;
    cost.resources_expected[ResourceDimension::Network as usize] =
        estimate.bytes_cost * rows.expected as f64;
    cost.resources_risk_upper[ResourceDimension::Network as usize] =
        estimate.bytes_cost * rows.max as f64;
    cost.validate()?;
    Ok(cost)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_catalog::entry::{
        CatalogObjectId, ColumnDefinition, TableCatalogEntry, VertexTableInfo,
    };
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_function::aggregate::distributive::count::get_count_star_function;
    use paro_planner::expression::{
        AggregateExpression, ConstantExpression, Expression, ReferenceExpression, WindowExpression,
        WindowFrame,
    };
    use paro_planner::operator::join::{Join, JoinCondition, JoinType};
    use paro_planner::operator::{
        CTERef, ComparisonJoin, EmptyResult, ExpressionGet, Filter, Get, GraphScan, Projection,
        TopN, Window as LogicalWindow,
    };
    use paro_planner::plan::CardinalityEstimate;
    use paro_storage::table::table_factory::TableFactory;

    use super::*;

    fn test_grant_classes() -> [ResourceGrantClass; 1] {
        [ResourceGrantClass {
            id: super::super::ids::ResourceGrantClassId(0),
            hard_memory_bytes: u64::MAX,
            spill_policy: crate::physical::SpillPolicy::Allowed,
            concurrency_class: 0,
        }]
    }

    #[test]
    fn cross_product_memory_tracks_only_the_materialized_build_side() {
        let mut left = LogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            Vec::new(),
            vec!["left".to_string()],
            vec![LogicalType::BigInt],
        )));
        left.stats.estimated_cardinality = Some(CardinalityEstimate::exact(1_000_000_000));
        let mut right = LogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            Vec::new(),
            vec!["right".to_string()],
            vec![LogicalType::BigInt],
        )));
        right.stats.estimated_cardinality = Some(CardinalityEstimate::exact(3));
        let mut product = LogicalPlan::synthetic(LogicalOperator::Join(Join::cross(left, right)));
        product.stats.estimated_cardinality = Some(CardinalityEstimate::exact(3_000_000_000));

        let cost = planner_operator_cost(&product, 2).expect("cross-product cost");

        assert_eq!(cost.peak_memory_upper, 3 * (8 + 32));
    }

    #[test]
    fn graph_relation_identity_is_part_of_the_query_ir_fingerprint() {
        let scan = |label: &str, table_oid: u64| {
            LogicalPlan::synthetic(LogicalOperator::GraphScan(GraphScan::new(
                VertexTableInfo {
                    table_name: label.to_ascii_lowercase(),
                    table_oid,
                    key_column_ids: vec![0],
                    label: label.to_string(),
                    property_column_ids: vec![1],
                },
                None,
                1,
                2,
                label.to_string(),
                "g".to_string(),
                "public".to_string(),
            )))
        };
        let person = scan("Person", 11);
        let company = scan("Company", 12);
        let scalars = ScalarArena::default();
        let person = query_operator_fingerprint(&person, &[], &scalars).unwrap();
        let company = query_operator_fingerprint(&company, &[], &scalars).unwrap();
        assert_ne!(person, company);
    }

    #[test]
    fn graph_variable_identity_is_part_of_the_query_ir_fingerprint() {
        let scan = |table_index: usize| {
            LogicalPlan::synthetic(LogicalOperator::GraphScan(GraphScan::new(
                VertexTableInfo {
                    table_name: "person".to_string(),
                    table_oid: 11,
                    key_column_ids: vec![0],
                    label: "Person".to_string(),
                    property_column_ids: vec![1],
                },
                None,
                table_index,
                100,
                "Person".to_string(),
                "g".to_string(),
                "public".to_string(),
            )))
        };
        let scalars = ScalarArena::default();
        let first = query_operator_fingerprint(&scan(7), &[], &scalars).unwrap();
        let second = query_operator_fingerprint(&scan(8), &[], &scalars).unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn graph_filter_is_part_of_the_query_ir_fingerprint() {
        let scan = |value: bool| {
            LogicalPlan::synthetic(LogicalOperator::GraphScan(GraphScan::new(
                VertexTableInfo {
                    table_name: "person".to_string(),
                    table_oid: 11,
                    key_column_ids: vec![0],
                    label: "Person".to_string(),
                    property_column_ids: vec![1],
                },
                Some(Expression::Constant(ConstantExpression {
                    value: Value::Boolean(value),
                    return_type: LogicalType::Boolean,
                })),
                1,
                2,
                "Person".to_string(),
                "g".to_string(),
                "public".to_string(),
            )))
        };
        let fingerprint = |mut plan: LogicalPlan| {
            let mut binding_ids = BTreeMap::new();
            let mut columns = ColumnCatalog::default();
            let mut scalars = ScalarArena::default();
            let roots = intern_operator_scalars(
                &mut plan.operator,
                &[],
                &[],
                &mut binding_ids,
                &mut columns,
                &mut scalars,
            )
            .unwrap();
            query_operator_fingerprint(&plan, &roots, &scalars).unwrap()
        };

        assert_ne!(fingerprint(scan(true)), fingerprint(scan(false)));
    }

    #[test]
    fn cte_owner_is_part_of_the_query_ir_fingerprint() {
        let reference = |cte_index| {
            LogicalPlan::synthetic(LogicalOperator::CTERef(CTERef::new(
                cte_index,
                7,
                "shared".to_string(),
                vec!["v".to_string()],
                vec![LogicalType::Integer],
            )))
        };
        let scalars = ScalarArena::default();
        let first = query_operator_fingerprint(&reference(1), &[], &scalars).unwrap();
        let second = query_operator_fingerprint(&reference(2), &[], &scalars).unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn memo_round_trip_preserves_tree_shape_without_positional_repair() {
        let bind_context = BindContext::new();
        let leaf = LogicalPlan::dummy_scan(&bind_context);
        let wrapped = LogicalPlan::new(
            &bind_context,
            LogicalOperator::EmptyResult(EmptyResult::new(leaf)),
        );
        let input = MemoBuilder::build(wrapped, bind_context, SearchBudget::default()).unwrap();
        let optimized = input.optimize(&test_grant_classes()).unwrap();
        let optimized = &optimized.variants[0];
        assert!(matches!(
            optimized.plan.operator,
            LogicalOperator::EmptyResult(_)
        ));
        assert!(matches!(
            optimized.plan.children()[0].operator,
            LogicalOperator::DummyScan
        ));
    }

    #[test]
    fn certified_transformation_alternative_names_its_baseline_source() {
        let bind_context = BindContext::new();
        let baseline = constant_projection(&bind_context, 7);
        let alternative =
            duplicate_plan_preserving_indices(&baseline, bind_context.shared().as_ref());
        let rule = super::super::ids::RuleId(42_001);
        let input = MemoBuilder::build_alternatives(
            vec![
                LogicalAlternative {
                    plan: baseline,
                    source: AlternativeOrigin::Baseline,
                },
                LogicalAlternative {
                    plan: alternative,
                    source: AlternativeOrigin::Transformation { rule },
                },
            ],
            bind_context,
            SearchBudget::default(),
        )
        .unwrap();
        assert_eq!(input.transformations.len(), 1);
        let optimized = input.optimize(&test_grant_classes()).unwrap();
        assert_eq!(optimized.rule_firings.get(&rule), Some(&1));
    }

    #[test]
    fn memo_winner_names_the_hash_join_implementation() {
        let bind_context = BindContext::new();
        let left = LogicalPlan::new(
            &bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                0,
                vec![],
                vec!["left".to_string()],
                vec![LogicalType::Integer],
            )),
        );
        let right = LogicalPlan::new(
            &bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                1,
                vec![],
                vec!["right".to_string()],
                vec![LogicalType::Integer],
            )),
        );
        let condition = JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        );
        let join = LogicalPlan::new(
            &bind_context,
            LogicalOperator::Join(Join::comparison(
                JoinType::Inner,
                left,
                right,
                vec![condition],
            )),
        );
        let input = MemoBuilder::build(join, bind_context, SearchBudget::default()).unwrap();
        let optimized = input.optimize(&test_grant_classes()).unwrap();
        let optimized = &optimized.variants[0];
        let contract = optimized
            .contracts
            .get(&optimized.plan.id)
            .expect("root winner contract");
        assert_eq!(
            contract.implementation,
            PhysicalImplementationFlavor::HashJoin
        );
    }

    #[test]
    fn memo_window_winner_is_the_node_lowered_by_the_physical_extractor() {
        let bind_context = BindContext::new();
        let mut values = LogicalPlan::new(
            &bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                0,
                vec![],
                vec!["grp".to_string(), "value".to_string()],
                vec![LogicalType::Integer, LogicalType::Integer],
            )),
        );
        values.stats.estimated_cardinality = Some(CardinalityEstimate::exact(1_024));
        let aggregate =
            AggregateExpression::new(get_count_star_function(), Vec::new(), LogicalType::BigInt);
        let mut plan = LogicalPlan::new(
            &bind_context,
            LogicalOperator::Window(LogicalWindow::new(
                1,
                vec![WindowExpression::aggregate(
                    aggregate,
                    vec![Expression::Reference(ReferenceExpression::new(
                        0,
                        LogicalType::Integer,
                    ))],
                    Vec::new(),
                    WindowFrame::default(),
                )],
                values,
            )),
        );
        plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(1_024));

        let input = MemoBuilder::build(plan, bind_context, SearchBudget::default()).unwrap();
        let optimized = input.optimize(&test_grant_classes()).unwrap();
        let optimized = optimized.variants.into_vec().remove(0);
        let contract = optimized.contracts.get(&optimized.plan.id).unwrap();
        assert_eq!(
            contract.implementation,
            PhysicalImplementationFlavor::PartitionAggregateWindow
        );

        let physical = crate::physical::PhysicalPlanExtractor::new(
            crate::physical::ExtractionContext::default(),
        )
        .with_winner_contracts(optimized.contracts)
        .with_enforcer_contracts(optimized.enforcers)
        .requiring_winner_contracts()
        .extract(&optimized.plan)
        .unwrap();
        assert!(matches!(
            physical.node(physical.root).kind,
            crate::physical::PhysicalNodeKind::PartitionAggregateWindow(_)
        ));
    }

    fn test_base_get(table_index: usize, oid: u64, name: &str) -> LogicalPlan {
        let storage = Arc::new(
            TableFactory::default()
                .create_table(&[LogicalType::Integer])
                .expect("table storage"),
        );
        let table = Arc::new(TableCatalogEntry::new(
            "paro".to_string(),
            "public".to_string(),
            name.to_string(),
            vec![ColumnDefinition::new(
                "id".to_string(),
                LogicalType::Integer,
            )],
            storage,
            CatalogObjectId::from_raw(oid),
            0,
        ));
        LogicalPlan::synthetic(LogicalOperator::Get(Get::new(
            table_index,
            vec!["id".to_string()],
            vec![LogicalType::Integer],
            table,
        )))
    }

    #[test]
    fn direct_rowset_reference_admits_and_selects_runtime_filter_region() {
        let mut left = test_base_get(0, 20_001, "probe");
        left.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20_000));
        let mut right = test_base_get(1, 20_002, "build");
        right.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));
        let condition = JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        );
        let join = ComparisonJoin::new(JoinType::Inner, left, right, vec![condition]);
        assert!(supports_runtime_filter_auxiliary(&join, true));
        let mut plan = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(join)));
        plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));

        let input = MemoBuilder::build(plan, BindContext::new(), SearchBudget::default())
            .expect("build memo");
        let region = input
            .memo
            .regions()
            .nodes
            .iter()
            .find(|region| {
                region
                    .facets
                    .iter()
                    .any(|facet| facet.kind == RegionFacetKind::RuntimeFilter)
            })
            .expect("runtime-filter AuxiliaryPlanRegion");
        let artifact = region
            .facets
            .iter()
            .find(|facet| facet.kind == RegionFacetKind::RuntimeFilter)
            .unwrap()
            .fingerprint;
        let optimized = input.optimize(&test_grant_classes()).expect("optimize");
        let variant = &optimized.variants[0];
        let contract = variant
            .contracts
            .get(&variant.plan.id)
            .expect("root winner contract");
        assert_eq!(
            contract.implementation,
            PhysicalImplementationFlavor::HashJoinRuntimeFilter
        );
        assert_eq!(contract.owned_artifacts.len(), 1);
        assert_eq!(contract.owned_artifacts[0].fingerprint, artifact);
        assert!(contract.region_owner.is_some());
        assert!(matches!(
            contract.origin,
            crate::physical::PlanOrigin::SpecializedRegion(_)
        ));
    }

    #[test]
    fn oversized_optional_runtime_filter_facet_yields_to_the_baseline() {
        let mut left = test_base_get(0, 20_011, "probe");
        left.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20_000));
        let mut right = test_base_get(1, 20_012, "build");
        right.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));
        let mut plan = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(
                JoinType::Inner,
                left,
                right,
                vec![JoinCondition::equality(
                    Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                    Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                )],
            ),
        )));
        plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));
        let mut budget = SearchBudget::default();
        budget.max_composite_region_groups = 2;

        let input = MemoBuilder::build(plan, BindContext::new(), budget).unwrap();
        assert!(input.memo.regions().nodes.is_empty());
        assert_eq!(input.memo.regions().dropped_optional_facets.len(), 1);
        let optimized = input.optimize(&test_grant_classes()).unwrap();
        let variant = &optimized.variants[0];
        let contract = variant.contracts.get(&variant.plan.id).unwrap();
        assert_eq!(
            contract.implementation,
            PhysicalImplementationFlavor::HashJoin
        );
        assert!(contract.owned_artifacts.is_empty());
        assert_eq!(contract.region_owner, None);
    }

    #[test]
    fn fully_pushable_filter_probe_requires_the_pushdown_compile_capability() {
        let left = LogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
            test_base_get(0, 20_003, "filtered_probe"),
            Vec::new(),
        )));
        let right = test_base_get(1, 20_004, "build");
        let join = ComparisonJoin::new(
            JoinType::Inner,
            left,
            right,
            vec![JoinCondition::equality(
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            )],
        );

        assert!(supports_runtime_filter_auxiliary(&join, true));
        assert!(!supports_runtime_filter_auxiliary(&join, false));
    }

    #[test]
    fn global_sort_enforcer_is_extracted_as_an_executable_plan_node() {
        let bind_context = BindContext::new();
        let mut input = MemoBuilder::build(
            constant_projection(&bind_context, 7),
            bind_context,
            SearchBudget::default(),
        )
        .unwrap();
        let column = input.presentation.columns[0];
        let mut required = input
            .memo
            .required(input.root_goal.required)
            .cloned()
            .unwrap();
        required.ordering = OrderingRequirement::Ordered(RequiredOrdering {
            keys: vec![OrderingKey {
                column,
                direction: SortDirection::Asc,
                nulls: NullOrder::Last,
                collation: None,
            }]
            .into_boxed_slice(),
            scope: OrderingScope::Global,
        });
        input.root_goal.required = input.memo.intern_required(required).unwrap();

        let optimized = input.optimize(&test_grant_classes()).unwrap();
        let optimized = optimized.variants.into_vec().remove(0);
        assert!(matches!(
            optimized
                .enforcers
                .get(&optimized.plan.id)
                .and_then(|chain| chain.last())
                .unwrap()
                .contract
                .origin,
            crate::physical::PlanOrigin::Enforcer(_)
        ));
        let physical = crate::physical::PhysicalPlanExtractor::new(
            crate::physical::ExtractionContext::default(),
        )
        .with_winner_contracts(optimized.contracts)
        .with_enforcer_contracts(optimized.enforcers)
        .extract(&optimized.plan)
        .unwrap();
        assert!(matches!(
            physical.node(physical.root).kind,
            crate::physical::PhysicalNodeKind::Sort(_)
        ));
    }

    fn constant_projection(bind_context: &BindContext, value: i32) -> LogicalPlan {
        LogicalPlan::new(
            bind_context,
            LogicalOperator::Projection(Projection::new(
                9,
                LogicalPlan::dummy_scan(bind_context),
                vec![Expression::Constant(ConstantExpression::new(
                    Value::Integer(value),
                    LogicalType::Integer,
                ))],
            )),
        )
    }

    #[test]
    fn fixed_membership_fingerprint_uses_set_semantics() {
        use paro_storage::index::{FixedMembership, FixedMembershipBuildPolicy, Predicate};

        let fingerprint = |values| {
            let mut fingerprint = StableFingerprintBuilder::default();
            encode_predicate(
                &mut fingerprint,
                &Predicate::FixedIn {
                    column_id: 7,
                    values,
                },
            );
            fingerprint.finish()
        };
        let dense = FixedMembership::i32_with_policy(
            vec![15, 10, 12, 12],
            FixedMembershipBuildPolicy::new(512, 256),
        );
        let sorted = FixedMembership::i32_with_policy(
            vec![12, 15, 10],
            FixedMembershipBuildPolicy::new(0, 0),
        );
        let wider = FixedMembership::i64(vec![10, 12, 15]);

        assert_eq!(fingerprint(dense), fingerprint(sorted));
        assert_ne!(
            fingerprint(FixedMembership::i32(vec![10, 12, 15])),
            fingerprint(wider)
        );
    }

    #[test]
    fn query_ir_identity_uses_scalar_semantics_not_planner_node_id() {
        let bind_context = BindContext::new();
        let first = MemoBuilder::build(
            constant_projection(&bind_context, 7),
            bind_context.clone(),
            SearchBudget::default(),
        )
        .unwrap();
        let second = MemoBuilder::build(
            constant_projection(&bind_context, 7),
            bind_context.clone(),
            SearchBudget::default(),
        )
        .unwrap();
        let different = MemoBuilder::build(
            constant_projection(&bind_context, 8),
            bind_context,
            SearchBudget::default(),
        )
        .unwrap();

        let key = |input: &OptimizationInput| {
            let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
            input.memo.logical_expr(expression).unwrap().key.clone()
        };
        let first_key = key(&first);
        let second_key = key(&second);
        let different_key = key(&different);
        assert_eq!(first_key.operator, second_key.operator);
        assert_eq!(first_key.scalars, second_key.scalars);
        assert_ne!(first_key.operator, different_key.operator);
        assert_eq!(first.scalars.len(), 1);
    }

    #[test]
    fn exact_is_the_default_root_contract_and_approximate_requires_opt_in() {
        let bind_context = BindContext::new();
        let exact = LogicalPlan::new(
            &bind_context,
            LogicalOperator::TopN(TopN::new(
                LogicalPlan::dummy_scan(&bind_context),
                Vec::new(),
                1,
                0,
            )),
        );
        assert_eq!(required_result_guarantee(&exact), ResultGuarantee::Exact);

        let approximate = LogicalPlan::new(
            &bind_context,
            LogicalOperator::TopN(
                TopN::new(LogicalPlan::dummy_scan(&bind_context), Vec::new(), 1, 0)
                    .with_hnsw_options(paro_storage::index::hnsw::HnswQueryOptions {
                        objective: paro_storage::index::hnsw::HnswSearchObjective::CostOptimized,
                        ..Default::default()
                    }),
            ),
        );
        assert_eq!(
            required_result_guarantee(&approximate),
            ResultGuarantee::ApproximateAllowed(COST_OPTIMIZED_SEARCH_POLICY)
        );
    }
}
