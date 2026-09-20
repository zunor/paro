// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Long-term optimizer entry point: semantic normalization, bounded search,
//! verified extraction. It has no pass-disable compatibility surface.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::physical::requirements::{
    ProvidedMaterialization, ProvidedMutationSafety, ProvidedOrdering, ProvidedPartitioning,
    ProvidedReplayability, ProvidedRepresentation, ResultGuarantee,
};
use crate::physical::{
    Fingerprint, PhysicalGrantContract, PhysicalPlanPortfolio, PlanOrigin, ProvidedProperties,
    RequiredProperties, ResourceGrantClass, ResourceGrantClassId, SpillPolicy,
    StableFingerprintBuilder,
};
use paro_catalog::entry::CatalogEntry;
use paro_common::error::{self as paro_error, Result};
use paro_common::identity::GraphId;
use paro_common::logging::targets;
use paro_context::StatementContext;
use paro_planner::binder::deep_copy::{
    duplicate_plan_preserving_indices, fork_plan_preserving_indices,
};
use paro_planner::binder::Binder;
use paro_planner::operator::{Join, JoinType, LogicalOperator, LogicalOperatorType};
use paro_planner::plan::OwnedLogicalPlan;
use paro_planner::verify::verify_physical_planner_invariants;
use paro_storage::statistics::ColumnStatistics;
use tracing::debug;

use crate::aggregate::common::CommonAggregateOptimizer;
use crate::aggregate::{distinct_decomposition, late_payload, singleton_groups};
use crate::cascades::{
    AlternativeOrigin, CompactRange, LocalOperatorWork, LogicalAlternative,
    MachineCalibrationBundle, MemoBuilder, OpClassId, PricedIncumbent, SearchBudget, SearchCost,
    GRAPH_REGION_ENUMERATOR_RULE,
};
use crate::column::lifetime::ColumnLifetimeAnalyzer;
use crate::column::remove_unused::RemoveUnusedColumns;
use crate::context::OptimizationContext;
use crate::cte::inlining::CTEInlining;
use crate::cte::iteration::normalize_iteration_ownership;
use crate::expression::in_clause::InClauseRewriter;
use crate::expression::normalize_scalar_expressions;
use crate::external::lowering::ExternalRoutineLoweringPass;
use crate::filter::pullup::FilterPullup;
use crate::filter::pushdown::FilterPushdown;
use crate::graph::frontier::GraphFrontierEnumerator;
use crate::graph::match_decompose::GraphMatchDecompose;
use crate::graph::predicate_pushdown::GraphPredicatePushdown;
use crate::join::mixed_predicates::JoinPredicateNormalizer;
use crate::join_order::optimizer::JoinOrderOptimizer;
use crate::limit::topn::TopNOptimizer;
use crate::physical::{
    ExtractionContext, PhysicalImplementationFlavor, PhysicalPlanExtractor, WinnerPhysicalContract,
};
use crate::profiler::{publish_optimizer_profile_snapshot, OptimizerComponent};
use crate::statement::{ExplainEnvelope, QueryStatementLayer, StatementBody, StatementPlan};
use crate::statistics::gathering::StatisticsGathering;
use crate::statistics::propagator::StatisticsPropagator;
use crate::subquery::delim_join_elimination::DelimJoinElimination;
use crate::subquery::empty_result::EmptyResultPullup;
use crate::subquery::partition_aggregate::CorrelatedPartitionAggregate;
use crate::subquery::{scalar_aggregate_fusion, scalar_aggregate_window};
use crate::verify::verify_logical_plan;

const CORRELATED_AGGREGATE_REGION_RULE: crate::cascades::RuleId = crate::cascades::RuleId(10_004);
const SCALAR_REUSE_REGION_RULE: crate::cascades::RuleId = crate::cascades::RuleId(10_005);
const DISTINCT_AGGREGATE_FEASIBILITY_RULE: crate::cascades::RuleId =
    crate::cascades::RuleId(10_020);
const CORRELATED_TOPN_PAYLOAD_REGION_RULE: crate::cascades::RuleId =
    crate::cascades::RuleId(10_022);
const STRONG_INCUMBENT_SEED_RULE: crate::cascades::RuleId = crate::cascades::RuleId(u32::MAX - 1);

struct CandidatePlan {
    plan: OwnedLogicalPlan,
    column_stats: Arc<HashMap<paro_planner::operator::ColumnBinding, Arc<ColumnStatistics>>>,
}

impl CandidatePlan {
    fn into_alternative(self, source: AlternativeOrigin) -> LogicalAlternative {
        LogicalAlternative {
            plan: self.plan,
            source,
            column_stats: self.column_stats,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct PlanShapeEvidence {
    nodes: u64,
    gets: u64,
    filters: u64,
    aggregates: u64,
    joins: u64,
    row_fetches: u64,
    materialized_ctes: u64,
    recursive_ctes: u64,
    cte_refs: u64,
    max_output_width: u64,
}

fn plan_shape_evidence(plan: &OwnedLogicalPlan) -> PlanShapeEvidence {
    let mut evidence = PlanShapeEvidence::default();
    let mut pending = vec![plan];
    while let Some(node) = pending.pop() {
        evidence.nodes = evidence.nodes.saturating_add(1);
        evidence.max_output_width = evidence
            .max_output_width
            .max(node.output_layout().len() as u64);
        match node.operator.op_type() {
            LogicalOperatorType::Get
            | LogicalOperatorType::TableFunctionGet
            | LogicalOperatorType::SearchScan
            | LogicalOperatorType::FullTextFilterScan
            | LogicalOperatorType::GraphScan => {
                evidence.gets = evidence.gets.saturating_add(1);
            }
            LogicalOperatorType::Filter => {
                evidence.filters = evidence.filters.saturating_add(1);
            }
            LogicalOperatorType::Aggregate => {
                evidence.aggregates = evidence.aggregates.saturating_add(1);
            }
            LogicalOperatorType::ComparisonJoin
            | LogicalOperatorType::AnyJoin
            | LogicalOperatorType::CrossProduct
            | LogicalOperatorType::DependentJoin => {
                evidence.joins = evidence.joins.saturating_add(1);
            }
            LogicalOperatorType::RowFetch => {
                evidence.row_fetches = evidence.row_fetches.saturating_add(1);
            }
            LogicalOperatorType::MaterializedCTE => {
                evidence.materialized_ctes = evidence.materialized_ctes.saturating_add(1);
            }
            LogicalOperatorType::RecursiveCTE => {
                evidence.recursive_ctes = evidence.recursive_ctes.saturating_add(1);
            }
            LogicalOperatorType::CTERef => {
                evidence.cte_refs = evidence.cte_refs.saturating_add(1);
            }
            _ => {}
        }
        pending.extend(node.children());
    }
    evidence
}

fn record_plan_shape(
    trace: &paro_context::StatementTrace,
    prefix: &str,
    evidence: PlanShapeEvidence,
) {
    for (name, value) in [
        ("nodes", evidence.nodes),
        ("gets", evidence.gets),
        ("filters", evidence.filters),
        ("aggregates", evidence.aggregates),
        ("joins", evidence.joins),
        ("row_fetches", evidence.row_fetches),
        ("materialized_ctes", evidence.materialized_ctes),
        ("recursive_ctes", evidence.recursive_ctes),
        ("cte_refs", evidence.cte_refs),
        ("max_output_width", evidence.max_output_width),
    ] {
        trace.record_value("optimizer", &format!("{prefix}.shape.{name}"), value);
    }
}

fn record_cost_evidence(trace: &paro_context::StatementTrace, prefix: &str, cost: SearchCost) {
    for (name, value) in [
        ("score_lower_bits", cost.score.range.lower.to_bits()),
        ("score_expected_bits", cost.score.range.expected.to_bits()),
        ("score_upper_bits", cost.score.range.upper.to_bits()),
        (
            "score_risk_adjusted_bits",
            cost.score.risk_adjusted.to_bits(),
        ),
        ("work_lower_bits", cost.work_latency.lower.to_bits()),
        ("work_expected_bits", cost.work_latency.expected.to_bits()),
        ("work_upper_bits", cost.work_latency.upper.to_bits()),
        (
            "critical_path_lower_bits",
            cost.critical_path.lower.to_bits(),
        ),
        (
            "critical_path_expected_bits",
            cost.critical_path.expected.to_bits(),
        ),
        (
            "critical_path_upper_bits",
            cost.critical_path.upper.to_bits(),
        ),
        (
            "non_revocable_memory_upper",
            cost.non_revocable_memory_upper,
        ),
        ("minimum_memory_bytes", cost.minimum_memory_bytes),
        ("revocable_memory_target", cost.revocable_memory_target),
        ("peak_memory_upper", cost.peak_memory_upper),
        ("spill_bytes_expected", cost.spill_bytes_expected),
        ("max_parallel_tasks", u64::from(cost.max_parallel_tasks)),
        (
            "output_pipeline_tasks",
            u64::from(cost.output_pipeline_tasks),
        ),
        ("external_workers", u64::from(cost.external_workers.0)),
        (
            "external_worker_slots_upper",
            u64::from(cost.external_worker_slots_upper),
        ),
    ] {
        trace.record_value("optimizer", &format!("{prefix}.cost.{name}"), value);
    }
    for (index, value) in cost.resources_expected.iter().enumerate() {
        trace.record_value(
            "optimizer",
            &format!("{prefix}.cost.resources_expected_{index}_bits"),
            value.to_bits(),
        );
    }
    for (index, value) in cost.resources_risk_upper.iter().enumerate() {
        trace.record_value(
            "optimizer",
            &format!("{prefix}.cost.resources_risk_upper_{index}_bits"),
            value.to_bits(),
        );
    }
}

fn record_fingerprint(
    trace: &paro_context::StatementTrace,
    prefix: &str,
    fingerprint: Fingerprint,
) {
    trace.record_value(
        "optimizer",
        &format!("{prefix}.fingerprint_lo"),
        fingerprint.0 as u64,
    );
    trace.record_value(
        "optimizer",
        &format!("{prefix}.fingerprint_hi"),
        (fingerprint.0 >> 64) as u64,
    );
}

fn record_seed_plan_evidence(
    trace: &paro_context::StatementTrace,
    prefix: &str,
    seed: &crate::cascades::SeedPlan,
    logical: Option<&OwnedLogicalPlan>,
) {
    record_fingerprint(trace, &format!("{prefix}.plan"), seed.plan_identity());
    trace.record_value(
        "optimizer",
        &format!("{prefix}.candidate"),
        seed.candidate().index() as u64,
    );
    trace.record_value(
        "optimizer",
        &format!("{prefix}.group"),
        seed.group().0 as u64,
    );
    trace.record_value(
        "optimizer",
        &format!("{prefix}.grant"),
        seed.goal().grant.stable_tag(),
    );
    let frozen = seed.frozen();
    trace.record_value(
        "optimizer",
        &format!("{prefix}.child_count"),
        frozen.winner.children.len() as u64,
    );
    trace.record_value(
        "optimizer",
        &format!("{prefix}.source_work_count"),
        frozen.winner.source_work.len() as u64,
    );
    trace.record_value(
        "optimizer",
        &format!("{prefix}.physical_implementation"),
        frozen.physical.key.implementation.0 as u64,
    );
    trace.record_value(
        "optimizer",
        &format!("{prefix}.physical_logical_expression"),
        frozen.physical.key.logical.0 as u64,
    );
    record_fingerprint(
        trace,
        &format!("{prefix}.physical_payload"),
        frozen.physical.key.payload_fingerprint,
    );
    record_cost_evidence(trace, prefix, frozen.winner.cost);
    if let Some(logical) = logical {
        record_plan_shape(trace, prefix, plan_shape_evidence(logical));
    }
}

fn record_priced_incumbent_evidence(
    trace: &paro_context::StatementTrace,
    prefix: &str,
    incumbent: &PricedIncumbent,
) {
    let seed = incumbent.plan();
    record_fingerprint(trace, &format!("{prefix}.plan"), seed.plan_identity());
    trace.record_value(
        "optimizer",
        &format!("{prefix}.candidate"),
        seed.candidate().index() as u64,
    );
    trace.record_value(
        "optimizer",
        &format!("{prefix}.target_group"),
        incumbent.target_group().0 as u64,
    );
    trace.record_value(
        "optimizer",
        &format!("{prefix}.grant"),
        incumbent.goal().grant.stable_tag(),
    );
    record_fingerprint(
        trace,
        &format!("{prefix}.physical_payload"),
        seed.frozen().physical.key.payload_fingerprint,
    );
    record_cost_evidence(trace, prefix, incumbent.cost());
    record_fingerprint(
        trace,
        &format!("{prefix}.cost_context"),
        incumbent.context().fingerprint(),
    );
    trace.record_value(
        "optimizer",
        &format!("{prefix}.read_count"),
        incumbent.context().reads().reads().len() as u64,
    );
}

fn record_variant_evidence(
    trace: &paro_context::StatementTrace,
    prefix: &str,
    variant: &crate::cascades::OptimizedVariant,
) {
    trace.record_value(
        "optimizer",
        &format!("{prefix}.grant"),
        variant.class.0 as u64,
    );
    record_fingerprint(
        trace,
        &format!("{prefix}.physical"),
        variant.physical_fingerprint,
    );
    record_cost_evidence(trace, prefix, variant.cost);
    record_plan_shape(trace, prefix, plan_shape_evidence(&variant.plan));
    let mut runtime_filter_contracts = 0_u64;
    let mut spill_contracts = 0_u64;
    for contract in variant.contracts.values() {
        if matches!(
            contract.implementation,
            PhysicalImplementationFlavor::HashJoinRuntimeFilter
                | PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
        ) {
            runtime_filter_contracts = runtime_filter_contracts.saturating_add(1);
        }
        if contract.cost.spill_bytes_expected > 0
            || matches!(
                contract.implementation,
                PhysicalImplementationFlavor::CrossProductExternal
                    | PhysicalImplementationFlavor::AdaptiveSort
            )
        {
            spill_contracts = spill_contracts.saturating_add(1);
        }
    }
    trace.record_value(
        "optimizer",
        &format!("{prefix}.runtime_filter_contracts"),
        runtime_filter_contracts,
    );
    trace.record_value(
        "optimizer",
        &format!("{prefix}.spill_contracts"),
        spill_contracts,
    );
}

pub struct Optimizer {
    binder: Binder,
    ctx: OptimizationContext,
    budget: SearchBudget,
    calibration: Arc<MachineCalibrationBundle>,
    compile_work: paro_context::CompileWork,
}

/// Complete optimizer output.  Execution receives no logical tree and makes
/// no algorithm choice; EXPLAIN ANALYZE is the sole statement-level wrapper.
#[derive(Debug)]
pub enum OptimizedStatement {
    Physical(PhysicalPlanPortfolio),
    ExplainAnalyze {
        target: PhysicalPlanPortfolio,
        spec: paro_planner::operator::ExplainSpec,
    },
}

impl Optimizer {
    pub fn new(binder: Binder, session: Arc<StatementContext>) -> Self {
        Self {
            ctx: OptimizationContext::new(session, binder.bind_context.clone()),
            binder,
            budget: SearchBudget::default(),
            calibration: Arc::new(MachineCalibrationBundle::builtin_production()),
            compile_work: Default::default(),
        }
    }

    pub fn with_budget(mut self, budget: SearchBudget) -> Self {
        self.budget = budget;
        self
    }

    pub fn compile_work(&self) -> paro_context::CompileWork {
        self.compile_work
    }

    pub fn with_calibration(mut self, calibration: Arc<MachineCalibrationBundle>) -> Self {
        self.calibration = calibration;
        self
    }

    /// Exposes one proof-driven relational frontier to structural optimizer
    /// tests.
    ///
    /// Production callers must consume [`OptimizedStatement`]; keeping this
    /// hook test-only prevents the planner carrier from becoming an accidental
    /// execution or compatibility API again.
    #[cfg(test)]
    pub(crate) fn correlated_frontier_for_test(
        &mut self,
        plan: OwnedLogicalPlan,
    ) -> Result<OwnedLogicalPlan> {
        let prepared = self.prepare_correlated_seed(plan);
        let canonical = self.canonicalize_query(prepared)?;
        Ok(self.correlated_aggregate_candidate(canonical)?.plan)
    }

    #[cfg(test)]
    pub(crate) fn scalar_reuse_frontier_for_test(
        &mut self,
        plan: OwnedLogicalPlan,
    ) -> Result<OwnedLogicalPlan> {
        let prepared = self.prepare_correlated_seed(plan);
        let canonical = self.canonicalize_query(prepared)?;
        let baseline = self.settle_query_candidate(canonical)?;
        Ok(self.scalar_reuse_candidate(baseline)?.plan)
    }

    pub fn optimize(&mut self, plan: OwnedLogicalPlan) -> Result<OptimizedStatement> {
        let pre_partition = crate::work_partition::enter(crate::work_partition::Bucket::Pre);
        // Allocation attribution is a planning concern. The global allocator
        // remains installed for observation, while counter updates are scoped
        // to this synchronous compiler operation so execution pays no tax.
        let _allocation_metrics = paro_common::allocator::begin_allocation_metrics();
        self.budget.disable_transformations_by_name(
            self.ctx.session.settings.disabled_optimizer_rules(),
        )?;
        let started_at = Instant::now();
        let statement = StatementPlan::split(plan, self.ctx.session.transaction_visible_version())?;
        let grant_classes = resource_grant_classes(
            self.ctx.session.limits.max_memory,
            self.ctx.session.limits.max_threads.max(1),
            self.budget.max_grant_classes,
            self.ctx.session.limits.use_temporary_directory,
        );
        let (query, statement_layer) = match statement.body {
            StatementBody::Query { query, layer } => (*query, *layer),
            StatementBody::Utility(utility) => {
                let result =
                    OptimizedStatement::Physical(self.extract_utility(*utility, &grant_classes)?);
                publish_optimizer_profile_snapshot(
                    self.ctx.session.diagnostics.as_ref(),
                    self.ctx.profiler.snapshot(),
                );
                debug!(
                    target: targets::OPTIMIZER,
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "statement-only optimizer lowering completed"
                );
                return Ok(result);
            }
        };
        let observes_optimizer_diagnostics = observes_optimizer_diagnostics(&query);
        let explain = statement.explain;
        let phase_started = Instant::now();
        let phase_allocated = paro_common::allocator::thread_allocated_bytes();
        let graph_plans = enumerate_graph_region_plans(
            query,
            &self.binder.bind_context,
            self.budget.max_graph_frontiers,
        )?;
        let mut alternatives = Vec::with_capacity(graph_plans.len().saturating_mul(3));
        for (index, graph_plan) in graph_plans.into_iter().enumerate() {
            let (graph_plan, correlated_seed) =
                if contains_redundant_computation_region(&graph_plan)
                    || scalar_aggregate_fusion::contains_candidate_root(&graph_plan)
                {
                    let (graph_plan, correlated_seed) = fork_plan_preserving_indices(
                        graph_plan,
                        self.binder.bind_context.shared().as_ref(),
                    )?;
                    (
                        graph_plan,
                        Some(self.prepare_correlated_seed(correlated_seed)),
                    )
                } else {
                    (graph_plan, None)
                };
            let canonical = self.canonicalize_query(graph_plan)?;
            let (baseline_input, distinct_input) = fork_plan_preserving_indices(
                canonical,
                self.binder.bind_context.shared().as_ref(),
            )?;
            let baseline = self.settle_relational_baseline(baseline_input)?;
            let distinct_feasibility_candidate =
                self.distinct_aggregate_feasibility_candidate(distinct_input)?;
            alternatives.push(baseline.into_alternative(if index == 0 {
                AlternativeOrigin::Baseline
            } else {
                AlternativeOrigin::Specialized {
                    rule: GRAPH_REGION_ENUMERATOR_RULE,
                }
            }));

            if let Some(plan) = distinct_feasibility_candidate {
                alternatives.push(plan.into_alternative(
                    // DISTINCT modifier state is not spillable.  This
                    // equivalent is a mandatory resource-feasibility path,
                    // not optional transformation work: exhausting or
                    // disabling optional rules must not make a valid query
                    // uncompilable under a finite grant.
                    AlternativeOrigin::Specialized {
                        rule: DISTINCT_AGGREGATE_FEASIBILITY_RULE,
                    },
                ));
            }
            let Some(correlated_seed) = correlated_seed else {
                continue;
            };
            let correlated_canonical = match self.canonicalize_query(correlated_seed) {
                Ok(plan) => plan,
                Err(error) => {
                    debug!(
                        target: targets::OPTIMIZER,
                        %error,
                        "correlated-region preparation rejected an invalid optional candidate"
                    );
                    continue;
                }
            };
            if alternatives.len() >= self.budget.max_optional_logical_exprs_per_group as usize + 1 {
                break;
            }
            let (aggregate_input, scalar_reuse_input) = fork_plan_preserving_indices(
                correlated_canonical,
                self.binder.bind_context.shared().as_ref(),
            )?;
            match self.correlated_aggregate_candidate(aggregate_input) {
                Ok(candidate) => {
                    let (base_plan, payload_plan) = fork_plan_preserving_indices(
                        candidate.plan,
                        self.binder.bind_context.shared().as_ref(),
                    )?;
                    let payload_input = CandidatePlan {
                        plan: payload_plan,
                        column_stats: candidate.column_stats.clone(),
                    };
                    alternatives.push(
                        CandidatePlan {
                            plan: base_plan,
                            column_stats: candidate.column_stats,
                        }
                        .into_alternative(AlternativeOrigin::Specialized {
                            rule: CORRELATED_AGGREGATE_REGION_RULE,
                        }),
                    );
                    if alternatives.len()
                        < self.budget.max_optional_logical_exprs_per_group as usize + 1
                    {
                        match self.correlated_topn_payload_candidate(payload_input) {
                            Ok(Some(plan)) => alternatives.push(plan.into_alternative(
                                AlternativeOrigin::Specialized {
                                    rule: CORRELATED_TOPN_PAYLOAD_REGION_RULE,
                                },
                            )),
                            Ok(None) => {}
                            Err(error) => debug!(
                                target: targets::OPTIMIZER,
                                %error,
                                "correlated TopN payload recipe rejected an invalid optional candidate"
                            ),
                        }
                    }
                }
                Err(error) => debug!(
                    target: targets::OPTIMIZER,
                    %error,
                    "correlated-aggregate enumerator rejected an invalid optional candidate"
                ),
            }
            if alternatives.len() >= self.budget.max_optional_logical_exprs_per_group as usize + 1 {
                break;
            }
            match self
                .settle_query_candidate(scalar_reuse_input)
                .and_then(|plan| self.scalar_reuse_candidate(plan))
            {
                Ok(plan) => {
                    alternatives.push(plan.into_alternative(AlternativeOrigin::Specialized {
                        rule: SCALAR_REUSE_REGION_RULE,
                    }))
                }
                Err(error) => debug!(
                    target: targets::OPTIMIZER,
                    %error,
                    "scalar-reuse enumerator rejected an invalid optional candidate"
                ),
            }
        }
        self.ctx.profiler.record(
            OptimizerComponent::SemanticNormalization,
            phase_started.elapsed(),
        );
        self.ctx.profiler.record_component_allocation(
            OptimizerComponent::SemanticNormalization,
            paro_common::allocator::allocated_bytes_since(phase_allocated),
        );
        let query_ir_phase_started = Instant::now();
        let query_ir_phase_allocated = paro_common::allocator::thread_allocated_bytes();
        for alternative in &alternatives {
            verify_physical_planner_invariants(&alternative.plan.operator)?;
        }
        let strong_incumbent_experiment = std::env::var_os("PARO_STRONG_INCUMBENT_EXPERIMENT")
            .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
        let strong_incumbent_provide_bound = strong_incumbent_experiment
            && std::env::var_os("PARO_STRONG_INCUMBENT_PROVIDE_BOUND")
                .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
        let strong_incumbent_inject_logical = strong_incumbent_experiment
            && std::env::var_os("PARO_STRONG_INCUMBENT_INJECT_LOGICAL")
                .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
        let (mode, extraction, search_phase_started, search_phase_allocated) =
            if strong_incumbent_experiment {
                // Build two independent Memo instances from the same semantic
                // alternatives. The first one is used only to produce a verified
                // immutable upper bound; the second one owns the measured proof
                // search. No search frontier, task registry, or planner payload
                // arena crosses this boundary.
                let mut source_binder = self.binder.clone();
                source_binder.bind_context = self.binder.bind_context.with_independent_plan_ids();
                let source_bind_shared = source_binder.bind_context.shared().clone();
                let mut source_alternatives = Vec::with_capacity(alternatives.len());
                let mut target_alternatives = Vec::with_capacity(alternatives.len());
                for alternative in alternatives {
                    let source = alternative.source;
                    let column_stats = alternative.column_stats;
                    // Keep the exact original plan in the measured target
                    // Memo so the no-seed controls have the same initial
                    // domain as the ordinary production path. Only the
                    // source Memo receives a deep duplicate; its fresh
                    // plan-node IDs must never become a target-search
                    // variable. Rebuilding both branches here can change
                    // insertion order and therefore confound the isolation
                    // experiment even when the trees are semantically equal.
                    let source_plan = duplicate_plan_preserving_indices(
                        &alternative.plan,
                        source_bind_shared.as_ref(),
                    );
                    let target_plan = alternative.plan;
                    source_alternatives.push(LogicalAlternative {
                        plan: source_plan,
                        source,
                        column_stats: column_stats.clone(),
                    });
                    target_alternatives.push(LogicalAlternative {
                        plan: target_plan,
                        source,
                        column_stats,
                    });
                }

                let source_input = self
                    .build_search_input_with_binder(
                        source_alternatives,
                        &statement_layer,
                        &source_binder,
                    )?
                    .with_strong_incumbent_export(true)
                    // C/D must use the same known seed.  The pruning switch
                    // belongs to the independent target proof Memo, not to
                    // source seed generation.
                    .with_certified_group_pruning(false);
                self.ctx.profiler.record(
                    OptimizerComponent::QueryIrConstruction,
                    query_ir_phase_started.elapsed(),
                );
                self.ctx.profiler.record_component_allocation(
                    OptimizerComponent::QueryIrConstruction,
                    paro_common::allocator::allocated_bytes_since(query_ir_phase_allocated),
                );
                let search_phase_started = Instant::now();
                let search_phase_allocated = paro_common::allocator::thread_allocated_bytes();
                let source_started = Instant::now();
                let source_output = source_input.optimize(&grant_classes)?;
                let source_total_us =
                    u64::try_from(source_started.elapsed().as_micros()).unwrap_or(u64::MAX);
                let source_complete = source_output.search_summary.is_complete();
                let source_obligations = source_output.search_summary.obligations.len() as u64;
                let source_profile_us = source_output.search_milestones.search_return_profile_us;
                let source_export_us = source_output.strong_incumbent_export_us;
                let source_logical_plans = source_output.strong_incumbent_logical_plans.into_vec();
                let plans = source_output.strong_incumbent_plans.into_vec();
                let source_seed_count = plans.len();
                if plans.is_empty() {
                    return Err(paro_error::internal(
                        "strong incumbent experiment produced no verified seed plan",
                    ));
                }
                if strong_incumbent_inject_logical && source_logical_plans.len() != plans.len() {
                    return Err(paro_error::internal(format!(
                        "strong incumbent logical/physical seed count mismatch: logical={} physical={}",
                        source_logical_plans.len(),
                        plans.len()
                    )));
                }
                if let Some(trace) = self.ctx.session.statement_trace() {
                    for (seed_ordinal, seed) in plans.iter().enumerate() {
                        record_seed_plan_evidence(
                            trace.as_ref(),
                            &format!("strong_incumbent_source_seed_{seed_ordinal}"),
                            seed,
                            source_logical_plans.get(seed_ordinal),
                        );
                    }
                    trace.record_value(
                        "optimizer",
                        "strong_incumbent_provide_bound",
                        u64::from(strong_incumbent_provide_bound),
                    );
                    trace.record_value(
                        "optimizer",
                        "strong_incumbent_inject_logical",
                        u64::from(strong_incumbent_inject_logical),
                    );
                    trace.record_event("optimizer", "strong_incumbent_seed_source_complete");
                    trace.record_value(
                        "optimizer",
                        "strong_incumbent_seed_source_count",
                        source_seed_count as u64,
                    );
                    if let Some(plan) = plans.first() {
                        trace.record_value(
                            "optimizer",
                            "strong_incumbent_seed_plan_identity_lo",
                            plan.plan_identity().0 as u64,
                        );
                        trace.record_value(
                            "optimizer",
                            "strong_incumbent_seed_plan_identity_hi",
                            (plan.plan_identity().0 >> 64) as u64,
                        );
                    }
                    trace.record_value(
                        "optimizer",
                        "strong_incumbent_seed_source_total_us",
                        source_total_us,
                    );
                    if let Some(export_us) = source_export_us {
                        trace.record_value(
                            "optimizer",
                            "strong_incumbent_seed_source_export_us",
                            export_us,
                        );
                    }
                    trace.record_value(
                        "optimizer",
                        "strong_incumbent_seed_source_search_complete",
                        u64::from(source_complete),
                    );
                    trace.record_value(
                        "optimizer",
                        "strong_incumbent_seed_source_obligations",
                        source_obligations,
                    );
                    if let Some(profile_us) = source_profile_us {
                        trace.record_value(
                            "optimizer",
                            "strong_incumbent_seed_source_search_us",
                            profile_us,
                        );
                    }
                }

                let seed_column_stats = target_alternatives
                    .first()
                    .map(|alternative| alternative.column_stats.clone())
                    .unwrap_or_else(|| Arc::new(HashMap::new()));
                let seed_output_table_index = target_alternatives
                    .first()
                    .and_then(|alternative| alternative.plan.get_column_bindings().first().copied())
                    .map(|binding| binding.table_index);
                let expected_seed_output_bindings = target_alternatives
                    .first()
                    .map(|alternative| alternative.plan.get_column_bindings())
                    .unwrap_or_default();
                let mut pricing_alternatives =
                    if strong_incumbent_provide_bound && !strong_incumbent_inject_logical {
                        target_alternatives
                            .iter()
                            .map(|alternative| LogicalAlternative {
                                plan: duplicate_plan_preserving_indices(
                                    &alternative.plan,
                                    source_bind_shared.as_ref(),
                                ),
                                source: alternative.source,
                                column_stats: alternative.column_stats.clone(),
                            })
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    };
                let mut target_alternatives = target_alternatives;
                for (seed_ordinal, plan) in source_logical_plans.into_iter().enumerate() {
                    if !strong_incumbent_inject_logical && !strong_incumbent_provide_bound {
                        continue;
                    }
                    {
                        let source_bindings = plan.get_column_bindings();
                        let plan = if strong_incumbent_inject_logical {
                            duplicate_plan_preserving_indices(
                                &plan,
                                self.binder.bind_context.shared().as_ref(),
                            )
                        } else {
                            plan
                        };
                        let plan = if let Some(table_index) = seed_output_table_index {
                            rebase_seed_output_table_index(plan, table_index)?
                        } else {
                            plan
                        };
                        let rebased_output_bindings = plan.get_column_bindings();
                        if rebased_output_bindings != expected_seed_output_bindings {
                            return Err(paro_error::internal(format!(
                                "strong seed output layout cannot be rebased: expected={expected_seed_output_bindings:?}, actual={rebased_output_bindings:?}"
                            )));
                        }
                        if let Some(trace) = self.ctx.session.statement_trace() {
                            trace.record_value(
                                "optimizer",
                                &format!("strong_incumbent_seed_{seed_ordinal}_target_table_index"),
                                seed_output_table_index.unwrap_or(usize::MAX) as u64,
                            );
                            trace.record_value(
                                "optimizer",
                                &format!(
                                    "strong_incumbent_seed_{seed_ordinal}_source_output_width"
                                ),
                                source_bindings.len() as u64,
                            );
                            trace.record_value(
                                "optimizer",
                                &format!(
                                    "strong_incumbent_seed_{seed_ordinal}_source_output_table"
                                ),
                                source_bindings
                                    .first()
                                    .map(|binding| binding.table_index as u64)
                                    .unwrap_or(u64::MAX),
                            );
                            let target_bindings = plan.get_column_bindings();
                            trace.record_value(
                                "optimizer",
                                &format!(
                                    "strong_incumbent_seed_{seed_ordinal}_rebased_output_table"
                                ),
                                target_bindings
                                    .first()
                                    .map(|binding| binding.table_index as u64)
                                    .unwrap_or(u64::MAX),
                            );
                        }
                        let alternative = LogicalAlternative {
                            plan,
                            source: AlternativeOrigin::Specialized {
                                rule: STRONG_INCUMBENT_SEED_RULE,
                            },
                            column_stats: seed_column_stats.clone(),
                        };
                        if strong_incumbent_inject_logical {
                            target_alternatives.push(alternative);
                        } else {
                            pricing_alternatives.push(alternative);
                        }
                    }
                }
                // Keep the Memo independent while making a source winner's
                // transformed logical shell available for re-pricing. The
                // target registry, facts, grant and costs are rebuilt below;
                // no source frontier or search cache crosses this boundary.
                let mut target_input =
                    self.build_search_input(target_alternatives, &statement_layer)?;
                let mode = target_input.mode;
                if strong_incumbent_provide_bound {
                    if strong_incumbent_inject_logical {
                        for plan in plans {
                            target_input = target_input.with_strong_incumbent_plan(plan);
                        }
                    } else {
                        let pricing_input = self.build_search_input_with_binder(
                            pricing_alternatives,
                            &statement_layer,
                            &source_binder,
                        )?;
                        let pricing_started = Instant::now();
                        let repriced = pricing_input
                            .reprice_seed_plans_in_isolated_memo(&grant_classes, &plans)?;
                        let pricing_us = u64::try_from(pricing_started.elapsed().as_micros())
                            .unwrap_or(u64::MAX);
                        if let Some(trace) = self.ctx.session.statement_trace() {
                            trace.record_value(
                                "optimizer",
                                "strong_incumbent_seed_isolated_reprice_us",
                                pricing_us,
                            );
                            trace.record_value(
                                "optimizer",
                                "strong_incumbent_seed_isolated_reprice_count",
                                repriced.len() as u64,
                            );
                            for (incumbent_ordinal, incumbent) in repriced.iter().enumerate() {
                                record_priced_incumbent_evidence(
                                    trace.as_ref(),
                                    &format!("strong_incumbent_target_priced_{incumbent_ordinal}"),
                                    incumbent,
                                );
                            }
                        }
                        for incumbent in repriced {
                            target_input = target_input.with_prepriced_strong_incumbent(incumbent);
                        }
                    }
                }
                if let Some(trace) = self.ctx.session.statement_trace() {
                    trace.record_event("optimizer", "strong_incumbent_seed_target_begin");
                }
                let target_started = Instant::now();
                let extraction = target_input.optimize(&grant_classes)?;
                let target_total_us =
                    u64::try_from(target_started.elapsed().as_micros()).unwrap_or(u64::MAX);
                if let Some(trace) = self.ctx.session.statement_trace() {
                    trace.record_event("optimizer", "strong_incumbent_seed_target_complete");
                    trace.record_value(
                        "optimizer",
                        "strong_incumbent_seed_target_total_us",
                        target_total_us,
                    );
                    if let Some(reprice_us) = extraction.strong_incumbent_reprice_us {
                        trace.record_value(
                            "optimizer",
                            "strong_incumbent_seed_target_reprice_us",
                            reprice_us,
                        );
                        trace.record_value(
                            "optimizer",
                            "strong_incumbent_seed_target_reprice_count",
                            extraction.strong_incumbent_reprice_count,
                        );
                        if let Some(identity) = extraction.strong_incumbent_plan_identity {
                            trace.record_value(
                                "optimizer",
                                "strong_incumbent_seed_target_plan_identity_lo",
                                identity.0 as u64,
                            );
                            trace.record_value(
                                "optimizer",
                                "strong_incumbent_seed_target_plan_identity_hi",
                                (identity.0 >> 64) as u64,
                            );
                        }
                    }
                    trace.record_value(
                        "optimizer",
                        "strong_incumbent_seed_target_search_complete",
                        u64::from(extraction.search_summary.is_complete()),
                    );
                }
                (
                    mode,
                    extraction,
                    search_phase_started,
                    search_phase_allocated,
                )
            } else {
                let input = self.build_search_input(alternatives, &statement_layer)?;
                let mode = input.mode;
                self.ctx.profiler.record(
                    OptimizerComponent::QueryIrConstruction,
                    query_ir_phase_started.elapsed(),
                );
                self.ctx.profiler.record_component_allocation(
                    OptimizerComponent::QueryIrConstruction,
                    paro_common::allocator::allocated_bytes_since(query_ir_phase_allocated),
                );
                let search_phase_started = Instant::now();
                let search_phase_allocated = paro_common::allocator::thread_allocated_bytes();
                drop(pre_partition);
                let extraction = input.optimize(&grant_classes)?;
                (
                    mode,
                    extraction,
                    search_phase_started,
                    search_phase_allocated,
                )
            };
        let _finish_partition = crate::work_partition::enter(crate::work_partition::Bucket::Finish);
        if let Some(capture) = &self.ctx.session.options.compile_capture {
            use paro_context::compile_diagnostics::{Observation::Observed, RuleSummary, SearchStop};
            capture.update(|record| {
                record.groups = Observed(extraction.search_summary.groups);
                record.logical_expressions = Observed(extraction.search_summary.logical_expressions);
                record.physical_expressions = Observed(extraction.search_summary.physical_expressions);
                record.obligations = Observed(extraction.search_summary.obligations.len() as u64);
                record.search_complete = Observed(extraction.search_summary.is_complete());
                record.quality_policy_satisfied = Observed(matches!(extraction.quality_policy_status, crate::cascades::quality::QualityPolicyStatus::Satisfied(_)));
                record.budget_limited = Observed(extraction.search_stop.budget_limited);
                record.search_stop = Observed(match extraction.search_stop.reason {
                    crate::cascades::engine::SearchStopReason::Complete => SearchStop::Complete,
                    crate::cascades::engine::SearchStopReason::SearchIncomplete => SearchStop::Incomplete,
                    crate::cascades::engine::SearchStopReason::Deadline => SearchStop::Deadline,
                    crate::cascades::engine::SearchStopReason::BudgetLimited => SearchStop::BudgetLimited,
                    crate::cascades::engine::SearchStopReason::RuleFailure => SearchStop::RuleFailure,
                    crate::cascades::engine::SearchStopReason::QualityPolicySatisfied => SearchStop::QualityPolicySatisfied,
                });
            });
            for (id, attempts) in &extraction.rule_attempts {
                capture.rule(RuleSummary { id: id.0, attempts: *attempts,
                    inserted: extraction.rule_insertions.get(id).copied().unwrap_or(0),
                    elapsed_ns: extraction.rule_elapsed.get(id).map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)) });
            }
        }
        if paro_context::compile_work_evidence_enabled() {
            self.compile_work.rule_elapsed_us = extraction.rule_elapsed.values()
                .map(|duration| u64::try_from(duration.as_micros()).unwrap_or(u64::MAX))
                .fold(0u64, u64::saturating_add);
            self.compile_work.child_combination_cost_synthesis_count = extraction.search_summary
                .work_counters.get("child_combination_cost_synthesis_count").copied().unwrap_or(0);
        }
        self.ctx
            .profiler
            .record_rule_attempts(extraction.rule_attempts.clone());
        self.ctx
            .profiler
            .record_rule_insertions(extraction.rule_insertions.clone());
        self.ctx
            .profiler
            .record_rule_elapsed(extraction.rule_elapsed.clone());
        self.ctx
            .profiler
            .record_rule_allocated_bytes(extraction.rule_allocated_bytes.clone());
        self.ctx
            .profiler
            .record_rule_budget_exhaustions(extraction.rule_budget_exhaustions.clone());
        self.ctx
            .profiler
            .record_search_summary(&extraction.search_summary);
        if let Some(trace) = self.ctx.session.statement_trace() {
            let summary = &extraction.search_summary;
            for (variant_ordinal, variant) in extraction.variants.iter().enumerate() {
                record_variant_evidence(
                    trace.as_ref(),
                    &format!("final_winner_{variant_ordinal}"),
                    variant,
                );
            }
            trace.record_value("optimizer", "memo_group_count", summary.groups);
            trace.record_value(
                "optimizer",
                "memo_logical_expression_count",
                summary.logical_expressions,
            );
            trace.record_value(
                "optimizer",
                "memo_physical_expression_count",
                summary.physical_expressions,
            );
            trace.record_value(
                "optimizer",
                "transformation_apply_attempt_count",
                extraction.rule_attempts.values().copied().sum(),
            );
            trace.record_value(
                "optimizer",
                "transformation_inserted_count",
                extraction.rule_insertions.values().copied().sum(),
            );
            trace.record_value(
                "optimizer",
                "transformation_allocated_bytes",
                extraction.rule_allocated_bytes.values().copied().sum(),
            );
            trace.record_value(
                "optimizer",
                "transformation_budget_exhaustion_count",
                extraction.rule_budget_exhaustions.values().copied().sum(),
            );
            for (name, count) in &summary.work_counters {
                trace.record_value("optimizer", name, *count);
            }
            for (rule, profile) in &extraction.rule_work_profile {
                let rule_name = crate::cascades::rules::transformation_rule_name(*rule)
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("unknown_rule_{}", rule.0));
                for (guard, count) in profile.rejection_guards.iter() {
                    if count != 0 {
                        let event = format!("rule.{rule_name}.rejection_guard.{}", guard.name());
                        trace.record_value("optimizer", &event, count);
                    }
                }
                for (phase, count) in [
                    ("discovered", profile.discovered),
                    ("matched", profile.matched),
                    ("applicable", profile.applicable),
                    ("constructed", profile.constructed),
                    ("published", profile.published),
                    ("rejected", profile.rejected),
                    ("ineffective", profile.ineffective),
                    ("root_consumed", profile.root_consumed),
                ] {
                    let event = format!("rule.{rule_name}.{phase}");
                    trace.record_value("optimizer", &event, count);
                }
                for (phase, elapsed) in [
                    ("first_enqueued_us", profile.first_enqueued_us),
                    (
                        "first_dependencies_ready_us",
                        profile.first_dependencies_ready_us,
                    ),
                    ("first_run_us", profile.first_run_us),
                    ("first_discovered_us", profile.first_discovered_us),
                    ("first_matched_us", profile.first_matched_us),
                    ("first_applicable_us", profile.first_applicable_us),
                    ("first_published_us", profile.first_published_us),
                ] {
                    if let Some(elapsed) = elapsed {
                        let event = format!("rule.{rule_name}.{phase}");
                        trace.record_value("optimizer", &event, elapsed);
                    }
                }
                if let Some(elapsed) = extraction.rule_elapsed.get(rule) {
                    let event = format!("rule.{rule_name}.elapsed_us");
                    trace.record_value(
                        "optimizer",
                        &event,
                        u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
                    );
                }
            }
            for (name, elapsed) in [
                ("first_safe_us", extraction.search_milestones.first_safe_us),
                (
                    "first_optional_ready_us",
                    extraction.search_milestones.first_optional_ready_us,
                ),
                (
                    "first_optional_selected_us",
                    extraction.search_milestones.first_optional_selected_us,
                ),
                (
                    "first_logical_publication_us",
                    extraction.search_milestones.first_logical_publication_us,
                ),
                (
                    "quality_policy_satisfied_us",
                    extraction.search_milestones.quality_policy_satisfied_us,
                ),
            ] {
                if let Some(elapsed) = elapsed {
                    trace.record_value("optimizer", name, elapsed);
                }
            }
            for (name, candidate) in [
                (
                    "first_safe_candidate",
                    extraction.search_milestones.safe_candidate,
                ),
                (
                    "first_optional_ready_candidate",
                    extraction.search_milestones.optional_ready_candidate,
                ),
                (
                    "first_optional_selected_candidate",
                    extraction.search_milestones.optional_selected_candidate,
                ),
                (
                    "quality_policy_candidate",
                    extraction.search_milestones.quality_policy_candidate,
                ),
            ] {
                if let Some(candidate) = candidate {
                    trace.record_value("optimizer", name, candidate.index() as u64);
                }
            }
            trace.record_value(
                "optimizer",
                "candidate_lifecycle_event_count",
                extraction.search_milestones.candidate_lifecycle.len() as u64,
            );
            trace.record_value(
                "optimizer",
                "candidate_lifecycle_event_dropped",
                extraction.search_milestones.candidate_lifecycle_dropped,
            );
            for (stage, (stored, dropped)) in extraction
                .search_milestones
                .candidate_lifecycle_stage_stored
                .iter()
                .zip(
                    extraction
                        .search_milestones
                        .candidate_lifecycle_stage_dropped
                        .iter(),
                )
                .enumerate()
            {
                trace.record_value(
                    "optimizer",
                    &format!("candidate_lifecycle_stage_{stage}.stored"),
                    *stored,
                );
                trace.record_value(
                    "optimizer",
                    &format!("candidate_lifecycle_stage_{stage}.dropped"),
                    *dropped,
                );
            }
            for (event_index, event) in extraction
                .search_milestones
                .candidate_lifecycle
                .iter()
                .enumerate()
            {
                let prefix = format!("candidate_lifecycle_{event_index}");
                trace.record_value("optimizer", &format!("{prefix}.stage"), event.stage as u64);
                trace.record_value(
                    "optimizer",
                    &format!("{prefix}.elapsed_us"),
                    event.elapsed_us,
                );
                trace.record_value(
                    "optimizer",
                    &format!("{prefix}.group"),
                    event.group.0 as u64,
                );
                if let Some(goal) = event.goal {
                    trace.record_value(
                        "optimizer",
                        &format!("{prefix}.goal_required"),
                        goal.required.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{prefix}.goal_grant"),
                        goal.grant.stable_tag(),
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{prefix}.goal_context"),
                        goal.context.0 as u64,
                    );
                }
                for (name, value) in [
                    (
                        "candidate",
                        event.candidate.map(|value| value.index() as u64),
                    ),
                    ("source", event.source.map(|value| value.index() as u64)),
                    (
                        "source_child",
                        event.source_child.map(|value| value.index() as u64),
                    ),
                    ("logical", event.logical.map(|value| value.index() as u64)),
                    ("physical", event.physical.map(|value| value.index() as u64)),
                    ("rule", event.rule.map(|value| value.0 as u64)),
                ] {
                    if let Some(value) = value {
                        trace.record_value("optimizer", &format!("{prefix}.{name}"), value);
                    }
                }
                if let Some(recipe) = event.recipe {
                    trace.record_value(
                        "optimizer",
                        &format!("{prefix}.recipe_lo"),
                        recipe.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{prefix}.recipe_hi"),
                        (recipe.0 >> 64) as u64,
                    );
                }
                if let Some(binding) = event.binding {
                    trace.record_value(
                        "optimizer",
                        &format!("{prefix}.binding_lo"),
                        binding.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{prefix}.binding_hi"),
                        (binding.0 >> 64) as u64,
                    );
                }
                if let Some(cost) = event.expected_cost_bits {
                    trace.record_value("optimizer", &format!("{prefix}.expected_cost_bits"), cost);
                }
                if let Some(cost) = event.upper_cost_bits {
                    trace.record_value("optimizer", &format!("{prefix}.upper_cost_bits"), cost);
                }
                trace.record_value(
                    "optimizer",
                    &format!("{prefix}.child_count"),
                    event.children.len() as u64,
                );
                for (child_index, child) in event.children.iter().enumerate() {
                    let child_prefix = format!("{prefix}.child_{child_index}");
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
                    &format!("{prefix}.fact_count"),
                    event.facts.len() as u64,
                );
                for (fact_index, fact) in event.facts.iter().enumerate() {
                    let fact_prefix = format!("{prefix}.fact_{fact_index}");
                    trace.record_value(
                        "optimizer",
                        &format!("{fact_prefix}.group"),
                        fact.group.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{fact_prefix}.logical_fact_lo"),
                        fact.logical_fact_fingerprint.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{fact_prefix}.logical_fact_hi"),
                        (fact.logical_fact_fingerprint.0 >> 64) as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{fact_prefix}.statistics_lo"),
                        fact.statistics_snapshot_fingerprint.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{fact_prefix}.statistics_hi"),
                        (fact.statistics_snapshot_fingerprint.0 >> 64) as u64,
                    );
                }
            }
            trace.record_value(
                "optimizer",
                "transformation_task_lifecycle_count",
                extraction
                    .search_milestones
                    .transformation_task_lifecycle
                    .len() as u64,
            );
            trace.record_value(
                "optimizer",
                "transformation_task_lifecycle_dropped",
                extraction
                    .search_milestones
                    .transformation_task_lifecycle_dropped,
            );
            for (task_index, task) in extraction
                .search_milestones
                .transformation_task_lifecycle
                .iter()
                .enumerate()
            {
                let prefix = format!("transformation_task_{task_index}");
                for (name, value) in [
                    ("group", Some(task.group.index() as u64)),
                    ("expression", Some(task.expression.index() as u64)),
                    ("rule", Some(task.rule.0 as u64)),
                    ("first_enqueued_us", task.first_enqueued_us),
                    ("first_run_us", task.first_run_us),
                    (
                        "first_dependencies_ready_us",
                        task.first_dependencies_ready_us,
                    ),
                    ("first_matched_us", task.first_matched_us),
                    ("first_no_match_us", task.first_no_match_us),
                    ("first_applicable_us", task.first_applicable_us),
                    ("first_published_us", task.first_published_us),
                    ("first_no_output_us", task.first_no_output_us),
                    ("first_budget_rejected_us", task.first_budget_rejected_us),
                    ("last_enqueued_us", task.last_enqueued_us),
                    (
                        "last_dependencies_ready_us",
                        task.last_dependencies_ready_us,
                    ),
                    ("last_run_us", task.last_run_us),
                    ("last_published_us", task.last_published_us),
                    (
                        "first_binding_lo",
                        task.first_binding.map(|value| value.0 as u64),
                    ),
                    (
                        "first_binding_hi",
                        task.first_binding.map(|value| (value.0 >> 64) as u64),
                    ),
                    ("match_count", Some(task.match_count)),
                    ("no_match_count", Some(task.no_match_count)),
                    ("applicable_count", Some(task.applicable_count)),
                    ("no_output_count", Some(task.no_output_count)),
                    ("published_count", Some(task.published_count)),
                    ("budget_rejected_count", Some(task.budget_rejected_count)),
                ] {
                    if let Some(value) = value {
                        trace.record_value("optimizer", &format!("{prefix}.{name}"), value);
                    }
                }
                trace.record_value(
                    "optimizer",
                    &format!("{prefix}.read_count"),
                    task.last_reads.len() as u64,
                );
                for (read_index, read) in task.last_reads.iter().enumerate() {
                    let read_prefix = format!("{prefix}.read_{read_index}");
                    trace.record_value(
                        "optimizer",
                        &format!("{read_prefix}.group"),
                        read.group.index() as u64,
                    );
                    if let Some(revision) = read.logical_frontier_revision {
                        trace.record_value(
                            "optimizer",
                            &format!("{read_prefix}.logical_frontier_revision"),
                            revision,
                        );
                    }
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
            }
            for checkpoint in &extraction.search_milestones.search_checkpoints {
                let goal = checkpoint.goal;
                let prefix = format!(
                    "search_checkpoint_{}ms_required{}_grant{}_row{}_objective{}_context{}",
                    checkpoint.target_ms,
                    goal.required.0,
                    goal.grant.stable_tag(),
                    goal.row_goal.stable_tag(),
                    goal.objective.stable_tag(),
                    goal.context.0,
                );
                trace.record_value(
                    "optimizer",
                    &format!("{prefix}.observed_us"),
                    checkpoint.observed_us,
                );
                trace.record_value(
                    "optimizer",
                    &format!("{prefix}.has_candidate"),
                    u64::from(checkpoint.candidate.is_some()),
                );
                if let Some(candidate) = checkpoint.candidate {
                    trace.record_value(
                        "optimizer",
                        &format!("{prefix}.candidate"),
                        candidate.index() as u64,
                    );
                }
                for (name, cost) in [
                    ("expected_cost_bits", checkpoint.expected_cost),
                    ("risk_adjusted_cost_bits", checkpoint.risk_adjusted_cost),
                    ("upper_cost_bits", checkpoint.upper_cost),
                ] {
                    if let Some(cost) = cost {
                        trace.record_value(
                            "optimizer",
                            &format!("{prefix}.{name}"),
                            cost.to_bits(),
                        );
                    }
                }
                trace.record_value(
                    "optimizer",
                    &format!("{prefix}.search_complete"),
                    u64::from(checkpoint.search_complete),
                );
                trace.record_value(
                    "optimizer",
                    &format!("{prefix}.frozen"),
                    u64::from(checkpoint.frozen),
                );
                trace.record_value(
                    "optimizer",
                    &format!("{prefix}.choice_count"),
                    checkpoint.choices.len() as u64,
                );
                for (choice_index, choice) in checkpoint.choices.iter().enumerate() {
                    let choice_prefix = format!("{prefix}.choice_{choice_index}");
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.group"),
                        choice.reference.group.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.candidate"),
                        choice.reference.candidate.index() as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.goal_required"),
                        choice.reference.goal.required.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.goal_grant"),
                        choice.reference.goal.grant.stable_tag(),
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.goal_context"),
                        choice.reference.goal.context.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.logical"),
                        choice.logical.index() as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.physical"),
                        choice.physical.index() as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.logical_payload"),
                        choice.logical_payload as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.physical_payload"),
                        choice.physical_payload as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.physical_fingerprint_lo"),
                        choice.physical_fingerprint.0 as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.physical_fingerprint_hi"),
                        (choice.physical_fingerprint.0 >> 64) as u64,
                    );
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.child_count"),
                        choice.children.len() as u64,
                    );
                    for (child_index, child) in choice.children.iter().enumerate() {
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
                        choice.rules.len() as u64,
                    );
                    for (rule_index, rule) in choice.rules.iter().enumerate() {
                        trace.record_value(
                            "optimizer",
                            &format!("{choice_prefix}.rule_{rule_index}"),
                            rule.0 as u64,
                        );
                    }
                    trace.record_value(
                        "optimizer",
                        &format!("{choice_prefix}.selected_rule_count"),
                        choice.selected_rules.len() as u64,
                    );
                    for (rule_index, rule) in choice.selected_rules.iter().enumerate() {
                        trace.record_value(
                            "optimizer",
                            &format!("{choice_prefix}.selected_rule_{rule_index}"),
                            rule.0 as u64,
                        );
                    }
                }
                trace.record_value(
                    "optimizer",
                    &format!("{prefix}.fact_read_count"),
                    checkpoint.fact_reads.len() as u64,
                );
                for (read_index, read) in checkpoint.fact_reads.iter().enumerate() {
                    let read_prefix = format!("{prefix}.fact_read_{read_index}");
                    trace.record_value(
                        "optimizer",
                        &format!("{read_prefix}.group"),
                        read.group.0 as u64,
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
            }
            trace.record_event(
                "optimizer",
                if summary.is_complete() {
                    "search_complete"
                } else {
                    "search_incomplete"
                },
            );
        }
        self.ctx.profiler.record(
            match mode {
                crate::cascades::SearchMode::Direct => OptimizerComponent::DirectPhysicalSearch,
                crate::cascades::SearchMode::Memo => OptimizerComponent::MemoExploration,
            },
            search_phase_started.elapsed(),
        );
        self.ctx.profiler.record_component_allocation(
            match mode {
                crate::cascades::SearchMode::Direct => OptimizerComponent::DirectPhysicalSearch,
                crate::cascades::SearchMode::Memo => OptimizerComponent::MemoExploration,
            },
            paro_common::allocator::allocated_bytes_since(search_phase_allocated),
        );
        let phase_started = Instant::now();
        let phase_allocated = paro_common::allocator::thread_allocated_bytes();
        for variant in &extraction.variants {
            verify_physical_planner_invariants(&variant.plan.operator)?;
        }
        self.ctx.profiler.record(
            OptimizerComponent::WinnerVerification,
            phase_started.elapsed(),
        );
        self.ctx.profiler.record_component_allocation(
            OptimizerComponent::WinnerVerification,
            paro_common::allocator::allocated_bytes_since(phase_allocated),
        );
        let phase_started = Instant::now();
        let phase_allocated = paro_common::allocator::thread_allocated_bytes();
        let mut variants = extraction.variants.into_vec();
        for variant in &mut variants {
            self.attach_statement_layer(variant, &statement_layer)?;
        }
        let analyze_spec = explain.as_ref().and_then(|explain| {
            (explain.spec.mode == paro_planner::operator::ExplainMode::Analyze)
                .then_some(explain.spec)
        });
        let result = if let Some(spec) = analyze_spec {
            OptimizedStatement::ExplainAnalyze {
                target: self.extract_physical(variants, &grant_classes, extraction.grant_search)?,
                spec,
            }
        } else {
            if let Some(explain) = &explain {
                for variant in &mut variants {
                    self.attach_explain_layer(variant, explain)?;
                }
            }
            OptimizedStatement::Physical(self.extract_physical(
                variants,
                &grant_classes,
                extraction.grant_search,
            )?)
        };
        self.ctx.profiler.record(
            OptimizerComponent::PhysicalExtraction,
            phase_started.elapsed(),
        );
        self.ctx.profiler.record_component_allocation(
            OptimizerComponent::PhysicalExtraction,
            paro_common::allocator::allocated_bytes_since(phase_allocated),
        );
        if !observes_optimizer_diagnostics {
            publish_optimizer_profile_snapshot(
                self.ctx.session.diagnostics.as_ref(),
                self.ctx.profiler.snapshot(),
            );
        }
        debug!(
            target: targets::OPTIMIZER,
            ?mode,
            elapsed_ms = started_at.elapsed().as_millis(),
            "bounded optimizer completed"
        );
        Ok(result)
    }

    fn build_search_input(
        &self,
        alternatives: Vec<LogicalAlternative>,
        statement_layer: &QueryStatementLayer,
    ) -> Result<crate::cascades::OptimizationInput> {
        self.build_search_input_with_binder(alternatives, statement_layer, &self.binder)
    }

    fn build_search_input_with_binder(
        &self,
        alternatives: Vec<LogicalAlternative>,
        statement_layer: &QueryStatementLayer,
        binder: &Binder,
    ) -> Result<crate::cascades::OptimizationInput> {
        let mut input =
            MemoBuilder::build_with_search(alternatives, binder, self.budget.clone(), &self.ctx)?
                .with_calibration(self.calibration.clone())
                .with_force_spill(self.ctx.session.limits.force_external);
        if let Some(write) = statement_layer.write_contract() {
            if let crate::physical::requirements::MutationSafetyRequirement::StableReadBeforeWrite {
                targets,
                snapshot,
            } = &write.mutation_safety
            {
                input = input.require_stable_mutation_input(targets.clone(), *snapshot)?;
            }
        }
        Ok(input)
    }

    fn attach_statement_layer(
        &self,
        variant: &mut crate::cascades::OptimizedVariant,
        layer: &QueryStatementLayer,
    ) -> Result<()> {
        if matches!(layer, QueryStatementLayer::Query) {
            return Ok(());
        }
        let child_contract = extracted_root_contract(variant)?.clone();
        let child_estimate = variant.plan.stats.estimated_cardinality;
        let child = std::mem::replace(
            &mut variant.plan,
            OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
        );
        let mut statement = layer.attach(child);
        if let Some(cardinality) = layer.result_cardinality() {
            statement.stats.estimated_cardinality = Some(cardinality);
        }
        let local_cost = self.statement_local_cost(layer.stable_tag(), child_estimate)?;
        let cost = child_contract.cost.sequential(local_cost)?;
        let physical_fingerprint = statement_fingerprint(
            layer,
            child_contract.physical_fingerprint,
            layer.write_contract(),
        );
        let contract = statement_contract(child_contract.grant, physical_fingerprint, cost);
        Arc::make_mut(&mut variant.contracts).insert(statement.id, contract);
        if let Some(write) = layer.write_contract() {
            Arc::make_mut(&mut variant.write_contracts).insert(statement.id, write.clone());
        }
        variant.plan = statement;
        variant.physical_fingerprint = physical_fingerprint;
        variant.cost = cost;
        Ok(())
    }

    fn attach_explain_layer(
        &self,
        variant: &mut crate::cascades::OptimizedVariant,
        explain: &ExplainEnvelope,
    ) -> Result<()> {
        let child_contract = extracted_root_contract(variant)?.clone();
        let child = std::mem::replace(
            &mut variant.plan,
            OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
        );
        let plan = explain.attach(child);
        let local_cost = self.statement_local_cost(100, None)?;
        let cost = child_contract.cost.sequential(local_cost)?;
        let mut fingerprint = StableFingerprintBuilder::default();
        fingerprint.write_bytes(b"paro.statement.explain.v1");
        fingerprint.write_fingerprint(child_contract.physical_fingerprint);
        fingerprint.write_u64(explain.spec.mode as u64);
        fingerprint.write_u64(explain.spec.format as u64);
        fingerprint.write_u64(explain.spec.detail.verbose as u64);
        fingerprint.write_u64(explain.spec.detail.summary as u64);
        fingerprint.write_u64(explain.spec.detail.timing as u64);
        fingerprint.write_u64(explain.spec.detail.memory as u64);
        let physical_fingerprint = fingerprint.finish();
        Arc::make_mut(&mut variant.contracts).insert(
            plan.id,
            statement_contract(child_contract.grant, physical_fingerprint, cost),
        );
        variant.plan = plan;
        variant.physical_fingerprint = physical_fingerprint;
        variant.cost = cost;
        Ok(())
    }

    fn statement_local_cost(
        &self,
        stable_tag: u64,
        cardinality: Option<paro_planner::plan::CardinalityEstimate>,
    ) -> Result<SearchCost> {
        let rows = match cardinality {
            Some(cardinality) => CompactRange::new(
                cardinality.min as f64,
                cardinality.expected as f64,
                cardinality.max as f64,
            )?,
            None => CompactRange::point(1.0)?,
        };
        let mut work = LocalOperatorWork::default();
        work.add(OpClassId(1_000 + stable_tag as u32), rows)?;
        self.calibration.fold(&work)
    }

    fn extract_utility(
        &self,
        utility: OwnedLogicalPlan,
        grant_classes: &[ResourceGrantClass],
    ) -> Result<PhysicalPlanPortfolio> {
        let mut class_plans = Vec::with_capacity(grant_classes.len());
        for grant in grant_classes {
            let mut logical = duplicate_plan_preserving_indices(
                &utility,
                self.binder.bind_context.shared().as_ref(),
            );
            crate::physical::slot_assignment::assign_expression_slots(&mut logical.operator)?;
            let max_memory = usize::try_from(grant.hard_memory_bytes).unwrap_or(usize::MAX);
            let plan = PhysicalPlanExtractor::new(ExtractionContext {
                force_external: self.ctx.session.limits.force_external,
                grant_spill_policy: grant.spill_policy,
                rowset_scan_pushdown: self.ctx.session.limits.rowset_scan_pushdown,
                max_memory,
                max_threads: usize::from(grant.max_parallel_tasks),
                scan_access_cost: Default::default(),
                dependency_template: self.plan_dependency_template_for(&logical)?,
            })
            .extract(&logical)?;
            let root = plan.properties.get(plan.root).ok_or_else(|| {
                paro_common::error::internal("utility physical plan has no root contract")
            })?;
            let cost = root.cumulative_cost;
            let fingerprint =
                plan.portfolio_fingerprint(utility_plan_fingerprint(&plan.node(plan.root).kind)?)?;
            class_plans.push((grant.id, plan, fingerprint, cost));
        }
        let portfolio = PhysicalPlanPortfolio::build(
            crate::physical::ObjectiveProfile::Latency,
            grant_classes.iter().copied(),
            class_plans,
        )?;
        portfolio.verify()?;
        Ok(portfolio)
    }

    fn extract_physical(
        &self,
        variants: Vec<crate::cascades::OptimizedVariant>,
        grant_classes: &[ResourceGrantClass],
        grant_search: Option<crate::physical::GrantSearchCoverage>,
    ) -> Result<PhysicalPlanPortfolio> {
        let class_map = grant_classes
            .iter()
            .map(|class| (class.id, *class))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut class_plans = Vec::with_capacity(variants.len());
        for mut variant in variants {
            let grant = class_map.get(&variant.class).ok_or_else(|| {
                paro_common::error::internal(
                    "planner extraction references an unknown resource grant class",
                )
            })?;
            let root_contract = variant
                .enforcers
                .get(&variant.plan.id)
                .and_then(|chain| chain.last())
                .map(|enforcer| &enforcer.contract)
                .or_else(|| variant.contracts.get(&variant.plan.id))
                .cloned()
                .ok_or_else(|| {
                    paro_common::error::internal("root winner has no physical contract")
                })?;
            if root_contract.physical_fingerprint != variant.physical_fingerprint
                || root_contract.cost != variant.cost
            {
                return Err(paro_common::error::internal(
                    "extracted root contract disagrees with its grant winner",
                ));
            }
            if matches!(
                root_contract.grant,
                crate::physical::PhysicalGrantContract::Class(class) if class != variant.class
            ) {
                return Err(paro_common::error::internal(
                    "class-specific root winner was extracted for the wrong grant class",
                ));
            }
            let max_memory = usize::try_from(grant.hard_memory_bytes).unwrap_or(usize::MAX);
            crate::physical::slot_assignment::assign_expression_slots(&mut variant.plan.operator)?;
            let dependency_template = self.plan_dependency_template_for(&variant.plan)?;
            let plan = PhysicalPlanExtractor::new(ExtractionContext {
                force_external: self.ctx.session.limits.force_external,
                grant_spill_policy: grant.spill_policy,
                rowset_scan_pushdown: self.ctx.session.limits.rowset_scan_pushdown,
                max_memory,
                max_threads: usize::from(grant.max_parallel_tasks),
                scan_access_cost: Default::default(),
                dependency_template,
            })
            .with_winner_contracts(variant.contracts)
            .with_enforcer_contracts(variant.enforcers)
            .with_statement_write_contracts(variant.write_contracts)
            .requiring_winner_contracts()
            .extract(&variant.plan)?;
            let fingerprint = plan.portfolio_fingerprint(variant.physical_fingerprint)?;
            class_plans.push((variant.class, plan, fingerprint, variant.cost));
        }
        let portfolio = PhysicalPlanPortfolio::build(
            crate::physical::ObjectiveProfile::Latency,
            grant_classes.iter().copied(),
            class_plans,
        )?;
        let portfolio = portfolio.with_grant_search(grant_search)?;
        portfolio.verify()?;
        Ok(portfolio)
    }

    fn plan_dependency_template(&self) -> crate::physical::PlanDependencies {
        fn revision(domain: &[u8], values: impl IntoIterator<Item = u64>) -> Fingerprint {
            let mut fingerprint = StableFingerprintBuilder::default();
            fingerprint.write_bytes(domain);
            for value in values {
                fingerprint.write_u64(value);
            }
            fingerprint.finish()
        }

        let mut calibration = StableFingerprintBuilder::default();
        calibration.write_bytes(b"paro.machine-calibration.v1");
        calibration.write_u64(self.calibration.revision.0 as u64);
        calibration.write_bytes(self.calibration.hardware_class.as_bytes());
        calibration.write_bytes(self.calibration.corpus_id.as_bytes());
        calibration.write_bytes(self.calibration.provenance.as_bytes());

        let budget = &self.budget;
        let config_values = [
            u64::from(budget.max_optional_groups_per_initial_group),
            u64::from(budget.max_optional_composition_groups_per_initial_group),
            u64::from(budget.max_optional_logical_exprs_per_group),
            u64::from(budget.max_optional_composition_logical_exprs_per_group),
            u64::from(budget.max_optional_physical_exprs_per_group),
            u64::from(budget.max_optional_interesting_goals_per_group),
            u64::from(budget.max_rule_firings_per_group),
            u64::from(budget.max_composition_rule_firings_per_group),
            u64::from(budget.max_rule_work_units_per_group),
            u64::from(budget.max_composition_rule_work_units_per_group),
            u64::from(budget.max_child_frontier_combinations_per_group),
            u64::from(budget.max_join_connected_pairs),
            u64::from(budget.max_join_exact_relations),
            u64::from(budget.join_beam_width),
            u64::from(budget.max_graph_frontiers),
            u64::from(budget.max_factorization_variants),
            u64::from(budget.max_multiway_join_candidates),
            u64::from(budget.max_search_candidates),
            u64::from(budget.max_search_fusions),
            u64::from(budget.max_parameter_contexts),
            u64::from(budget.max_optional_region_product_depth),
            u64::from(budget.max_composite_region_groups),
            u64::from(budget.max_mandatory_region_groups),
            u64::from(budget.max_composite_region_candidates),
            u64::from(budget.max_recursive_candidates),
            u64::from(budget.max_optional_enforcer_depth),
            u64::from(budget.max_optional_enforcer_chains_per_goal),
            u64::from(budget.max_grant_classes),
            self.ctx.session.limits.max_threads as u64,
            self.ctx.session.limits.rowset_scan_pushdown as u64,
            self.ctx.session.limits.force_external as u64,
            self.ctx.session.limits.use_temporary_directory as u64,
        ];
        crate::physical::PlanDependencies {
            machine_calibration_revision: calibration.finish(),
            estimator_revision: revision(b"paro.estimator-algebra", [1]),
            rule_set_revision: revision(b"paro.rule-set", [4]),
            plan_stability_policy_revision: revision(b"paro.plan-stability-policy", [1]),
            optimizer_config_fingerprint: revision(
                b"paro.optimizer-config",
                config_values
                    .into_iter()
                    .chain(std::iter::once(
                        budget.disabled_transformation_rules.len() as u64
                    ))
                    .chain(
                        budget
                            .disabled_transformation_rules
                            .iter()
                            .map(|rule| rule.0 as u64),
                    ),
            ),
            physical_abi_revision: revision(b"paro.physical-abi", [4]),
            ..Default::default()
        }
    }

    fn plan_dependency_template_for(
        &self,
        plan: &OwnedLogicalPlan,
    ) -> Result<crate::physical::PlanDependencies> {
        fn graph_key(id: &GraphId) -> Fingerprint {
            let mut fingerprint = StableFingerprintBuilder::default();
            fingerprint.write_bytes(b"paro.graph-generation.v1");
            fingerprint.write_bytes(id.runtime_key().as_bytes());
            fingerprint.finish()
        }

        fn collect(
            optimizer: &Optimizer,
            plan: &OwnedLogicalPlan,
            dependencies: &mut crate::physical::PlanDependencies,
        ) {
            if let LogicalOperator::GraphScan(scan) = &plan.operator {
                let id = GraphId::new(
                    optimizer.ctx.session.current_database(),
                    &scan.schema_name,
                    &scan.graph_name,
                );
                if let Some(snapshot) = optimizer.ctx.session.graph_snapshot(&id) {
                    dependencies
                        .graph_generations
                        .insert(graph_key(&id), snapshot.generation_id());
                }
            }
            for child in plan.children() {
                collect(optimizer, child, dependencies);
            }
        }

        let mut dependencies = self.plan_dependency_template();
        collect(self, plan, &mut dependencies);
        for table in crate::search::optimizer::SearchOptimizer::planning_observation_tables(plan)? {
            let object = {
                let mut fingerprint = StableFingerprintBuilder::default();
                fingerprint.write_u64(1);
                fingerprint.write_u64(table.object_id().raw());
                fingerprint.finish()
            };
            dependencies.search_planning_signatures.insert(
                object,
                table
                    .storage
                    .as_ref()
                    .map_or(0, |storage| storage.search_planning_signature()),
            );
        }
        Ok(dependencies)
    }

    /// Only canonical, mandatory semantic work belongs here. Cost alternatives
    /// are owned by the implementation registry after this boundary.
    fn canonicalize_query(&mut self, mut plan: OwnedLogicalPlan) -> Result<OwnedLogicalPlan> {
        if self.ctx.verify_enabled {
            verify_logical_plan(&self.ctx.bind_context, &plan)?;
        }

        // GraphMatch is still a planner surface node today, while the executor
        // consumes canonical graph scan/expand semantics. The decomposition is
        // mandatory lowering; start/access alternatives belong in graph region
        // implementations and are not selected here.
        plan = GraphMatchDecompose::new().optimize_plan(plan);
        plan = GraphPredicatePushdown::new().optimize_plan(plan);

        normalize_scalar_expressions(&mut plan);

        CommonAggregateOptimizer::new().optimize(&mut plan);
        plan = DelimJoinElimination::canonical().optimize_plan(plan);
        plan = EmptyResultPullup::new().optimize_plan(plan);
        plan = JoinPredicateNormalizer::new(&self.ctx.bind_context).optimize_plan(plan)?;
        plan = InClauseRewriter::new().rewrite(plan)?;
        // Predicate ownership is part of canonical relational semantics, not
        // a cost alternative. In particular, side-local predicates above a
        // delim/MARK boundary must reach that side before join normalization;
        // otherwise comma joins remain executable cross products with a late
        // filter and can create unbounded intermediates.
        plan = FilterPushdown::new().rewrite_plan(plan);
        plan = JoinPredicateNormalizer::new(&self.ctx.bind_context).optimize_plan(plan)?;
        plan = ExternalRoutineLoweringPass::lower(plan, &self.ctx.bind_context)?.plan;
        plan = CTEInlining::new(&self.ctx.bind_context)
            .single_reference_defaults()
            .optimize_plan(plan);
        // Mandatory substitution creates fresh filter/projection/set
        // boundaries. Canonicalize predicate placement before Query IR
        // construction just as Memo does for optional multi-consumer choices.
        plan = FilterPushdown::new().rewrite_plan(plan);
        normalize_scalar_expressions(&mut plan);
        plan = FilterPushdown::new().rewrite_plan(plan);
        plan = EmptyResultPullup::new().optimize_plan(plan);
        if self.ctx.verify_enabled {
            verify_logical_plan(&self.ctx.bind_context, &plan)?;
        }
        Ok(plan)
    }

    fn estimate_query_candidate(&self, mut plan: OwnedLogicalPlan) -> Result<CandidatePlan> {
        let mut context = self.ctx.fork_for_candidate(Arc::new(HashMap::new()));

        plan = StatisticsGathering::new().gather(plan, &mut context)?;
        let mut propagator = StatisticsPropagator::new();
        plan = propagator.propagate(context.session.clone(), plan);
        context.column_stats = Arc::new(propagator.take_statistics_map());
        plan = StatisticsGathering::new().gather(plan, &mut context)?;

        Ok(CandidatePlan {
            plan,
            column_stats: context.column_stats,
        })
    }

    fn settle_schema_candidate(&self, mut plan: OwnedLogicalPlan) -> Result<CandidatePlan> {
        // Optional region rewrites and CTE substitution can expose a new
        // Filter(CrossProduct) boundary after the initial semantic pass. Keep
        // the Query-IR boundary canonical so equality edges always reach join
        // enumeration and physical implementation selection.
        plan = JoinPredicateNormalizer::new(&self.ctx.bind_context).optimize_plan(plan)?;
        // Dependent-join flattening can preserve strict equality as a
        // null-safe comparison plus an explicit null rejection. Recover the
        // canonical equality before costing and physical artifact selection.
        plan = crate::join::null_rejected_equality::optimize_plan(plan).0;
        // Query-IR output contracts are demand driven. Until every planner
        // operator natively exposes ColumnIds, derive the same canonical
        // demand projection once at the Query IR boundary; this is not an
        // optional cost rewrite and never removes an observable evaluation.
        RemoveUnusedColumns::optimize(&mut plan, &self.binder, self.ctx.session.as_ref(), true);

        self.estimate_query_candidate(plan)
    }

    fn finalize_query_candidate(&self, mut candidate: CandidatePlan) -> Result<CandidatePlan> {
        candidate.plan = normalize_iteration_ownership(candidate.plan)?;
        candidate.plan = singleton_groups::optimize_plan(candidate.plan)?;
        candidate.plan = ColumnLifetimeAnalyzer::new(true).optimize(candidate.plan)?;

        if self.ctx.verify_enabled {
            verify_logical_plan(&self.ctx.bind_context, &candidate.plan)?;
        }
        Ok(candidate)
    }

    fn settle_query_candidate(&self, plan: OwnedLogicalPlan) -> Result<CandidatePlan> {
        let candidate = self.settle_schema_candidate(plan)?;
        self.finalize_query_candidate(candidate)
    }

    fn distinct_aggregate_feasibility_candidate(
        &self,
        plan: OwnedLogicalPlan,
    ) -> Result<Option<CandidatePlan>> {
        let (plan, changed) = distinct_decomposition::optimize_plan(plan, &self.ctx.bind_context)?;
        if !changed {
            return Ok(None);
        }
        self.settle_query_candidate(plan).map(Some)
    }

    fn settle_relational_baseline(&self, plan: OwnedLogicalPlan) -> Result<CandidatePlan> {
        // Direct two-valued existence decorrelation and sibling-marker folding
        // erase only delimiter carriers and multiplicity that no SQL result can
        // observe. They therefore define the baseline relational form rather
        // than a cost choice. Specialized correlated candidates are forked
        // before this boundary and retain the carrier required by their proofs.
        let plan = DelimJoinElimination::projected_existence().optimize_plan(plan);
        // Marker observability is encoded by executable projection maps. Settle
        // once before recognizing a disjunction, then rebuild statistics and
        // layouts exactly once if either normalization changes the tree.
        let candidate = self.settle_query_candidate(plan)?;
        let (plan, disjunction_changed) = crate::subquery::existence_disjunction::optimize_plan(
            candidate.plan,
            &self.ctx.bind_context,
        )?;
        let (plan, reduction_changed) = crate::subquery::existence_reduction::optimize_plan(plan)?;
        if !disjunction_changed && !reduction_changed {
            return Ok(CandidatePlan {
                plan,
                column_stats: candidate.column_stats,
            });
        }
        self.settle_query_candidate(plan)
    }

    fn correlated_aggregate_candidate(&self, plan: OwnedLogicalPlan) -> Result<CandidatePlan> {
        let input_shape = tracing::enabled!(target: targets::OPTIMIZER, tracing::Level::DEBUG)
            .then(|| logical_plan_shape(&plan));
        let ordered = JoinOrderOptimizer::new(self.ctx.cost_model.defaults.clone())
            .with_search_budget(&self.budget)
            .optimize_plan(
                self.ctx.session.as_ref(),
                plan,
                &self.ctx.column_stats,
                &self.ctx.bind_context,
            )?;
        let candidate = CorrelatedPartitionAggregate::new(self.ctx.bind_context.clone())
            .optimize_plan(ordered)?;
        let candidate = CTEInlining::new(&self.ctx.bind_context)
            .single_reference_defaults()
            .optimize_plan(candidate);
        if let Some(input_shape) = input_shape {
            debug!(
                target: targets::OPTIMIZER,
                %input_shape,
                output_shape = %logical_plan_shape(&candidate),
                "enumerated correlated aggregate region"
            );
        }
        self.settle_query_candidate(candidate)
    }

    fn correlated_topn_payload_candidate(
        &self,
        mut candidate: CandidatePlan,
    ) -> Result<Option<CandidatePlan>> {
        // This is a registered, finite compound recipe. Keep the plain
        // decorrelated candidate as a sibling; TopN introduction and payload
        // deferral are costed together only because the latter's proof needs
        // the former's bounded frontier.
        candidate.plan = TopNOptimizer::new().optimize_plan(candidate.plan);
        candidate = self.settle_query_candidate(candidate.plan)?;
        if self.ctx.session.settings.rowset_scan_pushdown() {
            let (plan, prefix_changed) =
                late_payload::optimize_matched_prefix_plan(candidate.plan)?;
            let (plan, payload_changed) =
                late_payload::optimize_plan(plan, &self.ctx.bind_context, &self.ctx.cost_model)?;
            if prefix_changed || payload_changed {
                return self.settle_query_candidate(plan).map(Some);
            }
        }
        Ok(None)
    }

    fn prepare_correlated_seed(&self, plan: OwnedLogicalPlan) -> OwnedLogicalPlan {
        // Correlated-region exploration must preserve sharing ownership. A
        // CTE reference is a semantic relation leaf whose producer statistics
        // and execution contract remain owned by MaterializedCTE; duplicating
        // the producer here makes decorrelation and sharing mutually exclusive
        // alternatives for no semantic reason.
        let mut candidate = FilterPullup::new().rewrite_plan(plan);
        candidate = FilterPushdown::new().rewrite_plan(candidate);
        candidate
    }

    fn scalar_reuse_candidate(&self, candidate: CandidatePlan) -> Result<CandidatePlan> {
        let context = self.ctx.fork_for_candidate(candidate.column_stats.clone());
        let mut candidate = JoinOrderOptimizer::new(self.ctx.cost_model.defaults.clone())
            .with_search_budget(&self.budget)
            .optimize_plan(
                context.session.as_ref(),
                candidate.plan,
                &context.column_stats,
                &context.bind_context,
            )?;
        candidate = scalar_aggregate_fusion::optimize_plan(candidate, &self.ctx.bind_context)?;
        candidate = scalar_aggregate_window::optimize_plan(candidate, &self.ctx.bind_context)?;
        if self.ctx.session.settings.rowset_scan_pushdown() {
            (candidate, _) = late_payload::optimize_matched_prefix_plan(candidate)?;
        }
        self.settle_query_candidate(candidate)
    }
}

/// Transformations may introduce a fresh output table index for a selected
/// CTE consumer even though its result is the same positional SQL schema as
/// the original bound root. Rebase only the output-producing Projection on a
/// pass-through path before staging the immutable seed in the destination
/// Memo; input bindings and scalar semantics are left untouched.
fn rebase_seed_output_table_index(
    plan: OwnedLogicalPlan,
    table_index: usize,
) -> Result<OwnedLogicalPlan> {
    let child_index = match &plan.operator {
        LogicalOperator::MaterializedCTE(_) => Some(1),
        LogicalOperator::Filter(_)
        | LogicalOperator::Order(_)
        | LogicalOperator::TopN(_)
        | LogicalOperator::Limit(_)
        | LogicalOperator::Distinct(_)
        | LogicalOperator::EmptyResult(_) => Some(0),
        _ => None,
    };
    if let Some(child_index) = child_index {
        let mut ordinal = 0;
        return plan.try_map_children(|child| {
            let current = ordinal;
            ordinal += 1;
            if current == child_index {
                rebase_seed_output_table_index(child, table_index)
            } else {
                Ok(child)
            }
        });
    }

    if matches!(plan.operator, LogicalOperator::Projection(_)) {
        return plan.try_map_operator(|operator| {
            let LogicalOperator::Projection(mut projection) = operator else {
                unreachable!("projection output rebasing matched another operator")
            };
            projection.table_index = table_index;
            Ok(LogicalOperator::Projection(projection))
        });
    }

    Err(paro_error::internal(
        "strong seed result layout has no safe output-producing projection",
    ))
}

fn observes_optimizer_diagnostics(plan: &OwnedLogicalPlan) -> bool {
    matches!(
        &plan.operator,
        LogicalOperator::TableFunctionGet(function)
            if function.function_name().eq_ignore_ascii_case("paro_optimizers")
    ) || plan
        .children()
        .into_iter()
        .any(observes_optimizer_diagnostics)
}

fn contains_redundant_computation_region(plan: &OwnedLogicalPlan) -> bool {
    let local_candidate = match &plan.operator {
        LogicalOperator::Join(Join::Comparison(join)) if join.join_type == JoinType::Single => {
            contains_aggregate(&join.right)
        }
        LogicalOperator::Projection(projection) => projection.expressions.iter().any(|expression| {
            matches!(
                expression,
                paro_planner::expression::Expression::Function(function)
                    if matches!(
                        function.function.predicate_projection.as_ref(),
                        Some(paro_function::scalar::ScalarPredicateProjection::Utf8Substring { .. })
                    )
            )
        }),
        _ => false,
    };
    local_candidate
        || plan
            .children()
            .into_iter()
            .any(contains_redundant_computation_region)
}

fn contains_aggregate(plan: &OwnedLogicalPlan) -> bool {
    matches!(plan.operator, LogicalOperator::Aggregate(_))
        || plan.children().into_iter().any(contains_aggregate)
}

fn logical_plan_shape(plan: &OwnedLogicalPlan) -> String {
    use std::fmt::Write;

    fn append(plan: &OwnedLogicalPlan, output: &mut String) {
        match &plan.operator {
            LogicalOperator::Join(Join::Comparison(join)) => {
                let _ = write!(
                    output,
                    "Join({:?},delim={},flipped={})",
                    join.join_type,
                    join.duplicate_eliminated_columns.len(),
                    join.delim_flipped
                );
            }
            operator => {
                let _ = write!(output, "{:?}", operator.op_type());
            }
        }
        let children = plan.children();
        if children.is_empty() {
            return;
        }
        output.push('[');
        for (index, child) in children.into_iter().enumerate() {
            if index > 0 {
                output.push(',');
            }
            append(child, output);
        }
        output.push(']');
    }

    let mut output = String::new();
    append(plan, &mut output);
    output
}

/// Build the bounded GraphPatternRegion candidate product before mandatory
/// GraphMatch decomposition.  The binder order is retained as candidate zero;
/// every additional frontier is only a certified equivalent input to Memo.
fn enumerate_graph_region_plans(
    plan: OwnedLogicalPlan,
    bind_context: &paro_planner::binder::context::BindContext,
    max_optional_frontiers: u32,
) -> Result<Vec<OwnedLogicalPlan>> {
    type PatternOrder = Vec<paro_planner::binder::bind::graph::BoundPatternElement>;

    fn collect(
        plan: &OwnedLogicalPlan,
        patterns: &mut Vec<Vec<PatternOrder>>,
        per_pattern_max: usize,
    ) {
        if let LogicalOperator::GraphMatch(graph_match) = &plan.operator {
            patterns.push(
                GraphFrontierEnumerator::new()
                    .enumerate_pattern_orders(graph_match, per_pattern_max),
            );
        }
        for child in plan.children() {
            collect(child, patterns, per_pattern_max);
        }
    }

    fn replace_nth(
        plan: &mut OwnedLogicalPlan,
        target: usize,
        seen: &mut usize,
        order: &[paro_planner::binder::bind::graph::BoundPatternElement],
    ) -> bool {
        if let LogicalOperator::GraphMatch(graph_match) = &mut plan.operator {
            if *seen == target {
                graph_match.bound_pattern.elements = order.to_vec();
                return true;
            }
            *seen += 1;
        }
        let mut replaced = false;
        let _ = plan.visit_children_mut(|child| {
            if replace_nth(child, target, seen, order) {
                replaced = true;
                std::ops::ControlFlow::Break(())
            } else {
                std::ops::ControlFlow::Continue(())
            }
        });
        replaced
    }

    let ceiling = (max_optional_frontiers as usize).saturating_add(1);
    let mut pattern_orders = Vec::new();
    collect(&plan, &mut pattern_orders, ceiling);
    let mut plans = vec![plan];
    for (ordinal, orders) in pattern_orders.iter().enumerate() {
        let mut expanded = Vec::new();
        for baseline in plans {
            let mut variants = vec![baseline];
            for order in orders.iter().skip(1) {
                if expanded.len().saturating_add(variants.len()) >= ceiling {
                    break;
                }
                let mut candidate =
                    duplicate_plan_preserving_indices(&variants[0], bind_context.shared().as_ref());
                let mut seen = 0;
                if !replace_nth(&mut candidate, ordinal, &mut seen, order) {
                    return Err(paro_common::error::internal(
                        "graph region candidate lost its GraphMatch boundary",
                    ));
                }
                variants.push(candidate);
            }
            expanded.extend(variants);
            if expanded.len() >= ceiling {
                expanded.truncate(ceiling);
                break;
            }
        }
        plans = expanded;
    }
    Ok(plans)
}

fn extracted_root_contract(
    variant: &crate::cascades::OptimizedVariant,
) -> Result<&WinnerPhysicalContract> {
    variant
        .enforcers
        .get(&variant.plan.id)
        .and_then(|chain| chain.last())
        .map(|enforcer| &enforcer.contract)
        .or_else(|| variant.contracts.get(&variant.plan.id))
        .ok_or_else(|| paro_common::error::internal("extracted root has no winner contract"))
}

fn statement_contract(
    grant: PhysicalGrantContract,
    physical_fingerprint: Fingerprint,
    cost: SearchCost,
) -> WinnerPhysicalContract {
    WinnerPhysicalContract {
        required: RequiredProperties::default(),
        provided: ProvidedProperties {
            ordering: ProvidedOrdering::Unordered,
            partitioning: ProvidedPartitioning::Singleton,
            materialization: ProvidedMaterialization::default(),
            mutation_safety: ProvidedMutationSafety::NotApplicable,
            representation: ProvidedRepresentation::Flat,
            replayability: ProvidedReplayability::OnePass,
            result_guarantee: ResultGuarantee::Exact,
        },
        cost,
        grant,
        origin: PlanOrigin::StatementLowering,
        goal_fingerprint: physical_fingerprint,
        physical_fingerprint,
        implementation: PhysicalImplementationFlavor::Structural,
        region_owner: None,
        owned_artifacts: Box::new([]),
    }
}

fn statement_fingerprint(
    layer: &QueryStatementLayer,
    child: Fingerprint,
    write: Option<&crate::physical::WriteContract>,
) -> Fingerprint {
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_bytes(b"paro.statement.query-wrapper.v1");
    fingerprint.write_u64(layer.stable_tag());
    fingerprint.write_fingerprint(child);
    if let Some(write) = write {
        fingerprint.write_u64(write.target_object_id);
        fingerprint.write_u64(write.snapshot_version);
        fingerprint.write_u64(write.modified_columns.len() as u64);
        for column in &write.modified_columns {
            fingerprint.write_u64(*column as u64);
        }
        fingerprint.write_u64(write.modified_key_columns.len() as u64);
        for column in &write.modified_key_columns {
            fingerprint.write_u64(*column as u64);
        }
        fingerprint.write_u64(match write.returning {
            crate::physical::ReturningImageContract::CountOnly => 0,
            crate::physical::ReturningImageContract::BeforeImage => 1,
            crate::physical::ReturningImageContract::AfterImage => 2,
        });
    }
    if let QueryStatementLayer::CopyTo { file_path, .. } = layer {
        fingerprint.write_bytes(file_path.as_bytes());
    }
    fingerprint.finish()
}

fn utility_plan_fingerprint(kind: &crate::physical::PhysicalNodeKind) -> Result<Fingerprint> {
    use crate::physical::{PhysicalNodeKind, UtilitySpec};

    let tag = match kind {
        PhysicalNodeKind::DummyScan(_) => 0,
        PhysicalNodeKind::Utility(utility) => match utility {
            UtilitySpec::CreateTable(_) => 1,
            UtilitySpec::CreateView(_) => 2,
            UtilitySpec::CreateSchema(_) => 3,
            UtilitySpec::CreateSequence(_) => 4,
            UtilitySpec::CreateIndex(_) => 5,
            UtilitySpec::CreateRoutine(_) => 6,
            UtilitySpec::CreatePropertyGraph(_) => 7,
            UtilitySpec::Alter(_) => 8,
            UtilitySpec::Drop(_) => 9,
            UtilitySpec::DropPropertyGraph(_) => 10,
            UtilitySpec::RefreshPropertyGraph(_) => 11,
        },
        _ => {
            return Err(paro_common::error::internal(
                "utility extraction produced a non-utility physical root",
            ));
        }
    };
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_bytes(b"paro.statement.utility-plan.v2");
    fingerprint.write_u64(tag);
    Ok(fingerprint.finish())
}

/// Versioned policy surface for bounded portfolio search. A configured
/// statement limit yields low/medium/full classes; an unspecified limit is a
/// single conservative unbounded class so admission remains deterministic.
fn resource_grant_classes(
    max_memory: usize,
    max_threads: usize,
    max_grant_classes: u8,
    spill_available: bool,
) -> Box<[ResourceGrantClass]> {
    paro_context::compile_grant_classes(max_memory, max_threads, max_grant_classes)
        .into_iter()
        .map(|class| ResourceGrantClass {
            id: ResourceGrantClassId::new(class.index),
            hard_memory_bytes: class.hard_memory_bytes,
            spill_policy: if spill_available {
                SpillPolicy::Allowed
            } else {
                SpillPolicy::Forbidden
            },
            max_parallel_tasks: class.max_parallel_tasks,
        })
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

#[cfg(test)]
mod resource_operating_point_tests {
    use super::resource_grant_classes;

    #[test]
    fn lazy_grants_default_operating_points_match_frozen_cache_selection() {
        let budget = crate::cascades::budget::SearchBudget::default();
        assert_eq!(
            budget.max_grant_classes, 3,
            "update the frozen compile-key contract with any default policy change"
        );
        let mut context = paro_context::TestStatementContextBuilder::minimal()
            .build()
            .as_ref()
            .clone();
        context.limits.max_memory = 1024;
        context.limits.max_threads = 4;
        let classes = resource_grant_classes(1024, 4, budget.max_grant_classes, false);
        for memory in [0, 255, 256, 511, 512, 1024, 2048] {
            for tasks in [0, 1, 2, 4, 8] {
                context.compile_resources = paro_context::CompileResources::capture(memory, tasks);
                let key = context.compile_environment_key();
                let selected = context.compile_resources.expected_grant(
                    1024, 4, budget.max_grant_classes,
                );
                assert_eq!(key.expected_grant, selected);
                let actual = classes.iter()
                    .filter(|class| class.hard_memory_bytes <= memory as u64
                        && usize::from(class.max_parallel_tasks) <= tasks)
                    .max_by_key(|class| (class.max_parallel_tasks, class.hard_memory_bytes, class.id));
                assert_eq!(
                    selected.map(|class| class.index),
                    actual.map(|class| class.id.index())
                );
            }
        }
    }

    #[test]
    fn low_memory_portfolio_contains_a_serial_executable_operating_point() {
        let classes = resource_grant_classes(4 * 1024 * 1024, 10, 3, true);

        assert_eq!(
            classes
                .iter()
                .map(|class| class.max_parallel_tasks)
                .collect::<Vec<_>>(),
            vec![1, 5, 10]
        );
        assert_eq!(classes[0].hard_memory_bytes, 1024 * 1024);
        assert_eq!(classes[2].hard_memory_bytes, 4 * 1024 * 1024);
    }
}
