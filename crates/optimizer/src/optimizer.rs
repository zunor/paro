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
use paro_context::compile_diagnostics::{Observation, SearchStop};
use paro_context::{CompileReceiptSummary, StatementContext};
use paro_planner::binder::deep_copy::{
    duplicate_plan_preserving_indices, fork_plan_preserving_indices,
};
use paro_planner::binder::Binder;
use paro_planner::operator::{Join, JoinType, LogicalOperator, LogicalOperatorType};
use paro_planner::plan::OwnedLogicalPlan;
use paro_planner::verify::verify_physical_planner_invariants;
use paro_storage::statistics::ColumnStatistics;
use tracing::debug;

use crate::cascades::{
    AlternativeOrigin, CompactRange, LocalOperatorWork, LogicalAlternative,
    MachineCalibrationBundle, MemoBuilder, OpClassId, PricedIncumbent, SearchBudget, SearchCost,
    GRAPH_REGION_ENUMERATOR_RULE,
};
use crate::context::OptimizationContext;
use crate::diagnostics::profile::{publish_optimizer_profile_snapshot, OptimizerComponent};
use crate::physical::access::late_payload;
use crate::physical::{
    PhysicalBuildContext, PhysicalImplementationFlavor, PhysicalPlanBuilder, WinnerPhysicalContract,
};
use crate::region::graph::GraphFrontierEnumerator;
use crate::region::join::optimizer::JoinOrderOptimizer;
use crate::rewrite::aggregate::common::CommonAggregateOptimizer;
use crate::rewrite::aggregate::{distinct_decomposition, singleton_groups};
use crate::rewrite::column::lifetime::ColumnLifetimeAnalyzer;
use crate::rewrite::column::remove_unused::RemoveUnusedColumns;
use crate::rewrite::cte::inlining::CTEInlining;
use crate::rewrite::cte::iteration::normalize_iteration_ownership;
use crate::rewrite::expr::in_clause::InClauseRewriter;
use crate::rewrite::external::lowering::ExternalRoutineLoweringPass;
use crate::rewrite::graph::match_decompose::GraphMatchDecompose;
use crate::rewrite::graph::predicate_pushdown::GraphPredicatePushdown;
use crate::rewrite::join::mixed_predicates::JoinPredicateNormalizer;
use crate::rewrite::limit::topn::TopNOptimizer;
use crate::rewrite::predicate::pullup::FilterPullup;
use crate::rewrite::predicate::pushdown::FilterPushdown;
use crate::rewrite::subquery::delim_join_elimination::DelimJoinElimination;
use crate::rewrite::subquery::empty_result::EmptyResultPullup;
use crate::rewrite::subquery::partition_aggregate::CorrelatedPartitionAggregate;
use crate::rewrite::subquery::{scalar_aggregate_fusion, scalar_aggregate_window};
use crate::statement::{ExplainEnvelope, QueryStatementLayer, StatementBody, StatementPlan};
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

mod cascades;
mod staged;

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

fn plan_shape_evidence<P: paro_planner::plan::LogicalPlanRead>(plan: &P) -> PlanShapeEvidence {
    let mut evidence = PlanShapeEvidence::default();
    let mut pending = vec![plan];
    while let Some(node) = pending.pop() {
        evidence.nodes = evidence.nodes.saturating_add(1);
        evidence.max_output_width = evidence
            .max_output_width
            .max(node.output_layout().len() as u64);
        match node.operator().op_type() {
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
        node.operator()
            .visit_child_links(&mut |child| pending.push(&**child));
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
    record_plan_shape(trace, prefix, plan_shape_evidence(variant.plan.as_ref()));
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
    compile_receipt: Option<CompileReceiptSummary>,
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
    #[cfg(test)]
    pub(crate) fn prepare_region_for_test(
        mut self,
        plan: OwnedLogicalPlan,
    ) -> Result<(
        OwnedLogicalPlan,
        OptimizationContext,
        Arc<MachineCalibrationBundle>,
    )> {
        let plan = self.canonicalize_query(plan)?;
        let candidate = self.settle_relational_baseline(plan)?;
        self.ctx.column_stats = candidate.column_stats;
        Ok((candidate.plan, self.ctx, self.calibration))
    }

    pub fn new(binder: Binder, session: Arc<StatementContext>) -> Self {
        Self {
            ctx: OptimizationContext::new(session, binder.bind_context.clone()),
            binder,
            budget: SearchBudget::default(),
            calibration: Arc::new(MachineCalibrationBundle::builtin_production()),
            compile_work: Default::default(),
            compile_receipt: None,
        }
    }

    pub fn with_budget(mut self, budget: SearchBudget) -> Self {
        self.budget = budget;
        self
    }

    pub fn compile_work(&self) -> paro_context::CompileWork {
        self.compile_work
    }

    pub fn compile_receipt(&self) -> Option<CompileReceiptSummary> {
        self.compile_receipt
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
        let pre_partition = crate::diagnostics::work::enter(crate::diagnostics::work::Bucket::Pre);
        // Allocation attribution is a planning concern. The global allocator
        // remains installed for observation, while counter updates are scoped
        // to this synchronous compiler operation so execution pays no tax.
        let _allocation_metrics = paro_common::allocator::begin_allocation_metrics();
        if self.budget.search_policy.is_none() {
            self.budget.search_policy = Some(self.ctx.session.settings.optimizer_search_policy()?);
        }
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
                    std::mem::take(&mut self.ctx.profiler).into_snapshot(),
                );
                debug!(
                    target: targets::OPTIMIZER,
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "statement-only optimizer lowering completed"
                );
                return Ok(result);
            }
        };
        let explain = statement.explain;
        if self.budget.search_policy == Some(paro_context::OptimizerSearchPolicy::Pipeline) {
            drop(pre_partition);
            return self.optimize_pipeline(query, statement_layer, explain);
        }
        self.optimize_cascades(
            query,
            statement_layer,
            explain,
            grant_classes,
            started_at,
            pre_partition,
        )
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
        let mut statement = layer.attach(variant.plan.boundary());
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
        variant.plan = crate::physical::selected::SelectedNode::from_local(
            statement,
            vec![variant.plan.clone()],
            true,
        )?;
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
        let plan = explain.attach(variant.plan.boundary());
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
        variant.plan = crate::physical::selected::SelectedNode::from_local(
            plan,
            vec![variant.plan.clone()],
            true,
        )?;
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
            let plan = PhysicalPlanBuilder::new(PhysicalBuildContext {
                force_external: self.ctx.session.limits.force_external,
                grant_spill_policy: grant.spill_policy,
                rowset_scan_pushdown: self.ctx.session.limits.rowset_scan_pushdown,
                max_memory,
                max_threads: usize::from(grant.max_parallel_tasks),
                scan_access_cost: Default::default(),
                dependency_template: self.plan_dependency_template_for(&logical)?,
            })
            .build(logical)?;
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
        let _partition =
            crate::diagnostics::work::enter(crate::diagnostics::work::Bucket::PhysicalLowering);
        let class_map = grant_classes
            .iter()
            .map(|class| (class.id, *class))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut class_plans = Vec::with_capacity(variants.len());
        for variant in variants {
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
            let dependency_template = self.plan_dependency_template_for(variant.plan.as_ref())?;
            let plan = PhysicalPlanBuilder::new(PhysicalBuildContext {
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
            .extract_selected(&variant.plan)?;
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
            match budget.search_policy {
                Some(paro_context::OptimizerSearchPolicy::Pipeline) => 3,
                Some(paro_context::OptimizerSearchPolicy::Regional) => 2,
                Some(paro_context::OptimizerSearchPolicy::QualityCoverage) => 1,
                _ => 0,
            },
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

    fn plan_dependency_template_for<P: paro_planner::plan::LogicalPlanRead>(
        &self,
        plan: &P,
    ) -> Result<crate::physical::PlanDependencies> {
        fn graph_key(id: &GraphId) -> Fingerprint {
            let mut fingerprint = StableFingerprintBuilder::default();
            fingerprint.write_bytes(b"paro.graph-generation.v1");
            fingerprint.write_bytes(id.runtime_key().as_bytes());
            fingerprint.finish()
        }

        fn collect<P: paro_planner::plan::LogicalPlanRead>(
            optimizer: &Optimizer,
            plan: &P,
            dependencies: &mut crate::physical::PlanDependencies,
        ) {
            if let LogicalOperator::GraphScan(scan) = plan.operator() {
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
            plan.operator()
                .visit_child_links(&mut |child| collect(optimizer, &**child, dependencies));
        }

        let mut dependencies = self.plan_dependency_template();
        collect(self, plan, &mut dependencies);
        for table in
            crate::physical::access::optimizer::SearchOptimizer::planning_observation_tables(plan)?
        {
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

        let mut scalar_construction = crate::rewrite::expr::CanonicalScalars::default();
        scalar_construction.normalize_plan(&mut plan);

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
        plan = crate::rewrite::cte::normalize::normalize(plan)?;
        // Mandatory substitution creates fresh filter/projection/set
        // boundaries. Canonicalize predicate placement before Query IR
        // construction just as Memo does for optional multi-consumer choices.
        plan = crate::rewrite::normalize::predicates(plan, &mut scalar_construction);
        plan = crate::rewrite::normalize::finish(plan)?;
        if self.ctx.verify_enabled {
            verify_logical_plan(&self.ctx.bind_context, &plan)?;
        }
        Ok(plan)
    }

    fn estimate_query_candidate(&self, plan: OwnedLogicalPlan) -> Result<CandidatePlan> {
        let mut context = self.ctx.fork_for_candidate(Arc::new(HashMap::new()));
        let plan = crate::estimate::settle_query_properties(plan, &mut context)?;

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
        plan = crate::rewrite::join::null_rejected_equality::optimize_plan(plan).0;
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
        let (plan, disjunction_changed) =
            crate::rewrite::subquery::existence_disjunction::optimize_plan(
                candidate.plan,
                &self.ctx.bind_context,
            )?;
        let (plan, reduction_changed) =
            crate::rewrite::subquery::existence_reduction::optimize_plan(plan)?;
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
                let selected =
                    context
                        .compile_resources
                        .expected_grant(1024, 4, budget.max_grant_classes);
                assert_eq!(key.expected_grant, selected);
                let actual = classes
                    .iter()
                    .filter(|class| {
                        class.hard_memory_bytes <= memory as u64
                            && usize::from(class.max_parallel_tasks) <= tasks
                    })
                    .max_by_key(|class| {
                        (class.max_parallel_tasks, class.hard_memory_bytes, class.id)
                    });
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
