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
use paro_common::error::Result;
use paro_common::identity::GraphId;
use paro_common::logging::targets;
use paro_context::StatementContext;
use paro_planner::binder::deep_copy::duplicate_plan_preserving_indices;
use paro_planner::binder::Binder;
use paro_planner::operator::{Join, JoinType, LogicalOperator};
use paro_planner::plan::LogicalPlan;
use paro_planner::verify::verify_physical_planner_invariants;
use paro_storage::statistics::ColumnStatistics;
use tracing::debug;

use crate::aggregate::common::CommonAggregateOptimizer;
use crate::aggregate::{distinct_decomposition, late_payload, singleton_groups};
use crate::cascades::{
    AlternativeOrigin, CompactRange, LocalOperatorWork, LogicalAlternative,
    MachineCalibrationBundle, MemoBuilder, OpClassId, SearchBudget, SearchCost,
    GRAPH_REGION_ENUMERATOR_RULE, JOIN_REGION_ENUMERATOR_RULE,
};
use crate::column::lifetime::ColumnLifetimeAnalyzer;
use crate::column::remove_unused::RemoveUnusedColumns;
use crate::context::OptimizationContext;
use crate::cte::filter_pusher::CTEFilterPusher;
use crate::cte::inlining::CTEInlining;
use crate::expression::in_clause::InClauseRewriter;
use crate::expression::rewriter::ExpressionRewriter;
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
use crate::rules::arithmetic::ArithmeticSimplificationRule;
use crate::rules::comparison::ComparisonSimplificationRule;
use crate::rules::conjunction::{CommonConjunctionFactorRule, ConjunctionSimplificationRule};
use crate::rules::constant_folding::ConstantFoldingRule;
use crate::rules::move_constants::MoveConstantsRule;
use crate::statement::{ExplainEnvelope, QueryStatementLayer, StatementBody, StatementPlan};
use crate::statistics::gathering::StatisticsGathering;
use crate::statistics::propagator::StatisticsPropagator;
use crate::subquery::delim_join_elimination::DelimJoinElimination;
use crate::subquery::empty_result::EmptyResultPullup;
use crate::subquery::partition_aggregate::CorrelatedPartitionAggregate;
use crate::subquery::scalar_aggregate_window;
use crate::verify::verify_logical_plan;

const CORRELATED_AGGREGATE_REGION_RULE: crate::cascades::RuleId = crate::cascades::RuleId(10_004);
const SCALAR_REUSE_REGION_RULE: crate::cascades::RuleId = crate::cascades::RuleId(10_005);
const DISTINCT_AGGREGATE_FEASIBILITY_RULE: crate::cascades::RuleId =
    crate::cascades::RuleId(10_020);

struct CandidatePlan {
    plan: LogicalPlan,
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

pub struct Optimizer {
    binder: Binder,
    ctx: OptimizationContext,
    budget: SearchBudget,
    calibration: Arc<MachineCalibrationBundle>,
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
        }
    }

    pub fn with_budget(mut self, budget: SearchBudget) -> Self {
        self.budget = budget;
        self
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
        plan: LogicalPlan,
    ) -> Result<LogicalPlan> {
        let prepared = self.prepare_correlated_seed(plan);
        let canonical = self.canonicalize_query(prepared)?;
        Ok(self.correlated_aggregate_candidate(canonical)?.plan)
    }

    #[cfg(test)]
    pub(crate) fn scalar_reuse_frontier_for_test(
        &mut self,
        plan: LogicalPlan,
    ) -> Result<LogicalPlan> {
        let prepared = self.prepare_correlated_seed(plan);
        let canonical = self.canonicalize_query(prepared)?;
        let baseline = self.settle_query_candidate(canonical)?;
        Ok(self.scalar_reuse_candidate(baseline)?.plan)
    }

    pub fn optimize(&mut self, plan: LogicalPlan) -> Result<OptimizedStatement> {
        self.budget.disable_transformations_by_name(
            self.ctx.session.settings.disabled_optimizer_rules(),
        )?;
        let started_at = Instant::now();
        let statement = StatementPlan::split(plan, self.ctx.session.transaction_visible_version())?;
        let grant_classes = resource_grant_classes(
            self.ctx.session.limits.max_memory,
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
        let graph_plans = enumerate_graph_region_plans(
            query,
            &self.binder.bind_context,
            self.budget.max_graph_frontiers,
        )?;
        let mut alternatives = Vec::with_capacity(graph_plans.len().saturating_mul(3));
        for (index, graph_plan) in graph_plans.into_iter().enumerate() {
            let correlated_seed = contains_redundant_computation_region(&graph_plan).then(|| {
                self.prepare_correlated_seed(duplicate_plan_preserving_indices(
                    &graph_plan,
                    self.binder.bind_context.shared().as_ref(),
                ))
            });
            let canonical = self.canonicalize_query(graph_plan)?;
            let baseline = self.settle_query_candidate(duplicate_plan_preserving_indices(
                &canonical,
                self.binder.bind_context.shared().as_ref(),
            ))?;
            let distinct_feasibility_candidate =
                self.distinct_aggregate_feasibility_candidate(duplicate_plan_preserving_indices(
                    &canonical,
                    self.binder.bind_context.shared().as_ref(),
                ))?;
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
            match self.correlated_aggregate_candidate(duplicate_plan_preserving_indices(
                &correlated_canonical,
                self.binder.bind_context.shared().as_ref(),
            )) {
                Ok(plan) => {
                    alternatives.push(plan.into_alternative(AlternativeOrigin::Specialized {
                        rule: CORRELATED_AGGREGATE_REGION_RULE,
                    }))
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
                .settle_query_candidate(duplicate_plan_preserving_indices(
                    &correlated_canonical,
                    self.binder.bind_context.shared().as_ref(),
                ))
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
        let phase_started = Instant::now();
        let mut join_alternatives = Vec::new();
        for alternative in &alternatives {
            if !contains_join_region(&alternative.plan) {
                continue;
            }
            let mut candidate_context = self
                .ctx
                .fork_for_candidate(Arc::unwrap_or_clone(alternative.column_stats.clone()));
            let mut join_candidate = JoinOrderOptimizer::new()
                .with_search_budget(&self.budget)
                .optimize_plan(
                    candidate_context.session.as_ref(),
                    duplicate_plan_preserving_indices(
                        &alternative.plan,
                        self.binder.bind_context.shared().as_ref(),
                    ),
                    &candidate_context.column_stats,
                    &candidate_context.bind_context,
                )?;
            join_candidate =
                StatisticsGathering::new().gather(join_candidate, &mut candidate_context)?;
            let mut propagator = StatisticsPropagator::new();
            join_candidate =
                propagator.propagate(candidate_context.session.clone(), join_candidate);
            candidate_context.column_stats = propagator.take_statistics_map();
            join_candidate =
                StatisticsGathering::new().gather(join_candidate, &mut candidate_context)?;
            let join_candidate = self.finalize_query_candidate(CandidatePlan {
                plan: join_candidate,
                column_stats: Arc::new(candidate_context.column_stats),
            })?;
            if let Err(error) = verify_logical_plan(&self.ctx.bind_context, &join_candidate.plan) {
                debug!(
                    target: targets::OPTIMIZER,
                    %error,
                    "join-region enumerator pruned a candidate that failed semantic verification"
                );
                continue;
            }
            join_alternatives.push(join_candidate.into_alternative(
                AlternativeOrigin::Specialized {
                    rule: JOIN_REGION_ENUMERATOR_RULE,
                },
            ));
            if join_alternatives.len() >= self.budget.max_optional_logical_exprs_per_group as usize
            {
                break;
            }
        }
        alternatives.extend(join_alternatives);
        for alternative in &alternatives {
            verify_physical_planner_invariants(&alternative.plan.operator)?;
        }
        let mut input = MemoBuilder::build_with_search(
            alternatives,
            &self.binder,
            self.budget.clone(),
            &self.ctx,
        )?
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
        self.ctx.profiler.record(
            OptimizerComponent::QueryIrConstruction,
            phase_started.elapsed(),
        );
        let mode = input.mode;
        let phase_started = Instant::now();
        let extraction = input.optimize(&grant_classes)?;
        self.ctx
            .profiler
            .record_rule_attempts(extraction.rule_attempts.clone());
        self.ctx
            .profiler
            .record_rule_insertions(extraction.rule_insertions.clone());
        self.ctx
            .profiler
            .record_search_summary(&extraction.search_summary);
        self.ctx.profiler.record(
            match mode {
                crate::cascades::SearchMode::Direct => OptimizerComponent::DirectPhysicalSearch,
                crate::cascades::SearchMode::Memo => OptimizerComponent::MemoExploration,
            },
            phase_started.elapsed(),
        );
        let phase_started = Instant::now();
        for variant in &extraction.variants {
            verify_physical_planner_invariants(&variant.plan.operator)?;
        }
        self.ctx.profiler.record(
            OptimizerComponent::WinnerVerification,
            phase_started.elapsed(),
        );
        let phase_started = Instant::now();
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
                target: self.extract_physical(variants, &grant_classes)?,
                spec,
            }
        } else {
            if let Some(explain) = &explain {
                for variant in &mut variants {
                    self.attach_explain_layer(variant, explain)?;
                }
            }
            OptimizedStatement::Physical(self.extract_physical(variants, &grant_classes)?)
        };
        self.ctx.profiler.record(
            OptimizerComponent::PhysicalExtraction,
            phase_started.elapsed(),
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
            LogicalPlan::synthetic(LogicalOperator::DummyScan),
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
            LogicalPlan::synthetic(LogicalOperator::DummyScan),
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
        utility: LogicalPlan,
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
                max_threads: self.ctx.session.limits.max_threads.max(1),
                scan_access_cost: Default::default(),
                dependency_template: self.plan_dependency_template_for(&logical),
            })
            .extract(&logical)?;
            let root = plan.properties.get(plan.root).ok_or_else(|| {
                paro_common::error::internal("utility physical plan has no root contract")
            })?;
            let cost = root.cumulative_cost;
            let fingerprint = utility_plan_fingerprint(&plan.node(plan.root).kind)?;
            class_plans.push((grant.id, plan, fingerprint, cost));
        }
        let portfolio = PhysicalPlanPortfolio::build(grant_classes.iter().copied(), class_plans)?;
        portfolio.verify()?;
        Ok(portfolio)
    }

    fn extract_physical(
        &self,
        variants: Vec<crate::cascades::OptimizedVariant>,
        grant_classes: &[ResourceGrantClass],
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
            let dependency_template = self.plan_dependency_template_for(&variant.plan);
            let plan = PhysicalPlanExtractor::new(ExtractionContext {
                force_external: self.ctx.session.limits.force_external,
                grant_spill_policy: grant.spill_policy,
                rowset_scan_pushdown: self.ctx.session.limits.rowset_scan_pushdown,
                max_memory,
                max_threads: self.ctx.session.limits.max_threads.max(1),
                scan_access_cost: Default::default(),
                dependency_template,
            })
            .with_winner_contracts(variant.contracts)
            .with_enforcer_contracts(variant.enforcers)
            .with_statement_write_contracts(variant.write_contracts)
            .requiring_winner_contracts()
            .extract(&variant.plan)?;
            class_plans.push((
                variant.class,
                plan,
                variant.physical_fingerprint,
                variant.cost,
            ));
        }
        let portfolio = PhysicalPlanPortfolio::build(grant_classes.iter().copied(), class_plans)?;
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
            u64::from(budget.max_optional_groups),
            u64::from(budget.max_optional_logical_exprs_per_group),
            u64::from(budget.max_optional_physical_exprs_per_group),
            u64::from(budget.max_optional_interesting_goals_per_group),
            u64::from(budget.max_rule_firings_per_group),
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
            u64::from(budget.max_pareto_winners_per_goal),
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
        plan: &LogicalPlan,
    ) -> crate::physical::PlanDependencies {
        fn graph_key(id: &GraphId) -> Fingerprint {
            let mut fingerprint = StableFingerprintBuilder::default();
            fingerprint.write_bytes(b"paro.graph-generation.v1");
            fingerprint.write_bytes(id.runtime_key().as_bytes());
            fingerprint.finish()
        }

        fn collect(
            optimizer: &Optimizer,
            plan: &LogicalPlan,
            dependencies: &mut crate::physical::PlanDependencies,
        ) {
            if let LogicalOperator::GraphScan(scan) = &plan.operator {
                let id = GraphId::new(
                    optimizer.ctx.session.current_database(),
                    &scan.schema_name,
                    &scan.graph_name,
                );
                if let Some(snapshot) = optimizer.ctx.session.services.graph_index.snapshot(&id) {
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
        dependencies
    }

    /// Only canonical, mandatory semantic work belongs here. Cost alternatives
    /// are owned by the implementation registry after this boundary.
    fn canonicalize_query(&mut self, mut plan: LogicalPlan) -> Result<LogicalPlan> {
        if self.ctx.verify_enabled {
            verify_logical_plan(&self.ctx.bind_context, &plan)?;
        }

        // GraphMatch is still a planner surface node today, while the executor
        // consumes canonical graph scan/expand semantics. The decomposition is
        // mandatory lowering; start/access alternatives belong in graph region
        // implementations and are not selected here.
        plan = GraphMatchDecompose::new().optimize_plan(plan);
        plan = GraphPredicatePushdown::new().optimize_plan(plan);

        let mut scalars = ExpressionRewriter::new();
        scalars.add_rule(Box::new(ConstantFoldingRule::new()));
        scalars.add_rule(Box::new(ArithmeticSimplificationRule::new()));
        scalars.add_rule(Box::new(ComparisonSimplificationRule::new()));
        scalars.add_rule(Box::new(ConjunctionSimplificationRule::new()));
        scalars.add_rule(Box::new(CommonConjunctionFactorRule::new()));
        scalars.add_rule(Box::new(MoveConstantsRule::new()));
        scalars.rewrite_plan(&mut plan);

        CommonAggregateOptimizer::new().optimize(&mut plan);
        plan = DelimJoinElimination::new().optimize_plan(plan);
        plan = EmptyResultPullup::new().optimize_plan(plan);
        plan = JoinPredicateNormalizer::new(&self.ctx.bind_context).optimize_plan(plan)?;
        plan = InClauseRewriter::new().rewrite(plan)?;
        if contains_mark_filter_over_cross_product(&plan) {
            // A MARK boundary can sit between an outer filter and its
            // comma-join probe. Push safe terms through that boundary, then
            // canonicalize the newly exposed equality predicate. Restricting
            // this repair to the blocked shape avoids rewriting independent
            // recursive/control regions in the same normalization phase.
            plan = FilterPushdown::new().rewrite_plan(plan);
            plan = JoinPredicateNormalizer::new(&self.ctx.bind_context).optimize_plan(plan)?;
        }
        plan = ExternalRoutineLoweringPass::lower(plan, &self.ctx.bind_context)?.plan;
        plan = TopNOptimizer::new().optimize_plan(plan);
        plan = CTEInlining::new(&self.ctx.bind_context)
            .only_not_materialized()
            .optimize_plan(plan);

        if self.ctx.verify_enabled {
            verify_logical_plan(&self.ctx.bind_context, &plan)?;
        }
        Ok(plan)
    }

    fn estimate_query_candidate(&self, mut plan: LogicalPlan) -> Result<CandidatePlan> {
        let mut context = self.ctx.fork_for_candidate(HashMap::new());

        plan = StatisticsGathering::new().gather(plan, &mut context)?;
        let mut propagator = StatisticsPropagator::new();
        plan = propagator.propagate(context.session.clone(), plan);
        context.column_stats = propagator.take_statistics_map();
        plan = StatisticsGathering::new().gather(plan, &mut context)?;

        Ok(CandidatePlan {
            plan,
            column_stats: Arc::new(context.column_stats),
        })
    }

    fn settle_schema_candidate(&self, mut plan: LogicalPlan) -> Result<CandidatePlan> {
        // Query-IR output contracts are demand driven. Until every planner
        // operator natively exposes ColumnIds, derive the same canonical
        // demand projection once at the Query IR boundary; this is not an
        // optional cost rewrite and never removes an observable evaluation.
        RemoveUnusedColumns::optimize(&mut plan, &self.binder, self.ctx.session.as_ref(), true);

        self.estimate_query_candidate(plan)
    }

    fn finalize_query_candidate(&self, mut candidate: CandidatePlan) -> Result<CandidatePlan> {
        candidate.plan =
            singleton_groups::optimize_plan(candidate.plan, candidate.column_stats.as_ref());
        candidate.plan = ColumnLifetimeAnalyzer::new(true).optimize(candidate.plan)?;

        if self.ctx.verify_enabled {
            verify_logical_plan(&self.ctx.bind_context, &candidate.plan)?;
        }
        Ok(candidate)
    }

    fn settle_query_candidate(&self, plan: LogicalPlan) -> Result<CandidatePlan> {
        let candidate = self.settle_schema_candidate(plan)?;
        self.finalize_query_candidate(candidate)
    }

    fn distinct_aggregate_feasibility_candidate(
        &self,
        plan: LogicalPlan,
    ) -> Result<Option<CandidatePlan>> {
        let (plan, changed) = distinct_decomposition::optimize_plan(plan, &self.ctx.bind_context)?;
        if !changed {
            return Ok(None);
        }
        self.settle_query_candidate(plan).map(Some)
    }

    fn correlated_aggregate_candidate(&self, plan: LogicalPlan) -> Result<CandidatePlan> {
        let candidate =
            CorrelatedPartitionAggregate::new(self.ctx.bind_context.clone()).optimize_plan(plan)?;
        self.settle_query_candidate(candidate)
    }

    fn prepare_correlated_seed(&self, plan: LogicalPlan) -> LogicalPlan {
        let mut candidate = CTEInlining::new(&self.ctx.bind_context).optimize_plan(plan);
        candidate = FilterPullup::new().rewrite_plan(candidate);
        candidate = FilterPushdown::new().rewrite_plan(candidate);
        candidate = CTEFilterPusher::new().optimize_plan(candidate);
        CTEInlining::new(&self.ctx.bind_context).optimize_plan(candidate)
    }

    fn scalar_reuse_candidate(&self, candidate: CandidatePlan) -> Result<CandidatePlan> {
        let context = self
            .ctx
            .fork_for_candidate(Arc::unwrap_or_clone(candidate.column_stats));
        let mut candidate = JoinOrderOptimizer::new()
            .with_search_budget(&self.budget)
            .optimize_plan(
                context.session.as_ref(),
                candidate.plan,
                &context.column_stats,
                &context.bind_context,
            )?;
        candidate = scalar_aggregate_window::optimize_plan(candidate, &self.ctx.bind_context)?;
        if self.ctx.session.settings.rowset_scan_pushdown() {
            (candidate, _) = late_payload::optimize_matched_prefix_plan(candidate)?;
        }
        self.settle_query_candidate(candidate)
    }
}

fn observes_optimizer_diagnostics(plan: &LogicalPlan) -> bool {
    matches!(
        &plan.operator,
        LogicalOperator::TableFunctionGet(function)
            if function.function_name().eq_ignore_ascii_case("paro_optimizers")
    ) || plan
        .children()
        .into_iter()
        .any(observes_optimizer_diagnostics)
}

fn contains_redundant_computation_region(plan: &LogicalPlan) -> bool {
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

fn contains_mark_filter_over_cross_product(plan: &LogicalPlan) -> bool {
    fn contains_cross_product(plan: &LogicalPlan) -> bool {
        matches!(plan.operator, LogicalOperator::Join(Join::Cross(_)))
            || plan.children().into_iter().any(contains_cross_product)
    }

    let local = matches!(
        &plan.operator,
        LogicalOperator::Filter(filter)
            if matches!(
                &filter.child.operator,
                LogicalOperator::Join(Join::Comparison(join))
                    if join.join_type == JoinType::Mark
                        && contains_cross_product(join.left.as_ref())
            )
    );
    local
        || plan
            .children()
            .into_iter()
            .any(contains_mark_filter_over_cross_product)
}

fn contains_aggregate(plan: &LogicalPlan) -> bool {
    matches!(plan.operator, LogicalOperator::Aggregate(_))
        || plan.children().into_iter().any(contains_aggregate)
}

/// Build the bounded GraphPatternRegion candidate product before mandatory
/// GraphMatch decomposition.  The binder order is retained as candidate zero;
/// every additional frontier is only a certified equivalent input to Memo.
fn enumerate_graph_region_plans(
    plan: LogicalPlan,
    bind_context: &paro_planner::binder::context::BindContext,
    max_optional_frontiers: u32,
) -> Result<Vec<LogicalPlan>> {
    type PatternOrder = Vec<paro_planner::binder::bind::graph::BoundPatternElement>;

    fn collect(plan: &LogicalPlan, patterns: &mut Vec<Vec<PatternOrder>>, per_pattern_max: usize) {
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
        plan: &mut LogicalPlan,
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
    max_grant_classes: u8,
    spill_available: bool,
) -> Box<[ResourceGrantClass]> {
    let class_limit = usize::from(max_grant_classes.max(1)).min(3);
    let hard_limits = if max_memory == 0 {
        vec![u64::MAX]
    } else {
        let full = u64::try_from(max_memory).unwrap_or(u64::MAX).max(1);
        match class_limit {
            1 => vec![full],
            2 => vec![(full / 2).max(1), full],
            _ => vec![(full / 4).max(1), (full / 2).max(1), full],
        }
    };
    let mut previous = None;
    hard_limits
        .into_iter()
        .filter(|hard| previous.replace(*hard) != Some(*hard))
        .enumerate()
        .map(|(index, hard_memory_bytes)| ResourceGrantClass {
            id: ResourceGrantClassId::new(index),
            hard_memory_bytes,
            spill_policy: if spill_available {
                SpillPolicy::Allowed
            } else {
                SpillPolicy::Forbidden
            },
            concurrency_class: u16::try_from(index).unwrap_or(u16::MAX),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

fn contains_join_region(plan: &LogicalPlan) -> bool {
    if matches!(
        plan.operator,
        LogicalOperator::Join(_) | LogicalOperator::DependentJoin(_)
    ) {
        return true;
    }
    plan.children().into_iter().any(contains_join_region)
}
