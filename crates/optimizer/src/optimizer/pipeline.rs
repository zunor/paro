// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! A single committed relation tree with short-lived region alternatives.
//! This path never builds a Memo or invokes a physical search engine.

use super::*;
use crate::physical::direct;
use paro_context::compile_diagnostics::Observation::Observed;
use paro_planner::operator::ExplainMode;

#[cfg(test)]
mod tests;

impl Optimizer {
    pub(super) fn optimize_pipeline(
        &mut self,
        query: OwnedLogicalPlan,
        layer: QueryStatementLayer,
        explain: Option<ExplainEnvelope>,
    ) -> Result<OptimizedStatement> {
        let started = Instant::now();
        self.ctx.session.cancellation.check()?;
        if !self.budget.disabled_transformation_rules.is_empty() {
            return Err(paro_error::invalid_input(
                "Memo rule ablations do not apply to the pipeline planning program",
            ));
        }
        // Write barriers must be selected explicitly, not inherited from a
        // structural lowering default. Until that owner is wired, fail closed.
        if !matches!(layer, QueryStatementLayer::Query) {
            return Err(paro_error::not_implemented(
                "pipeline write planning is not yet supported",
            ));
        }
        let grant = ResourceGrantClass {
            id: ResourceGrantClassId(0),
            hard_memory_bytes: self.ctx.session.limits.max_memory as u64,
            spill_policy: if self.ctx.session.limits.use_temporary_directory {
                SpillPolicy::Allowed
            } else {
                SpillPolicy::Forbidden
            },
            max_parallel_tasks: u16::try_from(self.ctx.session.limits.max_threads.max(1))
                .unwrap_or(u16::MAX),
        };
        let query = self.canonicalize_query(query)?;
        let (query, _) = distinct_decomposition::optimize_plan(query, &self.ctx.bind_context)?;
        let candidate = self.settle_relational_baseline(query)?;
        let mut plan = self.pipeline_occurrences(candidate.plan)?;
        self.ctx.column_stats = candidate.column_stats;
        self.ctx
            .profiler
            .record(OptimizerComponent::SemanticNormalization, started.elapsed());

        let region_started = Instant::now();
        // Compare only root-local aggregate alternatives. Descendants already
        // have one committed shape; the losing alternative dies at this call.
        let mut region_choices = 0_u64;
        plan = plan.try_map_post_order(|input| {
            self.ctx.session.cancellation.check()?;
            if !matches!(&input.operator, LogicalOperator::Aggregate(a)
                if crate::aggregate::dimension_deferral::root_eligible(a))
            {
                return Ok(input);
            }
            let copy =
                duplicate_plan_preserving_indices(&input, self.ctx.bind_context.shared().as_ref());
            let (alternative, changed) =
                crate::aggregate::dimension_deferral::optimize_plan(copy, &self.ctx.bind_context)?;
            if !changed {
                return Ok(input);
            }
            let mut alternative = self.settle_query_candidate(alternative)?;
            alternative.plan = self.pipeline_occurrences(alternative.plan)?;
            let baseline = direct::select(
                &input,
                &self.ctx.column_stats,
                grant,
                &self.calibration,
                &self.ctx.session,
            )?;
            let proposed = direct::select(
                &alternative.plan,
                &alternative.column_stats,
                grant,
                &self.calibration,
                &self.ctx.session,
            )?;
            region_choices += 1;
            if proposed.as_ref().is_some_and(|proposed| {
                baseline.as_ref().is_none_or(|baseline| {
                    crate::physical::ObjectiveProfile::Latency
                        .compare(&proposed.cost, &baseline.cost)
                        .is_lt()
                })
            }) {
                Ok(alternative.plan)
            } else {
                Ok(input)
            }
        })?;
        // Every committed rewrite reopens predicate routing before join DP.
        // Required domains must reach the source, not remain as join residuals.
        plan = crate::construction::predicates(plan, &mut Default::default());
        let candidate = self.settle_query_candidate(plan)?;
        self.ctx.column_stats = candidate.column_stats;
        plan = JoinOrderOptimizer::new(self.ctx.cost_model.defaults.clone())
            .with_search_budget(&self.budget)
            .optimize_regions(
                self.ctx.session.as_ref(),
                candidate.plan,
                &self.ctx.column_stats,
                &self.ctx.bind_context,
            )?;
        plan = crate::construction::predicates(plan, &mut Default::default());
        plan = TopNOptimizer::new().optimize_plan(plan);
        let mut candidate = self.settle_query_candidate(plan)?;
        candidate.plan = self.pipeline_occurrences(candidate.plan)?;
        self.ctx.column_stats = candidate.column_stats;
        self.ctx.profiler.record(
            OptimizerComponent::RegionOptimization,
            region_started.elapsed(),
        );

        let physical_started = Instant::now();
        let selection = direct::select(
            &candidate.plan,
            &self.ctx.column_stats,
            grant,
            &self.calibration,
            &self.ctx.session,
        )?
        .ok_or_else(|| {
            paro_error::invalid_input("pipeline plan exceeds its executable memory envelope")
        })?;
        self.ctx.profiler.record(
            OptimizerComponent::PhysicalSelection,
            physical_started.elapsed(),
        );
        let physical_started = Instant::now();
        let mut plan = candidate.plan;
        let analyze_spec = explain
            .as_ref()
            .and_then(|e| (e.spec.mode == ExplainMode::Analyze).then_some(e.spec));
        let mut contracts = selection.contracts;
        if let Some(e) = explain.filter(|_| analyze_spec.is_none()) {
            let mut contract = contracts
                .get(&plan.id)
                .ok_or_else(|| paro_error::internal("pipeline root contract is absent"))?
                .clone();
            contract.implementation = PhysicalImplementationFlavor::Structural;
            contract.region_owner = None;
            contract.origin = crate::physical::PlanOrigin::StatementLowering;
            contract.owned_artifacts = Box::new([]);
            plan = e.attach(plan);
            Arc::make_mut(&mut contracts).insert(plan.id, contract);
        }
        let dependency_template = self.plan_dependency_template_for(&plan)?;
        let physical = PhysicalPlanExtractor::new(ExtractionContext {
            force_external: self.ctx.session.limits.force_external,
            grant_spill_policy: grant.spill_policy,
            rowset_scan_pushdown: self.ctx.session.limits.rowset_scan_pushdown,
            max_memory: usize::try_from(grant.hard_memory_bytes).unwrap_or(usize::MAX),
            max_threads: usize::from(grant.max_parallel_tasks),
            scan_access_cost: Default::default(),
            dependency_template,
        })
        .with_winner_contracts(contracts)
        .requiring_winner_contracts()
        .extract(plan)?;
        let identity = physical
            .structural_identity_fingerprint()
            .map_err(|error| {
                paro_error::internal(format!("pipeline physical identity: {error}"))
            })?;
        let fingerprint = physical.portfolio_fingerprint(identity)?;
        let portfolio = PhysicalPlanPortfolio::build(
            crate::physical::ObjectiveProfile::Latency,
            [grant],
            [(grant.id, physical, fingerprint, selection.cost)],
        )?;
        portfolio.verify()?;
        self.ctx.profiler.record(
            OptimizerComponent::PhysicalExtraction,
            physical_started.elapsed(),
        );
        self.compile_work = paro_context::CompileWork {
            optimizer_elapsed_us: started.elapsed().as_micros() as u64,
            child_combination_cost_synthesis_count: selection.alternatives,
            ..Default::default()
        };
        self.compile_receipt = Some(CompileReceiptSummary {
            schema_version: paro_context::COMPILE_RECEIPT_SCHEMA_VERSION,
            artifact_identity: None,
            // A completed finite program is not a proof over all SQL plans.
            search_stop: Observed(SearchStop::Incomplete),
            search_complete: Observed(false),
            quality_policy_satisfied: Observed(false),
            budget_limited: Observed(false),
            obligations: Observed(1),
            groups: Observed(0),
            logical_expressions: Observed(0),
            physical_expressions: Observed(0),
            expected_class: Observed(grant.id.0),
            variant_count: Observed(1),
            omitted_variants: 0,
            compile_work: None,
        });
        if let Some(capture) = &self.ctx.session.options.compile_capture {
            capture.update(|record| {
                record.groups = Observed(0);
                record.logical_expressions = Observed(0);
                record.physical_expressions = Observed(0);
                record.obligations = Observed(1);
                record.search_complete = Observed(false);
                record.quality_policy_satisfied = Observed(false);
                record.budget_limited = Observed(false);
                record.search_stop = Observed(SearchStop::Incomplete);
                record.safety_verified = Observed(true);
            });
            capture.search_counters(std::collections::BTreeMap::from([
                ("pipeline_selected_nodes", selection.nodes),
                ("pipeline_local_alternatives", selection.alternatives),
                ("pipeline_aggregate_decisions", region_choices),
                ("memo_group_count", 0),
                ("memo_logical_expression_count", 0),
                ("memo_physical_expression_count", 0),
                ("search_complete", 0),
                ("search_rule_failure_count", 0),
                ("search_deadline_reached", 0),
                ("settlement_local_hit_count", 0),
                ("settlement_local_miss_count", 0),
            ]));
        }
        publish_optimizer_profile_snapshot(
            self.ctx.session.diagnostics.as_ref(),
            std::mem::take(&mut self.ctx.profiler).into_snapshot(),
        );
        Ok(match analyze_spec {
            Some(spec) => OptimizedStatement::ExplainAnalyze {
                target: portfolio,
                spec,
            },
            None => OptimizedStatement::Physical(portfolio),
        })
    }

    /// Committed tree occurrences, not rewrite source ids, own physical
    /// contracts. Rewrites can preserve a source id on multiple replacement
    /// shells; assign unique occurrences once at each selection boundary.
    fn pipeline_occurrences(&self, plan: OwnedLogicalPlan) -> Result<OwnedLogicalPlan> {
        plan.try_map_post_order(|mut node| {
            node.id = self.ctx.bind_context.next_plan_id();
            Ok(node)
        })
    }
}
