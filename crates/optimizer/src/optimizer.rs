// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Unified pipeline runner for logical-plan rewrites.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use paro_common::error::{self as paro_error, Result};
use paro_common::logging::targets;
use paro_context::StatementContext;
use paro_planner::binder::Binder;
use paro_planner::operator::LogicalOperator;
use paro_planner::plan::LogicalPlan;
use tracing::debug;

use crate::context::OptimizationContext;
use crate::external::lowering::ExternalRoutineLoweringPass;
use crate::optimizer_type::OptimizerType;
use crate::pipeline_passes::{
    BuildProbeSidePass, ColumnLifetimePass, CommonAggregatePass, CteFilterPusherPass,
    CteInliningPass, DelimJoinEliminationPass, EmptyResultPullupPass, ExpressionRewriterPass,
    FilterPullupPass, FilterPushdownPass, GraphMatchDecomposePass, GraphPredicatePushdownPass,
    GraphStartSelectionPass, InClausePass, JoinEliminationPass, JoinFilterPushdownPass,
    JoinOrderPass, LimitPushdownPass, MixedJoinPredicatePass, ReorderFilterPass,
    SearchOptimizationPass, SegmentPrunerPass, StatisticsGatheringPass, StatisticsPropagationPass,
    TopNPass, UnusedColumnsPass,
};
use crate::profiler::publish_optimizer_profile_snapshot;
use crate::rewriter::Rewriter;
use crate::statistics::gathering::StatisticsGathering;
use crate::verify::verify_logical_plan;

pub struct Optimizer {
    pipeline: Vec<Box<dyn Rewriter>>,
    disabled: HashSet<OptimizerType>,
    ctx: OptimizationContext,
}

impl Optimizer {
    pub fn new(binder: Binder, session: Arc<StatementContext>) -> Self {
        Self::with_disabled(binder, session, HashSet::new())
    }

    pub fn with_disabled(
        binder: Binder,
        session: Arc<StatementContext>,
        disabled: HashSet<OptimizerType>,
    ) -> Self {
        let ctx = OptimizationContext::new(session, binder.bind_context.clone());
        let pipeline = Self::build_pipeline(binder);
        Self {
            pipeline,
            disabled,
            ctx,
        }
    }

    pub fn optimizer_disabled(&self, opt_type: OptimizerType) -> bool {
        self.disabled.contains(&opt_type)
    }

    pub fn disable_optimizer(&mut self, opt_type: OptimizerType) {
        self.disabled.insert(opt_type);
    }

    pub fn enable_optimizer(&mut self, opt_type: OptimizerType) {
        self.disabled.remove(&opt_type);
    }

    pub fn optimize(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let started_at = Instant::now();
        debug!(
            target: targets::OPTIMIZER,
            disabled_optimizers = self.disabled.len(),
            pipeline_len = self.pipeline.len(),
            "Optimization started"
        );

        let optimized = self.optimize_plan(plan)?;
        let lowering = ExternalRoutineLoweringPass::lower(optimized, &self.ctx.bind_context)?;
        let mut optimized = lowering.plan;

        if lowering.changed {
            self.ctx.column_stats.clear();
            optimized = StatisticsGathering::new().gather(optimized, &mut self.ctx)?;
            if self.ctx.verify_enabled {
                verify_logical_plan(&self.ctx.bind_context, &optimized)?;
            }
        }

        debug!(
            target: targets::OPTIMIZER,
            elapsed_ms = started_at.elapsed().as_millis(),
            external_lowering = lowering.changed,
            "Optimization completed"
        );
        publish_optimizer_profile_snapshot(
            self.ctx
                .profiler
                .snapshot(&self.pipeline_types(), &self.disabled),
        );
        Ok(optimized)
    }

    fn optimize_plan(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        if matches!(plan.operator, LogicalOperator::Explain(_)) {
            return self.optimize_explain(plan);
        }
        if Self::should_skip_optimization(&plan) {
            return Ok(plan);
        }

        if self.ctx.verify_enabled {
            verify_logical_plan(&self.ctx.bind_context, &plan)?;
        }

        let mut current = plan;
        for pass in &mut self.pipeline {
            let opt_type = pass.optimizer_type();
            if self.disabled.contains(&opt_type) {
                continue;
            }

            let started_at = Instant::now();
            let rewrite_result = pass.rewrite(current, &mut self.ctx);
            self.ctx.profiler.record(opt_type, started_at.elapsed());
            current = rewrite_result?;

            if self.ctx.verify_enabled {
                verify_logical_plan(&self.ctx.bind_context, &current).map_err(|error| {
                    paro_error::internal(format!(
                        "Logical plan invariant failed after optimizer pass {opt_type}: {}",
                        error.message()
                    ))
                })?;
            }
        }

        Ok(current)
    }

    fn optimize_explain(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        if !matches!(plan.operator, LogicalOperator::Explain(_)) {
            unreachable!("optimize_explain requires a LogicalOperator::Explain root")
        }

        let optimized = plan.try_map_children(|child| self.optimize_plan(child))?;
        if self.ctx.verify_enabled {
            verify_logical_plan(&self.ctx.bind_context, &optimized)?;
        }
        Ok(optimized)
    }

    fn should_skip_optimization(plan: &LogicalPlan) -> bool {
        matches!(plan.operator, LogicalOperator::DummyScan)
    }

    fn pipeline_types(&self) -> Vec<OptimizerType> {
        self.pipeline
            .iter()
            .map(|pass| pass.optimizer_type())
            .collect()
    }

    fn build_pipeline(binder: Binder) -> Vec<Box<dyn Rewriter>> {
        let unused_columns_binder = binder.clone();
        vec![
            Box::new(GraphStartSelectionPass),
            Box::new(GraphMatchDecomposePass),
            Box::new(GraphPredicatePushdownPass),
            Box::new(ExpressionRewriterPass),
            Box::new(CommonAggregatePass),
            Box::new(CteInliningPass),
            Box::new(FilterPullupPass),
            Box::new(FilterPushdownPass),
            Box::new(CteFilterPusherPass),
            // Second inlining pass: filters were pushed into CTE bodies; inline again when beneficial.
            Box::new(CteInliningPass),
            Box::new(DelimJoinEliminationPass),
            Box::new(EmptyResultPullupPass),
            Box::new(StatisticsGatheringPass),
            Box::new(ReorderFilterPass),
            Box::new(JoinEliminationPass),
            Box::new(JoinOrderPass),
            Box::new(UnusedColumnsPass {
                binder: unused_columns_binder,
            }),
            Box::new(BuildProbeSidePass),
            Box::new(JoinFilterPushdownPass),
            Box::new(MixedJoinPredicatePass),
            Box::new(TopNPass),
            Box::new(LimitPushdownPass),
            Box::new(InClausePass),
            Box::new(SearchOptimizationPass),
            Box::new(SegmentPrunerPass),
            // Join ordering and the later structural passes replace plan
            // nodes while preserving the old parents. Recompute cardinality
            // and output statistics over the settled tree so projections and
            // aggregates cannot retain pre-rewrite estimates.
            Box::new(StatisticsGatheringPass),
            Box::new(StatisticsPropagationPass),
            // Projection maps are positional annotations over the final logical
            // layout. Derive them only after every structural rewrite (most
            // notably build/probe-side flips) has settled that layout. This
            // terminal pass may reduce output widths, but it must not reorder
            // retained columns or invalidate operator/cardinality statistics:
            // statistics propagated above are operator-level, never indexed by
            // the pre-pruning output position.
            Box::new(ColumnLifetimePass),
        ]
    }
}

#[cfg(test)]
mod tests {
    use paro_context::test_support::TestStatementContextBuilder;
    use paro_planner::planner::Planner;

    use super::Optimizer;
    use crate::optimizer_type::OptimizerType;

    #[test]
    fn column_lifetime_is_the_terminal_optimizer_pass() {
        let session = TestStatementContextBuilder::minimal().build();
        let planner = Planner::new(session.clone());
        let optimizer = Optimizer::new(planner.binder.clone(), session);

        assert_eq!(
            optimizer.pipeline_types().last(),
            Some(&OptimizerType::ColumnLifetime)
        );
    }

    #[test]
    fn correlated_having_retains_delim_capture_key_through_optimization() {
        let session = TestStatementContextBuilder::minimal().build();
        let mut planner = Planner::new(session.clone());
        let statement = paro_parser::parse_one(
            "SELECT o.grp \
             FROM (VALUES (1, 10), (2, 10), (3, 20)) AS o(id, grp) \
             GROUP BY o.grp \
             HAVING EXISTS( \
                 SELECT 1 \
                 FROM (VALUES (10), (30)) AS d(grp) \
                 WHERE d.grp = o.grp \
             )",
        )
        .expect("parse correlated HAVING")
        .stmt;
        planner
            .create_plan(statement)
            .expect("plan correlated HAVING");
        let plan = planner.take_plan().expect("logical plan");
        let mut optimizer = Optimizer::new(planner.binder.clone(), session);

        optimizer
            .optimize(plan)
            .expect("optimize correlated HAVING without losing delim keys");
    }
}
