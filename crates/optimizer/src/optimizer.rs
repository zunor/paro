// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Unified pipeline runner for logical-plan rewrites.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use paro_common::error::Result;
use paro_common::logging::targets;
use paro_context::StatementContext;
use paro_planner::binder::Binder;
use paro_planner::operator::LogicalOperator;
use paro_planner::plan::LogicalPlan;
use tracing::debug;

use crate::context::OptimizationContext;
use crate::optimizer_type::OptimizerType;
use crate::pipeline_passes::{
    BuildProbeSidePass, ColumnLifetimePass, CommonAggregatePass, CteFilterPusherPass,
    CteInliningPass, DelimJoinEliminationPass, EmptyResultPullupPass, ExpressionRewriterPass,
    FilterPullupPass, FilterPushdownPass, GraphMatchDecomposePass, GraphPredicatePushdownPass,
    GraphStartSelectionPass, InClausePass, JoinEliminationPass, JoinFilterPushdownPass,
    JoinOrderPass, LimitPushdownPass, ReorderFilterPass, SearchOptimizationPass, SegmentPrunerPass,
    StatisticsGatheringPass, StatisticsPropagationPass, TopNPass, UnusedColumnsPass,
};
use crate::profiler::publish_optimizer_profile_snapshot;
use crate::rewriter::Rewriter;
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

        debug!(
            target: targets::OPTIMIZER,
            elapsed_ms = started_at.elapsed().as_millis(),
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
                verify_logical_plan(&self.ctx.bind_context, &current)?;
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
            Box::new(ColumnLifetimePass),
            Box::new(BuildProbeSidePass),
            Box::new(JoinFilterPushdownPass),
            Box::new(TopNPass),
            Box::new(LimitPushdownPass),
            Box::new(InClausePass),
            Box::new(SearchOptimizationPass),
            Box::new(SegmentPrunerPass),
            Box::new(StatisticsPropagationPass),
        ]
    }
}
