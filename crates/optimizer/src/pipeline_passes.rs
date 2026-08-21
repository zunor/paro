// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Named [`Rewriter`] implementations for the default optimization pipeline.

use paro_common::error::Result;
use paro_planner::binder::Binder;
use paro_planner::plan::LogicalPlan;

use crate::aggregate::common::CommonAggregateOptimizer;
use crate::aggregate::dimension_deferral;
use crate::aggregate::input_materialization;
use crate::aggregate::join_preaggregation;
use crate::aggregate::join_subsumption;
use crate::aggregate::late_payload;
use crate::aggregate::non_null_inputs;
use crate::aggregate::post_reduction;
use crate::aggregate::singleton_groups;
use crate::aggregate::statistics_exec::AggregateStatisticsExecutor;
use crate::column::lifetime::ColumnLifetimeAnalyzer;
use crate::column::remove_unused::RemoveUnusedColumns;
use crate::context::OptimizationContext;
use crate::cte::filter_pusher::CTEFilterPusher;
use crate::cte::inlining::CTEInlining;
use crate::expression::in_clause::InClauseRewriter;
use crate::expression::rewriter::ExpressionRewriter;
use crate::filter::pullup::FilterPullup;
use crate::filter::pushdown::FilterPushdown;
use crate::filter::reorder::ReorderFilter;
use crate::graph::match_decompose::GraphMatchDecompose;
use crate::graph::predicate_pushdown::GraphPredicatePushdown;
use crate::graph::start_selection::GraphStartSelection;
use crate::join::build_probe_side::BuildProbeSideOptimizer;
use crate::join::elimination::JoinElimination;
use crate::join::filter_pushdown::JoinFilterPushdown;
use crate::join::mixed_predicates::JoinPredicateNormalizer;
use crate::join_order::optimizer::JoinOrderOptimizer;
use crate::limit::pushdown::LimitPushdown;
use crate::limit::topn::TopNOptimizer;
use crate::optimizer_type::OptimizerType;
use crate::rewriter::Rewriter;
use crate::rules::arithmetic::ArithmeticSimplificationRule;
use crate::rules::comparison::ComparisonSimplificationRule;
use crate::rules::conjunction::{CommonConjunctionFactorRule, ConjunctionSimplificationRule};
use crate::rules::constant_folding::ConstantFoldingRule;
use crate::rules::move_constants::MoveConstantsRule;
use crate::search::optimizer::SearchOptimizer;
use crate::statistics::gathering::StatisticsGathering;
use crate::statistics::propagator::StatisticsPropagator;
use crate::statistics::segment_pruner::SegmentPruner;
use crate::subquery::delim_join_elimination::DelimJoinElimination;
use crate::subquery::empty_result::EmptyResultPullup;
use crate::subquery::partition_aggregate::CorrelatedPartitionAggregate;
use crate::subquery::scalar_aggregate_window;

pub struct GraphStartSelectionPass;

impl Rewriter for GraphStartSelectionPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::GraphStartSelection
    }

    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan> {
        GraphStartSelection::new().optimize(plan, ctx)
    }
}

pub struct GraphMatchDecomposePass;

impl Rewriter for GraphMatchDecomposePass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::GraphMatchDecompose
    }

    fn rewrite(
        &mut self,
        plan: LogicalPlan,
        _ctx: &mut OptimizationContext,
    ) -> Result<LogicalPlan> {
        Ok(GraphMatchDecompose::new().optimize_plan(plan))
    }
}

pub struct GraphPredicatePushdownPass;

impl Rewriter for GraphPredicatePushdownPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::GraphPredicatePushdown
    }

    fn rewrite(
        &mut self,
        plan: LogicalPlan,
        _ctx: &mut OptimizationContext,
    ) -> Result<LogicalPlan> {
        Ok(GraphPredicatePushdown::new().optimize_plan(plan))
    }
}

pub struct ExpressionRewriterPass;

impl Rewriter for ExpressionRewriterPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::ExpressionRewriter
    }

    fn rewrite(
        &mut self,
        mut plan: LogicalPlan,
        _ctx: &mut OptimizationContext,
    ) -> Result<LogicalPlan> {
        let mut rewriter = ExpressionRewriter::new();
        rewriter.add_rule(Box::new(ConstantFoldingRule::new()));
        rewriter.add_rule(Box::new(ArithmeticSimplificationRule::new()));
        rewriter.add_rule(Box::new(ComparisonSimplificationRule::new()));
        rewriter.add_rule(Box::new(ConjunctionSimplificationRule::new()));
        rewriter.add_rule(Box::new(CommonConjunctionFactorRule::new()));
        rewriter.add_rule(Box::new(MoveConstantsRule::new()));
        rewriter.rewrite_plan(&mut plan);
        Ok(plan)
    }
}

pub struct CommonAggregatePass;

impl Rewriter for CommonAggregatePass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::CommonAggregate
    }

    fn rewrite(
        &mut self,
        mut plan: LogicalPlan,
        _ctx: &mut OptimizationContext,
    ) -> Result<LogicalPlan> {
        CommonAggregateOptimizer::new().optimize(&mut plan);
        Ok(plan)
    }
}

pub struct AggregateNonNullInputPass;

impl Rewriter for AggregateNonNullInputPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::AggregateNonNullInput
    }

    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan> {
        Ok(non_null_inputs::optimize_plan(plan, &ctx.column_stats))
    }
}

pub struct CteInliningPass;

impl Rewriter for CteInliningPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::CteInlining
    }

    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan> {
        Ok(CTEInlining::new(&ctx.bind_context).optimize_plan(plan))
    }
}

pub struct FilterPullupPass;

impl Rewriter for FilterPullupPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::FilterPullup
    }

    fn rewrite(
        &mut self,
        plan: LogicalPlan,
        _ctx: &mut OptimizationContext,
    ) -> Result<LogicalPlan> {
        Ok(FilterPullup::new().rewrite_plan(plan))
    }
}

pub struct FilterPushdownPass;

impl Rewriter for FilterPushdownPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::FilterPushdown
    }

    fn rewrite(
        &mut self,
        plan: LogicalPlan,
        _ctx: &mut OptimizationContext,
    ) -> Result<LogicalPlan> {
        Ok(FilterPushdown::new().rewrite_plan(plan))
    }
}

pub struct CteFilterPusherPass;

impl Rewriter for CteFilterPusherPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::CteFilterPusher
    }

    fn rewrite(
        &mut self,
        plan: LogicalPlan,
        _ctx: &mut OptimizationContext,
    ) -> Result<LogicalPlan> {
        Ok(CTEFilterPusher::new().optimize_plan(plan))
    }
}

pub struct DelimJoinEliminationPass;

impl Rewriter for DelimJoinEliminationPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::DelimJoinElimination
    }

    fn rewrite(
        &mut self,
        plan: LogicalPlan,
        _ctx: &mut OptimizationContext,
    ) -> Result<LogicalPlan> {
        Ok(DelimJoinElimination::new().optimize_plan(plan))
    }
}

pub struct EmptyResultPullupPass;

impl Rewriter for EmptyResultPullupPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::EmptyResultPullup
    }

    fn rewrite(
        &mut self,
        plan: LogicalPlan,
        _ctx: &mut OptimizationContext,
    ) -> Result<LogicalPlan> {
        Ok(EmptyResultPullup::new().optimize_plan(plan))
    }
}

pub struct CorrelatedPartitionAggregatePass;

impl Rewriter for CorrelatedPartitionAggregatePass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::CorrelatedPartitionAggregate
    }

    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan> {
        CorrelatedPartitionAggregate::new(ctx.bind_context.clone()).optimize_plan(plan)
    }
}

pub struct ScalarAggregateWindowPass;

impl Rewriter for ScalarAggregateWindowPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::ScalarAggregateWindow
    }

    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan> {
        scalar_aggregate_window::optimize_plan(plan, &ctx.bind_context)
    }
}

pub struct JoinEliminationPass;

impl Rewriter for JoinEliminationPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::JoinElimination
    }

    fn rewrite(
        &mut self,
        plan: LogicalPlan,
        _ctx: &mut OptimizationContext,
    ) -> Result<LogicalPlan> {
        Ok(JoinElimination::new().optimize_plan(plan))
    }
}

pub struct JoinOrderPass;

impl Rewriter for JoinOrderPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::JoinOrder
    }

    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan> {
        JoinOrderOptimizer::new().optimize_plan(
            ctx.session.as_ref(),
            plan,
            &ctx.column_stats,
            &ctx.bind_context,
        )
    }
}

pub struct AggregateJoinSubsumptionPass;

impl Rewriter for AggregateJoinSubsumptionPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::AggregateJoinSubsumption
    }

    fn rewrite(
        &mut self,
        plan: LogicalPlan,
        _ctx: &mut OptimizationContext,
    ) -> Result<LogicalPlan> {
        Ok(join_subsumption::optimize_plan(plan))
    }
}

pub struct AggregateJoinPreaggregationPass;

impl Rewriter for AggregateJoinPreaggregationPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::AggregateJoinPreaggregation
    }

    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan> {
        Ok(join_preaggregation::optimize_plan(
            plan,
            &ctx.bind_context,
            &ctx.column_stats,
        ))
    }
}

pub struct AggregateDimensionDeferralPass;

impl Rewriter for AggregateDimensionDeferralPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::AggregateDimensionDeferral
    }

    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan> {
        let (plan, changed) =
            dimension_deferral::optimize_plan(plan, &ctx.bind_context, &ctx.cost_model)?;
        if changed {
            ctx.invalidations.mark_aggregate_schema();
        }
        Ok(plan)
    }
}

pub struct AggregateInputMaterializationPass;

impl Rewriter for AggregateInputMaterializationPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::AggregateInputMaterialization
    }

    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan> {
        let (plan, changed) = input_materialization::optimize_plan(plan, &ctx.bind_context)?;
        if changed {
            ctx.invalidations.mark_aggregate_schema();
        }
        Ok(plan)
    }
}

pub struct AggregateSingletonGroupsPass;

impl Rewriter for AggregateSingletonGroupsPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::AggregateSingletonGroups
    }

    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan> {
        Ok(singleton_groups::optimize_plan(plan, &ctx.column_stats))
    }
}

pub struct StatisticsGatheringPass;

/// Reuse a grouped aggregate to derive an alpha-equivalent scalar reduction.
pub struct AggregatePostReductionPass;

impl Rewriter for AggregatePostReductionPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::AggregatePostReduction
    }

    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan> {
        Ok(post_reduction::optimize_plan(plan, &ctx.bind_context))
    }
}

impl Rewriter for StatisticsGatheringPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::StatisticsGathering
    }

    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan> {
        ctx.column_stats.clear();
        StatisticsGathering::new().gather(plan, ctx)
    }
}

pub struct ReorderFilterPass;

impl Rewriter for ReorderFilterPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::ReorderFilter
    }

    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan> {
        ReorderFilter::new().rewrite(plan, ctx)
    }
}

pub struct UnusedColumnsPass {
    pub binder: Binder,
}

impl Rewriter for UnusedColumnsPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::UnusedColumns
    }

    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan> {
        let mut plan = plan;
        RemoveUnusedColumns::optimize(&mut plan, &self.binder, ctx.session.as_ref(), true);
        // Get compaction reuses a table index with new column ordinals. A
        // keyed statistic cannot be remapped in place without risking a
        // collision with a different former ordinal, so pruning explicitly
        // invalidates the whole binding domain. Every downstream statistics
        // consumer must sit behind an observable StatisticsGathering pass.
        ctx.column_stats.clear();
        Ok(plan)
    }
}

pub struct ColumnLifetimePass;

pub struct LatePayloadFetchPass;

impl Rewriter for LatePayloadFetchPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::LateMaterialization
    }

    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan> {
        let (plan, changed) =
            late_payload::optimize_plan(plan, &ctx.bind_context, &ctx.cost_model)?;
        if changed {
            ctx.invalidations.mark_late_materialization();
        }
        Ok(plan)
    }
}

/// Produces a derived scan binding for an exact ASCII prefix that is already
/// proven by the directly fused scan predicate. This is deliberately not a
/// late-materialization pass: no stored value changes identity and no row
/// fetch is introduced.
pub struct MatchedPrefixScanProjectionPass;

impl Rewriter for MatchedPrefixScanProjectionPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::MatchedPrefixScanProjection
    }

    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan> {
        if !ctx.session.settings.rowset_scan_pushdown() {
            return Ok(plan);
        }
        let (plan, changed) = late_payload::optimize_matched_prefix_plan(plan)?;
        if changed {
            ctx.invalidations.mark_scan_projection();
        }
        Ok(plan)
    }
}

impl Rewriter for ColumnLifetimePass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::ColumnLifetime
    }

    fn rewrite(
        &mut self,
        plan: LogicalPlan,
        _ctx: &mut OptimizationContext,
    ) -> Result<LogicalPlan> {
        ColumnLifetimeAnalyzer::new(true).optimize(plan)
    }
}

pub struct BuildProbeSidePass;

impl Rewriter for BuildProbeSidePass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::BuildProbeSide
    }

    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan> {
        Ok(BuildProbeSideOptimizer::new(ctx.session.clone()).optimize_plan(plan))
    }
}

pub struct JoinFilterPushdownPass;

impl Rewriter for JoinFilterPushdownPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::JoinFilterPushdown
    }

    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan> {
        Ok(JoinFilterPushdown::new(ctx.session.clone()).optimize_plan(plan))
    }
}

pub struct MixedJoinPredicatePass;

impl Rewriter for MixedJoinPredicatePass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::MixedJoinPredicate
    }

    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan> {
        JoinPredicateNormalizer::new(&ctx.bind_context).optimize_plan(plan)
    }
}

pub struct TopNPass;

impl Rewriter for TopNPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::TopN
    }

    fn rewrite(
        &mut self,
        plan: LogicalPlan,
        _ctx: &mut OptimizationContext,
    ) -> Result<LogicalPlan> {
        Ok(TopNOptimizer::new().optimize_plan(plan))
    }
}

pub struct LimitPushdownPass;

impl Rewriter for LimitPushdownPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::LimitPushdown
    }

    fn rewrite(
        &mut self,
        plan: LogicalPlan,
        _ctx: &mut OptimizationContext,
    ) -> Result<LogicalPlan> {
        Ok(LimitPushdown::new().optimize_plan(plan))
    }
}

pub struct SearchOptimizationPass;

impl Rewriter for SearchOptimizationPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::SearchOptimization
    }

    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan> {
        SearchOptimizer::new().rewrite(plan, ctx)
    }
}

pub struct InClausePass;

impl Rewriter for InClausePass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::InClause
    }

    fn rewrite(
        &mut self,
        plan: LogicalPlan,
        _ctx: &mut OptimizationContext,
    ) -> Result<LogicalPlan> {
        InClauseRewriter::new().rewrite(plan)
    }
}

pub struct SegmentPrunerPass;

impl Rewriter for SegmentPrunerPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::SegmentPruner
    }

    fn rewrite(
        &mut self,
        plan: LogicalPlan,
        _ctx: &mut OptimizationContext,
    ) -> Result<LogicalPlan> {
        Ok(SegmentPruner::new().optimize(plan))
    }
}

/// [`StatisticsPropagator`] followed by [`AggregateStatisticsExecutor`] as a single pipeline stage.
pub struct StatisticsPropagationPass;

impl Rewriter for StatisticsPropagationPass {
    fn optimizer_type(&self) -> OptimizerType {
        OptimizerType::StatisticsPropagation
    }

    fn rewrite(&mut self, plan: LogicalPlan, ctx: &mut OptimizationContext) -> Result<LogicalPlan> {
        let mut propagator = StatisticsPropagator::new();
        let plan = propagator.propagate(ctx.session.clone(), plan);
        let mut executor = AggregateStatisticsExecutor::new();
        Ok(executor.optimize(plan))
    }
}
