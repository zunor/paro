// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

pub(crate) mod column;
pub(crate) mod relation;

/// The query construction boundary owns column-domain propagation followed by
/// relation properties. Propagation reads storage/bound-reference evidence,
/// not provisional NodeStats; gathering then sees the completed domains and
/// publishes CTE/delim producers before their dependent siblings. Do not run a
/// preliminary gather whose annotations are immediately invalidated/replaced.
pub(crate) fn annotate(
    plan: paro_planner::logical::plan::OwnedLogicalPlan,
    context: &mut crate::context::OptimizationContext,
) -> paro_common::error::Result<paro_planner::logical::plan::OwnedLogicalPlan> {
    let mut propagation = column::StatisticsPropagator::new();
    let plan = propagation.propagate(context.session.clone(), plan);
    context.column_stats = std::sync::Arc::new(propagation.take_statistics_map());
    relation::StatisticsGathering::new().gather(plan, context)
}
