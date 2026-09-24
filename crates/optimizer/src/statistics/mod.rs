// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Statistics propagation and cost estimation.

pub(crate) mod aggregate_filter;
pub(crate) mod cardinality_bound;
pub mod cost;
pub mod gathering;
pub mod propagator;
pub(crate) mod relation_proofs;
pub(crate) mod unique_keys;

/// The query construction boundary owns column-domain propagation followed by
/// relation properties. Propagation reads storage/bound-reference evidence,
/// not provisional NodeStats; gathering then sees the completed domains and
/// publishes CTE/delim producers before their dependent siblings. Do not run a
/// preliminary gather whose annotations are immediately invalidated/replaced.
pub(crate) fn settle_query_properties(
    plan: paro_planner::plan::OwnedLogicalPlan,
    context: &mut crate::context::OptimizationContext,
) -> paro_common::error::Result<paro_planner::plan::OwnedLogicalPlan> {
    let mut propagation = propagator::StatisticsPropagator::new();
    let plan = propagation.propagate(context.session.clone(), plan);
    context.column_stats = std::sync::Arc::new(propagation.take_statistics_map());
    gathering::StatisticsGathering::new().gather(plan, context)
}
