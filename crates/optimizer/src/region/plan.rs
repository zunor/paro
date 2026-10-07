// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Own the bounded region program, including annotation after committed choices.
use super::{
    aggregate as aggregate_region, join::planner::JoinRegionPlanner, limits::RegionLimits,
};
use crate::{
    context::OptimizationContext,
    cost::calibration::MachineCalibrationBundle,
    estimate::AnnotatedRelation,
    physical::ResourceGrantClass,
    rewrite::{limit::topn::TopNOptimizer, program::Normalization},
};
use paro_common::error::Result;
use paro_planner::{binder::Binder, logical::plan::OwnedLogicalPlan};

pub(crate) struct PlannedRegions {
    pub relation: AnnotatedRelation,
    pub aggregates: aggregate_region::Work,
    pub joins: aggregate_region::Work,
}

pub(crate) fn plan(
    mut plan: OwnedLogicalPlan,
    ctx: &mut OptimizationContext,
    binder: &Binder,
    grant: ResourceGrantClass,
    calibration: &MachineCalibrationBundle,
    limits: &RegionLimits,
) -> Result<PlannedRegions> {
    // Each aggregate region has a bounded grain domain: original SQL
    // grain, or a proven partial-state/merge decomposition. Join DP runs
    // inside each legal state before comparing it; neither state's cost
    // may be based on the incoming, unoptimized join order.
    let mut region_work = aggregate_region::Work::default();
    plan = plan.try_map_post_order(|input| {
        ctx.session.cancellation.check()?;
        let Some(selected) = aggregate_region::optimize(
            &input,
            ctx,
            grant,
            calibration,
            limits.connected_pairs as usize,
            &mut region_work,
        )?
        else {
            return Ok(input);
        };
        ctx.column_stats_mut().extend(selected.columns);
        Ok(selected.plan)
    })?;
    // Every committed rewrite reopens predicate routing before join DP.
    // Required domains must reach the source, not remain as join residuals.
    plan = crate::rewrite::predicate::canonical::predicates(plan, &mut Default::default());
    let candidate = Normalization { ctx, binder }.settle_query_candidate(plan)?;
    ctx.column_stats = candidate.column_stats;
    let mut roots = std::collections::HashSet::new();
    let mut pending = vec![(&candidate.plan, false)];
    while let Some((node, inside)) = pending.pop() {
        let is_region = aggregate_region::join_root(node);
        if is_region && !inside {
            roots.insert(node.id);
        }
        pending.extend(node.children().into_iter().map(|child| (child, is_region)));
    }
    let mut join_work = aggregate_region::Work::default();
    let joined = candidate.plan.try_map_post_order(|input| {
        if roots.contains(&input.id) {
            if let Some(selected) = aggregate_region::optimize_joins(
                &input,
                ctx,
                grant,
                calibration,
                limits.connected_pairs as usize,
                &mut join_work,
            )? {
                ctx.column_stats_mut().extend(selected.columns);
                return Ok(selected.plan);
            }
        }
        Ok(input)
    })?;
    plan = JoinRegionPlanner::new(ctx.cost_model.defaults.clone())
        .with_limits(limits)
        .with_physical_pricing(calibration)?
        .optimize_regions(
            ctx.session.as_ref(),
            joined,
            &ctx.column_stats,
            &ctx.bind_context,
        )?;
    plan = crate::rewrite::predicate::canonical::predicates(plan, &mut Default::default());
    plan = TopNOptimizer::new().optimize_plan(plan);
    // Access paths are local decisions over a committed tree. Visit the
    // outer window first so a filter rewrite cannot destroy a TopK window.
    plan = crate::physical::choose::access(plan, ctx)?;
    let candidate = Normalization { ctx, binder }.settle_query_candidate(plan)?;

    Ok(PlannedRegions {
        relation: candidate,
        aggregates: region_work,
        joins: join_work,
    })
}
