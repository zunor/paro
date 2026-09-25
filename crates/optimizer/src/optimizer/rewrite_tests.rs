// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::physical::access::late_payload;
use crate::region::join::planner::JoinRegionPlanner;
use crate::rewrite::cte::inlining::CTEInlining;
use crate::rewrite::predicate::{pullup::FilterPullup, pushdown::FilterPushdown};
use crate::rewrite::subquery::partition_aggregate::CorrelatedPartitionAggregate;
use crate::rewrite::subquery::{scalar_aggregate_fusion, scalar_aggregate_window};
type CandidatePlan = crate::estimate::AnnotatedRelation;

impl Optimizer {
    #[cfg(test)]
    pub(super) fn correlated_aggregate_candidate(
        &self,
        plan: OwnedLogicalPlan,
    ) -> Result<CandidatePlan> {
        let input_shape = tracing::enabled!(target: targets::OPTIMIZER, tracing::Level::DEBUG)
            .then(|| logical_plan_shape(&plan));
        let ordered = JoinRegionPlanner::new(self.ctx.cost_model.defaults.clone())
            .with_limits(&self.limits)
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
        self.normalization().settle_query_candidate(candidate)
    }

    #[cfg(test)]
    pub(super) fn prepare_correlated_seed(&self, plan: OwnedLogicalPlan) -> OwnedLogicalPlan {
        // Correlated-region exploration must preserve sharing ownership. A
        // CTE reference is a semantic relation leaf whose producer statistics
        // and execution contract remain owned by MaterializedCTE; duplicating
        // the producer here makes decorrelation and sharing mutually exclusive
        // alternatives for no semantic reason.
        let mut candidate = FilterPullup::new().rewrite_plan(plan);
        candidate = FilterPushdown::new().rewrite_plan(candidate);
        candidate
    }

    #[cfg(test)]
    pub(super) fn scalar_reuse_candidate(&self, candidate: CandidatePlan) -> Result<CandidatePlan> {
        let context = self.ctx.fork_for_candidate(candidate.column_stats.clone());
        let mut candidate = JoinRegionPlanner::new(self.ctx.cost_model.defaults.clone())
            .with_limits(&self.limits)
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
        self.normalization().settle_query_candidate(candidate)
    }
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
