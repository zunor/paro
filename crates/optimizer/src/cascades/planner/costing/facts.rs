// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Group-resolved cardinality and row-width inputs for physical costing.

use super::*;

pub(in crate::cascades::planner) fn planner_cost_facts(
    plan: &LogicalPlan,
    scan_access_cost: paro_storage::rowset::scan_cost::ScanAccessCostModel,
) -> Result<PlannerCostFacts> {
    let children = plan.children();
    let child_row_widths = children
        .iter()
        .map(|child| planner_row_width(child, scan_access_cost))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let output_row_width = planner_row_width(plan, scan_access_cost);
    let perfect_hash = match &plan.operator {
        LogicalOperator::Aggregate(aggregate) => {
            crate::physical::aggregate_planning::plan_perfect_hash_aggregate(
                aggregate,
                &aggregate.groups,
                &aggregate.aggregates,
            )
            .map(|plan| plan.resource)
        }
        _ => None,
    };
    Ok(PlannerCostFacts {
        child_row_widths,
        output_row_width,
        perfect_hash,
    })
}

pub(in crate::cascades::planner) fn planner_row_width(
    plan: &LogicalPlan,
    scan_access_cost: paro_storage::rowset::scan_cost::ScanAccessCostModel,
) -> u64 {
    plan.types()
        .iter()
        .map(|logical_type| scan_access_cost.estimated_width(logical_type) as u64)
        .sum::<u64>()
        .saturating_add(std::mem::size_of::<u64>() as u64)
}

pub(in crate::cascades::planner) fn expression_cost_facts(
    memo: &Memo,
    group: GroupId,
    children: &[GroupId],
    template: &PlannerCostFacts,
) -> Result<ResolvedPlannerCostFacts> {
    let output = memo
        .group(group)
        .ok_or_else(|| paro_error::internal("costing references an unknown output group"))?;
    let output_rows = group_cardinality_work_range(memo.cardinality_envelope(group))?;
    let output_rows_hard_upper = output.logical_properties.maximum_cardinality;
    let child_rows = children
        .iter()
        .map(|child| {
            memo.group(*child)
                .ok_or_else(|| paro_error::internal("costing references an unknown child group"))?;
            group_cardinality_work_range(memo.cardinality_envelope(*child))
        })
        .collect::<Result<Vec<_>>>()?
        .into_boxed_slice();
    let child_rows_hard_upper = children
        .iter()
        .map(|child| {
            memo.group(*child)
                .and_then(|group| group.logical_properties.maximum_cardinality)
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Ok(ResolvedPlannerCostFacts {
        output_rows,
        child_rows,
        output_rows_hard_upper,
        child_rows_hard_upper,
        child_row_widths: template.child_row_widths.clone(),
        output_row_width: template.output_row_width,
        perfect_hash: template.perfect_hash,
    })
}

fn group_cardinality_work_range(cardinality: Option<CardinalityEnvelope>) -> Result<CompactRange> {
    match cardinality {
        Some(range) => CompactRange::new(
            range.lower as f64,
            range
                .expected_lower
                .saturating_add(range.expected_upper.saturating_sub(range.expected_lower) / 2)
                as f64,
            range.upper as f64,
        ),
        None => CompactRange::new(0.0, 1.0, 4.0),
    }
}
