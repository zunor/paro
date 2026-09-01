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
    let topn_capacity = match &plan.operator {
        LogicalOperator::TopN(topn) => Some(
            u64::try_from(topn.limit)
                .unwrap_or(u64::MAX)
                .saturating_add(u64::try_from(topn.offset).unwrap_or(u64::MAX)),
        ),
        _ => None,
    };
    let runtime_filter_probe_multiplicity = match &plan.operator {
        LogicalOperator::Join(Join::Comparison(join)) => runtime_filter_probe_multiplicity(join),
        _ => RuntimeFilterProbeMultiplicity::Unknown,
    };
    let runtime_filter_probe_source_rows = match &plan.operator {
        LogicalOperator::Join(Join::Comparison(join)) => {
            super::runtime_filter_probe_source_rows(join)
        }
        _ => None,
    };
    Ok(PlannerCostFacts {
        child_row_widths,
        output_row_width,
        perfect_hash,
        topn_capacity,
        runtime_filter_probe_multiplicity,
        runtime_filter_probe_source_rows,
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
        topn_capacity: template.topn_capacity,
        runtime_filter_probe_multiplicity: template.runtime_filter_probe_multiplicity,
        runtime_filter_probe_source_rows: template
            .runtime_filter_probe_source_rows
            .map(|rows| CompactRange::new(rows.min as f64, rows.expected as f64, rows.max as f64))
            .transpose()?,
    })
}

fn runtime_filter_probe_multiplicity(
    join: &paro_planner::operator::ComparisonJoin,
) -> RuntimeFilterProbeMultiplicity {
    let get = match &join.left.operator {
        LogicalOperator::Get(get) => get,
        LogicalOperator::Filter(filter) if filter.projection_map.is_all() => {
            let LogicalOperator::Get(get) = &filter.child.operator else {
                return RuntimeFilterProbeMultiplicity::Unknown;
            };
            get
        }
        _ => return RuntimeFilterProbeMultiplicity::Unknown,
    };
    let equality_bindings = join
        .conditions
        .iter()
        .filter_map(|condition| {
            if condition.comparison != JoinComparisonType::Equal {
                return None;
            }
            match &condition.left {
                Expression::ColumnRef(column) if column.depth == 0 => Some(column.binding),
                _ => None,
            }
        })
        .collect::<std::collections::HashSet<_>>();
    if crate::statistics::unique_keys::declared_unique_keys(get)
        .iter()
        .any(|key| {
            !key.bindings.is_empty()
                && key
                    .bindings
                    .iter()
                    .all(|binding| equality_bindings.contains(binding))
        })
    {
        return RuntimeFilterProbeMultiplicity::DeclaredUnique;
    }
    let bindings = equality_bindings.iter().copied().collect::<Vec<_>>();
    let [binding] = bindings.as_slice() else {
        return RuntimeFilterProbeMultiplicity::Unknown;
    };
    if binding.table_index != get.table_index {
        return RuntimeFilterProbeMultiplicity::Unknown;
    }
    let column_index = binding.column_index;
    let Some(column_id) = get.stored_column(column_index) else {
        return RuntimeFilterProbeMultiplicity::Unknown;
    };
    let Some(storage) = get.table.as_ref().and_then(|table| table.get_storage()) else {
        return RuntimeFilterProbeMultiplicity::Unknown;
    };
    let Some(rows) = storage.total_rows().ok().filter(|rows| *rows > 0) else {
        return RuntimeFilterProbeMultiplicity::Unknown;
    };
    let Some(distinct) = storage
        .column_statistics(column_id)
        .map(|statistics| statistics.get_distinct_count())
        .filter(|distinct| *distinct > 0)
    else {
        return RuntimeFilterProbeMultiplicity::Unknown;
    };
    if distinct.saturating_mul(10) >= rows.saturating_mul(9) {
        RuntimeFilterProbeMultiplicity::EstimatedUnique
    } else {
        RuntimeFilterProbeMultiplicity::Unknown
    }
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
