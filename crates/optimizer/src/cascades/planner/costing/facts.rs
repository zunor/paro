// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Group-resolved cardinality and row-width inputs for physical costing.

use super::*;

pub(in crate::cascades::planner) fn planner_cost_facts(
    plan: &LogicalPlan,
    column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    scan_access_cost: paro_storage::rowset::scan_cost::ScanAccessCostModel,
) -> Result<PlannerCostFacts> {
    let children = plan.children();
    let child_row_widths = children
        .iter()
        .map(|child| planner_row_width(child, scan_access_cost))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let child_materialization_risk_rows = children
        .iter()
        .map(|child| {
            child
                .stats
                .materialization_risk_cardinality
                .or_else(|| child.stats.estimated_cardinality.map(|rows| rows.max))
                .unwrap_or(1)
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let output_row_width = planner_row_width(plan, scan_access_cost);
    let hash_key_width = match &plan.operator {
        LogicalOperator::Aggregate(aggregate) => Some(
            aggregate
                .groups
                .iter()
                .map(|group| scan_access_cost.estimated_width(&group.return_type()) as u64)
                .sum(),
        ),
        LogicalOperator::Join(Join::Comparison(join))
            if join.conditions.iter().any(|condition| {
                matches!(
                    condition.comparison,
                    JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
                )
            }) =>
        {
            Some(
                join.conditions
                    .iter()
                    .filter(|condition| {
                        matches!(
                            condition.comparison,
                            JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
                        )
                    })
                    .map(|condition| {
                        scan_access_cost.estimated_width(&condition.right.return_type()) as u64
                    })
                    .sum(),
            )
        }
        _ => None,
    };
    let scan_access_width = match &plan.operator {
        LogicalOperator::Get(get) => Some(planner_scan_access_width(get, scan_access_cost)),
        _ => None,
    };
    let scan_physical_rows = match &plan.operator {
        LogicalOperator::Get(get) => get
            .table
            .as_ref()
            .and_then(|table| table.statistics())
            .map(|statistics| statistics.row_count)
            .filter(|rows| *rows > 0),
        _ => None,
    };
    let scan_work_source = match &plan.operator {
        LogicalOperator::Get(get) if get.table.is_some() => Some(WorkSourceId(get.table_index)),
        LogicalOperator::SearchScan(search) if search.get.table.is_some() => {
            Some(WorkSourceId(search.get.table_index))
        }
        LogicalOperator::FullTextFilterScan(search) if search.get.table.is_some() => {
            Some(WorkSourceId(search.get.table_index))
        }
        _ => None,
    };
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
        LogicalOperator::Join(Join::Comparison(join)) => infer_runtime_filter_probe_multiplicity(
            &join.left,
            join.conditions
                .iter()
                .filter(|condition| condition.comparison == JoinComparisonType::Equal)
                .map(|condition| &condition.left),
        ),
        _ => RuntimeFilterProbeMultiplicity::Unknown,
    };
    let runtime_filter_build_left_probe_multiplicity = match &plan.operator {
        LogicalOperator::Join(Join::Comparison(join)) => infer_runtime_filter_probe_multiplicity(
            &join.right,
            join.conditions
                .iter()
                .filter(|condition| condition.comparison == JoinComparisonType::Equal)
                .map(|condition| &condition.right),
        ),
        _ => RuntimeFilterProbeMultiplicity::Unknown,
    };
    let runtime_filter_probe_source_rows = match &plan.operator {
        LogicalOperator::Join(Join::Comparison(join)) => {
            super::runtime_filter_probe_source_rows(join)
        }
        _ => None,
    };
    let runtime_filter_build_left_probe_source_rows = match &plan.operator {
        LogicalOperator::Join(Join::Comparison(join)) => {
            super::runtime_filter_build_left_probe_source_rows(join)
        }
        _ => None,
    };
    let runtime_filter_probe_sources = match &plan.operator {
        LogicalOperator::Join(Join::Comparison(join)) => {
            super::runtime_filter_probe_sources(join).unwrap_or_default()
        }
        _ => Box::new([]),
    };
    let runtime_filter_build_left_probe_sources = match &plan.operator {
        LogicalOperator::Join(Join::Comparison(join)) => {
            super::runtime_filter_build_left_probe_sources(join).unwrap_or_default()
        }
        _ => Box::new([]),
    };
    let runtime_filter_build_distinct_expected = match &plan.operator {
        LogicalOperator::Join(Join::Comparison(join)) => {
            join_key_distinct_expected(join, column_stats, JoinKeySide::Right)
        }
        _ => None,
    };
    let runtime_filter_build_left_distinct_expected = match &plan.operator {
        LogicalOperator::Join(Join::Comparison(join)) => {
            join_key_distinct_expected(join, column_stats, JoinKeySide::Left)
        }
        _ => None,
    };
    let runtime_filter_key_types = match &plan.operator {
        LogicalOperator::Join(Join::Comparison(join)) => join
            .conditions
            .iter()
            .filter(|condition| condition.comparison == JoinComparisonType::Equal)
            .map(|condition| condition.right.return_type())
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        _ => Box::new([]),
    };
    Ok(PlannerCostFacts {
        child_row_widths,
        child_materialization_risk_rows,
        output_row_width,
        hash_key_width,
        scan_access_width,
        scan_physical_rows,
        scan_work_source,
        perfect_hash,
        topn_capacity,
        runtime_filter_probe_multiplicity,
        runtime_filter_build_left_probe_multiplicity,
        runtime_filter_probe_source_rows,
        runtime_filter_build_left_probe_source_rows,
        runtime_filter_probe_sources,
        runtime_filter_build_left_probe_sources,
        runtime_filter_build_distinct_expected,
        runtime_filter_build_left_distinct_expected,
        runtime_filter_key_types,
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

/// Bytes physically sourced by one base-table scan row. Virtual rowids are
/// already available from the scan cursor and therefore carry through parent
/// tuples without reading a stored column. Derived prefixes pay only their
/// bounded produced width; duplicate stored projections share one source.
pub(in crate::cascades::planner) fn planner_scan_access_width(
    get: &paro_planner::operator::Get,
    scan_access_cost: paro_storage::rowset::scan_cost::ScanAccessCostModel,
) -> u64 {
    use paro_planner::operator::GetColumnSource;

    let mut stored_widths = std::collections::BTreeMap::<usize, u64>::new();
    for (index, source) in get.column_sources.iter().enumerate() {
        match source {
            GetColumnSource::Stored { column_id } => {
                let source_width = get
                    .column_types
                    .get(index)
                    .map(|ty| scan_access_cost.estimated_width(ty) as u64)
                    .unwrap_or(0);
                stored_widths
                    .entry(*column_id)
                    .and_modify(|width| *width = (*width).max(source_width))
                    .or_insert(source_width);
            }
            GetColumnSource::MatchedUtf8Prefix {
                source_column,
                byte_width,
            } => {
                let source_width = u64::try_from(*byte_width).unwrap_or(u64::MAX);
                stored_widths
                    .entry(*source_column)
                    .and_modify(|width| *width = (*width).max(source_width))
                    .or_insert(source_width);
            }
            GetColumnSource::VirtualRowId => {}
        }
    }
    stored_widths
        .values()
        .copied()
        .fold(0u64, u64::saturating_add)
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
        child_materialization_risk_rows: template.child_materialization_risk_rows.clone(),
        output_row_width: template.output_row_width,
        hash_key_width: template.hash_key_width,
        scan_access_width: template.scan_access_width,
        scan_physical_rows: template.scan_physical_rows,
        scan_work_source: template.scan_work_source,
        perfect_hash: template.perfect_hash,
        topn_capacity: template.topn_capacity,
        runtime_filter_probe_multiplicity: template.runtime_filter_probe_multiplicity,
        runtime_filter_build_left_probe_multiplicity: template
            .runtime_filter_build_left_probe_multiplicity,
        runtime_filter_probe_source_rows: template
            .runtime_filter_probe_source_rows
            .map(|rows| CompactRange::new(rows.min as f64, rows.expected as f64, rows.max as f64))
            .transpose()?,
        runtime_filter_build_left_probe_source_rows: template
            .runtime_filter_build_left_probe_source_rows
            .map(|rows| CompactRange::new(rows.min as f64, rows.expected as f64, rows.max as f64))
            .transpose()?,
        runtime_filter_probe_sources: template
            .runtime_filter_probe_sources
            .iter()
            .map(|source| {
                Ok(ResolvedRuntimeFilterSource {
                    source: source.source,
                    rows: CompactRange::new(
                        source.rows.min as f64,
                        source.rows.expected as f64,
                        source.rows.max as f64,
                    )?,
                    multiplicity: source.multiplicity,
                })
            })
            .collect::<Result<Vec<_>>>()?
            .into_boxed_slice(),
        runtime_filter_build_left_probe_sources: template
            .runtime_filter_build_left_probe_sources
            .iter()
            .map(|source| {
                Ok(ResolvedRuntimeFilterSource {
                    source: source.source,
                    rows: CompactRange::new(
                        source.rows.min as f64,
                        source.rows.expected as f64,
                        source.rows.max as f64,
                    )?,
                    multiplicity: source.multiplicity,
                })
            })
            .collect::<Result<Vec<_>>>()?
            .into_boxed_slice(),
        runtime_filter_build_distinct_expected: template.runtime_filter_build_distinct_expected,
        runtime_filter_build_left_distinct_expected: template
            .runtime_filter_build_left_distinct_expected,
        runtime_filter_key_types: template.runtime_filter_key_types.clone(),
    })
}

#[derive(Debug, Clone, Copy)]
enum JoinKeySide {
    Left,
    Right,
}

fn join_key_distinct_expected(
    join: &paro_planner::operator::ComparisonJoin,
    column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    side: JoinKeySide,
) -> Option<u64> {
    let mut equalities = join
        .conditions
        .iter()
        .filter(|condition| condition.comparison == JoinComparisonType::Equal);
    let condition = equalities.next()?;
    if equalities.next().is_some() {
        // Per-column membership for a composite key represents a superset of
        // build tuples, so a single-column NDV is not its retained domain.
        return None;
    }
    let (expression, input) = match side {
        JoinKeySide::Left => (&condition.left, join.left.as_ref()),
        JoinKeySide::Right => (&condition.right, join.right.as_ref()),
    };
    let binding = match expression {
        Expression::ColumnRef(column) if column.depth == 0 => column.binding,
        Expression::Reference(reference) => *input.get_column_bindings().get(reference.index)?,
        _ => return None,
    };
    column_stats
        .get(&binding)
        .map(|statistics| statistics.get_distinct_count() as u64)
        .filter(|distinct| *distinct > 0)
}

pub(super) fn infer_runtime_filter_probe_multiplicity<'a>(
    plan: &LogicalPlan,
    equality_expressions: impl IntoIterator<Item = &'a Expression>,
) -> RuntimeFilterProbeMultiplicity {
    let equality_expressions = equality_expressions.into_iter().collect::<Vec<_>>();
    if !equality_expressions.is_empty()
        && crate::statistics::unique_keys::expressions_cover_unique_key(plan, &equality_expressions)
    {
        return RuntimeFilterProbeMultiplicity::DeclaredUnique;
    }
    let get = match &plan.operator {
        LogicalOperator::Get(get) => get,
        LogicalOperator::Filter(filter) if filter.projection_map.is_all() => {
            let LogicalOperator::Get(get) = &filter.child.operator else {
                return RuntimeFilterProbeMultiplicity::Unknown;
            };
            get
        }
        _ => return RuntimeFilterProbeMultiplicity::Unknown,
    };
    let equality_bindings = equality_expressions
        .into_iter()
        .filter_map(|expression| match expression {
            Expression::ColumnRef(column) if column.depth == 0 => Some(column.binding),
            _ => None,
        })
        .collect::<std::collections::HashSet<_>>();
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
    RuntimeFilterProbeMultiplicity::EstimatedDistinct {
        keys: u64::try_from(distinct.min(rows)).unwrap_or(u64::MAX),
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
