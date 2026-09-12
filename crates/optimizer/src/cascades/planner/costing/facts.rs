// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Group-resolved cardinality and row-width inputs for physical costing.

use super::*;

pub(in crate::cascades::planner) fn planner_cost_facts(
    plan: &OwnedLogicalPlan,
    column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    binding_ids: &BindingCatalog,
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
            .and_then(|table| table.get_storage())
            // Task supply describes physical source work, not an ANALYZE
            // catalog estimate. The latter can be absent on a fully populated
            // table, or stale after an append. Capture storage's row evidence
            // on this cost-fact boundary; it is advisory, never a row bound.
            .and_then(|storage| storage.total_rows().ok())
            .map(|rows| rows as u64),
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
    let mut result = PlannerCostFacts {
        child_row_widths,
        child_materialization_risk_rows,
        output_row_width,
        hash_key_width,
        scan_access_width,
        scan_physical_rows,
        scan_work_source,
        perfect_hash,
        topn_capacity,
        runtime_filter_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        runtime_filter_build_left_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        runtime_filter_probe_source_rows: None,
        runtime_filter_build_left_probe_source_rows: None,
        runtime_filter_probe_sources: Box::new([]),
        runtime_filter_build_left_probe_sources: Box::new([]),
        runtime_filter_build_distinct_expected: None,
        runtime_filter_build_domain_column: None,
        runtime_filter_build_left_distinct_expected: None,
        runtime_filter_build_left_domain_column: None,
        runtime_filter_key_types,
    };
    if let LogicalOperator::Join(Join::Comparison(join)) = &plan.operator {
        fill_runtime_filter_cost_facts(
            &mut result,
            join,
            RuntimeFilterInput::Owned(&join.left),
            RuntimeFilterInput::Owned(&join.right),
            column_stats,
            binding_ids,
        );
    }
    Ok(result)
}

/// Build cost facts directly from a closed native operator shell.  The child
/// tree has already been reduced to immutable `NodeState` facts by staging, so
/// recreating `BoundReference` plans here would only pay owned-IR allocation
/// without adding evidence. RF source work is derived by the same routines as
/// owned staging, using the boundary's complete source/occurrence coverage.
pub(in crate::cascades::planner) fn planner_native_cost_facts<Child>(
    operator: &LogicalOperator<Child>,
    child_materialization_risk_rows: &[u64],
    child_row_widths: &[u64],
    output_row_width: u64,
    scan_access_cost: paro_storage::rowset::scan_cost::ScanAccessCostModel,
    inputs: &[RuntimeFilterInput<'_>],
    column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    binding_ids: &BindingCatalog,
) -> Result<PlannerCostFacts> {
    let mut operator_child_count = 0;
    operator.visit_child_links(&mut |_| operator_child_count += 1);
    if operator_child_count != child_materialization_risk_rows.len()
        || child_materialization_risk_rows.len() != child_row_widths.len()
    {
        return Err(paro_error::internal(
            "native cost facts child stats/width arity mismatch",
        ));
    }
    let child_materialization_risk_rows = child_materialization_risk_rows
        .iter()
        .copied()
        .collect::<Box<[_]>>();
    let hash_key_width = match operator {
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
    let perfect_hash = match operator {
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
    let topn_capacity = match operator {
        LogicalOperator::TopN(topn) => Some(
            u64::try_from(topn.limit)
                .unwrap_or(u64::MAX)
                .saturating_add(u64::try_from(topn.offset).unwrap_or(u64::MAX)),
        ),
        _ => None,
    };
    let runtime_filter_key_types = match operator {
        LogicalOperator::Join(Join::Comparison(join)) => join
            .conditions
            .iter()
            .filter(|condition| condition.comparison == JoinComparisonType::Equal)
            .map(|condition| condition.right.return_type())
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        _ => Box::new([]),
    };
    let mut result = PlannerCostFacts {
        child_row_widths: child_row_widths.iter().copied().collect(),
        child_materialization_risk_rows,
        output_row_width,
        hash_key_width,
        scan_access_width: None,
        scan_physical_rows: None,
        scan_work_source: None,
        perfect_hash,
        topn_capacity,
        runtime_filter_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        runtime_filter_build_left_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        runtime_filter_probe_source_rows: None,
        runtime_filter_build_left_probe_source_rows: None,
        runtime_filter_probe_sources: Box::new([]),
        runtime_filter_build_left_probe_sources: Box::new([]),
        runtime_filter_build_distinct_expected: None,
        runtime_filter_build_domain_column: None,
        runtime_filter_build_left_distinct_expected: None,
        runtime_filter_build_left_domain_column: None,
        runtime_filter_key_types,
    };
    if let (LogicalOperator::Join(Join::Comparison(join)), [left, right]) = (operator, inputs) {
        fill_runtime_filter_cost_facts(&mut result, join, *left, *right, column_stats, binding_ids);
    }
    Ok(result)
}

fn fill_runtime_filter_cost_facts<Child>(
    result: &mut PlannerCostFacts,
    join: &paro_planner::operator::join::ComparisonJoin<Child>,
    left: RuntimeFilterInput<'_>,
    right: RuntimeFilterInput<'_>,
    column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    binding_ids: &BindingCatalog,
) {
    let left_keys = join
        .conditions
        .iter()
        .filter(|condition| condition.comparison == JoinComparisonType::Equal)
        .map(|condition| &condition.left)
        .collect::<Vec<_>>();
    let right_keys = join
        .conditions
        .iter()
        .filter(|condition| condition.comparison == JoinComparisonType::Equal)
        .map(|condition| &condition.right)
        .collect::<Vec<_>>();
    result.runtime_filter_probe_multiplicity = left.probe_multiplicity(&left_keys);
    result.runtime_filter_build_left_probe_multiplicity = right.probe_multiplicity(&right_keys);
    result.runtime_filter_probe_source_rows =
        runtime_filter_input_source_rows(left, left_keys.iter().copied());
    result.runtime_filter_build_left_probe_source_rows =
        runtime_filter_input_source_rows(right, right_keys.iter().copied());
    result.runtime_filter_probe_sources =
        runtime_filter_input_source_facts(left, left_keys.iter().copied()).unwrap_or_default();
    result.runtime_filter_build_left_probe_sources =
        runtime_filter_input_source_facts(right, right_keys.iter().copied()).unwrap_or_default();
    let left_bindings = left.bindings();
    let right_bindings = right.bindings();
    result.runtime_filter_build_distinct_expected =
        join_key_distinct_expected(join, column_stats, JoinKeySide::Right, &right_bindings);
    result.runtime_filter_build_left_distinct_expected =
        join_key_distinct_expected(join, column_stats, JoinKeySide::Left, &left_bindings);
    result.runtime_filter_build_domain_column =
        join_key_domain_column(join, binding_ids, JoinKeySide::Right, &right_bindings);
    result.runtime_filter_build_left_domain_column =
        join_key_domain_column(join, binding_ids, JoinKeySide::Left, &left_bindings);
}

pub(in crate::cascades::planner) fn planner_row_width_from_layout(
    layout: &paro_planner::operator::LogicalOutputLayout,
    scan_access_cost: paro_storage::rowset::scan_cost::ScanAccessCostModel,
) -> u64 {
    layout
        .types()
        .iter()
        .map(|logical_type| scan_access_cost.estimated_width(logical_type) as u64)
        .sum::<u64>()
        .saturating_add(std::mem::size_of::<u64>() as u64)
}

pub(in crate::cascades::planner) fn planner_row_width(
    plan: &OwnedLogicalPlan,
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
        runtime_filter_build_distinct_expected: children
            .get(1)
            .zip(template.runtime_filter_build_domain_column)
            .and_then(|(group, column)| {
                memo.column_domain(*group, column)
                    .and_then(|domain| domain.expected())
            })
            .or(template.runtime_filter_build_distinct_expected),
        runtime_filter_build_left_distinct_expected: children
            .first()
            .zip(template.runtime_filter_build_left_domain_column)
            .and_then(|(group, column)| {
                memo.column_domain(*group, column)
                    .and_then(|domain| domain.expected())
            })
            .or(template.runtime_filter_build_left_distinct_expected),
        runtime_filter_key_types: template.runtime_filter_key_types.clone(),
    })
}

#[derive(Debug, Clone, Copy)]
enum JoinKeySide {
    Left,
    Right,
}

fn join_key_distinct_expected<Child>(
    join: &paro_planner::operator::join::ComparisonJoin<Child>,
    column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    side: JoinKeySide,
    bindings: &[ColumnBinding],
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
    let expression = match side {
        JoinKeySide::Left => &condition.left,
        JoinKeySide::Right => &condition.right,
    };
    let binding = match expression {
        Expression::ColumnRef(column) if column.depth == 0 => column.binding,
        Expression::Reference(reference) => *bindings.get(reference.index)?,
        _ => return None,
    };
    column_stats
        .get(&binding)
        .map(|statistics| statistics.distinct_evidence().point)
        .filter(|distinct| *distinct > 0)
}

fn join_key_domain_column<Child>(
    join: &paro_planner::operator::join::ComparisonJoin<Child>,
    binding_ids: &BindingCatalog,
    side: JoinKeySide,
    bindings: &[ColumnBinding],
) -> Option<ColumnId> {
    let mut equalities = join
        .conditions
        .iter()
        .filter(|condition| condition.comparison == JoinComparisonType::Equal);
    let condition = equalities.next()?;
    if equalities.next().is_some() {
        return None;
    }
    let expression = match side {
        JoinKeySide::Left => &condition.left,
        JoinKeySide::Right => &condition.right,
    };
    let (binding, logical_type) = match expression {
        Expression::ColumnRef(column) if column.depth == 0 => {
            (column.binding, column.return_type.clone())
        }
        Expression::Reference(reference) => (
            *bindings.get(reference.index)?,
            reference.return_type.clone(),
        ),
        _ => return None,
    };
    binding_ids
        .get(binding.table_index, binding.column_index, &logical_type)
        .copied()
}

pub(super) fn infer_runtime_filter_probe_multiplicity<'a>(
    plan: &OwnedLogicalPlan,
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
        .map(|statistics| statistics.distinct_evidence().point)
        .filter(|distinct| *distinct > 0)
    else {
        return RuntimeFilterProbeMultiplicity::Unknown;
    };
    RuntimeFilterProbeMultiplicity::EstimatedDistinct {
        keys: distinct.min(rows as u64),
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
