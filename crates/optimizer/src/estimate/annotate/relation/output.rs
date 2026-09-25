// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(super) fn collect_output_stats_for_layout(
    layout: &LogicalOutputLayout,
    ctx: &impl ColumnStatsView,
) -> Vec<Arc<ColumnStatistics>> {
    layout
        .types()
        .iter()
        .cloned()
        .zip(layout.bindings().iter().copied())
        .map(|(ty, binding)| {
            ctx.get_stat(&binding)
                .unwrap_or_else(|| ColumnStatistics::create_unknown(ty))
        })
        .collect()
}

pub(super) fn filter_output_stats<Child>(
    filter: &Filter<Child>,
    child_layout: &LogicalOutputLayout,
    ctx: &impl ColumnStatsView,
) -> Vec<Arc<ColumnStatistics>> {
    let mut child_output = collect_output_stats_for_layout(child_layout, ctx);

    fn refine(
        expression: &Expression,
        bindings: &[ColumnBinding],
        output: &mut [Arc<ColumnStatistics>],
    ) {
        if let Expression::Conjunction(conjunction) = expression {
            if conjunction.conjunction_type == ConjunctionType::And {
                for child in &conjunction.children {
                    refine(child, bindings, output);
                }
                return;
            }
        }

        let Some((binding, values)) = finite_equality_domain(expression) else {
            return;
        };
        let Some(index) = bindings.iter().position(|candidate| *candidate == binding) else {
            return;
        };
        let Some((first, rest)) = values.split_first() else {
            return;
        };
        let mut domain = BaseStatistics::from_constant(first);
        for value in rest {
            domain.merge(&BaseStatistics::from_constant(value));
        }
        let Some(statistics) = output.get_mut(index) else {
            return;
        };
        *statistics = Arc::new(
            ColumnStatistics::with_estimated_distinct(domain, Some(values.len()))
                .with_guaranteed_distinct_upper(values.len() as u64),
        );
    }

    for expression in &filter.expressions {
        refine(expression, child_layout.bindings(), &mut child_output);
    }

    filter
        .projection_map
        .to_indices(child_layout.len())
        .into_iter()
        .filter_map(|child_index| {
            child_layout
                .types()
                .get(child_index)
                .cloned()
                .map(|output_type| {
                    child_output
                        .get(child_index)
                        .cloned()
                        .unwrap_or_else(|| ColumnStatistics::create_unknown(output_type))
                })
        })
        .collect()
}

/// Extract a finite value domain proven by an equality predicate.
///
/// OR is accepted only when every branch constrains the same column. The
/// resulting bound follows from the predicate itself and remains valid after
/// DML, unlike a min/max range observed in one table snapshot.
pub(crate) fn finite_equality_domain(
    expression: &Expression,
) -> Option<(ColumnBinding, Vec<Value>)> {
    match expression {
        Expression::Comparison(comparison)
            if matches!(
                comparison.comparison_type,
                ComparisonType::Equal | ComparisonType::NotDistinctFrom
            ) =>
        {
            let (column, constant) = match (comparison.left.as_ref(), comparison.right.as_ref()) {
                (Expression::ColumnRef(column), Expression::Constant(constant))
                | (Expression::Constant(constant), Expression::ColumnRef(column))
                    if column.depth == 0 && !constant.value.is_null() =>
                {
                    (column, constant)
                }
                _ => return None,
            };
            Some((column.binding, vec![constant.value.clone()]))
        }
        Expression::Conjunction(conjunction)
            if conjunction.conjunction_type == ConjunctionType::Or
                && !conjunction.children.is_empty() =>
        {
            let mut binding = None;
            let mut values = Vec::new();
            for child in &conjunction.children {
                let (child_binding, child_values) = finite_equality_domain(child)?;
                if binding.is_some_and(|binding| binding != child_binding) {
                    return None;
                }
                binding = Some(child_binding);
                for value in child_values {
                    if !values.contains(&value) {
                        values.push(value);
                    }
                }
            }
            Some((binding?, values))
        }
        _ => None,
    }
}

pub(super) fn merge_setop_output_stats(
    left_layout: &LogicalOutputLayout,
    right_layout: &LogicalOutputLayout,
    types: &[LogicalType],
    ctx: &impl ColumnStatsView,
) -> Vec<Arc<ColumnStatistics>> {
    types
        .iter()
        .enumerate()
        .map(|(idx, ty)| {
            let left_stats = left_layout
                .bindings()
                .get(idx)
                .and_then(|binding| ctx.get_stat(binding));
            let right_stats = right_layout
                .bindings()
                .get(idx)
                .and_then(|binding| ctx.get_stat(binding));
            merge_column_statistics(left_stats, right_stats, ty.clone())
        })
        .collect()
}

pub(super) fn project_column_statistics(
    statistics: Vec<Arc<ColumnStatistics>>,
    projection: &paro_planner::logical::operator::ProjectionMap,
) -> Vec<Arc<ColumnStatistics>> {
    projection
        .to_indices(statistics.len())
        .into_iter()
        .filter_map(|index| statistics.get(index).cloned())
        .collect()
}

pub(super) fn merge_column_statistics(
    left: Option<Arc<ColumnStatistics>>,
    right: Option<Arc<ColumnStatistics>>,
    ty: LogicalType,
) -> Arc<ColumnStatistics> {
    match (left, right) {
        (Some(left), Some(right)) => {
            let mut merged = left.copy();
            merged.merge(right.as_ref());
            Arc::new(merged)
        }
        (Some(left), None) => left,
        (None, Some(right)) => right,
        (None, None) => ColumnStatistics::create_unknown(ty),
    }
}

pub(super) fn aggregate_expression_statistics(
    expr: &Expression,
    ctx: &impl ColumnStatsView,
    guaranteed_output_rows: Option<u64>,
) -> Arc<ColumnStatistics> {
    let Expression::Aggregate(agg) = expr else {
        return expression_statistics(expr, ctx);
    };

    match agg.function.name.to_ascii_lowercase().as_str() {
        "count" | "count_star" => Arc::new(ColumnStatistics::new(BaseStatistics::new(
            LogicalType::BigInt,
        ))),
        _ if agg.function.preserves_input_domain() && agg.children.len() == 1 => {
            // These aggregates can only publish a value drawn from their
            // input domain. Cap its NDV where the result is produced so every
            // downstream consumer observes self-consistent column statistics.
            let mut statistics = expression_statistics(&agg.children[0], ctx).as_ref().copy();
            if let Some(output_rows) = guaranteed_output_rows {
                statistics = statistics.with_guaranteed_distinct_upper(output_rows);
            }
            Arc::new(statistics)
        }
        _ => ColumnStatistics::create_unknown(agg.return_type.clone()),
    }
}

pub(super) fn expression_statistics(
    expr: &Expression,
    ctx: &impl ColumnStatsView,
) -> Arc<ColumnStatistics> {
    match expr {
        Expression::ColumnRef(col_ref) => ctx
            .get_stat(&col_ref.binding)
            .unwrap_or_else(|| ColumnStatistics::create_unknown(col_ref.return_type.clone())),
        Expression::Constant(constant) => Arc::new(
            ColumnStatistics::new(BaseStatistics::from_constant(&constant.value))
                .with_guaranteed_distinct_upper(1),
        ),
        Expression::Cast(cast) => ColumnStatistics::create_unknown(cast.target_type.clone()),
        Expression::Reference(reference) => {
            ColumnStatistics::create_unknown(reference.return_type.clone())
        }
        _ => ColumnStatistics::create_unknown(expr.return_type()),
    }
}

pub(super) fn estimate_group_distinct(
    expr: &Expression,
    ctx: &impl ColumnStatsView,
    child_expected_rows: u64,
    child_max_rows: u64,
) -> (u64, Option<u64>) {
    match expr {
        Expression::ColumnRef(col_ref) => {
            let statistics = ctx.get_stat(&col_ref.binding);
            let guaranteed_upper = statistics
                .as_ref()
                .and_then(|stats| stats.guaranteed_distinct_upper())
                .map(|upper| upper.min(child_max_rows));
            let distinct = statistics
                .as_ref()
                .map(|stats| stats.distinct_evidence().point)
                .filter(|count| *count > 0);
            match (distinct, guaranteed_upper) {
                (Some(distinct), Some(upper)) => (distinct.min(upper), Some(upper)),
                (None, Some(upper)) => (upper.min(child_expected_rows), Some(upper)),
                // HLL is an estimate rather than a semantic bound. A 2x
                // envelope remains conservative for planning while avoiding
                // the useless input-cardinality upper bound that made a
                // proven preaggregation look riskier than its unreduced join.
                (Some(distinct), None) => (
                    distinct,
                    Some(distinct.saturating_mul(2).min(child_max_rows).max(distinct)),
                ),
                (None, None) => (fallback_group_distinct(child_expected_rows), None),
            }
        }
        Expression::Constant(_) => (1, Some(1)),
        _ => (fallback_group_distinct(child_expected_rows), None),
    }
}

pub(super) fn fallback_group_distinct(child_rows: u64) -> u64 {
    ((child_rows.max(1) as f64).sqrt().ceil() as u64).max(1)
}
