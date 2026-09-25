// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(super) fn estimate_same_domain_semi_join<Child>(
    join: &paro_planner::logical::operator::ComparisonJoin<Child>,
    left: CardinalityEstimate,
    right: CardinalityEstimate,
    left_bindings: &[ColumnBinding],
    right_bindings: &[ColumnBinding],
    ctx: &CardinalityInputs<'_>,
) -> Option<CardinalityEstimate> {
    let (preserved, demand, preserved_bindings, demand_bindings) = match join.join_type {
        JoinType::Semi => (left, right, left_bindings, right_bindings),
        JoinType::RightSemi => (right, left, right_bindings, left_bindings),
        _ => return None,
    };
    let [condition] = join.conditions.as_slice() else {
        return None;
    };
    if condition.comparison != JoinComparisonType::Equal {
        return None;
    }
    let preserved_key = expression_binding(&condition.left, preserved_bindings)
        .or_else(|| expression_binding(&condition.right, preserved_bindings))?;
    let demand_key = expression_binding(&condition.right, demand_bindings)
        .or_else(|| expression_binding(&condition.left, demand_bindings))?;
    let preserved_stats = ctx.column_stats.get(&preserved_key)?;
    let demand_stats = ctx.column_stats.get(&demand_key)?;
    let preserved_distinct = preserved_stats.distinct_evidence().point;
    if preserved_distinct == 0
        || preserved_distinct != demand_stats.distinct_evidence().point
        || preserved_stats.get_type() != demand_stats.get_type()
        || preserved_stats.statistics().min_value() != demand_stats.statistics().min_value()
        || preserved_stats.statistics().max_value() != demand_stats.statistics().max_value()
    {
        return None;
    }

    let expected = preserved.expected.min(demand.expected);
    Some(CardinalityEstimate {
        min: 0,
        expected,
        // `demand.max` already carries the estimator's uncertainty envelope.
        // Applying another arbitrary factor here double-counts uncertainty
        // and makes a duplicate-insensitive semi join look riskier than the
        // unfiltered relation it replaces.
        max: preserved.max.min(demand.max).max(expected),
    })
}

/// Estimate an equality lookup into a declared unique relation against the
/// key domain actually present on the fact side.
///
/// A filtered date/customer dimension retains base-column NDV statistics even
/// though its row count is selective. Dividing by that historical dimension
/// NDV can underestimate a foreign-key-shaped join by orders of magnitude.
/// Uniqueness proves at most one match per fact row; the expected match ratio
/// is therefore `selected_dimension_rows / fact_key_domain`, capped at one.
pub(super) fn estimate_unique_dimension_join<Child: LocalChildFacts>(
    join: &paro_planner::logical::operator::ComparisonJoin<Child>,
    left: CardinalityEstimate,
    right: CardinalityEstimate,
    left_layout: &LogicalOutputLayout,
    right_layout: &LogicalOutputLayout,
    ctx: &CardinalityInputs<'_>,
) -> Option<CardinalityEstimate> {
    let [condition] = join.conditions.as_slice() else {
        return None;
    };
    if condition.comparison != JoinComparisonType::Equal {
        return None;
    }
    let left_key = expression_binding(&condition.left, left_layout.bindings())?;
    let right_key = expression_binding(&condition.right, right_layout.bindings())?;

    if plan_has_single_column_unique_key(&join.right, right_key) {
        return unique_lookup_estimate(left, right, left_key, ctx);
    }
    if plan_has_single_column_unique_key(&join.left, left_key) {
        return unique_lookup_estimate(right, left, right_key, ctx);
    }
    None
}

pub(super) fn plan_has_single_column_unique_key<Child: LocalChildFacts>(
    child: &Child,
    binding: ColumnBinding,
) -> bool {
    child
        .unique_keys()
        .iter()
        .any(|key| key.len() == 1 && key[0] == binding)
}

pub(super) fn cap_unique_join<Child: LocalChildFacts>(
    mut estimate: CardinalityEstimate,
    join: &paro_planner::logical::operator::ComparisonJoin<Child>,
    left: CardinalityEstimate,
    right: CardinalityEstimate,
    left_layout: &LogicalOutputLayout,
    right_layout: &LogicalOutputLayout,
) -> CardinalityEstimate {
    let mut left_keys = BTreeSet::new();
    let mut right_keys = BTreeSet::new();
    for condition in &join.conditions {
        if condition.comparison == JoinComparisonType::Equal {
            if let Some(binding) = expression_binding(&condition.left, left_layout.bindings()) {
                left_keys.insert(binding);
            }
            if let Some(binding) = expression_binding(&condition.right, right_layout.bindings()) {
                right_keys.insert(binding);
            }
        }
    }
    let covered = |keys: Vec<Vec<ColumnBinding>>, matched: &BTreeSet<ColumnBinding>| {
        keys.iter()
            .any(|key| !key.is_empty() && key.iter().all(|k| matched.contains(k)))
    };
    for bound in [
        covered(join.right.unique_keys(), &right_keys).then_some(left),
        covered(join.left.unique_keys(), &left_keys).then_some(right),
    ]
    .into_iter()
    .flatten()
    {
        estimate.expected = estimate.expected.min(bound.expected);
        estimate.max = estimate.max.min(bound.max).max(estimate.expected);
        estimate.min = estimate.min.min(estimate.expected);
    }
    estimate
}

pub(super) fn unique_lookup_estimate(
    fact: CardinalityEstimate,
    dimension: CardinalityEstimate,
    fact_key: ColumnBinding,
    ctx: &CardinalityInputs<'_>,
) -> Option<CardinalityEstimate> {
    let domain = ctx.column_stats.get(&fact_key)?.distinct_evidence().point;
    if domain == 0 {
        return None;
    }
    let scale = |rows: u64, selected: u64| {
        ((rows as u128).saturating_mul(selected as u128) / domain as u128).min(rows as u128) as u64
    };
    let expected = scale(fact.expected, dimension.expected);
    Some(CardinalityEstimate {
        min: scale(fact.min, dimension.min).min(expected),
        expected,
        max: scale(fact.max, dimension.max).max(expected),
    })
}

/// Estimate a comparison join without manufacturing independence between
/// marginal statistics of one composite relation pair.
///
/// Two equality keys between the same aliases are commonly a composite key.
/// Multiplying their individual NDV selectivities can underestimate the join
/// by orders of magnitude unless joint-domain statistics prove independence.
/// Keep the strongest equality domain for each concrete alias pair, while
/// conditions connecting different pairs and non-equality residuals remain
/// independent factors. This matches the correlation contract used by the
/// join-order estimator and keeps post-reorder statistics from reversing a
/// sound build/probe decision.
pub(super) fn estimate_comparison_join_selectivity(
    conditions: &[JoinCondition],
    left_bindings: &[ColumnBinding],
    right_bindings: &[ColumnBinding],
    left_rows: u64,
    right_rows: u64,
    ctx: &CardinalityInputs<'_>,
) -> f64 {
    correlate_join_condition_selectivities(conditions.iter().map(|condition| {
        (
            equality_relation_pair(condition, left_bindings, right_bindings),
            estimate_join_condition_selectivity(
                condition,
                left_bindings,
                right_bindings,
                left_rows,
                right_rows,
                ctx,
            ),
        )
    }))
}

pub(super) fn correlate_join_condition_selectivities(
    conditions: impl IntoIterator<Item = (Option<(usize, usize)>, f64)>,
) -> f64 {
    // This map participates in a floating-point reduction. Iterating a HashMap
    // would make the estimate (and potentially the winning physical plan)
    // depend on the process hash seed.
    let mut equality_by_relation_pair = BTreeMap::<(usize, usize), f64>::new();
    let mut independent_selectivity = 1.0;

    for (relation_pair, selectivity) in conditions {
        if let Some(pair) = relation_pair {
            equality_by_relation_pair
                .entry(pair)
                .and_modify(|strongest| *strongest = strongest.min(selectivity))
                .or_insert(selectivity);
        } else {
            independent_selectivity *= selectivity;
        }
    }

    equality_by_relation_pair
        .values()
        .fold(independent_selectivity, |product, selectivity| {
            product * selectivity
        })
        .clamp(0.0, 1.0)
}

pub(super) fn equality_relation_pair(
    condition: &JoinCondition,
    left_bindings: &[ColumnBinding],
    right_bindings: &[ColumnBinding],
) -> Option<(usize, usize)> {
    if !matches!(
        condition.comparison,
        JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
    ) {
        return None;
    }
    let left = expression_binding(&condition.left, left_bindings)?;
    let right = expression_binding(&condition.right, right_bindings)?;
    let pair = (left.table_index, right.table_index);
    Some(if pair.0 <= pair.1 {
        pair
    } else {
        (pair.1, pair.0)
    })
}

pub(super) fn expression_binding(
    expression: &Expression,
    positional_bindings: &[ColumnBinding],
) -> Option<ColumnBinding> {
    match expression {
        Expression::ColumnRef(column) => Some(column.binding),
        Expression::Reference(reference) => positional_bindings.get(reference.index).copied(),
        // A cast preserves column lineage for correlation purposes. It changes
        // the comparison domain, whose selectivity is still estimated by the
        // ordinary expression model, but not which aliases form the pair.
        Expression::Cast(cast) => expression_binding(cast.child.as_ref(), positional_bindings),
        _ => None,
    }
}
