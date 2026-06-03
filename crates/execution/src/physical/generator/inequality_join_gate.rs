// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Planner gate for inequality join paths.

use super::*;

const SORT_RANGE_JOIN_MIN_INPUT_PAIRS: u128 = 64 * 1024;
const SORT_RANGE_JOIN_DENSE_INPUT_PAIRS: u128 = 256 * 1024;
const SORT_RANGE_JOIN_VERY_LARGE_INPUT_PAIRS: u128 = 4 * 1024 * 1024;
const SORT_RANGE_JOIN_SPARSE_SELECTIVITY_LIMIT: f64 = 0.25;
const SORT_RANGE_JOIN_DENSE_SELECTIVITY_LIMIT: f64 = 0.75;
const SORT_RANGE_JOIN_VERY_LARGE_SELECTIVITY_LIMIT: f64 = 0.90;
const CLASSIC_IE_JOIN_MIN_INPUT_PAIRS: u128 = SORT_RANGE_JOIN_DENSE_INPUT_PAIRS;
const CLASSIC_IE_JOIN_SELECTIVITY_LIMIT: f64 = SORT_RANGE_JOIN_SPARSE_SELECTIVITY_LIMIT;

pub(crate) fn is_classic_ie_join_candidate(
    join: &ComparisonJoin,
    join_cardinality: Option<paro_planner::plan::CardinalityEstimate>,
) -> bool {
    join.join_type == JoinType::Inner
        && sort_range_join_conditions_pass_gate(&join.conditions)
        && classic_ie_join_shared_right_bound_shape(join)
        && classic_ie_join_selectivity_passes_gate(join, join_cardinality)
}

pub(crate) fn is_sort_range_join_candidate(
    join: &ComparisonJoin,
    join_cardinality: Option<paro_planner::plan::CardinalityEstimate>,
) -> bool {
    sort_range_join_conditions_pass_gate(&join.conditions)
        && sort_range_join_cardinality_passes_gate(join, join_cardinality)
        && sort_range_join_column_stats_passes_gate(join)
}

fn sort_range_join_conditions_pass_gate(conditions: &[JoinCondition]) -> bool {
    conditions.len() == 2 && conditions.iter().all(sort_range_join_condition_passes_gate)
}

fn sort_range_join_condition_passes_gate(condition: &JoinCondition) -> bool {
    matches!(
        condition.comparison,
        JoinComparisonType::LessThan
            | JoinComparisonType::LessThanOrEqual
            | JoinComparisonType::GreaterThan
            | JoinComparisonType::GreaterThanOrEqual
    ) && sort_range_join_key_kind(&condition.left.return_type())
        .zip(sort_range_join_key_kind(&condition.right.return_type()))
        .is_some_and(|(left, right)| left == right)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortRangeJoinKeyKind {
    Signed,
    Unsigned,
}

fn sort_range_join_key_kind(logical_type: &LogicalType) -> Option<SortRangeJoinKeyKind> {
    match logical_type {
        LogicalType::TinyInt
        | LogicalType::SmallInt
        | LogicalType::Integer
        | LogicalType::BigInt
        | LogicalType::HugeInt
        | LogicalType::Date
        | LogicalType::Timestamp
        | LogicalType::TimestampTz
        | LogicalType::Time => Some(SortRangeJoinKeyKind::Signed),
        LogicalType::UTinyInt
        | LogicalType::USmallInt
        | LogicalType::UInteger
        | LogicalType::UBigInt
        | LogicalType::UHugeInt
        | LogicalType::Uuid => Some(SortRangeJoinKeyKind::Unsigned),
        _ => None,
    }
}

fn sort_range_join_cardinality_passes_gate(
    join: &ComparisonJoin,
    join_cardinality: Option<paro_planner::plan::CardinalityEstimate>,
) -> bool {
    let (Some(left), Some(right), Some(output)) = (
        join.left.stats.estimated_cardinality,
        join.right.stats.estimated_cardinality,
        join_cardinality,
    ) else {
        return true;
    };
    let input_pairs = (left.expected as u128).saturating_mul(right.expected as u128);
    if input_pairs == 0 {
        return true;
    }
    if input_pairs < SORT_RANGE_JOIN_MIN_INPUT_PAIRS {
        return false;
    }

    let selectivity = ((output.expected as f64) / (input_pairs as f64)).clamp(0.0, 1.0);
    selectivity <= sort_range_join_selectivity_limit(input_pairs)
}

fn classic_ie_join_selectivity_passes_gate(
    join: &ComparisonJoin,
    join_cardinality: Option<paro_planner::plan::CardinalityEstimate>,
) -> bool {
    if let (Some(left), Some(right), Some(output)) = (
        join.left.stats.estimated_cardinality,
        join.right.stats.estimated_cardinality,
        join_cardinality,
    ) {
        let input_pairs = (left.expected as u128).saturating_mul(right.expected as u128);
        if input_pairs < CLASSIC_IE_JOIN_MIN_INPUT_PAIRS {
            return false;
        }

        let selectivity = ((output.expected as f64) / (input_pairs as f64)).clamp(0.0, 1.0);
        return selectivity <= CLASSIC_IE_JOIN_SELECTIVITY_LIMIT;
    }

    classic_ie_join_column_stats_passes_gate(join)
}

fn classic_ie_join_shared_right_bound_shape(join: &ComparisonJoin) -> bool {
    let [first, second] = join.conditions.as_slice() else {
        return false;
    };
    let (
        Expression::Reference(first_right),
        Expression::Reference(second_right),
        Expression::Reference(first_left),
        Expression::Reference(second_left),
    ) = (&first.right, &second.right, &first.left, &second.left)
    else {
        return false;
    };
    first_right.index == second_right.index
        && first_left.index != second_left.index
        && matches!(
            (first.comparison, second.comparison),
            (
                JoinComparisonType::LessThan | JoinComparisonType::LessThanOrEqual,
                JoinComparisonType::GreaterThan | JoinComparisonType::GreaterThanOrEqual
            ) | (
                JoinComparisonType::GreaterThan | JoinComparisonType::GreaterThanOrEqual,
                JoinComparisonType::LessThan | JoinComparisonType::LessThanOrEqual
            )
        )
}

fn classic_ie_join_column_stats_passes_gate(join: &ComparisonJoin) -> bool {
    let mut predicates = Vec::with_capacity(join.conditions.len());
    for condition in &join.conditions {
        let Some(left) = sort_range_column_stats_for_expr(join.left.as_ref(), &condition.left)
        else {
            return false;
        };
        let Some(right) = sort_range_column_stats_for_expr(join.right.as_ref(), &condition.right)
        else {
            return false;
        };
        predicates.push(SortRangePredicateStats {
            left,
            right,
            comparison: condition.comparison,
        });
    }
    classic_ie_join_column_stats_passes_gate_for_predicates(&predicates)
}

fn classic_ie_join_column_stats_passes_gate_for_predicates(
    predicates: &[SortRangePredicateStats],
) -> bool {
    let Some(first) = predicates.first() else {
        return false;
    };
    let input_pairs = (first.left.rows as u128).saturating_mul(first.right.rows as u128);
    if input_pairs < CLASSIC_IE_JOIN_MIN_INPUT_PAIRS {
        return false;
    }
    let selectivity = predicates
        .iter()
        .map(SortRangePredicateStats::selectivity)
        .product::<f64>()
        .clamp(0.0, 1.0);
    selectivity <= CLASSIC_IE_JOIN_SELECTIVITY_LIMIT
}

fn sort_range_join_column_stats_passes_gate(join: &ComparisonJoin) -> bool {
    let mut predicates = Vec::with_capacity(join.conditions.len());
    for condition in &join.conditions {
        let Some(left) = sort_range_column_stats_for_expr(join.left.as_ref(), &condition.left)
        else {
            return true;
        };
        let Some(right) = sort_range_column_stats_for_expr(join.right.as_ref(), &condition.right)
        else {
            return true;
        };
        predicates.push(SortRangePredicateStats {
            left,
            right,
            comparison: condition.comparison,
        });
    }
    sort_range_join_column_stats_passes_gate_for_predicates(&predicates)
}

fn sort_range_join_column_stats_passes_gate_for_predicates(
    predicates: &[SortRangePredicateStats],
) -> bool {
    let Some(first) = predicates.first() else {
        return true;
    };
    let input_pairs = (first.left.rows as u128).saturating_mul(first.right.rows as u128);
    if input_pairs == 0 {
        return true;
    }
    if input_pairs < SORT_RANGE_JOIN_MIN_INPUT_PAIRS {
        return false;
    }

    let selectivity = predicates
        .iter()
        .map(SortRangePredicateStats::selectivity)
        .product::<f64>()
        .clamp(0.0, 1.0);
    selectivity <= sort_range_join_selectivity_limit(input_pairs)
}

fn sort_range_join_selectivity_limit(input_pairs: u128) -> f64 {
    if input_pairs >= SORT_RANGE_JOIN_VERY_LARGE_INPUT_PAIRS {
        SORT_RANGE_JOIN_VERY_LARGE_SELECTIVITY_LIMIT
    } else if input_pairs >= SORT_RANGE_JOIN_DENSE_INPUT_PAIRS {
        SORT_RANGE_JOIN_DENSE_SELECTIVITY_LIMIT
    } else {
        SORT_RANGE_JOIN_SPARSE_SELECTIVITY_LIMIT
    }
}

#[derive(Debug, Clone, Copy)]
struct SortRangePredicateStats {
    left: SortRangeColumnStats,
    right: SortRangeColumnStats,
    comparison: JoinComparisonType,
}

impl SortRangePredicateStats {
    fn selectivity(&self) -> f64 {
        match self.comparison {
            JoinComparisonType::LessThan => self
                .left
                .histogram
                .probability_less_than(&self.right.histogram, InequalityInclusivity::Strict),
            JoinComparisonType::LessThanOrEqual => self
                .left
                .histogram
                .probability_less_than(&self.right.histogram, InequalityInclusivity::Inclusive),
            JoinComparisonType::GreaterThan => self
                .right
                .histogram
                .probability_less_than(&self.left.histogram, InequalityInclusivity::Strict),
            JoinComparisonType::GreaterThanOrEqual => self
                .right
                .histogram
                .probability_less_than(&self.left.histogram, InequalityInclusivity::Inclusive),
            _ => 1.0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct SortRangeColumnStats {
    rows: u64,
    histogram: SortRangeUniformHistogram,
}

#[derive(Debug, Clone, Copy)]
struct SortRangeUniformHistogram {
    lower: f64,
    upper: f64,
    distinct_buckets: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InequalityInclusivity {
    Strict,
    Inclusive,
}

impl SortRangeUniformHistogram {
    fn from_base_statistics(
        stats: &paro_storage::statistics::BaseStatistics,
        rows: u64,
    ) -> Option<Self> {
        if rows == 0 || !stats.can_have_no_null() {
            return None;
        }
        let lower = sort_range_stats_value_to_f64(&stats.min_value()?)?;
        let upper = sort_range_stats_value_to_f64(&stats.max_value()?)?;
        if !lower.is_finite() || !upper.is_finite() || lower > upper {
            return None;
        }
        Some(Self {
            lower,
            upper,
            distinct_buckets: stats.get_distinct_count().max(1) as u64,
        })
    }

    fn probability_less_than(&self, other: &Self, inclusivity: InequalityInclusivity) -> f64 {
        if self.is_point() && other.is_point() {
            return match inclusivity {
                InequalityInclusivity::Strict => probability_of(self.lower < other.lower),
                InequalityInclusivity::Inclusive => probability_of(self.lower <= other.lower),
            };
        }

        let mut probability = self.continuous_probability_less_than(other);
        if inclusivity == InequalityInclusivity::Inclusive {
            probability += self.equality_overlap_probability(other);
        }
        probability.clamp(0.0, 1.0)
    }

    fn continuous_probability_less_than(&self, other: &Self) -> f64 {
        if self.upper <= other.lower {
            return 1.0;
        }
        if self.lower >= other.upper {
            return 0.0;
        }
        if self.is_point() {
            return other.probability_greater_than_point(self.lower);
        }
        if other.is_point() {
            return self.probability_less_than_point(other.lower);
        }

        let self_span = self.span();
        let other_span = other.span();
        if self_span <= 0.0 || other_span <= 0.0 {
            return 0.0;
        }

        let guaranteed_start = self.lower;
        let guaranteed_end = self.upper.min(other.lower);
        let guaranteed_area = (guaranteed_end - guaranteed_start).max(0.0);

        let overlap_start = self.lower.max(other.lower);
        let overlap_end = self.upper.min(other.upper);
        let overlap_area = if overlap_end > overlap_start {
            integral_rhs_survival(other.upper, overlap_end, other_span)
                - integral_rhs_survival(other.upper, overlap_start, other_span)
        } else {
            0.0
        };

        ((guaranteed_area + overlap_area) / self_span).clamp(0.0, 1.0)
    }

    fn probability_greater_than_point(&self, point: f64) -> f64 {
        if self.is_point() {
            return probability_of(self.lower > point);
        }
        if point <= self.lower {
            1.0
        } else if point >= self.upper {
            0.0
        } else {
            ((self.upper - point) / self.span()).clamp(0.0, 1.0)
        }
    }

    fn probability_less_than_point(&self, point: f64) -> f64 {
        if self.is_point() {
            return probability_of(self.lower < point);
        }
        if point <= self.lower {
            0.0
        } else if point >= self.upper {
            1.0
        } else {
            ((point - self.lower) / self.span()).clamp(0.0, 1.0)
        }
    }

    fn equality_overlap_probability(&self, other: &Self) -> f64 {
        let overlap = self.upper.min(other.upper) - self.lower.max(other.lower);
        if overlap < 0.0 {
            return 0.0;
        }
        let domain = (self.upper.max(other.upper) - self.lower.min(other.lower)).max(1.0);
        let overlap_fraction = if self.is_point() && other.is_point() {
            probability_of(self.lower == other.lower)
        } else {
            (overlap / domain).clamp(0.0, 1.0)
        };
        let buckets = self.distinct_buckets.max(other.distinct_buckets).max(1) as f64;
        overlap_fraction / buckets
    }

    fn is_point(&self) -> bool {
        self.lower == self.upper
    }

    fn span(&self) -> f64 {
        (self.upper - self.lower).max(0.0)
    }
}

fn integral_rhs_survival(rhs_upper: f64, x: f64, rhs_span: f64) -> f64 {
    (rhs_upper * x - 0.5 * x * x) / rhs_span
}

fn probability_of(condition: bool) -> f64 {
    if condition {
        1.0
    } else {
        0.0
    }
}

fn sort_range_column_stats_for_expr(
    plan: &LogicalPlan,
    expression: &Expression,
) -> Option<SortRangeColumnStats> {
    let Expression::Reference(reference) = expression else {
        return None;
    };
    sort_range_column_stats_for_output(plan, reference.index)
}

fn sort_range_column_stats_for_output(
    plan: &LogicalPlan,
    output_idx: usize,
) -> Option<SortRangeColumnStats> {
    match &plan.operator {
        LogicalOperator::Get(get) => sort_range_get_column_stats(get, output_idx),
        LogicalOperator::Filter(filter) => {
            let child_idx = projected_child_index(&filter.projection_map, output_idx)?;
            sort_range_column_stats_for_output(filter.child.as_ref(), child_idx)
        }
        LogicalOperator::Projection(project) => {
            let expression = project.expressions.get(output_idx)?;
            sort_range_column_stats_for_expr(project.child.as_ref(), expression)
        }
        LogicalOperator::Limit(limit) => {
            sort_range_column_stats_for_output(limit.child.as_ref(), output_idx)
        }
        LogicalOperator::Order(order) => {
            let child_idx = projected_child_index(&order.projection_map, output_idx)?;
            sort_range_column_stats_for_output(order.child.as_ref(), child_idx)
        }
        LogicalOperator::TopN(topn) => {
            sort_range_column_stats_for_output(topn.child.as_ref(), output_idx)
        }
        _ => None,
    }
}

fn projected_child_index(projection_map: &[usize], output_idx: usize) -> Option<usize> {
    if projection_map.is_empty() {
        Some(output_idx)
    } else {
        projection_map.get(output_idx).copied()
    }
}

fn sort_range_get_column_stats(get: &Get, output_idx: usize) -> Option<SortRangeColumnStats> {
    let table = get.table.as_ref()?;
    let column_id = *get.column_ids.get(output_idx)?;
    if column_id >= table.columns.len() {
        return None;
    }
    let storage = table.get_storage()?;
    let base_stats = storage.column_statistics(column_id)?;
    let rows = storage
        .tablet()
        .statistics()
        .ok()
        .map(|stats| stats.num_rows)
        .filter(|rows| *rows > 0)
        .or_else(|| {
            table
                .statistics()
                .map(|stats| stats.row_count)
                .filter(|rows| *rows > 0)
        })?;
    let histogram = SortRangeUniformHistogram::from_base_statistics(&base_stats, rows)?;
    Some(SortRangeColumnStats { rows, histogram })
}

fn sort_range_stats_value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::TinyInt(value) => Some(*value as f64),
        Value::SmallInt(value) => Some(*value as f64),
        Value::Integer(value) => Some(*value as f64),
        Value::BigInt(value) => Some(*value as f64),
        Value::HugeInt(value) => Some(*value as f64),
        Value::UTinyInt(value) => Some(*value as f64),
        Value::USmallInt(value) => Some(*value as f64),
        Value::UInteger(value) => Some(*value as f64),
        Value::UBigInt(value) => Some(*value as f64),
        Value::UHugeInt(value) => Some(*value as f64),
        Value::Uuid(value) => Some(*value as f64),
        Value::Date(value) => Some(*value as f64),
        Value::Timestamp(value) => Some(*value as f64),
        Value::TimestampTz(value) => Some(*value as f64),
        Value::Time(value) => Some(*value as f64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inequality_join_gate_requires_typed_ordered_range_keys() {
        assert!(sort_range_join_conditions_pass_gate(&[
            range_condition(LogicalType::Integer, JoinComparisonType::LessThan),
            range_condition(LogicalType::BigInt, JoinComparisonType::GreaterThan),
        ]));
        assert!(!sort_range_join_conditions_pass_gate(&[
            range_condition(LogicalType::Varchar, JoinComparisonType::LessThan),
            range_condition(LogicalType::Integer, JoinComparisonType::GreaterThan),
        ]));
        assert!(!sort_range_join_conditions_pass_gate(&[
            range_condition(LogicalType::Integer, JoinComparisonType::Equal),
            range_condition(LogicalType::Integer, JoinComparisonType::GreaterThan),
        ]));
        assert!(!sort_range_join_conditions_pass_gate(&[
            mixed_range_condition(
                LogicalType::Integer,
                LogicalType::UInteger,
                JoinComparisonType::LessThan,
            ),
            range_condition(LogicalType::Integer, JoinComparisonType::GreaterThan),
        ]));
    }

    #[test]
    fn inequality_join_gate_uses_cardinality_selectivity_curve_when_stats_exist() {
        let small = range_join_with_rows(32, 32);
        assert!(!is_sort_range_join_candidate(
            &small,
            Some(paro_planner::plan::CardinalityEstimate::exact(64)),
        ));

        let large_selective = range_join_with_rows(512, 512);
        assert!(is_sort_range_join_candidate(
            &large_selective,
            Some(paro_planner::plan::CardinalityEstimate::exact(8_056)),
        ));

        let large_dense = range_join_with_rows(512, 512);
        assert!(is_sort_range_join_candidate(
            &large_dense,
            Some(paro_planner::plan::CardinalityEstimate::exact(130_816)),
        ));

        let too_dense = range_join_with_rows(512, 512);
        assert!(!is_sort_range_join_candidate(
            &too_dense,
            Some(paro_planner::plan::CardinalityEstimate::exact(250_000)),
        ));

        let missing_stats = range_join_with_rows(0, 0);
        assert!(is_sort_range_join_candidate(&missing_stats, None));
    }

    #[test]
    fn classic_ie_join_gate_is_stricter_than_sort_range() {
        let selective = point_window_join_with_rows(512, 512);
        assert!(is_classic_ie_join_candidate(
            &selective,
            Some(paro_planner::plan::CardinalityEstimate::exact(8_056)),
        ));

        let dense = point_window_join_with_rows(512, 512);
        assert!(!is_classic_ie_join_candidate(
            &dense,
            Some(paro_planner::plan::CardinalityEstimate::exact(130_816)),
        ));
        assert!(is_sort_range_join_candidate(
            &dense,
            Some(paro_planner::plan::CardinalityEstimate::exact(130_816)),
        ));

        let general_two_bound_range = range_join_with_rows(512, 512);
        assert!(!is_classic_ie_join_candidate(
            &general_two_bound_range,
            Some(paro_planner::plan::CardinalityEstimate::exact(8_056)),
        ));

        let missing_stats = range_join_with_rows(0, 0);
        assert!(!is_classic_ie_join_candidate(&missing_stats, None));

        let right_join = ComparisonJoin::new(
            JoinType::Right,
            plan_with_cardinality(512),
            plan_with_cardinality(512),
            point_window_conditions(),
        );
        assert!(!is_classic_ie_join_candidate(
            &right_join,
            Some(paro_planner::plan::CardinalityEstimate::exact(8_056)),
        ));
    }

    #[test]
    fn inequality_uniform_histogram_estimates_selectivity() {
        let low = histogram(0.0, 100.0, 101);
        let high = histogram(200.0, 300.0, 101);
        assert_eq!(
            low.probability_less_than(&high, InequalityInclusivity::Strict),
            1.0
        );
        assert_eq!(
            high.probability_less_than(&low, InequalityInclusivity::Strict),
            0.0
        );

        let left = histogram(0.0, 10.0, 11);
        let right = histogram(0.0, 10.0, 11);
        let selectivity = left.probability_less_than(&right, InequalityInclusivity::Strict);
        assert!(
            (0.49..=0.51).contains(&selectivity),
            "equal uniform ranges should estimate near 50%, got {selectivity}"
        );
    }

    #[test]
    fn inequality_join_gate_uses_column_stat_histogram_curve() {
        let selective = [
            predicate_stats(
                column_stats(1_000, 0.0, 100.0),
                column_stats(1_000, 0.0, 100.0),
                JoinComparisonType::LessThan,
            ),
            predicate_stats(
                column_stats(1_000, 0.0, 100.0),
                column_stats(1_000, 0.0, 100.0),
                JoinComparisonType::GreaterThan,
            ),
        ];
        assert!(sort_range_join_column_stats_passes_gate_for_predicates(
            &selective
        ));

        let dense = [
            predicate_stats(
                column_stats(1_000, 0.0, 100.0),
                column_stats(1_000, 200.0, 300.0),
                JoinComparisonType::LessThan,
            ),
            predicate_stats(
                column_stats(1_000, 200.0, 300.0),
                column_stats(1_000, 0.0, 100.0),
                JoinComparisonType::GreaterThan,
            ),
        ];
        assert!(!sort_range_join_column_stats_passes_gate_for_predicates(
            &dense
        ));
    }

    fn range_condition(logical_type: LogicalType, comparison: JoinComparisonType) -> JoinCondition {
        mixed_range_condition(logical_type.clone(), logical_type, comparison)
    }

    fn mixed_range_condition(
        left_type: LogicalType,
        right_type: LogicalType,
        comparison: JoinComparisonType,
    ) -> JoinCondition {
        JoinCondition::new(
            Expression::Reference(ReferenceExpression::new(0, left_type)),
            Expression::Reference(ReferenceExpression::new(0, right_type)),
            comparison,
        )
    }

    fn range_join_with_rows(left_rows: u64, right_rows: u64) -> ComparisonJoin {
        ComparisonJoin::new(
            JoinType::Inner,
            plan_with_cardinality(left_rows),
            plan_with_cardinality(right_rows),
            vec![
                range_condition(LogicalType::Integer, JoinComparisonType::LessThan),
                range_condition(LogicalType::Integer, JoinComparisonType::GreaterThan),
            ],
        )
    }

    fn point_window_join_with_rows(left_rows: u64, right_rows: u64) -> ComparisonJoin {
        ComparisonJoin::new(
            JoinType::Inner,
            plan_with_cardinality(left_rows),
            plan_with_cardinality(right_rows),
            point_window_conditions(),
        )
    }

    fn point_window_conditions() -> Vec<JoinCondition> {
        vec![
            indexed_range_condition(0, 0, JoinComparisonType::LessThan),
            indexed_range_condition(1, 0, JoinComparisonType::GreaterThanOrEqual),
        ]
    }

    fn indexed_range_condition(
        left_index: usize,
        right_index: usize,
        comparison: JoinComparisonType,
    ) -> JoinCondition {
        JoinCondition::new(
            Expression::Reference(ReferenceExpression::new(left_index, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(right_index, LogicalType::Integer)),
            comparison,
        )
    }

    fn plan_with_cardinality(rows: u64) -> LogicalPlan {
        let mut plan = LogicalPlan::synthetic(LogicalOperator::DummyScan);
        if rows > 0 {
            plan.stats.estimated_cardinality =
                Some(paro_planner::plan::CardinalityEstimate::exact(rows));
        }
        plan
    }

    fn histogram(lower: f64, upper: f64, distinct_buckets: u64) -> SortRangeUniformHistogram {
        SortRangeUniformHistogram {
            lower,
            upper,
            distinct_buckets,
        }
    }

    fn column_stats(rows: u64, lower: f64, upper: f64) -> SortRangeColumnStats {
        SortRangeColumnStats {
            rows,
            histogram: histogram(lower, upper, 101),
        }
    }

    fn predicate_stats(
        left: SortRangeColumnStats,
        right: SortRangeColumnStats,
        comparison: JoinComparisonType,
    ) -> SortRangePredicateStats {
        SortRangePredicateStats {
            left,
            right,
            comparison,
        }
    }
}
