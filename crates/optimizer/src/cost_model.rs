// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_external::routine::identity::BuiltinIntrinsicId;
use paro_planner::expression::{
    ComparisonExpression, ComparisonType, ConjunctionType, Expression, ExpressionIterator,
    ExpressionVisitDecision, OperatorType,
};
use paro_planner::operator::ColumnBinding;
use paro_planner::plan::CardinalityEstimate;
use paro_storage::statistics::{ColumnStatistics, NumericStats};

const MIN_SELECTIVITY: f64 = 0.000_001;

#[derive(Debug, Clone, Copy)]
struct SelectivityEstimate {
    fraction: f64,
    proven: bool,
}

impl SelectivityEstimate {
    fn proven(fraction: f64) -> Self {
        Self {
            fraction: clamp_selectivity(fraction),
            proven: true,
        }
    }

    fn estimated(fraction: f64) -> Self {
        Self {
            fraction: clamp_selectivity(fraction).max(MIN_SELECTIVITY),
            proven: false,
        }
    }

    fn complement(self) -> Self {
        if self.proven {
            Self::proven(1.0 - self.fraction)
        } else {
            Self::estimated(1.0 - self.fraction)
        }
    }
}

fn conjunction_estimate(
    estimates: impl Iterator<Item = SelectivityEstimate>,
) -> SelectivityEstimate {
    let mut fraction = 1.0;
    let mut proven = true;
    for estimate in estimates {
        if estimate.proven && estimate.fraction == 0.0 {
            return SelectivityEstimate::proven(0.0);
        }
        fraction *= estimate.fraction;
        proven &= estimate.proven;
    }
    if proven {
        SelectivityEstimate::proven(fraction)
    } else {
        SelectivityEstimate::estimated(fraction)
    }
}

/// Combine predicates with exponential backoff only across distinct columns
/// of the same relation.
///
/// Per-column statistics cannot prove independence between category-like
/// attributes. Multiplying all such estimates systematically underestimates
/// filtered relations. Predicates on one column are still combined exactly
/// (and ordered ranges are coalesced before reaching this function), while
/// predicates on different relations remain independent.
fn column_aware_conjunction_estimate(
    estimates: impl Iterator<Item = (SelectivityEstimate, Option<ColumnBinding>)>,
) -> SelectivityEstimate {
    let mut independent = Vec::new();
    let mut by_relation = HashMap::<usize, HashMap<usize, Vec<SelectivityEstimate>>>::new();
    for (estimate, binding) in estimates {
        if estimate.proven && estimate.fraction == 0.0 {
            return SelectivityEstimate::proven(0.0);
        }
        match binding {
            Some(binding) => by_relation
                .entry(binding.table_index)
                .or_default()
                .entry(binding.column_index)
                .or_default()
                .push(estimate),
            None => independent.push(estimate),
        }
    }

    for columns in by_relation.into_values() {
        let mut column_estimates = columns
            .into_values()
            .map(|estimates| conjunction_estimate(estimates.into_iter()))
            .collect::<Vec<_>>();
        column_estimates.sort_by(|left, right| left.fraction.total_cmp(&right.fraction));
        let is_damped = column_estimates.len() > 1;
        let proven = column_estimates.iter().all(|estimate| estimate.proven);
        let mut exponent = 1.0;
        let mut fraction = 1.0;
        for estimate in column_estimates {
            fraction *= estimate.fraction.powf(exponent);
            exponent *= 0.5;
        }
        independent.push(if proven && !is_damped {
            SelectivityEstimate::proven(fraction)
        } else {
            SelectivityEstimate::estimated(fraction)
        });
    }
    conjunction_estimate(independent.into_iter())
}

fn disjunction_estimate(
    estimates: impl Iterator<Item = SelectivityEstimate>,
) -> SelectivityEstimate {
    let mut miss_fraction = 1.0;
    let mut proven = true;
    for estimate in estimates {
        if estimate.proven && estimate.fraction == 1.0 {
            return SelectivityEstimate::proven(1.0);
        }
        miss_fraction *= 1.0 - estimate.fraction;
        proven &= estimate.proven;
    }
    let fraction = 1.0 - miss_fraction;
    if proven {
        SelectivityEstimate::proven(fraction)
    } else {
        SelectivityEstimate::estimated(fraction)
    }
}

#[derive(Debug, Clone, Default)]
pub struct CostModel {
    pub defaults: SelectivityDefaults,
    pub scan_access: paro_storage::rowset::scan_cost::ScanAccessCostModel,
}

#[derive(Debug, Clone)]
pub struct SelectivityDefaults {
    pub equality: f64,
    pub range: f64,
    pub predicate: f64,
    pub like_prefix: f64,
    pub like_contains: f64,
    pub fulltext_match: f64,
    pub vector_topk_fraction: f64,
    pub is_not_null: f64,
}

impl Default for SelectivityDefaults {
    fn default() -> Self {
        Self {
            equality: 0.1,
            range: 0.3,
            predicate: 0.75,
            like_prefix: 0.25,
            like_contains: 0.05,
            fulltext_match: 0.1,
            vector_topk_fraction: 0.001,
            is_not_null: 0.9,
        }
    }
}

struct StatisticsResolver<'a> {
    column_stats: &'a HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    positional_bindings: Option<&'a [ColumnBinding]>,
}

impl<'a> StatisticsResolver<'a> {
    fn logical(column_stats: &'a HashMap<ColumnBinding, Arc<ColumnStatistics>>) -> Self {
        Self {
            column_stats,
            positional_bindings: None,
        }
    }

    fn with_positions(
        column_stats: &'a HashMap<ColumnBinding, Arc<ColumnStatistics>>,
        positional_bindings: &'a [ColumnBinding],
    ) -> Self {
        Self {
            column_stats,
            positional_bindings: Some(positional_bindings),
        }
    }

    fn binding(&self, expression: &Expression) -> Option<ColumnBinding> {
        Some(match expression {
            Expression::ColumnRef(column) => column.binding,
            Expression::Reference(reference) => *self.positional_bindings?.get(reference.index)?,
            _ => return None,
        })
    }

    fn get(&self, expression: &Expression) -> Option<&'a Arc<ColumnStatistics>> {
        self.column_stats.get(&self.binding(expression)?)
    }
}

impl CostModel {
    /// Compare carrying payload through a row-preserving operator path with
    /// carrying one stable rowid and gathering the payload at a later
    /// frontier. The stage count makes blocking/serialized intermediates an
    /// explicit cost input instead of a hidden syntactic heuristic.
    pub(crate) fn late_row_fetch_benefit(
        &self,
        carrier_rows: u64,
        fetched_rows: u64,
        payload_types: impl IntoIterator<Item = LogicalType>,
        carrier_stages: usize,
    ) -> Option<f64> {
        if carrier_rows == 0 || carrier_stages == 0 {
            return None;
        }
        let payload_width = payload_types
            .into_iter()
            .map(|ty| self.scan_access.estimated_width(&ty))
            .sum::<usize>();
        if payload_width == 0 {
            return None;
        }
        let rowid_width = self.scan_access.estimated_width(&LogicalType::BigInt);
        let carrier_work = carrier_rows as f64 * carrier_stages as f64;
        let eager = carrier_work * payload_width as f64;
        let late = carrier_work * rowid_width as f64
            + fetched_rows as f64 * payload_width as f64 * self.scan_access.gather_access_penalty()
            + self.scan_access.gather_startup_cost() as f64;
        let benefit = eager - late;
        (benefit > 0.0).then_some(benefit)
    }

    pub fn estimate_selectivity(
        &self,
        expr: &Expression,
        column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    ) -> f64 {
        let resolver = StatisticsResolver::logical(column_stats);
        self.estimate_selectivity_with_provenance(expr, &resolver)
            .fraction
    }

    fn estimate_selectivity_with_provenance(
        &self,
        expr: &Expression,
        resolver: &StatisticsResolver<'_>,
    ) -> SelectivityEstimate {
        match expr {
            Expression::Constant(constant) => match &constant.value {
                Value::Boolean(true) => SelectivityEstimate::proven(1.0),
                Value::Boolean(false) => SelectivityEstimate::proven(0.0),
                _ => SelectivityEstimate::estimated(self.defaults.predicate),
            },
            Expression::Comparison(comparison) => SelectivityEstimate::estimated(
                self.estimate_comparison_selectivity(comparison, resolver),
            ),
            Expression::Conjunction(conjunction) => match conjunction.conjunction_type {
                ConjunctionType::And => {
                    self.estimate_conjunction(conjunction.children.iter(), resolver)
                }
                ConjunctionType::Or => disjunction_estimate(
                    conjunction
                        .children
                        .iter()
                        .map(|child| self.estimate_selectivity_with_provenance(child, resolver)),
                ),
            },
            Expression::Operator(operator) => match operator.operator_type {
                OperatorType::Like => SelectivityEstimate::estimated(
                    match like_pattern_shape(operator.children.get(1)) {
                        LikePatternShape::MatchAll => 1.0,
                        LikePatternShape::Exact => self
                            .estimate_exact_like_selectivity(operator.children.first(), resolver),
                        LikePatternShape::Wildcard(pattern) => pattern.selectivity(&self.defaults),
                        LikePatternShape::Generic => self.defaults.predicate,
                    },
                ),
                OperatorType::ILike => SelectivityEstimate::estimated(
                    match like_pattern_shape(operator.children.get(1)) {
                        LikePatternShape::MatchAll => 1.0,
                        // Case folding can merge several stored values into one
                        // comparison domain, so the raw column NDV is not a sound
                        // denominator for an exact ILIKE pattern.
                        LikePatternShape::Exact => self.defaults.equality,
                        LikePatternShape::Wildcard(pattern) => pattern.selectivity(&self.defaults),
                        LikePatternShape::Generic => self.defaults.predicate,
                    },
                ),
                OperatorType::Not => operator.children.first().map_or_else(
                    || SelectivityEstimate::estimated(self.defaults.predicate),
                    |child| {
                        self.estimate_selectivity_with_provenance(child, resolver)
                            .complement()
                    },
                ),
                OperatorType::IsNull => {
                    SelectivityEstimate::estimated(1.0 - self.defaults.is_not_null)
                }
                OperatorType::IsNotNull => {
                    SelectivityEstimate::estimated(self.defaults.is_not_null)
                }
                OperatorType::In => SelectivityEstimate::estimated(
                    self.estimate_in_selectivity(operator.children.as_slice(), resolver),
                ),
                OperatorType::NotIn => SelectivityEstimate::estimated(
                    1.0 - self.estimate_in_selectivity(operator.children.as_slice(), resolver),
                ),
                _ => SelectivityEstimate::estimated(self.defaults.predicate),
            },
            Expression::Function(function) => {
                SelectivityEstimate::estimated(match function.builtin_intrinsic() {
                    Some(
                        BuiltinIntrinsicId::FullTextMatch
                        | BuiltinIntrinsicId::FullTextMatchInternal
                        | BuiltinIntrinsicId::Bm25
                        | BuiltinIntrinsicId::Bm25ScoreInternal
                        | BuiltinIntrinsicId::TsRank
                        | BuiltinIntrinsicId::TsRankCd
                        | BuiltinIntrinsicId::ToTsVector
                        | BuiltinIntrinsicId::PlainToTsQuery
                        | BuiltinIntrinsicId::ToTsQuery
                        | BuiltinIntrinsicId::PhraseToTsQuery
                        | BuiltinIntrinsicId::WebSearchToTsQuery,
                    ) => self.defaults.fulltext_match,
                    Some(
                        BuiltinIntrinsicId::L2Distance
                        | BuiltinIntrinsicId::L1Distance
                        | BuiltinIntrinsicId::CosineDistance
                        | BuiltinIntrinsicId::NegativeInnerProduct
                        | BuiltinIntrinsicId::SparseDistance,
                    ) => self.defaults.vector_topk_fraction,
                    _ => self.defaults.predicate,
                })
            }
            _ => SelectivityEstimate::estimated(self.defaults.predicate),
        }
    }

    pub fn estimate_filter_cardinality(
        &self,
        base_cardinality: u64,
        expressions: &[Expression],
        column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    ) -> CardinalityEstimate {
        self.estimate_filter_cardinality_with_resolver(
            base_cardinality,
            expressions,
            &StatisticsResolver::logical(column_stats),
        )
    }

    pub fn estimate_filter_cardinality_with_positions(
        &self,
        base_cardinality: u64,
        expressions: &[Expression],
        column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
        positional_bindings: &[ColumnBinding],
    ) -> CardinalityEstimate {
        self.estimate_filter_cardinality_with_resolver(
            base_cardinality,
            expressions,
            &StatisticsResolver::with_positions(column_stats, positional_bindings),
        )
    }

    fn estimate_filter_cardinality_with_resolver(
        &self,
        base_cardinality: u64,
        expressions: &[Expression],
        resolver: &StatisticsResolver<'_>,
    ) -> CardinalityEstimate {
        if base_cardinality == 0 {
            return CardinalityEstimate::exact(0);
        }
        if expressions.is_empty() {
            return CardinalityEstimate::exact(base_cardinality);
        }

        let combined = self.estimate_conjunction(expressions.iter(), resolver);
        self.cardinality_from_selectivity(base_cardinality, combined.fraction, combined.proven)
    }

    pub(crate) fn estimate_cardinality_from_selectivity(
        &self,
        base_cardinality: u64,
        selectivity: f64,
    ) -> CardinalityEstimate {
        self.cardinality_from_selectivity(base_cardinality, selectivity, false)
    }

    pub(crate) fn apply_selectivity_to_cardinality(
        &self,
        base: CardinalityEstimate,
        selectivity: f64,
    ) -> CardinalityEstimate {
        let min = self.cardinality_from_selectivity(base.min, selectivity, false);
        let expected = self.cardinality_from_selectivity(base.expected, selectivity, false);
        let max = self.cardinality_from_selectivity(base.max, selectivity, false);
        CardinalityEstimate {
            min: min.min,
            expected: expected.expected,
            max: max.max.max(expected.expected),
        }
    }

    fn cardinality_from_selectivity(
        &self,
        base_cardinality: u64,
        selectivity: f64,
        proven: bool,
    ) -> CardinalityEstimate {
        if base_cardinality == 0 {
            return CardinalityEstimate::exact(0);
        }
        let selectivity = clamp_selectivity(selectivity);
        let expected = ((base_cardinality as f64) * selectivity).round() as u64;
        let expected = if proven && selectivity == 0.0 {
            0
        } else {
            expected.max(1)
        };
        let min = ((base_cardinality as f64) * (selectivity * 0.5).clamp(0.0, 1.0)).floor() as u64;
        let max = ((base_cardinality as f64) * (selectivity * 1.5).clamp(0.0, 1.0)).ceil() as u64;

        CardinalityEstimate {
            min,
            expected: expected.min(base_cardinality),
            max: max.max(expected).min(base_cardinality),
        }
    }

    fn estimate_comparison_selectivity(
        &self,
        expr: &ComparisonExpression,
        resolver: &StatisticsResolver<'_>,
    ) -> f64 {
        let default = if matches!(
            expr.comparison_type,
            ComparisonType::LessThan
                | ComparisonType::LessThanOrEqual
                | ComparisonType::GreaterThan
                | ComparisonType::GreaterThanOrEqual
        ) {
            self.defaults.range
        } else {
            self.defaults.equality
        };

        let Some((column, constant, comparison_type)) = column_constant_comparison(expr) else {
            return default;
        };

        let Some(stats) = resolver.get(column) else {
            return default;
        };

        match comparison_type {
            ComparisonType::Equal | ComparisonType::NotDistinctFrom => {
                let distinct = stats.get_distinct_count();
                if distinct == 0 {
                    return default;
                }
                (1.0 / distinct as f64).max(MIN_SELECTIVITY)
            }
            ComparisonType::NotEqual | ComparisonType::DistinctFrom => {
                let distinct = stats.get_distinct_count();
                if distinct == 0 {
                    return default;
                }
                (1.0 - (1.0 / distinct as f64)).clamp(MIN_SELECTIVITY, 1.0)
            }
            ComparisonType::LessThan
            | ComparisonType::LessThanOrEqual
            | ComparisonType::GreaterThan
            | ComparisonType::GreaterThanOrEqual => {
                estimate_range_selectivity(stats, constant, comparison_type)
                    .unwrap_or(self.defaults.range)
            }
        }
    }

    /// Estimate an implicit or explicit AND after coalescing ordered bounds on
    /// the same integral column. Treating `x >= a` and `x < b` as independent
    /// events systematically overestimates bounded intervals; their shared
    /// statistics domain makes the intersection directly measurable.
    fn estimate_conjunction<'e>(
        &self,
        expressions: impl IntoIterator<Item = &'e Expression>,
        resolver: &StatisticsResolver<'_>,
    ) -> SelectivityEstimate {
        let mut flattened = Vec::new();
        for expression in expressions {
            flatten_and(expression, &mut flattened);
        }

        let mut intervals = Vec::<IntegralIntervalEstimate>::new();
        let mut interval_by_binding = HashMap::<ColumnBinding, usize>::new();
        let mut interval_for_expression = vec![None; flattened.len()];
        for (expression_idx, expression) in flattened.iter().copied().enumerate() {
            let Some(constraint) = integral_range_constraint(expression, resolver) else {
                continue;
            };
            let interval_idx = match interval_by_binding.get(&constraint.binding).copied() {
                Some(interval_idx) => interval_idx,
                None => {
                    let interval_idx = intervals.len();
                    intervals.push(IntegralIntervalEstimate::new(expression_idx, &constraint));
                    interval_by_binding.insert(constraint.binding, interval_idx);
                    interval_idx
                }
            };
            let interval = &mut intervals[interval_idx];
            if interval.domain != constraint.domain
                || interval.minimum != constraint.minimum
                || interval.maximum != constraint.maximum
            {
                continue;
            }
            interval.intersect(constraint.bound, constraint.constant);
            interval_for_expression[expression_idx] = Some(interval_idx);
        }

        column_aware_conjunction_estimate(flattened.iter().enumerate().filter_map(
            |(expression_idx, expression)| match interval_for_expression[expression_idx] {
                Some(interval_idx)
                    if intervals[interval_idx].first_expression == expression_idx =>
                {
                    Some((
                        SelectivityEstimate::estimated(intervals[interval_idx].selectivity()),
                        Some(intervals[interval_idx].binding),
                    ))
                }
                Some(_) => None,
                None => Some((
                    self.estimate_selectivity_with_provenance(expression, resolver),
                    expression_single_binding(expression, resolver),
                )),
            },
        ))
    }

    fn estimate_in_selectivity(
        &self,
        children: &[Expression],
        resolver: &StatisticsResolver<'_>,
    ) -> f64 {
        let Some(column) = children.first() else {
            return self.defaults.predicate;
        };

        let probe_count = children.len().saturating_sub(1).max(1) as f64;
        let Some(stats) = resolver.get(column) else {
            return clamp_selectivity(self.defaults.equality * probe_count);
        };
        let distinct = stats.get_distinct_count();
        if distinct == 0 {
            return clamp_selectivity(self.defaults.equality * probe_count);
        }
        clamp_selectivity((probe_count / distinct as f64).max(MIN_SELECTIVITY))
    }

    fn estimate_exact_like_selectivity(
        &self,
        candidate: Option<&Expression>,
        resolver: &StatisticsResolver<'_>,
    ) -> f64 {
        let Some(candidate) = candidate else {
            return self.defaults.equality;
        };
        let distinct = resolver
            .get(candidate)
            .map(|stats| stats.get_distinct_count())
            .unwrap_or(0);
        if distinct == 0 {
            self.defaults.equality
        } else {
            (1.0 / distinct as f64).max(MIN_SELECTIVITY)
        }
    }
}

fn clamp_selectivity(value: f64) -> f64 {
    if value.is_finite() {
        value.clamp(0.0, 1.0)
    } else {
        1.0
    }
}

fn column_constant_comparison(
    expression: &ComparisonExpression,
) -> Option<(&Expression, &Value, ComparisonType)> {
    match (expression.left.as_ref(), expression.right.as_ref()) {
        (
            column @ (Expression::ColumnRef(_) | Expression::Reference(_)),
            Expression::Constant(constant),
        ) => Some((column, &constant.value, expression.comparison_type)),
        (
            Expression::Constant(constant),
            column @ (Expression::ColumnRef(_) | Expression::Reference(_)),
        ) => Some((
            column,
            &constant.value,
            reverse_comparison(expression.comparison_type),
        )),
        _ => None,
    }
}

fn reverse_comparison(comparison_type: ComparisonType) -> ComparisonType {
    match comparison_type {
        ComparisonType::LessThan => ComparisonType::GreaterThan,
        ComparisonType::LessThanOrEqual => ComparisonType::GreaterThanOrEqual,
        ComparisonType::GreaterThan => ComparisonType::LessThan,
        ComparisonType::GreaterThanOrEqual => ComparisonType::LessThanOrEqual,
        other => other,
    }
}

fn flatten_and<'e>(expression: &'e Expression, output: &mut Vec<&'e Expression>) {
    if let Expression::Conjunction(conjunction) = expression {
        if conjunction.conjunction_type == ConjunctionType::And {
            for child in &conjunction.children {
                flatten_and(child, output);
            }
            return;
        }
    }
    output.push(expression);
}

fn expression_single_binding(
    expression: &Expression,
    resolver: &StatisticsResolver<'_>,
) -> Option<ColumnBinding> {
    let mut binding = None;
    let mut ambiguous = false;
    ExpressionIterator::visit(expression, &mut |node| {
        let Some(candidate) = resolver.binding(node) else {
            return ExpressionVisitDecision::Descend;
        };
        match binding {
            None => binding = Some(candidate),
            Some(existing) if existing != candidate => ambiguous = true,
            Some(_) => {}
        }
        ExpressionVisitDecision::SkipChildren
    });
    (!ambiguous).then_some(binding).flatten()
}

#[derive(Debug, Clone, Copy)]
struct IntegralRangeConstraint {
    binding: ColumnBinding,
    domain: IntegralDomain,
    minimum: u128,
    maximum: u128,
    bound: IntegralRangeBound,
    constant: u128,
}

#[derive(Debug, Clone, Copy)]
enum IntegralRangeBound {
    Upper { inclusive: bool },
    Lower { inclusive: bool },
}

fn integral_range_constraint(
    expression: &Expression,
    resolver: &StatisticsResolver<'_>,
) -> Option<IntegralRangeConstraint> {
    let Expression::Comparison(comparison) = expression else {
        return None;
    };
    if !matches!(
        comparison.comparison_type,
        ComparisonType::LessThan
            | ComparisonType::LessThanOrEqual
            | ComparisonType::GreaterThan
            | ComparisonType::GreaterThanOrEqual
    ) {
        return None;
    }
    let (column, constant, comparison_type) = column_constant_comparison(comparison)?;
    let binding = resolver.binding(column)?;
    let stats = resolver.get(column)?;
    let minimum = ordered_integral_value(&NumericStats::min(stats.statistics())?)?;
    let maximum = ordered_integral_value(&NumericStats::max(stats.statistics())?)?;
    let constant = ordered_integral_value(constant)?;
    if minimum.domain != maximum.domain
        || minimum.domain != constant.domain
        || minimum.coordinate > maximum.coordinate
    {
        return None;
    }
    let bound = match comparison_type {
        ComparisonType::LessThan => IntegralRangeBound::Upper { inclusive: false },
        ComparisonType::LessThanOrEqual => IntegralRangeBound::Upper { inclusive: true },
        ComparisonType::GreaterThan => IntegralRangeBound::Lower { inclusive: false },
        ComparisonType::GreaterThanOrEqual => IntegralRangeBound::Lower { inclusive: true },
        _ => return None,
    };
    Some(IntegralRangeConstraint {
        binding,
        domain: minimum.domain,
        minimum: minimum.coordinate,
        maximum: maximum.coordinate,
        bound,
        constant: constant.coordinate,
    })
}

#[derive(Debug)]
struct IntegralIntervalEstimate {
    binding: ColumnBinding,
    domain: IntegralDomain,
    minimum: u128,
    maximum: u128,
    lower: u128,
    upper: u128,
    empty: bool,
    first_expression: usize,
}

impl IntegralIntervalEstimate {
    fn new(first_expression: usize, constraint: &IntegralRangeConstraint) -> Self {
        Self {
            binding: constraint.binding,
            domain: constraint.domain,
            minimum: constraint.minimum,
            maximum: constraint.maximum,
            lower: constraint.minimum,
            upper: constraint.maximum,
            empty: false,
            first_expression,
        }
    }

    fn intersect(&mut self, bound: IntegralRangeBound, constant: u128) {
        if self.empty {
            return;
        }
        match bound {
            IntegralRangeBound::Upper { inclusive: false } => {
                if constant <= self.minimum {
                    self.empty = true;
                } else {
                    self.upper = self.upper.min(constant - 1);
                }
            }
            IntegralRangeBound::Upper { inclusive: true } => {
                if constant < self.minimum {
                    self.empty = true;
                } else {
                    self.upper = self.upper.min(constant);
                }
            }
            IntegralRangeBound::Lower { inclusive: false } => {
                if constant >= self.maximum {
                    self.empty = true;
                } else {
                    self.lower = self.lower.max(constant + 1);
                }
            }
            IntegralRangeBound::Lower { inclusive: true } => {
                if constant > self.maximum {
                    self.empty = true;
                } else {
                    self.lower = self.lower.max(constant);
                }
            }
        }
        self.empty |= self.lower > self.upper;
    }

    fn selectivity(&self) -> f64 {
        if self.empty {
            return 0.0;
        }
        let domain = self.maximum.saturating_sub(self.minimum).saturating_add(1) as f64;
        let matching = self.lower.abs_diff(self.upper).saturating_add(1) as f64;
        clamp_selectivity(matching.min(domain) / domain)
    }
}

/// Estimate an ordered comparison from complete-population numeric bounds.
///
/// Integral domains use their exact number of representable values so that
/// inclusive and exclusive predicates differ at the endpoints. Floating-point
/// domains use the conventional continuous uniform approximation.
fn estimate_range_selectivity(
    stats: &ColumnStatistics,
    constant: &Value,
    comparison_type: ComparisonType,
) -> Option<f64> {
    let minimum = NumericStats::min(stats.statistics())?;
    let maximum = NumericStats::max(stats.statistics())?;

    if let (Some(minimum), Some(maximum), Some(constant)) = (
        ordered_integral_value(&minimum),
        ordered_integral_value(&maximum),
        ordered_integral_value(constant),
    ) {
        if minimum.domain != maximum.domain || minimum.domain != constant.domain {
            return None;
        }
        let (minimum, maximum, constant) =
            (minimum.coordinate, maximum.coordinate, constant.coordinate);
        if minimum > maximum {
            return None;
        }
        let domain = maximum.saturating_sub(minimum).saturating_add(1) as f64;
        let matching = match comparison_type {
            ComparisonType::LessThan if constant <= minimum => 0,
            ComparisonType::LessThan => constant.saturating_sub(minimum),
            ComparisonType::LessThanOrEqual if constant < minimum => 0,
            ComparisonType::LessThanOrEqual => constant.saturating_sub(minimum).saturating_add(1),
            ComparisonType::GreaterThan if constant >= maximum => 0,
            ComparisonType::GreaterThan => maximum.saturating_sub(constant),
            ComparisonType::GreaterThanOrEqual if constant > maximum => 0,
            ComparisonType::GreaterThanOrEqual => {
                maximum.saturating_sub(constant).saturating_add(1)
            }
            _ => return None,
        };
        return Some(clamp_selectivity((matching as f64).min(domain) / domain));
    }

    let minimum = ordered_float_value(&minimum)?;
    let maximum = ordered_float_value(&maximum)?;
    let constant = ordered_float_value(constant)?;
    if !minimum.is_finite() || !maximum.is_finite() || !constant.is_finite() || minimum > maximum {
        return None;
    }
    if minimum == maximum {
        return Some(match comparison_type {
            ComparisonType::LessThan => {
                if minimum < constant {
                    1.0
                } else {
                    0.0
                }
            }
            ComparisonType::LessThanOrEqual => {
                if minimum <= constant {
                    1.0
                } else {
                    0.0
                }
            }
            ComparisonType::GreaterThan => {
                if minimum > constant {
                    1.0
                } else {
                    0.0
                }
            }
            ComparisonType::GreaterThanOrEqual => {
                if minimum >= constant {
                    1.0
                } else {
                    0.0
                }
            }
            _ => return None,
        });
    }
    let below = ((constant - minimum) / (maximum - minimum)).clamp(0.0, 1.0);
    Some(match comparison_type {
        ComparisonType::LessThan | ComparisonType::LessThanOrEqual => below,
        ComparisonType::GreaterThan | ComparisonType::GreaterThanOrEqual => 1.0 - below,
        _ => return None,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IntegralDomain {
    Boolean,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    HugeInt,
    UTinyInt,
    USmallInt,
    UInteger,
    UBigInt,
    UHugeInt,
    Date,
    Timestamp,
    TimestampTz,
    Time,
    Decimal { scale: u8 },
}

struct OrderedIntegralValue {
    domain: IntegralDomain,
    coordinate: u128,
}

fn ordered_integral_value(value: &Value) -> Option<OrderedIntegralValue> {
    let (domain, coordinate) = match value {
        Value::Boolean(value) => (IntegralDomain::Boolean, u128::from(*value)),
        Value::TinyInt(value) => (
            IntegralDomain::TinyInt,
            u128::from((*value as u8) ^ (1 << 7)),
        ),
        Value::SmallInt(value) => (
            IntegralDomain::SmallInt,
            u128::from((*value as u16) ^ (1 << 15)),
        ),
        Value::Integer(value) => (
            IntegralDomain::Integer,
            u128::from((*value as u32) ^ (1 << 31)),
        ),
        Value::BigInt(value) => (
            IntegralDomain::BigInt,
            u128::from((*value as u64) ^ (1 << 63)),
        ),
        Value::HugeInt(value) => (IntegralDomain::HugeInt, (*value as u128) ^ (1 << 127)),
        Value::UTinyInt(value) => (IntegralDomain::UTinyInt, u128::from(*value)),
        Value::USmallInt(value) => (IntegralDomain::USmallInt, u128::from(*value)),
        Value::UInteger(value) => (IntegralDomain::UInteger, u128::from(*value)),
        Value::UBigInt(value) => (IntegralDomain::UBigInt, u128::from(*value)),
        Value::UHugeInt(value) => (IntegralDomain::UHugeInt, *value),
        Value::Date(value) => (
            IntegralDomain::Date,
            u128::from((*value as u32) ^ (1 << 31)),
        ),
        Value::Timestamp(value) => (
            IntegralDomain::Timestamp,
            u128::from((*value as u64) ^ (1 << 63)),
        ),
        Value::TimestampTz(value) => (
            IntegralDomain::TimestampTz,
            u128::from((*value as u64) ^ (1 << 63)),
        ),
        Value::Time(value) => (
            IntegralDomain::Time,
            u128::from((*value as u64) ^ (1 << 63)),
        ),
        Value::Decimal(value, _, scale) => (
            IntegralDomain::Decimal { scale: *scale },
            (*value as u128) ^ (1 << 127),
        ),
        _ => return None,
    };
    Some(OrderedIntegralValue { domain, coordinate })
}

fn ordered_float_value(value: &Value) -> Option<f64> {
    match value {
        Value::Float(value) => Some(*value as f64),
        Value::Double(value) => Some(*value),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LikePatternShape {
    MatchAll,
    Exact,
    Wildcard(WildcardLikePattern),
    Generic,
}

/// Semantic shape of a `%`-only wildcard pattern after consecutive wildcards
/// have been normalized away.
///
/// Wildcard count is deliberately absent: `%%needle%` and `%needle%` have the
/// same language and therefore must receive the same estimate. Anchors and
/// non-empty literal fragments are the properties that strengthen a pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WildcardLikePattern {
    anchored_start: bool,
    anchored_end: bool,
    literal_fragments: usize,
}

impl WildcardLikePattern {
    fn selectivity(self, defaults: &SelectivityDefaults) -> f64 {
        let base = match (self.anchored_start, self.anchored_end) {
            (true, false) => defaults.like_prefix,
            (false, true) | (false, false) => defaults.like_contains,
            // An internally wildcarded pattern anchored at both ends is at
            // least as selective as either one-ended form.
            (true, true) => defaults.like_prefix.min(defaults.like_contains),
        };

        // Ordered fragments overlap on one string value and are consequently
        // more correlated than independent same-column predicates. Give each
        // additional fragment half the previous weight, saturating at the
        // equivalent of two independent occurrences. The finite bound avoids
        // converting an arbitrarily long SQL literal into an integer exponent.
        let remaining_weight = if self.literal_fragments >= 64 {
            0.0
        } else {
            0.5f64.powi(self.literal_fragments as i32)
        };
        let exponent = 2.0 * (1.0 - remaining_weight);
        base.powf(exponent)
    }
}

fn like_pattern_shape(pattern: Option<&Expression>) -> LikePatternShape {
    let Some(Expression::Constant(constant)) = pattern else {
        return LikePatternShape::Generic;
    };
    let Value::Varchar(value) = &constant.value else {
        return LikePatternShape::Generic;
    };
    if value.contains('_') || value.contains('\\') {
        return LikePatternShape::Generic;
    }
    if !value.contains('%') {
        return LikePatternShape::Exact;
    }
    let literal_fragments = value
        .split('%')
        .filter(|fragment| !fragment.is_empty())
        .count();
    if literal_fragments == 0 {
        return LikePatternShape::MatchAll;
    }
    LikePatternShape::Wildcard(WildcardLikePattern {
        anchored_start: !value.starts_with('%'),
        anchored_end: !value.ends_with('%'),
        literal_fragments,
    })
}

#[cfg(test)]
mod tests {
    use paro_common::types::LogicalType;
    use paro_planner::expression::{
        ColumnRefExpression, ComparisonExpression, ConstantExpression, OperatorExpression,
    };

    use super::*;

    #[test]
    fn sparse_fetch_requires_enough_work_to_amortize_its_frontier() {
        let model = CostModel::default();
        assert!(model
            .late_row_fetch_benefit(1, 1, [LogicalType::Varchar], 8)
            .is_none());
        assert!(model
            .late_row_fetch_benefit(100_000, 100, [LogicalType::Varchar], 3)
            .is_some());
    }

    #[test]
    fn estimate_filter_cardinality_preserves_empty_predicates() {
        let model = CostModel::default();
        let estimate = model.estimate_filter_cardinality(42, &[], &HashMap::new());
        assert_eq!(estimate, CardinalityEstimate::exact(42));
    }

    #[test]
    fn constant_false_selectivity_is_zero() {
        let model = CostModel::default();
        let expr = Expression::Constant(ConstantExpression::new(
            Value::Boolean(false),
            LogicalType::Boolean,
        ));
        assert_eq!(model.estimate_selectivity(&expr, &HashMap::new()), 0.0);
    }

    #[test]
    fn comparison_without_stats_uses_default_equality_selectivity() {
        let model = CostModel::default();
        let expr = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(1, 0),
                LogicalType::Integer,
            )),
            Expression::Constant(ConstantExpression::new(
                Value::Integer(7),
                LogicalType::Integer,
            )),
        ));

        assert_eq!(
            model.estimate_selectivity(&expr, &HashMap::new()),
            model.defaults.equality
        );
    }

    #[test]
    fn integral_range_selectivity_uses_bounds_and_preserves_orientation() {
        let model = CostModel::default();
        let binding = ColumnBinding::new(1, 0);
        let mut stats = ColumnStatistics::new(
            paro_storage::statistics::BaseStatistics::create_empty(LogicalType::Date),
        );
        NumericStats::set_guaranteed_min(stats.statistics_mut(), &Value::Date(0));
        NumericStats::set_guaranteed_max(stats.statistics_mut(), &Value::Date(100));
        let column_stats = HashMap::from([(binding, Arc::new(stats))]);
        let column = || Expression::ColumnRef(ColumnRefExpression::new(binding, LogicalType::Date));
        let constant =
            || Expression::Constant(ConstantExpression::new(Value::Date(90), LogicalType::Date));
        let upper_bound = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::LessThanOrEqual,
            column(),
            constant(),
        ));
        let reversed = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::GreaterThanOrEqual,
            constant(),
            column(),
        ));

        let expected = 91.0 / 101.0;
        assert_eq!(
            model.estimate_selectivity(&upper_bound, &column_stats),
            expected
        );
        assert_eq!(
            model.estimate_selectivity(&reversed, &column_stats),
            expected
        );
    }

    #[test]
    fn integral_range_selectivity_handles_domain_boundaries() {
        let mut stats = ColumnStatistics::new(
            paro_storage::statistics::BaseStatistics::create_empty(LogicalType::Integer),
        );
        NumericStats::set_guaranteed_min(stats.statistics_mut(), &Value::Integer(10));
        NumericStats::set_guaranteed_max(stats.statistics_mut(), &Value::Integer(20));

        assert_eq!(
            estimate_range_selectivity(&stats, &Value::Integer(9), ComparisonType::LessThanOrEqual,),
            Some(0.0)
        );
        assert_eq!(
            estimate_range_selectivity(
                &stats,
                &Value::Integer(21),
                ComparisonType::GreaterThanOrEqual,
            ),
            Some(0.0)
        );
        assert_eq!(
            estimate_range_selectivity(
                &stats,
                &Value::Integer(20),
                ComparisonType::LessThanOrEqual,
            ),
            Some(1.0)
        );
    }

    #[test]
    fn conjunction_coalesces_bounds_on_the_same_integral_column() {
        let model = CostModel::default();
        let binding = ColumnBinding::new(1, 0);
        let mut stats = ColumnStatistics::new(
            paro_storage::statistics::BaseStatistics::create_empty(LogicalType::Integer),
        );
        NumericStats::set_guaranteed_min(stats.statistics_mut(), &Value::Integer(0));
        NumericStats::set_guaranteed_max(stats.statistics_mut(), &Value::Integer(9));
        let column_stats = HashMap::from([(binding, Arc::new(stats))]);
        let comparison = |comparison_type, value| {
            Expression::Comparison(ComparisonExpression::new(
                comparison_type,
                Expression::ColumnRef(ColumnRefExpression::new(binding, LogicalType::Integer)),
                Expression::Constant(ConstantExpression::new(
                    Value::Integer(value),
                    LogicalType::Integer,
                )),
            ))
        };
        let expression =
            Expression::Conjunction(paro_planner::expression::ConjunctionExpression::new(
                ConjunctionType::And,
                vec![
                    comparison(ComparisonType::GreaterThanOrEqual, 2),
                    comparison(ComparisonType::LessThan, 5),
                ],
            ));

        assert_eq!(model.estimate_selectivity(&expression, &column_stats), 0.3);
        assert_eq!(
            model
                .estimate_filter_cardinality(64, &[expression], &column_stats)
                .expected,
            19
        );
    }

    #[test]
    fn conjunction_dampens_only_distinct_columns_within_one_relation() {
        let model = CostModel::default();
        let size_binding = ColumnBinding::new(1, 0);
        let type_binding = ColumnBinding::new(1, 1);
        let mut size_stats = ColumnStatistics::new(
            paro_storage::statistics::BaseStatistics::create_empty(LogicalType::Integer),
        );
        let hashes = (0..50).map(paro_common::hash::hash_u64).collect::<Vec<_>>();
        size_stats.update_distinct_statistics(&hashes, hashes.len());
        let distinct = size_stats.get_distinct_count();
        let column_stats = HashMap::from([(size_binding, Arc::new(size_stats))]);
        let equality = |value| {
            Expression::Comparison(ComparisonExpression::new(
                ComparisonType::NotEqual,
                Expression::ColumnRef(ColumnRefExpression::new(size_binding, LogicalType::Integer)),
                Expression::Constant(ConstantExpression::new(
                    Value::Integer(value),
                    LogicalType::Integer,
                )),
            ))
        };
        let suffix = Expression::Operator(OperatorExpression::new(
            OperatorType::Like,
            vec![
                Expression::ColumnRef(ColumnRefExpression::new(type_binding, LogicalType::Varchar)),
                Expression::Constant(ConstantExpression::new(
                    Value::Varchar("%BRASS".to_string()),
                    LogicalType::Varchar,
                )),
            ],
            LogicalType::Boolean,
        ));

        let same_column = (1.0 - 1.0 / distinct as f64).powi(2);
        let expected = model.defaults.like_contains * same_column.sqrt();
        let actual = model
            .estimate_filter_cardinality(
                200_000,
                &[equality(14), equality(16), suffix],
                &column_stats,
            )
            .expected;
        assert_eq!(actual, (200_000.0 * expected).round() as u64);
    }

    #[test]
    fn conjunction_keeps_different_relations_independent() {
        let model = CostModel::default();
        let equality = |table_index| {
            Expression::Comparison(ComparisonExpression::new(
                ComparisonType::Equal,
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(table_index, 0),
                    LogicalType::Integer,
                )),
                Expression::Constant(ConstantExpression::new(
                    Value::Integer(1),
                    LogicalType::Integer,
                )),
            ))
        };

        assert_eq!(
            model
                .estimate_filter_cardinality(10_000, &[equality(1), equality(2)], &HashMap::new())
                .expected,
            100
        );
    }

    #[test]
    fn integral_range_selectivity_rejects_mixed_types_and_decimal_scales() {
        let mut integer_stats = ColumnStatistics::new(
            paro_storage::statistics::BaseStatistics::create_empty(LogicalType::Integer),
        );
        NumericStats::set_guaranteed_min(integer_stats.statistics_mut(), &Value::Integer(10));
        NumericStats::set_guaranteed_max(integer_stats.statistics_mut(), &Value::Integer(20));
        assert_eq!(
            estimate_range_selectivity(
                &integer_stats,
                &Value::BigInt(15),
                ComparisonType::LessThanOrEqual,
            ),
            None
        );

        let decimal_type = LogicalType::Decimal {
            precision: 10,
            scale: 2,
        };
        let mut decimal_stats = ColumnStatistics::new(
            paro_storage::statistics::BaseStatistics::create_empty(decimal_type),
        );
        NumericStats::set_guaranteed_min(
            decimal_stats.statistics_mut(),
            &Value::Decimal(1_000, 10, 2),
        );
        NumericStats::set_guaranteed_max(
            decimal_stats.statistics_mut(),
            &Value::Decimal(2_000, 10, 2),
        );
        assert_eq!(
            estimate_range_selectivity(
                &decimal_stats,
                &Value::Decimal(1_500, 10, 3),
                ComparisonType::LessThanOrEqual,
            ),
            None
        );
    }

    #[test]
    fn like_selectivity_distinguishes_pattern_shapes() {
        let model = CostModel::default();
        let like = |pattern: &str| {
            Expression::Operator(OperatorExpression::new(
                OperatorType::Like,
                vec![
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(1, 0),
                        LogicalType::Varchar,
                    )),
                    Expression::Constant(ConstantExpression::new(
                        Value::Varchar(pattern.to_string()),
                        LogicalType::Varchar,
                    )),
                ],
                LogicalType::Boolean,
            ))
        };

        assert_eq!(
            model.estimate_selectivity(&like("green"), &HashMap::new()),
            0.1
        );
        assert_eq!(
            model.estimate_selectivity(&like("green%"), &HashMap::new()),
            0.25
        );
        assert_eq!(
            model.estimate_selectivity(&like("%green"), &HashMap::new()),
            0.05
        );
        assert_eq!(
            model.estimate_selectivity(&like("%green%"), &HashMap::new()),
            0.05
        );
        assert_eq!(
            model.estimate_selectivity(&like("%Customer%Complaints%"), &HashMap::new()),
            0.05f64.powf(1.5)
        );
        assert_eq!(
            model.estimate_selectivity(&like("%%green%"), &HashMap::new()),
            model.estimate_selectivity(&like("%green%"), &HashMap::new()),
            "consecutive percent wildcards do not change the pattern language"
        );
        assert!(
            model.estimate_selectivity(&like("%a%b"), &HashMap::new())
                <= model.estimate_selectivity(&like("%b"), &HashMap::new())
        );
        assert!(
            model.estimate_selectivity(&like("a%b%"), &HashMap::new())
                <= model.estimate_selectivity(&like("a%"), &HashMap::new())
        );
        assert!(
            model.estimate_selectivity(&like("a%b%c"), &HashMap::new())
                <= model.estimate_selectivity(&like("%b%"), &HashMap::new())
        );
        assert_eq!(model.estimate_selectivity(&like("%"), &HashMap::new()), 1.0);
        assert_eq!(
            model.estimate_selectivity(&like("gr_en%"), &HashMap::new()),
            0.75
        );

        let not_like = Expression::Operator(OperatorExpression::new_unary(
            OperatorType::Not,
            like("%green%"),
            LogicalType::Boolean,
        ));
        assert_eq!(model.estimate_selectivity(&not_like, &HashMap::new()), 0.95);

        let not_match_all = Expression::Operator(OperatorExpression::new_unary(
            OperatorType::Not,
            like("%"),
            LogicalType::Boolean,
        ));
        assert_eq!(
            model.estimate_selectivity(&not_match_all, &HashMap::new()),
            MIN_SELECTIVITY
        );
    }

    #[test]
    fn only_proven_false_predicates_receive_zero_selectivity() {
        let model = CostModel::default();
        let constant = |value| {
            Expression::Constant(ConstantExpression::new(
                Value::Boolean(value),
                LogicalType::Boolean,
            ))
        };
        let estimated_zero =
            Expression::Conjunction(paro_planner::expression::ConjunctionExpression::new(
                ConjunctionType::And,
                vec![
                    Expression::Operator(OperatorExpression::new_unary(
                        OperatorType::Not,
                        Expression::Operator(OperatorExpression::new(
                            OperatorType::Like,
                            vec![
                                Expression::ColumnRef(ColumnRefExpression::new(
                                    ColumnBinding::new(1, 0),
                                    LogicalType::Varchar,
                                )),
                                Expression::Constant(ConstantExpression::new(
                                    Value::Varchar("%".to_string()),
                                    LogicalType::Varchar,
                                )),
                            ],
                            LogicalType::Boolean,
                        )),
                        LogicalType::Boolean,
                    )),
                    constant(true),
                ],
            ));
        let proven_zero =
            Expression::Conjunction(paro_planner::expression::ConjunctionExpression::new(
                ConjunctionType::And,
                vec![estimated_zero.clone(), constant(false)],
            ));

        assert_eq!(
            model.estimate_selectivity(&estimated_zero, &HashMap::new()),
            MIN_SELECTIVITY
        );
        assert_eq!(
            model.estimate_selectivity(&proven_zero, &HashMap::new()),
            0.0
        );
        assert_eq!(
            model
                .estimate_filter_cardinality(42, &[estimated_zero], &HashMap::new())
                .expected,
            1
        );
        assert_eq!(
            model.estimate_filter_cardinality(42, &[proven_zero], &HashMap::new()),
            CardinalityEstimate::exact(0)
        );
    }

    #[test]
    fn exponential_damping_never_claims_a_proven_bound() {
        let estimate = column_aware_conjunction_estimate(
            [
                (
                    SelectivityEstimate::proven(0.1),
                    Some(ColumnBinding::new(7, 0)),
                ),
                (
                    SelectivityEstimate::proven(0.2),
                    Some(ColumnBinding::new(7, 1)),
                ),
            ]
            .into_iter(),
        );

        assert!(!estimate.proven);
        assert!((estimate.fraction - (0.1 * 0.2_f64.sqrt())).abs() < f64::EPSILON);
    }

    #[test]
    fn exact_like_uses_column_distinct_count() {
        let model = CostModel::default();
        let binding = ColumnBinding::new(1, 0);
        let mut stats = ColumnStatistics::new(
            paro_storage::statistics::BaseStatistics::create_empty(LogicalType::Varchar),
        );
        let hashes = (0..100)
            .map(paro_common::hash::hash_u64)
            .collect::<Vec<_>>();
        stats.update_distinct_statistics(&hashes, hashes.len());
        let distinct = stats.get_distinct_count();
        let column_stats = HashMap::from([(binding, Arc::new(stats))]);
        let expression = Expression::Operator(OperatorExpression::new(
            OperatorType::Like,
            vec![
                Expression::ColumnRef(ColumnRefExpression::new(binding, LogicalType::Varchar)),
                Expression::Constant(ConstantExpression::new(
                    Value::Varchar("green".to_string()),
                    LogicalType::Varchar,
                )),
            ],
            LogicalType::Boolean,
        ));

        assert_eq!(
            model.estimate_selectivity(&expression, &column_stats),
            1.0 / distinct as f64
        );
    }
}
