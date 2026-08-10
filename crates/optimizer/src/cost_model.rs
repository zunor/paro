// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use paro_common::runtime_value::Value;
use paro_external::routine::identity::BuiltinIntrinsicId;
use paro_planner::expression::{
    ComparisonExpression, ComparisonType, ConjunctionType, Expression, OperatorType,
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

    fn get(&self, expression: &Expression) -> Option<&'a Arc<ColumnStatistics>> {
        let binding = match expression {
            Expression::ColumnRef(column) => column.binding,
            Expression::Reference(reference) => *self.positional_bindings?.get(reference.index)?,
            _ => return None,
        };
        self.column_stats.get(&binding)
    }
}

impl CostModel {
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
            Expression::Conjunction(conjunction) => {
                let children = || {
                    conjunction
                        .children
                        .iter()
                        .map(|child| self.estimate_selectivity_with_provenance(child, resolver))
                };
                match conjunction.conjunction_type {
                    ConjunctionType::And => conjunction_estimate(children()),
                    ConjunctionType::Or => disjunction_estimate(children()),
                }
            }
            Expression::Operator(operator) => match operator.operator_type {
                OperatorType::Like => SelectivityEstimate::estimated(
                    match like_pattern_shape(operator.children.get(1)) {
                        LikePatternShape::MatchAll => 1.0,
                        LikePatternShape::Exact => self
                            .estimate_exact_like_selectivity(operator.children.first(), resolver),
                        LikePatternShape::Prefix => self.defaults.like_prefix,
                        LikePatternShape::Contains | LikePatternShape::Suffix => {
                            self.defaults.like_contains
                        }
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
                        LikePatternShape::Prefix => self.defaults.like_prefix,
                        LikePatternShape::Contains | LikePatternShape::Suffix => {
                            self.defaults.like_contains
                        }
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

        let combined = conjunction_estimate(
            expressions
                .iter()
                .map(|expr| self.estimate_selectivity_with_provenance(expr, resolver)),
        );
        let combined_selectivity = combined.fraction;

        let expected = ((base_cardinality as f64) * combined_selectivity).round() as u64;
        let expected = if combined.proven && combined_selectivity == 0.0 {
            0
        } else {
            expected.max(1)
        };
        let min = ((base_cardinality as f64) * (combined_selectivity * 0.5).clamp(0.0, 1.0)).floor()
            as u64;
        let max = ((base_cardinality as f64) * (combined_selectivity * 1.5).clamp(0.0, 1.0)).ceil()
            as u64;

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
    Prefix,
    Suffix,
    Contains,
    Generic,
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
    if !value.is_empty() && value.bytes().all(|byte| byte == b'%') {
        return LikePatternShape::MatchAll;
    }
    let wildcard_count = value.bytes().filter(|byte| *byte == b'%').count();
    match wildcard_count {
        0 => LikePatternShape::Exact,
        1 if value.ends_with('%') => LikePatternShape::Prefix,
        1 if value.starts_with('%') => LikePatternShape::Suffix,
        2 if value.starts_with('%') && value.ends_with('%') => LikePatternShape::Contains,
        _ => LikePatternShape::Generic,
    }
}

#[cfg(test)]
mod tests {
    use paro_common::types::LogicalType;
    use paro_planner::expression::{
        ColumnRefExpression, ComparisonExpression, ConstantExpression, OperatorExpression,
    };

    use super::*;

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
