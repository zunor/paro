// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use paro_common::runtime_value::Value;
use paro_external::routine::identity::BuiltinIntrinsicId;
use paro_planner::expression::{
    ColumnRefExpression, ComparisonExpression, ComparisonType, ConjunctionType, Expression,
    OperatorType,
};
use paro_planner::operator::ColumnBinding;
use paro_planner::plan::CardinalityEstimate;
use paro_storage::statistics::ColumnStatistics;

const MIN_SELECTIVITY: f64 = 0.000_001;

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

impl CostModel {
    pub fn estimate_selectivity(
        &self,
        expr: &Expression,
        column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    ) -> f64 {
        let selectivity = match expr {
            Expression::Constant(constant) => match &constant.value {
                Value::Boolean(true) => 1.0,
                Value::Boolean(false) => 0.0,
                _ => self.defaults.predicate,
            },
            Expression::Comparison(comparison) => {
                self.estimate_comparison_selectivity(comparison, column_stats)
            }
            Expression::Conjunction(conjunction) => match conjunction.conjunction_type {
                ConjunctionType::And => conjunction
                    .children
                    .iter()
                    .map(|child| self.estimate_selectivity(child, column_stats))
                    .product(),
                ConjunctionType::Or => {
                    let independent_miss = conjunction
                        .children
                        .iter()
                        .map(|child| 1.0 - self.estimate_selectivity(child, column_stats))
                        .product::<f64>();
                    1.0 - independent_miss
                }
            },
            Expression::Operator(operator) => match operator.operator_type {
                OperatorType::Like | OperatorType::ILike => {
                    match like_pattern_shape(operator.children.get(1)) {
                        LikePatternShape::MatchAll => 1.0,
                        LikePatternShape::Exact => self.defaults.equality,
                        LikePatternShape::Prefix => self.defaults.like_prefix,
                        LikePatternShape::Contains | LikePatternShape::Suffix => {
                            self.defaults.like_contains
                        }
                        LikePatternShape::Generic => self.defaults.predicate,
                    }
                }
                OperatorType::Not => operator
                    .children
                    .first()
                    .map(|child| 1.0 - self.estimate_selectivity(child, column_stats))
                    .unwrap_or(self.defaults.predicate),
                OperatorType::IsNull => 1.0 - self.defaults.is_not_null,
                OperatorType::IsNotNull => self.defaults.is_not_null,
                OperatorType::In => {
                    self.estimate_in_selectivity(operator.children.as_slice(), column_stats)
                }
                OperatorType::NotIn => {
                    1.0 - self.estimate_in_selectivity(operator.children.as_slice(), column_stats)
                }
                _ => self.defaults.predicate,
            },
            Expression::Function(function) => match function.builtin_intrinsic() {
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
            },
            _ => self.defaults.predicate,
        };
        clamp_selectivity(selectivity)
    }

    pub fn estimate_filter_cardinality(
        &self,
        base_cardinality: u64,
        expressions: &[Expression],
        column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    ) -> CardinalityEstimate {
        if base_cardinality == 0 {
            return CardinalityEstimate::exact(0);
        }
        if expressions.is_empty() {
            return CardinalityEstimate::exact(base_cardinality);
        }

        let combined_selectivity = expressions
            .iter()
            .map(|expr| self.estimate_selectivity(expr, column_stats))
            .product::<f64>()
            .clamp(0.0, 1.0);

        let expected = ((base_cardinality as f64) * combined_selectivity).round() as u64;
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
        column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
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

        let Some(column_ref) = column_ref_with_constant(&expr.left, &expr.right)
            .or_else(|| column_ref_with_constant(&expr.right, &expr.left))
        else {
            return default;
        };

        let Some(stats) = column_stats.get(&column_ref.binding) else {
            return default;
        };

        let distinct = stats.get_distinct_count();
        if distinct == 0 {
            return default;
        }

        match expr.comparison_type {
            ComparisonType::Equal | ComparisonType::NotDistinctFrom => {
                (1.0 / distinct as f64).max(MIN_SELECTIVITY)
            }
            ComparisonType::NotEqual | ComparisonType::DistinctFrom => {
                (1.0 - (1.0 / distinct as f64)).clamp(MIN_SELECTIVITY, 1.0)
            }
            ComparisonType::LessThan
            | ComparisonType::LessThanOrEqual
            | ComparisonType::GreaterThan
            | ComparisonType::GreaterThanOrEqual => self.defaults.range,
        }
    }

    fn estimate_in_selectivity(
        &self,
        children: &[Expression],
        column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    ) -> f64 {
        let Some(Expression::ColumnRef(column_ref)) = children.first() else {
            return self.defaults.predicate;
        };

        let probe_count = children.len().saturating_sub(1).max(1) as f64;
        let Some(stats) = column_stats.get(&column_ref.binding) else {
            return clamp_selectivity(self.defaults.equality * probe_count);
        };
        let distinct = stats.get_distinct_count();
        if distinct == 0 {
            return clamp_selectivity(self.defaults.equality * probe_count);
        }
        clamp_selectivity((probe_count / distinct as f64).max(MIN_SELECTIVITY))
    }
}

fn clamp_selectivity(value: f64) -> f64 {
    if value.is_finite() {
        value.clamp(0.0, 1.0)
    } else {
        1.0
    }
}

fn column_ref_with_constant<'a>(
    candidate: &'a Expression,
    other: &'a Expression,
) -> Option<&'a ColumnRefExpression> {
    match (candidate, other) {
        (Expression::ColumnRef(column_ref), Expression::Constant(_)) => Some(column_ref),
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
    use paro_planner::expression::{ComparisonExpression, ConstantExpression, OperatorExpression};

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
    }
}
