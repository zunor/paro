// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Selectivity estimation for predicates over grouped aggregate results.
//!
//! Ordinary column bounds do not describe a grouped `SUM`: its distribution
//! also depends on the expected number of input rows per group. Treating such
//! a predicate as an unknown range comparison is especially harmful around
//! joins because a highly selective reduction can look larger than a base
//! table. This module derives a compound-distribution estimate from exact
//! input bounds and the input/group cardinalities. The estimate is used only
//! for costing; execution never relies on it as a correctness bound.

use std::collections::HashMap;
use std::sync::Arc;

use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::aggregate::AggregateAlgebra;
use paro_planner::expression::{AggregateType, ComparisonType, Expression};
use paro_planner::operator::ColumnBinding;
use paro_storage::statistics::{ColumnStatistics, EstimatedNumericDistribution, NumericStats};

/// Derive the aggregate output once, at its producer. Consumers read these
/// moments as column evidence, including through a Memo group hole or a pure
/// renaming projection; they never reconstruct an Aggregate child to discover
/// the distribution. The model and its assumptions are unchanged.
pub(crate) fn estimate_grouped_sum_distribution(
    expression: &Expression,
    input_rows: u64,
    group_rows: u64,
    input_bindings: &[ColumnBinding],
    column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
) -> Option<EstimatedNumericDistribution> {
    let Expression::Aggregate(sum) = expression else {
        return None;
    };
    if sum.function.algebra != Some(AggregateAlgebra::Sum)
        || sum.aggr_type != AggregateType::NonDistinct
        || sum.filter.is_some()
        || !sum.order_bys.is_empty()
    {
        return None;
    }
    let [input] = sum.children.as_slice() else {
        return None;
    };
    let input_binding = match input {
        Expression::ColumnRef(column) if column.depth == 0 => column.binding,
        Expression::Reference(reference) => *input_bindings.get(reference.index)?,
        _ => return None,
    };
    let stats = column_stats.get(&input_binding)?;
    let (minimum, maximum) = NumericStats::guaranteed_bounds(stats.statistics())?;
    let input_type = input.return_type();
    let minimum = numeric_value(&minimum, &input_type)?;
    let maximum = numeric_value(&maximum, &input_type)?;
    if !minimum.is_finite() || !maximum.is_finite() || minimum > maximum {
        return None;
    }

    let input_rows = input_rows.max(1) as f64;
    let group_rows = group_rows.max(1) as f64;
    let rows_per_group = (input_rows / group_rows).max(1.0);

    // Model every non-empty group as one mandatory observation plus a
    // Poisson-distributed suffix. With only exact bounds available, use the
    // maximum-entropy uniform distribution for each input value. This keeps
    // the estimator data-derived without pretending its bounds are exact.
    let input_mean = (minimum + maximum) * 0.5;
    let input_variance = (maximum - minimum).powi(2) / 12.0;
    let sum_mean = rows_per_group * input_mean;
    let sum_variance =
        rows_per_group * input_variance + (rows_per_group - 1.0).max(0.0) * input_mean.powi(2);
    EstimatedNumericDistribution::normal(sum_mean, sum_variance)
}

pub(crate) fn normal_comparison_selectivity(
    mean: f64,
    variance: f64,
    constant: f64,
    comparison: ComparisonType,
) -> Option<f64> {
    if variance <= f64::EPSILON {
        return Some(match comparison {
            ComparisonType::LessThan => (mean < constant) as u8 as f64,
            ComparisonType::LessThanOrEqual => (mean <= constant) as u8 as f64,
            ComparisonType::GreaterThan => (mean > constant) as u8 as f64,
            ComparisonType::GreaterThanOrEqual => (mean >= constant) as u8 as f64,
            _ => return None,
        });
    }
    let below = normal_cdf((constant - mean) / variance.sqrt());
    Some(match comparison {
        ComparisonType::LessThan | ComparisonType::LessThanOrEqual => below,
        ComparisonType::GreaterThan | ComparisonType::GreaterThanOrEqual => 1.0 - below,
        _ => return None,
    })
}

pub(crate) fn numeric_value(value: &Value, logical_type: &LogicalType) -> Option<f64> {
    let raw = match value {
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
        Value::Float(value) => Some(*value as f64),
        Value::Double(value) => Some(*value),
        Value::Decimal(value, _, _) => Some(*value as f64),
        _ => None,
    }?;
    Some(match logical_type {
        LogicalType::Decimal { scale, .. } => raw / 10_f64.powi(i32::from(*scale)),
        _ => raw,
    })
}

/// Standard normal CDF using the Abramowitz-Stegun 7.1.26 approximation.
fn normal_cdf(value: f64) -> f64 {
    let absolute = value.abs();
    let t = 1.0 / (1.0 + 0.231_641_9 * absolute);
    let density = 0.398_942_280_401_432_7 * (-0.5 * absolute * absolute).exp();
    let tail = density
        * t
        * (0.319_381_530
            + t * (-0.356_563_782
                + t * (1.781_477_937 + t * (-1.821_255_978 + t * 1.330_274_429))));
    if value >= 0.0 {
        1.0 - tail
    } else {
        tail
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_function::aggregate::distributive::sum::get_sum_function;
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::{
        AggregateExpression, ComparisonExpression, ComparisonType, ConstantExpression, Expression,
        ReferenceExpression,
    };
    use paro_planner::operator::{
        Aggregate, ColumnBinding, ExpressionGet, Filter, LogicalOperator,
    };
    use paro_planner::plan::{CardinalityEstimate, OwnedLogicalPlan};
    use paro_storage::statistics::{BaseStatistics, ColumnStatistics, NumericStats};

    use super::{estimate_grouped_sum_distribution, normal_cdf, normal_comparison_selectivity};

    fn decimal(precision: u8) -> LogicalType {
        LogicalType::Decimal {
            precision,
            scale: 2,
        }
    }

    fn q18_sum_filter(
        constant_on_left: bool,
    ) -> (Filter, HashMap<ColumnBinding, Arc<ColumnStatistics>>) {
        let ctx = BindContext::new();
        let input_type = decimal(15);
        let output_type = decimal(38);
        let mut input = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                10,
                Vec::new(),
                vec!["orderkey".to_string(), "quantity".to_string()],
                vec![LogicalType::BigInt, input_type.clone()],
            )),
        );
        input.stats.estimated_cardinality = Some(CardinalityEstimate::exact(6_000_000));
        let (function, targets) = get_sum_function()
            .bind(std::slice::from_ref(&input_type))
            .unwrap();
        assert_eq!(targets, [input_type.clone()]);
        let sum = Expression::Aggregate(
            AggregateExpression::new(
                function,
                vec![Expression::Reference(
                    ReferenceExpression::new(1, input_type.clone()).into(),
                )],
                output_type.clone(),
            )
            .into(),
        );
        let mut aggregate = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Aggregate(Box::new(Aggregate::new(
                20,
                21,
                22,
                input,
                vec![Expression::Reference(
                    ReferenceExpression::new(0, LogicalType::BigInt).into(),
                )],
                Vec::new(),
                vec![sum],
                Vec::new(),
            ))),
        );
        aggregate.stats.estimated_cardinality = Some(CardinalityEstimate::exact(1_500_000));

        let output = Expression::Reference(ReferenceExpression::new(1, output_type.clone()).into());
        let constant = Expression::Constant(
            ConstantExpression::new(Value::Decimal(30_000, 38, 2), output_type).into(),
        );
        let comparison = if constant_on_left {
            ComparisonExpression::new(ComparisonType::LessThan, constant, output)
        } else {
            ComparisonExpression::new(ComparisonType::GreaterThan, output, constant)
        };

        let mut base = BaseStatistics::create_empty(input_type);
        NumericStats::update(&mut base, &Value::Decimal(100, 15, 2));
        NumericStats::update(&mut base, &Value::Decimal(5_000, 15, 2));
        let stats = HashMap::from([(
            ColumnBinding::new(10, 1),
            Arc::new(ColumnStatistics::new(base)),
        )]);
        (
            Filter::new(aggregate, vec![Expression::Comparison(comparison.into())]),
            stats,
        )
    }

    #[test]
    fn normal_cdf_is_symmetric_and_monotonic() {
        assert!((normal_cdf(0.0) - 0.5).abs() < 1e-6);
        assert!((normal_cdf(-2.0) - (1.0 - normal_cdf(2.0))).abs() < 1e-6);
        assert!(normal_cdf(3.0) > normal_cdf(2.0));
    }

    #[test]
    fn degenerate_sum_distribution_preserves_comparison_boundaries() {
        let at_boundary =
            |comparison| normal_comparison_selectivity(300.0, 0.0, 300.0, comparison).unwrap();
        assert_eq!(at_boundary(ComparisonType::LessThan), 0.0);
        assert_eq!(at_boundary(ComparisonType::LessThanOrEqual), 1.0);
        assert_eq!(at_boundary(ComparisonType::GreaterThan), 0.0);
        assert_eq!(at_boundary(ComparisonType::GreaterThanOrEqual), 1.0);
    }

    #[test]
    fn grouped_decimal_sum_uses_logical_scale_and_reference_lineage() {
        for constant_on_left in [false, true] {
            let (filter, stats) = q18_sum_filter(constant_on_left);
            let LogicalOperator::Aggregate(aggregate) = &filter.child.operator else {
                unreachable!()
            };
            let distribution = estimate_grouped_sum_distribution(
                &aggregate.aggregates[0],
                6_000_000,
                1_500_000,
                &aggregate.child.get_column_bindings(),
                &stats,
            )
            .unwrap();
            assert_eq!(
                distribution.mean(),
                102.0,
                "decimal moments use logical units"
            );
            let output = Arc::new(
                ColumnStatistics::with_estimated_distinct(
                    BaseStatistics::create_unknown(decimal(38)),
                    None,
                )
                .with_estimated_numeric_distribution(Some(distribution)),
            );
            let estimate = crate::estimate::selectivity::SelectivityModel::default()
                .estimate_filter_cardinality_with_positions(
                    1_500_000,
                    &filter.expressions,
                    &HashMap::from([(ColumnBinding::new(21, 0), output)]),
                    &aggregate.get_column_bindings(),
                );
            assert!(
                estimate.expected < 1_500,
                "expected a selective upper tail: {estimate:?}"
            );
            assert!(estimate.expected > 1);
        }
    }
}
