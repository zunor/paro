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
use paro_planner::expression::{AggregateType, ComparisonType, ConstantExpression, Expression};
use paro_planner::operator::{ColumnBinding, Filter, LogicalOperator};
use paro_storage::statistics::{ColumnStatistics, NumericStats};

pub(crate) fn estimate_grouped_sum_filter_selectivity(
    filter: &Filter,
    column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
) -> Option<f64> {
    let [Expression::Comparison(comparison)] = filter.expressions.as_slice() else {
        return None;
    };
    let (output, constant, comparison_type) = normalized_comparison(
        comparison.left.as_ref(),
        comparison.right.as_ref(),
        comparison.comparison_type,
    )?;
    let LogicalOperator::Aggregate(aggregate) = &filter.child.operator else {
        return None;
    };
    let aggregate_index = match output {
        Expression::ColumnRef(column)
            if column.depth == 0 && column.binding.table_index == aggregate.aggregate_index =>
        {
            column.binding.column_index
        }
        Expression::Reference(reference)
            if reference.index >= aggregate.groups.len()
                && reference.index < aggregate.groups.len() + aggregate.aggregates.len() =>
        {
            reference.index - aggregate.groups.len()
        }
        _ => return None,
    };
    let Expression::Aggregate(sum) = aggregate.aggregates.get(aggregate_index)? else {
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
        Expression::Reference(reference) => {
            *aggregate.child.get_column_bindings().get(reference.index)?
        }
        _ => return None,
    };
    let stats = column_stats.get(&input_binding)?;
    let (minimum, maximum) = NumericStats::guaranteed_bounds(stats.statistics())?;
    let input_type = input.return_type();
    let minimum = numeric_value(&minimum, &input_type)?;
    let maximum = numeric_value(&maximum, &input_type)?;
    let constant = numeric_value(&constant.value, &constant.return_type)?;
    if !minimum.is_finite() || !maximum.is_finite() || !constant.is_finite() || minimum > maximum {
        return None;
    }

    let input_rows = aggregate.child.stats.estimated_cardinality?.expected.max(1) as f64;
    let group_rows = filter.child.stats.estimated_cardinality?.expected.max(1) as f64;
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
    normal_comparison_selectivity(sum_mean, sum_variance, constant, comparison_type)
}

fn normal_comparison_selectivity(
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

fn normalized_comparison<'a>(
    left: &'a Expression,
    right: &'a Expression,
    comparison_type: ComparisonType,
) -> Option<(&'a Expression, &'a ConstantExpression, ComparisonType)> {
    match (left, right) {
        (
            output @ (Expression::ColumnRef(_) | Expression::Reference(_)),
            Expression::Constant(constant),
        ) => Some((output, constant, comparison_type)),
        (
            Expression::Constant(constant),
            output @ (Expression::ColumnRef(_) | Expression::Reference(_)),
        ) => Some((output, constant, flip_comparison(comparison_type))),
        _ => None,
    }
}

fn flip_comparison(comparison: ComparisonType) -> ComparisonType {
    match comparison {
        ComparisonType::Equal => ComparisonType::Equal,
        ComparisonType::NotEqual => ComparisonType::NotEqual,
        ComparisonType::LessThan => ComparisonType::GreaterThan,
        ComparisonType::GreaterThan => ComparisonType::LessThan,
        ComparisonType::LessThanOrEqual => ComparisonType::GreaterThanOrEqual,
        ComparisonType::GreaterThanOrEqual => ComparisonType::LessThanOrEqual,
        ComparisonType::DistinctFrom => ComparisonType::DistinctFrom,
        ComparisonType::NotDistinctFrom => ComparisonType::NotDistinctFrom,
    }
}

fn numeric_value(value: &Value, logical_type: &LogicalType) -> Option<f64> {
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
    use paro_planner::plan::{CardinalityEstimate, LogicalPlan};
    use paro_storage::statistics::{BaseStatistics, ColumnStatistics, NumericStats};

    use super::{
        estimate_grouped_sum_filter_selectivity, normal_cdf, normal_comparison_selectivity,
    };

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
        let mut input = LogicalPlan::new(
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
        let sum = Expression::Aggregate(AggregateExpression::new(
            function,
            vec![Expression::Reference(ReferenceExpression::new(
                1,
                input_type.clone(),
            ))],
            output_type.clone(),
        ));
        let mut aggregate = LogicalPlan::new(
            &ctx,
            LogicalOperator::Aggregate(Aggregate::new(
                20,
                21,
                22,
                input,
                vec![Expression::Reference(ReferenceExpression::new(
                    0,
                    LogicalType::BigInt,
                ))],
                Vec::new(),
                vec![sum],
                Vec::new(),
            )),
        );
        aggregate.stats.estimated_cardinality = Some(CardinalityEstimate::exact(1_500_000));

        let output = Expression::Reference(ReferenceExpression::new(1, output_type.clone()));
        let constant = Expression::Constant(ConstantExpression::new(
            Value::Decimal(30_000, 38, 2),
            output_type,
        ));
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
            Filter::new(aggregate, vec![Expression::Comparison(comparison)]),
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
            let selectivity = estimate_grouped_sum_filter_selectivity(&filter, &stats).unwrap();
            assert!(
                selectivity < 0.001,
                "expected a selective upper tail, got {selectivity}"
            );
            assert!(selectivity > 0.0);
        }
    }
}
