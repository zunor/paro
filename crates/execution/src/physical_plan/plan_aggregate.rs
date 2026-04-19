// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical Plan Generation for Aggregate
//!
//! Maps Aggregate to either HashAggregate or UngroupedAggregate.

use super::generator::PhysicalPlanGenerator;
use crate::operator::aggregate::grouped_aggregate_data::{reference_index, GroupedAggregateData};
use crate::operator::aggregate::hash_aggregate::HashAggregate;
use crate::operator::aggregate::perfect_hash_aggregate::PerfectHashAggregate;
use crate::operator::aggregate::ungrouped_aggregate::UngroupedAggregate;
use crate::operator::projection::Projection;
use crate::operator::PhysicalOperator;
use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::expression::{Expression, ReferenceExpression};
use paro_planner::operator::aggregate::Aggregate;
use paro_storage::statistics::BaseStatistics;
use std::sync::Arc;

const PERFECT_HASH_RANGE_LIMIT: u128 = 1u128 << 32;
const PERFECT_HASH_MAX_BITS: usize = 20;

#[derive(Debug, Clone)]
struct PerfectHashPlanInfo {
    group_minima: Vec<i128>,
    required_bits: Vec<usize>,
}

impl PhysicalPlanGenerator {
    pub fn create_plan_aggregate(
        &self,
        op: &Aggregate,
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let (aggregate_data, child) = self.extract_aggregate_expressions(op, child)?;

        if aggregate_data.groups.is_empty() && op.grouping_sets.len() <= 1 {
            // Ungrouped aggregate
            Ok(Arc::new(UngroupedAggregate::new(
                aggregate_data,
                op.returned_types.clone(),
                child,
            )?))
        } else if let Some(perfect_hash_info) = can_use_perfect_hash_aggregate(op, &aggregate_data)
        {
            Ok(Arc::new(PerfectHashAggregate::new(
                aggregate_data,
                op.returned_types.clone(),
                child,
                perfect_hash_info.group_minima,
                perfect_hash_info.required_bits,
            )?))
        } else {
            // Grouped aggregate fallback.
            Ok(Arc::new(HashAggregate::new(
                aggregate_data,
                op.returned_types.clone(),
                child,
            )?))
        }
    }

    fn extract_aggregate_expressions(
        &self,
        op: &Aggregate,
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<(GroupedAggregateData, Arc<dyn PhysicalOperator>)> {
        let mut projection_exprs = Vec::new();
        let mut payload_types = Vec::new();

        let groups = op
            .groups
            .iter()
            .cloned()
            .map(|expr| extract_payload_expression(expr, &mut projection_exprs, &mut payload_types))
            .collect();

        let mut aggregates = Vec::with_capacity(op.aggregates.len());
        let mut aggregate_inputs = Vec::with_capacity(op.aggregates.len());
        let mut aggregate_filters = Vec::with_capacity(op.aggregates.len());
        let mut aggregate_orders = Vec::with_capacity(op.aggregates.len());

        for aggregate in op.aggregates.iter().cloned() {
            let Expression::Aggregate(mut aggregate) = aggregate else {
                return Err(paro_common::error::internal(
                    "Expected AggregateExpression in Aggregate".to_string(),
                ));
            };

            let mut input_mapping = Vec::with_capacity(aggregate.children.len());
            let mut extracted_children = Vec::with_capacity(aggregate.children.len());
            for child in std::mem::take(&mut aggregate.children) {
                let extracted =
                    extract_payload_expression(child, &mut projection_exprs, &mut payload_types);
                input_mapping.push(reference_index(&extracted)?);
                extracted_children.push(extracted);
            }
            aggregate.children = extracted_children;

            let filter_mapping = if let Some(filter) = aggregate.filter.take() {
                let extracted =
                    extract_payload_expression(*filter, &mut projection_exprs, &mut payload_types);
                let filter_index = reference_index(&extracted)?;
                aggregate.filter = Some(Box::new(extracted));
                Some(filter_index)
            } else {
                None
            };

            let mut order_mapping = Vec::with_capacity(aggregate.order_bys.len());
            let mut extracted_orders = Vec::with_capacity(aggregate.order_bys.len());
            for mut order in std::mem::take(&mut aggregate.order_bys) {
                let extracted = extract_payload_expression(
                    order.expression,
                    &mut projection_exprs,
                    &mut payload_types,
                );
                order_mapping.push(reference_index(&extracted)?);
                order.expression = extracted;
                extracted_orders.push(order);
            }
            aggregate.order_bys = extracted_orders;

            aggregate_inputs.push(input_mapping);
            aggregate_filters.push(filter_mapping);
            aggregate_orders.push(order_mapping);
            aggregates.push(Expression::Aggregate(aggregate));
        }

        let aggregate_data = GroupedAggregateData {
            projection_exprs,
            payload_types,
            groups,
            grouping_sets: op
                .grouping_sets
                .iter()
                .map(|set| set.expressions.clone())
                .collect(),
            aggregates,
            grouping_functions: op.grouping_functions.clone(),
            aggregate_inputs,
            aggregate_filters,
            aggregate_orders,
        };

        let child = if aggregate_data.has_projection() {
            let projection: Arc<dyn PhysicalOperator> = Arc::new(Projection::new(
                aggregate_data.projection_exprs.clone(),
                child,
            ));
            self.annotate_schema(
                projection.clone(),
                self.passthrough_schema(&projection, Vec::new()),
            )
        } else {
            child
        };

        Ok((aggregate_data, child))
    }
}

fn extract_payload_expression(
    expr: Expression,
    projection_exprs: &mut Vec<Expression>,
    payload_types: &mut Vec<paro_common::types::LogicalType>,
) -> Expression {
    let return_type = expr.return_type();
    let reference_index = projection_exprs.len();
    payload_types.push(return_type.clone());
    projection_exprs.push(expr);
    Expression::Reference(ReferenceExpression::new(reference_index, return_type))
}

fn can_use_perfect_hash_aggregate(
    op: &Aggregate,
    aggregate_data: &GroupedAggregateData,
) -> Option<PerfectHashPlanInfo> {
    if aggregate_data.groups.is_empty()
        || op.grouping_sets.len() > 1
        || !op.grouping_functions.is_empty()
    {
        return None;
    }
    if op.groups.len() != aggregate_data.groups.len() {
        return None;
    }

    for aggregate in &aggregate_data.aggregates {
        let Expression::Aggregate(aggregate) = aggregate else {
            return None;
        };
        if aggregate.is_distinct() || !aggregate.order_bys.is_empty() {
            return None;
        }
    }

    let mut total_bits = 0usize;
    let mut group_minima = Vec::with_capacity(op.groups.len());
    let mut required_bits = Vec::with_capacity(op.groups.len());

    for group_idx in 0..op.groups.len() {
        let group_type = op.groups[group_idx].return_type();
        if !group_type.is_integer() {
            return None;
        }

        let min_max = op
            .group_stats
            .get(group_idx)
            .and_then(|stats| stats.as_ref())
            .and_then(integer_min_max_from_stats)
            .or_else(|| integer_type_bounds(&group_type));
        let (min_value, max_value) = min_max?;
        let range = max_value.checked_sub(min_value)?;
        let range_u128 = u128::try_from(range).ok()?;
        if range_u128 >= PERFECT_HASH_RANGE_LIMIT {
            return None;
        }
        let bits = required_bits_for_value(range_u128.checked_add(2)?)?;
        total_bits = total_bits.checked_add(bits)?;
        if total_bits > PERFECT_HASH_MAX_BITS {
            return None;
        }
        group_minima.push(min_value);
        required_bits.push(bits);
    }

    Some(PerfectHashPlanInfo {
        group_minima,
        required_bits,
    })
}

fn integer_min_max_from_stats(stats: &BaseStatistics) -> Option<(i128, i128)> {
    let min = stats.min_value().and_then(|value| value_to_i128(&value))?;
    let max = stats.max_value().and_then(|value| value_to_i128(&value))?;
    Some((min, max))
}

fn value_to_i128(value: &Value) -> Option<i128> {
    match value {
        Value::TinyInt(v) => Some(*v as i128),
        Value::SmallInt(v) => Some(*v as i128),
        Value::Integer(v) => Some(*v as i128),
        Value::BigInt(v) => Some(*v as i128),
        Value::HugeInt(v) => Some(*v),
        Value::UTinyInt(v) => Some(*v as i128),
        Value::USmallInt(v) => Some(*v as i128),
        Value::UInteger(v) => Some(*v as i128),
        Value::UBigInt(v) => Some(*v as i128),
        Value::UHugeInt(v) => i128::try_from(*v).ok(),
        _ => None,
    }
}

fn integer_type_bounds(ty: &LogicalType) -> Option<(i128, i128)> {
    match ty {
        LogicalType::TinyInt => Some((i8::MIN as i128, i8::MAX as i128)),
        LogicalType::SmallInt => Some((i16::MIN as i128, i16::MAX as i128)),
        LogicalType::Integer => Some((i32::MIN as i128, i32::MAX as i128)),
        LogicalType::BigInt => Some((i64::MIN as i128, i64::MAX as i128)),
        LogicalType::UTinyInt => Some((0, u8::MAX as i128)),
        LogicalType::USmallInt => Some((0, u16::MAX as i128)),
        LogicalType::UInteger => Some((0, u32::MAX as i128)),
        LogicalType::UBigInt => Some((0, u64::MAX as i128)),
        _ => None,
    }
}

fn required_bits_for_value(mut value: u128) -> Option<usize> {
    let mut bits = 0usize;
    while value > 0 {
        bits = bits.checked_add(1)?;
        value >>= 1;
    }
    Some(bits)
}
