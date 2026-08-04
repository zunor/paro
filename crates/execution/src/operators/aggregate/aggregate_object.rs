// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Aggregate execution metadata.

use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_function::aggregate::{AggregateFunction, FunctionData};
use paro_planner::expression::{AggregateExpression, AggregateType, Expression};

use super::grouped_aggregate_data::{reference_index, GroupedAggregateData};

const MIN_STATE_ALIGNMENT: usize = 8;

/// Execution-time metadata for a single aggregate.
#[derive(Debug, Clone)]
pub struct AggregateObject {
    /// Aggregate function implementation.
    pub function: AggregateFunction,
    /// Optional bind-time data.
    pub bind_info: Option<Arc<dyn FunctionData>>,
    /// Number of aggregate input columns.
    pub child_count: usize,
    /// Aligned state payload size.
    pub payload_size: usize,
    /// DISTINCT or non-DISTINCT aggregate.
    pub aggr_type: AggregateType,
    /// Aggregate return type.
    pub return_type: LogicalType,
    /// Optional FILTER payload reference.
    pub filter: Option<usize>,
    /// Ordered aggregate payload references.
    pub order_bys: Vec<usize>,
}

impl AggregateObject {
    /// Build aggregate execution metadata from a bound aggregate expression.
    pub fn from_bound(expr: &AggregateExpression) -> Result<Self> {
        let payload_size = align_to(expr.function.state_size, MIN_STATE_ALIGNMENT)?;
        let filter = expr
            .filter
            .as_ref()
            .map(|filter_expr| reference_index(filter_expr))
            .transpose()?;
        let mut order_bys = Vec::with_capacity(expr.order_bys.len());
        for order in &expr.order_bys {
            order_bys.push(reference_index(&order.expression)?);
        }

        Ok(Self {
            function: expr.function.clone(),
            bind_info: expr.bind_info.clone(),
            child_count: expr.children.len(),
            payload_size,
            aggr_type: expr.aggr_type,
            return_type: expr.return_type.clone(),
            filter,
            order_bys,
        })
    }

    pub fn is_distinct(&self) -> bool {
        self.aggr_type == AggregateType::Distinct
    }

    pub fn validate_with_plan(&self, plan: &GroupedAggregateData, agg_idx: usize) -> Result<()> {
        let plan_inputs = plan.aggregate_inputs.get(agg_idx).ok_or_else(|| {
            paro_error::internal(format!("aggregate_inputs index out of bounds: {agg_idx}"))
        })?;
        if plan_inputs.len() != self.child_count {
            return Err(paro_error::internal(format!(
                "Aggregate child_count mismatch at index {agg_idx}: object={} plan={}",
                self.child_count,
                plan_inputs.len()
            )));
        }

        let plan_filter = plan.aggregate_filters.get(agg_idx).ok_or_else(|| {
            paro_error::internal(format!("aggregate_filters index out of bounds: {agg_idx}"))
        })?;
        if *plan_filter != self.filter {
            return Err(paro_error::internal(format!(
                "Aggregate filter mismatch at index {agg_idx}: object={:?} plan={:?}",
                self.filter, plan_filter
            )));
        }

        let plan_orders = plan.aggregate_orders.get(agg_idx).ok_or_else(|| {
            paro_error::internal(format!("aggregate_orders index out of bounds: {agg_idx}"))
        })?;
        if *plan_orders != self.order_bys {
            return Err(paro_error::internal(format!(
                "Aggregate order refs mismatch at index {agg_idx}: object={:?} plan={:?}",
                self.order_bys, plan_orders
            )));
        }
        Ok(())
    }
}

pub fn create_aggregate_objects(aggregates: &[Expression]) -> Result<Vec<AggregateObject>> {
    let mut objects = Vec::with_capacity(aggregates.len());
    for (idx, aggregate) in aggregates.iter().enumerate() {
        let bound = match aggregate {
            Expression::Aggregate(bound) => bound,
            _ => {
                return Err(paro_error::internal(format!(
                    "Expected AggregateExpression at index {idx}, found {aggregate:?}"
                )));
            }
        };
        objects.push(AggregateObject::from_bound(bound)?);
    }
    Ok(objects)
}

pub fn create_validated_aggregate_objects(
    aggregate_data: &GroupedAggregateData,
) -> Result<Vec<AggregateObject>> {
    let aggregate_count = aggregate_data.aggregate_count();
    let expected_sizes = [
        ("aggregate_inputs", aggregate_data.aggregate_inputs.len()),
        ("aggregate_filters", aggregate_data.aggregate_filters.len()),
        ("aggregate_orders", aggregate_data.aggregate_orders.len()),
    ];
    for (name, size) in expected_sizes {
        if size != aggregate_count {
            return Err(paro_error::internal(format!(
                "GroupedAggregateData {name} length mismatch: expected {aggregate_count}, found {size}"
            )));
        }
    }

    let objects = create_aggregate_objects(&aggregate_data.aggregates)?;
    for (idx, object) in objects.iter().enumerate() {
        object.validate_with_plan(aggregate_data, idx)?;
    }
    Ok(objects)
}

fn align_to(value: usize, alignment: usize) -> Result<usize> {
    debug_assert!(alignment.is_power_of_two());
    let addend = alignment - 1;
    value
        .checked_add(addend)
        .map(|aligned| aligned & !addend)
        .ok_or_else(|| {
            paro_error::internal(format!(
                "Failed to align aggregate state size {value} to {alignment}"
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_function::aggregate::AggregateInputData;
    use paro_planner::expression::{OrderByExpression, ReferenceExpression};

    unsafe fn initialize(_state: *mut u8) {}

    unsafe fn update(
        _inputs: &[&paro_common::vector::Vector],
        _input_data: &AggregateInputData,
        _states: &paro_common::vector::Vector,
        _count: usize,
    ) {
    }

    unsafe fn combine(
        _source: &paro_common::vector::Vector,
        _target: &paro_common::vector::Vector,
        _input_data: &AggregateInputData,
        _count: usize,
    ) {
    }

    unsafe fn finalize(
        _states: &paro_common::vector::Vector,
        _input_data: &AggregateInputData,
        _result: &mut paro_common::vector::Vector,
        _count: usize,
    ) -> paro_common::error::Result<()> {
        Ok(())
    }

    fn make_test_function(state_size: usize) -> AggregateFunction {
        AggregateFunction::new(
            "test_sum".to_string(),
            vec![LogicalType::Integer],
            LogicalType::BigInt,
            state_size,
            initialize,
            update,
            combine,
            finalize,
            None,
            None,
        )
    }

    fn make_ref(index: usize, ty: LogicalType) -> Expression {
        Expression::Reference(ReferenceExpression::new(index, ty))
    }

    #[test]
    fn aggregate_object_from_bound_extracts_refs() {
        let aggregate = AggregateExpression::new(
            make_test_function(9),
            vec![make_ref(0, LogicalType::Integer)],
            LogicalType::BigInt,
        )
        .with_aggr_type(AggregateType::Distinct)
        .with_filter(Some(make_ref(1, LogicalType::Boolean)))
        .with_order_bys(vec![OrderByExpression {
            expression: make_ref(2, LogicalType::Integer),
            ascending: true,
            nulls_first: false,
        }]);

        let object = AggregateObject::from_bound(&aggregate).expect("build aggregate object");
        assert_eq!(object.child_count, 1);
        assert_eq!(object.payload_size, 16);
        assert_eq!(object.filter, Some(1));
        assert_eq!(object.order_bys, vec![2]);
        assert!(object.is_distinct());
    }

    #[test]
    fn create_aggregate_objects_rejects_non_aggregate() {
        let result = create_aggregate_objects(&[make_ref(0, LogicalType::Integer)]);
        assert!(result.is_err());
    }

    #[test]
    fn create_validated_aggregate_objects_checks_plan_alignment() {
        let aggregate = AggregateExpression::new(
            make_test_function(8),
            vec![make_ref(0, LogicalType::Integer)],
            LogicalType::BigInt,
        )
        .with_filter(Some(make_ref(1, LogicalType::Boolean)))
        .with_order_bys(vec![OrderByExpression {
            expression: make_ref(2, LogicalType::Integer),
            ascending: true,
            nulls_first: false,
        }]);

        let aggregate_data = GroupedAggregateData {
            aggregates: vec![Expression::Aggregate(aggregate)],
            aggregate_inputs: vec![vec![0]],
            aggregate_filters: vec![Some(1)],
            aggregate_orders: vec![vec![2]],
            ..Default::default()
        };
        let objects = create_validated_aggregate_objects(&aggregate_data).expect("valid objects");
        assert_eq!(objects.len(), 1);

        let mismatch_data = GroupedAggregateData {
            aggregate_inputs: vec![vec![0, 3]],
            ..aggregate_data
        };
        let mismatch = create_validated_aggregate_objects(&mismatch_data);
        assert!(mismatch.is_err());
    }
}
