// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical Plan Generation for DISTINCT
//!
//!
//! ## Dependencies Check
//! - HashAggregate: ✅
//! - Projection: ✅
//! - FIRST aggregate function: ✅
//!
//! ## Implementation Notes
//! - DISTINCT is implemented using GROUP BY (HashAggregate)
//! - For regular DISTINCT: all columns become group keys
//! - For DISTINCT ON: ON columns become group keys, other columns use FIRST aggregate
//! - A projection may be added to reorder columns to match original output

use super::generator::PhysicalPlanGenerator;
use crate::operator::aggregate::grouped_aggregate_data::{reference_index, GroupedAggregateData};
use crate::operator::aggregate::hash_aggregate::HashAggregate;
use crate::operator::projection::Projection;
use crate::operator::PhysicalOperator;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_function::aggregate::distributive::first_last::get_first_function;
use paro_function::aggregate::AggregateFunction;
use paro_planner::expression::{AggregateExpression, Expression, ReferenceExpression};
use paro_planner::operator::distinct::{Distinct, DistinctType};
use std::collections::HashMap;
use std::sync::Arc;

impl PhysicalPlanGenerator {
    /// Create physical plan for Distinct.
    ///
    /// DISTINCT is implemented as a GROUP BY operation:
    /// - Regular DISTINCT: GROUP BY all columns
    /// - DISTINCT ON: GROUP BY ON columns, FIRST aggregate for other columns
    pub fn create_plan_distinct(
        &self,
        distinct: &Distinct,
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let child_types = child.types().to_vec();

        match distinct.distinct_type {
            DistinctType::Distinct => self.create_plan_regular_distinct(&child_types, child),
            DistinctType::DistinctOn => self.create_plan_distinct_on(distinct, &child_types, child),
        }
    }

    /// Create physical plan for regular DISTINCT.
    ///
    /// All columns become group keys, no aggregates needed.
    fn create_plan_regular_distinct(
        &self,
        child_types: &[LogicalType],
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        // For regular DISTINCT, all columns are group keys
        let groups: Vec<Expression> = child_types
            .iter()
            .enumerate()
            .map(|(i, t)| {
                Expression::Reference(ReferenceExpression {
                    index: i,
                    return_type: t.clone(),
                })
            })
            .collect();

        let aggregate_data = GroupedAggregateData {
            projection_exprs: Vec::new(),
            payload_types: child_types.to_vec(),
            groups,
            grouping_sets: Vec::new(),
            aggregates: Vec::new(),
            grouping_functions: Vec::new(),
            aggregate_inputs: Vec::new(),
            aggregate_filters: Vec::new(),
            aggregate_orders: Vec::new(),
        };

        let hash_agg = HashAggregate::new(aggregate_data, child_types.to_vec(), child)?;

        Ok(Arc::new(hash_agg))
    }

    /// Create physical plan for DISTINCT ON.
    ///
    /// ON columns become group keys, other columns use FIRST aggregate.
    fn create_plan_distinct_on(
        &self,
        distinct: &Distinct,
        child_types: &[LogicalType],
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let distinct_targets = &distinct.distinct_targets;

        // Build a map from child column index to group index
        // This helps us identify which columns are in the DISTINCT ON list
        let mut group_by_references: HashMap<usize, usize> = HashMap::new();

        // Create group expressions from distinct_targets
        let mut groups: Vec<Expression> = Vec::new();
        let mut aggregate_types: Vec<LogicalType> = Vec::new();

        for (i, target) in distinct_targets.iter().enumerate() {
            if let Expression::Reference(ref_expr) = target {
                group_by_references.insert(ref_expr.index, i);
            }
            aggregate_types.push(target.return_type());
            groups.push(target.clone());
        }

        let group_count = groups.len();

        // Determine if we need a projection to reorder columns
        let mut requires_projection = child_types.len() != group_count;

        // Build projections and aggregates for non-group columns
        let mut projections: Vec<Expression> = Vec::new();
        let mut aggregates = Vec::new();

        for (i, logical_type) in child_types.iter().enumerate() {
            // Check if this column is one of the group keys
            if let Some(&group_index) = group_by_references.get(&i) {
                // Column is a group key - reference it directly
                projections.push(Expression::Reference(ReferenceExpression {
                    index: group_index,
                    return_type: logical_type.clone(),
                }));

                if group_index != i {
                    // Column is out of order, need projection
                    requires_projection = true;
                }
            } else {
                // Column is not a group key - need FIRST aggregate
                let first_func = self.get_first_function_for_type(logical_type)?;

                // Add projection reference to aggregate result
                // Aggregate results come after group columns
                projections.push(Expression::Reference(ReferenceExpression {
                    index: group_count + aggregates.len(),
                    return_type: logical_type.clone(),
                }));

                aggregate_types.push(logical_type.clone());
                aggregates.push(Expression::Aggregate(AggregateExpression::new(
                    first_func,
                    vec![Expression::Reference(ReferenceExpression {
                        index: i,
                        return_type: logical_type.clone(),
                    })],
                    logical_type.clone(),
                )));
                requires_projection = true;
            }
        }

        let (aggregate_data, child) =
            self.extract_grouped_aggregate_data(groups, aggregates, child)?;
        let child_output_names = child
            .explain_schema()
            .map(|schema| schema.output_names.clone())
            .unwrap_or_default();
        let mut hash_output_names = Vec::with_capacity(aggregate_types.len());
        for target in distinct_targets {
            match target {
                Expression::Reference(reference) => hash_output_names.push(
                    child_output_names
                        .get(reference.index)
                        .cloned()
                        .unwrap_or_else(|| format!("col_{}", reference.index + 1)),
                ),
                _ => hash_output_names.push(format!("group_{}", hash_output_names.len() + 1)),
            }
        }
        for (idx, name) in child_output_names.iter().enumerate() {
            if !group_by_references.contains_key(&idx) {
                hash_output_names.push(name.clone());
            }
        }
        let hash_agg: Arc<dyn PhysicalOperator> =
            Arc::new(HashAggregate::new(aggregate_data, aggregate_types, child)?);
        let hash_agg = self.annotate_schema(
            hash_agg.clone(),
            self.passthrough_schema(&hash_agg, hash_output_names),
        );

        if !requires_projection {
            return Ok(hash_agg);
        }

        // Add projection to reorder columns
        let projection: Arc<dyn PhysicalOperator> =
            Arc::new(Projection::new(projections, hash_agg));

        Ok(projection)
    }

    fn extract_grouped_aggregate_data(
        &self,
        groups: Vec<Expression>,
        aggregates: Vec<Expression>,
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<(GroupedAggregateData, Arc<dyn PhysicalOperator>)> {
        let mut projection_exprs = Vec::new();
        let mut payload_types = Vec::new();

        let groups = groups
            .into_iter()
            .map(|expr| extract_distinct_payload(expr, &mut projection_exprs, &mut payload_types))
            .collect();

        let mut extracted_aggregates = Vec::with_capacity(aggregates.len());
        let mut aggregate_inputs = Vec::with_capacity(aggregates.len());
        let mut aggregate_filters = Vec::with_capacity(aggregates.len());
        let mut aggregate_orders = Vec::with_capacity(aggregates.len());

        for aggregate in aggregates {
            let Expression::Aggregate(mut aggregate) = aggregate else {
                return Err(paro_common::error::internal(
                    "Expected aggregate expression in DISTINCT ON planning".to_string(),
                ));
            };

            let mut inputs = Vec::with_capacity(aggregate.children.len());
            let mut children = Vec::with_capacity(aggregate.children.len());
            for child_expr in std::mem::take(&mut aggregate.children) {
                let extracted =
                    extract_distinct_payload(child_expr, &mut projection_exprs, &mut payload_types);
                inputs.push(reference_index(&extracted)?);
                children.push(extracted);
            }
            aggregate.children = children;

            aggregate_inputs.push(inputs);
            aggregate_filters.push(None);
            aggregate_orders.push(Vec::new());
            extracted_aggregates.push(Expression::Aggregate(aggregate));
        }

        let aggregate_data = GroupedAggregateData {
            projection_exprs,
            payload_types,
            groups,
            grouping_sets: Vec::new(),
            aggregates: extracted_aggregates,
            grouping_functions: Vec::new(),
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

    /// Get the FIRST aggregate function for a given type.
    fn get_first_function_for_type(&self, logical_type: &LogicalType) -> Result<AggregateFunction> {
        let first_set = get_first_function();
        let (func, _) = first_set.bind(std::slice::from_ref(logical_type))?;
        Ok(func)
    }
}

fn extract_distinct_payload(
    expr: Expression,
    projection_exprs: &mut Vec<Expression>,
    payload_types: &mut Vec<LogicalType>,
) -> Expression {
    if let Expression::Reference(_) = expr {
        return expr;
    }

    let return_type = expr.return_type();
    let reference_index = projection_exprs.len();
    payload_types.push(return_type.clone());
    projection_exprs.push(expr);
    Expression::Reference(ReferenceExpression {
        index: reference_index,
        return_type,
    })
}
