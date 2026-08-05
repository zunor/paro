// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use paro_planner::operator::DistinctType;

impl PhysicalPlanGenerator {
    pub(crate) fn lower_aggregate(
        &mut self,
        aggregate: &LogicalAggregate,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        self.lower_aggregate_with_having(aggregate, Box::new([]))
    }

    pub(crate) fn lower_aggregate_with_having(
        &mut self,
        aggregate: &LogicalAggregate,
        having_filter: Box<[Expression]>,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = self.generate_node(aggregate.child.as_ref())?;

        let mut projection_exprs = Vec::new();
        let mut payload_types = Vec::new();
        let groups = aggregate
            .groups
            .iter()
            .cloned()
            .map(|expr| extract_payload_expression(expr, &mut projection_exprs, &mut payload_types))
            .collect::<Vec<_>>();
        let mut aggregate_inputs = Vec::with_capacity(aggregate.aggregates.len());
        let mut aggregate_filters = Vec::with_capacity(aggregate.aggregates.len());
        let mut aggregate_orders = Vec::with_capacity(aggregate.aggregates.len());
        let mut aggregates = Vec::with_capacity(aggregate.aggregates.len());

        for aggregate_expr in aggregate.aggregates.iter().cloned() {
            let Expression::Aggregate(mut bound) = aggregate_expr else {
                return Ok((
                    self.unsupported("AGGREGATE", "non-aggregate expression in aggregate list"),
                    vec![child],
                ));
            };

            let mut inputs = Vec::with_capacity(bound.children.len());
            let mut children = Vec::with_capacity(bound.children.len());
            for child_expr in std::mem::take(&mut bound.children) {
                let reference = extract_payload_expression(
                    child_expr,
                    &mut projection_exprs,
                    &mut payload_types,
                );
                let Expression::Reference(reference_expr) = &reference else {
                    unreachable!("extract_payload_expression returns a reference");
                };
                inputs.push(reference_expr.index);
                children.push(reference);
            }
            bound.children = children;

            let filter_index = if let Some(filter) = bound.filter.take() {
                let reference =
                    extract_payload_expression(*filter, &mut projection_exprs, &mut payload_types);
                let Expression::Reference(reference_expr) = &reference else {
                    unreachable!("extract_payload_expression returns a reference");
                };
                let index = reference_expr.index;
                bound.filter = Some(Box::new(reference));
                Some(index)
            } else {
                None
            };

            let mut order_inputs = Vec::with_capacity(bound.order_bys.len());
            let mut order_bys = Vec::with_capacity(bound.order_bys.len());
            for mut order in std::mem::take(&mut bound.order_bys) {
                let reference = extract_payload_expression(
                    order.expression,
                    &mut projection_exprs,
                    &mut payload_types,
                );
                let Expression::Reference(reference_expr) = &reference else {
                    unreachable!("extract_payload_expression returns a reference");
                };
                order_inputs.push(reference_expr.index);
                order.expression = reference;
                order_bys.push(order);
            }
            bound.order_bys = order_bys;

            aggregate_inputs.push(inputs.into_boxed_slice());
            aggregate_filters.push(filter_index);
            aggregate_orders.push(order_inputs.into_boxed_slice());
            aggregates.push(Expression::Aggregate(bound));
        }

        let perfect_hash =
            can_use_perfect_hash_aggregate(aggregate, &groups, &aggregates).map(|info| {
                PerfectHashAggregatePlan {
                    group_minima: info.group_minima.into_boxed_slice(),
                    required_bits: info.required_bits.into_boxed_slice(),
                }
            });

        let spec = AggregateSpec {
            grouping_key_count: groups.len(),
            projection_exprs: projection_exprs.into_boxed_slice(),
            payload_types: payload_types.into_boxed_slice(),
            groups: groups.into_boxed_slice(),
            grouping_sets: aggregate
                .grouping_sets
                .iter()
                .map(|set| set.expressions.clone().into_boxed_slice())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            aggregates: aggregates.into_boxed_slice(),
            grouping_functions: aggregate
                .grouping_functions
                .iter()
                .cloned()
                .map(Vec::into_boxed_slice)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            aggregate_inputs: aggregate_inputs.into_boxed_slice(),
            aggregate_filters: aggregate_filters.into_boxed_slice(),
            aggregate_orders: aggregate_orders.into_boxed_slice(),
            having_filter,
            perfect_hash,
            output_names: aggregate
                .get_column_bindings()
                .iter()
                .enumerate()
                .map(|(idx, _)| format!("aggr_{idx}"))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            output_types: aggregate.returned_types.clone().into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::Aggregate(spec), vec![child]))
    }

    pub(crate) fn lower_distinct(
        &mut self,
        distinct: &LogicalDistinct,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        if distinct.distinct_type != DistinctType::Distinct {
            return self.unsupported_preserving_children(
                distinct.name(),
                "typed DISTINCT ON lowering requires ordered first-row selection",
                &[distinct.child.as_ref()],
            );
        }

        let child = self.generate_node(distinct.child.as_ref())?;
        let child_types = distinct.child.types();
        let child_names = align_output_names(
            distinct.child.output_names(),
            child_types.len(),
            "distinct output",
        )?;
        let mut projection_exprs = Vec::with_capacity(child_types.len());
        let mut groups = Vec::with_capacity(child_types.len());
        for (idx, ty) in child_types.iter().cloned().enumerate() {
            projection_exprs.push(Expression::Reference(ReferenceExpression::new(
                idx,
                ty.clone(),
            )));
            groups.push(Expression::Reference(ReferenceExpression::new(idx, ty)));
        }

        let spec = AggregateSpec {
            grouping_key_count: groups.len(),
            projection_exprs: projection_exprs.into_boxed_slice(),
            payload_types: child_types.clone().into_boxed_slice(),
            groups: groups.into_boxed_slice(),
            grouping_sets: Box::new([]),
            aggregates: Box::new([]),
            grouping_functions: Box::new([]),
            aggregate_inputs: Box::new([]),
            aggregate_filters: Box::new([]),
            aggregate_orders: Box::new([]),
            having_filter: Box::new([]),
            perfect_hash: None,
            output_names: child_names.into_boxed_slice(),
            output_types: child_types.into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::Aggregate(spec), vec![child]))
    }
}
