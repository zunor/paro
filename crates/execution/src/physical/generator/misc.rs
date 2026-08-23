// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::aggregate::{plan_aggregate_payload_with_prefix, AggregatePayloadPlan};
use super::*;
use crate::physical::specs::GroupKeyEncoding;

impl PhysicalPlanGenerator {
    pub(crate) fn plan_node_output(&self, id: PhysicalPlanNodeId) -> RowType {
        self.arena
            .get(id)
            .expect("synthetic physical node must exist")
            .output
            .clone()
    }

    pub(crate) fn lower_window(
        &mut self,
        window: &LogicalWindow,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        for expression in &window.expressions {
            expression.verify_bound_contract()?;
        }
        let mut child = self.generate_node(window.child.as_ref())?;
        let input_width = window.child.types().len();
        let mut output_names = align_output_names(
            window.child.output_names(),
            input_width,
            "window child output",
        )?;
        output_names.extend((0..window.expressions.len()).map(|idx| format!("window_{}", idx + 1)));
        if let Some(spec) = lower_partition_aggregate_window_spec(window, output_names.clone())? {
            return Ok((
                PhysicalNodeKind::PartitionAggregateWindow(spec),
                vec![child],
            ));
        }
        let mut expressions = window.expressions.clone();
        let mut input_expressions = window
            .child
            .types()
            .into_iter()
            .enumerate()
            .map(|(index, ty)| Expression::Reference(ReferenceExpression::new(index, ty)))
            .collect::<Vec<_>>();
        materialize_window_inputs(&mut expressions, &mut input_expressions);
        if input_expressions.len() > input_width {
            let mut input_names = align_output_names(
                window.child.output_names(),
                input_width,
                "window input projection",
            )?;
            input_names.extend(
                (input_width..input_expressions.len())
                    .map(|index| format!("__window_input_{index}")),
            );
            let input_types = input_expressions
                .iter()
                .map(Expression::return_type)
                .collect::<Vec<_>>();
            let projection = ProjectSpec {
                expressions: input_expressions.into_boxed_slice(),
                output_names: input_names.clone().into_boxed_slice(),
            };
            child = self.push_node(
                PhysicalNodeKind::Project(projection),
                RowType::new(input_names, input_types),
                vec![child],
                OperatorLabel::new(window.child.id, "WINDOW_INPUT"),
                window.child.stats.estimated_cardinality,
            );
        }
        let spec = WindowSpec {
            window_index: window.window_index,
            expressions: expressions.into_boxed_slice(),
            input_width,
            output_names: output_names.into_boxed_slice(),
            output_types: window.get_types().into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::Window(spec), vec![child]))
    }

    pub(crate) fn lower_table_function(
        &mut self,
        get: &LogicalTableFunctionGet,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        if get.is_in_out_function() {
            return Ok((
                self.unsupported(
                    "TABLE_FUNCTION_GET",
                    "table-in-out functions lower as transforms in a later phase",
                ),
                Vec::new(),
            ));
        }
        let spec = TableFunctionScanSpec {
            function: get.function.clone(),
            bind_data: get.bind_data.clone(),
            table_index: get.table_index,
            arguments: get.arguments.clone().into_boxed_slice(),
            projection_ids: get
                .projection_ids
                .as_ref()
                .map(|ids| ids.clone().into_boxed_slice()),
            input_table_types: get.input_table_types.clone().into_boxed_slice(),
            input_table_names: get.input_table_names.clone().into_boxed_slice(),
            output_names: get.get_names().into_boxed_slice(),
            output_types: get.get_types().into_boxed_slice(),
            with_ordinality: get.with_ordinality,
        };
        Ok((PhysicalNodeKind::TableFunctionScan(spec), Vec::new()))
    }

    pub(crate) fn lower_delim_get(
        &mut self,
        get: &LogicalDelimGet,
    ) -> (PhysicalNodeKind, Vec<PhysicalPlanNodeId>) {
        let spec = DelimScanSpec {
            target: DelimScanTarget::Values {
                table_index: get.table_index,
            },
            output_names: get.chunk_names.clone().into_boxed_slice(),
            output_types: get.chunk_types.clone().into_boxed_slice(),
        };
        (PhysicalNodeKind::DelimScan(spec), Vec::new())
    }
}

fn materialize_window_inputs(
    expressions: &mut [WindowExpression],
    input_expressions: &mut Vec<Expression>,
) {
    for expression in expressions {
        for partition in &mut expression.partitions {
            materialize_window_input(partition, input_expressions);
        }
        for order in &mut expression.orders {
            materialize_window_input(&mut order.expression, input_expressions);
        }
        for argument in expression.arguments_mut() {
            materialize_window_input(argument, input_expressions);
        }
        if let WindowInvocation::Aggregate(aggregate) = &mut expression.invocation {
            if let Some(filter) = &mut aggregate.filter {
                materialize_window_input(filter, input_expressions);
            }
            for order in &mut aggregate.order_bys {
                materialize_window_input(&mut order.expression, input_expressions);
            }
        }
        if let WindowFrameBound::Offset(offset) = &mut expression.frame.start_bound {
            materialize_window_input(offset, input_expressions);
        }
        if let WindowFrameBound::Offset(offset) = &mut expression.frame.end_bound {
            materialize_window_input(offset, input_expressions);
        }
    }
}

fn materialize_window_input(expression: &mut Expression, inputs: &mut Vec<Expression>) {
    if matches!(
        expression,
        Expression::Constant(_) | Expression::Reference(_) | Expression::ColumnRef(_)
    ) {
        return;
    }
    let index = expression
        .evaluation_properties()
        .can_share_evaluation()
        .then(|| {
            inputs
                .iter()
                .position(|candidate| candidate.equals(expression))
        })
        .flatten()
        .unwrap_or_else(|| {
            let index = inputs.len();
            inputs.push(expression.clone());
            index
        });
    *expression = Expression::Reference(ReferenceExpression::new(index, expression.return_type()));
}

/// Lower complete-partition aggregate windows to a sort-free breaker.
///
/// Eligibility is semantic rather than name based: every invocation must be a
/// bound, unordered aggregate over the same partition domain, and
/// every frame must cover that domain completely. Unsupported modifiers keep
/// the ordinary window implementation as the preserving fallback.
fn lower_partition_aggregate_window_spec(
    window: &LogicalWindow,
    output_names: Vec<String>,
) -> Result<Option<PartitionAggregateWindowSpec>> {
    let Some(first) = window.expressions.first() else {
        return Ok(None);
    };
    // Other key domains retain the preserving sorted window path until the
    // immutable lookup index consumes the aggregate tuple codec directly.
    // Boxed Value equality is not a correctness substitute for SQL grouping
    // semantics (collations and floating keys in particular).
    if !first.partitions.is_empty()
        && (first.partitions.len() != 1
            || !matches!(
                first.partitions[0].return_type(),
                LogicalType::Integer | LogicalType::BigInt
            ))
    {
        return Ok(None);
    }
    for expression in &window.expressions {
        expression.verify_bound_contract()?;
        let Some(aggregate) = expression.aggregate_invocation() else {
            return Ok(None);
        };
        if expression.ignore_nulls
            || !expression.orders.is_empty()
            || !expression
                .frame
                .covers_whole_partition(!expression.orders.is_empty())
            || aggregate.is_distinct()
            // Resource fallback discards a partially built hash table after
            // replaying the stable input payload. Until aggregate teardown is
            // allocation-free, states with explicit destructors must retain
            // the ordinary sorted-window lifetime protocol.
            || aggregate.function.destructor.is_some()
            || !aggregate.order_bys.is_empty()
            || expression.partitions.len() != first.partitions.len()
            || !expression
                .partitions
                .iter()
                .zip(&first.partitions)
                .all(|(left, right)| left.equals(right))
        {
            return Ok(None);
        }
    }

    let aggregate_expressions = window
        .expressions
        .iter()
        .map(|expression| {
            Expression::Aggregate(
                expression
                    .aggregate_invocation()
                    .expect("partition aggregate eligibility checked")
                    .clone(),
            )
        })
        .collect::<Vec<_>>();
    let input_types = window.child.types();
    let AggregatePayloadPlan {
        projection_exprs,
        payload_types,
        groups,
        aggregates,
        aggregate_inputs,
        aggregate_filters,
        aggregate_orders,
    } = plan_aggregate_payload_with_prefix(
        input_types
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, ty)| Expression::Reference(ReferenceExpression::new(index, ty)))
            .collect(),
        first.partitions.clone(),
        aggregate_expressions,
    )?;
    let group_count = groups.len();
    let detail_columns = (0..input_types.len()).collect::<Vec<_>>();
    let aggregate_output_types = groups
        .iter()
        .chain(&aggregates)
        .map(Expression::return_type)
        .collect::<Vec<_>>();
    let aggregate_output_names = (0..aggregate_output_types.len())
        .map(|idx| format!("partition_aggr_{idx}"))
        .collect::<Vec<_>>();
    let aggregate = AggregateSpec {
        grouping_key_count: group_count,
        state_output_projection: Box::new([]),
        estimated_input_rows: window
            .child
            .stats
            .estimated_cardinality
            .map(|estimate| estimate.expected),
        projection_exprs: projection_exprs.into_boxed_slice(),
        payload_types: payload_types.into_boxed_slice(),
        groups: groups.into_boxed_slice(),
        group_key_encodings: (0..group_count)
            .map(|_| GroupKeyEncoding::Identity)
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        grouping_sets: Box::new([]),
        aggregates: aggregates.into_boxed_slice(),
        grouping_functions: Box::new([]),
        aggregate_inputs: aggregate_inputs.into_boxed_slice(),
        aggregate_filters: aggregate_filters.into_boxed_slice(),
        aggregate_orders: aggregate_orders.into_boxed_slice(),
        post_reduction: None,
        having_filter: Box::new([]),
        perfect_hash: None,
        output_names: aggregate_output_names.into_boxed_slice(),
        output_types: aggregate_output_types.into_boxed_slice(),
    };
    let spec = PartitionAggregateWindowSpec {
        domain: if first.partitions.is_empty() {
            PartitionAggregateDomain::Global
        } else {
            PartitionAggregateDomain::Keyed
        },
        input_types: input_types.into_boxed_slice(),
        detail_columns: detail_columns.into_boxed_slice(),
        aggregate,
        output_names: output_names.into_boxed_slice(),
        output_types: window.get_types().into_boxed_slice(),
    };
    spec.verify()?;
    Ok(Some(spec))
}
