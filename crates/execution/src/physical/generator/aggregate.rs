// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::operators::aggregate::aggregate_state::AggregateStateLayout;
use crate::operators::aggregate::build_helpers::aggregate_objects;
use crate::operators::aggregate::perfect_aggregate_hashtable::compile_direct_update_program;
use crate::operators::aggregate::perfect_aggregate_hashtable::perfect_hash_occupancy_bytes;
use crate::operators::aggregate::tuple_layout::TupleLayout;
use crate::physical::specs::GroupKeyEncoding;
use paro_function::aggregate::distributive::first_last::get_first_function;
use paro_function::aggregate::{AggregateFunction, AggregateSingletonMerge};
use paro_function::scalar::function_data_equals;
use paro_planner::expression::{OperatorExpression, OperatorType};
use paro_planner::operator::{DistinctType, GroupInputMultiplicity};
use paro_storage::statistics::{NumericStats, StringStats};

fn plan_group_key_encodings(
    aggregate: &LogicalAggregate,
    group_indices: &[usize],
) -> Box<[GroupKeyEncoding]> {
    let supports_physical_keys = aggregate.aggregates.iter().all(|expression| {
        matches!(expression, Expression::Aggregate(bound) if bound.order_bys.is_empty())
    });
    let logical_types = group_indices
        .iter()
        .map(|&group_idx| &aggregate.groups[group_idx])
        .map(Expression::return_type)
        .collect::<Vec<_>>();
    let mut encodings = group_indices
        .iter()
        .map(|&group_idx| {
            let expression = &aggregate.groups[group_idx];
            if !supports_physical_keys {
                return GroupKeyEncoding::Identity;
            }
            let Some(stats) = aggregate
                .group_stats
                .get(group_idx)
                .and_then(Option::as_ref)
            else {
                return GroupKeyEncoding::Identity;
            };
            if stats.get_type() != &expression.return_type() {
                return GroupKeyEncoding::Identity;
            }
            match expression.return_type() {
                LogicalType::Varchar => plan_string_group_key(stats),
                LogicalType::TinyInt
                | LogicalType::SmallInt
                | LogicalType::Integer
                | LogicalType::BigInt
                | LogicalType::HugeInt
                | LogicalType::UTinyInt
                | LogicalType::USmallInt
                | LogicalType::UInteger
                | LogicalType::UBigInt
                | LogicalType::UHugeInt => plan_integer_group_key(&expression.return_type(), stats),
                _ => GroupKeyEncoding::Identity,
            }
        })
        .collect::<Vec<_>>();
    retain_row_reducing_encodings(&logical_types, &mut encodings);
    encodings.into_boxed_slice()
}

#[derive(Debug)]
struct DependentGroupLayout {
    lookup_groups: Vec<usize>,
    dependent_groups: Vec<usize>,
    dependent_functions: Vec<AggregateFunction>,
    state_output_projection: Vec<usize>,
}

fn plan_dependent_groups(aggregate: &LogicalAggregate) -> Option<DependentGroupLayout> {
    if aggregate.groups.len() < 2
        || !aggregate.grouping_functions.is_empty()
        || !aggregate.has_plain_grouping_domain()
        // Post-reduction positional domains address the SQL aggregate list.
        // Do not append hidden dependent-group states until that representation
        // carries an explicit source/output map for the reduction runtime.
        || aggregate.post_reduction.is_some()
    {
        return None;
    }

    let (dependency, dependent_functions, _) = aggregate
        .group_dependencies
        .iter()
        .filter_map(|dependency| {
            if !dependency.is_valid_for(aggregate.groups.len()) {
                return None;
            }
            let functions = dependency
                .dependents
                .iter()
                .map(|&group_idx| {
                    let input_type = aggregate.groups[group_idx].return_type();
                    let (function, targets) = get_first_function()
                        .bind(std::slice::from_ref(&input_type))
                        .ok()?;
                    (targets.as_slice() == std::slice::from_ref(&input_type)
                        && function.return_type == input_type)
                        .then_some(function)
                })
                .collect::<Option<Vec<_>>>()?;
            let removed_width =
                dependency
                    .dependents
                    .iter()
                    .try_fold(0usize, |width, &group_idx| {
                        width.checked_add(aggregate.groups[group_idx].return_type().type_size())
                    })?;
            Some((dependency, functions, removed_width))
        })
        .max_by_key(|(_, _, removed_width)| *removed_width)?;
    let dependent_groups = dependency.dependents.to_vec();
    if dependent_groups.is_empty() {
        return None;
    }
    let dependent_set = dependent_groups
        .iter()
        .copied()
        .collect::<std::collections::HashSet<_>>();
    let lookup_groups = (0..aggregate.groups.len())
        .filter(|idx| !dependent_set.contains(idx))
        .collect::<Vec<_>>();
    if !dependency
        .determinants
        .iter()
        .all(|idx| lookup_groups.contains(idx))
    {
        return None;
    }

    let original_aggregate_count = aggregate.aggregates.len();
    let mut state_output_projection = Vec::with_capacity(aggregate.returned_types.len());
    for group_idx in 0..aggregate.groups.len() {
        if let Some(state_idx) = lookup_groups.iter().position(|idx| *idx == group_idx) {
            state_output_projection.push(state_idx);
        } else {
            let dependent_idx = dependent_groups
                .iter()
                .position(|idx| *idx == group_idx)
                .expect("every removed group is functionally dependent");
            state_output_projection
                .push(lookup_groups.len() + original_aggregate_count + dependent_idx);
        }
    }
    state_output_projection
        .extend((0..original_aggregate_count).map(|idx| lookup_groups.len() + idx));

    Some(DependentGroupLayout {
        lookup_groups,
        dependent_groups,
        dependent_functions,
        state_output_projection,
    })
}

/// Drop physical encodings that do not reduce the aligned group-key prefix.
///
/// Per-column width is insufficient here: aggregate states start at an
/// eight-byte boundary, so a narrow integer at the end of the group list can
/// merely turn useful bytes into padding while every input row still pays the
/// encoding cost. Starting from the compact layout and removing redundant
/// encodings also handles alignment interactions between adjacent keys.
fn retain_row_reducing_encodings(
    logical_types: &[LogicalType],
    encodings: &mut [GroupKeyEncoding],
) {
    let Some(logical_width) = TupleLayout::group_storage_width(logical_types).ok() else {
        encodings.fill(GroupKeyEncoding::Identity);
        return;
    };
    let Some(compact_width) = encoded_group_storage_width(logical_types, encodings) else {
        encodings.fill(GroupKeyEncoding::Identity);
        return;
    };
    if compact_width > logical_width {
        encodings.fill(GroupKeyEncoding::Identity);
        return;
    }

    for encoding_idx in 0..encodings.len() {
        if matches!(encodings[encoding_idx], GroupKeyEncoding::Identity) {
            continue;
        }
        let candidate = std::mem::replace(&mut encodings[encoding_idx], GroupKeyEncoding::Identity);
        let preserves_fixed_key_execution =
            matches!(candidate, GroupKeyEncoding::PackedString { .. });
        if preserves_fixed_key_execution
            || encoded_group_storage_width(logical_types, encodings) != Some(compact_width)
        {
            encodings[encoding_idx] = candidate;
        }
    }
}

fn encoded_group_storage_width(
    logical_types: &[LogicalType],
    encodings: &[GroupKeyEncoding],
) -> Option<usize> {
    if logical_types.len() != encodings.len() {
        return None;
    }
    let physical_types = logical_types
        .iter()
        .zip(encodings)
        .map(|(logical_type, encoding)| match encoding {
            GroupKeyEncoding::Identity => logical_type.clone(),
            GroupKeyEncoding::PackedString { physical_type, .. }
            | GroupKeyEncoding::OffsetInteger { physical_type, .. } => physical_type.clone(),
        })
        .collect::<Vec<_>>();
    TupleLayout::group_storage_width(&physical_types).ok()
}

fn plan_string_group_key(stats: &paro_storage::statistics::BaseStatistics) -> GroupKeyEncoding {
    let Some(max_length) =
        StringStats::max_string_length(stats).and_then(|length| usize::try_from(length).ok())
    else {
        return GroupKeyEncoding::Identity;
    };
    [
        LogicalType::UTinyInt,
        LogicalType::USmallInt,
        LogicalType::UInteger,
        LogicalType::UBigInt,
        LogicalType::UHugeInt,
    ]
    .into_iter()
    .find(|ty| max_length < ty.type_size() && ty.type_size() <= LogicalType::Varchar.type_size())
    .map(|physical_type| GroupKeyEncoding::PackedString {
        physical_type,
        max_length,
    })
    .unwrap_or(GroupKeyEncoding::Identity)
}

fn plan_integer_group_key(
    logical_type: &LogicalType,
    stats: &paro_storage::statistics::BaseStatistics,
) -> GroupKeyEncoding {
    let Some((minimum, maximum)) = NumericStats::guaranteed_bounds(stats) else {
        return GroupKeyEncoding::Identity;
    };
    let Some((minimum, maximum)) =
        integer_value_as_i128(&minimum).zip(integer_value_as_i128(&maximum))
    else {
        return GroupKeyEncoding::Identity;
    };
    let Some(range) = maximum
        .checked_sub(minimum)
        .and_then(|range| u128::try_from(range).ok())
    else {
        return GroupKeyEncoding::Identity;
    };
    [
        (LogicalType::UTinyInt, u8::MAX as u128),
        (LogicalType::USmallInt, u16::MAX as u128),
        (LogicalType::UInteger, u32::MAX as u128),
        (LogicalType::UBigInt, u64::MAX as u128),
        (LogicalType::UHugeInt, u128::MAX),
    ]
    .into_iter()
    .find(|(physical_type, maximum)| {
        range <= *maximum && physical_type.type_size() < logical_type.type_size()
    })
    .map(|(physical_type, _)| GroupKeyEncoding::OffsetInteger {
        physical_type,
        minimum,
    })
    .unwrap_or(GroupKeyEncoding::Identity)
}

fn integer_value_as_i128(value: &paro_common::runtime_value::Value) -> Option<i128> {
    use paro_common::runtime_value::Value;

    match value {
        Value::TinyInt(value) => Some(*value as i128),
        Value::SmallInt(value) => Some(*value as i128),
        Value::Integer(value) => Some(*value as i128),
        Value::BigInt(value) => Some(*value as i128),
        Value::HugeInt(value) => Some(*value),
        Value::UTinyInt(value) => Some(*value as i128),
        Value::USmallInt(value) => Some(*value as i128),
        Value::UInteger(value) => Some(*value as i128),
        Value::UBigInt(value) => Some(i128::from(*value)),
        Value::UHugeInt(value) => i128::try_from(*value).ok(),
        _ => None,
    }
}

/// Aggregate expressions rebased onto a compact payload projection.
///
/// Grouped aggregates and full-partition aggregate windows consume the same
/// aggregate ABI. Keeping extraction here gives both physical operators one
/// definition of argument, FILTER, and ordered-input column domains.
pub(crate) struct AggregatePayloadPlan {
    pub(crate) projection_exprs: Vec<Expression>,
    pub(crate) payload_types: Vec<LogicalType>,
    pub(crate) groups: Vec<Expression>,
    pub(crate) aggregates: Vec<Expression>,
    pub(crate) aggregate_inputs: Vec<Box<[usize]>>,
    pub(crate) aggregate_filters: Vec<Option<usize>>,
    pub(crate) aggregate_orders: Vec<Box<[usize]>>,
}

pub(crate) fn plan_aggregate_payload(
    group_expressions: Vec<Expression>,
    aggregate_expressions: Vec<Expression>,
) -> Result<AggregatePayloadPlan> {
    plan_aggregate_payload_with_prefix(Vec::new(), group_expressions, aggregate_expressions)
}

/// Plan an aggregate payload whose leading columns have a stable physical
/// meaning outside the aggregate itself.
///
/// Full-partition windows use the input row as this prefix. The same projected
/// payload can then be retained columnarly or externalized as raw aggregate
/// input without keeping a second copy of the detail row. Ordinary grouped
/// aggregates pass an empty prefix and retain their compact payload.
pub(crate) fn plan_aggregate_payload_with_prefix(
    payload_prefix: Vec<Expression>,
    group_expressions: Vec<Expression>,
    aggregate_expressions: Vec<Expression>,
) -> Result<AggregatePayloadPlan> {
    let mut projection_exprs = Vec::with_capacity(payload_prefix.len());
    let mut payload_types = Vec::with_capacity(payload_prefix.len());
    for expression in payload_prefix {
        payload_types.push(expression.return_type());
        projection_exprs.push(expression);
    }
    let groups = group_expressions
        .into_iter()
        .map(|expression| {
            extract_payload_expression(expression, &mut projection_exprs, &mut payload_types)
        })
        .collect::<Vec<_>>();
    let mut aggregate_inputs = Vec::with_capacity(aggregate_expressions.len());
    let mut aggregate_filters = Vec::with_capacity(aggregate_expressions.len());
    let mut aggregate_orders = Vec::with_capacity(aggregate_expressions.len());
    let mut aggregates = Vec::with_capacity(aggregate_expressions.len());

    for (aggregate_idx, aggregate_expr) in aggregate_expressions.into_iter().enumerate() {
        let Expression::Aggregate(mut bound) = aggregate_expr else {
            return Err(paro_error::internal(format!(
                "aggregate payload expression {aggregate_idx} is not an aggregate"
            )));
        };

        let mut inputs = Vec::with_capacity(bound.children.len());
        let mut children = Vec::with_capacity(bound.children.len());
        for child_expr in std::mem::take(&mut bound.children) {
            let reference =
                extract_payload_expression(child_expr, &mut projection_exprs, &mut payload_types);
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

    Ok(AggregatePayloadPlan {
        projection_exprs,
        payload_types,
        groups,
        aggregates,
        aggregate_inputs,
        aggregate_filters,
        aggregate_orders,
    })
}

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
        let singleton_proof = match &aggregate.group_input_multiplicity {
            GroupInputMultiplicity::AtMostOne(proof) if proof.is_valid_for(aggregate) => {
                Some(proof)
            }
            GroupInputMultiplicity::Arbitrary | GroupInputMultiplicity::AtMostOne(_) => None,
        };
        if singleton_proof.is_some() && having_filter.is_empty() {
            let child = self.generate_node(aggregate.child.as_ref())?;
            let expressions = singleton_group_projection(aggregate)?;
            return Ok((
                PhysicalNodeKind::Project(ProjectSpec {
                    expressions: expressions.into_boxed_slice(),
                    output_names: (0..aggregate.returned_types.len())
                        .map(|idx| format!("aggr_{idx}"))
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                }),
                vec![child],
            ));
        }
        let child = self.generate_node(aggregate.child.as_ref())?;

        let dependent_layout = plan_dependent_groups(aggregate);
        let group_indices = dependent_layout
            .as_ref()
            .map(|layout| layout.lookup_groups.clone())
            .unwrap_or_else(|| (0..aggregate.groups.len()).collect());

        let group_expressions = group_indices
            .iter()
            .map(|&group_idx| aggregate.groups[group_idx].clone())
            .collect::<Vec<_>>();

        let mut aggregate_expressions = aggregate.aggregates.clone();
        if let Some(layout) = &dependent_layout {
            for (&group_idx, function) in layout
                .dependent_groups
                .iter()
                .zip(&layout.dependent_functions)
            {
                let input = aggregate.groups[group_idx].clone();
                let input_type = input.return_type();
                aggregate_expressions.push(Expression::Aggregate(
                    paro_planner::expression::AggregateExpression::new(
                        function.clone(),
                        vec![input],
                        input_type,
                    ),
                ));
            }
        }
        let AggregatePayloadPlan {
            projection_exprs,
            payload_types,
            groups,
            aggregates,
            aggregate_inputs,
            aggregate_filters,
            aggregate_orders,
        } = plan_aggregate_payload(group_expressions, aggregate_expressions)?;

        // Perfect hash is a fixed-size, planner-admitted representation rather
        // than a spillable hash table. `force_external` must not change the
        // physical plan; memory admission below remains the single gate.
        let perfect_hash = if dependent_layout.is_none() {
            can_use_perfect_hash_aggregate(aggregate, &groups, &aggregates).map(|info| {
                PerfectHashAggregatePlan {
                    group_minima: info.group_minima.into_boxed_slice(),
                    group_cardinalities: info.group_cardinalities.into_boxed_slice(),
                    max_local_tables: 1,
                }
            })
        } else {
            None
        };

        let state_output_projection = dependent_layout
            .as_ref()
            .map(|layout| layout.state_output_projection.clone())
            .unwrap_or_default();
        let grouping_sets = if dependent_layout.is_some() {
            Box::new([])
        } else {
            aggregate
                .grouping_sets
                .iter()
                .map(|set| set.expressions.clone().into_boxed_slice())
                .collect::<Vec<_>>()
                .into_boxed_slice()
        };

        let group_key_encodings = if dependent_layout.is_some() {
            (0..groups.len())
                .map(|_| GroupKeyEncoding::Identity)
                .collect::<Vec<_>>()
                .into_boxed_slice()
        } else {
            plan_group_key_encodings(aggregate, &group_indices)
        };
        let mut spec = AggregateSpec {
            grouping_key_count: groups.len(),
            state_output_projection: state_output_projection.into_boxed_slice(),
            estimated_input_rows: aggregate
                .child
                .stats
                .estimated_cardinality
                .map(|estimate| estimate.expected),
            projection_exprs: projection_exprs.into_boxed_slice(),
            payload_types: payload_types.into_boxed_slice(),
            groups: groups.into_boxed_slice(),
            group_key_encodings,
            grouping_sets,
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
            post_reduction: lower_post_aggregate_reduction(aggregate)?,
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
        if spec.perfect_hash.is_some() {
            match perfect_hash_max_local_tables(&spec, self.ctx.max_memory, self.ctx.max_threads) {
                Some(max_local_tables) => {
                    if let Some(plan) = spec.perfect_hash.as_mut() {
                        plan.max_local_tables = max_local_tables;
                    }
                }
                None => spec.perfect_hash = None,
            }
        }
        finalize_post_aggregate_strategy(&mut spec);
        spec.verify_post_reduction()?;
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
            state_output_projection: Box::new([]),
            estimated_input_rows: distinct
                .child
                .stats
                .estimated_cardinality
                .map(|estimate| estimate.expected),
            projection_exprs: projection_exprs.into_boxed_slice(),
            payload_types: child_types.clone().into_boxed_slice(),
            groups: groups.into_boxed_slice(),
            group_key_encodings: (0..child_types.len())
                .map(|_| GroupKeyEncoding::Identity)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            grouping_sets: Box::new([]),
            aggregates: Box::new([]),
            grouping_functions: Box::new([]),
            aggregate_inputs: Box::new([]),
            aggregate_filters: Box::new([]),
            aggregate_orders: Box::new([]),
            post_reduction: None,
            having_filter: Box::new([]),
            perfect_hash: None,
            output_names: child_names.into_boxed_slice(),
            output_types: child_types.into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::Aggregate(spec), vec![child]))
    }
}

fn singleton_group_projection(aggregate: &LogicalAggregate) -> Result<Vec<Expression>> {
    if !aggregate.has_plain_grouping_domain()
        || aggregate.post_reduction.is_some()
        || !aggregate.grouping_functions.is_empty()
    {
        return Err(paro_error::internal(
            "At-most-one aggregate annotation has an unsupported grouping domain",
        ));
    }
    let mut expressions = aggregate.groups.clone();
    expressions.reserve(aggregate.aggregates.len());
    for (ordinal, expression) in aggregate.aggregates.iter().enumerate() {
        let Expression::Aggregate(merge) = expression else {
            return Err(paro_error::internal(format!(
                "At-most-one aggregate output {ordinal} is not an aggregate expression"
            )));
        };
        if merge.aggr_type != paro_planner::expression::AggregateType::NonDistinct
            || merge.filter.is_some()
            || !merge.order_bys.is_empty()
            || merge.children.len() != 1
        {
            return Err(paro_error::internal(format!(
                "At-most-one aggregate output {ordinal} has no scalar singleton form"
            )));
        }
        let input = merge.children[0].clone();
        let projected = match merge.function.singleton_merge() {
            Some(AggregateSingletonMerge::Input) => input,
            Some(law @ AggregateSingletonMerge::InputOr(_)) => {
                let value = law.normalized_fallback(&merge.return_type).ok_or_else(|| {
                    paro_error::internal(format!(
                        "At-most-one aggregate output {ordinal} has an invalid fallback type"
                    ))
                })?;
                Expression::Operator(OperatorExpression::new(
                    OperatorType::Coalesce,
                    vec![
                        input,
                        Expression::Constant(ConstantExpression::new(
                            value,
                            merge.return_type.clone(),
                        )),
                    ],
                    merge.return_type.clone(),
                ))
            }
            None => {
                return Err(paro_error::internal(format!(
                    "At-most-one aggregate output {ordinal} lacks a singleton merge contract"
                )))
            }
        };
        expressions.push(projected);
    }
    if expressions.len() != aggregate.returned_types.len() {
        return Err(paro_error::internal(
            "At-most-one aggregate projection width does not match its logical output",
        ));
    }
    if let Some((ordinal, (expression, expected))) = expressions
        .iter()
        .zip(&aggregate.returned_types)
        .enumerate()
        .find(|(_, (expression, expected))| expression.return_type() != **expected)
    {
        return Err(paro_error::internal(format!(
            "At-most-one aggregate projection type {ordinal} differs from its logical output: projected={:?}, expected={expected:?}",
            expression.return_type()
        )));
    }
    Ok(expressions)
}

fn lower_post_aggregate_reduction(
    aggregate: &LogicalAggregate,
) -> Result<Option<PostAggregateReductionSpec>> {
    aggregate.verify_post_reduction()?;
    let Some(reduction) = &aggregate.post_reduction else {
        return Ok(None);
    };

    let mut reducers = reduction.reducers.clone();
    for (reducer_idx, reducer) in reducers.iter_mut().enumerate() {
        let Expression::Aggregate(reducer) = reducer else {
            return Err(paro_error::internal(format!(
                "validated post-aggregate reducer {reducer_idx} lost its aggregate root"
            )));
        };
        for (child_idx, child) in reducer.children.iter_mut().enumerate() {
            let Expression::ColumnRef(column) = child else {
                return Err(paro_error::internal(format!(
                    "validated post-aggregate reducer {reducer_idx} argument {child_idx} lost its aggregate binding"
                )));
            };
            if column.depth != 0
                || column.binding.table_index != aggregate.aggregate_index
                || column.binding.column_index >= aggregate.aggregates.len()
            {
                return Err(paro_error::internal(format!(
                    "post-aggregate reducer {reducer_idx} argument {child_idx} cannot be lowered to the aggregate-only value domain"
                )));
            }
            *child = Expression::Reference(ReferenceExpression::new(
                column.binding.column_index,
                column.return_type.clone(),
            ));
        }
    }
    let reducer_types = reducers
        .iter()
        .map(Expression::return_type)
        .collect::<Vec<_>>();
    // The current perfect-table seal proves finalize semantics through the
    // same direct predicate that filters its sole aggregate state. With more
    // aggregate columns, a rejected row could otherwise hide a finalize-time
    // error in an unrelated state. Keep rollup admission single-state until
    // the aggregate ABI exposes complete per-row finalize validation.
    let input_rollup_sources = (aggregate.aggregates.len() == 1 && reducers.len() == 1)
        .then(|| {
            reducers
                .iter()
                .map(|reducer| {
                    let Expression::Aggregate(reducer) = reducer else {
                        return None;
                    };
                    let [Expression::Reference(reference)] = reducer.children.as_slice() else {
                        return None;
                    };
                    let Expression::Aggregate(source) =
                        aggregate.aggregates.get(reference.index)?
                    else {
                        return None;
                    };
                    if source.is_distinct()
                        || source.filter.is_some()
                        || !source.order_bys.is_empty()
                        || source.function.destructor.is_some()
                        || !source.function.state_is_trivially_copyable()
                        || source.return_type != reducer.return_type
                        || reference.return_type != source.return_type
                    {
                        return None;
                    }
                    let expected_source = source.function.input_rollup_function()?;
                    let expected_reducer = source.function.partial_merge_function()?;
                    (expected_source.execution_semantics_equal(&source.function)
                        && expected_reducer.execution_semantics_equal(&reducer.function)
                        && function_data_equals(
                            source.bind_info.as_ref(),
                            source.function.bind_data.as_ref(),
                        )
                        && function_data_equals(
                            reducer.bind_info.as_ref(),
                            reducer.function.bind_data.as_ref(),
                        ))
                    .then_some(reference.index)
                })
                .collect::<Option<Vec<_>>>()
        })
        .flatten()
        .map(Vec::into_boxed_slice);
    let scalar_expressions = reduction.scalar_expressions.clone();
    let scalar_types = scalar_expressions
        .iter()
        .map(Expression::return_type)
        .collect::<Vec<_>>();
    let mut predicate = reduction.predicate.clone();
    rebase_post_reduction_predicate(
        &mut predicate,
        aggregate.aggregate_index,
        reduction.reduction_index,
        aggregate.aggregates.len(),
        scalar_types.len(),
    )?;

    Ok(Some(PostAggregateReductionSpec {
        aggregate_types: aggregate
            .aggregates
            .iter()
            .map(Expression::return_type)
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        reducers: reducers.into_boxed_slice(),
        reducer_types: reducer_types.into_boxed_slice(),
        scalar_expressions: scalar_expressions.into_boxed_slice(),
        scalar_types: scalar_types.into_boxed_slice(),
        predicate,
        input_rollup_sources,
    }))
}

fn rebase_post_reduction_predicate(
    expression: &mut Expression,
    aggregate_index: usize,
    reduction_index: usize,
    aggregate_count: usize,
    scalar_count: usize,
) -> Result<()> {
    match expression {
        Expression::ColumnRef(column) => {
            if column.depth != 0 {
                return Err(paro_error::internal(
                    "post-aggregate reduction predicate retained a correlated column",
                ));
            }
            let index = if column.binding.table_index == aggregate_index {
                if column.binding.column_index >= aggregate_count {
                    return Err(paro_error::internal(
                        "post-aggregate reduction predicate aggregate reference is out of bounds",
                    ));
                }
                column.binding.column_index
            } else if column.binding.table_index == reduction_index {
                if column.binding.column_index >= scalar_count {
                    return Err(paro_error::internal(
                        "post-aggregate reduction predicate scalar reference is out of bounds",
                    ));
                }
                aggregate_count
                    .checked_add(column.binding.column_index)
                    .ok_or_else(|| {
                        paro_error::internal(
                            "post-aggregate reduction predicate reference index overflow",
                        )
                    })?
            } else {
                return Err(paro_error::internal(
                    "post-aggregate reduction predicate retained an unavailable binding",
                ));
            };
            *expression =
                Expression::Reference(ReferenceExpression::new(index, column.return_type.clone()));
            Ok(())
        }
        Expression::Reference(_)
        | Expression::Aggregate(_)
        | Expression::Subquery(_)
        | Expression::Window(_) => Err(paro_error::internal(
            "post-aggregate reduction predicate retained an invalid expression domain",
        )),
        _ => {
            let mut result = Ok(());
            ExpressionIterator::enumerate_children_mut(expression, |child| {
                if result.is_ok() {
                    result = rebase_post_reduction_predicate(
                        child,
                        aggregate_index,
                        reduction_index,
                        aggregate_count,
                        scalar_count,
                    );
                }
            });
            result
        }
    }
}

const CONSERVATIVE_PERFECT_HASH_MAX_SLOTS: usize = 1 << 20;
const PERFECT_HASH_MEMORY_BUDGET_DIVISOR: usize = 4;

fn perfect_hash_max_local_tables(
    spec: &AggregateSpec,
    max_memory: usize,
    parallelism: usize,
) -> Option<usize> {
    let plan = spec.perfect_hash.as_ref()?;
    let slots = plan
        .group_cardinalities
        .iter()
        .try_fold(1usize, |total, cardinality| total.checked_mul(*cardinality))?;
    if max_memory == 0 {
        return (slots <= CONSERVATIVE_PERFECT_HASH_MAX_SLOTS).then_some(parallelism.max(1));
    }
    let objects = aggregate_objects(spec).ok()?;
    let layout = AggregateStateLayout::new(&objects).ok()?;
    let state_bytes = layout.total_size().max(1).checked_mul(slots)?;
    let state_storage_bytes = state_bytes
        .div_ceil(std::mem::size_of::<u64>())
        .checked_mul(std::mem::size_of::<u64>())?;
    let storage_bytes = state_storage_bytes.checked_add(perfect_hash_occupancy_bytes(slots)?)?;
    let direct_program = compile_direct_update_program(
        &objects,
        &spec
            .aggregate_inputs
            .iter()
            .map(|inputs| inputs.to_vec())
            .collect::<Vec<_>>(),
        &layout,
    );
    let aggregate_scratch_bytes = direct_program.scratch_bytes(slots)?;
    let scratch_bytes =
        aggregate_scratch_bytes.checked_add(direct_program.materialized_slot_bytes()?)?;
    let bytes_per_table = storage_bytes.checked_add(scratch_bytes)?;
    let table_budget = max_memory / PERFECT_HASH_MEMORY_BUDGET_DIVISOR;
    let admitted_tables = table_budget / bytes_per_table;
    (admitted_tables > 0).then_some(admitted_tables.min(parallelism.max(1)))
}

/// Resolve a proof-level input-rollup candidate into a concrete execution
/// strategy only after the complete aggregate representation and memory
/// admission are known. Unsupported shapes retain the preserving finalized-
/// group traversal rather than making physical specialization a query
/// correctness requirement.
fn finalize_post_aggregate_strategy(spec: &mut AggregateSpec) {
    let candidate = spec
        .post_reduction
        .as_ref()
        .is_some_and(|post| post.input_rollup_sources.is_some());
    if candidate && !can_execute_post_input_rollup(spec) {
        spec.post_reduction
            .as_mut()
            .expect("input-rollup candidate requires post reduction")
            .input_rollup_sources = None;
    }
}

fn can_execute_post_input_rollup(spec: &AggregateSpec) -> bool {
    let Some(post) = &spec.post_reduction else {
        return false;
    };
    let Some(sources) = post.input_rollup_sources.as_deref() else {
        return false;
    };
    let Some(perfect) = &spec.perfect_hash else {
        return false;
    };
    if perfect.max_local_tables <= 1
        || !spec.having_filter.is_empty()
        || !spec.has_plain_grouping_domain()
        || spec.aggregates.len() != 1
        || post.reducers.len() != 1
        || sources != [0]
    {
        return false;
    }
    let Some(state_filter) = post.state_filter_plan() else {
        return false;
    };
    if state_filter.aggregate_index != 0 {
        return false;
    }
    let Some(Expression::Aggregate(source)) = spec.aggregates.first() else {
        return false;
    };
    if source.is_distinct()
        || source.filter.is_some()
        || !source.order_bys.is_empty()
        || spec.aggregate_filters.first() != Some(&None)
        || !spec
            .aggregate_orders
            .first()
            .is_some_and(|orders| orders.is_empty())
        || source.function.destructor.is_some()
        || !source.function.state_is_trivially_copyable()
        || source.function.simple_update.is_none()
        || source.function.state_filter.is_none()
        || source.function.direct_state_filter.is_none()
        || source.function.direct_update.is_none()
    {
        return false;
    }
    let Some(inputs) = spec.aggregate_inputs.first() else {
        return false;
    };
    if inputs.len() != source.function.arguments.len() {
        return false;
    }
    if !inputs.iter().enumerate().all(|(argument_idx, &input)| {
        let Some(expected) = source.function.arguments.get(argument_idx) else {
            return false;
        };
        spec.payload_types.get(input) == Some(expected)
            && matches!(
                source.children.get(argument_idx),
                Some(Expression::Reference(reference))
                    if reference.index == input && &reference.return_type == expected
            )
    }) {
        return false;
    }
    let Ok(objects) = aggregate_objects(spec) else {
        return false;
    };
    let Ok(layout) = AggregateStateLayout::new(&objects) else {
        return false;
    };
    let program = compile_direct_update_program(
        &objects,
        &spec
            .aggregate_inputs
            .iter()
            .map(|inputs| inputs.to_vec())
            .collect::<Vec<_>>(),
        &layout,
    );
    program.supports_direct_combine() && program.supports_trivial_state_copy()
}
