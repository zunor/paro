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
use paro_function::aggregate::AggregateFunction;
use paro_planner::operator::DistinctType;
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
        || !plain_grouping_set(aggregate)
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
                        width.checked_add(aggregate.groups[group_idx].return_type().physical_size())
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

fn plain_grouping_set(aggregate: &LogicalAggregate) -> bool {
    aggregate.grouping_sets.is_empty()
        || (aggregate.grouping_sets.len() == 1
            && aggregate.grouping_sets[0].expressions.len() == aggregate.groups.len()
            && aggregate.grouping_sets[0]
                .expressions
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                == (0..aggregate.groups.len()).collect())
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
    if compact_width >= logical_width {
        encodings.fill(GroupKeyEncoding::Identity);
        return;
    }

    for encoding_idx in 0..encodings.len() {
        if matches!(encodings[encoding_idx], GroupKeyEncoding::Identity) {
            continue;
        }
        let candidate = std::mem::replace(&mut encodings[encoding_idx], GroupKeyEncoding::Identity);
        if encoded_group_storage_width(logical_types, encodings) != Some(compact_width) {
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
    .find(|ty| {
        max_length < ty.physical_size() && ty.physical_size() < LogicalType::Varchar.physical_size()
    })
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
        range <= *maximum && physical_type.physical_size() < logical_type.physical_size()
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

        let dependent_layout = plan_dependent_groups(aggregate);
        let group_indices = dependent_layout
            .as_ref()
            .map(|layout| layout.lookup_groups.clone())
            .unwrap_or_else(|| (0..aggregate.groups.len()).collect());

        let mut projection_exprs = Vec::new();
        let mut payload_types = Vec::new();
        let groups = group_indices
            .iter()
            .map(|&group_idx| aggregate.groups[group_idx].clone())
            .map(|expr| extract_payload_expression(expr, &mut projection_exprs, &mut payload_types))
            .collect::<Vec<_>>();
        let mut aggregate_inputs = Vec::with_capacity(aggregate.aggregates.len());
        let mut aggregate_filters = Vec::with_capacity(aggregate.aggregates.len());
        let mut aggregate_orders = Vec::with_capacity(aggregate.aggregates.len());
        let mut aggregates = Vec::with_capacity(aggregate.aggregates.len());

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

        for aggregate_expr in aggregate_expressions {
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
            having_filter: Box::new([]),
            perfect_hash: None,
            output_names: child_names.into_boxed_slice(),
            output_types: child_types.into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::Aggregate(spec), vec![child]))
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
