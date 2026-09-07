// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Share one deferred dimension attachment across `UNION ALL` branches.
//!
//! [`super::dimension_deferral`] turns a wide dimension aggregate into a
//! fact-side partial aggregate followed by the dimension join and a merge.
//! When compatible branches attach the same dimension, the join distributes
//! over `UNION ALL`: union the narrow partial states first, attach the
//! dimension once, and perform one merge aggregate. Branch constants are
//! carried through the narrow union and become merge grouping keys. A hidden
//! branch identity remains a grouping key even when the visible constants are
//! equal, preserving the duplicate rows required by `UNION ALL` bag semantics.
//! Nested binary parser nodes are treated as one associative n-ary UNION
//! expression. Recognition binds every leaf against one semantic shell and
//! application rebuilds the physical binary carrier only after the complete
//! n-ary equivalence proof succeeds, so insertion/tree order cannot change
//! which dimension is shared.

use std::collections::{HashMap, HashSet};

use paro_catalog::entry::CatalogEntry;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{ColumnRefExpression, ConstantExpression, Expression};
use paro_planner::operator::{
    Aggregate, ColumnBinding, ComparisonJoin, Filter, Join, JoinComparisonType, JoinType,
    LogicalOperator, Projection, SetOperation,
};
use paro_planner::plan::OwnedLogicalPlan;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputSlot {
    Group(usize),
    Aggregate(usize),
    Constant,
}

struct BranchView<'a> {
    projection: &'a Projection,
    filter: Option<&'a Filter>,
    outer: &'a Aggregate,
    join: &'a ComparisonJoin,
    dimension: &'a paro_planner::operator::Get,
    partial: &'a Aggregate,
    output_slots: Vec<OutputSlot>,
}

struct SharedDimensionWitness {
    output_slots: Vec<OutputSlot>,
    constant_outputs: Vec<usize>,
    needs_hidden_branch_identity: bool,
}

struct OwnedBranch {
    projection_expressions: Vec<Expression>,
    filter_expressions: Option<Vec<Expression>>,
    outer: Aggregate,
    join: ComparisonJoin,
    dimension: OwnedLogicalPlan,
    partial: OwnedLogicalPlan,
}

/// Produce one root-local alternative that shares a compatible dimension
/// attachment across two `UNION ALL` arms. Memo owns descendant enumeration;
/// this rule consumes the exact pair of child alternatives in its binding.
pub fn optimize_plan(
    plan: OwnedLogicalPlan,
    bind_context: &BindContext,
) -> Result<(OwnedLogicalPlan, bool)> {
    let Some(witness) = recognize(&plan) else {
        return Ok((plan, false));
    };
    Ok((apply(plan, witness, bind_context)?, true))
}

/// Exact semantic predicate used by the native Memo matcher before it spends
/// the bounded transformation-output frontier. The rewrite calls the same
/// recognizer again at apply time, keeping matching advisory and the
/// equivalence proof self-validating.
pub(crate) fn recognizes_plan(plan: &OwnedLogicalPlan) -> bool {
    recognize(plan).is_some()
}

fn recognize(plan: &OwnedLogicalPlan) -> Option<SharedDimensionWitness> {
    let LogicalOperator::SetOperation(setop) = &plan.operator else {
        return None;
    };
    if !setop.is_union_all()
        || setop.column_count != setop.types.len()
        || setop.left.types() != setop.types
        || setop.right.types() != setop.types
    {
        return None;
    }
    let mut arm_plans = Vec::new();
    collect_union_all_arms(plan, &setop.types, &mut arm_plans)?;
    if arm_plans.len() < 2 {
        return None;
    }
    let branches = arm_plans
        .into_iter()
        .map(branch_view)
        .collect::<Option<Vec<_>>>()?;
    let left = branches.first()?;
    if left.output_slots.len() != setop.column_count {
        return None;
    }

    for right in branches.iter().skip(1) {
        if left.output_slots.len() != right.output_slots.len()
            || left.outer.groups.len() != right.outer.groups.len()
            || left.outer.aggregates.len() != right.outer.aggregates.len()
            || left.partial.groups.len() != right.partial.groups.len()
            || left.partial.aggregates.len() != right.partial.aggregates.len()
            || left.join.conditions.len() != right.join.conditions.len()
            || left
                .partial
                .groups
                .iter()
                .map(Expression::return_type)
                .ne(right.partial.groups.iter().map(Expression::return_type))
            || left
                .partial
                .aggregates
                .iter()
                .map(Expression::return_type)
                .ne(right.partial.aggregates.iter().map(Expression::return_type))
        {
            return None;
        }
        let binding_map = equivalent_branch_bindings(left, right)?;
        let filters_match = match (left.filter, right.filter) {
            (None, None) => true,
            (Some(left), Some(right)) => {
                let right = right
                    .expressions
                    .iter()
                    .map(|expression| remap_expression(expression, &binding_map))
                    .collect::<Option<Vec<_>>>();
                right.is_some_and(|right| expression_multisets_equal(&left.expressions, &right))
            }
            _ => false,
        };
        if !filters_match
            || !left
                .outer
                .groups
                .iter()
                .zip(&right.outer.groups)
                .all(|(left, right)| {
                    remap_expression(right, &binding_map).is_some_and(|right| left.equals(&right))
                })
            || !left
                .outer
                .aggregates
                .iter()
                .zip(&right.outer.aggregates)
                .all(|(left, right)| {
                    remap_expression(right, &binding_map).is_some_and(|right| left.equals(&right))
                })
            || !left
                .join
                .conditions
                .iter()
                .zip(&right.join.conditions)
                .all(|(left, right)| {
                    left.comparison == right.comparison
                        && remap_expression(&right.left, &binding_map)
                            .is_some_and(|right| left.left.equals(&right))
                        && remap_expression(&right.right, &binding_map)
                            .is_some_and(|right| left.right.equals(&right))
                })
            || left.output_slots != right.output_slots
        {
            return None;
        }
        for (slot, (left_expression, right_expression)) in left.output_slots.iter().zip(
            left.projection
                .expressions
                .iter()
                .zip(&right.projection.expressions),
        ) {
            if *slot == OutputSlot::Constant
                && left_expression.return_type() != right_expression.return_type()
            {
                return None;
            }
        }
    }

    let constant_outputs = left
        .output_slots
        .iter()
        .enumerate()
        .filter_map(|(ordinal, slot)| (*slot == OutputSlot::Constant).then_some(ordinal))
        .collect::<Vec<_>>();
    let visible_constants_prove_disjoint = (0..branches.len()).all(|left_index| {
        ((left_index + 1)..branches.len()).all(|right_index| {
            constant_outputs.iter().any(|ordinal| {
                let (Expression::Constant(left_constant), Expression::Constant(right_constant)) = (
                    &branches[left_index].projection.expressions[*ordinal],
                    &branches[right_index].projection.expressions[*ordinal],
                ) else {
                    return false;
                };
                grouping_constants_prove_distinct(left_constant, right_constant)
            })
        })
    });
    Some(SharedDimensionWitness {
        constant_outputs,
        needs_hidden_branch_identity: !visible_constants_prove_disjoint,
        output_slots: left.output_slots.clone(),
    })
}

fn collect_union_all_arms<'a>(
    plan: &'a OwnedLogicalPlan,
    output_types: &[LogicalType],
    arms: &mut Vec<&'a OwnedLogicalPlan>,
) -> Option<()> {
    let LogicalOperator::SetOperation(setop) = &plan.operator else {
        arms.push(plan);
        return Some(());
    };
    if !setop.is_union_all()
        || setop.column_count != output_types.len()
        || setop.types != output_types
        || setop.left.types() != output_types
        || setop.right.types() != output_types
    {
        return None;
    }
    collect_union_all_arms(setop.left.as_ref(), output_types, arms)?;
    collect_union_all_arms(setop.right.as_ref(), output_types, arms)
}

/// Prove two literals cannot land in the same SQL grouping domain. Floating
/// NaNs and NULLs deliberately stay unproven because their grouping equality
/// is not ordinary Rust value inequality.
fn grouping_constants_prove_distinct(
    left: &ConstantExpression,
    right: &ConstantExpression,
) -> bool {
    if left.return_type != right.return_type
        || left.value.is_null()
        || right.value.is_null()
        || matches!(left.value, Value::Float(_) | Value::Double(_))
        || matches!(right.value, Value::Float(_) | Value::Double(_))
        || matches!(left.return_type, LogicalType::VarcharCollation(_))
    {
        return false;
    }
    left.value != right.value
}

fn branch_view(plan: &OwnedLogicalPlan) -> Option<BranchView<'_>> {
    let LogicalOperator::Projection(projection) = &plan.operator else {
        return None;
    };
    let (filter, aggregate_plan) = match &projection.child.operator {
        LogicalOperator::Filter(filter) => (Some(filter), filter.child.as_ref()),
        _ => (None, projection.child.as_ref()),
    };
    let LogicalOperator::Aggregate(outer) = &aggregate_plan.operator else {
        return None;
    };
    if outer.post_reduction.is_some()
        || outer.aggregates.is_empty()
        || !outer.has_plain_grouping_domain()
    {
        return None;
    }
    let LogicalOperator::Join(Join::Comparison(join)) = &outer.child.operator else {
        return None;
    };
    if join.join_type != JoinType::Inner
        || join.conditions.is_empty()
        || join.mark_index.is_some()
        || !join.duplicate_eliminated_columns.is_empty()
        || join.delim_flipped
        || join
            .conditions
            .iter()
            .any(|condition| condition.comparison != JoinComparisonType::Equal)
    {
        return None;
    }
    let LogicalOperator::Get(dimension) = &join.left.operator else {
        return None;
    };
    let LogicalOperator::Aggregate(partial) = &join.right.operator else {
        return None;
    };
    if partial.post_reduction.is_some()
        || partial.aggregates.is_empty()
        || !partial.has_plain_grouping_domain()
        || !join_conditions_match_partial(join, dimension, partial)
        || !merge_contract_matches_partial(outer, partial)
    {
        return None;
    }

    let output_slots = projection
        .expressions
        .iter()
        .map(|expression| output_slot(expression, outer))
        .collect::<Option<Vec<_>>>()?;
    Some(BranchView {
        projection,
        filter,
        outer,
        join,
        dimension,
        partial,
        output_slots,
    })
}

fn output_slot(expression: &Expression, aggregate: &Aggregate) -> Option<OutputSlot> {
    match expression {
        Expression::ColumnRef(column) if column.depth == 0 => {
            if column.binding.table_index == aggregate.group_index
                && column.binding.column_index < aggregate.groups.len()
            {
                Some(OutputSlot::Group(column.binding.column_index))
            } else if column.binding.table_index == aggregate.aggregate_index
                && column.binding.column_index < aggregate.aggregates.len()
            {
                Some(OutputSlot::Aggregate(column.binding.column_index))
            } else {
                None
            }
        }
        Expression::Constant(_) => Some(OutputSlot::Constant),
        _ => None,
    }
}

fn join_conditions_match_partial(
    join: &ComparisonJoin,
    dimension: &paro_planner::operator::Get,
    partial: &Aggregate,
) -> bool {
    let dimension_bindings = join
        .left
        .get_column_bindings()
        .into_iter()
        .collect::<HashSet<_>>();
    join.conditions.iter().all(|condition| {
        expression_reads_only(&condition.left, &dimension_bindings)
            && matches!(
                &condition.right,
                Expression::ColumnRef(column)
                    if column.depth == 0
                        && column.binding.table_index == partial.group_index
                        && column.binding.column_index < partial.groups.len()
            )
    }) && dimension.table.is_some()
}

fn merge_contract_matches_partial(outer: &Aggregate, partial: &Aggregate) -> bool {
    outer.aggregates.iter().all(|expression| {
        let Expression::Aggregate(merge) = expression else {
            return false;
        };
        if merge.children.len() != 1 || merge.filter.is_some() || !merge.order_bys.is_empty() {
            return false;
        }
        let Expression::ColumnRef(column) = &merge.children[0] else {
            return false;
        };
        if column.depth != 0
            || column.binding.table_index != partial.aggregate_index
            || column.binding.column_index >= partial.aggregates.len()
        {
            return false;
        }
        let Expression::Aggregate(source) = &partial.aggregates[column.binding.column_index] else {
            return false;
        };
        source
            .function
            .partial_merge_function()
            .is_some_and(|expected| expected.execution_semantics_equal(&merge.function))
    })
}

fn equivalent_branch_bindings(
    left: &BranchView<'_>,
    right: &BranchView<'_>,
) -> Option<HashMap<ColumnBinding, ColumnBinding>> {
    if !equivalent_dimension_gets(left.dimension, right.dimension) {
        return None;
    }

    let mut bindings = HashMap::new();
    extend_positional_bindings(
        &mut bindings,
        right.dimension.table_index,
        left.dimension.table_index,
        left.dimension.returned_types.len(),
    );
    extend_positional_bindings(
        &mut bindings,
        right.partial.group_index,
        left.partial.group_index,
        left.partial.groups.len(),
    );
    extend_positional_bindings(
        &mut bindings,
        right.partial.aggregate_index,
        left.partial.aggregate_index,
        left.partial.aggregates.len(),
    );
    extend_positional_bindings(
        &mut bindings,
        right.outer.group_index,
        left.outer.group_index,
        left.outer.groups.len(),
    );
    extend_positional_bindings(
        &mut bindings,
        right.outer.aggregate_index,
        left.outer.aggregate_index,
        left.outer.aggregates.len(),
    );
    Some(bindings)
}

/// Immutable shell proof used by both the Memo matcher and the semantic
/// rewrite. Keeping it centralized prevents pattern ordering from spending a
/// bounded candidate frontier on dimension pairs the rule must later reject.
pub(crate) fn equivalent_dimension_gets(
    left: &paro_planner::operator::Get,
    right: &paro_planner::operator::Get,
) -> bool {
    left.table
        .as_ref()
        .zip(right.table.as_ref())
        .is_some_and(|(left_table, right_table)| left_table.object_id() == right_table.object_id())
        && left.returned_types == right.returned_types
        && left.column_types == right.column_types
        && left.column_sources == right.column_sources
        && left.scan_order.is_none()
        && right.scan_order.is_none()
        && left.runtime_filter_expressions.is_empty()
        && right.runtime_filter_expressions.is_empty()
}

fn extend_positional_bindings(
    bindings: &mut HashMap<ColumnBinding, ColumnBinding>,
    from_table: usize,
    to_table: usize,
    count: usize,
) {
    bindings.extend((0..count).map(|ordinal| {
        (
            ColumnBinding::new(from_table, ordinal),
            ColumnBinding::new(to_table, ordinal),
        )
    }));
}

fn expression_reads_only(expression: &Expression, allowed: &HashSet<ColumnBinding>) -> bool {
    let mut valid = true;
    let mut read = false;
    crate::expression::traversal::visit_expression(expression, &mut |expression| {
        if let Expression::ColumnRef(column) = expression {
            read = true;
            valid &= column.depth == 0 && allowed.contains(&column.binding);
        }
    });
    valid && read
}

fn remap_expression(
    expression: &Expression,
    bindings: &HashMap<ColumnBinding, ColumnBinding>,
) -> Option<Expression> {
    let valid = std::cell::Cell::new(true);
    let expression = expression.clone().replace_column_ref(&|column| {
        if column.depth != 0 {
            valid.set(false);
            return None;
        }
        match bindings.get(&column.binding) {
            Some(binding) => Some(Expression::ColumnRef(ColumnRefExpression::new(
                *binding,
                column.return_type.clone(),
            ))),
            None => {
                valid.set(false);
                None
            }
        }
    });
    valid.get().then_some(expression)
}

fn replace_known_bindings(
    expression: Expression,
    bindings: &HashMap<ColumnBinding, ColumnBinding>,
) -> Expression {
    expression.replace_column_ref(&|column| {
        bindings.get(&column.binding).map(|binding| {
            Expression::ColumnRef(ColumnRefExpression::new(
                *binding,
                column.return_type.clone(),
            ))
        })
    })
}

/// `AND` and `OR` are commutative in the already-bound SQL boolean domain.
/// Filter normalization is free to preserve either encounter order, so an
/// equivalence proof must compare the conjunction as a multiset rather than
/// making sharing depend on branch text order.
fn expressions_equivalent(left: &Expression, right: &Expression) -> bool {
    let (Expression::Conjunction(left), Expression::Conjunction(right)) = (left, right) else {
        return left.equals(right);
    };
    if left.conjunction_type != right.conjunction_type
        || left.children.len() != right.children.len()
    {
        return false;
    }
    let mut matched = vec![false; right.children.len()];
    left.children.iter().all(|left_child| {
        right
            .children
            .iter()
            .enumerate()
            .find(|(ordinal, right_child)| {
                !matched[*ordinal] && expressions_equivalent(left_child, right_child)
            })
            .map(|(ordinal, _)| matched[ordinal] = true)
            .is_some()
    })
}

fn expression_multisets_equal(left: &[Expression], right: &[Expression]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut matched = vec![false; right.len()];
    left.iter().all(|left_expression| {
        right
            .iter()
            .enumerate()
            .find(|(ordinal, right_expression)| {
                !matched[*ordinal] && expressions_equivalent(left_expression, right_expression)
            })
            .map(|(ordinal, _)| matched[ordinal] = true)
            .is_some()
    })
}

fn apply(
    plan: OwnedLogicalPlan,
    witness: SharedDimensionWitness,
    bind_context: &BindContext,
) -> Result<OwnedLogicalPlan> {
    let (root_id, root_stats, operator) = plan.into_parts();
    let LogicalOperator::SetOperation(setop) = operator else {
        return Err(paro_error::internal(
            "shared dimension witness lost its UNION ALL root",
        ));
    };
    let mut branch_plans = Vec::new();
    collect_owned_union_all_arms(*setop.left, &setop.types, &mut branch_plans)?;
    collect_owned_union_all_arms(*setop.right, &setop.types, &mut branch_plans)?;
    let mut branches = branch_plans
        .into_iter()
        .map(take_branch)
        .collect::<Result<Vec<_>>>()?;
    if branches.len() < 2 {
        return Err(paro_error::internal(
            "shared dimension n-ary witness requires at least two arms",
        ));
    }
    let left = branches.remove(0);

    let left_partial = aggregate_from_plan(&left.partial)?;
    let partial_group_count = left_partial.groups.len();
    let partial_aggregate_count = left_partial.aggregates.len();
    let left_partial_group_index = left_partial.group_index;
    let left_partial_aggregate_index = left_partial.aggregate_index;
    let union_types = left_partial
        .groups
        .iter()
        .map(Expression::return_type)
        .chain(left_partial.aggregates.iter().map(Expression::return_type))
        .chain(
            witness
                .constant_outputs
                .iter()
                .map(|ordinal| left.projection_expressions[*ordinal].return_type()),
        )
        .chain(
            witness
                .needs_hidden_branch_identity
                .then_some(LogicalType::UBigInt),
        )
        .collect::<Vec<LogicalType>>();

    let OwnedBranch {
        projection_expressions: left_projection_expressions,
        filter_expressions: left_filter_expressions,
        outer: left_outer,
        join: left_join,
        dimension: left_dimension,
        partial: left_partial,
    } = left;
    let left_constants = witness
        .constant_outputs
        .iter()
        .map(|ordinal| left_projection_expressions[*ordinal].clone())
        .collect();
    let mut partial_arms = vec![partial_union_arm(
        left_partial,
        left_constants,
        witness.needs_hidden_branch_identity.then_some(0),
        bind_context,
    )?];
    for (index, branch) in branches.into_iter().enumerate() {
        let constants = witness
            .constant_outputs
            .iter()
            .map(|ordinal| branch.projection_expressions[*ordinal].clone())
            .collect();
        partial_arms.push(partial_union_arm(
            branch.partial,
            constants,
            witness
                .needs_hidden_branch_identity
                .then_some(u64::try_from(index + 1).unwrap_or(u64::MAX)),
            bind_context,
        )?);
    }
    let branch_identity_ordinal = witness
        .needs_hidden_branch_identity
        .then_some(union_types.len() - 1);
    let allow_out_of_order = setop.allow_out_of_order;
    let (partial_union, union_index) =
        build_nary_union_all(partial_arms, &union_types, allow_out_of_order, bind_context)?;

    let mut partial_to_union = HashMap::new();
    extend_positional_bindings(
        &mut partial_to_union,
        left_partial_group_index,
        union_index,
        partial_group_count,
    );
    partial_to_union.extend((0..partial_aggregate_count).map(|ordinal| {
        (
            ColumnBinding::new(left_partial_aggregate_index, ordinal),
            ColumnBinding::new(union_index, partial_group_count + ordinal),
        )
    }));

    let mut conditions = left_join.conditions;
    for condition in &mut conditions {
        condition.left = replace_known_bindings(condition.left.clone(), &partial_to_union);
        condition.right = replace_known_bindings(condition.right.clone(), &partial_to_union);
    }
    let joined = OwnedLogicalPlan::new(
        bind_context,
        LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
            JoinType::Inner,
            left_dimension,
            partial_union,
            conditions,
        ))),
    );

    let original_group_count = left_outer.groups.len();
    let original_aggregate_count = left_outer.aggregates.len();
    let original_group_index = left_outer.group_index;
    let original_aggregate_index = left_outer.aggregate_index;
    let mut final_groups = left_outer
        .groups
        .into_iter()
        .map(|expression| replace_known_bindings(expression, &partial_to_union))
        .collect::<Vec<_>>();
    final_groups.extend(witness.constant_outputs.iter().enumerate().map(
        |(constant_ordinal, _)| {
            let input_ordinal = partial_group_count + partial_aggregate_count + constant_ordinal;
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(union_index, input_ordinal),
                setop.types[witness.constant_outputs[constant_ordinal]].clone(),
            ))
        },
    ));
    // Visible branch constants are not a branch identity: both arms may emit
    // the same constant tuple, and UNION ALL must still retain one group from
    // each arm. Keep an internal identity through the merge and project it
    // away at the root.
    if let Some(branch_identity_ordinal) = branch_identity_ordinal {
        final_groups.push(Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(union_index, branch_identity_ordinal),
            LogicalType::UBigInt,
        )));
    }
    let final_aggregates = left_outer
        .aggregates
        .into_iter()
        .map(|expression| replace_known_bindings(expression, &partial_to_union))
        .collect::<Vec<_>>();
    let final_group_index = bind_context.generate_table_index();
    let final_aggregate_index = bind_context.generate_table_index();
    let final_groupings_index = bind_context.generate_table_index();
    let final_aggregate = OwnedLogicalPlan::new(
        bind_context,
        LogicalOperator::Aggregate(Aggregate::new(
            final_group_index,
            final_aggregate_index,
            final_groupings_index,
            joined,
            final_groups,
            vec![],
            final_aggregates,
            vec![],
        )),
    );

    let mut outer_to_final = HashMap::new();
    extend_positional_bindings(
        &mut outer_to_final,
        original_group_index,
        final_group_index,
        original_group_count,
    );
    extend_positional_bindings(
        &mut outer_to_final,
        original_aggregate_index,
        final_aggregate_index,
        original_aggregate_count,
    );
    let final_input = if let Some(filter_expressions) = left_filter_expressions {
        OwnedLogicalPlan::new(
            bind_context,
            LogicalOperator::Filter(Filter::new(
                final_aggregate,
                filter_expressions
                    .into_iter()
                    .map(|expression| replace_known_bindings(expression, &outer_to_final))
                    .collect(),
            )),
        )
    } else {
        final_aggregate
    };

    let mut constant_ordinal = 0usize;
    let output_expressions = witness
        .output_slots
        .into_iter()
        .enumerate()
        .map(|(output_ordinal, slot)| match slot {
            OutputSlot::Group(ordinal) => Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(final_group_index, ordinal),
                setop.types[output_ordinal].clone(),
            )),
            OutputSlot::Aggregate(ordinal) => Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(final_aggregate_index, ordinal),
                setop.types[output_ordinal].clone(),
            )),
            OutputSlot::Constant => {
                let group_ordinal = original_group_count + constant_ordinal;
                let output_ordinal = witness.constant_outputs[constant_ordinal];
                constant_ordinal += 1;
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(final_group_index, group_ordinal),
                    setop.types[output_ordinal].clone(),
                ))
            }
        })
        .collect();
    let projection = Projection::new(setop.table_index, final_input, output_expressions);
    Ok(OwnedLogicalPlan {
        id: root_id,
        stats: root_stats,
        operator: LogicalOperator::Projection(projection),
    })
}

fn take_branch(plan: OwnedLogicalPlan) -> Result<OwnedBranch> {
    let (_, _, operator) = plan.into_parts();
    let LogicalOperator::Projection(projection) = operator else {
        return Err(paro_error::internal(
            "shared dimension witness lost a branch projection",
        ));
    };
    let Projection {
        expressions: projection_expressions,
        child,
        ..
    } = projection;
    let (_, _, operator) = child.into_parts();
    let (filter_expressions, operator) = match operator {
        LogicalOperator::Filter(filter) => {
            let Filter {
                expressions, child, ..
            } = filter;
            let (_, _, child_operator) = child.into_parts();
            (Some(expressions), child_operator)
        }
        operator => (None, operator),
    };
    let LogicalOperator::Aggregate(mut outer) = operator else {
        return Err(paro_error::internal(
            "shared dimension witness lost an outer aggregate",
        ));
    };
    let child = *std::mem::replace(
        &mut outer.child,
        Box::new(OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan)),
    );
    let (_, _, operator) = child.into_parts();
    let LogicalOperator::Join(Join::Comparison(mut join)) = operator else {
        return Err(paro_error::internal(
            "shared dimension witness lost its comparison join",
        ));
    };
    let dimension = *join.left;
    let partial = *join.right;
    join.left = Box::new(OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan));
    join.right = Box::new(OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan));
    Ok(OwnedBranch {
        projection_expressions,
        filter_expressions,
        outer,
        join,
        dimension,
        partial,
    })
}

fn collect_owned_union_all_arms(
    plan: OwnedLogicalPlan,
    output_types: &[LogicalType],
    arms: &mut Vec<OwnedLogicalPlan>,
) -> Result<()> {
    let LogicalOperator::SetOperation(_) = &plan.operator else {
        arms.push(plan);
        return Ok(());
    };
    let (_, _, operator) = plan.into_parts();
    let LogicalOperator::SetOperation(setop) = operator else {
        return Err(paro_error::internal(
            "shared dimension set-operation ownership split changed operator",
        ));
    };
    if !setop.is_union_all()
        || setop.column_count != output_types.len()
        || setop.types != output_types
        || setop.left.types() != output_types
        || setop.right.types() != output_types
    {
        return Err(paro_error::internal(
            "shared dimension n-ary witness changed before application",
        ));
    }
    collect_owned_union_all_arms(*setop.left, output_types, arms)?;
    collect_owned_union_all_arms(*setop.right, output_types, arms)
}

fn aggregate_from_plan(plan: &OwnedLogicalPlan) -> Result<&Aggregate> {
    let LogicalOperator::Aggregate(aggregate) = &plan.operator else {
        return Err(paro_error::internal(
            "shared dimension witness lost a partial aggregate",
        ));
    };
    Ok(aggregate)
}

fn partial_union_arm(
    partial_plan: OwnedLogicalPlan,
    constants: Vec<Expression>,
    branch_identity: Option<u64>,
    bind_context: &BindContext,
) -> Result<OwnedLogicalPlan> {
    let partial = aggregate_from_plan(&partial_plan)?;
    let expressions = (0..partial.groups.len())
        .map(|ordinal| {
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(partial.group_index, ordinal),
                partial.groups[ordinal].return_type(),
            ))
        })
        .chain((0..partial.aggregates.len()).map(|ordinal| {
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(partial.aggregate_index, ordinal),
                partial.aggregates[ordinal].return_type(),
            ))
        }))
        .chain(constants)
        .chain(branch_identity.map(|branch_identity| {
            Expression::Constant(ConstantExpression::new(
                Value::UBigInt(branch_identity),
                LogicalType::UBigInt,
            ))
        }))
        .collect();
    Ok(OwnedLogicalPlan::new(
        bind_context,
        LogicalOperator::Projection(
            Projection::new(
                bind_context.generate_table_index(),
                partial_plan,
                expressions,
            )
            .with_internal_outputs(),
        ),
    ))
}

fn build_nary_union_all(
    arms: Vec<OwnedLogicalPlan>,
    types: &[LogicalType],
    allow_out_of_order: bool,
    bind_context: &BindContext,
) -> Result<(OwnedLogicalPlan, usize)> {
    let mut arms = arms.into_iter();
    let mut union = arms
        .next()
        .ok_or_else(|| paro_error::internal("shared dimension n-ary union has no partial arms"))?;
    let mut union_index = 0usize;
    for arm in arms {
        union_index = bind_context.generate_table_index();
        let mut setop = SetOperation::union(union_index, union, arm, true, types.to_vec());
        setop.allow_out_of_order = allow_out_of_order;
        union = OwnedLogicalPlan::new(bind_context, LogicalOperator::SetOperation(setop));
    }
    if union_index == 0 {
        return Err(paro_error::internal(
            "shared dimension n-ary union requires at least two partial arms",
        ));
    }
    Ok((union, union_index))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::dimension_deferral;
    use crate::cascades::planner::{AlternativeOrigin, LogicalAlternative, MemoBuilder};
    use crate::cascades::SearchBudget;
    use crate::physical::{ResourceGrantClass, SpillPolicy};
    use crate::subquery::partition_aggregate_tests::setup_session;
    use crate::verify::verify_logical_plan;
    use paro_planner::planner::Planner;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn planned_union(sql: &str) -> (OwnedLogicalPlan, Planner) {
        let session = setup_session();
        let statement = paro_parser::parse_one(sql)
            .expect("parse union aggregate")
            .stmt;
        let mut planner = Planner::new(session);
        planner
            .create_plan(statement)
            .expect("plan union aggregate");
        let plan = planner.take_plan().expect("logical union aggregate");
        (plan, planner)
    }

    #[test]
    fn collated_constants_never_prove_disjoint_grouping_domains() {
        let logical_type = LogicalType::VarcharCollation("NOCASE".to_string());
        let lower = ConstantExpression::new(Value::Varchar("s".to_string()), logical_type.clone());
        let upper = ConstantExpression::new(Value::Varchar("S".to_string()), logical_type);
        assert!(!grouping_constants_prove_distinct(&lower, &upper));
    }

    fn defer_branch_aggregates(
        plan: OwnedLogicalPlan,
        bind_context: &BindContext,
    ) -> OwnedLogicalPlan {
        plan.try_map_post_order(|plan| {
            let (plan, _) = dimension_deferral::optimize_plan(plan, bind_context)?;
            Ok(plan)
        })
        .expect("defer branch dimensions")
    }

    #[test]
    fn union_partials_share_one_dimension_attachment() {
        let (plan, planner) = planned_union(
            "SELECT n_name, sum(s_acctbal), 's' FROM supplier JOIN nation \
             ON s_nationkey = n_nationkey GROUP BY n_name HAVING sum(s_acctbal) > 0 \
             UNION ALL \
             SELECT n_name, sum(c_acctbal), 'c' FROM customer JOIN nation \
             ON c_nationkey = n_nationkey GROUP BY n_name HAVING sum(c_acctbal) > 0",
        );
        let plan = defer_branch_aggregates(plan, &planner.binder.bind_context);
        let (plan, changed) =
            optimize_plan(plan, &planner.binder.bind_context).expect("share compatible dimension");
        assert!(changed, "{plan:#?}");
        verify_logical_plan(&planner.binder.bind_context, &plan).expect("verify shared plan");

        let mut nation_scans = 0;
        let mut aggregates = 0;
        let mut merge_group_width = None;
        plan.try_visit_pre_order(|node| {
            match &node.operator {
                LogicalOperator::Get(get)
                    if get
                        .table
                        .as_ref()
                        .is_some_and(|table| table.base.base.name == "nation") =>
                {
                    nation_scans += 1;
                }
                LogicalOperator::Aggregate(aggregate) => {
                    aggregates += 1;
                    if matches!(aggregate.child.operator, LogicalOperator::Join(_)) {
                        merge_group_width = Some(aggregate.groups.len());
                    }
                }
                _ => {}
            }
            Ok(())
        })
        .expect("inspect shared plan");
        assert_eq!(nation_scans, 1, "{plan:#?}");
        assert_eq!(aggregates, 3, "two partials plus one merge: {plan:#?}");
        assert_eq!(
            merge_group_width,
            Some(2),
            "a proven-distinct visible discriminator makes a hidden identity redundant: {plan:#?}"
        );
    }

    #[test]
    fn equal_visible_constants_keep_union_all_branch_identity() {
        let (plan, planner) = planned_union(
            "SELECT n_name, sum(s_acctbal), 'same' FROM supplier JOIN nation \
             ON s_nationkey = n_nationkey GROUP BY n_name \
             UNION ALL \
             SELECT n_name, sum(c_acctbal), 'same' FROM customer JOIN nation \
             ON c_nationkey = n_nationkey GROUP BY n_name",
        );
        let plan = defer_branch_aggregates(plan, &planner.binder.bind_context);
        let (plan, changed) =
            optimize_plan(plan, &planner.binder.bind_context).expect("share compatible dimension");
        assert!(changed, "{plan:#?}");
        verify_logical_plan(&planner.binder.bind_context, &plan).expect("verify shared plan");

        let mut merge_group_width = None;
        plan.try_visit_pre_order(|node| {
            if let LogicalOperator::Aggregate(aggregate) = &node.operator {
                if matches!(aggregate.child.operator, LogicalOperator::Join(_)) {
                    merge_group_width = Some(aggregate.groups.len());
                }
            }
            Ok(())
        })
        .expect("inspect shared plan");
        assert_eq!(
            merge_group_width,
            Some(3),
            "dimension group, visible constant, and hidden branch identity are all required: {plan:#?}"
        );
    }

    #[test]
    fn three_union_arms_share_one_dimension_independent_of_binary_shape() {
        let (plan, planner) = planned_union(
            "SELECT n_name, sum(s_acctbal), 'a' FROM supplier JOIN nation \
             ON s_nationkey = n_nationkey GROUP BY n_name \
             UNION ALL \
             SELECT n_name, sum(s_acctbal), 'b' FROM supplier JOIN nation \
             ON s_nationkey = n_nationkey GROUP BY n_name \
             UNION ALL \
             SELECT n_name, sum(s_acctbal), 'c' FROM supplier JOIN nation \
             ON s_nationkey = n_nationkey GROUP BY n_name",
        );
        let plan = defer_branch_aggregates(plan, &planner.binder.bind_context);
        let (plan, changed) =
            optimize_plan(plan, &planner.binder.bind_context).expect("share n-ary dimension");
        assert!(changed, "{plan:#?}");
        verify_logical_plan(&planner.binder.bind_context, &plan).expect("verify shared plan");

        let mut nation_scans = 0;
        let mut aggregates = 0;
        plan.try_visit_pre_order(|node| {
            match &node.operator {
                LogicalOperator::Get(get)
                    if get
                        .table
                        .as_ref()
                        .is_some_and(|table| table.base.base.name == "nation") =>
                {
                    nation_scans += 1;
                }
                LogicalOperator::Aggregate(_) => aggregates += 1,
                _ => {}
            }
            Ok(())
        })
        .expect("inspect n-ary shared plan");
        assert_eq!(nation_scans, 1, "{plan:#?}");
        assert_eq!(aggregates, 4, "three partials plus one merge: {plan:#?}");
    }

    #[test]
    fn nary_sharing_plan_is_stable_across_default_budget_envelope() {
        fn optimize(group_factor: u32) -> (crate::cascades::Fingerprint, f64, usize, u64) {
            let session = setup_session();
            let statement = paro_parser::parse_one(
                "SELECT n_name, sum(s_acctbal), 'a' FROM supplier JOIN nation \
                 ON s_nationkey = n_nationkey GROUP BY n_name \
                 UNION ALL \
                 SELECT n_name, sum(s_acctbal), 'b' FROM supplier JOIN nation \
                 ON s_nationkey = n_nationkey GROUP BY n_name \
                 UNION ALL \
                 SELECT n_name, sum(s_acctbal), 'c' FROM supplier JOIN nation \
                 ON s_nationkey = n_nationkey GROUP BY n_name",
            )
            .unwrap()
            .stmt;
            let mut planner = Planner::new(session.clone());
            planner.create_plan(statement).unwrap();
            let plan =
                defer_branch_aggregates(planner.take_plan().unwrap(), &planner.binder.bind_context);
            let mut budget = SearchBudget::default();
            budget.max_optional_groups_per_initial_group = group_factor;
            budget.max_optional_composition_groups_per_initial_group = group_factor;
            let context = crate::context::OptimizationContext::new(
                session,
                planner.binder.bind_context.clone(),
            );
            let output = MemoBuilder::build_with_search(
                vec![LogicalAlternative {
                    plan,
                    source: AlternativeOrigin::Baseline,
                    column_stats: Arc::new(HashMap::new()),
                }],
                &planner.binder,
                budget,
                &context,
            )
            .unwrap()
            .optimize(&[ResourceGrantClass {
                id: crate::cascades::ResourceGrantClassId(0),
                hard_memory_bytes: u64::MAX,
                spill_policy: SpillPolicy::Allowed,
                max_parallel_tasks: 1,
            }])
            .unwrap();
            assert!(output
                .rule_insertions
                .get(&crate::cascades::rules::AGGREGATE_DIMENSION_SHARING_RULE)
                .is_some_and(|count| *count > 0));
            let winner = &output.variants[0];
            let mut dimension_scans = 0;
            winner
                .plan
                .try_visit_pre_order(|node| {
                    if matches!(&node.operator, LogicalOperator::Get(get)
                        if get.table.as_ref().is_some_and(|table| table.base.base.name == "nation"))
                    {
                        dimension_scans += 1;
                    }
                    Ok(())
                })
                .unwrap();
            (
                winner.physical_fingerprint,
                winner.cost.score.range.expected,
                dimension_scans,
                output
                    .rule_insertions
                    .get(&crate::cascades::rules::AGGREGATE_DIMENSION_SHARING_RULE)
                    .copied()
                    .unwrap_or(0),
            )
        }

        let variants = [8, 16, 32, 64, 128]
            .into_iter()
            .map(optimize)
            .collect::<Vec<_>>();
        assert!(variants
            .iter()
            .all(|(_, _, _, insertions)| *insertions >= 2));
        assert!(variants.windows(2).all(|pair| pair[0].2 == pair[1].2));
        assert!(
            variants.windows(2).all(|pair| pair[0].0 == pair[1].0),
            "{variants:?}"
        );
        assert!(variants
            .windows(2)
            .all(|pair| (pair[0].1 - pair[1].1).abs() < 1e-9));
    }

    #[test]
    fn different_dimensions_are_not_factored() {
        let (plan, planner) = planned_union(
            "SELECT n_name, sum(s_acctbal), 's' FROM supplier JOIN nation \
             ON s_nationkey = n_nationkey GROUP BY n_name \
             UNION ALL \
             SELECT r_name, sum(s_acctbal), 'r' FROM supplier JOIN region \
             ON s_nationkey = r_regionkey GROUP BY r_name",
        );
        let plan = defer_branch_aggregates(plan, &planner.binder.bind_context);
        let (plan, changed) = optimize_plan(plan, &planner.binder.bind_context)
            .expect("reject incompatible dimension");
        assert!(!changed, "{plan:#?}");
    }
}
