// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Materialize narrow, total aggregate inputs below inner joins.
//!
//! Aggregate payload extraction normally evaluates scalar inputs immediately
//! above the aggregate. When an input depends on several columns from one
//! inner-join subtree, those raw columns can therefore survive through one or
//! more serialized join rows. A total expression may instead be evaluated at
//! its lowest complete binding domain: unmatched rows can observe extra work,
//! but cannot observe a new SQL error, volatile call, or external boundary.

use std::collections::{HashMap, HashSet};

use paro_common::error::Result;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{
    ColumnRefExpression, Expression, ExpressionIterator, ExpressionVisitDecision,
};
use paro_planner::operator::{
    ColumnBinding, ComparisonJoin, Join, JoinType, LogicalOperator, Projection,
};
use paro_planner::plan::LogicalPlan;

use crate::expression::traversal::visit_expression;

pub fn optimize_plan(plan: LogicalPlan, bind_context: &BindContext) -> Result<(LogicalPlan, bool)> {
    let mut changed = false;
    let plan = plan.try_map_post_order(|mut plan| {
        if let LogicalOperator::Aggregate(aggregate) = &mut plan.operator {
            let node_changed = materialize_inputs(
                aggregate.child.as_mut(),
                &mut aggregate.groups,
                &mut aggregate.aggregates,
                bind_context,
            );
            changed |= node_changed;
            if node_changed {
                aggregate.recompute_returned_types();
            }
        }
        Ok(plan)
    })?;
    Ok((plan, changed))
}

struct MaterializedInput {
    binding_map: HashMap<ColumnBinding, ColumnBinding>,
    binding: ColumnBinding,
    return_type: paro_common::types::LogicalType,
}

fn materialize_inputs(
    child: &mut LogicalPlan,
    groups: &mut [Expression],
    aggregates: &mut [Expression],
    bind_context: &BindContext,
) -> bool {
    let mut changed = false;
    let mut rejected = Vec::<Expression>::new();
    // Every success replaces at least one candidate occurrence with a passive
    // ColumnRef, while `rejected` prevents retrying unchanged failures. A
    // successful binding rewrite can change the remaining candidates' lowest
    // complete domain, so only then is the rejection set deliberately reset.
    loop {
        let candidate = aggregates.iter().find_map(|expression| {
            let Expression::Aggregate(aggregate) = expression else {
                return None;
            };
            aggregate
                .children
                .iter()
                .find(|child| {
                    aggregate_input_is_narrowing_total(child)
                        && inputs_are_dead_outside_candidate(child, groups, aggregates)
                        && !rejected.iter().any(|seen| seen.equals(child))
                })
                .cloned()
        });
        let Some(candidate) = candidate else {
            break;
        };
        let Some(materialized) =
            materialize_at_deepest_join_domain(child, &candidate, bind_context, 0)
        else {
            rejected.push(candidate);
            continue;
        };
        let replacement = Expression::ColumnRef(ColumnRefExpression::new(
            materialized.binding,
            materialized.return_type,
        ));
        for expression in groups.iter_mut().chain(aggregates.iter_mut()) {
            replace_equal_subexpressions(expression, &candidate, &replacement);
            *expression = remap_bindings(expression.clone(), &materialized.binding_map);
        }
        changed = true;
        rejected.clear();
    }
    changed
}

fn aggregate_input_is_narrowing_total(expression: &Expression) -> bool {
    if expression.is_passive_value() {
        return false;
    }
    let properties = expression.evaluation_properties();
    if !properties.can_share_evaluation()
        || properties.is_reorder_fence()
        || !properties.is_infallible()
    {
        return false;
    }
    let mut bindings = HashMap::new();
    let mut invalid = false;
    visit_expression(expression, &mut |expression| {
        if let Expression::ColumnRef(column) = expression {
            if column.depth != 0 {
                invalid = true;
            } else {
                bindings
                    .entry(column.binding)
                    .or_insert_with(|| column.return_type.clone());
            }
        }
    });
    if invalid || bindings.len() < 2 {
        return false;
    }
    let source_width = bindings.values().map(|ty| ty.type_size()).sum::<usize>();
    expression.return_type().type_size() < source_width
}

/// The width proof is real only if pruning can remove every source binding
/// above the inserted projection. Equal candidate uses share its one computed
/// output; any other use keeps the corresponding raw payload live and declines
/// this deliberately all-or-nothing cost proof.
fn inputs_are_dead_outside_candidate(
    candidate: &Expression,
    groups: &[Expression],
    aggregates: &[Expression],
) -> bool {
    let Some(bindings) = expression_bindings(candidate) else {
        return false;
    };
    !groups
        .iter()
        .chain(aggregates)
        .any(|root| expression_uses_bindings_outside(root, candidate, &bindings))
}

fn expression_uses_bindings_outside(
    root: &Expression,
    candidate: &Expression,
    bindings: &HashSet<ColumnBinding>,
) -> bool {
    let mut used = false;
    ExpressionIterator::visit(root, &mut |expression| {
        if expression.equals(candidate) {
            return ExpressionVisitDecision::SkipChildren;
        }
        if matches!(expression,
            Expression::ColumnRef(column)
                if column.depth == 0 && bindings.contains(&column.binding))
        {
            used = true;
            return ExpressionVisitDecision::SkipChildren;
        }
        ExpressionVisitDecision::Descend
    });
    used
}

fn materialize_at_deepest_join_domain(
    plan: &mut LogicalPlan,
    expression: &Expression,
    bind_context: &BindContext,
    crossed_joins: usize,
) -> Option<MaterializedInput> {
    let expression_bindings = expression_bindings(expression)?;
    if let LogicalOperator::Join(Join::Comparison(join)) = &mut plan.operator {
        if plain_inner_join(join) && !join_uses_bindings(join, &expression_bindings) {
            let left_bindings = join
                .left
                .get_column_bindings()
                .into_iter()
                .collect::<HashSet<_>>();
            let right_bindings = join
                .right
                .get_column_bindings()
                .into_iter()
                .collect::<HashSet<_>>();
            let materialized = if expression_bindings.is_subset(&left_bindings) {
                let materialized = materialize_at_deepest_join_domain(
                    join.left.as_mut(),
                    expression,
                    bind_context,
                    crossed_joins + 1,
                )?;
                include_materialized_binding(
                    join.left.as_ref(),
                    &mut join.left_projection_map,
                    materialized.binding,
                );
                Some(materialized)
            } else if expression_bindings.is_subset(&right_bindings) {
                let materialized = materialize_at_deepest_join_domain(
                    join.right.as_mut(),
                    expression,
                    bind_context,
                    crossed_joins + 1,
                )?;
                include_materialized_binding(
                    join.right.as_ref(),
                    &mut join.right_projection_map,
                    materialized.binding,
                );
                Some(materialized)
            } else {
                None
            };
            if let Some(materialized) = materialized {
                remap_join_expressions(join, &materialized.binding_map);
                return Some(materialized);
            }
        }
    }
    if crossed_joins == 0 {
        return None;
    }
    wrap_projection(plan, expression, bind_context)
}

fn join_uses_bindings(join: &ComparisonJoin, bindings: &HashSet<ColumnBinding>) -> bool {
    join.conditions.iter().any(|condition| {
        expression_uses_any_binding(&condition.left, bindings)
            || expression_uses_any_binding(&condition.right, bindings)
    })
}

fn expression_uses_any_binding(expression: &Expression, bindings: &HashSet<ColumnBinding>) -> bool {
    let mut used = false;
    visit_expression(expression, &mut |expression| {
        if matches!(expression,
            Expression::ColumnRef(column)
                if column.depth == 0 && bindings.contains(&column.binding))
        {
            used = true;
        }
    });
    used
}

fn include_materialized_binding(
    child: &LogicalPlan,
    projection: &mut paro_planner::operator::ProjectionMap,
    binding: ColumnBinding,
) {
    let output_ordinal = child
        .get_column_bindings()
        .iter()
        .position(|candidate| *candidate == binding);
    debug_assert!(
        output_ordinal.is_some(),
        "materialized aggregate binding must remain in every child domain"
    );
    // The binding is the append-only output constructed by `wrap_projection`;
    // every recursive frame above has already preserved it. Do not silently
    // widen a positional contract if that construction invariant drifts.
    if let Some(output_ordinal) = output_ordinal {
        projection.include(output_ordinal);
    }
}

fn wrap_projection(
    plan: &mut LogicalPlan,
    expression: &Expression,
    bind_context: &BindContext,
) -> Option<MaterializedInput> {
    let expression_bindings = expression_bindings(expression)?;
    let original = std::mem::replace(plan, LogicalPlan::synthetic(LogicalOperator::DummyScan));
    let old_bindings = original.get_column_bindings();
    let old_types = original.types();
    if old_bindings.len() != old_types.len()
        || !expression_bindings
            .iter()
            .all(|binding| old_bindings.contains(binding))
    {
        *plan = original;
        return None;
    }
    let mut output_names = original.output_names();
    let projection_index = bind_context.generate_table_index();
    let output_width = old_bindings.len();
    let return_type = expression.return_type();
    let mut expressions = old_bindings
        .iter()
        .copied()
        .zip(old_types)
        .map(|(binding, ty)| Expression::ColumnRef(ColumnRefExpression::new(binding, ty)))
        .collect::<Vec<_>>();
    expressions.push(expression.clone());
    output_names.push("__paro_materialized_aggregate_input".to_string());
    *plan = LogicalPlan::new(
        bind_context,
        LogicalOperator::Projection(
            Projection::new(projection_index, original, expressions)
                .with_visible_names(output_names),
        ),
    );
    let binding_map = old_bindings
        .into_iter()
        .enumerate()
        .map(|(ordinal, old)| (old, ColumnBinding::new(projection_index, ordinal)))
        .collect();
    Some(MaterializedInput {
        binding_map,
        binding: ColumnBinding::new(projection_index, output_width),
        return_type,
    })
}

fn plain_inner_join(join: &ComparisonJoin) -> bool {
    join.join_type == JoinType::Inner
        && join.mark_index.is_none()
        && join.duplicate_eliminated_columns.is_empty()
        && !join.delim_flipped
}

fn expression_bindings(expression: &Expression) -> Option<HashSet<ColumnBinding>> {
    let mut bindings = HashSet::new();
    let mut invalid = false;
    visit_expression(expression, &mut |expression| {
        if let Expression::ColumnRef(column) = expression {
            if column.depth == 0 {
                bindings.insert(column.binding);
            } else {
                invalid = true;
            }
        }
    });
    (!invalid && !bindings.is_empty()).then_some(bindings)
}

fn remap_join_expressions(
    join: &mut ComparisonJoin,
    bindings: &HashMap<ColumnBinding, ColumnBinding>,
) {
    for condition in &mut join.conditions {
        condition.left = remap_bindings(condition.left.clone(), bindings);
        condition.right = remap_bindings(condition.right.clone(), bindings);
    }
    for expression in &mut join.duplicate_eliminated_columns {
        *expression = remap_bindings(expression.clone(), bindings);
    }
}

fn remap_bindings(
    expression: Expression,
    bindings: &HashMap<ColumnBinding, ColumnBinding>,
) -> Expression {
    expression.replace_column_ref(&|column| {
        bindings.get(&column.binding).copied().map(|binding| {
            Expression::ColumnRef(ColumnRefExpression {
                binding,
                depth: column.depth,
                return_type: column.return_type.clone(),
            })
        })
    })
}

fn replace_equal_subexpressions(
    expression: &mut Expression,
    target: &Expression,
    replacement: &Expression,
) {
    if expression.equals(target) {
        *expression = replacement.clone();
        return;
    }
    ExpressionIterator::enumerate_children_mut(expression, |child| {
        replace_equal_subexpressions(child, target, replacement);
    });
}
