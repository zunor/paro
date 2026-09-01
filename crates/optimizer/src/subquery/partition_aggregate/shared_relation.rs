// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Reuse a shared relation for detail rows and complete partition aggregates.

use super::*;

pub(super) struct SharedRelationRewrite {
    scalar_binding: ColumnBinding,
    presence_binding: Option<ColumnBinding>,
    scalar_source_binding: ColumnBinding,
    scalar_expression: Expression,
    aggregate: AggregateExpression,
    partitions: Vec<Expression>,
    cte_index: usize,
    outer_table_index: usize,
}

/// Reuse one materialized relation snapshot for both the detail row and its
/// complete per-key aggregate. The window is inserted immediately above the
/// outer shared-relation leaf, before filtering or dimension joins, so it
/// observes exactly the same relation domain as the scalar branch.
pub(super) fn recognize_shared_relation_filter(
    plan: &LogicalPlan,
    output_contract: Option<&OutputContract>,
) -> Option<SharedRelationRewrite> {
    let shape = recognize_delim_shape(plan, output_contract)?;
    let source_side = direct_delim_join_source(
        &shape.scalar.aggregate.child,
        shape.delim.table_index,
        shape.correlation.inner_keys.len(),
    )?;
    let LogicalOperator::Join(Join::Comparison(inner_join)) =
        &shape.scalar.aggregate.child.operator
    else {
        return None;
    };
    let inner_source = match source_side {
        DirectSourceSide::LeftDelim => inner_join.right.as_ref(),
        DirectSourceSide::RightDelim => inner_join.left.as_ref(),
    };
    let LogicalOperator::CTERef(inner_ref) = &inner_source.operator else {
        return None;
    };

    let mut outer_refs = Vec::new();
    collect_cte_refs(&shape.join.left, inner_ref.cte_index, &mut outer_refs);
    let [outer_ref] = outer_refs.as_slice() else {
        return None;
    };
    if inner_ref.column_types != outer_ref.column_types
        || !shared_path_is_extensible(&shape.join.left, inner_ref.cte_index, outer_ref.table_index)
    {
        return None;
    }

    let mut bindings = BindingMap::default();
    for (ordinal, ty) in inner_ref.column_types.iter().enumerate() {
        if outer_ref.column_types.get(ordinal) != Some(ty)
            || !bindings.bind(
                ColumnBinding::new(inner_ref.table_index, ordinal),
                ColumnBinding::new(outer_ref.table_index, ordinal),
            )
        {
            return None;
        }
    }
    let partitions = shape
        .correlation
        .inner_keys
        .iter()
        .map(|key| rebase_expression(key, &bindings))
        .collect::<Option<Vec<_>>>()?;
    let aggregate = rebase_aggregate(shape.scalar.aggregate_expression, &bindings)?;
    Some(SharedRelationRewrite {
        scalar_binding: shape.scalar.scalar_binding,
        presence_binding: shape.scalar.presence_binding,
        scalar_source_binding: ColumnBinding::new(shape.scalar.aggregate.aggregate_index, 0),
        scalar_expression: shape.scalar.scalar_expression.clone(),
        aggregate,
        partitions,
        cte_index: inner_ref.cte_index,
        outer_table_index: outer_ref.table_index,
    })
}

fn collect_cte_refs<'a>(
    plan: &'a LogicalPlan,
    cte_index: usize,
    refs: &mut Vec<&'a paro_planner::operator::CTERef>,
) {
    if let LogicalOperator::CTERef(reference) = &plan.operator {
        if reference.cte_index == cte_index {
            refs.push(reference);
        }
    }
    for child in plan.children() {
        collect_cte_refs(child, cte_index, refs);
    }
}

/// Only layout-relative operators may carry a newly inserted window binding
/// from the shared leaf to the scalar filter. Positional projections and
/// reductions deliberately terminate this optimization domain.
fn shared_path_is_extensible(plan: &LogicalPlan, cte_index: usize, table_index: usize) -> bool {
    if matches!(&plan.operator, LogicalOperator::CTERef(reference)
        if reference.cte_index == cte_index && reference.table_index == table_index)
    {
        return true;
    }
    match &plan.operator {
        LogicalOperator::Filter(filter) if filter.projection_map.is_all() => {
            shared_path_is_extensible(&filter.child, cte_index, table_index)
        }
        LogicalOperator::Join(Join::Comparison(join)) => {
            let left = shared_path_is_extensible(&join.left, cte_index, table_index);
            let right = shared_path_is_extensible(&join.right, cte_index, table_index);
            match (left, right) {
                (true, false) => join.left_projection_map.is_all(),
                (false, true) => join.right_projection_map.is_all(),
                _ => false,
            }
        }
        LogicalOperator::Join(Join::Cross(cross)) => {
            shared_path_is_extensible(&cross.left, cte_index, table_index)
                ^ shared_path_is_extensible(&cross.right, cte_index, table_index)
        }
        _ => false,
    }
}

pub(super) fn apply_shared_relation_rewrite(
    plan: LogicalPlan,
    rewrite: SharedRelationRewrite,
    bind_context: &BindContext,
) -> Result<LogicalPlan> {
    let LogicalOperator::Filter(mut filter) = plan.operator else {
        return Err(paro_error::internal(
            "shared-relation partition witness no longer points to a Filter",
        ));
    };
    let LogicalOperator::Join(Join::Comparison(mut join)) = filter.child.operator else {
        return Err(paro_error::internal(
            "shared-relation partition witness lost its scalar join",
        ));
    };
    let detail = std::mem::replace(
        &mut *join.left,
        LogicalPlan::synthetic(LogicalOperator::DummyScan),
    );
    let window_index = bind_context.generate_table_index();
    let window_binding = ColumnBinding::new(window_index, 0);
    let window_type = rewrite.aggregate.return_type.clone();
    let frame = WindowFrame {
        frame_type: WindowFrameType::Rows,
        start_bound: WindowFrameBound::Unbounded,
        start_is_preceding: true,
        end_bound: WindowFrameBound::Unbounded,
        end_is_preceding: false,
    };
    let mut window_expression = Some(WindowExpression::aggregate(
        rewrite.aggregate,
        rewrite.partitions,
        Vec::new(),
        frame,
    ));
    window_expression
        .as_ref()
        .expect("window expression was just created")
        .verify_bound_contract()?;
    let (detail, inserted) = insert_partition_window(
        detail,
        rewrite.cte_index,
        rewrite.outer_table_index,
        window_index,
        &mut window_expression,
        bind_context,
    )?;
    if !inserted {
        return Err(paro_error::internal(
            "shared-relation partition witness lost its outer reference",
        ));
    }

    let scalar = rewrite.scalar_expression.replace_column_ref(&|column| {
        (column.depth == 0 && column.binding == rewrite.scalar_source_binding).then(|| {
            Expression::ColumnRef(ColumnRefExpression::new(
                window_binding,
                window_type.clone(),
            ))
        })
    });
    if !expression_uses_only_binding(&scalar, window_binding) {
        return Err(paro_error::internal(
            "shared-relation partition scalar contains an unexpected binding",
        ));
    }
    filter.expressions = filter
        .expressions
        .into_iter()
        .map(|expression| {
            expression.replace_column_ref(&|column| {
                if column.binding == rewrite.scalar_binding {
                    Some(scalar.clone())
                } else if Some(column.binding) == rewrite.presence_binding {
                    Some(scalar_presence_true())
                } else {
                    None
                }
            })
        })
        .collect();
    filter.child = Box::new(detail);
    filter.projection_map = paro_planner::operator::ProjectionMap::all();
    Ok(LogicalPlan::new(
        bind_context,
        LogicalOperator::Filter(filter),
    ))
}

fn insert_partition_window(
    plan: LogicalPlan,
    cte_index: usize,
    table_index: usize,
    window_index: usize,
    window_expression: &mut Option<WindowExpression>,
    bind_context: &BindContext,
) -> Result<(LogicalPlan, bool)> {
    if matches!(&plan.operator, LogicalOperator::CTERef(reference)
        if reference.cte_index == cte_index && reference.table_index == table_index)
    {
        return Ok((
            LogicalPlan::new(
                bind_context,
                LogicalOperator::Window(Window::new(
                    window_index,
                    vec![window_expression.take().ok_or_else(|| {
                        paro_error::internal("shared-relation window expression was consumed twice")
                    })?],
                    plan,
                )),
            ),
            true,
        ));
    }
    let mut inserted = false;
    let plan = plan.try_map_children(|child| {
        if inserted {
            return Ok(child);
        }
        let (child, child_inserted) = insert_partition_window(
            child,
            cte_index,
            table_index,
            window_index,
            window_expression,
            bind_context,
        )?;
        inserted = child_inserted;
        Ok(child)
    })?;
    Ok((plan, inserted))
}
