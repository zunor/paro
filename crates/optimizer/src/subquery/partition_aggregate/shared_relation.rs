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
    source: SharedSource,
}

#[derive(Debug, Clone, Copy)]
enum SharedSource {
    Cte {
        cte_index: usize,
        table_index: usize,
    },
    Table {
        table_index: usize,
    },
}

impl SharedSource {
    fn matches(self, plan: &OwnedLogicalPlan) -> bool {
        match (self, &plan.operator) {
            (
                Self::Cte {
                    cte_index,
                    table_index,
                },
                LogicalOperator::CTERef(reference),
            ) => reference.cte_index == cte_index && reference.table_index == table_index,
            (Self::Table { table_index }, LogicalOperator::Get(get)) => {
                get.table_index == table_index
            }
            _ => false,
        }
    }
}

/// Reuse one materialized relation snapshot for both the detail row and its
/// complete per-key aggregate. The window is inserted immediately above the
/// outer shared-relation leaf, before filtering or dimension joins, so it
/// observes exactly the same relation domain as the scalar branch.
pub(super) fn recognize_shared_relation_filter(
    plan: &OwnedLogicalPlan,
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
    // Replacing equality correlation with a SQL partition groups NULL keys
    // together, while the original scalar subquery observes an empty input for
    // every NULL outer key. A strict predicate over a NULL-on-empty aggregate
    // lets us restore that distinction with explicit key guards.
    if !shape.filter.expressions.iter().any(|expression| {
        filter_rejects_null_scalar(
            expression,
            shape.scalar.scalar_binding,
            shape.scalar.presence_binding,
        )
    }) || !matches!(
        shape.scalar.aggregate_expression.function.empty_input,
        AggregateEmptyInput::Null
    ) {
        return None;
    }

    let (bindings, source) = match &inner_source.operator {
        LogicalOperator::CTERef(inner_ref) => {
            let mut outer_refs = Vec::new();
            collect_cte_refs(&shape.join.left, inner_ref.cte_index, &mut outer_refs);
            let [outer_ref] = outer_refs.as_slice() else {
                return None;
            };
            if inner_ref.column_types != outer_ref.column_types {
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
            (
                bindings,
                SharedSource::Cte {
                    cte_index: inner_ref.cte_index,
                    table_index: outer_ref.table_index,
                },
            )
        }
        LogicalOperator::Get(inner_get) => {
            if inner_get.scan_order.is_some() || !inner_get.runtime_filter_expressions.is_empty() {
                return None;
            }
            let mut outer_gets = Vec::new();
            collect_gets(&shape.join.left, &mut outer_gets);
            let mut candidates = outer_gets
                .into_iter()
                .filter_map(|outer_get| {
                    if outer_get.scan_order.is_some()
                        || !outer_get.runtime_filter_expressions.is_empty()
                    {
                        return None;
                    }
                    let mut bindings = BindingMap::default();
                    bind_scan_columns(inner_get, outer_get, &mut bindings)?;
                    if !shape
                        .correlation
                        .inner_keys
                        .iter()
                        .zip(&shape.join.duplicate_eliminated_columns)
                        .all(|(inner, outer)| semantic_expression_equal(inner, outer, &bindings))
                    {
                        return None;
                    }
                    Some((
                        bindings,
                        SharedSource::Table {
                            table_index: outer_get.table_index,
                        },
                    ))
                })
                .collect::<Vec<_>>()
                .into_iter();
            let candidate = candidates.next()?;
            if candidates.next().is_some() {
                return None;
            }
            candidate
        }
        _ => return None,
    };
    if !shared_path_is_extensible(&shape.join.left, source) {
        return None;
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
        source,
    })
}

fn collect_cte_refs<'a>(
    plan: &'a OwnedLogicalPlan,
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

fn collect_gets<'a>(plan: &'a OwnedLogicalPlan, gets: &mut Vec<&'a Get>) {
    if let LogicalOperator::Get(get) = &plan.operator {
        gets.push(get);
    }
    for child in plan.children() {
        collect_gets(child, gets);
    }
}

/// Only layout-relative operators may carry a newly inserted window binding
/// from the shared leaf to the scalar filter. Positional projections and
/// reductions deliberately terminate this optimization domain.
fn shared_path_is_extensible(plan: &OwnedLogicalPlan, source: SharedSource) -> bool {
    if source.matches(plan) {
        return true;
    }
    match &plan.operator {
        LogicalOperator::Filter(filter) if filter.projection_map.is_all() => {
            shared_path_is_extensible(&filter.child, source)
        }
        LogicalOperator::Join(Join::Comparison(join)) if clean_inner_join(join) => {
            let left = shared_path_is_extensible(&join.left, source);
            let right = shared_path_is_extensible(&join.right, source);
            match (left, right) {
                (true, false) => join.left_projection_map.is_all(),
                (false, true) => join.right_projection_map.is_all(),
                _ => false,
            }
        }
        LogicalOperator::Join(Join::Cross(cross)) => {
            shared_path_is_extensible(&cross.left, source)
                ^ shared_path_is_extensible(&cross.right, source)
        }
        _ => false,
    }
}

pub(super) fn apply_shared_relation_rewrite(
    plan: OwnedLogicalPlan,
    rewrite: SharedRelationRewrite,
    bind_context: &BindContext,
) -> Result<OwnedLogicalPlan> {
    let LogicalOperator::Filter(mut filter) = plan.into_operator() else {
        return Err(paro_error::internal(
            "shared-relation partition witness no longer points to a Filter",
        ));
    };
    let LogicalOperator::Join(Join::Comparison(mut join)) = (*filter.child).into_operator() else {
        return Err(paro_error::internal(
            "shared-relation partition witness lost its scalar join",
        ));
    };
    let detail = std::mem::replace(
        &mut *join.left,
        OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
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
    let null_guards = rewrite
        .partitions
        .iter()
        .cloned()
        .map(|partition| {
            Expression::Operator(OperatorExpression::new_unary(
                OperatorType::IsNotNull,
                partition,
                paro_common::types::LogicalType::Boolean,
            ))
        })
        .collect::<Vec<_>>();
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
    filter.expressions.extend(null_guards);
    let mut required = HashSet::new();
    for expression in &filter.expressions {
        ExpressionIterator::visit(expression, &mut |candidate| {
            if let Expression::ColumnRef(column) = candidate {
                if column.depth == 0 {
                    required.insert(column.binding);
                }
                ExpressionVisitDecision::Descend
            } else {
                ExpressionVisitDecision::Descend
            }
        });
    }
    if !required.contains(&window_binding) {
        return Err(paro_error::internal(
            "shared-relation partition filter lost its window dependency",
        ));
    }

    let (detail, inserted) = insert_partition_window(
        detail,
        rewrite.source,
        window_index,
        &mut window_expression,
        bind_context,
    )?;
    if !inserted {
        return Err(paro_error::internal(
            "shared-relation partition witness lost its outer reference",
        ));
    }
    let target_id = smallest_filter_owner(&detail, &required);
    let mut expressions = Some(filter.expressions);
    let mut localized = false;
    let detail = detail.try_map_post_order(|target| {
        if localized || target.id != target_id {
            return Ok(target);
        }
        localized = true;
        Ok(OwnedLogicalPlan::new(
            bind_context,
            LogicalOperator::Filter(paro_planner::operator::Filter {
                expressions: expressions.take().ok_or_else(|| {
                    paro_error::internal(
                        "shared-relation partition filter was consumed more than once",
                    )
                })?,
                child: Box::new(target),
                projection_map: paro_planner::operator::ProjectionMap::all(),
            }),
        ))
    })?;
    if !localized {
        return Err(paro_error::internal(
            "shared-relation partition filter owner disappeared after recognition",
        ));
    }
    Ok(detail)
}

fn insert_partition_window(
    plan: OwnedLogicalPlan,
    source: SharedSource,
    window_index: usize,
    window_expression: &mut Option<WindowExpression>,
    bind_context: &BindContext,
) -> Result<(OwnedLogicalPlan, bool)> {
    if source.matches(&plan) {
        return Ok((
            OwnedLogicalPlan::new(
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
        let (child, child_inserted) =
            insert_partition_window(child, source, window_index, window_expression, bind_context)?;
        inserted = child_inserted;
        Ok(child)
    })?;
    Ok((plan, inserted))
}
