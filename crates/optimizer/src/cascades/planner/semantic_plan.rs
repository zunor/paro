// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Binding-based planner semantics stored by the Memo.
//!
//! Logical payloads contain canonical operator semantics, never the positional
//! layout selected by an earlier planner tree. Rules materialize these nodes
//! directly. Winner extraction freezes projection maps from the selected
//! group's output columns; physical slot assignment happens later.

use super::*;

pub(super) fn detach_template(mut plan: LogicalPlan) -> LogicalPlan {
    canonicalize_projection_maps(&mut plan.operator);
    plan.stats = NodeStats::default();
    plan
}

pub(super) fn instantiate_bound_plan(
    memo: &Memo,
    state: &PlannerTransformState,
    binding: &PatternOperand,
) -> Result<LogicalPlan> {
    instantiate_bound_plan_raw(memo, state, binding)
}

pub(super) struct InstantiatedPlanWithGroupHoles {
    pub(super) plan: LogicalPlan,
    /// Plan-node identities are transport labels only. Staging consumes every
    /// entry and substitutes the named Memo group before publishing a logical
    /// expression, so the representative subtree can never become semantics.
    pub(super) group_holes: BTreeMap<u32, GroupId>,
}

/// Instantiate the operator shells explicitly consumed by a pattern while
/// retaining opaque group operands as staging boundaries.
///
/// The temporary representative below a hole supplies the planner adapter's
/// binding layout to legacy semantic code. Its descendants are never staged,
/// costed, or used as an equivalence choice: the returned hole map replaces
/// the complete subtree with the original Memo group atomically.
pub(super) fn instantiate_bound_plan_with_group_holes(
    memo: &Memo,
    state: &PlannerTransformState,
    binding: &PatternOperand,
) -> Result<InstantiatedPlanWithGroupHoles> {
    fn materialize_group(
        memo: &Memo,
        state: &PlannerTransformState,
        group: GroupId,
        active: &mut BTreeSet<GroupId>,
    ) -> Result<LogicalPlan> {
        let group = memo.canonical_group(group);
        if !active.insert(group) {
            return Err(paro_error::internal(
                "recursive Memo group cannot be used as a finite planner transport",
            ));
        }
        let group_ref = memo
            .group(group)
            .ok_or_else(|| paro_error::internal("group hole references an unknown Memo group"))?;
        let expression = group_ref
            .logical_exprs()
            .iter()
            .copied()
            .min_by_key(|expression| {
                memo.logical_expr(*expression)
                    .map(|logical| logical.key.stable_fingerprint())
                    .unwrap_or_default()
            })
            .ok_or_else(|| paro_error::internal("group hole references an empty Memo group"))?;
        let logical = memo
            .logical_expr(expression)
            .ok_or_else(|| paro_error::internal("group hole lost its logical expression"))?;
        let payload = state
            .payloads
            .logical
            .get(logical.payload.index())
            .ok_or_else(|| paro_error::internal("group hole lost its planner payload"))?;
        let children = logical
            .key
            .children
            .iter()
            .copied()
            .map(|child| materialize_group(memo, state, child, active))
            .collect::<Result<Vec<_>>>()?;
        active.remove(&group);
        let mut children = children.into_iter();
        let mut plan = duplicate_plan_preserving_indices(
            &payload.semantic_template,
            state.bind_context.shared().as_ref(),
        )
        .try_map_children(|_| {
            children
                .next()
                .ok_or_else(|| paro_error::internal("group-hole transport lost a child expression"))
        })?;
        if children.next().is_some() {
            return Err(paro_error::internal(
                "group-hole transport produced an extra child expression",
            ));
        }
        plan.stats.estimated_cardinality =
            memo.cardinality_estimate(group)
                .map(
                    |(min, expected, max)| paro_planner::plan::CardinalityEstimate {
                        min,
                        expected,
                        max,
                    },
                );
        let output_columns = state
            .metadata
            .get(&logical.payload)
            .ok_or_else(|| paro_error::internal("group hole has no operator metadata"))?
            .output_columns
            .clone();
        freeze_output_layout(plan, &output_columns, state)
    }

    fn instantiate(
        memo: &Memo,
        state: &PlannerTransformState,
        binding: &PatternOperand,
        holes: &mut BTreeMap<u32, GroupId>,
    ) -> Result<LogicalPlan> {
        match binding {
            PatternOperand::Group(group) => {
                let group = memo.canonical_group(*group);
                let plan = materialize_group(memo, state, group, &mut BTreeSet::new())?;
                if plan.id == paro_planner::plan::PlanNodeId::SYNTHETIC {
                    return Err(paro_error::internal(
                        "group-hole transport requires a stable planner node identity",
                    ));
                }
                if holes.insert(plan.id.0, group).is_some() {
                    return Err(paro_error::internal(
                        "group-hole transport reused a planner node identity",
                    ));
                }
                Ok(plan)
            }
            PatternOperand::Expression {
                group,
                expression,
                children,
            } => {
                let logical = memo.logical_expr(*expression).ok_or_else(|| {
                    paro_error::internal("planner rule references unknown expression")
                })?;
                let payload = state
                    .payloads
                    .logical
                    .get(logical.payload.index())
                    .ok_or_else(|| {
                        paro_error::internal("planner rule references unknown payload")
                    })?;
                if children.len() != logical.key.children.len() {
                    return Err(paro_error::internal(
                        "pattern binding child arity disagrees with its logical expression",
                    ));
                }
                let bound_children = children
                    .iter()
                    .map(|child| instantiate(memo, state, child, holes))
                    .collect::<Result<Vec<_>>>()?;
                let mut bound_children = bound_children.into_iter();
                let mut plan = duplicate_plan_preserving_indices(
                    &payload.semantic_template,
                    state.bind_context.shared().as_ref(),
                )
                .try_map_children(|_| {
                    bound_children.next().ok_or_else(|| {
                        paro_error::internal("planner payload lost a child expression")
                    })
                })?;
                if bound_children.next().is_some() {
                    return Err(paro_error::internal(
                        "planner payload child arity disagrees with Memo expression",
                    ));
                }
                plan.stats.estimated_cardinality =
                    memo.cardinality_estimate(*group)
                        .map(
                            |(min, expected, max)| paro_planner::plan::CardinalityEstimate {
                                min,
                                expected,
                                max,
                            },
                        );
                let output_columns = state
                    .metadata
                    .get(&logical.payload)
                    .ok_or_else(|| {
                        paro_error::internal("planner rule payload has no operator metadata")
                    })?
                    .output_columns
                    .clone();
                freeze_output_layout(plan, &output_columns, state)
            }
        }
    }

    let mut group_holes = BTreeMap::new();
    let plan = instantiate(memo, state, binding, &mut group_holes)?;
    Ok(InstantiatedPlanWithGroupHoles { plan, group_holes })
}

fn instantiate_bound_plan_raw(
    memo: &Memo,
    state: &PlannerTransformState,
    binding: &PatternOperand,
) -> Result<LogicalPlan> {
    let PatternOperand::Expression {
        group,
        expression: expr,
        children: bound_children,
    } = binding
    else {
        return Err(paro_error::internal(
            "planner-plan instantiation reached an unconsumed Memo group hole",
        ));
    };
    let logical = memo
        .logical_expr(*expr)
        .ok_or_else(|| paro_error::internal("planner rule references unknown expression"))?;
    let payload = state
        .payloads
        .logical
        .get(logical.payload.index())
        .ok_or_else(|| paro_error::internal("planner rule references unknown payload"))?;
    if bound_children.len() != logical.key.children.len() {
        return Err(paro_error::internal(
            "pattern binding child arity disagrees with its logical expression",
        ));
    }
    let children = bound_children
        .iter()
        .map(|child| instantiate_bound_plan_raw(memo, state, child))
        .collect::<Result<Vec<_>>>()?;
    let mut children = children.into_iter();
    let mut plan = duplicate_plan_preserving_indices(
        &payload.semantic_template,
        state.bind_context.shared().as_ref(),
    )
    .try_map_children(|_| {
        children
            .next()
            .ok_or_else(|| paro_error::internal("planner payload lost a child expression"))
    })?;
    if children.next().is_some() {
        return Err(paro_error::internal(
            "planner payload child arity disagrees with Memo expression",
        ));
    }
    plan.stats.estimated_cardinality = memo
        .cardinality_estimate(*group)
        .map(|(min, expected, max)| paro_planner::plan::CardinalityEstimate { min, expected, max });
    let output_columns = state
        .metadata
        .get(&logical.payload)
        .ok_or_else(|| paro_error::internal("planner rule payload has no operator metadata"))?
        .output_columns
        .clone();
    freeze_output_layout(plan, &output_columns, state)
}

/// Restore the occurrence's output column set after materializing a canonical
/// Memo template. Unary projection maps retain the requested order. Joins
/// retain their natural left/right partition because Memo schemas are
/// unordered ColumnId sets; parents and final presentation map identities to
/// slots after winner selection.
pub(super) fn freeze_output_layout(
    mut plan: LogicalPlan,
    output_columns: &[ColumnId],
    state: &PlannerTransformState,
) -> Result<LogicalPlan> {
    match &mut plan.operator {
        LogicalOperator::Filter(filter) => {
            filter.projection_map =
                projection_for_columns(&filter.child, output_columns, &state.binding_ids)?;
        }
        LogicalOperator::Order(order) => {
            order.projection_map =
                projection_for_columns(&order.child, output_columns, &state.binding_ids)?;
        }
        LogicalOperator::TopN(topn) => {
            topn.projection_map =
                projection_for_columns(&topn.child, output_columns, &state.binding_ids)?;
        }
        LogicalOperator::Join(Join::Comparison(join)) => {
            let marker = marker_column(join.mark_index, &state.binding_ids)?;
            let (left, right) = join_projections(
                &join.left,
                &join.right,
                output_columns,
                marker,
                &state.binding_ids,
            )?;
            join.left_projection_map = left;
            join.right_projection_map = right;
        }
        LogicalOperator::Join(Join::Any(join)) => {
            let marker = marker_column(join.mark_index, &state.binding_ids)?;
            let (left, right) = join_projections(
                &join.left,
                &join.right,
                output_columns,
                marker,
                &state.binding_ids,
            )?;
            join.left_projection_map = left;
            join.right_projection_map = right;
        }
        LogicalOperator::FullTextFilterScan(scan) => {
            scan.projection_map = projection_for_bindings(
                &LogicalOperator::generate_column_bindings(
                    scan.get.table_index,
                    scan.get.returned_types.len(),
                ),
                &scan.get.returned_types,
                output_columns,
                &state.binding_ids,
            )?;
        }
        LogicalOperator::Join(Join::Cross(_))
        | LogicalOperator::Get(_)
        | LogicalOperator::Projection(_)
        | LogicalOperator::RowFetch(_)
        | LogicalOperator::ExternalProject(_)
        | LogicalOperator::ExternalTable(_)
        | LogicalOperator::Limit(_)
        | LogicalOperator::CreateTable(_)
        | LogicalOperator::CreateRoutine(_)
        | LogicalOperator::Alter(_)
        | LogicalOperator::CreateSequence(_)
        | LogicalOperator::CreateSchema(_)
        | LogicalOperator::CreateIndex(_)
        | LogicalOperator::CreateView(_)
        | LogicalOperator::Drop(_)
        | LogicalOperator::CreatePropertyGraph(_)
        | LogicalOperator::DropPropertyGraph(_)
        | LogicalOperator::RefreshPropertyGraph(_)
        | LogicalOperator::Aggregate(_)
        | LogicalOperator::Insert(_)
        | LogicalOperator::Delete(_)
        | LogicalOperator::Update(_)
        | LogicalOperator::ExpressionGet(_)
        | LogicalOperator::DelimGet(_)
        | LogicalOperator::DependentJoin(_)
        | LogicalOperator::SetOperation(_)
        | LogicalOperator::Distinct(_)
        | LogicalOperator::Window(_)
        | LogicalOperator::Explain(_)
        | LogicalOperator::EmptyResult(_)
        | LogicalOperator::MaterializedCTE(_)
        | LogicalOperator::RecursiveCTE(_)
        | LogicalOperator::CTERef(_)
        | LogicalOperator::TableFunctionGet(_)
        | LogicalOperator::SearchScan(_)
        | LogicalOperator::CopyTo(_)
        | LogicalOperator::GraphMatch(_)
        | LogicalOperator::GraphScan(_)
        | LogicalOperator::GraphExpand(_)
        | LogicalOperator::DummyScan => {}
    }
    Ok(plan)
}

fn canonicalize_projection_maps(operator: &mut LogicalOperator) {
    match operator {
        LogicalOperator::Filter(filter) => {
            filter.projection_map = paro_planner::operator::ProjectionMap::all();
        }
        LogicalOperator::Order(order) => {
            order.projection_map = paro_planner::operator::ProjectionMap::all();
        }
        LogicalOperator::TopN(topn) => {
            topn.projection_map = paro_planner::operator::ProjectionMap::all();
        }
        LogicalOperator::Join(Join::Comparison(join)) => {
            (join.left_projection_map, join.right_projection_map) =
                paro_planner::operator::default_join_projections(join.join_type);
        }
        LogicalOperator::Join(Join::Any(join)) => {
            (join.left_projection_map, join.right_projection_map) =
                paro_planner::operator::default_join_projections(join.join_type);
        }
        LogicalOperator::FullTextFilterScan(scan) => {
            scan.projection_map = paro_planner::operator::ProjectionMap::all();
        }
        LogicalOperator::Join(Join::Cross(_))
        | LogicalOperator::Get(_)
        | LogicalOperator::Projection(_)
        | LogicalOperator::RowFetch(_)
        | LogicalOperator::ExternalProject(_)
        | LogicalOperator::ExternalTable(_)
        | LogicalOperator::Limit(_)
        | LogicalOperator::CreateTable(_)
        | LogicalOperator::CreateRoutine(_)
        | LogicalOperator::Alter(_)
        | LogicalOperator::CreateSequence(_)
        | LogicalOperator::CreateSchema(_)
        | LogicalOperator::CreateIndex(_)
        | LogicalOperator::CreateView(_)
        | LogicalOperator::Drop(_)
        | LogicalOperator::CreatePropertyGraph(_)
        | LogicalOperator::DropPropertyGraph(_)
        | LogicalOperator::RefreshPropertyGraph(_)
        | LogicalOperator::Aggregate(_)
        | LogicalOperator::Insert(_)
        | LogicalOperator::Delete(_)
        | LogicalOperator::Update(_)
        | LogicalOperator::ExpressionGet(_)
        | LogicalOperator::DelimGet(_)
        | LogicalOperator::DependentJoin(_)
        | LogicalOperator::SetOperation(_)
        | LogicalOperator::Distinct(_)
        | LogicalOperator::Window(_)
        | LogicalOperator::Explain(_)
        | LogicalOperator::EmptyResult(_)
        | LogicalOperator::MaterializedCTE(_)
        | LogicalOperator::RecursiveCTE(_)
        | LogicalOperator::CTERef(_)
        | LogicalOperator::TableFunctionGet(_)
        | LogicalOperator::SearchScan(_)
        | LogicalOperator::CopyTo(_)
        | LogicalOperator::GraphMatch(_)
        | LogicalOperator::GraphScan(_)
        | LogicalOperator::GraphExpand(_)
        | LogicalOperator::DummyScan => {}
    }
}

fn projection_for_columns(
    child: &LogicalPlan,
    output_columns: &[ColumnId],
    bindings: &BindingCatalog,
) -> Result<paro_planner::operator::ProjectionMap> {
    projection_for_bindings(
        &child.get_column_bindings(),
        &child.types(),
        output_columns,
        bindings,
    )
}

fn projection_for_bindings(
    input_bindings: &[ColumnBinding],
    input_types: &[paro_common::types::LogicalType],
    output_columns: &[ColumnId],
    bindings: &BindingCatalog,
) -> Result<paro_planner::operator::ProjectionMap> {
    if input_bindings.len() != input_types.len() {
        return Err(paro_error::internal(
            "extraction input binding/type arity mismatch",
        ));
    }
    let input_columns = input_bindings
        .iter()
        .copied()
        .zip(input_types)
        .map(|(binding, logical_type)| {
            bindings
                .get(binding.table_index, binding.column_index, logical_type)
                .copied()
                .ok_or_else(|| {
                    paro_error::internal(
                        "extraction layout references a binding outside the Query IR catalog",
                    )
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let indices = output_columns
        .iter()
        .map(|output| {
            input_columns
                .iter()
                .position(|candidate| candidate == output)
                .ok_or_else(|| {
                    paro_error::internal(
                        "selected group output is absent from its semantic child layout",
                    )
                })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(if indices.iter().copied().eq(0..input_columns.len()) {
        paro_planner::operator::ProjectionMap::all()
    } else {
        paro_planner::operator::ProjectionMap::new(indices)
    })
}

fn join_projections(
    left: &LogicalPlan,
    right: &LogicalPlan,
    output_columns: &[ColumnId],
    marker: Option<ColumnId>,
    bindings: &BindingCatalog,
) -> Result<(
    paro_planner::operator::ProjectionMap,
    paro_planner::operator::ProjectionMap,
)> {
    let left_columns = resolved_columns(left, bindings)?;
    let right_columns = resolved_columns(right, bindings)?;
    let mut left_indices = Vec::new();
    let mut right_indices = Vec::new();
    for output in output_columns {
        if let Some(index) = left_columns
            .iter()
            .position(|candidate| candidate == output)
        {
            left_indices.push(index);
        } else if let Some(index) = right_columns
            .iter()
            .position(|candidate| candidate == output)
        {
            right_indices.push(index);
        } else if Some(*output) != marker {
            return Err(paro_error::internal(
                "join group output is absent from both semantic children",
            ));
        }
    }
    Ok((
        exact_projection(left_indices, left_columns.len()),
        exact_projection(right_indices, right_columns.len()),
    ))
}

fn marker_column(mark_index: Option<usize>, bindings: &BindingCatalog) -> Result<Option<ColumnId>> {
    let Some(mark_index) = mark_index else {
        return Ok(None);
    };
    bindings
        .get(mark_index, 0, &paro_common::types::LogicalType::Boolean)
        .copied()
        .map(Some)
        .ok_or_else(|| {
            paro_error::internal("mark join output is absent from the Query IR binding catalog")
        })
}

fn resolved_columns(plan: &LogicalPlan, bindings: &BindingCatalog) -> Result<Vec<ColumnId>> {
    plan.get_column_bindings()
        .into_iter()
        .zip(plan.types())
        .map(|(binding, logical_type)| {
            bindings
                .get(binding.table_index, binding.column_index, &logical_type)
                .copied()
                .ok_or_else(|| {
                    paro_error::internal(
                        "join extraction references a binding outside the Query IR catalog",
                    )
                })
        })
        .collect()
}

fn exact_projection(
    indices: Vec<usize>,
    input_width: usize,
) -> paro_planner::operator::ProjectionMap {
    if indices.iter().copied().eq(0..input_width) {
        paro_planner::operator::ProjectionMap::all()
    } else {
        paro_planner::operator::ProjectionMap::new(indices)
    }
}
