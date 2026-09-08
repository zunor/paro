// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Binding-based planner semantics stored by the Memo.
//!
//! Logical payloads contain canonical operator semantics, never the positional
//! layout selected by an earlier planner tree. Rules materialize these nodes
//! directly. Winner extraction freezes projection maps from the selected
//! group's output columns; physical slot assignment happens later.

use super::*;

pub(super) fn canonical_template(
    mut plan: paro_planner::plan::arena::LogicalPlanNode<()>,
) -> paro_planner::plan::arena::LogicalPlanNode<()> {
    canonicalize_projection_maps(&mut plan.operator);
    plan.stats = NodeStats::default();
    plan
}

pub(super) struct InstantiatedPlanWithGroupHoles {
    pub(super) plan: OwnedLogicalPlan,
    /// Plan-node identities are transport labels only. Staging consumes every
    /// entry and substitutes the named Memo group before publishing a logical
    /// expression, so the representative subtree can never become semantics.
    pub(super) group_holes: BTreeMap<paro_planner::operator::BoundReferenceId, GroupId>,
}

/// Instantiate the operator shells explicitly consumed by a pattern while
/// retaining opaque group operands as staging boundaries.
///
/// A [`paro_planner::operator::BoundReference`] supplies only the binding,
/// type, and cardinality contract consumed by legacy semantic code. It cannot
/// be implemented and the returned hole map replaces it with the original
/// Memo group atomically before the transformed expression is published.
pub(super) fn instantiate_bound_plan_with_group_holes(
    memo: &Memo,
    state: &PlannerTransformState,
    binding: &PatternOperand,
    facts: Option<&boundary::BoundarySnapshot>,
) -> Result<InstantiatedPlanWithGroupHoles> {
    fn group_hole_transport(
        state: &PlannerTransformState,
        layout: &PlannerBindingLayout,
        cardinality: Option<(u64, u64, u64)>,
        facts: Option<Arc<paro_planner::operator::bound_reference::BoundRelationFacts>>,
    ) -> Result<OwnedLogicalPlan> {
        if layout.bindings.len() != layout.types.len() {
            return Err(paro_error::internal(
                "group-hole binding/type layout has inconsistent arity",
            ));
        }
        let reference_id = paro_planner::operator::BoundReferenceId::group_hole(
            state.bind_context.next_plan_id().0,
        );
        let mut reference = paro_planner::operator::BoundReference::new(
            reference_id,
            layout.bindings.to_vec(),
            layout.types.to_vec(),
        );
        if let Some(facts) = facts {
            reference = reference.with_facts(facts);
        }
        let unique_keys = reference.facts.unique_keys.clone();
        let mut plan = OwnedLogicalPlan::new(
            &state.bind_context,
            LogicalOperator::BoundReference(reference),
        );
        plan.stats.unique_keys = unique_keys;
        debug_assert!(
            matches!(reference_id, paro_planner::operator::BoundReferenceId::GroupHole(id) if id != paro_planner::plan::PlanNodeId::SYNTHETIC.0)
        );
        plan.stats.estimated_cardinality =
            cardinality.map(
                |(min, expected, max)| paro_planner::plan::CardinalityEstimate {
                    min,
                    expected,
                    max,
                },
            );
        Ok(plan)
    }

    let mut group_holes = BTreeMap::new();
    let mut pending = vec![(binding, None, false)];
    let mut completed = Vec::new();
    while let Some((binding, expected_layout, finish)) = pending.pop() {
        match binding {
            PatternOperand::Group(group) => {
                let group = memo.canonical_group(*group);
                let layout = expected_layout.ok_or_else(|| {
                    paro_error::internal("root Memo group cannot be an untyped pattern hole")
                })?;
                let cardinality = facts.and_then(|facts| facts.cardinality(memo, group));
                let transport = facts
                    .map(|facts| facts.transport(memo, state, group, layout))
                    .transpose()?;
                let plan = group_hole_transport(
                    state,
                    layout,
                    cardinality.map(|range| (range.min, range.expected, range.max)),
                    transport,
                )?;
                let LogicalOperator::BoundReference(reference) = &plan.operator else {
                    unreachable!("group-hole transport constructor returned another operator")
                };
                if group_holes.insert(reference.reference_id, group).is_some() {
                    return Err(paro_error::internal(
                        "group-hole transport reused a planner node identity",
                    ));
                }
                completed.push(plan);
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
                let metadata = state.metadata.get(&logical.payload).ok_or_else(|| {
                    paro_error::internal("planner rule payload has no operator metadata")
                })?;
                if children.len() != logical.key.children.len() {
                    return Err(paro_error::internal(
                        "pattern binding child arity disagrees with its logical expression",
                    ));
                }
                if !finish {
                    pending.push((binding, expected_layout, true));
                    pending.extend(
                        children.iter().enumerate().rev().map(|(index, child)| {
                            (child, metadata.child_layouts.get(index), false)
                        }),
                    );
                    continue;
                }
                let start = completed
                    .len()
                    .checked_sub(children.len())
                    .ok_or_else(|| paro_error::internal("bound shell lost a completed child"))?;
                let mut plan = payload.semantic_template.instantiate(
                    state.bind_context.next_plan_id(),
                    completed.split_off(start),
                )?;
                plan.stats.estimated_cardinality =
                    facts.and_then(|facts| facts.cardinality(memo, *group));
                let output_columns = metadata.output_columns.clone();
                let mut plan = freeze_output_layout(plan, &output_columns, state)?;
                let child_layouts = plan
                    .children()
                    .iter()
                    .map(|child| child.output_layout())
                    .collect::<Vec<_>>();
                let layout = plan.operator.output_layout_from_children(&child_layouts);
                plan.stats.unique_keys = crate::statistics::unique_keys::derive_local_unique_keys(
                    &plan.operator,
                    &layout,
                    &child_layouts,
                );
                completed.push(plan);
            }
        }
    }

    if completed.len() != 1 {
        return Err(paro_error::internal(
            "bound shell assembly has no unique root",
        ));
    }
    let plan = completed.pop().unwrap();
    Ok(InstantiatedPlanWithGroupHoles { plan, group_holes })
}

/// Restore the occurrence's output column set after materializing a canonical
/// Memo template. Unary projection maps retain the requested order. Joins
/// retain their natural left/right partition because Memo schemas are
/// unordered ColumnId sets; parents and final presentation map identities to
/// slots after winner selection.
pub(super) fn freeze_arena_output_layout(
    mut plan: paro_planner::plan::LogicalPlan,
    output_columns: &[ColumnId],
    state: &PlannerTransformState,
) -> Result<paro_planner::plan::LogicalPlan> {
    use paro_planner::plan::arena::LogicalPlanNode;
    let root = plan.root_node().clone();
    let mut edges = Vec::new();
    let operator = root.operator.try_map_child_links(&mut |child| {
        edges.push(child);
        let layout = plan.arena().output_layout(child)?;
        let reference = paro_planner::operator::BoundReference::new(
            paro_planner::operator::BoundReferenceId::frozen_output(),
            layout.bindings().to_vec(),
            layout.types().to_vec(),
        );
        Ok::<_, paro_common::error::ParoError>(Box::new(OwnedLogicalPlan {
            id: plan.arena().get(child)?.id,
            stats: plan.arena().get(child)?.stats.clone(),
            operator: LogicalOperator::BoundReference(reference),
        }))
    })?;
    let frozen = freeze_output_layout(
        OwnedLogicalPlan {
            id: root.id,
            stats: root.stats,
            operator,
        },
        output_columns,
        state,
    )?;
    let shell = LogicalPlanNode::from_shell(frozen);
    let mut edges = edges.into_iter();
    let operator = shell.operator.try_map_child_links(&mut |_| {
        edges
            .next()
            .ok_or_else(|| paro_error::internal("freezing output changed the input arity"))
    })?;
    if edges.next().is_some() {
        return Err(paro_error::internal("freezing output dropped an input"));
    }
    plan.append_root(LogicalPlanNode {
        id: shell.id,
        stats: shell.stats,
        operator,
    })?;
    Ok(plan)
}

pub(super) fn freeze_output_layout(
    mut plan: OwnedLogicalPlan,
    output_columns: &[ColumnId],
    state: &PlannerTransformState,
) -> Result<OwnedLogicalPlan> {
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
        | LogicalOperator::BoundReference(_)
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

fn canonicalize_projection_maps<Child>(operator: &mut LogicalOperator<Child>) {
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
        | LogicalOperator::BoundReference(_)
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
    child: &OwnedLogicalPlan,
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
    left: &OwnedLogicalPlan,
    right: &OwnedLogicalPlan,
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

fn resolved_columns(plan: &OwnedLogicalPlan, bindings: &BindingCatalog) -> Result<Vec<ColumnId>> {
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
