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

pub(super) fn materialize(
    memo: &Memo,
    state: &PlannerTransformState,
    expr: LogicalExprId,
    semantic_dependencies: &[RuleId],
) -> Result<LogicalPlan> {
    materialize_raw(memo, state, expr, semantic_dependencies)
}

fn materialize_raw(
    memo: &Memo,
    state: &PlannerTransformState,
    expr: LogicalExprId,
    semantic_dependencies: &[RuleId],
) -> Result<LogicalPlan> {
    let logical = memo
        .logical_expr(expr)
        .ok_or_else(|| paro_error::internal("planner rule references unknown expression"))?;
    let payload = state
        .payloads
        .logical
        .get(logical.payload.index())
        .ok_or_else(|| paro_error::internal("planner rule references unknown payload"))?;
    let mut children = Vec::with_capacity(logical.key.children.len());
    for child in logical.key.children.iter().copied() {
        let child_expr = memo
            .group(child)
            .and_then(|group| {
                preferred_semantic_expression(group.logical_exprs(), memo, semantic_dependencies)
            })
            .ok_or_else(|| paro_error::internal("planner rule found an empty child group"))?;
        children.push(materialize_raw(
            memo,
            state,
            child_expr,
            semantic_dependencies,
        )?);
    }
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
    let owner = memo
        .logical_owner(expr)
        .ok_or_else(|| paro_error::internal("planner rule expression has no owning group"))?;
    plan.stats.estimated_cardinality = memo
        .cardinality_estimate(owner)
        .map(|(min, expected, max)| paro_planner::plan::CardinalityEstimate { min, expected, max });
    Ok(plan)
}

/// Pick a semantic representative from the dependencies declared by the
/// consuming rule. Declaration order is precedence order; the canonical
/// initial expression is the fallback. A fingerprint only disambiguates
/// multiple expressions produced by the same declared rule and never assigns
/// semantic quality across rules.
fn preferred_semantic_expression(
    expressions: &[LogicalExprId],
    memo: &Memo,
    semantic_dependencies: &[RuleId],
) -> Option<LogicalExprId> {
    let expression_for = |producer: Option<RuleId>| {
        expressions
            .iter()
            .copied()
            .filter(|expression| {
                let logical = memo
                    .logical_expr(*expression)
                    .expect("group expression must exist in the Memo");
                match producer {
                    Some(producer) => logical.proofs.iter().any(|proof| {
                        matches!(proof, EquivalenceProof::Transformation { rule, .. } if *rule == producer)
                    }),
                    None => logical.proofs.contains(&EquivalenceProof::Initial),
                }
            })
            .min_by_key(|expression| {
                memo.logical_expr(*expression)
                    .expect("group expression must exist in the Memo")
                    .key
                    .stable_fingerprint()
            })
    };

    semantic_dependencies
        .iter()
        .find_map(|dependency| expression_for(Some(*dependency)))
        .or_else(|| expression_for(None))
}

pub(super) fn freeze_extraction_layout(
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
        _ => {}
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
        _ => {}
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
            let domain = logical_type_fingerprint(logical_type);
            bindings
                .get(&(binding.table_index, binding.column_index, domain))
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
    let mut reached_right = false;
    for output in output_columns {
        if let Some(index) = left_columns
            .iter()
            .position(|candidate| candidate == output)
        {
            if reached_right {
                return Err(paro_error::internal(
                    "inner-join group output interleaves its child layouts",
                ));
            }
            left_indices.push(index);
        } else if let Some(index) = right_columns
            .iter()
            .position(|candidate| candidate == output)
        {
            reached_right = true;
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
    let domain = logical_type_fingerprint(&paro_common::types::LogicalType::Boolean);
    bindings
        .get(&(mark_index, 0, domain))
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
            let domain = logical_type_fingerprint(&logical_type);
            bindings
                .get(&(binding.table_index, binding.column_index, domain))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cascades::column::{ColumnDesc, ColumnOrigin, ColumnVisibility};

    #[test]
    fn semantic_representative_uses_declared_dependencies_not_rule_ids() {
        let schema = GroupSchema::new([ColumnDesc {
            id: ColumnId::new(0),
            logical_type: paro_common::types::LogicalType::Integer,
            nullable: false,
            origin: ColumnOrigin::Derived {
                key: Fingerprint(1),
            },
            visibility: ColumnVisibility::Visible,
            name_hint: None,
        }])
        .unwrap();
        let mut memo = Memo::new(SearchBudget::default());
        let group = memo.create_group(
            schema,
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        let insert = |memo: &mut Memo, operator: u128, payload: usize, proof: EquivalenceProof| {
            memo.insert_logical(
                group,
                LogicalExprKey {
                    operator: Fingerprint(operator),
                    scalars: Box::new([]),
                    children: Box::new([]),
                },
                LogicalPayloadId::new(payload),
                proof,
            )
            .unwrap()
        };
        let initial = insert(&mut memo, 1, 0, EquivalenceProof::Initial);
        let declared_rule = RuleId(2);
        let declared = insert(
            &mut memo,
            2,
            1,
            EquivalenceProof::Transformation {
                rule: declared_rule,
                source: initial,
                premise: Fingerprint(1),
            },
        );
        insert(
            &mut memo,
            3,
            2,
            EquivalenceProof::Transformation {
                rule: RuleId(u32::MAX - 1),
                source: initial,
                premise: Fingerprint(1),
            },
        );
        let expressions = memo.group(group).unwrap().logical_exprs();

        assert_eq!(
            preferred_semantic_expression(expressions, &memo, &[]),
            Some(initial)
        );
        assert_eq!(
            preferred_semantic_expression(expressions, &memo, &[declared_rule]),
            Some(declared)
        );
    }
}
