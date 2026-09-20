// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Native staging for aggregate join pre-aggregation.
//!
//! `AggregateJoinPreaggregation` is a local, closed rewrite: a grouped
//! aggregate over a clean LEFT join can aggregate the nullable side by its
//! join key and merge the partial state after the join.  The legacy rule
//! materializes an owned tree before it can perform that rewrite.  This
//! module performs the same conservative shape check over the immutable
//! pattern shell and emits the replacement shell directly.
//!
//! The rule remains an alternative, not a cost decision. The selected grammar
//! is closed; a negative native result does not retry an owned rewrite.

use paro_common::error as paro_error;
use paro_planner::expression::{
    AggregateExpression, AggregateType, ColumnRefExpression, Expression,
};
use paro_planner::operator::{
    Aggregate, ColumnBinding, Join, JoinComparisonType, JoinType, LogicalOperator, ProjectionMap,
};

use super::staging::{NativeChild, NativeNode, NativeShell};
use super::{Memo, PatternOperand, PlannerTransformState, boundary};

/// Try the exact native subset of `AggregateJoinPreaggregation`.
///
/// The matched pattern owns the root aggregate and left join while its two
/// inputs are immutable Memo holes.  No child frontier is re-expanded and no
/// owned representative is created on the successful path.
pub(super) fn try_native_aggregate_join_preaggregation(
    binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
    facts: &boundary::BoundarySnapshot,
) -> paro_common::error::Result<Option<NativeShell>> {
    let Some((shell, layouts)) =
        NativeShell::from_pattern_with_layouts(memo, state, binding, facts)?
    else {
        return Ok(None);
    };
    // Partial aggregation wraps the nullable input as a whole. Neither input
    // is duplicated or traversed through an opaque control boundary.
    let original_root_layout = layouts
        .get(shell.root)
        .cloned()
        .ok_or_else(|| paro_error::internal("native preaggregation has no root layout"))?;
    try_native_shell_with_layout(shell, state, original_root_layout, layouts)
}

#[cfg(test)]
fn try_native_shell(
    shell: NativeShell,
    state: &PlannerTransformState,
) -> paro_common::error::Result<Option<NativeShell>> {
    let layouts = shell.layouts()?;
    let original_root_layout = layouts
        .get(shell.root)
        .cloned()
        .ok_or_else(|| paro_error::internal("native preaggregation has no root layout"))?;
    try_native_shell_with_layout(shell, state, original_root_layout, layouts)
}

fn try_native_shell_with_layout(
    shell: NativeShell,
    state: &PlannerTransformState,
    original_root_layout: paro_planner::operator::LogicalOutputLayout,
    layouts: Vec<paro_planner::operator::LogicalOutputLayout>,
) -> paro_common::error::Result<Option<NativeShell>> {
    let root = shell.root;
    let LogicalOperator::Aggregate(aggregate) = shell.root_operator().clone() else {
        return Ok(None);
    };
    if !aggregate_is_candidate(&aggregate) {
        return Ok(None);
    }
    let NativeChild::Node(join_index) = aggregate.child.clone() else {
        return Ok(None);
    };
    let LogicalOperator::Join(Join::Comparison(join)) = shell
        .nodes
        .get(join_index)
        .ok_or_else(|| paro_error::internal("native preaggregation lost its join"))?
        .operator
        .clone()
    else {
        return Ok(None);
    };
    if !clean_left_join(&join) || join.conditions.len() != 1 {
        return Ok(None);
    }
    let condition = &join.conditions[0];
    if condition.comparison != JoinComparisonType::Equal {
        return Ok(None);
    }
    let left_layout = child_layout(&layouts, &join.left)?;
    let right_layout = child_layout(&layouts, &join.right)?;
    let Some(condition_left) = column_binding(&condition.left) else {
        return Ok(None);
    };
    let Some(condition_right) = column_binding(&condition.right) else {
        return Ok(None);
    };
    let (left_key, right_key) = if left_layout.bindings().contains(&condition_left)
        && right_layout.bindings().contains(&condition_right)
    {
        (condition_left, condition_right)
    } else if left_layout.bindings().contains(&condition_right)
        && right_layout.bindings().contains(&condition_left)
    {
        (condition_right, condition_left)
    } else {
        return Ok(None);
    };
    if column_binding(&aggregate.groups[0]) != Some(left_key) {
        return Ok(None);
    }

    // A partial aggregate on the nullable side is already the result of this
    // rewrite.  Declining here keeps the Memo alternative set finite and
    // matches the legacy rule's idempotence guard.
    if matches!(
        &join.right,
        NativeChild::Node(index)
            if matches!(shell.nodes.get(*index).map(|node| &node.operator), Some(LogicalOperator::Aggregate(_)))
    ) {
        return Ok(None);
    }

    for expression in &aggregate.aggregates {
        if !aggregate_input_is_right_side(expression, &right_layout)
            || !partial_merge_contract(expression)
        {
            return Ok(None);
        }
    }

    let partial_group_index = state.bind_context.generate_table_index();
    let partial_aggregate_index = state.bind_context.generate_table_index();
    let partial_groupings_index = state.bind_context.generate_table_index();
    let partials = aggregate.aggregates.clone();
    let merge_expressions = partials
        .iter()
        .enumerate()
        .map(|(index, expression)| {
            let Expression::Aggregate(partial) = expression else {
                unreachable!("partial aggregate contract was checked")
            };
            let merge = partial
                .function
                .partial_merge_function()
                .expect("partial merge contract was checked");
            Expression::Aggregate(
                AggregateExpression::new(
                    merge,
                    vec![Expression::ColumnRef(
                        ColumnRefExpression::new(
                            ColumnBinding::new(partial_aggregate_index, index),
                            partial.return_type.clone(),
                        )
                        .into(),
                    )],
                    partial.return_type.clone(),
                )
                .into(),
            )
        })
        .collect::<Vec<_>>();

    let mut nodes = shell.nodes.into_vec();
    let right = join.right.clone();
    let partial_stats = native_child_stats(&nodes, &right);
    let right_key_ordinal = right_layout
        .bindings()
        .iter()
        .position(|binding| *binding == right_key)
        .ok_or_else(|| paro_error::internal("native preaggregation lost right key"))?;
    let right_key_type = right_layout.types()[right_key_ordinal].clone();
    let right_key_is_condition_right = column_binding(&condition.right) == Some(right_key);
    let right_key_is_condition_left = column_binding(&condition.left) == Some(right_key);
    let mut partial = Aggregate {
        group_index: partial_group_index,
        aggregate_index: partial_aggregate_index,
        groupings_index: partial_groupings_index,
        child: right,
        groups: vec![Expression::ColumnRef(
            ColumnRefExpression::new(right_key, right_key_type.clone()).into(),
        )],
        grouping_sets: Vec::new(),
        aggregates: partials,
        post_reduction: None,
        group_stats: Vec::new(),
        group_dependencies: Vec::new(),
        group_input_multiplicity: paro_planner::operator::GroupInputMultiplicity::Arbitrary,
        returned_types: Vec::new(),
        grouping_functions: Vec::new(),
    };
    super::reset_native_aggregate_output(&mut partial);
    let partial_index = nodes.len();
    nodes.push(NativeNode {
        id: state.bind_context.next_plan_id(),
        stats: partial_stats,
        operator: LogicalOperator::Aggregate(Box::new(partial)),
        source_proofs: Box::new([]),
    });

    let mut rewritten_join = join;
    rewritten_join.right = NativeChild::Node(partial_index);
    rewritten_join.left_projection_map = ProjectionMap::all();
    rewritten_join.right_projection_map = ProjectionMap::all();
    let right_key_ref = Expression::ColumnRef(
        ColumnRefExpression::new(ColumnBinding::new(partial_group_index, 0), right_key_type).into(),
    );
    if right_key_is_condition_right {
        rewritten_join.conditions[0].right = right_key_ref;
    } else if right_key_is_condition_left {
        rewritten_join.conditions[0].left = right_key_ref;
    } else {
        return Ok(None);
    }
    nodes[join_index].operator = LogicalOperator::Join(Join::Comparison(rewritten_join));
    nodes[join_index].source_proofs = Box::new([]);

    let mut rewritten_aggregate = *aggregate;
    rewritten_aggregate.child = NativeChild::Node(join_index);
    rewritten_aggregate.aggregates = merge_expressions;
    super::reset_native_aggregate_output(&mut rewritten_aggregate);
    nodes[root].operator = LogicalOperator::Aggregate(Box::new(rewritten_aggregate));
    nodes[root].source_proofs = Box::new([]);

    let (result, result_layout) = super::compact_native_shell_with_layout(NativeShell {
        nodes: nodes.into_boxed_slice(),
        root,
    })?;
    if result_layout != original_root_layout {
        return Ok(None);
    }
    Ok(Some(result))
}

fn aggregate_is_candidate(aggregate: &Aggregate<NativeChild>) -> bool {
    aggregate.post_reduction.is_none()
        && aggregate.groups.len() == 1
        && !aggregate.aggregates.is_empty()
        && aggregate.grouping_sets.is_empty()
        && aggregate.grouping_functions.is_empty()
}

fn clean_left_join(join: &paro_planner::operator::ComparisonJoin<NativeChild>) -> bool {
    join.join_type == JoinType::Left
        && join.mark_index.is_none()
        && join.duplicate_eliminated_columns.is_empty()
        && !join.delim_flipped
}

fn aggregate_input_is_right_side(
    expression: &Expression,
    right_layout: &paro_planner::operator::LogicalOutputLayout,
) -> bool {
    let Expression::Aggregate(aggregate) = expression else {
        return false;
    };
    let Some(input) = aggregate
        .children
        .as_slice()
        .first()
        .and_then(column_binding)
    else {
        return false;
    };
    right_layout.bindings().contains(&input)
        && aggregate.children.len() == 1
        && aggregate.children.iter().all(|child| {
            column_binding(child).is_some_and(|binding| right_layout.bindings().contains(&binding))
        })
}

fn partial_merge_contract(expression: &Expression) -> bool {
    let Expression::Aggregate(aggregate) = expression else {
        return false;
    };
    aggregate.aggr_type == AggregateType::NonDistinct
        && aggregate.filter.is_none()
        && aggregate.order_bys.is_empty()
        && aggregate.children.len() == 1
        && aggregate
            .function
            .partial_merge_function()
            .is_some_and(|merge| {
                merge.arguments == [aggregate.return_type.clone()]
                    && merge.return_type == aggregate.return_type
            })
}

fn column_binding(expression: &Expression) -> Option<ColumnBinding> {
    let Expression::ColumnRef(column) = expression else {
        return None;
    };
    (column.depth == 0).then_some(column.binding)
}

fn child_layout(
    layouts: &[paro_planner::operator::LogicalOutputLayout],
    child: &NativeChild,
) -> paro_common::error::Result<paro_planner::operator::LogicalOutputLayout> {
    match child {
        NativeChild::Node(index) => layouts
            .get(*index)
            .cloned()
            .ok_or_else(|| paro_error::internal("native preaggregation child has no layout")),
        NativeChild::MemoGroup { layout, .. } | NativeChild::Group { layout, .. } => {
            Ok(layout.clone())
        }
    }
}

fn native_child_stats(nodes: &[NativeNode], child: &NativeChild) -> super::NodeStats {
    match child {
        NativeChild::Node(index) => nodes
            .get(*index)
            .map(|node| node.stats.clone())
            .unwrap_or_default(),
        NativeChild::MemoGroup { stats, .. } | NativeChild::Group { stats, .. } => stats.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::{
        PlannerTransformation, PlannerTransformationRule, TransformContext, matching,
    };
    use crate::cascades::budget::{BudgetDimension, SearchBudget};
    use crate::cascades::planner::MemoBuilder;
    use crate::cascades::rules::TransformationRule;
    use paro_common::types::LogicalType;
    use paro_function::aggregate::distributive::count::get_count_function;
    use paro_planner::binder::context::BindContext;
    use paro_planner::operator::{ExpressionGet, JoinCondition};
    use paro_planner::plan::OwnedLogicalPlan;

    fn column(table: usize, index: usize) -> Expression {
        Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(table, index), LogicalType::BigInt).into(),
        )
    }

    fn input(table: usize, columns: usize) -> OwnedLogicalPlan {
        OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
            table,
            vec![],
            (0..columns).map(|index| format!("c{index}")).collect(),
            vec![LogicalType::BigInt; columns],
        )))
    }

    fn count(value: Expression) -> Expression {
        let (function, types) = get_count_function().bind(&[LogicalType::BigInt]).unwrap();
        assert_eq!(types, [LogicalType::BigInt]);
        Expression::Aggregate(
            AggregateExpression::new(function, vec![value], LogicalType::BigInt).into(),
        )
    }

    fn candidate() -> OwnedLogicalPlan {
        let context = BindContext::new();
        let join = OwnedLogicalPlan::new(
            &context,
            LogicalOperator::Join(Join::comparison(
                JoinType::Left,
                input(10, 1),
                input(20, 2),
                vec![JoinCondition::equality(column(10, 0), column(20, 0))],
            )),
        );
        OwnedLogicalPlan::new(
            &context,
            LogicalOperator::Aggregate(Box::new(Aggregate::new(
                30,
                31,
                32,
                join,
                vec![column(10, 0)],
                vec![],
                vec![count(column(20, 1))],
                vec![],
            ))),
        )
    }

    #[test]
    fn native_preaggregation_preserves_root_layout_and_builds_partial() {
        let source = candidate();
        let expected = source.output_layout();
        let input =
            MemoBuilder::build(candidate(), BindContext::new(), SearchBudget::default()).unwrap();
        let shell = NativeShell::from_owned(source, &std::collections::HashMap::new()).unwrap();
        let result = try_native_shell(shell, &input.planner_state.read().unwrap())
            .unwrap()
            .expect("left join aggregate should be native");
        assert_eq!(result.root_layout().unwrap(), expected);

        let LogicalOperator::Aggregate(aggregate) = result.root_operator() else {
            panic!("expected aggregate root")
        };
        let Expression::Aggregate(merge) = &aggregate.aggregates[0] else {
            panic!("expected merge aggregate")
        };
        assert_eq!(merge.function.name, "count_partial_merge");
        let NativeChild::Node(join) = aggregate.child.clone() else {
            panic!("expected join child")
        };
        let LogicalOperator::Join(Join::Comparison(join)) = &result.nodes[join].operator else {
            panic!("expected comparison join")
        };
        assert!(matches!(join.right, NativeChild::Node(_)));
    }

    #[test]
    fn production_binding_builds_native_preaggregation_shell() {
        let mut input =
            MemoBuilder::build(candidate(), BindContext::new(), SearchBudget::default()).unwrap();
        let state = input.planner_state.read().unwrap();
        let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
        let binding = matching::scoped_pattern_bindings(
            PlannerTransformation::AggregateJoinPreaggregation,
            input.root,
            expression,
            &input.memo,
            &state,
            None,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .bindings
        .first()
        .cloned()
        .expect("the production pattern should expose the preaggregation shell");
        let mut context = TransformContext::new(&mut input.memo, input.root);
        let facts = boundary::BoundarySnapshot::read(
            &mut context,
            &state,
            &binding.root,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .expect("production binding should have boundary facts");
        let shell =
            try_native_aggregate_join_preaggregation(&binding.root, context.memo(), &state, &facts)
                .unwrap()
                .expect("production binding should take the native path");
        assert_eq!(shell.root_layout().unwrap().len(), 2);
    }

    #[test]
    fn production_apply_stages_preaggregation_without_owned_settlement() {
        for eligible in [false, true] {
            for control in [false, true] {
                let mut plan = candidate();
                let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
                    unreachable!()
                };
                let LogicalOperator::Join(Join::Comparison(join)) = &mut aggregate.child.operator
                else {
                    unreachable!()
                };
                if !eligible {
                    join.conditions[0].comparison = JoinComparisonType::NotEqual;
                }
                if control {
                    let child = std::mem::replace(
                        &mut *join.right,
                        OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
                    );
                    *join.right = OwnedLogicalPlan::synthetic(LogicalOperator::MaterializedCTE(
                        paro_planner::operator::MaterializedCTE::new(
                            50,
                            "retained_right".into(),
                            vec!["c0".into()],
                            vec![LogicalType::BigInt],
                            paro_planner::binder::ir::CTEMaterialize::Materialized,
                            input(99, 1),
                            child,
                        ),
                    ));
                }
                let mut input =
                    MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
                let state = input.planner_state.clone();
                state.write().unwrap().session =
                    Some(paro_context::TestStatementContextBuilder::minimal().build());
                let binding = {
                    let state = state.read().unwrap();
                    let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
                    let binding = matching::scoped_pattern_bindings(
                        PlannerTransformation::AggregateJoinPreaggregation,
                        input.root,
                        expression,
                        &input.memo,
                        &state,
                        None,
                        BudgetDimension::RuleWorkPerGroup,
                    )
                    .unwrap()
                    .bindings
                    .first()
                    .cloned()
                    .expect("the production pattern should match");
                    binding
                };
                let arena_before = state.read().unwrap().staging_arena.len();
                let rule = PlannerTransformationRule {
                    transformation: PlannerTransformation::AggregateJoinPreaggregation,
                    planner_state: state.clone(),
                };
                let mut context = TransformContext::new(&mut input.memo, input.root);
                let bridges = super::super::semantic_plan::owned_binding_instantiation_count();
                let outputs = rule.apply_binding(&binding, &mut context).unwrap();
                assert_eq!(outputs.len(), usize::from(eligible));
                assert_eq!(
                    super::super::semantic_plan::owned_binding_instantiation_count(),
                    bridges,
                    "eligible={eligible}, control={control}"
                );
                assert_eq!(state.read().unwrap().staging_arena.len(), arena_before);
            }
        }
    }
}
