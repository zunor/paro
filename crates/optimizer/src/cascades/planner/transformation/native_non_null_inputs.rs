// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Native staging for the aggregate non-NULL-input rewrite.
//!
//! The owned rule changes a bound aggregate function only after proving that
//! its input reaches a stored Get column through row-preserving unary
//! operators. Keep that proof at Memo boundaries and change only the
//! aggregate payload. The selected matcher grammar is closed; unknown
//! non-NULL evidence yields no rewrite, not an owned fallback. Fact reads
//! retain responsibility for reopening the rule when evidence changes.

use paro_common::error::Result;
use paro_planner::expression::{AggregateType, Expression};
use paro_planner::operator::{ColumnBinding, LogicalOperator};

use super::staging::{NativeChild, NativeShell};
use super::{Memo, PatternOperand, PlannerTransformState, boundary};

pub(super) fn try_native_aggregate_non_null_input(
    root_binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
    facts: &boundary::BoundarySnapshot,
) -> Result<Option<NativeShell>> {
    let Some((shell, layouts)) =
        NativeShell::from_pattern_with_layouts(memo, state, root_binding, facts)?
    else {
        return Ok(None);
    };
    if super::native_shell_contains_control_boundary(&shell) {
        return Ok(None);
    }
    let LogicalOperator::Aggregate(aggregate) = shell.root_operator().clone() else {
        return Ok(None);
    };
    if !preserved_get_path(&shell, aggregate.child.clone()) {
        return Ok(None);
    }

    let mut aggregate = *aggregate;
    let mut changed = false;
    for expression in &mut aggregate.aggregates {
        let Some(input_binding) = aggregate_input_binding(expression) else {
            continue;
        };
        let Some(source) = find_preserved_get(root_binding, memo, state, input_binding) else {
            continue;
        };
        if facts.binding_is_non_null(
            memo,
            state,
            source.group,
            source.binding,
            &source.logical_type,
        ) {
            changed |= rewrite_aggregate(expression);
        }
    }
    if !changed {
        return Ok(None);
    }

    // The rewrite preserves every visible output type and binding, but the
    // aggregate's derived grouping facts belong to the old operator payload.
    super::reset_native_aggregate_output(&mut aggregate);
    let root = shell.root;
    let mut nodes = shell.nodes.into_vec();
    nodes[root].operator = LogicalOperator::Aggregate(Box::new(aggregate));
    nodes[root].source_proofs = Box::new([]);
    let (shell, output_layout) = super::compact_native_shell_with_layout(NativeShell {
        nodes: nodes.into_boxed_slice(),
        root,
    })?;
    if output_layout != layouts[root] {
        return Ok(None);
    }
    Ok(Some(shell))
}

#[derive(Clone)]
struct PreservedGet {
    group: super::GroupId,
    binding: ColumnBinding,
    logical_type: paro_common::types::LogicalType,
}

fn find_preserved_get(
    operand: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
    binding: ColumnBinding,
) -> Option<PreservedGet> {
    let PatternOperand::Expression {
        group,
        expression,
        children,
    } = operand
    else {
        return None;
    };
    let logical = memo.logical_expr(*expression)?;
    let payload = state.payloads.logical.get(logical.payload.index())?;
    if let LogicalOperator::Get(get) = &payload.semantic_template.operator {
        if get.table_index != binding.table_index
            || get.stored_column(binding.column_index).is_none()
        {
            return None;
        }
        return Some(PreservedGet {
            group: *group,
            binding,
            logical_type: get.returned_types.get(binding.column_index)?.clone(),
        });
    }
    children
        .iter()
        .find_map(|child| find_preserved_get(child, memo, state, binding))
}

/// The matcher admits only Filter/Order/TopN/Limit shells on this path. Keep
/// the explicit whitelist here so a future matcher broadening cannot turn a
/// row-changing or NULL-extending operator into a non-NULL proof.
fn preserved_get_path(shell: &NativeShell, child: NativeChild) -> bool {
    let NativeChild::Node(index) = child else {
        return false;
    };
    let Some(node) = shell.nodes.get(index) else {
        return false;
    };
    match &node.operator {
        LogicalOperator::Get(_) => true,
        LogicalOperator::Filter(filter) => preserved_get_path(shell, filter.child.clone()),
        LogicalOperator::Order(order) => preserved_get_path(shell, order.child.clone()),
        LogicalOperator::TopN(topn) => preserved_get_path(shell, topn.child.clone()),
        LogicalOperator::Limit(limit) => preserved_get_path(shell, limit.child.clone()),
        _ => false,
    }
}

fn rewrite_aggregate(expression: &mut Expression) -> bool {
    let Expression::Aggregate(aggregate) = expression else {
        return false;
    };
    if aggregate.aggr_type != AggregateType::NonDistinct || aggregate.children.len() != 1 {
        return false;
    }
    let Expression::ColumnRef(input) = &aggregate.children[0] else {
        return false;
    };
    if input.depth != 0 {
        return false;
    }
    let Some(replacement) = aggregate.function.non_null_input_function() else {
        return false;
    };
    if replacement.return_type != aggregate.function.return_type
        || replacement.return_type != aggregate.return_type
        || replacement.empty_input != aggregate.function.empty_input
        || !replacement.arguments.is_empty()
        || replacement.varargs.is_some()
    {
        return false;
    }
    aggregate.function = replacement;
    aggregate.children.clear();
    true
}

fn aggregate_input_binding(expression: &Expression) -> Option<ColumnBinding> {
    let Expression::Aggregate(aggregate) = expression else {
        return None;
    };
    if aggregate.aggr_type != AggregateType::NonDistinct || aggregate.children.len() != 1 {
        return None;
    }
    let Expression::ColumnRef(input) = &aggregate.children[0] else {
        return None;
    };
    (input.depth == 0).then_some(input.binding)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cascades::budget::{BudgetDimension, SearchBudget};
    use crate::cascades::planner::MemoBuilder;
    use crate::cascades::planner::transformation::{
        PlannerTransformation, PlannerTransformationRule, TransformContext, matching,
    };
    use crate::cascades::rules::TransformationRule;
    use paro_function::aggregate::distributive::count::get_count_function;
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::{AggregateExpression, ColumnRefExpression};
    use paro_planner::operator::bound_reference::BoundColumnValues;
    use paro_planner::operator::{Aggregate, Get};
    use paro_planner::plan::OwnedLogicalPlan;
    use paro_storage::statistics::BaseStatistics;

    fn candidate() -> OwnedLogicalPlan {
        let context = BindContext::new();
        let get = OwnedLogicalPlan::new(
            &context,
            LogicalOperator::Get(Box::new(Get::new_without_table(
                0,
                vec!["value".to_string()],
                vec![paro_common::types::LogicalType::BigInt],
            ))),
        );
        let (function, _) = get_count_function()
            .bind(&[paro_common::types::LogicalType::BigInt])
            .unwrap();
        OwnedLogicalPlan::new(
            &context,
            LogicalOperator::Aggregate(Box::new(Aggregate::new(
                1,
                2,
                3,
                get,
                vec![],
                vec![],
                vec![Expression::Aggregate(
                    AggregateExpression::new(
                        function,
                        vec![Expression::ColumnRef(
                            ColumnRefExpression::new(
                                ColumnBinding::new(0, 0),
                                paro_common::types::LogicalType::BigInt,
                            )
                            .into(),
                        )],
                        paro_common::types::LogicalType::BigInt,
                    )
                    .into(),
                )],
                vec![],
            ))),
        )
    }

    #[test]
    fn production_binding_stages_non_null_rewrite_from_boundary_evidence() {
        let mut input =
            MemoBuilder::build(candidate(), BindContext::new(), SearchBudget::default()).unwrap();
        let state = input.planner_state.clone();
        let get_group = input
            .memo
            .logical_expr(input.memo.group(input.root).unwrap().logical_exprs()[0])
            .unwrap()
            .key
            .children[0];
        let column = state
            .read()
            .unwrap()
            .binding_ids
            .get(0, 0, &paro_common::types::LogicalType::BigInt)
            .copied()
            .unwrap();
        input
            .memo
            .group_mut(get_group)
            .unwrap()
            .logical_properties
            .column_values
            .insert(
                column,
                BoundColumnValues::new(BaseStatistics::new(
                    paro_common::types::LogicalType::BigInt,
                ))
                .unwrap(),
            );
        state.write().unwrap().session =
            Some(paro_context::TestStatementContextBuilder::minimal().build());
        let state_read = state.read().unwrap();
        let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
        let binding = matching::scoped_pattern_bindings(
            PlannerTransformation::AggregateNonNullInput,
            input.root,
            expression,
            &input.memo,
            &state_read,
            None,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .bindings
        .first()
        .cloned()
        .expect("aggregate/get path should match");
        drop(state_read);

        let rule = PlannerTransformationRule {
            transformation: PlannerTransformation::AggregateNonNullInput,
            planner_state: state.clone(),
        };
        let mut context = TransformContext::new(&mut input.memo, input.root);
        let outputs = rule.apply_binding(&binding, &mut context).unwrap();
        assert_eq!(outputs.len(), 1);
        let state_read = state.read().unwrap();
        let payload = state_read
            .payloads
            .logical
            .get(outputs[0].payload.index())
            .unwrap();
        let LogicalOperator::Aggregate(aggregate) = &payload.semantic_template.operator else {
            panic!("expected aggregate output")
        };
        let Expression::Aggregate(aggregate) = &aggregate.aggregates[0] else {
            panic!("expected aggregate expression")
        };
        assert_eq!(aggregate.function.name, "count_star");
        assert!(aggregate.children.is_empty());
    }

    #[test]
    fn native_nullable_rejection_does_not_instantiate_owned_binding() {
        let input = candidate();
        let mut memo_input =
            MemoBuilder::build(input, BindContext::new(), SearchBudget::default()).unwrap();
        let root = memo_input.root;
        let state = memo_input.planner_state.clone();
        state.write().unwrap().session =
            Some(paro_context::TestStatementContextBuilder::minimal().build());
        let state_read = state.read().unwrap();
        let expression = memo_input.memo.group(root).unwrap().logical_exprs()[0];
        let binding = matching::scoped_pattern_bindings(
            PlannerTransformation::AggregateNonNullInput,
            root,
            expression,
            &memo_input.memo,
            &state_read,
            None,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .bindings
        .first()
        .cloned()
        .unwrap();
        drop(state_read);
        let rule = PlannerTransformationRule {
            transformation: PlannerTransformation::AggregateNonNullInput,
            planner_state: state.clone(),
        };
        let mut context = TransformContext::new(&mut memo_input.memo, root);
        let bridges = super::super::semantic_plan::owned_binding_instantiation_count();
        assert!(
            rule.apply_binding(&binding, &mut context)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            super::super::semantic_plan::owned_binding_instantiation_count(),
            bridges,
            "a complete native negative result must not construct an owned binding"
        );
        let reads = context.take_fact_reads();
        drop(context);
        let get_group = memo_input
            .memo
            .logical_expr(expression)
            .unwrap()
            .key
            .children[0];
        let column = state
            .read()
            .unwrap()
            .binding_ids
            .get(0, 0, &paro_common::types::LogicalType::BigInt)
            .copied()
            .unwrap();
        memo_input
            .memo
            .group_mut(get_group)
            .unwrap()
            .logical_properties
            .column_values
            .insert(
                column,
                BoundColumnValues::new(BaseStatistics::new(
                    paro_common::types::LogicalType::BigInt,
                ))
                .unwrap(),
            );
        assert!(
            reads
                .iter()
                .any(|read| !read.is_current(&memo_input.memo).unwrap()),
            "native rejection must retain the evidence dependency"
        );
        let mut context = TransformContext::new(&mut memo_input.memo, root);
        assert_eq!(rule.apply_binding(&binding, &mut context).unwrap().len(), 1);
        assert_eq!(
            super::super::semantic_plan::owned_binding_instantiation_count(),
            bridges
        );
    }
}
