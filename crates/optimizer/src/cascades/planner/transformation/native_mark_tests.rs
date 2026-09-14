// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::cascades::budget::{BudgetDimension, SearchBudget};
use crate::cascades::planner::MemoBuilder;
use paro_common::types::LogicalType;
use paro_planner::binder::{context::BindContext, ir::CTEMaterialize};
use paro_planner::expression::ColumnRefExpression;
use paro_planner::operator::{Get, JoinCondition, MaterializedCTE};

fn column(table: usize, ty: LogicalType) -> Expression {
    Expression::ColumnRef(ColumnRefExpression::new(ColumnBinding::new(table, 0), ty).into())
}

fn scan(table: usize) -> OwnedLogicalPlan {
    OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(Get::new_without_table(
        table,
        vec!["key".into()],
        vec![LogicalType::Integer],
    ))))
}

#[test]
fn selected_mark_bindings_do_not_need_owned_positive_or_negative_rewrites() {
    for control in [false, true] {
        for depth in [0, 1] {
            let make = || {
                let left = if control {
                    OwnedLogicalPlan::synthetic(LogicalOperator::MaterializedCTE(
                        MaterializedCTE::new(
                            3,
                            "opaque_control".into(),
                            vec!["key".into()],
                            vec![LogicalType::Integer],
                            CTEMaterialize::Materialized,
                            scan(2),
                            scan(0),
                        ),
                    ))
                } else {
                    scan(0)
                };
                let mut join = Join::comparison(
                    JoinType::Mark,
                    left,
                    scan(1),
                    vec![JoinCondition::equality(
                        column(0, LogicalType::Integer),
                        column(1, LogicalType::Integer),
                    )],
                );
                let Join::Comparison(mark) = &mut join else {
                    unreachable!()
                };
                mark.mark_index = Some(30);
                let mut marker = column(30, LogicalType::Boolean);
                let Expression::ColumnRef(reference) = &mut marker else {
                    unreachable!()
                };
                reference.depth = depth;
                OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
                    40,
                    OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
                        OwnedLogicalPlan::synthetic(LogicalOperator::Join(join)),
                        vec![marker],
                    ))),
                    vec![column(0, LogicalType::Integer)],
                )))
            };
            assert_eq!(
                rewrite_positive_consumed_mark_filter(make()).is_some(),
                depth == 0
            );
            let mut input =
                MemoBuilder::build(make(), BindContext::new(), SearchBudget::default()).unwrap();
            let state = input.planner_state.clone();
            state.write().unwrap().session =
                Some(paro_context::TestStatementContextBuilder::minimal().build());
            let root = input.memo.group(input.root).unwrap().logical_exprs()[0];
            let filter_group = input.memo.logical_expr(root).unwrap().key.children[0];
            let filter = input.memo.group(filter_group).unwrap().logical_exprs()[0];
            let join_group = input.memo.logical_expr(filter).unwrap().key.children[0];
            let join = input.memo.group(join_group).unwrap().logical_exprs()[0];
            let original_children = input.memo.logical_expr(join).unwrap().key.children.clone();
            let binding = matching::scoped_pattern_bindings(
                PlannerTransformation::MarkJoinToSemi,
                input.root,
                root,
                &input.memo,
                &state.read().unwrap(),
                None,
                BudgetDimension::RuleWorkPerGroup,
            )
            .unwrap()
            .bindings
            .first()
            .cloned()
            .expect("selected mark consumer");
            let rule = PlannerTransformationRule {
                transformation: PlannerTransformation::MarkJoinToSemi,
                planner_state: state.clone(),
            };
            let before = semantic_plan::owned_binding_instantiation_count();
            let mut ctx = TransformContext::new(&mut input.memo, input.root);
            let outputs = rule.apply_binding(&binding, &mut ctx).unwrap();
            assert_eq!(outputs.len(), usize::from(depth == 0));
            assert_eq!(
                semantic_plan::owned_binding_instantiation_count(),
                before,
                "control={control}, depth={depth}"
            );
            if let Some(output) = outputs.first() {
                let group = ctx.memo().group(output.key.children[0]).unwrap();
                let logical = ctx.memo().logical_expr(group.logical_exprs()[0]).unwrap();
                assert_eq!(
                    logical.key.children, original_children,
                    "MARK conversion must retain both exact Memo inputs"
                );
                let state = state.read().unwrap();
                let LogicalOperator::Join(Join::Comparison(join)) = &state.payloads.logical
                    [logical.payload.index()]
                .semantic_template
                .operator
                else {
                    panic!("expected native semi join")
                };
                assert_eq!(join.join_type, JoinType::Semi);
                assert_eq!(join.mark_index, None);
                assert_eq!(
                    join.mark_semantics,
                    paro_planner::operator::MarkJoinSemantics::NotMark
                );
            }
        }
    }
}
