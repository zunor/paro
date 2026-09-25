// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::cascades::budget::{BudgetDimension, SearchBudget};
use crate::cascades::planner::MemoBuilder;
use paro_common::types::LogicalType;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{ColumnRefExpression, ConstantExpression, Expression};
use paro_planner::operator::{ColumnBinding, ExpressionGet, Limit, Order};

fn number(value: i32) -> Expression {
    Expression::Constant(
        ConstantExpression::new(Value::Integer(value), LogicalType::Integer).into(),
    )
}

fn column() -> Expression {
    Expression::ColumnRef(
        ColumnRefExpression::new(ColumnBinding::new(0, 0), LogicalType::Integer).into(),
    )
}

fn plan(topn: bool, count: i32, offset: i32, control: bool) -> OwnedLogicalPlan {
    let scan = OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
        0,
        vec![vec![number(3)], vec![number(1)], vec![number(1)]],
        vec!["value".into()],
        vec![LogicalType::Integer],
    )));
    let scan = if control {
        OwnedLogicalPlan::synthetic(LogicalOperator::MaterializedCTE(
            paro_planner::operator::MaterializedCTE::new(
                50,
                "retained_control".into(),
                vec![],
                vec![],
                paro_planner::binder::ir::CTEMaterialize::Materialized,
                OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
                scan,
            ),
        ))
    } else {
        scan
    };
    let child = if topn {
        OwnedLogicalPlan::synthetic(LogicalOperator::Order(Order::new(
            scan,
            vec![paro_planner::binder::ir::OrderByNode {
                expression: column(),
                ascending: true,
                nulls_first: false,
            }],
        )))
    } else {
        OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
            1,
            scan,
            vec![column()],
        )))
    };
    OwnedLogicalPlan::synthetic(LogicalOperator::Limit(Box::new(Limit::new(
        child,
        Some(number(count)),
        Some(number(offset)),
    ))))
}

#[test]
fn native_limit_positive_and_negative_bindings_never_export_owned_ir() {
    for control in [false, true] {
        for topn in [false, true] {
            for (count, offset) in [(-1, 0), (0, 0), (3, 1), (8192, 0), (3, -1)] {
                let transformation = if topn {
                    PlannerTransformation::TopNIntroduction
                } else {
                    PlannerTransformation::LimitPushdown
                };
                let expected = usize::from(count >= 0 && offset >= 0 && (topn || count < 8192));
                let reference = plan(topn, count, offset, control);
                let reference_changed = if topn {
                    matches!(
                        crate::rewrite::limit::topn::TopNOptimizer::new()
                            .optimize_plan(reference)
                            .operator,
                        LogicalOperator::TopN(_)
                    )
                } else {
                    crate::rewrite::limit::pushdown::LimitPushdown::new()
                        .optimize_plan_with_change(reference)
                        .1
                };
                assert_eq!(usize::from(reference_changed), expected);
                let mut input = MemoBuilder::build(
                    plan(topn, count, offset, control),
                    BindContext::new(),
                    SearchBudget::default(),
                )
                .unwrap();
                let state = input.planner_state.clone();
                state.write().unwrap().session =
                    Some(paro_context::TestStatementContextBuilder::minimal().build());
                let root = input.memo.group(input.root).unwrap().logical_exprs()[0];
                let unary_group = input.memo.logical_expr(root).unwrap().key.children[0];
                let unary = input.memo.group(unary_group).unwrap().logical_exprs()[0];
                let exact_input = input.memo.logical_expr(unary).unwrap().key.children[0];
                let binding = {
                    let state = state.read().unwrap();
                    matching::scoped_pattern_bindings(
                        transformation,
                        input.root,
                        root,
                        &input.memo,
                        &state,
                        None,
                        BudgetDimension::RuleWorkPerGroup,
                    )
                    .unwrap()
                    .bindings
                    .first()
                    .cloned()
                    .expect("limit structural binding")
                };
                let rule = PlannerTransformationRule {
                    transformation,
                    planner_state: state.clone(),
                };
                let before = semantic_plan::owned_binding_instantiation_count();
                let mut ctx = TransformContext::new(&mut input.memo, input.root);
                let outputs = rule.apply_binding(&binding, &mut ctx).unwrap();
                assert_eq!(
                    outputs.len(),
                    expected,
                    "topn={topn}, count={count}, offset={offset}, control={control}"
                );
                assert_eq!(
                    semantic_plan::owned_binding_instantiation_count(),
                    before,
                    "native negative results must not export owned IR: topn={topn}, count={count}, offset={offset}"
                );
                if let Some(output) = outputs.first() {
                    let state = state.read().unwrap();
                    let operator = &state.payloads.logical[output.payload.index()]
                        .semantic_template
                        .operator;
                    if topn {
                        assert_eq!(output.key.children.as_ref(), &[exact_input]);
                        let LogicalOperator::TopN(topn) = operator else {
                            panic!("expected native TopN")
                        };
                        assert_eq!((topn.limit, topn.offset), (count as usize, offset as usize));
                        assert_eq!(topn.orders.len(), 1);
                        assert!(topn.orders[0].ascending);
                        assert!(!topn.orders[0].nulls_first);
                    } else {
                        assert!(matches!(operator, LogicalOperator::Projection(_)));
                        let group = context_child_limit(&ctx, &output.key.children);
                        assert_eq!(
                            ctx.memo()
                                .logical_expr(group)
                                .unwrap()
                                .key
                                .children
                                .as_ref(),
                            &[exact_input]
                        );
                        let payload = ctx.memo().logical_expr(group).unwrap().payload;
                        let LogicalOperator::Limit(limit) = &state.payloads.logical
                            [payload.index()]
                        .semantic_template
                        .operator
                        else {
                            panic!("expected pushed native Limit")
                        };
                        assert_eq!(
                            limit.limit.as_ref().and_then(native_constant_value),
                            Some(count as usize)
                        );
                        assert_eq!(
                            limit.offset.as_ref().and_then(native_constant_value),
                            Some(offset as usize)
                        );
                    }
                }
            }
        }
    }
}

fn context_child_limit(ctx: &TransformContext<'_>, children: &[GroupId]) -> LogicalExprId {
    assert_eq!(children.len(), 1);
    let expressions = ctx.memo().group(children[0]).unwrap().logical_exprs();
    assert_eq!(expressions.len(), 1);
    expressions[0]
}
