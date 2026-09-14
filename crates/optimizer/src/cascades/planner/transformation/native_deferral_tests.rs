// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::cascades::budget::{BudgetDimension, SearchBudget};
use crate::cascades::planner::MemoBuilder;
use crate::cascades::rules::TransformationRule;
use paro_common::types::LogicalType;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{AggregateExpression, ColumnRefExpression};
use paro_planner::operator::{CTERef, ExpressionGet, Get, JoinCondition, MaterializedCTE};

fn col(table: usize, ordinal: usize, ty: LogicalType) -> Expression {
    Expression::ColumnRef(ColumnRefExpression::new(ColumnBinding::new(table, ordinal), ty).into())
}

fn assert_column(expressions: &[Expression], table: usize, ordinal: usize) {
    assert_eq!(expressions.len(), 1);
    let Expression::ColumnRef(column) = &expressions[0] else {
        panic!("expected exact input column")
    };
    assert_eq!(column.depth, 0);
    assert_eq!(column.binding, ColumnBinding::new(table, ordinal));
}

#[test]
fn production_deferral_does_not_erase_build_side_enforcement() {
    use paro_planner::operator::JoinBuildSideConstraint;
    for constraint in [
        JoinBuildSideConstraint::Left,
        JoinBuildSideConstraint::Right,
    ] {
        let make_plan = || {
            let mut plan = candidate(false);
            let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
                unreachable!()
            };
            let LogicalOperator::Join(Join::Comparison(join)) = &mut aggregate.child.operator
            else {
                unreachable!()
            };
            join.build_side_constraint = constraint;
            plan
        };
        let (reference, changed) =
            dimension_deferral::optimize_plan(make_plan(), &BindContext::new()).unwrap();
        let LogicalOperator::Aggregate(aggregate) = &reference.operator else {
            unreachable!()
        };
        let LogicalOperator::Join(Join::Comparison(join)) = &aggregate.child.operator else {
            unreachable!()
        };
        assert!(
            !changed,
            "constrained root must remain a boundary, got {:?}",
            join.build_side_constraint
        );
        assert_eq!(join.build_side_constraint, constraint);
        let mut input =
            MemoBuilder::build(make_plan(), BindContext::new(), SearchBudget::default()).unwrap();
        let state = input.planner_state.clone();
        state.write().unwrap().session =
            Some(paro_context::TestStatementContextBuilder::minimal().build());
        let root = input.root;
        let expression = input.memo.group(root).unwrap().logical_exprs()[0];
        let binding = matching::scoped_pattern_bindings(
            PlannerTransformation::AggregateDimensionDeferral,
            root,
            expression,
            &input.memo,
            &state.read().unwrap(),
            None,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .bindings
        .first()
        .cloned()
        .unwrap();
        let rule = PlannerTransformationRule {
            transformation: PlannerTransformation::AggregateDimensionDeferral,
            planner_state: state.clone(),
        };
        let arena = state.read().unwrap().staging_arena.len();
        let bridges = semantic_plan::owned_binding_instantiation_count();
        let mut context = TransformContext::new(&mut input.memo, root);
        assert!(rule
            .apply_binding(&binding, &mut context)
            .unwrap()
            .is_empty());
        assert_eq!(semantic_plan::owned_binding_instantiation_count(), bridges);
        assert_eq!(state.read().unwrap().staging_arena.len(), arena);
    }
}

#[test]
fn production_deferral_semantic_rejections_do_not_export_owned_ir() {
    for case in [0, 1, 2] {
        let make_plan = || {
            let mut plan = candidate(false);
            let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
                unreachable!()
            };
            if case == 1 {
                let Expression::Aggregate(sum) = &mut aggregate.aggregates[0] else {
                    unreachable!()
                };
                sum.aggr_type = paro_planner::expression::AggregateType::Distinct;
            } else if case == 0 {
                aggregate.groups = vec![col(0, 0, LogicalType::Integer)];
                aggregate.recompute_returned_types();
            } else {
                let LogicalOperator::Join(Join::Comparison(join)) = &mut aggregate.child.operator
                else {
                    unreachable!()
                };
                join.join_type = JoinType::Left;
            }
            plan
        };
        let (_, changed) =
            dimension_deferral::optimize_plan(make_plan(), &BindContext::new()).unwrap();
        assert!(!changed);
        let mut input =
            MemoBuilder::build(make_plan(), BindContext::new(), SearchBudget::default()).unwrap();
        let state = input.planner_state.clone();
        state.write().unwrap().session =
            Some(paro_context::TestStatementContextBuilder::minimal().build());
        let root = input.root;
        let expression = input.memo.group(root).unwrap().logical_exprs()[0];
        let binding = matching::scoped_pattern_bindings(
            PlannerTransformation::AggregateDimensionDeferral,
            root,
            expression,
            &input.memo,
            &state.read().unwrap(),
            None,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .bindings
        .first()
        .cloned()
        .unwrap();
        let rule = PlannerTransformationRule {
            transformation: PlannerTransformation::AggregateDimensionDeferral,
            planner_state: state.clone(),
        };
        let arena = state.read().unwrap().staging_arena.len();
        let bridges = semantic_plan::owned_binding_instantiation_count();
        let mut context = TransformContext::new(&mut input.memo, root);
        assert!(rule
            .apply_binding(&binding, &mut context)
            .unwrap()
            .is_empty());
        assert_eq!(
            semantic_plan::owned_binding_instantiation_count(),
            bridges,
            "case={case}"
        );
        assert_eq!(state.read().unwrap().staging_arena.len(), arena);
    }
}

#[test]
fn production_deferral_respects_existing_partial_key_coverage() {
    use paro_planner::plan::{
        UniqueKey, UniqueKeyColumn, UniqueKeyNullSemantics, UniqueKeyProvenance,
    };
    for key_column in [0, 1] {
        let make_plan = || {
            let mut plan = candidate(false);
            let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
                unreachable!()
            };
            let LogicalOperator::Join(Join::Comparison(join)) = &mut aggregate.child.operator
            else {
                unreachable!()
            };
            let key = UniqueKey::new(
                (0..=key_column).map(|ordinal| UniqueKeyColumn {
                    output_index: ordinal,
                    binding: ColumnBinding::new(0, ordinal),
                }),
                UniqueKeyProvenance::Structural,
                UniqueKeyNullSemantics::NullsEqual,
            );
            let (sum, _) = paro_function::aggregate::distributive::sum::get_sum_function()
                .bind(&[LogicalType::Double])
                .unwrap();
            let mut groups = vec![col(4, 0, LogicalType::Integer)];
            if key_column == 1 {
                groups.push(col(4, 1, LogicalType::Double));
            }
            join.left = Box::new(OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(
                Box::new(Aggregate::new(
                    0,
                    3,
                    5,
                    OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
                        ExpressionGet::new(
                            4,
                            vec![],
                            vec!["key".into(), "amount".into()],
                            vec![LogicalType::Integer, LogicalType::Double],
                        ),
                    )),
                    groups,
                    vec![],
                    vec![Expression::Aggregate(
                        AggregateExpression::new(
                            sum,
                            vec![col(4, 1, LogicalType::Double)],
                            LogicalType::Double,
                        )
                        .into(),
                    )],
                    vec![],
                )),
            )));
            join.left.stats.unique_keys = vec![key];
            let Expression::Aggregate(sum) = &mut aggregate.aggregates[0] else {
                unreachable!()
            };
            sum.children = vec![col(3, 0, LogicalType::Double)];
            plan
        };
        let (_, changed) =
            dimension_deferral::optimize_plan(make_plan(), &BindContext::new()).unwrap();
        assert_eq!(changed, key_column != 0, "reference key coverage");
        let mut input =
            MemoBuilder::build(make_plan(), BindContext::new(), SearchBudget::default()).unwrap();
        let state = input.planner_state.clone();
        state.write().unwrap().session =
            Some(paro_context::TestStatementContextBuilder::minimal().build());
        let root = input.root;
        let expression = input.memo.group(root).unwrap().logical_exprs()[0];
        let binding = matching::scoped_pattern_bindings(
            PlannerTransformation::AggregateDimensionDeferral,
            root,
            expression,
            &input.memo,
            &state.read().unwrap(),
            None,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .bindings
        .first()
        .cloned()
        .unwrap();
        let rule = PlannerTransformationRule {
            transformation: PlannerTransformation::AggregateDimensionDeferral,
            planner_state: state,
        };
        let mut context = TransformContext::new(&mut input.memo, root);
        let bridges = semantic_plan::owned_binding_instantiation_count();
        assert_eq!(
            !rule
                .apply_binding(&binding, &mut context)
                .unwrap()
                .is_empty(),
            changed,
            "key_column={key_column}"
        );
        assert_eq!(semantic_plan::owned_binding_instantiation_count(), bridges);
    }
}

#[test]
fn production_deferral_preserves_dimension_key_evaluation_barrier() {
    use paro_function::scalar::{BoundScalarFunction, FunctionStability, ScalarFunctionSet};
    use paro_planner::expression::FunctionExpression;
    for reversed in [false, true] {
        let make_plan = || {
            let mut plan = candidate(false);
            let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
                unreachable!()
            };
            let LogicalOperator::Join(Join::Comparison(join)) = &mut aggregate.child.operator
            else {
                unreachable!()
            };
            let mut arithmetic = ScalarFunctionSet::new("-".into());
            paro_function::scalar::operators::arithmetic::register_arithmetic_functions(
                &mut arithmetic,
            );
            let (function, _) = arithmetic
                .bind(&[LogicalType::Integer, LogicalType::Integer])
                .unwrap();
            let mut function = BoundScalarFunction::from(function);
            function.stability = FunctionStability::Volatile;
            let expression = Expression::Function(
                FunctionExpression::new(
                    function,
                    vec![
                        col(1, 0, LogicalType::Integer),
                        col(1, 0, LogicalType::Integer),
                    ],
                    LogicalType::Integer,
                )
                .into(),
            );
            assert!(expression.evaluation_properties().is_reorder_fence());
            join.conditions[0].right = expression;
            if reversed {
                let condition = &mut join.conditions[0];
                std::mem::swap(&mut condition.left, &mut condition.right);
            }
            plan
        };
        let (_, changed) =
            dimension_deferral::optimize_plan(make_plan(), &BindContext::new()).unwrap();
        assert!(!changed, "reference must preserve evaluation boundary");
        let mut input =
            MemoBuilder::build(make_plan(), BindContext::new(), SearchBudget::default()).unwrap();
        let state = input.planner_state.clone();
        state.write().unwrap().session =
            Some(paro_context::TestStatementContextBuilder::minimal().build());
        let root = input.root;
        let expression = input.memo.group(root).unwrap().logical_exprs()[0];
        let binding = matching::scoped_pattern_bindings(
            PlannerTransformation::AggregateDimensionDeferral,
            root,
            expression,
            &input.memo,
            &state.read().unwrap(),
            None,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .bindings
        .first()
        .cloned()
        .unwrap();
        let rule = PlannerTransformationRule {
            transformation: PlannerTransformation::AggregateDimensionDeferral,
            planner_state: state,
        };
        let mut context = TransformContext::new(&mut input.memo, root);
        let bridges = semantic_plan::owned_binding_instantiation_count();
        assert!(
            rule.apply_binding(&binding, &mut context)
                .unwrap()
                .is_empty(),
            "reversed={reversed}"
        );
        assert_eq!(semantic_plan::owned_binding_instantiation_count(), bridges);
    }
}

fn candidate(reference: bool) -> OwnedLogicalPlan {
    let dimension = if reference {
        LogicalOperator::CTERef(CTERef::new(
            50,
            1,
            "dimension_ref".into(),
            vec!["key".into(), "label".into()],
            vec![LogicalType::Integer, LogicalType::Varchar],
        ))
    } else {
        LogicalOperator::Get(Box::new(Get::new_without_table(
            1,
            vec!["key".into(), "label".into()],
            vec![LogicalType::Integer, LogicalType::Varchar],
        )))
    };
    let fact = OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
        0,
        vec![],
        vec!["key".into(), "amount".into()],
        vec![LogicalType::Integer, LogicalType::Double],
    )));
    let (sum, _) = paro_function::aggregate::distributive::sum::get_sum_function()
        .bind(&[LogicalType::Double])
        .unwrap();
    OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(Aggregate::new(
        10,
        11,
        12,
        OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            fact,
            OwnedLogicalPlan::synthetic(dimension),
            vec![JoinCondition::equality(
                col(0, 0, LogicalType::Integer),
                col(1, 0, LogicalType::Integer),
            )],
        ))),
        vec![col(1, 1, LogicalType::Varchar)],
        vec![],
        vec![Expression::Aggregate(
            AggregateExpression::new(
                sum,
                vec![col(0, 1, LogicalType::Double)],
                LogicalType::Double,
            )
            .into(),
        )],
        vec![],
    ))))
}

#[test]
fn production_deferral_inlines_nonidentity_projection_spines() {
    for (depth, nary) in [(1, false), (2, false), (0, true), (2, true)] {
        let make_plan = || {
            let mut plan = candidate(false);
            let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
                unreachable!()
            };
            if nary {
                let child = std::mem::replace(&mut aggregate.child, Box::new(candidate(false)));
                aggregate.child = Box::new(OwnedLogicalPlan::synthetic(LogicalOperator::Join(
                    Join::comparison(
                        JoinType::Inner,
                        *child,
                        OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(
                            Get::new_without_table(
                                2,
                                vec!["key".into()],
                                vec![LogicalType::Integer],
                            ),
                        ))),
                        vec![JoinCondition::equality(
                            col(0, 0, LogicalType::Integer),
                            col(2, 0, LogicalType::Integer),
                        )],
                    ),
                )));
                if depth == 2 {
                    let LogicalOperator::Join(Join::Comparison(join)) =
                        &mut aggregate.child.operator
                    else {
                        unreachable!()
                    };
                    std::mem::swap(&mut join.left, &mut join.right);
                    let condition = &mut join.conditions[0];
                    std::mem::swap(&mut condition.left, &mut condition.right);
                }
            }
            for level in 0..depth {
                let (label, amount) = if level == 0 {
                    (
                        col(1, 1, LogicalType::Varchar),
                        col(0, 1, LogicalType::Double),
                    )
                } else {
                    (
                        col(20 + level - 1, 1, LogicalType::Varchar),
                        col(20 + level - 1, 0, LogicalType::Double),
                    )
                };
                let child = std::mem::replace(&mut aggregate.child, Box::new(candidate(false)));
                aggregate.child = Box::new(OwnedLogicalPlan::synthetic(
                    LogicalOperator::Projection(paro_planner::operator::Projection::new(
                        20 + level,
                        *child,
                        vec![amount, label],
                    )),
                ));
            }
            if depth > 0 {
                aggregate.groups = vec![col(19 + depth, 1, LogicalType::Varchar)];
                let Expression::Aggregate(sum) = &mut aggregate.aggregates[0] else {
                    unreachable!()
                };
                sum.children = vec![col(19 + depth, 0, LogicalType::Double)];
            }
            plan
        };
        let bind = BindContext::new();
        for _ in 0..52 {
            bind.generate_table_index();
        }
        let (expected, changed) = dimension_deferral::optimize_plan(make_plan(), &bind).unwrap();
        assert!(changed);
        let mut input =
            MemoBuilder::build(make_plan(), BindContext::new(), SearchBudget::default()).unwrap();
        let state = input.planner_state.clone();
        state.write().unwrap().session =
            Some(paro_context::TestStatementContextBuilder::minimal().build());
        let root = input.root;
        let expression = input.memo.group(root).unwrap().logical_exprs()[0];
        let binding = matching::scoped_pattern_bindings(
            PlannerTransformation::AggregateDimensionDeferral,
            root,
            expression,
            &input.memo,
            &state.read().unwrap(),
            None,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .bindings
        .first()
        .cloned()
        .unwrap();
        let rule = PlannerTransformationRule {
            transformation: PlannerTransformation::AggregateDimensionDeferral,
            planner_state: state.clone(),
        };
        let before = semantic_plan::owned_binding_instantiation_count();
        let arena = state.read().unwrap().staging_arena.len();
        let mut context = TransformContext::new(&mut input.memo, root);
        let outputs = rule.apply_binding(&binding, &mut context).unwrap();
        assert_eq!(outputs.len(), 1);
        assert_eq!(
            semantic_plan::owned_binding_instantiation_count(),
            before,
            "depth={depth}"
        );
        let state = state.read().unwrap();
        assert_eq!(state.staging_arena.len(), arena);
        let LogicalOperator::Aggregate(actual) = &state.payloads.logical
            [outputs[0].payload.index()]
        .semantic_template
        .operator
        else {
            unreachable!()
        };
        assert_eq!(actual.returned_types, expected.types());
        assert_column(&actual.groups, 1, 1);
        let join_group = outputs[0].key.children[0];
        let join_expr = context.memo().group(join_group).unwrap().logical_exprs()[0];
        let partial_group = context.memo().logical_expr(join_expr).unwrap().key.children[1];
        let partial_expr = context.memo().group(partial_group).unwrap().logical_exprs()[0];
        let payload = context.memo().logical_expr(partial_expr).unwrap().payload;
        let LogicalOperator::Aggregate(partial) = &state.payloads.logical[payload.index()]
            .semantic_template
            .operator
        else {
            unreachable!()
        };
        let Expression::Aggregate(sum) = &partial.aggregates[0] else {
            unreachable!()
        };
        assert_column(&sum.children, 0, 1);
        assert_column(&partial.groups, 0, 0);
        if nary {
            let fact = context
                .memo()
                .logical_expr(partial_expr)
                .unwrap()
                .key
                .children[0];
            let fact_expression = context.memo().group(fact).unwrap().logical_exprs()[0];
            let payload = context
                .memo()
                .logical_expr(fact_expression)
                .unwrap()
                .payload;
            let LogicalOperator::Join(Join::Comparison(join)) = &state.payloads.logical
                [payload.index()]
            .semantic_template
            .operator
            else {
                panic!("fact region must retain the other join")
            };
            assert_eq!(join.conditions.len(), 1);
            assert_column(
                std::slice::from_ref(&join.conditions[0].left),
                if depth == 2 { 2 } else { 0 },
                0,
            );
            assert_column(
                std::slice::from_ref(&join.conditions[0].right),
                if depth == 2 { 0 } else { 2 },
                0,
            );
        }
    }
}

#[test]
fn production_deferral_retains_the_exact_dimension_reference() {
    for (reference, projected, constrained_fact) in [
        (false, false, false),
        (true, false, false),
        (false, true, false),
        (true, true, false),
        (false, false, true),
        (true, false, true),
    ] {
        let make_plan = || {
            let mut plan = candidate(reference);
            if constrained_fact {
                let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
                    unreachable!()
                };
                let LogicalOperator::Join(Join::Comparison(join)) = &mut aggregate.child.operator
                else {
                    unreachable!()
                };
                let fact = std::mem::replace(&mut join.left, Box::new(candidate(false)));
                let mut inner = Join::comparison(
                    JoinType::Inner,
                    *fact,
                    OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(
                        Get::new_without_table(2, vec!["key".into()], vec![LogicalType::Integer]),
                    ))),
                    vec![JoinCondition::equality(
                        col(0, 0, LogicalType::Integer),
                        col(2, 0, LogicalType::Integer),
                    )],
                );
                let Join::Comparison(inner_join) = &mut inner else {
                    unreachable!()
                };
                inner_join.build_side_constraint =
                    paro_planner::operator::JoinBuildSideConstraint::Right;
                join.left = Box::new(OwnedLogicalPlan::synthetic(LogicalOperator::Join(inner)));
            }
            if projected {
                let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
                    unreachable!()
                };
                let LogicalOperator::Join(Join::Comparison(join)) = &mut aggregate.child.operator
                else {
                    unreachable!()
                };
                join.left_projection_map = paro_planner::operator::ProjectionMap::new(vec![1]);
                join.right_projection_map = paro_planner::operator::ProjectionMap::new(vec![1]);
            }
            plan
        };
        let bind = BindContext::new();
        for _ in 0..52 {
            bind.generate_table_index();
        }
        let (expected, changed) = dimension_deferral::optimize_plan(make_plan(), &bind).unwrap();
        assert!(changed);
        let plan = if reference {
            OwnedLogicalPlan::synthetic(LogicalOperator::MaterializedCTE(
                MaterializedCTE::new(
                    50,
                    "dimension".into(),
                    vec!["key".into(), "label".into()],
                    vec![LogicalType::Integer, LogicalType::Varchar],
                    paro_planner::binder::ir::CTEMaterialize::Materialized,
                    OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
                        ExpressionGet::new(
                            51,
                            vec![],
                            vec!["key".into(), "label".into()],
                            vec![LogicalType::Integer, LogicalType::Varchar],
                        ),
                    )),
                    make_plan(),
                )
                .with_ref_count(1),
            ))
        } else {
            make_plan()
        };
        let mut input =
            MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
        let state = input.planner_state.clone();
        state.write().unwrap().session =
            Some(paro_context::TestStatementContextBuilder::minimal().build());
        let root = if reference {
            let owner = input.memo.group(input.root).unwrap().logical_exprs()[0];
            input.memo.logical_expr(owner).unwrap().key.children[1]
        } else {
            input.root
        };
        let root_expr = input.memo.group(root).unwrap().logical_exprs()[0];
        let join_group = input.memo.logical_expr(root_expr).unwrap().key.children[0];
        let join_expr = input.memo.group(join_group).unwrap().logical_exprs()[0];
        if projected {
            let state = state.read().unwrap();
            let payload = input.memo.logical_expr(join_expr).unwrap().payload;
            let LogicalOperator::Join(Join::Comparison(join)) = &state.payloads.logical
                [payload.index()]
            .semantic_template
            .operator
            else {
                unreachable!()
            };
            assert!(join.left_projection_map.is_all());
            assert!(join.right_projection_map.is_all());
        }
        let dimension_group = input.memo.logical_expr(join_expr).unwrap().key.children[1];
        let original_fact = input.memo.logical_expr(join_expr).unwrap().key.children[0];
        let before = input.memo.local_statistics_fingerprint(dimension_group);
        let binding = matching::scoped_pattern_bindings(
            PlannerTransformation::AggregateDimensionDeferral,
            root,
            root_expr,
            &input.memo,
            &state.read().unwrap(),
            None,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .bindings
        .first()
        .cloned()
        .unwrap();
        let rule = PlannerTransformationRule {
            transformation: PlannerTransformation::AggregateDimensionDeferral,
            planner_state: state.clone(),
        };
        let bridges = semantic_plan::owned_binding_instantiation_count();
        let arena = state.read().unwrap().staging_arena.len();
        let mut context = TransformContext::new(&mut input.memo, root);
        let outputs = rule.apply_binding(&binding, &mut context).unwrap();
        assert_eq!(outputs.len(), 1);
        assert_eq!(
            semantic_plan::owned_binding_instantiation_count(),
            bridges,
            "reference={reference}"
        );
        let joined = context
            .memo()
            .group(outputs[0].key.children[0])
            .unwrap()
            .logical_exprs()[0];
        assert_eq!(
            context.memo().logical_expr(joined).unwrap().key.children[0],
            dimension_group
        );
        let partial = context.memo().logical_expr(joined).unwrap().key.children[1];
        let partial_expr = context.memo().group(partial).unwrap().logical_exprs()[0];
        assert_eq!(
            context
                .memo()
                .logical_expr(partial_expr)
                .unwrap()
                .key
                .children[0],
            original_fact
        );
        if constrained_fact {
            let fact_expr = context.memo().group(original_fact).unwrap().logical_exprs()[0];
            let payload = context.memo().logical_expr(fact_expr).unwrap().payload;
            let state = state.read().unwrap();
            let LogicalOperator::Join(Join::Comparison(join)) = &state.payloads.logical
                [payload.index()]
            .semantic_template
            .operator
            else {
                unreachable!()
            };
            assert_eq!(
                join.build_side_constraint,
                paro_planner::operator::JoinBuildSideConstraint::Right
            );
        }
        assert_eq!(
            context.memo().local_statistics_fingerprint(dimension_group),
            before
        );
        let state = state.read().unwrap();
        assert_eq!(state.staging_arena.len(), arena);
        let LogicalOperator::Aggregate(actual) = &state.payloads.logical
            [outputs[0].payload.index()]
        .semantic_template
        .operator
        else {
            panic!("aggregate root")
        };
        assert_eq!(actual.returned_types, expected.types());
    }
}
