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
fn production_deferral_retains_the_exact_dimension_reference() {
    for reference in [false, true] {
        let bind = BindContext::new();
        for _ in 0..52 {
            bind.generate_table_index();
        }
        let (expected, changed) =
            dimension_deferral::optimize_plan(candidate(reference), &bind).unwrap();
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
                    candidate(true),
                )
                .with_ref_count(1),
            ))
        } else {
            candidate(false)
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
        let dimension_group = input.memo.logical_expr(join_expr).unwrap().key.children[1];
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
