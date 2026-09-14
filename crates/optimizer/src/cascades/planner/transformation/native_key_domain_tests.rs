// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::cascades::budget::{BudgetDimension, SearchBudget};
use crate::cascades::planner::MemoBuilder;
use paro_common::types::LogicalType;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{ColumnRefExpression, FunctionExpression};
use paro_planner::operator::{Get, JoinCondition};

fn col(table: usize) -> Expression {
    Expression::ColumnRef(
        ColumnRefExpression::new(ColumnBinding::new(table, 0), LogicalType::Integer).into(),
    )
}
fn scan(table: usize) -> OwnedLogicalPlan {
    OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(Get::new_without_table(
        table,
        vec!["key".into()],
        vec![LogicalType::Integer],
    ))))
}

fn candidate(volatile: bool) -> OwnedLogicalPlan {
    let mut expressions = vec![col(0)];
    if volatile {
        let function = paro_function::scalar::math::get_random_function()
            .functions
            .into_iter()
            .next()
            .unwrap();
        expressions.push(Expression::Function(
            FunctionExpression::new(function, vec![], LogicalType::Double).into(),
        ));
    }
    let probe = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
        10,
        scan(0),
        expressions,
    )));
    OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::comparison(
        JoinType::Semi,
        probe,
        scan(20),
        vec![JoinCondition::equality(col(10), col(20))],
    )))
}

#[test]
fn production_key_domain_respects_the_entire_probe_evaluation_barrier() {
    for volatile in [false, true] {
        assert_eq!(
            crate::filter::domain_transfer::transfer(candidate(volatile))
                .unwrap()
                .is_some(),
            !volatile
        );
        let mut input = MemoBuilder::build(
            candidate(volatile),
            BindContext::new(),
            SearchBudget::default(),
        )
        .unwrap();
        let state = input.planner_state.clone();
        state.write().unwrap().session =
            Some(paro_context::TestStatementContextBuilder::minimal().build());
        let root = input.memo.group(input.root).unwrap().logical_exprs()[0];
        let binding = matching::scoped_pattern_bindings(
            PlannerTransformation::KeyDomainTransfer,
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
        .expect("key-domain binding");
        let rule = PlannerTransformationRule {
            transformation: PlannerTransformation::KeyDomainTransfer,
            planner_state: state,
        };
        let mut ctx = TransformContext::new(&mut input.memo, input.root);
        let outputs = rule.apply_binding(&binding, &mut ctx).unwrap();
        assert_eq!(outputs.len(), usize::from(!volatile), "volatile={volatile}");
    }
}
