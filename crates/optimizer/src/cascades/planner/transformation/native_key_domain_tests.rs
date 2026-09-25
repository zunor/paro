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

fn candidate(volatile: bool, control: bool) -> OwnedLogicalPlan {
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
    let input = if control {
        OwnedLogicalPlan::synthetic(LogicalOperator::MaterializedCTE(
            paro_planner::operator::MaterializedCTE::new(
                50,
                "retained_input".into(),
                vec![],
                vec![],
                paro_planner::binder::ir::CTEMaterialize::Materialized,
                OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
                scan(0),
            ),
        ))
    } else {
        scan(0)
    };
    let probe = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
        10,
        input,
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
    for control in [false, true] {
        for volatile in [false, true] {
            assert_eq!(
                crate::rewrite::predicate::domain_transfer::transfer(candidate(volatile, control))
                    .unwrap()
                    .is_some(),
                !volatile
            );
            let mut input = MemoBuilder::build(
                candidate(volatile, control),
                BindContext::new(),
                SearchBudget::default(),
            )
            .unwrap();
            let state = input.planner_state.clone();
            state.write().unwrap().session =
                Some(paro_context::TestStatementContextBuilder::minimal().build());
            let root = input.memo.group(input.root).unwrap().logical_exprs()[0];
            let domain = input.memo.logical_expr(root).unwrap();
            let right = domain.key.children[1];
            let probe = input
                .memo
                .group(domain.key.children[0])
                .unwrap()
                .logical_exprs()[0];
            let left = input.memo.logical_expr(probe).unwrap().key.children[0];
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
            let bridges = semantic_plan::owned_binding_instantiation_count();
            let mut ctx = TransformContext::new(&mut input.memo, input.root);
            let outputs = rule.apply_binding(&binding, &mut ctx).unwrap();
            assert_eq!(outputs.len(), usize::from(!volatile), "volatile={volatile}");
            if let Some(output) = outputs.first() {
                let expression = ctx
                    .memo()
                    .group(output.key.children[0])
                    .unwrap()
                    .logical_exprs()[0];
                let restricted = ctx.memo().logical_expr(expression).unwrap();
                assert_eq!(
                    restricted.key.children.as_ref(),
                    &[left, right],
                    "restricted input and domain source must keep their exact group identities"
                );
            }
            assert_eq!(
                semantic_plan::owned_binding_instantiation_count(),
                bridges,
                "control={control}, volatile={volatile}"
            );
        }
    }
}

#[test]
fn key_domain_uses_the_canonical_memo_output_not_physical_projection_maps() {
    use paro_planner::operator::ProjectionMap;
    for map in [
        ProjectionMap::all(),
        ProjectionMap::new(vec![1]),
        ProjectionMap::new(vec![1, 0]),
        ProjectionMap::none(),
    ] {
        let mut plan = candidate(false, false);
        let LogicalOperator::Join(Join::Comparison(domain)) = &mut plan.operator else {
            unreachable!()
        };
        let LogicalOperator::Projection(probe) = &mut domain.left.operator else {
            unreachable!()
        };
        probe.expressions.push(col(0));
        probe.returned_types.push(LogicalType::Integer);
        domain.left_projection_map = map;
        let mut target_bindings = plan.output_layout().bindings().to_vec();
        target_bindings.sort();
        let mut input =
            MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
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
        .unwrap();
        let mut ctx = TransformContext::new(&mut input.memo, input.root);
        {
            let state = state.read().unwrap();
            let facts = boundary::BoundarySnapshot::read(
                &mut ctx,
                &state,
                &binding.root,
                BudgetDimension::RuleWorkPerGroup,
            )
            .unwrap()
            .unwrap();
            let original = NativeShell::from_pattern(ctx.memo(), &state, &binding.root, &facts)
                .unwrap()
                .unwrap();
            let LogicalOperator::Join(Join::Comparison(domain)) = original.root_operator() else {
                unreachable!()
            };
            assert!(domain.left_projection_map.is_all());
            assert!(domain.right_projection_map.is_none());
            let expected = original.root_layout().unwrap();
            assert_eq!(
                expected.bindings(),
                &[ColumnBinding::new(10, 0), ColumnBinding::new(10, 1)]
            );
            let rewritten =
                try_native_key_domain_transfer(&binding.root, ctx.memo(), &state, &facts)
                    .unwrap()
                    .unwrap();
            assert_eq!(rewritten.root_layout().unwrap().bindings(), target_bindings);
        }
        let before = semantic_plan::owned_binding_instantiation_count();
        let rule = PlannerTransformationRule {
            transformation: PlannerTransformation::KeyDomainTransfer,
            planner_state: state,
        };
        assert_eq!(rule.apply_binding(&binding, &mut ctx).unwrap().len(), 1);
        assert_eq!(semantic_plan::owned_binding_instantiation_count(), before);
    }
}
