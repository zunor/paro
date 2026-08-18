// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn random_call() -> Expression {
    let function = paro_function::scalar::math::get_random_function()
        .functions
        .into_iter()
        .next()
        .expect("random overload");
    Expression::Function(paro_planner::expression::FunctionExpression::new(
        function,
        vec![],
        LogicalType::Double,
    ))
}

#[test]
fn arena_generator_materializes_computed_window_arguments_once() {
    let ctx = BindContext::new();
    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["value".to_string(), "offset".to_string()],
            vec![LogicalType::Integer, LogicalType::BigInt],
        )),
    );
    let offset = Expression::Operator(OperatorExpression::new(
        OperatorType::Coalesce,
        vec![
            ref_expr(1, LogicalType::BigInt),
            Expression::Constant(ConstantExpression::new(
                Value::BigInt(1),
                LogicalType::BigInt,
            )),
        ],
        LogicalType::BigInt,
    ));
    let function = WindowFunction::nth_value(LogicalType::Integer);
    let window = LogicalPlan::new(
        &ctx,
        LogicalOperator::Window(LogicalWindow::new(
            2,
            vec![WindowExpression::native(
                function.clone(),
                vec![ref_expr(0, LogicalType::Integer), offset],
                Vec::new(),
                Vec::new(),
                WindowFrame::get_default_frame(&function),
                false,
            )],
            values,
        )),
    );

    let plan = PhysicalPlanGenerator::new(PlanBuildContext::default())
        .generate(&window)
        .expect("computed window arguments should lower through one input projection");
    let root = plan.node(plan.root);
    let PhysicalNodeKind::Window(spec) = &root.kind else {
        panic!("expected ordinary window")
    };
    let [project_id] = plan.child_ids(&root.children) else {
        panic!("window should have one projected input")
    };
    let PhysicalNodeKind::Project(project) = &plan.node(*project_id).kind else {
        panic!("computed argument should be materialized by a project")
    };
    assert_eq!(project.expressions.len(), 3);
    assert!(matches!(
        spec.expressions[0].arguments()[1],
        Expression::Reference(ref reference) if reference.index == 2
    ));
    assert_eq!(
        spec.input_width, 2,
        "synthetic inputs are not detail output"
    );
}

#[test]
fn arena_generator_preserves_independent_volatile_window_arguments() {
    let ctx = BindContext::new();
    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["value".to_string()],
            vec![LogicalType::Double],
        )),
    );
    let function = WindowFunction::first_value(LogicalType::Double);
    let expression = WindowExpression::native(
        function.clone(),
        vec![random_call()],
        Vec::new(),
        Vec::new(),
        WindowFrame::get_default_frame(&function),
        false,
    );
    let window = LogicalPlan::new(
        &ctx,
        LogicalOperator::Window(LogicalWindow::new(
            1,
            vec![expression.clone(), expression],
            values,
        )),
    );

    let plan = PhysicalPlanGenerator::new(PlanBuildContext::default())
        .generate(&window)
        .expect("volatile window arguments should remain independent");
    let root = plan.node(plan.root);
    let [project_id] = plan.child_ids(&root.children) else {
        panic!("window should have one projected input")
    };
    let PhysicalNodeKind::Project(project) = &plan.node(*project_id).kind else {
        panic!("volatile arguments should be materialized by a project")
    };
    assert_eq!(project.expressions.len(), 3);
}
