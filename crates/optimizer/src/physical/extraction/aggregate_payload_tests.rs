// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::aggregate::distributive::sum::get_sum_function;
use paro_planner::expression::{
    AggregateExpression, CaseExpression, ConstantExpression, Expression, ReferenceExpression,
};

use super::plan_aggregate_payload;

#[test]
fn passive_conditional_decimal_sum_becomes_a_filtered_input() {
    let input_type = LogicalType::Decimal {
        precision: 15,
        scale: 2,
    };
    let (function, _) = get_sum_function()
        .bind(std::slice::from_ref(&input_type))
        .expect("bind decimal sum");
    let result_type = function.return_type.clone();
    let check = Expression::Reference(ReferenceExpression::new(0, LogicalType::Boolean));
    let value = Expression::Reference(ReferenceExpression::new(1, input_type.clone()));
    let conditional = Expression::Case(CaseExpression::new(
        check,
        value,
        Expression::Constant(ConstantExpression::new(
            Value::Null(input_type.clone()),
            input_type.clone(),
        )),
        input_type,
    ));
    let aggregate = Expression::Aggregate(AggregateExpression::new(
        function,
        vec![conditional],
        result_type,
    ));

    let payload = plan_aggregate_payload(vec![], vec![aggregate]).expect("plan payload");

    assert_eq!(payload.projection_exprs.len(), 2);
    assert_eq!(payload.aggregate_inputs[0].as_ref(), [0]);
    assert_eq!(payload.aggregate_filters[0], Some(1));
    let Expression::Aggregate(aggregate) = &payload.aggregates[0] else {
        panic!("expected aggregate descriptor");
    };
    assert!(
        matches!(aggregate.children.as_slice(), [Expression::Reference(reference)] if reference.index == 0)
    );
    assert!(
        matches!(aggregate.filter.as_deref(), Some(Expression::Reference(reference)) if reference.index == 1)
    );
}

#[test]
fn non_null_conditional_else_preserves_case_semantics() {
    let input_type = LogicalType::Decimal {
        precision: 15,
        scale: 2,
    };
    let (function, _) = get_sum_function()
        .bind(std::slice::from_ref(&input_type))
        .expect("bind decimal sum");
    let result_type = function.return_type.clone();
    let conditional = Expression::Case(CaseExpression::new(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Boolean)),
        Expression::Reference(ReferenceExpression::new(1, input_type.clone())),
        Expression::Constant(ConstantExpression::new(
            Value::Decimal(0, 15, 2),
            input_type.clone(),
        )),
        input_type,
    ));
    let aggregate = Expression::Aggregate(AggregateExpression::new(
        function,
        vec![conditional],
        result_type,
    ));

    let payload = plan_aggregate_payload(vec![], vec![aggregate]).expect("plan payload");

    assert_eq!(payload.projection_exprs.len(), 1);
    assert_eq!(payload.aggregate_filters[0], None);
    assert!(matches!(payload.projection_exprs[0], Expression::Case(_)));
}
