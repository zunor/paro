// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for dependent-join decorrelation (pushdown + lateral flatten).

use crate::binder::Binder;
use crate::expression::{ConstantExpression, Expression};
use crate::operator::{Filter, JoinType, LogicalOperator};
use crate::plan::LogicalPlan;
use paro_common::runtime_value::Value;

pub(super) fn can_push_to_left_child(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Inner | JoinType::Semi | JoinType::Right
    )
}

pub(super) fn can_push_to_right_child(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Inner | JoinType::Semi | JoinType::Anti | JoinType::Left
    )
}

pub(super) fn push_filter_to_child(binder: &Binder, child: &mut LogicalPlan, expr: Expression) {
    match &mut child.operator {
        LogicalOperator::Filter(filter) => filter.expressions.push(expr),
        _ => {
            let inner_op = std::mem::replace(&mut child.operator, LogicalOperator::DummyScan);
            child.operator =
                LogicalOperator::Filter(Filter::new(binder.wrap_plan(inner_op), vec![expr]));
        }
    }
}

pub(super) fn should_eliminate_join_condition(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Constant(ConstantExpression {
            value: Value::Boolean(true),
            ..
        })
    )
}
