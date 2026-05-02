// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bound Expression
//!
//!

use paro_common::types::LogicalType;
use paro_external::routine::identity::RoutineCallIdentity;

use super::{
    AggregateExpression, CaseExpression, CastExpression, ColumnRefExpression, ComparisonExpression,
    ConjunctionExpression, ConstantExpression, FunctionExpression, OperatorExpression,
    ReferenceExpression, SubqueryExpression, WindowExpression,
};

/// Expression represents a semantic-aware version of a SQL expression.
#[derive(Debug, Clone)]
pub enum Expression {
    Constant(ConstantExpression),
    ColumnRef(ColumnRefExpression),
    Function(FunctionExpression),
    Cast(CastExpression),
    Conjunction(ConjunctionExpression),
    Case(CaseExpression),
    Comparison(ComparisonExpression),
    Operator(OperatorExpression),
    Reference(ReferenceExpression),
    Aggregate(AggregateExpression),
    Subquery(SubqueryExpression),
    Window(WindowExpression),
}

impl Expression {
    pub fn return_type(&self) -> LogicalType {
        match self {
            Expression::Constant(expr) => expr.return_type.clone(),
            Expression::ColumnRef(expr) => expr.return_type.clone(),
            Expression::Function(expr) => expr.return_type.clone(),
            Expression::Cast(expr) => expr.target_type.clone(),
            Expression::Conjunction(expr) => expr.return_type(),
            Expression::Case(expr) => expr.return_type(),
            Expression::Comparison(expr) => expr.return_type(),
            Expression::Operator(expr) => expr.return_type.clone(),
            Expression::Reference(expr) => expr.return_type.clone(),
            Expression::Aggregate(expr) => expr.return_type.clone(),
            Expression::Subquery(expr) => expr.return_type(),
            Expression::Window(expr) => expr.return_type(),
        }
    }

    /// Get the expression return type for type inference during binding.
    pub fn get_expression_return_type(&self) -> LogicalType {
        if let Expression::Constant(constant) = self {
            if matches!(&constant.return_type, LogicalType::Varchar) {
                return LogicalType::StringLiteral;
            }
            if constant.return_type.is_integral() {
                if let Some(v) = constant.value.as_i64() {
                    return LogicalType::IntegerLiteral(v);
                }
            }
        }
        self.return_type()
    }

    pub fn contains_external_routine(&self) -> bool {
        match self {
            Expression::Function(expr) => {
                expr.crosses_execution_boundary()
                    || expr.children.iter().any(Self::contains_external_routine)
            }
            Expression::Cast(expr) => expr.child.contains_external_routine(),
            Expression::Conjunction(expr) => {
                expr.children.iter().any(Self::contains_external_routine)
            }
            Expression::Case(expr) => {
                expr.check.contains_external_routine()
                    || expr.result_if_true.contains_external_routine()
                    || expr.result_if_false.contains_external_routine()
            }
            Expression::Comparison(expr) => {
                expr.left.contains_external_routine() || expr.right.contains_external_routine()
            }
            Expression::Operator(expr) => expr.children.iter().any(Self::contains_external_routine),
            Expression::Aggregate(expr) => {
                expr.children.iter().any(Self::contains_external_routine)
                    || expr
                        .filter
                        .as_ref()
                        .is_some_and(|filter| filter.contains_external_routine())
                    || expr
                        .order_bys
                        .iter()
                        .any(|order| order.expression.contains_external_routine())
            }
            Expression::Subquery(expr) => expr.children.iter().any(Self::contains_external_routine),
            Expression::Window(expr) => {
                expr.children.iter().any(Self::contains_external_routine)
                    || expr.partitions.iter().any(Self::contains_external_routine)
                    || expr
                        .orders
                        .iter()
                        .any(|order| order.expression.contains_external_routine())
            }
            Expression::Constant(_) | Expression::ColumnRef(_) | Expression::Reference(_) => false,
        }
    }

    /// Recursively replace ColumnRef expressions using the provided mapping function.
    pub fn replace_column_ref<F>(self, f: &F) -> Expression
    where
        F: Fn(&ColumnRefExpression) -> Option<Expression>,
    {
        match self {
            Expression::ColumnRef(ref expr) => {
                if let Some(new_expr) = f(expr) {
                    new_expr
                } else {
                    self
                }
            }
            Expression::Function(mut expr) => {
                expr.children = expr
                    .children
                    .into_iter()
                    .map(|c| c.replace_column_ref(f))
                    .collect();
                Expression::Function(expr)
            }
            Expression::Cast(mut expr) => {
                expr.child = Box::new(expr.child.replace_column_ref(f));
                Expression::Cast(expr)
            }
            Expression::Conjunction(mut expr) => {
                expr.children = expr
                    .children
                    .into_iter()
                    .map(|c| c.replace_column_ref(f))
                    .collect();
                Expression::Conjunction(expr)
            }
            Expression::Case(mut expr) => {
                expr.check = Box::new(expr.check.replace_column_ref(f));
                expr.result_if_true = Box::new(expr.result_if_true.replace_column_ref(f));
                expr.result_if_false = Box::new(expr.result_if_false.replace_column_ref(f));
                Expression::Case(expr)
            }
            Expression::Comparison(mut expr) => {
                expr.left = Box::new(expr.left.replace_column_ref(f));
                expr.right = Box::new(expr.right.replace_column_ref(f));
                Expression::Comparison(expr)
            }
            Expression::Operator(mut expr) => {
                expr.children = expr
                    .children
                    .into_iter()
                    .map(|c| c.replace_column_ref(f))
                    .collect();
                Expression::Operator(expr)
            }
            Expression::Aggregate(mut expr) => {
                expr.children = expr
                    .children
                    .into_iter()
                    .map(|c| c.replace_column_ref(f))
                    .collect();
                expr.filter = expr
                    .filter
                    .map(|filter| Box::new(filter.replace_column_ref(f)));
                expr.order_bys = expr
                    .order_bys
                    .into_iter()
                    .map(|mut order| {
                        order.expression = order.expression.replace_column_ref(f);
                        order
                    })
                    .collect();
                Expression::Aggregate(expr)
            }
            Expression::Subquery(mut expr) => {
                expr.children = expr
                    .children
                    .into_iter()
                    .map(|c| c.replace_column_ref(f))
                    .collect();
                Expression::Subquery(expr)
            }
            Expression::Window(mut expr) => {
                expr.children = expr
                    .children
                    .into_iter()
                    .map(|c| c.replace_column_ref(f))
                    .collect();
                expr.partitions = expr
                    .partitions
                    .into_iter()
                    .map(|c| c.replace_column_ref(f))
                    .collect();
                expr.orders = expr
                    .orders
                    .into_iter()
                    .map(|mut o| {
                        o.expression = o.expression.replace_column_ref(f);
                        o
                    })
                    .collect();
                Expression::Window(expr)
            }
            _ => self,
        }
    }

    /// Recursively replace expressions that match grouping expressions with BoundReferenceExpressions.
    pub fn replace_groups(self, groups: &[Expression]) -> Expression {
        for (i, group) in groups.iter().enumerate() {
            if self.equals(group) {
                return Expression::Reference(ReferenceExpression::new(i, self.return_type()));
            }
        }

        match self {
            Expression::Function(mut expr) => {
                expr.children = expr
                    .children
                    .into_iter()
                    .map(|c| c.replace_groups(groups))
                    .collect();
                Expression::Function(expr)
            }
            Expression::Cast(mut expr) => {
                expr.child = Box::new(expr.child.replace_groups(groups));
                Expression::Cast(expr)
            }
            Expression::Conjunction(mut expr) => {
                expr.children = expr
                    .children
                    .into_iter()
                    .map(|c| c.replace_groups(groups))
                    .collect();
                Expression::Conjunction(expr)
            }
            Expression::Case(mut expr) => {
                expr.check = Box::new(expr.check.replace_groups(groups));
                expr.result_if_true = Box::new(expr.result_if_true.replace_groups(groups));
                expr.result_if_false = Box::new(expr.result_if_false.replace_groups(groups));
                Expression::Case(expr)
            }
            Expression::Comparison(mut expr) => {
                expr.left = Box::new(expr.left.replace_groups(groups));
                expr.right = Box::new(expr.right.replace_groups(groups));
                Expression::Comparison(expr)
            }
            Expression::Operator(mut expr) => {
                expr.children = expr
                    .children
                    .into_iter()
                    .map(|c| c.replace_groups(groups))
                    .collect();
                Expression::Operator(expr)
            }
            Expression::Aggregate(mut expr) => {
                expr.children = expr
                    .children
                    .into_iter()
                    .map(|c| c.replace_groups(groups))
                    .collect();
                expr.filter = expr
                    .filter
                    .map(|filter| Box::new(filter.replace_groups(groups)));
                expr.order_bys = expr
                    .order_bys
                    .into_iter()
                    .map(|mut order| {
                        order.expression = order.expression.replace_groups(groups);
                        order
                    })
                    .collect();
                Expression::Aggregate(expr)
            }
            Expression::Subquery(mut expr) => {
                expr.children = expr
                    .children
                    .into_iter()
                    .map(|c| c.replace_groups(groups))
                    .collect();
                Expression::Subquery(expr)
            }
            Expression::Window(mut expr) => {
                expr.children = expr
                    .children
                    .into_iter()
                    .map(|c| c.replace_groups(groups))
                    .collect();
                expr.partitions = expr
                    .partitions
                    .into_iter()
                    .map(|c| c.replace_groups(groups))
                    .collect();
                expr.orders = expr
                    .orders
                    .into_iter()
                    .map(|mut o| {
                        o.expression = o.expression.replace_groups(groups);
                        o
                    })
                    .collect();
                Expression::Window(expr)
            }
            _ => self,
        }
    }

    /// Recursively find all aggregate expressions and replace them with BoundReferenceExpressions.
    pub fn extract_aggregates(self, aggregates: &mut Vec<Expression>, offset: usize) -> Expression {
        match self {
            Expression::Aggregate(agg) => {
                let index = offset + aggregates.len();
                let return_type = agg.return_type.clone();
                aggregates.push(Expression::Aggregate(agg));
                Expression::Reference(ReferenceExpression::new(index, return_type))
            }
            Expression::Function(mut expr) => {
                expr.children = expr
                    .children
                    .into_iter()
                    .map(|c| c.extract_aggregates(aggregates, offset))
                    .collect();
                Expression::Function(expr)
            }
            Expression::Operator(mut expr) => {
                expr.children = expr
                    .children
                    .into_iter()
                    .map(|c| c.extract_aggregates(aggregates, offset))
                    .collect();
                Expression::Operator(expr)
            }
            Expression::Comparison(mut expr) => {
                expr.left = Box::new(expr.left.extract_aggregates(aggregates, offset));
                expr.right = Box::new(expr.right.extract_aggregates(aggregates, offset));
                Expression::Comparison(expr)
            }
            Expression::Cast(mut expr) => {
                expr.child = Box::new(expr.child.extract_aggregates(aggregates, offset));
                Expression::Cast(expr)
            }
            Expression::Conjunction(mut expr) => {
                expr.children = expr
                    .children
                    .into_iter()
                    .map(|c| c.extract_aggregates(aggregates, offset))
                    .collect();
                Expression::Conjunction(expr)
            }
            Expression::Case(mut expr) => {
                expr.check = Box::new(expr.check.extract_aggregates(aggregates, offset));
                expr.result_if_true =
                    Box::new(expr.result_if_true.extract_aggregates(aggregates, offset));
                expr.result_if_false =
                    Box::new(expr.result_if_false.extract_aggregates(aggregates, offset));
                Expression::Case(expr)
            }
            _ => self,
        }
    }

    /// Check if two expressions are semantically equal.
    pub fn equals(&self, other: &Expression) -> bool {
        match (self, other) {
            (Expression::ColumnRef(a), Expression::ColumnRef(b)) => {
                a.binding == b.binding && a.depth == b.depth
            }
            (Expression::Constant(a), Expression::Constant(b)) => a.value == b.value,
            (Expression::Function(a), Expression::Function(b)) => {
                routine_identities_equal(a.routine_identity(), b.routine_identity(), || {
                    a.function.name == b.function.name
                }) && a.function.arguments == b.function.arguments
                    && a.children.len() == b.children.len()
                    && a.children
                        .iter()
                        .zip(&b.children)
                        .all(|(ca, cb)| ca.equals(cb))
                    && match (&a.function.bind_data, &b.function.bind_data) {
                        (Some(ad), Some(bd)) => ad.equals(&**bd),
                        (None, None) => true,
                        _ => false,
                    }
            }
            (Expression::Cast(a), Expression::Cast(b)) => {
                a.target_type == b.target_type && a.child.equals(&b.child)
            }
            (Expression::Conjunction(a), Expression::Conjunction(b)) => {
                a.conjunction_type == b.conjunction_type
                    && a.children.len() == b.children.len()
                    && a.children
                        .iter()
                        .zip(&b.children)
                        .all(|(ca, cb)| ca.equals(cb))
            }
            (Expression::Case(a), Expression::Case(b)) => {
                a.check.equals(&b.check)
                    && a.result_if_true.equals(&b.result_if_true)
                    && a.result_if_false.equals(&b.result_if_false)
            }
            (Expression::Comparison(a), Expression::Comparison(b)) => {
                a.comparison_type == b.comparison_type
                    && a.left.equals(&b.left)
                    && a.right.equals(&b.right)
            }
            (Expression::Operator(a), Expression::Operator(b)) => {
                a.operator_type == b.operator_type
                    && a.children.len() == b.children.len()
                    && a.children
                        .iter()
                        .zip(&b.children)
                        .all(|(ca, cb)| ca.equals(cb))
            }
            (Expression::Reference(a), Expression::Reference(b)) => a.index == b.index,
            (Expression::Aggregate(a), Expression::Aggregate(b)) => {
                a.function.name == b.function.name
                    && a.function.arguments == b.function.arguments
                    && a.children.len() == b.children.len()
                    && a.children
                        .iter()
                        .zip(&b.children)
                        .all(|(ca, cb)| ca.equals(cb))
                    && a.aggr_type == b.aggr_type
                    && match (&a.filter, &b.filter) {
                        (Some(af), Some(bf)) => af.equals(bf),
                        (None, None) => true,
                        _ => false,
                    }
                    && a.order_bys.len() == b.order_bys.len()
                    && a.order_bys.iter().zip(&b.order_bys).all(|(ao, bo)| {
                        ao.ascending == bo.ascending
                            && ao.nulls_first == bo.nulls_first
                            && ao.expression.equals(&bo.expression)
                    })
                    && match (&a.bind_info, &b.bind_info) {
                        (Some(ad), Some(bd)) => ad.equals(&**bd),
                        (None, None) => true,
                        _ => false,
                    }
                    && match (&a.function.bind_data, &b.function.bind_data) {
                        (Some(ad), Some(bd)) => ad.equals(&**bd),
                        (None, None) => true,
                        _ => false,
                    }
            }
            (Expression::Window(a), Expression::Window(b)) => {
                a.function.name == b.function.name
                    && a.function.arguments == b.function.arguments
                    && a.children.len() == b.children.len()
                    && a.children
                        .iter()
                        .zip(&b.children)
                        .all(|(ca, cb)| ca.equals(cb))
            }
            _ => false,
        }
    }
}

fn routine_identities_equal(
    left: Option<&RoutineCallIdentity>,
    right: Option<&RoutineCallIdentity>,
    fallback: impl FnOnce() -> bool,
) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left == right,
        _ => fallback(),
    }
}
