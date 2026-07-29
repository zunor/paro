// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bound Expression
//!
//!

use paro_common::types::LogicalType;
use paro_external::routine::identity::RoutineCallIdentity;

use super::{
    AggregateExpression, CaseExpression, CastExpression, ColumnRefExpression, ComparisonExpression,
    ConjunctionExpression, ConstantExpression, ExpressionIterator, FunctionExpression,
    OperatorExpression, ParameterExpression, ReferenceExpression, SubqueryExpression,
    WindowExpression, WindowFrameBound,
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
    Parameter(ParameterExpression),
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
            Expression::Parameter(expr) => expr.return_type(),
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
        if matches!(self, Expression::Function(expr) if expr.crosses_execution_boundary()) {
            return true;
        }

        let mut contains_external = false;
        ExpressionIterator::enumerate_children(self, |child| {
            if !contains_external {
                contains_external = child.contains_external_routine();
            }
        });
        contains_external
    }

    /// Recursively replace ColumnRef expressions using the provided mapping function.
    pub fn replace_column_ref<F>(mut self, f: &F) -> Expression
    where
        F: Fn(&ColumnRefExpression) -> Option<Expression>,
    {
        self.replace_column_ref_in_place(f);
        self
    }

    fn replace_column_ref_in_place<F>(&mut self, f: &F)
    where
        F: Fn(&ColumnRefExpression) -> Option<Expression>,
    {
        if let Expression::ColumnRef(column_ref) = self {
            if let Some(replacement) = f(column_ref) {
                *self = replacement;
            }
            return;
        }

        ExpressionIterator::enumerate_children_mut(self, |child| {
            child.replace_column_ref_in_place(f);
        });
    }

    /// Recursively replace expressions that match grouping expressions with BoundReferenceExpressions.
    pub fn replace_groups(mut self, groups: &[Expression]) -> Expression {
        self.replace_groups_in_place(groups);
        self
    }

    fn replace_groups_in_place(&mut self, groups: &[Expression]) {
        for (i, group) in groups.iter().enumerate() {
            if self.equals(group) {
                *self = Expression::Reference(ReferenceExpression::new(i, self.return_type()));
                return;
            }
        }

        ExpressionIterator::enumerate_children_mut(self, |child| {
            child.replace_groups_in_place(groups);
        });
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
            Expression::Parameter(_) => self,
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
            (Expression::Parameter(a), Expression::Parameter(b)) => a.slot == b.slot,
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
                    && a.function.function_type == b.function.function_type
                    && a.function.arguments == b.function.arguments
                    && a.children.len() == b.children.len()
                    && a.children
                        .iter()
                        .zip(&b.children)
                        .all(|(ca, cb)| ca.equals(cb))
                    && a.partitions.len() == b.partitions.len()
                    && a.partitions
                        .iter()
                        .zip(&b.partitions)
                        .all(|(pa, pb)| pa.equals(pb))
                    && a.orders.len() == b.orders.len()
                    && a.orders.iter().zip(&b.orders).all(|(ao, bo)| {
                        ao.ascending == bo.ascending
                            && ao.nulls_first == bo.nulls_first
                            && ao.expression.equals(&bo.expression)
                    })
                    && a.frame.frame_type == b.frame.frame_type
                    && a.frame.start_is_preceding == b.frame.start_is_preceding
                    && a.frame.end_is_preceding == b.frame.end_is_preceding
                    && window_frame_bounds_equal(&a.frame.start_bound, &b.frame.start_bound)
                    && window_frame_bounds_equal(&a.frame.end_bound, &b.frame.end_bound)
                    && a.ignore_nulls == b.ignore_nulls
            }
            _ => false,
        }
    }
}

fn window_frame_bounds_equal(left: &WindowFrameBound, right: &WindowFrameBound) -> bool {
    match (left, right) {
        (WindowFrameBound::Unbounded, WindowFrameBound::Unbounded)
        | (WindowFrameBound::CurrentRow, WindowFrameBound::CurrentRow) => true,
        (WindowFrameBound::Offset(left), WindowFrameBound::Offset(right)) => left.equals(right),
        _ => false,
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

#[cfg(test)]
mod tests {
    use super::Expression;
    use crate::expression::{
        ColumnRefExpression, ConstantExpression, OrderByExpression, ReferenceExpression,
        WindowExpression, WindowFrame, WindowFrameBound, WindowFrameType,
    };
    use crate::operator::ColumnBinding;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_function::window::WindowFunction;

    fn int_column(column_index: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(10, column_index),
            LogicalType::Integer,
        ))
    }

    fn int_constant(value: i32) -> Expression {
        Expression::Constant(ConstantExpression::new(
            Value::Integer(value),
            LogicalType::Integer,
        ))
    }

    fn window_expression(start_bound: WindowFrameBound) -> Expression {
        Expression::Window(WindowExpression {
            function: WindowFunction::first_value(LogicalType::Integer),
            children: vec![int_column(0)],
            partitions: vec![int_column(1)],
            orders: vec![OrderByExpression {
                expression: int_column(2),
                ascending: true,
                nulls_first: false,
            }],
            frame: WindowFrame {
                frame_type: WindowFrameType::Rows,
                start_bound,
                start_is_preceding: true,
                end_bound: WindowFrameBound::CurrentRow,
                end_is_preceding: false,
            },
            ignore_nulls: false,
            return_type: LogicalType::Integer,
        })
    }

    #[test]
    fn replace_column_ref_visits_window_frame_offsets() {
        let rewritten = window_expression(WindowFrameBound::Offset(Box::new(int_column(3))))
            .replace_column_ref(&|column| {
                (column.binding.column_index == 3).then(|| int_constant(7))
            });

        let Expression::Window(window) = rewritten else {
            panic!("expected window expression");
        };
        let WindowFrameBound::Offset(offset) = window.frame.start_bound else {
            panic!("expected frame offset");
        };
        assert!(matches!(*offset, Expression::Constant(_)));
    }

    #[test]
    fn replace_groups_visits_window_frame_offsets() {
        let rewritten = window_expression(WindowFrameBound::Offset(Box::new(int_column(3))))
            .replace_groups(&[int_column(3)]);

        let Expression::Window(window) = rewritten else {
            panic!("expected window expression");
        };
        let WindowFrameBound::Offset(offset) = window.frame.start_bound else {
            panic!("expected frame offset");
        };
        assert!(matches!(
            *offset,
            Expression::Reference(ReferenceExpression { index: 0, .. })
        ));
    }

    #[test]
    fn window_equality_includes_window_semantics() {
        let original = window_expression(WindowFrameBound::Offset(Box::new(int_constant(1))));
        assert!(original.equals(&original.clone()));

        let mut different_partition = original.clone();
        let Expression::Window(window) = &mut different_partition else {
            unreachable!();
        };
        window.partitions[0] = int_column(9);
        assert!(!original.equals(&different_partition));

        let mut different_order = original.clone();
        let Expression::Window(window) = &mut different_order else {
            unreachable!();
        };
        window.orders[0].ascending = false;
        assert!(!original.equals(&different_order));

        let mut different_frame = original.clone();
        let Expression::Window(window) = &mut different_frame else {
            unreachable!();
        };
        window.frame.start_bound = WindowFrameBound::Offset(Box::new(int_constant(2)));
        assert!(!original.equals(&different_frame));

        let mut different_null_treatment = original.clone();
        let Expression::Window(window) = &mut different_null_treatment else {
            unreachable!();
        };
        window.ignore_nulls = true;
        assert!(!original.equals(&different_null_treatment));
    }
}
