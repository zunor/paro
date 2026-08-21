// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::expression::{Expression, WindowExpression, WindowFrameBound, WindowInvocation};

pub struct ExpressionIterator;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpressionVisitDecision {
    Descend,
    SkipChildren,
}

impl ExpressionIterator {
    /// Visit an expression tree in pre-order with explicit subtree pruning.
    /// All child enumeration remains centralized here, so adding an expression
    /// variant cannot silently omit it from downstream analyses.
    pub fn visit<'a>(
        expr: &'a Expression,
        visitor: &mut impl FnMut(&'a Expression) -> ExpressionVisitDecision,
    ) {
        if visitor(expr) == ExpressionVisitDecision::SkipChildren {
            return;
        }
        Self::enumerate_children(expr, |child| Self::visit(child, visitor));
    }

    pub fn enumerate_children<'a>(expr: &'a Expression, mut f: impl FnMut(&'a Expression)) {
        match expr {
            Expression::Aggregate(e) => {
                for child in &e.children {
                    f(child);
                }
                if let Some(filter) = &e.filter {
                    f(filter);
                }
                for order in &e.order_bys {
                    f(&order.expression);
                }
            }
            Expression::Case(e) => {
                f(&e.check);
                f(&e.result_if_true);
                f(&e.result_if_false);
            }
            Expression::Cast(e) => {
                f(&e.child);
            }
            Expression::Comparison(e) => {
                f(&e.left);
                f(&e.right);
            }
            Expression::Conjunction(e) => {
                for child in &e.children {
                    f(child);
                }
            }
            Expression::Function(e) => {
                for child in &e.children {
                    f(child);
                }
            }
            Expression::Operator(e) => {
                for child in &e.children {
                    f(child);
                }
            }
            Expression::Subquery(e) => {
                for child in &e.children {
                    f(child);
                }
            }
            Expression::Window(e) => {
                Self::enumerate_window_children(e, f);
            }
            Expression::Constant(_)
            | Expression::ColumnRef(_)
            | Expression::Parameter(_)
            | Expression::Reference(_) => {}
        }
    }

    pub fn enumerate_children_mut(expr: &mut Expression, mut f: impl FnMut(&mut Expression)) {
        match expr {
            Expression::Aggregate(e) => {
                for child in &mut e.children {
                    f(child);
                }
                if let Some(filter) = &mut e.filter {
                    f(filter);
                }
                for order in &mut e.order_bys {
                    f(&mut order.expression);
                }
            }
            Expression::Case(e) => {
                f(&mut e.check);
                f(&mut e.result_if_true);
                f(&mut e.result_if_false);
            }
            Expression::Cast(e) => {
                f(&mut e.child);
            }
            Expression::Comparison(e) => {
                f(&mut e.left);
                f(&mut e.right);
            }
            Expression::Conjunction(e) => {
                for child in &mut e.children {
                    f(child);
                }
            }
            Expression::Function(e) => {
                for child in &mut e.children {
                    f(child);
                }
            }
            Expression::Operator(e) => {
                for child in &mut e.children {
                    f(child);
                }
            }
            Expression::Subquery(e) => {
                for child in &mut e.children {
                    f(child);
                }
            }
            Expression::Window(e) => {
                Self::enumerate_window_children_mut(e, f);
            }
            Expression::Constant(_)
            | Expression::ColumnRef(_)
            | Expression::Parameter(_)
            | Expression::Reference(_) => {}
        }
    }

    pub fn enumerate_window_children<'a>(
        window: &'a WindowExpression,
        mut f: impl FnMut(&'a Expression),
    ) {
        match &window.invocation {
            WindowInvocation::Native { arguments, .. } => {
                for argument in arguments {
                    f(argument);
                }
            }
            WindowInvocation::Aggregate(aggregate) => {
                for child in &aggregate.children {
                    f(child);
                }
                if let Some(filter) = &aggregate.filter {
                    f(filter);
                }
                for order in &aggregate.order_bys {
                    f(&order.expression);
                }
            }
        }
        for partition in &window.partitions {
            f(partition);
        }
        for order in &window.orders {
            f(&order.expression);
        }
        if let WindowFrameBound::Offset(expr) = &window.frame.start_bound {
            f(expr);
        }
        if let WindowFrameBound::Offset(expr) = &window.frame.end_bound {
            f(expr);
        }
    }

    pub fn enumerate_window_children_mut(
        window: &mut WindowExpression,
        mut f: impl FnMut(&mut Expression),
    ) {
        match &mut window.invocation {
            WindowInvocation::Native { arguments, .. } => {
                for argument in arguments {
                    f(argument);
                }
            }
            WindowInvocation::Aggregate(aggregate) => {
                for child in &mut aggregate.children {
                    f(child);
                }
                if let Some(filter) = &mut aggregate.filter {
                    f(filter);
                }
                for order in &mut aggregate.order_bys {
                    f(&mut order.expression);
                }
            }
        }
        for partition in &mut window.partitions {
            f(partition);
        }
        for order in &mut window.orders {
            f(&mut order.expression);
        }
        if let WindowFrameBound::Offset(expr) = &mut window.frame.start_bound {
            f(expr);
        }
        if let WindowFrameBound::Offset(expr) = &mut window.frame.end_bound {
            f(expr);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ExpressionIterator;
    use crate::expression::{
        AggregateExpression, ColumnRefExpression, ComparisonExpression, ComparisonType,
        ConstantExpression, Expression, OrderByExpression, WindowExpression, WindowFrame,
        WindowFrameBound, WindowFrameType,
    };
    use crate::operator::ColumnBinding;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_function::aggregate::distributive::count::get_count_function;
    use paro_function::window::WindowFunction;

    fn int_column(idx: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(10, idx),
            LogicalType::Integer,
        ))
    }

    #[test]
    fn enumerate_children_visits_window_offsets_and_orders() {
        let expr = Expression::Window(WindowExpression::native(
            WindowFunction::first_value(LogicalType::Integer),
            vec![int_column(0)],
            vec![int_column(1)],
            vec![crate::expression::OrderByExpression {
                expression: int_column(2),
                ascending: true,
                nulls_first: false,
            }],
            WindowFrame {
                frame_type: WindowFrameType::Rows,
                start_bound: WindowFrameBound::Offset(Box::new(Expression::Constant(
                    ConstantExpression {
                        value: Value::Integer(1),
                        return_type: LogicalType::Integer,
                    },
                ))),
                start_is_preceding: true,
                end_bound: WindowFrameBound::Offset(Box::new(int_column(3))),
                end_is_preceding: false,
            },
            false,
        ));

        let mut count = 0;
        ExpressionIterator::enumerate_children(&expr, |_| {
            count += 1;
        });
        assert_eq!(count, 5);
    }

    #[test]
    fn enumerate_children_mut_allows_recursive_updates() {
        let mut expr = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            int_column(0),
            int_column(1),
        ));

        ExpressionIterator::enumerate_children_mut(&mut expr, |child| {
            if let Expression::ColumnRef(col_ref) = child {
                col_ref.binding.column_index += 10;
            }
        });

        match expr {
            Expression::Comparison(comp) => match (&*comp.left, &*comp.right) {
                (Expression::ColumnRef(left), Expression::ColumnRef(right)) => {
                    assert_eq!(left.binding.column_index, 10);
                    assert_eq!(right.binding.column_index, 11);
                }
                other => panic!("expected column refs, got {other:?}"),
            },
            other => panic!("expected comparison, got {other:?}"),
        }
    }

    #[test]
    fn enumerate_children_visits_aggregate_window_modifiers() {
        let (count, _) = get_count_function()
            .bind(&[LogicalType::Integer])
            .expect("bind count(integer)");
        let aggregate = AggregateExpression::new(count, vec![int_column(0)], LogicalType::BigInt)
            .with_filter(Some(Expression::Constant(ConstantExpression::new(
                Value::Boolean(true),
                LogicalType::Boolean,
            ))))
            .with_order_bys(vec![OrderByExpression {
                expression: int_column(1),
                ascending: true,
                nulls_first: false,
            }]);
        let expression = Expression::Window(WindowExpression::aggregate(
            aggregate,
            vec![int_column(2)],
            vec![OrderByExpression {
                expression: int_column(3),
                ascending: true,
                nulls_first: false,
            }],
            WindowFrame {
                frame_type: WindowFrameType::Rows,
                start_bound: WindowFrameBound::Offset(Box::new(int_column(4))),
                start_is_preceding: true,
                end_bound: WindowFrameBound::Offset(Box::new(int_column(5))),
                end_is_preceding: false,
            },
        ));

        let mut children = Vec::new();
        ExpressionIterator::enumerate_children(&expression, |child| {
            children.push(child.return_type());
        });
        assert_eq!(children.len(), 7);
        assert_eq!(children[1], LogicalType::Boolean);
    }
}
