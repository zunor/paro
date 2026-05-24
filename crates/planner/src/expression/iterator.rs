// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::expression::{Expression, WindowFrameBound};

pub struct ExpressionIterator;

impl ExpressionIterator {
    pub fn enumerate_children(expr: &Expression, mut f: impl FnMut(&Expression)) {
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
                for child in &e.children {
                    f(child);
                }
                for partition in &e.partitions {
                    f(partition);
                }
                for order in &e.orders {
                    f(&order.expression);
                }
                if let WindowFrameBound::Offset(expr) = &e.frame.start_bound {
                    f(expr);
                }
                if let WindowFrameBound::Offset(expr) = &e.frame.end_bound {
                    f(expr);
                }
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
                for child in &mut e.children {
                    f(child);
                }
                for partition in &mut e.partitions {
                    f(partition);
                }
                for order in &mut e.orders {
                    f(&mut order.expression);
                }
                if let WindowFrameBound::Offset(expr) = &mut e.frame.start_bound {
                    f(expr);
                }
                if let WindowFrameBound::Offset(expr) = &mut e.frame.end_bound {
                    f(expr);
                }
            }
            Expression::Constant(_)
            | Expression::ColumnRef(_)
            | Expression::Parameter(_)
            | Expression::Reference(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ExpressionIterator;
    use crate::expression::{
        ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression, Expression,
        WindowExpression, WindowFrame, WindowFrameBound, WindowFrameType,
    };
    use crate::operator::ColumnBinding;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_function::window::WindowFunction;

    fn int_column(idx: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(10, idx),
            LogicalType::Integer,
        ))
    }

    #[test]
    fn enumerate_children_visits_window_offsets_and_orders() {
        let expr = Expression::Window(WindowExpression {
            function: WindowFunction::row_number(),
            children: vec![int_column(0)],
            partitions: vec![int_column(1)],
            orders: vec![crate::expression::OrderByExpression {
                expression: int_column(2),
                ascending: true,
                nulls_first: false,
            }],
            frame: WindowFrame {
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
            ignore_nulls: false,
            return_type: LogicalType::BigInt,
        });

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
}
