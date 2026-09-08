// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::expression::{Expression, WindowExpression, WindowFrameBound, WindowInvocation};
use paro_common::error::Result;

pub struct ExpressionIterator;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpressionVisitDecision {
    Descend,
    SkipChildren,
}

impl ExpressionIterator {
    /// Fold an expression in post-order without using the native call stack.
    /// Expression children include aggregate/window modifiers and frame
    /// offsets, so callers get one complete traversal contract instead of
    /// reimplementing recursive walkers for each new expression variant.
    pub fn try_fold_post_order<State>(
        expr: &Expression,
        mut fold: impl FnMut(&Expression, &[State]) -> Result<State>,
    ) -> Result<State> {
        struct Frame<'a, State> {
            expr: &'a Expression,
            children: Vec<&'a Expression>,
            next_child: usize,
            completed: Vec<State>,
        }

        fn frame<'a, State>(expr: &'a Expression) -> Frame<'a, State> {
            let mut children = Vec::new();
            ExpressionIterator::enumerate_children(expr, |child| children.push(child));
            Frame {
                expr,
                next_child: 0,
                completed: Vec::with_capacity(children.len()),
                children,
            }
        }

        let mut stack = vec![frame(expr)];
        loop {
            let descend = {
                let current = stack
                    .last_mut()
                    .expect("expression post-order traversal retains its root frame");
                if let Some(child) = current.children.get(current.next_child).copied() {
                    current.next_child += 1;
                    Some(child)
                } else {
                    None
                }
            };
            if let Some(child) = descend {
                stack.push(frame(child));
                continue;
            }

            let completed = stack
                .pop()
                .expect("expression post-order traversal retains its completed frame");
            let state = fold(completed.expr, &completed.completed)?;
            let Some(parent) = stack.last_mut() else {
                return Ok(state);
            };
            parent.completed.push(state);
        }
    }

    /// Visit an expression tree in pre-order with explicit subtree pruning.
    /// All child enumeration remains centralized here, so adding an expression
    /// variant cannot silently omit it from downstream analyses.
    pub fn visit<'a>(
        expr: &'a Expression,
        visitor: &mut impl FnMut(&'a Expression) -> ExpressionVisitDecision,
    ) {
        // Expression trees can be generated from very long IN/OR lists. Keep
        // the public traversal contract stack-safe just like the logical-plan
        // walkers; recursive visitors make otherwise harmless diagnostics
        // depend on the native thread stack size.
        let mut pending = vec![expr];
        while let Some(current) = pending.pop() {
            if visitor(current) == ExpressionVisitDecision::SkipChildren {
                continue;
            }
            let mut children = Vec::new();
            Self::enumerate_children(current, |child| children.push(child));
            pending.extend(children.into_iter().rev());
        }
    }

    /// Visit an expression tree mutably without recursion.
    ///
    /// The expression itself is never moved while this function runs.  The
    /// raw pointers are therefore stable for the duration of the walk; the
    /// visitor receives one exclusive reference at a time and child pointers
    /// are collected before that borrow ends.  Keeping this primitive here
    /// gives all in-place rewrites the same stack-safety contract as the
    /// immutable walkers instead of each rewrite growing its own recursive
    /// helper.
    pub fn visit_mut(
        expr: &mut Expression,
        visitor: &mut impl FnMut(&mut Expression) -> ExpressionVisitDecision,
    ) {
        let mut pending = vec![expr as *mut Expression];
        while let Some(pointer) = pending.pop() {
            // SAFETY: `expr` owns the complete expression tree and is not
            // moved during the walk. We create at most one mutable reference
            // from a pointer at a time; child pointers are disjoint fields of
            // that reference and are consumed only after the borrow ends.
            let current = unsafe { &mut *pointer };
            if visitor(current) == ExpressionVisitDecision::SkipChildren {
                continue;
            }
            let mut children = Vec::new();
            Self::enumerate_children_mut(current, |child| {
                children.push(child as *mut Expression);
            });
            pending.extend(children.into_iter().rev());
        }
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
    use super::{ExpressionIterator, ExpressionVisitDecision};
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
        let expr = Expression::Window(Box::new(WindowExpression::native(
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
        )));

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
        let expression = Expression::Window(Box::new(WindowExpression::aggregate(
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
        )));

        let mut children = Vec::new();
        ExpressionIterator::enumerate_children(&expression, |child| {
            children.push(child.return_type());
        });
        assert_eq!(children.len(), 7);
        assert_eq!(children[1], LogicalType::Boolean);
    }

    #[test]
    fn post_order_fold_handles_deep_generated_conjunctions() {
        let mut expression = int_column(0);
        for _ in 0..10_000 {
            expression = Expression::Conjunction(crate::expression::ConjunctionExpression::new(
                crate::expression::ConjunctionType::And,
                vec![expression],
            ));
        }
        let nodes = ExpressionIterator::try_fold_post_order(&expression, |_, children| {
            Ok::<_, paro_common::error::ParoError>(
                1usize.saturating_add(children.iter().copied().sum::<usize>()),
            )
        })
        .unwrap();
        assert_eq!(nodes, 10_001);
        std::mem::forget(expression);
    }

    #[test]
    fn pre_order_visit_handles_deep_generated_conjunctions() {
        let mut expression = int_column(0);
        for _ in 0..10_000 {
            expression = Expression::Conjunction(crate::expression::ConjunctionExpression::new(
                crate::expression::ConjunctionType::And,
                vec![expression],
            ));
        }
        let mut visited = 0usize;
        ExpressionIterator::visit(&expression, &mut |_| {
            visited += 1;
            ExpressionVisitDecision::Descend
        });
        assert_eq!(visited, 10_001);
        std::mem::forget(expression);
    }
}
