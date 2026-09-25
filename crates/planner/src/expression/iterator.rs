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
    /// Rewrite immutable scalar nodes in post-order, preserving every unchanged
    /// allocation. The callback is a context-free local substitution, not an
    /// evaluation/occurrence visitor: a shared input node is rewritten once and
    /// its result is shared by all incoming edges. Runtime occurrence counts
    /// are not changed. Returning `None` leaves the node (with rewritten
    /// children) unchanged; a replacement is not recursively rewritten again.
    pub fn try_rewrite_dag(
        expr: &Expression,
        mut rewrite: impl FnMut(&Expression) -> Result<Option<Expression>>,
    ) -> Result<Expression> {
        use std::cell::RefCell;
        use std::collections::HashMap;

        // All input allocations remain borrowed until the fold finishes;
        // address identities cannot be recycled within this local memo.
        let rewritten = RefCell::new(HashMap::new());
        Self::try_fold_post_order_cached(
            expr,
            |node| rewritten.borrow().get(&node.allocation_identity()).cloned(),
            |node, children: &[Expression]| {
                let mut changed = false;
                let mut ordinal = 0;
                Self::enumerate_children(node, |child| {
                    changed |=
                        child.allocation_identity() != children[ordinal].allocation_identity();
                    ordinal += 1;
                });
                let mut result = node.clone();
                if changed {
                    let mut children = children.iter();
                    Self::enumerate_children_mut(&mut result, |child| {
                        *child = children
                            .next()
                            .expect("rewrite retained every edge")
                            .clone();
                    });
                    debug_assert!(children.next().is_none());
                }
                if let Some(replacement) = rewrite(&result)? {
                    result = replacement;
                }
                rewritten
                    .borrow_mut()
                    .insert(node.allocation_identity(), result.clone());
                Ok(result)
            },
        )
    }

    /// Fold an expression in post-order without using the native call stack.
    /// Expression children include aggregate/window modifiers and frame
    /// offsets, so callers get one complete traversal contract instead of
    /// reimplementing recursive walkers for each new expression variant.
    pub fn try_fold_post_order<State>(
        expr: &Expression,
        fold: impl FnMut(&Expression, &[State]) -> Result<State>,
    ) -> Result<State> {
        Self::try_fold_post_order_cached(expr, |_| None, fold)
    }

    /// A fact fold over immutable nodes. Lookup may prune an already-derived
    /// subtree; publishing the result in `fold` lets later DAG edges consume
    /// it without expanding the same node again. This is not an occurrence
    /// visitor: evaluation counts and effectful rewrites must use their own
    /// explicit occurrence contract instead.
    pub fn try_fold_post_order_cached<State>(
        expr: &Expression,
        mut lookup: impl FnMut(&Expression) -> Option<State>,
        mut fold: impl FnMut(&Expression, &[State]) -> Result<State>,
    ) -> Result<State> {
        enum Task<'a> {
            Enter(&'a Expression),
            Finish(&'a Expression, usize),
        }
        // Two reusable buffers for the whole traversal. Per-node child and
        // completed-state vectors turn a stack-safety primitive into O(n)
        // allocator calls even when the fold itself allocates nothing.
        let mut pending = vec![Task::Enter(expr)];
        let mut completed = Vec::new();
        while let Some(task) = pending.pop() {
            match task {
                Task::Enter(expression) => {
                    if let Some(state) = lookup(expression) {
                        completed.push(state);
                        continue;
                    }
                    let start = pending.len();
                    Self::enumerate_children(expression, |child| pending.push(Task::Enter(child)));
                    let count = pending.len() - start;
                    if count == 0 {
                        completed.push(fold(expression, &[])?);
                    } else {
                        pending.push(Task::Finish(expression, count));
                        pending[start..].reverse();
                    }
                }
                Task::Finish(expression, count) => {
                    let start = completed
                        .len()
                        .checked_sub(count)
                        .expect("expression fold retained every child state");
                    let state = fold(expression, &completed[start..])?;
                    completed.truncate(start);
                    completed.push(state);
                }
            }
        }
        debug_assert_eq!(completed.len(), 1);
        Ok(completed.pop().expect("expression fold retained its root"))
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
            let start = pending.len();
            Self::enumerate_children(current, |child| pending.push(child));
            pending[start..].reverse();
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
            // SAFETY: the root slot is not moved during the walk. Mutable
            // child enumeration detaches a shared payload before collecting
            // pointers, so all pending pointers address disjoint owned slots
            // even if their scalar payloads still share descendants. Neither
            // visiting nor detaching a descendant can move a sibling slot.
            // Only one pointer is exposed as a mutable reference at a time.
            let current = unsafe { &mut *pointer };
            if visitor(current) == ExpressionVisitDecision::SkipChildren {
                continue;
            }
            let start = pending.len();
            Self::enumerate_children_mut(current, |child| {
                pending.push(child as *mut Expression);
            });
            pending[start..].reverse();
        }
    }

    pub fn enumerate_children<'a>(expr: &'a Expression, mut f: impl FnMut(&'a Expression)) {
        let result: std::result::Result<(), std::convert::Infallible> =
            Self::try_enumerate_children(expr, |child| {
                f(child);
                Ok(())
            });
        match result {
            Ok(()) => {}
            Err(never) => match never {},
        }
    }

    /// The same child-field contract, with immediate short-circuiting. Budget
    /// admission and cancellation must be able to stop a wide node before
    /// retaining all its children, not only before visiting the next parent.
    pub fn try_enumerate_children<'a, E>(
        expr: &'a Expression,
        mut f: impl FnMut(&'a Expression) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        match expr {
            Expression::Aggregate(e) => {
                for child in &e.children {
                    f(child)?;
                }
                if let Some(filter) = &e.filter {
                    f(filter)?;
                }
                for order in &e.order_bys {
                    f(&order.expression)?;
                }
            }
            Expression::Case(e) => {
                f(&e.check)?;
                f(&e.result_if_true)?;
                f(&e.result_if_false)?;
            }
            Expression::Cast(e) => {
                f(&e.child)?;
            }
            Expression::Comparison(e) => {
                f(&e.left)?;
                f(&e.right)?;
            }
            Expression::Conjunction(e) => {
                for child in &e.children {
                    f(child)?;
                }
            }
            Expression::Function(e) => {
                for child in &e.children {
                    f(child)?;
                }
            }
            Expression::Operator(e) => {
                for child in &e.children {
                    f(child)?;
                }
            }
            Expression::Subquery(e) => {
                for child in &e.children {
                    f(child)?;
                }
            }
            Expression::Window(e) => {
                Self::try_enumerate_window_children(e, f)?;
            }
            Expression::Constant(_)
            | Expression::ColumnRef(_)
            | Expression::Parameter(_)
            | Expression::Reference(_) => {}
        }
        Ok(())
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
        let result: std::result::Result<(), std::convert::Infallible> =
            Self::try_enumerate_window_children(window, |child| {
                f(child);
                Ok(())
            });
        match result {
            Ok(()) => {}
            Err(never) => match never {},
        }
    }

    pub fn try_enumerate_window_children<'a, E>(
        window: &'a WindowExpression,
        mut f: impl FnMut(&'a Expression) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        match &window.invocation {
            WindowInvocation::Native { arguments, .. } => {
                for argument in arguments {
                    f(argument)?;
                }
            }
            WindowInvocation::Aggregate(aggregate) => {
                for child in &aggregate.children {
                    f(child)?;
                }
                if let Some(filter) = &aggregate.filter {
                    f(filter)?;
                }
                for order in &aggregate.order_bys {
                    f(&order.expression)?;
                }
            }
        }
        for partition in &window.partitions {
            f(partition)?;
        }
        for order in &window.orders {
            f(&order.expression)?;
        }
        if let WindowFrameBound::Offset(expr) = &window.frame.start_bound {
            f(expr)?;
        }
        if let WindowFrameBound::Offset(expr) = &window.frame.end_bound {
            f(expr)?;
        }
        Ok(())
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
    use crate::logical::operator::ColumnBinding;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_function::aggregate::distributive::count::get_count_function;
    use paro_function::window::WindowFunction;

    fn int_column(idx: usize) -> Expression {
        Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(10, idx), LogicalType::Integer).into(),
        )
    }

    #[test]
    fn persistent_dag_rewrite_preserves_no_ops_and_rewrites_shared_nodes_once() {
        use crate::expression::{ConjunctionExpression, ConjunctionType};
        let leaf = int_column(0);
        let untouched = int_column(1);
        let mut root = Expression::Conjunction(
            ConjunctionExpression::new(ConjunctionType::And, vec![leaf, untouched.clone()]).into(),
        );
        for _ in 0..50 {
            root = Expression::Conjunction(
                ConjunctionExpression::new(ConjunctionType::Or, vec![root.clone(), root]).into(),
            );
        }
        let mut visited = 0;
        let noop = ExpressionIterator::try_rewrite_dag(&root, |_| {
            visited += 1;
            assert!(visited <= 53, "a shared scalar was expanded more than once");
            Ok(None)
        })
        .unwrap();
        assert_eq!(visited, 53);
        assert_eq!(noop.allocation_identity(), root.allocation_identity());
        visited = 0;
        let changed = ExpressionIterator::try_rewrite_dag(&root, |node| {
            visited += 1;
            assert!(visited <= 53);
            Ok(
                matches!(node, Expression::ColumnRef(c) if c.binding.column_index == 0)
                    .then(|| int_column(2)),
            )
        })
        .unwrap();
        assert_eq!(visited, 53);
        assert_ne!(changed.allocation_identity(), root.allocation_identity());
        let mut cursor = &changed;
        for _ in 0..50 {
            let Expression::Conjunction(node) = cursor else {
                panic!()
            };
            assert_eq!(
                node.children[0].allocation_identity(),
                node.children[1].allocation_identity()
            );
            cursor = &node.children[0];
        }
        let Expression::Conjunction(node) = cursor else {
            panic!()
        };
        assert!(node.children[0].equals(&int_column(2)));
        assert_eq!(
            node.children[1].allocation_identity(),
            untouched.allocation_identity()
        );
    }

    #[test]
    fn persistent_rewrite_is_stack_safe_and_short_circuits_errors() {
        use crate::expression::{ConjunctionExpression, ConjunctionType};
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(|| {
                let mut root = int_column(0);
                for _ in 0..10_000 {
                    root = Expression::Conjunction(
                        ConjunctionExpression::new(ConjunctionType::And, vec![root]).into(),
                    );
                }
                let rewritten = ExpressionIterator::try_rewrite_dag(&root, |node| {
                    Ok(matches!(node, Expression::ColumnRef(_)).then(|| int_column(1)))
                })
                .unwrap();
                assert!(!rewritten.equals(&root));
                let mut reads = 0;
                let failed = ExpressionIterator::try_rewrite_dag(&root, |_| {
                    reads += 1;
                    Err(paro_common::error::internal(
                        "intentional local rewrite failure",
                    ))
                });
                assert!(failed.is_err());
                assert_eq!(reads, 1);
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn flat_post_order_preserves_sibling_states_and_stops_at_first_error() {
        use crate::expression::{ConjunctionExpression, ConjunctionType};
        let expression = Expression::Conjunction(
            ConjunctionExpression::new(
                ConjunctionType::Or,
                vec![
                    Expression::Conjunction(
                        ConjunctionExpression::new(
                            ConjunctionType::And,
                            vec![int_column(0), int_column(1)],
                        )
                        .into(),
                    ),
                    int_column(2),
                ],
            )
            .into(),
        );
        let mut groups = Vec::new();
        let value = ExpressionIterator::try_fold_post_order(&expression, |expression, children| {
            if let Expression::ColumnRef(column) = expression {
                return Ok(column.binding.column_index);
            }
            groups.push(children.to_vec());
            Ok(children.iter().fold(9, |prefix, child| prefix * 10 + child))
        })
        .unwrap();
        assert_eq!(groups, vec![vec![0, 1], vec![901, 2]]);
        assert_eq!(value, 9912);

        let mut visited = Vec::new();
        let result =
            ExpressionIterator::try_fold_post_order(&expression, |expression, _: &[()]| {
                let Expression::ColumnRef(column) = expression else {
                    panic!("parent ran after a failed child")
                };
                visited.push(column.binding.column_index);
                if column.binding.column_index == 1 {
                    return Err(paro_common::error::internal("expected traversal stop"));
                }
                Ok(())
            });
        assert!(result.is_err());
        assert_eq!(visited, vec![0, 1]);
    }

    #[test]
    fn cached_fold_visits_shared_nodes_once_without_losing_edge_multiplicity() {
        use crate::expression::{ConjunctionExpression, ConjunctionType};
        let mut expression = int_column(0);
        for _ in 0..50 {
            expression = Expression::Conjunction(
                ConjunctionExpression::new(
                    ConjunctionType::And,
                    vec![expression.clone(), expression],
                )
                .into(),
            );
        }
        let facts = std::cell::RefCell::new(std::collections::HashMap::new());
        let mut derived = 0;
        let leaves = ExpressionIterator::try_fold_post_order_cached(
            &expression,
            |node| facts.borrow().get(&node.allocation_identity()).copied(),
            |node, children: &[u64]| {
                derived += 1;
                // Fail quickly if pruning regresses instead of expanding
                // 2^50 paths and hanging the test process.
                if derived > 51 {
                    return Err(paro_common::error::internal("shared fact expanded again"));
                }
                let value = if children.is_empty() {
                    1
                } else {
                    children.iter().sum()
                };
                facts.borrow_mut().insert(node.allocation_identity(), value);
                Ok(value)
            },
        )
        .unwrap();
        assert_eq!(derived, 51);
        assert_eq!(leaves, 1u64 << 50);
    }

    #[test]
    fn enumerate_children_visits_window_offsets_and_orders() {
        let expr = Expression::Window(
            WindowExpression::native(
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
                        }
                        .into(),
                    ))),
                    start_is_preceding: true,
                    end_bound: WindowFrameBound::Offset(Box::new(int_column(3))),
                    end_is_preceding: false,
                },
                false,
            )
            .into(),
        );

        let mut count = 0;
        ExpressionIterator::enumerate_children(&expr, |_| {
            count += 1;
        });
        assert_eq!(count, 5);
    }

    #[test]
    fn enumerate_children_mut_allows_recursive_updates() {
        let mut expr = Expression::Comparison(
            ComparisonExpression::new(ComparisonType::Equal, int_column(0), int_column(1)).into(),
        );

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
            .with_filter(Some(Expression::Constant(
                ConstantExpression::new(Value::Boolean(true), LogicalType::Boolean).into(),
            )))
            .with_order_bys(vec![OrderByExpression {
                expression: int_column(1),
                ascending: true,
                nulls_first: false,
            }]);
        let expression = Expression::Window(
            WindowExpression::aggregate(
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
            )
            .into(),
        );

        let mut children = Vec::new();
        ExpressionIterator::enumerate_children(&expression, |child| {
            children.push(child.return_type());
        });
        assert_eq!(children.len(), 7);
        assert_eq!(children[1], LogicalType::Boolean);
        for stop in 0..children.len() {
            let mut visits = 0;
            let result = ExpressionIterator::try_enumerate_children(&expression, |_| {
                let current = visits;
                visits += 1;
                if current == stop {
                    Err(stop)
                } else {
                    Ok(())
                }
            });
            assert_eq!(result, Err(stop));
            assert_eq!(visits, stop + 1);
        }
    }

    #[test]
    fn post_order_fold_handles_deep_generated_conjunctions() {
        let mut expression = int_column(0);
        for _ in 0..10_000 {
            expression = Expression::Conjunction(
                crate::expression::ConjunctionExpression::new(
                    crate::expression::ConjunctionType::And,
                    vec![expression],
                )
                .into(),
            );
        }
        let nodes = ExpressionIterator::try_fold_post_order(&expression, |_, children| {
            Ok::<_, paro_common::error::ParoError>(
                1usize.saturating_add(children.iter().copied().sum::<usize>()),
            )
        })
        .unwrap();
        assert_eq!(nodes, 10_001);
        drop(expression);
    }

    #[test]
    fn pre_order_visit_handles_deep_generated_conjunctions() {
        let mut expression = int_column(0);
        for _ in 0..10_000 {
            expression = Expression::Conjunction(
                crate::expression::ConjunctionExpression::new(
                    crate::expression::ConjunctionType::And,
                    vec![expression],
                )
                .into(),
            );
        }
        let mut visited = 0usize;
        ExpressionIterator::visit(&expression, &mut |_| {
            visited += 1;
            ExpressionVisitDecision::Descend
        });
        assert_eq!(visited, 10_001);
        drop(expression);
    }
}
