// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Copy-on-write bound scalar payloads. Cloning an expression shares its
//! immutable node; mutation detaches only that node, never its descendant DAG.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use super::*;

/// Identity of one currently live immutable allocation, not scalar semantics.
/// Equality is useful while both expressions are borrowed. A retained cache
/// must also hold a liveness witness; an address alone may be reused after drop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExpressionIdentity(usize);

/// The complete owned-child contract of a scalar payload. Destruction consumes
/// these edges iteratively, including aggregate modifiers and window frames.
pub trait ExpressionPayload: Clone + std::fmt::Debug {
    fn into_expression_children(self, pending: &mut Vec<Expression>);
}

pub struct SharedExpressionPayload<T: ExpressionPayload> {
    inner: Option<Arc<T>>,
}

impl<T: ExpressionPayload> std::fmt::Debug for SharedExpressionPayload<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Ownership is not a scalar semantic property or diagnostic payload.
        self.deref().fmt(formatter)
    }
}

impl<T: ExpressionPayload> SharedExpressionPayload<T> {
    pub(crate) fn allocation_identity(&self) -> ExpressionIdentity {
        ExpressionIdentity(Arc::as_ptr(self.inner.as_ref().expect("live scalar payload")) as usize)
    }

    pub fn new(payload: T) -> Self {
        Self {
            inner: Some(Arc::new(payload)),
        }
    }

    pub fn into_inner(mut self) -> T {
        let shared = self.inner.as_ref().expect("live scalar payload");
        if Arc::strong_count(shared) == 1 {
            return Arc::into_inner(self.inner.take().expect("live scalar payload"))
                .expect("unique scalar payload");
        }
        // Dropping `self` uses the same iterative release path, even if the
        // last peer disappears concurrently while this local copy is made.
        shared.as_ref().clone()
    }

    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(
            self.inner.as_ref().expect("live scalar payload"),
            other.inner.as_ref().expect("live scalar payload"),
        )
    }

    pub(crate) fn release_into(&mut self, pending: &mut Vec<Expression>) {
        if let Some(payload) = self.inner.take().and_then(Arc::into_inner) {
            payload.into_expression_children(pending);
        }
    }
}

impl<T: ExpressionPayload> Clone for SharedExpressionPayload<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: ExpressionPayload> Deref for SharedExpressionPayload<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.inner.as_deref().expect("live scalar payload")
    }
}

impl<T: ExpressionPayload> DerefMut for SharedExpressionPayload<T> {
    fn deref_mut(&mut self) -> &mut T {
        Arc::make_mut(self.inner.as_mut().expect("live scalar payload"))
    }
}

impl<T: ExpressionPayload> AsRef<T> for SharedExpressionPayload<T> {
    fn as_ref(&self) -> &T {
        self
    }
}

impl<T: ExpressionPayload> AsMut<T> for SharedExpressionPayload<T> {
    fn as_mut(&mut self) -> &mut T {
        self
    }
}

impl<T: ExpressionPayload> From<T> for SharedExpressionPayload<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: ExpressionPayload> Drop for SharedExpressionPayload<T> {
    fn drop(&mut self) {
        let mut pending = Vec::new();
        self.release_into(&mut pending);
        while let Some(mut expression) = pending.pop() {
            expression.release_children_into(&mut pending);
        }
    }
}

macro_rules! payload {
    ($ty:ty, $self:ident, $pending:ident, $body:block) => {
        impl ExpressionPayload for $ty {
            fn into_expression_children($self, $pending: &mut Vec<Expression>) $body
        }
    };
}

payload!(ConstantExpression, self, _pending, {
    let Self {
        value: _,
        return_type: _,
    } = self;
});
payload!(ColumnRefExpression, self, _pending, {
    let Self {
        binding: _,
        return_type: _,
        depth: _,
    } = self;
});
payload!(ParameterExpression, self, _pending, {
    let Self { slot: _ } = self;
});
payload!(ReferenceExpression, self, _pending, {
    let Self {
        index: _,
        return_type: _,
    } = self;
});
payload!(FunctionExpression, self, pending, {
    let Self {
        function: _,
        children,
        return_type: _,
        routine_meta: _,
    } = self;
    pending.extend(children);
});
payload!(OperatorExpression, self, pending, {
    let Self {
        operator_type: _,
        children,
        return_type: _,
    } = self;
    pending.extend(children);
});
payload!(ConjunctionExpression, self, pending, {
    let Self {
        conjunction_type: _,
        children,
    } = self;
    pending.extend(children);
});
payload!(CaseExpression, self, pending, {
    let Self {
        check,
        result_if_true,
        result_if_false,
        return_type: _,
    } = self;
    pending.extend([*check, *result_if_true, *result_if_false]);
});
payload!(CastExpression, self, pending, {
    let Self {
        child,
        target_type: _,
        try_cast: _,
        cast_info: _,
    } = self;
    pending.push(*child);
});
payload!(ComparisonExpression, self, pending, {
    let Self {
        left,
        right,
        comparison_type: _,
    } = self;
    pending.extend([*left, *right]);
});
payload!(SubqueryExpression, self, pending, {
    let Self {
        subquery_type: _,
        subquery: _,
        children,
        child_types: _,
        child_targets: _,
        comparison_type: _,
        return_type: _,
        correlated_columns: _,
        bind_snapshot: _,
        planning_state: _,
    } = self;
    pending.extend(children);
});
payload!(AggregateExpression, self, pending, {
    let Self {
        function: _,
        children,
        return_type: _,
        aggr_type: _,
        filter,
        order_bys,
        bind_info: _,
    } = self;
    pending.extend(children);
    pending.extend(filter.map(|filter| *filter));
    pending.extend(order_bys.into_iter().map(|order| order.expression));
});
payload!(WindowExpression, self, pending, {
    let Self {
        invocation,
        partitions,
        orders,
        frame,
        ignore_nulls: _,
    } = self;
    match invocation {
        WindowInvocation::Native {
            function: _,
            arguments,
        } => pending.extend(arguments),
        WindowInvocation::Aggregate(aggregate) => aggregate.into_expression_children(pending),
    }
    pending.extend(partitions);
    pending.extend(orders.into_iter().map(|order| order.expression));
    let WindowFrame {
        frame_type: _,
        start_bound,
        start_is_preceding: _,
        end_bound,
        end_is_preceding: _,
    } = frame;
    for bound in [start_bound, end_bound] {
        if let WindowFrameBound::Offset(offset) = bound {
            pending.push(*offset);
        }
    }
});

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;

    fn leaf() -> Expression {
        Expression::Constant(
            ConstantExpression::new(Value::Boolean(true), LogicalType::Boolean).into(),
        )
    }

    fn leaf_weak(expression: &Expression) -> std::sync::Weak<ConstantExpression> {
        let Expression::Constant(payload) = expression else {
            panic!("expected constant")
        };
        Arc::downgrade(payload.inner.as_ref().unwrap())
    }

    #[test]
    fn clone_mutation_and_last_owner_drop_are_stack_safe() {
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(|| {
                let mut expression = leaf();
                let observed_leaf = leaf_weak(&expression);
                for _ in 0..10_000 {
                    expression = Expression::Conjunction(
                        ConjunctionExpression::new(ConjunctionType::And, vec![expression]).into(),
                    );
                }
                let mut edited = expression.clone();
                let (Expression::Conjunction(left), Expression::Conjunction(right)) =
                    (&expression, &edited)
                else {
                    unreachable!()
                };
                assert!(left.ptr_eq(right));

                let mut visited = 0;
                ExpressionIterator::visit_mut(&mut edited, &mut |node| {
                    visited += 1;
                    if let Expression::Constant(constant) = node {
                        constant.value = Value::Boolean(false);
                    }
                    ExpressionVisitDecision::Descend
                });
                assert_eq!(visited, 10_001);
                assert!(!expression.equals(&edited));
                assert_eq!(observed_leaf.upgrade().unwrap().value, Value::Boolean(true));
                drop(expression);
                assert!(observed_leaf.upgrade().is_none());
                drop(edited);
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn shared_dag_release_is_linear_in_owned_nodes_not_expanded_paths() {
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(|| {
                let mut expression = leaf();
                let observed_leaf = leaf_weak(&expression);
                for _ in 0..10_000 {
                    expression = Expression::Conjunction(
                        ConjunctionExpression::new(
                            ConjunctionType::Or,
                            vec![expression.clone(), expression],
                        )
                        .into(),
                    );
                }
                let peer = expression.clone();
                drop(expression);
                assert!(observed_leaf.upgrade().is_some());
                drop(peer);
                assert!(observed_leaf.upgrade().is_none());
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn concurrent_last_owner_release_keeps_iterative_drop_contract() {
        let mut expression = leaf();
        let observed_leaf = leaf_weak(&expression);
        for _ in 0..10_000 {
            expression = Expression::Conjunction(
                ConjunctionExpression::new(ConjunctionType::And, vec![expression]).into(),
            );
        }
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let mut threads = Vec::new();
        for expression in [expression.clone(), expression] {
            let barrier = barrier.clone();
            threads.push(
                std::thread::Builder::new()
                    .stack_size(128 * 1024)
                    .spawn(move || {
                        barrier.wait();
                        drop(expression);
                    })
                    .unwrap(),
            );
        }
        for thread in threads {
            thread.join().unwrap();
        }
        assert!(observed_leaf.upgrade().is_none());
    }

    #[test]
    fn owned_release_and_borrowed_children_agree_for_window_modifiers() {
        use paro_function::aggregate::distributive::count::get_count_function;
        let integer = || {
            Expression::Constant(
                ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into(),
            )
        };
        let (function, _) = get_count_function().bind(&[LogicalType::Integer]).unwrap();
        let aggregate = AggregateExpression::new(function, vec![integer()], LogicalType::BigInt)
            .with_filter(Some(leaf()))
            .with_order_bys(vec![OrderByExpression {
                expression: integer(),
                ascending: true,
                nulls_first: false,
            }]);
        let window = WindowExpression::aggregate(
            aggregate,
            vec![integer()],
            vec![OrderByExpression {
                expression: integer(),
                ascending: false,
                nulls_first: true,
            }],
            WindowFrame {
                start_bound: WindowFrameBound::Offset(Box::new(integer())),
                end_bound: WindowFrameBound::Offset(Box::new(integer())),
                ..WindowFrame::default()
            },
        );
        let mut borrowed = Vec::new();
        ExpressionIterator::enumerate_window_children(&window, |child| {
            borrowed.push(child.clone())
        });
        let mut owned = Vec::new();
        window.into_expression_children(&mut owned);
        assert_eq!(owned.len(), 7);
        assert_eq!(owned.len(), borrowed.len());
        for (left, right) in owned.iter().zip(&borrowed) {
            let (Expression::Constant(left), Expression::Constant(right)) = (left, right) else {
                unreachable!()
            };
            assert!(left.ptr_eq(right));
        }
    }
}
