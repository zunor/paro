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
    WindowExpression, WindowFrameBound, WindowInvocation,
};
use crate::operator::ColumnBinding;

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
        let mut expression = self;
        expression.extract_aggregates_in_place(aggregates, offset);
        expression
    }

    /// Extract aggregates through every scalar-expression child, including window clauses.
    ///
    /// A subquery is a query-level boundary: aggregates owned by its plan must not be hoisted into
    /// the surrounding SELECT.
    pub fn extract_aggregates_in_place(&mut self, aggregates: &mut Vec<Expression>, offset: usize) {
        if let Expression::Aggregate(aggregate) = self {
            let index = offset + aggregates.len();
            let return_type = aggregate.return_type.clone();
            let replacement = Expression::Reference(ReferenceExpression::new(index, return_type));
            aggregates.push(std::mem::replace(self, replacement));
            return;
        }
        if matches!(self, Expression::Subquery(_)) {
            return;
        }

        ExpressionIterator::enumerate_children_mut(self, |child| {
            child.extract_aggregates_in_place(aggregates, offset);
        });
    }

    /// Hoist window expressions into a window operator and replace uses with its output binding.
    ///
    /// Window bindings use a producer-local column index. The physical position is resolved after
    /// the child plan has been finalized, so subquery planning cannot invalidate the binding.
    pub fn extract_windows_in_place(&mut self, windows: &mut Vec<Expression>, window_index: usize) {
        if matches!(self, Expression::Window(_)) {
            let return_type = self.return_type();
            let existing = self
                .evaluation_properties()
                .can_share_evaluation()
                .then(|| windows.iter().position(|window| window.equals(self)))
                .flatten();
            let output_index = existing.unwrap_or(windows.len());
            let replacement = Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(window_index, output_index),
                return_type,
            ));

            if existing.is_some() {
                *self = replacement;
            } else {
                windows.push(std::mem::replace(self, replacement));
            }
            return;
        }
        if matches!(self, Expression::Subquery(_)) {
            return;
        }

        ExpressionIterator::enumerate_children_mut(self, |child| {
            child.extract_windows_in_place(windows, window_index);
        });
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
                aggregate_expressions_equal(a, b)
            }
            (Expression::Window(a), Expression::Window(b)) => {
                window_invocations_equal(&a.invocation, &b.invocation)
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

fn aggregate_expressions_equal(left: &AggregateExpression, right: &AggregateExpression) -> bool {
    left.function.execution_semantics_equal(&right.function)
        && left.return_type == right.return_type
        && left.children.len() == right.children.len()
        && left
            .children
            .iter()
            .zip(&right.children)
            .all(|(left, right)| left.equals(right))
        && left.aggr_type == right.aggr_type
        && match (&left.filter, &right.filter) {
            (Some(left), Some(right)) => left.equals(right),
            (None, None) => true,
            _ => false,
        }
        && left.order_bys.len() == right.order_bys.len()
        && left
            .order_bys
            .iter()
            .zip(&right.order_bys)
            .all(|(left, right)| {
                left.ascending == right.ascending
                    && left.nulls_first == right.nulls_first
                    && left.expression.equals(&right.expression)
            })
        && match (&left.bind_info, &right.bind_info) {
            (Some(left), Some(right)) => left.equals(&**right),
            (None, None) => true,
            _ => false,
        }
}

fn window_invocations_equal(left: &WindowInvocation, right: &WindowInvocation) -> bool {
    match (left, right) {
        (
            WindowInvocation::Native {
                function: left_function,
                arguments: left_arguments,
            },
            WindowInvocation::Native {
                function: right_function,
                arguments: right_arguments,
            },
        ) => {
            left_function.name == right_function.name
                && left_function.function_type == right_function.function_type
                && left_function.arguments == right_function.arguments
                && left_function.return_type == right_function.return_type
                && left_arguments.len() == right_arguments.len()
                && left_arguments
                    .iter()
                    .zip(right_arguments)
                    .all(|(left, right)| left.equals(right))
        }
        (WindowInvocation::Aggregate(left), WindowInvocation::Aggregate(right)) => {
            aggregate_expressions_equal(left, right)
        }
        _ => false,
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
        AggregateExpression, ColumnRefExpression, ConstantExpression, FunctionExpression,
        OrderByExpression, ReferenceExpression, WindowExpression, WindowFrame, WindowFrameBound,
        WindowFrameType,
    };
    use crate::operator::ColumnBinding;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_function::aggregate::distributive::count::get_count_star_function;
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

    fn random_call() -> Expression {
        let function = paro_function::scalar::math::get_random_function()
            .functions
            .into_iter()
            .next()
            .expect("random overload");
        Expression::Function(FunctionExpression::new(
            function,
            vec![],
            LogicalType::Double,
        ))
    }

    fn window_expression(start_bound: WindowFrameBound) -> Expression {
        Expression::Window(WindowExpression::native(
            WindowFunction::first_value(LogicalType::Integer),
            vec![int_column(0)],
            vec![int_column(1)],
            vec![OrderByExpression {
                expression: int_column(2),
                ascending: true,
                nulls_first: false,
            }],
            WindowFrame {
                frame_type: WindowFrameType::Rows,
                start_bound,
                start_is_preceding: true,
                end_bound: WindowFrameBound::CurrentRow,
                end_is_preceding: false,
            },
            false,
        ))
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
    fn extract_aggregates_visits_window_clauses() {
        let aggregate = Expression::Aggregate(AggregateExpression::new(
            get_count_star_function(),
            vec![],
            LogicalType::BigInt,
        ));
        let mut expression = window_expression(WindowFrameBound::CurrentRow);
        let Expression::Window(window) = &mut expression else {
            unreachable!();
        };
        window.orders[0].expression = aggregate;

        let mut aggregates = Vec::new();
        expression.extract_aggregates_in_place(&mut aggregates, 3);

        assert_eq!(aggregates.len(), 1);
        assert!(matches!(aggregates[0], Expression::Aggregate(_)));
        let Expression::Window(window) = expression else {
            panic!("expected window expression");
        };
        assert!(matches!(
            window.orders[0].expression,
            Expression::Reference(ReferenceExpression { index: 3, .. })
        ));
    }

    #[test]
    fn extract_aggregates_preserves_window_owned_aggregate_kernel() {
        let aggregate =
            AggregateExpression::new(get_count_star_function(), vec![], LogicalType::BigInt);
        let mut expression = Expression::Window(WindowExpression::aggregate(
            aggregate,
            vec![int_column(0)],
            vec![],
            WindowFrame::default(),
        ));
        let mut aggregates = Vec::new();

        expression.extract_aggregates_in_place(&mut aggregates, 0);

        assert!(aggregates.is_empty());
        let Expression::Window(window) = expression else {
            panic!("expected aggregate window");
        };
        assert!(window.aggregate_invocation().is_some());
    }

    #[test]
    fn extract_windows_reuses_semantically_equal_outputs() {
        let mut first = window_expression(WindowFrameBound::CurrentRow);
        let mut second = first.clone();
        let mut windows = Vec::new();

        first.extract_windows_in_place(&mut windows, 42);
        second.extract_windows_in_place(&mut windows, 42);

        assert_eq!(windows.len(), 1);
        for expression in [first, second] {
            let Expression::ColumnRef(column) = expression else {
                panic!("expected window output reference");
            };
            assert_eq!(column.binding, ColumnBinding::new(42, 0));
        }
    }

    #[test]
    fn extract_windows_preserves_volatile_evaluations() {
        let mut first = window_expression(WindowFrameBound::CurrentRow);
        let Expression::Window(window) = &mut first else {
            unreachable!();
        };
        *window.arguments_mut() = vec![random_call()];
        let mut second = first.clone();
        let mut windows = Vec::new();

        first.extract_windows_in_place(&mut windows, 42);
        second.extract_windows_in_place(&mut windows, 42);

        assert_eq!(windows.len(), 2);
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
