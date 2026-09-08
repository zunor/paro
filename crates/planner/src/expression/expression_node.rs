// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bound Expression
//!
//!

use paro_common::types::LogicalType;
use paro_external::routine::identity::RoutineCallIdentity;

use super::{
    AggregateExpression, CaseExpression, CastExpression, ColumnRefExpression, ComparisonExpression,
    ConjunctionExpression, ConstantExpression, ExpressionIterator, ExpressionVisitDecision,
    FunctionExpression, OperatorExpression, ParameterExpression, ReferenceExpression,
    SharedExpressionPayload, SubqueryExpression, WindowExpression, WindowFrameBound,
    WindowInvocation,
};
use crate::operator::ColumnBinding;

/// Expression represents a semantic-aware version of a SQL expression.
#[derive(Debug, Clone)]
pub enum Expression {
    Constant(SharedExpressionPayload<ConstantExpression>),
    ColumnRef(SharedExpressionPayload<ColumnRefExpression>),
    Function(SharedExpressionPayload<FunctionExpression>),
    Cast(SharedExpressionPayload<CastExpression>),
    Conjunction(SharedExpressionPayload<ConjunctionExpression>),
    Case(SharedExpressionPayload<CaseExpression>),
    Comparison(SharedExpressionPayload<ComparisonExpression>),
    Operator(SharedExpressionPayload<OperatorExpression>),
    Parameter(SharedExpressionPayload<ParameterExpression>),
    Reference(SharedExpressionPayload<ReferenceExpression>),
    Aggregate(SharedExpressionPayload<AggregateExpression>),
    Subquery(SharedExpressionPayload<SubqueryExpression>),
    Window(SharedExpressionPayload<WindowExpression>),
}

// One tag and one immutable payload handle, irrespective of scalar kind.
const _: () = assert!(std::mem::size_of::<Expression>() <= 16);

impl Expression {
    pub(crate) fn release_children_into(&mut self, pending: &mut Vec<Expression>) {
        match self {
            Self::Constant(value) => value.release_into(pending),
            Self::ColumnRef(value) => value.release_into(pending),
            Self::Function(value) => value.release_into(pending),
            Self::Cast(value) => value.release_into(pending),
            Self::Conjunction(value) => value.release_into(pending),
            Self::Case(value) => value.release_into(pending),
            Self::Comparison(value) => value.release_into(pending),
            Self::Operator(value) => value.release_into(pending),
            Self::Parameter(value) => value.release_into(pending),
            Self::Reference(value) => value.release_into(pending),
            Self::Aggregate(value) => value.release_into(pending),
            Self::Subquery(value) => value.release_into(pending),
            Self::Window(value) => value.release_into(pending),
        }
    }
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
        let mut contains_external = false;
        ExpressionIterator::visit(self, &mut |expression| {
            if matches!(expression, Expression::Function(function) if function.crosses_execution_boundary())
            {
                contains_external = true;
                ExpressionVisitDecision::SkipChildren
            } else {
                ExpressionVisitDecision::Descend
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
        ExpressionIterator::visit_mut(self, &mut |expression| {
            if let Expression::ColumnRef(column_ref) = expression {
                if let Some(replacement) = f(column_ref) {
                    *expression = replacement;
                }
                ExpressionVisitDecision::SkipChildren
            } else {
                ExpressionVisitDecision::Descend
            }
        });
    }

    /// Recursively replace expressions that match grouping expressions with BoundReferenceExpressions.
    pub fn replace_groups(mut self, groups: &[Expression]) -> Expression {
        self.replace_groups_in_place(groups);
        self
    }

    fn replace_groups_in_place(&mut self, groups: &[Expression]) {
        ExpressionIterator::visit_mut(self, &mut |expression| {
            if let Some(index) = groups.iter().position(|group| expression.equals(group)) {
                let return_type = expression.return_type();
                *expression =
                    Expression::Reference(ReferenceExpression::new(index, return_type).into());
                ExpressionVisitDecision::SkipChildren
            } else {
                ExpressionVisitDecision::Descend
            }
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
        ExpressionIterator::visit_mut(self, &mut |expression| {
            if let Expression::Aggregate(aggregate) = expression {
                let index = offset + aggregates.len();
                let return_type = aggregate.return_type.clone();
                let replacement =
                    Expression::Reference(ReferenceExpression::new(index, return_type).into());
                aggregates.push(std::mem::replace(expression, replacement));
                return ExpressionVisitDecision::SkipChildren;
            }
            if matches!(expression, Expression::Subquery(_)) {
                ExpressionVisitDecision::SkipChildren
            } else {
                ExpressionVisitDecision::Descend
            }
        });
    }

    /// Hoist window expressions into a window operator and replace uses with its output binding.
    ///
    /// Window bindings use a producer-local column index. The physical position is resolved after
    /// the child plan has been finalized, so subquery planning cannot invalidate the binding.
    pub fn extract_windows_in_place(&mut self, windows: &mut Vec<Expression>, window_index: usize) {
        ExpressionIterator::visit_mut(self, &mut |expression| {
            if !matches!(expression, Expression::Window(_)) {
                return if matches!(expression, Expression::Subquery(_)) {
                    ExpressionVisitDecision::SkipChildren
                } else {
                    ExpressionVisitDecision::Descend
                };
            }
            let return_type = expression.return_type();
            let existing = expression
                .evaluation_properties()
                .can_share_evaluation()
                .then(|| windows.iter().position(|window| window.equals(expression)))
                .flatten();
            let output_index = existing.unwrap_or(windows.len());
            let replacement = Expression::ColumnRef(
                ColumnRefExpression::new(
                    ColumnBinding::new(window_index, output_index),
                    return_type,
                )
                .into(),
            );

            if existing.is_some() {
                *expression = replacement;
            } else {
                windows.push(std::mem::replace(expression, replacement));
            }
            ExpressionVisitDecision::SkipChildren
        });
    }

    /// Check if two expressions are semantically equal.
    pub fn equals(&self, other: &Expression) -> bool {
        // Expressions can be much deeper than the surrounding SQL (for
        // example a generated OR chain or a nested CASE).  Keep equality on
        // the same explicit work stack as fingerprinting so a user supplied
        // expression cannot overflow the native stack.
        enum Pending<'a> {
            Expression(&'a Expression, &'a Expression),
            Aggregate(&'a AggregateExpression, &'a AggregateExpression),
            Invocation(&'a WindowInvocation, &'a WindowInvocation),
            FrameBound(&'a WindowFrameBound, &'a WindowFrameBound),
        }

        let mut pending = vec![Pending::Expression(self, other)];
        while let Some(item) = pending.pop() {
            match item {
                Pending::Expression(left, right) => match (left, right) {
                    (Expression::ColumnRef(a), Expression::ColumnRef(b)) => {
                        if a.binding != b.binding || a.depth != b.depth {
                            return false;
                        }
                    }
                    (Expression::Constant(a), Expression::Constant(b)) => {
                        if a.value != b.value {
                            return false;
                        }
                    }
                    (Expression::Function(a), Expression::Function(b)) => {
                        if !routine_identities_equal(
                            a.routine_identity(),
                            b.routine_identity(),
                            || a.function.name == b.function.name,
                        ) || a.function.arguments != b.function.arguments
                            || a.children.len() != b.children.len()
                        {
                            return false;
                        }
                        match (&a.function.bind_data, &b.function.bind_data) {
                            (Some(ad), Some(bd)) if !ad.equals(&**bd) => return false,
                            (None, None) | (Some(_), Some(_)) => {}
                            _ => return false,
                        }
                        pending.extend(
                            a.children
                                .iter()
                                .zip(&b.children)
                                .map(|(left, right)| Pending::Expression(left, right)),
                        );
                    }
                    (Expression::Cast(a), Expression::Cast(b)) => {
                        if a.target_type != b.target_type || a.try_cast != b.try_cast {
                            return false;
                        }
                        pending.push(Pending::Expression(&a.child, &b.child));
                    }
                    (Expression::Conjunction(a), Expression::Conjunction(b)) => {
                        if a.conjunction_type != b.conjunction_type
                            || a.children.len() != b.children.len()
                        {
                            return false;
                        }
                        pending.extend(
                            a.children
                                .iter()
                                .zip(&b.children)
                                .map(|(left, right)| Pending::Expression(left, right)),
                        );
                    }
                    (Expression::Case(a), Expression::Case(b)) => {
                        if a.return_type != b.return_type {
                            return false;
                        }
                        pending.push(Pending::Expression(&a.check, &b.check));
                        pending.push(Pending::Expression(&a.result_if_true, &b.result_if_true));
                        pending.push(Pending::Expression(&a.result_if_false, &b.result_if_false));
                    }
                    (Expression::Comparison(a), Expression::Comparison(b)) => {
                        if a.comparison_type != b.comparison_type {
                            return false;
                        }
                        pending.push(Pending::Expression(&a.left, &b.left));
                        pending.push(Pending::Expression(&a.right, &b.right));
                    }
                    (Expression::Operator(a), Expression::Operator(b)) => {
                        if a.operator_type != b.operator_type
                            || a.children.len() != b.children.len()
                        {
                            return false;
                        }
                        pending.extend(
                            a.children
                                .iter()
                                .zip(&b.children)
                                .map(|(left, right)| Pending::Expression(left, right)),
                        );
                    }
                    (Expression::Parameter(a), Expression::Parameter(b)) => {
                        if a.slot != b.slot || a.return_type() != b.return_type() {
                            return false;
                        }
                    }
                    (Expression::Reference(a), Expression::Reference(b)) => {
                        if a.index != b.index || a.return_type != b.return_type {
                            return false;
                        }
                    }
                    (Expression::Aggregate(a), Expression::Aggregate(b)) => {
                        pending.push(Pending::Aggregate(a, b));
                    }
                    (Expression::Window(a), Expression::Window(b)) => {
                        if a.partitions.len() != b.partitions.len()
                            || a.orders.len() != b.orders.len()
                            || a.frame.frame_type != b.frame.frame_type
                            || a.frame.start_is_preceding != b.frame.start_is_preceding
                            || a.frame.end_is_preceding != b.frame.end_is_preceding
                            || a.ignore_nulls != b.ignore_nulls
                        {
                            return false;
                        }
                        pending.push(Pending::Invocation(&a.invocation, &b.invocation));
                        pending.extend(
                            a.partitions
                                .iter()
                                .zip(&b.partitions)
                                .map(|(left, right)| Pending::Expression(left, right)),
                        );
                        if a.orders.iter().zip(&b.orders).any(|(left, right)| {
                            left.ascending != right.ascending
                                || left.nulls_first != right.nulls_first
                        }) {
                            return false;
                        }
                        pending.extend(a.orders.iter().zip(&b.orders).map(|(left, right)| {
                            Pending::Expression(&left.expression, &right.expression)
                        }));
                        pending.push(Pending::FrameBound(
                            &a.frame.start_bound,
                            &b.frame.start_bound,
                        ));
                        pending.push(Pending::FrameBound(&a.frame.end_bound, &b.frame.end_bound));
                    }
                    _ => return false,
                },
                Pending::Aggregate(left, right) => {
                    if !left.function.execution_semantics_equal(&right.function)
                        || left.return_type != right.return_type
                        || left.aggr_type != right.aggr_type
                        || left.children.len() != right.children.len()
                        || left.order_bys.len() != right.order_bys.len()
                    {
                        return false;
                    }
                    match (&left.filter, &right.filter) {
                        (Some(left), Some(right)) => pending.push(Pending::Expression(left, right)),
                        (None, None) => {}
                        _ => return false,
                    }
                    match (&left.bind_info, &right.bind_info) {
                        (Some(left), Some(right)) if !left.equals(&**right) => return false,
                        (None, None) | (Some(_), Some(_)) => {}
                        _ => return false,
                    }
                    pending.extend(
                        left.children
                            .iter()
                            .zip(&right.children)
                            .map(|(left, right)| Pending::Expression(left, right)),
                    );
                    if left
                        .order_bys
                        .iter()
                        .zip(&right.order_bys)
                        .any(|(left, right)| {
                            left.ascending != right.ascending
                                || left.nulls_first != right.nulls_first
                        })
                    {
                        return false;
                    }
                    pending.extend(left.order_bys.iter().zip(&right.order_bys).map(
                        |(left, right)| Pending::Expression(&left.expression, &right.expression),
                    ));
                }
                Pending::Invocation(left, right) => match (left, right) {
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
                        if left_function.name != right_function.name
                            || left_function.function_type != right_function.function_type
                            || left_function.arguments != right_function.arguments
                            || left_function.return_type != right_function.return_type
                            || left_arguments.len() != right_arguments.len()
                        {
                            return false;
                        }
                        pending.extend(
                            left_arguments
                                .iter()
                                .zip(right_arguments)
                                .map(|(left, right)| Pending::Expression(left, right)),
                        );
                    }
                    (WindowInvocation::Aggregate(left), WindowInvocation::Aggregate(right)) => {
                        pending.push(Pending::Aggregate(left, right));
                    }
                    _ => return false,
                },
                Pending::FrameBound(left, right) => match (left, right) {
                    (WindowFrameBound::Unbounded, WindowFrameBound::Unbounded)
                    | (WindowFrameBound::CurrentRow, WindowFrameBound::CurrentRow) => {}
                    (WindowFrameBound::Offset(left), WindowFrameBound::Offset(right)) => {
                        pending.push(Pending::Expression(left, right));
                    }
                    _ => return false,
                },
            }
        }
        true
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
        AggregateExpression, ColumnRefExpression, ConjunctionExpression, ConjunctionType,
        ConstantExpression, FunctionExpression, OrderByExpression, WindowExpression, WindowFrame,
        WindowFrameBound, WindowFrameType,
    };
    use crate::operator::ColumnBinding;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_function::aggregate::distributive::count::get_count_star_function;
    use paro_function::window::WindowFunction;

    fn int_column(column_index: usize) -> Expression {
        Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(10, column_index), LogicalType::Integer)
                .into(),
        )
    }

    fn int_constant(value: i32) -> Expression {
        Expression::Constant(
            ConstantExpression::new(Value::Integer(value), LogicalType::Integer).into(),
        )
    }

    fn random_call() -> Expression {
        let function = paro_function::scalar::math::get_random_function()
            .functions
            .into_iter()
            .next()
            .expect("random overload");
        Expression::Function(FunctionExpression::new(function, vec![], LogicalType::Double).into())
    }

    fn window_expression(start_bound: WindowFrameBound) -> Expression {
        Expression::Window(
            WindowExpression::native(
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
            )
            .into(),
        )
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
        let WindowFrameBound::Offset(offset) = window.into_inner().frame.start_bound else {
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
        let WindowFrameBound::Offset(offset) = window.into_inner().frame.start_bound else {
            panic!("expected frame offset");
        };
        assert!(matches!(
            *offset,
            Expression::Reference(reference) if reference.index == 0
        ));
    }

    #[test]
    fn extract_aggregates_visits_window_clauses() {
        let aggregate = Expression::Aggregate(
            AggregateExpression::new(get_count_star_function(), vec![], LogicalType::BigInt).into(),
        );
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
            &window.orders[0].expression,
            Expression::Reference(reference) if reference.index == 3
        ));
    }

    #[test]
    fn extract_aggregates_preserves_window_owned_aggregate_kernel() {
        let aggregate =
            AggregateExpression::new(get_count_star_function(), vec![], LogicalType::BigInt);
        let mut expression = Expression::Window(
            WindowExpression::aggregate(
                aggregate,
                vec![int_column(0)],
                vec![],
                WindowFrame::default(),
            )
            .into(),
        );
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

    #[test]
    fn equality_handles_deep_conjunction_without_native_recursion() {
        let mut left = int_constant(1);
        let mut right = int_constant(1);
        for _ in 0..10_000 {
            left = Expression::Conjunction(
                ConjunctionExpression::new(ConjunctionType::And, vec![left]).into(),
            );
            right = Expression::Conjunction(
                ConjunctionExpression::new(ConjunctionType::And, vec![right]).into(),
            );
        }
        assert!(left.equals(&right));
        drop(left);
        drop(right);
    }
}
