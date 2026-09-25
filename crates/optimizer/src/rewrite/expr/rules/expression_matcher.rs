// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Expression Matcher
//!
//! Pattern matchers for Expression used by optimization rules.
//! Each matcher can match specific expression patterns and collect bindings.
//! Many matcher types are part of the rule-extension API (used by tests and future rules).

use paro_planner::expression::Expression;
#[cfg(test)]
use paro_planner::expression::{ComparisonType, ConjunctionType};

#[cfg(test)]
use super::function_matcher::FunctionMatcher;
#[cfg(test)]
use super::set_matcher::{SetMatcher, SetMatcherPolicy};

/// Trait for matching expressions.
///
/// Matchers are used by optimization rules to identify expressions
/// that can be transformed.
pub trait ExpressionMatcher {
    /// Check if the expression matches this pattern.
    ///
    /// # Arguments
    /// * `expr` - The expression to match
    /// * `bindings` - Output vector for matched sub-expressions
    ///
    /// # Returns
    /// `true` if the expression matches the pattern
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool;
}

/// Matches any expression.
#[cfg(any(test, feature = "test-support"))]
pub struct AnyExpressionMatcher;

#[cfg(any(test, feature = "test-support"))]
impl ExpressionMatcher for AnyExpressionMatcher {
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool {
        bindings.push(expr);
        true
    }
}

/// Matches constant expressions.
#[cfg(test)]
pub struct ConstantExpressionMatcher;

#[cfg(test)]
impl ExpressionMatcher for ConstantExpressionMatcher {
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool {
        if matches!(expr, Expression::Constant(_)) {
            bindings.push(expr);
            true
        } else {
            false
        }
    }
}

/// Matches column reference expressions.
#[cfg(test)]
pub struct ColumnRefExpressionMatcher;

#[cfg(test)]
impl ExpressionMatcher for ColumnRefExpressionMatcher {
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool {
        if matches!(expr, Expression::ColumnRef(_)) {
            bindings.push(expr);
            true
        } else {
            false
        }
    }
}

/// Matches comparison expressions with optional child matchers.
#[cfg(test)]
pub struct ComparisonExpressionMatcher {
    /// Optional matcher for comparison type.
    pub comparison_type: Option<ComparisonType>,
    /// Matchers for left and right operands.
    pub child_matchers: Vec<Box<dyn ExpressionMatcher>>,
    /// Policy for matching children.
    pub policy: SetMatcherPolicy,
}

#[cfg(test)]
impl ComparisonExpressionMatcher {
    /// Create a matcher for any comparison expression.
    #[cfg(test)]
    pub fn any() -> Self {
        Self {
            comparison_type: None,
            child_matchers: Vec::new(),
            policy: SetMatcherPolicy::Ordered,
        }
    }

    /// Create a matcher for a specific comparison type.
    #[cfg(test)]
    pub fn with_type(comparison_type: ComparisonType) -> Self {
        Self {
            comparison_type: Some(comparison_type),
            child_matchers: Vec::new(),
            policy: SetMatcherPolicy::Ordered,
        }
    }

    /// Add child matchers for left and right operands.
    #[cfg(test)]
    pub fn with_children(
        mut self,
        left: Box<dyn ExpressionMatcher>,
        right: Box<dyn ExpressionMatcher>,
    ) -> Self {
        self.child_matchers = vec![left, right];
        self
    }
}

#[cfg(test)]
impl ExpressionMatcher for ComparisonExpressionMatcher {
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool {
        let Expression::Comparison(comp) = expr else {
            return false;
        };

        // Check comparison type if specified
        if let Some(expected_type) = &self.comparison_type {
            if comp.comparison_type != *expected_type {
                return false;
            }
        }

        bindings.push(expr);

        // Match children if matchers are provided
        if !self.child_matchers.is_empty() {
            let children: Vec<&Expression> = vec![&*comp.left, &*comp.right];
            return SetMatcher::matches(&self.child_matchers, &children, bindings, self.policy);
        }

        true
    }
}

/// Matches conjunction expressions (AND/OR).
#[cfg(test)]
pub struct ConjunctionExpressionMatcher {
    /// Optional matcher for conjunction type.
    pub conjunction_type: Option<ConjunctionType>,
    /// Matchers for children.
    pub child_matchers: Vec<Box<dyn ExpressionMatcher>>,
    /// Policy for matching children.
    pub policy: SetMatcherPolicy,
}

#[cfg(test)]
impl ConjunctionExpressionMatcher {
    /// Create a matcher for AND expressions.
    #[cfg(test)]
    pub fn and() -> Self {
        Self {
            conjunction_type: Some(ConjunctionType::And),
            child_matchers: Vec::new(),
            policy: SetMatcherPolicy::Unordered,
        }
    }
}

#[cfg(test)]
impl ExpressionMatcher for ConjunctionExpressionMatcher {
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool {
        let Expression::Conjunction(conj) = expr else {
            return false;
        };

        // Check conjunction type if specified
        if let Some(expected_type) = &self.conjunction_type {
            if conj.conjunction_type != *expected_type {
                return false;
            }
        }

        bindings.push(expr);

        // Match children if matchers are provided
        if !self.child_matchers.is_empty() {
            let children: Vec<&Expression> = conj.children.iter().collect();
            return SetMatcher::matches(&self.child_matchers, &children, bindings, self.policy);
        }

        true
    }
}

/// Matches function expressions.
#[cfg(test)]
pub struct FunctionExpressionMatcher {
    /// Optional function name matcher.
    pub function_matcher: Option<Box<dyn FunctionMatcher>>,
    /// Matchers for arguments.
    pub child_matchers: Vec<Box<dyn ExpressionMatcher>>,
    /// Policy for matching children.
    pub policy: SetMatcherPolicy,
}

#[cfg(test)]
impl FunctionExpressionMatcher {
    /// Create a matcher for a specific function.
    #[cfg(test)]
    pub fn with_function(function_matcher: Box<dyn FunctionMatcher>) -> Self {
        Self {
            function_matcher: Some(function_matcher),
            child_matchers: Vec::new(),
            policy: SetMatcherPolicy::Ordered,
        }
    }
}

#[cfg(test)]
impl ExpressionMatcher for FunctionExpressionMatcher {
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool {
        let Expression::Function(func) = expr else {
            return false;
        };

        // Check function name if matcher is provided
        if let Some(ref matcher) = self.function_matcher {
            if !matcher.matches(&func.function.name) {
                return false;
            }
        }

        bindings.push(expr);

        // Match children if matchers are provided
        if !self.child_matchers.is_empty() {
            let children: Vec<&Expression> = func.children.iter().collect();
            return SetMatcher::matches(&self.child_matchers, &children, bindings, self.policy);
        }

        true
    }
}

/// Matches cast expressions.
pub struct FoldableConstantMatcher;

impl FoldableConstantMatcher {
    /// Check if an expression is foldable (can be evaluated at compile time).
    pub fn is_foldable(expr: &Expression) -> bool {
        match expr {
            Expression::Constant(_) => true,
            Expression::Function(func) => {
                func.is_foldable_native() && func.children.iter().all(Self::is_foldable)
            }
            Expression::Cast(cast) => Self::is_foldable(&cast.child),
            Expression::Comparison(comp) => {
                Self::is_foldable(&comp.left) && Self::is_foldable(&comp.right)
            }
            Expression::Conjunction(conj) => conj.children.iter().all(Self::is_foldable),
            Expression::Operator(op) => op.children.iter().all(Self::is_foldable),
            Expression::Case(case) => {
                Self::is_foldable(&case.check)
                    && Self::is_foldable(&case.result_if_true)
                    && Self::is_foldable(&case.result_if_false)
            }
            // Column references, runtime parameters, aggregates, subqueries, windows are not foldable.
            Expression::ColumnRef(_)
            | Expression::Parameter(_)
            | Expression::Reference(_)
            | Expression::Aggregate(_)
            | Expression::Subquery(_)
            | Expression::Window(_) => false,
        }
    }
}

impl ExpressionMatcher for FoldableConstantMatcher {
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool {
        // Don't match pure constants (they're already folded)
        if matches!(expr, Expression::Constant(_)) {
            return false;
        }

        if Self::is_foldable(expr) {
            bindings.push(expr);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rewrite::expr::rules::function_matcher::SpecificFunctionMatcher;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::expression::ComparisonExpression;
    use paro_planner::expression::ConjunctionExpression;
    use paro_planner::expression::{ColumnRefExpression, ConstantExpression};

    fn make_constant(value: i32) -> Expression {
        Expression::Constant(
            ConstantExpression {
                value: Value::Integer(value),
                return_type: LogicalType::Integer,
            }
            .into(),
        )
    }

    fn make_column_ref(table_index: usize, column_index: usize) -> Expression {
        Expression::ColumnRef(
            ColumnRefExpression {
                binding: paro_planner::logical::operator::ColumnBinding {
                    table_index,
                    column_index,
                },
                depth: 0,
                return_type: LogicalType::Integer,
            }
            .into(),
        )
    }

    #[test]
    fn test_any_expression_matcher() {
        let matcher = AnyExpressionMatcher;
        let expr = make_constant(42);
        let mut bindings = Vec::new();

        assert!(matcher.matches(&expr, &mut bindings));
        assert_eq!(bindings.len(), 1);
    }

    #[test]
    fn test_constant_expression_matcher() {
        let matcher = ConstantExpressionMatcher;

        let const_expr = make_constant(42);
        let col_expr = make_column_ref(0, 0);

        let mut bindings = Vec::new();
        assert!(matcher.matches(&const_expr, &mut bindings));
        assert_eq!(bindings.len(), 1);

        bindings.clear();
        assert!(!matcher.matches(&col_expr, &mut bindings));
        assert!(bindings.is_empty());
    }

    #[test]
    fn test_column_ref_expression_matcher() {
        let matcher = ColumnRefExpressionMatcher;

        let const_expr = make_constant(42);
        let col_expr = make_column_ref(0, 0);

        let mut bindings = Vec::new();
        assert!(!matcher.matches(&const_expr, &mut bindings));

        assert!(matcher.matches(&col_expr, &mut bindings));
        assert_eq!(bindings.len(), 1);
    }

    #[test]
    fn test_comparison_expression_matcher() {
        let matcher = ComparisonExpressionMatcher::with_type(ComparisonType::Equal);

        let comp_expr = Expression::Comparison(
            ComparisonExpression::new(ComparisonType::Equal, make_constant(1), make_constant(2))
                .into(),
        );

        let mut bindings = Vec::new();
        assert!(matcher.matches(&comp_expr, &mut bindings));
        assert_eq!(bindings.len(), 1);

        // Wrong comparison type
        let lt_expr = Expression::Comparison(
            ComparisonExpression::new(ComparisonType::LessThan, make_constant(1), make_constant(2))
                .into(),
        );

        bindings.clear();
        assert!(!matcher.matches(&lt_expr, &mut bindings));
    }

    #[test]
    fn test_comparison_with_child_matchers() {
        let matcher = ComparisonExpressionMatcher::any().with_children(
            Box::new(ConstantExpressionMatcher),
            Box::new(ConstantExpressionMatcher),
        );

        let comp_expr = Expression::Comparison(
            ComparisonExpression::new(ComparisonType::Equal, make_constant(1), make_constant(2))
                .into(),
        );

        let mut bindings = Vec::new();
        assert!(matcher.matches(&comp_expr, &mut bindings));
        // 1 for comparison + 2 for children
        assert_eq!(bindings.len(), 3);
    }

    #[test]
    fn test_conjunction_expression_matcher() {
        let matcher = ConjunctionExpressionMatcher::and();

        let and_expr = Expression::Conjunction(
            ConjunctionExpression {
                conjunction_type: ConjunctionType::And,
                children: vec![make_constant(1), make_constant(2)],
            }
            .into(),
        );

        let mut bindings = Vec::new();
        assert!(matcher.matches(&and_expr, &mut bindings));

        // Wrong conjunction type
        let or_expr = Expression::Conjunction(
            ConjunctionExpression {
                conjunction_type: ConjunctionType::Or,
                children: vec![make_constant(1), make_constant(2)],
            }
            .into(),
        );

        bindings.clear();
        assert!(!matcher.matches(&or_expr, &mut bindings));
    }

    #[test]
    fn test_foldable_constant_matcher() {
        let matcher = FoldableConstantMatcher;

        // Pure constant - should NOT match (already folded)
        let const_expr = make_constant(42);
        let mut bindings = Vec::new();
        assert!(!matcher.matches(&const_expr, &mut bindings));

        // Column ref - should NOT match (not foldable)
        let col_expr = make_column_ref(0, 0);
        assert!(!matcher.matches(&col_expr, &mut bindings));

        // Comparison of two constants - should match (foldable but not a constant)
        let comp_expr = Expression::Comparison(
            ComparisonExpression::new(ComparisonType::Equal, make_constant(1), make_constant(2))
                .into(),
        );
        assert!(matcher.matches(&comp_expr, &mut bindings));
        assert_eq!(bindings.len(), 1);
    }

    #[test]
    fn test_function_expression_matcher_with_name() {
        use paro_common::chunk::Chunk;
        use paro_common::vector::Vector;
        use paro_function::scalar::{ExpressionState, ScalarFunction};
        use paro_planner::expression::FunctionExpression;

        fn dummy_fn(
            _input: &Chunk,
            _state: &dyn ExpressionState,
            _result: &mut Vector,
        ) -> paro_common::error::Result<()> {
            Ok(())
        }

        let matcher =
            FunctionExpressionMatcher::with_function(Box::new(SpecificFunctionMatcher::new("add")));

        let func_expr = Expression::Function(
            FunctionExpression::new(
                ScalarFunction::new(
                    "add".to_string(),
                    vec![LogicalType::Integer, LogicalType::Integer],
                    LogicalType::Integer,
                    dummy_fn,
                ),
                vec![make_constant(1), make_constant(2)],
                LogicalType::Integer,
            )
            .into(),
        );

        let mut bindings = Vec::new();
        assert!(matcher.matches(&func_expr, &mut bindings));

        // Wrong function name
        let wrong_func = Expression::Function(
            FunctionExpression::new(
                ScalarFunction::new(
                    "subtract".to_string(),
                    vec![LogicalType::Integer, LogicalType::Integer],
                    LogicalType::Integer,
                    dummy_fn,
                ),
                vec![make_constant(1), make_constant(2)],
                LogicalType::Integer,
            )
            .into(),
        );

        bindings.clear();
        assert!(!matcher.matches(&wrong_func, &mut bindings));
    }
}
