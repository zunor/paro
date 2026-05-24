// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Expression Matcher
//!
//! Pattern matchers for Expression used by optimization rules.
//! Each matcher can match specific expression patterns and collect bindings.
//! Many matcher types are part of the rule-extension API (used by tests and future rules).

use paro_planner::expression::{ComparisonType, ConjunctionType, Expression, OperatorType};

use super::function_matcher::FunctionMatcher;
use super::set_matcher::{SetMatcher, SetMatcherPolicy};
use super::type_matcher::TypeMatcher;

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
pub struct AnyExpressionMatcher;

impl ExpressionMatcher for AnyExpressionMatcher {
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool {
        bindings.push(expr);
        true
    }
}

/// Matches constant expressions.
pub struct ConstantExpressionMatcher;

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
pub struct ColumnRefExpressionMatcher;

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
pub struct ComparisonExpressionMatcher {
    /// Optional matcher for comparison type.
    pub comparison_type: Option<ComparisonType>,
    /// Matchers for left and right operands.
    pub child_matchers: Vec<Box<dyn ExpressionMatcher>>,
    /// Policy for matching children.
    pub policy: SetMatcherPolicy,
}

impl ComparisonExpressionMatcher {
    /// Create a matcher for any comparison expression.
    pub fn any() -> Self {
        Self {
            comparison_type: None,
            child_matchers: Vec::new(),
            policy: SetMatcherPolicy::Ordered,
        }
    }

    /// Create a matcher for a specific comparison type.
    pub fn with_type(comparison_type: ComparisonType) -> Self {
        Self {
            comparison_type: Some(comparison_type),
            child_matchers: Vec::new(),
            policy: SetMatcherPolicy::Ordered,
        }
    }

    /// Add child matchers for left and right operands.
    pub fn with_children(
        mut self,
        left: Box<dyn ExpressionMatcher>,
        right: Box<dyn ExpressionMatcher>,
    ) -> Self {
        self.child_matchers = vec![left, right];
        self
    }

    /// Set the matching policy.
    pub fn with_policy(mut self, policy: SetMatcherPolicy) -> Self {
        self.policy = policy;
        self
    }
}

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
pub struct ConjunctionExpressionMatcher {
    /// Optional matcher for conjunction type.
    pub conjunction_type: Option<ConjunctionType>,
    /// Matchers for children.
    pub child_matchers: Vec<Box<dyn ExpressionMatcher>>,
    /// Policy for matching children.
    pub policy: SetMatcherPolicy,
}

impl ConjunctionExpressionMatcher {
    /// Create a matcher for any conjunction expression.
    pub fn any() -> Self {
        Self {
            conjunction_type: None,
            child_matchers: Vec::new(),
            policy: SetMatcherPolicy::Unordered,
        }
    }

    /// Create a matcher for AND expressions.
    pub fn and() -> Self {
        Self {
            conjunction_type: Some(ConjunctionType::And),
            child_matchers: Vec::new(),
            policy: SetMatcherPolicy::Unordered,
        }
    }

    /// Create a matcher for OR expressions.
    pub fn or() -> Self {
        Self {
            conjunction_type: Some(ConjunctionType::Or),
            child_matchers: Vec::new(),
            policy: SetMatcherPolicy::Unordered,
        }
    }

    /// Add child matchers.
    pub fn with_children(mut self, matchers: Vec<Box<dyn ExpressionMatcher>>) -> Self {
        self.child_matchers = matchers;
        self
    }

    /// Set the matching policy.
    pub fn with_policy(mut self, policy: SetMatcherPolicy) -> Self {
        self.policy = policy;
        self
    }
}

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
pub struct FunctionExpressionMatcher {
    /// Optional function name matcher.
    pub function_matcher: Option<Box<dyn FunctionMatcher>>,
    /// Matchers for arguments.
    pub child_matchers: Vec<Box<dyn ExpressionMatcher>>,
    /// Policy for matching children.
    pub policy: SetMatcherPolicy,
}

impl FunctionExpressionMatcher {
    /// Create a matcher for any function expression.
    pub fn any() -> Self {
        Self {
            function_matcher: None,
            child_matchers: Vec::new(),
            policy: SetMatcherPolicy::Ordered,
        }
    }

    /// Create a matcher for a specific function.
    pub fn with_function(function_matcher: Box<dyn FunctionMatcher>) -> Self {
        Self {
            function_matcher: Some(function_matcher),
            child_matchers: Vec::new(),
            policy: SetMatcherPolicy::Ordered,
        }
    }

    /// Add child matchers for arguments.
    pub fn with_children(mut self, matchers: Vec<Box<dyn ExpressionMatcher>>) -> Self {
        self.child_matchers = matchers;
        self
    }

    /// Set the matching policy.
    pub fn with_policy(mut self, policy: SetMatcherPolicy) -> Self {
        self.policy = policy;
        self
    }
}

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
pub struct CastExpressionMatcher {
    /// Optional type matcher for target type.
    pub type_matcher: Option<Box<dyn TypeMatcher>>,
    /// Optional matcher for the child expression.
    pub child_matcher: Option<Box<dyn ExpressionMatcher>>,
}

impl CastExpressionMatcher {
    /// Create a matcher for any cast expression.
    pub fn any() -> Self {
        Self {
            type_matcher: None,
            child_matcher: None,
        }
    }

    /// Create a matcher for cast to a specific type.
    pub fn with_type(type_matcher: Box<dyn TypeMatcher>) -> Self {
        Self {
            type_matcher: Some(type_matcher),
            child_matcher: None,
        }
    }

    /// Add a child matcher.
    pub fn with_child(mut self, matcher: Box<dyn ExpressionMatcher>) -> Self {
        self.child_matcher = Some(matcher);
        self
    }
}

impl ExpressionMatcher for CastExpressionMatcher {
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool {
        let Expression::Cast(cast) = expr else {
            return false;
        };

        // Check target type if matcher is provided
        if let Some(ref matcher) = self.type_matcher {
            if !matcher.matches(&cast.target_type) {
                return false;
            }
        }

        bindings.push(expr);

        // Match child if matcher is provided
        if let Some(ref matcher) = self.child_matcher {
            return matcher.matches(&cast.child, bindings);
        }

        true
    }
}

/// Matches operator expressions (IN, NOT, IS NULL, etc.).
pub struct OperatorExpressionMatcher {
    /// Optional operator type.
    pub operator_type: Option<OperatorType>,
    /// Matchers for children.
    pub child_matchers: Vec<Box<dyn ExpressionMatcher>>,
    /// Policy for matching children.
    pub policy: SetMatcherPolicy,
}

impl OperatorExpressionMatcher {
    /// Create a matcher for any operator expression.
    pub fn any() -> Self {
        Self {
            operator_type: None,
            child_matchers: Vec::new(),
            policy: SetMatcherPolicy::Ordered,
        }
    }

    /// Create a matcher for a specific operator type.
    pub fn with_type(operator_type: OperatorType) -> Self {
        Self {
            operator_type: Some(operator_type),
            child_matchers: Vec::new(),
            policy: SetMatcherPolicy::Ordered,
        }
    }

    /// Add child matchers.
    pub fn with_children(mut self, matchers: Vec<Box<dyn ExpressionMatcher>>) -> Self {
        self.child_matchers = matchers;
        self
    }
}

impl ExpressionMatcher for OperatorExpressionMatcher {
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool {
        let Expression::Operator(op) = expr else {
            return false;
        };

        // Check operator type if specified
        if let Some(expected_type) = &self.operator_type {
            if op.operator_type != *expected_type {
                return false;
            }
        }

        bindings.push(expr);

        // Match children if matchers are provided
        if !self.child_matchers.is_empty() {
            let children: Vec<&Expression> = op.children.iter().collect();
            return SetMatcher::matches(&self.child_matchers, &children, bindings, self.policy);
        }

        true
    }
}

/// Matches aggregate expressions.
pub struct AggregateExpressionMatcher {
    /// Optional function name matcher.
    pub function_matcher: Option<Box<dyn FunctionMatcher>>,
    /// Matchers for arguments.
    pub child_matchers: Vec<Box<dyn ExpressionMatcher>>,
    /// Policy for matching children.
    pub policy: SetMatcherPolicy,
}

impl AggregateExpressionMatcher {
    /// Create a matcher for any aggregate expression.
    pub fn any() -> Self {
        Self {
            function_matcher: None,
            child_matchers: Vec::new(),
            policy: SetMatcherPolicy::Ordered,
        }
    }

    /// Create a matcher for a specific aggregate function.
    pub fn with_function(function_matcher: Box<dyn FunctionMatcher>) -> Self {
        Self {
            function_matcher: Some(function_matcher),
            child_matchers: Vec::new(),
            policy: SetMatcherPolicy::Ordered,
        }
    }
}

impl ExpressionMatcher for AggregateExpressionMatcher {
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool {
        let Expression::Aggregate(agg) = expr else {
            return false;
        };

        // Check function name if matcher is provided
        if let Some(ref matcher) = self.function_matcher {
            if !matcher.matches(&agg.function.name) {
                return false;
            }
        }

        bindings.push(expr);

        // Match children if matchers are provided
        if !self.child_matchers.is_empty() {
            let children: Vec<&Expression> = agg.children.iter().collect();
            return SetMatcher::matches(&self.child_matchers, &children, bindings, self.policy);
        }

        true
    }
}

/// Matches any foldable constant expression.
///
/// A foldable expression is one that can be evaluated at compile time
/// (contains only constants and deterministic functions).
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

/// Matches expressions with a specific return type.
pub struct TypedExpressionMatcher {
    type_matcher: Box<dyn TypeMatcher>,
}

impl TypedExpressionMatcher {
    pub fn new(type_matcher: Box<dyn TypeMatcher>) -> Self {
        Self { type_matcher }
    }
}

impl ExpressionMatcher for TypedExpressionMatcher {
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool {
        if self.type_matcher.matches(&expr.return_type()) {
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
    use crate::rules::function_matcher::SpecificFunctionMatcher;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::expression::ComparisonExpression;
    use paro_planner::expression::ConjunctionExpression;
    use paro_planner::expression::{ColumnRefExpression, ConstantExpression};

    fn make_constant(value: i32) -> Expression {
        Expression::Constant(ConstantExpression {
            value: Value::Integer(value),
            return_type: LogicalType::Integer,
        })
    }

    fn make_column_ref(table_index: usize, column_index: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression {
            binding: paro_planner::operator::ColumnBinding {
                table_index,
                column_index,
            },
            depth: 0,
            return_type: LogicalType::Integer,
        })
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

        let comp_expr = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            make_constant(1),
            make_constant(2),
        ));

        let mut bindings = Vec::new();
        assert!(matcher.matches(&comp_expr, &mut bindings));
        assert_eq!(bindings.len(), 1);

        // Wrong comparison type
        let lt_expr = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::LessThan,
            make_constant(1),
            make_constant(2),
        ));

        bindings.clear();
        assert!(!matcher.matches(&lt_expr, &mut bindings));
    }

    #[test]
    fn test_comparison_with_child_matchers() {
        let matcher = ComparisonExpressionMatcher::any().with_children(
            Box::new(ConstantExpressionMatcher),
            Box::new(ConstantExpressionMatcher),
        );

        let comp_expr = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            make_constant(1),
            make_constant(2),
        ));

        let mut bindings = Vec::new();
        assert!(matcher.matches(&comp_expr, &mut bindings));
        // 1 for comparison + 2 for children
        assert_eq!(bindings.len(), 3);
    }

    #[test]
    fn test_conjunction_expression_matcher() {
        let matcher = ConjunctionExpressionMatcher::and();

        let and_expr = Expression::Conjunction(ConjunctionExpression {
            conjunction_type: ConjunctionType::And,
            children: vec![make_constant(1), make_constant(2)],
        });

        let mut bindings = Vec::new();
        assert!(matcher.matches(&and_expr, &mut bindings));

        // Wrong conjunction type
        let or_expr = Expression::Conjunction(ConjunctionExpression {
            conjunction_type: ConjunctionType::Or,
            children: vec![make_constant(1), make_constant(2)],
        });

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
        let comp_expr = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            make_constant(1),
            make_constant(2),
        ));
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

        let func_expr = Expression::Function(FunctionExpression::new(
            ScalarFunction::new(
                "add".to_string(),
                vec![LogicalType::Integer, LogicalType::Integer],
                LogicalType::Integer,
                dummy_fn,
            ),
            vec![make_constant(1), make_constant(2)],
            LogicalType::Integer,
        ));

        let mut bindings = Vec::new();
        assert!(matcher.matches(&func_expr, &mut bindings));

        // Wrong function name
        let wrong_func = Expression::Function(FunctionExpression::new(
            ScalarFunction::new(
                "subtract".to_string(),
                vec![LogicalType::Integer, LogicalType::Integer],
                LogicalType::Integer,
                dummy_fn,
            ),
            vec![make_constant(1), make_constant(2)],
            LogicalType::Integer,
        ));

        bindings.clear();
        assert!(!matcher.matches(&wrong_func, &mut bindings));
    }
}
