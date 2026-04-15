// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Traits and result types for expression rewrite rules.

use paro_planner::expression::Expression;
use paro_planner::operator::LogicalOperator;

use super::expression_matcher::ExpressionMatcher;

/// Result of applying a rule.
pub enum RuleResult {
    /// The rule made changes and returned a new expression.
    Changed(Box<Expression>),
    /// The rule made changes but the root expression is the same (in-place modification).
    /// The rewriter should re-run rules on the children.
    Rerun,
    /// The rule did not make any changes.
    NoChange,
}

/// Base trait for optimization rules.
///
/// Rules are used by the ExpressionRewriter to transform expressions.
/// Each rule has a matcher that identifies expressions it can optimize,
/// and an apply method that performs the transformation.
///
/// # Example
/// ```ignore
/// struct ConstantFoldingRule {
///     matcher: FoldableConstantMatcher,
/// }
///
/// impl Rule for ConstantFoldingRule {
///     fn matcher(&self) -> &dyn ExpressionMatcher {
///         &self.matcher
///     }
///
///     fn apply(&self, op: &LogicalOperator, bindings: Vec<&Expression>, is_root: bool)
///         -> RuleResult
///     {
///         // Evaluate the foldable expression and return a constant
///         let result = evaluate_constant(&bindings[0]);
///         RuleResult::Changed(Expression::Constant(result))
///     }
/// }
/// ```
pub trait Rule {
    /// Get the expression matcher for this rule.
    ///
    /// The matcher identifies expressions that this rule can potentially optimize.
    fn matcher(&self) -> &dyn ExpressionMatcher;

    /// Apply the rule to the matched expression.
    ///
    /// # Arguments
    /// * `op` - The logical operator containing the expression
    /// * `bindings` - The matched sub-expressions from the matcher
    /// * `is_root` - Whether this is the root expression of the operator
    ///
    /// # Returns
    /// * `RuleResult::Changed(expr)` - The rule transformed the expression
    /// * `RuleResult::Rerun` - The rule made changes, re-run rules on same expression
    /// * `RuleResult::NoChange` - The rule did not apply
    fn apply(&self, op: &LogicalOperator, bindings: Vec<&Expression>, is_root: bool) -> RuleResult;

    /// Get the rule name for diagnostics.
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}
