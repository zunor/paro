// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Match sets of expressions using different policies.

use paro_planner::expression::Expression;
use std::collections::HashSet;

use super::expression_matcher::ExpressionMatcher;

/// Policy for matching sets of expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetMatcherPolicy {
    /// All entries must be matched in order.
    Ordered,
    /// All entries must be matched, but order doesn't matter.
    Unordered,
    /// Only some entries need to be matched, order doesn't matter.
    Some,
    /// Only some entries need to be matched, order matters.
    SomeOrdered,
}

/// Utility for matching sets of expressions.
pub struct SetMatcher;

impl SetMatcher {
    /// Match a set of matchers against a set of expressions.
    ///
    /// # Arguments
    /// * `matchers` - The matchers to apply
    /// * `entries` - The expressions to match against
    /// * `bindings` - Output vector for matched expressions
    /// * `policy` - The matching policy to use
    ///
    /// # Returns
    /// `true` if matching succeeded according to the policy
    pub fn matches<'a>(
        matchers: &[Box<dyn ExpressionMatcher>],
        entries: &[&'a Expression],
        bindings: &mut Vec<&'a Expression>,
        policy: SetMatcherPolicy,
    ) -> bool {
        match policy {
            SetMatcherPolicy::Ordered => {
                // Count must match and entries must match in order
                if matchers.len() != entries.len() {
                    return false;
                }
                for (matcher, entry) in matchers.iter().zip(entries.iter()) {
                    if !matcher.matches(entry, bindings) {
                        return false;
                    }
                }
                true
            }
            SetMatcherPolicy::SomeOrdered => {
                // Entries must be at least as many as matchers
                if entries.len() < matchers.len() {
                    return false;
                }
                // Provided matchers must match in order
                for (matcher, entry) in matchers.iter().zip(entries.iter()) {
                    if !matcher.matches(entry, bindings) {
                        return false;
                    }
                }
                true
            }
            SetMatcherPolicy::Unordered => {
                // Count must match
                if matchers.len() != entries.len() {
                    return false;
                }
                Self::match_recursive(matchers, entries, bindings, &mut HashSet::new(), 0)
            }
            SetMatcherPolicy::Some => {
                // Every matcher must match a unique entry
                if matchers.len() > entries.len() {
                    return false;
                }
                Self::match_recursive(matchers, entries, bindings, &mut HashSet::new(), 0)
            }
        }
    }

    /// Recursive helper for unordered matching.
    fn match_recursive<'a>(
        matchers: &[Box<dyn ExpressionMatcher>],
        entries: &[&'a Expression],
        bindings: &mut Vec<&'a Expression>,
        excluded: &mut HashSet<usize>,
        matcher_idx: usize,
    ) -> bool {
        if matcher_idx == matchers.len() {
            // All matchers matched
            return true;
        }

        let previous_binding_count = bindings.len();

        for (entry_idx, entry) in entries.iter().enumerate() {
            // Skip already matched entries
            if excluded.contains(&entry_idx) {
                continue;
            }

            // Try to match this entry
            if matchers[matcher_idx].matches(entry, bindings) {
                // Mark as used and try to match remaining
                excluded.insert(entry_idx);

                if Self::match_recursive(matchers, entries, bindings, excluded, matcher_idx + 1) {
                    return true;
                }

                // Backtrack: remove from excluded and restore bindings
                excluded.remove(&entry_idx);
                bindings.truncate(previous_binding_count);
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::expression_matcher::ConstantExpressionMatcher;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::expression::ConstantExpression;

    fn make_constant(value: i32) -> Expression {
        Expression::Constant(ConstantExpression {
            value: Value::Integer(value),
            return_type: LogicalType::Integer,
        })
    }

    #[test]
    fn test_ordered_policy_match() {
        let matchers: Vec<Box<dyn ExpressionMatcher>> = vec![
            Box::new(ConstantExpressionMatcher),
            Box::new(ConstantExpressionMatcher),
        ];

        let expr1 = make_constant(1);
        let expr2 = make_constant(2);
        let entries: Vec<&Expression> = vec![&expr1, &expr2];
        let mut bindings = Vec::new();

        assert!(SetMatcher::matches(
            &matchers,
            &entries,
            &mut bindings,
            SetMatcherPolicy::Ordered
        ));
        assert_eq!(bindings.len(), 2);
    }

    #[test]
    fn test_ordered_policy_count_mismatch() {
        let matchers: Vec<Box<dyn ExpressionMatcher>> = vec![
            Box::new(ConstantExpressionMatcher),
            Box::new(ConstantExpressionMatcher),
        ];

        let expr1 = make_constant(1);
        let entries: Vec<&Expression> = vec![&expr1];
        let mut bindings = Vec::new();

        assert!(!SetMatcher::matches(
            &matchers,
            &entries,
            &mut bindings,
            SetMatcherPolicy::Ordered
        ));
    }

    #[test]
    fn test_unordered_policy_match() {
        let matchers: Vec<Box<dyn ExpressionMatcher>> = vec![
            Box::new(ConstantExpressionMatcher),
            Box::new(ConstantExpressionMatcher),
        ];

        let expr1 = make_constant(1);
        let expr2 = make_constant(2);
        let entries: Vec<&Expression> = vec![&expr1, &expr2];
        let mut bindings = Vec::new();

        assert!(SetMatcher::matches(
            &matchers,
            &entries,
            &mut bindings,
            SetMatcherPolicy::Unordered
        ));
    }

    #[test]
    fn test_some_policy_match() {
        let matchers: Vec<Box<dyn ExpressionMatcher>> = vec![Box::new(ConstantExpressionMatcher)];

        let expr1 = make_constant(1);
        let expr2 = make_constant(2);
        let entries: Vec<&Expression> = vec![&expr1, &expr2];
        let mut bindings = Vec::new();

        // Only one matcher, two entries - should match
        assert!(SetMatcher::matches(
            &matchers,
            &entries,
            &mut bindings,
            SetMatcherPolicy::Some
        ));
        assert_eq!(bindings.len(), 1);
    }

    #[test]
    fn test_some_ordered_policy_match() {
        let matchers: Vec<Box<dyn ExpressionMatcher>> = vec![Box::new(ConstantExpressionMatcher)];

        let expr1 = make_constant(1);
        let expr2 = make_constant(2);
        let entries: Vec<&Expression> = vec![&expr1, &expr2];
        let mut bindings = Vec::new();

        assert!(SetMatcher::matches(
            &matchers,
            &entries,
            &mut bindings,
            SetMatcherPolicy::SomeOrdered
        ));
        assert_eq!(bindings.len(), 1);
    }
}
