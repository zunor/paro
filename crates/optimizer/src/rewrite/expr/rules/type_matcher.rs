// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Type Matcher
//!
//! Matchers for LogicalType used in expression pattern matching.
//! Types here are part of the rule-extension API (used by tests and future rules).

use paro_common::types::LogicalType;

/// Trait for matching LogicalType in expressions.
pub trait TypeMatcher: Send + Sync {
    /// Check if the given type matches this matcher.
    fn matches(&self, logical_type: &LogicalType) -> bool;
}

/// Matches a specific LogicalType.
pub struct SpecificTypeMatcher {
    target_type: LogicalType,
}

impl SpecificTypeMatcher {
    pub fn new(target_type: LogicalType) -> Self {
        Self { target_type }
    }
}

impl TypeMatcher for SpecificTypeMatcher {
    fn matches(&self, logical_type: &LogicalType) -> bool {
        *logical_type == self.target_type
    }
}

/// Matches any type from a set of types.
pub struct SetTypeMatcher {
    types: Vec<LogicalType>,
}

impl SetTypeMatcher {
    pub fn new(types: Vec<LogicalType>) -> Self {
        Self { types }
    }
}

impl TypeMatcher for SetTypeMatcher {
    fn matches(&self, logical_type: &LogicalType) -> bool {
        self.types.iter().any(|t| t == logical_type)
    }
}

/// Matches any numeric type (Integer, BigInt, Float, Double, Decimal).
pub struct NumericTypeMatcher;

impl TypeMatcher for NumericTypeMatcher {
    fn matches(&self, logical_type: &LogicalType) -> bool {
        matches!(
            logical_type,
            LogicalType::TinyInt
                | LogicalType::SmallInt
                | LogicalType::Integer
                | LogicalType::BigInt
                | LogicalType::Float
                | LogicalType::Double
                | LogicalType::Decimal { .. }
        )
    }
}

/// Matches any integer type (TinyInt, SmallInt, Integer, BigInt).
pub struct IntegerTypeMatcher;

impl TypeMatcher for IntegerTypeMatcher {
    fn matches(&self, logical_type: &LogicalType) -> bool {
        matches!(
            logical_type,
            LogicalType::TinyInt
                | LogicalType::SmallInt
                | LogicalType::Integer
                | LogicalType::BigInt
        )
    }
}

/// Matches any type (always returns true).
pub struct AnyTypeMatcher;

impl TypeMatcher for AnyTypeMatcher {
    fn matches(&self, _logical_type: &LogicalType) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_specific_type_matcher() {
        let matcher = SpecificTypeMatcher::new(LogicalType::Integer);
        assert!(matcher.matches(&LogicalType::Integer));
        assert!(!matcher.matches(&LogicalType::Varchar));
        assert!(!matcher.matches(&LogicalType::Boolean));
    }

    #[test]
    fn test_set_type_matcher() {
        let matcher = SetTypeMatcher::new(vec![LogicalType::Integer, LogicalType::BigInt]);
        assert!(matcher.matches(&LogicalType::Integer));
        assert!(matcher.matches(&LogicalType::BigInt));
        assert!(!matcher.matches(&LogicalType::Varchar));
    }

    #[test]
    fn test_numeric_type_matcher() {
        let matcher = NumericTypeMatcher;
        assert!(matcher.matches(&LogicalType::Integer));
        assert!(matcher.matches(&LogicalType::BigInt));
        assert!(matcher.matches(&LogicalType::Float));
        assert!(matcher.matches(&LogicalType::Double));
        assert!(!matcher.matches(&LogicalType::Varchar));
        assert!(!matcher.matches(&LogicalType::Boolean));
    }

    #[test]
    fn test_integer_type_matcher() {
        let matcher = IntegerTypeMatcher;
        assert!(matcher.matches(&LogicalType::Integer));
        assert!(matcher.matches(&LogicalType::BigInt));
        assert!(matcher.matches(&LogicalType::TinyInt));
        assert!(matcher.matches(&LogicalType::SmallInt));
        assert!(!matcher.matches(&LogicalType::Float));
        assert!(!matcher.matches(&LogicalType::Double));
    }

    #[test]
    fn test_any_type_matcher() {
        let matcher = AnyTypeMatcher;
        assert!(matcher.matches(&LogicalType::Integer));
        assert!(matcher.matches(&LogicalType::Varchar));
        assert!(matcher.matches(&LogicalType::Boolean));
    }
}
