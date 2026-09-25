// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Function Matcher
//!
//! Matchers for function names used in expression pattern matching.
//! Types here are part of the rule-extension API (used by tests and future rules).

use std::collections::HashSet;

/// Trait for matching function names.
pub trait FunctionMatcher: Send + Sync {
    /// Check if the given function name matches this matcher.
    fn matches(&self, name: &str) -> bool;
}

/// Matches a specific function name.
pub struct SpecificFunctionMatcher {
    name: String,
}

impl SpecificFunctionMatcher {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl FunctionMatcher for SpecificFunctionMatcher {
    fn matches(&self, name: &str) -> bool {
        self.name == name
    }
}

/// Matches any function name from a set.
pub struct ManyFunctionMatcher {
    names: HashSet<String>,
}

impl ManyFunctionMatcher {
    pub fn new(names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            names: names.into_iter().map(|s| s.into()).collect(),
        }
    }
}

impl FunctionMatcher for ManyFunctionMatcher {
    fn matches(&self, name: &str) -> bool {
        self.names.contains(name)
    }
}

/// Matches any function name (always returns true).
pub struct AnyFunctionMatcher;

impl FunctionMatcher for AnyFunctionMatcher {
    fn matches(&self, _name: &str) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_specific_function_matcher() {
        let matcher = SpecificFunctionMatcher::new("add");
        assert!(matcher.matches("add"));
        assert!(!matcher.matches("subtract"));
        assert!(!matcher.matches("ADD")); // case sensitive
    }

    #[test]
    fn test_many_function_matcher() {
        let matcher = ManyFunctionMatcher::new(["add", "subtract", "multiply"]);
        assert!(matcher.matches("add"));
        assert!(matcher.matches("subtract"));
        assert!(matcher.matches("multiply"));
        assert!(!matcher.matches("divide"));
    }

    #[test]
    fn test_any_function_matcher() {
        let matcher = AnyFunctionMatcher;
        assert!(matcher.matches("add"));
        assert!(matcher.matches("any_function"));
        assert!(matcher.matches(""));
    }
}
