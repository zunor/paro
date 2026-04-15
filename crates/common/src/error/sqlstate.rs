// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! SQLSTATE error codes following PostgreSQL conventions.

use super::ErrorClass;

/// SQLSTATE error code.
///
/// A 5-character code following the SQL standard and PostgreSQL extensions.
/// Supports exact matching via [`is()`](Self::is) and category matching via
/// [`error_class()`](Self::error_class).
///
/// # Example
///
/// ```ignore
/// use paro_common::error::{SqlState, codes};
///
/// let code = codes::syntax::UNDEFINED_TABLE;
///
/// // Exact match
/// assert!(code.is(codes::syntax::UNDEFINED_TABLE));
///
/// // Class check
/// assert!(code.is_class("42"));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SqlState([u8; 5]);

impl SqlState {
    /// Creates a new SQLSTATE from a 5-byte array.
    #[inline]
    pub const fn new(code: [u8; 5]) -> Self {
        Self(code)
    }

    /// Creates a SQLSTATE from a string slice.
    pub fn try_from_str(s: &str) -> Option<Self> {
        if s.len() != 5 {
            return None;
        }
        let bytes = s.as_bytes();
        Some(Self([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4]]))
    }

    /// Returns the SQLSTATE as a string slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap_or("XX000")
    }

    /// Returns the error class code (first 2 characters).
    #[inline]
    pub fn class(&self) -> &str {
        std::str::from_utf8(&self.0[0..2]).unwrap_or("XX")
    }

    /// Checks if the SQLSTATE belongs to the given class code.
    #[inline]
    pub fn is_class(&self, class: &str) -> bool {
        self.class() == class
    }

    /// Returns the raw bytes of the SQLSTATE.
    #[inline]
    pub fn as_bytes(&self) -> &[u8; 5] {
        &self.0
    }

    // ========================================
    // Error identification API
    // ========================================

    /// Checks if this SQLSTATE exactly matches another.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use paro_common::error::codes;
    ///
    /// let code = codes::syntax::UNDEFINED_TABLE;
    /// assert!(code.is(codes::syntax::UNDEFINED_TABLE));
    /// assert!(!code.is(codes::syntax::UNDEFINED_COLUMN));
    /// ```
    #[inline]
    pub fn is(&self, other: SqlState) -> bool {
        self.0 == other.0
    }

    /// Returns the error class category for this SQLSTATE.
    ///
    /// This enables Rust pattern matching on error categories.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use paro_common::error::{codes, ErrorClass};
    ///
    /// let code = codes::syntax::UNDEFINED_TABLE;
    /// assert_eq!(code.error_class(), ErrorClass::Syntax);
    /// ```
    #[inline]
    pub fn error_class(&self) -> ErrorClass {
        ErrorClass::from_class_code(self.class())
    }

    // ========================================
    // Class predicate methods
    // ========================================

    /// Returns true if this is a syntax/access rule error (Class 42).
    #[inline]
    pub fn is_syntax_class(&self) -> bool {
        self.is_class("42")
    }

    /// Returns true if this is a data exception (Class 22).
    #[inline]
    pub fn is_data_exception(&self) -> bool {
        self.is_class("22")
    }

    /// Returns true if this is a constraint violation (Class 23).
    #[inline]
    pub fn is_constraint_violation(&self) -> bool {
        self.is_class("23")
    }

    /// Returns true if this is a transaction state error (Class 25).
    #[inline]
    pub fn is_transaction_error(&self) -> bool {
        self.is_class("25")
    }

    /// Returns true if this is a transaction rollback (Class 40).
    #[inline]
    pub fn is_transaction_rollback(&self) -> bool {
        self.is_class("40")
    }

    /// Returns true if this is an internal error (Class XX).
    #[inline]
    pub fn is_internal_error(&self) -> bool {
        self.is_class("XX")
    }

    /// Returns true if this is a feature not supported error (Class 0A).
    #[inline]
    pub fn is_feature_not_supported(&self) -> bool {
        self.is_class("0A")
    }

    /// Returns true if this is a system error (Class 58).
    #[inline]
    pub fn is_system_error(&self) -> bool {
        self.is_class("58")
    }

    /// Returns true if this is a resource error (Class 53).
    #[inline]
    pub fn is_resource_error(&self) -> bool {
        self.is_class("53")
    }

    /// Returns true if this is a connection exception (Class 08).
    #[inline]
    pub fn is_connection_error(&self) -> bool {
        self.is_class("08")
    }

    /// Returns true if this is an operator intervention (Class 57).
    #[inline]
    pub fn is_operator_intervention(&self) -> bool {
        self.is_class("57")
    }
}

impl std::fmt::Display for SqlState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Default for SqlState {
    fn default() -> Self {
        Self::new(*b"XX000")
    }
}
