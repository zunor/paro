// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! ParoError - the main error type for Paro operations.

use super::{ErrorData, SqlState};
use std::borrow::Cow;

/// The main error type for Paro operations.
#[derive(Debug, Clone, Default)]
pub struct ParoError(pub Box<ErrorData>);

impl ParoError {
    #[inline]
    pub fn new(data: ErrorData) -> Self {
        Self(Box::new(data))
    }

    #[inline]
    pub fn data(&self) -> &ErrorData {
        &self.0
    }

    #[inline]
    pub fn data_mut(&mut self) -> &mut ErrorData {
        &mut self.0
    }

    #[inline]
    pub fn sqlstate(&self) -> SqlState {
        self.0.sqlstate
    }

    #[inline]
    pub fn message(&self) -> &str {
        &self.0.message
    }

    #[inline]
    pub fn aborts_transaction(&self) -> bool {
        self.0.aborts_transaction()
    }

    #[inline]
    pub fn is_fatal(&self) -> bool {
        self.0.is_fatal()
    }

    // Builder proxy methods
    #[inline]
    pub fn detail(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.0.detail = Some(v.into());
        self
    }

    #[inline]
    pub fn hint(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.0.hint = Some(v.into());
        self
    }

    #[inline]
    pub fn context(mut self, v: impl Into<String>) -> Self {
        self.0.context = Some(v.into());
        self
    }

    #[inline]
    pub fn schema(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.0.schema_name = Some(v.into());
        self
    }

    #[inline]
    pub fn table(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.0.table_name = Some(v.into());
        self
    }

    #[inline]
    pub fn column(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.0.column_name = Some(v.into());
        self
    }

    #[inline]
    pub fn datatype(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.0.datatype_name = Some(v.into());
        self
    }

    #[inline]
    pub fn constraint(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.0.constraint_name = Some(v.into());
        self
    }

    #[inline]
    pub fn position(mut self, pos: u32) -> Self {
        self.0.position = Some(pos);
        self
    }

    // ========================================
    // Error identification API
    // ========================================

    /// Checks if this error has the specified SQLSTATE code.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use paro_common::error::{self, codes};
    ///
    /// let err = error::table_not_found("users");
    /// assert!(err.is(codes::syntax::UNDEFINED_TABLE));
    /// ```
    #[inline]
    pub fn is(&self, code: SqlState) -> bool {
        self.0.sqlstate.is(code)
    }

    /// Returns the error class category.
    ///
    /// This enables Rust pattern matching on error categories:
    ///
    /// ```ignore
    /// use paro_common::error::{ParoError, ErrorClass};
    ///
    /// fn handle_error(err: &ParoError) {
    ///     match err.error_class() {
    ///         ErrorClass::Syntax => { /* handle syntax error */ }
    ///         ErrorClass::Constraint => { /* handle constraint violation */ }
    ///         ErrorClass::Internal => { /* escalate internal error */ }
    ///         _ => { /* fallback handling */ }
    ///     }
    /// }
    /// ```
    #[inline]
    pub fn error_class(&self) -> super::ErrorClass {
        self.0.sqlstate.error_class()
    }

    /// Checks if this error belongs to the specified SQLSTATE class.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use paro_common::error;
    ///
    /// let err = error::table_not_found("users");
    /// assert!(err.is_class("42")); // Syntax class
    /// ```
    #[inline]
    pub fn is_class(&self, class: &str) -> bool {
        self.0.sqlstate.is_class(class)
    }

    // ========================================
    // Common predicate methods
    // ========================================

    /// Returns true if this is a syntax/access rule error (Class 42).
    #[inline]
    pub fn is_syntax_error(&self) -> bool {
        self.0.sqlstate.is_syntax_class()
    }

    /// Returns true if this is a data exception (Class 22).
    #[inline]
    pub fn is_data_error(&self) -> bool {
        self.0.sqlstate.is_data_exception()
    }

    /// Returns true if this is a constraint violation (Class 23).
    #[inline]
    pub fn is_constraint_error(&self) -> bool {
        self.0.sqlstate.is_constraint_violation()
    }

    /// Returns true if this is a transaction state error (Class 25).
    #[inline]
    pub fn is_transaction_error(&self) -> bool {
        self.0.sqlstate.is_transaction_error()
    }

    /// Returns true if this is an internal error (Class XX).
    #[inline]
    pub fn is_internal_error(&self) -> bool {
        self.0.sqlstate.is_internal_error()
    }

    /// Returns true if this is a feature not supported error (Class 0A).
    #[inline]
    pub fn is_feature_not_supported(&self) -> bool {
        self.0.sqlstate.is_feature_not_supported()
    }

    /// Returns true if this is a system error (Class 58).
    #[inline]
    pub fn is_system_error(&self) -> bool {
        self.0.sqlstate.is_system_error()
    }

    /// Returns true if this is a connection error (Class 08).
    #[inline]
    pub fn is_connection_error(&self) -> bool {
        self.0.sqlstate.is_connection_error()
    }

    // ========================================
    // Semantic predicates
    // ========================================

    /// Returns true if this error indicates a retryable condition.
    ///
    /// Retryable errors include serialization failures and deadlocks.
    #[inline]
    pub fn is_retryable(&self) -> bool {
        self.is(super::codes::rollback::SERIALIZATION_FAILURE)
            || self.is(super::codes::rollback::DEADLOCK_DETECTED)
    }

    /// Returns true if this error indicates the query was canceled.
    #[inline]
    pub fn is_query_canceled(&self) -> bool {
        self.is(super::codes::operator::QUERY_CANCELED)
            || self.is(super::codes::operator::STATEMENT_TIMEOUT)
    }

    /// Returns true if this indicates an undefined object (table, column, etc.).
    #[inline]
    pub fn is_undefined_object(&self) -> bool {
        self.is(super::codes::syntax::UNDEFINED_TABLE)
            || self.is(super::codes::syntax::UNDEFINED_COLUMN)
            || self.is(super::codes::syntax::UNDEFINED_FUNCTION)
            || self.is(super::codes::syntax::UNDEFINED_SCHEMA)
            || self.is(super::codes::syntax::UNDEFINED_DATABASE)
            || self.is(super::codes::syntax::UNDEFINED_OBJECT)
    }

    /// Returns true if this indicates a duplicate object (table, column, etc.).
    #[inline]
    pub fn is_duplicate_object(&self) -> bool {
        self.is(super::codes::syntax::DUPLICATE_TABLE)
            || self.is(super::codes::syntax::DUPLICATE_COLUMN)
            || self.is(super::codes::syntax::DUPLICATE_SCHEMA)
            || self.is(super::codes::syntax::DUPLICATE_DATABASE)
            || self.is(super::codes::syntax::DUPLICATE_OBJECT)
    }
}

impl std::fmt::Display for ParoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ParoError {}

impl From<ErrorData> for ParoError {
    fn from(data: ErrorData) -> Self {
        Self(Box::new(data))
    }
}

impl From<std::io::Error> for ParoError {
    fn from(err: std::io::Error) -> Self {
        super::io(err)
    }
}
