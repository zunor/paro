//! Structured error data matching PostgreSQL's ErrorData.

use super::{Severity, SqlState};
use std::borrow::Cow;

/// Structured error data.
#[derive(Debug, Clone)]
pub struct ErrorData {
    pub severity: Severity,
    pub sqlstate: SqlState,
    pub message: Cow<'static, str>,

    pub detail: Option<Cow<'static, str>>,
    pub hint: Option<Cow<'static, str>>,
    pub context: Option<String>,

    pub schema_name: Option<Cow<'static, str>>,
    pub table_name: Option<Cow<'static, str>>,
    pub column_name: Option<Cow<'static, str>>,
    pub datatype_name: Option<Cow<'static, str>>,
    pub constraint_name: Option<Cow<'static, str>>,

    pub position: Option<u32>,
    pub internal_position: Option<u32>,
    pub internal_query: Option<String>,
}

impl ErrorData {
    /// Creates a new ErrorData with the minimum required fields.
    pub fn new(
        severity: Severity,
        sqlstate: SqlState,
        message: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self {
            severity,
            sqlstate,
            message: message.into(),
            detail: None,
            hint: None,
            context: None,
            schema_name: None,
            table_name: None,
            column_name: None,
            datatype_name: None,
            constraint_name: None,
            position: None,
            internal_position: None,
            internal_query: None,
        }
    }

    #[inline]
    pub fn detail(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.detail = Some(v.into());
        self
    }

    #[inline]
    pub fn hint(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.hint = Some(v.into());
        self
    }

    #[inline]
    pub fn context(mut self, v: impl Into<String>) -> Self {
        self.context = Some(v.into());
        self
    }

    #[inline]
    pub fn schema(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.schema_name = Some(v.into());
        self
    }

    #[inline]
    pub fn table(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.table_name = Some(v.into());
        self
    }

    #[inline]
    pub fn column(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.column_name = Some(v.into());
        self
    }

    #[inline]
    pub fn datatype(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.datatype_name = Some(v.into());
        self
    }

    #[inline]
    pub fn constraint(mut self, v: impl Into<Cow<'static, str>>) -> Self {
        self.constraint_name = Some(v.into());
        self
    }

    #[inline]
    pub fn position(mut self, pos: u32) -> Self {
        self.position = Some(pos);
        self
    }

    #[inline]
    pub fn aborts_transaction(&self) -> bool {
        self.severity.aborts_transaction()
    }

    #[inline]
    pub fn is_fatal(&self) -> bool {
        self.severity.is_fatal()
    }
}

impl std::fmt::Display for ErrorData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ErrorData {}

impl Default for ErrorData {
    fn default() -> Self {
        Self::new(Severity::Error, SqlState::default(), "")
    }
}
