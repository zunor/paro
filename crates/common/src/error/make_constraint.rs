//! Constraint violation error constructors.

use crate::error::{codes, ErrorData, ParoError, Severity};
use std::borrow::Cow;

/// Unique constraint violation.
pub fn unique_violation(constraint: impl AsRef<str>) -> ParoError {
    let name = constraint.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::constraint::UNIQUE_VIOLATION,
            format!(
                "duplicate key value violates unique constraint \"{}\"",
                name
            ),
        )
        .constraint(name.to_string()),
    )
}

/// Not-null constraint violation.
pub fn not_null_violation(column: impl AsRef<str>) -> ParoError {
    let name = column.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::constraint::NOT_NULL_VIOLATION,
            format!(
                "null value in column \"{}\" violates not-null constraint",
                name
            ),
        )
        .column(name.to_string()),
    )
}

/// Foreign key constraint violation.
pub fn foreign_key_violation(constraint: impl AsRef<str>) -> ParoError {
    let name = constraint.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::constraint::FOREIGN_KEY_VIOLATION,
            format!("violates foreign key constraint \"{}\"", name),
        )
        .constraint(name.to_string()),
    )
}

/// Check constraint violation.
pub fn check_violation(constraint: impl AsRef<str>) -> ParoError {
    let name = constraint.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::constraint::CHECK_VIOLATION,
            format!("new row violates check constraint \"{}\"", name),
        )
        .constraint(name.to_string()),
    )
}

/// Exclusion constraint violation.
pub fn exclusion_violation(constraint: impl AsRef<str>) -> ParoError {
    let name = constraint.as_ref();
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::constraint::EXCLUSION_VIOLATION,
            format!(
                "conflicting key value violates exclusion constraint \"{}\"",
                name
            ),
        )
        .constraint(name.to_string()),
    )
}

/// Restrict violation.
pub fn restrict_violation(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::constraint::RESTRICT_VIOLATION,
        message,
    ))
}
