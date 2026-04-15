// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Syntax-related error constructors.

use crate::error::{codes, ErrorData, ParoError, Severity};
use std::borrow::Cow;

/// Syntax error.
pub fn syntax(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::syntax::SYNTAX_ERROR,
        message,
    ))
}

/// Syntax error with position.
pub fn syntax_at(message: impl Into<Cow<'static, str>>, position: u32) -> ParoError {
    ParoError::new(
        ErrorData::new(Severity::Error, codes::syntax::SYNTAX_ERROR, message).position(position),
    )
}

/// Create an error from an external parser.
pub fn from_parser(message: impl Into<Cow<'static, str>>) -> ParoError {
    syntax(message)
}

/// Create error from external parser with position.
pub fn from_parser_at(message: impl Into<Cow<'static, str>>, position: u32) -> ParoError {
    syntax_at(message, position)
}

/// Feature not yet implemented.
pub fn not_implemented(feature: impl AsRef<str>) -> ParoError {
    ParoError::new(
        ErrorData::new(
            Severity::Error,
            codes::feature::FEATURE_NOT_SUPPORTED,
            format!("{} is not yet implemented", feature.as_ref()),
        )
        .hint("This feature may be added in a future release."),
    )
}

/// Feature not supported.
pub fn not_supported(feature: impl AsRef<str>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::feature::FEATURE_NOT_SUPPORTED,
        format!("{} is not supported", feature.as_ref()),
    ))
}

/// Datatype mismatch.
pub fn type_mismatch(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::syntax::DATATYPE_MISMATCH,
        message,
    ))
}

/// Grouping error.
pub fn grouping_error(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::syntax::GROUPING_ERROR,
        message,
    ))
}

/// Windowing error.
pub fn windowing_error(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::syntax::WINDOWING_ERROR,
        message,
    ))
}

/// Insufficient privilege.
pub fn insufficient_privilege(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::syntax::INSUFFICIENT_PRIVILEGE,
        message,
    ))
}
