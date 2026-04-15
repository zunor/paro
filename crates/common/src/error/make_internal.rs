//! Internal error constructors.

use crate::error::{codes, ErrorData, ParoError, Severity};
use std::borrow::Cow;

/// Internal error.
pub fn internal(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::internal::INTERNAL_ERROR,
        message,
    ))
}

/// Internal error with detail.
pub fn internal_detail(
    message: impl Into<Cow<'static, str>>,
    detail: impl Into<Cow<'static, str>>,
) -> ParoError {
    ParoError::new(
        ErrorData::new(Severity::Error, codes::internal::INTERNAL_ERROR, message).detail(detail),
    )
}

/// Create internal error from any std::error::Error.
pub fn from_std<E: std::error::Error>(err: E) -> ParoError {
    internal(err.to_string())
}

/// Data corrupted.
pub fn data_corrupted(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::internal::DATA_CORRUPTED,
        message,
    ))
}

/// Index corrupted.
pub fn index_corrupted(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::internal::INDEX_CORRUPTED,
        message,
    ))
}

/// Serialization error.
pub fn serialization_error(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::internal::SERIALIZATION_ERROR,
        message,
    ))
}

/// Panic-level internal error.
pub fn panic(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(
        ErrorData::new(Severity::Panic, codes::internal::INTERNAL_ERROR, message)
            .hint("This is a bug. Please report it."),
    )
}
