// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! System-related error constructors.

use crate::error::{codes, ErrorData, ParoError, Severity};
use std::borrow::Cow;

/// IO error.
pub fn io(err: std::io::Error) -> ParoError {
    ParoError::new(
        ErrorData::new(Severity::Error, codes::system::IO_ERROR, err.to_string())
            .detail(format!("System error: {:?}", err.kind())),
    )
}

/// IO error with custom message.
pub fn io_error(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::system::IO_ERROR,
        message,
    ))
}

/// Query canceled by user request.
pub fn query_canceled() -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::operator::QUERY_CANCELED,
        "canceling statement due to user request",
    ))
}

/// Statement canceled because statement_timeout fired.
pub fn statement_timeout() -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::operator::QUERY_CANCELED,
        "canceling statement due to statement timeout",
    ))
}

/// Admin shutdown.
pub fn admin_shutdown() -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Fatal,
        codes::operator::ADMIN_SHUTDOWN,
        "terminating connection due to administrator command",
    ))
}

/// Cannot connect now.
pub fn cannot_connect_now() -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Fatal,
        codes::operator::CANNOT_CONNECT_NOW,
        "the database system is not yet accepting connections",
    ))
}

/// Connection failure.
pub fn connection_failure(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Fatal,
        codes::connection::CONNECTION_FAILURE,
        message,
    ))
}

/// Protocol violation.
pub fn protocol_violation(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Fatal,
        codes::connection::PROTOCOL_VIOLATION,
        message,
    ))
}

/// System error (generic).
pub fn system_error(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::system::SYSTEM_ERROR,
        message,
    ))
}
