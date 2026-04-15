// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Resource-related error constructors (Class 53).

use crate::error::{codes, ErrorData, ParoError, Severity};
use std::borrow::Cow;

/// Out of memory.
pub fn out_of_memory(message: impl Into<Cow<'static, str>>) -> ParoError {
    let msg = message.into();
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::resource::OUT_OF_MEMORY,
        if msg.is_empty() {
            Cow::Borrowed("out of memory")
        } else {
            msg
        },
    ))
}

/// Disk full.
pub fn disk_full(message: impl Into<Cow<'static, str>>) -> ParoError {
    let msg = message.into();
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::resource::DISK_FULL,
        if msg.is_empty() {
            Cow::Borrowed("could not write to disk: disk full")
        } else {
            msg
        },
    ))
}

/// Too many connections.
pub fn too_many_connections() -> ParoError {
    ParoError::new(
        ErrorData::new(
            Severity::Fatal,
            codes::resource::TOO_MANY_CONNECTIONS,
            "sorry, too many clients already",
        )
        .hint("Close some connections and try again."),
    )
}

/// Configuration limit exceeded.
pub fn configuration_limit_exceeded(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::resource::CONFIGURATION_LIMIT_EXCEEDED,
        message,
    ))
}
