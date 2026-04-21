// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! External routine error constructors.

use std::borrow::Cow;

use crate::error::{codes, ErrorData, ParoError, Severity};

pub fn python_exception(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::external_routine::PYTHON_EXCEPTION,
        message,
    ))
}

pub fn contract_violation(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::external_routine::CONTRACT_VIOLATION,
        message,
    ))
}

pub fn worker_failure(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::external_routine::WORKER_FAILURE,
        message,
    ))
}

pub fn external_protocol_mismatch(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::external_routine::PROTOCOL_MISMATCH,
        message,
    ))
}

pub fn sandbox_violation(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::external_routine::SANDBOX_VIOLATION,
        message,
    ))
}

pub fn epoch_mismatch(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::external_routine::EPOCH_MISMATCH,
        message,
    ))
}

pub fn python_runtime_unavailable(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::external_routine::PYTHON_RUNTIME_UNAVAILABLE,
        message,
    ))
}

pub fn artifact_not_ready(message: impl Into<Cow<'static, str>>) -> ParoError {
    ParoError::new(ErrorData::new(
        Severity::Error,
        codes::external_routine::ARTIFACT_NOT_READY,
        message,
    ))
}
