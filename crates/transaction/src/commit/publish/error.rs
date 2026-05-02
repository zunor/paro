// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Publish submit error classification.

use paro_journal::ApplyRuntimeError;
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishSubmitError {
    RuntimeUnavailable { message: Arc<str> },
    Fatal { message: Arc<str> },
}

impl PublishSubmitError {
    pub fn runtime_unavailable(message: impl Into<Arc<str>>) -> Self {
        Self::RuntimeUnavailable {
            message: message.into(),
        }
    }

    pub fn fatal(message: impl Into<Arc<str>>) -> Self {
        Self::Fatal {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::RuntimeUnavailable { message } | Self::Fatal { message } => message,
        }
    }
}

impl From<ApplyRuntimeError> for PublishSubmitError {
    fn from(value: ApplyRuntimeError) -> Self {
        match value {
            ApplyRuntimeError::RuntimeUnavailable { message } => {
                Self::RuntimeUnavailable { message }
            }
            ApplyRuntimeError::Fatal { message } => Self::Fatal { message },
        }
    }
}

impl fmt::Display for PublishSubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RuntimeUnavailable { message } => {
                write!(f, "publish submit runtime unavailable: {message}")
            }
            Self::Fatal { message } => write!(f, "publish submit fatal error: {message}"),
        }
    }
}

impl std::error::Error for PublishSubmitError {}
