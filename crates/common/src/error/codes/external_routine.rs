// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Class 38/39 - External Routine and Invocation Exceptions
use crate::error::SqlState;

pub const EXTERNAL_ROUTINE_EXCEPTION: SqlState = SqlState::new(*b"38000");
pub const PYTHON_EXCEPTION: SqlState = SqlState::new(*b"38001");

pub const EXTERNAL_ROUTINE_INVOCATION_EXCEPTION: SqlState = SqlState::new(*b"39000");
pub const CONTRACT_VIOLATION: SqlState = SqlState::new(*b"39001");
pub const WORKER_FAILURE: SqlState = SqlState::new(*b"39002");
pub const PROTOCOL_MISMATCH: SqlState = SqlState::new(*b"39P01");
pub const SANDBOX_VIOLATION: SqlState = SqlState::new(*b"39P02");
pub const EPOCH_MISMATCH: SqlState = SqlState::new(*b"39P03");
pub const PYTHON_RUNTIME_UNAVAILABLE: SqlState = SqlState::new(*b"39P04");
pub const ARTIFACT_NOT_READY: SqlState = SqlState::new(*b"39P05");
