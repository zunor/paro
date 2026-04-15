// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Class 23 - Integrity Constraint Violation
use crate::error::SqlState;

pub const INTEGRITY_CONSTRAINT_VIOLATION: SqlState = SqlState::new(*b"23000");
pub const RESTRICT_VIOLATION: SqlState = SqlState::new(*b"23001");
pub const NOT_NULL_VIOLATION: SqlState = SqlState::new(*b"23502");
pub const FOREIGN_KEY_VIOLATION: SqlState = SqlState::new(*b"23503");
pub const UNIQUE_VIOLATION: SqlState = SqlState::new(*b"23505");
pub const CHECK_VIOLATION: SqlState = SqlState::new(*b"23514");
pub const EXCLUSION_VIOLATION: SqlState = SqlState::new(*b"23P01");
