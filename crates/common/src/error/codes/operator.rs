// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Class 57 - Operator Intervention
use crate::error::SqlState;

pub const OPERATOR_INTERVENTION: SqlState = SqlState::new(*b"57000");
pub const QUERY_CANCELED: SqlState = SqlState::new(*b"57014");
pub const ADMIN_SHUTDOWN: SqlState = SqlState::new(*b"57P01");
pub const CRASH_SHUTDOWN: SqlState = SqlState::new(*b"57P02");
pub const CANNOT_CONNECT_NOW: SqlState = SqlState::new(*b"57P03");
pub const DATABASE_DROPPED: SqlState = SqlState::new(*b"57P04");
pub const IDLE_SESSION_TIMEOUT: SqlState = SqlState::new(*b"57P05");
