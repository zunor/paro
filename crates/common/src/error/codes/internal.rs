// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Class XX - Internal Error
use crate::error::SqlState;

pub const INTERNAL_ERROR: SqlState = SqlState::new(*b"XX000");
pub const DATA_CORRUPTED: SqlState = SqlState::new(*b"XX001");
pub const INDEX_CORRUPTED: SqlState = SqlState::new(*b"XX002");
pub const SERIALIZATION_ERROR: SqlState = SqlState::new(*b"XX003");
