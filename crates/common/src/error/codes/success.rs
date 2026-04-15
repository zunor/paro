// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Class 00 - Successful Completion
use crate::error::SqlState;
pub const SUCCESSFUL_COMPLETION: SqlState = SqlState::new(*b"00000");
