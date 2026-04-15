// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Class 0A - Feature Not Supported
use crate::error::SqlState;
pub const FEATURE_NOT_SUPPORTED: SqlState = SqlState::new(*b"0A000");
