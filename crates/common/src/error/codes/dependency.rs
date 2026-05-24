// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::error::SqlState;

pub const DEPENDENT_OBJECTS_STILL_EXIST: SqlState = SqlState::new(*b"2BP01");
