// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Append-failure rollback plan.

use paro_common::error::Result as ParoResult;
use std::fmt;

pub type AppendFailureRollbackAction = Box<dyn FnOnce() -> ParoResult<()> + Send + 'static>;

pub struct AppendFailureRollbackPlan {
    action: Option<AppendFailureRollbackAction>,
}

impl AppendFailureRollbackPlan {
    #[inline]
    pub const fn noop() -> Self {
        Self { action: None }
    }

    #[inline]
    pub fn new(action: impl FnOnce() -> ParoResult<()> + Send + 'static) -> Self {
        Self {
            action: Some(Box::new(action)),
        }
    }

    #[inline]
    pub fn apply(mut self) -> ParoResult<()> {
        if let Some(action) = self.action.take() {
            return action();
        }
        Ok(())
    }
}

impl fmt::Debug for AppendFailureRollbackPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppendFailureRollbackPlan")
            .field("has_action", &self.action.is_some())
            .finish()
    }
}

impl Default for AppendFailureRollbackPlan {
    fn default() -> Self {
        Self::noop()
    }
}
