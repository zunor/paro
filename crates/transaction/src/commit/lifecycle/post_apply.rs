// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Apply-after-publish transaction finalization plan.

use crate::commit::durable_handle::DurableCommitHandle;
use paro_journal::JournalApplyError;
use std::fmt;

pub type PostApplyFinalizeAction =
    Box<dyn FnOnce(&DurableCommitHandle) -> Result<(), JournalApplyError> + Send + 'static>;

pub struct PostApplyFinalizePlan {
    action: Option<PostApplyFinalizeAction>,
}

impl PostApplyFinalizePlan {
    #[inline]
    pub const fn noop() -> Self {
        Self { action: None }
    }

    #[inline]
    pub fn new(
        action: impl FnOnce(&DurableCommitHandle) -> Result<(), JournalApplyError> + Send + 'static,
    ) -> Self {
        Self {
            action: Some(Box::new(action)),
        }
    }

    #[inline]
    pub fn finalize_and_enqueue(
        mut self,
        handle: &DurableCommitHandle,
    ) -> Result<(), JournalApplyError> {
        if let Some(action) = self.action.take() {
            return action(handle);
        }
        Ok(())
    }
}

impl fmt::Debug for PostApplyFinalizePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostApplyFinalizePlan")
            .field("has_action", &self.action.is_some())
            .finish()
    }
}

impl Default for PostApplyFinalizePlan {
    fn default() -> Self {
        Self::noop()
    }
}
