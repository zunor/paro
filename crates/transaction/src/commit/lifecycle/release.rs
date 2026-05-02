// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Phase-1 lock and pre-publish release plans.

use std::fmt;

pub type CommitLifecycleAction = Box<dyn FnOnce() + Send + 'static>;

pub struct LockReleasePlan {
    action: Option<CommitLifecycleAction>,
}

impl LockReleasePlan {
    #[inline]
    pub const fn noop() -> Self {
        Self { action: None }
    }

    #[inline]
    pub fn new(action: impl FnOnce() + Send + 'static) -> Self {
        Self {
            action: Some(Box::new(action)),
        }
    }

    #[inline]
    pub fn apply(mut self) {
        if let Some(action) = self.action.take() {
            action();
        }
    }
}

impl fmt::Debug for LockReleasePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LockReleasePlan")
            .field("has_action", &self.action.is_some())
            .finish()
    }
}

impl Default for LockReleasePlan {
    fn default() -> Self {
        Self::noop()
    }
}

pub struct PrePublishReleasePlan {
    action: Option<CommitLifecycleAction>,
}

impl PrePublishReleasePlan {
    #[inline]
    pub const fn noop() -> Self {
        Self { action: None }
    }

    #[inline]
    pub fn new(action: impl FnOnce() + Send + 'static) -> Self {
        Self {
            action: Some(Box::new(action)),
        }
    }

    #[inline]
    pub fn apply(mut self) {
        if let Some(action) = self.action.take() {
            action();
        }
    }
}

impl fmt::Debug for PrePublishReleasePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrePublishReleasePlan")
            .field("has_action", &self.action.is_some())
            .finish()
    }
}

impl Default for PrePublishReleasePlan {
    fn default() -> Self {
        Self::noop()
    }
}
