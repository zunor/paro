// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Cleanup protocol for runtime-owned breaker resources.

use std::sync::atomic::{AtomicU8, Ordering};

use paro_common::error::Result;
use paro_context::StatementCancelReason;

use crate::runtime::context::{OperatorCleanupContext, QueryErrorId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupReason {
    Finished,
    Cancelled(StatementCancelReason),
    Failed(QueryErrorId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupStatus {
    Live,
    Finished,
    Cancelled,
    Failed,
}

impl CleanupStatus {
    const fn code(self) -> u8 {
        match self {
            Self::Live => 0,
            Self::Finished => 1,
            Self::Cancelled => 2,
            Self::Failed => 3,
        }
    }

    const fn from_code(code: u8) -> Self {
        match code {
            1 => Self::Finished,
            2 => Self::Cancelled,
            3 => Self::Failed,
            _ => Self::Live,
        }
    }
}

impl From<CleanupReason> for CleanupStatus {
    fn from(reason: CleanupReason) -> Self {
        match reason {
            CleanupReason::Finished => Self::Finished,
            CleanupReason::Cancelled(_) => Self::Cancelled,
            CleanupReason::Failed(_) => Self::Failed,
        }
    }
}

pub trait RuntimeCleanup: Send + Sync + std::fmt::Debug {
    fn cleanup(&self, ctx: &mut OperatorCleanupContext, reason: CleanupReason) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct CleanupState {
    status: AtomicU8,
}

impl CleanupState {
    #[inline]
    pub fn status(&self) -> CleanupStatus {
        CleanupStatus::from_code(self.status.load(Ordering::Acquire))
    }

    #[inline]
    pub fn is_cleaned(&self) -> bool {
        self.status() != CleanupStatus::Live
    }

    pub fn mark(&self, reason: CleanupReason) -> bool {
        self.status
            .compare_exchange(
                CleanupStatus::Live.code(),
                CleanupStatus::from(reason).code(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_state_records_first_reason_and_stays_idempotent() {
        let state = CleanupState::default();

        assert_eq!(state.status(), CleanupStatus::Live);
        assert!(state.mark(CleanupReason::Finished));
        assert_eq!(state.status(), CleanupStatus::Finished);
        assert!(!state.mark(CleanupReason::Cancelled(StatementCancelReason::UserRequest)));
        assert_eq!(state.status(), CleanupStatus::Finished);
    }
}
