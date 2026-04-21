// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmissionState {
    Submitted,
    Started,
    Finished,
    Failed,
    Cancelled,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SubmissionStateError {
    #[error("submission cannot transition from {from:?} to {to:?}")]
    InvalidTransition {
        from: SubmissionState,
        to: SubmissionState,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionLifecycle {
    pub state: SubmissionState,
    pub retry_count: usize,
}

impl Default for SubmissionLifecycle {
    fn default() -> Self {
        Self {
            state: SubmissionState::Submitted,
            retry_count: 0,
        }
    }
}

impl SubmissionLifecycle {
    pub fn transition_to(&mut self, next: SubmissionState) -> Result<(), SubmissionStateError> {
        let allowed = matches!(
            (self.state, next),
            (SubmissionState::Submitted, SubmissionState::Started)
                | (SubmissionState::Submitted, SubmissionState::Cancelled)
                | (SubmissionState::Submitted, SubmissionState::Failed)
                | (SubmissionState::Started, SubmissionState::Finished)
                | (SubmissionState::Started, SubmissionState::Failed)
                | (SubmissionState::Started, SubmissionState::Cancelled)
        );
        if !allowed {
            return Err(SubmissionStateError::InvalidTransition {
                from: self.state,
                to: next,
            });
        }
        self.state = next;
        Ok(())
    }

    pub fn can_retry(&self, has_side_effects: bool) -> bool {
        !has_side_effects && self.state == SubmissionState::Submitted
    }

    pub fn record_retry(&mut self) {
        self.retry_count += 1;
    }
}
