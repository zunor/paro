// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Per-database commit admission health.

use parking_lot::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitAdmissionState {
    Open,
    BlockedRecovery,
    BlockedPoisoned,
}

impl CommitAdmissionState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::BlockedRecovery => "blocked_recovery",
            Self::BlockedPoisoned => "blocked_poisoned",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitHealthSnapshot {
    pub admission_state: CommitAdmissionState,
    pub admission_open: bool,
    pub poisoned: bool,
    pub poison_cause: Option<String>,
    pub first_blocked_commit_ts: Option<u64>,
}

impl Default for CommitHealthSnapshot {
    fn default() -> Self {
        Self {
            admission_state: CommitAdmissionState::Open,
            admission_open: true,
            poisoned: false,
            poison_cause: None,
            first_blocked_commit_ts: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct CommitHealth {
    inner: Mutex<CommitHealthInner>,
}

#[derive(Debug)]
struct CommitHealthInner {
    recovery_complete: bool,
    poison_cause: Option<String>,
}

impl Default for CommitHealthInner {
    fn default() -> Self {
        Self {
            recovery_complete: true,
            poison_cause: None,
        }
    }
}

impl CommitHealth {
    pub fn block_recovery(&self) {
        let mut guard = self.inner.lock();
        if guard.poison_cause.is_none() {
            guard.recovery_complete = false;
        }
    }

    pub fn complete_recovery(&self) {
        let mut guard = self.inner.lock();
        guard.recovery_complete = true;
        guard.poison_cause = None;
    }

    pub fn mark_poisoned(&self, cause: impl Into<String>) {
        let mut guard = self.inner.lock();
        if guard.poison_cause.is_none() {
            guard.poison_cause = Some(cause.into());
        }
    }

    pub fn is_open(&self) -> bool {
        let guard = self.inner.lock();
        guard.recovery_complete && guard.poison_cause.is_none()
    }

    pub fn detail(&self) -> String {
        let snapshot = self.snapshot(None, None);
        match snapshot.admission_state {
            CommitAdmissionState::Open => "commit admission open".to_string(),
            CommitAdmissionState::BlockedRecovery => {
                "commit admission blocked by recovery".to_string()
            }
            CommitAdmissionState::BlockedPoisoned => snapshot
                .poison_cause
                .unwrap_or_else(|| "commit runtime poisoned".to_string()),
        }
    }

    pub fn snapshot(
        &self,
        runtime_admission_open: Option<bool>,
        first_blocked_commit_ts: Option<u64>,
    ) -> CommitHealthSnapshot {
        let guard = self.inner.lock();
        if let Some(cause) = guard.poison_cause.clone() {
            return CommitHealthSnapshot {
                admission_state: CommitAdmissionState::BlockedPoisoned,
                admission_open: false,
                poisoned: true,
                poison_cause: Some(cause),
                first_blocked_commit_ts,
            };
        }
        if !guard.recovery_complete {
            return CommitHealthSnapshot {
                admission_state: CommitAdmissionState::BlockedRecovery,
                admission_open: false,
                poisoned: false,
                poison_cause: None,
                first_blocked_commit_ts: None,
            };
        }
        let runtime_open = runtime_admission_open.unwrap_or(true);
        CommitHealthSnapshot {
            admission_state: if runtime_open {
                CommitAdmissionState::Open
            } else {
                CommitAdmissionState::BlockedRecovery
            },
            admission_open: runtime_open,
            poisoned: false,
            poison_cause: None,
            first_blocked_commit_ts: None,
        }
    }
}
