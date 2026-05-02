// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Commit registration gate shared by queue drain and commit-finalize stage.

use super::finalize::{CommitFinalizeStageError, CommitFinalizeWaitError};
use crate::sync::{Condvar, Mutex};
use crate::types::CommitTs;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct RegistrationGate {
    registered_commit_ts: AtomicU64,
    wait_lock: Mutex<()>,
    wait: Condvar,
}

impl RegistrationGate {
    pub fn registered_commit_ts(&self) -> CommitTs {
        CommitTs::new(self.registered_commit_ts.load(Ordering::Acquire))
    }

    pub(crate) fn mark_registered(&self, commit_ts: CommitTs) {
        let mut current = self.registered_commit_ts.load(Ordering::Acquire);
        let candidate = commit_ts.into_raw();
        while candidate > current {
            match self.registered_commit_ts.compare_exchange_weak(
                current,
                candidate,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
        self.wait.notify_all();
    }

    pub(crate) fn wait_until_registered(
        &self,
        floor: CommitTs,
        poison_snapshot: impl Fn() -> Option<CommitFinalizeStageError>,
    ) -> Result<(), CommitFinalizeWaitError> {
        if self.registered_commit_ts().into_raw() >= floor.into_raw() {
            return Ok(());
        }
        let mut guard = self.wait_lock.lock();
        loop {
            if self.registered_commit_ts().into_raw() >= floor.into_raw() {
                return Ok(());
            }
            if let Some(error) = poison_snapshot() {
                return Err(CommitFinalizeWaitError::Poisoned(error));
            }
            guard = self.wait.wait(guard);
        }
    }

    pub(crate) fn notify_all(&self) {
        self.wait.notify_all();
    }
}
