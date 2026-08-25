// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Per-slot commit completion registry.

use super::super::{CommitAckPolicy, CommitCompletionHandle, DurableCommitHandle};
use super::{
    CommitCompletionError, CommitRuntimeAck, CommitRuntimeCommitOutcome, CommitRuntimeFailure,
    CommitRuntimeRejection,
};
use crate::sync::{Condvar, Mutex};
use crate::types::CommitTs;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone)]
enum CommitCompletionState {
    Queued,
    Durable {
        handle: DurableCommitHandle,
        ack_policy: CommitAckPolicy,
        publish_completed: bool,
    },
    PublishSubmitted {
        handle: DurableCommitHandle,
    },
    DurableOnlyAcked {
        handle: DurableCommitHandle,
    },
    Published {
        handle: DurableCommitHandle,
    },
    Rejected(CommitRuntimeRejection),
    Failed(CommitRuntimeFailure),
    AmbiguousCommitted(CommitRuntimeFailure),
}

#[derive(Default)]
pub(super) struct CommitCompletionRegistry {
    state: Mutex<CommitCompletionRegistryState>,
    next_slot_id: AtomicU64,
}

#[derive(Default)]
struct CommitCompletionRegistryState {
    slots: HashMap<u64, Arc<CommitCompletionSlot>>,
    commit_to_slot: HashMap<u64, u64>,
    slot_to_commit: HashMap<u64, u64>,
}

struct CommitCompletionSlot {
    state: Mutex<CommitCompletionState>,
    changed: Condvar,
}

impl CommitCompletionSlot {
    fn new(state: CommitCompletionState) -> Self {
        Self {
            state: Mutex::new(state),
            changed: Condvar::new(),
        }
    }

    fn set(&self, next: CommitCompletionState) {
        *self.state.lock() = next;
        self.changed.notify_all();
    }

    fn update(&self, update: impl FnOnce(CommitCompletionState) -> CommitCompletionState) {
        let mut guard = self.state.lock();
        let next = update(guard.clone());
        let notify = next.is_terminal();
        *guard = next;
        if notify {
            self.changed.notify_all();
        }
    }

    fn wait_terminal(&self) -> CommitCompletionState {
        let mut guard = self.state.lock();
        loop {
            if guard.is_terminal() {
                return guard.clone();
            }
            guard = self.changed.wait(guard);
        }
    }
}

impl CommitCompletionState {
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::DurableOnlyAcked { .. }
                | Self::Published { .. }
                | Self::Rejected(_)
                | Self::Failed(_)
                | Self::AmbiguousCommitted(_)
        )
    }
}

impl CommitCompletionRegistry {
    pub(super) fn allocate(&self) -> CommitCompletionHandle {
        let slot_id = self.next_slot_id.fetch_add(1, Ordering::AcqRel) + 1;
        self.state.lock().slots.insert(
            slot_id,
            Arc::new(CommitCompletionSlot::new(CommitCompletionState::Queued)),
        );
        CommitCompletionHandle { slot_id }
    }

    pub(super) fn mark_durable(
        &self,
        completion: CommitCompletionHandle,
        handle: DurableCommitHandle,
        ack_policy: CommitAckPolicy,
    ) {
        let slot = {
            let mut state = self.state.lock();
            let slot = state.slots.get(&completion.slot_id).cloned();
            if slot.is_some() {
                let commit_ts = handle.commit_ts().into_raw();
                state.commit_to_slot.insert(commit_ts, completion.slot_id);
                state.slot_to_commit.insert(completion.slot_id, commit_ts);
            }
            slot
        };
        if let Some(slot) = slot {
            slot.set(CommitCompletionState::Durable {
                handle,
                ack_policy,
                publish_completed: false,
            });
        }
    }

    pub(super) fn mark_publish_submitted(&self, commit_ts: CommitTs) {
        if let Some(slot) = self.slot_for_commit_ts(commit_ts) {
            slot.update(|current| match current {
                CommitCompletionState::Durable {
                    handle,
                    ack_policy,
                    publish_completed,
                } => match ack_policy {
                    CommitAckPolicy::DurableOnlyAsync => {
                        CommitCompletionState::DurableOnlyAcked { handle }
                    }
                    CommitAckPolicy::RequiredPublished if publish_completed => {
                        CommitCompletionState::Published { handle }
                    }
                    CommitAckPolicy::RequiredPublished => {
                        CommitCompletionState::PublishSubmitted { handle }
                    }
                },
                current => current,
            });
        }
    }

    pub(super) fn mark_published(&self, commit_ts: CommitTs) {
        if let Some(slot) = self.slot_for_commit_ts(commit_ts) {
            slot.update(|current| match current {
                CommitCompletionState::PublishSubmitted { handle } => {
                    CommitCompletionState::Published { handle }
                }
                CommitCompletionState::Durable {
                    handle, ack_policy, ..
                } => CommitCompletionState::Durable {
                    handle,
                    ack_policy,
                    publish_completed: true,
                },
                current => current,
            });
        }
    }

    pub(super) fn mark_rejected(
        &self,
        completion: CommitCompletionHandle,
        reason: CommitRuntimeRejection,
    ) {
        if let Some(slot) = self.slot_for_completion(completion) {
            slot.set(CommitCompletionState::Rejected(reason));
        }
    }

    pub(super) fn mark_failed(&self, commit_ts: CommitTs, failure: CommitRuntimeFailure) {
        if let Some(slot) = self.slot_for_commit_ts(commit_ts) {
            slot.set(CommitCompletionState::Failed(failure));
        }
    }

    pub(super) fn mark_failed_handle(
        &self,
        completion: CommitCompletionHandle,
        failure: CommitRuntimeFailure,
    ) {
        if let Some(slot) = self.slot_for_completion(completion) {
            slot.set(CommitCompletionState::Failed(failure));
        }
    }

    pub(super) fn mark_ambiguous(
        &self,
        completion: CommitCompletionHandle,
        failure: CommitRuntimeFailure,
    ) {
        if let Some(slot) = self.slot_for_completion(completion) {
            slot.set(CommitCompletionState::AmbiguousCommitted(failure));
        }
    }

    pub(super) fn mark_ambiguous_commit_ts(
        &self,
        commit_ts: CommitTs,
        failure: CommitRuntimeFailure,
    ) {
        if let Some(slot) = self.slot_for_commit_ts(commit_ts) {
            slot.set(CommitCompletionState::AmbiguousCommitted(failure));
        }
    }

    pub(super) fn wait(
        &self,
        completion: CommitCompletionHandle,
    ) -> Result<CommitRuntimeCommitOutcome, CommitCompletionError> {
        let Some(slot) = self.slot_for_completion(completion) else {
            return Err(CommitCompletionError::UnknownSlot(completion));
        };
        let terminal = slot.wait_terminal();
        self.remove_slot(completion);
        match terminal {
            CommitCompletionState::DurableOnlyAcked { handle } => Ok(CommitRuntimeCommitOutcome {
                commit_ts: handle.commit_ts(),
                handle,
                ack: CommitRuntimeAck::DurableOnly,
            }),
            CommitCompletionState::Published { handle } => Ok(CommitRuntimeCommitOutcome {
                commit_ts: handle.commit_ts(),
                handle,
                ack: CommitRuntimeAck::Published,
            }),
            CommitCompletionState::Rejected(reason) => Err(CommitCompletionError::Rejected(reason)),
            CommitCompletionState::Failed(failure) => Err(CommitCompletionError::Failed(failure)),
            CommitCompletionState::AmbiguousCommitted(failure) => {
                Err(CommitCompletionError::AmbiguousCommitted(failure))
            }
            CommitCompletionState::Queued
            | CommitCompletionState::Durable { .. }
            | CommitCompletionState::PublishSubmitted { .. } => {
                unreachable!("wait_terminal only returns terminal states")
            }
        }
    }

    #[cfg(test)]
    pub(super) fn slot_count(&self) -> usize {
        self.state.lock().slots.len()
    }

    fn slot_for_completion(
        &self,
        completion: CommitCompletionHandle,
    ) -> Option<Arc<CommitCompletionSlot>> {
        self.state.lock().slots.get(&completion.slot_id).cloned()
    }

    fn slot_for_commit_ts(&self, commit_ts: CommitTs) -> Option<Arc<CommitCompletionSlot>> {
        let state = self.state.lock();
        let slot_id = state.commit_to_slot.get(&commit_ts.into_raw()).copied()?;
        state.slots.get(&slot_id).cloned()
    }

    fn remove_slot(&self, completion: CommitCompletionHandle) {
        let mut state = self.state.lock();
        state.slots.remove(&completion.slot_id);
        if let Some(commit_ts) = state.slot_to_commit.remove(&completion.slot_id) {
            state.commit_to_slot.remove(&commit_ts);
        }
    }
}
