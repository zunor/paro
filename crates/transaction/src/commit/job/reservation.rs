// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Commit-finalize reservations built after a commit timestamp is accepted.

use super::super::{CommitPlan, FrozenReadSet};
use crate::types::{ReadTs, TxnId};
use crate::LockResource;
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteConflictPlacementInput {
    pub shard_ids: Arc<[u32]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitFinalizeReservationInput {
    pub txn_id: TxnId,
    pub read_ts: ReadTs,
    pub write_set: Vec<LockResource>,
    pub wci_placement_input: WriteConflictPlacementInput,
    pub frozen_read_set: FrozenReadSet,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteConflictReservation {
    pub slot_id: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SummaryReservation {
    pub slot_id: u64,
}

type CommitFinalizeReservationAction = Box<dyn FnOnce() + Send + 'static>;

#[derive(Default)]
pub struct CommitFinalizeReservation {
    pub write_conflict: WriteConflictReservation,
    pub summary: SummaryReservation,
    registration_action: Option<CommitFinalizeReservationAction>,
    release_action: Option<CommitFinalizeReservationAction>,
}

impl CommitFinalizeReservation {
    pub fn new(
        write_conflict: WriteConflictReservation,
        summary: SummaryReservation,
        registration_action: impl FnOnce() + Send + 'static,
        release_action: impl FnOnce() + Send + 'static,
    ) -> Self {
        Self {
            write_conflict,
            summary,
            registration_action: Some(Box::new(registration_action)),
            release_action: Some(Box::new(release_action)),
        }
    }

    #[inline]
    pub fn apply(mut self) {
        self.release_action.take();
        if let Some(action) = self.registration_action.take() {
            action();
        }
    }

    #[inline]
    pub fn release(mut self) {
        self.registration_action.take();
        if let Some(action) = self.release_action.take() {
            action();
        }
    }
}

impl fmt::Debug for CommitFinalizeReservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommitFinalizeReservation")
            .field("write_conflict", &self.write_conflict)
            .field("summary", &self.summary)
            .field(
                "has_registration_action",
                &self.registration_action.is_some(),
            )
            .field("has_release_action", &self.release_action.is_some())
            .finish()
    }
}

impl Drop for CommitFinalizeReservation {
    fn drop(&mut self) {
        debug_assert!(
            self.registration_action.is_none() && self.release_action.is_none(),
            "CommitFinalizeReservation dropped without apply() or release()"
        );
    }
}

impl From<CommitPlan> for CommitFinalizeReservationInput {
    fn from(plan: CommitPlan) -> Self {
        Self {
            txn_id: plan.txn_id,
            read_ts: plan.read_ts,
            write_set: Vec::new(),
            wci_placement_input: WriteConflictPlacementInput::default(),
            frozen_read_set: plan.frozen_read_set,
        }
    }
}
