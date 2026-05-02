// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Commit runtime construction inputs.

use super::super::{
    CleanupBackpressureSnapshot, CommitBatchPolicy, CommitDrainWakeHandle,
    CommitFinalizeReservation, CommitFinalizeReservationInput, CommitFrontier, CommitJournal,
    CommitSequencer, CommitSequencingPlan, InFlightCommitBatch,
};
use super::CommitRuntimePoison;
use crate::types::CommitTs;
use paro_journal::JournalApplyRuntime;
use std::fmt;
use std::sync::Arc;

pub type CommitFinalFence = Arc<
    dyn Fn(
            &CommitSequencingPlan,
            &InFlightCommitBatch,
        ) -> Option<super::super::CommitFenceRejectReason>
        + Send
        + Sync
        + 'static,
>;

pub type CommitFinalizeReservationFactory = Arc<
    dyn Fn(CommitTs, &CommitFinalizeReservationInput) -> CommitFinalizeReservation
        + Send
        + Sync
        + 'static,
>;

pub type CommitRuntimeHealthSink = Arc<dyn Fn(CommitRuntimePoison) + Send + Sync + 'static>;

pub struct CommitRuntimeAssembly {
    pub journal: Arc<dyn CommitJournal>,
    pub apply_runtime: Arc<JournalApplyRuntime>,
    pub policy: CommitBatchPolicy,
    pub sequencer: Arc<CommitSequencer>,
    pub frontier: Arc<CommitFrontier>,
    pub reservation_factory: CommitFinalizeReservationFactory,
    pub final_fence: CommitFinalFence,
    pub cleanup_snapshot: Arc<dyn Fn() -> CleanupBackpressureSnapshot + Send + Sync + 'static>,
    pub wake_handle: Option<CommitDrainWakeHandle>,
    pub health_sink: Option<CommitRuntimeHealthSink>,
}

impl CommitRuntimeAssembly {
    #[cfg(test)]
    pub(super) fn for_tests(
        journal: Arc<dyn CommitJournal>,
        apply_runtime: Arc<JournalApplyRuntime>,
    ) -> Self {
        Self {
            journal,
            apply_runtime,
            policy: CommitBatchPolicy::default(),
            sequencer: Arc::new(CommitSequencer::default()),
            frontier: Arc::new(CommitFrontier::new()),
            reservation_factory: Arc::new(|_, _| CommitFinalizeReservation::default()),
            final_fence: Arc::new(|_, _| None),
            cleanup_snapshot: Arc::new(CleanupBackpressureSnapshot::default),
            wake_handle: None,
            health_sink: None,
        }
    }
}

impl fmt::Debug for CommitRuntimeAssembly {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommitRuntimeAssembly")
            .field("policy", &self.policy)
            .field("sequencer", &self.sequencer)
            .field("frontier", &self.frontier)
            .finish_non_exhaustive()
    }
}
