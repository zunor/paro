// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Thin transaction commit facade and participant contracts.

#[cfg(feature = "runtime")]
mod apply_target;
mod atomic;
#[cfg(feature = "runtime")]
mod drain_wake;
#[cfg(feature = "runtime")]
mod durable_append;
mod durable_handle;
#[cfg(feature = "runtime")]
mod finalize;
mod frontier;
mod gate;
#[cfg(feature = "runtime")]
mod job;
#[cfg(feature = "runtime")]
mod lifecycle;
#[cfg(feature = "runtime")]
mod publish;
#[cfg(feature = "runtime")]
mod queue;
#[cfg(feature = "runtime")]
mod recovery;
mod request;
#[cfg(feature = "runtime")]
mod runtime;
mod sequencer;
#[cfg(test)]
mod tests;
mod txn_record;
mod types;

#[cfg(feature = "runtime")]
pub use apply_target::{
    ApplyTargetDescriptor, ApplyTargetKind, ApplyTargetSet, CommitApplyTarget, PreparedApplyTarget,
};
pub(crate) use atomic::fetch_max_relaxed;
#[cfg(feature = "runtime")]
pub use drain_wake::{
    CommitDrainWakeCallback, CommitDrainWakeHandle, CommitDrainWakePool,
    CommitDrainWakePoolMetrics, CommitDrainWakePoolOptions,
};
#[cfg(feature = "runtime")]
pub use durable_append::{
    append_durable_commit_batch, AppendCommitError, CommitAppendBatch, CommitJournal,
    JournalProtocolViolationKind,
};
pub use durable_handle::{CommitDurableBatch, DurableCommitHandle, DurableCommitHandleError};
#[cfg(feature = "runtime")]
pub use finalize::{
    CommitFinalizeShutdownMode, CommitFinalizeStage, CommitFinalizeStageError,
    CommitFinalizeStageHooks, CommitFinalizeStageOptions, CommitFinalizeStageScheduleError,
    CommitFinalizeWaitError,
};
pub use frontier::{
    CommitFrontier, CommitFrontierHandle, CommitFrontierMetrics, CommitFrontierSnapshot,
    PublishFailure, PublishFailureCause, PublishWaitError,
};
pub use gate::RegistrationGate;
#[cfg(feature = "runtime")]
pub use job::{
    AppendFailureCleanupBundle, CommitCompletionHandle, CommitFinalizeReservation,
    CommitFinalizeReservationInput, DeferredPublishPlan, DurableAmbiguousCleanupBundle,
    DurableCommitJob, PreparedCommitJob, SequencedCommitJob, SequencedCommitJobStateError,
    SequencedCommitPostAppend, SummaryReservation, WriteConflictPlacementInput,
    WriteConflictReservation,
};
#[cfg(feature = "runtime")]
pub use lifecycle::{
    AppendFailureRollbackAction, AppendFailureRollbackPlan, CommitLifecycleAction, LockReleasePlan,
    PostApplyFinalizeAction, PostApplyFinalizePlan, PrePublishReleasePlan,
};
#[cfg(feature = "runtime")]
pub use publish::{
    ApplyErrorSource, ApplyPhase, BuildApplyRequest, JournalApplyError, PublishCompletion,
    PublishCompletionFallbackAck, PublishFatalSink, PublishSubmission, PublishSubmitError,
    RequiredPublishPlan,
};
#[cfg(feature = "runtime")]
pub use queue::{
    CleanupBackpressureSnapshot, CommitBatchPolicy, CommitDrainBackpressure,
    CommitDrainBackpressureInput, CommitDrainOwner, CommitQueue, CommitQueueBackpressure,
    CommitQueueEntry, CommitQueueError, CommitQueueMetrics, CommitQueueSnapshot, CommitQueueTicket,
    DrainInlinePolicy, DrainSignalReason, FenceBlockedBatch, PendingFenceGuard,
};
#[cfg(feature = "runtime")]
pub use recovery::{
    RecoveryPlaceholderRecordKind, RecoveryReplayCommit, RecoveryReplayError, RecoveryReplayEvent,
    RecoveryReplaySummary,
};
pub use request::{
    CommitParticipant, CommitPlan, CommitRequest, PreparedCommitPart, PreparedParticipant,
    ValidationContext,
};
#[cfg(feature = "runtime")]
pub use runtime::{
    CommitCompletionError, CommitFinalFence, CommitFinalizeReservationFactory, CommitRuntime,
    CommitRuntimeAck, CommitRuntimeAssembly, CommitRuntimeCommitOutcome, CommitRuntimeError,
    CommitRuntimeFailure, CommitRuntimeHealthSink, CommitRuntimePoison, CommitRuntimeRejection,
    CommitRuntimeSnapshot,
};
pub use sequencer::{
    CommitBackpressureController, CommitBackpressureError, CommitBackpressureOptions,
    CommitBackpressureSnapshot, CommitFenceRejectReason, CommitSequencer,
    CommitSequencerAppendError, CommitSequencerError, CommitSequencerMetrics,
    CommitSequencerOptions, CommitSequencerOrderedBatch, CommitSequencerOrderedError,
    CommitSequencingPlan, InFlightAcceptedPlan, InFlightCommitBatch, OrderedCommitPlan,
    RejectedCommitPlan, RejectedOrderedCommit, SequencedCommit, SequencedCommitBatch,
};
pub use txn_record::{
    CommittedRecord, CommittedRecordApplier, CommittedTxnRecord, MaintenanceRecord, PublishResult,
};
pub use types::{
    AbortReason, CommandId, CommitAckPolicy, CommitRecordVersionError,
    CommitRequestValidationError, FrozenLockSet, FrozenReadSet, IsolationLevel,
    ParticipantDescriptor, ParticipantRole, COMMITTED_TXN_RECORD_VERSION,
    DEFAULT_MAX_GROUP_COMMIT_BATCH_SIZE, DEFAULT_MAX_GROUP_COMMIT_FENCE_US,
    DEFAULT_MAX_PARTICIPANT_APPLY_LAG, DEFAULT_MAX_UNPUBLISHED_COMMITS, MAINTENANCE_RECORD_VERSION,
    PARTICIPANT_DESCRIPTOR_VERSION,
};
