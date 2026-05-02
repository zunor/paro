// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared transaction-core types.
//!
//! This crate is the dependency boundary for transaction hot-path scalar types
//! and opaque participant identifiers. It intentionally does not depend on
//! storage, catalog, session, execution, Arrow, Parquet, Tokio, logging, or
//! configuration crates.

mod cache;
mod sync;

pub mod active;
pub mod commit;
pub mod committed_txn_summary;
pub mod error;
pub mod lock_manager;
pub mod participant_state;
pub mod predicate;
pub mod read_dependency_index;
pub mod retention;
pub mod ssi_validator;
pub mod types;
pub mod view;
pub mod write_conflict_index;

pub use active::{
    ActiveRwTxnHandle, ActiveTxnAggregator, ActiveTxnHandle, ActiveTxnRegistry,
    ActiveTxnRegistryOptions, ActiveTxnSlotInfo, ActiveTxnState, ActiveTxnWatermarks,
};
#[cfg(feature = "runtime")]
pub use commit::{
    append_durable_commit_batch, AppendCommitError, AppendFailureCleanupBundle,
    AppendFailureRollbackAction, AppendFailureRollbackPlan, ApplyErrorSource, ApplyPhase,
    ApplyTargetDescriptor, ApplyTargetKind, ApplyTargetSet, BuildApplyRequest,
    CleanupBackpressureSnapshot, CommitAppendBatch, CommitApplyTarget, CommitBatchPolicy,
    CommitCompletionError, CommitCompletionHandle, CommitDrainBackpressure,
    CommitDrainBackpressureInput, CommitDrainOwner, CommitDrainWakeCallback, CommitDrainWakeHandle,
    CommitDrainWakePool, CommitDrainWakePoolMetrics, CommitDrainWakePoolOptions, CommitFinalFence,
    CommitFinalizeReservation, CommitFinalizeReservationFactory, CommitFinalizeReservationInput,
    CommitFinalizeShutdownMode, CommitFinalizeStage, CommitFinalizeStageError,
    CommitFinalizeStageHooks, CommitFinalizeStageOptions, CommitFinalizeStageScheduleError,
    CommitFinalizeWaitError, CommitFrontier, CommitFrontierHandle, CommitFrontierMetrics,
    CommitFrontierSnapshot, CommitJournal, CommitLifecycleAction, CommitQueue,
    CommitQueueBackpressure, CommitQueueEntry, CommitQueueError, CommitQueueMetrics,
    CommitQueueSnapshot, CommitQueueTicket, CommitRuntime, CommitRuntimeAck, CommitRuntimeAssembly,
    CommitRuntimeCommitOutcome, CommitRuntimeError, CommitRuntimeFailure, CommitRuntimeHealthSink,
    CommitRuntimePoison, CommitRuntimeRejection, CommitRuntimeSnapshot, DeferredPublishPlan,
    DrainInlinePolicy, DrainSignalReason, DurableAmbiguousCleanupBundle, DurableCommitJob,
    FenceBlockedBatch, JournalApplyError, JournalProtocolViolationKind, LockReleasePlan,
    PendingFenceGuard, PostApplyFinalizeAction, PostApplyFinalizePlan, PrePublishReleasePlan,
    PreparedApplyTarget, PreparedCommitJob, PublishCompletion, PublishCompletionFallbackAck,
    PublishFailure, PublishFailureCause, PublishFatalSink, PublishSubmission, PublishSubmitError,
    PublishWaitError, RecoveryPlaceholderRecordKind, RecoveryReplayCommit, RecoveryReplayError,
    RecoveryReplayEvent, RecoveryReplaySummary, RegistrationGate, RequiredPublishPlan,
    SequencedCommitJob, SequencedCommitJobStateError, SequencedCommitPostAppend,
    SummaryReservation, WriteConflictPlacementInput, WriteConflictReservation,
};
pub use commit::{
    AbortReason, CommandId, CommitAckPolicy, CommitBackpressureController, CommitBackpressureError,
    CommitBackpressureOptions, CommitBackpressureSnapshot, CommitDurableBatch,
    CommitFenceRejectReason, CommitParticipant, CommitPlan, CommitRecordVersionError,
    CommitRequest, CommitRequestValidationError, CommitSequencer, CommitSequencerAppendError,
    CommitSequencerError, CommitSequencerMetrics, CommitSequencerOptions,
    CommitSequencerOrderedBatch, CommitSequencerOrderedError, CommitSequencingPlan,
    CommittedRecord, CommittedRecordApplier, CommittedTxnRecord, DurableCommitHandle,
    DurableCommitHandleError, FrozenLockSet, FrozenReadSet, InFlightCommitBatch, IsolationLevel,
    MaintenanceRecord, OrderedCommitPlan, ParticipantDescriptor, ParticipantRole,
    PreparedCommitPart, PreparedParticipant, PublishResult, RejectedCommitPlan,
    RejectedOrderedCommit, SequencedCommit, SequencedCommitBatch, ValidationContext,
    COMMITTED_TXN_RECORD_VERSION, DEFAULT_MAX_GROUP_COMMIT_BATCH_SIZE,
    DEFAULT_MAX_GROUP_COMMIT_FENCE_US, DEFAULT_MAX_PARTICIPANT_APPLY_LAG,
    DEFAULT_MAX_UNPUBLISHED_COMMITS, MAINTENANCE_RECORD_VERSION, PARTICIPANT_DESCRIPTOR_VERSION,
};
pub use committed_txn_summary::{
    CommittedTxnConflict, CommittedTxnSummary, CommittedTxnSummaryError, CommittedTxnSummaryIndex,
    CommittedTxnSummaryIndexOptions, CommittedTxnSummaryStats, CompressedReadSetSummary,
    DEFAULT_COMMITTED_TXN_SUMMARY_SHARDS,
};
pub use error::{RegistryError, Result};
pub use lock_manager::{
    LockAcquireError, LockEscalationFailureAction, LockEscalationPolicy, LockManagerStats,
    LockMode, LockNamespace, LockRequest, LockResource, ShardedLockManager,
    ShardedLockManagerOptions, TxnLockSet,
};
pub use participant_state::{ParticipantStateRef, ParticipantStateSet, TxnParticipantState};
pub use predicate::{
    NormalizedPredicate, NormalizedPredicateRead, NormalizedPredicateTerm, PredicateAtom,
    PredicateBound, PredicateExpr, PredicateFallbackScope, PredicateNormalizer, PredicateValue,
};
pub use read_dependency_index::{
    ActiveReadConflict, ActiveWriteConflictEffects, IndexedReadTracker, ReadDependencyIndex,
    ReadDependencyIndexMark, ReadDependencyIndexOptions, ReadDependencyIndexStats,
    ReadDependencyRollback, SsiTxnState, DEFAULT_GLOBAL_READ_SET_BUDGET_BYTES,
    DEFAULT_PER_TXN_READ_SET_BUDGET_BYTES, DEFAULT_READ_DEPENDENCY_SHARDS,
};
pub use retention::{
    BackfillLease, CheckpointLease, DerivedLagLease, LayoutEpochLease, ReadSnapshotLease,
    ReadSnapshotLeaseOwner, ReadSnapshotLeaseTransferError, RetentionAggregator,
    RetentionLeaseInfo, RetentionLeaseKind, RetentionRegistry, RetentionRegistryOptions,
    RetentionWatermarks, WriteConflictLease,
};
pub use ssi_validator::{SsiValidationError, SsiValidationOutcome, SsiValidator};
pub use types::{
    CommitTs, DatabaseId, LayoutEpoch, ParticipantId, ParticipantKind, ReadTs, SnapshotId, TableId,
    TxnId, TxnResourceKey, WriterId, MAX_TRANSACTION_ID, TRANSACTION_ID_START,
};
pub use view::{
    validate_as_of_timestamp, AsOfTimestampError, ReadDependency, ReadRecorder, ReadSnapshot,
    ReadTrackerHandle, ReadTrackerSavepointMark, ReadTrackingPolicy, ReadWritePromotion,
    RecordingReadTracker, TransactionView,
};
pub use write_conflict_index::{
    ConflictIndexStats, ConflictMatch, ConflictWrite, WriteConflictIndex, WriteConflictIndexError,
    WriteConflictIndexOptions,
};
