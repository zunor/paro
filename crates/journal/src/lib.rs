// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Binary journal codec and batched append queue.

mod appender;
pub mod apply;
mod apply_queue;
mod codec;
mod publish_frontier;
mod runtime;
pub mod segments;
mod waiter;
pub mod wal;

pub use appender::{AppendResult, JournalAppender, JournalAppenderMetricsSnapshot, JournalSink};
pub use apply::{
    mutation_identities, mutation_identity_for_tablet, publish_committed_record, ApplyCompletion,
    ApplyCompletionFallbackAck, ApplyErrorSource, ApplyFatalSink, ApplyPhase, ApplyRequest,
    ApplyRuntimeError, ApplySubmitResult, JournalApplyError, JournalApplyMetricsSnapshot,
    JournalApplyRuntime, JournalPublicationObserver, MaintenanceApplyHandler, MutationIdentity,
    MutationKind, RecoveryPlaceholderRecordKind, TabletApplyPart, VisibilityPublisher, WaitMode,
};
pub use codec::{
    codec_size_calibration_sample_for_plan, codec_size_calibration_sample_for_record, decode_frame,
    encode_record, encoded_journal_record_size_upper_bound, encoded_size_upper_bound_for_plan,
    CodecSizeCalibrationSample, CodecSizeOverflow, DecodedJournalFrame, JournalFrameHeader,
    COMMIT_BATCH_BYTES_ESTIMATE_RATIO_CLAMP_MAX, COMMIT_BATCH_BYTES_ESTIMATE_RATIO_CLAMP_MIN,
    COMMIT_BATCH_BYTES_ESTIMATE_RATIO_EWMA_ALPHA, COMMIT_BATCH_BYTES_ESTIMATE_TYPICAL_MIN_RATIO,
    JOURNAL_FRAME_HEADER_SIZE,
};
pub use runtime::{JournalCoordinator, JournalFrontierSnapshot, MaintenanceAppendContext};
