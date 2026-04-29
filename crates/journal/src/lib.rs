// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Binary journal codec and batched append queue.

mod appender;
pub mod apply;
mod apply_queue;
mod codec;
mod coordinator;
mod publish_frontier;
pub mod segments;
mod waiter;
pub mod wal;

pub use appender::{AppendResult, JournalAppender, JournalAppenderMetricsSnapshot, JournalSink};
pub use apply::{
    mutation_identities, mutation_identity_for_tablet, publish_committed_record, ApplyRequest,
    ApplySubmitResult, JournalApplyMetricsSnapshot, JournalApplyRuntime, MaintenanceApplyHandler,
    MutationIdentity, MutationKind, TabletApplyPart, VisibilityPublisher, WaitMode,
};
pub use codec::{
    decode_frame, encode_record, DecodedJournalFrame, JournalFrameHeader, JOURNAL_FRAME_HEADER_SIZE,
};
pub use coordinator::{JournalCoordinator, JournalFrontierSnapshot, MaintenanceAppendContext};
