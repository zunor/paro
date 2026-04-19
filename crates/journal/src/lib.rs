// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Binary journal codec and batched append queue.

mod appender;
mod apply_queue;
mod codec;
mod coordinator;
mod publish_frontier;
pub mod segments;
mod waiter;

pub use appender::{AppendResult, JournalAppender, JournalAppenderMetricsSnapshot, JournalSink};
pub use apply_queue::{
    ApplyRequest, ApplySubmitResult, JournalApplyMetricsSnapshot, JournalApplyRuntime,
    TabletApplyPart,
};
pub use codec::{
    decode_frame, encode_record, DecodedJournalFrame, JournalFrameHeader, JOURNAL_FRAME_HEADER_SIZE,
};
pub use coordinator::{
    CommitExecutionContext, JournalCoordinator, JournalFrontierSnapshot,
    MaintenanceExecutionContext,
};
pub use waiter::WaitMode;
