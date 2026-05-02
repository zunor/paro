// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Publish submit/completion handoff types.

use crate::types::CommitTs;
use paro_journal::{ApplyCompletion, ApplyCompletionFallbackAck, ApplyFatalSink};

pub type PublishCompletion = ApplyCompletion;
pub type PublishCompletionFallbackAck = ApplyCompletionFallbackAck;
pub type PublishFatalSink = ApplyFatalSink;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishSubmission {
    pub commit_ts: CommitTs,
}
