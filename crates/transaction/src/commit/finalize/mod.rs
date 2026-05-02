// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Ordered commit-finalize stage facade.

mod stage;
mod worker;

pub use stage::{
    CommitFinalizeShutdownMode, CommitFinalizeStage, CommitFinalizeStageError,
    CommitFinalizeStageHooks, CommitFinalizeStageOptions, CommitFinalizeStageScheduleError,
    CommitFinalizeWaitError,
};
