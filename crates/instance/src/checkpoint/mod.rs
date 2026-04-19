// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Checkpoint ownership boundary for instance-side checkpoint orchestration,
//! snapshot loading, and writers.

pub mod artifact_gc;
pub mod checkpoint_gc;
pub mod coordinator;
pub mod manifest_store;
pub mod recovery;
pub mod retention;
pub mod runtime;
pub mod segments;
pub mod view;
pub mod writers;

pub use coordinator::{CheckpointCoordinator, CheckpointExecutionContext};
pub use recovery::CheckpointRecovery;
pub use retention::RetentionCoordinator;
pub use runtime::{
    frontier_from_summary, ApplyRequest, ExactPrefixTimeout, PublishedPrefixTracker,
    RecordWatermarks,
};
pub use view::{CheckpointCut, CheckpointView};
