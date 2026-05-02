// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Opaque lifecycle actions consumed by the commit runtime.

mod post_apply;
mod release;
mod rollback;

pub use post_apply::{PostApplyFinalizeAction, PostApplyFinalizePlan};
pub use release::{CommitLifecycleAction, LockReleasePlan, PrePublishReleasePlan};
pub use rollback::{AppendFailureRollbackAction, AppendFailureRollbackPlan};
