// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Apply-target DTOs ordered by commit lifecycle phase.

mod descriptor;
mod durable;
mod prepared;

pub use descriptor::{ApplyTargetDescriptor, ApplyTargetKind, ApplyTargetSet};
pub use durable::CommitApplyTarget;
pub use prepared::PreparedApplyTarget;
