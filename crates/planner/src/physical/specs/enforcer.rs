// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Typed physical property enforcers.

use std::collections::BTreeSet;

use crate::physical::identity::{BaseRelationId, MutationBarrierId, SnapshotId};

/// A correctness barrier that completely materializes DML input before the
/// first external write side effect is allowed to run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationInputSpoolSpec {
    pub barrier: MutationBarrierId,
    pub targets: BTreeSet<BaseRelationId>,
    pub snapshot: SnapshotId,
}
