// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::ddl::DdlChangeRecord;
use crate::effect::{CleanupDescriptor, RuntimeTransitionDescriptor, StagedArtifactDescriptor};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogTxnOp {
    pub change: DdlChangeRecord,
    pub staged_artifacts: Vec<StagedArtifactDescriptor>,
    pub runtime_transitions: Vec<RuntimeTransitionDescriptor>,
    pub cleanups: Vec<CleanupDescriptor>,
}
