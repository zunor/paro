// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::effect::{CleanupDescriptor, RuntimeTransitionDescriptor, StagedArtifactDescriptor};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplyDescriptor {
    PublishStagedArtifact(StagedArtifactDescriptor),
    RuntimeTransition(RuntimeTransitionDescriptor),
    Cleanup(CleanupDescriptor),
}
