// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactLifecycleState {
    Pending,
    Ready,
    Draining,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactGcPolicy {
    pub grace_window_ms: u64,
    pub min_unused_age_ms: u64,
    pub sweep_interval_ms: u64,
    pub max_total_bytes: Option<u64>,
}

impl Default for ArtifactGcPolicy {
    fn default() -> Self {
        Self {
            grace_window_ms: 30_000,
            min_unused_age_ms: 300_000,
            sweep_interval_ms: 60_000,
            max_total_bytes: None,
        }
    }
}

impl ArtifactGcPolicy {
    pub fn can_delete(&self, state: ArtifactLifecycleState, last_used_age_ms: u64) -> bool {
        matches!(
            state,
            ArtifactLifecycleState::Draining | ArtifactLifecycleState::Deleted
        ) && last_used_age_ms >= self.min_unused_age_ms
    }
}
