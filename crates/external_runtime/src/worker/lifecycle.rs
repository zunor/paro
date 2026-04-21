// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerLifecycleState {
    Spawning,
    Warm,
    Busy,
    Idle,
    Draining,
    Retired,
    HardRetired,
}

impl WorkerLifecycleState {
    pub fn reusable(self) -> bool {
        matches!(
            self,
            WorkerLifecycleState::Warm | WorkerLifecycleState::Idle
        )
    }

    pub fn terminal(self) -> bool {
        matches!(
            self,
            WorkerLifecycleState::Retired | WorkerLifecycleState::HardRetired
        )
    }
}
