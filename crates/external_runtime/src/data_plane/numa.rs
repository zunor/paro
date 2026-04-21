// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NumaShardPolicy {
    pub enabled: bool,
    pub prefer_node_local_workers: bool,
    pub arena_node_count: usize,
}

impl Default for NumaShardPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            prefer_node_local_workers: true,
            arena_node_count: 1,
        }
    }
}

impl NumaShardPolicy {
    pub fn assign_node(&self, shard_hash: u64) -> usize {
        if !self.enabled || self.arena_node_count <= 1 {
            0
        } else {
            (shard_hash as usize) % self.arena_node_count
        }
    }
}
