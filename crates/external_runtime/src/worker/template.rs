// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::worker::pool::WorkerShardKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TemplateStrategy {
    Disabled,
    ForkTemplate,
    SnapshotRestore,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerTemplate {
    pub shard_key: WorkerShardKey,
    pub strategy: TemplateStrategy,
    pub template_epoch: u64,
    pub ready: bool,
}

#[derive(Debug, Default)]
pub struct WorkerTemplateRegistry {
    entries: BTreeMap<WorkerShardKey, WorkerTemplate>,
}

impl WorkerTemplateRegistry {
    pub fn insert(&mut self, template: WorkerTemplate) {
        self.entries.insert(template.shard_key.clone(), template);
    }

    pub fn get(&self, shard_key: &WorkerShardKey) -> Option<&WorkerTemplate> {
        self.entries.get(shard_key)
    }

    pub fn contains(&self, shard_key: &WorkerShardKey) -> bool {
        self.entries.contains_key(shard_key)
    }

    pub fn remove(&mut self, shard_key: &WorkerShardKey) -> Option<WorkerTemplate> {
        self.entries.remove(shard_key)
    }
}
