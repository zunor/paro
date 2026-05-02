// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermitBudget {
    pub global_external_permits: usize,
    pub per_shard_permits: usize,
    pub per_query_permits: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermitTicket {
    pub shard_key: String,
    pub query_id: u64,
}

#[derive(Debug, Default)]
pub struct PermitPool {
    budget: PermitBudget,
    global_in_use: usize,
    per_shard_in_use: BTreeMap<String, usize>,
    per_query_in_use: BTreeMap<u64, usize>,
}

impl PermitPool {
    pub fn new(budget: PermitBudget) -> Self {
        Self {
            budget,
            ..Self::default()
        }
    }

    pub fn try_acquire(
        &mut self,
        shard_key: impl Into<String>,
        query_id: u64,
    ) -> Option<PermitTicket> {
        let shard_key = shard_key.into();
        let shard_in_use = *self.per_shard_in_use.get(&shard_key).unwrap_or(&0);
        let query_in_use = *self.per_query_in_use.get(&query_id).unwrap_or(&0);
        if self.global_in_use >= self.budget.global_external_permits
            || shard_in_use >= self.budget.per_shard_permits
            || query_in_use >= self.budget.per_query_permits
        {
            return None;
        }

        self.global_in_use += 1;
        *self.per_shard_in_use.entry(shard_key.clone()).or_default() += 1;
        *self.per_query_in_use.entry(query_id).or_default() += 1;

        Some(PermitTicket {
            shard_key,
            query_id,
        })
    }

    pub fn release(&mut self, ticket: PermitTicket) {
        self.global_in_use = self.global_in_use.saturating_sub(1);
        decrement_entry(&mut self.per_shard_in_use, &ticket.shard_key);
        decrement_entry(&mut self.per_query_in_use, &ticket.query_id);
    }
}

impl Default for PermitBudget {
    fn default() -> Self {
        Self {
            global_external_permits: 64,
            per_shard_permits: 8,
            per_query_permits: 4,
        }
    }
}

fn decrement_entry<K: Ord + Clone>(map: &mut BTreeMap<K, usize>, key: &K) {
    if let Some(value) = map.get_mut(key) {
        *value = value.saturating_sub(1);
        if *value == 0 {
            map.remove(key);
        }
    }
}
