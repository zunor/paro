// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, VecDeque};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FairnessPolicy {
    pub round_robin_queries: bool,
    pub max_batches_per_query_turn: usize,
}

impl Default for FairnessPolicy {
    fn default() -> Self {
        Self {
            round_robin_queries: true,
            max_batches_per_query_turn: 1,
        }
    }
}

#[derive(Debug, Default)]
pub struct QueryTurnScheduler {
    queue: VecDeque<u64>,
    seen: BTreeMap<u64, usize>,
}

impl QueryTurnScheduler {
    pub fn push(&mut self, query_id: u64) {
        if self.seen.contains_key(&query_id) {
            return;
        }
        self.queue.push_back(query_id);
        self.seen.insert(query_id, 0);
    }

    pub fn next(&mut self, policy: &FairnessPolicy) -> Option<u64> {
        let query_id = self.queue.pop_front()?;
        let turn_count = self.seen.entry(query_id).or_default();
        *turn_count += 1;
        if policy.round_robin_queries && *turn_count < policy.max_batches_per_query_turn {
            self.queue.push_front(query_id);
        } else {
            self.queue.push_back(query_id);
            *turn_count = 0;
        }
        Some(query_id)
    }
}
