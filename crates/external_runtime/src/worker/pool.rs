// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::worker::lifecycle::WorkerLifecycleState;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerPoolPolicy {
    pub min_idle_workers: usize,
    pub target_idle_workers: usize,
    pub max_idle_workers: usize,
    pub idle_ttl_ms: u64,
    pub max_worker_age_ms: u64,
    pub scale_down_cooldown_ms: u64,
}

impl Default for WorkerPoolPolicy {
    fn default() -> Self {
        Self {
            min_idle_workers: 0,
            target_idle_workers: 1,
            max_idle_workers: 4,
            idle_ttl_ms: 30_000,
            max_worker_age_ms: 3_600_000,
            scale_down_cooldown_ms: 5_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WorkerShardKey {
    pub tenant_or_security_domain: String,
    pub env_artifact_id: String,
    pub routine_generation: u64,
    pub backend_kind: String,
    pub runtime_contract: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerRecord {
    pub worker_id: u64,
    pub shard_key: WorkerShardKey,
    pub state: WorkerLifecycleState,
    pub started_at_ms: u64,
    pub last_used_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerAcquireDecision {
    Reused(u64),
    SpawnRequired,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WorkerPoolError {
    #[error("worker {worker_id} not found")]
    WorkerNotFound { worker_id: u64 },
}

#[derive(Debug, Default)]
pub struct WorkerPool {
    next_worker_id: u64,
    workers: BTreeMap<u64, WorkerRecord>,
}

impl WorkerPoolPolicy {
    pub fn scale_down_count(&self, idle_workers: usize) -> usize {
        idle_workers.saturating_sub(self.target_idle_workers)
    }
}

impl WorkerPool {
    pub fn register_worker(
        &mut self,
        shard_key: WorkerShardKey,
        state: WorkerLifecycleState,
        now_ms: u64,
    ) -> u64 {
        let worker_id = self.next_worker_id + 1;
        self.next_worker_id = worker_id;
        self.workers.insert(
            worker_id,
            WorkerRecord {
                worker_id,
                shard_key,
                state,
                started_at_ms: now_ms,
                last_used_at_ms: now_ms,
            },
        );
        worker_id
    }

    pub fn acquire(&mut self, shard_key: &WorkerShardKey, now_ms: u64) -> WorkerAcquireDecision {
        if let Some(worker) = self
            .workers
            .values_mut()
            .find(|worker| worker.shard_key == *shard_key && worker.state.reusable())
        {
            worker.state = WorkerLifecycleState::Busy;
            worker.last_used_at_ms = now_ms;
            return WorkerAcquireDecision::Reused(worker.worker_id);
        }

        WorkerAcquireDecision::SpawnRequired
    }

    pub fn release(&mut self, worker_id: u64, now_ms: u64) -> Result<(), WorkerPoolError> {
        let worker = self
            .workers
            .get_mut(&worker_id)
            .ok_or(WorkerPoolError::WorkerNotFound { worker_id })?;
        worker.state = WorkerLifecycleState::Idle;
        worker.last_used_at_ms = now_ms;
        Ok(())
    }

    pub fn start_generation_drain(&mut self, shard_key: &WorkerShardKey) {
        for worker in self.workers.values_mut().filter(|worker| {
            worker.shard_key.tenant_or_security_domain == shard_key.tenant_or_security_domain
                && worker.shard_key.env_artifact_id == shard_key.env_artifact_id
                && worker.shard_key.backend_kind == shard_key.backend_kind
                && worker.shard_key.runtime_contract == shard_key.runtime_contract
                && worker.shard_key.routine_generation < shard_key.routine_generation
        }) {
            worker.state = WorkerLifecycleState::Draining;
        }
    }

    pub fn hard_retire_contract(&mut self, runtime_contract: &str) -> usize {
        let mut retired = 0;
        for worker in self.workers.values_mut() {
            if worker.shard_key.runtime_contract == runtime_contract {
                worker.state = WorkerLifecycleState::HardRetired;
                retired += 1;
            }
        }
        retired
    }

    pub fn scale_down_candidates(
        &self,
        shard_key: &WorkerShardKey,
        policy: &WorkerPoolPolicy,
        now_ms: u64,
    ) -> Vec<u64> {
        let mut idle = self
            .workers
            .values()
            .filter(|worker| {
                worker.shard_key == *shard_key
                    && worker.state == WorkerLifecycleState::Idle
                    && now_ms.saturating_sub(worker.last_used_at_ms) >= policy.idle_ttl_ms
            })
            .map(|worker| worker.worker_id)
            .collect::<Vec<_>>();
        idle.sort_unstable();
        idle.truncate(policy.scale_down_count(idle.len()));
        idle
    }

    pub fn retire(&mut self, worker_id: u64, hard: bool) -> Result<(), WorkerPoolError> {
        let worker = self
            .workers
            .get_mut(&worker_id)
            .ok_or(WorkerPoolError::WorkerNotFound { worker_id })?;
        worker.state = if hard {
            WorkerLifecycleState::HardRetired
        } else {
            WorkerLifecycleState::Retired
        };
        Ok(())
    }

    pub fn state(&self, worker_id: u64) -> Option<WorkerLifecycleState> {
        self.workers.get(&worker_id).map(|worker| worker.state)
    }
}
