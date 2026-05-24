// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Scheduler policy data for ready pipeline ordering and wake coalescing.

use std::collections::HashSet;

use super::context::{PendingWakeRegistration, WakeKey};
use crate::pipeline::graph::PipelineId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineSchedulingPolicy {
    pub ready_queue: ReadyQueuePolicy,
    pub wake_storm: WakeStormPolicy,
    pub fairness: FairnessPolicy,
}

impl Default for PipelineSchedulingPolicy {
    fn default() -> Self {
        Self {
            ready_queue: ReadyQueuePolicy::MemoryAwareCriticalPath,
            wake_storm: WakeStormPolicy::CoalesceByWakeKey,
            fairness: FairnessPolicy::AgeReadyPipelines,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadyQueuePolicy {
    Fifo,
    MemoryAwareCriticalPath,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeStormPolicy {
    WakeAll,
    CoalesceByWakeKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FairnessPolicy {
    None,
    AgeReadyPipelines,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineReadyEvent {
    pub pipeline: PipelineId,
    pub critical_path_distance: u32,
    pub dependency_unblocks: u32,
    pub releases_memory_bytes: usize,
    pub estimated_memory_bytes: usize,
    pub ready_age_ticks: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PipelineReadyPriority(i64);

impl PipelineReadyPriority {
    #[inline]
    pub fn score(self) -> i64 {
        self.0
    }
}

impl PipelineSchedulingPolicy {
    pub fn ready_priority(
        &self,
        event: PipelineReadyEvent,
        available_memory: usize,
    ) -> PipelineReadyPriority {
        match self.ready_queue {
            ReadyQueuePolicy::Fifo => PipelineReadyPriority(event.ready_age_ticks as i64),
            ReadyQueuePolicy::MemoryAwareCriticalPath => {
                let memory_fit_bonus = if event.estimated_memory_bytes <= available_memory {
                    2_000
                } else {
                    -2_000
                };
                let release_bonus = (event.releases_memory_bytes / (64 * 1024)).min(10_000) as i64;
                let fairness_bonus = match self.fairness {
                    FairnessPolicy::None => 0,
                    FairnessPolicy::AgeReadyPipelines => event.ready_age_ticks as i64,
                };
                PipelineReadyPriority(
                    memory_fit_bonus
                        + release_bonus
                        + event.critical_path_distance as i64 * 100
                        + event.dependency_unblocks as i64 * 50
                        + fairness_bonus,
                )
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct WakeCoalescer {
    seen: HashSet<WakeKey>,
}

impl WakeCoalescer {
    pub fn insert(&mut self, registration: PendingWakeRegistration) -> bool {
        self.seen.insert(registration.key())
    }

    pub fn clear(&mut self) {
        self.seen.clear();
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.seen.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::{
        OperatorWakeScope, PipelineTaskId, WakeGeneration, WakeSource, WakeToken,
    };

    use super::*;

    #[test]
    fn memory_aware_policy_prioritizes_release_and_critical_path() {
        let policy = PipelineSchedulingPolicy::default();
        let short = PipelineReadyEvent {
            pipeline: PipelineId::new(0),
            critical_path_distance: 1,
            dependency_unblocks: 0,
            releases_memory_bytes: 0,
            estimated_memory_bytes: 4 * 1024 * 1024,
            ready_age_ticks: 1,
        };
        let release = PipelineReadyEvent {
            pipeline: PipelineId::new(1),
            critical_path_distance: 4,
            dependency_unblocks: 2,
            releases_memory_bytes: 512 * 1024,
            estimated_memory_bytes: 16 * 1024,
            ready_age_ticks: 1,
        };

        assert!(
            policy.ready_priority(release, 128 * 1024).score()
                > policy.ready_priority(short, 128 * 1024).score()
        );
    }

    #[test]
    fn wake_coalescer_deduplicates_same_source_token_generation() {
        let wake = OperatorWakeScope {
            task_id: PipelineTaskId(1),
            generation: WakeGeneration(9),
        };
        let registration = wake.register(WakeSource::Memory, WakeToken(42));
        let mut coalescer = WakeCoalescer::default();

        assert!(coalescer.insert(registration));
        assert!(!coalescer.insert(registration));
        assert_eq!(coalescer.len(), 1);

        let next = OperatorWakeScope {
            task_id: PipelineTaskId(2),
            generation: WakeGeneration(10),
        }
        .register(WakeSource::Memory, WakeToken(42));
        assert!(coalescer.insert(next));
    }
}
