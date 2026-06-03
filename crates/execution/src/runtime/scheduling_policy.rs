// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Scheduler policy data for ready work ordering.

use std::cmp::Ordering as CmpOrdering;
use std::time::Duration;

use crate::physical::properties::MemoryClass;
use crate::pipeline::graph::{PipelineGraph, PipelineId};

pub(crate) const RUNTIME_WAIT_TIMEOUT: Duration = Duration::from_millis(1);

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
    pub fn new(score: i64) -> Self {
        Self(score)
    }

    #[inline]
    pub fn score(self) -> i64 {
        self.0
    }
}

impl PipelineSchedulingPolicy {
    pub fn ready_priority_for_pipeline(
        &self,
        graph: &PipelineGraph,
        pipeline: PipelineId,
        dependency_unblocks: u32,
        ready_age_ticks: u32,
        available_memory: usize,
    ) -> PipelineReadyPriority {
        self.ready_priority(
            PipelineReadyEvent {
                pipeline,
                critical_path_distance: critical_path_distance(graph, pipeline),
                dependency_unblocks,
                releases_memory_bytes: pipeline_release_bytes(graph, pipeline),
                estimated_memory_bytes: pipeline_estimated_memory_bytes(graph, pipeline),
                ready_age_ticks,
            },
            available_memory,
        )
    }

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

fn critical_path_distance(graph: &PipelineGraph, pipeline: PipelineId) -> u32 {
    fn longest_downstream_path(
        graph: &PipelineGraph,
        pipeline_idx: usize,
        memo: &mut [Option<u32>],
    ) -> u32 {
        if let Some(distance) = memo[pipeline_idx] {
            return distance;
        }

        let mut best = 0u32;
        for dependency in graph
            .dependencies
            .iter()
            .filter(|dependency| dependency.producer.index() == pipeline_idx)
        {
            let consumer_idx = dependency.consumer.index();
            if consumer_idx >= memo.len() {
                continue;
            }
            best =
                best.max(1u32.saturating_add(longest_downstream_path(graph, consumer_idx, memo)));
        }
        memo[pipeline_idx] = Some(best);
        best
    }

    if pipeline.index() >= graph.pipelines.len() {
        return 0;
    }
    let mut memo = vec![None; graph.pipelines.len()];
    longest_downstream_path(graph, pipeline.index(), &mut memo)
}

fn pipeline_release_bytes(graph: &PipelineGraph, pipeline: PipelineId) -> usize {
    graph
        .pipeline(pipeline)
        .map(|spec| {
            if spec.properties.memory.class >= MemoryClass::Blocking {
                spec.properties.memory.preferred_grant as usize
            } else {
                0
            }
        })
        .unwrap_or(0)
}

fn pipeline_estimated_memory_bytes(graph: &PipelineGraph, pipeline: PipelineId) -> usize {
    graph
        .pipeline(pipeline)
        .map(|spec| {
            spec.properties
                .memory
                .per_task_grant
                .max(spec.properties.memory.min_grant) as usize
        })
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy)]
pub struct ReadyEntry<T> {
    pub priority: PipelineReadyPriority,
    pub seq: u64,
    pub payload: T,
}

impl<T: PartialEq> PartialEq for ReadyEntry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.seq == other.seq && self.payload == other.payload
    }
}

impl<T: Eq> Eq for ReadyEntry<T> {}

impl<T: Eq> PartialOrd for ReadyEntry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl<T: Eq> Ord for ReadyEntry<T> {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

#[cfg(test)]
mod tests {
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
}
