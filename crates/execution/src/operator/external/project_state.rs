// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;
use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::types::LogicalType;

use crate::operator::state::{GlobalOperatorState, OperatorState};

use super::result_cache::{
    QueryLocalResultCache, QueryLocalResultCacheKey, QueryLocalResultCacheStats,
};
use super::runtime_bridge::RuntimeBridgeResponse;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExternalProjectRuntimeStats {
    pub submissions: u64,
    pub blocked_submissions: u64,
    pub total_input_rows: u64,
    pub total_input_bytes: u64,
    pub total_output_rows: u64,
    pub total_output_bytes: u64,
    pub worker_acquire_time_us: u64,
    pub queue_wait_us: u64,
    pub kernel_time_us: u64,
    pub encode_decode_time_us: u64,
    pub data_plane_bytes: u64,
    pub warm_batches: u64,
    pub cold_batches: u64,
    pub retired_count: u64,
    pub peak_accumulation_bytes: u64,
    pub peak_ready_output_bytes: u64,
    pub max_submission_rows: u64,
    pub max_submission_bytes: u64,
}

#[derive(Debug)]
pub struct ExternalProjectSharedState {
    stats: Mutex<ExternalProjectRuntimeStats>,
    cache: Mutex<QueryLocalResultCache>,
}

impl ExternalProjectSharedState {
    pub fn new(cache_bytes_budget: u64) -> Self {
        Self {
            stats: Mutex::new(ExternalProjectRuntimeStats::default()),
            cache: Mutex::new(QueryLocalResultCache::new(cache_bytes_budget)),
        }
    }

    pub fn observe_accumulation_bytes(&self, accumulation_bytes: u64) {
        let mut stats = self.stats.lock();
        stats.peak_accumulation_bytes = stats.peak_accumulation_bytes.max(accumulation_bytes);
    }

    pub fn observe_ready_output_bytes(&self, ready_output_bytes: u64) {
        let mut stats = self.stats.lock();
        stats.peak_ready_output_bytes = stats.peak_ready_output_bytes.max(ready_output_bytes);
    }

    pub fn record_submission(
        &self,
        input_rows: usize,
        input_bytes: u64,
        blocked: bool,
        response: &RuntimeBridgeResponse,
    ) {
        let mut stats = self.stats.lock();
        stats.submissions = stats.submissions.saturating_add(1);
        if blocked {
            stats.blocked_submissions = stats.blocked_submissions.saturating_add(1);
        }
        stats.total_input_rows = stats.total_input_rows.saturating_add(input_rows as u64);
        stats.total_input_bytes = stats.total_input_bytes.saturating_add(input_bytes);
        stats.total_output_rows = stats
            .total_output_rows
            .saturating_add(response.metrics.output_rows);
        stats.total_output_bytes = stats
            .total_output_bytes
            .saturating_add(response.metrics.output_bytes);
        stats.worker_acquire_time_us = stats
            .worker_acquire_time_us
            .saturating_add(response.metrics.worker_acquire_time_us);
        stats.queue_wait_us = stats
            .queue_wait_us
            .saturating_add(response.metrics.queue_wait_us);
        stats.kernel_time_us = stats
            .kernel_time_us
            .saturating_add(response.metrics.kernel_time_us);
        stats.encode_decode_time_us = stats
            .encode_decode_time_us
            .saturating_add(response.metrics.encode_decode_time_us);
        stats.data_plane_bytes = stats
            .data_plane_bytes
            .saturating_add(response.metrics.data_plane_bytes);
        match response.metrics.warm_state {
            super::runtime_bridge::RuntimeWarmState::Warm => {
                stats.warm_batches = stats.warm_batches.saturating_add(1);
            }
            super::runtime_bridge::RuntimeWarmState::Cold => {
                stats.cold_batches = stats.cold_batches.saturating_add(1);
            }
        }
        stats.retired_count = stats
            .retired_count
            .saturating_add(response.metrics.retired_count);
        stats.max_submission_rows = stats.max_submission_rows.max(input_rows as u64);
        stats.max_submission_bytes = stats.max_submission_bytes.max(input_bytes);
    }

    pub fn runtime_stats(&self) -> ExternalProjectRuntimeStats {
        self.stats.lock().clone()
    }

    pub fn cache_lookup(&self, key: &QueryLocalResultCacheKey) -> Option<Chunk> {
        self.cache.lock().get(key)
    }

    pub fn cache_insert(&self, key: QueryLocalResultCacheKey, result: Chunk, bytes: u64) -> bool {
        self.cache.lock().insert(key, result, bytes)
    }

    pub fn cache_stats(&self) -> QueryLocalResultCacheStats {
        self.cache.lock().stats()
    }
}

#[derive(Debug)]
pub struct ExternalProjectGlobalState {
    pub shared: Arc<ExternalProjectSharedState>,
}

impl GlobalOperatorState for ExternalProjectGlobalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct InflightProjectBatch {
    pub input_batch: Chunk,
    pub input_bytes: u64,
    pub response: RuntimeBridgeResponse,
    pub cache_key: Option<QueryLocalResultCacheKey>,
    pub cache_candidate: bool,
}

#[derive(Debug)]
pub struct ExternalProjectState {
    pub accumulation: Chunk,
    pub accumulation_uses_runtime_allocator: bool,
    pub accumulation_bytes: u64,
    pub ready_output: VecDeque<Chunk>,
    pub ready_output_bytes: u64,
    pub inflight: Option<InflightProjectBatch>,
    pub current_input_staged: bool,
    pub next_batch_id: u64,
}

impl ExternalProjectState {
    pub fn new(
        input_types: &[LogicalType],
        allocator: Arc<dyn Allocator>,
    ) -> paro_common::error::Result<Self> {
        Ok(Self {
            accumulation: Chunk::try_init_empty(input_types, allocator)?,
            accumulation_uses_runtime_allocator: false,
            accumulation_bytes: 0,
            ready_output: VecDeque::new(),
            ready_output_bytes: 0,
            inflight: None,
            current_input_staged: false,
            next_batch_id: 1,
        })
    }
}

impl OperatorState for ExternalProjectState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
