// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;
use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::types::LogicalType;

use crate::operator::state::{
    GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState,
};

use super::runtime_bridge::{RuntimeBridgeResponse, RuntimeWarmState};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalTableFlowControl {
    pub max_inflight_output_batches: usize,
    pub max_inflight_output_bytes: u64,
    pub credit_granularity_bytes: u64,
}

impl Default for ExternalTableFlowControl {
    fn default() -> Self {
        Self {
            max_inflight_output_batches: 16,
            max_inflight_output_bytes: 4 * 1024 * 1024,
            credit_granularity_bytes: 64 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableOutputBatch {
    pub chunk: Chunk,
    pub bytes: u64,
    pub partition_id: u64,
    pub partition_end: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExternalTableRuntimeStats {
    pub submissions: u64,
    pub blocked_submissions: u64,
    pub promoted_backlog_batches: u64,
    pub total_input_rows: u64,
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
    pub peak_visible_output_bytes: u64,
    pub peak_visible_output_batches: u64,
}

#[derive(Debug)]
pub struct ExternalTableSharedState {
    flow_control: ExternalTableFlowControl,
    visible_queue: Mutex<VecDeque<TableOutputBatch>>,
    deferred_backlog: Mutex<VecDeque<TableOutputBatch>>,
    visible_output_bytes: Mutex<u64>,
    finalized: Mutex<bool>,
    stats: Mutex<ExternalTableRuntimeStats>,
}

impl ExternalTableSharedState {
    pub fn new(flow_control: ExternalTableFlowControl) -> Self {
        Self {
            flow_control,
            visible_queue: Mutex::new(VecDeque::new()),
            deferred_backlog: Mutex::new(VecDeque::new()),
            visible_output_bytes: Mutex::new(0),
            finalized: Mutex::new(false),
            stats: Mutex::new(ExternalTableRuntimeStats::default()),
        }
    }

    pub fn flow_control(&self) -> &ExternalTableFlowControl {
        &self.flow_control
    }

    pub fn observe_accumulation_bytes(&self, accumulation_bytes: u64) {
        let mut stats = self.stats.lock();
        stats.peak_accumulation_bytes = stats.peak_accumulation_bytes.max(accumulation_bytes);
    }

    pub fn record_submission(
        &self,
        input_rows: usize,
        blocked: bool,
        response: &RuntimeBridgeResponse,
    ) {
        let mut stats = self.stats.lock();
        stats.submissions = stats.submissions.saturating_add(1);
        if blocked {
            stats.blocked_submissions = stats.blocked_submissions.saturating_add(1);
        }
        stats.total_input_rows = stats.total_input_rows.saturating_add(input_rows as u64);
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
            RuntimeWarmState::Warm => {
                stats.warm_batches = stats.warm_batches.saturating_add(1);
            }
            RuntimeWarmState::Cold => {
                stats.cold_batches = stats.cold_batches.saturating_add(1);
            }
        }
        stats.retired_count = stats
            .retired_count
            .saturating_add(response.metrics.retired_count);
    }

    pub fn enqueue_output_batches(&self, batches: impl IntoIterator<Item = TableOutputBatch>) {
        let mut visible_queue = self.visible_queue.lock();
        let mut deferred_backlog = self.deferred_backlog.lock();
        let mut visible_bytes = self.visible_output_bytes.lock();
        let mut stats = self.stats.lock();

        for batch in batches {
            let can_promote = visible_queue.len() < self.flow_control.max_inflight_output_batches
                && visible_bytes.saturating_add(batch.bytes)
                    <= self.flow_control.max_inflight_output_bytes;

            if can_promote {
                *visible_bytes = visible_bytes.saturating_add(batch.bytes);
                visible_queue.push_back(batch);
            } else {
                deferred_backlog.push_back(batch);
            }
        }

        stats.peak_visible_output_batches = stats
            .peak_visible_output_batches
            .max(visible_queue.len() as u64);
        stats.peak_visible_output_bytes = stats.peak_visible_output_bytes.max(*visible_bytes);
    }

    pub fn pop_visible_batch(&self) -> Option<TableOutputBatch> {
        let batch = {
            let mut visible_queue = self.visible_queue.lock();
            let mut visible_bytes = self.visible_output_bytes.lock();
            let batch = visible_queue.pop_front()?;
            *visible_bytes = visible_bytes.saturating_sub(batch.bytes);
            batch
        };
        self.promote_backlog();
        Some(batch)
    }

    pub fn promote_backlog(&self) {
        let mut visible_queue = self.visible_queue.lock();
        let mut deferred_backlog = self.deferred_backlog.lock();
        let mut visible_bytes = self.visible_output_bytes.lock();
        let mut stats = self.stats.lock();

        while let Some(next) = deferred_backlog.front() {
            let can_promote = visible_queue.len() < self.flow_control.max_inflight_output_batches
                && visible_bytes.saturating_add(next.bytes)
                    <= self.flow_control.max_inflight_output_bytes;
            if !can_promote {
                break;
            }

            let next = deferred_backlog
                .pop_front()
                .expect("backlog front should exist while promoting");
            *visible_bytes = visible_bytes.saturating_add(next.bytes);
            visible_queue.push_back(next);
            stats.promoted_backlog_batches = stats.promoted_backlog_batches.saturating_add(1);
        }

        stats.peak_visible_output_batches = stats
            .peak_visible_output_batches
            .max(visible_queue.len() as u64);
        stats.peak_visible_output_bytes = stats.peak_visible_output_bytes.max(*visible_bytes);
    }

    pub fn mark_finalized(&self) {
        *self.finalized.lock() = true;
    }

    pub fn is_finalized(&self) -> bool {
        *self.finalized.lock()
    }

    pub fn runtime_stats(&self) -> ExternalTableRuntimeStats {
        self.stats.lock().clone()
    }

    pub fn has_visible_or_backlog_output(&self) -> bool {
        !self.visible_queue.lock().is_empty() || !self.deferred_backlog.lock().is_empty()
    }
}

#[derive(Debug)]
pub struct ExternalTableGlobalSinkState {
    pub shared: Arc<ExternalTableSharedState>,
}

impl GlobalSinkState for ExternalTableGlobalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn sink_state_name(&self) -> &str {
        "ExternalTableGlobalSinkState"
    }
}

#[derive(Debug)]
pub struct ExternalTableGlobalSourceState {
    pub shared: Arc<ExternalTableSharedState>,
}

impl GlobalSourceState for ExternalTableGlobalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct InflightTableBatch {
    pub response: RuntimeBridgeResponse,
}

#[derive(Debug)]
pub struct ExternalTableLocalSinkState {
    pub accumulation: Chunk,
    pub accumulation_bytes: u64,
    pub inflight: Option<InflightTableBatch>,
    pub current_input_staged: bool,
    pub next_batch_id: u64,
    pub combined: bool,
    pub next_partition_id: u64,
}

impl ExternalTableLocalSinkState {
    pub fn new(input_types: &[LogicalType], allocator: Arc<dyn Allocator>) -> Self {
        Self {
            accumulation: Chunk::init_empty_with_allocator(input_types, allocator),
            accumulation_bytes: 0,
            inflight: None,
            current_input_staged: false,
            next_batch_id: 1,
            combined: false,
            next_partition_id: 1,
        }
    }
}

impl LocalSinkState for ExternalTableLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug, Default)]
pub struct ExternalTableLocalSourceState;

impl LocalSourceState for ExternalTableLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
