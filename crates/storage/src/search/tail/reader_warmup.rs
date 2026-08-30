// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Governed preparation of immutable HNSW exact-tail readers.
//!
//! A committed rowset is immediately queryable through the exact-tail path,
//! but opening a plain vector column also authenticates its page envelope and
//! installs mmap views. Doing that work on the first query turns ingest
//! segment fan-out into a user-visible latency spike. This scheduler moves
//! reader preparation to the rowset-publication lifecycle while preserving
//! the same checks and the same segment-owned cache used by foreground reads.

use std::collections::BTreeSet;
use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use paro_common::error::Result;
use paro_scheduler::scheduler::TaskScheduler;
use paro_scheduler::task::{ProducerToken, Task, TaskExecutionMode, TaskExecutionResult};

use crate::rowset::{Rowset, RowsetId, RowsetSharedPtr, SegmentSharedPtr};
use crate::tablet::ColumnId;

const HNSW_TAIL_READER_WARMUP_PRIORITY: i32 = -5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct WarmupKey {
    rowset_id: RowsetId,
    column_id: ColumnId,
    dimension: usize,
}

struct WarmupState {
    producer: ProducerToken,
    pending: Mutex<BTreeSet<WarmupKey>>,
}

/// Table-scoped admission point for exact-tail reader preparation.
///
/// One cooperative task owns each `(rowset, column, dimension)` image and
/// opens at most one segment per scheduler turn. This bounds latency for a
/// large atomic transaction and lets higher-priority instance work run
/// between immutable page-authentication slices.
#[derive(Clone)]
pub(crate) struct HnswTailReaderWarmupScheduler {
    state: Arc<WarmupState>,
}

impl std::fmt::Debug for HnswTailReaderWarmupScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HnswTailReaderWarmupScheduler")
            .field("pending", &self.state.pending.lock().len())
            .finish()
    }
}

impl HnswTailReaderWarmupScheduler {
    pub(crate) fn new(scheduler: Arc<TaskScheduler>) -> Self {
        Self {
            state: Arc::new(WarmupState {
                producer: scheduler.create_producer_with_priority(HNSW_TAIL_READER_WARMUP_PRIORITY),
                pending: Mutex::new(BTreeSet::new()),
            }),
        }
    }

    pub(crate) fn schedule(&self, rowset: &RowsetSharedPtr, column_id: ColumnId, dimension: usize) {
        let key = WarmupKey {
            rowset_id: rowset.rowset_id(),
            column_id,
            dimension,
        };
        if !self.state.pending.lock().insert(key) {
            return;
        }
        let task: Arc<Mutex<dyn Task>> = Arc::new(Mutex::new(TailReaderWarmupTask {
            state: Arc::clone(&self.state),
            key,
            rowset: Arc::downgrade(rowset),
            segments: None,
            next_segment: 0,
        }));
        self.state.producer.schedule_task(task);
    }
}

struct TailReaderWarmupTask {
    state: Arc<WarmupState>,
    key: WarmupKey,
    rowset: Weak<Rowset>,
    segments: Option<Vec<SegmentSharedPtr>>,
    next_segment: usize,
}

impl TailReaderWarmupTask {
    fn finish(&mut self) -> TaskExecutionResult {
        self.state.pending.lock().remove(&self.key);
        TaskExecutionResult::Finished
    }
}

impl Task for TailReaderWarmupTask {
    fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
        let Some(rowset) = self.rowset.upgrade() else {
            return Ok(self.finish());
        };
        if self.segments.is_none() {
            if let Err(error) = rowset.load() {
                tracing::warn!(
                    rowset_id = self.key.rowset_id,
                    column_id = self.key.column_id,
                    error = %error,
                    "failed to load rowset for HNSW exact-tail reader warmup"
                );
                return Ok(self.finish());
            }
            self.segments = Some(rowset.segments());
        }
        let Some(segment) = self
            .segments
            .as_ref()
            .and_then(|segments| segments.get(self.next_segment))
        else {
            return Ok(self.finish());
        };
        if let Err(error) =
            segment.open_plain_vector_storage(self.key.column_id, self.key.dimension)
        {
            // Warmup is an optimization. The foreground open remains the
            // correctness boundary and will surface the same durable error.
            tracing::warn!(
                rowset_id = self.key.rowset_id,
                segment_id = segment.segment_id(),
                column_id = self.key.column_id,
                error = %error,
                "failed to prepare HNSW exact-tail vector reader"
            );
        }
        self.next_segment = self.next_segment.saturating_add(1);
        if self.next_segment >= self.segments.as_ref().map_or(0, std::vec::Vec::len) {
            Ok(self.finish())
        } else {
            Ok(TaskExecutionResult::NotFinished)
        }
    }

    fn task_type(&self) -> &str {
        "HnswTailReaderWarmupTask"
    }
}
