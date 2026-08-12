// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Parallel materialization of ordered in-memory sort partitions.
//!
//! Finish tasks merge disjoint global ranges into chunks. The handle installs
//! those chunks in partition order and the source emits them sequentially, so
//! parallel merge work cannot reorder client output.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::MemoryAccountingClass;

use crate::physical::properties::MemoryClass;
use crate::runtime::breaker::{SortHandle, SortMaterializationBuild};
use crate::runtime::context::{FinishTaskId, OperatorFinishContext};
use crate::runtime::sink::{
    FinishCoordinatorParticipation, FinishTaskGroup, FinishTaskPoll, NextFinishTask,
    ParallelFinishDriver,
};

const MATERIALIZED_SORT_AVAILABLE_MEMORY_DIVISOR: usize = 4;

#[derive(Debug)]
struct SortMaterializeFinalizeDriver {
    handle: Arc<SortHandle>,
    build: SortMaterializationBuild,
    partitions: Mutex<Vec<Option<Vec<Chunk>>>>,
    next_task: AtomicUsize,
}

impl SortMaterializeFinalizeDriver {
    fn group(handle: Arc<SortHandle>, build: SortMaterializationBuild) -> FinishTaskGroup {
        let task_count = build.task_count;
        FinishTaskGroup {
            task_count,
            driver: Arc::new(Self {
                handle,
                build,
                partitions: Mutex::new((0..task_count).map(|_| None).collect()),
                next_task: AtomicUsize::new(0),
            }),
            memory_class: MemoryClass::Blocking,
            coordinator_participation: FinishCoordinatorParticipation::DrainAvailable,
        }
    }

    fn install_partition(&self, partition_idx: usize, partition: Vec<Chunk>) -> Result<()> {
        let mut partitions = self.partitions.lock();
        let slot = partitions.get_mut(partition_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "materialized sort partition is out of bounds: index={partition_idx}, count={}",
                self.build.task_count
            ))
        })?;
        if slot.replace(partition).is_some() {
            return Err(paro_error::internal(format!(
                "materialized sort partition was installed twice: index={partition_idx}"
            )));
        }
        Ok(())
    }

    fn publish_result(&self) -> Result<()> {
        let chunks = {
            let mut slots = self.partitions.lock();
            let mut chunks = Vec::new();
            for (idx, slot) in slots.iter_mut().enumerate() {
                let partition = slot.take().ok_or_else(|| {
                    paro_error::internal(format!(
                        "materialized sort is missing completed partition {idx}"
                    ))
                })?;
                chunks.extend(partition);
            }
            chunks
        };
        self.handle
            .install_materialized(chunks, self.build.total_count)
    }
}

impl ParallelFinishDriver for SortMaterializeFinalizeDriver {
    fn next_task(&self, _ctx: &mut OperatorFinishContext) -> Result<NextFinishTask> {
        let task_idx = self.next_task.fetch_add(1, Ordering::AcqRel);
        if task_idx >= self.build.task_count {
            return Ok(NextFinishTask::Drained);
        }
        let task_id = u32::try_from(task_idx).map_err(|_| {
            paro_error::internal(format!(
                "materialized sort task exceeds runtime id range: {task_idx}"
            ))
        })?;
        Ok(NextFinishTask::Task(FinishTaskId(task_id)))
    }

    fn run_task(
        &self,
        task: FinishTaskId,
        ctx: &mut OperatorFinishContext,
    ) -> Result<FinishTaskPoll> {
        ctx.cancel.check()?;
        let partition_idx = task.0 as usize;
        if partition_idx >= self.build.task_count {
            return Err(paro_error::internal(format!(
                "materialized sort task is out of bounds: index={partition_idx}, count={}",
                self.build.task_count
            )));
        }
        let start = partition_idx
            .checked_mul(self.build.partition_size)
            .ok_or_else(|| paro_error::internal("materialized sort range overflow"))?;
        let end = start
            .saturating_add(self.build.partition_size)
            .min(self.build.total_count);
        let allocator = ctx
            .memory
            .accounted_allocator_for(MemoryTag::OrderBy, MemoryAccountingClass::NonRevocable);
        let partition =
            self.build
                .merger
                .materialize_range(start, end, &self.build.output_types, allocator)?;
        self.install_partition(partition_idx, partition)?;
        Ok(FinishTaskPoll::Done)
    }

    fn finish_group(&self, _ctx: &mut OperatorFinishContext) -> Result<()> {
        self.publish_result()
    }
}

pub(crate) fn prepare_parallel_sort_finalize(
    handle: Arc<SortHandle>,
    num_threads: usize,
    available_memory_bytes: usize,
) -> Result<Option<FinishTaskGroup>> {
    let materialization_budget =
        available_memory_bytes / MATERIALIZED_SORT_AVAILABLE_MEMORY_DIVISOR;
    let Some(build) =
        handle.prepare_parallel_materialization(num_threads, materialization_budget)?
    else {
        return Ok(None);
    };
    Ok(Some(SortMaterializeFinalizeDriver::group(handle, build)))
}
