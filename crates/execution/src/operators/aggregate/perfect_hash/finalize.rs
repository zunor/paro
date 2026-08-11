// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Parallel finish driver for perfect aggregate local-table reduction.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};
use paro_common::memory::MemoryAccountingContext;

use crate::operators::aggregate::perfect_aggregate_hashtable::{
    ParallelPerfectAggregateMerge, PerfectAggregateStateFilter,
};
use crate::physical::properties::MemoryClass;
use crate::runtime::breaker::{AggregateHandle, AggregateRuntimeState};
use crate::runtime::context::{FinishTaskId, OperatorFinishContext};
use crate::runtime::sink::{
    FinishCoordinatorParticipation, FinishTaskGroup, FinishTaskPoll, FinishWork, NextFinishTask,
    ParallelFinishDriver,
};

#[derive(Debug)]
struct PerfectAggregateMergeDriver {
    handle: Arc<AggregateHandle>,
    merge: ParallelPerfectAggregateMerge,
    next_task: AtomicUsize,
    remaining: AtomicUsize,
}

impl PerfectAggregateMergeDriver {
    fn group(
        handle: Arc<AggregateHandle>,
        merge: ParallelPerfectAggregateMerge,
    ) -> FinishTaskGroup {
        let task_count = merge.task_count();
        FinishTaskGroup {
            task_count_hint: task_count,
            driver: Arc::new(Self {
                handle,
                merge,
                next_task: AtomicUsize::new(0),
                remaining: AtomicUsize::new(task_count),
            }),
            memory_class: MemoryClass::Blocking,
            coordinator_participation: FinishCoordinatorParticipation::SingleTask,
        }
    }

    fn complete_task(&self) -> Result<()> {
        let remaining = self
            .remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .map_err(|_| {
                paro_error::internal("perfect aggregate merge completed more tasks than scheduled")
            })?;
        if remaining != 1 {
            return Ok(());
        }

        let table = self.merge.finish()?;
        self.handle.with_state_mut(|state| {
            let AggregateRuntimeState::Perfect(global) = state else {
                return Err(paro_error::internal(
                    "aggregate handle does not contain perfect aggregate state",
                ));
            };
            if global.build_table.is_some()
                || global.finalized_table.is_some()
                || !global.pending_tables.is_empty()
            {
                return Err(paro_error::internal(
                    "parallel perfect aggregate merge result has no empty destination",
                ));
            }
            global.finalized_table = Some(table);
            Ok(())
        })
    }
}

impl ParallelFinishDriver for PerfectAggregateMergeDriver {
    fn next_task(&self, _ctx: &mut OperatorFinishContext) -> Result<NextFinishTask> {
        let task_idx = self.next_task.fetch_add(1, Ordering::AcqRel);
        if task_idx >= self.merge.task_count() {
            return Ok(NextFinishTask::Drained);
        }
        let task_id = u32::try_from(task_idx).map_err(|_| {
            paro_error::internal(format!(
                "perfect aggregate merge task index exceeds runtime id range: {task_idx}"
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
        self.merge.combine_task(task.0 as usize)?;
        self.complete_task()?;
        Ok(FinishTaskPoll::Done)
    }
}

pub(crate) fn prepare_parallel_perfect_merge(
    handle: Arc<AggregateHandle>,
    max_tasks: usize,
    state_filter: Option<PerfectAggregateStateFilter>,
    memory: MemoryAccountingContext,
) -> Result<FinishWork> {
    let mut tables = Vec::new();
    handle.with_state_mut(|state| {
        let AggregateRuntimeState::Perfect(global) = state else {
            return Err(paro_error::internal(
                "aggregate handle does not contain perfect aggregate state",
            ));
        };
        if global.finalized_table.is_some() {
            return Err(paro_error::internal(
                "perfect aggregate merge prepared after finalization",
            ));
        }
        if let Some(table) = global.build_table.take() {
            tables.push(table);
        }
        tables.append(&mut global.pending_tables);
        Ok(())
    })?;

    if tables.len() <= 1 {
        // Zero-input aggregation has no table yet. One local table needs no
        // reduction and is restored for the synchronous finish tail.
        if let Some(table) = tables.pop() {
            handle.with_state_mut(|state| {
                let AggregateRuntimeState::Perfect(global) = state else {
                    return Err(paro_error::internal(
                        "aggregate handle does not contain perfect aggregate state",
                    ));
                };
                global.build_table = Some(table);
                Ok(())
            })?;
        }
        return Ok(FinishWork::None);
    }
    let merge = ParallelPerfectAggregateMerge::try_new(tables, max_tasks, state_filter, memory)?
        .ok_or_else(|| {
            paro_error::internal("perfect aggregate merge declined multiple compatible tables")
        })?;
    Ok(FinishWork::Parallel(PerfectAggregateMergeDriver::group(
        handle, merge,
    )))
}
