// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Partition-parallel merge for completed local hash-aggregate tables.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use paro_common::error::{self as paro_error, Result};
use paro_common::vector::VECTOR_SIZE;

use crate::operators::aggregate::radix_partitioned_aggregate_hashtable::{
    AggregateHashTable, ConcurrentRadixAggregateBuild,
};
use crate::physical::properties::MemoryClass;
use crate::runtime::breaker::{AggregateHandle, AggregateRuntimeState};
use crate::runtime::context::{FinishTaskId, OperatorFinishContext};
use crate::runtime::sink::{
    FinishCoordinatorParticipation, FinishTaskGroup, FinishTaskPoll, NextFinishTask,
    ParallelFinishDriver,
};

use super::distinct_finalize::{
    take_partitioned_distinct_work, DistinctAggregatePartition, PartitionedDistinctFinalize,
};

const PARALLEL_RADIX_MERGE_MIN_SOURCE_ROWS: usize = VECTOR_SIZE * 2;

#[derive(Debug)]
struct RadixMergePartition {
    partition_idx: usize,
    sources: Vec<AggregateHashTable>,
    distinct: Vec<DistinctAggregatePartition>,
}

#[derive(Debug)]
struct RadixMergeDriver {
    handle: Arc<AggregateHandle>,
    result: ConcurrentRadixAggregateBuild,
    distinct: Option<PartitionedDistinctFinalize>,
    work: Mutex<Vec<Option<RadixMergePartition>>>,
    next_task: AtomicUsize,
}

impl RadixMergeDriver {
    fn group(
        handle: Arc<AggregateHandle>,
        result: ConcurrentRadixAggregateBuild,
        distinct: Option<PartitionedDistinctFinalize>,
        work: Vec<RadixMergePartition>,
    ) -> FinishTaskGroup {
        FinishTaskGroup {
            task_count: work.len(),
            driver: Arc::new(Self {
                handle,
                result,
                distinct,
                work: Mutex::new(work.into_iter().map(Some).collect()),
                next_task: AtomicUsize::new(0),
            }),
            memory_class: MemoryClass::Blocking,
            coordinator_participation: FinishCoordinatorParticipation::DrainAvailable,
        }
    }

    fn publish_result(&self) -> Result<()> {
        let table = self.result.finish()?;
        self.handle.with_state_mut(|state| {
            let AggregateRuntimeState::Hash(global) = state else {
                return Err(paro_error::internal(
                    "aggregate handle does not contain hash aggregate state",
                ));
            };
            if !global.tables.is_empty() || !global.pending_radix_merges.is_empty() {
                return Err(paro_error::internal(format!(
                    "parallel aggregate merge result has competing state: tables={} pending={}",
                    global.tables.len(),
                    global.pending_radix_merges.len()
                )));
            }
            global.tables.push(table);
            Ok(())
        })
    }
}

impl ParallelFinishDriver for RadixMergeDriver {
    fn next_task(&self, _ctx: &mut OperatorFinishContext) -> Result<NextFinishTask> {
        let task_idx = self.next_task.fetch_add(1, Ordering::AcqRel);
        if task_idx >= self.work.lock().len() {
            return Ok(NextFinishTask::Drained);
        }
        Ok(NextFinishTask::Task(FinishTaskId(
            u32::try_from(task_idx).map_err(|_| {
                paro_error::internal(format!(
                    "aggregate merge task index exceeds runtime id range: {task_idx}"
                ))
            })?,
        )))
    }

    fn run_task(
        &self,
        task: FinishTaskId,
        ctx: &mut OperatorFinishContext,
    ) -> Result<FinishTaskPoll> {
        ctx.cancel.check()?;
        let task_idx = task.0 as usize;
        let work = self
            .work
            .lock()
            .get_mut(task_idx)
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "aggregate merge task index out of bounds: index={task_idx}"
                ))
            })?
            .take()
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "aggregate merge task was executed more than once: index={task_idx}"
                ))
            })?;
        let mut target = self.result.take_partition(work.partition_idx)?;
        for mut source in work.sources {
            target.combine(&mut source)?;
        }
        if let Some(distinct) = &self.distinct {
            distinct.finalize_partition(work.distinct, ctx, &mut target)?;
        }
        self.result.install(work.partition_idx, target)?;
        Ok(FinishTaskPoll::Done)
    }

    fn finish_group(&self, _ctx: &mut OperatorFinishContext) -> Result<()> {
        self.publish_result()
    }
}

pub(super) fn prepare_parallel_radix_merge(
    handle: Arc<AggregateHandle>,
    spec: &crate::physical::specs::AggregateSpec,
) -> Result<Option<FinishTaskGroup>> {
    let mut merge_tables = Vec::new();
    handle.with_state_mut(|state| {
        let AggregateRuntimeState::Hash(global) = state else {
            return Err(paro_error::internal(
                "aggregate handle does not contain hash aggregate state",
            ));
        };
        if global.pending_radix_merges.is_empty() {
            return Ok(());
        }
        if !global.tables.is_empty() {
            merge_tables.push(std::mem::take(&mut global.tables));
        }
        merge_tables.append(&mut global.pending_radix_merges);
        Ok(())
    })?;
    if merge_tables.is_empty() {
        return Ok(None);
    }
    if merge_tables.iter().any(|tables| tables.len() != 1) {
        return Err(paro_error::internal(
            "parallel radix aggregate merge requires one grouping table per local",
        ));
    }

    let mut tables = Vec::with_capacity(merge_tables.len());
    for mut local in merge_tables {
        tables.push(local.pop().ok_or_else(|| {
            paro_error::internal("validated radix aggregate local had no grouping table")
        })?);
    }
    if tables.len() == 1 {
        let table = tables.pop().ok_or_else(|| {
            paro_error::internal("single radix aggregate merge lost its grouping table")
        })?;
        handle.with_state_mut(|state| {
            let AggregateRuntimeState::Hash(global) = state else {
                return Err(paro_error::internal(
                    "aggregate handle does not contain hash aggregate state",
                ));
            };
            global.tables.push(table);
            Ok(())
        })?;
        return Ok(None);
    }

    let source_rows = tables
        .iter()
        .skip(1)
        .try_fold(0usize, |rows, table| rows.checked_add(table.count()))
        .unwrap_or(usize::MAX);
    if source_rows < PARALLEL_RADIX_MERGE_MIN_SOURCE_ROWS {
        let mut target = tables.remove(0);
        for mut source in tables {
            target.combine(&mut source)?;
        }
        handle.with_state_mut(|state| {
            let AggregateRuntimeState::Hash(global) = state else {
                return Err(paro_error::internal(
                    "aggregate handle does not contain hash aggregate state",
                ));
            };
            global.tables.push(target);
            Ok(())
        })?;
        return Ok(None);
    }

    let result_table = tables.remove(0);
    let partition_count = result_table.radix_partition_count().ok_or_else(|| {
        paro_error::internal("parallel aggregate merge target is not radix partitioned")
    })?;
    let mut work = (0..partition_count)
        .map(|partition_idx| RadixMergePartition {
            partition_idx,
            sources: Vec::with_capacity(tables.len()),
            distinct: Vec::new(),
        })
        .collect::<Vec<_>>();
    for table in tables {
        let partitions = table.into_scan_partitions();
        if partitions.len() != partition_count {
            return Err(paro_error::internal(format!(
                "aggregate merge radix partition mismatch: expected={partition_count} actual={}",
                partitions.len()
            )));
        }
        for (partition_idx, partition) in partitions.into_iter().enumerate() {
            work[partition_idx].sources.push(partition);
        }
    }
    let distinct = handle.with_state_mut(|state| {
        let AggregateRuntimeState::Hash(global) = state else {
            return Err(paro_error::internal(
                "aggregate handle does not contain hash aggregate state",
            ));
        };
        take_partitioned_distinct_work(spec, global, partition_count)
    })?;
    let distinct_context = if let Some((context, partitions)) = distinct {
        if partitions.len() != work.len() {
            return Err(paro_error::internal(format!(
                "aggregate merge DISTINCT partition mismatch: merge={} distinct={}",
                work.len(),
                partitions.len()
            )));
        }
        for (partition, aggregates) in work.iter_mut().zip(partitions) {
            partition.distinct = aggregates;
        }
        Some(context)
    } else {
        None
    };
    let result = ConcurrentRadixAggregateBuild::try_new(result_table)?;
    Ok(Some(RadixMergeDriver::group(
        handle,
        result,
        distinct_context,
        work,
    )))
}
