// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Parallel finalization for grouped unordered DISTINCT aggregates.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use paro_common::error::{self as paro_error, Result};

use crate::operators::aggregate::aggregate_object::AggregateObject;
use crate::operators::aggregate::build_helpers::{
    aggregate_objects, group_payload_refs, normalized_grouping_sets, query_hash_table_memory,
};
use crate::operators::aggregate::distinct_helpers::finalize_distinct_fragments_into_table;
use crate::operators::aggregate::distinct_state::DistinctKeyTable;
use crate::operators::aggregate::radix_partitioned_aggregate_hashtable::{
    AggregateHashTable, ConcurrentRadixAggregateBuild,
};
use crate::physical::properties::MemoryClass;
use crate::physical::specs::AggregateSpec;
use crate::runtime::breaker::{AggregateHandle, AggregateRuntimeState};
use crate::runtime::context::{FinishTaskId, OperatorFinishContext};
use crate::runtime::sink::{FinishTaskGroup, FinishTaskPoll, NextFinishTask, ParallelFinishDriver};

#[derive(Debug)]
struct DistinctFinalizePartition {
    partition_idx: usize,
    aggregates: Vec<DistinctAggregatePartition>,
}

#[derive(Debug)]
struct DistinctAggregatePartition {
    aggregate_idx: usize,
    fragments: Vec<DistinctKeyTable>,
}

#[derive(Debug)]
struct DistinctFinalizeDriver {
    handle: Arc<AggregateHandle>,
    spec: AggregateSpec,
    aggregate_objects: Arc<[AggregateObject]>,
    group_refs: Arc<[usize]>,
    result: ConcurrentRadixAggregateBuild,
    work: Mutex<Vec<Option<DistinctFinalizePartition>>>,
    next_task: AtomicUsize,
    remaining: AtomicUsize,
}

impl DistinctFinalizeDriver {
    fn group(
        handle: Arc<AggregateHandle>,
        spec: AggregateSpec,
        aggregate_objects: Arc<[AggregateObject]>,
        group_refs: Arc<[usize]>,
        result: ConcurrentRadixAggregateBuild,
        work: Vec<DistinctFinalizePartition>,
    ) -> FinishTaskGroup {
        let task_count = work.len();
        FinishTaskGroup {
            task_count_hint: task_count,
            driver: Arc::new(Self {
                handle,
                spec,
                aggregate_objects,
                group_refs,
                result,
                work: Mutex::new(work.into_iter().map(Some).collect()),
                next_task: AtomicUsize::new(0),
                remaining: AtomicUsize::new(task_count),
            }),
            memory_class: MemoryClass::Blocking,
        }
    }

    fn complete_task(&self, partition_idx: usize, local: AggregateHashTable) -> Result<()> {
        self.result.install(partition_idx, local)?;
        let remaining = self.remaining.fetch_sub(1, Ordering::AcqRel);
        if remaining == 0 {
            return Err(paro_error::internal(
                "parallel DISTINCT finalize completed more tasks than it scheduled",
            ));
        }
        if remaining != 1 {
            return Ok(());
        }

        let table = self.result.finish()?;
        self.handle.with_state_mut(|state| {
            let AggregateRuntimeState::Hash(global) = state else {
                return Err(paro_error::internal(
                    "aggregate handle does not contain hash aggregate state",
                ));
            };
            if !global.tables.is_empty() {
                return Err(paro_error::internal(format!(
                    "parallel DISTINCT result was installed into non-empty aggregate state: tables={}",
                    global.tables.len()
                )));
            }
            global.tables.push(table);
            Ok(())
        })
    }
}

impl ParallelFinishDriver for DistinctFinalizeDriver {
    fn next_task(&self, _ctx: &mut OperatorFinishContext) -> Result<NextFinishTask> {
        let task_idx = self.next_task.fetch_add(1, Ordering::AcqRel);
        if task_idx >= self.work.lock().len() {
            return Ok(NextFinishTask::Drained);
        }
        let task_id = u32::try_from(task_idx).map_err(|_| {
            paro_error::internal(format!(
                "distinct finalize task index exceeds runtime id range: {task_idx}"
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
        let task_idx = task.0 as usize;
        let work = self
            .work
            .lock()
            .get_mut(task_idx)
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "distinct finalize task index out of bounds: index={task_idx}"
                ))
            })?
            .take()
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "distinct finalize task was executed more than once: index={task_idx}"
                ))
            })?;
        let partition_idx = work.partition_idx;
        let modifier_memory = query_hash_table_memory(ctx.query);
        let mut local = self.result.take_partition(partition_idx)?;
        for aggregate in work.aggregates {
            let aggregate_idx = aggregate.aggregate_idx;
            if aggregate.fragments.iter().all(|keys| keys.count() == 0) {
                continue;
            }
            finalize_distinct_fragments_into_table(
                &self.spec,
                &self.aggregate_objects,
                &self.group_refs,
                aggregate_idx,
                &aggregate.fragments,
                &modifier_memory,
                &mut local,
            )?;
        }
        self.complete_task(partition_idx, local)?;
        Ok(FinishTaskPoll::Done)
    }
}

pub(super) fn prepare_parallel_distinct_finalize(
    handle: Arc<AggregateHandle>,
    spec: &AggregateSpec,
) -> Result<Option<FinishTaskGroup>> {
    let grouping_sets = normalized_grouping_sets(spec)?;
    if grouping_sets.len() != 1
        || grouping_sets[0].as_slice() != (0..spec.grouping_key_count).collect::<Vec<_>>()
    {
        return Ok(None);
    }

    let aggregate_objects: Arc<[AggregateObject]> =
        Arc::from(aggregate_objects(spec)?.into_boxed_slice());
    let group_refs: Arc<[usize]> = Arc::from(group_payload_refs(spec)?.into_boxed_slice());
    let mut work = Vec::new();
    let mut result_table = None;
    handle.with_state_mut(|state| {
        let AggregateRuntimeState::Hash(global) = state else {
            return Err(paro_error::internal(
                "aggregate handle does not contain hash aggregate state",
            ));
        };
        if global.tables.len() != 1 {
            return Ok(());
        }
        let Some(partition_count) = global.tables[0].radix_partition_count() else {
            return Ok(());
        };
        let mut partitions = (0..partition_count)
            .map(|partition_idx| DistinctFinalizePartition {
                partition_idx,
                aggregates: Vec::new(),
            })
            .collect::<Vec<_>>();
        for (aggregate_idx, object) in aggregate_objects.iter().enumerate() {
            if !object.is_distinct() || !object.order_bys.is_empty() {
                continue;
            }
            let partition_groups = global.distinct.take_partition_groups(aggregate_idx)?;
            if partition_groups.len() != partition_count {
                return Err(paro_error::internal(format!(
                    "DISTINCT/output radix partition count mismatch at aggregate {aggregate_idx}: distinct={}, output={partition_count}",
                    partition_groups.len()
                )));
            }
            for (partition_idx, fragments) in partition_groups.into_iter().enumerate() {
                if fragments.iter().any(|table| table.count() > 0) {
                    partitions[partition_idx]
                        .aggregates
                        .push(DistinctAggregatePartition {
                        aggregate_idx,
                        fragments,
                    });
                }
            }
        }
        work = partitions
            .into_iter()
            .filter(|partition| !partition.aggregates.is_empty())
            .collect();
        if !work.is_empty() {
            result_table = global.tables.pop();
        }
        Ok(())
    })?;
    if work.is_empty() {
        return Ok(None);
    }
    let result = ConcurrentRadixAggregateBuild::try_new(result_table.ok_or_else(|| {
        paro_error::internal("parallel DISTINCT finalize did not detach its result table")
    })?)?;
    Ok(Some(DistinctFinalizeDriver::group(
        handle,
        spec.clone(),
        aggregate_objects,
        group_refs,
        result,
        work,
    )))
}
