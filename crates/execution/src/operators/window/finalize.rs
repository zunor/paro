// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Finish tasks own complete SQL partitions; publication preserves sort order.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
use paro_function::table::TableFunctionRuntimeContext;

use super::runtime::{
    evaluate_window_work, publish_window_work, window_radix_keys, WindowWorkResult,
};
use crate::physical::properties::MemoryClass;
use crate::physical::specs::WindowSpec;
use crate::runtime::breaker::radix::{finish_routing, radix_work, RadixRows};
use crate::runtime::breaker::WindowHandle;
use crate::runtime::context::{FinishTaskId, OperatorFinishContext};
use crate::runtime::sink::{
    FinishCoordinatorParticipation, FinishTaskGroup, FinishTaskPoll, FinishWork, NextFinishTask,
    ParallelFinishDriver,
};

#[derive(Debug)]
struct WindowFinalizeDriver {
    handle: Arc<WindowHandle>,
    spec: WindowSpec,
    chunks: Vec<Chunk>,
    routing: Vec<RadixRows>,
    work: Vec<std::ops::Range<usize>>,
    memory: MemoryAccountingContext,
    results: Mutex<Vec<Option<WindowWorkResult>>>,
    next_task: AtomicUsize,
}

impl ParallelFinishDriver for WindowFinalizeDriver {
    fn next_task(&self, ctx: &mut OperatorFinishContext) -> Result<NextFinishTask> {
        ctx.cancel.check()?;
        let idx = self.next_task.fetch_add(1, Ordering::Relaxed);
        if idx >= self.results.lock().len() {
            return Ok(NextFinishTask::Drained);
        }
        Ok(NextFinishTask::Task(FinishTaskId(
            u32::try_from(idx)
                .map_err(|_| paro_error::internal("window finish task id overflow"))?,
        )))
    }

    fn run_task(
        &self,
        task: FinishTaskId,
        ctx: &mut OperatorFinishContext,
    ) -> Result<FinishTaskPoll> {
        ctx.cancel.check()?;
        let idx = task.0 as usize;
        let keys = window_radix_keys(
            &self.chunks,
            &self.routing,
            self.work[idx].clone(),
            &self.memory,
            ctx.cancel,
        )?;
        let allocator = ctx
            .memory
            .accounted_allocator_for(MemoryTag::BaseTable, MemoryAccountingClass::NonRevocable);
        let chunks = evaluate_window_work(&self.spec, &self.chunks, keys, allocator, ctx.cancel)?;
        self.results.lock()[idx] = Some(chunks);
        Ok(FinishTaskPoll::Done)
    }

    fn finish_group(&self, ctx: &mut OperatorFinishContext) -> Result<()> {
        ctx.cancel.check()?;
        let mut results = Vec::new();
        for result in self.results.lock().iter_mut() {
            results.push(
                result
                    .take()
                    .ok_or_else(|| paro_error::internal("window finish result missing"))?,
            );
        }
        let allocator = ctx
            .memory
            .accounted_allocator_for(MemoryTag::BaseTable, MemoryAccountingClass::NonRevocable);
        self.handle.publish(publish_window_work(
            &self.spec,
            &self.chunks,
            results,
            allocator,
            &self.memory,
            ctx.cancel,
        )?)
    }
}

pub(super) fn prepare_window_finalize(
    handle: Arc<WindowHandle>,
    spec: &WindowSpec,
    ctx: &mut OperatorFinishContext,
) -> Result<FinishWork> {
    ctx.cancel.check()?;
    let mut input = handle.take_input();
    finish_routing(
        &mut input,
        &super::runtime::window_radix_columns(spec)?,
        ctx.query.max_parallel_tasks().max(1).next_power_of_two(),
        || {
            ctx.memory
                .accounted_allocator_for(MemoryTag::BaseTable, MemoryAccountingClass::NonRevocable)
        },
        ctx.cancel,
    )?;
    let (chunks, routing): (Vec<_>, Vec<_>) =
        input.into_iter().map(|c| (c.input, c.routing)).unzip();
    let memory = ctx
        .query
        .memory_accounting_context(MemoryTag::BaseTable, MemoryAccountingClass::NonRevocable);
    let work = radix_work(&chunks, &routing);
    let task_count = work.len();
    if task_count == 1 {
        let keys = window_radix_keys(&chunks, &routing, work[0].clone(), &memory, ctx.cancel)?;
        let allocator = ctx
            .memory
            .accounted_allocator_for(MemoryTag::BaseTable, MemoryAccountingClass::NonRevocable);
        let output = evaluate_window_work(spec, &chunks, keys, allocator.clone(), ctx.cancel)?;
        handle.publish(publish_window_work(
            spec,
            &chunks,
            vec![output],
            allocator,
            &memory,
            ctx.cancel,
        )?)?;
        return Ok(FinishWork::None);
    }
    Ok(FinishWork::Parallel(FinishTaskGroup {
        task_count,
        driver: Arc::new(WindowFinalizeDriver {
            handle,
            spec: spec.clone(),
            chunks,
            routing,
            work,
            memory,
            results: Mutex::new((0..task_count).map(|_| None).collect()),
            next_task: AtomicUsize::new(0),
        }),
        memory_class: MemoryClass::Blocking,
        coordinator_participation: FinishCoordinatorParticipation::DrainAvailable,
    }))
}
