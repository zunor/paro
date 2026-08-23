// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_context::StatementCancelReason;

use crate::physical::properties::MemoryClass;

pub use super::breaker::MaterializeSinkExec;
use super::context::{
    Blocker, FinishTaskId, OperatorCallContext, OperatorCleanupContext, OperatorFinishContext,
    PipelineInitContext,
};
use super::state::{SinkGlobal, SinkLocal};

pub use crate::operators::aggregate::hash::build::HashAggregateBuildSinkExec;
pub use crate::operators::aggregate::perfect_hash::build::PerfectHashAggregateSinkExec;
pub use crate::operators::aggregate::ungrouped::build::UngroupedAggregateSinkExec;
pub use crate::operators::dml::{
    CopyToFileSinkExec, DeleteSinkExec, InsertSinkExec, UpdateSinkExec,
};
pub use crate::operators::external::ExternalTableSinkExec;
pub use crate::operators::join::hash::HashJoinBuildSinkExec;
pub use crate::operators::result::ClientResultSinkExec;
pub use crate::operators::set::{
    CteMaterializeSinkExec, DelimCaptureSinkExec, RecursiveTableAppendSinkExec,
    SetOperationInputSinkExec,
};
pub use crate::operators::sort::build::SortBuildSinkExec;
pub use crate::operators::sort::topn_build::TopNBuildSinkExec;
pub use crate::operators::window::{PartitionAggregateWindowBuildSinkExec, WindowBuildSinkExec};

#[derive(Debug)]
pub enum SinkExec {
    ClientResult(ClientResultSinkExec),
    Materialize(MaterializeSinkExec),
    HashJoinBuild(HashJoinBuildSinkExec),
    HashAggregateBuild(HashAggregateBuildSinkExec),
    UngroupedAggregate(UngroupedAggregateSinkExec),
    PerfectHashAggregate(PerfectHashAggregateSinkExec),
    SortBuild(SortBuildSinkExec),
    TopNBuild(TopNBuildSinkExec),
    WindowBuild(WindowBuildSinkExec),
    PartitionAggregateWindowBuild(PartitionAggregateWindowBuildSinkExec),
    SetOperationInput(SetOperationInputSinkExec),
    CteMaterialize(CteMaterializeSinkExec),
    DelimCapture(DelimCaptureSinkExec),
    RecursiveTableAppend(RecursiveTableAppendSinkExec),
    ExternalTable(ExternalTableSinkExec),
    Insert(InsertSinkExec),
    Update(UpdateSinkExec),
    Delete(DeleteSinkExec),
    CopyToFile(CopyToFileSinkExec),
    Dyn(Box<dyn DynSinkExec>),
}

impl SinkExec {
    pub fn name(&self) -> &'static str {
        match self {
            Self::ClientResult(_) => "CLIENT_RESULT",
            Self::Materialize(_) => "MATERIALIZE",
            Self::HashJoinBuild(_) => "HASH_JOIN_BUILD",
            Self::HashAggregateBuild(_) => "HASH_AGGREGATE_BUILD",
            Self::UngroupedAggregate(_) => "UNGROUPED_AGGREGATE",
            Self::PerfectHashAggregate(_) => "PERFECT_HASH_AGGREGATE",
            Self::SortBuild(_) => "SORT_BUILD",
            Self::TopNBuild(_) => "TOP_N_BUILD",
            Self::WindowBuild(_) => "WINDOW_BUILD",
            Self::PartitionAggregateWindowBuild(_) => "PARTITION_AGGREGATE_WINDOW_BUILD",
            Self::SetOperationInput(_) => "SET_OPERATION_INPUT",
            Self::CteMaterialize(_) => "CTE_MATERIALIZE",
            Self::DelimCapture(_) => "DELIM_CAPTURE",
            Self::RecursiveTableAppend(_) => "RECURSIVE_TABLE_APPEND",
            Self::ExternalTable(_) => "EXTERNAL_TABLE",
            Self::Insert(_) => "INSERT",
            Self::Update(_) => "UPDATE",
            Self::Delete(_) => "DELETE",
            Self::CopyToFile(_) => "COPY_TO_FILE",
            Self::Dyn(_) => "DYN_SINK",
        }
    }

    /// Whether omitting an empty task-local sink state is an exact identity operation.
    ///
    /// This capability is deliberately narrow. It is consumed only after a dependency-gated
    /// source proves that it has no rows. Unknown/plugin sinks keep the ordinary data-task path.
    pub(crate) fn empty_local_merge_is_identity(&self) -> bool {
        matches!(
            self,
            Self::ClientResult(_)
                | Self::Materialize(_)
                | Self::HashJoinBuild(_)
                | Self::TopNBuild(_)
                | Self::PartitionAggregateWindowBuild(_)
        )
    }

    /// Cold lifecycle dispatch; see [`SourceExec::create_global`](super::source::SourceExec::create_global).
    #[inline(never)]
    pub fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        match self {
            Self::ClientResult(exec) => exec.create_global(ctx),
            Self::Materialize(exec) => exec.create_global(ctx),
            Self::HashJoinBuild(exec) => exec.create_global(ctx),
            Self::HashAggregateBuild(exec) => exec.create_global(ctx),
            Self::UngroupedAggregate(exec) => exec.create_global(ctx),
            Self::PerfectHashAggregate(exec) => exec.create_global(ctx),
            Self::SortBuild(exec) => exec.create_global(ctx),
            Self::TopNBuild(exec) => exec.create_global(ctx),
            Self::WindowBuild(exec) => exec.create_global(ctx),
            Self::PartitionAggregateWindowBuild(exec) => exec.create_global(ctx),
            Self::SetOperationInput(exec) => exec.create_global(ctx),
            Self::CteMaterialize(exec) => exec.create_global(ctx),
            Self::DelimCapture(exec) => exec.create_global(ctx),
            Self::RecursiveTableAppend(exec) => exec.create_global(ctx),
            Self::ExternalTable(exec) => exec.create_global(ctx),
            Self::Insert(exec) => exec.create_global(ctx),
            Self::Update(exec) => exec.create_global(ctx),
            Self::Delete(exec) => exec.create_global(ctx),
            Self::CopyToFile(exec) => exec.create_global(ctx),
            Self::Dyn(exec) => exec.create_global(ctx),
        }
    }

    #[inline(never)]
    pub fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        match self {
            Self::ClientResult(exec) => exec.create_local(ctx, global),
            Self::Materialize(exec) => exec.create_local(ctx, global),
            Self::HashJoinBuild(exec) => exec.create_local(ctx, global),
            Self::HashAggregateBuild(exec) => exec.create_local(ctx, global),
            Self::UngroupedAggregate(exec) => exec.create_local(ctx, global),
            Self::PerfectHashAggregate(exec) => exec.create_local(ctx, global),
            Self::SortBuild(exec) => exec.create_local(ctx, global),
            Self::TopNBuild(exec) => exec.create_local(ctx, global),
            Self::WindowBuild(exec) => exec.create_local(ctx, global),
            Self::PartitionAggregateWindowBuild(exec) => exec.create_local(ctx, global),
            Self::SetOperationInput(exec) => exec.create_local(ctx, global),
            Self::CteMaterialize(exec) => exec.create_local(ctx, global),
            Self::DelimCapture(exec) => exec.create_local(ctx, global),
            Self::RecursiveTableAppend(exec) => exec.create_local(ctx, global),
            Self::ExternalTable(exec) => exec.create_local(ctx, global),
            Self::Insert(exec) => exec.create_local(ctx, global),
            Self::Update(exec) => exec.create_local(ctx, global),
            Self::Delete(exec) => exec.create_local(ctx, global),
            Self::CopyToFile(exec) => exec.create_local(ctx, global),
            Self::Dyn(exec) => exec.create_local(ctx, global),
        }
    }

    #[inline]
    pub fn consume(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        local: &mut SinkLocal,
        input: &mut Chunk,
    ) -> Result<SinkPoll> {
        match self {
            Self::ClientResult(exec) => exec.consume(ctx, global, local, input),
            Self::Materialize(exec) => exec.consume(ctx, global, local, input),
            Self::HashJoinBuild(exec) => exec.consume(ctx, global, local, input),
            Self::HashAggregateBuild(exec) => exec.consume(ctx, global, local, input),
            Self::UngroupedAggregate(exec) => exec.consume(ctx, global, local, input),
            Self::PerfectHashAggregate(exec) => exec.consume(ctx, global, local, input),
            Self::SortBuild(exec) => exec.consume(ctx, global, local, input),
            Self::TopNBuild(exec) => exec.consume(ctx, global, local, input),
            Self::WindowBuild(exec) => exec.consume(ctx, global, local, input),
            Self::PartitionAggregateWindowBuild(exec) => exec.consume(ctx, global, local, input),
            Self::SetOperationInput(exec) => exec.consume(ctx, global, local, input),
            Self::CteMaterialize(exec) => exec.consume(ctx, global, local, input),
            Self::DelimCapture(exec) => exec.consume(ctx, global, local, input),
            Self::RecursiveTableAppend(exec) => exec.consume(ctx, global, local, input),
            Self::ExternalTable(exec) => exec.consume(ctx, global, local, input),
            Self::Insert(exec) => exec.consume(ctx, global, local, input),
            Self::Update(exec) => exec.consume(ctx, global, local, input),
            Self::Delete(exec) => exec.consume(ctx, global, local, input),
            Self::CopyToFile(exec) => exec.consume(ctx, global, local, input),
            Self::Dyn(exec) => exec.consume(ctx, global, local, input),
        }
    }

    #[inline]
    pub fn merge_local(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        local: &mut SinkLocal,
    ) -> Result<MergePoll> {
        match self {
            Self::ClientResult(exec) => exec.merge_local(ctx, global, local),
            Self::Materialize(exec) => exec.merge_local(ctx, global, local),
            Self::HashJoinBuild(exec) => exec.merge_local(ctx, global, local),
            Self::HashAggregateBuild(exec) => exec.merge_local(ctx, global, local),
            Self::UngroupedAggregate(exec) => exec.merge_local(ctx, global, local),
            Self::PerfectHashAggregate(exec) => exec.merge_local(ctx, global, local),
            Self::SortBuild(exec) => exec.merge_local(ctx, global, local),
            Self::TopNBuild(exec) => exec.merge_local(ctx, global, local),
            Self::WindowBuild(exec) => exec.merge_local(ctx, global, local),
            Self::PartitionAggregateWindowBuild(exec) => exec.merge_local(ctx, global, local),
            Self::SetOperationInput(exec) => exec.merge_local(ctx, global, local),
            Self::CteMaterialize(exec) => exec.merge_local(ctx, global, local),
            Self::DelimCapture(exec) => exec.merge_local(ctx, global, local),
            Self::RecursiveTableAppend(exec) => exec.merge_local(ctx, global, local),
            Self::ExternalTable(exec) => exec.merge_local(ctx, global, local),
            Self::Insert(exec) => exec.merge_local(ctx, global, local),
            Self::Update(exec) => exec.merge_local(ctx, global, local),
            Self::Delete(exec) => exec.merge_local(ctx, global, local),
            Self::CopyToFile(exec) => exec.merge_local(ctx, global, local),
            Self::Dyn(exec) => exec.merge_local(ctx, global, local),
        }
    }

    #[inline]
    pub fn prepare_finish(
        &self,
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<PrepareFinishPoll> {
        match self {
            Self::ClientResult(exec) => exec.prepare_finish(ctx, global),
            Self::Materialize(exec) => exec.prepare_finish(ctx, global),
            Self::HashJoinBuild(exec) => exec.prepare_finish(ctx, global),
            Self::HashAggregateBuild(exec) => exec.prepare_finish(ctx, global),
            Self::UngroupedAggregate(exec) => exec.prepare_finish(ctx, global),
            Self::PerfectHashAggregate(exec) => exec.prepare_finish(ctx, global),
            Self::SortBuild(exec) => exec.prepare_finish(ctx, global),
            Self::TopNBuild(exec) => exec.prepare_finish(ctx, global),
            Self::WindowBuild(exec) => exec.prepare_finish(ctx, global),
            Self::PartitionAggregateWindowBuild(exec) => exec.prepare_finish(ctx, global),
            Self::SetOperationInput(exec) => exec.prepare_finish(ctx, global),
            Self::CteMaterialize(exec) => exec.prepare_finish(ctx, global),
            Self::DelimCapture(exec) => exec.prepare_finish(ctx, global),
            Self::RecursiveTableAppend(exec) => exec.prepare_finish(ctx, global),
            Self::ExternalTable(exec) => exec.prepare_finish(ctx, global),
            Self::Insert(exec) => exec.prepare_finish(ctx, global),
            Self::Update(exec) => exec.prepare_finish(ctx, global),
            Self::Delete(exec) => exec.prepare_finish(ctx, global),
            Self::CopyToFile(exec) => exec.prepare_finish(ctx, global),
            Self::Dyn(exec) => exec.prepare_finish(ctx, global),
        }
    }

    #[inline]
    pub fn finish_work(
        &self,
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishWork> {
        match self {
            Self::ClientResult(exec) => exec.finish_work(ctx, global),
            Self::Materialize(exec) => exec.finish_work(ctx, global),
            Self::HashJoinBuild(exec) => exec.finish_work(ctx, global),
            Self::HashAggregateBuild(exec) => exec.finish_work(ctx, global),
            Self::UngroupedAggregate(exec) => exec.finish_work(ctx, global),
            Self::PerfectHashAggregate(exec) => exec.finish_work(ctx, global),
            Self::SortBuild(exec) => exec.finish_work(ctx, global),
            Self::TopNBuild(exec) => exec.finish_work(ctx, global),
            Self::WindowBuild(exec) => exec.finish_work(ctx, global),
            Self::PartitionAggregateWindowBuild(exec) => exec.finish_work(ctx, global),
            Self::SetOperationInput(exec) => exec.finish_work(ctx, global),
            Self::CteMaterialize(exec) => exec.finish_work(ctx, global),
            Self::DelimCapture(exec) => exec.finish_work(ctx, global),
            Self::RecursiveTableAppend(exec) => exec.finish_work(ctx, global),
            Self::ExternalTable(exec) => exec.finish_work(ctx, global),
            Self::Insert(exec) => exec.finish_work(ctx, global),
            Self::Update(exec) => exec.finish_work(ctx, global),
            Self::Delete(exec) => exec.finish_work(ctx, global),
            Self::CopyToFile(exec) => exec.finish_work(ctx, global),
            Self::Dyn(exec) => exec.finish_work(ctx, global),
        }
    }

    #[inline]
    pub fn finish(
        &self,
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishPoll> {
        match self {
            Self::ClientResult(exec) => exec.finish(ctx, global),
            Self::Materialize(exec) => exec.finish(ctx, global),
            Self::HashJoinBuild(exec) => exec.finish(ctx, global),
            Self::HashAggregateBuild(exec) => exec.finish(ctx, global),
            Self::UngroupedAggregate(exec) => exec.finish(ctx, global),
            Self::PerfectHashAggregate(exec) => exec.finish(ctx, global),
            Self::SortBuild(exec) => exec.finish(ctx, global),
            Self::TopNBuild(exec) => exec.finish(ctx, global),
            Self::WindowBuild(exec) => exec.finish(ctx, global),
            Self::PartitionAggregateWindowBuild(exec) => exec.finish(ctx, global),
            Self::SetOperationInput(exec) => exec.finish(ctx, global),
            Self::CteMaterialize(exec) => exec.finish(ctx, global),
            Self::DelimCapture(exec) => exec.finish(ctx, global),
            Self::RecursiveTableAppend(exec) => exec.finish(ctx, global),
            Self::ExternalTable(exec) => exec.finish(ctx, global),
            Self::Insert(exec) => exec.finish(ctx, global),
            Self::Update(exec) => exec.finish(ctx, global),
            Self::Delete(exec) => exec.finish(ctx, global),
            Self::CopyToFile(exec) => exec.finish(ctx, global),
            Self::Dyn(exec) => exec.finish(ctx, global),
        }
    }
}

pub trait DynSinkExec: Send + Sync + std::fmt::Debug {
    fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SinkGlobal>;
    fn create_local(&self, ctx: &mut PipelineInitContext, global: &SinkGlobal)
        -> Result<SinkLocal>;
    fn consume(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        local: &mut SinkLocal,
        input: &mut Chunk,
    ) -> Result<SinkPoll>;
    fn merge_local(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        local: &mut SinkLocal,
    ) -> Result<MergePoll>;
    fn prepare_finish(
        &self,
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<PrepareFinishPoll>;
    fn finish_work(
        &self,
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishWork>;
    fn finish(&self, ctx: &mut OperatorFinishContext, global: &SinkGlobal) -> Result<FinishPoll>;
}

#[derive(Debug)]
pub enum SinkPoll {
    NeedMoreInput,
    StopPipeline,
    Pending(Blocker),
}

#[derive(Debug)]
pub enum MergePoll {
    Done,
    Pending(Blocker),
}

#[derive(Debug)]
pub enum PrepareFinishPoll {
    Done,
    Pending(Blocker),
}

#[derive(Debug)]
pub enum FinishWork {
    None,
    Parallel(FinishTaskGroup),
}

#[derive(Debug, Clone)]
pub struct FinishTaskGroup {
    /// Exact number of tasks the driver will issue before returning Drained.
    pub task_count: usize,
    pub driver: Arc<dyn ParallelFinishDriver>,
    pub memory_class: MemoryClass,
    pub coordinator_participation: FinishCoordinatorParticipation,
}

/// How aggressively the pipeline-completion thread should help a parallel
/// finish group while scheduler workers consume the same producer.
///
/// This is an operator policy rather than a scheduler heuristic: some finish
/// tasks are independent partitions that benefit from immediate draining,
/// while reductions over shared state need the coordinator to yield after one
/// task so sibling workers can make progress in parallel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinishCoordinatorParticipation {
    DrainAvailable,
    SingleTask,
}

impl FinishCoordinatorParticipation {
    pub fn max_tasks(self) -> usize {
        match self {
            Self::DrainAvailable => usize::MAX,
            Self::SingleTask => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelReason {
    UserRequest,
    StatementTimeout,
    OperatorError,
}

impl CancelReason {
    pub fn from_statement(reason: Option<StatementCancelReason>) -> Option<Self> {
        reason.map(|reason| match reason {
            StatementCancelReason::UserRequest => Self::UserRequest,
            StatementCancelReason::StatementTimeout => Self::StatementTimeout,
        })
    }
}

pub trait ParallelFinishDriver: Send + Sync + std::fmt::Debug {
    fn next_task(&self, ctx: &mut OperatorFinishContext) -> Result<NextFinishTask>;

    fn run_task(
        &self,
        task: FinishTaskId,
        ctx: &mut OperatorFinishContext,
    ) -> Result<FinishTaskPoll>;

    /// Publish the completed group's result after every issued task succeeds.
    /// The runtime calls this exactly once and never calls it after task error
    /// or cancellation, so drivers do not reconstruct group completion from
    /// worker-local counters.
    fn finish_group(&self, _ctx: &mut OperatorFinishContext) -> Result<()> {
        Ok(())
    }

    fn cancel_group(&self, _ctx: &mut OperatorCleanupContext, _reason: CancelReason) -> Result<()> {
        Ok(())
    }
}

type SingleFinishTaskFn =
    dyn for<'a> Fn(&mut OperatorFinishContext<'a>) -> Result<()> + Send + Sync + 'static;

/// One-shot finish work adapter for sinks whose expensive seal/finalize step is
/// already internally parallel or externally blocking, but still needs to leave
/// the synchronous `finish()` tail.
pub struct FinishTaskGroupRunner {
    label: &'static str,
    issued: AtomicBool,
    completed: AtomicBool,
    run: Box<SingleFinishTaskFn>,
}

impl FinishTaskGroupRunner {
    pub fn group(
        label: &'static str,
        memory_class: MemoryClass,
        run: impl for<'a> Fn(&mut OperatorFinishContext<'a>) -> Result<()> + Send + Sync + 'static,
    ) -> FinishTaskGroup {
        FinishTaskGroup {
            task_count: 1,
            driver: Arc::new(Self {
                label,
                issued: AtomicBool::new(false),
                completed: AtomicBool::new(false),
                run: Box::new(run),
            }),
            memory_class,
            coordinator_participation: FinishCoordinatorParticipation::DrainAvailable,
        }
    }
}

impl std::fmt::Debug for FinishTaskGroupRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FinishTaskGroupRunner")
            .field("label", &self.label)
            .field("issued", &self.issued.load(Ordering::Acquire))
            .field("completed", &self.completed.load(Ordering::Acquire))
            .finish()
    }
}

impl ParallelFinishDriver for FinishTaskGroupRunner {
    fn next_task(&self, _ctx: &mut OperatorFinishContext) -> Result<NextFinishTask> {
        if self.completed.load(Ordering::Acquire) {
            return Ok(NextFinishTask::Drained);
        }
        if !self.issued.swap(true, Ordering::AcqRel) {
            return Ok(NextFinishTask::Task(FinishTaskId(0)));
        }
        Ok(NextFinishTask::Drained)
    }

    fn run_task(
        &self,
        task: FinishTaskId,
        ctx: &mut OperatorFinishContext,
    ) -> Result<FinishTaskPoll> {
        if task != FinishTaskId(0) {
            return Err(paro_common::error::internal(format!(
                "{} received invalid finish task id {}",
                self.label, task.0
            )));
        }
        if !self.completed.swap(true, Ordering::AcqRel) {
            (self.run)(ctx)?;
        }
        Ok(FinishTaskPoll::Done)
    }
}

#[derive(Debug)]
pub enum NextFinishTask {
    Task(FinishTaskId),
    Drained,
    Pending(Blocker),
}

#[derive(Debug)]
pub enum FinishTaskPoll {
    Done,
    Pending(Blocker),
}

#[derive(Debug)]
pub enum FinishPoll {
    Done,
    DoneWithResult(Chunk),
    Pending(Blocker),
}
