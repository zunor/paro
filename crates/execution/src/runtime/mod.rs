// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Role-specific runtime program and state slots for the new operator runtime.

pub mod breaker;
pub mod context;
pub mod control_region;
pub mod ids;
pub mod parameter;
pub mod pipeline_runtime;
pub mod scheduler;
pub mod scheduling_policy;
pub mod scratch;
pub mod sink;
pub mod source;
pub mod state;
pub mod task_executor;
pub mod transform;
pub mod utility;
pub(crate) mod work_group;

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_planner::expression::{ColumnRefExpression, Expression};

pub(crate) fn visit_column_refs<F>(expr: &Expression, visitor: &mut F)
where
    F: FnMut(&ColumnRefExpression),
{
    match expr {
        Expression::ColumnRef(expr) => visitor(expr),
        Expression::Comparison(expr) => {
            visit_column_refs(&expr.left, visitor);
            visit_column_refs(&expr.right, visitor);
        }
        Expression::Conjunction(expr) => {
            for child in &expr.children {
                visit_column_refs(child, visitor);
            }
        }
        Expression::Function(expr) => {
            for child in &expr.children {
                visit_column_refs(child, visitor);
            }
        }
        Expression::Cast(expr) => visit_column_refs(&expr.child, visitor),
        Expression::Operator(expr) => {
            for child in &expr.children {
                visit_column_refs(child, visitor);
            }
        }
        Expression::Case(expr) => {
            visit_column_refs(&expr.check, visitor);
            visit_column_refs(&expr.result_if_true, visitor);
            visit_column_refs(&expr.result_if_false, visitor);
        }
        Expression::Aggregate(expr) => {
            for child in &expr.children {
                visit_column_refs(child, visitor);
            }
            if let Some(filter) = &expr.filter {
                visit_column_refs(filter, visitor);
            }
            for order in &expr.order_bys {
                visit_column_refs(&order.expression, visitor);
            }
        }
        Expression::Subquery(expr) => {
            for child in &expr.children {
                visit_column_refs(child, visitor);
            }
        }
        Expression::Window(expr) => {
            for child in &expr.children {
                visit_column_refs(child, visitor);
            }
            for partition in &expr.partitions {
                visit_column_refs(partition, visitor);
            }
            for order in &expr.orders {
                visit_column_refs(&order.expression, visitor);
            }
        }
        Expression::Reference(_) | Expression::Parameter(_) | Expression::Constant(_) => {}
    }
}

pub(crate) fn read_u64_from_vector(vector: &Vector, row: usize, label: &str) -> Result<u64> {
    match vector.logical_type() {
        LogicalType::UBigInt => vector
            .get_u64(row)
            .ok_or_else(|| null_u64_error(label, row)),
        LogicalType::BigInt => match vector.get_i64(row) {
            Some(value) if value >= 0 => Ok(value as u64),
            Some(value) => Err(paro_error::internal(format!(
                "{label} at row {row} is negative: {value}"
            ))),
            None => Err(null_u64_error(label, row)),
        },
        LogicalType::UInteger => vector
            .get_u32(row)
            .map(u64::from)
            .ok_or_else(|| null_u64_error(label, row)),
        LogicalType::Integer => match vector.get_i32(row) {
            Some(value) if value >= 0 => Ok(value as u64),
            Some(value) => Err(paro_error::internal(format!(
                "{label} at row {row} is negative: {value}"
            ))),
            None => Err(null_u64_error(label, row)),
        },
        other => Err(paro_error::internal(format!(
            "{label} at row {row} must be an integer vector, got {other:?}"
        ))),
    }
}

pub(crate) fn read_u32_from_vector(vector: &Vector, row: usize, label: &str) -> Result<u32> {
    let value = read_u64_from_vector(vector, row, label)?;
    u32::try_from(value).map_err(|_| {
        paro_error::internal(format!("{label} at row {row} is out of u32 range: {value}"))
    })
}

fn null_u64_error(label: &str, row: usize) -> paro_common::error::ParoError {
    paro_error::internal(format!("{label} at row {row} is NULL"))
}

pub use crate::operators::join::nested_loop::{
    NestedLoopJoinProbeTransformExec, NljUnmatchedSourceExec,
};
pub use crate::operators::join::sort_range::SortRangeJoinProbeTransformExec;
pub use breaker::{
    AggregateHandle, BreakerHandleMetadata, BreakerHandleRegistry, CleanupReason, CleanupState,
    CleanupStatus, CompletionLatch, CteHandle, DelimHandle, ExternalTableHandle, HandleRef,
    JoinBuildHandle, JoinBuildId, JoinBuildMode, JoinBuildStats, JoinExternalModeConfig,
    JoinPartitionSet, JoinSpillState, MaterializedHandle, ProbeSpillSet, RecursiveTableHandle,
    RuntimeBreakerHandle, RuntimeCleanup, SetOperationHandle, SharedSinkCoordinator,
    SharedSinkMergeEvent, SharedSinkProducerIndex, SharedSinkState, SortHandle, TopNHandle,
    TypedBreakerHandle, WindowHandle,
};
pub use context::{
    BlockReason, Blocker, FinishTaskId, OperatorCallContext, OperatorCleanupContext,
    OperatorFinishContext, OperatorScratchScope, OperatorWakeScope, PendingWakeRegistration,
    PipelineInitContext, PipelineTaskId, QueryErrorId, QueryErrorRegistry, QueryOutputPort,
    QueryOutputPortStats, QueryOutputReferenceWrite, QueryOutputWrite, QueryProfilerRegistry,
    QueryRuntimeContext, RetainedMemorySnapshot, UtilityContext, WakeGeneration, WakeKey,
    WakeSource, WakeToken,
};
pub use control_region::{
    ControlRegionRuntime, ControlRegionRuntimeSet, CorrelatedSubqueryControllerState,
    CorrelatedSubqueryPhase, PipelineDependencyGates, PipelineGateEvent,
    RecursiveCteControllerState, RecursiveCteGateAction, RecursiveCteIterationRuntime,
    RecursiveCtePersistentState, RecursiveCtePhase, RecursiveCtePrograms, SharedSinkRuntimeSet,
    SideEffectOrderGate,
};
pub use ids::{
    OperatorRole, RuntimeOperatorId, RuntimeOperatorOrigin, RuntimeRoleOrdinal, SubRoleIndex,
};
pub use parameter::{ExpressionEvalInput, ParameterBindingEpoch, ParameterBindings};
pub use pipeline_runtime::PipelineRuntime;
pub use scheduler::PipelineScheduler;
pub(crate) use scheduling_policy::RUNTIME_WAIT_TIMEOUT;
pub use scheduling_policy::{
    FairnessPolicy, PipelineReadyEvent, PipelineReadyPriority, PipelineSchedulingPolicy,
    ReadyEntry, ReadyQueuePolicy, WakeStormPolicy,
};
pub use scratch::{
    ChunkLayout, ChunkLayoutKind, ChunkLease, ExpressionScratchArena, ExpressionScratchLease,
    PendingChunkState, PipelineScratch, PipelineScratchLayout, PipelineTaskState, SinkResumeState,
    TaskMemoryGrants, TransformResumeState,
};
pub use sink::{
    CancelReason, ClientResultSinkExec, CopyToFileSinkExec, CteMaterializeSinkExec, DeleteSinkExec,
    DelimCaptureSinkExec, DynSinkExec, ExternalTableSinkExec, FinishPoll, FinishTaskGroup,
    FinishTaskPoll, FinishWork, HashAggregateBuildSinkExec, HashJoinBuildSinkExec, InsertSinkExec,
    MaterializeSinkExec, MergePoll, NextFinishTask, ParallelFinishDriver,
    PerfectHashAggregateSinkExec, PrepareFinishPoll, RecursiveTableAppendSinkExec,
    SetOperationInputSinkExec, SinkExec, SinkPoll, SortBuildSinkExec, TopNBuildSinkExec,
    UngroupedAggregateSinkExec, UpdateSinkExec, WindowBuildSinkExec,
};
pub use source::{
    AdaptiveSearchSourceExec, ChunkSourceExec, ClassicIeJoinSourceExec, CteScanSourceExec,
    DelimScanSourceExec, DummySourceExec, DynSourceExec, EmptySourceExec, ExpressionSourceExec,
    ExternalTableSourceExec, FullTextSearchSourceExec, GraphScanSourceExec,
    HashAggregateEmitSourceExec, HashJoinSpillReplaySourceExec, HashJoinUnmatchedSourceExec,
    MaterializedSourceExec, PerfectHashAggregateEmitSourceExec, RecursiveTableScanSourceExec,
    RowsetSourceDesc, RowsetSourceExec, SetOperationEmitSourceExec, SortEmitSourceExec, SourceExec,
    SourcePoll, SparseVectorSearchSourceExec, TableFunctionSourceExec, TopNEmitSourceExec,
    UngroupedAggregateEmitSourceExec, ValuesSourceExec, VectorSearchSourceExec,
    WindowEmitSourceExec,
};
pub use state::{
    BreakerHandleGlobal, ChunkSourceGlobal, ChunkSourceLocal, ClientResultSinkGlobal,
    ClientResultSinkLocal, CrossProductProbeTransformLocal, CteMaterializeSinkLocal,
    CteScanSourceLocal, DelimCaptureSinkGlobal, DelimCaptureSinkLocal, DelimScanSourceLocal,
    DynGlobalState, DynGlobalStateBox, DynLocalState, DynLocalStateBox, DynStateTypeId,
    EmptySourceGlobal, EmptySourceLocal, ExpressionSourceGlobal, ExpressionSourceLocal,
    FilterTransformGlobal, FilterTransformLocal, HashAggregateBuildSinkLocal,
    HashAggregateEmitSourceLocal, HashJoinBuildSinkLocal, HashJoinProbeTransformLocal,
    HashJoinSpillReplaySourceLocal, HashJoinUnmatchedSourceLocal, MaterializeSinkGlobal,
    MaterializeSinkLocal, MaterializedSourceGlobal, MaterializedSourceLocal,
    PerfectHashAggregateEmitSourceLocal, PerfectHashAggregateSinkLocal, ProjectTransformGlobal,
    ProjectTransformLocal, RecursiveTableAppendSinkGlobal, RecursiveTableAppendSinkLocal,
    RecursiveTableScanSourceLocal, RowsetSourceGlobal, RowsetSourceLocal,
    SetOperationEmitSourceLocal, SetOperationInputSinkLocal, SinkGlobal, SinkLocal,
    SortBuildSinkLocal, SortEmitSourceLocal, SourceGlobal, SourceLocal,
    StreamingLimitTransformGlobal, StreamingLimitTransformLocal, StreamingTopNTransformGlobal,
    StreamingTopNTransformLocal, StreamingWindowTransformGlobal, StreamingWindowTransformLocal,
    TableFunctionSourceGlobal, TableFunctionSourceLocal, TopNBuildSinkLocal, TopNEmitSourceLocal,
    TransformGlobal, TransformGlobalSlots, TransformLocal, UngroupedAggregateEmitSourceLocal,
    UngroupedAggregateSinkLocal, ValuesSourceGlobal, ValuesSourceLocal, WindowBuildSinkLocal,
    WindowEmitSourceLocal,
};
pub use task_executor::{
    PipelineTaskExecutor, PipelineTaskPhase, PipelineTaskStepContext, TaskStepResult,
};
pub use transform::{
    CrossProductProbeTransformExec, DynTransformExec, ExternalProjectTransformExec,
    FilterTransformExec, GraphExpandTransformExec, GraphProjectTransformExec,
    GraphShortestPathTransformExec, HashJoinProbeTransformExec, ProjectTransformExec,
    PropertyRepairTransformExec, StreamingLimitTransformExec, StreamingTopNTransformExec,
    StreamingWindowTransformExec, TransformExec, TransformFinishPoll, TransformFlushPoll,
    TransformPoll,
};
pub use utility::{run_once as run_utility_once, UtilityRunResult};
pub(crate) use work_group::WorkGroupCompletion;
