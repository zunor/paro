// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::error::Result;

use super::context::{Blocker, OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use super::state::{TransformGlobal, TransformLocal};

pub use crate::operators::external::ExternalProjectTransformExec;
pub use crate::operators::graph::GraphExpandTransformExec;
pub use crate::operators::graph::GraphProjectTransformExec;
pub use crate::operators::graph::GraphShortestPathTransformExec;
pub use crate::operators::join::hash::HashJoinProbeTransformExec;
pub use crate::operators::join::nested_loop::NestedLoopJoinProbeTransformExec;
pub use crate::operators::join::sort_range::SortRangeJoinProbeTransformExec;
pub use crate::operators::join::CrossProductProbeTransformExec;
pub use crate::operators::row_fetch::RowFetchTransformExec;
pub use crate::operators::sort::StreamingTopNTransformExec;
pub use crate::operators::transform::FilterTransformExec;
pub use crate::operators::transform::ProjectTransformExec;
pub use crate::operators::transform::PropertyRepairTransformExec;
pub use crate::operators::transform::StreamingLimitTransformExec;
pub use crate::operators::window::StreamingWindowTransformExec;

#[derive(Debug)]
pub enum TransformExec {
    Filter(FilterTransformExec),
    Project(ProjectTransformExec),
    HashJoinProbe(HashJoinProbeTransformExec),
    NestedLoopJoinProbe(NestedLoopJoinProbeTransformExec),
    SortRangeJoinProbe(SortRangeJoinProbeTransformExec),
    CrossProductProbe(CrossProductProbeTransformExec),
    StreamingLimit(StreamingLimitTransformExec),
    StreamingTopN(StreamingTopNTransformExec),
    StreamingWindow(StreamingWindowTransformExec),
    ExternalProject(ExternalProjectTransformExec),
    GraphExpand(GraphExpandTransformExec),
    RowFetch(RowFetchTransformExec),
    GraphProject(GraphProjectTransformExec),
    GraphShortestPath(GraphShortestPathTransformExec),
    PropertyRepair(PropertyRepairTransformExec),
    Dyn(Box<dyn DynTransformExec>),
}

impl TransformExec {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Filter(_) => "FILTER",
            Self::Project(_) => "PROJECTION",
            Self::HashJoinProbe(_) => "HASH_JOIN_PROBE",
            Self::NestedLoopJoinProbe(_) => "NESTED_LOOP_JOIN_PROBE",
            Self::SortRangeJoinProbe(_) => "SORT_RANGE_JOIN_PROBE",
            Self::CrossProductProbe(_) => "CROSS_PRODUCT_PROBE",
            Self::StreamingLimit(_) => "STREAMING_LIMIT",
            Self::StreamingTopN(_) => "TOP_N",
            Self::StreamingWindow(_) => "STREAMING_WINDOW",
            Self::ExternalProject(_) => "EXTERNAL_PROJECT",
            Self::GraphExpand(_) => "GRAPH_EXPAND",
            Self::RowFetch(_) => "ROW_FETCH",
            Self::GraphProject(_) => "GRAPH_PROJECT",
            Self::GraphShortestPath(_) => "GRAPH_SHORTEST_PATH",
            Self::PropertyRepair(_) => "PROPERTY_REPAIR",
            Self::Dyn(_) => "DYN_TRANSFORM",
        }
    }

    #[inline]
    pub fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<TransformGlobal> {
        match self {
            Self::Filter(exec) => exec.create_global(ctx),
            Self::Project(exec) => exec.create_global(ctx),
            Self::HashJoinProbe(exec) => exec.create_global(ctx),
            Self::NestedLoopJoinProbe(exec) => exec.create_global(ctx),
            Self::SortRangeJoinProbe(exec) => exec.create_global(ctx),
            Self::CrossProductProbe(exec) => exec.create_global(ctx),
            Self::StreamingLimit(exec) => exec.create_global(ctx),
            Self::StreamingTopN(exec) => exec.create_global(ctx),
            Self::StreamingWindow(exec) => exec.create_global(ctx),
            Self::ExternalProject(exec) => exec.create_global(ctx),
            Self::GraphExpand(exec) => exec.create_global(ctx),
            Self::RowFetch(exec) => exec.create_global(ctx),
            Self::GraphProject(exec) => exec.create_global(ctx),
            Self::GraphShortestPath(exec) => exec.create_global(ctx),
            Self::PropertyRepair(exec) => exec.create_global(ctx),
            Self::Dyn(exec) => exec.create_global(ctx),
        }
    }

    #[inline]
    pub fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        global: &TransformGlobal,
    ) -> Result<TransformLocal> {
        match self {
            Self::Filter(exec) => exec.create_local(ctx, global),
            Self::Project(exec) => exec.create_local(ctx, global),
            Self::HashJoinProbe(exec) => exec.create_local(ctx, global),
            Self::NestedLoopJoinProbe(exec) => exec.create_local(ctx, global),
            Self::SortRangeJoinProbe(exec) => exec.create_local(ctx, global),
            Self::CrossProductProbe(exec) => exec.create_local(ctx, global),
            Self::StreamingLimit(exec) => exec.create_local(ctx, global),
            Self::StreamingTopN(exec) => exec.create_local(ctx, global),
            Self::StreamingWindow(exec) => exec.create_local(ctx, global),
            Self::ExternalProject(exec) => exec.create_local(ctx, global),
            Self::GraphExpand(exec) => exec.create_local(ctx, global),
            Self::RowFetch(exec) => exec.create_local(ctx, global),
            Self::GraphProject(exec) => exec.create_local(ctx, global),
            Self::GraphShortestPath(exec) => exec.create_local(ctx, global),
            Self::PropertyRepair(exec) => exec.create_local(ctx, global),
            Self::Dyn(exec) => exec.create_local(ctx, global),
        }
    }

    #[inline]
    pub fn transform(
        &self,
        ctx: &mut OperatorCallContext,
        global: &TransformGlobal,
        local: &mut TransformLocal,
        input: &Chunk,
        output: &mut Chunk,
    ) -> Result<TransformPoll> {
        match self {
            Self::Filter(exec) => exec.transform(ctx, global, local, input, output),
            Self::Project(exec) => exec.transform(ctx, global, local, input, output),
            Self::HashJoinProbe(exec) => exec.transform(ctx, global, local, input, output),
            Self::NestedLoopJoinProbe(exec) => exec.transform(ctx, global, local, input, output),
            Self::SortRangeJoinProbe(exec) => exec.transform(ctx, global, local, input, output),
            Self::CrossProductProbe(exec) => exec.transform(ctx, global, local, input, output),
            Self::StreamingLimit(exec) => exec.transform(ctx, global, local, input, output),
            Self::StreamingTopN(exec) => exec.transform(ctx, global, local, input, output),
            Self::StreamingWindow(exec) => exec.transform(ctx, global, local, input, output),
            Self::ExternalProject(exec) => exec.transform(ctx, global, local, input, output),
            Self::GraphExpand(exec) => exec.transform(ctx, global, local, input, output),
            Self::RowFetch(exec) => exec.transform(ctx, global, local, input, output),
            Self::GraphProject(exec) => exec.transform(ctx, global, local, input, output),
            Self::GraphShortestPath(exec) => exec.transform(ctx, global, local, input, output),
            Self::PropertyRepair(exec) => exec.transform(ctx, global, local, input, output),
            Self::Dyn(exec) => exec.transform(ctx, global, local, input, output),
        }
    }

    #[inline]
    pub fn flush(
        &self,
        ctx: &mut OperatorCallContext,
        global: &TransformGlobal,
        local: &mut TransformLocal,
        output: &mut Chunk,
    ) -> Result<TransformFlushPoll> {
        match self {
            Self::Filter(exec) => exec.flush(ctx, global, local, output),
            Self::Project(exec) => exec.flush(ctx, global, local, output),
            Self::HashJoinProbe(exec) => exec.flush(ctx, global, local, output),
            Self::NestedLoopJoinProbe(exec) => exec.flush(ctx, global, local, output),
            Self::SortRangeJoinProbe(exec) => exec.flush(ctx, global, local, output),
            Self::CrossProductProbe(exec) => exec.flush(ctx, global, local, output),
            Self::StreamingLimit(exec) => exec.flush(ctx, global, local, output),
            Self::StreamingTopN(exec) => exec.flush(ctx, global, local, output),
            Self::StreamingWindow(exec) => exec.flush(ctx, global, local, output),
            Self::ExternalProject(exec) => exec.flush(ctx, global, local, output),
            Self::GraphExpand(exec) => exec.flush(ctx, global, local, output),
            Self::RowFetch(exec) => exec.flush(ctx, global, local, output),
            Self::GraphProject(exec) => exec.flush(ctx, global, local, output),
            Self::GraphShortestPath(exec) => exec.flush(ctx, global, local, output),
            Self::PropertyRepair(exec) => exec.flush(ctx, global, local, output),
            Self::Dyn(exec) => exec.flush(ctx, global, local, output),
        }
    }

    #[inline]
    pub fn finish_global(
        &self,
        ctx: &mut OperatorFinishContext,
        global: &TransformGlobal,
    ) -> Result<TransformFinishPoll> {
        match self {
            Self::Filter(exec) => exec.finish_global(ctx, global),
            Self::Project(exec) => exec.finish_global(ctx, global),
            Self::HashJoinProbe(exec) => exec.finish_global(ctx, global),
            Self::NestedLoopJoinProbe(exec) => exec.finish_global(ctx, global),
            Self::SortRangeJoinProbe(exec) => exec.finish_global(ctx, global),
            Self::CrossProductProbe(exec) => exec.finish_global(ctx, global),
            Self::StreamingLimit(exec) => exec.finish_global(ctx, global),
            Self::StreamingTopN(exec) => exec.finish_global(ctx, global),
            Self::StreamingWindow(exec) => exec.finish_global(ctx, global),
            Self::ExternalProject(exec) => exec.finish_global(ctx, global),
            Self::GraphExpand(exec) => exec.finish_global(ctx, global),
            Self::RowFetch(exec) => exec.finish_global(ctx, global),
            Self::GraphProject(exec) => exec.finish_global(ctx, global),
            Self::GraphShortestPath(exec) => exec.finish_global(ctx, global),
            Self::PropertyRepair(exec) => exec.finish_global(ctx, global),
            Self::Dyn(exec) => exec.finish_global(ctx, global),
        }
    }
}

pub trait DynTransformExec: Send + Sync + std::fmt::Debug {
    fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<TransformGlobal>;
    fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        global: &TransformGlobal,
    ) -> Result<TransformLocal>;
    fn transform(
        &self,
        ctx: &mut OperatorCallContext,
        global: &TransformGlobal,
        local: &mut TransformLocal,
        input: &Chunk,
        output: &mut Chunk,
    ) -> Result<TransformPoll>;
    fn flush(
        &self,
        ctx: &mut OperatorCallContext,
        global: &TransformGlobal,
        local: &mut TransformLocal,
        output: &mut Chunk,
    ) -> Result<TransformFlushPoll>;
    fn finish_global(
        &self,
        ctx: &mut OperatorFinishContext,
        global: &TransformGlobal,
    ) -> Result<TransformFinishPoll>;
}

#[derive(Debug)]
pub enum TransformPoll {
    NeedMoreInput,
    Output,
    OutputMore,
    StopPipeline,
    Pending(Blocker),
}

#[derive(Debug)]
pub enum TransformFlushPoll {
    Done,
    Output,
    OutputMore,
    Pending(Blocker),
}

#[derive(Debug)]
pub enum TransformFinishPoll {
    Done,
    Pending(Blocker),
}
