// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Lower arena physical plans into pipeline graph specs.

use std::collections::HashMap;
use std::mem;

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_function::window::WindowFunctionType;
use paro_planner::binder::ir::OrderByNode;
use paro_planner::expression::{AggregateType, Expression, ReferenceExpression};
use paro_planner::operator::join::{JoinComparisonType, JoinType};

use crate::physical::ids::PhysicalPlanNodeId;
use crate::physical::plan::PhysicalPlan;
use crate::physical::properties::{
    NullOrdering, OrderingColumn, OrderingDirection, OrderingSpec, PropertyRepairKind,
};
use crate::physical::row_type::RowType;
use crate::physical::specs::{
    AggregateSpec, ClassicIeJoinSpec, CrossProductSpec, DelimJoinSideSpec, DelimJoinSpec,
    DelimScanTarget, ExternalTableSpec, HashJoinSpec, MaterializedCteSpec, NestedLoopJoinSpec,
    PhysicalNodeKind, RecursiveCteSpec, SetOperationInputSide, SetOperationSpec, SortRangeJoinSpec,
    SortSpec, TopNSpec, WindowSpec,
};

use super::graph::{
    ClassicIeJoinSourceSpec, ClientResultSpec, ControlRegion, ControlRegionId, CopyToFileSinkSpec,
    CorrelatedSubqueryRegion, CrossProductBuildSinkSpec, CrossProductProbeSpec,
    CteMaterializeSinkSpec, CteScanSourceSpec, DeleteSinkSpec, DelimCaptureSinkSpec, DelimJoinSide,
    DelimScanSourceSpec, DependencyKind, ExternalTableSinkSpec, ExternalTableSourceSpec,
    HashAggregateBuildSinkSpec, HashAggregateEmitSourceSpec, HashJoinBuildSinkSpec,
    HashJoinProbeSpec, HashJoinSpillReplaySourceSpec, HashJoinUnmatchedSourceSpec, InsertSinkSpec,
    MaterializeSinkSpec, MaterializedSourceSpec, NestedLoopJoinProbeSpec,
    PerfectHashAggregateEmitSourceSpec, PerfectHashAggregateSinkSpec, PipelineDependency,
    PipelineGraph, PipelineId, PipelineRoot, PipelineSpec, PipelineSubgraphRoot, RecursiveCteDedup,
    RecursiveCteRegion, RecursiveTableAppendSinkSpec, RecursiveTableScanSourceSpec,
    RecursiveTermination, RowsetDynamicRuntimeFilterSpec, RowsetSourceSpec,
    SetOperationEmitSourceSpec, SetOperationInputSinkSpec, SharedSinkId, SinkSharing, SinkSpec,
    SortBuildSinkSpec, SortEmitSourceSpec, SortRangeJoinProbeSpec, SourceSpec, TopNBuildSinkSpec,
    TopNEmitSourceSpec, TransformSpec, UngroupedAggregateEmitSourceSpec,
    UngroupedAggregateSinkSpec, UpdateSinkSpec, WindowBuildSinkSpec, WindowEmitSourceSpec,
};
use super::handles::{BreakerHandleCatalogBuilder, BreakerHandleId, BreakerHandleKind};
use super::properties::{repair_transform, PipelinePropertyAccumulator};

pub struct PipelineLowerer<'a> {
    plan: &'a PhysicalPlan,
    handles: BreakerHandleCatalogBuilder,
    next_shared_sink: usize,
    post_join_fanout_cache: Vec<Option<bool>>,
    cte_handles: HashMap<usize, BreakerHandleId>,
    cte_producers: HashMap<BreakerHandleId, PipelineId>,
    recursive_cte_handles: HashMap<usize, BreakerHandleId>,
    delim_value_handles: HashMap<usize, BreakerHandleId>,
    cached_outer_handles: Vec<BreakerHandleId>,
    control_regions: Vec<ControlRegion>,
    control_region_roots: HashMap<PipelineId, ControlRegionId>,
}

pub(crate) struct BreakerTail {
    pub(crate) breaker: PhysicalPlanNodeId,
    pub(crate) transforms: Vec<TransformSpec>,
    pub(crate) output: RowType,
}

pub(crate) struct PendingProbeBuild {
    pub(crate) producer: PipelineId,
    pub(crate) handle: super::handles::BreakerHandleId,
}

pub(crate) struct PipelineChain {
    pub(crate) entry: PipelineId,
    pub(crate) tail: PipelineId,
}

impl<'a> PipelineLowerer<'a> {
    pub fn new(plan: &'a PhysicalPlan) -> Self {
        Self {
            plan,
            handles: BreakerHandleCatalogBuilder::default(),
            next_shared_sink: 0,
            post_join_fanout_cache: vec![None; plan.nodes.len()],
            cte_handles: HashMap::new(),
            cte_producers: HashMap::new(),
            recursive_cte_handles: HashMap::new(),
            delim_value_handles: HashMap::new(),
            cached_outer_handles: Vec::new(),
            control_regions: Vec::new(),
            control_region_roots: HashMap::new(),
        }
    }

    pub fn lower_to_pipeline_graph(&mut self, root: PhysicalPlanNodeId) -> Result<PipelineGraph> {
        match &self.plan.node(root).kind {
            PhysicalNodeKind::Insert(spec) => {
                let child = self.only_child(root)?;
                return self.lower_terminal_sink(
                    child,
                    SinkSpec::Insert(InsertSinkSpec {
                        spec: spec.clone(),
                        required: Default::default(),
                    }),
                    self.plan.node(root).output.clone(),
                );
            }
            PhysicalNodeKind::Update(spec) => {
                let child = self.only_child(root)?;
                return self.lower_terminal_sink(
                    child,
                    SinkSpec::Update(UpdateSinkSpec {
                        spec: spec.clone(),
                        required: Default::default(),
                    }),
                    self.plan.node(root).output.clone(),
                );
            }
            PhysicalNodeKind::Delete(spec) => {
                let child = self.only_child(root)?;
                return self.lower_terminal_sink(
                    child,
                    SinkSpec::Delete(DeleteSinkSpec {
                        spec: spec.clone(),
                        required: Default::default(),
                    }),
                    self.plan.node(root).output.clone(),
                );
            }
            PhysicalNodeKind::CopyToFile(spec) => {
                let child = self.only_child(root)?;
                return self.lower_terminal_sink(
                    child,
                    SinkSpec::CopyToFile(CopyToFileSinkSpec {
                        spec: spec.clone(),
                        required: Default::default(),
                    }),
                    self.plan.node(root).output.clone(),
                );
            }
            _ => {}
        }
        let mut pipelines = Vec::new();
        let mut dependencies = Vec::new();
        let root_pipeline = self.lower_subtree_to_sink(
            root,
            SinkSpec::ClientResult(ClientResultSpec::default()),
            SinkSharing::Exclusive,
            self.plan.node(root).output.clone(),
            &mut pipelines,
            &mut dependencies,
        )?;
        let root = self.pipeline_root_for(root_pipeline)?;
        let graph = PipelineGraph {
            pipelines,
            dependencies,
            handles: mem::take(&mut self.handles).finish(),
            control_regions: mem::take(&mut self.control_regions),
            root,
        };
        graph.validate()?;
        Ok(graph)
    }
}

mod aggregate_sort;
mod breaker_lowering;
mod classic_ie_join;
mod cte;
mod dispatch;
mod external;
mod helpers;
mod join_breakers;
mod join_probes;
mod linear;
mod materialized_pair;
mod pipeline_dispatch;
mod pipelines;
mod set_operation;

pub(crate) use breaker_lowering::BreakerDispatch;
use helpers::*;

#[cfg(test)]
mod tests;
