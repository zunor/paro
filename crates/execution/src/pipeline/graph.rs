// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable pipeline graph produced by physical plan lowering.

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_planner::binder::ir::OrderByNode;
use paro_planner::expression::Expression;
use paro_planner::operator::join::{JoinCondition, JoinType};

use crate::physical::properties::{PipelineProperties, RequiredProperties};
use crate::physical::row_type::RowType;
use crate::physical::specs::{
    AdaptiveSearchSpec, AggregateSpec, ChunkScanSpec, ClassicIeJoinSpec, CopyToFileSpec,
    DeleteSpec, DummyScanSpec, EmptyResultSpec, ExpressionScanSpec, ExternalProjectSpec,
    ExternalTableSpec, FilterSpec, FullTextSearchSpec, GraphExpandSpec, GraphProjectSpec,
    GraphScanSpec, GraphShortestPathSpec, HashReductionCascadeSpec, InsertSpec, LimitSpec,
    ProjectSpec, RowsetScanSpec, SetOperationInputSide, SetOperationSpec, SparseVectorSearchSpec,
    TableFunctionScanSpec, TopNSpec, UpdateSpec, ValuesSpec, VectorSearchSpec, WindowSpec,
};

use super::handles::{BreakerHandleCatalog, BreakerHandleId, BreakerHandleKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PipelineId(u32);

impl PipelineId {
    pub fn new(index: usize) -> Self {
        assert!(index <= u32::MAX as usize, "pipeline graph exhausted");
        Self(index as u32)
    }

    pub fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SharedSinkId(u32);

impl SharedSinkId {
    pub fn new(index: usize) -> Self {
        assert!(index <= u32::MAX as usize, "shared sink registry exhausted");
        Self(index as u32)
    }

    pub fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ControlRegionId(u32);

impl ControlRegionId {
    pub fn new(index: usize) -> Self {
        assert!(
            index <= u32::MAX as usize,
            "control region registry exhausted"
        );
        Self(index as u32)
    }

    pub fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UtilityProgramId(u32);

impl UtilityProgramId {
    pub fn new(index: usize) -> Self {
        assert!(
            index <= u32::MAX as usize,
            "utility program registry exhausted"
        );
        Self(index as u32)
    }

    pub fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug, Clone)]
pub struct PipelineGraph {
    pub pipelines: Vec<PipelineSpec>,
    pub dependencies: Vec<PipelineDependency>,
    pub handles: BreakerHandleCatalog,
    pub control_regions: Vec<ControlRegion>,
    pub root: PipelineRoot,
}

impl PipelineGraph {
    pub fn validate(&self) -> Result<()> {
        self.validate_root()?;
        self.validate_pipeline_ids()?;
        self.validate_dependencies()?;
        self.validate_control_regions()?;
        self.handles.validate()?;
        self.validate_pipeline_handles()?;
        Ok(())
    }

    pub fn pipeline(&self, id: PipelineId) -> Option<&PipelineSpec> {
        self.pipelines.get(id.index())
    }

    pub fn shared_sink_producers(&self, shared: SharedSinkId) -> Vec<PipelineId> {
        self.pipelines
            .iter()
            .filter_map(|pipeline| match pipeline.sink_sharing {
                SinkSharing::Shared(id) if id == shared => Some(pipeline.id),
                _ => None,
            })
            .collect()
    }

    fn validate_root(&self) -> Result<()> {
        match self.root {
            PipelineRoot::Pipeline(id) => {
                if self.pipeline(id).is_none() {
                    return Err(paro_error::internal("pipeline graph root id is invalid"));
                }
            }
            PipelineRoot::ControlRegion(id) => {
                if self.control_regions.get(id.0 as usize).is_none() {
                    return Err(paro_error::internal(
                        "pipeline graph root control region id is invalid",
                    ));
                }
            }
            PipelineRoot::Utility(_) => {}
        }
        Ok(())
    }

    fn validate_pipeline_ids(&self) -> Result<()> {
        for (idx, pipeline) in self.pipelines.iter().enumerate() {
            if pipeline.id.index() != idx {
                return Err(paro_error::internal(format!(
                    "pipeline id mismatch at graph index {idx}"
                )));
            }
        }
        Ok(())
    }

    fn validate_dependencies(&self) -> Result<()> {
        let pipeline_count = self.pipelines.len();
        let mut indegree = vec![0usize; pipeline_count];
        let mut adjacency = vec![Vec::<usize>::new(); pipeline_count];

        for dependency in &self.dependencies {
            let producer = dependency.producer.index();
            let consumer = dependency.consumer.index();
            if producer >= pipeline_count || consumer >= pipeline_count {
                return Err(paro_error::internal(
                    "pipeline dependency references invalid id",
                ));
            }
            if matches!(
                dependency.kind,
                DependencyKind::LoopEntry(_) | DependencyKind::LoopBack(_)
            ) {
                let region = match dependency.kind {
                    DependencyKind::LoopEntry(region) | DependencyKind::LoopBack(region) => region,
                    _ => unreachable!(),
                };
                if self.control_regions.get(region.index()).is_none() {
                    return Err(paro_error::internal(
                        "loop dependency references invalid control region",
                    ));
                }
                continue;
            }
            adjacency[producer].push(consumer);
            indegree[consumer] += 1;
        }

        let mut stack = indegree
            .iter()
            .enumerate()
            .filter_map(|(idx, degree)| (*degree == 0).then_some(idx))
            .collect::<Vec<_>>();
        let mut visited = 0usize;
        while let Some(node) = stack.pop() {
            visited += 1;
            for &next in &adjacency[node] {
                indegree[next] -= 1;
                if indegree[next] == 0 {
                    stack.push(next);
                }
            }
        }

        if visited != pipeline_count {
            return Err(paro_error::internal(
                "pipeline graph contains a dependency cycle",
            ));
        }
        Ok(())
    }

    fn validate_control_regions(&self) -> Result<()> {
        for (idx, region) in self.control_regions.iter().enumerate() {
            let id = ControlRegionId::new(idx);
            match region {
                ControlRegion::RecursiveCte(region) => {
                    self.require_pipeline(region.anchor, "recursive CTE anchor")?;
                    self.require_pipeline(region.emit, "recursive CTE emit")?;
                    for pipeline in &region.recursive {
                        self.require_pipeline(*pipeline, "recursive CTE recursive member")?;
                    }
                    self.require_handle_kind(
                        region.working,
                        super::handles::BreakerHandleKind::RecursiveTable,
                        "recursive CTE working table",
                    )?;
                    self.require_handle_kind(
                        region.intermediate,
                        super::handles::BreakerHandleKind::RecursiveTable,
                        "recursive CTE intermediate table",
                    )?;
                    if let Some(accumulated) = region.accumulated {
                        self.require_handle_kind(
                            accumulated,
                            super::handles::BreakerHandleKind::RecursiveTable,
                            "recursive CTE accumulated table",
                        )?;
                    }
                }
                ControlRegion::CorrelatedSubquery(region) => {
                    self.require_pipeline(region.capture, "correlated subquery capture")?;
                    self.require_pipeline(region.join, "correlated subquery join")?;
                    for root in &region.dependent_roots {
                        match root {
                            PipelineSubgraphRoot::Pipeline(pipeline) => {
                                self.require_pipeline(
                                    *pipeline,
                                    "correlated subquery dependent pipeline",
                                )?;
                            }
                            PipelineSubgraphRoot::ControlRegion(region) => {
                                if *region == id
                                    || self.control_regions.get(region.index()).is_none()
                                {
                                    return Err(paro_error::internal(
                                        "correlated subquery references invalid dependent control region",
                                    ));
                                }
                            }
                        }
                    }
                    self.require_handle_kind(
                        region.delim_values,
                        super::handles::BreakerHandleKind::Delim,
                        "correlated subquery delim values",
                    )?;
                    if let Some(cached_outer) = region.cached_outer {
                        self.require_handle_kind(
                            cached_outer,
                            super::handles::BreakerHandleKind::Delim,
                            "correlated subquery cached outer",
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_pipeline_handles(&self) -> Result<()> {
        for pipeline in &self.pipelines {
            let pid = pipeline.id;
            pipeline
                .source
                .visit_expected_handles(|handle, expected_kind| {
                    let Some(entry) = self.handles.get(handle) else {
                        return Err(paro_error::internal(format!(
                            "pipeline {:?} source references invalid breaker handle {:?}",
                            pid, handle
                        )));
                    };
                    if entry.kind != expected_kind {
                        return Err(paro_error::internal(format!(
                            "pipeline {:?} source expects {:?} handle, got {:?}",
                            pid, expected_kind, entry.kind
                        )));
                    }
                    Ok(())
                })?;
            for (idx, transform) in pipeline.transforms.iter().enumerate() {
                transform.visit_expected_handles(|handle, expected_kind| {
                    let Some(entry) = self.handles.get(handle) else {
                        return Err(paro_error::internal(format!(
                            "pipeline {:?} transform[{idx}] references invalid breaker handle {:?}",
                            pid, handle
                        )));
                    };
                    if entry.kind != expected_kind {
                        return Err(paro_error::internal(format!(
                            "pipeline {:?} transform[{idx}] expects {:?} handle, got {:?}",
                            pid, expected_kind, entry.kind
                        )));
                    }
                    Ok(())
                })?;
            }
            pipeline
                .sink
                .visit_expected_handles(|handle, expected_kind| {
                    let Some(entry) = self.handles.get(handle) else {
                        return Err(paro_error::internal(format!(
                            "pipeline {:?} sink references invalid breaker handle {:?}",
                            pid, handle
                        )));
                    };
                    if entry.kind != expected_kind {
                        return Err(paro_error::internal(format!(
                            "pipeline {:?} sink expects {:?} handle, got {:?}",
                            pid, expected_kind, entry.kind
                        )));
                    }
                    Ok(())
                })?;
        }
        Ok(())
    }

    fn require_pipeline(&self, pipeline: PipelineId, label: &'static str) -> Result<()> {
        if self.pipeline(pipeline).is_none() {
            return Err(paro_error::internal(format!(
                "{label} references invalid pipeline id"
            )));
        }
        Ok(())
    }

    fn require_handle_kind(
        &self,
        handle: BreakerHandleId,
        expected: super::handles::BreakerHandleKind,
        label: &'static str,
    ) -> Result<()> {
        let Some(entry) = self.handles.get(handle) else {
            return Err(paro_error::internal(format!(
                "{label} references invalid breaker handle"
            )));
        };
        if entry.kind != expected {
            return Err(paro_error::internal(format!(
                "{label} expects {:?} breaker handle, got {:?}",
                expected, entry.kind
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PipelineSpec {
    pub id: PipelineId,
    pub source: SourceSpec,
    pub transforms: Vec<TransformSpec>,
    pub sink: SinkSpec,
    pub sink_sharing: SinkSharing,
    pub properties: PipelineProperties,
    pub output: RowType,
}

#[derive(Debug, Clone)]
pub enum SourceSpec {
    Rowset(RowsetSourceSpec),
    Values(ValuesSpec),
    Dummy(DummyScanSpec),
    Empty(EmptyResultSpec),
    Chunk(ChunkScanSpec),
    Expression(ExpressionScanSpec),
    TableFunction(TableFunctionScanSpec),
    VectorSearch(VectorSearchSpec),
    SparseVectorSearch(SparseVectorSearchSpec),
    FullTextSearch(FullTextSearchSpec),
    AdaptiveSearch(AdaptiveSearchSpec),
    GraphScan(GraphScanSpec),
    ExternalTable(ExternalTableSourceSpec),
    Materialized(MaterializedSourceSpec),
    ClassicIeJoin(ClassicIeJoinSourceSpec),
    NljUnmatched(NljUnmatchedSourceSpec),
    HashJoinSpillReplay(HashJoinSpillReplaySourceSpec),
    HashJoinUnmatched(HashJoinUnmatchedSourceSpec),
    HashAggregateEmit(HashAggregateEmitSourceSpec),
    UngroupedAggregateEmit(UngroupedAggregateEmitSourceSpec),
    PerfectHashAggregateEmit(PerfectHashAggregateEmitSourceSpec),
    SortEmit(SortEmitSourceSpec),
    TopNEmit(TopNEmitSourceSpec),
    WindowEmit(WindowEmitSourceSpec),
    SetOperationEmit(SetOperationEmitSourceSpec),
    CteScan(CteScanSourceSpec),
    DelimScan(DelimScanSourceSpec),
    RecursiveTableScan(RecursiveTableScanSourceSpec),
}

impl SourceSpec {
    #[inline]
    pub fn output_row_type(&self, fallback: &RowType) -> RowType {
        match self {
            Self::Rowset(spec) => RowType::new(
                spec.scan.output_names.to_vec(),
                spec.scan.returned_types.to_vec(),
            ),
            Self::Values(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::Chunk(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::Expression(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::TableFunction(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::VectorSearch(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::SparseVectorSearch(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::FullTextSearch(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::AdaptiveSearch(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::GraphScan(spec) => RowType::new(
                vec!["local_vertex_id".to_string(), "rowid".to_string()],
                spec.output_types.to_vec(),
            ),
            Self::ExternalTable(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::Dummy(_) => RowType::new(Vec::new(), Vec::new()),
            Self::ClassicIeJoin(spec) => RowType::new(
                spec.spec.output_names.to_vec(),
                spec.spec.output_types.to_vec(),
            ),
            Self::NljUnmatched(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::HashJoinSpillReplay(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::HashJoinUnmatched(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::HashAggregateEmit(spec) => RowType::new(
                spec.spec.output_names.to_vec(),
                spec.spec.output_types.to_vec(),
            ),
            Self::UngroupedAggregateEmit(spec) => RowType::new(
                spec.spec.output_names.to_vec(),
                spec.spec.output_types.to_vec(),
            ),
            Self::PerfectHashAggregateEmit(spec) => RowType::new(
                spec.spec.output_names.to_vec(),
                spec.spec.output_types.to_vec(),
            ),
            Self::SortEmit(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::TopNEmit(spec) => RowType::new(
                spec.spec.output_names.to_vec(),
                spec.spec.output_types.to_vec(),
            ),
            Self::WindowEmit(spec) => RowType::new(
                spec.spec.output_names.to_vec(),
                spec.spec.output_types.to_vec(),
            ),
            Self::SetOperationEmit(spec) => RowType::new(
                spec.spec.output_names.to_vec(),
                spec.spec.output_types.to_vec(),
            ),
            Self::Empty(_)
            | Self::Materialized(_)
            | Self::CteScan(_)
            | Self::DelimScan(_)
            | Self::RecursiveTableScan(_) => fallback.clone(),
        }
    }

    #[inline]
    pub fn visit_expected_handles(
        &self,
        mut visit: impl FnMut(BreakerHandleId, BreakerHandleKind) -> Result<()>,
    ) -> Result<()> {
        match self {
            Self::Rowset(source) => {
                for filter in &source.dynamic_runtime_filters {
                    visit(filter.handle, BreakerHandleKind::HashJoinBuild)?;
                }
            }
            Self::Materialized(source) => {
                visit(source.handle, BreakerHandleKind::Materialized)?;
            }
            Self::ClassicIeJoin(source) => {
                visit(source.left_handle, BreakerHandleKind::Materialized)?;
                visit(source.right_handle, BreakerHandleKind::Materialized)?;
            }
            Self::NljUnmatched(source) => {
                visit(source.handle, BreakerHandleKind::Materialized)?;
            }
            Self::HashJoinSpillReplay(source) => {
                visit(source.handle, BreakerHandleKind::HashJoinBuild)?;
            }
            Self::HashJoinUnmatched(source) => {
                visit(source.handle, BreakerHandleKind::HashJoinBuild)?;
            }
            Self::HashAggregateEmit(source) => {
                visit(source.handle, BreakerHandleKind::Aggregate)?;
            }
            Self::UngroupedAggregateEmit(source) => {
                visit(source.handle, BreakerHandleKind::Aggregate)?;
            }
            Self::PerfectHashAggregateEmit(source) => {
                visit(source.handle, BreakerHandleKind::Aggregate)?;
            }
            Self::SortEmit(source) => {
                visit(source.handle, BreakerHandleKind::Sort)?;
            }
            Self::TopNEmit(source) => {
                visit(source.handle, BreakerHandleKind::TopN)?;
            }
            Self::WindowEmit(source) => {
                visit(source.handle, BreakerHandleKind::Window)?;
            }
            Self::SetOperationEmit(source) => {
                visit(source.handle, BreakerHandleKind::SetOperation)?;
            }
            Self::CteScan(source) => {
                visit(source.handle, BreakerHandleKind::Cte)?;
            }
            Self::DelimScan(source) => {
                visit(source.handle, BreakerHandleKind::Delim)?;
            }
            Self::RecursiveTableScan(source) => {
                visit(source.handle, BreakerHandleKind::RecursiveTable)?;
            }
            Self::ExternalTable(source) => {
                visit(source.handle, BreakerHandleKind::ExternalTable)?;
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RowsetSourceSpec {
    pub scan: RowsetScanSpec,
    pub dynamic_runtime_filters: Box<[RowsetDynamicRuntimeFilterSpec]>,
}

impl RowsetSourceSpec {
    pub fn new(scan: RowsetScanSpec) -> Self {
        Self {
            scan,
            dynamic_runtime_filters: Vec::new().into_boxed_slice(),
        }
    }

    pub fn add_dynamic_runtime_filter(&mut self, filter: RowsetDynamicRuntimeFilterSpec) {
        let mut filters = self.dynamic_runtime_filters.to_vec();
        if filters.iter().any(|existing| existing == &filter) {
            return;
        }
        filters.push(filter);
        self.dynamic_runtime_filters = filters.into_boxed_slice();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowsetDynamicRuntimeFilterSpec {
    pub handle: BreakerHandleId,
    pub build_key_index: usize,
    pub probe_column_id: u32,
}

#[derive(Debug, Clone)]
pub struct MaterializedSourceSpec {
    pub handle: BreakerHandleId,
}

#[derive(Debug, Clone)]
pub struct ClassicIeJoinSourceSpec {
    pub left_handle: BreakerHandleId,
    pub right_handle: BreakerHandleId,
    pub spec: ClassicIeJoinSpec,
}

#[derive(Debug, Clone)]
pub struct NljUnmatchedSourceSpec {
    pub handle: BreakerHandleId,
    pub join_type: JoinType,
    pub left_output_types: Box<[LogicalType]>,
    pub right_projection: Box<[usize]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct HashJoinSpillReplaySourceSpec {
    pub handle: BreakerHandleId,
    pub join_type: JoinType,
    pub anti_join_mode: paro_planner::operator::join::AntiJoinMode,
    pub key_conditions: Box<[JoinCondition]>,
    pub build_residual_conditions: Box<[JoinCondition]>,
    pub probe_residual_count: usize,
    pub probe_types: Box<[LogicalType]>,
    pub build_payload_types: Box<[LogicalType]>,
    pub build_output_count: usize,
    pub left_projection: Box<[usize]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
    pub reduction_cascade: Option<HashReductionCascadeSpec>,
}

#[derive(Debug, Clone)]
pub struct HashJoinUnmatchedSourceSpec {
    pub handle: BreakerHandleId,
    pub join_type: JoinType,
    pub left_output_types: Box<[LogicalType]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
    pub reduction_cascade: Option<HashReductionCascadeSpec>,
}

#[derive(Debug, Clone)]
pub struct HashAggregateEmitSourceSpec {
    pub handle: BreakerHandleId,
    pub spec: AggregateSpec,
}

#[derive(Debug, Clone)]
pub struct UngroupedAggregateEmitSourceSpec {
    pub handle: BreakerHandleId,
    pub spec: AggregateSpec,
}

#[derive(Debug, Clone)]
pub struct PerfectHashAggregateEmitSourceSpec {
    pub handle: BreakerHandleId,
    pub spec: AggregateSpec,
}

#[derive(Debug, Clone)]
pub struct SortEmitSourceSpec {
    pub handle: BreakerHandleId,
    pub ordering: crate::physical::properties::OrderingSpec,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct TopNEmitSourceSpec {
    pub handle: BreakerHandleId,
    pub spec: TopNSpec,
}

#[derive(Debug, Clone)]
pub struct WindowEmitSourceSpec {
    pub handle: BreakerHandleId,
    pub spec: WindowSpec,
}

#[derive(Debug, Clone)]
pub struct SetOperationEmitSourceSpec {
    pub handle: BreakerHandleId,
    pub spec: SetOperationSpec,
}

#[derive(Debug, Clone)]
pub struct CteScanSourceSpec {
    pub handle: BreakerHandleId,
}

#[derive(Debug, Clone)]
pub struct DelimScanSourceSpec {
    pub handle: BreakerHandleId,
}

#[derive(Debug, Clone)]
pub struct RecursiveTableScanSourceSpec {
    pub handle: BreakerHandleId,
}

#[derive(Debug, Clone)]
pub struct ExternalTableSourceSpec {
    pub handle: BreakerHandleId,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub enum TransformSpec {
    Filter(FilterSpec),
    Project(ProjectSpec),
    HashJoinProbe(HashJoinProbeSpec),
    NestedLoopJoinProbe(NestedLoopJoinProbeSpec),
    SortRangeJoinProbe(SortRangeJoinProbeSpec),
    CrossProductProbe(CrossProductProbeSpec),
    Limit(LimitSpec),
    StreamingTopN(TopNSpec),
    StreamingWindow(WindowSpec),
    ExternalProject(ExternalProjectSpec),
    GraphExpand(GraphExpandSpec),
    GraphProject(GraphProjectSpec),
    GraphShortestPath(GraphShortestPathSpec),
    PropertyRepair(PropertyRepairSpec),
}

impl TransformSpec {
    #[inline]
    pub fn output_row_type(&self, input: &RowType) -> RowType {
        match self {
            Self::Project(spec) => RowType::new(
                spec.output_names.to_vec(),
                spec.expressions
                    .iter()
                    .map(|expr| expr.return_type())
                    .collect(),
            ),
            Self::StreamingTopN(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::StreamingWindow(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::ExternalProject(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::GraphExpand(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::GraphProject(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::GraphShortestPath(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::HashJoinProbe(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::NestedLoopJoinProbe(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::SortRangeJoinProbe(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::CrossProductProbe(spec) => {
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec())
            }
            Self::Filter(_) | Self::Limit(_) | Self::PropertyRepair(_) => input.clone(),
        }
    }

    #[inline]
    pub fn visit_expected_handles(
        &self,
        mut visit: impl FnMut(BreakerHandleId, BreakerHandleKind) -> Result<()>,
    ) -> Result<()> {
        match self {
            Self::HashJoinProbe(transform) => {
                visit(transform.handle, BreakerHandleKind::HashJoinBuild)?;
            }
            Self::NestedLoopJoinProbe(transform) => {
                visit(transform.handle, BreakerHandleKind::Materialized)?;
            }
            Self::SortRangeJoinProbe(transform) => {
                visit(transform.handle, BreakerHandleKind::Materialized)?;
            }
            Self::CrossProductProbe(transform) => {
                visit(transform.handle, BreakerHandleKind::Materialized)?;
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PropertyRepairSpec {
    pub kind: crate::physical::properties::PropertyRepairKind,
}

#[derive(Debug, Clone)]
pub struct HashJoinProbeSpec {
    pub handle: BreakerHandleId,
    pub join_type: JoinType,
    pub anti_join_mode: paro_planner::operator::join::AntiJoinMode,
    pub key_conditions: Box<[JoinCondition]>,
    pub build_residual_conditions: Box<[JoinCondition]>,
    pub probe_residual_count: usize,
    pub left_projection: Box<[usize]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
    pub reduction_cascade: Option<HashReductionCascadeSpec>,
}

#[derive(Debug, Clone)]
pub struct NestedLoopJoinProbeSpec {
    pub handle: BreakerHandleId,
    pub join_type: JoinType,
    pub conditions: Box<[JoinCondition]>,
    pub mark_semantics: paro_planner::operator::MarkJoinSemantics,
    pub arbitrary_condition: Option<Expression>,
    pub left_projection: Box<[usize]>,
    pub right_projection: Box<[usize]>,
    pub right_output_types: Box<[LogicalType]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct SortRangeJoinProbeSpec {
    pub handle: BreakerHandleId,
    pub join_type: JoinType,
    pub conditions: Box<[JoinCondition]>,
    pub mark_semantics: paro_planner::operator::MarkJoinSemantics,
    pub left_projection: Box<[usize]>,
    pub right_projection: Box<[usize]>,
    pub right_output_types: Box<[LogicalType]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct CrossProductProbeSpec {
    pub handle: BreakerHandleId,
    pub left_column_count: usize,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub enum SinkSpec {
    ClientResult(ClientResultSpec),
    Materialize(MaterializeSinkSpec),
    CrossProductBuild(CrossProductBuildSinkSpec),
    HashJoinBuild(HashJoinBuildSinkSpec),
    HashAggregateBuild(HashAggregateBuildSinkSpec),
    UngroupedAggregate(UngroupedAggregateSinkSpec),
    PerfectHashAggregate(PerfectHashAggregateSinkSpec),
    SortBuild(SortBuildSinkSpec),
    TopNBuild(TopNBuildSinkSpec),
    WindowBuild(WindowBuildSinkSpec),
    SetOperationInput(SetOperationInputSinkSpec),
    CteMaterialize(CteMaterializeSinkSpec),
    DelimCapture(DelimCaptureSinkSpec),
    RecursiveTableAppend(RecursiveTableAppendSinkSpec),
    ExternalTable(ExternalTableSinkSpec),
    Insert(InsertSinkSpec),
    Update(UpdateSinkSpec),
    Delete(DeleteSinkSpec),
    CopyToFile(CopyToFileSinkSpec),
}

impl SinkSpec {
    #[inline]
    pub fn required_properties(&self) -> RequiredProperties {
        match self {
            Self::ClientResult(spec) => spec.required.clone(),
            Self::Materialize(spec) => spec.required.clone(),
            Self::CrossProductBuild(spec) => spec.required.clone(),
            Self::HashJoinBuild(spec) => spec.required.clone(),
            Self::HashAggregateBuild(spec) => spec.required.clone(),
            Self::UngroupedAggregate(spec) => spec.required.clone(),
            Self::PerfectHashAggregate(spec) => spec.required.clone(),
            Self::SortBuild(spec) => spec.required.clone(),
            Self::TopNBuild(spec) => spec.required.clone(),
            Self::WindowBuild(spec) => spec.required.clone(),
            Self::SetOperationInput(spec) => spec.required.clone(),
            Self::CteMaterialize(spec) => spec.required.clone(),
            Self::DelimCapture(spec) => spec.required.clone(),
            Self::RecursiveTableAppend(spec) => spec.required.clone(),
            Self::ExternalTable(spec) => spec.required.clone(),
            Self::Insert(spec) => spec.required.clone(),
            Self::Update(spec) => spec.required.clone(),
            Self::Delete(spec) => spec.required.clone(),
            Self::CopyToFile(spec) => spec.required.clone(),
        }
    }

    #[inline]
    pub fn visit_expected_handles(
        &self,
        mut visit: impl FnMut(BreakerHandleId, BreakerHandleKind) -> Result<()>,
    ) -> Result<()> {
        match self {
            Self::Materialize(sink) => {
                visit(sink.handle, BreakerHandleKind::Materialized)?;
            }
            Self::CrossProductBuild(sink) => {
                visit(sink.handle, BreakerHandleKind::Materialized)?;
            }
            Self::HashJoinBuild(sink) => {
                visit(sink.handle, BreakerHandleKind::HashJoinBuild)?;
            }
            Self::HashAggregateBuild(sink) => {
                visit(sink.handle, BreakerHandleKind::Aggregate)?;
            }
            Self::UngroupedAggregate(sink) => {
                visit(sink.handle, BreakerHandleKind::Aggregate)?;
            }
            Self::PerfectHashAggregate(sink) => {
                visit(sink.handle, BreakerHandleKind::Aggregate)?;
            }
            Self::SortBuild(sink) => {
                visit(sink.handle, BreakerHandleKind::Sort)?;
            }
            Self::TopNBuild(sink) => {
                visit(sink.handle, BreakerHandleKind::TopN)?;
            }
            Self::WindowBuild(sink) => {
                visit(sink.handle, BreakerHandleKind::Window)?;
            }
            Self::SetOperationInput(sink) => {
                visit(sink.handle, BreakerHandleKind::SetOperation)?;
            }
            Self::CteMaterialize(sink) => {
                visit(sink.handle, BreakerHandleKind::Cte)?;
            }
            Self::DelimCapture(sink) => {
                visit(sink.handle, BreakerHandleKind::Delim)?;
                if let Some(cached_outer) = sink.cached_outer {
                    visit(cached_outer, BreakerHandleKind::Delim)?;
                }
            }
            Self::RecursiveTableAppend(sink) => {
                visit(sink.handle, BreakerHandleKind::RecursiveTable)?;
            }
            Self::ExternalTable(sink) => {
                visit(sink.handle, BreakerHandleKind::ExternalTable)?;
            }
            Self::ClientResult(_)
            | Self::Insert(_)
            | Self::Update(_)
            | Self::Delete(_)
            | Self::CopyToFile(_) => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct ClientResultSpec {
    pub required: RequiredProperties,
}

#[derive(Debug, Clone)]
pub struct MaterializeSinkSpec {
    pub handle: BreakerHandleId,
    pub required: RequiredProperties,
}

#[derive(Debug, Clone)]
pub struct CrossProductBuildSinkSpec {
    pub handle: BreakerHandleId,
    pub required: RequiredProperties,
}

#[derive(Debug, Clone)]
pub struct HashJoinBuildSinkSpec {
    pub handle: BreakerHandleId,
    pub join_type: JoinType,
    pub key_conditions: Box<[JoinCondition]>,
    pub residual_conditions: Box<[JoinCondition]>,
    pub build_projection: Box<[usize]>,
    pub build_payload_types: Box<[LogicalType]>,
    pub build_output_count: usize,
    pub grouped_reduction_channels: Option<usize>,
    pub required: RequiredProperties,
    pub force_external: bool,
}

#[derive(Debug, Clone)]
pub struct HashAggregateBuildSinkSpec {
    pub handle: BreakerHandleId,
    pub spec: AggregateSpec,
    pub required: RequiredProperties,
}

#[derive(Debug, Clone)]
pub struct UngroupedAggregateSinkSpec {
    pub handle: BreakerHandleId,
    pub spec: AggregateSpec,
    pub required: RequiredProperties,
}

#[derive(Debug, Clone)]
pub struct PerfectHashAggregateSinkSpec {
    pub handle: BreakerHandleId,
    pub spec: AggregateSpec,
    pub required: RequiredProperties,
}

#[derive(Debug, Clone)]
pub struct SortBuildSinkSpec {
    pub handle: BreakerHandleId,
    pub orders: Box<[OrderByNode]>,
    pub projection_map: Box<[usize]>,
    pub input_types: Box<[LogicalType]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
    pub force_external: bool,
    pub required: RequiredProperties,
}

#[derive(Debug, Clone)]
pub struct TopNBuildSinkSpec {
    pub handle: BreakerHandleId,
    pub spec: TopNSpec,
    pub required: RequiredProperties,
}

#[derive(Debug, Clone)]
pub struct WindowBuildSinkSpec {
    pub handle: BreakerHandleId,
    pub spec: WindowSpec,
    pub required: RequiredProperties,
}

#[derive(Debug, Clone)]
pub struct SetOperationInputSinkSpec {
    pub handle: BreakerHandleId,
    pub spec: SetOperationSpec,
    pub side: SetOperationInputSide,
    pub required: RequiredProperties,
}

#[derive(Debug, Clone)]
pub struct CteMaterializeSinkSpec {
    pub handle: BreakerHandleId,
    pub required: RequiredProperties,
}

#[derive(Debug, Clone)]
pub struct DelimCaptureSinkSpec {
    pub handle: BreakerHandleId,
    pub duplicate_keys: Box<[Expression]>,
    pub cached_outer: Option<BreakerHandleId>,
    pub required: RequiredProperties,
}

#[derive(Debug, Clone)]
pub struct RecursiveTableAppendSinkSpec {
    pub handle: BreakerHandleId,
    pub required: RequiredProperties,
}

#[derive(Debug, Clone)]
pub struct ExternalTableSinkSpec {
    pub handle: BreakerHandleId,
    pub spec: ExternalTableSpec,
    pub required: RequiredProperties,
}

#[derive(Debug, Clone)]
pub struct InsertSinkSpec {
    pub spec: InsertSpec,
    pub required: RequiredProperties,
}

#[derive(Debug, Clone)]
pub struct UpdateSinkSpec {
    pub spec: UpdateSpec,
    pub required: RequiredProperties,
}

#[derive(Debug, Clone)]
pub struct DeleteSinkSpec {
    pub spec: DeleteSpec,
    pub required: RequiredProperties,
}

#[derive(Debug, Clone)]
pub struct CopyToFileSinkSpec {
    pub spec: CopyToFileSpec,
    pub required: RequiredProperties,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkSharing {
    Exclusive,
    Shared(SharedSinkId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineRoot {
    Pipeline(PipelineId),
    ControlRegion(ControlRegionId),
    Utility(UtilityProgramId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineDependency {
    pub producer: PipelineId,
    pub consumer: PipelineId,
    pub kind: DependencyKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DependencyKind {
    BuildBeforeProbe,
    ProbeBeforeSpillReplay,
    MaterializeBeforeRead,
    FinalizeBeforeEmit,
    PrepareBeforeFinish,
    SideEffectOrder,
    /// Reserved for future explicit shared-sink barrier scheduling.
    /// Currently, shared-sink producer relationships are discovered via
    /// `PipelineGraph::shared_sink_producers()` scanning `SinkSharing::Shared` fields.
    SharedSinkInput,
    LoopEntry(ControlRegionId),
    LoopBack(ControlRegionId),
}

#[derive(Debug, Clone)]
pub enum ControlRegion {
    RecursiveCte(RecursiveCteRegion),
    CorrelatedSubquery(CorrelatedSubqueryRegion),
}

#[derive(Debug, Clone)]
pub struct RecursiveCteRegion {
    pub anchor: PipelineId,
    pub recursive: Vec<PipelineId>,
    pub emit: PipelineId,
    pub working: BreakerHandleId,
    pub intermediate: BreakerHandleId,
    pub accumulated: Option<BreakerHandleId>,
    pub termination: RecursiveTermination,
    pub dedup: RecursiveCteDedup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecursiveTermination {
    UntilEmpty,
    MaxIterations(usize),
    UntilEmptyOrMaxIterations(usize),
}

impl RecursiveTermination {
    pub fn allows_next_iteration(self, next_iteration: usize, has_delta: bool) -> bool {
        match self {
            Self::UntilEmpty => has_delta,
            Self::MaxIterations(max) => next_iteration <= max,
            Self::UntilEmptyOrMaxIterations(max) => has_delta && next_iteration <= max,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecursiveCteDedup {
    None,
    HashSet,
}

#[derive(Debug, Clone)]
pub struct CorrelatedSubqueryRegion {
    pub side: DelimJoinSide,
    pub capture: PipelineId,
    pub dependent_roots: Vec<PipelineSubgraphRoot>,
    pub join: PipelineId,
    pub delim_values: BreakerHandleId,
    pub cached_outer: Option<BreakerHandleId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelimJoinSide {
    Left,
    Right,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineSubgraphRoot {
    Pipeline(PipelineId),
    ControlRegion(ControlRegionId),
}
