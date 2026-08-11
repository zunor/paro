// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable runtime program image compiled from a lowered pipeline graph.

use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};
use paro_common::vector::VECTOR_SIZE;

use crate::physical::plan::PhysicalPlan;
use crate::physical::row_type::RowType;
use crate::physical::specs::UtilitySpec;
use crate::runtime::context::UtilityContext;
use crate::runtime::HandleRef;
use crate::runtime::{
    AdaptiveSearchSourceExec, ChunkSourceExec, ClassicIeJoinSourceExec, ClientResultSinkExec,
    CopyToFileSinkExec, CrossProductProbeTransformExec, CteMaterializeSinkExec, CteScanSourceExec,
    DeleteSinkExec, DelimCaptureSinkExec, DelimScanSourceExec, DummySourceExec, EmptySourceExec,
    ExpressionSourceExec, ExternalProjectTransformExec, ExternalTableSinkExec,
    ExternalTableSourceExec, FilterTransformExec, FullTextSearchSourceExec,
    GraphExpandTransformExec, GraphProjectTransformExec, GraphScanSourceExec,
    GraphShortestPathTransformExec, HashAggregateBuildSinkExec, HashAggregateEmitSourceExec,
    HashJoinBuildSinkExec, HashJoinProbeTransformExec, HashJoinSpillReplaySourceExec,
    HashJoinUnmatchedSourceExec, InsertSinkExec, MaterializeSinkExec, MaterializedSourceExec,
    NestedLoopJoinProbeTransformExec, NljUnmatchedSourceExec, OperatorRole,
    PerfectHashAggregateEmitSourceExec, PerfectHashAggregateSinkExec, ProjectTransformExec,
    PropertyRepairTransformExec, RecursiveTableAppendSinkExec, RecursiveTableScanSourceExec,
    RowsetSourceDesc, RowsetSourceExec, RuntimeOperatorOrigin, RuntimeRoleOrdinal,
    SetOperationEmitSourceExec, SetOperationInputSinkExec, SinkExec, SortBuildSinkExec,
    SortEmitSourceExec, SortRangeJoinProbeTransformExec, SourceExec, SparseVectorSearchSourceExec,
    StreamingLimitTransformExec, StreamingTopNTransformExec, StreamingWindowTransformExec,
    TableFunctionSourceExec, TopNBuildSinkExec, TopNEmitSourceExec, TransformExec,
    UngroupedAggregateEmitSourceExec, UngroupedAggregateSinkExec, UpdateSinkExec, ValuesSourceExec,
    VectorSearchSourceExec, WindowBuildSinkExec, WindowEmitSourceExec,
};
use crate::runtime::{ChunkLayout, PipelineScratchLayout, RuntimeOperatorId};

use super::graph::{
    ControlRegion, ControlRegionId, PipelineGraph, PipelineId, PipelineRoot, PipelineSpec,
    SinkSharing, SinkSpec, SourceSpec, TransformSpec,
};
use super::handles::{BreakerHandleCatalog, BreakerHandleKind};

#[derive(Debug, Clone)]
pub enum StatementProgram {
    Pipeline {
        plan: Arc<PhysicalPlan>,
        graph: Arc<PipelineGraph>,
        programs: PipelineProgramSet,
    },
    ExplainAnalyze {
        target: Box<StatementProgram>,
        spec: paro_planner::operator::ExplainSpec,
    },
    Utility(UtilityProgram),
}

#[derive(Debug)]
pub struct PipelineProgram {
    pub id: PipelineId,
    pub source: SourceSlot,
    pub transforms: Box<[TransformSlot]>,
    pub sink: SinkSlot,
    pub sink_sharing: SinkSharing,
    pub scratch: PipelineScratchLayout,
    pub properties: crate::physical::properties::PipelineProperties,
}

#[derive(Debug)]
pub struct SourceSlot {
    pub operator_id: RuntimeOperatorId,
    pub origin: RuntimeOperatorOrigin,
    pub exec: SourceExec,
}

#[derive(Debug)]
pub struct TransformSlot {
    pub operator_id: RuntimeOperatorId,
    pub origin: RuntimeOperatorOrigin,
    pub exec: TransformExec,
}

#[derive(Debug)]
pub struct SinkSlot {
    pub operator_id: RuntimeOperatorId,
    pub origin: RuntimeOperatorOrigin,
    pub exec: SinkExec,
}

#[derive(Debug, Clone)]
pub struct PipelineProgramSet {
    pub pipelines: Box<[Arc<PipelineProgram>]>,
    pub by_pipeline_id: PipelineIdMap<PipelineProgramIndex>,
    pub control_regions: Box<[ControlRegionProgram]>,
    pub root: PipelineRoot,
}

impl PipelineProgramSet {
    pub fn get(&self, id: PipelineId) -> Option<&Arc<PipelineProgram>> {
        self.by_pipeline_id
            .get(id)
            .and_then(|index| self.pipelines.get(index.index()))
    }

    #[inline]
    pub fn pipeline_count(&self) -> usize {
        self.pipelines.len()
    }
}

#[derive(Debug, Clone)]
pub struct PipelineIdMap<T> {
    entries: Box<[Option<T>]>,
}

impl<T: Copy> PipelineIdMap<T> {
    pub fn new(pipeline_count: usize) -> Self {
        let entries = std::iter::repeat_with(|| None)
            .take(pipeline_count)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { entries }
    }

    pub fn insert(&mut self, id: PipelineId, value: T) -> Result<()> {
        let Some(slot) = self.entries.get_mut(id.index()) else {
            return Err(paro_error::internal("pipeline id map insert out of bounds"));
        };
        *slot = Some(value);
        Ok(())
    }

    pub fn get(&self, id: PipelineId) -> Option<T> {
        self.entries.get(id.index()).and_then(|entry| *entry)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineProgramIndex(u32);

impl PipelineProgramIndex {
    pub fn new(index: usize) -> Self {
        assert!(
            index <= u32::MAX as usize,
            "pipeline program index exhausted"
        );
        Self(index as u32)
    }

    #[inline]
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug, Clone)]
pub struct ControlRegionProgram {
    pub id: ControlRegionId,
    pub kind: ControlRegionKind,
    pub pipelines: Box<[PipelineId]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlRegionKind {
    RecursiveCte,
    CorrelatedSubquery,
}

#[derive(Debug, Clone)]
pub struct UtilityProgram {
    pub spec: UtilitySpec,
}

impl UtilityProgram {
    pub fn run_once(
        &self,
        ctx: &mut UtilityContext<'_>,
    ) -> Result<crate::runtime::UtilityRunResult> {
        crate::runtime::run_utility_once(&self.spec, ctx)
    }
}

impl StatementProgram {
    pub fn pipeline(
        plan: Arc<PhysicalPlan>,
        graph: Arc<PipelineGraph>,
        programs: PipelineProgramSet,
    ) -> Self {
        Self::Pipeline {
            graph,
            programs,
            plan,
        }
    }

    pub fn from_physical_plan(plan: PhysicalPlan) -> Result<Self> {
        if let crate::physical::specs::PhysicalNodeKind::Utility(spec) = &plan.node(plan.root).kind
        {
            return Ok(Self::Utility(UtilityProgram { spec: spec.clone() }));
        }

        let plan = Arc::new(plan);
        let graph = {
            let mut lowerer = crate::pipeline::lowerer::PipelineLowerer::new(plan.as_ref());
            Arc::new(lowerer.lower_to_pipeline_graph(plan.root)?)
        };
        let programs = PipelineProgramBuilder::default().build_program_set(graph.as_ref())?;
        Ok(Self::pipeline(plan, graph, programs))
    }
}

#[derive(Debug, Default)]
pub struct PipelineProgramBuilder {
    registry: OperatorRuntimeRegistry,
}

impl PipelineProgramBuilder {
    pub fn new(registry: OperatorRuntimeRegistry) -> Self {
        Self { registry }
    }

    pub fn build_program(&self, spec: &PipelineSpec) -> Result<PipelineProgram> {
        let mut next_operator_id = 0usize;
        self.build_program_with_handles(
            spec,
            &BreakerHandleCatalog::default(),
            &mut next_operator_id,
        )
    }

    pub fn build_program_set(&self, graph: &PipelineGraph) -> Result<PipelineProgramSet> {
        graph.validate()?;
        let mut programs = Vec::with_capacity(graph.pipelines.len());
        let mut by_pipeline_id = PipelineIdMap::new(graph.pipelines.len());
        let mut next_operator_id = 0usize;
        for (dense_idx, spec) in graph.pipelines.iter().enumerate() {
            let program = Arc::new(self.build_program_with_handles(
                spec,
                &graph.handles,
                &mut next_operator_id,
            )?);
            by_pipeline_id.insert(spec.id, PipelineProgramIndex::new(dense_idx))?;
            programs.push(program);
        }

        let control_regions = graph
            .control_regions
            .iter()
            .enumerate()
            .map(|(idx, region)| control_region_program(ControlRegionId::new(idx), region))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Ok(PipelineProgramSet {
            pipelines: programs.into_boxed_slice(),
            by_pipeline_id,
            control_regions,
            root: graph.root,
        })
    }

    fn build_program_with_handles(
        &self,
        spec: &PipelineSpec,
        handles: &BreakerHandleCatalog,
        next_operator_id: &mut usize,
    ) -> Result<PipelineProgram> {
        validate_handles(spec, handles)?;
        let source = self.registry.source_slot(
            &spec.source,
            RuntimeOperatorOrigin::new(spec.id, OperatorRole::Source, RuntimeRoleOrdinal::new(0)),
            next_runtime_operator_id(next_operator_id),
        )?;
        let transforms = spec
            .transforms
            .iter()
            .enumerate()
            .map(|(idx, transform)| {
                self.registry.transform_slot(
                    transform,
                    RuntimeOperatorOrigin::new(
                        spec.id,
                        OperatorRole::Transform,
                        RuntimeRoleOrdinal::new(idx),
                    ),
                    next_runtime_operator_id(next_operator_id),
                )
            })
            .collect::<Result<Vec<_>>>()?
            .into_boxed_slice();
        let sink = self.registry.sink_slot(
            &spec.sink,
            RuntimeOperatorOrigin::new(spec.id, OperatorRole::Sink, RuntimeRoleOrdinal::new(0)),
            next_runtime_operator_id(next_operator_id),
        )?;
        let scratch = scratch_layout_for(spec, handles)?;

        Ok(PipelineProgram {
            id: spec.id,
            source,
            transforms,
            sink,
            sink_sharing: spec.sink_sharing,
            scratch,
            properties: spec.properties.clone(),
        })
    }
}

#[derive(Debug, Default)]
pub struct OperatorRuntimeRegistry {
    extensions: Vec<Arc<dyn ExtensionOperatorFactory>>,
}

impl OperatorRuntimeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_extension(mut self, extension: Arc<dyn ExtensionOperatorFactory>) -> Self {
        self.extensions.push(extension);
        self
    }

    pub fn source_slot(
        &self,
        spec: &SourceSpec,
        origin: RuntimeOperatorOrigin,
        operator_id: RuntimeOperatorId,
    ) -> Result<SourceSlot> {
        let exec = match spec {
            SourceSpec::Rowset(spec) => SourceExec::Rowset(RowsetSourceExec {
                desc: RowsetSourceDesc::from_source_spec(spec),
            }),
            SourceSpec::Values(spec) => SourceExec::Values(ValuesSourceExec { spec: spec.clone() }),
            SourceSpec::Dummy(spec) => SourceExec::Dummy(DummySourceExec { spec: spec.clone() }),
            SourceSpec::Empty(spec) => SourceExec::Empty(EmptySourceExec { spec: spec.clone() }),
            SourceSpec::Chunk(spec) => SourceExec::Chunk(ChunkSourceExec { spec: spec.clone() }),
            SourceSpec::Expression(spec) => {
                SourceExec::Expression(ExpressionSourceExec { spec: spec.clone() })
            }
            SourceSpec::TableFunction(spec) => {
                SourceExec::TableFunction(TableFunctionSourceExec { spec: spec.clone() })
            }
            SourceSpec::VectorSearch(spec) => {
                SourceExec::VectorSearch(VectorSearchSourceExec { spec: spec.clone() })
            }
            SourceSpec::SparseVectorSearch(spec) => {
                SourceExec::SparseVectorSearch(SparseVectorSearchSourceExec { spec: spec.clone() })
            }
            SourceSpec::FullTextSearch(spec) => {
                SourceExec::FullTextSearch(FullTextSearchSourceExec { spec: spec.clone() })
            }
            SourceSpec::AdaptiveSearch(spec) => {
                SourceExec::AdaptiveSearch(AdaptiveSearchSourceExec { spec: spec.clone() })
            }
            SourceSpec::GraphScan(spec) => {
                SourceExec::GraphScan(GraphScanSourceExec { spec: spec.clone() })
            }
            SourceSpec::ExternalTable(spec) => SourceExec::ExternalTable(ExternalTableSourceExec {
                handle: HandleRef::new(spec.handle),
            }),
            SourceSpec::Materialized(spec) => SourceExec::Materialized(MaterializedSourceExec {
                handle: HandleRef::new(spec.handle),
            }),
            SourceSpec::ClassicIeJoin(spec) => SourceExec::ClassicIeJoin(ClassicIeJoinSourceExec {
                left_handle: HandleRef::new(spec.left_handle),
                right_handle: HandleRef::new(spec.right_handle),
                join_type: spec.spec.join_type,
                conditions: spec.spec.conditions.clone(),
                mark_null_condition_start: spec.spec.mark_null_condition_start,
                left_projection: spec.spec.left_projection.clone(),
                right_projection: spec.spec.right_projection.clone(),
                right_output_types: spec.spec.right_output_types.clone(),
                output_types: spec.spec.output_types.clone(),
            }),
            SourceSpec::NljUnmatched(spec) => SourceExec::NljUnmatched(NljUnmatchedSourceExec {
                handle: HandleRef::new(spec.handle),
                join_type: spec.join_type,
                left_output_types: spec.left_output_types.clone(),
                right_projection: spec.right_projection.clone(),
                output_types: spec.output_types.clone(),
            }),
            SourceSpec::HashJoinSpillReplay(spec) => {
                SourceExec::HashJoinSpillReplay(HashJoinSpillReplaySourceExec {
                    handle: HandleRef::new(spec.handle),
                    join_type: spec.join_type,
                    anti_join_mode: spec.anti_join_mode,
                    conditions: spec.conditions.clone(),
                    probe_types: spec.probe_types.clone(),
                    build_payload_types: spec.build_payload_types.clone(),
                    left_projection: spec.left_projection.clone(),
                    output_types: spec.output_types.clone(),
                })
            }
            SourceSpec::HashJoinUnmatched(spec) => {
                SourceExec::HashJoinUnmatched(HashJoinUnmatchedSourceExec {
                    handle: HandleRef::new(spec.handle),
                    join_type: spec.join_type,
                    left_output_types: spec.left_output_types.clone(),
                    output_types: spec.output_types.clone(),
                })
            }
            SourceSpec::HashAggregateEmit(spec) => {
                SourceExec::HashAggregateEmit(HashAggregateEmitSourceExec {
                    handle: HandleRef::new(spec.handle),
                    spec: spec.spec.clone(),
                })
            }
            SourceSpec::UngroupedAggregateEmit(spec) => {
                SourceExec::UngroupedAggregateEmit(UngroupedAggregateEmitSourceExec {
                    handle: HandleRef::new(spec.handle),
                    spec: spec.spec.clone(),
                })
            }
            SourceSpec::PerfectHashAggregateEmit(spec) => {
                SourceExec::PerfectHashAggregateEmit(PerfectHashAggregateEmitSourceExec {
                    handle: HandleRef::new(spec.handle),
                    spec: spec.spec.clone(),
                })
            }
            SourceSpec::SortEmit(spec) => SourceExec::SortEmit(SortEmitSourceExec {
                handle: HandleRef::new(spec.handle),
            }),
            SourceSpec::TopNEmit(spec) => SourceExec::TopNEmit(TopNEmitSourceExec {
                handle: HandleRef::new(spec.handle),
            }),
            SourceSpec::WindowEmit(spec) => SourceExec::WindowEmit(WindowEmitSourceExec {
                handle: HandleRef::new(spec.handle),
                spec: spec.spec.clone(),
            }),
            SourceSpec::SetOperationEmit(spec) => {
                SourceExec::SetOperationEmit(SetOperationEmitSourceExec {
                    handle: HandleRef::new(spec.handle),
                })
            }
            SourceSpec::CteScan(spec) => SourceExec::CteScan(CteScanSourceExec {
                handle: HandleRef::new(spec.handle),
            }),
            SourceSpec::DelimScan(spec) => SourceExec::DelimScan(DelimScanSourceExec {
                handle: HandleRef::new(spec.handle),
            }),
            SourceSpec::RecursiveTableScan(spec) => {
                SourceExec::RecursiveTableScan(RecursiveTableScanSourceExec {
                    handle: HandleRef::new(spec.handle),
                })
            }
        };
        Ok(SourceSlot {
            operator_id,
            origin,
            exec,
        })
    }

    pub fn transform_slot(
        &self,
        spec: &TransformSpec,
        origin: RuntimeOperatorOrigin,
        operator_id: RuntimeOperatorId,
    ) -> Result<TransformSlot> {
        let exec = match spec {
            TransformSpec::Filter(spec) => {
                TransformExec::Filter(FilterTransformExec { spec: spec.clone() })
            }
            TransformSpec::Project(spec) => {
                TransformExec::Project(ProjectTransformExec { spec: spec.clone() })
            }
            TransformSpec::HashJoinProbe(spec) => {
                TransformExec::HashJoinProbe(HashJoinProbeTransformExec {
                    handle: HandleRef::new(spec.handle),
                    join_type: spec.join_type,
                    anti_join_mode: spec.anti_join_mode,
                    conditions: spec.conditions.clone(),
                    left_projection: spec.left_projection.clone(),
                    output_types: spec.output_types.clone(),
                })
            }
            TransformSpec::NestedLoopJoinProbe(spec) => {
                TransformExec::NestedLoopJoinProbe(NestedLoopJoinProbeTransformExec {
                    handle: HandleRef::new(spec.handle),
                    join_type: spec.join_type,
                    conditions: spec.conditions.clone(),
                    mark_null_condition_start: spec.mark_null_condition_start,
                    arbitrary_condition: spec.arbitrary_condition.clone(),
                    left_projection: spec.left_projection.clone(),
                    right_projection: spec.right_projection.clone(),
                    right_output_types: spec.right_output_types.clone(),
                    output_types: spec.output_types.clone(),
                })
            }
            TransformSpec::SortRangeJoinProbe(spec) => {
                TransformExec::SortRangeJoinProbe(SortRangeJoinProbeTransformExec {
                    handle: HandleRef::new(spec.handle),
                    join_type: spec.join_type,
                    conditions: spec.conditions.clone(),
                    mark_null_condition_start: spec.mark_null_condition_start,
                    left_projection: spec.left_projection.clone(),
                    right_projection: spec.right_projection.clone(),
                    right_output_types: spec.right_output_types.clone(),
                    output_types: spec.output_types.clone(),
                })
            }
            TransformSpec::CrossProductProbe(spec) => {
                TransformExec::CrossProductProbe(CrossProductProbeTransformExec {
                    handle: HandleRef::new(spec.handle),
                    left_column_count: spec.left_column_count,
                    output_types: spec.output_types.clone(),
                })
            }
            TransformSpec::Limit(spec) => {
                TransformExec::StreamingLimit(StreamingLimitTransformExec { spec: spec.clone() })
            }
            TransformSpec::StreamingTopN(spec) => {
                TransformExec::StreamingTopN(StreamingTopNTransformExec { spec: spec.clone() })
            }
            TransformSpec::StreamingWindow(spec) => {
                TransformExec::StreamingWindow(StreamingWindowTransformExec { spec: spec.clone() })
            }
            TransformSpec::ExternalProject(spec) => {
                TransformExec::ExternalProject(ExternalProjectTransformExec { spec: spec.clone() })
            }
            TransformSpec::GraphExpand(spec) => {
                TransformExec::GraphExpand(GraphExpandTransformExec { spec: spec.clone() })
            }
            TransformSpec::GraphProject(spec) => {
                TransformExec::GraphProject(GraphProjectTransformExec { spec: spec.clone() })
            }
            TransformSpec::GraphShortestPath(spec) => {
                TransformExec::GraphShortestPath(GraphShortestPathTransformExec {
                    spec: spec.clone(),
                })
            }
            TransformSpec::PropertyRepair(spec) => TransformExec::PropertyRepair(
                PropertyRepairTransformExec::try_new(spec.kind.clone())?,
            ),
        };
        Ok(TransformSlot {
            operator_id,
            origin,
            exec,
        })
    }

    pub fn sink_slot(
        &self,
        spec: &SinkSpec,
        origin: RuntimeOperatorOrigin,
        operator_id: RuntimeOperatorId,
    ) -> Result<SinkSlot> {
        let exec = match spec {
            SinkSpec::ClientResult(spec) => {
                SinkExec::ClientResult(ClientResultSinkExec { spec: spec.clone() })
            }
            SinkSpec::Materialize(spec) => SinkExec::Materialize(MaterializeSinkExec {
                handle: HandleRef::new(spec.handle),
                required: spec.required.clone(),
            }),
            SinkSpec::CrossProductBuild(spec) => SinkExec::Materialize(MaterializeSinkExec {
                handle: HandleRef::new(spec.handle),
                required: spec.required.clone(),
            }),
            SinkSpec::HashJoinBuild(spec) => SinkExec::HashJoinBuild(HashJoinBuildSinkExec {
                handle: HandleRef::new(spec.handle),
                join_type: spec.join_type,
                conditions: spec.conditions.clone(),
                build_projection: spec.build_projection.clone(),
                build_payload_types: spec.build_payload_types.clone(),
                required: spec.required.clone(),
                force_external: spec.force_external,
            }),
            SinkSpec::HashAggregateBuild(spec) => {
                SinkExec::HashAggregateBuild(HashAggregateBuildSinkExec {
                    handle: HandleRef::new(spec.handle),
                    spec: spec.spec.clone(),
                    required: spec.required.clone(),
                })
            }
            SinkSpec::UngroupedAggregate(spec) => {
                SinkExec::UngroupedAggregate(UngroupedAggregateSinkExec {
                    handle: HandleRef::new(spec.handle),
                    spec: spec.spec.clone(),
                    required: spec.required.clone(),
                })
            }
            SinkSpec::PerfectHashAggregate(spec) => {
                SinkExec::PerfectHashAggregate(PerfectHashAggregateSinkExec {
                    handle: HandleRef::new(spec.handle),
                    spec: spec.spec.clone(),
                    required: spec.required.clone(),
                })
            }
            SinkSpec::SortBuild(spec) => SinkExec::SortBuild(SortBuildSinkExec {
                handle: HandleRef::new(spec.handle),
                orders: spec.orders.clone(),
                projection_map: spec.projection_map.clone(),
                input_types: spec.input_types.clone(),
                output_names: spec.output_names.clone(),
                output_types: spec.output_types.clone(),
                force_external: spec.force_external,
                required: spec.required.clone(),
            }),
            SinkSpec::TopNBuild(spec) => SinkExec::TopNBuild(TopNBuildSinkExec {
                handle: HandleRef::new(spec.handle),
                spec: spec.spec.clone(),
                required: spec.required.clone(),
            }),
            SinkSpec::WindowBuild(spec) => SinkExec::WindowBuild(WindowBuildSinkExec {
                handle: HandleRef::new(spec.handle),
                spec: spec.spec.clone(),
                required: spec.required.clone(),
            }),
            SinkSpec::SetOperationInput(spec) => {
                SinkExec::SetOperationInput(SetOperationInputSinkExec {
                    handle: HandleRef::new(spec.handle),
                    spec: spec.spec.clone(),
                    side: spec.side,
                    required: spec.required.clone(),
                })
            }
            SinkSpec::CteMaterialize(spec) => SinkExec::CteMaterialize(CteMaterializeSinkExec {
                handle: HandleRef::new(spec.handle),
                required: spec.required.clone(),
            }),
            SinkSpec::DelimCapture(spec) => SinkExec::DelimCapture(DelimCaptureSinkExec {
                handle: HandleRef::new(spec.handle),
                duplicate_keys: spec.duplicate_keys.clone(),
                cached_outer: spec.cached_outer.map(HandleRef::new),
                required: spec.required.clone(),
            }),
            SinkSpec::RecursiveTableAppend(spec) => {
                SinkExec::RecursiveTableAppend(RecursiveTableAppendSinkExec {
                    handle: HandleRef::new(spec.handle),
                    required: spec.required.clone(),
                })
            }
            SinkSpec::ExternalTable(spec) => SinkExec::ExternalTable(ExternalTableSinkExec {
                handle: HandleRef::new(spec.handle),
                spec: spec.spec.clone(),
                required: spec.required.clone(),
            }),
            SinkSpec::Insert(spec) => SinkExec::Insert(InsertSinkExec {
                spec: spec.spec.clone(),
                required: spec.required.clone(),
            }),
            SinkSpec::Update(spec) => SinkExec::Update(UpdateSinkExec {
                spec: spec.spec.clone(),
                required: spec.required.clone(),
            }),
            SinkSpec::Delete(spec) => SinkExec::Delete(DeleteSinkExec {
                spec: spec.spec.clone(),
                required: spec.required.clone(),
            }),
            SinkSpec::CopyToFile(spec) => SinkExec::CopyToFile(CopyToFileSinkExec {
                spec: spec.spec.clone(),
                required: spec.required.clone(),
            }),
        };
        Ok(SinkSlot {
            operator_id,
            origin,
            exec,
        })
    }
}

pub trait ExtensionOperatorFactory: Send + Sync + std::fmt::Debug {
    fn source_slot(
        &self,
        spec: &ExtensionSourceSpec,
        origin: RuntimeOperatorOrigin,
        operator_id: RuntimeOperatorId,
    ) -> Result<SourceSlot>;
    fn transform_slot(
        &self,
        spec: &ExtensionTransformSpec,
        origin: RuntimeOperatorOrigin,
        operator_id: RuntimeOperatorId,
    ) -> Result<TransformSlot>;
    fn sink_slot(
        &self,
        spec: &ExtensionSinkSpec,
        origin: RuntimeOperatorOrigin,
        operator_id: RuntimeOperatorId,
    ) -> Result<SinkSlot>;
}

#[derive(Debug)]
pub struct ExtensionSourceSpec;

#[derive(Debug)]
pub struct ExtensionTransformSpec;

#[derive(Debug)]
pub struct ExtensionSinkSpec;

fn validate_handles(spec: &PipelineSpec, handles: &BreakerHandleCatalog) -> Result<()> {
    spec.source
        .visit_expected_handles(|handle, kind| validate_handle_kind(handles, handle, kind))?;
    for transform in &spec.transforms {
        transform
            .visit_expected_handles(|handle, kind| validate_handle_kind(handles, handle, kind))?;
    }
    spec.sink
        .visit_expected_handles(|handle, kind| validate_handle_kind(handles, handle, kind))?;
    Ok(())
}

fn validate_handle_kind(
    handles: &BreakerHandleCatalog,
    handle: super::handles::BreakerHandleId,
    expected: BreakerHandleKind,
) -> Result<()> {
    let Some(entry) = handles.get(handle) else {
        return Err(paro_error::internal(
            "breaker role references unknown handle",
        ));
    };
    if entry.kind != expected {
        return Err(paro_error::internal(format!(
            "breaker role handle kind mismatch: expected {:?}, got {:?}",
            expected, entry.kind
        )));
    }
    Ok(())
}

fn scratch_layout_for(
    spec: &PipelineSpec,
    handles: &BreakerHandleCatalog,
) -> Result<PipelineScratchLayout> {
    let source = source_output_row_type(&spec.source, &spec.output, handles)?;
    let mut current = source.clone();
    let mut transform_layouts = Vec::with_capacity(spec.transforms.len());
    for transform in &spec.transforms {
        current = transform.output_row_type(&current);
        transform_layouts.push(transform_chunk_layout(transform, &current));
    }
    Ok(PipelineScratchLayout::new(
        chunk_layout(&source),
        transform_layouts,
        spec.output.column_count().max(1),
    ))
}

fn chunk_layout(row_type: &RowType) -> ChunkLayout {
    ChunkLayout::new(row_type.types.to_vec(), VECTOR_SIZE)
}

fn transform_chunk_layout(transform: &TransformSpec, row_type: &RowType) -> ChunkLayout {
    match transform {
        TransformSpec::Filter(_) | TransformSpec::Limit(_) => {
            ChunkLayout::view(row_type.types.to_vec(), VECTOR_SIZE)
        }
        _ => chunk_layout(row_type),
    }
}

fn source_output_row_type(
    source: &SourceSpec,
    fallback: &RowType,
    handles: &BreakerHandleCatalog,
) -> Result<RowType> {
    let handle = match source {
        SourceSpec::Materialized(source) => Some(source.handle),
        SourceSpec::CteScan(source) => Some(source.handle),
        SourceSpec::DelimScan(source) => Some(source.handle),
        SourceSpec::RecursiveTableScan(source) => Some(source.handle),
        _ => None,
    };
    let Some(handle) = handle else {
        return Ok(source.output_row_type(fallback));
    };
    handles
        .get(handle)
        .map(|entry| entry.row_type.clone())
        .ok_or_else(|| paro_error::internal("handle-backed source references unknown handle"))
}

fn next_runtime_operator_id(next: &mut usize) -> RuntimeOperatorId {
    let id = RuntimeOperatorId::new(*next);
    *next = next
        .checked_add(1)
        .expect("runtime operator id counter exhausted");
    id
}

fn control_region_program(id: ControlRegionId, region: &ControlRegion) -> ControlRegionProgram {
    match region {
        ControlRegion::RecursiveCte(region) => {
            let mut pipelines = Vec::with_capacity(2 + region.recursive.len());
            pipelines.push(region.anchor);
            pipelines.extend(region.recursive.iter().copied());
            pipelines.push(region.emit);
            ControlRegionProgram {
                id,
                kind: ControlRegionKind::RecursiveCte,
                pipelines: pipelines.into_boxed_slice(),
            }
        }
        ControlRegion::CorrelatedSubquery(region) => {
            let mut pipelines = Vec::with_capacity(2 + region.dependent_roots.len());
            pipelines.push(region.capture);
            pipelines.extend(region.dependent_roots.iter().filter_map(|root| match root {
                super::graph::PipelineSubgraphRoot::Pipeline(id) => Some(*id),
                super::graph::PipelineSubgraphRoot::ControlRegion(_) => None,
            }));
            pipelines.push(region.join);
            ControlRegionProgram {
                id,
                kind: ControlRegionKind::CorrelatedSubquery,
                pipelines: pipelines.into_boxed_slice(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_catalog::entry::{
        ColumnDefinition, EdgeTableInfo, TableCatalogEntry, VertexTableInfo,
    };
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_function::window::WindowFunction;
    use paro_planner::expression::{
        ConstantExpression, Expression, ReferenceExpression, WindowExpression, WindowFrame,
    };
    use paro_planner::operator::join::{AntiJoinMode, JoinCondition, JoinType};

    use crate::physical::properties::{
        NullOrdering, OrderingColumn, OrderingDirection, OrderingSpec, PipelineProperties,
        PropertyRepairKind,
    };
    use crate::physical::row_type::RowType;
    use crate::physical::specs::{
        AggregateSpec, DeleteSpec, DummyScanSpec, FilterSpec, GraphExpandSpec, GraphScanSpec,
        LimitSpec, ProjectSpec, ValuesSpec, WindowSpec,
    };
    use crate::runtime::{OperatorRole, RuntimeRoleOrdinal};

    use super::*;
    use crate::pipeline::graph::{
        ClientResultSpec, ControlRegion, PipelineGraph, PipelineRoot, PipelineSpec,
        RecursiveCteDedup, RecursiveCteRegion, RecursiveTermination, SinkSharing,
    };
    use crate::pipeline::handles::{
        BreakerHandleCatalogBuilder, BreakerHandleId, BreakerHandleKind,
    };

    fn row_type(names: &[&str], types: &[LogicalType]) -> RowType {
        RowType::new(
            names.iter().map(|name| (*name).to_string()).collect(),
            types.to_vec(),
        )
    }

    fn values_source() -> SourceSpec {
        SourceSpec::Values(ValuesSpec {
            table_index: 7,
            expressions: Box::new([]),
            output_names: Box::new(["a".to_string(), "b".to_string()]),
            output_types: Box::new([LogicalType::Integer, LogicalType::Boolean]),
        })
    }

    fn client_sink() -> SinkSpec {
        SinkSpec::ClientResult(ClientResultSpec::default())
    }

    fn test_table() -> Arc<TableCatalogEntry> {
        let storage = Arc::new(
            paro_storage::table::table_factory::TableFactory::default()
                .create_table(&[LogicalType::Integer])
                .expect("table storage"),
        );
        Arc::new(TableCatalogEntry::new(
            "paro".to_string(),
            "public".to_string(),
            "t".to_string(),
            vec![ColumnDefinition::new(
                "id".to_string(),
                LogicalType::Integer,
            )],
            storage,
            paro_catalog::entry::CatalogObjectId::from_raw(10_001),
            0,
        ))
    }

    fn join_condition() -> JoinCondition {
        JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        )
    }

    fn empty_aggregate_spec() -> AggregateSpec {
        AggregateSpec {
            grouping_key_count: 0,
            estimated_input_rows: None,
            projection_exprs: Box::new([]),
            payload_types: Box::new([]),
            groups: Box::new([]),
            group_key_encodings: Box::new([]),
            grouping_sets: Box::new([]),
            aggregates: Box::new([]),
            grouping_functions: Box::new([]),
            aggregate_inputs: Box::new([]),
            aggregate_filters: Box::new([]),
            aggregate_orders: Box::new([]),
            having_filter: Box::new([]),
            perfect_hash: None,
            output_names: Box::new(["a".to_string()]),
            output_types: Box::new([LogicalType::Integer]),
        }
    }

    fn order_by_first_column() -> paro_planner::binder::ir::OrderByNode {
        paro_planner::binder::ir::OrderByNode {
            expression: Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            ascending: true,
            nulls_first: false,
        }
    }

    fn topn_spec() -> crate::physical::specs::TopNSpec {
        crate::physical::specs::TopNSpec {
            orders: vec![order_by_first_column()].into_boxed_slice(),
            limit: 2,
            offset: 0,
            hnsw_ef_hint: None,
            output_names: Box::new(["a".to_string()]),
            output_types: Box::new([LogicalType::Integer]),
        }
    }

    fn window_spec() -> WindowSpec {
        WindowSpec {
            window_index: 1,
            expressions: vec![WindowExpression {
                function: WindowFunction::row_number(),
                children: Vec::new(),
                partitions: Vec::new(),
                orders: Vec::new(),
                frame: WindowFrame::default(),
                ignore_nulls: false,
                return_type: LogicalType::BigInt,
            }]
            .into_boxed_slice(),
            input_width: 1,
            output_names: Box::new(["a".to_string(), "rn".to_string()]),
            output_types: Box::new([LogicalType::Integer, LogicalType::BigInt]),
        }
    }

    fn simple_pipeline(id: usize) -> PipelineSpec {
        PipelineSpec {
            id: PipelineId::new(id),
            source: SourceSpec::Dummy(DummyScanSpec),
            transforms: Vec::new(),
            sink: client_sink(),
            sink_sharing: SinkSharing::Exclusive,
            properties: PipelineProperties::default(),
            output: row_type(&[], &[]),
        }
    }

    #[test]
    fn program_builder_compiles_graph_roles_and_dml_sinks() {
        let graph_scan = GraphScanSpec {
            vertex_info: VertexTableInfo {
                table_name: "vertices".to_string(),
                table_oid: 1,
                key_column_ids: vec![0],
                label: "Person".to_string(),
                property_column_ids: vec![],
            },
            filter: None,
            table_index: 10,
            label: "Person".to_string(),
            graph_name: "g".to_string(),
            schema_name: "public".to_string(),
            output_types: Box::new([LogicalType::UBigInt, LogicalType::UBigInt]),
        };
        let graph_expand = GraphExpandSpec {
            graph_name: "g".to_string(),
            schema_name: "public".to_string(),
            edge_info: EdgeTableInfo {
                table_name: "knows".to_string(),
                table_oid: 2,
                key_column_ids: vec![0],
                source_key_column_ids: vec![0],
                source_vertex_table: "vertices".to_string(),
                source_ref_column_ids: vec![1],
                destination_key_column_ids: vec![0],
                destination_vertex_table: "vertices".to_string(),
                destination_ref_column_ids: vec![2],
                label: "KNOWS".to_string(),
                property_column_ids: vec![],
            },
            direction: paro_planner::operator::ExpandDirection::Forward,
            source_label: "Person".to_string(),
            edge_filter: None,
            target_filter: None,
            source_table_index: 10,
            edge_table_index: 11,
            target_table_index: 12,
            target_label: "Person".to_string(),
            source_local_col_idx: 0,
            source_rowid_col_idx: 1,
            min_hops: 1,
            max_hops: 1,
            source_table_oid: 1,
            target_table_oid: 1,
            target_table_name: "vertices".to_string(),
            has_path_functions: false,
            output_names: Box::new([
                "src_local".to_string(),
                "src_rowid".to_string(),
                "edge_rowid".to_string(),
                "dst_local".to_string(),
                "dst_rowid".to_string(),
            ]),
            output_types: Box::new([
                LogicalType::UBigInt,
                LogicalType::UBigInt,
                LogicalType::UBigInt,
                LogicalType::UBigInt,
                LogicalType::UBigInt,
            ]),
        };
        let spec = PipelineSpec {
            id: PipelineId::new(0),
            source: SourceSpec::GraphScan(graph_scan),
            transforms: vec![TransformSpec::GraphExpand(graph_expand)],
            sink: SinkSpec::Delete(crate::pipeline::graph::DeleteSinkSpec {
                spec: DeleteSpec {
                    table: test_table(),
                    row_id_index: 1,
                    is_full_table_delete: false,
                },
                required: Default::default(),
            }),
            sink_sharing: SinkSharing::Exclusive,
            properties: PipelineProperties::default(),
            output: row_type(&["Count"], &[LogicalType::BigInt]),
        };

        let program = PipelineProgramBuilder::default()
            .build_program(&spec)
            .expect("graph role program should build");
        assert!(matches!(program.source.exec, SourceExec::GraphScan(_)));
        assert!(matches!(
            program.transforms[0].exec,
            TransformExec::GraphExpand(_)
        ));
        assert!(matches!(program.sink.exec, SinkExec::Delete(_)));
    }

    #[test]
    fn program_builder_compiles_builtin_slots_without_dyn_state() {
        let output = row_type(&["a"], &[LogicalType::Integer]);
        let spec = PipelineSpec {
            id: PipelineId::new(0),
            source: values_source(),
            transforms: vec![
                TransformSpec::Filter(FilterSpec {
                    expressions: Box::new([]),
                    projection_map: Box::new([0]),
                }),
                TransformSpec::Project(ProjectSpec {
                    expressions: Box::new([Expression::Reference(ReferenceExpression::new(
                        0,
                        LogicalType::Integer,
                    ))]),
                    output_names: Box::new(["a".to_string()]),
                }),
                TransformSpec::Limit(LimitSpec {
                    limit: Some(Expression::Constant(ConstantExpression::new(
                        Value::Integer(10),
                        LogicalType::Integer,
                    ))),
                    offset: None,
                    hnsw_ef_hint: None,
                }),
            ],
            sink: client_sink(),
            sink_sharing: SinkSharing::Exclusive,
            properties: PipelineProperties::default(),
            output: output.clone(),
        };

        let program = PipelineProgramBuilder::default()
            .build_program(&spec)
            .expect("program build should succeed");

        assert!(matches!(program.source.exec, SourceExec::Values(_)));
        assert!(matches!(
            program.transforms[0].exec,
            TransformExec::Filter(_)
        ));
        assert!(matches!(
            program.transforms[1].exec,
            TransformExec::Project(_)
        ));
        assert!(matches!(
            program.transforms[2].exec,
            TransformExec::StreamingLimit(_)
        ));
        assert!(matches!(program.sink.exec, SinkExec::ClientResult(_)));
        assert_eq!(program.source.operator_id, RuntimeOperatorId::new(0));
        assert_eq!(program.transforms[0].operator_id, RuntimeOperatorId::new(1));
        assert_eq!(program.transforms[1].operator_id, RuntimeOperatorId::new(2));
        assert_eq!(program.transforms[2].operator_id, RuntimeOperatorId::new(3));
        assert_eq!(program.sink.operator_id, RuntimeOperatorId::new(4));
        assert_eq!(
            program.transforms[1].origin,
            RuntimeOperatorOrigin::new(
                PipelineId::new(0),
                OperatorRole::Transform,
                RuntimeRoleOrdinal::new(1),
            )
        );
        assert_eq!(program.scratch.source_output.types.len(), 2);
        assert_eq!(program.scratch.transform_outputs.len(), 3);
        assert_eq!(
            program.scratch.transform_outputs[0].kind,
            crate::runtime::ChunkLayoutKind::View
        );
        assert_eq!(
            program.scratch.transform_outputs[1].kind,
            crate::runtime::ChunkLayoutKind::Materialized
        );
        assert_eq!(
            program.scratch.transform_outputs[2].kind,
            crate::runtime::ChunkLayoutKind::View
        );
        assert_eq!(program.scratch.transform_outputs[1].types, output.types);
    }

    #[test]
    fn program_set_keeps_dense_pipeline_id_mapping_and_control_regions() {
        let mut handles = BreakerHandleCatalogBuilder::default();
        let working = handles.register(
            BreakerHandleKind::RecursiveTable,
            row_type(&["a"], &[LogicalType::Integer]),
            PipelineProperties::default(),
        );
        let intermediate = handles.register(
            BreakerHandleKind::RecursiveTable,
            row_type(&["a"], &[LogicalType::Integer]),
            PipelineProperties::default(),
        );
        let graph = PipelineGraph {
            pipelines: vec![simple_pipeline(0), simple_pipeline(1), simple_pipeline(2)],
            dependencies: Vec::new(),
            handles: handles.finish(),
            control_regions: vec![ControlRegion::RecursiveCte(RecursiveCteRegion {
                anchor: PipelineId::new(0),
                recursive: vec![PipelineId::new(1)],
                emit: PipelineId::new(2),
                working,
                intermediate,
                accumulated: None,
                termination: RecursiveTermination::UntilEmpty,
                dedup: RecursiveCteDedup::None,
            })],
            root: PipelineRoot::ControlRegion(ControlRegionId::new(0)),
        };

        let set = PipelineProgramBuilder::default()
            .build_program_set(&graph)
            .expect("program set should build");

        assert_eq!(set.pipeline_count(), 3);
        assert_eq!(
            set.get(PipelineId::new(1)).expect("pipeline 1").id,
            PipelineId::new(1)
        );
        assert_eq!(set.control_regions.len(), 1);
        assert_eq!(set.control_regions[0].kind, ControlRegionKind::RecursiveCte);
        assert_eq!(
            set.control_regions[0].pipelines.as_ref(),
            &[PipelineId::new(0), PipelineId::new(1), PipelineId::new(2)]
        );
    }

    #[test]
    fn materialized_slots_fail_fast_when_handle_metadata_is_missing() {
        let spec = PipelineSpec {
            id: PipelineId::new(0),
            source: SourceSpec::Materialized(super::super::graph::MaterializedSourceSpec {
                handle: BreakerHandleId::new(0),
            }),
            transforms: Vec::new(),
            sink: client_sink(),
            sink_sharing: SinkSharing::Exclusive,
            properties: PipelineProperties::default(),
            output: row_type(&["a"], &[LogicalType::Integer]),
        };

        let err = PipelineProgramBuilder::default()
            .build_program(&spec)
            .expect_err("unknown materialized handle should fail");
        assert!(err.to_string().contains("unknown handle"));
    }

    #[test]
    fn blocking_property_repair_transform_fails_program_build() {
        let output = row_type(&["a"], &[LogicalType::Integer]);
        let blocking_repairs = [
            PropertyRepairKind::Sort(OrderingSpec::new(vec![OrderingColumn {
                column: 0,
                direction: OrderingDirection::Asc,
                nulls: NullOrdering::Last,
            }])),
            PropertyRepairKind::MaterializationAdapter,
        ];

        for repair in blocking_repairs {
            let spec = PipelineSpec {
                id: PipelineId::new(0),
                source: values_source(),
                transforms: vec![TransformSpec::PropertyRepair(
                    super::super::graph::PropertyRepairSpec { kind: repair },
                )],
                sink: client_sink(),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: output.clone(),
            };

            let err = PipelineProgramBuilder::default()
                .build_program(&spec)
                .expect_err("blocking repair should fail before runtime execution");
            assert!(err
                .to_string()
                .contains("blocking property repair must be lowered into breaker pipelines"));
        }
    }

    #[test]
    fn materialized_slots_store_typed_handle_refs_for_runtime_binding() {
        let mut handles = BreakerHandleCatalogBuilder::default();
        let handle = handles.register(
            BreakerHandleKind::Materialized,
            row_type(&["a"], &[LogicalType::Integer]),
            PipelineProperties::default(),
        );
        let spec = PipelineSpec {
            id: PipelineId::new(0),
            source: SourceSpec::Materialized(super::super::graph::MaterializedSourceSpec {
                handle,
            }),
            transforms: Vec::new(),
            sink: SinkSpec::Materialize(super::super::graph::MaterializeSinkSpec {
                handle,
                required: Default::default(),
            }),
            sink_sharing: SinkSharing::Exclusive,
            properties: PipelineProperties::default(),
            output: row_type(&["a"], &[LogicalType::Integer]),
        };
        let graph = PipelineGraph {
            pipelines: vec![spec],
            dependencies: Vec::new(),
            handles: handles.finish(),
            control_regions: Vec::new(),
            root: PipelineRoot::Pipeline(PipelineId::new(0)),
        };

        let programs = PipelineProgramBuilder::default()
            .build_program_set(&graph)
            .expect("program set should build");
        let program = programs.get(PipelineId::new(0)).expect("pipeline program");

        let SourceExec::Materialized(source) = &program.source.exec else {
            panic!("expected materialized source");
        };
        assert_eq!(source.handle.id(), handle);
        let SinkExec::Materialize(sink) = &program.sink.exec else {
            panic!("expected materialize sink");
        };
        assert_eq!(sink.handle.id(), handle);
    }

    #[test]
    fn breaker_role_slots_bind_typed_handles_without_dyn_adapters() {
        let output = row_type(&["a"], &[LogicalType::Integer]);
        let mut handles = BreakerHandleCatalogBuilder::default();
        let join = handles.register(
            BreakerHandleKind::HashJoinBuild,
            output.clone(),
            PipelineProperties::default(),
        );
        let aggregate = handles.register(
            BreakerHandleKind::Aggregate,
            output.clone(),
            PipelineProperties::default(),
        );
        let aggregate_spec = empty_aggregate_spec();
        let sort = handles.register(
            BreakerHandleKind::Sort,
            output.clone(),
            PipelineProperties::default(),
        );
        let topn = handles.register(
            BreakerHandleKind::TopN,
            output.clone(),
            PipelineProperties::default(),
        );
        let window = handles.register(
            BreakerHandleKind::Window,
            output.clone(),
            PipelineProperties::default(),
        );
        let cte = handles.register(
            BreakerHandleKind::Cte,
            output.clone(),
            PipelineProperties::default(),
        );

        let graph = PipelineGraph {
            pipelines: vec![
                PipelineSpec {
                    id: PipelineId::new(0),
                    source: SourceSpec::HashJoinSpillReplay(
                        super::super::graph::HashJoinSpillReplaySourceSpec {
                            handle: join,
                            join_type: JoinType::Inner,
                            anti_join_mode: AntiJoinMode::Regular,
                            conditions: Box::new([join_condition()]),
                            probe_types: Box::new([LogicalType::Integer]),
                            build_payload_types: Box::new([LogicalType::Integer]),
                            left_projection: Box::new([0]),
                            output_names: Box::new(["l".to_string(), "r".to_string()]),
                            output_types: Box::new([LogicalType::Integer, LogicalType::Integer]),
                        },
                    ),
                    transforms: vec![TransformSpec::HashJoinProbe(
                        super::super::graph::HashJoinProbeSpec {
                            handle: join,
                            join_type: JoinType::Inner,
                            anti_join_mode: AntiJoinMode::Regular,
                            conditions: Box::new([join_condition()]),
                            left_projection: Box::new([0]),
                            output_names: Box::new(["l".to_string(), "r".to_string()]),
                            output_types: Box::new([LogicalType::Integer, LogicalType::Integer]),
                        },
                    )],
                    sink: SinkSpec::HashJoinBuild(super::super::graph::HashJoinBuildSinkSpec {
                        handle: join,
                        join_type: JoinType::Inner,
                        conditions: Box::new([join_condition()]),
                        build_projection: Box::new([0]),
                        build_payload_types: Box::new([LogicalType::Integer]),
                        required: Default::default(),
                        force_external: false,
                    }),
                    sink_sharing: SinkSharing::Exclusive,
                    properties: PipelineProperties::default(),
                    output: output.clone(),
                },
                PipelineSpec {
                    id: PipelineId::new(1),
                    source: SourceSpec::HashAggregateEmit(
                        super::super::graph::HashAggregateEmitSourceSpec {
                            handle: aggregate,
                            spec: aggregate_spec.clone(),
                        },
                    ),
                    transforms: Vec::new(),
                    sink: SinkSpec::HashAggregateBuild(
                        super::super::graph::HashAggregateBuildSinkSpec {
                            handle: aggregate,
                            spec: aggregate_spec,
                            required: Default::default(),
                        },
                    ),
                    sink_sharing: SinkSharing::Exclusive,
                    properties: PipelineProperties::default(),
                    output: output.clone(),
                },
                PipelineSpec {
                    id: PipelineId::new(2),
                    source: SourceSpec::SortEmit(super::super::graph::SortEmitSourceSpec {
                        handle: sort,
                        ordering: crate::physical::properties::OrderingSpec::new(Vec::new()),
                        output_names: Box::new(["a".to_string()]),
                        output_types: Box::new([LogicalType::Integer]),
                    }),
                    transforms: Vec::new(),
                    sink: SinkSpec::SortBuild(super::super::graph::SortBuildSinkSpec {
                        handle: sort,
                        orders: vec![order_by_first_column()].into_boxed_slice(),
                        projection_map: Box::new([0]),
                        input_types: Box::new([LogicalType::Integer]),
                        output_names: Box::new(["a".to_string()]),
                        output_types: Box::new([LogicalType::Integer]),
                        force_external: false,
                        required: Default::default(),
                    }),
                    sink_sharing: SinkSharing::Exclusive,
                    properties: PipelineProperties::default(),
                    output: output.clone(),
                },
                PipelineSpec {
                    id: PipelineId::new(3),
                    source: SourceSpec::TopNEmit(super::super::graph::TopNEmitSourceSpec {
                        handle: topn,
                        spec: topn_spec(),
                    }),
                    transforms: Vec::new(),
                    sink: SinkSpec::TopNBuild(super::super::graph::TopNBuildSinkSpec {
                        handle: topn,
                        spec: topn_spec(),
                        required: Default::default(),
                    }),
                    sink_sharing: SinkSharing::Exclusive,
                    properties: PipelineProperties::default(),
                    output: output.clone(),
                },
                PipelineSpec {
                    id: PipelineId::new(4),
                    source: SourceSpec::WindowEmit(super::super::graph::WindowEmitSourceSpec {
                        handle: window,
                        spec: window_spec(),
                    }),
                    transforms: Vec::new(),
                    sink: SinkSpec::WindowBuild(super::super::graph::WindowBuildSinkSpec {
                        handle: window,
                        spec: window_spec(),
                        required: Default::default(),
                    }),
                    sink_sharing: SinkSharing::Exclusive,
                    properties: PipelineProperties::default(),
                    output: output.clone(),
                },
                PipelineSpec {
                    id: PipelineId::new(5),
                    source: SourceSpec::CteScan(super::super::graph::CteScanSourceSpec {
                        handle: cte,
                    }),
                    transforms: Vec::new(),
                    sink: SinkSpec::CteMaterialize(super::super::graph::CteMaterializeSinkSpec {
                        handle: cte,
                        required: Default::default(),
                    }),
                    sink_sharing: SinkSharing::Exclusive,
                    properties: PipelineProperties::default(),
                    output,
                },
            ],
            dependencies: Vec::new(),
            handles: handles.finish(),
            control_regions: Vec::new(),
            root: PipelineRoot::Pipeline(PipelineId::new(0)),
        };

        let programs = PipelineProgramBuilder::default()
            .build_program_set(&graph)
            .expect("breaker role program set");
        assert!(matches!(
            programs.get(PipelineId::new(0)).unwrap().source.exec,
            SourceExec::HashJoinSpillReplay(_)
        ));
        assert!(matches!(
            programs.get(PipelineId::new(0)).unwrap().transforms[0].exec,
            TransformExec::HashJoinProbe(_)
        ));
        assert!(matches!(
            programs.get(PipelineId::new(1)).unwrap().sink.exec,
            SinkExec::HashAggregateBuild(_)
        ));
        assert!(matches!(
            programs.get(PipelineId::new(5)).unwrap().source.exec,
            SourceExec::CteScan(_)
        ));
    }
}
