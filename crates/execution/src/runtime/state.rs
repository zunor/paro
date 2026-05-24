// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Enum-backed runtime state slots.
//!
//! `*Global` means pipeline-global execution state and `*Local` means
//! task-local execution state. These names never refer to plan specs or process
//! global variables.

use std::any::Any;
use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};

use super::breaker::{
    AggregateHandle, CteHandle, DelimHandle, JoinBuildHandle, MaterializedHandle,
    RecursiveTableHandle, SetOperationHandle, SortHandle, TopNHandle, WindowHandle,
};
pub use super::breaker::{
    MaterializeSinkGlobal, MaterializeSinkLocal, MaterializedSourceGlobal, MaterializedSourceLocal,
};
pub use crate::operators::aggregate::state::{
    HashAggregateBuildSinkLocal, HashAggregateEmitSourceLocal, PerfectHashAggregateEmitSourceLocal,
    PerfectHashAggregateSinkLocal, StreamingAggregateTransformGlobal,
    StreamingAggregateTransformLocal, UngroupedAggregateEmitSourceLocal,
    UngroupedAggregateSinkLocal,
};
pub use crate::operators::dml::state::{
    CopyToSinkGlobal, CopyToSinkLocal, DmlSinkGlobal, EmptyDmlSinkLocal, InsertSinkLocal,
};
pub use crate::operators::external::state::{
    ExternalProjectTransformGlobal, ExternalProjectTransformLocal, ExternalTableSinkGlobal,
    ExternalTableSinkLocal, ExternalTableSourceGlobal, ExternalTableSourceLocal,
};
pub use crate::operators::graph::state::{
    GraphExpandTransformGlobal, GraphExpandTransformLocal, GraphFilterScanState,
    GraphProjectMaterializedRuntime, GraphProjectTableFetchPlan, GraphProjectTransformLocal,
    GraphScanSourceGlobal, GraphScanSourceLocal, GraphShortestPathTransformGlobal,
    GraphShortestPathTransformLocal,
};
pub use crate::operators::join::state::{
    CrossProductProbeTransformLocal, HashJoinBuildSinkLocal, HashJoinProbeTransformLocal,
    HashJoinSpillReplayPartitionLocal, HashJoinSpillReplaySourceLocal,
    HashJoinUnmatchedSourceLocal, NestedLoopJoinProbeTransformLocal, NljUnmatchedSourceLocal,
};
pub use crate::operators::result::state::{ClientResultSinkGlobal, ClientResultSinkLocal};
pub use crate::operators::scan::state::{
    ChunkSourceGlobal, ChunkSourceLocal, EmptySourceGlobal, EmptySourceLocal,
    ExpressionSourceGlobal, ExpressionSourceLocal, RowsetSourceGlobal, RowsetSourceLocal,
    TableFunctionSourceGlobal, TableFunctionSourceLocal, ValuesSourceGlobal, ValuesSourceLocal,
};
pub use crate::operators::search::state::{SearchSourceGlobal, SearchSourceLocal};
pub use crate::operators::set::state::{
    CteMaterializeSinkLocal, CteScanSourceLocal, DelimCaptureSinkGlobal, DelimCaptureSinkLocal,
    DelimScanSourceLocal, RecursiveTableAppendSinkGlobal, RecursiveTableAppendSinkLocal,
    RecursiveTableScanSourceLocal, SetOperationEmitSourceLocal, SetOperationInputSinkLocal,
};
pub use crate::operators::sort::state::{
    SortBuildSinkLocal, SortEmitSourceLocal, StreamingTopNTransformGlobal,
    StreamingTopNTransformLocal, TopNBuildSinkLocal, TopNEmitSourceLocal,
};
pub use crate::operators::transform::state::{
    StreamingLimitTransformGlobal, StreamingLimitTransformLocal,
};
pub use crate::operators::transform::{
    FilterTransformGlobal, FilterTransformLocal, ProjectTransformGlobal, ProjectTransformLocal,
};
pub use crate::operators::window::state::{
    StreamingWindowTransformGlobal, StreamingWindowTransformLocal, WindowBuildSinkLocal,
    WindowEmitSourceLocal,
};

pub type DynGlobalStateBox = Box<dyn DynGlobalState>;
pub type DynLocalStateBox = Box<dyn DynLocalState>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DynStateTypeId(pub &'static str);

pub trait DynGlobalState: Send + Sync + std::fmt::Debug {
    fn state_type(&self) -> DynStateTypeId;
    fn as_any(&self) -> &(dyn Any + Send + Sync);
    fn as_any_mut(&mut self) -> &mut (dyn Any + Send + Sync);
}

pub trait DynLocalState: Send + std::fmt::Debug {
    fn state_type(&self) -> DynStateTypeId;
    fn as_any(&self) -> &(dyn Any + Send);
    fn as_any_mut(&mut self) -> &mut (dyn Any + Send);
}

#[derive(Debug)]
pub struct BreakerHandleGlobal<T> {
    pub handle: Arc<T>,
}

// ─── Enums ───────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum SourceGlobal {
    Rowset(Arc<RowsetSourceGlobal>),
    Values(Arc<ValuesSourceGlobal>),
    Dummy(Arc<EmptySourceGlobal>),
    Empty(Arc<EmptySourceGlobal>),
    Chunk(Arc<ChunkSourceGlobal>),
    Expression(Arc<ExpressionSourceGlobal>),
    TableFunction(Arc<TableFunctionSourceGlobal>),
    Materialized(Arc<MaterializedSourceGlobal>),
    HashJoinSpillReplay(Arc<BreakerHandleGlobal<JoinBuildHandle>>),
    HashJoinUnmatched(Arc<BreakerHandleGlobal<JoinBuildHandle>>),
    HashAggregateEmit(Arc<BreakerHandleGlobal<AggregateHandle>>),
    UngroupedAggregateEmit(Arc<BreakerHandleGlobal<AggregateHandle>>),
    PerfectHashAggregateEmit(Arc<BreakerHandleGlobal<AggregateHandle>>),
    SortEmit(Arc<BreakerHandleGlobal<SortHandle>>),
    TopNEmit(Arc<BreakerHandleGlobal<TopNHandle>>),
    WindowEmit(Arc<BreakerHandleGlobal<WindowHandle>>),
    SetOperationEmit(Arc<BreakerHandleGlobal<SetOperationHandle>>),
    CteScan(Arc<BreakerHandleGlobal<CteHandle>>),
    DelimScan(Arc<BreakerHandleGlobal<DelimHandle>>),
    RecursiveTableScan(Arc<BreakerHandleGlobal<RecursiveTableHandle>>),
    Search(Arc<SearchSourceGlobal>),
    GraphScan(Arc<GraphScanSourceGlobal>),
    ExternalTable(Arc<ExternalTableSourceGlobal>),
    Dyn(DynGlobalStateBox),
}

impl SourceGlobal {
    #[inline(always)]
    pub fn rowset(&self) -> Result<&Arc<RowsetSourceGlobal>> {
        match self {
            Self::Rowset(state) => Ok(state),
            _ => Err(state_mismatch("SourceGlobal::Rowset", self.variant_name())),
        }
    }

    #[inline(always)]
    pub fn values(&self) -> Result<&Arc<ValuesSourceGlobal>> {
        match self {
            Self::Values(state) => Ok(state),
            _ => Err(state_mismatch("SourceGlobal::Values", self.variant_name())),
        }
    }

    #[inline(always)]
    pub fn materialized(&self) -> Result<&Arc<MaterializedSourceGlobal>> {
        match self {
            Self::Materialized(state) => Ok(state),
            _ => Err(state_mismatch(
                "SourceGlobal::Materialized",
                self.variant_name(),
            )),
        }
    }

    #[inline(always)]
    pub fn chunk(&self) -> Result<&Arc<ChunkSourceGlobal>> {
        match self {
            Self::Chunk(state) => Ok(state),
            _ => Err(state_mismatch("SourceGlobal::Chunk", self.variant_name())),
        }
    }

    #[inline(always)]
    pub fn expression(&self) -> Result<&Arc<ExpressionSourceGlobal>> {
        match self {
            Self::Expression(state) => Ok(state),
            _ => Err(state_mismatch(
                "SourceGlobal::Expression",
                self.variant_name(),
            )),
        }
    }

    #[inline(always)]
    pub fn table_function(&self) -> Result<&Arc<TableFunctionSourceGlobal>> {
        match self {
            Self::TableFunction(state) => Ok(state),
            _ => Err(state_mismatch(
                "SourceGlobal::TableFunction",
                self.variant_name(),
            )),
        }
    }

    fn variant_name(&self) -> &'static str {
        match self {
            Self::Rowset(_) => "Rowset",
            Self::Values(_) => "Values",
            Self::Dummy(_) => "Dummy",
            Self::Empty(_) => "Empty",
            Self::Chunk(_) => "Chunk",
            Self::Expression(_) => "Expression",
            Self::TableFunction(_) => "TableFunction",
            Self::Materialized(_) => "Materialized",
            Self::HashJoinSpillReplay(_) => "HashJoinSpillReplay",
            Self::HashJoinUnmatched(_) => "HashJoinUnmatched",
            Self::HashAggregateEmit(_) => "HashAggregateEmit",
            Self::UngroupedAggregateEmit(_) => "UngroupedAggregateEmit",
            Self::PerfectHashAggregateEmit(_) => "PerfectHashAggregateEmit",
            Self::SortEmit(_) => "SortEmit",
            Self::TopNEmit(_) => "TopNEmit",
            Self::WindowEmit(_) => "WindowEmit",
            Self::SetOperationEmit(_) => "SetOperationEmit",
            Self::CteScan(_) => "CteScan",
            Self::DelimScan(_) => "DelimScan",
            Self::RecursiveTableScan(_) => "RecursiveTableScan",
            Self::Search(_) => "Search",
            Self::GraphScan(_) => "GraphScan",
            Self::ExternalTable(_) => "ExternalTable",
            Self::Dyn(_) => "Dyn",
        }
    }
}

#[derive(Debug)]
pub enum SourceLocal {
    Rowset(RowsetSourceLocal),
    Values(ValuesSourceLocal),
    Dummy(EmptySourceLocal),
    Empty(EmptySourceLocal),
    Chunk(ChunkSourceLocal),
    Expression(ExpressionSourceLocal),
    TableFunction(TableFunctionSourceLocal),
    Materialized(MaterializedSourceLocal),
    NljUnmatched(NljUnmatchedSourceLocal),
    HashJoinSpillReplay(HashJoinSpillReplaySourceLocal),
    HashJoinUnmatched(HashJoinUnmatchedSourceLocal),
    HashAggregateEmit(HashAggregateEmitSourceLocal),
    UngroupedAggregateEmit(UngroupedAggregateEmitSourceLocal),
    PerfectHashAggregateEmit(PerfectHashAggregateEmitSourceLocal),
    SortEmit(SortEmitSourceLocal),
    TopNEmit(TopNEmitSourceLocal),
    WindowEmit(WindowEmitSourceLocal),
    SetOperationEmit(SetOperationEmitSourceLocal),
    CteScan(CteScanSourceLocal),
    DelimScan(DelimScanSourceLocal),
    RecursiveTableScan(RecursiveTableScanSourceLocal),
    Search(SearchSourceLocal),
    GraphScan(GraphScanSourceLocal),
    ExternalTable(ExternalTableSourceLocal),
    Dyn(DynLocalStateBox),
}

impl SourceLocal {
    #[inline(always)]
    pub fn rowset_mut(&mut self) -> Result<&mut RowsetSourceLocal> {
        match self {
            Self::Rowset(state) => Ok(state),
            _ => Err(state_mismatch("SourceLocal::Rowset", self.variant_name())),
        }
    }

    fn variant_name(&self) -> &'static str {
        match self {
            Self::Rowset(_) => "Rowset",
            Self::Values(_) => "Values",
            Self::Dummy(_) => "Dummy",
            Self::Empty(_) => "Empty",
            Self::Chunk(_) => "Chunk",
            Self::Expression(_) => "Expression",
            Self::TableFunction(_) => "TableFunction",
            Self::Materialized(_) => "Materialized",
            Self::NljUnmatched(_) => "NljUnmatched",
            Self::HashJoinSpillReplay(_) => "HashJoinSpillReplay",
            Self::HashJoinUnmatched(_) => "HashJoinUnmatched",
            Self::HashAggregateEmit(_) => "HashAggregateEmit",
            Self::UngroupedAggregateEmit(_) => "UngroupedAggregateEmit",
            Self::PerfectHashAggregateEmit(_) => "PerfectHashAggregateEmit",
            Self::SortEmit(_) => "SortEmit",
            Self::TopNEmit(_) => "TopNEmit",
            Self::WindowEmit(_) => "WindowEmit",
            Self::SetOperationEmit(_) => "SetOperationEmit",
            Self::CteScan(_) => "CteScan",
            Self::DelimScan(_) => "DelimScan",
            Self::RecursiveTableScan(_) => "RecursiveTableScan",
            Self::Search(_) => "Search",
            Self::GraphScan(_) => "GraphScan",
            Self::ExternalTable(_) => "ExternalTable",
            Self::Dyn(_) => "Dyn",
        }
    }
}

#[derive(Debug)]
pub enum TransformGlobal {
    Empty,
    Filter(Arc<FilterTransformGlobal>),
    Project(Arc<ProjectTransformGlobal>),
    StreamingLimit(Arc<StreamingLimitTransformGlobal>),
    StreamingTopN(Arc<StreamingTopNTransformGlobal>),
    StreamingAggregate(Arc<StreamingAggregateTransformGlobal>),
    StreamingWindow(Arc<StreamingWindowTransformGlobal>),
    HashJoinProbe(Arc<BreakerHandleGlobal<JoinBuildHandle>>),
    NestedLoopJoinProbe(Arc<crate::operators::join::nested_loop::runtime::NljProbeGlobal>),
    CrossProductProbe(Arc<BreakerHandleGlobal<MaterializedHandle>>),
    ExternalProject(Arc<ExternalProjectTransformGlobal>),
    GraphExpand(Arc<GraphExpandTransformGlobal>),
    GraphShortestPath(Arc<GraphShortestPathTransformGlobal>),
    PropertyRepair,
    Dyn(DynGlobalStateBox),
}

#[derive(Debug)]
pub enum TransformLocal {
    Empty,
    Filter(FilterTransformLocal),
    Project(ProjectTransformLocal),
    StreamingLimit(StreamingLimitTransformLocal),
    StreamingTopN(StreamingTopNTransformLocal),
    StreamingAggregate(StreamingAggregateTransformLocal),
    StreamingWindow(StreamingWindowTransformLocal),
    HashJoinProbe(HashJoinProbeTransformLocal),
    NestedLoopJoinProbe(NestedLoopJoinProbeTransformLocal),
    CrossProductProbe(CrossProductProbeTransformLocal),
    ExternalProject(ExternalProjectTransformLocal),
    GraphExpand(GraphExpandTransformLocal),
    GraphProject(GraphProjectTransformLocal),
    GraphShortestPath(GraphShortestPathTransformLocal),
    PropertyRepair,
    Dyn(DynLocalStateBox),
}

#[derive(Debug)]
pub struct TransformGlobalSlots {
    slots: Box<[TransformGlobal]>,
}

impl TransformGlobalSlots {
    pub fn new(slots: Vec<TransformGlobal>) -> Self {
        Self {
            slots: slots.into_boxed_slice(),
        }
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<&TransformGlobal> {
        self.slots.get(index)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

#[derive(Debug)]
pub enum SinkGlobal {
    ClientResult(Arc<ClientResultSinkGlobal>),
    Materialize(Arc<MaterializeSinkGlobal>),
    HashJoinBuild(Arc<BreakerHandleGlobal<JoinBuildHandle>>),
    HashAggregateBuild(Arc<BreakerHandleGlobal<AggregateHandle>>),
    UngroupedAggregate(Arc<BreakerHandleGlobal<AggregateHandle>>),
    PerfectHashAggregate(Arc<BreakerHandleGlobal<AggregateHandle>>),
    SortBuild(Arc<BreakerHandleGlobal<SortHandle>>),
    TopNBuild(Arc<BreakerHandleGlobal<TopNHandle>>),
    WindowBuild(Arc<BreakerHandleGlobal<WindowHandle>>),
    SetOperationInput(Arc<BreakerHandleGlobal<SetOperationHandle>>),
    CteMaterialize(Arc<BreakerHandleGlobal<CteHandle>>),
    DelimCapture(Arc<DelimCaptureSinkGlobal>),
    RecursiveTableAppend(Arc<RecursiveTableAppendSinkGlobal>),
    Dml(Arc<DmlSinkGlobal>),
    CopyToFile(Arc<CopyToSinkGlobal>),
    ExternalTable(Arc<ExternalTableSinkGlobal>),
    Dyn(DynGlobalStateBox),
}

impl SinkGlobal {
    #[inline(always)]
    pub fn client_result(&self) -> Result<&Arc<ClientResultSinkGlobal>> {
        match self {
            Self::ClientResult(state) => Ok(state),
            _ => Err(state_mismatch(
                "SinkGlobal::ClientResult",
                self.variant_name(),
            )),
        }
    }

    fn variant_name(&self) -> &'static str {
        match self {
            Self::ClientResult(_) => "ClientResult",
            Self::Materialize(_) => "Materialize",
            Self::HashJoinBuild(_) => "HashJoinBuild",
            Self::HashAggregateBuild(_) => "HashAggregateBuild",
            Self::UngroupedAggregate(_) => "UngroupedAggregate",
            Self::PerfectHashAggregate(_) => "PerfectHashAggregate",
            Self::SortBuild(_) => "SortBuild",
            Self::TopNBuild(_) => "TopNBuild",
            Self::WindowBuild(_) => "WindowBuild",
            Self::SetOperationInput(_) => "SetOperationInput",
            Self::CteMaterialize(_) => "CteMaterialize",
            Self::DelimCapture(_) => "DelimCapture",
            Self::RecursiveTableAppend(_) => "RecursiveTableAppend",
            Self::Dml(_) => "Dml",
            Self::CopyToFile(_) => "CopyToFile",
            Self::ExternalTable(_) => "ExternalTable",
            Self::Dyn(_) => "Dyn",
        }
    }
}

#[derive(Debug)]
pub enum SinkLocal {
    ClientResult(ClientResultSinkLocal),
    Materialize(MaterializeSinkLocal),
    HashJoinBuild(HashJoinBuildSinkLocal),
    HashAggregateBuild(HashAggregateBuildSinkLocal),
    UngroupedAggregate(UngroupedAggregateSinkLocal),
    PerfectHashAggregate(PerfectHashAggregateSinkLocal),
    SortBuild(SortBuildSinkLocal),
    TopNBuild(TopNBuildSinkLocal),
    WindowBuild(WindowBuildSinkLocal),
    SetOperationInput(SetOperationInputSinkLocal),
    CteMaterialize(CteMaterializeSinkLocal),
    DelimCapture(DelimCaptureSinkLocal),
    RecursiveTableAppend(RecursiveTableAppendSinkLocal),
    Insert(InsertSinkLocal),
    EmptyDml(EmptyDmlSinkLocal),
    CopyToFile(CopyToSinkLocal),
    ExternalTable(ExternalTableSinkLocal),
    Dyn(DynLocalStateBox),
}

fn state_mismatch(expected: &'static str, actual: &'static str) -> paro_common::error::ParoError {
    paro_error::internal(format!(
        "runtime state variant mismatch: expected {expected}, got {actual}"
    ))
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use super::*;

    #[derive(Debug)]
    struct TestDynState {
        value: usize,
    }

    impl DynGlobalState for TestDynState {
        fn state_type(&self) -> DynStateTypeId {
            DynStateTypeId("test")
        }

        fn as_any(&self) -> &(dyn Any + Send + Sync) {
            self
        }

        fn as_any_mut(&mut self) -> &mut (dyn Any + Send + Sync) {
            self
        }
    }

    #[test]
    fn builtin_accessors_are_enum_checked_without_any_downcast() {
        let state = SourceGlobal::Values(Arc::new(ValuesSourceGlobal { row_count: 3 }));

        assert_eq!(state.values().expect("values state").row_count, 3);
        assert!(state.rowset().is_err());
    }

    #[test]
    fn dyn_state_preserves_plugin_downcast_escape_hatch() {
        let mut state = SourceGlobal::Dyn(Box::new(TestDynState { value: 7 }));

        let SourceGlobal::Dyn(dyn_state) = &mut state else {
            panic!("expected dyn state");
        };
        assert_eq!(dyn_state.state_type(), DynStateTypeId("test"));
        assert_eq!(
            dyn_state
                .as_any()
                .downcast_ref::<TestDynState>()
                .expect("dyn state type")
                .value,
            7
        );
        dyn_state
            .as_any_mut()
            .downcast_mut::<TestDynState>()
            .expect("dyn state type")
            .value = 11;
        assert_eq!(
            dyn_state
                .as_any()
                .downcast_ref::<TestDynState>()
                .expect("dyn state type")
                .value,
            11
        );
    }

    #[test]
    fn transform_global_slots_keep_dense_o1_indexing() {
        let slots = TransformGlobalSlots::new(vec![
            TransformGlobal::Empty,
            TransformGlobal::Filter(Arc::new(FilterTransformGlobal)),
            TransformGlobal::PropertyRepair,
        ]);

        assert!(matches!(slots.get(0), Some(TransformGlobal::Empty)));
        assert!(matches!(slots.get(1), Some(TransformGlobal::Filter(_))));
        assert!(matches!(
            slots.get(2),
            Some(TransformGlobal::PropertyRepair)
        ));
        assert!(slots.get(3).is_none());
    }
}
