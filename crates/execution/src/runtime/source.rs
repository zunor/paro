// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::error::Result;

pub use super::breaker::MaterializedSourceExec;
use super::context::{Blocker, OperatorCallContext, PipelineInitContext};
use super::state::{SourceGlobal, SourceLocal};

pub use crate::operators::aggregate::hash::emit::HashAggregateEmitSourceExec;
pub use crate::operators::aggregate::perfect_hash::emit::PerfectHashAggregateEmitSourceExec;
pub use crate::operators::aggregate::ungrouped::emit::UngroupedAggregateEmitSourceExec;
pub use crate::operators::external::ExternalTableSourceExec;
pub use crate::operators::graph::GraphScanSourceExec;
pub use crate::operators::join::hash::{
    HashJoinSpillReplaySourceExec, HashJoinUnmatchedSourceExec,
};
pub use crate::operators::join::nested_loop::NljUnmatchedSourceExec;
pub use crate::operators::join::sort_range::ClassicIeJoinSourceExec;
pub use crate::operators::scan::{
    ChunkSourceExec, DummySourceExec, EmptySourceExec, ExpressionSourceExec, RowsetSourceDesc,
    RowsetSourceExec, TableFunctionSourceExec, ValuesSourceExec,
};
pub use crate::operators::search::{
    AdaptiveSearchSourceExec, FullTextSearchSourceExec, SparseVectorSearchSourceExec,
    VectorSearchSourceExec,
};
pub use crate::operators::set::{
    CteScanSourceExec, DelimScanSourceExec, RecursiveTableScanSourceExec,
    SetOperationEmitSourceExec,
};
pub use crate::operators::sort::{SortEmitSourceExec, TopNEmitSourceExec};
pub use crate::operators::window::{PartitionAggregateWindowEmitSourceExec, WindowEmitSourceExec};

#[derive(Debug)]
pub enum SourceExec {
    Rowset(RowsetSourceExec),
    Values(ValuesSourceExec),
    Dummy(DummySourceExec),
    Empty(EmptySourceExec),
    Chunk(ChunkSourceExec),
    Expression(ExpressionSourceExec),
    TableFunction(TableFunctionSourceExec),
    VectorSearch(VectorSearchSourceExec),
    SparseVectorSearch(SparseVectorSearchSourceExec),
    FullTextSearch(FullTextSearchSourceExec),
    AdaptiveSearch(AdaptiveSearchSourceExec),
    GraphScan(GraphScanSourceExec),
    ExternalTable(ExternalTableSourceExec),
    Materialized(MaterializedSourceExec),
    ClassicIeJoin(ClassicIeJoinSourceExec),
    NljUnmatched(NljUnmatchedSourceExec),
    HashJoinSpillReplay(HashJoinSpillReplaySourceExec),
    HashJoinUnmatched(HashJoinUnmatchedSourceExec),
    HashAggregateEmit(HashAggregateEmitSourceExec),
    UngroupedAggregateEmit(UngroupedAggregateEmitSourceExec),
    PerfectHashAggregateEmit(PerfectHashAggregateEmitSourceExec),
    SortEmit(SortEmitSourceExec),
    TopNEmit(TopNEmitSourceExec),
    WindowEmit(WindowEmitSourceExec),
    PartitionAggregateWindowEmit(PartitionAggregateWindowEmitSourceExec),
    SetOperationEmit(SetOperationEmitSourceExec),
    CteScan(CteScanSourceExec),
    DelimScan(DelimScanSourceExec),
    RecursiveTableScan(RecursiveTableScanSourceExec),
    Dyn(Box<dyn DynSourceExec>),
}

impl SourceExec {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Rowset(_) => "ROWSET_SCAN",
            Self::Values(_) => "VALUES",
            Self::Dummy(_) => "DUMMY_SCAN",
            Self::Empty(_) => "EMPTY_RESULT",
            Self::Chunk(_) => "CHUNK_SCAN",
            Self::Expression(_) => "EXPRESSION_SCAN",
            Self::TableFunction(_) => "TABLE_FUNCTION_SCAN",
            Self::VectorSearch(_) => "VECTOR_SEARCH",
            Self::SparseVectorSearch(_) => "SPARSE_VECTOR_SEARCH",
            Self::FullTextSearch(_) => "FULLTEXT_SCAN",
            Self::AdaptiveSearch(_) => "ADAPTIVE_SEARCH",
            Self::GraphScan(_) => "GRAPH_SCAN",
            Self::ExternalTable(_) => "EXTERNAL_TABLE",
            Self::Materialized(_) => "MATERIALIZED",
            Self::ClassicIeJoin(_) => "CLASSIC_IE_JOIN",
            Self::NljUnmatched(_) => "NLJ_UNMATCHED",
            Self::HashJoinSpillReplay(_) => "HASH_JOIN_SPILL_REPLAY",
            Self::HashJoinUnmatched(_) => "HASH_JOIN_UNMATCHED",
            Self::HashAggregateEmit(_) => "HASH_AGGREGATE_EMIT",
            Self::UngroupedAggregateEmit(_) => "UNGROUPED_AGGREGATE_EMIT",
            Self::PerfectHashAggregateEmit(_) => "PERFECT_HASH_AGGREGATE_EMIT",
            Self::SortEmit(_) => "SORT_EMIT",
            Self::TopNEmit(_) => "TOP_N_EMIT",
            Self::WindowEmit(_) => "WINDOW_EMIT",
            Self::PartitionAggregateWindowEmit(_) => "PARTITION_AGGREGATE_WINDOW_EMIT",
            Self::SetOperationEmit(_) => "SET_OPERATION_EMIT",
            Self::CteScan(_) => "CTE_SCAN",
            Self::DelimScan(_) => "DELIM_SCAN",
            Self::RecursiveTableScan(_) => "RECURSIVE_TABLE_SCAN",
            Self::Dyn(_) => "DYN_SOURCE",
        }
    }

    /// Cold lifecycle dispatch stays in one code-generation unit. Rust 1.92 / LLVM 21
    /// miscompiles the large payload-enum jump table when this function is forced
    /// inline across release CGUs; keeping one canonical dispatch also avoids code
    /// growth. The hot data-path dispatch below remains inline.
    #[inline(never)]
    pub fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        match self {
            Self::Rowset(exec) => exec.create_global(ctx),
            Self::Values(exec) => exec.create_global(ctx),
            Self::Dummy(exec) => exec.create_global(ctx),
            Self::Empty(exec) => exec.create_global(ctx),
            Self::Chunk(exec) => exec.create_global(ctx),
            Self::Expression(exec) => exec.create_global(ctx),
            Self::TableFunction(exec) => exec.create_global(ctx),
            Self::VectorSearch(exec) => exec.create_global(ctx),
            Self::SparseVectorSearch(exec) => exec.create_global(ctx),
            Self::FullTextSearch(exec) => exec.create_global(ctx),
            Self::AdaptiveSearch(exec) => exec.create_global(ctx),
            Self::GraphScan(exec) => exec.create_global(ctx),
            Self::ExternalTable(exec) => exec.create_global(ctx),
            Self::Materialized(exec) => exec.create_global(ctx),
            Self::ClassicIeJoin(exec) => exec.create_global(ctx),
            Self::NljUnmatched(exec) => exec.create_global(ctx),
            Self::HashJoinSpillReplay(exec) => exec.create_global(ctx),
            Self::HashJoinUnmatched(exec) => exec.create_global(ctx),
            Self::HashAggregateEmit(exec) => exec.create_global(ctx),
            Self::UngroupedAggregateEmit(exec) => exec.create_global(ctx),
            Self::PerfectHashAggregateEmit(exec) => exec.create_global(ctx),
            Self::SortEmit(exec) => exec.create_global(ctx),
            Self::TopNEmit(exec) => exec.create_global(ctx),
            Self::WindowEmit(exec) => exec.create_global(ctx),
            Self::PartitionAggregateWindowEmit(exec) => exec.create_global(ctx),
            Self::SetOperationEmit(exec) => exec.create_global(ctx),
            Self::CteScan(exec) => exec.create_global(ctx),
            Self::DelimScan(exec) => exec.create_global(ctx),
            Self::RecursiveTableScan(exec) => exec.create_global(ctx),
            Self::Dyn(exec) => exec.create_global(ctx),
        }
    }

    #[inline(never)]
    pub fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        match self {
            Self::Rowset(exec) => exec.create_local(ctx, global),
            Self::Values(exec) => exec.create_local(ctx, global),
            Self::Dummy(exec) => exec.create_local(ctx, global),
            Self::Empty(exec) => exec.create_local(ctx, global),
            Self::Chunk(exec) => exec.create_local(ctx, global),
            Self::Expression(exec) => exec.create_local(ctx, global),
            Self::TableFunction(exec) => exec.create_local(ctx, global),
            Self::VectorSearch(exec) => exec.create_local(ctx, global),
            Self::SparseVectorSearch(exec) => exec.create_local(ctx, global),
            Self::FullTextSearch(exec) => exec.create_local(ctx, global),
            Self::AdaptiveSearch(exec) => exec.create_local(ctx, global),
            Self::GraphScan(exec) => exec.create_local(ctx, global),
            Self::ExternalTable(exec) => exec.create_local(ctx, global),
            Self::Materialized(exec) => exec.create_local(ctx, global),
            Self::ClassicIeJoin(exec) => exec.create_local(ctx, global),
            Self::NljUnmatched(exec) => exec.create_local(ctx, global),
            Self::HashJoinSpillReplay(exec) => exec.create_local(ctx, global),
            Self::HashJoinUnmatched(exec) => exec.create_local(ctx, global),
            Self::HashAggregateEmit(exec) => exec.create_local(ctx, global),
            Self::UngroupedAggregateEmit(exec) => exec.create_local(ctx, global),
            Self::PerfectHashAggregateEmit(exec) => exec.create_local(ctx, global),
            Self::SortEmit(exec) => exec.create_local(ctx, global),
            Self::TopNEmit(exec) => exec.create_local(ctx, global),
            Self::WindowEmit(exec) => exec.create_local(ctx, global),
            Self::PartitionAggregateWindowEmit(exec) => exec.create_local(ctx, global),
            Self::SetOperationEmit(exec) => exec.create_local(ctx, global),
            Self::CteScan(exec) => exec.create_local(ctx, global),
            Self::DelimScan(exec) => exec.create_local(ctx, global),
            Self::RecursiveTableScan(exec) => exec.create_local(ctx, global),
            Self::Dyn(exec) => exec.create_local(ctx, global),
        }
    }

    #[inline]
    pub fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        match self {
            Self::Rowset(exec) => exec.poll_next(ctx, global, local, output),
            Self::Values(exec) => exec.poll_next(ctx, global, local, output),
            Self::Dummy(exec) => exec.poll_next(ctx, global, local, output),
            Self::Empty(exec) => exec.poll_next(ctx, global, local, output),
            Self::Chunk(exec) => exec.poll_next(ctx, global, local, output),
            Self::Expression(exec) => exec.poll_next(ctx, global, local, output),
            Self::TableFunction(exec) => exec.poll_next(ctx, global, local, output),
            Self::VectorSearch(exec) => exec.poll_next(ctx, global, local, output),
            Self::SparseVectorSearch(exec) => exec.poll_next(ctx, global, local, output),
            Self::FullTextSearch(exec) => exec.poll_next(ctx, global, local, output),
            Self::AdaptiveSearch(exec) => exec.poll_next(ctx, global, local, output),
            Self::GraphScan(exec) => exec.poll_next(ctx, global, local, output),
            Self::ExternalTable(exec) => exec.poll_next(ctx, global, local, output),
            Self::Materialized(exec) => exec.poll_next(ctx, global, local, output),
            Self::ClassicIeJoin(exec) => exec.poll_next(ctx, global, local, output),
            Self::NljUnmatched(exec) => exec.poll_next(ctx, global, local, output),
            Self::HashJoinSpillReplay(exec) => exec.poll_next(ctx, global, local, output),
            Self::HashJoinUnmatched(exec) => exec.poll_next(ctx, global, local, output),
            Self::HashAggregateEmit(exec) => exec.poll_next(ctx, global, local, output),
            Self::UngroupedAggregateEmit(exec) => exec.poll_next(ctx, global, local, output),
            Self::PerfectHashAggregateEmit(exec) => exec.poll_next(ctx, global, local, output),
            Self::SortEmit(exec) => exec.poll_next(ctx, global, local, output),
            Self::TopNEmit(exec) => exec.poll_next(ctx, global, local, output),
            Self::WindowEmit(exec) => exec.poll_next(ctx, global, local, output),
            Self::PartitionAggregateWindowEmit(exec) => exec.poll_next(ctx, global, local, output),
            Self::SetOperationEmit(exec) => exec.poll_next(ctx, global, local, output),
            Self::CteScan(exec) => exec.poll_next(ctx, global, local, output),
            Self::DelimScan(exec) => exec.poll_next(ctx, global, local, output),
            Self::RecursiveTableScan(exec) => exec.poll_next(ctx, global, local, output),
            Self::Dyn(exec) => exec.poll_next(ctx, global, local, output),
        }
    }
}

pub trait DynSourceExec: Send + Sync + std::fmt::Debug {
    fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal>;
    fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        global: &SourceGlobal,
    ) -> Result<SourceLocal>;
    fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll>;
}

#[derive(Debug)]
pub enum SourcePoll {
    Output,
    Finished,
    Pending(Blocker),
}
