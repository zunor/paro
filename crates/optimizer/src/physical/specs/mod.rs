// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable specs stored in `PhysicalNodeKind`.
//!
//! Expression-bearing specs retain the planner's bound scalar representation.
//! These fields are immutable plan semantics, not runtime executor state;
//! execution compiles them into physical expression programs before running a
//! pipeline.

/// Optimizer-owned execution contract for algorithms that can spill. Runtime
/// pressure may exercise `Allowed`, but may never upgrade `Forbidden` or
/// reinterpret a mutable session setting as `Forced`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillExecutionPolicy {
    Forbidden,
    Allowed,
    Forced,
}

pub mod aggregate;
pub mod dml;
pub mod enforcer;
pub mod external;
pub mod graph;
pub mod join;
pub mod row_fetch;
pub mod scan;
pub mod search;
pub mod sort;
pub mod utility;
pub mod window;

pub use aggregate::*;
pub use dml::*;
pub use enforcer::*;
pub use external::*;
pub use graph::*;
pub use join::*;
pub use row_fetch::*;
pub use scan::*;
pub use search::*;
pub use sort::*;
pub use utility::*;
pub use window::*;

#[derive(Debug, Clone)]
pub enum PhysicalNodeKind {
    RowsetScan(RowsetScanSpec),
    DummyScan(DummyScanSpec),
    Values(ValuesSpec),
    EmptyResult(EmptyResultSpec),
    Filter(FilterSpec),
    Project(ProjectSpec),
    Limit(Box<LimitSpec>),
    Sort(SortSpec),
    MutationInputSpool(MutationInputSpoolSpec),
    TopN(TopNSpec),
    HashJoin(HashJoinSpec),
    NestedLoopJoin(Box<NestedLoopJoinSpec>),
    SortRangeJoin(SortRangeJoinSpec),
    ClassicIeJoin(ClassicIeJoinSpec),
    CrossProduct(CrossProductSpec),
    DelimJoin(DelimJoinSpec),
    DelimScan(DelimScanSpec),
    MaterializedCte(MaterializedCteSpec),
    RecursiveCte(RecursiveCteSpec),
    CteScan(CteScanSpec),
    SetOperation(SetOperationSpec),
    Aggregate(Box<AggregateSpec>),
    Window(WindowSpec),
    PartitionAggregateWindow(Box<PartitionAggregateWindowSpec>),
    ChunkScan(ChunkScanSpec),
    ExpressionScan(ExpressionScanSpec),
    TableFunctionScan(TableFunctionScanSpec),
    VectorSearch(VectorSearchSpec),
    SparseVectorSearch(SparseVectorSearchSpec),
    FullTextSearch(FullTextSearchSpec),
    AdaptiveSearch(AdaptiveSearchSpec),
    GraphScan(Box<GraphScanSpec>),
    GraphExpand(Box<GraphExpandSpec>),
    RowFetch(RowFetchSpec),
    GraphProject(GraphProjectSpec),
    GraphShortestPath(Box<GraphShortestPathSpec>),
    ExternalProject(ExternalProjectSpec),
    ExternalTable(ExternalTableSpec),
    Insert(InsertSpec),
    Update(UpdateSpec),
    Delete(DeleteSpec),
    CopyToFile(CopyToFileSpec),
    Utility(UtilitySpec),
}

impl PhysicalNodeKind {
    pub fn name(&self) -> &'static str {
        match self {
            Self::RowsetScan(_) => "ROWSET_SCAN",
            Self::DummyScan(_) => "DUMMY_SCAN",
            Self::Values(_) => "VALUES",
            Self::EmptyResult(_) => "EMPTY_RESULT",
            Self::Filter(_) => "FILTER",
            Self::Project(_) => "PROJECT",
            Self::Limit(_) => "LIMIT",
            Self::Sort(_) => "SORT",
            Self::MutationInputSpool(_) => "MUTATION_INPUT_SPOOL",
            Self::TopN(_) => "TOP_N",
            Self::HashJoin(_) => "HASH_JOIN",
            Self::NestedLoopJoin(_) => "NESTED_LOOP_JOIN",
            Self::SortRangeJoin(_) => "SORT_RANGE_JOIN",
            Self::ClassicIeJoin(_) => "CLASSIC_IE_JOIN",
            Self::CrossProduct(_) => "CROSS_PRODUCT",
            Self::DelimJoin(_) => "DELIM_JOIN",
            Self::DelimScan(_) => "DELIM_SCAN",
            Self::MaterializedCte(_) => "MATERIALIZED_CTE",
            Self::RecursiveCte(_) => "RECURSIVE_CTE",
            Self::CteScan(_) => "CTE_SCAN",
            Self::SetOperation(_) => "SET_OPERATION",
            Self::Aggregate(_) => "AGGREGATE",
            Self::Window(_) => "WINDOW",
            Self::PartitionAggregateWindow(_) => "PARTITION_AGGREGATE_WINDOW",
            Self::ChunkScan(_) => "CHUNK_SCAN",
            Self::ExpressionScan(_) => "EXPRESSION_SCAN",
            Self::TableFunctionScan(_) => "TABLE_FUNCTION_SCAN",
            Self::VectorSearch(_) => "VECTOR_SEARCH",
            Self::SparseVectorSearch(_) => "SPARSE_VECTOR_SEARCH",
            Self::FullTextSearch(_) => "FULLTEXT_SCAN",
            Self::AdaptiveSearch(_) => "ADAPTIVE_SEARCH",
            Self::GraphScan(_) => "GRAPH_SCAN",
            Self::GraphExpand(_) => "GRAPH_EXPAND",
            Self::RowFetch(_) => "ROW_FETCH",
            Self::GraphProject(_) => "GRAPH_PROJECT",
            Self::GraphShortestPath(_) => "GRAPH_SHORTEST_PATH",
            Self::ExternalProject(_) => "EXTERNAL_PROJECT",
            Self::ExternalTable(_) => "EXTERNAL_TABLE",
            Self::Insert(_) => "INSERT",
            Self::Update(_) => "UPDATE",
            Self::Delete(_) => "DELETE",
            Self::CopyToFile(_) => "COPY_TO_FILE",
            Self::Utility(_) => "UTILITY",
        }
    }
}

#[cfg(test)]
mod layout_tests {
    use super::PhysicalNodeKind;

    #[test]
    fn physical_node_kind_stays_small_enough_for_arena_transport() {
        let bytes = std::mem::size_of::<PhysicalNodeKind>();
        assert!(
            bytes <= 1024,
            "PhysicalNodeKind is {bytes} bytes; large immutable payloads must be boxed instead of inflating every arena node"
        );
    }
}
