// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable specs stored in `PhysicalNodeKind`.
//!
//! Expression-bearing specs keep `paro_planner::expression::Expression` as a
//! bridge from the current planner. These fields are still immutable plan
//! semantics, not runtime executor state; they should eventually lower to a
//! physical expression program before runtime execution.

use std::sync::Arc;

use paro_catalog::entry::{CreateIndexInfo, EdgeTableInfo, TableCatalogEntry, VertexTableInfo};
use paro_common::chunk::Chunk;
use paro_common::types::LogicalType;
use paro_function::copy::{CopyFunction, CopyFunctionBindData};
use paro_function::table::TableFunction;
use paro_parser::ast::PathMode;
use paro_planner::binder::ir::statement::{
    BoundAlterEntryInfo, BoundCreatePropertyGraphInfo, BoundCreateRoutineInfo,
    BoundCreateSchemaInfo, BoundCreateSequenceInfo, BoundCreateTableInfo, BoundCreateViewInfo,
    BoundDropInfo, BoundDropPropertyGraphInfo, BoundRefreshPropertyGraphInfo,
};
use paro_planner::binder::ir::CTEMaterialize;
use paro_planner::binder::ir::OrderByNode;
use paro_planner::expression::{Expression, WindowExpression};
use paro_planner::operator::external_project::{ExternalCostEstimate, ExternalProjectExpression};
use paro_planner::operator::graph_expand::ExpandDirection;
use paro_planner::operator::join::{JoinCondition, JoinType};
use paro_planner::operator::{InsertOnConflict, SearchDecision, SetOpType};
use paro_storage::index::hnsw::types::SearchParams;
use paro_storage::index::PredicateTree;
use paro_storage::rowset::SparseVector;
use paro_storage::search::{
    FullTextQueryKind, FullTextQueryStats, FullTextScoreMode, NormalizedSearchRequest,
    SearchRequestMode,
};
use paro_storage::table::segment_reorderer::SegmentOrderOptions;

use crate::operators::external::runtime_bridge::{
    ExternalRoutineDescriptor, ExternalRuntimeBridge,
};

#[derive(Debug, Clone)]
pub enum PhysicalNodeKind {
    RowsetScan(RowsetScanSpec),
    DummyScan(DummyScanSpec),
    Values(ValuesSpec),
    EmptyResult(EmptyResultSpec),
    Filter(FilterSpec),
    Project(ProjectSpec),
    Limit(LimitSpec),
    Sort(SortSpec),
    TopN(TopNSpec),
    HashJoin(HashJoinSpec),
    NestedLoopJoin(NestedLoopJoinSpec),
    IEJoin(IEJoinSpec),
    CrossProduct(CrossProductSpec),
    DelimJoin(DelimJoinSpec),
    DelimScan(DelimScanSpec),
    MaterializedCte(MaterializedCteSpec),
    RecursiveCte(RecursiveCteSpec),
    CteScan(CteScanSpec),
    SetOperation(SetOperationSpec),
    Aggregate(AggregateSpec),
    Window(WindowSpec),
    ChunkScan(ChunkScanSpec),
    ExpressionScan(ExpressionScanSpec),
    TableFunctionScan(TableFunctionScanSpec),
    VectorSearch(VectorSearchSpec),
    SparseVectorSearch(SparseVectorSearchSpec),
    FullTextSearch(FullTextSearchSpec),
    AdaptiveSearch(AdaptiveSearchSpec),
    GraphScan(GraphScanSpec),
    GraphExpand(GraphExpandSpec),
    GraphProject(GraphProjectSpec),
    GraphShortestPath(GraphShortestPathSpec),
    ExternalProject(ExternalProjectSpec),
    ExternalTable(ExternalTableSpec),
    Insert(InsertSpec),
    Update(UpdateSpec),
    Delete(DeleteSpec),
    CopyToFile(CopyToFileSpec),
    Utility(UtilitySpec),
    Unsupported(UnsupportedSpec),
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
            Self::TopN(_) => "TOP_N",
            Self::HashJoin(_) => "HASH_JOIN",
            Self::NestedLoopJoin(_) => "NESTED_LOOP_JOIN",
            Self::IEJoin(_) => "IE_JOIN",
            Self::CrossProduct(_) => "CROSS_PRODUCT",
            Self::DelimJoin(_) => "DELIM_JOIN",
            Self::DelimScan(_) => "DELIM_SCAN",
            Self::MaterializedCte(_) => "MATERIALIZED_CTE",
            Self::RecursiveCte(_) => "RECURSIVE_CTE",
            Self::CteScan(_) => "CTE_SCAN",
            Self::SetOperation(_) => "SET_OPERATION",
            Self::Aggregate(_) => "AGGREGATE",
            Self::Window(_) => "WINDOW",
            Self::ChunkScan(_) => "CHUNK_SCAN",
            Self::ExpressionScan(_) => "EXPRESSION_SCAN",
            Self::TableFunctionScan(_) => "TABLE_FUNCTION_SCAN",
            Self::VectorSearch(_) => "VECTOR_SEARCH",
            Self::SparseVectorSearch(_) => "SPARSE_VECTOR_SEARCH",
            Self::FullTextSearch(_) => "FULLTEXT_SCAN",
            Self::AdaptiveSearch(_) => "ADAPTIVE_SEARCH",
            Self::GraphScan(_) => "GRAPH_SCAN",
            Self::GraphExpand(_) => "GRAPH_EXPAND",
            Self::GraphProject(_) => "GRAPH_PROJECT",
            Self::GraphShortestPath(_) => "GRAPH_SHORTEST_PATH",
            Self::ExternalProject(_) => "EXTERNAL_PROJECT",
            Self::ExternalTable(_) => "EXTERNAL_TABLE",
            Self::Insert(_) => "INSERT",
            Self::Update(_) => "UPDATE",
            Self::Delete(_) => "DELETE",
            Self::CopyToFile(_) => "COPY_TO_FILE",
            Self::Utility(_) => "UTILITY",
            Self::Unsupported(_) => "UNSUPPORTED",
        }
    }
}

#[derive(Debug, Clone)]
pub struct RowsetScanSpec {
    pub table_index: usize,
    pub output_names: Box<[String]>,
    pub returned_types: Box<[LogicalType]>,
    pub relation_name: Option<String>,
    pub relation_alias: Option<String>,
    pub column_ids: Box<[usize]>,
    pub emit_row_id: bool,
    pub column_types: Box<[LogicalType]>,
    pub table: Arc<TableCatalogEntry>,
    pub scan_order: Option<SegmentOrderOptions>,
    pub runtime_filter_expressions: Box<[Expression]>,
}

#[derive(Debug, Clone)]
pub struct DummyScanSpec;

#[derive(Debug, Clone)]
pub struct ValuesSpec {
    pub table_index: usize,
    pub expressions: Box<[Box<[Expression]>]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct EmptyResultSpec;

#[derive(Debug, Clone)]
pub struct FilterSpec {
    pub expressions: Box<[Expression]>,
    pub projection_map: Box<[usize]>,
}

#[derive(Debug, Clone)]
pub struct ProjectSpec {
    pub table_index: usize,
    pub expressions: Box<[Expression]>,
    pub output_names: Box<[String]>,
}

#[derive(Debug, Clone)]
pub struct LimitSpec {
    pub limit: Option<Expression>,
    pub offset: Option<Expression>,
    pub hnsw_ef_hint: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct SortSpec {
    pub orders: Box<[OrderByNode]>,
    pub projection_map: Box<[usize]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct TopNSpec {
    pub orders: Box<[OrderByNode]>,
    pub limit: usize,
    pub offset: usize,
    pub hnsw_ef_hint: Option<usize>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct HashJoinSpec {
    pub join_type: JoinType,
    pub conditions: Box<[JoinCondition]>,
    pub left_projection: Box<[usize]>,
    pub right_projection: Box<[usize]>,
    pub left_output_types: Box<[LogicalType]>,
    pub right_output_types: Box<[LogicalType]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
    pub force_external: bool,
}

#[derive(Debug, Clone)]
pub struct NestedLoopJoinSpec {
    pub join_type: JoinType,
    pub conditions: Box<[JoinCondition]>,
    pub mark_null_condition_start: Option<usize>,
    pub arbitrary_condition: Option<Expression>,
    pub left_projection: Box<[usize]>,
    pub right_projection: Box<[usize]>,
    pub left_output_types: Box<[LogicalType]>,
    pub right_output_types: Box<[LogicalType]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct IEJoinSpec {
    pub join_type: JoinType,
    pub conditions: Box<[JoinCondition]>,
    pub mark_null_condition_start: Option<usize>,
    pub left_projection: Box<[usize]>,
    pub right_projection: Box<[usize]>,
    pub left_output_types: Box<[LogicalType]>,
    pub right_output_types: Box<[LogicalType]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct CrossProductSpec {
    pub left_output_types: Box<[LogicalType]>,
    pub right_output_types: Box<[LogicalType]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct DelimJoinSpec {
    pub side: DelimJoinSideSpec,
    pub duplicate_keys: Box<[Expression]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelimJoinSideSpec {
    Left,
    Right,
}

#[derive(Debug, Clone)]
pub struct DelimScanSpec {
    pub target: DelimScanTarget,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelimScanTarget {
    Values { table_index: usize },
    CachedOuter,
}

#[derive(Debug, Clone)]
pub struct MaterializedCteSpec {
    pub cte_index: usize,
    pub cte_name: String,
    pub materialized: CTEMaterialize,
    pub ref_count: usize,
    pub column_names: Box<[String]>,
    pub column_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct RecursiveCteSpec {
    pub cte_index: usize,
    pub cte_name: String,
    pub column_names: Box<[String]>,
    pub column_types: Box<[LogicalType]>,
    pub union_all: bool,
}

#[derive(Debug, Clone)]
pub struct CteScanSpec {
    pub cte_index: usize,
    pub table_index: usize,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct SetOperationSpec {
    pub table_index: usize,
    pub op: SetOpType,
    pub all: bool,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOperationInputSide {
    Left,
    Right,
}

#[derive(Debug, Clone)]
pub struct AggregateSpec {
    pub grouping_key_count: usize,
    pub projection_exprs: Box<[Expression]>,
    pub payload_types: Box<[LogicalType]>,
    pub groups: Box<[Expression]>,
    pub grouping_sets: Box<[Box<[usize]>]>,
    pub aggregates: Box<[Expression]>,
    pub grouping_functions: Box<[Box<[usize]>]>,
    pub aggregate_inputs: Box<[Box<[usize]>]>,
    pub aggregate_filters: Box<[Option<usize>]>,
    pub aggregate_orders: Box<[Box<[usize]>]>,
    pub perfect_hash: Option<PerfectHashAggregatePlan>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct PerfectHashAggregatePlan {
    pub group_minima: Box<[i128]>,
    pub required_bits: Box<[usize]>,
}

#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub window_index: usize,
    pub expressions: Box<[WindowExpression]>,
    pub input_width: usize,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct ChunkScanSpec {
    pub chunks: Arc<[Chunk]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct ExpressionScanSpec {
    pub table_index: usize,
    pub expressions: Box<[Box<[Expression]>]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct TableFunctionScanSpec {
    pub function: Arc<TableFunction>,
    pub table_index: usize,
    pub arguments: Box<[Expression]>,
    pub projection_ids: Option<Box<[usize]>>,
    pub input_table_types: Box<[LogicalType]>,
    pub input_table_names: Box<[String]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
    pub with_ordinality: bool,
}

#[derive(Debug, Clone)]
pub struct VectorSearchSpec {
    pub table: Arc<TableCatalogEntry>,
    pub column_id: usize,
    pub query_vector: Vec<f32>,
    pub k: usize,
    pub params: SearchParams,
    pub predicate: Option<PredicateTree>,
    pub projected_columns: Box<[usize]>,
    pub emit_score: bool,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct SparseVectorSearchSpec {
    pub table: Arc<TableCatalogEntry>,
    pub column_id: usize,
    pub query_vector: SparseVector,
    pub k: usize,
    pub predicate: Option<PredicateTree>,
    pub projected_columns: Box<[usize]>,
    pub emit_score: bool,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct FullTextSearchSpec {
    pub table: Arc<TableCatalogEntry>,
    pub column_id: usize,
    pub query: String,
    pub query_kind: FullTextQueryKind,
    pub query_stats: FullTextQueryStats,
    pub config: String,
    pub score_mode: FullTextScoreMode,
    pub mode: SearchRequestMode,
    pub predicate: Option<PredicateTree>,
    pub projected_columns: Box<[usize]>,
    pub emit_score: bool,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct AdaptiveSearchSpec {
    pub table: Arc<TableCatalogEntry>,
    pub request: NormalizedSearchRequest,
    pub decision: SearchDecision,
    pub selected: Box<SearchSourceSpec>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub enum SearchSourceSpec {
    Vector(VectorSearchSpec),
    Sparse(SparseVectorSearchSpec),
    FullText(FullTextSearchSpec),
}

#[derive(Debug, Clone)]
pub struct GraphScanSpec {
    pub vertex_info: VertexTableInfo,
    pub filter: Option<Expression>,
    pub table_index: usize,
    pub label: String,
    pub graph_name: String,
    pub schema_name: String,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct GraphExpandSpec {
    pub graph_name: String,
    pub schema_name: String,
    pub edge_info: EdgeTableInfo,
    pub direction: ExpandDirection,
    pub source_label: String,
    pub edge_filter: Option<Expression>,
    pub target_filter: Option<Expression>,
    pub source_table_index: usize,
    pub edge_table_index: usize,
    pub target_table_index: usize,
    pub target_label: String,
    pub source_local_col_idx: usize,
    pub source_rowid_col_idx: usize,
    pub min_hops: u64,
    pub max_hops: u64,
    pub source_table_oid: u64,
    pub target_table_oid: u64,
    pub target_table_name: String,
    pub has_path_functions: bool,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct GraphRowidMapping {
    pub table_index: usize,
    pub rowid_col_idx: usize,
    pub table_name: String,
    pub schema_name: String,
}

#[derive(Debug, Clone)]
pub struct GraphProjectSpec {
    pub expressions: Box<[Expression]>,
    pub filters: Box<[Expression]>,
    pub rowid_mappings: Box<[GraphRowidMapping]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct GraphShortestPathSpec {
    pub graph_name: String,
    pub edge_info: EdgeTableInfo,
    pub direction: ExpandDirection,
    pub source_label: String,
    pub target_label: String,
    pub source_local_col_idx: usize,
    pub source_rowid_col_idx: usize,
    pub target_local_col_idx: Option<usize>,
    pub source_table_oid: u64,
    pub target_table_oid: u64,
    pub min_hops: u64,
    pub max_hops: u64,
    pub path_mode: Option<PathMode>,
    pub target_filter: Option<Expression>,
    pub has_path_functions: bool,
    pub target_table_name: String,
    pub schema_name: String,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct ExternalProjectSpec {
    pub routines: Box<[ExternalRoutineDescriptor]>,
    pub expressions: Box<[ExternalProjectExpression]>,
    pub cost: ExternalCostEstimate,
    pub bridge: Arc<ExternalRuntimeBridge>,
    pub input_names: Box<[String]>,
    pub input_types: Box<[LogicalType]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct ExternalTableSpec {
    pub routine: ExternalRoutineDescriptor,
    pub worker_output_types: Box<[LogicalType]>,
    pub emitted_output_types: Box<[LogicalType]>,
    pub argument_count: usize,
    pub lateral: bool,
    pub parameterized: bool,
    pub estimated_cardinality: usize,
    pub cost: ExternalCostEstimate,
    pub bridge: Arc<ExternalRuntimeBridge>,
}

#[derive(Debug, Clone)]
pub struct InsertSpec {
    pub table: Arc<TableCatalogEntry>,
    pub column_index_map: Box<[usize]>,
    pub expected_types: Box<[LogicalType]>,
    pub on_conflict: Option<InsertOnConflict>,
    pub copy_from_read_csv: bool,
}

#[derive(Debug, Clone)]
pub struct UpdateSpec {
    pub table: Arc<TableCatalogEntry>,
    pub columns: Box<[usize]>,
    pub row_id_index: usize,
}

#[derive(Debug, Clone)]
pub struct DeleteSpec {
    pub table: Arc<TableCatalogEntry>,
    pub row_id_index: usize,
    pub is_full_table_delete: bool,
}

#[derive(Clone)]
pub struct CopyToFileSpec {
    pub copy_function: CopyFunction,
    pub bind_data: Arc<dyn CopyFunctionBindData>,
    pub file_path: String,
    pub per_thread_output: bool,
    pub output_types: Box<[LogicalType]>,
}

impl std::fmt::Debug for CopyToFileSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CopyToFileSpec")
            .field("copy_function", &self.copy_function.name)
            .field("file_path", &self.file_path)
            .field("per_thread_output", &self.per_thread_output)
            .field("output_types", &self.output_types)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub enum UtilitySpec {
    CreateTable(BoundCreateTableInfo),
    CreateView(BoundCreateViewInfo),
    CreateSchema(BoundCreateSchemaInfo),
    CreateSequence(BoundCreateSequenceInfo),
    CreateIndex(CreateIndexUtilitySpec),
    CreateRoutine(BoundCreateRoutineInfo),
    CreatePropertyGraph(BoundCreatePropertyGraphInfo),
    Alter(BoundAlterEntryInfo),
    Drop(BoundDropInfo),
    DropPropertyGraph(BoundDropPropertyGraphInfo),
    RefreshPropertyGraph(BoundRefreshPropertyGraphInfo),
    Unsupported(UnsupportedUtilitySpec),
}

#[derive(Debug, Clone)]
pub struct CreateIndexUtilitySpec {
    pub table: Arc<TableCatalogEntry>,
    pub info: CreateIndexInfo,
}

#[derive(Debug, Clone)]
pub struct UnsupportedUtilitySpec {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct UnsupportedSpec {
    pub logical_name: String,
    pub reason: String,
}
