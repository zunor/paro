// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical operator types enumeration.
//!
//!

use std::fmt;

/// Physical operator type enumeration.
///
/// Categorizes all physical operators in the execution engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum PhysicalOperatorType {
    /// Invalid operator (placeholder)
    #[default]
    Invalid = 0,

    // ========== Basic Operators ==========
    /// ORDER BY clause
    OrderBy,
    /// TOP N (ORDER BY + LIMIT optimized)
    TopN,
    /// LIMIT clause
    Limit,
    /// Streaming LIMIT
    StreamingLimit,
    /// Window functions
    Window,
    /// UNNEST operation
    Unnest,
    /// Ungrouped aggregation (e.g., COUNT(*) without GROUP BY)
    UngroupedAggregate,
    /// Hash-based GROUP BY
    HashGroupBy,
    /// Perfect hash GROUP BY (for small cardinality)
    PerfectHashGroupBy,
    /// FILTER clause
    Filter,
    /// PROJECTION (SELECT columns)
    Projection,

    // ========== Scans ==========
    /// Rowset scan (Paro storage engine)
    RowsetScan,
    /// Vector similarity search (HNSW)
    VectorScan,
    /// Sparse vector search
    SparseVectorScan,
    /// Full-text search
    FullTextScan,
    /// Runtime adaptive search-path selection
    AdaptiveScan,
    /// Dummy scan (empty result)
    DummyScan,
    /// Column data scan
    ColumnDataScan,
    /// Sink that materializes rows into a column-data collection
    ColumnDataSink,
    /// Chunk scan
    ChunkScan,
    /// Expression scan
    ExpressionScan,
    /// Positional scan
    PositionalScan,
    /// Table function scan
    TableFunctionScan,
    InOutFunction,

    // ========== Joins ==========
    /// Blockwise nested loop join
    BlockwiseNLJoin,
    /// Nested loop join
    NestedLoopJoin,
    /// Hash join
    HashJoin,
    /// Left delim join
    LeftDelimJoin,
    /// Right delim join
    RightDelimJoin,
    /// Cross product (CROSS JOIN)
    CrossProduct,
    /// Piecewise merge join
    PiecewiseMergeJoin,
    /// Inequality join
    IEJoin,
    /// Positional join
    PositionalJoin,

    // ========== Set Operations ==========
    /// UNION
    Union,
    /// CTE (Common Table Expression)
    Cte,
    /// CTE Scan (scan materialized CTE results)
    CteScan,
    /// Recursive CTE
    RecursiveCte,

    // ========== DML Operations ==========
    /// INSERT
    Insert,
    /// DELETE
    Delete,
    /// UPDATE
    Update,
    /// COPY TO file
    CopyToFile,

    // ========== DDL Operations ==========
    /// CREATE TABLE
    CreateTable,
    /// CREATE TABLE AS SELECT
    CreateTableAs,
    /// CREATE INDEX
    CreateIndex,
    /// ALTER
    Alter,
    /// CREATE SEQUENCE
    CreateSequence,
    /// CREATE VIEW
    CreateView,
    /// CREATE SCHEMA
    CreateSchema,
    /// DROP
    Drop,
    /// CREATE PROPERTY GRAPH
    CreatePropertyGraph,
    /// DROP PROPERTY GRAPH
    DropPropertyGraph,
    /// REFRESH PROPERTY GRAPH
    RefreshPropertyGraph,

    // ========== Graph Query ==========
    /// Graph vertex scan
    GraphScan,
    /// Graph edge expand
    GraphExpand,
    /// Graph late materialization project
    GraphProject,
    /// Graph BFS shortest path
    GraphShortestPath,

    // ========== Utility ==========
    /// EXPLAIN
    Explain,
    /// EXPLAIN ANALYZE
    ExplainAnalyze,
    /// Empty result
    EmptyResult,
    /// PREPARE statement
    Prepare,
    /// EXECUTE statement
    Execute,
    /// Result collector
    ResultCollector,
}

impl PhysicalOperatorType {
    /// Convert operator type to string representation.
    pub fn to_string(&self) -> &'static str {
        match self {
            Self::Invalid => "INVALID",
            Self::OrderBy => "ORDER_BY",
            Self::TopN => "TOP_N",
            Self::Limit => "LIMIT",
            Self::StreamingLimit => "STREAMING_LIMIT",
            Self::Window => "WINDOW",
            Self::Unnest => "UNNEST",
            Self::UngroupedAggregate => "UNGROUPED_AGGREGATE",
            Self::HashGroupBy => "HASH_GROUP_BY",
            Self::PerfectHashGroupBy => "PERFECT_HASH_GROUP_BY",
            Self::Filter => "FILTER",
            Self::Projection => "PROJECTION",
            Self::RowsetScan => "ROWSET_SCAN",
            Self::VectorScan => "VECTOR_SCAN",
            Self::SparseVectorScan => "SPARSE_VECTOR_SCAN",
            Self::FullTextScan => "FULLTEXT_SCAN",
            Self::AdaptiveScan => "ADAPTIVE_SCAN",
            Self::DummyScan => "DUMMY_SCAN",
            Self::ColumnDataScan => "COLUMN_DATA_SCAN",
            Self::ColumnDataSink => "COLUMN_DATA_SINK",
            Self::ChunkScan => "CHUNK_SCAN",
            Self::ExpressionScan => "EXPRESSION_SCAN",
            Self::PositionalScan => "POSITIONAL_SCAN",
            Self::TableFunctionScan => "TABLE_FUNCTION_SCAN",
            Self::InOutFunction => "INOUT_FUNCTION",
            Self::BlockwiseNLJoin => "BLOCKWISE_NL_JOIN",
            Self::NestedLoopJoin => "NESTED_LOOP_JOIN",
            Self::HashJoin => "HASH_JOIN",
            Self::LeftDelimJoin => "LEFT_DELIM_JOIN",
            Self::RightDelimJoin => "RIGHT_DELIM_JOIN",
            Self::CrossProduct => "CROSS_PRODUCT",
            Self::PiecewiseMergeJoin => "PIECEWISE_MERGE_JOIN",
            Self::IEJoin => "IE_JOIN",
            Self::PositionalJoin => "POSITIONAL_JOIN",
            Self::Union => "UNION",
            Self::Cte => "CTE",
            Self::CteScan => "CTE_SCAN",
            Self::RecursiveCte => "RECURSIVE_CTE",
            Self::Insert => "INSERT",
            Self::Delete => "DELETE",
            Self::Update => "UPDATE",
            Self::CopyToFile => "COPY_TO_FILE",
            Self::CreateTable => "CREATE_TABLE",
            Self::CreateTableAs => "CREATE_TABLE_AS",
            Self::CreateIndex => "CREATE_INDEX",
            Self::Alter => "ALTER",
            Self::CreateSequence => "CREATE_SEQUENCE",
            Self::CreateView => "CREATE_VIEW",
            Self::CreateSchema => "CREATE_SCHEMA",
            Self::Drop => "DROP",
            Self::CreatePropertyGraph => "CREATE_PROPERTY_GRAPH",
            Self::DropPropertyGraph => "DROP_PROPERTY_GRAPH",
            Self::RefreshPropertyGraph => "REFRESH_PROPERTY_GRAPH",
            Self::GraphScan => "GRAPH_SCAN",
            Self::GraphExpand => "GRAPH_EXPAND",
            Self::GraphProject => "GRAPH_PROJECT",
            Self::GraphShortestPath => "GRAPH_SHORTEST_PATH",
            Self::Explain => "EXPLAIN",
            Self::ExplainAnalyze => "EXPLAIN_ANALYZE",
            Self::EmptyResult => "EMPTY_RESULT",
            Self::Prepare => "PREPARE",
            Self::Execute => "EXECUTE",
            Self::ResultCollector => "RESULT_COLLECTOR",
        }
    }

    /// Check if this operator is a source (produces data without input).
    pub fn is_source(&self) -> bool {
        matches!(
            self,
            Self::RowsetScan
                | Self::VectorScan
                | Self::SparseVectorScan
                | Self::FullTextScan
                | Self::DummyScan
                | Self::ColumnDataScan
                | Self::ColumnDataSink
                | Self::ChunkScan
                | Self::ExpressionScan
                | Self::PositionalScan
                | Self::TableFunctionScan
                | Self::CteScan
                | Self::RecursiveCte
                | Self::Alter
                | Self::CreateSequence
                | Self::CreateSchema
                | Self::CreatePropertyGraph
                | Self::DropPropertyGraph
                | Self::RefreshPropertyGraph
                | Self::GraphScan
                | Self::EmptyResult
        )
    }

    /// Check if this operator is a sink (consumes data and produces side effects).
    pub fn is_sink(&self) -> bool {
        matches!(
            self,
            Self::Insert
                | Self::Delete
                | Self::Update
                | Self::CopyToFile
                | Self::CreateTable
                | Self::CreateTableAs
                | Self::CreateIndex
                | Self::CreateView
                | Self::Drop
                | Self::OrderBy
                | Self::TopN
                | Self::HashGroupBy
                | Self::UngroupedAggregate
                | Self::NestedLoopJoin
                | Self::HashJoin
                | Self::PiecewiseMergeJoin
                | Self::LeftDelimJoin
                | Self::RightDelimJoin
                | Self::Window
                | Self::Cte
                | Self::RecursiveCte
        )
    }
}

impl fmt::Display for PhysicalOperatorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operator_type_strings() {
        assert_eq!(PhysicalOperatorType::RowsetScan.to_string(), "ROWSET_SCAN");
        assert_eq!(PhysicalOperatorType::VectorScan.to_string(), "VECTOR_SCAN");
        assert_eq!(
            PhysicalOperatorType::SparseVectorScan.to_string(),
            "SPARSE_VECTOR_SCAN"
        );
        assert_eq!(
            PhysicalOperatorType::FullTextScan.to_string(),
            "FULLTEXT_SCAN"
        );
    }

    #[test]
    fn test_operator_type_is_source() {
        assert!(PhysicalOperatorType::RowsetScan.is_source());
        assert!(PhysicalOperatorType::VectorScan.is_source());
        assert!(PhysicalOperatorType::SparseVectorScan.is_source());
        assert!(PhysicalOperatorType::FullTextScan.is_source());

        assert!(!PhysicalOperatorType::Filter.is_source());
        assert!(!PhysicalOperatorType::Projection.is_source());
    }

    #[test]
    fn test_operator_type_is_sink_includes_nested_loop_join() {
        assert!(PhysicalOperatorType::NestedLoopJoin.is_sink());
        assert!(PhysicalOperatorType::HashJoin.is_sink());
        assert!(PhysicalOperatorType::PiecewiseMergeJoin.is_sink());
        assert!(!PhysicalOperatorType::Filter.is_sink());
    }
}
