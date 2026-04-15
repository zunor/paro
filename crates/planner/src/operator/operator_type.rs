//! Logical Operator Type
//!
//! Enumerates the types of logical operators.

/// LogicalOperatorType enumerates the types of logical operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalOperatorType {
    Get,
    Filter,
    Projection,
    Limit,
    Order,
    /// TopN (optimized ORDER BY + LIMIT)
    TopN,
    Alter,
    CreateTable,
    CreateSequence,
    CreateSchema,
    /// CREATE INDEX operation
    CreateIndex,
    Drop,
    Insert,
    Delete,
    Update,
    LogicalCopy,
    Explain,
    EmptyResult,
    Aggregate,
    ComparisonJoin,
    AnyJoin,
    CrossProduct,
    DelimGet,
    /// Dependent join (for correlated subqueries)
    DependentJoin,
    /// UNION operation
    LogicalUnion,
    /// INTERSECT operation
    LogicalIntersect,
    /// EXCEPT operation
    LogicalExcept,
    /// DISTINCT operation
    Distinct,
    /// Window function operation
    Window,
    /// Materialized CTE wrapper
    MaterializedCTE,
    /// Recursive CTE producer
    RecursiveCTE,
    /// CTE reference
    CTERef,
    /// Table function scan
    TableFunctionGet,
    /// Search path scan replacing TopN/Projection/Filter/Get subgraphs.
    SearchScan,
    /// Full-text filter scan replacing Filter/Get subgraphs.
    FullTextFilterScan,
    /// CREATE VIEW operation
    CreateView,
    /// CREATE PROPERTY GRAPH operation
    CreatePropertyGraph,
    /// DROP PROPERTY GRAPH operation
    DropPropertyGraph,
    /// REFRESH PROPERTY GRAPH operation
    RefreshPropertyGraph,
    /// Graph pattern match (undecomposed GRAPH_TABLE)
    GraphMatch,
    /// Graph vertex scan
    GraphScan,
    /// Graph edge expansion
    GraphExpand,
}
