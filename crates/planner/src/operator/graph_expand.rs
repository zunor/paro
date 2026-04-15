//! Logical Graph Expand Operator
//!
//! Expands from a set of source vertices along edges to their neighbors.
//! Produced by GraphMatchDecompose for each (edge, vertex) pair in the pattern.

use paro_catalog::entry::EdgeTableInfo;
use paro_parser::ast::{PathMode, PathQuantifier};

use crate::expression::Expression;
use crate::plan::LogicalPlan;

/// Direction of edge expansion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpandDirection {
    Forward,
    Backward,
    Both,
}

/// GraphExpand expands from source vertices along edges to neighbors.
///
/// Takes input containing source vertex IDs and produces tuples of
/// `(src_local_id, src_rowid, edge_rowid, dst_local_id, dst_rowid)`.
#[derive(Debug)]
pub struct GraphExpand {
    /// Edge table metadata from the property graph definition.
    pub edge_info: EdgeTableInfo,
    /// Expansion direction.
    pub direction: ExpandDirection,
    /// Source vertex label for this expansion step.
    pub source_label: String,
    /// Optional filter on edge properties.
    pub edge_filter: Option<Expression>,
    /// Optional filter on target vertex properties.
    pub target_filter: Option<Expression>,
    /// Optional path quantifier for multi-hop expansion.
    pub quantifier: Option<PathQuantifier>,
    /// Optional path mode (ANY SHORTEST, ALL SHORTEST, etc.)
    pub path_mode: Option<PathMode>,
    /// Table index of the source vertex variable.
    pub source_table_index: usize,
    /// Table index of the edge variable.
    pub edge_table_index: usize,
    /// Table index of the target vertex variable.
    pub target_table_index: usize,
    /// Target vertex label.
    pub target_label: String,
    /// Source vertex table oid for path materialization.
    pub source_table_oid: u64,
    /// Target vertex table oid for path materialization.
    pub target_table_oid: u64,
    /// Target vertex table name (for late materialization rowid mapping).
    pub target_table_name: String,
    /// Whether path functions (path_length, etc.) are used in COLUMNS.
    pub has_path_functions: bool,
    /// The child operator (source of vertex IDs).
    pub child: Box<LogicalPlan>,
}

impl GraphExpand {
    pub fn new(
        edge_info: EdgeTableInfo,
        direction: ExpandDirection,
        source_label: String,
        source_table_index: usize,
        edge_table_index: usize,
        target_table_index: usize,
        target_label: String,
        source_table_oid: u64,
        target_table_oid: u64,
        target_table_name: String,
        child: LogicalPlan,
    ) -> Self {
        Self {
            edge_info,
            direction,
            source_label,
            edge_filter: None,
            target_filter: None,
            quantifier: None,
            path_mode: None,
            source_table_index,
            edge_table_index,
            target_table_index,
            target_label,
            source_table_oid,
            target_table_oid,
            target_table_name,
            has_path_functions: false,
            child: Box::new(child),
        }
    }
}
