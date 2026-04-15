//! Logical Graph Match Operator
//!
//! Top-level logical operator for GRAPH_TABLE. The optimizer's GraphMatchDecompose
//! rule will decompose this into a chain of GraphScan + GraphExpand.

use std::sync::Arc;

use paro_catalog::entry::PropertyGraphCatalogEntry;
use paro_common::types::LogicalType;
use paro_parser::ast::PathMode;

use crate::binder::ir::{BoundGraphColumn, BoundGraphPattern};

/// GraphMatch represents an undecomposed GRAPH_TABLE query.
///
/// This is the initial logical operator produced by `plan_graph_table`.
/// The optimizer will decompose it into finer-grained graph operators.
#[derive(Debug, Clone)]
pub struct GraphMatch {
    /// The property graph catalog entry.
    pub graph_entry: Arc<PropertyGraphCatalogEntry>,
    /// The bound pattern (vertex-edge chain).
    pub bound_pattern: BoundGraphPattern,
    /// The COLUMNS projection list.
    pub columns: Vec<BoundGraphColumn>,
    /// Table index assigned to this GRAPH_TABLE reference.
    pub table_index: usize,
    /// Output column types (derived from COLUMNS).
    pub output_types: Vec<LogicalType>,
    /// Optional path mode (ANY SHORTEST, ALL SHORTEST, etc.)
    pub path_mode: Option<PathMode>,
    /// Whether path functions (path_length, etc.) are used in COLUMNS.
    pub has_path_functions: bool,
}

impl GraphMatch {
    pub fn new(
        graph_entry: Arc<PropertyGraphCatalogEntry>,
        bound_pattern: BoundGraphPattern,
        columns: Vec<BoundGraphColumn>,
        table_index: usize,
        output_types: Vec<LogicalType>,
        path_mode: Option<PathMode>,
        has_path_functions: bool,
    ) -> Self {
        Self {
            graph_entry,
            bound_pattern,
            columns,
            table_index,
            output_types,
            path_mode,
            has_path_functions,
        }
    }
}
