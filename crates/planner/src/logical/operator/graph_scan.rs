// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Graph Scan Operator
//!
//! Scans a vertex table with optional filter. Produced by GraphMatchDecompose
//! as the starting point of a graph pattern traversal.

use paro_catalog::entry::VertexTableInfo;
use paro_common::types::LogicalType;

use crate::expression::Expression;

/// GraphScan scans vertices from a vertex table.
///
/// This is a leaf operator that produces `(local_vertex_id, rowid)` tuples.
/// An optional filter restricts which vertices are returned.
#[derive(Debug, Clone)]
pub struct GraphScan {
    /// Vertex table metadata from the property graph definition.
    pub vertex_info: VertexTableInfo,
    /// Optional filter expression on vertex properties.
    pub filter: Option<Expression>,
    /// Table index for this vertex variable.
    pub table_index: usize,
    /// Binding namespace for the graph chain's physical carrier columns.
    ///
    /// Property expressions keep using `table_index` and are materialized by
    /// GraphProject. The carrier namespace describes the actual columns passed
    /// between GraphScan/GraphExpand operators.
    pub output_table_index: usize,
    /// Vertex label.
    pub label: String,
    /// Graph name (for index lookup at execution time).
    pub graph_name: String,
    /// Schema name (for catalog lookup at execution time).
    pub schema_name: String,
    /// Output column types: [local_vertex_id (UBigInt), rowid (UBigInt)].
    pub output_types: Vec<LogicalType>,
}

impl GraphScan {
    pub fn new(
        vertex_info: VertexTableInfo,
        filter: Option<Expression>,
        table_index: usize,
        output_table_index: usize,
        label: String,
        graph_name: String,
        schema_name: String,
    ) -> Self {
        let output_types = vec![LogicalType::UBigInt, LogicalType::UBigInt];
        Self {
            vertex_info,
            filter,
            table_index,
            output_table_index,
            label,
            graph_name,
            schema_name,
            output_types,
        }
    }
}
