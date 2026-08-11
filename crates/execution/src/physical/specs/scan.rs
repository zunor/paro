// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_catalog::entry::TableCatalogEntry;
use paro_common::chunk::Chunk;
use paro_common::types::LogicalType;
use paro_function::table::{BoundTableFunctionData, TableFunction};
use paro_planner::expression::Expression;
use paro_storage::index::PredicateTree;
use paro_storage::table::segment_reorderer::SegmentOrderOptions;

#[derive(Debug, Clone)]
pub struct RowsetScanSpec {
    pub table_index: usize,
    pub output_names: Box<[String]>,
    pub returned_types: Box<[LogicalType]>,
    pub relation_name: Option<String>,
    pub relation_alias: Option<String>,
    pub column_projection: RowsetColumnProjection,
    pub emit_row_id: bool,
    pub column_types: Box<[LogicalType]>,
    pub table: Arc<TableCatalogEntry>,
    pub predicate: Option<PredicateTree>,
    pub residual_predicates: Box<[Expression]>,
    pub late_materialize: bool,
    pub scan_access_cost: paro_storage::rowset::scan_cost::ScanAccessCostModel,
    pub scan_order: Option<SegmentOrderOptions>,
    pub runtime_filter_expressions: Box<[Expression]>,
}

/// Exact physical base-table projection.
///
/// An empty projection means that only row cardinality is requested. Reading
/// every table column is represented by listing every column explicitly at the
/// logical scan boundary, so no layer has to guess what an empty list means.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowsetColumnProjection(Box<[usize]>);

impl RowsetColumnProjection {
    pub fn new(columns: impl Into<Box<[usize]>>) -> Self {
        Self(columns.into())
    }

    pub fn column_id(&self, output_index: usize) -> Option<usize> {
        self.0.get(output_index).copied()
    }

    pub fn columns(&self) -> &[usize] {
        &self.0
    }
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
    pub bind_data: Option<BoundTableFunctionData>,
    pub table_index: usize,
    pub arguments: Box<[Expression]>,
    pub projection_ids: Option<Box<[usize]>>,
    pub input_table_types: Box<[LogicalType]>,
    pub input_table_names: Box<[String]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
    pub with_ordinality: bool,
}
