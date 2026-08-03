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
    pub column_ids: Box<[usize]>,
    pub emit_row_id: bool,
    pub column_types: Box<[LogicalType]>,
    pub table: Arc<TableCatalogEntry>,
    pub predicate: Option<PredicateTree>,
    pub residual_predicates: Box<[Expression]>,
    pub late_materialize: bool,
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
