// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_catalog::entry::{EdgeTableInfo, VertexTableInfo};
use paro_common::types::LogicalType;
use paro_parser::ast::PathMode;
use paro_planner::expression::Expression;
use paro_planner::operator::graph_expand::ExpandDirection;

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
