// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Late base-table row fetch after a narrow relational carrier.

use paro_common::types::LogicalType;
use paro_planner::expression::Expression;

#[derive(Debug, Clone)]
pub struct GraphRowFetchMapping {
    /// Private expression namespace for the materialized base-table columns.
    pub table_index: usize,
    /// Physical input column containing stable tablet rowids.
    pub rowid_col_idx: usize,
    pub table_name: String,
    pub schema_name: String,
}

/// One relational base-table fetch over a carried physical rowid column.
#[derive(Debug, Clone)]
pub struct RelationalRowFetchMapping {
    pub table_index: usize,
    pub rowid_col_idx: usize,
    pub table_name: String,
    pub schema_name: String,
    /// Physical catalog ordinals, in the order appended to the carrier.
    pub column_ids: Box<[u32]>,
}

/// Optional physical fusion of the projection immediately above a logical
/// [`paro_planner::operator::RowFetch`]. The RowFetch operator remains a
/// complete standalone transform; fusion only removes an avoidable transform
/// dispatch and intermediate chunk on the common TopN path.
#[derive(Debug, Clone)]
pub struct RowFetchProjectionSpec {
    pub expressions: Box<[Expression]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct RowFetchSpec {
    pub mappings: Box<[RelationalRowFetchMapping]>,
    pub raw_output_names: Box<[String]>,
    pub raw_output_types: Box<[LogicalType]>,
    pub projection: Option<RowFetchProjectionSpec>,
}

#[derive(Debug, Clone)]
pub struct GraphProjectSpec {
    /// Expressions over materialized table namespaces plus the carrier.
    pub expressions: Box<[Expression]>,
    pub filters: Box<[Expression]>,
    pub carrier_table_index: usize,
    pub rowid_mappings: Box<[GraphRowFetchMapping]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}
