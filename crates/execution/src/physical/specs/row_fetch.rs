// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Late base-table row fetch after a narrow relational carrier.

use paro_common::types::LogicalType;
use paro_planner::expression::Expression;

#[derive(Debug, Clone)]
pub struct RowFetchMapping {
    /// Private expression namespace for the materialized base-table columns.
    pub table_index: usize,
    /// Physical input column containing stable tablet rowids.
    pub rowid_col_idx: usize,
    pub table_name: String,
    pub schema_name: String,
}

#[derive(Debug, Clone)]
pub struct RowFetchProjectSpec {
    /// Expressions over materialized table namespaces plus the carrier.
    pub expressions: Box<[Expression]>,
    pub filters: Box<[Expression]>,
    pub carrier_table_index: usize,
    pub rowid_mappings: Box<[RowFetchMapping]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
    pub coalesce_input: bool,
}
