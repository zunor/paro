// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_catalog::entry::TableCatalogEntry;
use paro_common::types::LogicalType;
use paro_planner::operator::InsertOnConflict;

#[derive(Debug, Clone)]
pub struct InsertSpec {
    pub table: Arc<TableCatalogEntry>,
    pub column_index_map: Box<[usize]>,
    pub expected_types: Box<[LogicalType]>,
    pub on_conflict: Option<InsertOnConflict>,
    pub copy_from_read_csv: bool,
}

#[derive(Debug, Clone)]
pub struct UpdateSpec {
    pub table: Arc<TableCatalogEntry>,
    pub columns: Box<[usize]>,
    pub row_id_index: usize,
}

#[derive(Debug, Clone)]
pub struct DeleteSpec {
    pub table: Arc<TableCatalogEntry>,
    pub row_id_index: usize,
    pub is_full_table_delete: bool,
}
