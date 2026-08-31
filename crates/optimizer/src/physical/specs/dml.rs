// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;

use paro_catalog::entry::TableCatalogEntry;
use paro_common::types::LogicalType;
use paro_planner::operator::InsertOnConflict;

use crate::physical::identity::BaseRelationId;
use crate::physical::requirements::MutationSafetyRequirement;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturningImageContract {
    CountOnly,
    BeforeImage,
    AfterImage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteContract {
    pub target_relation: BaseRelationId,
    pub target_object_id: u64,
    pub modified_columns: BTreeSet<usize>,
    pub modified_key_columns: BTreeSet<usize>,
    pub snapshot_version: u64,
    pub mutation_safety: MutationSafetyRequirement,
    pub returning: ReturningImageContract,
}

#[derive(Debug, Clone)]
pub struct InsertSpec {
    pub table: Arc<TableCatalogEntry>,
    pub column_index_map: Box<[usize]>,
    pub expected_types: Box<[LogicalType]>,
    pub on_conflict: Option<InsertOnConflict>,
    pub copy_from_read_csv: bool,
    pub write: WriteContract,
}

#[derive(Debug, Clone)]
pub struct UpdateSpec {
    pub table: Arc<TableCatalogEntry>,
    pub columns: Box<[usize]>,
    pub row_id_index: usize,
    pub write: WriteContract,
}

#[derive(Debug, Clone)]
pub struct DeleteSpec {
    pub table: Arc<TableCatalogEntry>,
    pub row_id_index: usize,
    pub is_full_table_delete: bool,
    pub write: WriteContract,
}
