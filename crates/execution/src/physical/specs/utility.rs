// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_catalog::entry::{CreateIndexInfo, TableCatalogEntry};
use paro_common::types::LogicalType;
use paro_function::copy::{CopyFunctionBindData, CopyToFunction};
use paro_planner::binder::ir::statement::{
    BoundAlterEntryInfo, BoundCreatePropertyGraphInfo, BoundCreateRoutineInfo,
    BoundCreateSchemaInfo, BoundCreateSequenceInfo, BoundCreateTableInfo, BoundCreateViewInfo,
    BoundDropInfo, BoundDropPropertyGraphInfo, BoundRefreshPropertyGraphInfo,
};
use paro_planner::binder::ir::CTEMaterialize;
use paro_planner::operator::SetOpType;

#[derive(Debug, Clone)]
pub struct MaterializedCteSpec {
    pub cte_index: usize,
    pub cte_name: String,
    pub materialized: CTEMaterialize,
    pub ref_count: usize,
    pub column_names: Box<[String]>,
    pub column_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct RecursiveCteSpec {
    pub cte_index: usize,
    pub cte_name: String,
    pub column_names: Box<[String]>,
    pub column_types: Box<[LogicalType]>,
    pub union_all: bool,
}

#[derive(Debug, Clone)]
pub struct CteScanSpec {
    pub cte_index: usize,
    pub table_index: usize,
    pub relation_alias: String,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct SetOperationSpec {
    pub table_index: usize,
    pub op: SetOpType,
    pub all: bool,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOperationInputSide {
    Left,
    Right,
}

#[derive(Clone)]
pub struct CopyToFileSpec {
    pub copy_function: CopyToFunction,
    pub bind_data: Arc<dyn CopyFunctionBindData>,
    pub file_path: String,
    pub per_thread_output: bool,
    pub output_types: Box<[LogicalType]>,
}

impl std::fmt::Debug for CopyToFileSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CopyToFileSpec")
            .field("copy_function", &"copy_to")
            .field("file_path", &self.file_path)
            .field("per_thread_output", &self.per_thread_output)
            .field("output_types", &self.output_types)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub enum UtilitySpec {
    CreateTable(BoundCreateTableInfo),
    CreateView(BoundCreateViewInfo),
    CreateSchema(BoundCreateSchemaInfo),
    CreateSequence(BoundCreateSequenceInfo),
    CreateIndex(CreateIndexUtilitySpec),
    CreateRoutine(BoundCreateRoutineInfo),
    CreatePropertyGraph(BoundCreatePropertyGraphInfo),
    Alter(BoundAlterEntryInfo),
    Drop(BoundDropInfo),
    DropPropertyGraph(BoundDropPropertyGraphInfo),
    RefreshPropertyGraph(BoundRefreshPropertyGraphInfo),
    Unsupported(UnsupportedUtilitySpec),
}

#[derive(Debug, Clone)]
pub struct CreateIndexUtilitySpec {
    pub table: Arc<TableCatalogEntry>,
    pub info: CreateIndexInfo,
}

#[derive(Debug, Clone)]
pub struct UnsupportedUtilitySpec {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct UnsupportedSpec {
    pub logical_name: String,
    pub reason: String,
}
