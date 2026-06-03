// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::types::LogicalType;
use paro_planner::binder::ir::OrderByNode;

#[derive(Debug, Clone)]
pub struct SortSpec {
    pub orders: Box<[OrderByNode]>,
    pub projection_map: Box<[usize]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct TopNSpec {
    pub orders: Box<[OrderByNode]>,
    pub limit: usize,
    pub offset: usize,
    pub hnsw_ef_hint: Option<usize>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}
