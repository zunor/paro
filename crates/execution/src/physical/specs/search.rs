// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_catalog::entry::TableCatalogEntry;
use paro_common::types::LogicalType;
use paro_planner::operator::SearchDecision;
use paro_storage::index::hnsw::types::SearchParams;
use paro_storage::index::PredicateTree;
use paro_storage::rowset::SparseVector;
use paro_storage::search::{
    FullTextQueryKind, FullTextQueryStats, FullTextScoreMode, NormalizedSearchRequest,
    SearchRequestMode,
};

#[derive(Debug, Clone)]
pub struct VectorSearchSpec {
    pub table: Arc<TableCatalogEntry>,
    pub column_id: usize,
    pub query_vector: Vec<f32>,
    pub k: usize,
    pub params: SearchParams,
    pub predicate: Option<PredicateTree>,
    pub projected_columns: Box<[usize]>,
    pub emit_score: bool,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct SparseVectorSearchSpec {
    pub table: Arc<TableCatalogEntry>,
    pub column_id: usize,
    pub query_vector: SparseVector,
    pub k: usize,
    pub predicate: Option<PredicateTree>,
    pub projected_columns: Box<[usize]>,
    pub emit_score: bool,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct FullTextSearchSpec {
    pub table: Arc<TableCatalogEntry>,
    pub column_id: usize,
    pub query: String,
    pub query_kind: FullTextQueryKind,
    pub query_stats: FullTextQueryStats,
    pub config: String,
    pub score_mode: FullTextScoreMode,
    pub mode: SearchRequestMode,
    pub predicate: Option<PredicateTree>,
    pub projected_columns: Box<[usize]>,
    pub emit_score: bool,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct AdaptiveSearchSpec {
    pub table: Arc<TableCatalogEntry>,
    pub request: NormalizedSearchRequest,
    pub decision: SearchDecision,
    pub selected: Box<SearchSourceSpec>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub enum SearchSourceSpec {
    Vector(VectorSearchSpec),
    Sparse(SparseVectorSearchSpec),
    FullText(FullTextSearchSpec),
}
