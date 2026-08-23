// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::sync::Arc;

use paro_catalog::entry::TableCatalogEntry;
use paro_common::typed_parameters::ParameterSlot;
use paro_common::types::LogicalType;
use paro_planner::operator::SearchDecision;
use paro_storage::index::hnsw::types::SearchParams;
use paro_storage::index::hnsw::DistanceMetric;
use paro_storage::index::{PredicateComparison, PredicateTree};
use paro_storage::rowset::SparseVector;
use paro_storage::search::{
    CapabilityToken, DenseVectorQuery, FullTextQueryKind, FullTextQueryStats, FullTextScoreMode,
    NormalizedSearchRequest, SearchRequestMode,
};

/// Predicate image retained by a reusable search plan. Storage predicates are
/// concrete values, so runtime parameters are bound once when the search
/// source opens rather than being evaluated for every candidate row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchPredicateTemplate {
    Bound(PredicateTree),
    ParameterComparison {
        column_id: u32,
        comparison: PredicateComparison,
        slot: ParameterSlot,
        target_type: LogicalType,
    },
    And(Vec<SearchPredicateTemplate>),
    Or(Vec<SearchPredicateTemplate>),
}

impl SearchPredicateTemplate {
    pub fn bound(tree: PredicateTree) -> Self {
        Self::Bound(tree)
    }

    pub fn and(children: impl IntoIterator<Item = Self>) -> Option<Self> {
        combine_predicate_templates(children, true)
    }

    pub fn or(children: impl IntoIterator<Item = Self>) -> Option<Self> {
        combine_predicate_templates(children, false)
    }

    pub fn has_runtime_parameters(&self) -> bool {
        match self {
            Self::Bound(_) => false,
            Self::ParameterComparison { .. } => true,
            Self::And(children) | Self::Or(children) => {
                children.iter().any(Self::has_runtime_parameters)
            }
        }
    }
}

fn combine_predicate_templates(
    children: impl IntoIterator<Item = SearchPredicateTemplate>,
    conjunction: bool,
) -> Option<SearchPredicateTemplate> {
    let mut combined = Vec::new();
    for child in children {
        match (conjunction, child) {
            (true, SearchPredicateTemplate::And(mut nested))
            | (false, SearchPredicateTemplate::Or(mut nested)) => combined.append(&mut nested),
            (_, child) => combined.push(child),
        }
    }
    match combined.len() {
        0 => None,
        1 => Some(combined.pop().expect("single predicate template child")),
        _ if conjunction => Some(SearchPredicateTemplate::And(combined)),
        _ => Some(SearchPredicateTemplate::Or(combined)),
    }
}

impl fmt::Display for SearchPredicateTemplate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bound(tree) => write!(f, "{tree}"),
            Self::ParameterComparison {
                column_id,
                comparison,
                slot,
                ..
            } => write!(
                f,
                "col#{column_id} {comparison} ${}",
                slot.index.index() + 1
            ),
            Self::And(children) => format_template_children(f, children, " AND "),
            Self::Or(children) => format_template_children(f, children, " OR "),
        }
    }
}

fn format_template_children(
    f: &mut fmt::Formatter<'_>,
    children: &[SearchPredicateTemplate],
    separator: &str,
) -> fmt::Result {
    f.write_str("(")?;
    for (index, child) in children.iter().enumerate() {
        if index > 0 {
            f.write_str(separator)?;
        }
        write!(f, "{child}")?;
    }
    f.write_str(")")
}

#[derive(Debug, Clone)]
pub struct VectorSearchSpec {
    pub table: Arc<TableCatalogEntry>,
    pub capability_token: CapabilityToken,
    pub column_id: usize,
    pub query: DenseVectorQuery,
    pub distance: DistanceMetric,
    pub k: usize,
    pub params: SearchParams,
    pub predicate: Option<SearchPredicateTemplate>,
    /// Cardinality estimate used to explain the provider's expected filtered
    /// exact-vs-graph strategy. Execution always decides from the exact bitmap.
    pub estimated_filter_rows: Option<u64>,
    pub projected_columns: Box<[usize]>,
    pub emit_score: bool,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct SparseVectorSearchSpec {
    pub table: Arc<TableCatalogEntry>,
    pub capability_token: CapabilityToken,
    pub column_id: usize,
    pub query_vector: SparseVector,
    pub k: usize,
    pub predicate: Option<SearchPredicateTemplate>,
    pub projected_columns: Box<[usize]>,
    pub emit_score: bool,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct FullTextSearchSpec {
    pub table: Arc<TableCatalogEntry>,
    pub capability_token: CapabilityToken,
    pub column_id: usize,
    pub query: String,
    pub query_kind: FullTextQueryKind,
    pub query_stats: FullTextQueryStats,
    pub config: String,
    pub score_mode: FullTextScoreMode,
    pub mode: SearchRequestMode,
    pub predicate: Option<SearchPredicateTemplate>,
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
