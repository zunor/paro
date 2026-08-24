// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::sync::Arc;

use paro_catalog::entry::TableCatalogEntry;
use paro_common::runtime_value::Value;
use paro_common::typed_parameters::ParameterSlot;
use paro_common::types::LogicalType;
use paro_planner::operator::SearchDecision;
use paro_storage::index::hnsw::types::{HnswSearchPolicy, SearchParams};
use paro_storage::index::hnsw::DistanceMetric;
use paro_storage::index::{PredicateComparison, PredicateTree};
use paro_storage::rowset::SparseVector;
use paro_storage::search::{
    CapabilityToken, DenseVectorQuery, ExactBitmapMaterialization, FullTextQueryKind,
    FullTextQueryStats, FullTextScoreMode, NormalizedSearchRequest, SearchRequestMode,
};

/// A scalar retained by a reusable search predicate.
///
/// The predicate structure is the storage predicate structure itself. Only
/// its scalar values are delayed, so every predicate form can acquire runtime
/// parameter support without growing a second, parallel predicate AST.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchPredicateValue {
    Bound(Value),
    RuntimeParameter {
        slot: ParameterSlot,
        target_type: LogicalType,
    },
}

impl fmt::Display for SearchPredicateValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bound(value) => write!(f, "{value}"),
            Self::RuntimeParameter { slot, .. } => write!(f, "${}", slot.index.index() + 1),
        }
    }
}

/// Predicate image retained by a reusable search plan. Runtime parameters are
/// bound once when the search source opens, before segment index evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchPredicateTemplate {
    tree: PredicateTree<SearchPredicateValue>,
}

impl SearchPredicateTemplate {
    pub fn bound(tree: PredicateTree) -> Self {
        Self {
            tree: tree.map_values(&mut SearchPredicateValue::Bound),
        }
    }

    pub fn parameter_comparison(
        column_id: u32,
        comparison: PredicateComparison,
        slot: ParameterSlot,
        target_type: LogicalType,
    ) -> Self {
        Self {
            tree: PredicateTree::leaf(comparison.with_value(
                column_id,
                SearchPredicateValue::RuntimeParameter { slot, target_type },
            )),
        }
    }

    pub fn and(children: impl IntoIterator<Item = Self>) -> Option<Self> {
        PredicateTree::and(children.into_iter().map(|child| child.tree)).map(|tree| Self { tree })
    }

    pub fn or(children: impl IntoIterator<Item = Self>) -> Option<Self> {
        PredicateTree::or(children.into_iter().map(|child| child.tree)).map(|tree| Self { tree })
    }

    pub fn has_runtime_parameters(&self) -> bool {
        self.tree
            .any_value(&|value| matches!(value, SearchPredicateValue::RuntimeParameter { .. }))
    }

    pub fn tree(&self) -> &PredicateTree<SearchPredicateValue> {
        &self.tree
    }
}

impl fmt::Display for SearchPredicateTemplate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.tree)
    }
}

/// Proof carried by a physical search source about its absorbed predicate.
/// EXPLAIN renders this local contract instead of assuming that every search
/// provider happened to reject residual predicates elsewhere in lowering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchFilterContract {
    None,
    ExactSegmentBitmapNoResidual {
        materialization: ExactBitmapMaterialization,
    },
}

impl SearchFilterContract {
    /// Construct the proof only after lowering has established that no
    /// residual filter remains above the search source.
    pub fn exact_no_residual(
        predicate: Option<&SearchPredicateTemplate>,
        materialization: ExactBitmapMaterialization,
    ) -> Self {
        predicate.map_or(Self::None, |_| Self::ExactSegmentBitmapNoResidual {
            materialization,
        })
    }
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
    pub search_policy: HnswSearchPolicy,
    /// Generation-level physical degree used only to predict the likely
    /// adaptive filtered-search phase in costing and EXPLAIN. Runtime observes
    /// actual admissions and does not trust this estimate.
    pub avg_level0_degree: f32,
    pub predicate: Option<SearchPredicateTemplate>,
    pub filter_contract: SearchFilterContract,
    /// Cardinality estimate used to explain the provider's expected filtered
    /// exact-vs-graph strategy. Execution always decides from the exact bitmap.
    pub estimated_filter_rows: Option<u64>,
    pub estimated_total_rows: Option<u64>,
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
    pub filter_contract: SearchFilterContract,
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
    pub filter_contract: SearchFilterContract,
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
