// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Search Cost Models
//!
//! Cost estimators for vector and full-text search operations.

use paro_storage::statistics::{
    FullTextIndexStatistics, HnswIndexStatistics, SparseIndexStatistics,
};

/// Strategy chosen for a search operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchStrategy {
    Hnsw,
    Sparse,
    FullTextFilter,
    FullTextBm25,
}

/// Estimated cost for a search operation.
#[derive(Debug, Clone, Copy)]
pub struct SearchCostEstimate {
    pub table_index: usize,
    pub column_id: u32,
    pub estimated_cost: f64,
    pub strategy: SearchStrategy,
    pub filter_selectivity: f64,
    pub k: Option<usize>,
    pub query_terms: Option<usize>,
    pub query_nnz: Option<usize>,
}

impl SearchCostEstimate {
    pub fn new(
        table_index: usize,
        column_id: u32,
        strategy: SearchStrategy,
        estimated_cost: f64,
        filter_selectivity: f64,
    ) -> Self {
        Self {
            table_index,
            column_id,
            estimated_cost,
            strategy,
            filter_selectivity,
            k: None,
            query_terms: None,
            query_nnz: None,
        }
    }

    pub fn with_k(mut self, k: usize) -> Self {
        self.k = Some(k);
        self
    }

    pub fn with_query_terms(mut self, terms: usize) -> Self {
        self.query_terms = Some(terms);
        self
    }

    pub fn with_query_nnz(mut self, nnz: usize) -> Self {
        self.query_nnz = Some(nnz);
        self
    }
}

fn clamp_selectivity(value: f64) -> f64 {
    if value.is_finite() {
        value.clamp(0.0, 1.0)
    } else {
        1.0
    }
}

/// Cost model for vector scans.
pub struct VectorScanCostModel;

impl VectorScanCostModel {
    /// HNSW cost: O(k * log(N) * ef)
    pub fn estimate_hnsw_cost(
        stats: &HnswIndexStatistics,
        k: usize,
        filter_selectivity: f64,
        ef: Option<usize>,
    ) -> f64 {
        let n = stats.num_indexed_vectors.max(1) as f64;
        let log_n = n.ln().max(1.0);
        let ef_val = ef.unwrap_or(stats.ef_construction).max(1) as f64;
        let dim_factor = (stats.dimension.max(1) as f64 / 128.0).max(1.0);
        let base = k.max(1) as f64 * log_n * ef_val * dim_factor;
        let selectivity = clamp_selectivity(filter_selectivity).max(0.01);
        base * selectivity
    }

    /// Sparse cost: O(|query_nnz| * avg_posting_len)
    pub fn estimate_sparse_cost(
        stats: &SparseIndexStatistics,
        query_nnz: usize,
        filter_selectivity: f64,
    ) -> f64 {
        let avg_posting_len = if stats.num_posting_lists == 0 {
            0.0
        } else {
            stats.total_postings as f64 / stats.num_posting_lists as f64
        };
        let base = query_nnz.max(1) as f64 * avg_posting_len;
        let selectivity = clamp_selectivity(filter_selectivity).max(0.01);
        base * selectivity
    }
}

/// Cost model for full-text scans.
pub struct FullTextScanCostModel;

impl FullTextScanCostModel {
    /// Estimated cost for filter mode: O(sum posting_list_len(term_i)).
    pub fn estimate_filter_cost(
        stats: &FullTextIndexStatistics,
        query_terms: usize,
        filter_selectivity: f64,
    ) -> f64 {
        let df_est = estimate_term_df(stats, query_terms);
        let base = query_terms.max(1) as f64 * df_est;
        base * clamp_selectivity(filter_selectivity).max(0.01)
    }

    /// Estimated cost for BM25 mode: O(match_count * query_terms).
    pub fn estimate_bm25_cost(
        stats: &FullTextIndexStatistics,
        query_terms: usize,
        filter_selectivity: f64,
    ) -> f64 {
        let total_docs = stats.total_docs.max(1) as f64;
        let term_selectivity = if stats.total_docs == 0 {
            0.0
        } else {
            (estimate_term_df(stats, query_terms) / total_docs).clamp(0.0, 1.0)
        };
        let combined_selectivity = clamp_selectivity(filter_selectivity)
            * term_selectivity.powi(query_terms.max(1) as i32);
        let match_count = total_docs * combined_selectivity;
        match_count * query_terms.max(1) as f64
    }

    /// Choose the cheaper strategy between filter and BM25 modes.
    pub fn choose_strategy(
        stats: &FullTextIndexStatistics,
        query_terms: usize,
        filter_selectivity: f64,
    ) -> (SearchStrategy, f64) {
        let filter_cost = Self::estimate_filter_cost(stats, query_terms, filter_selectivity);
        let bm25_cost = Self::estimate_bm25_cost(stats, query_terms, filter_selectivity);
        if filter_cost <= bm25_cost {
            (SearchStrategy::FullTextFilter, filter_cost)
        } else {
            (SearchStrategy::FullTextBm25, bm25_cost)
        }
    }
}

fn estimate_term_df(stats: &FullTextIndexStatistics, query_terms: usize) -> f64 {
    if stats.unique_terms == 0 {
        return 0.0;
    }
    let avg_df = stats.total_postings as f64 / stats.unique_terms as f64;
    if query_terms > 1 {
        // Prefer lower df for multi-term queries (higher IDF -> shorter lists).
        let min_df = stats.min_posting_list_len.max(1) as f64;
        avg_df.min(min_df)
    } else {
        avg_df.max(1.0)
    }
}
