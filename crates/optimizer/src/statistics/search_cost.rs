// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Search Cost Models
//!
//! Cost estimators for vector and full-text search operations.

use paro_planner::operator::{FullTextQueryStats, FullTextScoreMode};
use paro_storage::index::hnsw::HnswSearchPolicy;
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
    /// Estimate the provider-owned dense Top-K source, including its exact
    /// filtered fallback. A selective predicate does not force the generic
    /// scan + row-fetch + Top-N plan: below the policy threshold the provider
    /// scores only the exact predicate bitmap, which is both exact and avoids
    /// materializing the vector column through the row pipeline.
    pub fn estimate_hnsw_cost(
        stats: &HnswIndexStatistics,
        k: usize,
        filter_selectivity: f64,
        ef: Option<usize>,
        policy: HnswSearchPolicy,
        filtered: bool,
    ) -> f64 {
        let n = stats.num_indexed_vectors.max(1) as f64;
        let selectivity = clamp_selectivity(filter_selectivity);
        let candidate_rows = if filtered {
            (n * selectivity).ceil().max(1.0)
        } else {
            n
        };
        let exact_threshold = if filtered {
            policy.filtered_plain_scan_threshold
        } else {
            policy.plain_scan_threshold
        } as f64;
        let log_n = n.ln().max(1.0);
        let ef_val = ef.unwrap_or(policy.ef_search).max(k).max(1) as f64;
        let dim_factor = (stats.dimension.max(1) as f64 / 128.0).max(1.0);

        if candidate_rows <= exact_threshold {
            // The specialized scorer reads contiguous vector values and feeds
            // Top-K directly. Keep the units relative to the generic per-row
            // pipeline rather than pretending both paths have equal overhead.
            return (candidate_rows * dim_factor * 0.25).max(1.0);
        }

        // Filtered Top-K shares the ordinary unfiltered navigation frontier;
        // exact bitmap admission is negligible next to vector scoring and does
        // not require inverse-selectivity oversampling.
        log_n * ef_val * dim_factor
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
        query_stats: &FullTextQueryStats,
        filter_selectivity: f64,
    ) -> f64 {
        let query_terms = weighted_query_terms(query_stats);
        let df_est = estimate_term_df(stats, query_terms.ceil() as usize);
        let branch_factor = query_stats.or_branch_count.max(1) as f64;
        let base = query_terms.max(1.0) * df_est * branch_factor;
        base * clamp_selectivity(filter_selectivity).max(0.01)
    }

    /// Estimated cost for BM25 mode: O(match_count * query_terms).
    pub fn estimate_bm25_cost(
        stats: &FullTextIndexStatistics,
        query_stats: &FullTextQueryStats,
        score_mode: FullTextScoreMode,
        filter_selectivity: f64,
    ) -> f64 {
        let query_terms = weighted_query_terms(query_stats);
        let total_docs = stats.total_docs.max(1) as f64;
        let term_selectivity = if stats.total_docs == 0 {
            0.0
        } else {
            (estimate_term_df(stats, query_terms.ceil() as usize) / total_docs).clamp(0.0, 1.0)
        };
        let branch_factor = query_stats.or_branch_count.max(1) as f64;
        let combined_selectivity = clamp_selectivity(filter_selectivity)
            * term_selectivity.powi(query_terms.max(1.0).ceil() as i32);
        let match_count = total_docs * combined_selectivity * branch_factor;
        let score_mode_factor = match score_mode {
            FullTextScoreMode::Bm25 => 1.0,
            FullTextScoreMode::CoverDensity => 1.25,
        };
        match_count * query_terms.max(1.0) * score_mode_factor
    }

    /// Choose the cheaper strategy between filter and BM25 modes.
    pub fn choose_strategy(
        stats: &FullTextIndexStatistics,
        query_stats: &FullTextQueryStats,
        score_mode: FullTextScoreMode,
        filter_selectivity: f64,
    ) -> (SearchStrategy, f64) {
        let filter_cost = Self::estimate_filter_cost(stats, query_stats, filter_selectivity);
        let bm25_cost =
            Self::estimate_bm25_cost(stats, query_stats, score_mode, filter_selectivity);
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

fn weighted_query_terms(query_stats: &FullTextQueryStats) -> f64 {
    let base = query_stats.effective_query_terms() as f64;
    let phrase_bonus = query_stats.phrase_count as f64 * 0.75;
    let proximity_bonus = query_stats.proximity_count as f64 * 1.0;
    let prefix_bonus = query_stats.prefix_count as f64 * 1.5;
    base + phrase_bonus + proximity_bonus + prefix_bonus
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hnsw_stats(rows: usize, dimension: usize) -> HnswIndexStatistics {
        HnswIndexStatistics {
            num_indexed_vectors: rows,
            dimension,
            max_level: 4,
            m: 16,
            ef_construction: 100,
            graph_size_bytes: 0,
            storage_size_bytes: 0,
            total_graph_links: 0,
            level0_graph_links: 0,
            max_level0_degree: 32,
            avg_level0_degree: 16.0,
        }
    }

    #[test]
    fn selective_filtered_topk_costs_the_provider_exact_path() {
        let policy = HnswSearchPolicy {
            ef_search: 40,
            plain_scan_threshold: 10_000,
            filtered_plain_scan_threshold: 20_000,
        };
        let cost = VectorScanCostModel::estimate_hnsw_cost(
            &hnsw_stats(20_000, 100),
            10,
            0.001,
            Some(40),
            policy,
            true,
        );

        assert_eq!(cost, 5.0);
        assert!(
            cost < 20.0,
            "exact search source must beat generic row Top-N"
        );
    }

    #[test]
    fn filtered_graph_cost_shares_the_unfiltered_navigation_frontier() {
        let policy = HnswSearchPolicy {
            ef_search: 40,
            plain_scan_threshold: 10_000,
            filtered_plain_scan_threshold: 20_000,
        };
        let stats = hnsw_stats(1_000_000, 128);
        let unfiltered =
            VectorScanCostModel::estimate_hnsw_cost(&stats, 10, 1.0, Some(40), policy, false);
        let filtered =
            VectorScanCostModel::estimate_hnsw_cost(&stats, 10, 0.1, Some(40), policy, true);

        assert_eq!(filtered, unfiltered);
    }
}
