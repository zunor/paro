// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Search Cost Models
//!
//! Cost estimators for vector and full-text search operations.

use paro_planner::operator::{FullTextQueryStats, FullTextScoreMode};
use paro_storage::index::hnsw::{
    estimate_filtered_search_strategy, HnswDistanceCostModel, HnswFilteredSearchStrategy,
    HnswSearchPolicy,
};
use paro_storage::search::ExactFilterMaterialization;
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
    /// Sequential vector pages are SIMD-friendly and cache-linear, while a
    /// graph score normally follows an unpredictable edge to a random vector.
    /// The policy threshold is calibrated in those physical units rather than
    /// pretending both distance evaluations cost the same.
    const SEQUENTIAL_VECTOR_SCAN_FACTOR: f64 =
        1.0 / HnswDistanceCostModel::SEQUENTIAL_SCORES_PER_RANDOM_SCORE as f64;
    const REFERENCE_VECTOR_DIMENSION: f64 = 128.0;
    /// A scalar comparison is one lane of the reference 128D sequential vector
    /// score. Bitmap emission is modeled separately below, so this coefficient
    /// is derived rather than being a second independently invented constant.
    const SEQUENTIAL_SCALAR_SCAN_FACTOR: f64 =
        Self::SEQUENTIAL_VECTOR_SCAN_FACTOR / Self::REFERENCE_VECTOR_DIMENSION;

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
        filter_materialization: Option<ExactFilterMaterialization>,
    ) -> f64 {
        let n = stats.num_indexed_vectors.max(1) as f64;
        let selectivity = clamp_selectivity(filter_selectivity);
        let filtered = filter_materialization.is_some();
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
        let effective_ef = policy.effective_ef(k, ef);
        let ef_val = effective_ef as f64;
        let dim_factor = (stats.dimension.max(1) as f64 / 128.0).max(1.0);
        let scored_points = ef_val * stats.avg_level0_degree.max(1.0) as f64;
        let raw_graph_cost = (log_n + scored_points) * dim_factor;
        let graph_cost = raw_graph_cost;
        let bitmap_cost = match filter_materialization {
            None => 0.0,
            Some(ExactFilterMaterialization::ScalarIndex) => {
                log_n + candidate_rows / u64::BITS as f64
            }
            Some(ExactFilterMaterialization::Mixed {
                indexed_rows,
                scanned_rows,
            }) => {
                let represented_rows = indexed_rows.saturating_add(scanned_rows).max(1) as f64;
                let scanned_fraction = scanned_rows as f64 / represented_rows;
                log_n
                    + candidate_rows / u64::BITS as f64
                    + n * scanned_fraction * Self::SEQUENTIAL_SCALAR_SCAN_FACTOR
            }
            Some(ExactFilterMaterialization::ColumnScan) => {
                n * Self::SEQUENTIAL_SCALAR_SCAN_FACTOR + candidate_rows / u64::BITS as f64
            }
        };
        let exact_scan_cost =
            (candidate_rows * dim_factor * Self::SEQUENTIAL_VECTOR_SCAN_FACTOR).max(1.0);

        if candidate_rows <= exact_threshold {
            return bitmap_cost + exact_scan_cost;
        }

        if filtered {
            return bitmap_cost
                + match estimate_filtered_search_strategy(
                    candidate_rows as u64,
                    stats.num_indexed_vectors as u64,
                    k,
                    effective_ef,
                    stats.avg_level0_degree,
                    policy,
                )
                .strategy
                {
                    // The exact-cardinality branch above and the shared policy
                    // use the same threshold, so this state is unreachable.
                    HnswFilteredSearchStrategy::ExactScan => exact_scan_cost,
                    HnswFilteredSearchStrategy::MaskedTopK => graph_cost,
                    // Adaptive refinement reuses the connected traversal's scored
                    // set and adds bounded two-hop work rather than a second full
                    // graph generation.
                    HnswFilteredSearchStrategy::RefinedTopK => graph_cost * 1.5,
                };
        }

        graph_cost
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
            Some(ExactFilterMaterialization::ScalarIndex),
        );

        assert!(cost > 1.0);
        assert!(
            cost < 50.0,
            "exact search source must beat generic row Top-N"
        );
    }

    #[test]
    fn broad_filtered_graph_cost_includes_bitmap_materialization() {
        let policy = HnswSearchPolicy {
            ef_search: 40,
            plain_scan_threshold: 10_000,
            filtered_plain_scan_threshold: 20_000,
        };
        let stats = hnsw_stats(1_000_000, 128);
        let unfiltered =
            VectorScanCostModel::estimate_hnsw_cost(&stats, 10, 1.0, Some(40), policy, None);
        let filtered = VectorScanCostModel::estimate_hnsw_cost(
            &stats,
            10,
            0.75,
            Some(40),
            policy,
            Some(ExactFilterMaterialization::ScalarIndex),
        );

        assert!(filtered > unfiltered);
    }

    #[test]
    fn column_scan_bitmap_cost_is_visible_to_the_optimizer() {
        let policy = HnswSearchPolicy {
            ef_search: 80,
            plain_scan_threshold: 10_000,
            filtered_plain_scan_threshold: 20_000,
        };
        let stats = hnsw_stats(1_000_000, 128);
        let postings = VectorScanCostModel::estimate_hnsw_cost(
            &stats,
            10,
            0.01,
            Some(80),
            policy,
            Some(ExactFilterMaterialization::ScalarIndex),
        );
        let scan = VectorScanCostModel::estimate_hnsw_cost(
            &stats,
            10,
            0.01,
            Some(80),
            policy,
            Some(ExactFilterMaterialization::ColumnScan),
        );
        assert!(scan > postings * 1.5, "scan={scan}, postings={postings}");
    }

    #[test]
    fn calibrated_exact_to_graph_crossover_is_non_decreasing() {
        let policy = HnswSearchPolicy {
            ef_search: 160,
            plain_scan_threshold: 10_000,
            filtered_plain_scan_threshold: 20_000,
        };
        let stats = hnsw_stats(1_000_000, 128);
        let at_threshold = VectorScanCostModel::estimate_hnsw_cost(
            &stats,
            10,
            20_000.0 / 1_000_000.0,
            Some(160),
            policy,
            Some(ExactFilterMaterialization::ScalarIndex),
        );
        let above_threshold = VectorScanCostModel::estimate_hnsw_cost(
            &stats,
            10,
            20_001.0 / 1_000_000.0,
            Some(160),
            policy,
            Some(ExactFilterMaterialization::ScalarIndex),
        );
        assert!(above_threshold >= at_threshold);
    }

    #[test]
    fn graph_cost_preserves_ef_and_degree_resolution_above_threshold() {
        let policy = HnswSearchPolicy {
            ef_search: 80,
            plain_scan_threshold: 10_000,
            filtered_plain_scan_threshold: 20_000,
        };
        let low = VectorScanCostModel::estimate_hnsw_cost(
            &hnsw_stats(10_000_000, 128),
            10,
            1.0,
            Some(80),
            policy,
            None,
        );
        let high = VectorScanCostModel::estimate_hnsw_cost(
            &hnsw_stats(10_000_000, 128),
            10,
            1.0,
            Some(320),
            policy,
            None,
        );
        assert!(high > low * 2.0, "low={low}, high={high}");
    }
}
