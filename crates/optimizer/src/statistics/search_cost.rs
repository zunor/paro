// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Search Cost Models
//!
//! Cost estimators for vector and full-text search operations.

use paro_planner::operator::{FullTextQueryStats, FullTextScoreMode};
use paro_storage::index::hnsw::{
    estimate_filtered_search_strategy, HnswFilteredSearchStrategy, HnswQueryOptions,
    HnswSearchObjective, HnswSearchPolicy,
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
    const REFERENCE_VECTOR_DIMENSION: f64 = 128.0;

    /// Estimate the provider-owned dense Top-K source, including its exact
    /// filtered fallback. A selective predicate does not force the generic
    /// scan + row-fetch + Top-N plan: the provider compares its physical exact
    /// row-set workload with the graph passes implied by deferred admission.
    pub fn estimate_hnsw_cost(
        stats: &HnswIndexStatistics,
        k: usize,
        filter_selectivity: f64,
        options: HnswQueryOptions,
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
        let log_n = n.ln().max(1.0);
        let effective_ef = policy.effective_ef(k, options.ef);
        let ef_val = effective_ef as f64;
        let dim_factor = (stats.dimension.max(1) as f64 / 128.0).max(1.0);
        // Planning and execution consume the same immutable definition-owned
        // profile. Timing history from this process cannot change EXPLAIN or
        // make otherwise identical replicas choose different paths.
        let vector_dimension = u32::try_from(stats.dimension).unwrap_or(u32::MAX).max(1);
        let dimension = f64::from(vector_dimension);
        let reference_dimension = f64::from(policy.distance_cost.reference_dimension.max(1));
        let reference_ratio = f64::from(
            policy
                .distance_cost
                .sequential_covering_scores_per_random_score
                .max(1),
        );
        let sequential_vector_scan_factor =
            dimension / (dimension + (reference_ratio - 1.0) * reference_dimension);
        // Planning cannot assume every predicate part has a generation
        // covering layout: a fresh tail may still gather base vectors. Use
        // the conservative indexed-base profile for filtered exact scoring;
        // runtime has exact per-part physical evidence and applies the
        // weighted profile without changing result semantics.
        let filtered_exact_scan_factor = 1.0
            / policy
                .distance_cost
                .indexed_base_scores_per_random_score
                .max(1) as f64;
        // A scalar comparison is one lane of the reference 128D sequential
        // vector score. Bitmap emission is modeled separately below, so this
        // coefficient remains derived rather than becoming another unrelated
        // threshold.
        let sequential_scalar_scan_factor =
            sequential_vector_scan_factor / Self::REFERENCE_VECTOR_DIMENSION;
        let scored_points_per_ef = policy.distance_cost.graph_scored_points_per_ef.max(1) as f64;
        let scored_points = ef_val * scored_points_per_ef;
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
                    + n * scanned_fraction * sequential_scalar_scan_factor
            }
            Some(ExactFilterMaterialization::ColumnScan) => {
                n * sequential_scalar_scan_factor + candidate_rows / u64::BITS as f64
            }
        };
        let exact_scan_factor = if filtered {
            filtered_exact_scan_factor
        } else {
            sequential_vector_scan_factor
        };
        let exact_scan_cost = (candidate_rows * dim_factor * exact_scan_factor).max(1.0);

        if options.objective == HnswSearchObjective::Exact {
            return bitmap_cost + exact_scan_cost;
        }

        if filtered {
            let decision = estimate_filtered_search_strategy(
                candidate_rows as u64,
                stats.num_indexed_vectors as u64,
                k,
                effective_ef,
                vector_dimension,
                policy,
            );
            let search_cost = match decision.strategy {
                HnswFilteredSearchStrategy::ExactScan => exact_scan_cost,
                HnswFilteredSearchStrategy::MaskedTopK => graph_cost,
                HnswFilteredSearchStrategy::RefinedTopK => {
                    graph_cost * decision.expected_graph_passes as f64
                }
            };
            return bitmap_cost + exact_scan_cost.min(search_cost);
        }

        exact_scan_cost.min(graph_cost)
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

    fn hnsw_options(ef: usize) -> HnswQueryOptions {
        HnswQueryOptions {
            ef: Some(ef),
            ..Default::default()
        }
    }

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
            ..HnswSearchPolicy::default()
        };
        let cost = VectorScanCostModel::estimate_hnsw_cost(
            &hnsw_stats(20_000, 100),
            10,
            0.001,
            hnsw_options(40),
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
            ..HnswSearchPolicy::default()
        };
        let stats = hnsw_stats(1_000_000, 128);
        let unfiltered = VectorScanCostModel::estimate_hnsw_cost(
            &stats,
            10,
            1.0,
            hnsw_options(40),
            policy,
            None,
        );
        let filtered = VectorScanCostModel::estimate_hnsw_cost(
            &stats,
            10,
            0.75,
            hnsw_options(40),
            policy,
            Some(ExactFilterMaterialization::ScalarIndex),
        );

        assert!(filtered > unfiltered);
    }

    #[test]
    fn column_scan_bitmap_cost_is_visible_to_the_optimizer() {
        let policy = HnswSearchPolicy {
            ef_search: 80,
            ..HnswSearchPolicy::default()
        };
        let stats = hnsw_stats(1_000_000, 128);
        let postings = VectorScanCostModel::estimate_hnsw_cost(
            &stats,
            10,
            0.01,
            hnsw_options(80),
            policy,
            Some(ExactFilterMaterialization::ScalarIndex),
        );
        let scan = VectorScanCostModel::estimate_hnsw_cost(
            &stats,
            10,
            0.01,
            hnsw_options(80),
            policy,
            Some(ExactFilterMaterialization::ColumnScan),
        );
        let dimension = 128.0;
        let reference_dimension = f64::from(policy.distance_cost.reference_dimension.max(1));
        let ratio = f64::from(
            policy
                .distance_cost
                .sequential_covering_scores_per_random_score
                .max(1),
        );
        let sequential_vector_scan_factor =
            dimension / (dimension + (ratio - 1.0) * reference_dimension);
        let expected_scan_delta = 1_000_000.0 * sequential_vector_scan_factor
            / VectorScanCostModel::REFERENCE_VECTOR_DIMENSION
            - 1_000_000.0_f64.ln();
        assert!(scan > postings, "scan={scan}, postings={postings}");
        assert!(
            ((scan - postings) - expected_scan_delta).abs() < 1e-9,
            "scan={scan}, postings={postings}, expected_delta={expected_scan_delta}"
        );
    }

    #[test]
    fn calibrated_exact_to_graph_crossover_is_non_decreasing() {
        let policy = HnswSearchPolicy {
            ef_search: 160,
            ..HnswSearchPolicy::default()
        };
        let stats = hnsw_stats(1_000_000, 128);
        let at_threshold = VectorScanCostModel::estimate_hnsw_cost(
            &stats,
            10,
            20_000.0 / 1_000_000.0,
            hnsw_options(160),
            policy,
            Some(ExactFilterMaterialization::ScalarIndex),
        );
        let above_threshold = VectorScanCostModel::estimate_hnsw_cost(
            &stats,
            10,
            20_001.0 / 1_000_000.0,
            hnsw_options(160),
            policy,
            Some(ExactFilterMaterialization::ScalarIndex),
        );
        assert!(above_threshold >= at_threshold);
    }

    #[test]
    fn graph_cost_preserves_ef_resolution_without_a_cardinality_clamp() {
        let policy = HnswSearchPolicy {
            ef_search: 80,
            ..HnswSearchPolicy::default()
        };
        let low = VectorScanCostModel::estimate_hnsw_cost(
            &hnsw_stats(10_000_000, 128),
            10,
            1.0,
            hnsw_options(80),
            policy,
            None,
        );
        let high = VectorScanCostModel::estimate_hnsw_cost(
            &hnsw_stats(10_000_000, 128),
            10,
            1.0,
            hnsw_options(320),
            policy,
            None,
        );
        assert!(high > low * 2.0, "low={low}, high={high}");
    }

    #[test]
    fn exact_objective_costs_the_exact_path_instead_of_the_graph_minimum() {
        let stats = hnsw_stats(10_000_000, 32);
        let policy = HnswSearchPolicy::default();
        let approximate = VectorScanCostModel::estimate_hnsw_cost(
            &stats,
            10,
            0.5,
            hnsw_options(160),
            policy,
            Some(ExactFilterMaterialization::ScalarIndex),
        );
        let exact = VectorScanCostModel::estimate_hnsw_cost(
            &stats,
            10,
            0.5,
            HnswQueryOptions {
                ef: Some(160),
                objective: HnswSearchObjective::Exact,
            },
            policy,
            Some(ExactFilterMaterialization::ScalarIndex),
        );

        assert!(
            exact > approximate,
            "exact={exact}, approximate={approximate}"
        );
    }
}
