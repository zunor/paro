// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # HNSW Basic Types
//!
//! Core types for the HNSW (Hierarchical Navigable Small World) index.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// Point offset type — identifies a vector within a segment.
pub type PointOffset = u32;

/// Score type for distance/similarity values.
pub type ScoreType = f32;

/// A scored point — a point with its similarity/distance score.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ScoredPoint {
    /// Point index within the segment
    pub idx: PointOffset,
    /// Similarity score (higher = more similar, for all metrics)
    pub score: ScoreType,
}

impl PartialEq for ScoredPoint {
    fn eq(&self, other: &Self) -> bool {
        self.idx == other.idx && self.score == other.score
    }
}

impl Eq for ScoredPoint {}

impl PartialOrd for ScoredPoint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredPoint {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare by score (for use in BinaryHeap — max-heap by score)
        match self.score.partial_cmp(&other.score) {
            Some(Ordering::Equal) | None => self.idx.cmp(&other.idx),
            Some(ord) => ord,
        }
    }
}

/// Search algorithm selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SearchAlgorithm {
    /// Standard HNSW search
    Hnsw,
    /// ACORN-1 search (improved recall for filtered searches)
    Acorn,
}

impl Default for SearchAlgorithm {
    fn default() -> Self {
        SearchAlgorithm::Hnsw
    }
}

/// ACORN search parameters.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct AcornParams {
    /// If true, ACORN may be used based on filter selectivity.
    pub enable: bool,
    /// Maximum selectivity of filters to enable ACORN.
    /// Selectivity = estimated matching points / total points.
    /// Default: 0.4
    pub max_selectivity: Option<f64>,
}

/// Default maximum selectivity for ACORN search.
pub const ACORN_MAX_SELECTIVITY_DEFAULT: f64 = 0.4;

impl Default for AcornParams {
    fn default() -> Self {
        AcornParams {
            enable: false,
            max_selectivity: None,
        }
    }
}

/// Search parameters for HNSW queries.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SearchParams {
    /// Size of the beam in beam-search. Larger = more accurate but slower.
    /// If None, uses the index's default ef.
    pub ef: Option<usize>,
    /// ACORN search parameters
    pub acorn: Option<AcornParams>,
    /// Whether to randomize entry point selection during graph search.
    /// `None` lets the index choose a default strategy.
    #[serde(default)]
    pub random_entry_point: Option<bool>,
}

/// Query-wide HNSW execution decision. Storage providers choose this after
/// considering the complete visible table, rather than applying an
/// unfiltered threshold independently to every physical segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HnswSearchMode {
    Auto,
    Exact,
    Graph,
}

impl Default for SearchParams {
    fn default() -> Self {
        SearchParams {
            ef: None,
            acorn: None,
            random_entry_point: None,
        }
    }
}

/// A query vector that has already been preprocessed for a specific metric.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedQuery {
    data: Vec<f32>,
    metric: super::DistanceMetric,
}

impl PreparedQuery {
    pub(crate) fn new(data: Vec<f32>, metric: super::DistanceMetric) -> Self {
        Self { data, metric }
    }

    pub fn as_slice(&self) -> &[f32] {
        &self.data
    }

    pub fn metric(&self) -> super::DistanceMetric {
        self.metric
    }
}

/// HNSW index configuration.
///
/// Controls the structure and behavior of the HNSW graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HnswConfig {
    /// Number of edges per node in the index graph (layers > 0).
    /// Larger = more accurate search, more space required.
    pub m: usize,
    /// Number of edges per node on level 0 (typically 2 * m).
    pub m0: usize,
    /// Number of neighbours to consider during index building.
    /// Larger = more accurate search, more time to build.
    pub ef_construct: usize,
    /// Default ef for search (can be overridden per query).
    pub ef: usize,
    /// Maximum visible-table row count below which an unfiltered exact scan is
    /// preferred. Providers must evaluate it once per query, globally.
    pub plain_scan_threshold: usize,
    /// Maximum filtered candidate count below which a filtered plain scan is preferred.
    pub filtered_plain_scan_threshold: usize,
    /// Whether to randomize entry point selection during index build.
    #[serde(default)]
    pub build_random_entry_point: bool,
}

impl Default for HnswConfig {
    fn default() -> Self {
        HnswConfig {
            m: 24,
            m0: 48, // 2 * m
            ef_construct: 100,
            ef: 100,
            plain_scan_threshold: 10_000,
            filtered_plain_scan_threshold: 0,
            build_random_entry_point: false,
        }
    }
}

impl HnswConfig {
    /// Create a new config with the given m and ef_construct.
    pub fn new(m: usize, ef_construct: usize) -> Self {
        HnswConfig {
            m,
            m0: m * 2,
            ef_construct,
            ef: ef_construct,
            plain_scan_threshold: 10_000,
            filtered_plain_scan_threshold: 0,
            build_random_entry_point: false,
        }
    }

    /// Create a config with custom plain_scan_threshold.
    pub fn with_plain_scan_threshold(mut self, threshold: usize) -> Self {
        self.plain_scan_threshold = threshold;
        self
    }

    /// Create a config with custom filtered_plain_scan_threshold.
    pub fn with_filtered_plain_scan_threshold(mut self, threshold: usize) -> Self {
        self.filtered_plain_scan_threshold = threshold;
        self
    }

    /// Create a config with custom ef.
    pub fn with_ef(mut self, ef: usize) -> Self {
        self.ef = ef;
        self
    }

    /// Enable or disable random entry point selection during graph construction.
    pub fn with_build_random_entry_point(mut self, enabled: bool) -> Self {
        self.build_random_entry_point = enabled;
        self
    }
}

/// HNSW M parameter wrapper — holds both m (general layers) and m0 (level 0).
///
/// Level 0 typically has 2x the connections of higher levels.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct HnswM {
    /// Number of edges per node on levels > 0
    pub m: usize,
    /// Number of edges per node on level 0 (typically 2 * m)
    pub m0: usize,
}

impl HnswM {
    pub fn new(m: usize) -> Self {
        HnswM { m, m0: m * 2 }
    }

    /// Get the maximum number of connections for the given level.
    pub fn get_m(&self, level: usize) -> usize {
        if level == 0 {
            self.m0
        } else {
            self.m
        }
    }
}

impl From<&HnswConfig> for HnswM {
    fn from(config: &HnswConfig) -> Self {
        HnswM {
            m: config.m,
            m0: config.m0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scored_point_ordering() {
        let a = ScoredPoint { idx: 0, score: 0.5 };
        let b = ScoredPoint { idx: 1, score: 0.8 };
        let c = ScoredPoint { idx: 2, score: 0.5 };

        // Higher score = greater
        assert!(b > a);
        assert!(a < b);
        // Equal score, compare by idx
        assert!(c > a);
    }

    #[test]
    fn test_hnsw_config_default() {
        let config = HnswConfig::default();
        assert_eq!(config.m, 24);
        assert_eq!(config.m0, 48);
        assert_eq!(config.ef_construct, 100);
        assert_eq!(config.ef, 100);
        assert_eq!(config.plain_scan_threshold, 10_000);
        assert_eq!(config.filtered_plain_scan_threshold, 0);
        assert!(!config.build_random_entry_point);
    }

    #[test]
    fn test_hnsw_config_new() {
        let config = HnswConfig::new(8, 200);
        assert_eq!(config.m, 8);
        assert_eq!(config.m0, 16);
        assert_eq!(config.ef_construct, 200);
        assert_eq!(config.ef, 200);
        assert_eq!(config.plain_scan_threshold, 10_000);
        assert_eq!(config.filtered_plain_scan_threshold, 0);
        assert!(!config.build_random_entry_point);
    }

    #[test]
    fn test_hnsw_m() {
        let hm = HnswM::new(16);
        assert_eq!(hm.get_m(0), 32);
        assert_eq!(hm.get_m(1), 16);
        assert_eq!(hm.get_m(5), 16);
    }

    #[test]
    fn test_search_algorithm_default() {
        assert_eq!(SearchAlgorithm::default(), SearchAlgorithm::Hnsw);
    }
}
