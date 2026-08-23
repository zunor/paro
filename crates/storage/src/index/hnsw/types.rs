// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # HNSW Basic Types
//!
//! Core types for the HNSW (Hierarchical Navigable Small World) index.

#[cfg(test)]
use paro_common::error::{self as paro_error, Result};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// Point offset type — identifies a vector within a segment.
pub type PointOffset = u32;

/// Score type for distance/similarity values.
pub type ScoreType = f32;
pub const DEFAULT_HNSW_BUILD_SEED: u64 = 0x5041_524f_484e_5357;
pub const DEFAULT_HNSW_M: u32 = 24;
pub const DEFAULT_HNSW_EF_CONSTRUCT: u32 = 100;
pub const DEFAULT_HNSW_EF_SEARCH: u32 = 100;
pub const DEFAULT_HNSW_PLAIN_SCAN_THRESHOLD: u32 = 10_000;
pub const DEFAULT_HNSW_FILTERED_PLAIN_SCAN_THRESHOLD: u32 = 0;
pub const DEFAULT_HNSW_PROPOSAL_WAVE_SIZE: u32 = 64;
pub const DEFAULT_HNSW_WARMUP_POINT_COUNT: u32 = 4_096;
/// Version 3 fixes graph construction to a seeded point permutation followed
/// by one-point warm-up waves and deterministic frozen proposal waves. Wave
/// boundaries are durable fields; changing publication semantics requires a
/// new contract version.
pub const HNSW_BUILD_CONTRACT_VERSION: u32 = 4;

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

/// Immutable physical contract of one HNSW graph artifact.
///
/// Only fields that change graph topology belong here. Query policy is kept in
/// [`HnswSearchPolicy`] and is deliberately absent from serialized artifacts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HnswBuildContract {
    pub version: u32,
    pub m: u32,
    pub m0: u32,
    pub ef_construct: u32,
    pub distance: super::DistanceMetric,
    pub build_seed: u64,
    /// Number of point proposals computed against one frozen topology.
    pub proposal_wave_size: u32,
    /// Number of points published as one-point waves before batched waves.
    pub warmup_point_count: u32,
}

impl HnswBuildContract {
    pub fn validate(&self) -> paro_common::error::Result<()> {
        if self.version != HNSW_BUILD_CONTRACT_VERSION {
            return Err(paro_common::error::data_corrupted(format!(
                "unsupported HNSW build contract version {}, expected {}",
                self.version, HNSW_BUILD_CONTRACT_VERSION
            )));
        }
        if !(2..=1_024).contains(&self.m) || self.m0 != self.m.saturating_mul(2) {
            return Err(paro_common::error::data_corrupted(format!(
                "invalid HNSW build degree contract: m={}, m0={}",
                self.m, self.m0
            )));
        }
        if self.ef_construct < self.m || self.ef_construct > 1_000_000 {
            return Err(paro_common::error::data_corrupted(format!(
                "invalid HNSW ef_construct {} for m {}",
                self.ef_construct, self.m
            )));
        }
        if !(1..=4_096).contains(&self.proposal_wave_size) {
            return Err(paro_common::error::data_corrupted(format!(
                "invalid HNSW proposal_wave_size {}, expected 1..=4096",
                self.proposal_wave_size
            )));
        }
        if self.warmup_point_count > 1_000_000_000 {
            return Err(paro_common::error::data_corrupted(format!(
                "invalid HNSW warmup_point_count {}",
                self.warmup_point_count
            )));
        }
        Ok(())
    }
}

/// Mutable HNSW query policy supplied by the active search definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswSearchPolicy {
    pub ef_search: usize,
    pub plain_scan_threshold: usize,
    pub filtered_plain_scan_threshold: usize,
}

impl Default for HnswSearchPolicy {
    fn default() -> Self {
        Self {
            ef_search: DEFAULT_HNSW_EF_SEARCH as usize,
            plain_scan_threshold: DEFAULT_HNSW_PLAIN_SCAN_THRESHOLD as usize,
            filtered_plain_scan_threshold: DEFAULT_HNSW_FILTERED_PLAIN_SCAN_THRESHOLD as usize,
        }
    }
}

/// Unit-test adapter for concise graph fixtures. Production build entrypoints
/// accept [`HnswBuildContract`] directly and cannot mix build/search settings.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswConfig {
    /// Number of edges per node in the index graph (layers > 0).
    /// Larger = more accurate search, more space required.
    pub m: usize,
    /// Number of edges per node on level 0 (typically 2 * m).
    pub m0: usize,
    /// Number of neighbours to consider during index building.
    /// Larger = more accurate search, more time to build.
    pub ef_construct: usize,
    pub ef: usize,
    pub plain_scan_threshold: usize,
    pub filtered_plain_scan_threshold: usize,
    /// Seed for the versioned deterministic construction RNG.
    pub build_seed: u64,
}

#[cfg(test)]
impl Default for HnswConfig {
    fn default() -> Self {
        HnswConfig {
            m: DEFAULT_HNSW_M as usize,
            m0: (DEFAULT_HNSW_M * 2) as usize,
            ef_construct: DEFAULT_HNSW_EF_CONSTRUCT as usize,
            ef: DEFAULT_HNSW_EF_SEARCH as usize,
            plain_scan_threshold: DEFAULT_HNSW_PLAIN_SCAN_THRESHOLD as usize,
            filtered_plain_scan_threshold: DEFAULT_HNSW_FILTERED_PLAIN_SCAN_THRESHOLD as usize,
            build_seed: DEFAULT_HNSW_BUILD_SEED,
        }
    }
}

#[cfg(test)]
impl HnswConfig {
    /// Create a new config with the given m and ef_construct.
    pub fn new(m: usize, ef_construct: usize) -> Self {
        HnswConfig {
            m,
            m0: m * 2,
            ef_construct,
            ef: ef_construct,
            plain_scan_threshold: DEFAULT_HNSW_PLAIN_SCAN_THRESHOLD as usize,
            filtered_plain_scan_threshold: DEFAULT_HNSW_FILTERED_PLAIN_SCAN_THRESHOLD as usize,
            build_seed: DEFAULT_HNSW_BUILD_SEED,
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

    pub fn with_build_seed(mut self, seed: u64) -> Self {
        self.build_seed = seed;
        self
    }

    pub fn try_build_contract(self, distance: super::DistanceMetric) -> Result<HnswBuildContract> {
        let contract = HnswBuildContract {
            version: HNSW_BUILD_CONTRACT_VERSION,
            m: u32::try_from(self.m)
                .map_err(|_| paro_error::out_of_range("HNSW m exceeds durable u32 width"))?,
            m0: u32::try_from(self.m0)
                .map_err(|_| paro_error::out_of_range("HNSW m0 exceeds durable u32 width"))?,
            ef_construct: u32::try_from(self.ef_construct).map_err(|_| {
                paro_error::out_of_range("HNSW ef_construct exceeds durable u32 width")
            })?,
            distance,
            build_seed: self.build_seed,
            proposal_wave_size: DEFAULT_HNSW_PROPOSAL_WAVE_SIZE,
            warmup_point_count: DEFAULT_HNSW_WARMUP_POINT_COUNT,
        };
        contract.validate()?;
        Ok(contract)
    }

    pub fn build_contract(self, distance: super::DistanceMetric) -> HnswBuildContract {
        self.try_build_contract(distance)
            .expect("test HNSW configuration is valid")
    }

    pub const fn search_policy(self) -> HnswSearchPolicy {
        HnswSearchPolicy {
            ef_search: self.ef,
            plain_scan_threshold: self.plain_scan_threshold,
            filtered_plain_scan_threshold: self.filtered_plain_scan_threshold,
        }
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

#[cfg(test)]
impl From<&HnswConfig> for HnswM {
    fn from(config: &HnswConfig) -> Self {
        HnswM {
            m: config.m,
            m0: config.m0,
        }
    }
}

impl From<&HnswBuildContract> for HnswM {
    fn from(contract: &HnswBuildContract) -> Self {
        HnswM {
            m: contract.m as usize,
            m0: contract.m0 as usize,
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
