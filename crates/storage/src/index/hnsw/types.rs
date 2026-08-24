// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # HNSW Basic Types
//!
//! Core types for the HNSW (Hierarchical Navigable Small World) index.

#[cfg(test)]
use paro_common::error::{self as paro_error, Result};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

use crate::index::ExactRowSet;
use crate::tablet::ColumnId;

/// Point offset type — identifies a vector within a segment.
pub type PointOffset = u32;

/// Score type for distance/similarity values.
pub type ScoreType = f32;
pub const DEFAULT_HNSW_BUILD_SEED: u64 = 0x5041_524f_484e_5357;
pub const DEFAULT_HNSW_M: u32 = 24;
pub const DEFAULT_HNSW_EF_CONSTRUCT: u32 = 100;
pub const DEFAULT_HNSW_EF_SEARCH: u32 = 100;
pub const DEFAULT_HNSW_PLAIN_SCAN_THRESHOLD: u32 = 10_000;
/// Filtered searches below this query-wide visible cardinality are exact scans.
/// The graph path has fixed traversal cost and loses connectivity as the
/// matching subgraph becomes sparse; 20k 128-dimensional distances remain a
/// cache-friendly SIMD workload and give deterministic recall.
pub const DEFAULT_HNSW_FILTERED_PLAIN_SCAN_THRESHOLD: u32 = 20_000;
pub const DEFAULT_HNSW_PROPOSAL_WAVE_SIZE: u32 = 64;
pub const DEFAULT_HNSW_WARMUP_POINT_COUNT: u32 = 4_096;
pub const DEFAULT_HNSW_FILTER_BLOCK_ROWS: u32 = 20_000;
pub const DEFAULT_HNSW_FILTER_M: u32 = 8;
pub const MAX_HNSW_FILTER_COLUMNS: usize = 8;
pub const HNSW_FILTER_TOPOLOGY_VERSION: u32 = 4;
/// Version 9 stores each scalar dictionary posting as an exact contiguous run
/// inside its covering block. Exact scans no longer over-read neighboring
/// ordinals or need a heuristic fallback to random base-vector gathers.
/// Version 8 adds a scalar-block covering vector layout. Exact predicate scans
/// read sequential artifact ranges instead of gathering base-column rows.
/// Version 7 retains every scalar block's hierarchy and tagged entry point.
/// Filtered navigation can enter each admitted block directly instead of
/// relying on an ordinary level-0 beam to discover disconnected partitions.
/// Version 6 connects deterministic scalar blocks with bounded vector-aware
/// cross-block routing edges. Range predicates can navigate the admitted
/// union without relying on the ordinary graph to seed every block.
/// Version 5 makes predicate-local level-0 topology part of the durable graph
/// contract. Filter-column identities and block construction parameters are
/// now self-describing artifact fields rather than mutable query policy.
/// Version 4 replaced the affine point order with a keyed Feistel permutation
/// followed by cycle walking. One-point warm-up waves and deterministic frozen
/// proposal waves remain durable topology fields; changing point ordering or
/// publication semantics requires a new contract version.
pub const HNSW_BUILD_CONTRACT_VERSION: u32 = 9;

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
    /// Standard unfiltered HNSW search.
    Hnsw,
    /// Keep HNSW navigation on the connected unfiltered graph while admitting
    /// every scored matching candidate into a separate exact-bitmap Top-K.
    /// This is sufficient for visibility masks and broad user predicates.
    MaskedTopK,
    /// Start with connected masked Top-K navigation and inspect the observed
    /// admission population. If it is too small, continue with bounded
    /// predicate-aware two-hop expansion before the caller considers an exact
    /// fallback. The decision to refine is made from observed work, not a
    /// selectivity estimate.
    AdaptiveFilteredTopK,
}

impl Default for SearchAlgorithm {
    fn default() -> Self {
        SearchAlgorithm::Hnsw
    }
}

/// Exact segment-local admission mask and its semantic origin.
///
/// Keeping visibility and user predicates distinct prevents MVCC delete masks
/// from accidentally selecting the more expensive predicate-refinement path.
#[derive(Debug, Clone, Copy)]
pub enum HnswSearchFilter<'a> {
    None,
    Visibility(&'a dyn ExactRowSet),
    Predicate {
        row_set: &'a dyn ExactRowSet,
        columns: &'a [ColumnId],
    },
}

/// Semantic origin of an HNSW admission mask. Strategy selection consumes the
/// origin and exact cardinalities without retaining a bitmap reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HnswFilterKind {
    None,
    Visibility,
    Predicate,
}

impl<'a> HnswSearchFilter<'a> {
    pub fn row_set(self) -> Option<&'a dyn ExactRowSet> {
        match self {
            Self::None => None,
            Self::Visibility(row_set) | Self::Predicate { row_set, .. } => Some(row_set),
        }
    }

    pub const fn kind(self) -> HnswFilterKind {
        match self {
            Self::None => HnswFilterKind::None,
            Self::Visibility(_) => HnswFilterKind::Visibility,
            Self::Predicate { .. } => HnswFilterKind::Predicate,
        }
    }

    pub const fn predicate(row_set: &'a dyn ExactRowSet, columns: &'a [ColumnId]) -> Self {
        Self::Predicate { row_set, columns }
    }

    pub const fn predicate_columns(self) -> &'a [ColumnId] {
        match self {
            Self::Predicate { columns, .. } => columns,
            Self::None | Self::Visibility(_) => &[],
        }
    }

    /// Select the predicate-local graph when exact runtime cardinality proves
    /// that the predicate-induced base graph is below its designed local
    /// degree. This is a per-segment connectivity decision, not a selectivity
    /// estimate: broad predicates stay on the connected base HNSW while sparse
    /// predicates use the denser scalar-block topology.
    pub fn uses_predicate_topology(
        self,
        topology: &HnswFilterTopologyContract,
        total_rows: usize,
        base_level0_degree: usize,
    ) -> bool {
        match self {
            Self::Predicate { row_set, columns } => {
                if !columns
                    .iter()
                    .any(|column| topology.columns().contains(column))
                {
                    return false;
                }
                let admitted_degree = row_set.len().saturating_mul(base_level0_degree as u64);
                let topology_degree = (total_rows as u64).saturating_mul(u64::from(topology.m));
                admitted_degree < topology_degree
            }
            Self::None | Self::Visibility(_) => false,
        }
    }
}

/// Search parameters for HNSW queries.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SearchParams {
    /// Size of the beam in beam-search. Larger = more accurate but slower.
    /// If None, uses the index's default ef.
    pub ef: Option<usize>,
    /// Whether to randomize entry point selection during graph search.
    /// `None` lets the index choose a default strategy.
    #[serde(default)]
    pub random_entry_point: Option<bool>,
}

/// Per-segment HNSW execution contract. Exact cardinalities are known at this
/// boundary, so each immutable segment can independently choose a sequential
/// exact scan or graph traversal. This avoids paying random graph-navigation
/// cost for locally small candidate sets without weakening result semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HnswSearchStrategy {
    ExactScan,
    UnfilteredGraph,
    MaskedGraph,
    AdaptiveFilteredGraph,
}

/// Query-wide execution decision derived from exact cardinalities. Segment
/// boundaries and machine width are physical concerns and must not change
/// whether the same logical candidate set is scanned exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HnswQueryWideStrategy {
    ExactScan,
    SegmentAdaptive,
}

/// Executed HNSW path. This is runtime evidence, not the strategy estimated by
/// the optimizer or printed by plain EXPLAIN.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HnswExactScanKind {
    BaseVectors,
    PredicateCovering,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HnswSearchPath {
    ExactScan(HnswExactScanKind),
    UnfilteredGraph,
    MaskedGraph,
    AdaptiveGraph,
}

/// Predicate admission work actually performed by a graph search. This is
/// orthogonal to graph topology and repair: broad predicates can validate the
/// retained global beam once, while selective/adversarial predicates retain
/// exact admission during candidate scoring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HnswPredicateAdmissionMode {
    NotApplicable,
    EagerPerCandidate,
    DeferredGlobalBeam,
}

/// Mutually consistent runtime outcome for one segment/query search. Keeping
/// the path as an enum prevents telemetry call sites from manufacturing
/// impossible combinations of positional booleans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswSearchOutcome {
    pub path: HnswSearchPath,
    pub predicate_admission: HnswPredicateAdmissionMode,
    pub predicate_topology_used: bool,
    pub predicate_refined: bool,
    pub exact_fallback: Option<HnswExactScanKind>,
}

impl HnswSearchOutcome {
    pub const fn new(path: HnswSearchPath) -> Self {
        Self {
            path,
            predicate_admission: HnswPredicateAdmissionMode::NotApplicable,
            predicate_topology_used: false,
            predicate_refined: false,
            exact_fallback: None,
        }
    }

    pub const fn with_predicate_admission(mut self, admission: HnswPredicateAdmissionMode) -> Self {
        self.predicate_admission = admission;
        self
    }

    pub const fn with_predicate_topology(mut self, used: bool) -> Self {
        self.predicate_topology_used = used;
        self
    }

    pub const fn with_predicate_refinement(mut self, refined: bool) -> Self {
        self.predicate_refined = refined;
        self
    }

    pub const fn with_exact_fallback(mut self, kind: HnswExactScanKind) -> Self {
        self.exact_fallback = Some(kind);
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HnswSearchResult {
    pub points: Vec<ScoredPoint>,
    pub scored_points: u64,
    pub outcome: HnswSearchOutcome,
}

impl HnswSearchResult {
    pub fn len(&self) -> usize {
        self.points.len()
    }

    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }
}

impl HnswSearchStrategy {
    fn graph_for_filter(filter_kind: HnswFilterKind) -> Self {
        match filter_kind {
            HnswFilterKind::None => Self::UnfilteredGraph,
            HnswFilterKind::Visibility => Self::MaskedGraph,
            HnswFilterKind::Predicate => Self::AdaptiveFilteredGraph,
        }
    }
}

/// Hardware-independent work units used to lower a query-wide HNSW decision
/// to one immutable segment.
///
/// A sequential vector score reads a contiguous artifact range and is much
/// cheaper than a graph score, which follows an edge to an effectively random
/// vector.  Keeping this ratio in the provider model makes the crossover
/// explicit and lets the optimizer consume the same physical assumption.
/// The dimension term cancels because both sides score the same vector type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswDistanceCostModel;

impl HnswDistanceCostModel {
    pub const SEQUENTIAL_SCORES_PER_RANDOM_SCORE: u64 = 8;

    pub const fn sequential_work(candidate_rows: u64) -> u64 {
        candidate_rows.div_ceil(Self::SEQUENTIAL_SCORES_PER_RANDOM_SCORE)
    }

    pub fn graph_work(total_rows: u64, effective_ef: usize, level0_degree: usize) -> u64 {
        let navigation = total_rows.max(1).ilog2() as u64;
        navigation.saturating_add(
            (effective_ef.max(1) as u64).saturating_mul(level0_degree.max(1) as u64),
        )
    }

    pub fn prefers_exact_scan(
        candidate_rows: u64,
        total_rows: u64,
        effective_ef: usize,
        level0_degree: usize,
    ) -> bool {
        Self::sequential_work(candidate_rows.min(total_rows))
            <= Self::graph_work(total_rows, effective_ef, level0_degree)
    }
}

/// Exact physical inputs needed to choose one segment execution path after
/// the logical query-wide exact/graph decision has already been made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswSegmentSearchInput {
    pub filter_kind: HnswFilterKind,
    pub matching_rows: u64,
    pub total_rows: u64,
    pub effective_ef: usize,
    pub level0_degree: usize,
}

impl HnswQueryWideStrategy {
    /// Select exact scanning from the total logical candidate set. Thresholds
    /// are logical cardinality crossovers measured for the complete query;
    /// scaling them by CPU count would make replicas choose different paths.
    pub fn choose(
        filter_kind: HnswFilterKind,
        matching_rows: u64,
        total_rows: u64,
        policy: HnswSearchPolicy,
    ) -> Self {
        let exact_capacity = match filter_kind {
            HnswFilterKind::None => policy.plain_scan_threshold,
            HnswFilterKind::Visibility | HnswFilterKind::Predicate => {
                policy.filtered_plain_scan_threshold
            }
        } as u64;
        if matching_rows.min(total_rows) <= exact_capacity {
            Self::ExactScan
        } else {
            Self::SegmentAdaptive
        }
    }

    /// Lower the query-wide contract to one segment. When the whole query is
    /// not an exact scan, locally tiny segments may still choose exact scoring;
    /// doing so is exact and avoids wasting graph setup on a small tail.
    pub fn for_segment(self, input: HnswSegmentSearchInput) -> HnswSearchStrategy {
        match self {
            Self::ExactScan => HnswSearchStrategy::ExactScan,
            Self::SegmentAdaptive => {
                if HnswDistanceCostModel::prefers_exact_scan(
                    input.matching_rows,
                    input.total_rows,
                    input.effective_ef,
                    input.level0_degree,
                ) {
                    HnswSearchStrategy::ExactScan
                } else {
                    HnswSearchStrategy::graph_for_filter(input.filter_kind)
                }
            }
        }
    }
}

/// Shared filtered-search strategy decision used by runtime, costing, and
/// EXPLAIN. Callers may use exact per-segment cardinalities or planning-time
/// estimates, but the threshold and selectivity boundaries stay identical.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HnswFilteredSearchStrategy {
    ExactScan,
    MaskedTopK,
    RefinedTopK,
}

/// Result of the shared filtered-search policy. The beam cardinalities are
/// estimates rather than correctness guarantees; the exact row set remains the
/// sole admission contract in every strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswFilteredSearchDecision {
    pub strategy: HnswFilteredSearchStrategy,
    /// Expected number of unique points scored by connected level-0 search.
    /// Runtime does not trust this estimate; it measures admissions directly.
    pub expected_scored_points: u64,
    /// Expected predicate-matching points among all scored neighbors, not just
    /// among the final `ef` beam.
    pub expected_admitted_points: u64,
    /// Minimum expected matching beam population required before predicate
    /// refinement is considered redundant.
    pub required_admitted_points: u64,
}

const MASKED_TOPK_HEADROOM_NUMERATOR: u64 = 3;
const MASKED_TOPK_HEADROOM_DENOMINATOR: u64 = 2;

pub fn required_filtered_admissions(top_k: usize) -> u64 {
    (top_k as u128 * MASKED_TOPK_HEADROOM_NUMERATOR as u128)
        .div_ceil(MASKED_TOPK_HEADROOM_DENOMINATOR as u128)
        .min(u64::MAX as u128) as u64
}

/// Estimate the likely adaptive filtered-graph outcome for costing and
/// EXPLAIN. Execution uses exact cardinalities only to choose exact scan versus
/// adaptive graph; after graph navigation it decides from the observed number
/// of admitted points. `avg_level0_degree` therefore cannot cause a correctness
/// or latency cliff when filters correlate with vector geometry.
pub fn estimate_filtered_search_strategy(
    matching_rows: u64,
    total_rows: u64,
    top_k: usize,
    effective_ef: usize,
    avg_level0_degree: f32,
    policy: HnswSearchPolicy,
) -> HnswFilteredSearchDecision {
    let matching_rows = matching_rows.min(total_rows);
    let effective_ef = effective_ef.max(top_k).max(1);
    let degree = if avg_level0_degree.is_finite() && avg_level0_degree > 0.0 {
        avg_level0_degree.ceil() as u64
    } else {
        1
    };
    let expected_scored_points = (effective_ef as u64).saturating_mul(degree).min(total_rows);
    let expected_admitted_points = if total_rows == 0 {
        0
    } else {
        ((matching_rows as u128 * expected_scored_points as u128) / total_rows as u128)
            .min(u64::MAX as u128) as u64
    };
    let required_admitted_points = required_filtered_admissions(top_k);

    let strategy = if matching_rows <= policy.filtered_plain_scan_threshold as u64 {
        HnswFilteredSearchStrategy::ExactScan
    } else if expected_admitted_points >= required_admitted_points {
        // Connected navigation already expects enough exact-bitmap-admitted
        // candidates to fill Top-K with 50% headroom. Predicate-local
        // refinement would repeat graph work without adding a useful frontier.
        HnswFilteredSearchStrategy::MaskedTopK
    } else {
        HnswFilteredSearchStrategy::RefinedTopK
    };

    HnswFilteredSearchDecision {
        strategy,
        expected_scored_points,
        expected_admitted_points,
        required_admitted_points,
    }
}

impl Default for SearchParams {
    fn default() -> Self {
        SearchParams {
            ef: None,
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
pub struct HnswFilterTopologyContract {
    pub version: u32,
    pub column_count: u32,
    pub column_ids: [u32; MAX_HNSW_FILTER_COLUMNS],
    /// Target cardinality of one ordered scalar block. Build-time scalar
    /// dictionaries may choose a slightly larger boundary to avoid splitting
    /// equal values across blocks.
    pub target_block_rows: u32,
    /// Per-point degree of each predicate-local hierarchy.
    pub m: u32,
}

impl Default for HnswFilterTopologyContract {
    fn default() -> Self {
        Self {
            version: HNSW_FILTER_TOPOLOGY_VERSION,
            column_count: 0,
            column_ids: [0; MAX_HNSW_FILTER_COLUMNS],
            target_block_rows: DEFAULT_HNSW_FILTER_BLOCK_ROWS,
            m: DEFAULT_HNSW_FILTER_M,
        }
    }
}

impl HnswFilterTopologyContract {
    pub fn from_columns(
        columns: &[u32],
        target_block_rows: u32,
        m: u32,
    ) -> paro_common::error::Result<Self> {
        if columns.len() > MAX_HNSW_FILTER_COLUMNS {
            return Err(paro_common::error::configuration_limit_exceeded(format!(
                "HNSW predicate topology supports at most {MAX_HNSW_FILTER_COLUMNS} columns"
            )));
        }
        let mut column_ids = [0; MAX_HNSW_FILTER_COLUMNS];
        column_ids[..columns.len()].copy_from_slice(columns);
        let contract = Self {
            version: HNSW_FILTER_TOPOLOGY_VERSION,
            column_count: columns.len() as u32,
            column_ids,
            target_block_rows,
            m,
        };
        contract.validate()?;
        Ok(contract)
    }

    pub fn columns(&self) -> &[u32] {
        &self.column_ids[..self.column_count as usize]
    }

    pub fn is_enabled(&self) -> bool {
        self.column_count != 0
    }

    pub fn validate(&self) -> paro_common::error::Result<()> {
        if self.version != HNSW_FILTER_TOPOLOGY_VERSION {
            return Err(paro_common::error::data_corrupted(format!(
                "unsupported HNSW filter-topology version {}, expected {}",
                self.version, HNSW_FILTER_TOPOLOGY_VERSION
            )));
        }
        if self.column_count as usize > MAX_HNSW_FILTER_COLUMNS {
            return Err(paro_common::error::data_corrupted(format!(
                "HNSW filter-topology column count {} exceeds {}",
                self.column_count, MAX_HNSW_FILTER_COLUMNS
            )));
        }
        let columns = self.columns();
        if columns.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(paro_common::error::data_corrupted(
                "HNSW filter-topology column ids must be strictly increasing",
            ));
        }
        if self.column_ids[columns.len()..].iter().any(|id| *id != 0) {
            return Err(paro_common::error::data_corrupted(
                "HNSW filter-topology unused column slots must be zero",
            ));
        }
        if self.target_block_rows == 0 {
            return Err(paro_common::error::data_corrupted(
                "HNSW filter-topology target_block_rows must be greater than zero",
            ));
        }
        if !(2..=64).contains(&self.m) {
            return Err(paro_common::error::data_corrupted(format!(
                "HNSW filter-topology m must be between 2 and 64, got {}",
                self.m
            )));
        }
        Ok(())
    }
}

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
    /// Predicate-local topology built from explicitly named scalar columns.
    pub filter_topology: HnswFilterTopologyContract,
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
        self.filter_topology.validate()?;
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

impl HnswSearchPolicy {
    pub fn effective_ef(self, top_k: usize, requested_ef: Option<usize>) -> usize {
        requested_ef.unwrap_or(self.ef_search).max(top_k).max(1)
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
            filter_topology: HnswFilterTopologyContract::default(),
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
        assert_eq!(
            config.filtered_plain_scan_threshold,
            DEFAULT_HNSW_FILTERED_PLAIN_SCAN_THRESHOLD as usize
        );
    }

    #[test]
    fn test_hnsw_config_new() {
        let config = HnswConfig::new(8, 200);
        assert_eq!(config.m, 8);
        assert_eq!(config.m0, 16);
        assert_eq!(config.ef_construct, 200);
        assert_eq!(config.ef, 200);
        assert_eq!(config.plain_scan_threshold, 10_000);
        assert_eq!(
            config.filtered_plain_scan_threshold,
            DEFAULT_HNSW_FILTERED_PLAIN_SCAN_THRESHOLD as usize
        );
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

    #[test]
    fn filtered_strategy_estimate_uses_scored_neighbors_not_final_beam() {
        let policy = HnswSearchPolicy::default();

        let exact = estimate_filtered_search_strategy(20_000, 1_000_000, 10, 160, 16.0, policy);
        assert_eq!(exact.strategy, HnswFilteredSearchStrategy::ExactScan);

        let masked = estimate_filtered_search_strategy(50_000, 1_000_000, 10, 160, 16.0, policy);
        assert_eq!(masked.expected_scored_points, 2_560);
        assert_eq!(masked.expected_admitted_points, 128);
        assert_eq!(masked.required_admitted_points, 15);
        assert_eq!(masked.strategy, HnswFilteredSearchStrategy::MaskedTopK);

        let no_degree = estimate_filtered_search_strategy(
            50_000,
            1_000_000,
            10,
            160,
            0.0,
            HnswSearchPolicy {
                filtered_plain_scan_threshold: 0,
                ..policy
            },
        );
        assert_eq!(no_degree.expected_admitted_points, 8);
        assert_eq!(no_degree.strategy, HnswFilteredSearchStrategy::RefinedTopK);
    }

    #[test]
    fn filtered_strategy_adapts_to_topk_and_effective_ef() {
        let policy = HnswSearchPolicy::default();
        let matching_rows = 10_000;
        let total_rows = 1_000_000;
        let policy = HnswSearchPolicy {
            filtered_plain_scan_threshold: 0,
            ..policy
        };

        assert_eq!(
            estimate_filtered_search_strategy(matching_rows, total_rows, 10, 100, 16.0, policy,)
                .strategy,
            HnswFilteredSearchStrategy::MaskedTopK
        );
        assert_eq!(
            estimate_filtered_search_strategy(matching_rows, total_rows, 10, 160, 16.0, policy,)
                .strategy,
            HnswFilteredSearchStrategy::MaskedTopK
        );
        assert_eq!(
            estimate_filtered_search_strategy(matching_rows, total_rows, 20, 160, 16.0, policy,)
                .strategy,
            HnswFilteredSearchStrategy::RefinedTopK
        );
    }

    #[test]
    fn segment_cost_compares_sequential_scoring_with_random_graph_work() {
        assert!(HnswDistanceCostModel::prefers_exact_scan(
            20_000, 2_000_000, 128, 32,
        ));
        assert!(!HnswDistanceCostModel::prefers_exact_scan(
            84_800, 169_600, 128, 32,
        ));
    }

    #[test]
    fn query_wide_exact_strategy_is_independent_of_segment_shape() {
        let policy = HnswSearchPolicy {
            filtered_plain_scan_threshold: 20_000,
            ..HnswSearchPolicy::default()
        };
        let query_strategy =
            HnswQueryWideStrategy::choose(HnswFilterKind::Predicate, 18_000, 10_000_000, policy);
        assert_eq!(query_strategy, HnswQueryWideStrategy::ExactScan);

        // The same logical 18k candidates remain exact regardless of how a
        // compaction partitions those rows across immutable segments.
        for (matching_rows, segment_rows) in
            [(3_000, 2_000_000), (10_000, 7_000_000), (5_000, 1_000_000)]
        {
            assert_eq!(
                query_strategy.for_segment(HnswSegmentSearchInput {
                    filter_kind: HnswFilterKind::Predicate,
                    matching_rows,
                    total_rows: segment_rows,
                    effective_ef: 100,
                    level0_degree: 32,
                },),
                HnswSearchStrategy::ExactScan
            );
        }

        let adaptive =
            HnswQueryWideStrategy::choose(HnswFilterKind::Predicate, 1_000_000, 10_000_000, policy);
        assert_eq!(adaptive, HnswQueryWideStrategy::SegmentAdaptive);
        assert_eq!(
            adaptive.for_segment(HnswSegmentSearchInput {
                filter_kind: HnswFilterKind::Predicate,
                matching_rows: 10_000,
                total_rows: 1_000_000,
                effective_ef: 100,
                level0_degree: 32,
            }),
            HnswSearchStrategy::ExactScan
        );
        assert_eq!(
            adaptive.for_segment(HnswSegmentSearchInput {
                filter_kind: HnswFilterKind::Predicate,
                matching_rows: 100_000,
                total_rows: 1_000_000,
                effective_ef: 100,
                level0_degree: 32,
            }),
            HnswSearchStrategy::AdaptiveFilteredGraph
        );
    }

    #[test]
    fn predicate_topology_is_selected_only_for_configured_predicate_columns() {
        let rows = roaring::RoaringBitmap::from_iter([1, 2, 3]);
        let topology = HnswFilterTopologyContract::from_columns(&[4, 7], 20_000, 8).unwrap();

        assert!(
            HnswSearchFilter::predicate(&rows, &[7]).uses_predicate_topology(&topology, 100, 32)
        );
        assert!(
            !HnswSearchFilter::predicate(&rows, &[8]).uses_predicate_topology(&topology, 100, 32)
        );
        assert!(!HnswSearchFilter::Visibility(&rows).uses_predicate_topology(&topology, 100, 32));

        let broad_rows = roaring::RoaringBitmap::from_iter(0..50);
        assert!(!HnswSearchFilter::predicate(&broad_rows, &[7])
            .uses_predicate_topology(&topology, 100, 32));
    }
}
