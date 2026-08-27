// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # HNSW Basic Types
//!
//! Core types for the HNSW (Hierarchical Navigable Small World) index.

#[cfg(test)]
use paro_common::error::{self as paro_error, Result};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::num::NonZeroU32;

use crate::index::{ExactRowPartitions, ExactRowSet};
use crate::tablet::ColumnId;

/// Point offset type — identifies a vector within a segment.
pub type PointOffset = u32;

/// Score type for distance/similarity values.
pub type ScoreType = f32;
pub const DEFAULT_HNSW_BUILD_SEED: u64 = 0x5041_524f_484e_5357;
pub const DEFAULT_HNSW_M: u32 = 24;
pub const DEFAULT_HNSW_EF_CONSTRUCT: u32 = 100;
pub const DEFAULT_HNSW_EF_SEARCH: u32 = 100;
pub const DEFAULT_HNSW_PROPOSAL_WAVE_SIZE: u32 = 64;
pub const DEFAULT_HNSW_WARMUP_POINT_COUNT: u32 = 4_096;
pub const DEFAULT_HNSW_FILTER_BLOCK_ROWS: u32 = 20_000;
pub const DEFAULT_HNSW_FILTER_M: u32 = 8;
/// Fixed cost of initiating one random vector-row access. Cost profiles use a
/// dimension unit rather than a reference-dimension ratio, so graph scoring
/// can price the actual durable routing representation.
pub const DEFAULT_HNSW_RANDOM_ACCESS_COST_UNITS: u32 = 416;
pub const DEFAULT_HNSW_EXACT_F32_DIMENSION_COST_UNITS: u32 = 1;
pub const DEFAULT_HNSW_SEQUENTIAL_DIMENSION_COST_UNITS: u32 = 1;
pub const DEFAULT_HNSW_SYMMETRIC_I16_DIMENSION_COST_UNITS: u32 = 1;
/// Deterministic graph-work profile. HNSW level-0 neighborhoods overlap, so
/// one unit of `ef` can score points across several expanded neighborhoods and
/// is not bounded by one row's average or maximum degree. Revisions 4-5 use 24,
/// measured as 24.7 at high `ef` on the reproducible 10M uniform-32d workload
/// with M=16, M0=32 and ef_construct=100. Different graph contracts or hardware
/// should publish an offline calibration instead of mutating process-local
/// timing state.
pub const DEFAULT_HNSW_GRAPH_SCORED_POINTS_PER_EF: u32 = 24;
/// Revision of the built-in, reproducible distance-cost calibration. Changing
/// a coefficient or the physical-work interpretation requires a new revision
/// so persisted definitions describe the exact decision surface they use.
/// Revision 5 represents physical work directly as fixed random-access and
/// per-dimension units. Graph scoring is charged against the artifact's actual
/// routing representation, so compact i16 navigation is not costed as a full
/// canonical f32 row. Revision 4 used derived reference-dimension ratios.
pub const HNSW_BUILT_IN_DISTANCE_COST_REVISION: u32 = 5;
pub const MAX_HNSW_FILTER_COLUMNS: usize = 8;
pub const HNSW_FILTER_TOPOLOGY_VERSION: u32 = 4;
/// Version 14 makes compact encoding and its non-zero routing dimension a
/// single sum type, so an invalid encoding/dimension pair cannot exist in a
/// catalog, build contract, compaction job, or in-memory plan. Version 13
/// upgrades compact construction routing to per-coordinate i16.
/// The wider code preserves ordered low-amplitude geometry that an i8 image
/// can collapse, while remaining substantially smaller than canonical f32.
/// Version 12 makes the construction routing-vector encoding part of the
/// durable topology contract. Changing the encoding or its parameters creates
/// a distinct topology rather than silently rebuilding different graph bytes.
/// Version 11 canonicalizes unordered point-pair scoring before every build
/// and repair heuristic, including cosine inverse-norm multiplication. This
/// makes the last-bit score image independent of call-site operand order.
/// Version 10 applies `ef_construct` on every HNSW layer. Earlier builders
/// silently narrowed upper-layer construction to M, weakening the sparse
/// routing hierarchy as graph partitions grew.
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
pub const HNSW_BUILD_CONTRACT_VERSION: u32 = 14;
/// Maximum number of deterministic source coordinates retained by the
/// compact construction routing space. The original f32 dimension remains
/// authoritative for SQL scoring and exact re-ranking.
pub const DEFAULT_HNSW_BUILD_ROUTING_DIMENSIONS: u32 = 128;

/// Physical representation used for construction-time pair scoring.
///
/// HNSW topology is already approximate, so a compact, deterministic routing
/// representation can remove the raw-vector random-I/O working set without
/// changing SQL storage. Query artifacts retain the canonical f32 vectors and
/// return scores in the requested metric; this field only defines how durable
/// graph topology was constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HnswBuildVectorEncoding {
    ExactF32,
    SymmetricI16 {
        routing_dimensions: std::num::NonZeroU16,
    },
}

impl HnswBuildVectorEncoding {
    pub fn symmetric_i16(routing_dimensions: u32) -> paro_common::error::Result<Self> {
        let routing_dimensions = u16::try_from(routing_dimensions)
            .ok()
            .and_then(std::num::NonZeroU16::new)
            .ok_or_else(|| {
                paro_common::error::invalid_input(format!(
                    "symmetric-i16 HNSW routing dimensions must be between 1 and {}, got {routing_dimensions}",
                    u16::MAX
                ))
            })?;
        Ok(Self::SymmetricI16 { routing_dimensions })
    }

    pub fn default_for_dimension(dimension: u32) -> paro_common::error::Result<Self> {
        Self::symmetric_i16(dimension.min(DEFAULT_HNSW_BUILD_ROUTING_DIMENSIONS))
    }

    pub const fn routing_dimensions(self) -> Option<u16> {
        match self {
            Self::ExactF32 => None,
            Self::SymmetricI16 { routing_dimensions } => Some(routing_dimensions.get()),
        }
    }
}

impl std::fmt::Display for HnswBuildVectorEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExactF32 => f.write_str("exact_f32"),
            Self::SymmetricI16 { routing_dimensions } => {
                write!(f, "symmetric_i16({routing_dimensions})")
            }
        }
    }
}

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
    #[inline(always)]
    fn cmp(&self, other: &Self) -> Ordering {
        // `total_cmp` gives the heap a real total order even if corrupted or
        // extension-provided vector data produces NaN. Falling back to point
        // id for every unordered pair is not transitive and violates `Ord`.
        match self.score.total_cmp(&other.score) {
            Ordering::Equal => self.idx.cmp(&other.idx),
            ordering => ordering,
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

/// Physical work owned by one exact scan.
///
/// A generation may contain both predicate-covering parts and freshly flushed
/// base-vector parts. Keeping both row counts avoids an all-or-nothing plan
/// cliff when one small tail segment has not acquired its covering artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswExactScanWorkload {
    pub sequential_rows: u64,
    pub indexed_base_rows: u64,
}

impl HnswExactScanWorkload {
    pub const fn sequential(rows: u64) -> Self {
        Self {
            sequential_rows: rows,
            indexed_base_rows: 0,
        }
    }

    pub const fn indexed_base(rows: u64) -> Self {
        Self {
            sequential_rows: 0,
            indexed_base_rows: rows,
        }
    }

    pub const fn total_rows(self) -> u64 {
        self.sequential_rows.saturating_add(self.indexed_base_rows)
    }

    fn for_partitions(
        partitions: ExactRowPartitions<'_>,
        has_covering_column: impl Fn(ColumnId) -> bool,
    ) -> Self {
        let mut workload = Self::sequential(0);
        let mut pending = vec![partitions];
        while let Some(partition) = pending.pop() {
            match partition {
                ExactRowPartitions::Dense(rows) => {
                    workload.sequential_rows =
                        workload.sequential_rows.saturating_add(u64::from(rows));
                }
                ExactRowPartitions::OrdinalSelection(row_set) => {
                    // The complete scalar index established this cardinality
                    // while compiling the ordinal selection. Re-summing every
                    // selected posting here turns a cost-class lookup into
                    // O(dictionary cardinality) work on every query.
                    let rows = row_set.len();
                    if has_covering_column(row_set.column_id()) {
                        workload.sequential_rows = workload.sequential_rows.saturating_add(rows);
                    } else {
                        workload.indexed_base_rows =
                            workload.indexed_base_rows.saturating_add(rows);
                    }
                }
                ExactRowPartitions::Partitioned(row_set) => {
                    pending.extend(
                        row_set
                            .physical_parts()
                            .map(|(_, part)| part.physical_partitions()),
                    );
                }
                ExactRowPartitions::Single(bitmap) => {
                    workload.indexed_base_rows =
                        workload.indexed_base_rows.saturating_add(bitmap.len());
                }
            }
        }
        workload
    }
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

    pub fn exact_scan_workload(
        self,
        total_rows: u64,
        has_covering_column: impl Fn(ColumnId) -> bool,
    ) -> HnswExactScanWorkload {
        self.row_set().map_or_else(
            || HnswExactScanWorkload::sequential(total_rows),
            |row_set| {
                HnswExactScanWorkload::for_partitions(
                    row_set.physical_partitions(),
                    has_covering_column,
                )
            },
        )
    }

    /// Whether the durable artifact contains a predicate-local topology for
    /// at least one referenced scalar column. Cardinality alone cannot decide
    /// whether that topology is useful: correlated predicates may disconnect
    /// a broad subset while an independent selective predicate remains locally
    /// navigable. The graph executor therefore consumes this as availability
    /// and decides from observed phase-one admission/locality.
    pub fn predicate_topology_available(self, topology: &HnswFilterTopologyContract) -> bool {
        match self {
            Self::Predicate { columns, .. } => columns
                .iter()
                .any(|column| topology.columns().contains(column)),
            Self::None | Self::Visibility(_) => false,
        }
    }
}

/// Query-level semantic objective for dense HNSW Top-K.
///
/// `CostOptimized` permits the immutable, definition-pinned cost model to
/// choose between graph traversal and exact scoring. `Exact` is a binding
/// result contract: every admitted vector is scored and the graph is not
/// consulted. It deliberately does not expose a numeric recall target because
/// an approximate graph cannot prove such a probability for one query.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HnswSearchObjective {
    #[default]
    CostOptimized,
    Exact,
}

/// Definition-pinned policy for crossing the lossy graph-routing boundary.
///
/// `TopK` retains only the user-visible result width, `Ef` retains the full
/// navigation beam, and `Fixed` retains at least the configured number of
/// candidates. Query-level hints may override the resolved window without
/// changing the durable graph contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HnswRerankPolicy {
    TopK,
    Ef,
    Fixed { candidates: NonZeroU32 },
}

impl HnswRerankPolicy {
    pub const fn default_for_encoding(encoding: HnswBuildVectorEncoding) -> Self {
        match encoding {
            HnswBuildVectorEncoding::ExactF32 => Self::TopK,
            HnswBuildVectorEncoding::SymmetricI16 { .. } => Self::Ef,
        }
    }

    fn resolve(self, top_k: usize, effective_ef: usize) -> usize {
        match self {
            Self::TopK => top_k,
            Self::Ef => effective_ef,
            Self::Fixed { candidates } => usize::try_from(candidates.get()).unwrap_or(usize::MAX),
        }
        .max(top_k)
    }
}

impl std::fmt::Display for HnswRerankPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TopK => f.write_str("top_k"),
            Self::Ef => f.write_str("ef"),
            Self::Fixed { candidates } => write!(f, "fixed({candidates})"),
        }
    }
}

impl std::fmt::Display for HnswSearchObjective {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CostOptimized => f.write_str("cost_optimized"),
            Self::Exact => f.write_str("exact"),
        }
    }
}

/// Planner-owned dense vector search options propagated as one typed value.
///
/// Keeping quality/performance intent beside `ef` prevents query modifiers,
/// logical operators, and prepared plans from growing parallel optional
/// fields whose combinations are never validated together.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HnswQueryOptions {
    pub ef: Option<usize>,
    pub rerank_window: Option<usize>,
    pub objective: HnswSearchObjective,
}

/// Search parameters for HNSW queries.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SearchParams {
    /// Size of the beam in beam-search. Larger = more accurate but slower.
    /// If None, uses the index's default ef.
    pub ef: Option<usize>,
    /// Candidate window re-scored with canonical f32 vectors after lossy
    /// graph navigation. `None` uses the definition-pinned rerank policy.
    pub rerank_window: Option<usize>,
    /// Binding result contract selected by the query.
    pub objective: HnswSearchObjective,
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

/// Executed HNSW path. This is runtime evidence, not the strategy estimated by
/// the optimizer or printed by plain EXPLAIN.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HnswExactScanKind {
    BaseVectors,
    PredicateCovering,
    /// One exact query combined generation-covering ranges with base-vector
    /// ranges from partitions that have not acquired the covering layout yet.
    Hybrid,
}

impl HnswExactScanKind {
    pub const fn uses_predicate_covering(self) -> bool {
        matches!(self, Self::PredicateCovering | Self::Hybrid)
    }
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

/// Immutable relative costs used to choose a path for one physical HNSW
/// artifact.
///
/// Timing history must not silently alter query plans: it makes replicas and
/// EXPLAIN disagree, lets unrelated tables contaminate one another, and never
/// forgets cold-start samples. A deployment may benchmark and pin all three
/// values in the search definition; all readers of that definition then make
/// the same decision until the policy is explicitly changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HnswDistanceCostProfileSource {
    /// Repository-owned calibration with stable, versioned coefficients.
    BuiltIn { revision: u32 },
    /// Explicit offline calibration. The identifier is supplied by the
    /// deployment and ties the three coefficients to a reproducible report.
    OfflineCalibration { calibration_id: u64 },
}

impl std::fmt::Display for HnswDistanceCostProfileSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BuiltIn { revision } => write!(f, "built-in-v{revision}"),
            Self::OfflineCalibration { calibration_id } => {
                write!(f, "offline-calibration-{calibration_id}")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HnswDistanceCostProfile {
    pub source: HnswDistanceCostProfileSource,
    pub random_access_cost_units: u32,
    pub exact_f32_dimension_cost_units: u32,
    pub sequential_dimension_cost_units: u32,
    pub symmetric_i16_dimension_cost_units: u32,
    pub graph_scored_points_per_ef: u32,
}

impl Default for HnswDistanceCostProfile {
    fn default() -> Self {
        Self {
            source: HnswDistanceCostProfileSource::BuiltIn {
                revision: HNSW_BUILT_IN_DISTANCE_COST_REVISION,
            },
            random_access_cost_units: DEFAULT_HNSW_RANDOM_ACCESS_COST_UNITS,
            exact_f32_dimension_cost_units: DEFAULT_HNSW_EXACT_F32_DIMENSION_COST_UNITS,
            sequential_dimension_cost_units: DEFAULT_HNSW_SEQUENTIAL_DIMENSION_COST_UNITS,
            symmetric_i16_dimension_cost_units: DEFAULT_HNSW_SYMMETRIC_I16_DIMENSION_COST_UNITS,
            graph_scored_points_per_ef: DEFAULT_HNSW_GRAPH_SCORED_POINTS_PER_EF,
        }
    }
}

/// Deterministic work-unit conversion shared by optimizer and execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswDistanceCostModel;

pub(crate) const HNSW_MIN_ROWS_PER_PARALLEL_EXACT_LANE: u64 = 16_384;

impl HnswDistanceCostModel {
    /// Return the exact number of independently scannable lanes used by the
    /// exact-score executor for this candidate cardinality. Costing and
    /// execution deliberately share this calculation: charging a query for
    /// every granted CPU slot when its input can only populate one lane makes
    /// the exact/graph crossover depend on an imaginary speedup.
    pub(crate) fn exact_scan_parallelism(candidate_rows: u64, granted_parallelism: usize) -> usize {
        let granted_parallelism = granted_parallelism.max(1);
        if granted_parallelism == 1
            || candidate_rows < HNSW_MIN_ROWS_PER_PARALLEL_EXACT_LANE.saturating_mul(2)
        {
            return 1;
        }
        granted_parallelism
            .min(
                usize::try_from(candidate_rows.div_ceil(HNSW_MIN_ROWS_PER_PARALLEL_EXACT_LANE))
                    .unwrap_or(usize::MAX),
            )
            .max(1)
    }

    pub fn exact_work(
        workload: HnswExactScanWorkload,
        vector_dimension: u32,
        profile: HnswDistanceCostProfile,
    ) -> u64 {
        let dimension = u64::from(vector_dimension.max(1));
        let sequential_score =
            dimension.saturating_mul(u64::from(profile.sequential_dimension_cost_units.max(1)));
        let indexed_score = u64::from(profile.random_access_cost_units.max(1)).saturating_add(
            dimension.saturating_mul(u64::from(profile.exact_f32_dimension_cost_units.max(1))),
        );
        workload
            .sequential_rows
            .saturating_mul(sequential_score)
            .saturating_add(workload.indexed_base_rows.saturating_mul(indexed_score))
    }

    /// Cost a contiguous exact scan in the same direct physical units used by
    /// graph and indexed-base scoring. Sequential rows pay no random-access
    /// charge; their cost grows only with the canonical vector width.
    pub fn sequential_work(
        rows: u64,
        vector_dimension: u32,
        profile: HnswDistanceCostProfile,
    ) -> u64 {
        rows.saturating_mul(u64::from(vector_dimension.max(1)))
            .saturating_mul(u64::from(profile.sequential_dimension_cost_units.max(1)))
    }

    pub fn graph_work(
        total_rows: u64,
        effective_ef: usize,
        rerank_window: usize,
        vector_dimension: u32,
        vector_encoding: HnswBuildVectorEncoding,
        cost_profile: HnswDistanceCostProfile,
    ) -> u64 {
        // Compact routing reduces random graph-row width, but its complete ef
        // beam crosses the lossy/exact boundary before final Top-K. Charge
        // that canonical random gather explicitly; otherwise the planner
        // would price only half of the executable compact path.
        let navigation = total_rows.max(1).ilog2() as u64;
        let scored_points = navigation.saturating_add(
            (effective_ef.max(1) as u64)
                .saturating_mul(u64::from(cost_profile.graph_scored_points_per_ef.max(1))),
        );
        let (scoring_dimension, dimension_units, exact_rerank_rows) = match vector_encoding {
            HnswBuildVectorEncoding::ExactF32 => (
                u64::from(vector_dimension.max(1)),
                u64::from(cost_profile.exact_f32_dimension_cost_units.max(1)),
                0,
            ),
            HnswBuildVectorEncoding::SymmetricI16 { routing_dimensions } => (
                u64::from(routing_dimensions.get()),
                u64::from(cost_profile.symmetric_i16_dimension_cost_units.max(1)),
                rerank_window.max(1) as u64,
            ),
        };
        let random_access = u64::from(cost_profile.random_access_cost_units.max(1));
        let score_cost =
            random_access.saturating_add(scoring_dimension.saturating_mul(dimension_units));
        let exact_rerank_cost =
            random_access.saturating_add(u64::from(vector_dimension.max(1)).saturating_mul(
                u64::from(cost_profile.exact_f32_dimension_cost_units.max(1)),
            ));
        scored_points
            .saturating_mul(score_cost)
            .saturating_add(exact_rerank_rows.saturating_mul(exact_rerank_cost))
    }

    /// Expected graph passes implied by the executable algorithm, not a
    /// selectivity threshold. Predicate search first tries exact admission
    /// from the final unfiltered `ef` beam. If that beam cannot be expected to
    /// hold Top-K plus the required headroom, execution necessarily retries
    /// with eager admission/predicate topology. Charging both passes keeps the
    /// exact-vs-graph crossover aligned with the implementation while leaving
    /// correctness and the runtime adaptive decision independent of estimates.
    pub fn graph_passes(
        filter_kind: HnswFilterKind,
        matching_rows: u64,
        total_rows: u64,
        top_k: usize,
        effective_ef: usize,
    ) -> u64 {
        if filter_kind != HnswFilterKind::Predicate {
            return 1;
        }
        let expected_deferred_admissions =
            expected_deferred_admissions(matching_rows, total_rows, effective_ef.max(top_k));
        if expected_deferred_admissions < required_filtered_admissions(top_k) {
            2
        } else {
            1
        }
    }

    pub fn graph_work_for_search(input: HnswSegmentSearchInput) -> u64 {
        Self::graph_work(
            input.total_rows,
            input.effective_ef,
            input.rerank_window,
            input.vector_dimension,
            input.vector_encoding,
            input.cost_profile,
        )
        .saturating_mul(Self::graph_passes(
            input.filter_kind,
            input.matching_rows,
            input.total_rows,
            input.top_k,
            input.effective_ef,
        ))
    }

    #[cfg(test)]
    fn prefers_exact_scan(
        total_rows: u64,
        effective_ef: usize,
        vector_dimension: u32,
        exact_scan_workload: HnswExactScanWorkload,
        cost_profile: HnswDistanceCostProfile,
    ) -> bool {
        let exact_work = Self::exact_work(exact_scan_workload, vector_dimension, cost_profile);
        exact_work
            <= Self::graph_work(
                total_rows,
                effective_ef,
                effective_ef,
                vector_dimension,
                HnswBuildVectorEncoding::ExactF32,
                cost_profile,
            )
    }
}

/// Exact physical inputs needed to choose one immutable artifact's execution
/// path. No query-wide cardinality gate precedes this decision: the artifact's
/// own covering/base work and graph degree are the complete cost boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswSegmentSearchInput {
    pub objective: HnswSearchObjective,
    pub filter_kind: HnswFilterKind,
    pub matching_rows: u64,
    pub total_rows: u64,
    pub top_k: usize,
    pub effective_ef: usize,
    pub rerank_window: usize,
    pub vector_dimension: u32,
    pub vector_encoding: HnswBuildVectorEncoding,
    pub exact_scan_workload: HnswExactScanWorkload,
    pub cost_profile: HnswDistanceCostProfile,
}

impl HnswSearchStrategy {
    /// Choose from physical work owned by one immutable artifact. Cardinality
    /// thresholds are deliberately absent: a fixed row threshold cannot stay
    /// correct when `ef`, graph degree, covering availability, or hardware
    /// calibration changes. The definition-pinned cost profile is the single
    /// decision surface for both small exact scans and graph traversal.
    pub fn choose(input: HnswSegmentSearchInput) -> Self {
        if input.objective == HnswSearchObjective::Exact {
            return Self::ExactScan;
        }
        // The physical cost contract charges a width-ef traversal at least ef
        // random scores, while exact scoring of at most ef admitted rows does
        // no more work under any valid profile. Keep that invariant explicit
        // instead of relying on today's arithmetic to rediscover it.
        if input.matching_rows <= input.effective_ef.max(input.top_k) as u64 {
            return Self::ExactScan;
        }
        let exact_work = HnswDistanceCostModel::exact_work(
            input.exact_scan_workload,
            input.vector_dimension,
            input.cost_profile,
        );
        if exact_work <= HnswDistanceCostModel::graph_work_for_search(input) {
            Self::ExactScan
        } else {
            Self::graph_for_filter(input.filter_kind)
        }
    }
}

/// Shared filtered-search strategy estimate used by costing and EXPLAIN.
/// Runtime makes the exact-vs-graph choice from artifact-local physical work,
/// then measures graph admissions before deciding whether to refine.
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
    /// Number of graph passes implied by the deferred-admission shape. This is
    /// one for a likely hit and two when execution is expected to retry with
    /// eager admission/topology.
    pub expected_graph_passes: u64,
    /// Expected number of unique points scored by connected level-0 search.
    /// Runtime does not trust this estimate; it measures admissions directly.
    pub expected_scored_points: u64,
    /// Expected predicate-matching points retained by the final unfiltered
    /// `ef` beam. This is the admission population available to the cheap
    /// deferred path; matches scored and discarded outside that beam cannot
    /// prevent the eager retry.
    pub expected_deferred_admitted_points: u64,
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

fn expected_deferred_admissions(matching_rows: u64, total_rows: u64, effective_ef: usize) -> u64 {
    if total_rows == 0 {
        return 0;
    }
    ((matching_rows.min(total_rows) as u128 * effective_ef as u128) / total_rows as u128)
        .min(u64::MAX as u128) as u64
}

/// Estimate the likely adaptive filtered-graph outcome for costing and
/// EXPLAIN. Execution uses exact cardinalities only to choose exact scan versus
/// adaptive graph; after graph navigation it decides from the observed number
/// of admitted points. The definition-pinned scored-points coefficient is
/// used directly: average degree is not an upper bound on unique scores per
/// beam slot, and mixing contract M0 with generation average degree made the
/// same graph choose different paths depending on its physical envelope.
pub fn estimate_filtered_search_strategy(
    matching_rows: u64,
    total_rows: u64,
    top_k: usize,
    effective_ef: usize,
    vector_dimension: u32,
    policy: HnswSearchPolicy,
) -> HnswFilteredSearchDecision {
    let matching_rows = matching_rows.min(total_rows);
    let effective_ef = effective_ef.max(top_k).max(1);
    let expected_scored_points = (effective_ef as u64)
        .saturating_mul(u64::from(
            policy.distance_cost.graph_scored_points_per_ef.max(1),
        ))
        .min(total_rows);
    let expected_deferred_admitted_points =
        expected_deferred_admissions(matching_rows, total_rows, effective_ef);
    let required_admitted_points = required_filtered_admissions(top_k);
    let expected_graph_passes = HnswDistanceCostModel::graph_passes(
        HnswFilterKind::Predicate,
        matching_rows,
        total_rows,
        top_k,
        effective_ef,
    );

    let search_input = HnswSegmentSearchInput {
        objective: HnswSearchObjective::CostOptimized,
        filter_kind: HnswFilterKind::Predicate,
        matching_rows,
        total_rows,
        top_k,
        effective_ef,
        rerank_window: policy
            .effective_widths(top_k, Some(effective_ef), None)
            .rerank_window,
        vector_dimension,
        vector_encoding: policy.vector_encoding,
        exact_scan_workload: HnswExactScanWorkload::indexed_base(matching_rows),
        cost_profile: policy.distance_cost,
    };
    let strategy = if HnswDistanceCostModel::exact_work(
        search_input.exact_scan_workload,
        search_input.vector_dimension,
        search_input.cost_profile,
    ) <= HnswDistanceCostModel::graph_work_for_search(search_input)
    {
        HnswFilteredSearchStrategy::ExactScan
    } else if expected_deferred_admitted_points >= required_admitted_points {
        // Connected navigation already expects enough exact-bitmap-admitted
        // candidates to fill Top-K with 50% headroom. Predicate-local
        // refinement would repeat graph work without adding a useful frontier.
        HnswFilteredSearchStrategy::MaskedTopK
    } else {
        HnswFilteredSearchStrategy::RefinedTopK
    };

    HnswFilteredSearchDecision {
        strategy,
        expected_graph_passes,
        expected_scored_points,
        expected_deferred_admitted_points,
        required_admitted_points,
    }
}

impl Default for SearchParams {
    fn default() -> Self {
        SearchParams {
            ef: None,
            rerank_window: None,
            objective: HnswSearchObjective::CostOptimized,
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

    /// Fixed level-0 capacity of the physically merged predicate graph.
    ///
    /// Each configured column contributes at most `2 * m` predicate-local
    /// links and `m` deterministic cross-block links. Keeping this derivation
    /// on the durable contract prevents the writer and reader from inventing
    /// parallel layout formulas.
    pub fn merged_level0_stride(&self) -> paro_common::error::Result<usize> {
        self.validate()?;
        let m = usize::try_from(self.m).map_err(|_| {
            paro_common::error::data_corrupted("HNSW filter-topology m exceeds usize")
        })?;
        let column_count = usize::try_from(self.column_count).map_err(|_| {
            paro_common::error::data_corrupted("HNSW filter-topology column count exceeds usize")
        })?;
        m.checked_mul(3)
            .and_then(|value| value.checked_mul(column_count))
            .ok_or_else(|| {
                paro_common::error::data_corrupted("HNSW predicate level-0 stride exceeds usize")
            })
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
    pub vector_encoding: HnswBuildVectorEncoding,
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
    pub rerank_policy: HnswRerankPolicy,
    pub distance_cost: HnswDistanceCostProfile,
    pub vector_encoding: HnswBuildVectorEncoding,
}

impl Default for HnswSearchPolicy {
    fn default() -> Self {
        Self {
            ef_search: DEFAULT_HNSW_EF_SEARCH as usize,
            rerank_policy: HnswRerankPolicy::TopK,
            distance_cost: HnswDistanceCostProfile::default(),
            vector_encoding: HnswBuildVectorEncoding::ExactF32,
        }
    }
}

impl HnswSearchPolicy {
    pub fn effective_widths(
        self,
        top_k: usize,
        requested_ef: Option<usize>,
        requested_rerank_window: Option<usize>,
    ) -> HnswSearchWidths {
        let top_k = top_k.max(1);
        let base_ef = requested_ef.unwrap_or(self.ef_search).max(top_k);
        let rerank_window = requested_rerank_window
            .unwrap_or_else(|| self.rerank_policy.resolve(top_k, base_ef))
            .max(top_k);
        HnswSearchWidths {
            ef: base_ef.max(rerank_window),
            rerank_window,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswSearchWidths {
    pub ef: usize,
    pub rerank_window: usize,
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
            build_seed: DEFAULT_HNSW_BUILD_SEED,
        }
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
            vector_encoding: HnswBuildVectorEncoding::ExactF32,
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

    pub fn search_policy(self) -> HnswSearchPolicy {
        HnswSearchPolicy {
            ef_search: self.ef,
            rerank_policy: HnswRerankPolicy::TopK,
            distance_cost: HnswDistanceCostProfile::default(),
            vector_encoding: HnswBuildVectorEncoding::ExactF32,
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
    }

    #[test]
    fn test_hnsw_config_new() {
        let config = HnswConfig::new(8, 200);
        assert_eq!(config.m, 8);
        assert_eq!(config.m0, 16);
        assert_eq!(config.ef_construct, 200);
        assert_eq!(config.ef, 200);
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
    fn filtered_strategy_estimate_models_the_deferred_beam_before_retry() {
        let policy = HnswSearchPolicy::default();

        let exact = estimate_filtered_search_strategy(1_000, 1_000_000, 10, 160, 32, policy);
        assert_eq!(exact.strategy, HnswFilteredSearchStrategy::ExactScan);

        let refined = estimate_filtered_search_strategy(50_000, 1_000_000, 10, 160, 32, policy);
        assert_eq!(refined.expected_scored_points, 3_840);
        assert_eq!(refined.expected_deferred_admitted_points, 8);
        assert_eq!(refined.required_admitted_points, 15);
        assert_eq!(refined.expected_graph_passes, 2);
        assert_eq!(refined.strategy, HnswFilteredSearchStrategy::RefinedTopK);

        let masked = estimate_filtered_search_strategy(100_000, 1_000_000, 10, 160, 32, policy);
        assert_eq!(masked.expected_deferred_admitted_points, 16);
        assert_eq!(masked.expected_graph_passes, 1);
        assert_eq!(masked.strategy, HnswFilteredSearchStrategy::MaskedTopK);
    }

    #[test]
    fn filtered_strategy_adapts_to_topk_and_effective_ef() {
        let policy = HnswSearchPolicy::default();
        let matching_rows = 10_000;
        let total_rows = 1_000_000;
        let policy = HnswSearchPolicy { ..policy };

        assert_eq!(
            estimate_filtered_search_strategy(matching_rows, total_rows, 10, 100, 32, policy)
                .strategy,
            HnswFilteredSearchStrategy::RefinedTopK
        );
        assert_eq!(
            estimate_filtered_search_strategy(matching_rows, total_rows, 10, 160, 32, policy)
                .strategy,
            HnswFilteredSearchStrategy::RefinedTopK
        );
        assert_eq!(
            estimate_filtered_search_strategy(matching_rows, total_rows, 20, 160, 32, policy)
                .strategy,
            HnswFilteredSearchStrategy::RefinedTopK
        );
        assert_eq!(
            estimate_filtered_search_strategy(100_000, total_rows, 10, 160, 32, policy).strategy,
            HnswFilteredSearchStrategy::MaskedTopK
        );
    }

    #[test]
    fn segment_cost_compares_sequential_scoring_with_random_graph_work() {
        assert!(HnswDistanceCostModel::prefers_exact_scan(
            2_000_000,
            128,
            32,
            HnswExactScanWorkload::sequential(20_000),
            HnswDistanceCostProfile::default(),
        ));
        assert!(!HnswDistanceCostModel::prefers_exact_scan(
            169_600,
            128,
            32,
            HnswExactScanWorkload::indexed_base(120_000),
            HnswDistanceCostProfile::default(),
        ));
        assert_eq!(
            HnswDistanceCostModel::exact_work(
                HnswExactScanWorkload::indexed_base(100_000),
                32,
                HnswDistanceCostProfile::default(),
            ),
            44_800_000
        );
        assert_eq!(
            HnswDistanceCostModel::graph_work(
                10_000_000,
                160,
                10,
                32,
                HnswBuildVectorEncoding::ExactF32,
                HnswDistanceCostProfile::default(),
            ),
            1_730_624
        );
        assert!(!HnswDistanceCostModel::prefers_exact_scan(
            10_000_000,
            160,
            32,
            HnswExactScanWorkload::indexed_base(100_000),
            HnswDistanceCostProfile::default(),
        ));
        assert!(!HnswDistanceCostModel::prefers_exact_scan(
            10_000_000,
            640,
            32,
            HnswExactScanWorkload::indexed_base(1_000_000),
            HnswDistanceCostProfile::default(),
        ));
    }

    #[test]
    fn predicate_cost_charges_the_deferred_beam_retry() {
        let profile = HnswDistanceCostProfile::default();
        let selective = HnswSegmentSearchInput {
            objective: HnswSearchObjective::CostOptimized,
            filter_kind: HnswFilterKind::Predicate,
            matching_rows: 100_000,
            total_rows: 10_000_000,
            top_k: 10,
            effective_ef: 160,
            rerank_window: 10,
            vector_dimension: 32,
            vector_encoding: HnswBuildVectorEncoding::ExactF32,
            exact_scan_workload: HnswExactScanWorkload::sequential(100_000),
            cost_profile: profile,
        };
        assert_eq!(
            HnswDistanceCostModel::graph_passes(
                selective.filter_kind,
                selective.matching_rows,
                selective.total_rows,
                selective.top_k,
                selective.effective_ef,
            ),
            2
        );
        assert_eq!(
            HnswSearchStrategy::choose(selective),
            HnswSearchStrategy::ExactScan
        );

        let wide_vectors = HnswSegmentSearchInput {
            vector_dimension: 768,
            ..selective
        };
        assert_eq!(
            HnswSearchStrategy::choose(wide_vectors),
            HnswSearchStrategy::AdaptiveFilteredGraph,
            "the 32D random-access ratio must not force a 100K-row 768D scan"
        );

        let broad = HnswSegmentSearchInput {
            matching_rows: 1_000_000,
            exact_scan_workload: HnswExactScanWorkload::sequential(1_000_000),
            ..selective
        };
        assert_eq!(
            HnswDistanceCostModel::graph_passes(
                broad.filter_kind,
                broad.matching_rows,
                broad.total_rows,
                broad.top_k,
                broad.effective_ef,
            ),
            1
        );
        assert_eq!(
            HnswSearchStrategy::choose(broad),
            HnswSearchStrategy::AdaptiveFilteredGraph
        );
    }

    #[test]
    fn graph_cost_uses_the_definition_pinned_unique_scores_per_ef() {
        let profile = HnswDistanceCostProfile::default();
        assert!(!HnswDistanceCostModel::prefers_exact_scan(
            10_000_000,
            8_192,
            32,
            HnswExactScanWorkload::sequential(5_000_000),
            profile,
        ));
        assert!(HnswDistanceCostModel::prefers_exact_scan(
            10_000_000,
            8_192,
            32,
            HnswExactScanWorkload::sequential(1_000_000),
            profile,
        ));
        assert_eq!(
            HnswDistanceCostModel::graph_work(
                10_000_000,
                8_192,
                10,
                32,
                HnswBuildVectorEncoding::ExactF32,
                profile,
            ),
            88_090_688
        );
    }

    #[test]
    fn graph_cost_uses_the_artifact_routing_representation() {
        let profile = HnswDistanceCostProfile::default();
        let exact = HnswDistanceCostModel::graph_work(
            10_000_000,
            640,
            10,
            768,
            HnswBuildVectorEncoding::ExactF32,
            profile,
        );
        let compact = HnswDistanceCostModel::graph_work(
            10_000_000,
            640,
            640,
            768,
            HnswBuildVectorEncoding::symmetric_i16(128).unwrap(),
            profile,
        );
        assert!(compact < exact);
        let scored_points = 10_000_000u64.ilog2() as u64 + 640 * 24;
        let expected_compact = scored_points * (416 + 128) + 640 * (416 + 768);
        assert_eq!(compact, expected_compact);
    }

    #[test]
    fn compact_rerank_width_is_explicit_and_can_raise_the_navigation_beam() {
        let compact = HnswBuildVectorEncoding::symmetric_i16(128).unwrap();
        let policy = HnswSearchPolicy {
            ef_search: 160,
            rerank_policy: HnswRerankPolicy::Ef,
            vector_encoding: compact,
            ..HnswSearchPolicy::default()
        };
        assert_eq!(
            policy.effective_widths(10, None, None),
            HnswSearchWidths {
                ef: 160,
                rerank_window: 160,
            }
        );
        assert_eq!(
            policy.effective_widths(10, Some(80), Some(256)),
            HnswSearchWidths {
                ef: 256,
                rerank_window: 256,
            },
            "an exact rerank window cannot exceed the candidate beam that feeds it"
        );

        let profile = HnswDistanceCostProfile::default();
        let top_k_cost =
            HnswDistanceCostModel::graph_work(10_000_000, 640, 10, 768, compact, profile);
        let ef_cost =
            HnswDistanceCostModel::graph_work(10_000_000, 640, 640, 768, compact, profile);
        assert!(top_k_cost < ef_cost);
    }

    #[test]
    fn distance_cost_profile_is_explicit_and_physical_class_specific() {
        let profile = HnswDistanceCostProfile {
            source: HnswDistanceCostProfileSource::OfflineCalibration { calibration_id: 7 },
            random_access_cost_units: 32,
            exact_f32_dimension_cost_units: 1,
            sequential_dimension_cost_units: 1,
            symmetric_i16_dimension_cost_units: 1,
            graph_scored_points_per_ef: 24,
        };
        assert_eq!(
            HnswDistanceCostModel::exact_work(
                HnswExactScanWorkload::sequential(32_000),
                32,
                profile,
            ),
            1_024_000
        );
        assert_eq!(
            HnswDistanceCostModel::exact_work(
                HnswExactScanWorkload::indexed_base(32_000),
                32,
                profile,
            ),
            2_048_000
        );
        assert_eq!(
            HnswDistanceCostModel::exact_work(
                HnswExactScanWorkload {
                    sequential_rows: 24_000,
                    indexed_base_rows: 8_000,
                },
                32,
                profile,
            ),
            1_280_000
        );
        assert_eq!(HnswDistanceCostModel::exact_scan_parallelism(32_767, 8), 1);
        assert_eq!(HnswDistanceCostModel::exact_scan_parallelism(32_768, 8), 2);
        assert_eq!(HnswDistanceCostModel::exact_scan_parallelism(100_000, 8), 7);
    }

    #[test]
    fn artifact_strategy_uses_physical_scan_class_without_a_row_threshold() {
        let policy = HnswSearchPolicy::default();
        assert_eq!(
            HnswSearchStrategy::choose(HnswSegmentSearchInput {
                objective: HnswSearchObjective::CostOptimized,
                filter_kind: HnswFilterKind::Predicate,
                matching_rows: 10_000,
                total_rows: 1_000_000,
                top_k: 10,
                effective_ef: 100,
                rerank_window: 10,
                vector_dimension: 32,
                vector_encoding: policy.vector_encoding,
                exact_scan_workload: HnswExactScanWorkload::sequential(10_000),
                cost_profile: policy.distance_cost,
            }),
            HnswSearchStrategy::ExactScan
        );
        assert_eq!(
            HnswSearchStrategy::choose(HnswSegmentSearchInput {
                objective: HnswSearchObjective::CostOptimized,
                filter_kind: HnswFilterKind::Predicate,
                matching_rows: 10_000,
                total_rows: 1_000_000,
                top_k: 10,
                effective_ef: 100,
                rerank_window: 10,
                vector_dimension: 32,
                vector_encoding: policy.vector_encoding,
                exact_scan_workload: HnswExactScanWorkload::indexed_base(10_000),
                cost_profile: policy.distance_cost,
            }),
            HnswSearchStrategy::AdaptiveFilteredGraph
        );
    }

    #[test]
    fn exact_objective_bypasses_the_latency_cost_model() {
        let policy = HnswSearchPolicy::default();
        let input = HnswSegmentSearchInput {
            objective: HnswSearchObjective::Exact,
            filter_kind: HnswFilterKind::Predicate,
            matching_rows: 5_000_000,
            total_rows: 10_000_000,
            top_k: 10,
            effective_ef: 160,
            rerank_window: 10,
            vector_dimension: 32,
            vector_encoding: policy.vector_encoding,
            exact_scan_workload: HnswExactScanWorkload::indexed_base(5_000_000),
            cost_profile: policy.distance_cost,
        };

        assert_eq!(
            HnswSearchStrategy::choose(input),
            HnswSearchStrategy::ExactScan
        );
        assert_ne!(
            HnswSearchStrategy::choose(HnswSegmentSearchInput {
                objective: HnswSearchObjective::CostOptimized,
                ..input
            }),
            HnswSearchStrategy::ExactScan
        );
    }

    #[test]
    fn predicate_topology_availability_depends_on_contract_not_selectivity() {
        let rows = roaring::RoaringBitmap::from_iter([1, 2, 3]);
        let topology = HnswFilterTopologyContract::from_columns(&[4, 7], 20_000, 8).unwrap();

        assert!(HnswSearchFilter::predicate(&rows, &[7]).predicate_topology_available(&topology));
        assert!(!HnswSearchFilter::predicate(&rows, &[8]).predicate_topology_available(&topology));
        assert!(!HnswSearchFilter::Visibility(&rows).predicate_topology_available(&topology));

        let broad_rows = roaring::RoaringBitmap::from_iter(0..50);
        assert!(
            HnswSearchFilter::predicate(&broad_rows, &[7]).predicate_topology_available(&topology)
        );

        let connected_rows = roaring::RoaringBitmap::from_iter(0..4);
        assert!(HnswSearchFilter::predicate(&connected_rows, &[7])
            .predicate_topology_available(&topology));
    }
}
