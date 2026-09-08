// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! ## Design
//! - ColumnStatistics combines BaseStatistics with optional DistinctStatistics
//! - DistinctStatistics is only created for supported types (not nested/boolean)
//! - Provides unified interface for column-level statistics

use std::io::{Read, Write};
use std::sync::Arc;

use paro_common::error::Result;
use paro_common::types::LogicalType;

use super::base_statistics::BaseStatistics;
use super::distinct_statistics::DistinctStatistics;

/// Column-level statistics combining base statistics with distinct statistics.
///
/// This structure provides a unified interface for managing all statistics
/// associated with a single column, including:
/// - Base statistics (min/max, null flags, type-specific stats)
/// - Distinct statistics (approximate unique count via HyperLogLog)
///
/// # Example
/// ```ignore
/// use crate::statistics::ColumnStatistics;
/// use paro_common::types::LogicalType;
///
/// // Create empty statistics for an integer column
/// let stats = ColumnStatistics::create_empty(LogicalType::Integer);
///
/// // Access base statistics
/// let base = stats.statistics();
///
/// // Check if distinct statistics are available
/// if stats.has_distinct_stats() {
///     let distinct = stats.distinct_stats().unwrap();
///     let _ = distinct.get_count();
/// }
/// ```
#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    /// Base statistics (min/max, null flags, etc.)
    stats: BaseStatistics,
    /// Optional distinct statistics (HyperLogLog-based)
    distinct_stats: Option<Arc<DistinctStatistics>>,
    /// Immutable planner-domain estimate. This is not an HLL observation and
    /// does not allocate or pretend to own a mergeable sketch. Storage never
    /// serializes this plan-local evidence.
    estimated_distinct: Option<usize>,
    /// Provenance for an explicit planner estimate. This lets a
    /// BoundReference transport an observed domain without manufacturing an
    /// HLL, while keeping ordinary estimates visibly non-observed.
    estimated_provenance: Option<DistinctProvenance>,
    /// A proof-backed upper bound on the number of values this column can
    /// contain in the current relational expression.
    ///
    /// This is deliberately separate from the observed HLL and min/max. A
    /// storage snapshot cannot prove a bound for a cached plan after later
    /// writes, while a schema constraint or a query predicate can. Storage
    /// serialization therefore never persists this plan-local fact.
    guaranteed_distinct_upper: Option<u64>,
    /// Number of rows represented by the retained sketch versus the complete
    /// row domain. This is populated by rowset/tablet aggregation when one
    /// input has no sketch. It keeps the surviving observation explicitly
    /// marked as partial; consumers may choose an uncertainty model, but the
    /// storage layer never silently extrapolates it. It is intentionally
    /// derived metadata and is not persisted in a segment's on-disk format.
    distinct_coverage: Option<DistinctCoverage>,
    /// Whether the retained HLL is a direct observation of a storage
    /// snapshot.  The planner must not infer this from the logical operator
    /// shape: a Projection, SearchScan, or CTE boundary can preserve the same
    /// observation while a bare Get can also carry a derived domain.
    storage_observation: bool,
}

/// Provenance of a distinct-count estimate.  A scalar NDV is not sufficient
/// for planning: an observed sketch, a partially covered sketch and a
/// planner-derived estimate have different safety properties.  Keep that
/// distinction at the statistics boundary so consumers cannot accidentally
/// use an extrapolated value as a proof of a complete domain.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DistinctProvenance {
    #[default]
    /// No usable distinct evidence is available.
    Unknown,
    /// The value was supplied by a planner transformation or a derived
    /// expression rather than directly observed from a complete row domain.
    Derived,
    /// A sketch covers the complete row domain represented by the statistic.
    ObservedFull,
    /// A sketch covers only a subset of the row domain.  The observed count is
    /// a lower bound; it is deliberately not linearly scaled to the full
    /// domain.
    ObservedPartial { observed_rows: u64, total_rows: u64 },
}

/// Evidence carried by a column's distinct statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DistinctEvidence {
    /// Conservative observed lower bound.
    pub lower: u64,
    /// Proof-backed upper bound, when one exists.
    pub upper: Option<u64>,
    /// Point used for cost ranking.  This is never a linear extrapolation of
    /// a sparse sketch; callers that need a proof must inspect provenance.
    pub point: u64,
    pub provenance: DistinctProvenance,
}

impl Default for DistinctEvidence {
    fn default() -> Self {
        Self {
            lower: 0,
            upper: None,
            point: 0,
            provenance: DistinctProvenance::Unknown,
        }
    }
}

impl DistinctEvidence {
    /// Normalize an evidence tuple at the statistics boundary.
    ///
    /// Statistics producers may combine an approximate point with a hard
    /// semantic upper bound.  If the sketch point overshoots that bound, the
    /// bound wins and the lower estimate is clipped as well; exposing an
    /// impossible `lower > point` tuple would make every downstream consumer
    /// invent its own (usually inconsistent) repair.
    pub fn normalized(mut self) -> Self {
        if let Some(upper) = self.upper {
            self.lower = self.lower.min(upper);
            self.point = self.point.min(upper);
        }
        self.point = self.point.max(self.lower);
        if let Some(upper) = self.upper {
            self.lower = self.lower.min(upper);
            self.point = self.point.min(upper).max(self.lower);
        }
        self
    }

    pub fn is_known(self) -> bool {
        !matches!(self.provenance, DistinctProvenance::Unknown)
    }

    pub fn is_complete_observation(self) -> bool {
        matches!(self.provenance, DistinctProvenance::ObservedFull)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DistinctCoverage {
    observed_rows: u64,
    total_rows: u64,
}

impl ColumnStatistics {
    /// Create new ColumnStatistics from BaseStatistics.
    ///
    /// If the type supports distinct statistics, a new DistinctStatistics
    /// will be automatically created.
    pub fn new(stats: BaseStatistics) -> Self {
        let distinct_stats = if DistinctStatistics::type_is_supported(stats.get_type()) {
            Some(Arc::new(DistinctStatistics::new()))
        } else {
            None
        };

        Self {
            stats,
            distinct_stats,
            estimated_distinct: None,
            estimated_provenance: None,
            guaranteed_distinct_upper: None,
            distinct_coverage: None,
            storage_observation: false,
        }
    }

    /// Create new ColumnStatistics with explicit distinct statistics.
    pub fn with_distinct(
        stats: BaseStatistics,
        distinct_stats: Option<DistinctStatistics>,
    ) -> Self {
        Self {
            stats,
            distinct_stats: distinct_stats.map(Arc::new),
            estimated_distinct: None,
            estimated_provenance: None,
            guaranteed_distinct_upper: None,
            distinct_coverage: None,
            storage_observation: false,
        }
    }

    /// Transport a resolved planner estimate without manufacturing an empty
    /// HLL, whose zero observation would erase the supplied NDV.
    pub fn with_estimated_distinct(stats: BaseStatistics, estimate: Option<usize>) -> Self {
        Self {
            stats,
            distinct_stats: None,
            estimated_distinct: estimate,
            estimated_provenance: estimate.map(|_| DistinctProvenance::Derived),
            guaranteed_distinct_upper: None,
            distinct_coverage: None,
            storage_observation: false,
        }
    }

    /// Transport an immutable planner-domain point together with the
    /// provenance that justifies it. No synthetic HLL is allocated.
    pub fn with_estimated_distinct_provenance(
        stats: BaseStatistics,
        estimate: Option<usize>,
        provenance: DistinctProvenance,
    ) -> Self {
        let mut result = Self::with_estimated_distinct(stats, estimate);
        if estimate.is_some() {
            result.estimated_provenance =
                Some(if matches!(provenance, DistinctProvenance::Unknown) {
                    // An explicit point is at least a derived estimate even when
                    // an older transport producer did not label its provenance.
                    DistinctProvenance::Derived
                } else {
                    provenance
                });
        }
        result
    }

    /// Attach a semantic or schema-derived distinct-count upper bound.
    ///
    /// Callers must not use observed table contents to create this proof.
    pub fn with_guaranteed_distinct_upper(mut self, upper: u64) -> Self {
        self.guaranteed_distinct_upper = Some(
            self.guaranteed_distinct_upper
                .map_or(upper, |current| current.min(upper)),
        );
        self
    }

    /// Mark the sketch as covering a complete row domain. Aggregators use
    /// this before combining rowsets so a later one-sided merge can retain
    /// the sketch together with an explicit coverage ratio.
    pub fn with_observation_coverage(mut self, rows: u64) -> Self {
        if self.distinct_stats.is_some() {
            self.distinct_coverage = Some(DistinctCoverage {
                observed_rows: rows,
                total_rows: rows,
            });
        }
        self
    }

    /// Mark a copied statistic as coming from a visible storage snapshot.
    /// This is transient provenance used by the optimizer and is deliberately
    /// not serialized with the sketch itself.
    pub fn with_storage_observation(mut self) -> Self {
        self.storage_observation = self.distinct_stats.is_some();
        self
    }

    /// Whether the distinct sketch is a direct storage observation rather
    /// than a derived planner statistic.
    pub fn is_storage_observation(&self) -> bool {
        self.storage_observation
            && self.distinct_stats.is_some()
            && self
                .distinct_coverage
                .is_none_or(|coverage| coverage.observed_rows >= coverage.total_rows)
    }

    /// Return distinct-count evidence without erasing coverage or provenance.
    /// Sparse rowset sketches intentionally remain a lower-bound observation;
    /// multiplying them by `total/observed` is unsound for low-cardinality
    /// columns and can poison every downstream selectivity estimate.
    pub fn distinct_evidence(&self) -> DistinctEvidence {
        if let Some(estimate) = self.estimated_distinct {
            let upper = self.guaranteed_distinct_upper;
            let point = upper
                .and_then(|bound| usize::try_from(bound).ok())
                .map_or(estimate, |bound| estimate.min(bound)) as u64;
            let provenance = self
                .estimated_provenance
                .unwrap_or(DistinctProvenance::Derived);
            return DistinctEvidence {
                lower: if matches!(
                    provenance,
                    DistinctProvenance::ObservedFull | DistinctProvenance::ObservedPartial { .. }
                ) {
                    point
                } else {
                    0
                },
                upper,
                point,
                provenance,
            }
            .normalized();
        }

        let Some(distinct) = &self.distinct_stats else {
            return DistinctEvidence {
                lower: 0,
                upper: self.guaranteed_distinct_upper,
                point: 0,
                provenance: DistinctProvenance::Unknown,
            }
            .normalized();
        };
        let observed = distinct.get_count() as u64;
        if observed == 0 {
            return DistinctEvidence {
                lower: 0,
                upper: self.guaranteed_distinct_upper,
                point: 0,
                provenance: DistinctProvenance::Unknown,
            }
            .normalized();
        }

        let (point, provenance) = match (self.storage_observation, self.distinct_coverage) {
            (true, Some(coverage))
                if coverage.observed_rows < coverage.total_rows && coverage.observed_rows > 0 =>
            {
                (
                    // A partial sketch proves only what it observed.  Keep the
                    // point conservative; a later, explicit estimator can choose
                    // to model uncertainty using the coverage values.
                    observed,
                    DistinctProvenance::ObservedPartial {
                        observed_rows: coverage.observed_rows,
                        total_rows: coverage.total_rows,
                    },
                )
            }
            (true, _) => (observed, DistinctProvenance::ObservedFull),
            // HLL state created by a planner expression is useful as a ranking
            // point, but it is not a storage-domain observation.  Do not let a
            // derived aggregate accidentally satisfy a complete-domain proof.
            (false, _) => (observed, DistinctProvenance::Derived),
        };
        let point = self
            .guaranteed_distinct_upper
            .and_then(|upper| usize::try_from(upper).ok())
            .map_or(point as usize, |upper| (point as usize).min(upper)) as u64;
        // Only a storage observation is a proof of values seen in the input
        // domain.  A planner-derived HLL is still useful as a ranking point,
        // but treating it as a lower bound would let an estimate leak into
        // domain-coverage and sizing decisions.
        let lower = if matches!(
            provenance,
            DistinctProvenance::ObservedFull | DistinctProvenance::ObservedPartial { .. }
        ) {
            observed
        } else {
            0
        };
        DistinctEvidence {
            lower,
            upper: self.guaranteed_distinct_upper,
            point,
            provenance,
        }
        .normalized()
    }

    /// Return the proof-backed distinct-count upper bound, when one exists.
    pub fn guaranteed_distinct_upper(&self) -> Option<u64> {
        self.guaranteed_distinct_upper
    }

    /// Create empty statistics for a given type.
    ///
    /// This is a convenience factory method that creates BaseStatistics::create_empty
    /// and wraps it in ColumnStatistics.
    pub fn create_empty(ty: LogicalType) -> Arc<Self> {
        Arc::new(Self::new(BaseStatistics::create_empty(ty)))
    }

    /// Create unknown statistics for a given type.
    ///
    /// This creates statistics where nothing is known about the data
    /// (has_null=true, has_no_null=true).
    pub fn create_unknown(ty: LogicalType) -> Arc<Self> {
        Arc::new(Self::new(BaseStatistics::create_unknown(ty)))
    }

    /// Merge another ColumnStatistics into this one.
    ///
    /// Both base statistics and distinct statistics are merged.
    pub fn merge(&mut self, other: &ColumnStatistics) {
        self.merge_impl(other, None, None);
    }

    /// Merge column statistics while retaining coverage information supplied
    /// by the owning rowset/tablet.  Unlike a plain `merge`, this operation can
    /// preserve a sketch when only one side has one and records the observed
    /// fraction so callers do not silently fall back to an unknown NDV.
    pub fn merge_with_coverage(
        &mut self,
        other: &ColumnStatistics,
        self_rows: u64,
        other_rows: u64,
    ) {
        self.merge_impl(other, Some(self_rows), Some(other_rows));
    }

    fn merge_impl(
        &mut self,
        other: &ColumnStatistics,
        self_rows: Option<u64>,
        other_rows: Option<u64>,
    ) {
        let estimated_provenance = match (self.estimated_distinct, other.estimated_distinct) {
            (Some(_), Some(_)) => Some(DistinctProvenance::Derived),
            (Some(_), None) => self.estimated_provenance,
            (None, Some(_)) => other.estimated_provenance,
            (None, None) => None,
        };
        let estimated_union =
            if self.estimated_distinct.is_some() || other.estimated_distinct.is_some() {
                let estimate = |column: &Self| {
                    column.estimated_distinct.or_else(|| {
                        column
                            .distinct_stats
                            .as_ref()
                            .map(|statistics| statistics.get_count())
                    })
                };
                estimate(self)
                    .zip(estimate(other))
                    .map(|(left, right)| left.saturating_add(right))
            } else {
                None
            };
        self.stats.merge(&other.stats);

        self.guaranteed_distinct_upper = self
            .guaranteed_distinct_upper
            .zip(other.guaranteed_distinct_upper)
            .map(|(left, right)| left.saturating_add(right));

        match (
            &mut self.distinct_stats,
            &other.distinct_stats,
            self_rows,
            other_rows,
        ) {
            (Some(self_distinct), Some(other_distinct), Some(left_rows), Some(right_rows)) => {
                Arc::make_mut(self_distinct).merge(other_distinct);
                self.distinct_coverage = Some(DistinctCoverage {
                    observed_rows: left_rows.saturating_add(right_rows),
                    total_rows: left_rows.saturating_add(right_rows),
                });
            }
            (Some(_), None, Some(left_rows), Some(right_rows)) => {
                self.distinct_coverage = Some(DistinctCoverage {
                    observed_rows: left_rows,
                    total_rows: left_rows.saturating_add(right_rows),
                });
            }
            (None, Some(other_distinct), Some(left_rows), Some(right_rows)) => {
                self.distinct_stats = Some(other_distinct.clone());
                self.distinct_coverage = Some(DistinctCoverage {
                    observed_rows: right_rows,
                    total_rows: left_rows.saturating_add(right_rows),
                });
            }
            (_, _, Some(_), Some(_)) => {
                self.distinct_stats = None;
                self.distinct_coverage = None;
            }
            (Some(self_distinct), Some(other_distinct), None, None) => {
                Arc::make_mut(self_distinct).merge(other_distinct);
                self.distinct_coverage = None;
            }
            _ => {
                // Without row-domain ownership, a one-sided sketch cannot be
                // interpreted as a union. Keep the conservative legacy
                // contract for direct callers and compaction paths.
                self.distinct_stats = None;
                self.distinct_coverage = None;
            }
        }
        self.estimated_distinct = estimated_union;
        self.estimated_provenance = self.estimated_distinct.and(estimated_provenance);
        // A one-sided merge with a column that has no sketch still describes
        // the observed side of the storage snapshot.  Do not erase that
        // provenance merely because the missing side cannot contribute an
        // HLL.  When both sides have sketches, however, both observations
        // must be storage-backed before the union can claim that provenance.
        let self_observed = self.storage_observation;
        let other_observed = other.storage_observation;
        self.storage_observation = match (&self.distinct_stats, &other.distinct_stats) {
            (Some(_), Some(_)) => self_observed && other_observed,
            (Some(_), None) => self_observed,
            (None, Some(_)) => other_observed,
            (None, None) => false,
        };
    }

    /// Update distinct statistics with hash values.
    ///
    /// Does nothing if distinct statistics are not available.
    ///
    /// # Arguments
    /// * `hashes` - Hash values of the data
    /// * `count` - Number of values
    pub fn update_distinct_statistics(&mut self, hashes: &[u64], count: usize) {
        if let Some(distinct) = &mut self.distinct_stats {
            Arc::make_mut(distinct).update(hashes, count);
            self.distinct_coverage = None;
        }
    }

    /// Get a reference to the base statistics.
    pub fn statistics(&self) -> &BaseStatistics {
        &self.stats
    }

    /// Get a mutable reference to the base statistics.
    pub fn statistics_mut(&mut self) -> &mut BaseStatistics {
        &mut self.stats
    }

    /// Check if distinct statistics are available.
    pub fn has_distinct_stats(&self) -> bool {
        self.distinct_stats.is_some()
    }

    /// Get a reference to the distinct statistics.
    ///
    /// Returns None if distinct statistics are not available.
    pub fn distinct_stats(&self) -> Option<&DistinctStatistics> {
        self.distinct_stats.as_deref()
    }

    /// Get a mutable reference to the distinct statistics.
    ///
    /// Returns None if distinct statistics are not available.
    pub fn distinct_stats_mut(&mut self) -> Option<&mut DistinctStatistics> {
        self.distinct_stats.as_mut().map(Arc::make_mut)
    }

    /// Set the distinct statistics.
    ///
    /// This replaces any existing distinct statistics.
    pub fn set_distinct(&mut self, distinct_stats: Option<DistinctStatistics>) {
        self.estimated_distinct = None;
        self.estimated_provenance = None;
        self.distinct_stats = distinct_stats.map(Arc::new);
        self.distinct_coverage = None;
        self.storage_observation = false;
    }

    /// Create a copy of this ColumnStatistics.
    pub fn copy(&self) -> Self {
        Self {
            stats: self.stats.copy(),
            distinct_stats: self.distinct_stats.clone(),
            estimated_distinct: self.estimated_distinct,
            estimated_provenance: self.estimated_provenance,
            guaranteed_distinct_upper: self.guaranteed_distinct_upper,
            distinct_coverage: self.distinct_coverage,
            storage_observation: self.storage_observation,
        }
    }

    /// Get the logical type of the column.
    pub fn get_type(&self) -> &LogicalType {
        self.stats.get_type()
    }

    /// Get the estimated distinct count.
    ///
    /// Reads either a sketch observation or an explicit planner estimate.
    /// Returns 0 if distinct statistics are not available.
    pub fn get_distinct_count(&self) -> usize {
        self.distinct_evidence().point as usize
    }

    /// Serialize the ColumnStatistics to a writer.
    pub fn serialize<W: Write>(&self, w: &mut W) -> Result<()> {
        // Serialize base statistics
        let stats_bytes = self.stats.to_bytes()?;
        w.write_all(&(stats_bytes.len() as u32).to_le_bytes())?;
        w.write_all(&stats_bytes)?;

        // Serialize distinct statistics presence flag
        let has_distinct = self.distinct_stats.is_some();
        w.write_all(&[has_distinct as u8])?;

        // Serialize distinct statistics if present
        if let Some(distinct) = &self.distinct_stats {
            distinct.serialize(w)?;
        }

        Ok(())
    }

    /// Deserialize a ColumnStatistics from a reader.
    pub fn deserialize<R: Read>(r: &mut R, data_type: LogicalType) -> Result<Self> {
        // Deserialize base statistics
        let mut len_buf = [0u8; 4];
        r.read_exact(&mut len_buf)?;
        let stats_len = u32::from_le_bytes(len_buf) as usize;

        let mut stats_bytes = vec![0u8; stats_len];
        r.read_exact(&mut stats_bytes)?;
        let stats = BaseStatistics::from_bytes(&stats_bytes, data_type)?;

        // Deserialize distinct statistics presence flag
        let mut has_distinct_buf = [0u8; 1];
        r.read_exact(&mut has_distinct_buf)?;
        let has_distinct = has_distinct_buf[0] != 0;

        // Deserialize distinct statistics if present
        let distinct_stats = if has_distinct {
            Some(Arc::new(DistinctStatistics::deserialize(r)?))
        } else {
            None
        };

        Ok(Self {
            stats,
            distinct_stats,
            estimated_distinct: None,
            estimated_provenance: None,
            guaranteed_distinct_upper: None,
            distinct_coverage: None,
            storage_observation: false,
        })
    }

    /// Serialize to a byte vector.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.serialize(&mut buf)?;
        Ok(buf)
    }

    /// Deserialize from a byte slice.
    pub fn from_bytes(bytes: &[u8], data_type: LogicalType) -> Result<Self> {
        let mut cursor = std::io::Cursor::new(bytes);
        Self::deserialize(&mut cursor, data_type)
    }

    /// Convert to a string representation.
    pub fn to_display_string(&self) -> String {
        let base_str = self.stats.to_display_string();
        let distinct_str = self
            .distinct_stats
            .as_ref()
            .map(|d| d.to_display_string())
            .unwrap_or_default();

        if distinct_str.is_empty() {
            base_str
        } else {
            format!("{}{}", base_str, distinct_str)
        }
    }
}

impl std::fmt::Display for ColumnStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_display_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;

    #[test]
    fn immutable_domain_ndv_is_not_an_empty_sketch_or_a_storage_observation() {
        let statistics = ColumnStatistics::with_estimated_distinct(
            BaseStatistics::create_unknown(LogicalType::Integer),
            Some(97),
        )
        .with_guaranteed_distinct_upper(80);
        assert_eq!(statistics.get_distinct_count(), 80);
        assert!(!statistics.has_distinct_stats());
        assert_eq!(statistics.copy().get_distinct_count(), 80);
        let restored =
            ColumnStatistics::from_bytes(&statistics.to_bytes().unwrap(), LogicalType::Integer)
                .unwrap();
        assert_eq!(
            restored.get_distinct_count(),
            0,
            "planning estimates are not persisted observations"
        );
        assert_eq!(restored.guaranteed_distinct_upper(), None);
    }

    #[test]
    fn immutable_domain_union_is_commutative_and_unknown_input_stays_unknown() {
        let domain = |rows| {
            ColumnStatistics::with_estimated_distinct(
                BaseStatistics::create_unknown(LogicalType::Integer),
                Some(rows),
            )
        };
        let mut left = domain(20);
        left.merge(&domain(30));
        let mut right = domain(30);
        right.merge(&domain(20));
        assert_eq!(left.get_distinct_count(), 50);
        assert_eq!(left.get_distinct_count(), right.get_distinct_count());
        assert!(!left.has_distinct_stats());
        left.merge(&ColumnStatistics::with_estimated_distinct(
            BaseStatistics::create_unknown(LogicalType::Integer),
            None,
        ));
        assert_eq!(left.get_distinct_count(), 0);
    }

    #[test]
    fn one_sided_sketch_is_retained_with_explicit_row_coverage() {
        let mut observed =
            ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Integer))
                .with_storage_observation();
        let hashes: Vec<u64> = (0..100u64).map(murmur_hash_mix).collect();
        observed.update_distinct_statistics(&hashes, hashes.len());
        let unknown = ColumnStatistics::with_distinct(
            BaseStatistics::create_unknown(LogicalType::Integer),
            None,
        );

        observed.merge_with_coverage(&unknown, 100, 900);

        assert!(observed.has_distinct_stats());
        let evidence = observed.distinct_evidence();
        assert!(matches!(
            evidence.provenance,
            DistinctProvenance::ObservedPartial {
                observed_rows: 100,
                total_rows: 1_000
            }
        ));
        assert_eq!(evidence.point, evidence.lower);
        assert!(observed.get_distinct_count() < 900);
    }

    #[test]
    fn partial_storage_observation_is_not_a_complete_domain() {
        let mut stats = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Integer));
        stats.update_distinct_statistics(&[1, 2, 3], 3);
        let unknown = ColumnStatistics::with_distinct(
            BaseStatistics::create_unknown(LogicalType::Integer),
            None,
        );
        stats.merge_with_coverage(&unknown, 3, 97);
        let storage = stats.with_storage_observation();
        assert!(!storage.is_storage_observation());
        assert!(matches!(
            storage.distinct_evidence().provenance,
            DistinctProvenance::ObservedPartial { .. }
        ));
    }

    #[test]
    fn storage_observation_provenance_is_transient_and_copyable() {
        let mut stats = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Integer));
        stats.update_distinct_statistics(&[1, 2, 3], 3);
        assert!(!stats.is_storage_observation());

        let storage = stats.clone().with_storage_observation();
        assert!(storage.is_storage_observation());
        assert!(storage.copy().is_storage_observation());
        let restored =
            ColumnStatistics::from_bytes(&storage.to_bytes().unwrap(), LogicalType::Integer)
                .unwrap();
        assert!(!restored.is_storage_observation());
    }

    #[test]
    fn derived_sketch_is_not_a_distinct_lower_bound_proof() {
        let mut stats = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Integer));
        stats.update_distinct_statistics(&[1, 2, 3, 4], 4);

        let evidence = stats.distinct_evidence();
        assert_eq!(evidence.provenance, DistinctProvenance::Derived);
        assert_eq!(evidence.lower, 0);
        assert!(evidence.point > 0);
    }

    #[test]
    fn distinct_evidence_normalizes_conflicting_point_and_upper_bound() {
        let evidence = DistinctEvidence {
            lower: 90,
            upper: Some(10),
            point: 100,
            provenance: DistinctProvenance::ObservedFull,
        }
        .normalized();
        assert_eq!(evidence.lower, 10);
        assert_eq!(evidence.point, 10);
        assert!(evidence.lower <= evidence.point);
        assert!(evidence.point <= evidence.upper.unwrap());
    }

    /// MurmurHash3 64-bit finalizer for better hash distribution in tests.
    fn murmur_hash_mix(mut h: u64) -> u64 {
        h ^= h >> 33;
        h = h.wrapping_mul(0xFF51AFD7ED558CCD);
        h ^= h >> 33;
        h = h.wrapping_mul(0xC4CEB9FE1A85EC53);
        h ^= h >> 33;
        h
    }

    #[test]
    fn test_new_integer() {
        let stats = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Integer));
        assert!(stats.has_distinct_stats());
        assert_eq!(stats.get_type(), &LogicalType::Integer);
    }

    #[test]
    fn test_new_list_no_distinct() {
        let list_type = LogicalType::List(Box::new(LogicalType::Integer));
        let stats = ColumnStatistics::new(BaseStatistics::create_empty(list_type));
        // List types don't support distinct statistics
        assert!(!stats.has_distinct_stats());
    }

    #[test]
    fn test_new_boolean_no_distinct() {
        let stats = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Boolean));
        // Boolean types don't support distinct statistics
        assert!(!stats.has_distinct_stats());
    }

    #[test]
    fn test_create_empty() {
        let stats = ColumnStatistics::create_empty(LogicalType::Varchar);
        assert!(stats.has_distinct_stats());
        assert!(!stats.statistics().can_have_null());
        assert!(!stats.statistics().can_have_no_null());
    }

    #[test]
    fn test_create_unknown() {
        let stats = ColumnStatistics::create_unknown(LogicalType::BigInt);
        assert!(stats.has_distinct_stats());
        assert!(stats.statistics().can_have_null());
        assert!(stats.statistics().can_have_no_null());
    }

    #[test]
    fn test_merge() {
        let mut stats1 = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Integer));
        let mut stats2 = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Integer));

        // Update base statistics
        stats1.statistics_mut().observe_value(&Value::Integer(10));
        stats2.statistics_mut().observe_value(&Value::Integer(20));

        // Update distinct statistics
        let hashes1: Vec<u64> = (0..100u64).map(murmur_hash_mix).collect();
        let hashes2: Vec<u64> = (100..200u64).map(murmur_hash_mix).collect();
        stats1.update_distinct_statistics(&hashes1, hashes1.len());
        stats2.update_distinct_statistics(&hashes2, hashes2.len());

        let count1 = stats1.get_distinct_count();
        let count2 = stats2.get_distinct_count();

        stats1.merge(&stats2);

        // Check base statistics merged
        assert_eq!(stats1.statistics().min_value(), Some(Value::Integer(10)));
        assert_eq!(stats1.statistics().max_value(), Some(Value::Integer(20)));

        // Check distinct statistics merged
        let merged_count = stats1.get_distinct_count();
        assert!(
            merged_count >= count1.max(count2),
            "Merged count {} should be >= max({}, {})",
            merged_count,
            count1,
            count2
        );
    }

    #[test]
    fn test_update_distinct_statistics() {
        let mut stats = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Integer));

        let hashes: Vec<u64> = (0..1000u64).map(murmur_hash_mix).collect();
        stats.update_distinct_statistics(&hashes, hashes.len());

        let count = stats.get_distinct_count();
        assert!(count > 0, "Distinct count should be positive");
    }

    #[test]
    fn test_copy() {
        let mut stats = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Integer));
        stats.statistics_mut().observe_value(&Value::Integer(42));

        let hashes: Vec<u64> = (0..100u64).map(murmur_hash_mix).collect();
        stats.update_distinct_statistics(&hashes, hashes.len());

        let copy = stats.copy();

        assert_eq!(
            stats.statistics().min_value(),
            copy.statistics().min_value()
        );
        assert_eq!(
            stats.statistics().max_value(),
            copy.statistics().max_value()
        );
        assert_eq!(stats.get_distinct_count(), copy.get_distinct_count());
    }

    #[test]
    fn copy_shares_distinct_sketch_until_mutation() {
        let mut stats = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Integer));
        stats.update_distinct_statistics(&[murmur_hash_mix(1)], 1);
        let mut copy = stats.copy();
        let original = stats.distinct_stats.as_ref().unwrap();
        let shared = copy.distinct_stats.as_ref().unwrap();
        assert!(Arc::ptr_eq(original, shared));

        copy.update_distinct_statistics(&[murmur_hash_mix(2)], 1);

        assert!(!Arc::ptr_eq(
            stats.distinct_stats.as_ref().unwrap(),
            copy.distinct_stats.as_ref().unwrap()
        ));
        assert_eq!(stats.distinct_stats().unwrap().get_total_count(), 1);
        assert_eq!(copy.distinct_stats().unwrap().get_total_count(), 2);
    }

    #[test]
    fn test_serialize_deserialize() {
        let mut stats = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Integer));
        stats.statistics_mut().observe_value(&Value::Integer(42));

        let hashes: Vec<u64> = (0..100u64).map(murmur_hash_mix).collect();
        stats.update_distinct_statistics(&hashes, hashes.len());

        let bytes = stats.to_bytes().expect("Serialization failed");
        let restored = ColumnStatistics::from_bytes(&bytes, LogicalType::Integer)
            .expect("Deserialization failed");

        assert_eq!(
            stats.statistics().min_value(),
            restored.statistics().min_value()
        );
        assert_eq!(
            stats.statistics().max_value(),
            restored.statistics().max_value()
        );
        assert_eq!(stats.has_distinct_stats(), restored.has_distinct_stats());
        assert_eq!(stats.get_distinct_count(), restored.get_distinct_count());
    }

    #[test]
    fn test_serialize_deserialize_no_distinct() {
        let list_type = LogicalType::List(Box::new(LogicalType::Integer));
        let stats = ColumnStatistics::new(BaseStatistics::create_empty(list_type.clone()));

        assert!(!stats.has_distinct_stats());

        let bytes = stats.to_bytes().expect("Serialization failed");
        let restored =
            ColumnStatistics::from_bytes(&bytes, list_type).expect("Deserialization failed");

        assert!(!restored.has_distinct_stats());
    }

    #[test]
    fn test_with_distinct() {
        let stats = BaseStatistics::create_empty(LogicalType::Integer);
        let distinct = DistinctStatistics::new();

        let col_stats = ColumnStatistics::with_distinct(stats, Some(distinct));
        assert!(col_stats.has_distinct_stats());
    }

    #[test]
    fn test_set_distinct() {
        let mut stats = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Boolean));
        assert!(!stats.has_distinct_stats());

        // Force set distinct stats even for boolean
        stats.set_distinct(Some(DistinctStatistics::new()));
        assert!(stats.has_distinct_stats());

        // Remove distinct stats
        stats.set_distinct(None);
        assert!(!stats.has_distinct_stats());
    }

    #[test]
    fn test_get_distinct_count_no_stats() {
        let list_type = LogicalType::List(Box::new(LogicalType::Integer));
        let stats = ColumnStatistics::new(BaseStatistics::create_empty(list_type));

        // Should return 0 when no distinct stats
        assert_eq!(stats.get_distinct_count(), 0);
    }

    #[test]
    fn guaranteed_upper_caps_an_observed_distinct_count_monotonically() {
        let mut stats = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Integer));
        stats.update_distinct_statistics(&[1, 2, 3], 3);

        let stats = stats
            .with_guaranteed_distinct_upper(2)
            .with_guaranteed_distinct_upper(1);

        assert_eq!(stats.guaranteed_distinct_upper(), Some(1));
        assert_eq!(stats.get_distinct_count(), 1);
    }

    #[test]
    fn test_to_string() {
        let mut stats = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Integer));
        stats.statistics_mut().observe_value(&Value::Integer(42));

        let s = stats.to_string();
        assert!(!s.is_empty());
    }

    #[test]
    fn test_display() {
        let stats = ColumnStatistics::create_empty(LogicalType::Varchar);
        let display = format!("{}", stats);
        assert!(!display.is_empty());
    }
}
