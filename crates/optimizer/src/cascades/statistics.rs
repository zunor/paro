//! Snapshot-consistent estimator algebra; estimates are recipes, not group facts.

use std::collections::BTreeMap;

use paro_common::error::{self as paro_error, Result};

use super::cost::CompactRange;
use super::estimate::{Coverage, Estimate, UncertaintySet, UncertaintySetArena};
use super::ids::{ErrorFactorId, Fingerprint, StatisticsSnapshotId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatisticsFreshness {
    Fresh,
    Stale,
    Missing,
}

#[derive(Debug, Clone)]
pub struct BaseRelationStatistics {
    pub rows: Estimate,
    pub average_row_width: Estimate,
    pub freshness: StatisticsFreshness,
    pub artifact_revision: Fingerprint,
}

#[derive(Debug, Clone)]
pub struct StatisticsSnapshot {
    pub id: StatisticsSnapshotId,
    pub catalog_revision: Fingerprint,
    pub estimator_revision: Fingerprint,
    relations: BTreeMap<Fingerprint, BaseRelationStatistics>,
}

impl StatisticsSnapshot {
    pub fn new(
        id: StatisticsSnapshotId,
        catalog_revision: Fingerprint,
        estimator_revision: Fingerprint,
    ) -> Self {
        Self {
            id,
            catalog_revision,
            estimator_revision,
            relations: BTreeMap::new(),
        }
    }

    pub fn insert_relation(
        &mut self,
        relation: Fingerprint,
        statistics: BaseRelationStatistics,
    ) -> Result<()> {
        if statistics.artifact_revision == Fingerprint::default() {
            return Err(paro_error::internal(
                "statistics artifact has no version identity",
            ));
        }
        if self.relations.insert(relation, statistics).is_some() {
            return Err(paro_error::internal(
                "relation statistics duplicated within one snapshot",
            ));
        }
        Ok(())
    }

    pub fn relation(&self, relation: Fingerprint) -> Option<&BaseRelationStatistics> {
        self.relations.get(&relation)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RelationEstimate {
    pub rows: Estimate,
    pub average_row_width: Estimate,
}

pub struct EstimatorAlgebra<'a> {
    uncertainty: &'a mut UncertaintySetArena,
    next_factor: u32,
    max_factors: usize,
}

impl<'a> EstimatorAlgebra<'a> {
    pub fn new(uncertainty: &'a mut UncertaintySetArena, max_factors: usize) -> Self {
        Self {
            uncertainty,
            next_factor: 0,
            max_factors,
        }
    }

    pub fn selection(
        &mut self,
        input: RelationEstimate,
        selectivity: Estimate,
    ) -> Result<RelationEstimate> {
        let uncertainty = self.uncertainty.combine_multiplicative(
            input.rows.uncertainty,
            selectivity.uncertainty,
            self.max_factors,
        )?;
        let expected = input.rows.expected * selectivity.expected;
        let calibrated = multiply(input.rows.calibrated, selectivity.calibrated)?;
        let hard = CompactRange::new(0.0, expected, input.rows.hard.upper)?;
        Ok(RelationEstimate {
            rows: Estimate::new(
                expected,
                calibrated,
                hard,
                combine_coverage(input.rows.coverage, selectivity.coverage),
                uncertainty,
            )?,
            average_row_width: input.average_row_width,
        })
    }

    pub fn equality_join(
        &mut self,
        left: RelationEstimate,
        right: RelationEstimate,
        selectivity: Estimate,
        output_width: Estimate,
    ) -> Result<RelationEstimate> {
        let inputs = self.uncertainty.combine_multiplicative(
            left.rows.uncertainty,
            right.rows.uncertainty,
            self.max_factors,
        )?;
        let uncertainty = self.uncertainty.combine_multiplicative(
            inputs,
            selectivity.uncertainty,
            self.max_factors,
        )?;
        let expected = left.rows.expected * right.rows.expected * selectivity.expected;
        let calibrated = multiply(
            multiply(left.rows.calibrated, right.rows.calibrated)?,
            selectivity.calibrated,
        )?;
        let hard_upper = left.rows.hard.upper * right.rows.hard.upper;
        Ok(RelationEstimate {
            rows: Estimate::new(
                expected,
                calibrated,
                CompactRange::new(0.0, expected, hard_upper)?,
                combine_coverage(
                    combine_coverage(left.rows.coverage, right.rows.coverage),
                    selectivity.coverage,
                ),
                uncertainty,
            )?,
            average_row_width: output_width,
        })
    }

    pub fn grouped_aggregate(
        &mut self,
        input: RelationEstimate,
        group_ndv: Estimate,
        output_width: Estimate,
    ) -> Result<RelationEstimate> {
        let uncertainty = self.uncertainty.combine_multiplicative(
            input.rows.uncertainty,
            group_ndv.uncertainty,
            self.max_factors,
        )?;
        let expected = group_ndv.expected.min(input.rows.expected);
        let calibrated = CompactRange::new(
            group_ndv.calibrated.lower.min(input.rows.calibrated.lower),
            expected,
            group_ndv.calibrated.upper.min(input.rows.calibrated.upper),
        )?;
        let hard = CompactRange::new(0.0, expected, input.rows.hard.upper)?;
        Ok(RelationEstimate {
            rows: Estimate::new(
                expected,
                calibrated,
                hard,
                combine_coverage(input.rows.coverage, group_ndv.coverage),
                uncertainty,
            )?,
            average_row_width: output_width,
        })
    }

    pub fn scalar_aggregate(
        &mut self,
        _input: RelationEstimate,
        output_width: Estimate,
    ) -> Result<RelationEstimate> {
        let uncertainty = self.uncertainty.insert(UncertaintySet::exact());
        Ok(RelationEstimate {
            rows: Estimate::exact(1.0, uncertainty)?,
            average_row_width: output_width,
        })
    }

    pub fn limit(&mut self, input: RelationEstimate, limit: u64) -> Result<RelationEstimate> {
        let limit = limit as f64;
        let expected = input.rows.expected.min(limit);
        Ok(RelationEstimate {
            rows: Estimate::new(
                expected,
                CompactRange::new(
                    input.rows.calibrated.lower.min(limit),
                    expected,
                    input.rows.calibrated.upper.min(limit),
                )?,
                CompactRange::new(
                    input.rows.hard.lower.min(limit),
                    expected,
                    input.rows.hard.upper.min(limit),
                )?,
                input.rows.coverage,
                input.rows.uncertainty,
            )?,
            average_row_width: input.average_row_width,
        })
    }

    /// External/table functions use an explicit profile when present. Missing
    /// profiles remain visibly uncalibrated with broad, finite safety bounds.
    pub fn external_relation(
        &mut self,
        declared_rows: Option<CompactRange>,
        declared_width: Option<CompactRange>,
    ) -> Result<RelationEstimate> {
        let factor = ErrorFactorId(self.next_factor);
        self.next_factor = self.next_factor.saturating_add(1);
        let uncertainty = self.uncertainty.insert(UncertaintySet {
            factors: [(factor, 1.0)].into_iter().collect(),
            residual: CompactRange::new(-4.0, 0.0, 4.0)?,
            may_be_zero: true,
        });
        let rows = declared_rows.unwrap_or(CompactRange::new(0.0, 1_000.0, 1_000_000_000.0)?);
        let width = declared_width.unwrap_or(CompactRange::new(1.0, 64.0, 1_048_576.0)?);
        Ok(RelationEstimate {
            rows: Estimate::new(
                rows.expected,
                rows,
                CompactRange::new(0.0, rows.expected, f64::from(u32::MAX))?,
                if declared_rows.is_some() {
                    Coverage::CalibratedBps(9_000)
                } else {
                    Coverage::Uncalibrated
                },
                uncertainty,
            )?,
            average_row_width: Estimate::new(
                width.expected,
                width,
                CompactRange::new(0.0, width.expected, 16_777_216.0)?,
                if declared_width.is_some() {
                    Coverage::CalibratedBps(9_000)
                } else {
                    Coverage::Uncalibrated
                },
                uncertainty,
            )?,
        })
    }
}

fn combine_coverage(left: Coverage, right: Coverage) -> Coverage {
    match (left, right) {
        (Coverage::CalibratedBps(left), Coverage::CalibratedBps(right)) => {
            Coverage::CalibratedBps(left.min(right))
        }
        _ => Coverage::Uncalibrated,
    }
}

fn multiply(left: CompactRange, right: CompactRange) -> Result<CompactRange> {
    CompactRange::new(
        left.lower * right.lower,
        left.expected * right.expected,
        left.upper * right.upper,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exact(value: f64, uncertainty: super::super::ids::UncertaintySetId) -> Estimate {
        Estimate::exact(value, uncertainty).unwrap()
    }

    #[test]
    fn external_relation_without_profile_is_explicitly_uncalibrated() {
        let mut arena = UncertaintySetArena::default();
        let mut algebra = EstimatorAlgebra::new(&mut arena, 8);
        let estimate = algebra.external_relation(None, None).unwrap();
        assert_eq!(estimate.rows.coverage, Coverage::Uncalibrated);
        assert_eq!(estimate.rows.hard.lower, 0.0);
    }

    #[test]
    fn selection_preserves_input_hard_cardinality_upper_bound() {
        let mut arena = UncertaintySetArena::default();
        let uncertainty = arena.insert(UncertaintySet::exact());
        let input = RelationEstimate {
            rows: exact(100.0, uncertainty),
            average_row_width: exact(8.0, uncertainty),
        };
        let selectivity = Estimate::new(
            0.1,
            CompactRange::new(0.01, 0.1, 0.5).unwrap(),
            CompactRange::new(0.0, 0.1, 1.0).unwrap(),
            Coverage::CalibratedBps(9_500),
            uncertainty,
        )
        .unwrap();
        let mut algebra = EstimatorAlgebra::new(&mut arena, 8);
        let filtered = algebra.selection(input, selectivity).unwrap();
        assert_eq!(filtered.rows.expected, 10.0);
        assert_eq!(filtered.rows.hard.upper, 100.0);
    }
}
