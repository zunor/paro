//! Calibrated estimates and bounded correlation-aware uncertainty propagation.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use paro_common::error::{self as paro_error, Result};

use super::cost::CompactRange;
use super::ids::{ErrorFactorId, UncertaintySetId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Coverage {
    CalibratedBps(u16),
    Uncalibrated,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Estimate {
    pub expected: f64,
    pub calibrated: CompactRange,
    pub hard: CompactRange,
    pub coverage: Coverage,
    pub uncertainty: UncertaintySetId,
}

impl Estimate {
    pub fn new(
        expected: f64,
        calibrated: CompactRange,
        hard: CompactRange,
        coverage: Coverage,
        uncertainty: UncertaintySetId,
    ) -> Result<Self> {
        let calibrated = calibrated.intersect(hard).ok_or_else(|| {
            paro_error::internal("calibrated estimate does not intersect its hard bounds")
        })?;
        if !calibrated.contains(expected) {
            return Err(paro_error::internal(
                "estimate expected value must lie inside calibrated and hard bounds",
            ));
        }
        Ok(Self {
            expected,
            calibrated,
            hard,
            coverage,
            uncertainty,
        })
    }

    pub fn exact(value: f64, uncertainty: UncertaintySetId) -> Result<Self> {
        let range = CompactRange::point(value)?;
        Self::new(
            value,
            range,
            range,
            Coverage::CalibratedBps(10_000),
            uncertainty,
        )
    }
}

/// Sparse affine form over log-error for the positive conditional magnitude.
/// `may_be_zero` carries the zero mass separately; no epsilon is introduced.
#[derive(Debug, Clone, PartialEq)]
pub struct UncertaintySet {
    pub factors: BTreeMap<ErrorFactorId, f64>,
    pub residual: CompactRange,
    pub may_be_zero: bool,
}

impl UncertaintySet {
    pub fn exact() -> Self {
        Self {
            factors: BTreeMap::new(),
            residual: CompactRange::ZERO,
            may_be_zero: false,
        }
    }

    pub fn combine_multiplicative(&self, other: &Self, max_factors: usize) -> Result<Self> {
        let mut factors = self.factors.clone();
        for (&factor, &coefficient) in &other.factors {
            *factors.entry(factor).or_insert(0.0) += coefficient;
        }

        let mut residual = self.residual.checked_add(other.residual)?;
        if factors.len() > max_factors {
            let mut ranked = factors
                .iter()
                .map(|(&factor, &coefficient)| (factor, coefficient))
                .collect::<Vec<_>>();
            ranked.sort_by(|(left_id, left), (right_id, right)| {
                right
                    .abs()
                    .partial_cmp(&left.abs())
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| left_id.cmp(right_id))
            });
            let retained = ranked
                .iter()
                .take(max_factors)
                .map(|(factor, coefficient)| (*factor, *coefficient))
                .collect::<BTreeMap<_, _>>();
            let tail = ranked
                .iter()
                .skip(max_factors)
                .map(|(_, coefficient)| coefficient.abs())
                .sum::<f64>();
            residual = residual.checked_add(CompactRange::new(-tail, 0.0, tail)?)?;
            factors = retained;
        }

        Ok(Self {
            factors,
            residual,
            may_be_zero: self.may_be_zero || other.may_be_zero,
        })
    }
}

#[derive(Debug, Default)]
pub struct UncertaintySetArena {
    sets: Vec<UncertaintySet>,
}

impl UncertaintySetArena {
    pub fn insert(&mut self, set: UncertaintySet) -> UncertaintySetId {
        let id = UncertaintySetId::new(self.sets.len());
        self.sets.push(set);
        id
    }

    pub fn get(&self, id: UncertaintySetId) -> Option<&UncertaintySet> {
        self.sets.get(id.index())
    }

    pub fn combine_multiplicative(
        &mut self,
        left: UncertaintySetId,
        right: UncertaintySetId,
        max_factors: usize,
    ) -> Result<UncertaintySetId> {
        let left = self
            .get(left)
            .ok_or_else(|| paro_error::internal("unknown left uncertainty set"))?;
        let right = self
            .get(right)
            .ok_or_else(|| paro_error::internal("unknown right uncertainty set"))?;
        let combined = left.combine_multiplicative(right, max_factors)?;
        Ok(self.insert(combined))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hard_bounds_clamp_calibrated_interval() {
        let estimate = Estimate::new(
            10.0,
            CompactRange::new(0.0, 10.0, 100.0).unwrap(),
            CompactRange::new(5.0, 10.0, 20.0).unwrap(),
            Coverage::Uncalibrated,
            UncertaintySetId(0),
        )
        .unwrap();
        assert_eq!(estimate.calibrated.lower, 5.0);
        assert_eq!(estimate.calibrated.upper, 20.0);
    }

    #[test]
    fn shared_factors_are_not_duplicated_as_independent_errors() {
        let factor = ErrorFactorId(7);
        let left = UncertaintySet {
            factors: [(factor, 0.3)].into_iter().collect(),
            residual: CompactRange::ZERO,
            may_be_zero: false,
        };
        let right = UncertaintySet {
            factors: [(factor, -0.1)].into_iter().collect(),
            residual: CompactRange::ZERO,
            may_be_zero: true,
        };
        let combined = left.combine_multiplicative(&right, 8).unwrap();
        assert_eq!(combined.factors.len(), 1);
        assert!((combined.factors[&factor] - 0.2).abs() < 1e-12);
        assert!(combined.may_be_zero);
    }

    #[test]
    fn factor_cap_merges_tail_into_residual() {
        let set = UncertaintySet {
            factors: [
                (ErrorFactorId(1), 0.5),
                (ErrorFactorId(2), 0.25),
                (ErrorFactorId(3), 0.1),
            ]
            .into_iter()
            .collect(),
            residual: CompactRange::ZERO,
            may_be_zero: false,
        };
        let combined = set
            .combine_multiplicative(&UncertaintySet::exact(), 2)
            .unwrap();
        assert_eq!(combined.factors.len(), 2);
        assert_eq!(combined.residual.lower, -0.1);
        assert_eq!(combined.residual.upper, 0.1);
    }
}
