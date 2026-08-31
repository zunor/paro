//! Fixed-size search cost used in the Memo hot path and extracted plans.

use paro_common::error::{self as paro_error, Result};

use super::identity::{ExternalWorkerRequirementSetId, ProgressSummaryId, UncertaintySummaryId};

pub const RESOURCE_DIMS: usize = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum ResourceDimension {
    Cpu = 0,
    MemoryRead = 1,
    MemoryWrite = 2,
    SequentialIo = 3,
    RandomIo = 4,
    Network = 5,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct CompactRange {
    pub lower: f64,
    pub expected: f64,
    pub upper: f64,
}

impl CompactRange {
    pub const ZERO: Self = Self {
        lower: 0.0,
        expected: 0.0,
        upper: 0.0,
    };

    pub fn new(lower: f64, expected: f64, upper: f64) -> Result<Self> {
        if !lower.is_finite()
            || !expected.is_finite()
            || !upper.is_finite()
            || lower > expected
            || expected > upper
        {
            return Err(paro_error::internal(format!(
                "invalid compact range [{lower}, {expected}, {upper}]"
            )));
        }
        Ok(Self {
            lower,
            expected,
            upper,
        })
    }

    pub fn point(value: f64) -> Result<Self> {
        Self::new(value, value, value)
    }

    pub fn contains(self, value: f64) -> bool {
        value >= self.lower && value <= self.upper
    }

    pub fn checked_add(self, other: Self) -> Result<Self> {
        Self::new(
            self.lower + other.lower,
            self.expected + other.expected,
            self.upper + other.upper,
        )
    }

    pub fn max(self, other: Self) -> Result<Self> {
        Self::new(
            self.lower.max(other.lower),
            self.expected.max(other.expected),
            self.upper.max(other.upper),
        )
    }

    pub fn intersect(self, other: Self) -> Option<Self> {
        let lower = self.lower.max(other.lower);
        let upper = self.upper.min(other.upper);
        if lower > upper {
            return None;
        }
        let expected = self.expected.clamp(lower, upper);
        Some(Self {
            lower,
            expected,
            upper,
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ScoreSummary {
    pub range: CompactRange,
    pub risk_adjusted: f64,
}

/// No heap-backed collection belongs in this structure.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SearchCost {
    pub score: ScoreSummary,
    pub resources_expected: [f64; RESOURCE_DIMS],
    pub resources_risk_upper: [f64; RESOURCE_DIMS],
    pub critical_path: CompactRange,
    pub peak_memory_upper: u64,
    pub spill_bytes_expected: u64,
    pub external_workers: ExternalWorkerRequirementSetId,
    pub external_worker_slots_upper: u16,
    pub uncertainty: UncertaintySummaryId,
    pub progress: ProgressSummaryId,
}

impl SearchCost {
    pub const ZERO: Self = Self {
        score: ScoreSummary {
            range: CompactRange::ZERO,
            risk_adjusted: 0.0,
        },
        resources_expected: [0.0; RESOURCE_DIMS],
        resources_risk_upper: [0.0; RESOURCE_DIMS],
        critical_path: CompactRange::ZERO,
        peak_memory_upper: 0,
        spill_bytes_expected: 0,
        external_workers: ExternalWorkerRequirementSetId(0),
        external_worker_slots_upper: 0,
        uncertainty: UncertaintySummaryId(0),
        progress: ProgressSummaryId(0),
    };

    pub fn validate(&self) -> Result<()> {
        let values = self
            .resources_expected
            .iter()
            .chain(self.resources_risk_upper.iter())
            .copied()
            .chain([
                self.score.range.lower,
                self.score.range.expected,
                self.score.range.upper,
                self.score.risk_adjusted,
                self.critical_path.lower,
                self.critical_path.expected,
                self.critical_path.upper,
            ]);
        if values
            .into_iter()
            .any(|value| !value.is_finite() || value < 0.0)
        {
            return Err(paro_error::internal(
                "search cost contains a negative or non-finite value",
            ));
        }
        if self.score.range.lower > self.score.range.expected
            || self.score.range.expected > self.score.range.upper
            || self.critical_path.lower > self.critical_path.expected
            || self.critical_path.expected > self.critical_path.upper
        {
            return Err(paro_error::internal("search cost interval is inverted"));
        }
        Ok(())
    }

    pub fn sequential(self, other: Self) -> Result<Self> {
        if self.external_worker_slots_upper > 0
            && other.external_worker_slots_upper > 0
            && self.external_workers != other.external_workers
        {
            return Err(paro_error::internal(
                "combining distinct external worker sets requires canonical union interning",
            ));
        }
        let mut resources_expected = [0.0; RESOURCE_DIMS];
        let mut resources_risk_upper = [0.0; RESOURCE_DIMS];
        for index in 0..RESOURCE_DIMS {
            resources_expected[index] =
                self.resources_expected[index] + other.resources_expected[index];
            resources_risk_upper[index] =
                self.resources_risk_upper[index] + other.resources_risk_upper[index];
        }
        let result = Self {
            score: ScoreSummary {
                range: self.score.range.checked_add(other.score.range)?,
                risk_adjusted: self.score.risk_adjusted + other.score.risk_adjusted,
            },
            resources_expected,
            resources_risk_upper,
            critical_path: self.critical_path.checked_add(other.critical_path)?,
            peak_memory_upper: self.peak_memory_upper.max(other.peak_memory_upper),
            spill_bytes_expected: self
                .spill_bytes_expected
                .saturating_add(other.spill_bytes_expected),
            external_workers: if self.external_worker_slots_upper > 0 {
                self.external_workers
            } else {
                other.external_workers
            },
            external_worker_slots_upper: self
                .external_worker_slots_upper
                .max(other.external_worker_slots_upper),
            uncertainty: other.uncertainty,
            progress: other.progress,
        };
        result.validate()?;
        Ok(result)
    }

    pub fn dominates(&self, other: &Self) -> bool {
        let no_worse = self.score.risk_adjusted <= other.score.risk_adjusted
            && self.score.range.upper <= other.score.range.upper
            && self.critical_path.upper <= other.critical_path.upper
            && self.peak_memory_upper <= other.peak_memory_upper
            && self.spill_bytes_expected <= other.spill_bytes_expected
            && self.external_worker_slots_upper <= other.external_worker_slots_upper
            && self
                .resources_expected
                .iter()
                .zip(other.resources_expected.iter())
                .all(|(left, right)| left <= right)
            && self
                .resources_risk_upper
                .iter()
                .zip(other.resources_risk_upper.iter())
                .all(|(left, right)| left <= right);
        let strictly_better = self.score.risk_adjusted < other.score.risk_adjusted
            || self.score.range.upper < other.score.range.upper
            || self.critical_path.upper < other.critical_path.upper
            || self.peak_memory_upper < other.peak_memory_upper
            || self.spill_bytes_expected < other.spill_bytes_expected
            || self.external_worker_slots_upper < other.external_worker_slots_upper
            || self
                .resources_expected
                .iter()
                .zip(other.resources_expected.iter())
                .any(|(left, right)| left < right)
            || self
                .resources_risk_upper
                .iter()
                .zip(other.resources_risk_upper.iter())
                .any(|(left, right)| left < right);
        no_worse && strictly_better
    }
}

const _: () = {
    assert!(std::mem::size_of::<SearchCost>() <= 256);
};

#[cfg(test)]
mod tests {
    use super::*;

    fn cost(score: f64, memory: u64) -> SearchCost {
        SearchCost {
            score: ScoreSummary {
                range: CompactRange::point(score).unwrap(),
                risk_adjusted: score,
            },
            peak_memory_upper: memory,
            ..SearchCost::ZERO
        }
    }

    #[test]
    fn sequential_cost_adds_work_but_not_disjoint_peak_memory() {
        let combined = cost(2.0, 100).sequential(cost(3.0, 40)).unwrap();
        assert_eq!(combined.score.risk_adjusted, 5.0);
        assert_eq!(combined.peak_memory_upper, 100);
    }

    #[test]
    fn pareto_dominance_requires_no_regression() {
        assert!(cost(1.0, 10).dominates(&cost(2.0, 20)));
        assert!(!cost(1.0, 30).dominates(&cost(2.0, 20)));
    }

    #[test]
    fn hot_cost_remains_small_copyable_pod() {
        assert!(std::mem::size_of::<SearchCost>() <= 256);
        let value = SearchCost::ZERO;
        let copied = value;
        assert_eq!(value, copied);
    }
}
