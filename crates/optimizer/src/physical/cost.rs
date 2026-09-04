// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Fixed-size search cost used in the Memo hot path and extracted plans.

use paro_common::error::{self as paro_error, Result};

use super::identity::ExternalWorkerRequirementSetId;

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
    /// Memory that cannot be reclaimed or spilled while this operator is
    /// active. This is the hard quantity that composes additively across
    /// overlapping retained-state pipelines.
    pub non_revocable_memory_upper: u64,
    /// Query-lifetime capacity floor required for the selected operator graph
    /// to make progress. It includes fixed scratch, per-task scratch at the
    /// declared maximum task concurrency, and mandatory spill buffers.
    pub minimum_memory_bytes: u64,
    /// Preferred revocable working set. Admission may shrink this value down
    /// to `minimum_memory_bytes`, but never below it.
    pub revocable_memory_target: u64,
    /// Query-local peak after applying the shared allocator/revocation
    /// protocol. Revocable working sets compose by maximum, not by addition.
    pub peak_memory_upper: u64,
    pub spill_bytes_expected: u64,
    pub external_workers: ExternalWorkerRequirementSetId,
    pub external_worker_slots_upper: u16,
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
        non_revocable_memory_upper: 0,
        minimum_memory_bytes: 0,
        revocable_memory_target: 0,
        peak_memory_upper: 0,
        spill_bytes_expected: 0,
        external_workers: ExternalWorkerRequirementSetId(0),
        external_worker_slots_upper: 0,
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
        if self.non_revocable_memory_upper > self.peak_memory_upper {
            return Err(paro_error::internal(
                "non-revocable memory exceeds the total peak memory contract",
            ));
        }
        if self.non_revocable_memory_upper > self.minimum_memory_bytes {
            return Err(paro_error::internal(
                "non-revocable memory exceeds the operational memory floor",
            ));
        }
        if self.minimum_memory_bytes > self.peak_memory_upper {
            return Err(paro_error::internal(
                "operational memory floor exceeds the total peak contract",
            ));
        }
        if self
            .minimum_memory_bytes
            .saturating_add(self.revocable_memory_target)
            > self.peak_memory_upper
        {
            return Err(paro_error::internal(
                "memory floor plus revocable target exceeds the total peak contract",
            ));
        }
        Ok(())
    }

    /// Memory at which the expected-cost estimate is valid. The peak remains
    /// a hard resident upper bound; spillable state below that bound is a
    /// preference and must not make an otherwise executable DOP inadmissible.
    pub fn preferred_memory_bytes(&self) -> u64 {
        self.minimum_memory_bytes
            .saturating_add(self.revocable_memory_target)
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
        let minimum_memory_bytes = self.minimum_memory_bytes.max(other.minimum_memory_bytes);
        let peak_memory_upper = self.peak_memory_upper.max(other.peak_memory_upper);
        let preferred_memory_bytes = self
            .preferred_memory_bytes()
            .max(other.preferred_memory_bytes())
            .max(minimum_memory_bytes);
        let result = Self {
            score: ScoreSummary {
                range: self.score.range.checked_add(other.score.range)?,
                risk_adjusted: self.score.risk_adjusted + other.score.risk_adjusted,
            },
            resources_expected,
            resources_risk_upper,
            critical_path: self.critical_path.checked_add(other.critical_path)?,
            non_revocable_memory_upper: self
                .non_revocable_memory_upper
                .max(other.non_revocable_memory_upper),
            minimum_memory_bytes,
            // Sequential phases never need each other's elastic working set.
            // Compose the expected-cost operating point independently from
            // the spill-bounded hard peak; equating the two makes the largest
            // grant class practically inadmissible under any process overhead.
            revocable_memory_target: preferred_memory_bytes.saturating_sub(minimum_memory_bytes),
            peak_memory_upper,
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
        };
        result.validate()?;
        Ok(result)
    }

    /// Scale expected and risk work for a region-owned selectivity effect.
    /// Hard resource quantities deliberately remain unchanged: fewer expected
    /// rows are not a memory, worker, or forward-progress proof.
    pub(crate) fn retain_work(
        mut self,
        expected_retained_ppm: u32,
        upper_retained_ppm: u32,
    ) -> Result<Self> {
        const SCALE: f64 = 1_000_000.0;
        if expected_retained_ppm > upper_retained_ppm || upper_retained_ppm > 1_000_000 {
            return Err(paro_error::internal(
                "sideways-filter work retention ratio is invalid",
            ));
        }
        let expected_factor = f64::from(expected_retained_ppm) / SCALE;
        let upper_factor = f64::from(upper_retained_ppm) / SCALE;
        let risk_weight = if self.score.range.upper > self.score.range.expected {
            ((self.score.risk_adjusted - self.score.range.expected)
                / (self.score.range.upper - self.score.range.expected))
                .clamp(0.0, 1.0)
        } else {
            0.5
        };
        self.score.range = CompactRange::new(
            self.score.range.lower * expected_factor,
            self.score.range.expected * expected_factor,
            self.score.range.upper * upper_factor,
        )?;
        self.score.risk_adjusted = self.score.range.expected
            + (self.score.range.upper - self.score.range.expected) * risk_weight;
        self.critical_path = CompactRange::new(
            self.critical_path.lower * expected_factor,
            self.critical_path.expected * expected_factor,
            self.critical_path.upper * upper_factor,
        )?;
        for value in &mut self.resources_expected {
            *value *= expected_factor;
        }
        for value in &mut self.resources_risk_upper {
            *value *= upper_factor;
        }
        self.spill_bytes_expected =
            ((self.spill_bytes_expected as f64) * expected_factor).ceil() as u64;
        self.validate()?;
        Ok(self)
    }

    /// Keep only quantities which describe divisible execution work. Source
    /// attribution deliberately carries no capacity or ownership proof: a
    /// runtime filter may reduce work, but it cannot make retained memory,
    /// worker slots, or an external dependency disappear.
    pub(crate) fn work_only(mut self) -> Self {
        self.non_revocable_memory_upper = 0;
        self.minimum_memory_bytes = 0;
        self.revocable_memory_target = 0;
        self.peak_memory_upper = 0;
        self.external_workers = ExternalWorkerRequirementSetId(0);
        self.external_worker_slots_upper = 0;
        self
    }

    /// Replace one attributed portion of divisible work with a revised
    /// version of the same work. Independent work remains unchanged and hard
    /// resource proofs stay attached to the complete candidate.
    ///
    /// The critical-path subtraction is valid only because source lanes and
    /// predicate application use `ParallelWorkProfile::Pipeline`: attributed
    /// work is serial within that lane. This operation must not be used for
    /// independent branches whose critical path composes by `max`.
    pub(crate) fn replace_work(self, old: Self, new: Self) -> Result<Self> {
        fn replace(total: f64, old: f64, new: f64) -> Result<f64> {
            let tolerance = total.abs().max(old.abs()).max(1.0) * 1e-10;
            if old > total + tolerance {
                return Err(paro_error::internal(
                    "attributed source work exceeds the complete candidate cost",
                ));
            }
            Ok((total - old).max(0.0) + new)
        }

        let mut result = self;
        result.score.range = CompactRange::new(
            replace(
                self.score.range.lower,
                old.score.range.lower,
                new.score.range.lower,
            )?,
            replace(
                self.score.range.expected,
                old.score.range.expected,
                new.score.range.expected,
            )?,
            replace(
                self.score.range.upper,
                old.score.range.upper,
                new.score.range.upper,
            )?,
        )?;
        result.score.risk_adjusted = replace(
            self.score.risk_adjusted,
            old.score.risk_adjusted,
            new.score.risk_adjusted,
        )?;
        result.critical_path = CompactRange::new(
            replace(
                self.critical_path.lower,
                old.critical_path.lower,
                new.critical_path.lower,
            )?,
            replace(
                self.critical_path.expected,
                old.critical_path.expected,
                new.critical_path.expected,
            )?,
            replace(
                self.critical_path.upper,
                old.critical_path.upper,
                new.critical_path.upper,
            )?,
        )?;
        for index in 0..RESOURCE_DIMS {
            result.resources_expected[index] = replace(
                self.resources_expected[index],
                old.resources_expected[index],
                new.resources_expected[index],
            )?;
            result.resources_risk_upper[index] = replace(
                self.resources_risk_upper[index],
                old.resources_risk_upper[index],
                new.resources_risk_upper[index],
            )?;
        }
        if old.spill_bytes_expected > self.spill_bytes_expected {
            return Err(paro_error::internal(
                "attributed source spill work exceeds the complete candidate cost",
            ));
        }
        result.spill_bytes_expected = self
            .spill_bytes_expected
            .saturating_sub(old.spill_bytes_expected)
            .saturating_add(new.spill_bytes_expected);
        result.validate()?;
        Ok(result)
    }

    pub fn dominates(&self, other: &Self) -> bool {
        let no_worse = self.score.risk_adjusted <= other.score.risk_adjusted
            && self.score.range.upper <= other.score.range.upper
            && self.critical_path.upper <= other.critical_path.upper
            && self.non_revocable_memory_upper <= other.non_revocable_memory_upper
            && self.minimum_memory_bytes <= other.minimum_memory_bytes
            && self.revocable_memory_target <= other.revocable_memory_target
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
            || self.non_revocable_memory_upper < other.non_revocable_memory_upper
            || self.minimum_memory_bytes < other.minimum_memory_bytes
            || self.revocable_memory_target < other.revocable_memory_target
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
mod memory_tests {
    use super::*;

    #[test]
    fn sequential_composition_preserves_preferred_memory_below_the_hard_peak() {
        let first = SearchCost {
            minimum_memory_bytes: 10,
            revocable_memory_target: 40,
            peak_memory_upper: 100,
            ..SearchCost::ZERO
        };
        let second = SearchCost {
            minimum_memory_bytes: 20,
            peak_memory_upper: 30,
            ..SearchCost::ZERO
        };

        let combined = first.sequential(second).unwrap();

        assert_eq!(combined.minimum_memory_bytes, 20);
        assert_eq!(combined.preferred_memory_bytes(), 50);
        assert_eq!(combined.peak_memory_upper, 100);
    }
}

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
