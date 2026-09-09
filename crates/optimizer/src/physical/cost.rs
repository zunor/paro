// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Fixed-size search cost used in the Memo hot path and extracted plans.

use std::cmp::Ordering;

use paro_common::error::{self as paro_error, Result};
pub use paro_common::memory::UncappedMemoryDemand;

use super::identity::ExternalWorkerRequirementSetId;

pub const RESOURCE_DIMS: usize = 6;

/// Whether the admitted memory ceiling proves completion for this plan.
///
/// `RuntimeCapped` is an explicit last-resort contract for operators whose
/// retained state cannot yet spill and whose input has no semantic row bound.
/// The query allocator still proves the resident-memory ceiling, but execution
/// may report resource exhaustion if the state reaches it. Keeping this state
/// explicit prevents an unknown estimate from either masquerading as a proof
/// or making every physical alternative disappear during planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryCompletion {
    Guaranteed,
    RuntimeCapped {
        /// Retained-state demand before grant admission imposed the resident
        /// ceiling.
        uncapped_memory_demand: UncappedMemoryDemand,
    },
}

impl MemoryCompletion {
    pub const fn runtime_capped_known(uncapped_peak_memory_upper: u64) -> Self {
        Self::RuntimeCapped {
            uncapped_memory_demand: UncappedMemoryDemand::KnownBytes(uncapped_peak_memory_upper),
        }
    }

    pub const fn runtime_capped_unbounded() -> Self {
        Self::RuntimeCapped {
            uncapped_memory_demand: UncappedMemoryDemand::Unbounded,
        }
    }

    pub const fn is_runtime_capped(self) -> bool {
        matches!(self, Self::RuntimeCapped { .. })
    }

    pub const fn uncapped_memory_demand(self) -> Option<UncappedMemoryDemand> {
        match self {
            Self::Guaranteed => None,
            Self::RuntimeCapped {
                uncapped_memory_demand,
            } => Some(uncapped_memory_demand),
        }
    }

    /// Deterministic portfolio preference. A completion proof always wins;
    /// among best-effort alternatives, a smaller known demand wins and an
    /// unbounded demand ranks last.
    pub fn preference_cmp(self, other: Self) -> Ordering {
        match (self, other) {
            (Self::Guaranteed, Self::Guaranteed) => Ordering::Equal,
            (Self::Guaranteed, Self::RuntimeCapped { .. }) => Ordering::Less,
            (Self::RuntimeCapped { .. }, Self::Guaranteed) => Ordering::Greater,
            (
                Self::RuntimeCapped {
                    uncapped_memory_demand: left,
                },
                Self::RuntimeCapped {
                    uncapped_memory_demand: right,
                },
            ) => demand_preference_cmp(left, right),
        }
    }

    pub fn no_worse_than(self, other: Self) -> bool {
        self.preference_cmp(other) != Ordering::Greater
    }

    pub fn strictly_better_than(self, other: Self) -> bool {
        self.preference_cmp(other) == Ordering::Less
    }

    /// Compose phases whose retained states do not overlap.
    pub fn sequential(self, admitted_peak: u64, other: Self, other_admitted_peak: u64) -> Self {
        if self == Self::Guaranteed && other == Self::Guaranteed {
            return Self::Guaranteed;
        }
        Self::RuntimeCapped {
            uncapped_memory_demand: demand_max(
                self.demand_or_admitted_peak(admitted_peak),
                other.demand_or_admitted_peak(other_admitted_peak),
            ),
        }
    }

    /// Compose retained states that must be resident at the same time.
    pub fn overlapping(self, admitted_peak: u64, other: Self, other_admitted_peak: u64) -> Self {
        if self == Self::Guaranteed && other == Self::Guaranteed {
            return Self::Guaranteed;
        }
        Self::RuntimeCapped {
            uncapped_memory_demand: demand_add(
                self.demand_or_admitted_peak(admitted_peak),
                other.demand_or_admitted_peak(other_admitted_peak),
            ),
        }
    }

    fn demand_or_admitted_peak(self, admitted_peak: u64) -> UncappedMemoryDemand {
        let admitted_peak = if admitted_peak == u64::MAX {
            UncappedMemoryDemand::Unbounded
        } else {
            UncappedMemoryDemand::KnownBytes(admitted_peak)
        };
        match self {
            Self::Guaranteed => admitted_peak,
            Self::RuntimeCapped {
                uncapped_memory_demand,
            } => demand_max(uncapped_memory_demand, admitted_peak),
        }
    }
}

fn demand_preference_cmp(left: UncappedMemoryDemand, right: UncappedMemoryDemand) -> Ordering {
    match (left, right) {
        (UncappedMemoryDemand::KnownBytes(left), UncappedMemoryDemand::KnownBytes(right)) => {
            left.cmp(&right)
        }
        (UncappedMemoryDemand::KnownBytes(_), UncappedMemoryDemand::Unbounded) => Ordering::Less,
        (UncappedMemoryDemand::Unbounded, UncappedMemoryDemand::KnownBytes(_)) => Ordering::Greater,
        (UncappedMemoryDemand::Unbounded, UncappedMemoryDemand::Unbounded) => Ordering::Equal,
    }
}

fn demand_max(left: UncappedMemoryDemand, right: UncappedMemoryDemand) -> UncappedMemoryDemand {
    if demand_preference_cmp(left, right) == Ordering::Less {
        right
    } else {
        left
    }
}

fn demand_add(left: UncappedMemoryDemand, right: UncappedMemoryDemand) -> UncappedMemoryDemand {
    match (left, right) {
        (UncappedMemoryDemand::KnownBytes(left), UncappedMemoryDemand::KnownBytes(right)) => left
            .checked_add(right)
            .map(UncappedMemoryDemand::KnownBytes)
            .unwrap_or(UncappedMemoryDemand::Unbounded),
        _ => UncappedMemoryDemand::Unbounded,
    }
}

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
    /// Serial work expressed in calibrated latency units. Unlike
    /// `critical_path`, this remains invariant when a phase is assigned more
    /// workers and supplies the W/P capacity lower bound.
    pub work_latency: CompactRange,
    /// Physical worker capacity used to derive this operating point.
    pub max_parallel_tasks: u16,
    /// Executable task supply carried by this plan's output pipeline. Unlike
    /// `max_parallel_tasks`, this is inherited through streaming operators and
    /// changes only at a source, exchange, or pipeline breaker.
    pub output_pipeline_tasks: u16,
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
    /// Strength of the forward-progress proof behind `peak_memory_upper`.
    pub memory_completion: MemoryCompletion,
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
        work_latency: CompactRange::ZERO,
        max_parallel_tasks: 1,
        output_pipeline_tasks: 1,
        critical_path: CompactRange::ZERO,
        non_revocable_memory_upper: 0,
        minimum_memory_bytes: 0,
        revocable_memory_target: 0,
        peak_memory_upper: 0,
        memory_completion: MemoryCompletion::Guaranteed,
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
                self.work_latency.lower,
                self.work_latency.expected,
                self.work_latency.upper,
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
            || self.work_latency.lower > self.work_latency.expected
            || self.work_latency.expected > self.work_latency.upper
            || self.critical_path.lower > self.critical_path.expected
            || self.critical_path.expected > self.critical_path.upper
        {
            return Err(paro_error::internal("search cost interval is inverted"));
        }
        if self.max_parallel_tasks == 0 || self.output_pipeline_tasks == 0 {
            return Err(paro_error::internal(
                "search cost declares zero physical worker or pipeline capacity",
            ));
        }
        if self.non_revocable_memory_upper > self.peak_memory_upper {
            return Err(paro_error::internal(
                "non-revocable memory exceeds the total peak memory contract",
            ));
        }
        if self.memory_completion == MemoryCompletion::Guaranteed
            && self.non_revocable_memory_upper > self.minimum_memory_bytes
        {
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
        if matches!(
            self.memory_completion.uncapped_memory_demand(),
            Some(UncappedMemoryDemand::KnownBytes(uncapped)) if uncapped < self.peak_memory_upper
        ) {
            return Err(paro_error::internal(
                "runtime-capped demand is below the admitted resident peak",
            ));
        }
        Ok(())
    }

    /// Memory at which the expected-cost estimate is valid. The admitted peak
    /// is a resident upper bound; a runtime-capped plan separately retains its
    /// uncapped demand because that ceiling is not a completion proof.
    pub fn preferred_memory_bytes(&self) -> u64 {
        self.minimum_memory_bytes
            .saturating_add(self.revocable_memory_target)
    }

    pub fn sequential(self, other: Self) -> Result<Self> {
        self.validate()?;
        other.validate()?;
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
            work_latency: self.work_latency.checked_add(other.work_latency)?,
            max_parallel_tasks: self.max_parallel_tasks.max(other.max_parallel_tasks),
            output_pipeline_tasks: other.output_pipeline_tasks,
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
            memory_completion: self.memory_completion.sequential(
                self.peak_memory_upper,
                other.memory_completion,
                other.peak_memory_upper,
            ),
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

    /// Apply a resident ceiling without erasing the demand that made this
    /// plan best-effort. Only an explicitly runtime-capped implementation may
    /// use this path.
    pub(crate) fn apply_runtime_cap(
        &mut self,
        memory_ceiling_bytes: u64,
        overlapping_minimum_bytes: u64,
    ) -> Result<()> {
        if overlapping_minimum_bytes > memory_ceiling_bytes {
            return Err(paro_error::internal(
                "runtime memory cap is below the overlapping execution floor",
            ));
        }
        let Some(previous_uncapped) = self.memory_completion.uncapped_memory_demand() else {
            return Err(paro_error::internal(
                "a guaranteed plan cannot be weakened by runtime grant clamping",
            ));
        };
        let uncapped_memory_demand = demand_max(
            previous_uncapped,
            self.memory_completion.demand_or_admitted_peak(
                self.peak_memory_upper.max(self.non_revocable_memory_upper),
            ),
        );
        self.memory_completion = MemoryCompletion::RuntimeCapped {
            uncapped_memory_demand,
        };
        self.non_revocable_memory_upper = self.non_revocable_memory_upper.min(memory_ceiling_bytes);
        self.peak_memory_upper = memory_ceiling_bytes.max(self.minimum_memory_bytes);
        self.revocable_memory_target = self
            .revocable_memory_target
            .min(memory_ceiling_bytes - overlapping_minimum_bytes);
        self.validate()
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
        self.work_latency = CompactRange::new(
            self.work_latency.lower * expected_factor,
            self.work_latency.expected * expected_factor,
            self.work_latency.upper * upper_factor,
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
        self.memory_completion = MemoryCompletion::Guaranteed;
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
        fn replace(kind: &str, total: f64, old: f64, new: f64) -> Result<f64> {
            let tolerance = total.abs().max(old.abs()).max(1.0) * 1e-10;
            if old > total + tolerance {
                return Err(paro_error::internal(format!(
                    "attributed {kind} source work {old} exceeds complete candidate work {total}"
                )));
            }
            Ok((total - old).max(0.0) + new)
        }

        let mut result = self;
        result.score.range = CompactRange::new(
            replace(
                "lower",
                self.score.range.lower,
                old.score.range.lower,
                new.score.range.lower,
            )?,
            replace(
                "expected",
                self.score.range.expected,
                old.score.range.expected,
                new.score.range.expected,
            )?,
            replace(
                "upper",
                self.score.range.upper,
                old.score.range.upper,
                new.score.range.upper,
            )?,
        )?;
        result.score.risk_adjusted = replace(
            "risk-adjusted",
            self.score.risk_adjusted,
            old.score.risk_adjusted,
            new.score.risk_adjusted,
        )?;
        result.critical_path = CompactRange::new(
            replace(
                "critical-path lower",
                self.critical_path.lower,
                old.critical_path.lower,
                new.critical_path.lower,
            )?,
            replace(
                "critical-path expected",
                self.critical_path.expected,
                old.critical_path.expected,
                new.critical_path.expected,
            )?,
            replace(
                "critical-path upper",
                self.critical_path.upper,
                old.critical_path.upper,
                new.critical_path.upper,
            )?,
        )?;
        result.work_latency = CompactRange::new(
            replace(
                "work-latency lower",
                self.work_latency.lower,
                old.work_latency.lower,
                new.work_latency.lower,
            )?,
            replace(
                "work-latency expected",
                self.work_latency.expected,
                old.work_latency.expected,
                new.work_latency.expected,
            )?,
            replace(
                "work-latency upper",
                self.work_latency.upper,
                old.work_latency.upper,
                new.work_latency.upper,
            )?,
        )?;
        for index in 0..RESOURCE_DIMS {
            result.resources_expected[index] = replace(
                "expected resource",
                self.resources_expected[index],
                old.resources_expected[index],
                new.resources_expected[index],
            )?;
            result.resources_risk_upper[index] = replace(
                "risk resource",
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
        self.continuation_cmp(other) == Some(Ordering::Less)
    }

    /// Partial order of the cost coordinates a physical continuation can
    /// observe. Equality belongs to this same relation: using full struct
    /// equality for ties retained unlimited candidates differing only in a
    /// lower-bound estimate, even though neither dominated the other.
    ///
    /// Lower bounds remain attached to the selected candidate as evidence;
    /// they are not ranking objectives or resource requirements. No epsilon,
    /// rounding, or projection of a ranking/feasibility axis is used here.
    /// Source response is a separate, goal-dependent contract checked by Memo.
    pub(crate) fn continuation_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.max_parallel_tasks != other.max_parallel_tasks
            || self.output_pipeline_tasks != other.output_pipeline_tasks
            || self.external_workers != other.external_workers
        {
            return None;
        }
        let mut result = Ordering::Equal;
        macro_rules! observe {
            ($comparison:expr) => {
                match $comparison? {
                    Ordering::Equal => {}
                    order if result == Ordering::Equal || result == order => result = order,
                    _ => return None,
                }
            };
        }
        macro_rules! axis {
            ($field:ident $(.$member:ident)*) => {
                observe!(self.$field$(.$member)*.partial_cmp(&other.$field$(.$member)*));
            };
        }
        axis!(score.range.expected);
        axis!(score.risk_adjusted);
        axis!(score.range.upper);
        axis!(work_latency.expected);
        axis!(work_latency.upper);
        axis!(critical_path.expected);
        axis!(critical_path.upper);
        axis!(non_revocable_memory_upper);
        axis!(minimum_memory_bytes);
        axis!(revocable_memory_target);
        axis!(peak_memory_upper);
        observe!(Some(
            self.memory_completion
                .preference_cmp(other.memory_completion)
        ));
        axis!(spill_bytes_expected);
        axis!(external_worker_slots_upper);
        for (left, right) in self
            .resources_expected
            .iter()
            .zip(&other.resources_expected)
        {
            observe!(left.partial_cmp(right));
        }
        for (left, right) in self
            .resources_risk_upper
            .iter()
            .zip(&other.resources_risk_upper)
        {
            observe!(left.partial_cmp(right));
        }
        Some(result)
    }
}

const _: () = {
    assert!(std::mem::size_of::<SearchCost>() <= 256);
};

#[cfg(test)]
mod memory_tests {
    use super::*;

    #[test]
    fn continuation_equivalence_ignores_only_non_ranking_lower_evidence() {
        let cost = SearchCost {
            score: ScoreSummary {
                range: CompactRange::new(0.0, 10.0, 100.0).unwrap(),
                risk_adjusted: 55.0,
            },
            work_latency: CompactRange::new(0.0, 20.0, 200.0).unwrap(),
            critical_path: CompactRange::new(0.0, 20.0, 200.0).unwrap(),
            ..SearchCost::ZERO
        };
        let mut other = cost;
        other.score.range.lower = 5.0;
        other.work_latency.lower = 10.0;
        other.critical_path.lower = 10.0;
        assert_ne!(cost, other);
        assert_eq!(cost.continuation_cmp(&other), Some(Ordering::Equal));
        assert!(!cost.dominates(&other));
        assert!(!other.dominates(&cost));
        for objective in [
            super::super::objective::ObjectiveProfile::Latency,
            super::super::objective::ObjectiveProfile::Throughput,
            super::super::objective::ObjectiveProfile::Memory,
            super::super::objective::ObjectiveProfile::Robustness,
        ] {
            assert_eq!(objective.compare(&cost, &other), Ordering::Equal);
            for parent in [SearchCost::ZERO, cost, other] {
                assert_eq!(
                    objective.compare(
                        &cost.sequential(parent).unwrap(),
                        &other.sequential(parent).unwrap()
                    ),
                    Ordering::Equal
                );
            }
        }
        // Capacity and external identity are observable by continuations,
        // even when all scalar objective coordinates happen to be equal.
        for distinct in [
            SearchCost {
                max_parallel_tasks: 4,
                ..cost
            },
            SearchCost {
                output_pipeline_tasks: 4,
                ..cost
            },
            SearchCost {
                external_workers: ExternalWorkerRequirementSetId(1),
                ..cost
            },
        ] {
            assert_eq!(cost.continuation_cmp(&distinct), None);
            assert_eq!(distinct.continuation_cmp(&cost), None);
        }
    }

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

    #[test]
    fn runtime_cap_preserves_the_uncapped_demand() {
        let mut cost = SearchCost {
            non_revocable_memory_upper: u64::MAX,
            minimum_memory_bytes: 10,
            peak_memory_upper: u64::MAX,
            memory_completion: MemoryCompletion::runtime_capped_unbounded(),
            ..SearchCost::ZERO
        };

        cost.apply_runtime_cap(100, 10).unwrap();

        assert_eq!(cost.peak_memory_upper, 100);
        assert_eq!(
            cost.memory_completion.uncapped_memory_demand(),
            Some(UncappedMemoryDemand::Unbounded)
        );
    }

    #[test]
    fn completion_composition_distinguishes_phases_from_overlapping_state() {
        let left = MemoryCompletion::runtime_capped_known(100);
        let right = MemoryCompletion::runtime_capped_known(70);

        assert_eq!(
            left.sequential(40, right, 30),
            MemoryCompletion::runtime_capped_known(100)
        );
        assert_eq!(
            left.overlapping(40, right, 30),
            MemoryCompletion::runtime_capped_known(170)
        );
        assert_eq!(
            left.overlapping(40, MemoryCompletion::runtime_capped_unbounded(), 30),
            MemoryCompletion::runtime_capped_unbounded()
        );
    }

    #[test]
    fn completion_preference_is_explicit_and_proof_first() {
        let bounded = MemoryCompletion::runtime_capped_known(100);
        let unbounded = MemoryCompletion::runtime_capped_unbounded();

        assert_eq!(
            MemoryCompletion::Guaranteed.preference_cmp(bounded),
            Ordering::Less
        );
        assert_eq!(bounded.preference_cmp(unbounded), Ordering::Less);
    }

    #[test]
    fn runtime_capped_peer_cannot_mask_an_invalid_guaranteed_component() {
        let invalid_guaranteed = SearchCost {
            non_revocable_memory_upper: 20,
            minimum_memory_bytes: 10,
            peak_memory_upper: 20,
            ..SearchCost::ZERO
        };
        let runtime_capped = SearchCost {
            non_revocable_memory_upper: 10,
            minimum_memory_bytes: 10,
            peak_memory_upper: 10,
            memory_completion: MemoryCompletion::runtime_capped_known(10),
            ..SearchCost::ZERO
        };

        assert!(invalid_guaranteed.sequential(runtime_capped).is_err());
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
