// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Certified, deliberately small lower-bound primitives for Cascades search.
//!
//! `CompactRange::lower` is an estimate interval endpoint. It is therefore
//! not a search proof. This module contains the opposite contract: a floor
//! may be constructed only from an exact operating-point fact and may then be
//! used for the work which the containing physical recipe must retain. The
//! engine currently uses this first for recipe-local work; child work,
//! source-filter retention, and unknown logical alternatives remain unknown
//! and consequently are not pruned by this module.

use crate::physical::ObjectiveProfile;

use super::cost::{CompactRange, MemoryCompletion, SearchCost};
use super::ids::Fingerprint;

/// A lower bound for a parent recipe after every child used by the recipe has
/// published a current completion proof.  The values are model operating
/// points, not interval endpoints: a child proof says that its complete
/// frontier has been enumerated, so taking the minimum expected work/span of
/// that frontier is conservative for every later composition.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ProvenChildLatencyFloor {
    pub(crate) work_latency: f64,
    pub(crate) critical_path: f64,
    /// The largest task capacity exposed by any candidate in the proven
    /// frontier.  A parent must retain this capacity when it composes the
    /// child; using the parent's requested capacity alone would understate
    /// the child's best possible makespan for streaming plans.
    pub(crate) max_parallel_tasks: u16,
}

impl ProvenChildLatencyFloor {
    pub(crate) fn from_frontier(
        work_latency: impl IntoIterator<Item = f64>,
        critical_path: impl IntoIterator<Item = f64>,
        max_parallel_tasks: impl IntoIterator<Item = u16>,
    ) -> Option<Self> {
        let mut work_latency = work_latency.into_iter();
        let work_latency = work_latency
            .next()
            .map(|first| work_latency.fold(first, f64::min));
        let mut critical_path = critical_path.into_iter();
        let critical_path = critical_path
            .next()
            .map(|first| critical_path.fold(first, f64::min));
        let max_parallel_tasks = max_parallel_tasks
            .into_iter()
            .fold(1, |current, next| current.max(next.max(1)));
        match (work_latency, critical_path) {
            (Some(work_latency), Some(critical_path))
                if work_latency.is_finite() && critical_path.is_finite() =>
            {
                Some(Self {
                    work_latency,
                    critical_path,
                    max_parallel_tasks,
                })
            }
            _ => None,
        }
    }
}

/// A conservative latency floor for a recipe whose source-work contract is
/// fixed and whose child floors come from complete physical subproblems.
/// Source-filtered and other context-sensitive compositions deliberately do
/// not use this type; those paths must remain unpruned until a stronger proof
/// covers their response and phase semantics.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ProvenRecipeLatencyFloor {
    pub(crate) work_latency: f64,
    pub(crate) critical_path: f64,
    pub(crate) max_parallel_tasks: u16,
    pub(crate) witness: Fingerprint,
}

impl ProvenRecipeLatencyFloor {
    pub(crate) fn from_fixed_recipe(
        local_cost: SearchCost,
        children: impl IntoIterator<Item = ProvenChildLatencyFloor>,
        max_parallel_tasks: u16,
        witness: Fingerprint,
    ) -> Option<Self> {
        local_cost.validate().ok()?;
        // The expected point is not a lower bound when the frozen model
        // carries an interval.  A recipe proof may use the local term only
        // when that term is exact; otherwise a cheaper operating point could
        // still exist inside the declared range.
        if !is_exact(local_cost.work_latency) {
            return None;
        }
        // `compose_candidate_cost` adds child work/span in dependency order
        // and handles these masks as an overlap phase.  Summing child spans
        // here would overstate the floor for an overlapping recipe and could
        // incorrectly exclude a legal plan.  Until a phase-aware proof is
        // available, an overlap bit makes this bound deliberately unknown.
        if local_cost.max_parallel_tasks == 0 {
            return None;
        }
        let mut work_latency = local_cost.work_latency.expected;
        let mut critical_path = 0.0;
        let mut child_parallel_tasks = 1;
        for child in children {
            if !child.work_latency.is_finite() || !child.critical_path.is_finite() {
                return None;
            }
            work_latency += child.work_latency;
            critical_path += child.critical_path;
            child_parallel_tasks = child_parallel_tasks.max(child.max_parallel_tasks.max(1));
        }
        (work_latency.is_finite() && critical_path.is_finite()).then_some(Self {
            work_latency,
            critical_path,
            max_parallel_tasks: max_parallel_tasks.max(child_parallel_tasks).max(1),
            witness,
        })
    }

    pub(crate) fn makespan(self) -> f64 {
        (self.work_latency / f64::from(self.max_parallel_tasks)).max(self.critical_path)
    }

    /// Strict comparison preserves equal-cost candidates for the declared
    /// tie-break contract.  The proof is useful only against a guaranteed
    /// incumbent; a runtime-capped incumbent has a separate completion axis
    /// which this work-only floor does not cover.
    pub(crate) fn proves_no_latency_improvement(self, incumbent: &SearchCost) -> bool {
        incumbent.memory_completion == MemoryCompletion::Guaranteed
            && self.makespan() > expected_makespan(incumbent)
    }
}

/// A lower bound on the serial work that every completion of one physical
/// recipe must retain.
///
/// This is intentionally not a `SearchCost`: a lower bound is not an
/// executable candidate and must not accidentally be admitted to a Memo
/// frontier. The witness is the stable recipe/cost evidence used by the
/// caller when it records diagnostics or a future `BoundProof`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct CertifiedLocalWorkFloor {
    pub(crate) work_latency: f64,
    pub(crate) witness: Fingerprint,
}

impl CertifiedLocalWorkFloor {
    /// Certify a local work floor only when the frozen cost model supplied an
    /// exact work point. In particular, this never reads `range.lower` from
    /// an uncertain interval. The caller must additionally establish that
    /// its composition cannot retain-filter this local term.
    pub(crate) fn from_exact_cost(cost: SearchCost, witness: Fingerprint) -> Option<Self> {
        cost.validate().ok()?;
        is_exact(cost.work_latency).then_some(Self {
            work_latency: cost.work_latency.expected,
            witness,
        })
    }

    /// Return the smallest possible latency contribution under a declared
    /// worker capacity. Work is divisible at best, so capacity is an upper
    /// bound and this quotient is conservative.
    pub(crate) fn makespan_floor(self, max_parallel_tasks: u16) -> f64 {
        self.work_latency / f64::from(max_parallel_tasks.max(1))
    }

    /// Strictly greater is required. Equality leaves the candidate in the
    /// search because objective tie-breaks and other axes can still matter.
    pub(crate) fn proves_no_latency_improvement(
        self,
        objective: ObjectiveProfile,
        incumbent: &SearchCost,
        max_parallel_tasks: u16,
    ) -> bool {
        // Latency is the only objective whose first ranking coordinate is a
        // makespan. A runtime-capped incumbent may still lose to a guaranteed
        // completion even at a larger makespan, so no work-only proof may
        // prune against it.
        objective == ObjectiveProfile::Latency
            && incumbent.memory_completion == MemoryCompletion::Guaranteed
            && self.makespan_floor(max_parallel_tasks) > expected_makespan(incumbent)
    }
}

pub(crate) fn expected_makespan(cost: &SearchCost) -> f64 {
    let capacity = f64::from(cost.max_parallel_tasks.max(1));
    (cost.work_latency.expected / capacity).max(cost.critical_path.expected)
}

fn is_exact(range: CompactRange) -> bool {
    range.lower.to_bits() == range.expected.to_bits()
        && range.expected.to_bits() == range.upper.to_bits()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exact(work: f64, span: f64, tasks: u16) -> SearchCost {
        SearchCost {
            work_latency: CompactRange::point(work).unwrap(),
            critical_path: CompactRange::point(span).unwrap(),
            max_parallel_tasks: tasks,
            ..SearchCost::ZERO
        }
    }

    #[test]
    fn uncertain_interval_is_not_a_certified_floor() {
        let mut cost = exact(12.0, 12.0, 1);
        cost.work_latency = CompactRange::new(1.0, 12.0, 24.0).unwrap();
        assert!(CertifiedLocalWorkFloor::from_exact_cost(cost, Fingerprint(1)).is_none());
    }

    #[test]
    fn exact_work_floor_uses_declared_capacity_not_an_estimate_endpoint() {
        let floor =
            CertifiedLocalWorkFloor::from_exact_cost(exact(40.0, 40.0, 1), Fingerprint(2)).unwrap();
        assert_eq!(floor.work_latency, 40.0);
        assert_eq!(floor.makespan_floor(4), 10.0);
    }

    #[test]
    fn strict_bound_keeps_equal_latency_candidates() {
        let floor =
            CertifiedLocalWorkFloor::from_exact_cost(exact(40.0, 40.0, 1), Fingerprint(3)).unwrap();
        let incumbent = exact(10.0, 10.0, 4);
        assert!(!floor.proves_no_latency_improvement(ObjectiveProfile::Latency, &incumbent, 4,));
    }

    #[test]
    fn runtime_capped_incumbent_disables_work_only_pruning() {
        let floor =
            CertifiedLocalWorkFloor::from_exact_cost(exact(80.0, 80.0, 1), Fingerprint(4)).unwrap();
        let mut incumbent = exact(1.0, 1.0, 1);
        incumbent.memory_completion = MemoryCompletion::runtime_capped_unbounded();
        incumbent.peak_memory_upper = 1;
        incumbent.validate().unwrap();
        assert!(!floor.proves_no_latency_improvement(ObjectiveProfile::Latency, &incumbent, 1,));
    }

    #[test]
    fn proven_child_frontiers_form_a_conservative_recipe_floor() {
        let local = exact(5.0, 1.0, 4);
        let floor = ProvenRecipeLatencyFloor::from_fixed_recipe(
            local,
            [
                ProvenChildLatencyFloor {
                    work_latency: 10.0,
                    critical_path: 3.0,
                    max_parallel_tasks: 2,
                },
                ProvenChildLatencyFloor {
                    work_latency: 20.0,
                    critical_path: 4.0,
                    max_parallel_tasks: 4,
                },
            ],
            4,
            Fingerprint(5),
        )
        .unwrap();
        assert_eq!(floor.work_latency, 35.0);
        assert_eq!(floor.critical_path, 7.0);
        assert_eq!(floor.makespan(), 8.75);
        assert!(floor.proves_no_latency_improvement(&exact(8.0, 8.0, 4)));
        assert!(!floor.proves_no_latency_improvement(&exact(8.75, 8.75, 4)));
    }

    #[test]
    fn uncertain_local_work_is_not_a_recipe_floor() {
        let mut local = exact(5.0, 1.0, 1);
        local.work_latency = CompactRange::new(1.0, 5.0, 9.0).unwrap();
        assert!(
            ProvenRecipeLatencyFloor::from_fixed_recipe(local, [], 1, Fingerprint(6),).is_none()
        );
    }
}
