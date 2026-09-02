// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable resource-grant plan portfolios and deterministic admission.

use std::collections::{BTreeMap, BTreeSet};

use paro_common::error::{self as paro_error, Result};
use tracing::debug;

use crate::physical::cost::SearchCost;
use crate::physical::identity::{Fingerprint, ResourceGrantClassId};
use crate::physical::{PhysicalPlan, PhysicalPlanVerifier};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SpillPolicy {
    Forbidden,
    Allowed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResourceGrantClass {
    pub id: ResourceGrantClassId,
    pub hard_memory_bytes: u64,
    pub spill_policy: SpillPolicy,
    /// Maximum query-local pipeline tasks admitted for this operating point.
    /// This is an executable DOP contract, not a cost-model label.
    pub max_parallel_tasks: u16,
}

#[derive(Debug, Clone)]
pub struct PortfolioVariant<P> {
    pub admissible_classes: BTreeSet<ResourceGrantClassId>,
    pub plan: P,
    pub physical_fingerprint: Fingerprint,
    pub cost: SearchCost,
}

#[derive(Debug, Clone)]
pub struct PhysicalPlanPortfolio<P = PhysicalPlan> {
    pub grant_classes: Box<[ResourceGrantClass]>,
    pub variants: Box<[PortfolioVariant<P>]>,
}

/// Immutable resource operating point selected by portfolio admission.
///
/// This is deliberately a contract, not a lease. Execution must atomically
/// materialize every field into a lifetime-owned `ExecutionLease` before the
/// physical plan may run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionResourceContract {
    pub class: ResourceGrantClassId,
    pub minimum_memory_bytes: u64,
    pub working_set_memory_bytes: u64,
    pub memory_ceiling_bytes: u64,
    pub max_parallel_tasks: u16,
    pub external_worker_slots: u16,
}

#[derive(Debug)]
pub struct AdmittedPlan<P> {
    pub plan: P,
    pub physical_fingerprint: Fingerprint,
    pub resources: ExecutionResourceContract,
}

impl<P> PhysicalPlanPortfolio<P> {
    pub fn build(
        grant_classes: impl IntoIterator<Item = ResourceGrantClass>,
        class_plans: impl IntoIterator<Item = (ResourceGrantClassId, P, Fingerprint, SearchCost)>,
    ) -> Result<Self> {
        let classes = grant_classes
            .into_iter()
            .map(|class| (class.id, class))
            .collect::<BTreeMap<_, _>>();
        if classes.is_empty() {
            return Err(paro_error::internal(
                "physical portfolio must declare at least one grant class",
            ));
        }
        let mut merged: Vec<PortfolioVariant<P>> = Vec::new();
        for (class, plan, fingerprint, cost) in class_plans {
            cost.validate()?;
            let grant = classes.get(&class).ok_or_else(|| {
                paro_error::internal("physical variant references an undeclared grant class")
            })?;
            if cost.peak_memory_upper > grant.hard_memory_bytes {
                return Err(paro_error::internal(format!(
                    "physical variant exceeds its grant class memory bound: class={class:?}, peak_memory_upper={}, hard_memory_bytes={}, fingerprint={fingerprint:?}",
                    cost.peak_memory_upper, grant.hard_memory_bytes,
                )));
            }
            if cost.spill_bytes_expected > 0 && grant.spill_policy == SpillPolicy::Forbidden {
                return Err(paro_error::internal(
                    "spilling physical variant is assigned to a no-spill grant class",
                ));
            }
            if let Some(existing) = merged.iter_mut().find(|existing| {
                existing.physical_fingerprint == fingerprint && existing.cost == cost
            }) {
                existing.admissible_classes.insert(class);
            } else {
                merged.push(PortfolioVariant {
                    admissible_classes: [class].into_iter().collect(),
                    plan,
                    physical_fingerprint: fingerprint,
                    cost,
                });
            }
        }
        if merged.is_empty() {
            return Err(paro_error::internal(
                "physical portfolio must contain at least one grant variant",
            ));
        }
        merged.sort_by(|left, right| {
            left.physical_fingerprint
                .cmp(&right.physical_fingerprint)
                .then_with(|| {
                    left.cost
                        .score
                        .risk_adjusted
                        .total_cmp(&right.cost.score.risk_adjusted)
                })
        });
        let all = merged;
        let retained = all
            .iter()
            .enumerate()
            .filter(|(index, variant)| {
                !all.iter().enumerate().any(|(other_index, other)| {
                    *index != other_index
                        // Distinct physical fingerprints may carry distinct
                        // dynamic capabilities. A cheaper specialized plan
                        // cannot erase the baseline that admission needs when
                        // that capability disappears after compilation.
                        && variant.physical_fingerprint == other.physical_fingerprint
                        && variant
                            .admissible_classes
                            .is_subset(&other.admissible_classes)
                        && other.cost.dominates(&variant.cost)
                })
            })
            .map(|(index, _)| index)
            .collect::<BTreeSet<_>>();
        Ok(Self {
            grant_classes: classes.into_values().collect::<Vec<_>>().into_boxed_slice(),
            variants: all
                .into_iter()
                .enumerate()
                .filter_map(|(index, variant)| retained.contains(&index).then_some(variant))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    /// Select only among optimizer-proved variants. Admission never changes
    /// algorithms or weakens the result guarantee.
    pub fn admit<F>(
        &self,
        available_memory_bytes: u64,
        available_parallel_tasks: u16,
        available_external_worker_slots: u16,
        dependency_available: F,
    ) -> Result<AdmittedPlan<P>>
    where
        P: Clone,
        F: Fn(&P) -> bool,
    {
        let classes = self
            .grant_classes
            .iter()
            .map(|class| (class.id, *class))
            .collect::<BTreeMap<_, _>>();
        let variants = &self.variants;
        let selected = variants
            .iter()
            .enumerate()
            .filter_map(|(index, variant)| {
                if !dependency_available(&variant.plan)
                    || variant.cost.minimum_memory_bytes > available_memory_bytes
                    || variant.cost.external_worker_slots_upper > available_external_worker_slots
                {
                    return None;
                }
                let class = variant
                    .admissible_classes
                    .iter()
                    .filter_map(|id| classes.get(id))
                    .filter(|class| {
                        // A class is one measured/planned operating point, not
                        // merely an upper label. Never reuse its optimistic
                        // cost below its explicit preferred working set. The
                        // hard peak is a spill-bounded resident upper, not a
                        // reservation that must be empty before admission.
                        (class.hard_memory_bytes <= available_memory_bytes
                            || variant.cost.preferred_memory_bytes() <= available_memory_bytes)
                            && class.max_parallel_tasks <= available_parallel_tasks.max(1)
                            && variant.cost.peak_memory_upper <= class.hard_memory_bytes
                            && (variant.cost.spill_bytes_expected == 0
                                || class.spill_policy == SpillPolicy::Allowed)
                    })
                    .min_by_key(|class| (class.hard_memory_bytes, class.id))?;
                debug!(
                    variant_index = index,
                    class = ?class.id,
                    dop = class.max_parallel_tasks,
                    risk_adjusted_cost = variant.cost.score.risk_adjusted,
                    critical_path = variant.cost.critical_path.expected,
                    minimum_memory_bytes = variant.cost.minimum_memory_bytes,
                    peak_memory_upper = variant.cost.peak_memory_upper,
                    fingerprint = ?variant.physical_fingerprint,
                    "physical portfolio candidate is admissible"
                );
                Some((index, *class))
            })
            .min_by(|(left_index, left_class), (right_index, right_class)| {
                let left = &variants[*left_index];
                let right = &variants[*right_index];
                left.cost
                    .score
                    .risk_adjusted
                    .total_cmp(&right.cost.score.risk_adjusted)
                    .then_with(|| {
                        left.cost
                            .critical_path
                            .expected
                            .total_cmp(&right.cost.critical_path.expected)
                    })
                    // Equal-work variants are distinct resource operating
                    // points. For the latency objective, consume the greatest
                    // admitted DOP before falling back to a plan-identity tie
                    // break; otherwise a fingerprint can silently cap a
                    // query below the parallelism the caller reserved.
                    .then_with(|| {
                        right_class
                            .max_parallel_tasks
                            .cmp(&left_class.max_parallel_tasks)
                    })
                    .then_with(|| left.physical_fingerprint.cmp(&right.physical_fingerprint))
                    .then_with(|| {
                        left_class
                            .hard_memory_bytes
                            .cmp(&right_class.hard_memory_bytes)
                    })
                    .then_with(|| left_class.id.cmp(&right_class.id))
            })
            .ok_or_else(|| {
                paro_error::internal("no physical portfolio variant is currently admissible")
            })?;
        let (selected_index, selected_class) = selected;
        let selected = variants
            .get(selected_index)
            .expect("selected portfolio index must remain valid");
        Ok(AdmittedPlan {
            resources: ExecutionResourceContract {
                class: selected_class.id,
                minimum_memory_bytes: selected.cost.minimum_memory_bytes,
                working_set_memory_bytes: selected.cost.preferred_memory_bytes(),
                memory_ceiling_bytes: selected_class.hard_memory_bytes,
                max_parallel_tasks: selected_class.max_parallel_tasks,
                external_worker_slots: selected.cost.external_worker_slots_upper,
            },
            physical_fingerprint: selected.physical_fingerprint,
            plan: selected.plan.clone(),
        })
    }
}

impl PhysicalPlanPortfolio<PhysicalPlan> {
    pub fn verify_result_types(&self, expected: &[paro_common::types::LogicalType]) -> Result<()> {
        // Statements without a client-visible row schema may still expose an
        // internal completion row to the execution protocol (for example a
        // mutation count). RETURNING and ordinary queries have a non-empty
        // expected schema and remain subject to exact root verification.
        if expected.is_empty() {
            return Ok(());
        }
        for variant in self.variants.iter() {
            let actual = &variant.plan.node(variant.plan.root).output.types;
            if actual.as_ref() != expected {
                return Err(paro_error::internal(format!(
                    "physical root violates the compiled result presentation: expected={expected:?}, actual={actual:?}, fingerprint={:?}",
                    variant.physical_fingerprint
                )));
            }
        }
        Ok(())
    }

    pub fn verify(&self) -> Result<()> {
        let classes = self
            .grant_classes
            .iter()
            .map(|class| (class.id, *class))
            .collect::<BTreeMap<_, _>>();
        if classes.len() != self.grant_classes.len() || classes.is_empty() {
            return Err(paro_error::internal(
                "physical portfolio grant classes are empty or duplicated",
            ));
        }
        if self.variants.is_empty() {
            return Err(paro_error::internal("physical portfolio is empty"));
        }
        for variant in &self.variants {
            if variant.admissible_classes.is_empty() {
                return Err(paro_error::internal(
                    "physical portfolio variant has no admissible grant class",
                ));
            }
            for class in &variant.admissible_classes {
                let grant = classes.get(class).ok_or_else(|| {
                    paro_error::internal(
                        "physical portfolio variant references an undeclared grant class",
                    )
                })?;
                if variant.cost.peak_memory_upper > grant.hard_memory_bytes {
                    return Err(paro_error::internal(
                        "physical portfolio variant exceeds an advertised grant class",
                    ));
                }
                if variant.cost.spill_bytes_expected > 0
                    && grant.spill_policy == SpillPolicy::Forbidden
                {
                    return Err(paro_error::internal(
                        "spilling variant advertises a no-spill grant class",
                    ));
                }
                if grant.spill_policy == SpillPolicy::Forbidden
                    && variant.plan.nodes.iter().any(|node| match &node.kind {
                        crate::physical::PhysicalNodeKind::Aggregate(spec) => {
                            spec.spill_policy != crate::physical::SpillExecutionPolicy::InMemory
                        }
                        crate::physical::PhysicalNodeKind::HashJoin(spec) => {
                            spec.spill_policy != crate::physical::SpillExecutionPolicy::InMemory
                        }
                        crate::physical::PhysicalNodeKind::Sort(spec) => {
                            spec.spill_policy != crate::physical::SpillExecutionPolicy::InMemory
                        }
                        _ => false,
                    })
                {
                    return Err(paro_error::internal(
                        "spill-capable physical operator advertises a no-spill grant class",
                    ));
                }
            }
            variant.cost.validate()?;
            PhysicalPlanVerifier::verify(&variant.plan)?;
            if variant.plan.execution_resources.is_some() {
                return Err(paro_error::internal(
                    "portfolio contains a plan with pre-bound execution resources",
                ));
            }
            let root_properties = variant
                .plan
                .properties
                .get(variant.plan.root)
                .ok_or_else(|| paro_error::internal("portfolio root has no property contract"))?;
            if root_properties.cumulative_cost != variant.cost {
                return Err(paro_error::internal(
                    "portfolio cost disagrees with the verified root winner cost",
                ));
            }
            if let crate::physical::PhysicalGrantContract::Class(required) =
                root_properties.grant_contract
            {
                if !variant.admissible_classes.contains(&required) {
                    return Err(paro_error::internal(
                        "class-specific plan is not advertised for its optimization class",
                    ));
                }
            }
        }
        let candidate_space = &self.variants[0].plan.dependencies;
        if self.variants[1..].iter().any(|variant| {
            let dependencies = &variant.plan.dependencies;
            dependencies.machine_calibration_revision
                != candidate_space.machine_calibration_revision
                || dependencies.estimator_revision != candidate_space.estimator_revision
                || dependencies.rule_set_revision != candidate_space.rule_set_revision
                || dependencies.plan_stability_policy_revision
                    != candidate_space.plan_stability_policy_revision
                || dependencies.optimizer_config_fingerprint
                    != candidate_space.optimizer_config_fingerprint
                || dependencies.physical_abi_revision != candidate_space.physical_abi_revision
                || dependencies.quality_policy_revision != candidate_space.quality_policy_revision
        }) {
            return Err(paro_error::internal(
                "physical portfolio variants belong to different static candidate spaces",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::cost::{CompactRange, ScoreSummary};

    fn cost(score: f64, memory: u64) -> SearchCost {
        SearchCost {
            score: ScoreSummary {
                range: CompactRange::point(score).unwrap(),
                risk_adjusted: score,
            },
            critical_path: CompactRange::point(score).unwrap(),
            peak_memory_upper: memory,
            minimum_memory_bytes: memory,
            ..SearchCost::ZERO
        }
    }

    #[test]
    fn identical_plan_is_shared_across_grant_classes() {
        let classes = [
            ResourceGrantClass {
                id: ResourceGrantClassId(1),
                hard_memory_bytes: 10,
                spill_policy: SpillPolicy::Allowed,
                max_parallel_tasks: 1,
            },
            ResourceGrantClass {
                id: ResourceGrantClassId(2),
                hard_memory_bytes: 20,
                spill_policy: SpillPolicy::Allowed,
                max_parallel_tasks: 1,
            },
        ];
        let portfolio = PhysicalPlanPortfolio::build(
            classes,
            [
                (ResourceGrantClassId(1), "p", Fingerprint(7), cost(2.0, 10)),
                (ResourceGrantClassId(2), "p", Fingerprint(7), cost(2.0, 10)),
            ],
        )
        .unwrap();
        assert_eq!(portfolio.variants.len(), 1);
        assert_eq!(portfolio.variants[0].admissible_classes.len(), 2);
    }

    #[test]
    fn admission_is_deterministic_and_respects_hard_resources() {
        let portfolio = PhysicalPlanPortfolio::build(
            [
                ResourceGrantClass {
                    id: ResourceGrantClassId(1),
                    hard_memory_bytes: 100,
                    spill_policy: SpillPolicy::Allowed,
                    max_parallel_tasks: 1,
                },
                ResourceGrantClass {
                    id: ResourceGrantClassId(2),
                    hard_memory_bytes: 20,
                    spill_policy: SpillPolicy::Allowed,
                    max_parallel_tasks: 1,
                },
            ],
            [
                (
                    ResourceGrantClassId(1),
                    "fast",
                    Fingerprint(1),
                    cost(1.0, 100),
                ),
                (
                    ResourceGrantClassId(2),
                    "small",
                    Fingerprint(2),
                    cost(2.0, 10),
                ),
            ],
        )
        .unwrap();
        let admitted = portfolio.admit(20, 1, 0, |_| true).unwrap();
        assert_eq!(admitted.plan, "small");
        assert_eq!(admitted.resources.minimum_memory_bytes, 10);
        assert_eq!(admitted.resources.working_set_memory_bytes, 10);
        assert_eq!(admitted.resources.memory_ceiling_bytes, 20);
    }

    #[test]
    fn admission_respects_the_costed_preferred_memory_operating_point() {
        let large = ResourceGrantClass {
            id: ResourceGrantClassId(1),
            hard_memory_bytes: 100,
            spill_policy: SpillPolicy::Allowed,
            max_parallel_tasks: 1,
        };
        let small = ResourceGrantClass {
            id: ResourceGrantClassId(2),
            hard_memory_bytes: 10,
            spill_policy: SpillPolicy::Allowed,
            max_parallel_tasks: 1,
        };
        let mut large_cost = cost(1.0, 100);
        large_cost.minimum_memory_bytes = 1;
        large_cost.revocable_memory_target = 99;
        let portfolio = PhysicalPlanPortfolio::build(
            [large, small],
            [
                (large.id, "large-fast", Fingerprint(1), large_cost),
                (small.id, "small-slower", Fingerprint(2), cost(2.0, 10)),
            ],
        )
        .unwrap();

        assert_eq!(
            portfolio.admit(10, 1, 0, |_| true).unwrap().plan,
            "small-slower"
        );
        assert_eq!(
            portfolio.admit(100, 1, 0, |_| true).unwrap().plan,
            "large-fast"
        );
    }

    #[test]
    fn spill_bounded_peak_does_not_hide_a_fitting_preferred_operating_point() {
        let class = ResourceGrantClass {
            id: ResourceGrantClassId::new(0),
            hard_memory_bytes: 100,
            spill_policy: SpillPolicy::Allowed,
            max_parallel_tasks: 4,
        };
        let mut operating_cost = cost(1.0, 100);
        operating_cost.minimum_memory_bytes = 5;
        operating_cost.revocable_memory_target = 5;
        let portfolio = PhysicalPlanPortfolio::build(
            [class],
            [(class.id, "parallel", Fingerprint(1), operating_cost)],
        )
        .unwrap();

        let admitted = portfolio.admit(10, 4, 0, |_| true).unwrap();

        assert_eq!(admitted.plan, "parallel");
        assert_eq!(admitted.resources.working_set_memory_bytes, 10);
        assert_eq!(admitted.resources.memory_ceiling_bytes, 100);
    }

    #[test]
    fn admission_selects_a_plan_whose_dop_is_currently_executable() {
        let serial = ResourceGrantClass {
            id: ResourceGrantClassId(1),
            hard_memory_bytes: 20,
            spill_policy: SpillPolicy::Allowed,
            max_parallel_tasks: 1,
        };
        let parallel = ResourceGrantClass {
            id: ResourceGrantClassId(2),
            hard_memory_bytes: 100,
            spill_policy: SpillPolicy::Allowed,
            max_parallel_tasks: 8,
        };
        let portfolio = PhysicalPlanPortfolio::build(
            [serial, parallel],
            [
                (serial.id, "serial", Fingerprint(1), cost(2.0, 20)),
                (parallel.id, "parallel", Fingerprint(2), cost(1.0, 100)),
            ],
        )
        .unwrap();

        let admitted = portfolio.admit(100, 2, 0, |_| true).unwrap();

        assert_eq!(admitted.plan, "serial");
        assert_eq!(admitted.resources.max_parallel_tasks, 1);
    }

    #[test]
    fn equal_work_prefers_the_highest_admitted_dop_for_latency() {
        let serial = ResourceGrantClass {
            id: ResourceGrantClassId::new(0),
            hard_memory_bytes: 100,
            spill_policy: SpillPolicy::Allowed,
            max_parallel_tasks: 1,
        };
        let parallel = ResourceGrantClass {
            id: ResourceGrantClassId::new(1),
            hard_memory_bytes: 200,
            spill_policy: SpillPolicy::Allowed,
            max_parallel_tasks: 8,
        };
        let portfolio = PhysicalPlanPortfolio::build(
            [serial, parallel],
            [
                (serial.id, "serial", Fingerprint(1), cost(1.0, 100)),
                (parallel.id, "parallel", Fingerprint(2), cost(1.0, 100)),
            ],
        )
        .unwrap();

        let admitted = portfolio.admit(200, 8, 0, |_| true).unwrap();

        assert_eq!(admitted.plan, "parallel");
        assert_eq!(admitted.resources.max_parallel_tasks, 8);
    }

    #[test]
    fn equal_fingerprint_with_different_cost_contracts_is_not_merged() {
        let class = ResourceGrantClass {
            id: ResourceGrantClassId(1),
            hard_memory_bytes: 100,
            spill_policy: SpillPolicy::Allowed,
            max_parallel_tasks: 1,
        };
        let portfolio = PhysicalPlanPortfolio::build(
            [class],
            [
                (class.id, "first", Fingerprint(7), cost(1.0, 10)),
                (class.id, "second", Fingerprint(7), cost(2.0, 10)),
            ],
        )
        .unwrap();
        assert_eq!(portfolio.variants.len(), 1, "dominated cost is pruned");
        assert_eq!(portfolio.variants[0].plan, "first");
    }

    #[test]
    fn admission_filters_variant_local_dependencies() {
        let class = ResourceGrantClass {
            id: ResourceGrantClassId(1),
            hard_memory_bytes: 100,
            spill_policy: SpillPolicy::Allowed,
            max_parallel_tasks: 1,
        };
        let portfolio = PhysicalPlanPortfolio::build(
            [class],
            [
                (class.id, "specialized", Fingerprint(1), cost(1.0, 10)),
                (class.id, "baseline", Fingerprint(2), cost(2.0, 10)),
            ],
        )
        .unwrap();

        let admitted = portfolio
            .admit(100, 1, 0, |plan| *plan == "baseline")
            .unwrap();
        assert_eq!(admitted.plan, "baseline");
    }
}
