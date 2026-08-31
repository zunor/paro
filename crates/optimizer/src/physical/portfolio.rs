// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable resource-grant plan portfolios and deterministic admission.

use std::collections::{BTreeMap, BTreeSet};

use paro_common::error::{self as paro_error, Result};

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
    pub concurrency_class: u16,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReservationToken {
    pub class: ResourceGrantClassId,
    pub memory_bytes: u64,
    pub external_worker_slots: u16,
}

#[derive(Debug)]
pub struct AdmittedPlan<P> {
    pub plan: P,
    pub physical_fingerprint: Fingerprint,
    pub reservation: ReservationToken,
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
    pub fn admit(
        self,
        available_memory_bytes: u64,
        available_external_worker_slots: u16,
    ) -> Result<AdmittedPlan<P>> {
        let classes = self
            .grant_classes
            .iter()
            .map(|class| (class.id, *class))
            .collect::<BTreeMap<_, _>>();
        let variants = self.variants.into_vec();
        let selected = variants
            .iter()
            .enumerate()
            .filter_map(|(index, variant)| {
                if variant.cost.peak_memory_upper > available_memory_bytes
                    || variant.cost.external_worker_slots_upper > available_external_worker_slots
                {
                    return None;
                }
                let class = variant
                    .admissible_classes
                    .iter()
                    .filter_map(|id| classes.get(id))
                    .filter(|class| {
                        variant.cost.peak_memory_upper <= class.hard_memory_bytes
                            && (variant.cost.spill_bytes_expected == 0
                                || class.spill_policy == SpillPolicy::Allowed)
                    })
                    .min_by_key(|class| (class.hard_memory_bytes, class.id))?;
                Some((index, *class))
            })
            .min_by(|(left_index, left_class), (right_index, right_class)| {
                let left = &variants[*left_index];
                let right = &variants[*right_index];
                left.cost
                    .score
                    .risk_adjusted
                    .total_cmp(&right.cost.score.risk_adjusted)
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
            .into_iter()
            .nth(selected_index)
            .expect("selected portfolio index must remain valid");
        Ok(AdmittedPlan {
            reservation: ReservationToken {
                class: selected_class.id,
                memory_bytes: selected.cost.peak_memory_upper,
                external_worker_slots: selected.cost.external_worker_slots_upper,
            },
            physical_fingerprint: selected.physical_fingerprint,
            plan: selected.plan,
        })
    }
}

impl PhysicalPlanPortfolio<PhysicalPlan> {
    pub fn combined_dependencies(&self) -> Result<crate::physical::PlanDependencies> {
        let mut variants = self.variants.iter();
        let first = variants
            .next()
            .ok_or_else(|| paro_error::internal("physical portfolio is empty"))?;
        let mut dependencies = first.plan.dependencies.clone();
        for variant in variants {
            dependencies.merge_artifact(&variant.plan.dependencies)?;
        }
        Ok(dependencies)
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
                            spec.spill_policy != crate::physical::SpillExecutionPolicy::Forbidden
                        }
                        crate::physical::PhysicalNodeKind::HashJoin(spec) => {
                            spec.spill_policy != crate::physical::SpillExecutionPolicy::Forbidden
                        }
                        crate::physical::PhysicalNodeKind::Sort(spec) => {
                            spec.spill_policy != crate::physical::SpillExecutionPolicy::Forbidden
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
            if variant.plan.reservation.is_some() {
                return Err(paro_error::internal(
                    "portfolio contains a plan with a pre-bound reservation",
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
        self.combined_dependencies()?;
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
                concurrency_class: 0,
            },
            ResourceGrantClass {
                id: ResourceGrantClassId(2),
                hard_memory_bytes: 20,
                spill_policy: SpillPolicy::Allowed,
                concurrency_class: 0,
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
            [ResourceGrantClass {
                id: ResourceGrantClassId(1),
                hard_memory_bytes: 100,
                spill_policy: SpillPolicy::Allowed,
                concurrency_class: 0,
            }],
            [
                (
                    ResourceGrantClassId(1),
                    "fast",
                    Fingerprint(1),
                    cost(1.0, 100),
                ),
                (
                    ResourceGrantClassId(1),
                    "small",
                    Fingerprint(2),
                    cost(2.0, 10),
                ),
            ],
        )
        .unwrap();
        let admitted = portfolio.admit(20, 0).unwrap();
        assert_eq!(admitted.plan, "small");
        assert_eq!(admitted.reservation.memory_bytes, 10);
    }

    #[test]
    fn equal_fingerprint_with_different_cost_contracts_is_not_merged() {
        let class = ResourceGrantClass {
            id: ResourceGrantClassId(1),
            hard_memory_bytes: 100,
            spill_policy: SpillPolicy::Allowed,
            concurrency_class: 0,
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
}
