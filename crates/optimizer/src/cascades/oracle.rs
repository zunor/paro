// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Small, independent search and publication oracles.
//!
//! These models intentionally do not call the production cost comparator or
//! task state machine. They provide a finite reference domain for D3-C/D4:
//! a production change can be checked against the same semantic inputs while
//! its internal representation and scheduling remain free to evolve.

#[cfg(test)]
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct OracleCandidate {
    pub id: u8,
    pub cost: u64,
    pub required_source: u8,
    pub required_grant: u8,
    pub shared_owner: u8,
    pub continuation_class: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OracleContext {
    pub source_demand: u8,
    pub grant: u8,
    pub shared_owner: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetedOracleResult {
    pub winner: Option<OracleCandidate>,
    pub omitted: bool,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct CertifiedOracleCandidate {
    id: u8,
    local_work: u64,
    child_work: u64,
    runtime_filter_work: u64,
    shared_cte_work: u64,
    required_source: u8,
    required_grant: u8,
    shared_owner: u8,
    stats_revision: u8,
    child_complete: bool,
    source_response_supported: bool,
    phase_overlap_supported: bool,
    logical_domain_complete: bool,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CertifiedOracleContext {
    source_demand: u8,
    grant: u8,
    shared_owner: u8,
    stats_revision: u8,
}

#[cfg(test)]
fn certified_oracle_cost(candidate: CertifiedOracleCandidate, shared_cte_charge: u64) -> u64 {
    candidate
        .local_work
        .saturating_add(candidate.child_work)
        .saturating_add(candidate.runtime_filter_work)
        .saturating_add(candidate.shared_cte_work)
        .saturating_add(shared_cte_charge)
}

#[cfg(test)]
fn certified_oracle_admissible(
    candidate: CertifiedOracleCandidate,
    context: CertifiedOracleContext,
) -> bool {
    candidate.required_source & !context.source_demand == 0
        && candidate.required_grant <= context.grant
        && candidate.shared_owner == context.shared_owner
}

/// Independent finite reference for the exact cases where the production
/// proof is allowed to prune.  It intentionally does not call `SearchCost`,
/// Memo or TaskRegistry: a lower bound is usable only when all declared
/// completion/context facts are present and its exact operating-point cost is
/// above the current incumbent.
#[cfg(test)]
fn certified_oracle_winner(
    candidates: impl IntoIterator<Item = CertifiedOracleCandidate>,
    context: CertifiedOracleContext,
    incumbent: CertifiedOracleCandidate,
    shared_cte_charge: u64,
) -> (CertifiedOracleCandidate, BTreeSet<u8>) {
    candidates.into_iter().fold(
        (incumbent, BTreeSet::new()),
        |(winner, mut pruned), candidate| {
            if !certified_oracle_admissible(candidate, context) {
                return (winner, pruned);
            }
            let cost = certified_oracle_cost(candidate, shared_cte_charge);
            let proof_available = candidate.stats_revision == context.stats_revision
                && candidate.child_complete
                && candidate.source_response_supported
                && candidate.phase_overlap_supported
                && candidate.logical_domain_complete;
            if proof_available && cost > certified_oracle_cost(winner, shared_cte_charge) {
                pruned.insert(candidate.id);
                return (winner, pruned);
            }
            if (cost, candidate.id) < (certified_oracle_cost(winner, shared_cte_charge), winner.id)
            {
                (candidate, pruned)
            } else {
                (winner, pruned)
            }
        },
    )
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SeedCostContext {
    source_stats: u8,
    child_stats: u8,
    grant_memory: u64,
    calibration: u8,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SeedPlanAttestation {
    /// Semantic plan identity; Memo allocation IDs are intentionally absent.
    plan: u8,
    source_stats: u8,
    child_stats: u8,
    grant_memory: u64,
    calibration: u8,
    cost: u64,
}

#[cfg(test)]
fn seed_oracle_price(plan: u8, context: SeedCostContext) -> SeedPlanAttestation {
    SeedPlanAttestation {
        plan,
        source_stats: context.source_stats,
        child_stats: context.child_stats,
        grant_memory: context.grant_memory,
        calibration: context.calibration,
        cost: 10 + u64::from(context.source_stats) + u64::from(context.child_stats),
    }
}

#[cfg(test)]
fn seed_oracle_is_current(
    attestation: SeedPlanAttestation,
    plan: u8,
    context: SeedCostContext,
) -> bool {
    attestation.plan == plan
        && attestation.source_stats == context.source_stats
        && attestation.child_stats == context.child_stats
        && attestation.grant_memory == context.grant_memory
        && attestation.calibration == context.calibration
}

fn admissible(candidate: OracleCandidate, context: OracleContext) -> bool {
    candidate.required_source & !context.source_demand == 0
        && candidate.required_grant <= context.grant
        && candidate.shared_owner == context.shared_owner
}

fn better(left: OracleCandidate, right: OracleCandidate) -> bool {
    (left.cost, left.continuation_class, left.id) < (right.cost, right.continuation_class, right.id)
}

/// Exhaustively choose the parent-observable winner for a context.
pub fn exhaustive_winner(
    candidates: impl IntoIterator<Item = OracleCandidate>,
    context: OracleContext,
) -> Option<OracleCandidate> {
    candidates
        .into_iter()
        .filter(|candidate| admissible(*candidate, context))
        .fold(None, |winner, candidate| match winner {
            Some(current) if !better(candidate, current) => Some(current),
            _ => Some(candidate),
        })
}

/// Evaluate only a finite prefix and keep the omission explicit. A prefix
/// winner is an anytime result, never a proof that the complete domain was
/// searched.
pub fn budgeted_winner(
    candidates: &[OracleCandidate],
    context: OracleContext,
    budget: usize,
) -> BudgetedOracleResult {
    BudgetedOracleResult {
        winner: exhaustive_winner(candidates.iter().copied().take(budget), context),
        omitted: candidates.len() > budget,
    }
}

#[cfg(test)]
fn permutations<T: Copy>(items: &[T]) -> Vec<Vec<T>> {
    if items.is_empty() {
        return vec![Vec::new()];
    }
    let mut result = Vec::new();
    for index in 0..items.len() {
        let mut rest = items.to_vec();
        let head = rest.remove(index);
        for mut tail in permutations(&rest) {
            let mut permutation = Vec::with_capacity(items.len());
            permutation.push(head);
            permutation.append(&mut tail);
            result.push(permutation);
        }
    }
    result
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(test)]
enum ReferenceTaskState {
    Runnable,
    Running,
    Awaiting,
    Completed,
    Failed,
    Invalidated,
}

#[derive(Debug, Clone, Default)]
#[cfg(test)]
struct ReferenceTask {
    state: Option<ReferenceTaskState>,
    dependencies: BTreeSet<u32>,
    objects: BTreeSet<u32>,
    reserved_units: u64,
}

#[derive(Debug, Default)]
#[cfg(test)]
struct ReferencePublicationKernel {
    tasks: BTreeMap<u32, ReferenceTask>,
    next_object: u32,
    published_owner: BTreeMap<u32, u32>,
}

#[cfg(test)]
impl ReferencePublicationKernel {
    fn create(&mut self, task: u32) {
        self.tasks.insert(
            task,
            ReferenceTask {
                state: Some(ReferenceTaskState::Runnable),
                ..ReferenceTask::default()
            },
        );
    }

    fn start(&mut self, task: u32) {
        assert_eq!(self.tasks[&task].state, Some(ReferenceTaskState::Runnable));
        self.tasks.get_mut(&task).unwrap().state = Some(ReferenceTaskState::Running);
    }

    fn reserve_and_allocate(&mut self, task: u32, units: u64) -> u32 {
        self.tasks.get_mut(&task).unwrap().reserved_units += units;
        let object = self.next_object;
        self.next_object += 1;
        self.tasks.get_mut(&task).unwrap().objects.insert(object);
        object
    }

    fn commit(&mut self, task: u32) {
        let units = self.tasks.get_mut(&task).unwrap().reserved_units;
        self.tasks.get_mut(&task).unwrap().reserved_units = 0;
        let objects = self.tasks[&task].objects.clone();
        for object in objects {
            self.published_owner.insert(object, task);
        }
        assert!(units > 0);
    }

    fn await_dependencies(&mut self, task: u32, dependencies: impl IntoIterator<Item = u32>) {
        let unresolved = dependencies
            .into_iter()
            .filter(|dependency| {
                !matches!(
                    self.tasks[dependency].state,
                    Some(ReferenceTaskState::Completed | ReferenceTaskState::Failed)
                )
            })
            .collect::<BTreeSet<_>>();
        self.tasks.get_mut(&task).unwrap().dependencies = unresolved.clone();
        self.tasks.get_mut(&task).unwrap().state = Some(if unresolved.is_empty() {
            ReferenceTaskState::Runnable
        } else {
            ReferenceTaskState::Awaiting
        });
    }

    fn complete(&mut self, task: u32) {
        self.tasks.get_mut(&task).unwrap().state = Some(ReferenceTaskState::Completed);
        for dependent in self.tasks.values_mut() {
            dependent.dependencies.remove(&task);
            if dependent.dependencies.is_empty()
                && dependent.state == Some(ReferenceTaskState::Awaiting)
            {
                dependent.state = Some(ReferenceTaskState::Runnable);
            }
        }
    }

    fn fail(&mut self, task: u32) {
        let objects = std::mem::take(&mut self.tasks.get_mut(&task).unwrap().objects);
        for object in objects {
            self.published_owner.remove(&object);
        }
        self.tasks.get_mut(&task).unwrap().reserved_units = 0;
        self.tasks.get_mut(&task).unwrap().state = Some(ReferenceTaskState::Failed);
    }

    fn redirect(&mut self, task: u32) {
        let objects = std::mem::take(&mut self.tasks.get_mut(&task).unwrap().objects);
        for object in objects {
            self.published_owner.remove(&object);
        }
        self.tasks.get_mut(&task).unwrap().reserved_units = 0;
        self.tasks.get_mut(&task).unwrap().state = Some(ReferenceTaskState::Invalidated);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cascades::ids::{LogicalExprId, RuleId};
    use crate::cascades::memo::Memo;
    use crate::cascades::tasks::{CursorId, ReadSet, TaskIntent, TaskOutcome, TaskRegistry};

    fn candidates() -> [OracleCandidate; 5] {
        [
            OracleCandidate {
                id: 1,
                cost: 5,
                required_source: 0b01,
                required_grant: 2,
                shared_owner: 7,
                continuation_class: 1,
            },
            OracleCandidate {
                id: 2,
                cost: 7,
                required_source: 0b10,
                required_grant: 2,
                shared_owner: 7,
                continuation_class: 0,
            },
            OracleCandidate {
                id: 3,
                cost: 3,
                required_source: 0b11,
                required_grant: 4,
                shared_owner: 7,
                continuation_class: 0,
            },
            OracleCandidate {
                id: 4,
                cost: 1,
                required_source: 0b01,
                required_grant: 1,
                shared_owner: 9,
                continuation_class: 0,
            },
            OracleCandidate {
                id: 5,
                cost: 9,
                required_source: 0,
                required_grant: 1,
                shared_owner: 7,
                continuation_class: 0,
            },
        ]
    }

    #[test]
    fn exhaustive_context_oracle_is_invariant_to_insertion_order() {
        let context = OracleContext {
            source_demand: 0b11,
            grant: 2,
            shared_owner: 7,
        };
        let expected = exhaustive_winner(candidates(), context).unwrap();
        for permutation in permutations(&candidates()) {
            assert_eq!(exhaustive_winner(permutation, context), Some(expected));
        }
    }

    #[test]
    fn context_oracle_preserves_parent_observable_source_and_ownership() {
        assert_eq!(
            exhaustive_winner(
                candidates(),
                OracleContext {
                    source_demand: 0b10,
                    grant: 2,
                    shared_owner: 7,
                },
            )
            .unwrap()
            .id,
            2
        );
        assert_eq!(
            exhaustive_winner(
                candidates(),
                OracleContext {
                    source_demand: 0b01,
                    grant: 2,
                    shared_owner: 9,
                },
            )
            .unwrap()
            .id,
            4
        );
    }

    #[test]
    fn budgeted_context_oracle_reports_omitted_candidates() {
        let all = candidates();
        let result = budgeted_winner(
            &all,
            OracleContext {
                source_demand: 0b11,
                grant: 4,
                shared_owner: 7,
            },
            2,
        );
        assert_eq!(result.winner.unwrap().id, 1);
        assert!(result.omitted);
        assert_eq!(
            exhaustive_winner(
                all,
                OracleContext {
                    source_demand: 0b11,
                    grant: 4,
                    shared_owner: 7,
                },
            )
            .unwrap()
            .id,
            3
        );
    }

    #[test]
    fn publication_oracle_matches_wait_publish_fail_and_redirect_contracts() {
        let mut reference = ReferencePublicationKernel::default();
        reference.create(0);
        reference.create(1);
        reference.start(0);
        reference.start(1);
        let _parent_object = reference.reserve_and_allocate(0, 4);
        let child_object = reference.reserve_and_allocate(1, 3);
        reference.commit(1);
        reference.await_dependencies(0, [1]);
        assert_eq!(
            reference.tasks[&0].state,
            Some(ReferenceTaskState::Awaiting)
        );
        reference.complete(1);
        assert_eq!(
            reference.tasks[&0].state,
            Some(ReferenceTaskState::Runnable)
        );
        reference.create(2);
        reference.start(2);
        let failed_reference_object = reference.reserve_and_allocate(2, 5);
        reference.fail(2);
        assert!(!reference
            .published_owner
            .contains_key(&failed_reference_object));
        reference.create(3);
        reference.start(3);
        let redirected_reference_object = reference.reserve_and_allocate(3, 2);
        reference.redirect(3);
        assert!(!reference
            .published_owner
            .contains_key(&redirected_reference_object));

        let mut production = TaskRegistry::default();
        let make_task = |registry: &mut TaskRegistry, expression| match registry
            .request(
                TaskIntent::Discover {
                    expression,
                    rule: RuleId::new(1),
                },
                ReadSet::empty(),
            )
            .unwrap()
        {
            crate::cascades::tasks::TaskRequest::Leader(task) => task,
            request => panic!("unexpected request: {request:?}"),
        };
        let parent = make_task(&mut production, LogicalExprId::new(0));
        let child = make_task(&mut production, LogicalExprId::new(1));
        production.start(parent).unwrap();
        production.start(child).unwrap();
        production.reserve_once(parent, 0, 4).unwrap();
        let parent_object = production.allocate_object(parent).unwrap();
        production.reserve_once(child, 0, 3).unwrap();
        let production_child_object = production.allocate_object(child).unwrap();
        production.commit_segment(child).unwrap();
        let memo = Memo::new(Default::default());
        production
            .publish_current(
                parent,
                &memo,
                [child],
                TaskOutcome::Progress {
                    cursor: CursorId::new(0),
                },
            )
            .unwrap();
        assert_eq!(
            production.state(parent),
            Some(crate::cascades::tasks::TaskState::Awaiting)
        );
        assert_eq!(
            production.published_owner(production_child_object),
            Some(child)
        );
        assert_eq!(reference.published_owner.get(&child_object), Some(&1));
        production.complete(child, TaskOutcome::Infeasible).unwrap();
        production.start(parent).unwrap();
        production
            .publish_current(
                parent,
                &memo,
                [],
                TaskOutcome::Progress {
                    cursor: CursorId::new(0),
                },
            )
            .unwrap();
        assert_eq!(production.published_owner(parent_object), Some(parent));
        assert_eq!(
            production.state(parent),
            Some(crate::cascades::tasks::TaskState::Completed)
        );

        let failed = make_task(&mut production, LogicalExprId::new(2));
        production.start(failed).unwrap();
        production.reserve_once(failed, 0, 5).unwrap();
        let failed_object = production.allocate_object(failed).unwrap();
        production.fail(failed, "oracle rollback").unwrap();
        assert!(!production.is_published(failed_object));

        let redirected = make_task(&mut production, LogicalExprId::new(3));
        production.start(redirected).unwrap();
        production.reserve_once(redirected, 0, 2).unwrap();
        let redirected_object = production.allocate_object(redirected).unwrap();
        production.invalidate(redirected).unwrap();
        assert!(!production.is_published(redirected_object));
    }

    #[test]
    fn certified_pruning_oracle_preserves_optimum_across_order_and_context_changes() {
        let context = CertifiedOracleContext {
            source_demand: 0b11,
            grant: 4,
            shared_owner: 7,
            stats_revision: 1,
        };
        let incumbent = CertifiedOracleCandidate {
            id: 0,
            local_work: 1,
            child_work: 1,
            runtime_filter_work: 0,
            shared_cte_work: 0,
            required_source: 0,
            required_grant: 1,
            shared_owner: 7,
            stats_revision: 1,
            child_complete: true,
            source_response_supported: true,
            phase_overlap_supported: true,
            logical_domain_complete: true,
        };
        let candidates = vec![
            // Equal cost is retained so the declared tie-break remains the
            // oracle's responsibility rather than a pruning side effect.
            CertifiedOracleCandidate {
                id: 1,
                local_work: 2,
                child_work: 0,
                runtime_filter_work: 0,
                shared_cte_work: 0,
                required_source: 0,
                required_grant: 1,
                shared_owner: 7,
                stats_revision: 1,
                child_complete: true,
                source_response_supported: true,
                phase_overlap_supported: true,
                logical_domain_complete: true,
            },
            // This is the non-selected child case: a child frontier that is
            // not complete cannot be used as a proof, even though its current
            // estimate is worse than the incumbent.
            CertifiedOracleCandidate {
                id: 2,
                local_work: 20,
                child_work: 0,
                runtime_filter_work: 0,
                shared_cte_work: 0,
                required_source: 0,
                required_grant: 1,
                shared_owner: 7,
                stats_revision: 1,
                child_complete: false,
                source_response_supported: true,
                phase_overlap_supported: true,
                logical_domain_complete: true,
            },
            // A valid RF/source response is part of the exact cost and can be
            // pruned only after that source work is included.
            CertifiedOracleCandidate {
                id: 3,
                local_work: 4,
                child_work: 0,
                runtime_filter_work: 5,
                shared_cte_work: 0,
                required_source: 0b10,
                required_grant: 2,
                shared_owner: 7,
                stats_revision: 1,
                child_complete: true,
                source_response_supported: true,
                phase_overlap_supported: true,
                logical_domain_complete: true,
            },
            // An unsupported phase overlap is fail-closed rather than
            // guessed into a pruning proof.
            CertifiedOracleCandidate {
                id: 4,
                local_work: 30,
                child_work: 0,
                runtime_filter_work: 0,
                shared_cte_work: 0,
                required_source: 0,
                required_grant: 1,
                shared_owner: 7,
                stats_revision: 1,
                child_complete: true,
                source_response_supported: true,
                phase_overlap_supported: false,
                logical_domain_complete: true,
            },
            // An unperformed logical rewrite keeps the domain open.
            CertifiedOracleCandidate {
                id: 5,
                local_work: 40,
                child_work: 0,
                runtime_filter_work: 0,
                shared_cte_work: 0,
                required_source: 0,
                required_grant: 1,
                shared_owner: 7,
                stats_revision: 1,
                child_complete: true,
                source_response_supported: true,
                phase_overlap_supported: true,
                logical_domain_complete: false,
            },
            // A shared CTE producer is charged once outside the child choice;
            // inline work remains visible in the candidate itself.
            CertifiedOracleCandidate {
                id: 6,
                local_work: 10,
                child_work: 0,
                runtime_filter_work: 0,
                shared_cte_work: 2,
                required_source: 0,
                required_grant: 1,
                shared_owner: 7,
                stats_revision: 1,
                child_complete: true,
                source_response_supported: true,
                phase_overlap_supported: true,
                logical_domain_complete: true,
            },
            // A changed statistics revision invalidates the old proof.
            CertifiedOracleCandidate {
                id: 7,
                local_work: 25,
                child_work: 0,
                runtime_filter_work: 0,
                shared_cte_work: 0,
                required_source: 0,
                required_grant: 1,
                shared_owner: 7,
                stats_revision: 1,
                child_complete: true,
                source_response_supported: true,
                phase_overlap_supported: true,
                logical_domain_complete: true,
            },
        ];
        let expected = candidates
            .iter()
            .copied()
            .chain([incumbent])
            .filter(|candidate| certified_oracle_admissible(*candidate, context))
            .min_by_key(|candidate| (certified_oracle_cost(*candidate, 3), candidate.id))
            .unwrap();

        let (winner, pruned) = certified_oracle_winner(candidates.clone(), context, incumbent, 3);
        assert_eq!(winner, expected);
        assert!(pruned.contains(&3));
        assert!(pruned.contains(&6));
        assert!(!pruned.contains(&2));
        assert!(!pruned.contains(&4));
        assert!(!pruned.contains(&5));

        for permutation in permutations(&candidates) {
            let (permuted_winner, _) = certified_oracle_winner(permutation, context, incumbent, 3);
            assert_eq!(permuted_winner, expected);
        }

        let changed_context = CertifiedOracleContext {
            stats_revision: 2,
            ..context
        };
        let (_, changed_pruned) =
            certified_oracle_winner(candidates, changed_context, incumbent, 3);
        assert!(!changed_pruned.contains(&3));
        assert!(!changed_pruned.contains(&6));
    }

    #[test]
    fn seed_cost_oracle_rejects_stale_source_and_child_facts() {
        let source = SeedCostContext {
            source_stats: 1,
            child_stats: 2,
            grant_memory: 1024,
            calibration: 3,
        };
        let attestation = seed_oracle_price(7, source);
        assert!(seed_oracle_is_current(attestation, 7, source));

        let refined_source = SeedCostContext {
            source_stats: 4,
            ..source
        };
        let refined_child = SeedCostContext {
            child_stats: 8,
            ..source
        };
        assert!(!seed_oracle_is_current(attestation, 7, refined_source));
        assert!(!seed_oracle_is_current(attestation, 7, refined_child));
        assert!(seed_oracle_is_current(
            seed_oracle_price(7, refined_source),
            7,
            refined_source
        ));
        assert!(seed_oracle_is_current(
            seed_oracle_price(7, refined_child),
            7,
            refined_child
        ));
    }

    #[test]
    fn seed_cost_oracle_reprices_grant_and_calibration_changes_but_ignores_memo_ids() {
        let context = SeedCostContext {
            source_stats: 1,
            child_stats: 1,
            grant_memory: 1024,
            calibration: 1,
        };
        let attestation = seed_oracle_price(9, context);
        assert!(!seed_oracle_is_current(
            attestation,
            9,
            SeedCostContext {
                grant_memory: 2048,
                ..context
            }
        ));
        assert!(!seed_oracle_is_current(
            attestation,
            9,
            SeedCostContext {
                calibration: 2,
                ..context
            }
        ));

        // Memo/group/property allocation is not a cost dependency. A
        // differently numbered destination can reuse the same semantic plan
        // after it is priced against its own facts and operating point.
        let renamed_memo_context = context;
        assert!(seed_oracle_is_current(
            seed_oracle_price(9, renamed_memo_context),
            9,
            renamed_memo_context
        ));
    }

    #[test]
    fn pruning_on_and_off_have_the_same_optimum_when_the_bound_is_certified() {
        let context = CertifiedOracleContext {
            source_demand: 0b11,
            grant: 4,
            shared_owner: 7,
            stats_revision: 1,
        };
        let incumbent = CertifiedOracleCandidate {
            id: 0,
            local_work: 8,
            child_work: 0,
            runtime_filter_work: 0,
            shared_cte_work: 0,
            required_source: 0,
            required_grant: 1,
            shared_owner: 7,
            stats_revision: 1,
            child_complete: true,
            source_response_supported: true,
            phase_overlap_supported: true,
            logical_domain_complete: true,
        };
        let candidates = [
            incumbent,
            CertifiedOracleCandidate {
                id: 1,
                local_work: 12,
                child_work: 2,
                runtime_filter_work: 0,
                shared_cte_work: 0,
                required_source: 0,
                required_grant: 1,
                shared_owner: 7,
                stats_revision: 1,
                child_complete: true,
                source_response_supported: true,
                phase_overlap_supported: true,
                logical_domain_complete: true,
            },
        ];
        let without_pruning = candidates
            .iter()
            .copied()
            .filter(|candidate| certified_oracle_admissible(*candidate, context))
            .min_by_key(|candidate| (certified_oracle_cost(*candidate, 0), candidate.id));
        let (with_pruning, pruned) = certified_oracle_winner(candidates, context, incumbent, 0);
        assert_eq!(without_pruning, Some(with_pruning));
        assert!(pruned.contains(&1));
    }
}
