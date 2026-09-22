// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Deterministic multidimensional search budgets.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::ids::{Fingerprint, GroupId, RuleId};

/// A first-class residual search obligation. An anytime winner is valid, but
/// cannot claim closure while any of these candidate classes remain omitted.
/// Global obligations have no group owner; their witness still has semantic
/// event identity in the ledger which rejected the work.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SearchObligation {
    pub group: Option<GroupId>,
    pub reason: SearchIncompleteReason,
    pub witness: Fingerprint,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum SearchIncompleteReason {
    /// A declared grant has only its mandatory executable baseline; optional
    /// closure was deferred by resource selection, not rejected by a budget.
    OptionalGrantDeferred(super::ids::ResourceGrantClassId),
    Budget(BudgetDimension),
    /// Cooperative wall deadline. Work ledgers remain unchanged: this is not
    /// evidence that the configured logical/physical closure was exhausted.
    Deadline,
    /// The baseline survives an advisory rule failure, but an unexamined
    /// equivalence class must not be advertised as a completed search.
    RuleFailure {
        rule: RuleId,
        detail: Arc<str>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BudgetDimension {
    Group,
    CompositionGroup,
    LogicalExprPerGroup,
    CompositionLogicalExprPerGroup,
    PhysicalExprPerGroup,
    InterestingGoalPerGroup,
    RuleFirePerGroup,
    CompositionRuleFirePerGroup,
    RuleWorkPerGroup,
    CompositionRuleWorkPerGroup,
    ChildFrontierCombination,
    JoinConnectedPair,
    GraphFrontier,
    FactorizationVariant,
    MultiwayJoinCandidate,
    SearchCandidate,
    SearchFusion,
    ParameterContext,
    CompositeRegionCandidate,
    RecursiveCandidate,
    EnforcerChain,
    WinnerFrontier,
}

impl BudgetDimension {
    pub const fn stable_name(self) -> &'static str {
        match self {
            Self::Group => "group",
            Self::CompositionGroup => "composition_group",
            Self::LogicalExprPerGroup => "logical_expr_per_group",
            Self::CompositionLogicalExprPerGroup => "composition_logical_expr_per_group",
            Self::PhysicalExprPerGroup => "physical_expr_per_group",
            Self::InterestingGoalPerGroup => "interesting_goal_per_group",
            Self::RuleFirePerGroup => "rule_fire_per_group",
            Self::CompositionRuleFirePerGroup => "composition_rule_fire_per_group",
            Self::RuleWorkPerGroup => "rule_work_per_group",
            Self::CompositionRuleWorkPerGroup => "composition_rule_work_per_group",
            Self::ChildFrontierCombination => "child_frontier_combination",
            Self::JoinConnectedPair => "join_connected_pair",
            Self::GraphFrontier => "graph_frontier",
            Self::FactorizationVariant => "factorization_variant",
            Self::MultiwayJoinCandidate => "multiway_join_candidate",
            Self::SearchCandidate => "search_candidate",
            Self::SearchFusion => "search_fusion",
            Self::ParameterContext => "parameter_context",
            Self::CompositeRegionCandidate => "composite_region_candidate",
            Self::RecursiveCandidate => "recursive_candidate",
            Self::EnforcerChain => "enforcer_chain",
            Self::WinnerFrontier => "winner_frontier",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchBudget {
    /// Explicit embedding override; otherwise the statement's typed setting
    /// supplies the policy before Memo construction.
    pub search_policy: Option<paro_context::OptimizerSearchPolicy>,
    /// Isolation ceiling, not a latency tuning knob. It includes incumbent
    /// construction; only optional work stops at this deadline. `None` is
    /// useful for exhaustive oracles and controlled profiling.
    pub optional_time_limit: Option<std::time::Duration>,
    /// Emergency isolation surface for a faulty optional equivalence rule.
    /// Mandatory normalization and baseline implementations are not rules and
    /// cannot be disabled through this set.
    pub disabled_transformation_rules: BTreeSet<RuleId>,
    /// Query-global local groups admitted per group in the immutable initial
    /// Memo. Scaling from the mandatory DAG keeps the search envelope stable
    /// across query size instead of imposing a cliff at one absolute count.
    pub max_optional_groups_per_initial_group: u32,
    /// Query-global composition groups admitted per initial Memo group.
    pub max_optional_composition_groups_per_initial_group: u32,
    pub max_optional_logical_exprs_per_group: u32,
    /// Root alternatives reserved for rules which combine child frontiers.
    /// Local rewrites cannot strand a bounded parent composition after they
    /// fill the ordinary logical-expression frontier.
    pub max_optional_composition_logical_exprs_per_group: u32,
    pub max_optional_physical_exprs_per_group: u32,
    pub max_optional_interesting_goals_per_group: u32,
    pub max_rule_firings_per_group: u32,
    /// Fire credits reserved for bindings across independent child
    /// frontiers. A composition binding has its own semantic event identity
    /// and must not compete with local rewrites of the same root group.
    pub max_composition_rule_firings_per_group: u32,
    /// Maximum number of logical group visits performed by optional rule
    /// materialization for one target group. This bounds legacy whole-region
    /// rules until each is expressed entirely as local Memo operands.
    pub max_rule_work_units_per_group: u32,
    /// Work reserved for parent rules which combine already-published child
    /// alternatives. Descendant expansion cannot starve this pool.
    pub max_composition_rule_work_units_per_group: u32,
    /// Additional child-frontier combinations costed for a group. The
    /// selected-child baseline is mandatory and does not consume this credit.
    pub max_child_frontier_combinations_per_group: u32,
    /// Hard resident bound for one physical winner frontier.  A frontier can
    /// be genuinely high-dimensional (source response, memory proof, and
    /// latency are independent axes), so arbitrary top-N truncation is not a
    /// valid optimization.  This bound is instead an explicit anytime-search
    /// boundary: evicted candidates leave a `WinnerFrontier` obligation and
    /// the result is never reported as a complete closure.
    pub max_winner_frontier_candidates_per_goal: u32,
    pub max_join_connected_pairs: u32,
    pub max_join_exact_relations: u16,
    pub join_beam_width: u16,
    pub max_graph_frontiers: u32,
    pub max_factorization_variants: u16,
    pub max_multiway_join_candidates: u16,
    pub max_search_candidates: u16,
    pub max_search_fusions: u16,
    pub max_parameter_contexts: u16,
    pub max_optional_region_product_depth: u8,
    pub max_composite_region_groups: u16,
    pub max_mandatory_region_groups: u32,
    pub max_composite_region_candidates: u16,
    pub max_recursive_candidates: u16,
    pub max_optional_enforcer_depth: u8,
    pub max_optional_enforcer_chains_per_goal: u8,
    pub max_grant_classes: u8,
}

impl Default for SearchBudget {
    fn default() -> Self {
        Self {
            search_policy: None,
            // This is an explicitly opt-in diagnostic override.  It is read
            // at budget construction so the benchmark can run a real
            // stop-and-execute process without changing the production
            // default or replaying a completed search.  Malformed values are
            // ignored and retain the normal 30 s isolation ceiling.
            optional_time_limit: std::env::var("PARO_DIAGNOSTIC_SEARCH_STOP_MS")
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok())
                .map(std::time::Duration::from_millis)
                .or(Some(std::time::Duration::from_secs(30))),
            disabled_transformation_rules: BTreeSet::new(),
            // Local and composition shells are separate pools, but both must
            // leave enough headroom for two independent child rewrites to be
            // composed at their parent. Eight groups per initial node strands
            // that closure on ordinary multi-consumer analytical queries.
            max_optional_groups_per_initial_group: 16,
            max_optional_composition_groups_per_initial_group: 16,
            max_optional_logical_exprs_per_group: 64,
            max_optional_composition_logical_exprs_per_group: 32,
            max_optional_physical_exprs_per_group: 64,
            max_optional_interesting_goals_per_group: 16,
            max_rule_firings_per_group: 256,
            max_composition_rule_firings_per_group: 32,
            max_rule_work_units_per_group: 65_536,
            max_composition_rule_work_units_per_group: 65_536,
            max_child_frontier_combinations_per_group: 4_096,
            // P1 sensitivity experiment only: truncation still records its
            // ordinary search obligation. This is never a completeness mode.
            max_winner_frontier_candidates_per_goal: std::env::var(
                "PARO_DIAGNOSTIC_FRONTIER_WIDTH",
            )
            .ok()
            .and_then(|value| diagnostic_frontier_width(&value))
            .unwrap_or(256),
            max_join_connected_pairs: 65_536,
            max_join_exact_relations: 12,
            join_beam_width: 64,
            max_graph_frontiers: 8,
            max_factorization_variants: 16,
            max_multiway_join_candidates: 16,
            max_search_candidates: 32,
            max_search_fusions: 16,
            max_parameter_contexts: 16,
            max_optional_region_product_depth: 8,
            max_composite_region_groups: 64,
            max_mandatory_region_groups: 4_096,
            max_composite_region_candidates: 64,
            max_recursive_candidates: 16,
            max_optional_enforcer_depth: 8,
            max_optional_enforcer_chains_per_goal: 8,
            max_grant_classes: 3,
        }
    }
}

fn diagnostic_frontier_width(value: &str) -> Option<u32> {
    match value.trim() {
        "unbounded" => Some(u32::MAX),
        value => value.parse::<u32>().ok().filter(|value| *value > 0),
    }
}

#[cfg(test)]
mod diagnostic_width_tests {
    #[test]
    fn malformed_or_zero_does_not_disable_the_frontier() {
        for value in ["", "0", "-1", "infinity", "4294967296"] {
            assert_eq!(super::diagnostic_frontier_width(value), None);
        }
        for width in [1, 2, 4, 8, 256] {
            assert_eq!(
                super::diagnostic_frontier_width(&width.to_string()),
                Some(width)
            );
        }
        assert_eq!(
            super::diagnostic_frontier_width("unbounded"),
            Some(u32::MAX)
        );
    }
}

impl SearchBudget {
    pub fn disable_transformation(&mut self, rule: RuleId) {
        self.disabled_transformation_rules.insert(rule);
    }

    pub fn transformation_enabled(&self, rule: RuleId) -> bool {
        !self.disabled_transformation_rules.contains(&rule)
    }

    pub fn disable_transformations_by_name(
        &mut self,
        names: &str,
    ) -> paro_common::error::Result<()> {
        super::rules::validate_transformation_rule_names(names)?;
        for name in names
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            let rule = super::rules::transformation_rule_id(name)
                .expect("validated optimizer transformation name disappeared");
            self.disable_transformation(rule);
        }
        Ok(())
    }

    /// Return a statically configured limit.
    ///
    /// Query-global group pools intentionally have no value until the initial
    /// Memo is sealed. Representing that state as `None` keeps an unsealed
    /// ledger distinct from a deliberately disabled (zero-credit) pool.
    pub fn optional_limit(&self, dimension: BudgetDimension) -> Option<u32> {
        Some(match dimension {
            // Group pools are query-global and receive their size-dependent
            // limits when the initial Memo is sealed.
            BudgetDimension::Group | BudgetDimension::CompositionGroup => return None,
            BudgetDimension::LogicalExprPerGroup => self.max_optional_logical_exprs_per_group,
            BudgetDimension::CompositionLogicalExprPerGroup => {
                self.max_optional_composition_logical_exprs_per_group
            }
            BudgetDimension::PhysicalExprPerGroup => self.max_optional_physical_exprs_per_group,
            BudgetDimension::InterestingGoalPerGroup => {
                self.max_optional_interesting_goals_per_group
            }
            BudgetDimension::RuleFirePerGroup => self.max_rule_firings_per_group,
            BudgetDimension::CompositionRuleFirePerGroup => {
                self.max_composition_rule_firings_per_group
            }
            BudgetDimension::RuleWorkPerGroup => self.max_rule_work_units_per_group,
            BudgetDimension::CompositionRuleWorkPerGroup => {
                self.max_composition_rule_work_units_per_group
            }
            BudgetDimension::ChildFrontierCombination => {
                self.max_child_frontier_combinations_per_group
            }
            // Winner frontier size is enforced by Memo admission and emits a
            // residual obligation when the cap is reached. It is exposed here
            // as a named contract so profiles and budget diagnostics can
            // distinguish it from child-product enumeration.
            BudgetDimension::WinnerFrontier => self.max_winner_frontier_candidates_per_goal,
            BudgetDimension::JoinConnectedPair => self.max_join_connected_pairs,
            BudgetDimension::GraphFrontier => self.max_graph_frontiers,
            BudgetDimension::FactorizationVariant => self.max_factorization_variants as u32,
            BudgetDimension::MultiwayJoinCandidate => self.max_multiway_join_candidates as u32,
            BudgetDimension::SearchCandidate => self.max_search_candidates as u32,
            BudgetDimension::SearchFusion => self.max_search_fusions as u32,
            BudgetDimension::ParameterContext => self.max_parameter_contexts as u32,
            BudgetDimension::CompositeRegionCandidate => {
                self.max_composite_region_candidates as u32
            }
            BudgetDimension::RecursiveCandidate => self.max_recursive_candidates as u32,
            BudgetDimension::EnforcerChain => self.max_optional_enforcer_chains_per_goal as u32,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetDecision {
    Allowed,
    Duplicate,
    Exhausted,
    /// The dimension receives its limit from a later lifecycle boundary and
    /// that boundary has not run. This is an optimizer invariant failure, not
    /// a zero-sized search envelope.
    Unconfigured,
}

/// Optional candidate reservations have semantic identities. Executed work
/// instead has monotone occurrence meters. Neither replaying a task nor
/// merging groups can manufacture fresh credit.
#[derive(Debug)]
pub struct SearchLedger {
    owner: u64,
    budget: Arc<SearchBudget>,
    limit_overrides: BTreeMap<BudgetDimension, u32>,
    consumed: BTreeMap<BudgetDimension, BudgetUsage>,
    exhaustion_events: BTreeSet<(BudgetDimension, Fingerprint)>,
    /// Journaling starts only when a transactional owner takes a checkpoint.
    /// Per-group work ledgers are not rolled back and allocate no journal.
    journal: Option<LedgerJournal>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LedgerCheckpoint {
    owner: u64,
    length: usize,
    last_revision: u64,
}

#[derive(Debug)]
enum LedgerChange {
    Reservation {
        dimension: BudgetDimension,
        event: Fingerprint,
        previous: Option<u32>,
        units: u32,
    },
    Limit {
        dimension: BudgetDimension,
        previous: Option<u32>,
    },
}

#[derive(Debug)]
struct LedgerJournal {
    owner: u64,
    revision: u64,
    changes: Vec<(u64, LedgerChange)>,
}

impl LedgerJournal {
    fn new(owner: u64) -> Self {
        Self {
            owner,
            revision: 0,
            changes: Vec::new(),
        }
    }

    fn record(&mut self, change: LedgerChange) {
        self.revision = self
            .revision
            .checked_add(1)
            .expect("search ledger revision space exhausted");
        self.changes.push((self.revision, change));
    }
}

/// Per-dimension reservations with semantic idempotence and O(1) admission.
///
/// One event may account for many homogeneous units. Keeping the aggregate
/// beside the event map avoids rescanning every previous reservation whenever
/// a query-global group budget admits another node.
#[derive(Debug, Clone, Default)]
struct BudgetUsage {
    /// Refundable reservations for distinct semantic tasks.
    units: u32,
    events: BTreeMap<Fingerprint, u32>,
    /// Actual work is not an idempotent task. Meter one monotone prefix per
    /// originating ledger, not an event per visit. Origin prefixes make
    /// repeated/transitive group merges idempotent without forgetting work
    /// executed before a transaction was rolled back.
    executed_units: u32,
    executed: BTreeMap<u64, u32>,
}

impl BudgetUsage {
    fn total(&self) -> u32 {
        self.units.saturating_add(self.executed_units)
    }
}

impl SearchLedger {
    pub fn new(budget: impl Into<Arc<SearchBudget>>) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT_OWNER: AtomicU64 = AtomicU64::new(1);
        Self {
            owner: NEXT_OWNER
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
                .expect("search ledger identity space exhausted"),
            budget: budget.into(),
            limit_overrides: BTreeMap::new(),
            consumed: BTreeMap::new(),
            exhaustion_events: BTreeSet::new(),
            journal: None,
        }
    }

    /// Mandatory baseline work never consumes optional credit.
    pub const fn admit_mandatory(&self) -> BudgetDecision {
        BudgetDecision::Allowed
    }

    pub fn admit_optional(
        &mut self,
        dimension: BudgetDimension,
        event: Fingerprint,
    ) -> BudgetDecision {
        self.admit_optional_units(dimension, event, 1)
    }

    pub fn admit_optional_units(
        &mut self,
        dimension: BudgetDimension,
        event: Fingerprint,
        units: u32,
    ) -> BudgetDecision {
        if units == 0 {
            return BudgetDecision::Allowed;
        }
        let Some(limit) = self.limit(dimension) else {
            return BudgetDecision::Unconfigured;
        };
        let usage = self.consumed.entry(dimension).or_default();
        let previous = usage.events.get(&event).copied().unwrap_or(0);
        if units <= previous {
            return BudgetDecision::Duplicate;
        }
        // An incrementally rediscovered semantic event may expose a larger
        // homogeneous batch. Charge only the newly observed suffix; silently
        // treating it as a duplicate would make the budget non-conservative.
        let additional = units - previous;
        if additional > limit.saturating_sub(usage.total()) || usage.total() > limit {
            self.exhaustion_events.insert((dimension, event));
            return BudgetDecision::Exhausted;
        }
        if let Some(journal) = &mut self.journal {
            journal.record(LedgerChange::Reservation {
                dimension,
                event,
                previous: usage.events.get(&event).copied(),
                units: usage.units,
            });
        }
        usage.events.insert(event, units);
        usage.units = usage.units.saturating_add(additional);
        BudgetDecision::Allowed
    }

    /// Admit real traversal/allocation work before it occurs. Each call is a
    /// new occurrence; it cannot be refunded or deduplicated like a candidate
    /// reservation. Only a rejected admission needs a diagnostic identity.
    /// The callback keeps stable omission evidence out of the admitted hot path.
    pub fn admit_executed_work(
        &mut self,
        dimension: BudgetDimension,
        units: u32,
        exhaustion_event: impl FnOnce(u32) -> Fingerprint,
    ) -> BudgetDecision {
        if units == 0 {
            return BudgetDecision::Allowed;
        }
        let Some(limit) = self.limit(dimension) else {
            return BudgetDecision::Unconfigured;
        };
        let usage = self.consumed.entry(dimension).or_default();
        let total = usage.total();
        if units > limit.saturating_sub(total) || total > limit {
            self.exhaustion_events
                .insert((dimension, exhaustion_event(total)));
            return BudgetDecision::Exhausted;
        }
        let prefix = usage.executed.entry(self.owner).or_default();
        *prefix = prefix.saturating_add(units);
        usage.executed_units = usage.executed_units.saturating_add(units);
        BudgetDecision::Allowed
    }

    pub fn consumed(&self, dimension: BudgetDimension) -> usize {
        self.consumed
            .get(&dimension)
            .map_or(0, |usage| usage.total() as usize)
    }

    pub fn set_limit(&mut self, dimension: BudgetDimension, limit: u32) {
        let previous = self.limit_overrides.get(&dimension).copied();
        if previous == Some(limit) {
            return;
        }
        if let Some(journal) = &mut self.journal {
            journal.record(LedgerChange::Limit {
                dimension,
                previous,
            });
        }
        self.limit_overrides.insert(dimension, limit);
    }

    fn limit(&self, dimension: BudgetDimension) -> Option<u32> {
        self.limit_overrides
            .get(&dimension)
            .copied()
            .or_else(|| self.budget.optional_limit(dimension))
    }

    /// Record an omission discovered by a lazy enumerator which stopped
    /// before constructing the rejected semantic event. This is completion
    /// evidence only; it cannot consume or manufacture optional credit.
    pub fn record_budget_limited(&mut self, dimension: BudgetDimension, witness: Fingerprint) {
        self.exhaustion_events.insert((dimension, witness));
    }

    /// Release a provisional optional admission that did not make any Memo
    /// state reachable.  Transformations reserve their output slot before
    /// running because their context is append-only; a no-op firing must not
    /// permanently consume that slot.
    pub fn release_optional_reservation(
        &mut self,
        dimension: BudgetDimension,
        event: Fingerprint,
    ) -> bool {
        self.consumed.get_mut(&dimension).is_some_and(|usage| {
            let Some(units) = usage.events.remove(&event) else {
                return false;
            };
            if let Some(journal) = &mut self.journal {
                journal.record(LedgerChange::Reservation {
                    dimension,
                    event,
                    previous: Some(units),
                    units: usage.units,
                });
            }
            usage.units = usage.units.saturating_sub(units);
            true
        })
    }

    pub fn merge_from(&mut self, other: &Self) {
        for (&dimension, usage) in &other.consumed {
            let target = self.consumed.entry(dimension).or_default();
            for (&owner, &prefix) in &usage.executed {
                let existing = target.executed.entry(owner).or_default();
                if prefix > *existing {
                    target.executed_units =
                        target.executed_units.saturating_add(prefix - *existing);
                    *existing = prefix;
                }
            }
            for (&event, &units) in &usage.events {
                let existing = target.events.entry(event).or_default();
                if units > *existing {
                    if let Some(journal) = &mut self.journal {
                        journal.record(LedgerChange::Reservation {
                            dimension,
                            event,
                            previous: (*existing > 0).then_some(*existing),
                            units: target.units,
                        });
                    }
                    target.units = target.units.saturating_add(units - *existing);
                    *existing = units;
                }
            }
        }
        for (&dimension, &limit) in &other.limit_overrides {
            self.set_limit(
                dimension,
                self.limit_overrides
                    .get(&dimension)
                    .map_or(limit, |existing| (*existing).min(limit)),
            );
        }
        self.exhaustion_events
            .extend(other.exhaustion_events.iter().copied());
    }

    /// Restore reservations to a transactional savepoint while retaining
    /// evidence that the abandoned attempt reached a search boundary.
    pub(crate) fn checkpoint(&mut self) -> LedgerCheckpoint {
        let journal = self
            .journal
            .get_or_insert_with(|| LedgerJournal::new(self.owner));
        LedgerCheckpoint {
            owner: journal.owner,
            length: journal.changes.len(),
            last_revision: journal.changes.last().map_or(0, |(revision, _)| *revision),
        }
    }

    pub(crate) fn rollback_to_preserving_exhaustion(
        &mut self,
        savepoint: LedgerCheckpoint,
    ) -> paro_common::error::Result<()> {
        let valid = self.journal.as_ref().is_some_and(|journal| {
            journal.owner == savepoint.owner
                && journal.changes.len() >= savepoint.length
                && (savepoint.length == 0
                    || journal.changes[savepoint.length - 1].0 == savepoint.last_revision)
        });
        if !valid {
            return Err(paro_common::error::internal(
                "stale or foreign search ledger checkpoint",
            ));
        }
        let journal = self.journal.as_mut().expect("validated ledger journal");
        while journal.changes.len() > savepoint.length {
            let (_, change) = journal.changes.pop().expect("journal length checked");
            match change {
                LedgerChange::Reservation {
                    dimension,
                    event,
                    previous,
                    units,
                } => {
                    let usage = self.consumed.entry(dimension).or_default();
                    usage.units = units;
                    match previous {
                        Some(value) => {
                            usage.events.insert(event, value);
                        }
                        None => {
                            usage.events.remove(&event);
                        }
                    }
                }
                LedgerChange::Limit {
                    dimension,
                    previous,
                } => match previous {
                    Some(limit) => {
                        self.limit_overrides.insert(dimension, limit);
                    }
                    None => {
                        self.limit_overrides.remove(&dimension);
                    }
                },
            }
        }
        Ok(())
    }

    pub fn exhaustion_events(&self) -> impl Iterator<Item = &(BudgetDimension, Fingerprint)> {
        self.exhaustion_events.iter()
    }

    /// Fast completion check used by recursive search. Callers that need
    /// audit details must still consume [`Self::exhaustion_events`]; the
    /// optimizer hot path only needs to know whether any omission witness
    /// exists.
    pub(crate) fn has_exhaustion_events(&self) -> bool {
        !self.exhaustion_events.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mandatory_work_is_never_rejected_by_optional_budget() {
        let mut budget = SearchBudget::default();
        budget.max_optional_groups_per_initial_group = 0;
        let ledger = SearchLedger::new(budget);
        assert_eq!(ledger.admit_mandatory(), BudgetDecision::Allowed);
    }

    #[test]
    fn duplicate_event_does_not_consume_twice() {
        let mut budget = SearchBudget::default();
        budget.max_search_candidates = 1;
        let mut ledger = SearchLedger::new(budget);
        let event = Fingerprint(7);
        assert_eq!(
            ledger.admit_optional(BudgetDimension::SearchCandidate, event),
            BudgetDecision::Allowed
        );
        assert_eq!(
            ledger.admit_optional(BudgetDimension::SearchCandidate, event),
            BudgetDecision::Duplicate
        );
        assert_eq!(
            ledger.admit_optional(BudgetDimension::SearchCandidate, Fingerprint(8)),
            BudgetDecision::Exhausted
        );
    }

    #[test]
    fn dynamic_group_pool_is_not_misreported_as_zero_credit() {
        let mut ledger = SearchLedger::new(SearchBudget::default());
        assert_eq!(
            ledger.admit_optional(BudgetDimension::Group, Fingerprint(1)),
            BudgetDecision::Unconfigured
        );
        assert!(ledger.exhaustion_events().next().is_none());

        ledger.set_limit(BudgetDimension::Group, 0);
        assert_eq!(
            ledger.admit_optional(BudgetDimension::Group, Fingerprint(1)),
            BudgetDecision::Exhausted
        );
    }

    #[test]
    fn ledger_merge_is_a_deduplicating_union() {
        let budget = SearchBudget::default();
        let mut left = SearchLedger::new(budget.clone());
        let mut right = SearchLedger::new(budget);
        left.admit_optional_units(BudgetDimension::RuleFirePerGroup, Fingerprint(1), 2);
        right.admit_optional_units(BudgetDimension::RuleFirePerGroup, Fingerprint(1), 3);
        right.admit_optional_units(BudgetDimension::RuleFirePerGroup, Fingerprint(2), 2);
        left.merge_from(&right);
        assert_eq!(left.consumed(BudgetDimension::RuleFirePerGroup), 5);
    }

    #[test]
    fn unused_reservation_returns_optional_credit() {
        let mut budget = SearchBudget::default();
        budget.max_optional_logical_exprs_per_group = 1;
        let mut ledger = SearchLedger::new(budget);
        let first = Fingerprint(1);
        let second = Fingerprint(2);
        assert_eq!(
            ledger.admit_optional(BudgetDimension::LogicalExprPerGroup, first),
            BudgetDecision::Allowed
        );
        assert!(ledger.release_optional_reservation(BudgetDimension::LogicalExprPerGroup, first));
        assert_eq!(
            ledger.admit_optional(BudgetDimension::LogicalExprPerGroup, second),
            BudgetDecision::Allowed
        );
    }

    #[test]
    fn optional_combination_prefix_is_not_whole_expression_atomic_admission() {
        // Mandatory baseline work is outside this optional credit. Replacing
        // two tuple events with one expression event cannot preserve admission
        // at the budget boundary, even when both tuples have frozen inputs.
        let mut budget = SearchBudget::default();
        budget.max_child_frontier_combinations_per_group = 1;
        let mut tuples = SearchLedger::new(budget.clone());
        let mut batch = SearchLedger::new(budget);
        let dimension = BudgetDimension::ChildFrontierCombination;
        assert_eq!(
            tuples.admit_optional(dimension, Fingerprint(1)),
            BudgetDecision::Allowed
        );
        assert_eq!(
            tuples.admit_optional(dimension, Fingerprint(2)),
            BudgetDecision::Exhausted
        );
        assert_eq!(
            batch.admit_optional_units(dimension, Fingerprint(3), 2),
            BudgetDecision::Exhausted
        );
        assert_eq!(tuples.consumed(dimension), 1);
        assert_eq!(batch.consumed(dimension), 0);
    }

    #[test]
    fn batch_admission_is_atomic_and_idempotent() {
        let mut budget = SearchBudget::default();
        budget.max_rule_work_units_per_group = 4;
        let mut ledger = SearchLedger::new(budget);
        let batch = Fingerprint(10);
        assert_eq!(
            ledger.admit_optional_units(BudgetDimension::RuleWorkPerGroup, batch, 2),
            BudgetDecision::Allowed
        );
        assert_eq!(
            ledger.admit_optional_units(BudgetDimension::RuleWorkPerGroup, batch, 2),
            BudgetDecision::Duplicate
        );
        assert_eq!(
            ledger.admit_optional_units(BudgetDimension::RuleWorkPerGroup, batch, 3),
            BudgetDecision::Allowed
        );
        assert_eq!(ledger.consumed(BudgetDimension::RuleWorkPerGroup), 3);
        assert_eq!(
            ledger.admit_optional_units(BudgetDimension::RuleWorkPerGroup, Fingerprint(11), 2,),
            BudgetDecision::Exhausted
        );
        assert_eq!(ledger.consumed(BudgetDimension::RuleWorkPerGroup), 3);
        assert!(ledger.release_optional_reservation(BudgetDimension::RuleWorkPerGroup, batch));
        assert_eq!(ledger.consumed(BudgetDimension::RuleWorkPerGroup), 0);
        assert_eq!(
            ledger.admit_optional_units(BudgetDimension::RuleWorkPerGroup, Fingerprint(11), 1,),
            BudgetDecision::Allowed
        );
    }

    #[test]
    fn actual_work_is_metered_without_events_or_refundable_credit() {
        let dimension = BudgetDimension::RuleWorkPerGroup;
        let mut ledger = SearchLedger::new(SearchBudget::default());
        ledger.set_limit(dimension, 10);
        let origin = ledger.checkpoint();
        for _ in 0..3 {
            assert_eq!(
                ledger.admit_executed_work(dimension, 2, |_| panic!("allowed work has no event")),
                BudgetDecision::Allowed
            );
        }
        assert_eq!(ledger.consumed(dimension), 6);
        assert_eq!(ledger.consumed[&dimension].executed.len(), 1);
        assert!(ledger.consumed[&dimension].events.is_empty());
        assert_eq!(ledger.checkpoint().length, origin.length);
        assert_eq!(
            ledger.admit_optional_units(dimension, Fingerprint(1), 3),
            BudgetDecision::Allowed
        );
        assert_eq!(ledger.consumed(dimension), 9);
        assert_eq!(
            ledger.admit_executed_work(dimension, 2, |used| Fingerprint(u128::from(used))),
            BudgetDecision::Exhausted
        );
        assert_eq!(ledger.consumed(dimension), 9);
        ledger.rollback_to_preserving_exhaustion(origin).unwrap();
        assert_eq!(ledger.consumed(dimension), 6);
        assert_eq!(
            ledger.exhaustion_events().copied().collect::<Vec<_>>(),
            vec![(dimension, Fingerprint(9))]
        );
        assert!(!ledger.release_optional_reservation(dimension, Fingerprint(1)));
        assert_eq!(
            ledger.admit_executed_work(dimension, 4, |_| panic!("exact fit")),
            BudgetDecision::Allowed
        );
        assert_eq!(
            ledger.admit_optional(dimension, Fingerprint(2)),
            BudgetDecision::Exhausted
        );
    }

    #[test]
    fn work_prefix_union_survives_merge_cycles_and_rollback() {
        let dimension = BudgetDimension::RuleWorkPerGroup;
        let mut left = SearchLedger::new(SearchBudget::default());
        let mut right = SearchLedger::new(SearchBudget::default());
        let mut third = SearchLedger::new(SearchBudget::default());
        assert_ne!(left.owner, right.owner);
        left.admit_executed_work(dimension, 3, |_| panic!("enough credit"));
        right.admit_executed_work(dimension, 4, |_| panic!("enough credit"));
        let checkpoint = left.checkpoint();
        left.merge_from(&right);
        left.merge_from(&right);
        assert_eq!(left.consumed(dimension), 7);
        third.merge_from(&left);
        third.merge_from(&right);
        assert_eq!(third.consumed(dimension), 7);
        right.admit_executed_work(dimension, 1, |_| panic!("enough credit"));
        left.merge_from(&right);
        assert_eq!(left.consumed(dimension), 8);
        left.admit_executed_work(dimension, 1, |_| panic!("enough credit"));
        right.merge_from(&left);
        assert_eq!(right.consumed(dimension), 9);
        right.admit_executed_work(dimension, 1, |_| panic!("enough credit"));
        third.merge_from(&right);
        left.merge_from(&third);
        assert_eq!(left.consumed(dimension), 10);
        left.rollback_to_preserving_exhaustion(checkpoint).unwrap();
        assert_eq!(
            left.consumed(dimension),
            10,
            "physical work cannot be undone"
        );
        assert_eq!(left.consumed[&dimension].executed.len(), 2);
        assert!(left.consumed[&dimension].events.is_empty());
    }

    #[test]
    fn work_admission_matches_an_independent_occurrence_ledger() {
        let dimension = BudgetDimension::RuleWorkPerGroup;
        for limit in [0, 1, 7, 31, u32::MAX] {
            let mut ledger = SearchLedger::new(SearchBudget::default());
            ledger.set_limit(dimension, limit);
            let mut actual = 0_u64;
            let mut reservations = BTreeMap::<u128, u32>::new();
            for step in 0..150_u32 {
                let key = u128::from(step % 11);
                let units = if step == 149 { u32::MAX } else { step % 5 + 1 };
                if step % 3 == 0 {
                    ledger.release_optional_reservation(dimension, Fingerprint(key));
                    reservations.remove(&key);
                } else {
                    let reserved: u64 = reservations.values().map(|value| u64::from(*value)).sum();
                    let work = step % 3 == 1;
                    let old = if work {
                        0
                    } else {
                        reservations.get(&key).copied().unwrap_or(0)
                    };
                    let extra = u64::from(units.saturating_sub(old));
                    let expected = if units <= old {
                        BudgetDecision::Duplicate
                    } else if actual + reserved + extra > u64::from(limit) {
                        BudgetDecision::Exhausted
                    } else {
                        BudgetDecision::Allowed
                    };
                    let decision = if work {
                        ledger.admit_executed_work(dimension, units, |_| Fingerprint(key))
                    } else {
                        ledger.admit_optional_units(dimension, Fingerprint(key), units)
                    };
                    assert_eq!(decision, expected, "limit {limit}, step {step}");
                    if decision == BudgetDecision::Allowed {
                        if work {
                            actual += extra;
                        } else {
                            reservations.insert(key, units);
                        }
                    }
                }
                let total = actual
                    + reservations
                        .values()
                        .map(|value| u64::from(*value))
                        .sum::<u64>();
                assert_eq!(ledger.consumed(dimension) as u64, total);
            }
        }
    }

    #[test]
    fn disable_rules_by_public_name_rejects_magic_or_unknown_values() {
        let mut budget = SearchBudget::default();
        budget
            .disable_transformations_by_name("join_elimination, late_payload_fetch")
            .unwrap();
        assert!(!budget.transformation_enabled(super::super::rules::JOIN_ELIMINATION_RULE));
        assert!(!budget.transformation_enabled(super::super::rules::LATE_PAYLOAD_FETCH_RULE));
        assert!(budget.disable_transformations_by_name("10011").is_err());
        assert!(budget.disable_transformations_by_name("missing").is_err());
    }

    #[test]
    fn nested_ledger_checkpoints_restore_changes_not_exhaustion() {
        let dimension = BudgetDimension::RuleWorkPerGroup;
        let mut ledger = SearchLedger::new(SearchBudget::default());
        ledger.set_limit(dimension, 8);
        ledger.admit_optional_units(dimension, Fingerprint(1), 2);
        assert!(ledger.journal.is_none());
        let outer = ledger.checkpoint();
        ledger.admit_optional_units(dimension, Fingerprint(1), 4);
        let inner = ledger.checkpoint();
        ledger.release_optional_reservation(dimension, Fingerprint(1));
        ledger.admit_optional_units(dimension, Fingerprint(2), 8);
        assert_eq!(
            ledger.admit_optional(dimension, Fingerprint(3)),
            BudgetDecision::Exhausted
        );
        ledger.set_limit(dimension, 20);
        ledger.rollback_to_preserving_exhaustion(inner).unwrap();
        assert_eq!(ledger.consumed(dimension), 4);
        assert_eq!(ledger.limit(dimension), Some(8));
        assert_eq!(ledger.consumed[&dimension].events.len(), 1);
        assert_eq!(ledger.consumed[&dimension].events[&Fingerprint(1)], 4);
        ledger.rollback_to_preserving_exhaustion(outer).unwrap();
        assert_eq!(ledger.consumed(dimension), 2);
        assert_eq!(ledger.consumed[&dimension].events[&Fingerprint(1)], 2);
        assert_eq!(ledger.exhaustion_events().count(), 1);
    }

    #[test]
    fn ledger_checkpoint_cannot_alias_a_rolled_back_branch_or_another_ledger() {
        let dimension = BudgetDimension::RuleWorkPerGroup;
        let mut ledger = SearchLedger::new(SearchBudget::default());
        let root = ledger.checkpoint();
        ledger.admit_optional(dimension, Fingerprint(1));
        let abandoned = ledger.checkpoint();
        ledger.rollback_to_preserving_exhaustion(root).unwrap();
        ledger.admit_optional(dimension, Fingerprint(2));
        assert!(ledger.rollback_to_preserving_exhaustion(abandoned).is_err());
        assert_eq!(ledger.consumed(dimension), 1);
        assert!(ledger.consumed[&dimension]
            .events
            .contains_key(&Fingerprint(2)));

        let mut other = SearchLedger::new(SearchBudget::default());
        let foreign = other.checkpoint();
        assert!(ledger.rollback_to_preserving_exhaustion(foreign).is_err());
        ledger.rollback_to_preserving_exhaustion(root).unwrap();
    }

    #[test]
    fn ledger_merge_is_transactional_and_checkpoints_do_not_copy_the_prefix() {
        let dimension = BudgetDimension::RuleWorkPerGroup;
        let mut ledger = SearchLedger::new(SearchBudget::default());
        ledger.set_limit(dimension, 10_000);
        let origin = ledger.checkpoint();
        for event in 0..1_000 {
            ledger.admit_optional(dimension, Fingerprint(event));
        }
        for _ in 0..1_000 {
            assert_eq!(ledger.checkpoint().length, 1_000);
        }
        let checkpoint = ledger.checkpoint();
        let mut other = SearchLedger::new(SearchBudget::default());
        other.set_limit(dimension, 2_000);
        other.admit_optional_units(dimension, Fingerprint(0), 3);
        other.admit_optional_units(dimension, Fingerprint(1_001), 5);
        other.record_budget_limited(dimension, Fingerprint(2_000));
        ledger.merge_from(&other);
        assert_eq!(ledger.consumed(dimension), 1_007);
        assert_eq!(ledger.limit(dimension), Some(2_000));
        ledger
            .rollback_to_preserving_exhaustion(checkpoint)
            .unwrap();
        assert_eq!(ledger.consumed(dimension), 1_000);
        assert_eq!(ledger.limit(dimension), Some(10_000));
        assert_eq!(ledger.consumed[&dimension].events[&Fingerprint(0)], 1);
        assert!(!ledger.consumed[&dimension]
            .events
            .contains_key(&Fingerprint(1_001)));
        assert_eq!(ledger.exhaustion_events().count(), 1);
        ledger.rollback_to_preserving_exhaustion(origin).unwrap();
        assert_eq!(ledger.consumed(dimension), 0);
    }
}
