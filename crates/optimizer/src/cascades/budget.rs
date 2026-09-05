// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Deterministic multidimensional search budgets.

use std::collections::{BTreeMap, BTreeSet};

use super::ids::{Fingerprint, RuleId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BudgetDimension {
    Group,
    LogicalExprPerGroup,
    PhysicalExprPerGroup,
    InterestingGoalPerGroup,
    RuleFirePerGroup,
    RuleWorkPerGroup,
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
}

impl BudgetDimension {
    pub const fn stable_name(self) -> &'static str {
        match self {
            Self::Group => "group",
            Self::LogicalExprPerGroup => "logical_expr_per_group",
            Self::PhysicalExprPerGroup => "physical_expr_per_group",
            Self::InterestingGoalPerGroup => "interesting_goal_per_group",
            Self::RuleFirePerGroup => "rule_fire_per_group",
            Self::RuleWorkPerGroup => "rule_work_per_group",
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
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchBudget {
    /// Emergency isolation surface for a faulty optional equivalence rule.
    /// Mandatory normalization and baseline implementations are not rules and
    /// cannot be disabled through this set.
    pub disabled_transformation_rules: BTreeSet<RuleId>,
    pub max_optional_groups: u32,
    pub max_optional_logical_exprs_per_group: u32,
    pub max_optional_physical_exprs_per_group: u32,
    pub max_optional_interesting_goals_per_group: u32,
    pub max_rule_firings_per_group: u32,
    /// Maximum number of logical group visits performed by optional rule
    /// materialization for one target group. This bounds legacy whole-region
    /// rules until each is expressed entirely as local Memo operands.
    pub max_rule_work_units_per_group: u32,
    /// Additional child-frontier combinations costed for a group. The
    /// selected-child baseline is mandatory and does not consume this credit.
    pub max_child_frontier_combinations_per_group: u32,
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
    pub max_pareto_winners_per_goal: u8,
    pub max_grant_classes: u8,
}

impl Default for SearchBudget {
    fn default() -> Self {
        Self {
            disabled_transformation_rules: BTreeSet::new(),
            max_optional_groups: 4_096,
            max_optional_logical_exprs_per_group: 64,
            max_optional_physical_exprs_per_group: 64,
            max_optional_interesting_goals_per_group: 16,
            max_rule_firings_per_group: 256,
            max_rule_work_units_per_group: 65_536,
            max_child_frontier_combinations_per_group: 4_096,
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
            max_pareto_winners_per_goal: 8,
            max_grant_classes: 3,
        }
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

    pub fn optional_limit(&self, dimension: BudgetDimension) -> u32 {
        match dimension {
            BudgetDimension::Group => self.max_optional_groups,
            BudgetDimension::LogicalExprPerGroup => self.max_optional_logical_exprs_per_group,
            BudgetDimension::PhysicalExprPerGroup => self.max_optional_physical_exprs_per_group,
            BudgetDimension::InterestingGoalPerGroup => {
                self.max_optional_interesting_goals_per_group
            }
            BudgetDimension::RuleFirePerGroup => self.max_rule_firings_per_group,
            BudgetDimension::RuleWorkPerGroup => self.max_rule_work_units_per_group,
            BudgetDimension::ChildFrontierCombination => {
                self.max_child_frontier_combinations_per_group
            }
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
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetDecision {
    Allowed,
    Duplicate,
    Exhausted,
}

/// Optional budget consumption is keyed by stable semantic events. Replaying a
/// task or merging groups cannot manufacture fresh credit.
#[derive(Debug, Clone)]
pub struct SearchLedger {
    budget: SearchBudget,
    consumed: BTreeMap<BudgetDimension, BTreeSet<Fingerprint>>,
    exhaustion_events: BTreeSet<(BudgetDimension, Fingerprint)>,
}

impl SearchLedger {
    pub fn new(budget: SearchBudget) -> Self {
        Self {
            budget,
            consumed: BTreeMap::new(),
            exhaustion_events: BTreeSet::new(),
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
        let events = self.consumed.entry(dimension).or_default();
        if events.contains(&event) {
            return BudgetDecision::Duplicate;
        }
        if events.len() as u32 >= self.budget.optional_limit(dimension) {
            self.exhaustion_events.insert((dimension, event));
            return BudgetDecision::Exhausted;
        }
        events.insert(event);
        BudgetDecision::Allowed
    }

    pub fn consumed(&self, dimension: BudgetDimension) -> usize {
        self.consumed.get(&dimension).map_or(0, BTreeSet::len)
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
        self.consumed
            .get_mut(&dimension)
            .is_some_and(|events| events.remove(&event))
    }

    pub fn merge_from(&mut self, other: &Self) {
        for (&dimension, events) in &other.consumed {
            self.consumed
                .entry(dimension)
                .or_default()
                .extend(events.iter().copied());
        }
        self.exhaustion_events
            .extend(other.exhaustion_events.iter().copied());
    }

    pub fn exhaustion_events(&self) -> impl Iterator<Item = &(BudgetDimension, Fingerprint)> {
        self.exhaustion_events.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mandatory_work_is_never_rejected_by_optional_budget() {
        let mut budget = SearchBudget::default();
        budget.max_optional_groups = 0;
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
    fn ledger_merge_is_a_deduplicating_union() {
        let budget = SearchBudget::default();
        let mut left = SearchLedger::new(budget.clone());
        let mut right = SearchLedger::new(budget);
        left.admit_optional(BudgetDimension::RuleFirePerGroup, Fingerprint(1));
        right.admit_optional(BudgetDimension::RuleFirePerGroup, Fingerprint(1));
        right.admit_optional(BudgetDimension::RuleFirePerGroup, Fingerprint(2));
        left.merge_from(&right);
        assert_eq!(left.consumed(BudgetDimension::RuleFirePerGroup), 2);
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
}
