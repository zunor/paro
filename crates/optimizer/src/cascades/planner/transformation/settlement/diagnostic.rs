// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Opt-in counterfactual over LIVE entries in this same statement cache.
//! This is not algebraic equivalence, a second cache, or a production key.
use super::*;
use crate::diagnostics::work::MissKind;
use paro_planner::plan::CardinalityProvenance;

fn same_local_content(a: &LocalKey, b: &LocalKey, cte_reference: bool) -> bool {
    if a.operator != b.operator
        || a.scalars != b.scalars
        || a.output != b.output
        || a.inputs != b.inputs
        || a.cte != b.cte
    {
        return false;
    }
    if cte_reference
        || a.input_stats.cardinality_provenance == CardinalityProvenance::JoinGraph
        || b.input_stats.cardinality_provenance == CardinalityProvenance::JoinGraph
    {
        return a.input_stats == b.input_stats;
    }
    // local() invalidates structural keys, re-estimates cardinality, and
    // assigns Statistics provenance. Risk is NOT overwritten. CTERef may
    // retain its estimate when no lexical producer estimate exists; excluded
    // above, as are region-owned JoinGraph estimates. All input FactIds remain
    // exact, value-interned contracts, including NDV and evidence provenance.
    a.input_stats.materialization_risk_cardinality == b.input_stats.materialization_risk_cardinality
}

impl SettlementCache {
    pub(super) fn classify_local_miss(
        &self,
        key: &LocalKey,
        operator: &LogicalOperator<()>,
        arena: &LogicalPlanArena,
    ) -> MissKind {
        let cte_reference = matches!(operator, LogicalOperator::CTERef(_));
        for (index, (existing, entry)) in self.locals.iter().enumerate() {
            if index == 4096 {
                return MissKind::Unverified;
            }
            if !arena.owns(entry.recipe) {
                if existing == key {
                    return MissKind::SameKeyUnresident;
                }
                continue;
            }
            if existing != key && same_local_content(existing, key, cte_reference) {
                return MissKind::SameContentDifferentKey;
            }
        }
        // No reusable match in CURRENT live locals, not "never seen before".
        MissKind::NewContent
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn key() -> LocalKey {
        LocalKey {
            operator: Box::new([1]),
            scalars: Box::new([]),
            output: Box::new([]),
            inputs: Box::new([0]),
            cte: None,
            input_stats: NodeStats::default(),
        }
    }
    #[test]
    fn counterfactual_keeps_context_risk_and_region_estimates() {
        let a = key();
        let mut b = key();
        b.input_stats.estimated_cardinality = Some(CardinalityEstimate::exact(100));
        assert!(same_local_content(&a, &b, false));
        assert!(!same_local_content(&a, &b, true));
        b.input_stats.materialization_risk_cardinality = Some(10);
        assert!(!same_local_content(&a, &b, false));
        b.input_stats.materialization_risk_cardinality = None;
        b.input_stats.cardinality_provenance = CardinalityProvenance::JoinGraph;
        assert!(!same_local_content(&a, &b, false));
        b = key();
        b.inputs = Box::new([1]);
        assert!(!same_local_content(&a, &b, false));
        b = key();
        b.cte = Some((1, 0));
        assert!(!same_local_content(&a, &b, false));
    }
}
