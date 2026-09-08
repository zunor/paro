// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Exact, non-owning shortcut for repeated immutable operator occurrences.
//!
//! The canonical scalar/fact cache remains authoritative. This layer skips
//! canonical lowering when the very same immutable scalar allocations, local
//! operator fields, output layout, and input evidence have already been seen.
//! Allocation identities never participate in Memo semantics or rule order.

use super::*;
use paro_planner::expression::{ExpressionIdentity, ExpressionIdentityWitness};

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct OccurrenceKey {
    operator: Box<[u8]>,
    roots: Box<[ExpressionIdentity]>,
    output: LogicalOutputLayout,
    inputs: Box<[FactId]>,
    input_stats: NodeStats,
}

#[derive(Debug)]
pub(super) struct PendingOccurrence {
    key: OccurrenceKey,
    witnesses: Box<[ExpressionIdentityWitness]>,
}

#[derive(Debug)]
struct CachedOccurrence {
    witnesses: Box<[ExpressionIdentityWitness]>,
    local: SettledLocal,
}

impl CachedOccurrence {
    fn is_live(&self, arena: &LogicalPlanArena) -> bool {
        arena.owns(self.local.recipe)
            && self
                .witnesses
                .iter()
                .all(ExpressionIdentityWitness::is_live)
    }
}

#[derive(Debug, Default)]
pub(super) struct OccurrenceCache {
    entries: HashMap<OccurrenceKey, CachedOccurrence>,
    next_sweep: usize,
    pub(super) hits: u64,
}

impl OccurrenceCache {
    pub(super) fn get(
        &mut self,
        key: &OccurrenceKey,
        arena: &LogicalPlanArena,
    ) -> Option<SettledLocal> {
        let entry = self.entries.get(key)?;
        if !entry.is_live(arena) {
            self.entries.remove(key);
            return None;
        }
        self.hits += 1;
        Some(entry.local.clone())
    }

    pub(super) fn insert(
        &mut self,
        pending: Option<PendingOccurrence>,
        local: &SettledLocal,
        arena: &LogicalPlanArena,
    ) {
        let Some(pending) = pending else { return };
        // A source shell may have been consumed and its scalar normalized
        // away. Do not retain even the dead weak control block in that case.
        if !pending
            .witnesses
            .iter()
            .all(ExpressionIdentityWitness::is_live)
        {
            return;
        }
        if self.entries.len() >= self.next_sweep {
            self.prune(arena);
        }
        self.entries.insert(
            pending.key,
            CachedOccurrence {
                witnesses: pending.witnesses,
                local: local.clone(),
            },
        );
    }

    pub(super) fn prune(&mut self, arena: &LogicalPlanArena) {
        self.entries.retain(|_, entry| entry.is_live(arena));
        self.next_sweep = self.entries.len().saturating_mul(2).max(64);
    }
}

impl OccurrenceKey {
    pub(super) fn capture(
        shell: &LogicalPlanNode<()>,
        inputs: &[FactId],
        facts: &[RelationFacts],
        scalars: &ScalarArena,
    ) -> Result<Option<Self>> {
        // This whitelist is a contract: these operators' non-scalar semantic
        // fields are completely encoded by query_operator_identity, and all
        // scalar roots are covered by the shared visitor. CTE references read
        // a lexical producer environment; joins also synthesize comparison
        // scalar roots. They use the canonical path until their full local
        // dependency key is represented here.
        if !matches!(
            shell.operator,
            LogicalOperator::Filter(_)
                | LogicalOperator::Projection(_)
                | LogicalOperator::Limit(_)
                | LogicalOperator::Order(_)
                | LogicalOperator::TopN(_)
                | LogicalOperator::Distinct(_)
                | LogicalOperator::Aggregate(_)
        ) || matches!(&shell.operator, LogicalOperator::Aggregate(aggregate)
                if !matches!(aggregate.group_input_multiplicity,
                    paro_planner::operator::GroupInputMultiplicity::Arbitrary))
        {
            return Ok(None);
        }
        let mut roots = Vec::new();
        let mut cacheable = true;
        paro_planner::visitor::enumerate_expression_refs(&shell.operator, |root| {
            cacheable &= !root.evaluation_properties().is_reorder_fence();
            roots.push(root.allocation_identity());
        });
        if roots.is_empty() || !cacheable {
            return Ok(None);
        }
        let inputs_layout = inputs
            .iter()
            .map(|id| &facts[*id].layout)
            .collect::<Vec<_>>();
        Ok(Some(Self {
            operator: query_operator_identity(&shell.operator, &[], scalars)?.1,
            roots: roots.into(),
            // Projection bindings/types are intentionally absent from the
            // alpha-invariant Memo operator encoding. Here they are exact
            // local facts, including every projection map and output alias.
            output: shell.operator.output_layout_from_child_refs(&inputs_layout),
            inputs: inputs.into(),
            input_stats: shell.stats.clone(),
        }))
    }

    pub(super) fn witnessed(self, operator: &LogicalOperator<()>) -> PendingOccurrence {
        let mut witnesses = Vec::with_capacity(self.roots.len());
        paro_planner::visitor::enumerate_expression_refs(operator, |root| {
            witnesses.push(root.identity_witness());
        });
        PendingOccurrence {
            key: self,
            witnesses: witnesses.into(),
        }
    }
}
