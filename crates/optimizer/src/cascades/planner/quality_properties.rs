// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Demand-driven properties of exact selected choices. A node owns its local
//! derivation and child revision vector. New ancestors do not invalidate an
//! unchanged subtree; changed facts and choices do. CTE demand is a separate
//! property of a producer input and its selected incoming consumer edges.

use super::*;

#[derive(Debug)]
struct NodeProperties {
    node: QualityCandidateNode,
    read: PatternRead,
    children: Box<[u64]>,
    revision: u64,
    choice: Fingerprint,
    aggregate: SelectedAggregateRegionShape,
    contains_union: bool,
    is_union: bool,
    pending_domain: Option<bool>,
}

/// No frontier membership or cost is stored here. Candidate identities are
/// query-local and never reused; retained entries are bounded by the same
/// winner archive which owns the exact choices. A failed derivation is not a
/// completed property, and does not publish a partially refreshed node.
#[derive(Debug, Default)]
pub(super) struct SelectedQualityProperties {
    nodes: BTreeMap<CandidateId, NodeProperties>,
    next_revision: u64,
    pub(super) cte_domains: quality_domain::CteDomainProperties,
    pub(super) builds: u64,
    pub(super) reuses: u64,
}

impl SelectedQualityProperties {
    pub(super) fn refresh(
        &mut self,
        memo: &Memo,
        root: ChildWinnerRef,
        nodes: &[QualityCandidateNode],
        state: &PlannerTransformState,
    ) -> Result<bool> {
        let nodes = quality_node_map(nodes);
        // Explicit postorder handles shared DAGs and rejects a cycle rather
        // than overflowing the native stack or reusing an unfinished entry.
        let mut stack = vec![(root, false)];
        let mut active = BTreeSet::new();
        let mut complete = BTreeSet::new();
        while let Some((reference, exit)) = stack.pop() {
            let Some(node) = quality_node(&nodes, reference) else {
                return Ok(false);
            };
            if complete.contains(&reference.candidate) {
                continue;
            }
            if !exit {
                if !active.insert(reference.candidate) {
                    return Ok(false);
                }
                stack.push((reference, true));
                stack.extend(node.children.iter().rev().map(|child| (*child, false)));
                continue;
            }
            active.remove(&reference.candidate);
            let read = PatternRead::facts_from_group(memo, reference.group)?;
            let children = node
                .children
                .iter()
                .map(|child| self.nodes[&child.candidate].revision)
                .collect::<Box<[_]>>();
            if self
                .nodes
                .get(&reference.candidate)
                .is_some_and(|previous| {
                    previous.node == *node && previous.read == read && previous.children == children
                })
            {
                self.reuses = self.reuses.saturating_add(1);
                complete.insert(reference.candidate);
                continue;
            }
            let Some(logical) = memo.logical_expr(node.logical) else {
                return Ok(false);
            };
            let Some(physical) = memo.physical_expr(node.physical) else {
                return Ok(false);
            };
            let Some(winner) = memo.resolve_child_winner(reference) else {
                return Ok(false);
            };
            let Some(payload) = state.payloads.logical.get(logical.payload.index()) else {
                return Ok(false);
            };
            let operator = &payload.semantic_template.operator;
            let mut aggregate = SelectedAggregateRegionShape::default();
            let is_union = matches!(operator, LogicalOperator::SetOperation(setop)
                if setop.setop_type == paro_planner::operator::SetOpType::Union && setop.setop_all);
            let mut contains_union = is_union;
            aggregate.aggregates = u32::from(matches!(operator, LogicalOperator::Aggregate(_)));
            aggregate.joins = u32::from(matches!(operator, LogicalOperator::Join(_)));
            // These counts are used only as existence predicates for a region.
            // Diagnostic occurrence counts are computed from the unique-node
            // inspection, not by summing shared descendants twice.
            for child in &node.children {
                let properties = &self.nodes[&child.candidate];
                aggregate.aggregates |= properties.aggregate.aggregates;
                aggregate.joins |= properties.aggregate.joins;
                aggregate.decomposed |= properties.aggregate.decomposed;
                contains_union |= properties.contains_union;
            }
            if let LogicalOperator::Aggregate(outer) = operator {
                if let [join_ref] = node.children.as_ref() {
                    let Some(join) = quality_node(&nodes, *join_ref) else {
                        return Ok(false);
                    };
                    let operator_of = |node: &QualityCandidateNode| {
                        memo.logical_expr(node.logical)
                            .and_then(|logical| state.payloads.logical.get(logical.payload.index()))
                            .map(|payload| &payload.semantic_template.operator)
                    };
                    let Some(join_operator) = operator_of(join) else {
                        return Ok(false);
                    };
                    if let LogicalOperator::Join(Join::Comparison(join_operator)) = join_operator {
                        if !join_operator.conditions.is_empty() && join.children.len() == 2 {
                            for partial in &join.children {
                                let Some(partial) =
                                    quality_node(&nodes, *partial).and_then(operator_of)
                                else {
                                    return Ok(false);
                                };
                                aggregate.decomposed |= matches!(partial, LogicalOperator::Aggregate(partial)
                                    if aggregate_merge_contract_matches(outer, partial));
                            }
                        }
                    }
                }
            }
            let pending_domain =
                quality_domain::pending_transfer_for_ref(memo, reference, &nodes, state);
            let revision = self
                .next_revision
                .checked_add(1)
                .ok_or_else(|| paro_error::internal("selected property revision exhausted"))?;
            self.next_revision = revision;
            self.nodes.insert(
                reference.candidate,
                NodeProperties {
                    node: node.clone(),
                    read,
                    children,
                    revision,
                    choice: candidate_choice_fingerprint(reference, winner, logical, physical),
                    aggregate,
                    contains_union,
                    is_union,
                    pending_domain,
                },
            );
            self.builds = self.builds.saturating_add(1);
            complete.insert(reference.candidate);
        }
        Ok(true)
    }

    pub(super) fn revision(&self, candidate: CandidateId) -> u64 {
        self.nodes[&candidate].revision
    }

    pub(super) fn choice(&self, candidate: CandidateId) -> Fingerprint {
        self.nodes[&candidate].choice
    }

    pub(super) fn aggregate_shape(&self, candidate: CandidateId) -> SelectedAggregateRegionShape {
        self.nodes[&candidate].aggregate
    }

    pub(super) fn is_union(&self, candidate: CandidateId) -> bool {
        self.nodes[&candidate].is_union
    }

    pub(super) fn contains_union(&self, candidate: CandidateId) -> bool {
        self.nodes[&candidate].contains_union
    }

    pub(super) fn pending_transfers(
        &self,
        nodes: &[QualityCandidateNode],
    ) -> Option<Box<[CandidateId]>> {
        let mut pending = Vec::new();
        for node in nodes {
            if self.nodes[&node.reference.candidate].pending_domain? {
                pending.push(node.reference.candidate);
            }
        }
        pending.sort_unstable();
        Some(pending.into_boxed_slice())
    }
}
