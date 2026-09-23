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
    // Group merging rewrites expression keys even when the candidate's
    // immutable child references and output facts remain unchanged.
    logical_children: Box<[GroupId]>,
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
    pub(super) dag: selected_dag::SelectedDagStore,
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
        dag: &selected_dag::SelectedDag,
        state: &PlannerTransformState,
    ) -> Result<bool> {
        // Observe facts, not the whole payload graph. Mark affected ancestors
        // through the selected incoming edges; unrelated branches retain their
        // properties. Structural DAG ordering/cycle checks are construction work.
        let mut reads = Vec::with_capacity(dag.nodes.len());
        let mut dirty = vec![false; dag.nodes.len()];
        let mut pending = Vec::new();
        for (index, node) in dag.nodes.iter().enumerate() {
            let read = PatternRead::facts_from_group(memo, node.reference.group)?;
            let current = self
                .nodes
                .get(&node.reference.candidate)
                .is_some_and(|previous| {
                    previous.node == *node
                        && previous.read == read
                        && memo.logical_expr(node.logical).is_some_and(|logical| {
                            logical.key.children == previous.logical_children
                        })
                        && previous.children.len() == node.children.len()
                        && node.children.iter().zip(previous.children.iter()).all(
                            |(child, revision)| {
                                self.nodes
                                    .get(&child.candidate)
                                    .is_some_and(|child| child.revision == *revision)
                            },
                        )
                });
            if !current {
                pending.push(index);
            }
            reads.push(read);
        }
        while let Some(index) = pending.pop() {
            if std::mem::replace(&mut dirty[index], true) {
                continue;
            }
            pending.extend(dag.parents[index].iter().copied());
        }
        self.reuses = self
            .reuses
            .saturating_add(dirty.iter().filter(|dirty| !**dirty).count() as u64);
        if !dirty.iter().any(|dirty| *dirty) {
            return Ok(true);
        }
        let nodes = dag.node_map();
        for &index in &dag.postorder {
            if !dirty[index] {
                continue;
            }
            let node = &dag.nodes[index];
            let reference = node.reference;
            let read = reads[index];
            let children = node
                .children
                .iter()
                .map(|child| self.nodes[&child.candidate].revision)
                .collect();
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
            for child in node.children.iter() {
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
                            for partial in join.children.iter() {
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
                    logical_children: logical.key.children.clone(),
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
