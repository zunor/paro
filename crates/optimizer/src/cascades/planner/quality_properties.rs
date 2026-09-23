// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Demand-driven properties of exact selected choices. A node owns its local
//! derivation and child revision vector. New ancestors do not invalidate an
//! unchanged subtree; changed facts and choices do. CTE demand is a separate
//! property of a producer input and its selected incoming consumer edges.

use super::*;

/// The local executable contract is independent of statistics and of the
/// ancestor which asks for it. Exact dependency values, not a fingerprint or
/// an apply audit, guard reuse. Published candidate choices and logical payloads
/// are immutable; Memo expression keys/proofs and implementation availability
/// are not (group merge, proof publication, RF withdrawal and rollback).
#[derive(Debug)]
pub(super) struct LocalContract {
    logical: super::super::memo::LogicalExprKey,
    physical: super::super::memo::PhysicalExprKey,
    owner: LogicalPayloadId,
    payload: Arc<PlannerPhysicalPayload>,
    proofs: BTreeSet<EquivalenceProof>,
    selected_proofs: Box<[EquivalenceProof]>,
    origin: Option<RuleId>,
    implementations: state::PlannerImplementationSet,
    search: Option<(PhysicalPayloadId, Fingerprint)>,
    pub operator: LogicalOperatorType,
    pub rules: BTreeSet<RuleId>,
    pub cte: Option<usize>,
    pub join_region: bool,
    pub runtime_filter: bool,
}

impl LocalContract {
    fn is_current(
        &self,
        logical: &super::super::memo::LogicalExpr,
        physical: &super::super::memo::PhysicalExpr,
        metadata: &PlannerOperatorMetadata,
        payload: &Arc<PlannerPhysicalPayload>,
        join_region: bool,
    ) -> bool {
        self.logical == logical.key
            && self.physical == physical.key
            && self.owner == logical.payload
            && Arc::ptr_eq(&self.payload, payload)
            && self.proofs == logical.proofs
            && self.selected_proofs == metadata.selected_proofs
            && self.origin == metadata.origin_rule
            && self.implementations == metadata.implementations
            && self.operator == metadata.operator_type
            && self.join_region == join_region
            && self.search
                == metadata
                    .search
                    .as_ref()
                    .map(|s| (s.payload, s.payload_fingerprint))
    }

    fn derive(
        logical: &super::super::memo::LogicalExpr,
        physical: &super::super::memo::PhysicalExpr,
        metadata: &PlannerOperatorMetadata,
        payload: Arc<PlannerPhysicalPayload>,
        state: &PlannerTransformState,
        join_region: bool,
    ) -> Option<Self> {
        if !selected_physical_contract_is_exact(logical, physical, metadata, &payload) {
            return None;
        }
        let rules: BTreeSet<_> = selected_payload_rule_proofs(logical, metadata).collect();
        if metadata.origin_rule.is_some() && rules.is_empty() {
            return None;
        }
        let operator = &state
            .payloads
            .logical
            .get(logical.payload.index())?
            .semantic_template
            .operator;
        let cte = match operator {
            LogicalOperator::CTERef(cte) => Some(cte.cte_index),
            LogicalOperator::MaterializedCTE(cte) => Some(cte.cte_index),
            LogicalOperator::RecursiveCTE(cte) => Some(cte.cte_index),
            _ => None,
        };
        Some(Self {
            logical: logical.key.clone(),
            physical: physical.key.clone(),
            owner: logical.payload,
            payload,
            proofs: logical.proofs.clone(),
            selected_proofs: metadata.selected_proofs.clone(),
            origin: metadata.origin_rule,
            implementations: metadata.implementations,
            search: metadata
                .search
                .as_ref()
                .map(|s| (s.payload, s.payload_fingerprint)),
            operator: metadata.operator_type,
            rules,
            cte,
            join_region,
            runtime_filter: matches!(
                physical.key.implementation,
                PLANNER_HASH_JOIN_RUNTIME_FILTER | PLANNER_HASH_JOIN_BUILD_LEFT_RUNTIME_FILTER
            ),
        })
    }
}

#[derive(Debug)]
struct NodeProperties {
    node: QualityCandidateNode,
    contract: LocalContract,
    read: PatternRead,
    children: Box<[u64]>,
    revision: u64,
    choice: Fingerprint,
    aggregate: SelectedAggregateRegionShape,
    contains_union: bool,
    is_union: bool,
    pending_domain: Option<bool>,
    // Only demanded region boundaries receive this scalar certificate, not
    // an expanded transitive closure per node. Replacing a derived node drops
    // all of its revision-dependent certificates.
    region_facts: BTreeMap<OptimizationGoal, Fingerprint>,
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
    pub(super) contract_builds: u64,
    pub(super) contract_reuses: u64,
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
        let mut contracts = Vec::with_capacity(dag.nodes.len());
        for (index, node) in dag.nodes.iter().enumerate() {
            let Some(winner) = memo.resolve_child_winner(node.reference) else {
                return Ok(false);
            };
            let Some(physical) = memo.physical_expr(winner.expression) else {
                return Ok(false);
            };
            let Some(logical) = memo.logical_expr(physical.key.logical) else {
                return Ok(false);
            };
            let Some(metadata) = state.metadata.get(&logical.payload) else {
                return Ok(false);
            };
            let Some(payload) = state.payloads.get_physical(physical.payload) else {
                return Ok(false);
            };
            if physical.id != winner.expression
                || physical.id != node.physical
                || logical.id != node.logical
                || winner.children.as_ref() != node.children.as_ref()
                || winner.provided.result_guarantee != ResultGuarantee::Exact
                || metadata.provided.result_guarantee != ResultGuarantee::Exact
            {
                return Ok(false);
            }
            // Canonical representatives are live dependencies. Do not cache a
            // group redirect, or allocate an expected-group vector per visit.
            let boundaries = winner.joint_cost_proof.as_ref().map(|p| &p.boundary_goals);
            let arity = boundaries.map_or(logical.key.children.len(), |b| b.len());
            if arity != node.children.len() {
                return Ok(false);
            }
            for (i, child) in node.children.iter().enumerate() {
                let expected = boundaries.map_or_else(|| logical.key.children[i], |b| b[i].0);
                if memo.canonical_group(child.group) != memo.canonical_group(expected) {
                    return Ok(false);
                }
            }
            // Every referenced child is also a reachable DAG node and is
            // resolved by this loop before any properties are published.
            let join_region = winner.joint_cost_proof.is_some();
            let contract_current =
                self.nodes
                    .get(&node.reference.candidate)
                    .is_some_and(|previous| {
                        previous.node == *node
                            && previous.contract.is_current(
                                logical,
                                physical,
                                metadata,
                                &payload,
                                join_region,
                            )
                    });
            let contract = if contract_current {
                self.contract_reuses = self.contract_reuses.saturating_add(1);
                None
            } else {
                let Some(contract) =
                    LocalContract::derive(logical, physical, metadata, payload, state, join_region)
                else {
                    return Ok(false);
                };
                self.contract_builds = self.contract_builds.saturating_add(1);
                Some(contract)
            };
            let read = PatternRead::facts_from_group(memo, node.reference.group)?;
            let current = contract_current
                && self
                    .nodes
                    .get(&node.reference.candidate)
                    .is_some_and(|previous| {
                        previous.node == *node
                            && previous.read == read
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
            contracts.push(contract);
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
            let choice = if contracts[index].is_none() {
                self.nodes[&reference.candidate].choice
            } else {
                candidate_choice_fingerprint(reference, winner, logical, physical)
            };
            let contract = contracts[index]
                .take()
                .or_else(|| {
                    self.nodes
                        .remove(&reference.candidate)
                        .map(|previous| previous.contract)
                })
                .ok_or_else(|| paro_error::internal("selected local contract missing"))?;
            self.nodes.insert(
                reference.candidate,
                NodeProperties {
                    node: node.clone(),
                    contract,
                    read,
                    children,
                    revision,
                    choice,
                    aggregate,
                    contains_union,
                    is_union,
                    pending_domain,
                    region_facts: BTreeMap::new(),
                },
            );
            self.builds = self.builds.saturating_add(1);
        }
        Ok(true)
    }

    pub(super) fn revision(&self, candidate: CandidateId) -> u64 {
        self.nodes[&candidate].revision
    }

    pub(super) fn contract(&self, candidate: CandidateId) -> &LocalContract {
        &self.nodes[&candidate].contract
    }

    pub(super) fn fact_reads(&self, nodes: &[QualityCandidateNode]) -> ReadSet {
        // These reads were checked by refresh under this same immutable Memo
        // borrow. Re-reading group facts cannot strengthen the certificate.
        ReadSet::new(
            nodes
                .iter()
                .map(|node| self.nodes[&node.reference.candidate].read),
        )
    }

    pub(super) fn region_fact_fingerprint(
        &mut self,
        memo: &Memo,
        root: ChildWinnerRef,
        goal: OptimizationGoal,
        nodes: &selected_dag::QualityNodeMap<'_>,
    ) -> Option<Fingerprint> {
        if let Some(fingerprint) = self.nodes.get(&root.candidate)?.region_facts.get(&goal) {
            return Some(*fingerprint);
        }
        // An arm enumerated from this root is already in its closure. The
        // independent oracle still walks root + arm to check this equivalence.
        let fingerprint = collect_quality_region_fact_fingerprint(memo, root, root, goal, nodes)?;
        self.nodes
            .get_mut(&root.candidate)?
            .region_facts
            .insert(goal, fingerprint);
        Some(fingerprint)
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
