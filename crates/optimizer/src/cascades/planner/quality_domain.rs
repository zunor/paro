// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Outstanding domain transfers in one exact executable candidate.
//!
//! A producer restriction above a wide/reducing boundary is not evidence that
//! its cheap, safely transferable domain reached that boundary's input. This
//! observes choices; it neither constructs a replacement tree nor authorizes
//! moving a predicate. The existing rule remains the semantic authority.

use super::*;

/// Certify the consumer predicate demand against the exact selected producer,
/// not the rule which constructed it. Every selected incoming consumer edge
/// participates; an unfiltered use requires the full domain. This certificate
/// covers predicates, not minimal storage width or join-search completion.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CteDemandKey {
    definition: LogicalExprId,
    producer_input: u64,
    consumers: Box<[(u64, Box<[u64]>)]>,
}

#[derive(Debug, Default)]
pub(super) struct CteDomainProperties {
    results: BTreeMap<CteDemandKey, bool>,
    pub(super) builds: u64,
    pub(super) reuses: u64,
}

#[cfg(test)]
pub(super) fn cte_domain_witnesses(
    memo: &Memo,
    nodes: &[QualityCandidateNode],
    state: &PlannerTransformState,
) -> BTreeSet<usize> {
    cte_domain_witnesses_with_properties(memo, nodes, state, None)
}

pub(super) fn cte_domain_witnesses_with_properties(
    memo: &Memo,
    nodes: &[QualityCandidateNode],
    state: &PlannerTransformState,
    mut properties: Option<&mut quality_properties::SelectedQualityProperties>,
) -> BTreeSet<usize> {
    let map = nodes
        .iter()
        .map(|node| (node.reference.candidate, node))
        .collect::<BTreeMap<_, _>>();
    let mut parents = BTreeMap::<CandidateId, Vec<&QualityCandidateNode>>::new();
    for parent in nodes {
        for child in &parent.children {
            parents.entry(child.candidate).or_default().push(parent);
        }
    }
    let operator = |node: &QualityCandidateNode| {
        memo.logical_expr(node.logical)
            .and_then(|logical| state.payloads.logical.get(logical.payload.index()))
            .map(|payload| &payload.semantic_template.operator)
    };
    let mut witnessed = BTreeSet::new();
    for producer in nodes {
        let Some(LogicalOperator::MaterializedCTE(cte)) = operator(producer) else {
            continue;
        };
        let Some(producer_child) = producer.children.first().copied() else {
            continue;
        };
        // A CTE's demand belongs to the selected incoming consumer edges, not
        // the root candidate or its arrival order. Unrelated new ancestors do
        // not change this key. Revisions cover exact choices and live facts.
        let key = properties.as_ref().map(|properties| {
            let mut consumers: Vec<_> = nodes
                .iter()
                .filter(|node| {
                    matches!(operator(node),
                Some(LogicalOperator::CTERef(reference)) if reference.cte_index == cte.cte_index)
                })
                .map(|consumer| {
                    let mut incoming = parents
                        .get(&consumer.reference.candidate)
                        .into_iter()
                        .flatten()
                        .map(|parent| properties.revision(parent.reference.candidate))
                        .collect::<Vec<_>>();
                    incoming.sort_unstable();
                    (
                        properties.revision(consumer.reference.candidate),
                        incoming.into_boxed_slice(),
                    )
                })
                .collect();
            consumers.sort_unstable();
            CteDemandKey {
                definition: producer.logical,
                producer_input: properties.revision(producer_child.candidate),
                consumers: consumers.into_boxed_slice(),
            }
        });
        if let Some((key, properties)) = key.as_ref().zip(properties.as_mut()) {
            if let Some(covered) = properties.cte_domains.results.get(key).copied() {
                properties.cte_domains.reuses += 1;
                if covered {
                    witnessed.insert(cte.cte_index);
                }
                continue;
            }
            properties.cte_domains.builds += 1;
        }
        let mut references = Vec::new();
        let mut unrestricted = false;
        let mut invalid = false;
        let mut found = false;
        for consumer in nodes {
            let Some(LogicalOperator::CTERef(reference)) = operator(consumer) else {
                continue;
            };
            if reference.cte_index != cte.cte_index {
                continue;
            }
            found = true;
            let mut incoming = false;
            for parent in parents
                .get(&consumer.reference.candidate)
                .into_iter()
                .flatten()
            {
                for child in &parent.children {
                    if *child != consumer.reference {
                        continue;
                    }
                    incoming = true;
                    if let Some(LogicalOperator::Filter(filter)) = operator(parent) {
                        // Filter has one exact input; its output projection
                        // does not change the input namespace of predicates.
                        match crate::cte::normalize::filtered_cte_ref(
                            reference,
                            filter,
                            &cte.output_columns,
                        ) {
                            Some(filtered) => references.push(filtered),
                            None => invalid = true,
                        }
                    } else {
                        unrestricted = true;
                    }
                }
            }
            unrestricted |= !incoming;
        }
        let covered = if !found || invalid {
            false
        } else if unrestricted {
            // No finite union of the other consumers' filters can restrict
            // this producer. No pushdown is required by the predicate policy.
            true
        } else {
            let bindings = cte
                .output_columns
                .iter()
                .map(|column| column.binding)
                .collect::<Vec<_>>();
            if let Some(expected) =
                crate::cte::predicate_domain::derive_producer_predicates(references, &bindings)
            {
                selected_consumes_ref(&map, producer_child, memo, &expected, state)
            } else {
                false
            }
        };
        if let Some((key, properties)) = key.zip(properties.as_mut()) {
            properties.cte_domains.results.insert(key, covered);
        }
        if covered {
            witnessed.insert(cte.cte_index);
        }
    }
    witnessed
}

fn is_column_domain(expression: &Expression) -> bool {
    domain_transfer::is_local_domain(expression)
}

fn selected_is_graph_chain(
    mut node: &FrozenCandidate,
    state: &PlannerTransformState,
) -> Option<bool> {
    loop {
        let operator = &state
            .payloads
            .logical
            .get(node.logical.payload.index())?
            .semantic_template
            .operator;
        match operator {
            LogicalOperator::GraphScan(_) | LogicalOperator::GraphExpand(_) => return Some(true),
            LogicalOperator::Filter(_) | LogicalOperator::EmptyResult(_) => {
                let [child] = node.children.as_ref() else {
                    return None;
                };
                node = child;
            }
            _ => return Some(false),
        }
    }
}

fn can_advance(
    predicate: &Expression,
    operator: &LogicalOperator<()>,
    child_layouts: &[PlannerBindingLayout],
    projection_is_graph_chain: bool,
) -> Option<bool> {
    if projection_is_graph_chain && matches!(operator, LogicalOperator::Projection(_)) {
        return Some(false);
    }
    let layouts = child_layouts
        .iter()
        .map(|layout| layout.as_ref())
        .collect::<Vec<_>>();
    match domain_transfer::transfer_predicates(operator, &layouts, std::slice::from_ref(predicate))
    {
        Some(routed) if routed.unsupported => None,
        Some(routed) => Some(routed.has_moved()),
        // A known leaf/control boundary is a semantic no-op for this local
        // transfer. Missing layouts or malformed supported operators remain
        // unavailable evidence and must not look like a completed barrier.
        None if child_layouts.is_empty()
            && matches!(
                operator,
                LogicalOperator::Get(_)
                    | LogicalOperator::ExpressionGet(_)
                    | LogicalOperator::DummyScan
                    | LogicalOperator::GraphScan(_)
                    | LogicalOperator::TableFunctionGet(_)
                    | LogicalOperator::SearchScan(_)
                    | LogicalOperator::FullTextFilterScan(_)
                    | LogicalOperator::CTERef(_)
                    | LogicalOperator::DelimGet(_)
            ) =>
        {
            Some(false)
        }
        None if child_layouts.len() == 1
            && matches!(
                operator,
                LogicalOperator::Limit(_)
                    | LogicalOperator::TopN(_)
                    | LogicalOperator::EmptyResult(_)
            ) =>
        {
            Some(false)
        }
        None => None,
    }
}

/// Prove consumption on every routed, selected child. The residual at this
/// boundary belongs to the native rewrite; it is not another child request.
fn selected_routes_consumed(
    node: &FrozenCandidate,
    routed: &domain_transfer::OperatorDomainTransfer,
    state: &PlannerTransformState,
) -> bool {
    !routed.unsupported
        && routed.has_moved()
        && node.logical.key.children.len() == node.children.len()
        && routed.child_predicates.len() == node.children.len()
        && routed
            .child_predicates
            .iter()
            .zip(node.children.iter())
            .all(|(predicates, child)| {
                predicates.is_empty() || selected_consumes(child, predicates, state)
            })
}

/// Only an actual Filter is evidence. AND terms may be covered separately;
/// OR branches and unrelated selected siblings cannot discharge each other.
fn selected_consumes(
    node: &FrozenCandidate,
    predicates: &[Expression],
    state: &PlannerTransformState,
) -> bool {
    use crate::expression::traversal::into_associative_terms;
    use paro_planner::expression::ConjunctionType;

    let normalized_terms = |expression: &Expression| {
        let mut expression = expression.clone();
        crate::expression::scalar_normalizer()
            .rewrite_expression(&mut expression, &LogicalOperator::DummyScan);
        into_associative_terms(expression, ConjunctionType::And)
    };
    let Some(payload) = state.payloads.logical.get(node.logical.payload.index()) else {
        return false;
    };
    let Some(metadata) = state.metadata.get(&node.logical.payload) else {
        return false;
    };
    if metadata.child_layouts.len() != node.children.len()
        || node.logical.key.children.len() != node.children.len()
    {
        return false;
    }
    let operator = &payload.semantic_template.operator;
    let layouts = metadata
        .child_layouts
        .iter()
        .map(|layout| layout.as_ref())
        .collect::<Vec<_>>();
    let mut uncovered = predicates
        .iter()
        .flat_map(&normalized_terms)
        .collect::<Vec<_>>();
    if let LogicalOperator::Filter(filter) = operator {
        let [layout] = layouts.as_slice() else {
            return false;
        };
        if !filter.projection_map.is_identity(layout.len())
            || filter
                .expressions
                .iter()
                .any(|expression| expression.evaluation_properties().is_reorder_fence())
        {
            return false;
        }
        let enforced = filter
            .expressions
            .iter()
            .filter(|expression| domain_transfer::predicate_is_local_to_layout(expression, layout))
            .flat_map(&normalized_terms)
            .collect::<Vec<_>>();
        uncovered.retain(|predicate| {
            !enforced.iter().any(|expression| {
                crate::cte::predicate_domain::predicate_domains_equal(
                    std::slice::from_ref(expression),
                    std::slice::from_ref(predicate),
                )
            })
        });
        if uncovered.is_empty() {
            return true;
        }
    }
    if matches!(operator, LogicalOperator::Projection(_)) {
        let [child] = node.children.as_ref() else {
            return false;
        };
        if selected_is_graph_chain(child, state) != Some(false) {
            return false;
        }
    }
    let Some(routed) = domain_transfer::transfer_predicates(operator, &layouts, &uncovered) else {
        return false;
    };
    // A weaker necessary condition deeper in the tree does not prove that
    // the domain requested here was consumed in full.
    routed.remaining.is_empty() && selected_routes_consumed(node, &routed, state)
}

fn selected_transfer_consumed(
    node: &FrozenCandidate,
    predicate: &Expression,
    state: &PlannerTransformState,
) -> bool {
    let Some(payload) = state.payloads.logical.get(node.logical.payload.index()) else {
        return false;
    };
    let Some(metadata) = state.metadata.get(&node.logical.payload) else {
        return false;
    };
    if metadata.child_layouts.len() != node.children.len() {
        return false;
    }
    let layouts = metadata
        .child_layouts
        .iter()
        .map(|layout| layout.as_ref())
        .collect::<Vec<_>>();
    domain_transfer::transfer_predicates(
        &payload.semantic_template.operator,
        &layouts,
        std::slice::from_ref(predicate),
    )
    .is_some_and(|routed| selected_routes_consumed(node, &routed, state))
}

#[cfg(test)]
pub(super) fn pending_transfers(
    root: &FrozenCandidate,
    state: &PlannerTransformState,
) -> Option<Box<[CandidateId]>> {
    let _partition = crate::work_partition::enter(crate::work_partition::Bucket::QualityDomain);
    let mut pending = Vec::new();
    let mut visited = BTreeSet::new();
    let mut stack = vec![root];
    while let Some(node) = stack.pop() {
        if !visited.insert(node.reference.candidate) {
            continue;
        }
        let operator = &state
            .payloads
            .logical
            .get(node.logical.payload.index())?
            .semantic_template
            .operator;
        if let LogicalOperator::Filter(filter) = operator {
            let [child] = node.children.as_ref() else {
                return None;
            };
            let child_operator = &state
                .payloads
                .logical
                .get(child.logical.payload.index())?
                .semantic_template
                .operator;
            let metadata = state.metadata.get(&child.logical.payload)?;
            if metadata.child_layouts.len() != child.children.len() {
                return None;
            }
            let projection_is_graph_chain =
                if matches!(child_operator, LogicalOperator::Projection(_)) {
                    let [input] = child.children.as_ref() else {
                        return None;
                    };
                    selected_is_graph_chain(input, state)?
                } else {
                    false
                };
            let owner_fenced = filter
                .expressions
                .iter()
                .any(|expression| expression.evaluation_properties().is_reorder_fence());
            let mut advances = false;
            for predicate in &filter.expressions {
                let transferable = can_advance(
                    predicate,
                    child_operator,
                    &metadata.child_layouts,
                    projection_is_graph_chain,
                )?;
                advances |= !owner_fenced
                    && transferable
                    && !selected_transfer_consumed(child, predicate, state);
            }
            if advances {
                pending.push(node.reference.candidate);
            }
        }
        // A legal fence suppresses this owner's request, not independent
        // transfers below it. Never prune the selected subtree here.
        stack.extend(node.children.iter().map(Arc::as_ref));
    }
    pending.sort_unstable();
    Some(pending.into_boxed_slice())
}

/// Build a bounded PredicateTransfer binding from the exact selected DAG.
///
/// This is a production hint for the quality lane, not a replacement search:
/// every node and child below the filter comes from one FrozenCandidate, and
/// every non-selected sibling remains a Memo group hole.  The caller may run
/// this binding once to close a necessary local path without enumerating the
/// complete child frontier.  Ordinary rule scheduling remains responsible for
/// every other legal alternative.
pub(super) fn selected_transfer_bindings(
    memo: &Memo,
    root: &FrozenCandidate,
    state: &PlannerTransformState,
) -> Box<[PatternBinding]> {
    let mut bindings = Vec::new();
    let mut pending = vec![root];
    let mut visited = BTreeSet::new();
    while let Some(node) = pending.pop() {
        if !visited.insert(node.reference.candidate) {
            continue;
        }
        if let Some(binding) = selected_transfer_binding(memo, node, state) {
            bindings.push(binding);
        }
        pending.extend(node.children.iter().map(Arc::as_ref));
    }
    bindings.sort_unstable_by_key(|binding| binding.fingerprint);
    bindings.dedup_by(|left, right| left == right);
    bindings.into_boxed_slice()
}

/// The preflight path uses the same transfer contract as the frozen path but
/// resolves only immutable Memo identities.  No `FrozenCandidate`, logical
/// payload clone, or child tree allocation is created here.  The returned
/// bindings are still only scheduling hints; the ordinary matcher remains the
/// semantic owner of the complete transformation search.
pub(super) fn pending_transfer_for_ref(
    memo: &Memo,
    reference: ChildWinnerRef,
    nodes: &BTreeMap<CandidateId, &QualityCandidateNode>,
    state: &PlannerTransformState,
) -> Option<bool> {
    let node = ref_node(nodes, reference)?;
    let logical = memo.logical_expr(node.logical)?;
    let operator = &state
        .payloads
        .logical
        .get(logical.payload.index())?
        .semantic_template
        .operator;
    if let LogicalOperator::Filter(filter) = operator {
        let [child] = node.children.as_ref() else {
            return None;
        };
        let child_node = ref_node(nodes, *child)?;
        let child_logical = memo.logical_expr(child_node.logical)?;
        let child_operator = &state
            .payloads
            .logical
            .get(child_logical.payload.index())?
            .semantic_template
            .operator;
        let metadata = state.metadata.get(&child_logical.payload)?;
        if metadata.child_layouts.len() != child_node.children.len() {
            return None;
        }
        let projection_is_graph_chain = if matches!(child_operator, LogicalOperator::Projection(_))
        {
            let [input] = child_node.children.as_ref() else {
                return None;
            };
            selected_is_graph_chain_ref(nodes, *input, memo, state)?
        } else {
            false
        };
        let owner_fenced = filter
            .expressions
            .iter()
            .any(|expression| expression.evaluation_properties().is_reorder_fence());
        let mut advances = false;
        for predicate in &filter.expressions {
            let transferable = can_advance(
                predicate,
                child_operator,
                &metadata.child_layouts,
                projection_is_graph_chain,
            )?;
            advances |= !owner_fenced
                && transferable
                && !selected_transfer_consumed_ref(nodes, *child, memo, predicate, state);
        }
        return Some(advances);
    }
    Some(false)
}

pub(super) fn selected_transfer_bindings_for_refs(
    memo: &Memo,
    root: ChildWinnerRef,
    nodes: &[QualityCandidateNode],
    state: &PlannerTransformState,
) -> Box<[PatternBinding]> {
    let nodes = nodes
        .iter()
        .map(|node| (node.reference.candidate, node))
        .collect::<BTreeMap<_, _>>();
    let mut bindings = Vec::new();
    let mut pending = vec![root];
    let mut visited = BTreeSet::new();
    while let Some(reference) = pending.pop() {
        if !visited.insert(reference.candidate) {
            continue;
        }
        if let Some(binding) = selected_transfer_binding_ref(&nodes, reference, memo, state) {
            bindings.push(binding);
        }
        let Some(node) = ref_node(&nodes, reference) else {
            continue;
        };
        pending.extend(node.children.iter().copied());
    }
    bindings.sort_unstable_by_key(|binding| binding.fingerprint);
    bindings.dedup_by(|left, right| left == right);
    bindings.into_boxed_slice()
}

fn ref_node<'a>(
    nodes: &'a BTreeMap<CandidateId, &'a QualityCandidateNode>,
    reference: ChildWinnerRef,
) -> Option<&'a QualityCandidateNode> {
    let node = nodes.get(&reference.candidate).copied()?;
    (node.reference.group == reference.group && node.reference.goal == reference.goal)
        .then_some(node)
}

fn selected_is_graph_chain_ref(
    nodes: &BTreeMap<CandidateId, &QualityCandidateNode>,
    mut reference: ChildWinnerRef,
    memo: &Memo,
    state: &PlannerTransformState,
) -> Option<bool> {
    loop {
        let node = ref_node(nodes, reference)?;
        let logical = memo.logical_expr(node.logical)?;
        let operator = &state
            .payloads
            .logical
            .get(logical.payload.index())?
            .semantic_template
            .operator;
        match operator {
            LogicalOperator::GraphScan(_) | LogicalOperator::GraphExpand(_) => return Some(true),
            LogicalOperator::Filter(_) | LogicalOperator::EmptyResult(_) => {
                let [child] = node.children.as_ref() else {
                    return None;
                };
                reference = *child;
            }
            _ => return Some(false),
        }
    }
}

fn selected_routes_consumed_ref(
    nodes: &BTreeMap<CandidateId, &QualityCandidateNode>,
    reference: ChildWinnerRef,
    memo: &Memo,
    routed: &domain_transfer::OperatorDomainTransfer,
    state: &PlannerTransformState,
) -> bool {
    let Some(node) = ref_node(nodes, reference) else {
        return false;
    };
    let Some(logical) = memo.logical_expr(node.logical) else {
        return false;
    };
    !routed.unsupported
        && routed.has_moved()
        && logical.key.children.len() == node.children.len()
        && routed.child_predicates.len() == node.children.len()
        && routed
            .child_predicates
            .iter()
            .zip(node.children.iter())
            .all(|(predicates, child)| {
                predicates.is_empty()
                    || selected_consumes_ref(nodes, *child, memo, predicates, state)
            })
}

fn selected_consumes_ref(
    nodes: &BTreeMap<CandidateId, &QualityCandidateNode>,
    reference: ChildWinnerRef,
    memo: &Memo,
    predicates: &[Expression],
    state: &PlannerTransformState,
) -> bool {
    use crate::expression::traversal::into_associative_terms;
    use paro_planner::expression::ConjunctionType;

    let normalized_terms = |expression: &Expression| {
        let mut expression = expression.clone();
        crate::expression::scalar_normalizer()
            .rewrite_expression(&mut expression, &LogicalOperator::DummyScan);
        into_associative_terms(expression, ConjunctionType::And)
    };
    let Some(node) = ref_node(nodes, reference) else {
        return false;
    };
    let Some(logical) = memo.logical_expr(node.logical) else {
        return false;
    };
    let Some(payload) = state.payloads.logical.get(logical.payload.index()) else {
        return false;
    };
    let Some(metadata) = state.metadata.get(&logical.payload) else {
        return false;
    };
    if metadata.child_layouts.len() != node.children.len()
        || logical.key.children.len() != node.children.len()
    {
        return false;
    }
    let operator = &payload.semantic_template.operator;
    let layouts = metadata
        .child_layouts
        .iter()
        .map(|layout| layout.as_ref())
        .collect::<Vec<_>>();
    let mut uncovered = predicates
        .iter()
        .flat_map(&normalized_terms)
        .collect::<Vec<_>>();
    if let LogicalOperator::Filter(filter) = operator {
        let [layout] = layouts.as_slice() else {
            return false;
        };
        if !filter.projection_map.is_identity(layout.len())
            || filter
                .expressions
                .iter()
                .any(|expression| expression.evaluation_properties().is_reorder_fence())
        {
            return false;
        }
        let enforced = filter
            .expressions
            .iter()
            .filter(|expression| domain_transfer::predicate_is_local_to_layout(expression, layout))
            .flat_map(&normalized_terms)
            .collect::<Vec<_>>();
        uncovered.retain(|predicate| {
            !enforced.iter().any(|expression| {
                crate::cte::predicate_domain::predicate_domains_equal(
                    std::slice::from_ref(expression),
                    std::slice::from_ref(predicate),
                )
            })
        });
        if uncovered.is_empty() {
            return true;
        }
    }
    if matches!(operator, LogicalOperator::Projection(_)) {
        let [child] = node.children.as_ref() else {
            return false;
        };
        if selected_is_graph_chain_ref(nodes, *child, memo, state) != Some(false) {
            return false;
        }
    }
    let Some(routed) = domain_transfer::transfer_predicates(operator, &layouts, &uncovered) else {
        return false;
    };
    routed.remaining.is_empty()
        && selected_routes_consumed_ref(nodes, reference, memo, &routed, state)
}

fn selected_transfer_consumed_ref(
    nodes: &BTreeMap<CandidateId, &QualityCandidateNode>,
    reference: ChildWinnerRef,
    memo: &Memo,
    predicate: &Expression,
    state: &PlannerTransformState,
) -> bool {
    let Some(node) = ref_node(nodes, reference) else {
        return false;
    };
    let Some(logical) = memo.logical_expr(node.logical) else {
        return false;
    };
    let Some(payload) = state.payloads.logical.get(logical.payload.index()) else {
        return false;
    };
    let Some(metadata) = state.metadata.get(&logical.payload) else {
        return false;
    };
    if metadata.child_layouts.len() != node.children.len() {
        return false;
    }
    let layouts = metadata
        .child_layouts
        .iter()
        .map(|layout| layout.as_ref())
        .collect::<Vec<_>>();
    domain_transfer::transfer_predicates(
        &payload.semantic_template.operator,
        &layouts,
        std::slice::from_ref(predicate),
    )
    .is_some_and(|routed| selected_routes_consumed_ref(nodes, reference, memo, &routed, state))
}

fn selected_transfer_binding_ref(
    nodes: &BTreeMap<CandidateId, &QualityCandidateNode>,
    reference: ChildWinnerRef,
    memo: &Memo,
    state: &PlannerTransformState,
) -> Option<PatternBinding> {
    let node = ref_node(nodes, reference)?;
    let logical = memo.logical_expr(node.logical)?;
    let payload = state.payloads.logical.get(logical.payload.index())?;
    let LogicalOperator::Filter(filter) = &payload.semantic_template.operator else {
        return None;
    };
    if filter.expressions.is_empty()
        || filter
            .expressions
            .iter()
            .any(|expression| !is_column_domain(expression))
        || filter
            .expressions
            .iter()
            .any(|expression| expression.evaluation_properties().is_reorder_fence())
        || logical.key.children.len() != node.children.len()
    {
        return None;
    }
    let group = memo.canonical_group(node.reference.group);
    let child_group = memo.canonical_group(*logical.key.children.first()?);
    let child = *node.children.first()?;
    if memo.canonical_group(child.group) != child_group {
        return None;
    }
    let child = selected_transfer_path_operand_ref(nodes, child, memo, &filter.expressions, state)?;
    let root = PatternOperand::Expression {
        group,
        expression: logical.id,
        children: Box::new([child]),
    };
    Some(PatternBinding {
        fingerprint: selected_binding_fingerprint(&root),
        root,
    })
}

fn selected_transfer_path_operand_ref(
    nodes: &BTreeMap<CandidateId, &QualityCandidateNode>,
    reference: ChildWinnerRef,
    memo: &Memo,
    predicates: &[Expression],
    state: &PlannerTransformState,
) -> Option<PatternOperand> {
    if predicates.is_empty() {
        return None;
    }
    let node = ref_node(nodes, reference)?;
    let logical = memo.logical_expr(node.logical)?;
    if logical.key.children.len() != node.children.len() {
        return None;
    }
    let payload = state.payloads.logical.get(logical.payload.index())?;
    let metadata = state.metadata.get(&logical.payload)?;
    let projection_is_graph_chain = if matches!(
        payload.semantic_template.operator,
        LogicalOperator::Projection(_)
    ) {
        let child = *node.children.first()?;
        selected_is_graph_chain_ref(nodes, child, memo, state)?
    } else {
        false
    };
    let movable = predicates
        .iter()
        .filter(|predicate| {
            can_advance(
                predicate,
                &payload.semantic_template.operator,
                &metadata.child_layouts,
                projection_is_graph_chain,
            ) == Some(true)
                && !selected_transfer_consumed_ref(nodes, reference, memo, predicate, state)
        })
        .cloned()
        .collect::<Vec<_>>();
    if movable.is_empty() {
        return None;
    }
    let layouts = metadata
        .child_layouts
        .iter()
        .map(|layout| layout.as_ref())
        .collect::<Vec<_>>();
    let routed = domain_transfer::transfer_predicates(
        &payload.semantic_template.operator,
        &layouts,
        &movable,
    )?;
    let children = logical
        .key
        .children
        .iter()
        .copied()
        .zip(node.children.iter().copied())
        .enumerate()
        .map(|(ordinal, (group, child))| {
            let group = memo.canonical_group(group);
            if memo.canonical_group(child.group) != group {
                return PatternOperand::Group(group);
            }
            let child_predicates = routed
                .child_predicates
                .get(ordinal)
                .map(|predicates| predicates.to_vec())
                .unwrap_or_default();
            if child_predicates.is_empty() {
                PatternOperand::Group(group)
            } else {
                selected_transfer_path_operand_ref(nodes, child, memo, &child_predicates, state)
                    .unwrap_or(PatternOperand::Group(group))
            }
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Some(PatternOperand::Expression {
        group: memo.canonical_group(node.reference.group),
        expression: logical.id,
        children,
    })
}

fn selected_transfer_binding(
    memo: &Memo,
    node: &FrozenCandidate,
    state: &PlannerTransformState,
) -> Option<PatternBinding> {
    let payload = state.payloads.logical.get(node.logical.payload.index())?;
    let LogicalOperator::Filter(filter) = &payload.semantic_template.operator else {
        return None;
    };
    if filter.expressions.is_empty()
        || filter
            .expressions
            .iter()
            .any(|expression| !is_column_domain(expression))
        || filter
            .expressions
            .iter()
            .any(|expression| expression.evaluation_properties().is_reorder_fence())
        || node.logical.key.children.len() != node.children.len()
    {
        return None;
    }
    let group = memo.canonical_group(node.reference.group);
    let child_group = memo.canonical_group(*node.logical.key.children.first()?);
    let child = node.children.first()?;
    if memo.canonical_group(child.reference.group) != child_group {
        return None;
    }
    let child = selected_transfer_path_operand(memo, child, &filter.expressions, state)?;
    let root = PatternOperand::Expression {
        group,
        expression: node.logical.id,
        children: Box::new([child]),
    };
    Some(PatternBinding {
        fingerprint: selected_binding_fingerprint(&root),
        root,
    })
}

fn selected_transfer_path_operand(
    memo: &Memo,
    node: &FrozenCandidate,
    predicates: &[Expression],
    state: &PlannerTransformState,
) -> Option<PatternOperand> {
    if predicates.is_empty() || node.logical.key.children.len() != node.children.len() {
        return None;
    }
    let payload = state.payloads.logical.get(node.logical.payload.index())?;
    let metadata = state.metadata.get(&node.logical.payload)?;
    let projection_is_graph_chain = if matches!(
        payload.semantic_template.operator,
        LogicalOperator::Projection(_)
    ) {
        let child = node.children.first()?;
        selected_is_graph_chain(child, state)?
    } else {
        false
    };
    let movable = predicates
        .iter()
        .filter(|predicate| {
            can_advance(
                predicate,
                &payload.semantic_template.operator,
                &metadata.child_layouts,
                projection_is_graph_chain,
            ) == Some(true)
                && !selected_transfer_consumed(node, predicate, state)
        })
        .cloned()
        .collect::<Vec<_>>();
    if movable.is_empty() {
        return None;
    }
    let layouts = metadata
        .child_layouts
        .iter()
        .map(|layout| layout.as_ref())
        .collect::<Vec<_>>();
    let routed = domain_transfer::transfer_predicates(
        &payload.semantic_template.operator,
        &layouts,
        &movable,
    )?;
    // Child routes include necessary domains. Native transfer retains the
    // original residual at this boundary; the binding only selects its path.
    let children = node
        .logical
        .key
        .children
        .iter()
        .copied()
        .zip(node.children.iter())
        .enumerate()
        .map(|(ordinal, (group, child))| {
            let group = memo.canonical_group(group);
            if memo.canonical_group(child.reference.group) != group {
                return PatternOperand::Group(group);
            }
            let child_predicates = routed
                .child_predicates
                .get(ordinal)
                .map(|predicates| predicates.to_vec())
                .unwrap_or_default();
            if child_predicates.is_empty() {
                PatternOperand::Group(group)
            } else {
                selected_transfer_path_operand(memo, child, &child_predicates, state)
                    .unwrap_or(PatternOperand::Group(group))
            }
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Some(PatternOperand::Expression {
        group: memo.canonical_group(node.reference.group),
        expression: node.logical.id,
        children,
    })
}

fn selected_binding_fingerprint(operand: &PatternOperand) -> Fingerprint {
    let mut fingerprint = StableFingerprintBuilder::default();
    fn write(fingerprint: &mut StableFingerprintBuilder, operand: &PatternOperand) {
        match operand {
            PatternOperand::Group(group) => {
                fingerprint.write_u64(0);
                fingerprint.write_u64(group.0 as u64);
            }
            PatternOperand::Expression {
                group,
                expression,
                children,
            } => {
                fingerprint.write_u64(1);
                fingerprint.write_u64(group.0 as u64);
                fingerprint.write_u64(expression.0 as u64);
                fingerprint.write_u64(children.len() as u64);
                for child in children {
                    write(fingerprint, child);
                }
            }
        }
    }
    fingerprint.write_bytes(b"paro.selected-domain-binding.v1");
    write(&mut fingerprint, operand);
    fingerprint.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_catalog::entry::{EdgeTableInfo, VertexTableInfo};
    use paro_common::{runtime_value::Value, types::LogicalType};
    use paro_function::aggregate::distributive::count::get_count_star_function;
    use paro_function::scalar::{FunctionStability, ScalarFunction};
    use paro_planner::expression::{
        AggregateExpression, ColumnRefExpression, ComparisonExpression, ComparisonType,
        ConjunctionExpression, ConjunctionType, ConstantExpression, FunctionExpression,
    };
    use paro_planner::operator::{
        Aggregate, EmptyResult, ExpandDirection, ExpressionGet, Filter, GraphExpand, GraphScan,
        LogicalOutputLayout, Projection, SetOpType, SetOperation,
    };

    fn column(table: usize, index: usize) -> Expression {
        Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(table, index), LogicalType::Integer).into(),
        )
    }

    fn equal(table: usize, index: usize) -> Expression {
        equal_value(table, index, 2)
    }

    fn equal_value(table: usize, index: usize, value: i32) -> Expression {
        Expression::Comparison(
            ComparisonExpression::new(
                ComparisonType::Equal,
                column(table, index),
                Expression::Constant(
                    ConstantExpression::new(Value::Integer(value), LogicalType::Integer).into(),
                ),
            )
            .into(),
        )
    }

    fn volatile_predicate() -> Expression {
        let function = ScalarFunction::new(
            "quality_domain_volatile".into(),
            vec![],
            LogicalType::Integer,
            |_, _, _| Ok(()),
        )
        .with_stability(FunctionStability::Volatile);
        Expression::Comparison(
            ComparisonExpression::new(
                ComparisonType::GreaterThan,
                Expression::Function(
                    FunctionExpression::new(function, vec![], LogicalType::Integer).into(),
                ),
                Expression::Constant(
                    ConstantExpression::new(Value::Integer(0), LogicalType::Integer).into(),
                ),
            )
            .into(),
        )
    }

    fn input(table: usize, width: usize) -> OwnedLogicalPlan {
        OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
            table,
            vec![],
            (0..width).map(|index| format!("key{index}")).collect(),
            vec![LogicalType::Integer; width],
        )))
    }

    fn filtered(child: OwnedLogicalPlan, expressions: Vec<Expression>) -> OwnedLogicalPlan {
        OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(child, expressions)))
    }

    fn projected(
        child: OwnedLogicalPlan,
        table: usize,
        expression: Expression,
    ) -> OwnedLogicalPlan {
        OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
            table,
            child,
            vec![expression],
        )))
    }

    fn layout(bindings: &[(usize, usize)]) -> PlannerBindingLayout {
        Arc::new(LogicalOutputLayout::new(
            vec![LogicalType::Integer; bindings.len()],
            bindings
                .iter()
                .map(|&(table, ordinal)| ColumnBinding::new(table, ordinal))
                .collect(),
        ))
    }

    fn detached(plan: OwnedLogicalPlan) -> LogicalOperator<()> {
        paro_planner::plan::arena::LogicalPlanNode::detach(plan)
            .0
            .operator
    }

    fn branch(base: usize, moved: bool, residual: bool) -> OwnedLogicalPlan {
        let input = OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
            ExpressionGet::new(base, vec![], vec!["key".into()], vec![LogicalType::Integer]),
        ));
        let input = if moved {
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
                input,
                vec![equal(base, 0)],
            )))
        } else {
            input
        };
        let count = Expression::Aggregate(
            AggregateExpression::new(get_count_star_function(), vec![], LogicalType::BigInt).into(),
        );
        let aggregate = OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(
            Aggregate::new(
                base + 1,
                base + 2,
                base + 3,
                input,
                vec![column(base, 0)],
                vec![],
                vec![count],
                vec![],
            )
            .into(),
        ));
        if residual {
            let predicate = Expression::Comparison(
                ComparisonExpression::new(
                    ComparisonType::GreaterThan,
                    Expression::ColumnRef(
                        ColumnRefExpression::new(
                            ColumnBinding::new(base + 2, 0),
                            LogicalType::BigInt,
                        )
                        .into(),
                    ),
                    Expression::Constant(
                        ConstantExpression::new(Value::BigInt(0), LogicalType::BigInt).into(),
                    ),
                )
                .into(),
            );
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
                aggregate,
                vec![predicate],
            )))
        } else if !moved {
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
                aggregate,
                vec![equal(base + 1, 0)],
            )))
        } else {
            aggregate
        }
    }

    fn frozen(
        plan: OwnedLogicalPlan,
    ) -> (
        CascadesEngine,
        Arc<RwLock<PlannerTransformState>>,
        Arc<FrozenCandidate>,
    ) {
        let input = MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
        let state = input.planner_state.clone();
        let grants = super::super::tests::test_grant_classes();
        let classes = Arc::new(grants.into_iter().map(|grant| (grant.id, grant)).collect());
        let mut registry = ImplementationRegistry::default();
        implementation::register_implementations(
            &mut registry,
            state.clone(),
            classes,
            input.calibration,
            false,
        )
        .unwrap();
        let mut engine = CascadesEngine::new(input.memo, registry);
        let optimized = engine
            .optimize_for_grants(
                input.root,
                input.root_goal,
                AdmissibleGrantSetId(0),
                grants,
                input.mode,
            )
            .unwrap();
        let winner = optimized.winners.first().unwrap();
        let root = engine
            .memo()
            .freeze_candidate_tree(ChildWinnerRef {
                group: input.root,
                goal: winner.goal,
                candidate: winner.winner.candidate,
            })
            .unwrap();
        (engine, state, root)
    }

    #[test]
    fn selected_domains_are_per_branch_and_do_not_move_aggregate_residuals() {
        for base in [0, 100] {
            for moved in [[false, false], [true, false], [false, true], [true, true]] {
                let plan =
                    OwnedLogicalPlan::synthetic(LogicalOperator::SetOperation(SetOperation::new(
                        base + 30,
                        branch(base, moved[0], false),
                        branch(base + 10, moved[1], false),
                        SetOpType::Union,
                        true,
                        vec![LogicalType::Integer, LogicalType::BigInt],
                    )));
                let (_, state, root) = frozen(plan);
                let pending = pending_transfers(&root, &state.read().unwrap()).unwrap();
                assert_eq!(pending.len(), moved.iter().filter(|moved| !**moved).count());
                assert!(pending.iter().all(|id| *id != root.reference.candidate));
                if pending.len() == 2 {
                    assert_ne!(pending[0], pending[1]);
                }
            }
            let (_, state, root) = frozen(branch(base, true, true));
            assert!(
                pending_transfers(&root, &state.read().unwrap())
                    .unwrap()
                    .is_empty(),
                "aggregate outputs (including SUM > 0) remain on the output side"
            );
        }
    }

    #[test]
    fn domain_request_excludes_nonplain_and_nonlocal_group_expressions() {
        let plan = branch(0, false, false);
        let LogicalOperator::Filter(filter) = plan.into_operator() else {
            unreachable!()
        };
        let LogicalOperator::Aggregate(aggregate) = &filter.child.operator else {
            unreachable!()
        };
        assert!(FilterPushdown::group_filter_can_move(
            aggregate,
            &equal(1, 0)
        ));
        assert!(!FilterPushdown::group_filter_can_move(
            aggregate,
            &equal(2, 0)
        ));
        assert!(!FilterPushdown::group_filter_can_move(
            aggregate,
            &equal(0, 0)
        ));
        let (mut shell, _) = paro_planner::plan::arena::LogicalPlanNode::detach(*filter.child);
        if let LogicalOperator::Aggregate(aggregate) = &mut shell.operator {
            aggregate.groups.clear();
        }
        assert_eq!(
            can_advance(&equal(1, 0), &shell.operator, &[], false),
            None,
            "an invalid child layout is unavailable evidence, not a completed barrier"
        );
    }

    #[test]
    fn fenced_owner_and_adjacent_filter_do_not_hide_descendant_transfers() {
        assert!(volatile_predicate()
            .evaluation_properties()
            .is_reorder_fence());
        for adjacent in [false, true] {
            let child = branch(0, false, false);
            let plan = if adjacent {
                filtered(
                    filtered(child, vec![volatile_predicate()]),
                    vec![equal(1, 0)],
                )
            } else {
                filtered(
                    projected(child, 10, column(1, 0)),
                    vec![equal(10, 0), volatile_predicate()],
                )
            };
            let (_, state, root) = frozen(plan);
            let pending = pending_transfers(&root, &state.read().unwrap()).unwrap();
            assert!(!pending.contains(&root.reference.candidate));
            assert!(pending.contains(&root.children[0].children[0].reference.candidate));
        }

        let child_filter = detached(filtered(input(0, 1), vec![equal(0, 0)]));
        assert_eq!(
            can_advance(&equal(0, 0), &child_filter, &[layout(&[(0, 0)])], false),
            Some(true),
            "unfenced adjacent filters can still merge"
        );
    }

    #[test]
    fn projection_guard_uses_only_the_selected_graph_chain() {
        // Start with ordinary executable choices. Only the semantic views
        // used by this guard are substituted below; no graph provider or
        // graph execution is needed to test the selected-chain walk.
        let plan = filtered(
            projected(
                filtered(
                    filtered(input(0, 1), vec![equal_value(0, 0, 3)]),
                    vec![equal_value(0, 0, 3)],
                ),
                10,
                column(0, 0),
            ),
            vec![equal(10, 0)],
        );
        let (_, state, root) = frozen(plan);
        let projection = &root.children[0];
        let outer = &projection.children[0];
        let inner = &outer.children[0];
        let leaf = &inner.children[0];
        let mut state = state.write().unwrap();
        assert_eq!(selected_is_graph_chain(outer, &state), Some(false));
        assert!(pending_transfers(&root, &state)
            .unwrap()
            .contains(&root.reference.candidate));

        state.payloads.logical[leaf.logical.payload.index()]
            .semantic_template
            .operator = LogicalOperator::GraphScan(Box::new(GraphScan::new(
            VertexTableInfo {
                table_name: "vertices".into(),
                table_oid: 0,
                key_column_ids: vec![0],
                label: "vertex".into(),
                property_column_ids: vec![],
            },
            None,
            0,
            0,
            "vertex".into(),
            "graph".into(),
            "main".into(),
        )));
        assert_eq!(selected_is_graph_chain(leaf, &state), Some(true));
        assert_eq!(selected_is_graph_chain(outer, &state), Some(true));
        assert!(!pending_transfers(&root, &state)
            .unwrap()
            .contains(&root.reference.candidate));

        state.payloads.logical[outer.logical.payload.index()]
            .semantic_template
            .operator = LogicalOperator::EmptyResult(EmptyResult { child: () });
        assert_eq!(selected_is_graph_chain(outer, &state), Some(true));
        assert!(!pending_transfers(&root, &state)
            .unwrap()
            .contains(&root.reference.candidate));
        let mut broken = outer.as_ref().clone();
        broken.children = Box::new([]);
        assert_eq!(selected_is_graph_chain(&broken, &state), None);

        state.payloads.logical[inner.logical.payload.index()]
            .semantic_template
            .operator = detached(OwnedLogicalPlan::synthetic(LogicalOperator::GraphExpand(
            Box::new(GraphExpand::new(
                EdgeTableInfo {
                    table_name: "edges".into(),
                    table_oid: 0,
                    key_column_ids: vec![0],
                    source_key_column_ids: vec![1],
                    source_vertex_table: "vertices".into(),
                    source_ref_column_ids: vec![0],
                    destination_key_column_ids: vec![2],
                    destination_vertex_table: "vertices".into(),
                    destination_ref_column_ids: vec![0],
                    label: "edge".into(),
                    property_column_ids: vec![],
                },
                ExpandDirection::Forward,
                "vertex".into(),
                0,
                1,
                2,
                3,
                "vertex".into(),
                0,
                0,
                "vertices".into(),
                input(0, 1),
            )),
        )));
        assert_eq!(selected_is_graph_chain(inner, &state), Some(true));
        assert_eq!(selected_is_graph_chain(outer, &state), Some(true));
        assert!(!pending_transfers(&root, &state)
            .unwrap()
            .contains(&root.reference.candidate));

        state.payloads.logical[outer.logical.payload.index()]
            .semantic_template
            .operator = detached(projected(input(0, 1), 0, column(0, 0)));
        assert_eq!(selected_is_graph_chain(outer, &state), Some(false));
        assert!(pending_transfers(&root, &state)
            .unwrap()
            .contains(&root.reference.candidate));
    }

    #[test]
    fn projection_rejects_missing_layout_and_invalid_output_ordinal() {
        let projection = detached(projected(input(0, 1), 10, column(0, 0)));
        let layouts = [layout(&[(0, 0)])];
        assert_eq!(
            can_advance(&equal(10, 0), &projection, &layouts, false),
            Some(true)
        );
        assert_eq!(
            can_advance(&equal(10, 0), &projection, &layouts, true),
            Some(false)
        );
        assert_eq!(can_advance(&equal(10, 0), &projection, &[], false), None);
        assert_eq!(
            can_advance(&equal(10, 1), &projection, &layouts, false),
            None
        );
        let fenced = detached(projected(input(0, 1), 10, volatile_predicate()));
        assert_eq!(
            can_advance(&equal(10, 0), &fenced, &layouts, false),
            Some(false)
        );
        let computed = detached(projected(input(0, 1), 10, equal(0, 0)));
        assert_eq!(
            can_advance(&equal(10, 0), &computed, &layouts, false),
            None,
            "an unsupported computed projection is not a proven barrier"
        );
    }

    #[test]
    fn unsupported_mapping_keeps_quality_evidence_unavailable() {
        let (engine, state, root) = frozen(filtered(
            projected(input(0, 1), 10, column(0, 0)),
            vec![equal(10, 0)],
        ));
        let mut state = state.write().unwrap();
        state.payloads.logical[root.children[0].logical.payload.index()]
            .semantic_template
            .operator = detached(projected(input(0, 1), 10, equal(0, 0)));
        assert!(pending_transfers(&root, &state).is_none());
        assert!(selected_transfer_binding(engine.memo(), &root, &state).is_none());

        let LogicalOperator::Filter(residual) = branch(0, true, true).into_operator() else {
            unreachable!()
        };
        let aggregate = detached(*residual.child);
        assert_eq!(
            can_advance(
                &residual.expressions[0],
                &aggregate,
                &[layout(&[(0, 0)])],
                false
            ),
            Some(false)
        );
        let mut unsupported = equal(1, 0);
        for _ in 0..130 {
            unsupported = conjunction(
                ConjunctionType::And,
                vec![unsupported, residual.expressions[0].clone()],
            );
        }
        assert_eq!(
            can_advance(&unsupported, &aggregate, &[layout(&[(0, 0)])], false),
            None,
            "an exhausted abstraction must not certify a barrier"
        );
    }

    #[test]
    fn set_operation_requires_complete_ordinal_layouts_and_local_predicates() {
        let left = layout(&[(10, 7), (10, 2)]);
        let right = layout(&[(20, 9), (20, 4)]);
        for kind in [SetOpType::Union, SetOpType::Intersect, SetOpType::Except] {
            for all in [false, true] {
                let operator = detached(OwnedLogicalPlan::synthetic(
                    LogicalOperator::SetOperation(SetOperation::new(
                        30,
                        input(10, 2),
                        input(20, 2),
                        kind,
                        all,
                        vec![LogicalType::Integer; 2],
                    )),
                ));
                let layouts = [left.clone(), right.clone()];
                assert_eq!(
                    can_advance(&equal(30, 1), &operator, &layouts, false),
                    Some(kind == SetOpType::Union && all)
                );
                assert_eq!(can_advance(&equal(10, 1), &operator, &layouts, false), None);
                assert_eq!(can_advance(&equal(30, 2), &operator, &layouts, false), None);
                assert_eq!(can_advance(&equal(30, 0), &operator, &[], false), None);
                assert_eq!(
                    can_advance(&equal(30, 0), &operator, &layouts[..1], false),
                    None
                );
                for incomplete in [
                    [layout(&[(10, 7)]), right.clone()],
                    [left.clone(), layout(&[(20, 9)])],
                ] {
                    assert_eq!(
                        can_advance(&equal(30, 0), &operator, &incomplete, false),
                        None
                    );
                }
                assert_eq!(
                    can_advance(&volatile_predicate(), &operator, &layouts, false),
                    Some(false)
                );
            }
        }
    }

    #[test]
    fn invalid_transfer_evidence_never_becomes_an_empty_pending_set() {
        let union =
            OwnedLogicalPlan::synthetic(LogicalOperator::SetOperation(SetOperation::union(
                30,
                input(10, 2),
                input(20, 2),
                true,
                vec![LogicalType::Integer; 2],
            )));
        let (_, state, root) = frozen(filtered(union, vec![equal(30, 0)]));
        let mut state = state.write().unwrap();
        assert!(pending_transfers(&root, &state)
            .unwrap()
            .contains(&root.reference.candidate));
        let child_payload = root.children[0].logical.payload;
        let metadata = state.metadata.remove(&child_payload).unwrap();
        assert!(pending_transfers(&root, &state).is_none());
        state.metadata.insert(child_payload, metadata);
        let layouts = state.metadata[&child_payload].child_layouts.clone();
        state
            .metadata
            .get_mut(&child_payload)
            .unwrap()
            .child_layouts = Box::new([]);
        assert!(pending_transfers(&root, &state).is_none());
        state
            .metadata
            .get_mut(&child_payload)
            .unwrap()
            .child_layouts = layouts;

        let LogicalOperator::Filter(filter) = &mut state.payloads.logical
            [root.logical.payload.index()]
        .semantic_template
        .operator
        else {
            unreachable!()
        };
        // The first predicate is transferable: do not short-circuit before
        // inspecting the later malformed ordinal, even with an owner fence.
        filter.expressions.push(equal(30, 2));
        filter.expressions.push(volatile_predicate());
        assert!(pending_transfers(&root, &state).is_none());
    }

    #[test]
    fn selected_binding_rebinds_through_projection_before_aggregate() {
        let count = Expression::Aggregate(
            AggregateExpression::new(get_count_star_function(), vec![], LogicalType::BigInt).into(),
        );
        let aggregate = Aggregate::new(
            10,
            11,
            12,
            input(0, 2),
            vec![column(0, 1)],
            vec![],
            vec![count],
            vec![],
        );
        let plan = filtered(
            projected(
                OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(aggregate))),
                20,
                column(10, 0),
            ),
            vec![equal(20, 0)],
        );
        let (engine, state, root) = frozen(plan);
        let state = state.read().unwrap();
        let bindings = selected_transfer_bindings(engine.memo(), &root, &state);
        assert_eq!(bindings.len(), 1);
        let PatternOperand::Expression {
            children: filter_children,
            ..
        } = &bindings[0].root
        else {
            panic!("selected binding root is not a filter");
        };
        let PatternOperand::Expression {
            children: projection_children,
            ..
        } = &filter_children[0]
        else {
            panic!("selected binding did not retain the projection");
        };
        assert!(
            matches!(&projection_children[0], PatternOperand::Expression { .. }),
            "the production binding stopped before the selected aggregate"
        );
    }

    fn conjunction(kind: ConjunctionType, expressions: Vec<Expression>) -> Expression {
        Expression::Conjunction(ConjunctionExpression::new(kind, expressions).into())
    }

    #[test]
    fn necessary_domain_residual_is_pending_only_until_selected_input_consumes_it() {
        for kind in [ConjunctionType::And, ConjunctionType::Or] {
            for consumed in [false, true] {
                let LogicalOperator::Filter(residual) = branch(0, true, true).into_operator()
                else {
                    unreachable!()
                };
                let mixed = conjunction(
                    ConjunctionType::And,
                    vec![equal(1, 0), residual.expressions[0].clone()],
                );
                let predicate = if kind == ConjunctionType::Or {
                    conjunction(kind, vec![mixed, equal(1, 0)])
                } else {
                    mixed
                };
                let aggregate = if consumed {
                    branch(0, true, false)
                } else {
                    let LogicalOperator::Filter(filter) = branch(0, false, false).into_operator()
                    else {
                        unreachable!()
                    };
                    *filter.child
                };
                let (engine, state, root) = frozen(filtered(aggregate, vec![predicate]));
                let state = state.read().unwrap();
                assert_eq!(
                    pending_transfers(&root, &state)
                        .unwrap()
                        .contains(&root.reference.candidate),
                    !consumed
                );
                assert_eq!(
                    selected_transfer_binding(engine.memo(), &root, &state).is_some(),
                    !consumed
                );
                if consumed && kind == ConjunctionType::And {
                    let LogicalOperator::Filter(filter) = &state.payloads.logical
                        [root.logical.payload.index()]
                    .semantic_template
                    .operator
                    else {
                        unreachable!()
                    };
                    assert!(
                        !selected_consumes(&root.children[0], &filter.expressions, &state),
                        "consuming a necessary domain does not consume the aggregate residual"
                    );
                }
            }
        }
    }

    #[test]
    fn consumption_follows_projection_and_requires_every_union_branch() {
        for consumed in [[false, false], [true, false], [false, true], [true, true]] {
            let child = |table, consumed| {
                let input = if consumed {
                    filtered(input(table, 1), vec![equal(table, 0)])
                } else {
                    filtered(input(table, 1), vec![equal_value(table, 0, 3)])
                };
                projected(input, table + 1, column(table, 0))
            };
            let union =
                OwnedLogicalPlan::synthetic(LogicalOperator::SetOperation(SetOperation::union(
                    30,
                    child(0, consumed[0]),
                    child(10, consumed[1]),
                    true,
                    vec![LogicalType::Integer],
                )));
            let (engine, state, root) = frozen(filtered(union, vec![equal(30, 0)]));
            let mut state = state.write().unwrap();
            let expected = !consumed.into_iter().all(|value| value);
            assert_eq!(
                pending_transfers(&root, &state)
                    .unwrap()
                    .contains(&root.reference.candidate),
                expected
            );
            assert_eq!(
                selected_transfer_binding(engine.memo(), &root, &state).is_some(),
                expected
            );
            if !expected {
                let input_filter = &root.children[0].children[1].children[0];
                state.metadata.remove(&input_filter.logical.payload);
                assert!(!selected_transfer_consumed(
                    &root.children[0],
                    &equal(30, 0),
                    &state
                ));
                assert!(selected_transfer_binding(engine.memo(), &root, &state).is_some());
            }
        }
    }

    #[test]
    fn consumption_accepts_safe_and_coverage_but_not_or_or_weaker_domains() {
        let and = conjunction(ConjunctionType::And, vec![equal(0, 0), equal(0, 1)]);
        let or = conjunction(ConjunctionType::Or, vec![equal(0, 0), equal(0, 1)]);
        let reordered_or = conjunction(ConjunctionType::Or, vec![equal(0, 1), equal(0, 0)]);
        for (enforced, requested, expected) in [
            (vec![and.clone()], vec![equal(0, 0)], true),
            (vec![equal(0, 0), equal(0, 1)], vec![and.clone()], true),
            (vec![or.clone()], vec![equal(0, 0)], false),
            (vec![equal(0, 0)], vec![and], false),
            (vec![or.clone()], vec![or.clone()], true),
            (vec![reordered_or], vec![or], true),
        ] {
            let (_, state, root) = frozen(filtered(input(0, 2), enforced));
            assert_eq!(
                selected_consumes(&root, &requested, &state.read().unwrap()),
                expected
            );
        }
    }

    #[test]
    fn consumption_uses_shared_projection_constant_folding() {
        for consumed in [false, true] {
            let child = if consumed {
                filtered(input(0, 1), vec![equal(0, 0)])
            } else {
                input(0, 1)
            };
            let projection =
                OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
                    10,
                    child,
                    vec![
                        column(0, 0),
                        Expression::Constant(
                            ConstantExpression::new(Value::Integer(2), LogicalType::Integer).into(),
                        ),
                    ],
                )));
            let predicate = conjunction(ConjunctionType::And, vec![equal(10, 0), equal(10, 1)]);
            let (engine, state, root) = frozen(filtered(projection, vec![predicate]));
            let state = state.read().unwrap();
            assert_eq!(
                pending_transfers(&root, &state)
                    .unwrap()
                    .contains(&root.reference.candidate),
                !consumed
            );
            assert_eq!(
                selected_transfer_binding(engine.memo(), &root, &state).is_some(),
                !consumed
            );
        }
    }
}
