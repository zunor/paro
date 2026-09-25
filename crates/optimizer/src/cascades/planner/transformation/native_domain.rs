// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! One exact domain-transfer binding, without rebuilding its Memo operands.
//!
//! This is a complete producer for the admitted local binding, not proof that
//! PredicateTransfer's other bindings or the surrounding logical domain closed.

use super::settlement::{NativeRelationEntry, NativeRelationInput};
use super::staging::{
    intern_columns_into, ColumnInternOrigin, ResidentInputFact, ResidentInputFacts,
    ResidentNodeContract,
};
use super::*;
use crate::cascades::memo::LogicalExpr;
use crate::cascades::planner::domain_transfer;
use crate::rewrite::expr::traversal::visit_expression;
use paro_planner::operator::{BoundReference, BoundReferenceId, LogicalOutputLayout};
use paro_planner::plan::PlanNodeId;

fn local_domain(predicate: &Expression) -> bool {
    domain_transfer::is_local_domain(predicate)
}

fn hole_layout(child: &NativeChild) -> Option<&LogicalOutputLayout> {
    match child {
        NativeChild::MemoGroup { layout, .. } => Some(layout),
        _ => None,
    }
}

#[derive(Debug)]
pub(super) struct NativeTransferResult {
    pub(super) shell: NativeShell,
    pub(super) continuations: Box<[DomainContinuation]>,
    /// Reads which keep a group-hole continuation alive, including a negative
    /// shell lookup.  A missing legal shell is not completion: a later Memo
    /// publication must wake the exact producer again.
    pub(super) reads: Box<[PatternRead]>,
}

/// Produce one exact native rewrite and, when explicitly enabled by the
/// quality lane, retain resumable work for transparent operators hidden
/// behind Memo group holes.  The continuation is an ordering hint: each
/// binding is still run through the normal transaction, proof validation and
/// Memo publication path.
pub(super) fn try_transfer_with_continuations(
    binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
    facts: &boundary::BoundarySnapshot,
    binding_fact_value: Fingerprint,
    enable_continuations: bool,
) -> Result<Option<NativeTransferResult>> {
    let PatternOperand::Expression {
        children,
        expression,
        ..
    } = binding
    else {
        return Ok(None);
    };
    let Some(payload) = memo
        .logical_expr(*expression)
        .and_then(|logical| state.payloads.logical.get(logical.payload.index()))
    else {
        return Ok(None);
    };
    let Some(logical) = memo.logical_expr(*expression) else {
        return Ok(None);
    };
    let Some(metadata) = state.metadata.get(&logical.payload) else {
        return Ok(None);
    };
    let LogicalOperator::Filter(filter) = &payload.semantic_template.operator else {
        return Ok(None);
    };
    if filter.expressions.is_empty()
        || !filter.expressions.iter().all(local_domain)
        || filter
            .expressions
            .iter()
            .any(|expression| expression.evaluation_properties().is_reorder_fence())
    {
        return Ok(None);
    }
    let [PatternOperand::Expression {
        expression: input,
        children: inputs,
        ..
    }] = children.as_ref()
    else {
        return Ok(None);
    };
    let Some(input) = memo
        .logical_expr(*input)
        .and_then(|logical| state.payloads.logical.get(logical.payload.index()))
    else {
        return Ok(None);
    };
    let supported = match &input.semantic_template.operator {
        LogicalOperator::Projection(projection) => {
            projection.expressions.iter().all(|expression| {
                matches!(expression, Expression::ColumnRef(column) if column.depth == 0)
                    || matches!(expression, Expression::Constant(_))
            })
        }
        LogicalOperator::Aggregate(aggregate) => aggregate.has_plain_grouping_domain(),
        LogicalOperator::SetOperation(setop) => {
            setop.setop_type == SetOpType::Union && setop.setop_all
        }
        LogicalOperator::Filter(_)
        | LogicalOperator::Join(Join::Comparison(_))
        | LogicalOperator::Join(Join::Cross(_)) => true,
        _ => false,
    };
    if !supported {
        return Ok(None);
    }
    // `from_pattern_with_layouts` already computes the exact layouts needed by
    // both routing and the closure walker.  Reusing that immutable vector is
    // important here: rebuilding the same post-order layout in
    // `transfer_shell` used to make every domain binding pay for a second
    // traversal before any semantic work began.
    let Some((shell, layouts)) =
        NativeShell::from_pattern_with_layouts(memo, state, binding, facts)?
    else {
        return Ok(None);
    };
    // This subset moves only within a producer. An opaque ownership/graph
    // boundary cannot become ordinary input merely because its layout fits.
    if native_shell_contains_control_boundary(&shell) {
        return Ok(None);
    }
    let binding_group = match binding {
        PatternOperand::Expression { group, .. } | PatternOperand::Group(group) => *group,
    };
    let root_group = memo.canonical_group(binding_group);
    let Some(root) = memo.group(root_group) else {
        return Ok(None);
    };
    let mut fixed_point =
        domain_transfer::DomainFixedPoint::new(domain_transfer::DomainFactContext {
            relation: root_group,
            occurrence: *expression,
            context: metadata.input_context,
            logical_facts: root.logical_fact_fingerprint(),
            statistics: root.statistics_snapshot_fingerprint(),
            binding_facts: binding_fact_value,
        });
    let nested_path = inputs
        .iter()
        .any(|input| matches!(input, PatternOperand::Expression { .. }));
    let Some((mut shell, layouts)) = (if nested_path {
        transfer_shell_closure_with_layouts(shell, layouts, state, &mut fixed_point)?
    } else {
        transfer_shell_with_layouts(shell, layouts, state)?
    }) else {
        return Ok(None);
    };
    // Canonical templates deliberately erase occurrence output demand. Do
    // not suppress the semantic producer until this result owns that exact
    // ordered output again, including residual Filter projection maps.
    if let LogicalOperator::Filter(filter) = &mut shell.nodes[shell.root].operator {
        let child_layout = match &filter.child {
            NativeChild::Node(index) => &layouts[*index],
            NativeChild::MemoGroup { layout, .. } => layout,
            NativeChild::Group { .. } => return Ok(None),
        };
        let Ok(projection) = semantic_plan::projection_for_bindings(
            child_layout.bindings(),
            child_layout.types(),
            &metadata.output_columns,
            &state.binding_ids,
        ) else {
            return Ok(None);
        };
        filter.projection_map = projection;
    }
    // The post-closure child layouts were already derived above for rebinding
    // the residual filter. Recompute only the root from those cached child
    // views: the projection map may change the root output layout, so simply
    // reusing the old root entry would be stale, while walking the whole shell
    // again through `root_layout()` is redundant on every accepted binding.
    let mut output_children = SmallVec::<[LogicalOutputLayout; 2]>::new();
    let mut output_child_count = 0;
    shell.nodes[shell.root]
        .operator
        .visit_child_links(&mut |child| {
            output_child_count += 1;
            let layout = match child {
                NativeChild::Node(index) => layouts.get(*index).cloned(),
                NativeChild::MemoGroup { layout, .. } | NativeChild::Group { layout, .. } => {
                    Some(layout.clone())
                }
            };
            if let Some(layout) = layout {
                output_children.push(layout);
            }
        });
    if output_children.len() != output_child_count {
        return Ok(None);
    }
    let output_child_refs = output_children.iter().collect::<SmallVec<[_; 2]>>();
    let output = shell.nodes[shell.root]
        .operator
        .output_layout_from_child_refs(&output_child_refs);
    let columns = output
        .bindings()
        .iter()
        .zip(output.types())
        .map(|(binding, ty)| {
            state
                .binding_ids
                .get(binding.table_index, binding.column_index, ty)
                .copied()
        })
        .collect::<Option<Vec<_>>>();
    if columns.as_deref() != Some(metadata.output_columns.as_ref()) {
        return Ok(None);
    }
    let (continuations, reads) = if enable_continuations {
        discover_group_hole_continuations(binding, memo, state)?
    } else {
        (Vec::new(), Vec::new())
    };
    Ok(Some(NativeTransferResult {
        shell,
        continuations: continuations.into_boxed_slice(),
        reads: reads.into_boxed_slice(),
    }))
}

/// Discover the next exact Memo choices after a native closure reaches a
/// group hole.  This is deliberately a one-edge operation: it observes the
/// current group's logical frontier, records the negative lookup, and emits
/// only bindings whose immediate operator can consume the already-proven
/// predicates.  The continuation is then run through the ordinary
/// transformation transaction, so publication, budget accounting and
/// verification remain single-sourced.
fn discover_group_hole_continuations(
    binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
) -> Result<(Vec<DomainContinuation>, Vec<PatternRead>)> {
    struct Discovery<'a> {
        memo: &'a Memo,
        state: &'a PlannerTransformState,
        continuations: Vec<DomainContinuation>,
        seen_bindings: BTreeSet<PatternBinding>,
        reads: BTreeMap<GroupId, PatternRead>,
    }

    impl Discovery<'_> {
        fn operator(
            &self,
            expression: LogicalExprId,
        ) -> Option<(&LogicalExpr, &PlannerOperatorMetadata, &LogicalOperator<()>)> {
            let logical = self.memo.logical_expr(expression)?;
            let payload = self.state.payloads.logical.get(logical.payload.index())?;
            let metadata = self.state.metadata.get(&logical.payload)?;
            Some((logical, metadata, &payload.semantic_template.operator))
        }

        fn read_group(&mut self, group: GroupId) -> Result<PatternRead> {
            let group = self.memo.canonical_group(group);
            if let Some(read) = self.reads.get(&group).copied() {
                return Ok(read);
            }
            // `from_group` includes the logical frontier even when no
            // supported shell is present.  That negative observation is what
            // makes a later shell publication wake this continuation.
            let read = PatternRead::from_group(self.memo, group)?;
            self.reads.insert(group, read);
            Ok(read)
        }

        fn inspect_hole(
            &mut self,
            group: GroupId,
            predicates: &[Expression],
            path: &[usize],
            root: &PatternOperand,
            parent_occurrence: LogicalExprId,
            parent_context: OptimizationContextId,
        ) -> Result<()> {
            let group = self.memo.canonical_group(group);
            let read = self.read_group(group)?;
            let Some(group_ref) = self.memo.group(group) else {
                return Ok(());
            };
            // Only inspect immediate logical alternatives.  In particular,
            // do not recursively enumerate a child's frontier here: the
            // emitted binding is the resumable unit for the next wake-up.
            let candidates = group_ref
                .logical_exprs()
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            for candidate in candidates {
                let Some((logical, metadata, operator)) = self.operator(candidate) else {
                    continue;
                };
                if !continuation_operator_supported(operator) {
                    continue;
                }
                if logical.key.children.len() != metadata.child_layouts.len() {
                    continue;
                }
                let layouts = metadata
                    .child_layouts
                    .iter()
                    .map(|layout| layout.as_ref())
                    .collect::<Vec<_>>();
                let Some(transfer) =
                    domain_transfer::transfer_predicates(operator, &layouts, predicates)
                else {
                    continue;
                };
                if !transfer
                    .child_predicates
                    .iter()
                    .any(|child| !child.is_empty())
                {
                    continue;
                }
                let child_operands = logical
                    .key
                    .children
                    .iter()
                    .copied()
                    .map(|child| PatternOperand::Group(self.memo.canonical_group(child)))
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                let replacement = PatternOperand::Expression {
                    group,
                    expression: candidate,
                    children: child_operands,
                };
                let Some(next_root) = replace_pattern_operand(root, path, &replacement) else {
                    continue;
                };
                let next_binding = PatternBinding {
                    fingerprint: continuation_binding_fingerprint(
                        &next_root,
                        parent_occurrence,
                        parent_context,
                    ),
                    root: next_root,
                };
                if !self.seen_bindings.insert(next_binding.clone()) {
                    continue;
                }
                let predicates = transfer
                    .child_predicates
                    .iter()
                    .flat_map(|predicates| predicates.iter().cloned())
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                self.continuations.push(DomainContinuation {
                    binding: next_binding,
                    hole: group,
                    predicates,
                    reads: Box::new([read]),
                    occurrence: parent_occurrence,
                    context: parent_context,
                });
            }
            Ok(())
        }

        fn walk(
            &mut self,
            operand: &PatternOperand,
            predicates: &[Expression],
            path: &[usize],
            root: &PatternOperand,
        ) -> Result<()> {
            if predicates.is_empty() {
                return Ok(());
            }
            let PatternOperand::Expression {
                expression,
                children,
                ..
            } = operand
            else {
                return Ok(());
            };
            let (routed, input_context) = {
                let Some((logical, metadata, operator)) = self.operator(*expression) else {
                    return Ok(());
                };
                if logical.key.children.len() != children.len()
                    || logical.key.children.len() != metadata.child_layouts.len()
                    || !continuation_operator_supported(operator)
                {
                    return Ok(());
                }
                let layouts = metadata
                    .child_layouts
                    .iter()
                    .map(|layout| layout.as_ref())
                    .collect::<Vec<_>>();
                let Some(transfer) =
                    domain_transfer::transfer_predicates(operator, &layouts, predicates)
                else {
                    return Ok(());
                };
                (
                    IntoIterator::into_iter(transfer.child_predicates)
                        .map(|predicates| predicates.to_vec())
                        .collect::<Vec<_>>(),
                    metadata.input_context,
                )
            };
            for (index, child_predicates) in routed.iter().enumerate() {
                if child_predicates.is_empty() {
                    continue;
                }
                let Some(child) = children.get(index) else {
                    continue;
                };
                let mut child_path = path.to_vec();
                child_path.push(index);
                match child {
                    PatternOperand::Group(group) => self.inspect_hole(
                        *group,
                        child_predicates,
                        &child_path,
                        root,
                        *expression,
                        input_context,
                    )?,
                    PatternOperand::Expression { .. } => {
                        self.walk(child, child_predicates, &child_path, root)?
                    }
                }
            }
            Ok(())
        }
    }

    let PatternOperand::Expression { expression, .. } = binding else {
        return Ok((Vec::new(), Vec::new()));
    };
    let predicates = {
        let discovery = Discovery {
            memo,
            state,
            continuations: Vec::new(),
            seen_bindings: BTreeSet::new(),
            reads: BTreeMap::new(),
        };
        let Some((_, _, LogicalOperator::Filter(filter))) = discovery.operator(*expression) else {
            return Ok((Vec::new(), Vec::new()));
        };
        filter.expressions.clone()
    };
    let mut discovery = Discovery {
        memo,
        state,
        continuations: Vec::new(),
        seen_bindings: BTreeSet::new(),
        reads: BTreeMap::new(),
    };
    discovery.walk(binding, &predicates, &[], binding)?;
    Ok((
        discovery.continuations,
        discovery.reads.into_values().collect(),
    ))
}

fn continuation_operator_supported(operator: &LogicalOperator<()>) -> bool {
    match operator {
        LogicalOperator::Filter(_)
        | LogicalOperator::Projection(_)
        | LogicalOperator::Aggregate(_)
        | LogicalOperator::Join(Join::Comparison(_))
        | LogicalOperator::Join(Join::Cross(_)) => true,
        LogicalOperator::SetOperation(setop) => {
            setop.setop_type == SetOpType::Union && setop.setop_all
        }
        _ => false,
    }
}

fn replace_pattern_operand(
    root: &PatternOperand,
    path: &[usize],
    replacement: &PatternOperand,
) -> Option<PatternOperand> {
    if path.is_empty() {
        return Some(replacement.clone());
    }
    let PatternOperand::Expression {
        group,
        expression,
        children,
    } = root
    else {
        return None;
    };
    let index = *path.first()?;
    let child = children.get(index)?;
    let child = replace_pattern_operand(child, &path[1..], replacement)?;
    let mut children = children.to_vec();
    children[index] = child;
    Some(PatternOperand::Expression {
        group: *group,
        expression: *expression,
        children: children.into_boxed_slice(),
    })
}

fn continuation_binding_fingerprint(
    operand: &PatternOperand,
    occurrence: LogicalExprId,
    context: OptimizationContextId,
) -> Fingerprint {
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
    // The same structural path can be requested by distinct CTE or quality
    // occurrences. Keep those proof contexts in the task identity so a
    // binding fingerprint cannot collapse different ownership/evaluation
    // contexts merely because their Memo choices match.
    fingerprint.write_u64(occurrence.0 as u64);
    fingerprint.write_u64(context.0 as u64);
    write(&mut fingerprint, operand);
    fingerprint.finish()
}

/// Test-facing compatibility wrapper. Production callers should pass the
/// layouts returned by `NativeShell::from_pattern_with_layouts` so the exact
/// same shell is not walked twice.
#[cfg(test)]
pub(super) fn transfer_shell(
    shell: NativeShell,
    state: &PlannerTransformState,
) -> Result<Option<NativeShell>> {
    let layouts = shell.layouts()?;
    transfer_shell_with_layouts(shell, layouts, state)
        .map(|result| result.map(|(shell, _layouts)| shell))
}

fn transfer_shell_with_layouts(
    shell: NativeShell,
    mut layouts: Vec<LogicalOutputLayout>,
    state: &PlannerTransformState,
) -> Result<Option<(NativeShell, Vec<LogicalOutputLayout>)>> {
    let LogicalOperator::Filter(filter) = shell.root_operator().clone() else {
        return Ok(None);
    };
    if filter
        .expressions
        .iter()
        .any(|predicate| predicate.evaluation_properties().is_reorder_fence())
    {
        return Ok(None);
    }
    let NativeChild::Node(input) = filter.child else {
        return Ok(None);
    };
    let Some(predicates) = FilterPushdown::normalize_predicates(filter.expressions) else {
        return Ok(None);
    };
    let mut nodes = shell.nodes.into_vec();
    let mut operator = nodes[input].operator.clone();
    let mut remaining = Vec::new();
    let mut routed = Vec::<(NativeChild, Vec<Expression>)>::new();
    match &mut operator {
        LogicalOperator::Projection(projection) => {
            let Some(layout) = hole_layout(&projection.child) else {
                return Ok(None);
            };
            let moved = predicates
                .iter()
                .map(|predicate| {
                    projection_domain(predicate, projection, &projection.child, layout, &nodes)
                })
                .collect::<Option<Vec<_>>>();
            let Some(moved) = moved else {
                return Ok(None);
            };
            routed.push((projection.child.clone(), moved));
        }
        LogicalOperator::Aggregate(aggregate) if aggregate.has_plain_grouping_domain() => {
            let Some(layout) = hole_layout(&aggregate.child) else {
                return Ok(None);
            };
            let mut moved = Vec::new();
            for predicate in predicates {
                if let Some((necessary, retain)) = aggregate_domain(&predicate, aggregate, layout) {
                    moved.push(necessary);
                    if retain {
                        remaining.push(predicate);
                    }
                } else {
                    remaining.push(predicate);
                }
            }
            routed.push((aggregate.child.clone(), moved));
        }
        LogicalOperator::SetOperation(setop)
            if setop.setop_type == SetOpType::Union && setop.setop_all =>
        {
            let (Some(left), Some(right)) = (hole_layout(&setop.left), hole_layout(&setop.right))
            else {
                return Ok(None);
            };
            if left.len() != setop.column_count
                || right.len() != setop.column_count
                || left.types() != right.types()
                || left.types() != setop.types.as_slice()
            {
                return Ok(None);
            }
            let mut moved_left = Vec::new();
            let mut moved_right = Vec::new();
            for predicate in predicates {
                let Some((left_predicate, right_predicate)) =
                    union_domain(&predicate, setop, left, right)
                else {
                    return Ok(None);
                };
                moved_left.push(left_predicate);
                moved_right.push(right_predicate);
            }
            routed.push((setop.left.clone(), moved_left));
            routed.push((setop.right.clone(), moved_right));
        }
        _ => return Ok(None),
    }
    if routed.iter().all(|(_, predicates)| predicates.is_empty()) {
        return Ok(None);
    }
    let mut children = Vec::new();
    for (child, predicates) in routed {
        if predicates.is_empty() {
            children.push(child);
            continue;
        }
        let Some(predicates) = FilterPushdown::normalize_predicates(predicates) else {
            return Ok(None);
        };
        if predicates.is_empty() {
            children.push(child);
            continue;
        }
        let next = nodes.len();
        let child_layout = native_child_layout(&child, &layouts)?;
        let operator = LogicalOperator::Filter(Filter {
            child,
            expressions: predicates,
            projection_map: paro_planner::operator::ProjectionMap::all(),
        });
        let output_layout = operator.output_layout_from_child_refs(&[&child_layout]);
        nodes.push(NativeNode {
            id: state.bind_context.next_plan_id(),
            stats: NodeStats::default(),
            operator,
            source_proofs: Box::new([]),
        });
        layouts.push(output_layout);
        children.push(NativeChild::Node(next));
    }
    let mut children = children.into_iter();
    operator = operator.try_map_child_links(&mut |_| {
        children
            .next()
            .ok_or_else(|| paro_error::internal("native domain lost a child"))
    })?;
    let output_layout = {
        let mut operator_children = SmallVec::<[&NativeChild; 2]>::new();
        operator.visit_child_links(&mut |child| operator_children.push(child));
        let operator_child_layouts = operator_children
            .iter()
            .map(|child| match child {
                NativeChild::Node(index) => layouts
                    .get(*index)
                    .ok_or_else(|| paro_error::internal("native domain child layout is missing")),
                NativeChild::MemoGroup { layout, .. } | NativeChild::Group { layout, .. } => {
                    Ok(layout)
                }
            })
            .collect::<Result<SmallVec<[&LogicalOutputLayout; 2]>>>()?;
        operator.output_layout_from_child_refs(&operator_child_layouts)
    };
    let next = nodes.len();
    nodes.push(NativeNode {
        id: state.bind_context.next_plan_id(),
        stats: NodeStats::default(),
        operator,
        source_proofs: nodes[input].source_proofs.clone(),
    });
    layouts.push(output_layout);
    let root = if remaining.is_empty() && filter.projection_map.is_identity(layouts[input].len()) {
        next
    } else {
        let root = nodes.len();
        let child_layout = layouts
            .get(next)
            .cloned()
            .ok_or_else(|| paro_error::internal("native domain root layout is missing"))?;
        let operator = LogicalOperator::Filter(Filter {
            expressions: remaining,
            child: NativeChild::Node(next),
            projection_map: filter.projection_map,
        });
        let output_layout = operator.output_layout_from_child_refs(&[&child_layout]);
        nodes.push(NativeNode {
            id: state.bind_context.next_plan_id(),
            stats: NodeStats::default(),
            operator,
            source_proofs: Box::new([]),
        });
        layouts.push(output_layout);
        root
    };
    super::compact_native_shell_with_layouts(
        NativeShell {
            nodes: nodes.into_boxed_slice(),
            root,
        },
        layouts,
    )
    .map(Some)
}

#[derive(Debug)]
struct RoutedDomain {
    child: NativeChild,
    /// Predicates that could not cross the current operator. They remain in
    /// the current output namespace; a projection/UNION caller must never put
    /// a child-namespaced residual above its parent.
    remaining: Vec<Expression>,
    moved: bool,
}

fn child_path(path: &[usize], edge: usize) -> Vec<usize> {
    let mut child = path.to_vec();
    child.push(edge);
    child
}

/// A cheap rollback journal for native closure construction.
///
/// Predicate routing speculatively walks several alternatives.  The old
/// implementation cloned the complete shell before every recursive attempt so
/// a rejected route could be undone.  That made the cost of a local rewrite
/// proportional to the whole partially-built shell.  The journal records only
/// operators that were actually replaced; appended nodes and layouts are
/// discarded by length.  It is deliberately local to one closure, so it does
/// not introduce a second Memo or result cache.
#[derive(Default)]
struct NativeRewriteJournal {
    operators: Vec<(usize, LogicalOperator<NativeChild>)>,
}

#[derive(Clone, Copy)]
struct NativeRewriteCheckpoint {
    node_len: usize,
    layout_len: usize,
    operator_len: usize,
    fixed_point_len: usize,
}

impl NativeRewriteJournal {
    fn checkpoint(
        &self,
        nodes: &[NativeNode],
        layouts: &[LogicalOutputLayout],
        fixed_point: &domain_transfer::DomainFixedPoint,
    ) -> NativeRewriteCheckpoint {
        NativeRewriteCheckpoint {
            node_len: nodes.len(),
            layout_len: layouts.len(),
            operator_len: self.operators.len(),
            fixed_point_len: fixed_point.checkpoint(),
        }
    }

    fn record_operator(&mut self, nodes: &[NativeNode], index: usize) {
        self.operators.push((index, nodes[index].operator.clone()));
    }

    fn rollback(
        &mut self,
        nodes: &mut Vec<NativeNode>,
        layouts: &mut Vec<LogicalOutputLayout>,
        fixed_point: &mut domain_transfer::DomainFixedPoint,
        checkpoint: NativeRewriteCheckpoint,
    ) {
        while self.operators.len() > checkpoint.operator_len {
            let (index, operator) = self
                .operators
                .pop()
                .expect("native rewrite journal length checked");
            nodes[index].operator = operator;
        }
        nodes.truncate(checkpoint.node_len);
        layouts.truncate(checkpoint.layout_len);
        fixed_point.rollback(checkpoint.fixed_point_len);
    }
}

fn native_child_layout(
    child: &NativeChild,
    layouts: &[LogicalOutputLayout],
) -> Result<LogicalOutputLayout> {
    match child {
        NativeChild::Node(index) => layouts
            .get(*index)
            .cloned()
            .ok_or_else(|| paro_error::internal("native domain references an unknown node")),
        NativeChild::MemoGroup { layout, .. } | NativeChild::Group { layout, .. } => {
            Ok(layout.clone())
        }
    }
}

fn native_child_stats(nodes: &[NativeNode], child: &NativeChild) -> NodeStats {
    match child {
        NativeChild::Node(index) => nodes
            .get(*index)
            .map(|node| node.stats.clone())
            .unwrap_or_default(),
        NativeChild::MemoGroup { stats, .. } | NativeChild::Group { stats, .. } => stats.clone(),
    }
}

fn add_native_filter(
    nodes: &mut Vec<NativeNode>,
    layouts: &mut Vec<LogicalOutputLayout>,
    child: NativeChild,
    expressions: Vec<Expression>,
    projection_map: paro_planner::operator::ProjectionMap,
    state: &PlannerTransformState,
) -> Result<NativeChild> {
    let child_layout = native_child_layout(&child, layouts)?;
    let stats = native_child_stats(nodes, &child);
    let index = nodes.len();
    let operator = LogicalOperator::Filter(Filter {
        child,
        expressions,
        projection_map,
    });
    let output = operator.output_layout_from_child_refs(&[&child_layout]);
    nodes.push(NativeNode {
        id: state.bind_context.next_plan_id(),
        stats,
        operator,
        source_proofs: Box::new([]),
    });
    layouts.push(output);
    Ok(NativeChild::Node(index))
}

fn native_child_contains_graph(
    child: &NativeChild,
    nodes: &[NativeNode],
    visiting: &mut BTreeSet<usize>,
) -> bool {
    let NativeChild::Node(index) = child else {
        return false;
    };
    if !visiting.insert(*index) {
        return false;
    }
    let Some(node) = nodes.get(*index) else {
        return true;
    };
    let mut graph = matches!(
        &node.operator,
        LogicalOperator::GraphMatch(_)
            | LogicalOperator::GraphScan(_)
            | LogicalOperator::GraphExpand(_)
    );
    node.operator.visit_child_links(&mut |child| {
        graph |= native_child_contains_graph(child, nodes, visiting);
    });
    graph
}

fn projection_domain(
    predicate: &Expression,
    projection: &Projection<NativeChild>,
    child: &NativeChild,
    child_layout: &LogicalOutputLayout,
    nodes: &[NativeNode],
) -> Option<Expression> {
    let operator = LogicalOperator::Projection(projection.clone());
    let routed =
        domain_transfer::transfer_predicates(&operator, &[child_layout], &[predicate.clone()])?;
    if !routed.remaining.is_empty() {
        return None;
    }
    let mapped = routed.child_predicates.first()?.first()?.clone();
    let mut owns = true;
    visit_expression(predicate, &mut |part| {
        if let Expression::ColumnRef(column) = part {
            owns &= column.depth == 0;
            if let Some(Expression::ColumnRef(projected)) =
                projection.expressions.get(column.binding.column_index)
            {
                owns &=
                    projected.depth == 0 && child_layout.bindings().contains(&projected.binding);
                // GRAPH_TABLE row-id/local-id projections are not ordinary
                // relational lineage. Group-hole references carry source
                // lineage; nested native nodes are checked recursively for a
                // graph operator below.
                if let NativeChild::MemoGroup { reference, .. } = child {
                    owns &= child_layout
                        .bindings()
                        .iter()
                        .position(|binding| *binding == projected.binding)
                        .and_then(|ordinal| reference.facts.source_lineage.get(ordinal))
                        .is_some_and(|sources| {
                            sources.as_ref().is_some_and(|sources| !sources.is_empty())
                        });
                }
            }
        }
    });
    if !owns || native_child_contains_graph(child, nodes, &mut BTreeSet::new()) {
        return None;
    }
    Some(mapped)
}

fn aggregate_domain(
    predicate: &Expression,
    aggregate: &Aggregate<NativeChild>,
    child_layout: &LogicalOutputLayout,
) -> Option<(Expression, bool)> {
    let operator = LogicalOperator::Aggregate(Box::new(aggregate.clone()));
    let routed =
        domain_transfer::transfer_predicates(&operator, &[child_layout], &[predicate.clone()])?;
    Some((
        routed.child_predicates.first()?.first()?.clone(),
        !routed.remaining.is_empty(),
    ))
}

fn union_domain(
    predicate: &Expression,
    setop: &SetOperation<NativeChild>,
    left: &LogicalOutputLayout,
    right: &LogicalOutputLayout,
) -> Option<(Expression, Expression)> {
    let operator = LogicalOperator::SetOperation(setop.clone());
    let routed =
        domain_transfer::transfer_predicates(&operator, &[left, right], &[predicate.clone()])?;
    if !routed.remaining.is_empty() {
        return None;
    }
    Some((
        routed.child_predicates.first()?.first()?.clone(),
        routed.child_predicates.get(1)?.first()?.clone(),
    ))
}

/// Push a batch of predicates through one exact transparent path. This is a
/// local closure, not a second Memo optimizer: every Node is already present
/// in the exact PatternOperand and every leaf remains an immutable Memo hole.
fn push_domain(
    arena: (&mut Vec<NativeNode>, &mut Vec<LogicalOutputLayout>),
    child: NativeChild,
    predicates: Vec<Expression>,
    state: &PlannerTransformState,
    journal: &mut NativeRewriteJournal,
    fixed_point: &mut domain_transfer::DomainFixedPoint,
    path: &[usize],
) -> Result<RoutedDomain> {
    let (nodes, layouts) = arena;
    if predicates.is_empty() {
        return Ok(RoutedDomain {
            child,
            remaining: Vec::new(),
            moved: false,
        });
    }
    let relation = match &child {
        NativeChild::MemoGroup { group, .. } => *group,
        NativeChild::Node(_) | NativeChild::Group { .. } => fixed_point.root_relation(),
    };
    let fresh_predicates = predicates
        .iter()
        .filter(|predicate| !fixed_point.is_seen(relation, path, predicate))
        .cloned()
        .collect::<Vec<_>>();
    if fresh_predicates.is_empty() {
        // The exact landing point already consumed this request in the same
        // immutable shell.  Treat it as moved so callers do not reinstall a
        // residual filter, but do not manufacture another node.
        return Ok(RoutedDomain {
            child,
            remaining: Vec::new(),
            moved: true,
        });
    }
    let predicates = fresh_predicates;
    let NativeChild::Node(index) = child.clone() else {
        let recorded_predicates = predicates.clone();
        let child = add_native_filter(
            nodes,
            layouts,
            child,
            predicates,
            paro_planner::operator::ProjectionMap::all(),
            state,
        )?;
        for predicate in &recorded_predicates {
            fixed_point.record(relation, path, predicate);
        }
        return Ok(RoutedDomain {
            child,
            remaining: Vec::new(),
            moved: true,
        });
    };
    let original = nodes
        .get(index)
        .ok_or_else(|| paro_error::internal("native domain child node disappeared"))?
        .operator
        .clone();
    match original {
        LogicalOperator::Filter(mut filter)
            if !filter
                .expressions
                .iter()
                .any(|expression| expression.evaluation_properties().is_reorder_fence())
                && filter.expressions.iter().all(local_domain)
                && filter
                    .projection_map
                    .is_identity(native_child_layout(&filter.child, layouts)?.len()) =>
        {
            let original_predicates = predicates.clone();
            if matches!(filter.child, NativeChild::MemoGroup { .. }) {
                let landing_path = child_path(path, 0);
                let landing_relation = match &filter.child {
                    NativeChild::MemoGroup { group, .. } => *group,
                    NativeChild::Node(_) | NativeChild::Group { .. } => relation,
                };
                let landing_predicates = predicates
                    .iter()
                    .filter(|predicate| {
                        !fixed_point.is_seen(landing_relation, &landing_path, predicate)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                if landing_predicates.is_empty() {
                    return Ok(RoutedDomain {
                        child,
                        remaining: Vec::new(),
                        moved: true,
                    });
                }
                let layout = native_child_layout(&filter.child, layouts)?;
                let transfer = domain_transfer::transfer_predicates(
                    &LogicalOperator::Filter(filter.clone()),
                    &[&layout],
                    &landing_predicates,
                );
                if transfer
                    .as_ref()
                    .is_some_and(|transfer| !transfer.unsupported && transfer.remaining.is_empty())
                {
                    // This is the legal input landing point, not another
                    // propagation hop. Keep its exact input and namespace;
                    // combine the new restriction with the existing filter
                    // before publishing any intermediate stacked filters.
                    let mut combined = filter.expressions.clone();
                    combined.extend(landing_predicates.iter().cloned());
                    let Some(combined) = FilterPushdown::normalize_predicates(combined) else {
                        return Ok(RoutedDomain {
                            child,
                            remaining: original_predicates,
                            moved: false,
                        });
                    };
                    filter.expressions = combined;
                    journal.record_operator(nodes, index);
                    nodes[index].operator = LogicalOperator::Filter(filter);
                    for predicate in &landing_predicates {
                        fixed_point.record(landing_relation, &landing_path, predicate);
                    }
                    return Ok(RoutedDomain {
                        child,
                        remaining: Vec::new(),
                        moved: true,
                    });
                }
            }
            // The filter already belongs to the selected path.  Route only
            // the new domain request through it; treating its existing
            // predicates as new input would silently relocate or remove a
            // predicate whose namespace/consumer contract was established by
            // the source candidate.  This is especially important when the
            // child is an Aggregate: group-key predicates may move, while an
            // aggregate-result residual must remain above that boundary.
            let checkpoint = journal.checkpoint(nodes, layouts, fixed_point);
            let routed = push_domain(
                (nodes, layouts),
                filter.child.clone(),
                predicates,
                state,
                journal,
                fixed_point,
                &child_path(path, 0),
            )?;
            if !routed.moved {
                journal.rollback(nodes, layouts, fixed_point, checkpoint);
                return Ok(RoutedDomain {
                    child,
                    remaining: original_predicates,
                    moved: false,
                });
            }
            let mut expressions = filter.expressions.to_vec();
            expressions.extend(routed.remaining);
            let Some(expressions) = FilterPushdown::normalize_predicates(expressions) else {
                journal.rollback(nodes, layouts, fixed_point, checkpoint);
                return Ok(RoutedDomain {
                    child,
                    remaining: original_predicates,
                    moved: false,
                });
            };
            filter.child = routed.child;
            filter.expressions = expressions;
            journal.record_operator(nodes, index);
            nodes[index].operator = LogicalOperator::Filter(filter);
            Ok(RoutedDomain {
                child: NativeChild::Node(index),
                remaining: Vec::new(),
                moved: true,
            })
        }
        LogicalOperator::Filter(_) => Ok(RoutedDomain {
            child,
            remaining: predicates,
            moved: false,
        }),
        LogicalOperator::Projection(mut projection) => {
            let mut remaining = Vec::new();
            let mut moved = false;
            for original_predicate in predicates {
                let child_layout = native_child_layout(&projection.child, layouts)?;
                let Some(predicate) = projection_domain(
                    &original_predicate,
                    &projection,
                    &projection.child,
                    &child_layout,
                    nodes,
                ) else {
                    remaining.push(original_predicate);
                    continue;
                };
                let checkpoint = journal.checkpoint(nodes, layouts, fixed_point);
                let routed = push_domain(
                    (nodes, layouts),
                    projection.child.clone(),
                    vec![predicate],
                    state,
                    journal,
                    fixed_point,
                    &child_path(path, 0),
                )?;
                if routed.moved && routed.remaining.is_empty() {
                    projection.child = routed.child;
                    moved = true;
                } else {
                    journal.rollback(nodes, layouts, fixed_point, checkpoint);
                    remaining.push(original_predicate);
                }
            }
            if moved {
                journal.record_operator(nodes, index);
                nodes[index].operator = LogicalOperator::Projection(projection);
            }
            Ok(RoutedDomain {
                child: NativeChild::Node(index),
                remaining,
                moved,
            })
        }
        LogicalOperator::Aggregate(mut aggregate) => {
            let mut remaining = Vec::new();
            let mut moved = false;
            for original_predicate in predicates {
                let child_layout = native_child_layout(&aggregate.child, layouts)?;
                let Some((predicate, retain)) =
                    aggregate_domain(&original_predicate, &aggregate, &child_layout)
                else {
                    remaining.push(original_predicate);
                    continue;
                };
                let checkpoint = journal.checkpoint(nodes, layouts, fixed_point);
                let routed = push_domain(
                    (nodes, layouts),
                    aggregate.child.clone(),
                    vec![predicate],
                    state,
                    journal,
                    fixed_point,
                    &child_path(path, 0),
                )?;
                if routed.moved && routed.remaining.is_empty() {
                    aggregate.child = routed.child;
                    moved = true;
                    if retain {
                        remaining.push(original_predicate);
                    }
                } else {
                    journal.rollback(nodes, layouts, fixed_point, checkpoint);
                    remaining.push(original_predicate);
                }
            }
            if moved {
                journal.record_operator(nodes, index);
                nodes[index].operator = LogicalOperator::Aggregate(aggregate);
            }
            // A necessary input condition does not discharge the original
            // aggregate-output predicate. Materialize that residual here, in
            // its original namespace, so enclosing projections can accept the
            // completed route without dropping or rebinding the residual.
            if moved && !remaining.is_empty() {
                let child = add_native_filter(
                    nodes,
                    layouts,
                    NativeChild::Node(index),
                    remaining,
                    paro_planner::operator::ProjectionMap::all(),
                    state,
                )?;
                return Ok(RoutedDomain {
                    child,
                    remaining: Vec::new(),
                    moved,
                });
            }
            Ok(RoutedDomain {
                child: NativeChild::Node(index),
                remaining,
                moved,
            })
        }
        LogicalOperator::SetOperation(mut setop)
            if setop.setop_type == SetOpType::Union && setop.setop_all =>
        {
            let left_layout = native_child_layout(&setop.left, layouts)?;
            let right_layout = native_child_layout(&setop.right, layouts)?;
            let mut remaining = Vec::new();
            let mut moved = false;
            for original_predicate in predicates {
                let Some((left_predicate, right_predicate)) =
                    union_domain(&original_predicate, &setop, &left_layout, &right_layout)
                else {
                    remaining.push(original_predicate);
                    continue;
                };
                let checkpoint = journal.checkpoint(nodes, layouts, fixed_point);
                let left = push_domain(
                    (nodes, layouts),
                    setop.left.clone(),
                    vec![left_predicate],
                    state,
                    journal,
                    fixed_point,
                    &child_path(path, 0),
                )?;
                let right = push_domain(
                    (nodes, layouts),
                    setop.right.clone(),
                    vec![right_predicate],
                    state,
                    journal,
                    fixed_point,
                    &child_path(path, 1),
                )?;
                if left.moved
                    && left.remaining.is_empty()
                    && right.moved
                    && right.remaining.is_empty()
                {
                    setop.left = left.child;
                    setop.right = right.child;
                    moved = true;
                } else {
                    journal.rollback(nodes, layouts, fixed_point, checkpoint);
                    remaining.push(original_predicate);
                }
            }
            if moved {
                journal.record_operator(nodes, index);
                nodes[index].operator = LogicalOperator::SetOperation(setop);
            }
            Ok(RoutedDomain {
                child: NativeChild::Node(index),
                remaining,
                moved,
            })
        }
        LogicalOperator::Join(Join::Comparison(mut join))
            if join.join_type == JoinType::Inner
                && join.duplicate_eliminated_columns.is_empty()
                && !join.delim_flipped
                && !crate::rewrite::expr::comparison_join_has_evaluation_fence(&join) =>
        {
            let left_layout = native_child_layout(&join.left, layouts)?;
            let right_layout = native_child_layout(&join.right, layouts)?;
            let mut remaining = Vec::new();
            let mut moved = false;
            for original_predicate in predicates {
                let transfer = domain_transfer::transfer_predicates(
                    &LogicalOperator::Join(Join::Comparison(join.clone())),
                    &[&left_layout, &right_layout],
                    std::slice::from_ref(&original_predicate),
                );
                let (left_side, right_side) =
                    transfer.as_ref().map_or((false, false), |transfer| {
                        (
                            !transfer.child_predicates[0].is_empty()
                                && transfer.child_predicates[1].is_empty(),
                            transfer.child_predicates[0].is_empty()
                                && !transfer.child_predicates[1].is_empty(),
                        )
                    });
                if !left_side && !right_side {
                    remaining.push(original_predicate);
                    continue;
                }
                let checkpoint = journal.checkpoint(nodes, layouts, fixed_point);
                let routed = if left_side {
                    push_domain(
                        (nodes, layouts),
                        join.left.clone(),
                        vec![original_predicate.clone()],
                        state,
                        journal,
                        fixed_point,
                        &child_path(path, 0),
                    )?
                } else {
                    push_domain(
                        (nodes, layouts),
                        join.right.clone(),
                        vec![original_predicate.clone()],
                        state,
                        journal,
                        fixed_point,
                        &child_path(path, 1),
                    )?
                };
                if routed.moved && routed.remaining.is_empty() {
                    if left_side {
                        join.left = routed.child;
                    } else {
                        join.right = routed.child;
                    }
                    moved = true;
                } else {
                    journal.rollback(nodes, layouts, fixed_point, checkpoint);
                    remaining.push(original_predicate);
                }
            }
            if moved {
                journal.record_operator(nodes, index);
                nodes[index].operator = LogicalOperator::Join(Join::Comparison(join));
            }
            Ok(RoutedDomain {
                child: NativeChild::Node(index),
                remaining,
                moved,
            })
        }
        LogicalOperator::Join(Join::Cross(mut join)) => {
            let left_layout = native_child_layout(&join.left, layouts)?;
            let right_layout = native_child_layout(&join.right, layouts)?;
            let mut remaining = Vec::new();
            let mut moved = false;
            for original_predicate in predicates {
                let transfer = domain_transfer::transfer_predicates(
                    &LogicalOperator::Join(Join::Cross(join.clone())),
                    &[&left_layout, &right_layout],
                    std::slice::from_ref(&original_predicate),
                );
                let (left_side, right_side) =
                    transfer.as_ref().map_or((false, false), |transfer| {
                        (
                            !transfer.child_predicates[0].is_empty()
                                && transfer.child_predicates[1].is_empty(),
                            transfer.child_predicates[0].is_empty()
                                && !transfer.child_predicates[1].is_empty(),
                        )
                    });
                if !left_side && !right_side {
                    remaining.push(original_predicate);
                    continue;
                }
                let checkpoint = journal.checkpoint(nodes, layouts, fixed_point);
                let routed = if left_side {
                    push_domain(
                        (nodes, layouts),
                        join.left.clone(),
                        vec![original_predicate.clone()],
                        state,
                        journal,
                        fixed_point,
                        &child_path(path, 0),
                    )?
                } else {
                    push_domain(
                        (nodes, layouts),
                        join.right.clone(),
                        vec![original_predicate.clone()],
                        state,
                        journal,
                        fixed_point,
                        &child_path(path, 1),
                    )?
                };
                if routed.moved && routed.remaining.is_empty() {
                    if left_side {
                        join.left = routed.child;
                    } else {
                        join.right = routed.child;
                    }
                    moved = true;
                } else {
                    journal.rollback(nodes, layouts, fixed_point, checkpoint);
                    remaining.push(original_predicate);
                }
            }
            if moved {
                journal.record_operator(nodes, index);
                nodes[index].operator = LogicalOperator::Join(Join::Cross(join));
            }
            Ok(RoutedDomain {
                child: NativeChild::Node(index),
                remaining,
                moved,
            })
        }
        _ => Ok(RoutedDomain {
            child,
            remaining: predicates,
            moved: false,
        }),
    }
}

/// Apply a predicate through a binding whose transparent descendants are
/// already exact Memo choices. This is intentionally separate from the
/// ordinary one-hop producer: callers must supply a selected path, so the
/// implementation never expands a whole child frontier or manufactures a
/// second optimizer. Every opaque leaf remains the original Memo group hole.
/// Test-facing compatibility wrapper for the closure helper. The production
/// path uses the already-computed layouts from the pattern lowering pass.
#[cfg(test)]
fn transfer_shell_closure(
    shell: NativeShell,
    state: &PlannerTransformState,
) -> Result<Option<NativeShell>> {
    let layouts = shell.layouts()?;
    let mut fixed_point = domain_transfer::DomainFixedPoint::default();
    transfer_shell_closure_with_layouts(shell, layouts, state, &mut fixed_point)
        .map(|result| result.map(|(shell, _layouts)| shell))
}

fn transfer_shell_closure_with_layouts(
    shell: NativeShell,
    mut layouts: Vec<LogicalOutputLayout>,
    state: &PlannerTransformState,
    fixed_point: &mut domain_transfer::DomainFixedPoint,
) -> Result<Option<(NativeShell, Vec<LogicalOutputLayout>)>> {
    let LogicalOperator::Filter(filter) = shell.root_operator().clone() else {
        return Ok(None);
    };
    if filter
        .expressions
        .iter()
        .any(|predicate| predicate.evaluation_properties().is_reorder_fence())
    {
        return Ok(None);
    }
    let Some(predicates) = FilterPushdown::normalize_predicates(filter.expressions.clone()) else {
        return Ok(None);
    };
    let input = filter.child.clone();
    let input_len = native_child_layout(&input, &layouts)?.len();
    let mut nodes = shell.nodes.into_vec();
    let mut journal = NativeRewriteJournal::default();
    let routed = push_domain(
        (&mut nodes, &mut layouts),
        input,
        predicates,
        state,
        &mut journal,
        fixed_point,
        &[],
    )?;
    if !routed.moved {
        return Ok(None);
    }
    let root = if routed.remaining.is_empty() && filter.projection_map.is_identity(input_len) {
        routed.child
    } else {
        add_native_filter(
            &mut nodes,
            &mut layouts,
            routed.child,
            routed.remaining,
            filter.projection_map,
            state,
        )?
    };
    let NativeChild::Node(root) = root else {
        return Err(paro_error::internal(
            "native domain root became a group hole",
        ));
    };
    super::compact_native_shell_with_layouts(
        NativeShell {
            nodes: nodes.into_boxed_slice(),
            root,
        },
        layouts,
    )
    .map(Some)
}

fn attach_native_operator(
    operator: LogicalOperator<BoundReference>,
    links: &[NativeChild],
) -> Result<LogicalOperator<NativeChild>> {
    let mut ordinal = 0;
    let operator = operator.try_map_child_links(&mut |_reference| {
        let child = links
            .get(ordinal)
            .cloned()
            .ok_or_else(|| paro_error::internal("native relation cache input arity changed"))?;
        ordinal += 1;
        Ok::<NativeChild, paro_error::ParoError>(child)
    })?;
    if ordinal != links.len() {
        return Err(paro_error::internal(
            "native relation cache retained an excess child reference",
        ));
    }
    Ok(operator)
}

pub(super) type RefreshedNativeStatistics = (
    NativeShell,
    HashMap<PlanNodeId, SharedColumnStatistics>,
    HashMap<PlanNodeId, ResidentNodeContract>,
);

/// Narrow transport outputs without changing source bindings, cardinality or
/// source evidence. Remap layout-dependent uniqueness below. Late-payload/TopN
/// rewrites are row-domain preserving below their
/// explicit reduction; rebuilding their statistics is a separate operation.
/// Memo holes keep their immutable interface, and Get keeps all predicate
/// inputs. A narrowed Filter map lets extraction omit predicate-only columns
/// from the rowset output after evaluating the pushed predicate.
pub(super) fn prune_output_demands(
    mut shell: NativeShell,
    state: &PlannerTransformState,
    memo: &Memo,
) -> Result<Option<NativeShell>> {
    use super::settlement::demand;
    let original = shell.layouts()?;
    let mut wanted = vec![BTreeSet::new(); shell.nodes.len()];
    wanted[shell.root].extend(original[shell.root].bindings().iter().copied());
    for index in (0..shell.nodes.len()).rev() {
        if !memo.control().checkpoint()? {
            return Ok(None);
        }
        let operator = &shell.nodes[index].operator;
        let (execution, positional) = demand::execution_demand(operator, &wanted[index]);
        let mut ordinal = 0;
        operator.visit_child_links(&mut |child| {
            if let NativeChild::Node(child) = child {
                let all = demand::child_needs_full_row(operator, ordinal, positional);
                wanted[*child].extend(
                    original[*child]
                        .bindings()
                        .iter()
                        .filter(|binding| all || execution.contains(binding))
                        .copied(),
                );
            }
            ordinal += 1;
        });
    }
    let mut completed = Vec::<LogicalOutputLayout>::with_capacity(shell.nodes.len());
    let mut completed_keys =
        Vec::<Vec<paro_planner::plan::UniqueKey>>::with_capacity(shell.nodes.len());
    let mut scan_bindings = demand::ScanBindings::new();
    for (index, node) in shell.nodes.iter_mut().enumerate() {
        if !memo.control().checkpoint()? {
            return Ok(None);
        }
        let mut before = Vec::new();
        let mut after = Vec::new();
        let mut child_keys = Vec::new();
        node.operator.visit_child_links(&mut |child| match child {
            NativeChild::Node(child) => {
                before.push(original[*child].clone());
                after.push(completed[*child].clone());
                child_keys.push(completed_keys[*child].clone());
            }
            NativeChild::MemoGroup { layout, stats, .. }
            | NativeChild::Group { layout, stats, .. } => {
                before.push(layout.clone());
                after.push(layout.clone());
                child_keys.push(stats.unique_keys.clone());
            }
        });
        if !matches!(&node.operator, LogicalOperator::Get(_)) {
            let identities = after
                .iter()
                .map(|layout| {
                    layout
                        .bindings()
                        .iter()
                        .map(|binding| (*binding, *binding))
                        .collect()
                })
                .collect::<Vec<_>>();
            let operator = std::mem::replace(&mut node.operator, LogicalOperator::DummyScan);
            (node.operator, _) = demand::apply_operator(
                operator,
                demand::Inputs {
                    old_carriers: &original[index],
                    before: &before,
                    after: &after,
                    children: &identities,
                },
                &wanted[index],
                &mut scan_bindings,
                &state.bind_context,
            )?;
        }
        let layout = node.operator.output_layout_from_children(&after);
        node.stats.unique_keys = crate::estimate::unique_keys::derive_unique_keys_from_facts(
            &node.operator,
            &layout,
            &after.iter().collect::<Vec<_>>(),
            &child_keys.iter().map(Vec::as_slice).collect::<Vec<_>>(),
        );
        completed_keys.push(node.stats.unique_keys.clone());
        completed.push(layout);
    }
    Ok(Some(shell))
}

pub(super) fn refresh_statistics(
    mut shell: NativeShell,
    state: &mut PlannerTransformState,
    memo: &Memo,
) -> Result<Option<RefreshedNativeStatistics>> {
    let _b3 = crate::diagnostics::work::enter_b3(crate::diagnostics::work::Bucket::Settlement);
    let _refresh = crate::diagnostics::work::native_refresh(shell.nodes.len());
    use super::settlement::demand;
    use paro_planner::operator::bound_reference::{BoundRelationFactValues, BoundRelationFacts};
    let Some(session) = state.session.clone() else {
        return Ok(None);
    };
    // Solve only this closed local shell. Memo holes retain their complete
    // interfaces; narrowing a new Filter does not narrow its input Memo group.
    // The output layout and carrier layout use the same child edges. Derive
    // both in one post-order walk so every native node pays for child-link
    // lookup only once; the two layouts still remain separate contracts.
    let mut original_layouts = Vec::<LogicalOutputLayout>::with_capacity(shell.nodes.len());
    let mut carriers = Vec::<LogicalOutputLayout>::with_capacity(shell.nodes.len());
    for node in &shell.nodes {
        let (layout, carrier) = {
            let mut children = SmallVec::<[&NativeChild; 2]>::new();
            node.operator
                .visit_child_links(&mut |child| children.push(child));
            let mut output_inputs = SmallVec::<[&LogicalOutputLayout; 2]>::new();
            let mut carrier_inputs = SmallVec::<[&LogicalOutputLayout; 2]>::new();
            for child in children {
                match child {
                    NativeChild::Node(index) => {
                        output_inputs.push(original_layouts.get(*index).ok_or_else(|| {
                            paro_error::internal("native shell output layout is incomplete")
                        })?);
                        carrier_inputs.push(carriers.get(*index).ok_or_else(|| {
                            paro_error::internal("native shell carrier layout is incomplete")
                        })?);
                    }
                    NativeChild::MemoGroup { layout, .. } | NativeChild::Group { layout, .. } => {
                        output_inputs.push(layout);
                        carrier_inputs.push(layout);
                    }
                }
            }
            (
                node.operator.output_layout_from_child_refs(&output_inputs),
                node.operator
                    .carrier_layout_from_child_refs(&carrier_inputs),
            )
        };
        original_layouts.push(layout);
        carriers.push(carrier);
    }
    let mut wanted = vec![BTreeSet::new(); shell.nodes.len()];
    wanted[shell.root].extend(original_layouts[shell.root].bindings().iter().copied());
    for index in (0..shell.nodes.len()).rev() {
        if !memo.control().checkpoint()? {
            return Ok(None);
        }
        let node = &shell.nodes[index];
        let (execution, positional) = demand::execution_demand(&node.operator, &wanted[index]);
        let mut ordinal = 0;
        node.operator.visit_child_links(&mut |child| {
            if let NativeChild::Node(child) = child {
                let all = demand::child_needs_full_row(&node.operator, ordinal, positional);
                let layout = if all {
                    &original_layouts[*child]
                } else {
                    &carriers[*child]
                };
                wanted[*child].extend(
                    layout
                        .bindings()
                        .iter()
                        .filter(|binding| all || execution.contains(binding))
                        .copied(),
                );
            }
            ordinal += 1;
        });
    }
    struct Completed {
        id: PlanNodeId,
        stats: NodeStats,
        layout: LogicalOutputLayout,
        maximum: Option<u64>,
        /// Columns in the exact positional order of `layout`.  This is an
        /// immutable view of the completed relation.  Parent edges share the
        /// slice; they must not rebuild a pointer vector merely to feed
        /// positional set-operation statistics.
        ordered_columns: Arc<[Arc<ColumnStatistics>]>,
        /// The local boundary fact is immutable for this refresh snapshot.
        /// Parent edges clone the Arc instead of reconstructing the same
        /// cardinality/types/unique-key witness.
        facts: Arc<BoundRelationFacts>,
        /// Column evidence is also fixed for this completed node. Keeping the
        /// derived fingerprints here avoids hashing every input edge again.
        column_fingerprints: Arc<[Fingerprint]>,
        aliases: demand::BindingMap,
        output_columns: Box<[ColumnId]>,
        names: Arc<[String]>,
    }
    type CompletedEvidence = (
        Arc<BoundRelationFacts>,
        Arc<[Arc<ColumnStatistics>]>,
        Arc<[Fingerprint]>,
    );
    fn completed_evidence(
        layout: &LogicalOutputLayout,
        stats: &NodeStats,
        maximum: Option<u64>,
        columns: &SharedColumnStatistics,
    ) -> Result<CompletedEvidence> {
        let facts = Arc::new(BoundRelationFacts::new(
            BoundRelationFactValues {
                cardinality: stats.estimated_cardinality,
                maximum_cardinality: maximum,
                unique_keys: stats.unique_keys.clone(),
                ..BoundRelationFactValues::default()
            },
            layout.types().to_vec(),
        ));
        let ordered_columns = layout
            .bindings()
            .iter()
            .map(|binding| {
                columns
                    .get(binding)
                    .cloned()
                    .ok_or_else(|| paro_error::internal("native relation output column missing"))
            })
            .collect::<Result<Arc<[_]>>>()?;
        let column_fingerprints = ordered_columns
            .iter()
            .map(|column| settlement::SettlementCache::native_column_fingerprint(column.as_ref()))
            .collect::<Result<Arc<[_]>>>()?;
        Ok((facts, ordered_columns, column_fingerprints))
    }
    let mut completed = Vec::<Completed>::new();
    let mut scopes = HashMap::new();
    let mut resident_nodes = HashMap::new();
    let mut scan_bindings = demand::ScanBindings::new();
    // Statistics propagation for native nodes uses this session context as a
    // reusable property workspace.  The old path allocated a fresh context
    // for every cache miss solely because the operator had been rebuilt as an
    // OwnedLogicalPlan.  Its column map is replaced with the exact immutable
    // input view for each node below; no facts are shared across unrelated
    // native relations by accident.
    let mut context =
        crate::context::OptimizationContext::new(session.clone(), state.bind_context.clone());
    context.cost_model = state.cost_model.clone();
    for (index, node) in shell.nodes.iter_mut().enumerate() {
        if !memo.control().checkpoint()? {
            return Ok(None);
        }
        crate::diagnostics::work::native_refresh_node();
        let mut layouts = Vec::new();
        let mut maximums = Vec::new();
        let mut columns = HashMap::new();
        let mut positional_columns = SmallVec::<[Arc<[Arc<ColumnStatistics>]>; 2]>::new();
        let mut input_aliases = Vec::new();
        let mut before = Vec::new();
        let mut input_facts = Vec::new();
        let mut native_inputs = Vec::new();
        node.operator.visit_child_links(&mut |child| match child {
            NativeChild::Node(index) => before.push(original_layouts[*index].clone()),
            NativeChild::MemoGroup { layout, .. } | NativeChild::Group { layout, .. } => {
                before.push(layout.clone())
            }
        });
        let mut links = Vec::new();
        let operator = node.operator.clone().try_map_child_links(&mut |child| {
            let (_stats, layout, maximum, reference) = match &child {
                NativeChild::MemoGroup {
                    stats,
                    layout,
                    reference,
                    ..
                } => {
                    input_aliases.push(
                        layout
                            .bindings()
                            .iter()
                            .map(|binding| (*binding, *binding))
                            .collect(),
                    );
                    let input_columns: Arc<[Arc<ColumnStatistics>]> =
                        reference.facts.column_statistics().to_vec().into();
                    input_facts.push(ResidentInputFact::Memo(reference.facts.clone()));
                    native_inputs.push(NativeRelationInput {
                        facts: reference.facts.clone(),
                        layout: layout.clone(),
                        stats: stats.clone(),
                        maximum: reference.facts.maximum_cardinality,
                        // Memo boundary facts already carry the complete
                        // immutable column values/provenance used to derive
                        // these views. Re-encoding the derived columns here
                        // would price the same fact twice. Local child nodes
                        // below are different: their synthetic boundary facts
                        // intentionally contain only row/uniqueness facts, so
                        // their derived column views need the explicit
                        // content fingerprint.
                        column_fingerprints: Arc::from([]),
                    });
                    for (binding, column) in layout.bindings().iter().zip(input_columns.iter()) {
                        columns.insert(*binding, column.clone());
                    }
                    positional_columns.push(input_columns);
                    (
                        stats.clone(),
                        layout.clone(),
                        reference.facts.maximum_cardinality,
                        reference.clone(),
                    )
                }
                NativeChild::Node(index) => {
                    let input = completed
                        .get(*index)
                        .ok_or_else(|| paro_error::internal("native statistics input not ready"))?;
                    input_aliases.push(input.aliases.clone());
                    for (binding, column) in input
                        .layout
                        .bindings()
                        .iter()
                        .copied()
                        .zip(input.ordered_columns.iter())
                    {
                        columns.insert(binding, Arc::clone(column));
                    }
                    positional_columns.push(Arc::clone(&input.ordered_columns));
                    state
                        .settlement_cache
                        .native_relation_ordered_column_view_reuses = state
                        .settlement_cache
                        .native_relation_ordered_column_view_reuses
                        .saturating_add(1);
                    native_inputs.push(NativeRelationInput {
                        facts: Arc::clone(&input.facts),
                        layout: input.layout.clone(),
                        stats: input.stats.clone(),
                        maximum: input.maximum,
                        column_fingerprints: Arc::clone(&input.column_fingerprints),
                    });
                    input_facts.push(ResidentInputFact::Local {
                        node_id: input.id,
                        stats: input.stats.clone(),
                        columns: input.output_columns.clone(),
                        layout: input.layout.clone(),
                    });
                    let reference = BoundReference::new(
                        BoundReferenceId::input_ordinal(links.len()),
                        input.layout.bindings().to_vec(),
                        input.layout.types().to_vec(),
                    )
                    .with_facts(Arc::clone(&input.facts))?;
                    (
                        input.stats.clone(),
                        input.layout.clone(),
                        input.maximum,
                        reference,
                    )
                }
                NativeChild::Group { .. } => {
                    return Err(paro_error::internal(
                        "native domain received an owned group transport",
                    ));
                }
            };
            layouts.push(layout);
            maximums.push(maximum);
            links.push(child);
            // This is already a native operator shell. Keep the immutable
            // fact-backed reference in its child slot instead of wrapping it
            // in an OwnedLogicalPlan only to detach it again below.
            Ok::<_, paro_error::ParoError>(reference)
        })?;
        let (mut local, aliases) = demand::apply_operator(
            operator,
            demand::Inputs {
                old_carriers: &carriers[index],
                before: &before,
                after: &layouts,
                children: &input_aliases,
            },
            &wanted[index],
            &mut scan_bindings,
            &state.bind_context,
        )?;
        // Establish the structural identity before running the expensive
        // relation-property fold. This is a construction-time admission key,
        // not a fact result: changed input facts must still miss and be
        // re-derived.
        let statistics_partition =
            crate::diagnostics::work::enter_b3(crate::diagnostics::work::Bucket::Statistics);
        crate::rewrite::expr::scalar_normalizer().visit_operator_expressions(&mut local);
        if local.op_type() != node.operator.op_type() {
            drop(statistics_partition);
            return Ok(None);
        }
        if matches!(&local, LogicalOperator::Filter(filter)
            if filter.expressions.is_empty() && filter.projection_map.is_identity(layouts[0].len()))
        {
            // An input alias is a different settlement result, not an extra
            // zero-predicate physical operator licensed by this producer.
            drop(statistics_partition);
            return Ok(None);
        }
        let child_names = links
            .iter()
            .map(|child| match child {
                NativeChild::Node(child) => completed
                    .get(*child)
                    .map(|completed| completed.names.as_ref())
                    .ok_or_else(|| paro_error::internal("native relation child name is missing")),
                NativeChild::MemoGroup { names, .. } | NativeChild::Group { names, .. } => {
                    Ok(names.as_ref())
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let child_columns = {
            let mut identity = PlannerResidentIdentity {
                columns: &mut state.columns,
                scalars: &mut state.scalars,
                binding_ids: &mut state.binding_ids,
            };
            links
                .iter()
                .map(|child| match child {
                    NativeChild::Node(child) => completed
                        .get(*child)
                        .map(|completed| completed.output_columns.clone())
                        .ok_or_else(|| {
                            paro_error::internal("native relation child columns are missing")
                        }),
                    NativeChild::MemoGroup { layout, names, .. } => intern_columns_into(
                        &mut identity,
                        layout,
                        ColumnInternOrigin::Derived,
                        Some(names.as_ref()),
                    ),
                    NativeChild::Group { .. } => Err(paro_error::internal(
                        "native resident contract crossed an owned group transport",
                    )),
                })
                .collect::<Result<Vec<_>>>()?
        };
        let child_column_refs = child_columns
            .iter()
            .map(|columns| columns.as_ref())
            .collect::<Vec<_>>();
        let output_names: Arc<[String]> = local
            .output_names_from_child_refs(child_names.as_slice())
            .into();
        let layout_before =
            local.output_layout_from_child_refs(&layouts.iter().collect::<Vec<_>>());
        let (pre_output_columns, pre_scalar_roots, pre_operator_fingerprint, pre_operator_encoding) = {
            let mut identity = PlannerResidentIdentity {
                columns: &mut state.columns,
                scalars: &mut state.scalars,
                binding_ids: &mut state.binding_ids,
            };
            let output_columns = intern_columns_into(
                &mut identity,
                &layout_before,
                ColumnInternOrigin::Derived,
                Some(output_names.as_ref()),
            )?;
            let scalar_roots = if super::settlement::operator_has_no_scalar_payload(&local) {
                Box::new([])
            } else {
                intern_operator_scalars(
                    &local,
                    &output_columns,
                    &child_column_refs,
                    identity.binding_ids,
                    identity.columns,
                    identity.scalars,
                )?
            };
            let (fingerprint, encoding) =
                query_operator_identity(&local, &scalar_roots, identity.scalars)?;
            (output_columns, scalar_roots, fingerprint, encoding)
        };
        if let Some(cached) = state.settlement_cache.native_lookup(
            &pre_operator_encoding,
            &layout_before,
            &native_inputs,
        ) {
            state.settlement_cache.native_relation_owned_assembly_skips = state
                .settlement_cache
                .native_relation_owned_assembly_skips
                .saturating_add(1);
            state
                .settlement_cache
                .native_relation_cached_evidence_reuses = state
                .settlement_cache
                .native_relation_cached_evidence_reuses
                .saturating_add(1);
            node.operator = attach_native_operator(cached.operator.clone(), &links)?;
            node.stats = cached.stats.clone();
            let cached_columns = cached.columns.clone();
            scopes.insert(node.id, cached_columns.clone());
            if resident_nodes
                .insert(
                    node.id,
                    ResidentNodeContract {
                        operator_fingerprint: cached.operator_fingerprint,
                        operator_encoding: cached.operator_encoding.clone(),
                        scalar_roots: cached.scalar_roots.clone(),
                        output_columns: cached.output_columns.clone(),
                        output_layout: cached.layout.clone(),
                        input_facts: ResidentInputFacts::Native(input_facts.into_boxed_slice()),
                    },
                )
                .is_some()
            {
                return Err(paro_error::internal(
                    "native resident contract was assigned twice",
                ));
            }
            drop(statistics_partition);
            completed.push(Completed {
                id: node.id,
                stats: cached.stats.clone(),
                layout: cached.layout.clone(),
                maximum: cached.maximum,
                ordered_columns: Arc::clone(&cached.ordered_columns),
                facts: Arc::clone(&cached.facts),
                column_fingerprints: Arc::clone(&cached.column_fingerprints),
                aliases,
                output_columns: cached.output_columns.clone(),
                names: output_names,
            });
            continue;
        }
        // Only a cache miss needs direct propagation and the expensive
        // statistics fold. A hit above can attach the immutable relation
        // operator directly to the current NativeChild links.
        state.settlement_cache.native_relation_fact_evaluations = state
            .settlement_cache
            .native_relation_fact_evaluations
            .saturating_add(1);
        let mut propagator = StatisticsPropagator::with_statistics_map(columns);
        let operator = propagator.propagate_native_operator(session.as_ref(), local);
        context.column_stats = Arc::new(propagator.take_statistics_map());
        let (stats, operator, layout, maximum) = StatisticsGathering::new().gather_native_local(
            operator,
            NodeStats::default(),
            &layouts,
            &maximums,
            context.column_stats.clone(),
            &mut context,
        );
        if matches!(&operator, LogicalOperator::SetOperation(_)) {
            let [left, right] = positional_columns.as_slice() else {
                return Err(paro_error::internal(
                    "native set-operation statistics arity changed",
                ));
            };
            crate::estimate::gathering::merge_set_operation_column_statistics(
                &layout,
                left.as_ref(),
                right.as_ref(),
                &mut context,
            );
        }
        // The positional views borrow completed native nodes.  Release those
        // borrows before appending the current node to the completed state.
        drop(positional_columns);
        drop(statistics_partition);
        let cached_operator = operator;
        node.stats = stats.clone();
        let output_columns_stats = context.column_stats.clone();
        scopes.insert(node.id, output_columns_stats.clone());
        let output_names: Arc<[String]> = cached_operator
            .output_names_from_child_refs(child_names.as_slice())
            .into();
        let (output_columns, scalar_roots, operator_fingerprint, operator_encoding) = if layout
            == layout_before
            && query_operator_identity(&cached_operator, &pre_scalar_roots, &state.scalars)
                .is_ok_and(|(_, encoding)| encoding == pre_operator_encoding)
        {
            (
                pre_output_columns,
                pre_scalar_roots,
                pre_operator_fingerprint,
                pre_operator_encoding.clone(),
            )
        } else {
            let mut identity = PlannerResidentIdentity {
                columns: &mut state.columns,
                scalars: &mut state.scalars,
                binding_ids: &mut state.binding_ids,
            };
            let output_columns = intern_columns_into(
                &mut identity,
                &layout,
                ColumnInternOrigin::Derived,
                Some(output_names.as_ref()),
            )?;
            let scalar_roots =
                if super::settlement::operator_has_no_scalar_payload(&cached_operator) {
                    Box::new([])
                } else {
                    intern_operator_scalars(
                        &cached_operator,
                        &output_columns,
                        &child_column_refs,
                        identity.binding_ids,
                        identity.columns,
                        identity.scalars,
                    )?
                };
            let (operator_fingerprint, operator_encoding) =
                query_operator_identity(&cached_operator, &scalar_roots, identity.scalars)?;
            (
                output_columns,
                scalar_roots,
                operator_fingerprint,
                operator_encoding,
            )
        };
        let columns = output_columns_stats;
        let (facts, ordered_columns, column_fingerprints) =
            completed_evidence(&layout, &stats, maximum, &columns)?;
        let cached = state.settlement_cache.native_insert(
            pre_operator_encoding.clone(),
            NativeRelationEntry {
                operator: cached_operator,
                operator_fingerprint,
                operator_encoding: operator_encoding.clone(),
                scalar_roots: scalar_roots.clone(),
                output_columns: output_columns.clone(),
                stats: stats.clone(),
                layout: layout.clone(),
                maximum,
                columns: columns.clone(),
                facts,
                ordered_columns,
                column_fingerprints,
                inputs: native_inputs.into_boxed_slice(),
                id: 0,
            },
        );
        // The cache is the canonical publisher.  If an equivalent relation
        // was inserted earlier in this transaction, use its immutable
        // identities and facts instead of publishing a second locally
        // assembled version.
        node.operator = attach_native_operator(cached.operator.clone(), &links)?;
        node.stats = cached.stats.clone();
        let columns = cached.columns.clone();
        scopes.insert(node.id, columns.clone());
        if resident_nodes
            .insert(
                node.id,
                ResidentNodeContract {
                    operator_fingerprint: cached.operator_fingerprint,
                    operator_encoding: cached.operator_encoding.clone(),
                    scalar_roots: cached.scalar_roots.clone(),
                    output_columns: cached.output_columns.clone(),
                    output_layout: cached.layout.clone(),
                    input_facts: ResidentInputFacts::Native(input_facts.into_boxed_slice()),
                },
            )
            .is_some()
        {
            return Err(paro_error::internal(
                "native resident contract was assigned twice",
            ));
        }
        completed.push(Completed {
            id: node.id,
            stats: cached.stats.clone(),
            layout: cached.layout.clone(),
            maximum: cached.maximum,
            ordered_columns: Arc::clone(&cached.ordered_columns),
            facts: Arc::clone(&cached.facts),
            column_fingerprints: Arc::clone(&cached.column_fingerprints),
            aliases,
            output_columns: cached.output_columns.clone(),
            names: output_names,
        });
    }
    Ok(Some((shell, scopes, resident_nodes)))
}

#[cfg(test)]
#[path = "native_domain_tests.rs"]
mod tests;
