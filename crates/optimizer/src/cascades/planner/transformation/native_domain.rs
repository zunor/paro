// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! One exact domain-transfer binding, without rebuilding its Memo operands.
//!
//! This is a complete producer for the admitted local binding, not proof that
//! PredicateTransfer's other bindings or the surrounding logical domain closed.

use super::*;
use crate::cascades::planner::domain_transfer;
use crate::expression::traversal::visit_expression;
use paro_planner::operator::{BoundReference, BoundReferenceId, LogicalOutputLayout};
use paro_planner::plan::PlanNodeId;
use super::staging::{
    intern_columns_into, ColumnInternOrigin, ResidentInputFact, ResidentInputFacts,
    ResidentNodeContract,
};

fn local_domain(predicate: &Expression) -> bool {
    domain_transfer::is_local_domain(predicate)
}

fn hole_layout(child: &NativeChild) -> Option<&LogicalOutputLayout> {
    match child {
        NativeChild::MemoGroup { layout, .. } => Some(layout),
        _ => None,
    }
}

/// Admit before constructing output. Inputs remain exact, immutable group
/// references; unsupported shapes retain the semantic producer unchanged.
pub(super) fn try_transfer(
    binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
    facts: &boundary::BoundarySnapshot,
    binding_fact_value: Fingerprint,
) -> Result<Option<NativeShell>> {
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
    let mut fixed_point = domain_transfer::DomainFixedPoint::new(
        domain_transfer::DomainFactContext {
            relation: root_group,
            occurrence: *expression,
            context: metadata.input_context,
            logical_facts: root.logical_fact_fingerprint(),
            statistics: root.statistics_snapshot_fingerprint(),
            binding_facts: binding_fact_value,
        },
    );
    let nested_path = inputs
        .iter()
        .any(|input| matches!(input, PatternOperand::Expression { .. }));
    let Some(mut shell) = (if nested_path {
        transfer_shell_closure_with_layouts(shell, layouts, state, &mut fixed_point)?
    } else {
        transfer_shell_with_layouts(shell, layouts, state)?
    }) else {
        return Ok(None);
    };
    // Canonical templates deliberately erase occurrence output demand. Do
    // not suppress the semantic producer until this result owns that exact
    // ordered output again, including residual Filter projection maps.
    let layouts = shell.layouts()?;
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
    let output = shell.root_layout()?;
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
    Ok(Some(shell))
}

/// Test-facing compatibility wrapper. Production callers should pass the
/// layouts returned by `NativeShell::from_pattern_with_layouts` so the exact
/// same shell is not walked twice.
pub(super) fn transfer_shell(
    shell: NativeShell,
    state: &PlannerTransformState,
) -> Result<Option<NativeShell>> {
    let layouts = shell.layouts()?;
    transfer_shell_with_layouts(shell, layouts, state)
}

fn transfer_shell_with_layouts(
    shell: NativeShell,
    layouts: Vec<LogicalOutputLayout>,
    state: &PlannerTransformState,
) -> Result<Option<NativeShell>> {
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
        nodes.push(NativeNode {
            id: state.bind_context.next_plan_id(),
            stats: NodeStats::default(),
            operator: LogicalOperator::Filter(Filter {
                child,
                expressions: predicates,
                projection_map: paro_planner::operator::ProjectionMap::all(),
            }),
            source_proofs: Box::new([]),
        });
        children.push(NativeChild::Node(next));
    }
    let mut children = children.into_iter();
    operator = operator.try_map_child_links(&mut |_| {
        children
            .next()
            .ok_or_else(|| paro_error::internal("native domain lost a child"))
    })?;
    let next = nodes.len();
    nodes.push(NativeNode {
        id: state.bind_context.next_plan_id(),
        stats: NodeStats::default(),
        operator,
        source_proofs: nodes[input].source_proofs.clone(),
    });
    let root = if remaining.is_empty() && filter.projection_map.is_identity(layouts[input].len()) {
        next
    } else {
        let root = nodes.len();
        nodes.push(NativeNode {
            id: state.bind_context.next_plan_id(),
            stats: NodeStats::default(),
            operator: LogicalOperator::Filter(Filter {
                expressions: remaining,
                child: NativeChild::Node(next),
                projection_map: filter.projection_map,
            }),
            source_proofs: Box::new([]),
        });
        root
    };
    compact_native_shell(NativeShell {
        nodes: nodes.into_boxed_slice(),
        root,
    })
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
    nodes: &mut Vec<NativeNode>,
    layouts: &mut Vec<LogicalOutputLayout>,
    child: NativeChild,
    predicates: Vec<Expression>,
    state: &PlannerTransformState,
    journal: &mut NativeRewriteJournal,
    fixed_point: &mut domain_transfer::DomainFixedPoint,
    path: &[usize],
) -> Result<RoutedDomain> {
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
                if transfer.as_ref().is_some_and(|transfer| {
                    !transfer.unsupported && transfer.remaining.is_empty()
                }) {
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
                nodes,
                layouts,
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
                    nodes,
                    layouts,
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
                    nodes,
                    layouts,
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
                    nodes,
                    layouts,
                    setop.left.clone(),
                    vec![left_predicate],
                    state,
                    journal,
                    fixed_point,
                    &child_path(path, 0),
                )?;
                let right = push_domain(
                    nodes,
                    layouts,
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
                && !crate::expression::comparison_join_has_evaluation_fence(&join) =>
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
                        nodes,
                        layouts,
                        join.left.clone(),
                        vec![original_predicate.clone()],
                        state,
                        journal,
                        fixed_point,
                        &child_path(path, 0),
                    )?
                } else {
                    push_domain(
                        nodes,
                        layouts,
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
                        nodes,
                        layouts,
                        join.left.clone(),
                        vec![original_predicate.clone()],
                        state,
                        journal,
                        fixed_point,
                        &child_path(path, 0),
                    )?
                } else {
                    push_domain(
                        nodes,
                        layouts,
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
fn transfer_shell_closure(
    shell: NativeShell,
    state: &PlannerTransformState,
) -> Result<Option<NativeShell>> {
    let layouts = shell.layouts()?;
    let mut fixed_point = domain_transfer::DomainFixedPoint::default();
    transfer_shell_closure_with_layouts(shell, layouts, state, &mut fixed_point)
}

fn transfer_shell_closure_with_layouts(
    shell: NativeShell,
    mut layouts: Vec<LogicalOutputLayout>,
    state: &PlannerTransformState,
    fixed_point: &mut domain_transfer::DomainFixedPoint,
) -> Result<Option<NativeShell>> {
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
        &mut nodes,
        &mut layouts,
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
    compact_native_shell(NativeShell {
        nodes: nodes.into_boxed_slice(),
        root,
    })
    .map(Some)
}

/// Statistics use the existing one-operator propagation/gathering contracts.
/// The temporary adapter contains only this shell and immutable input facts,
/// never an owned descendant tree, and is never inserted as a Memo alternative.
pub(super) fn refresh_statistics(
    mut shell: NativeShell,
    state: &mut PlannerTransformState,
    memo: &Memo,
) -> Result<
    Option<(
        NativeShell,
        HashMap<PlanNodeId, SharedColumnStatistics>,
        HashMap<PlanNodeId, ResidentNodeContract>,
    )>,
> {
    let _b3 = crate::work_partition::enter_b3(crate::work_partition::Bucket::Settlement);
    let _refresh = crate::work_partition::native_refresh(shell.nodes.len());
    use super::settlement::demand;
    use paro_planner::operator::bound_reference::{BoundRelationFactValues, BoundRelationFacts};
    use paro_planner::plan::arena::LogicalPlanNode;
    let Some(session) = state.session.clone() else {
        return Ok(None);
    };
    // Solve only this closed local shell. Memo holes retain their complete
    // interfaces; narrowing a new Filter does not narrow its input Memo group.
    let original_layouts = shell.layouts()?;
    let mut carriers = Vec::<LogicalOutputLayout>::with_capacity(shell.nodes.len());
    for node in &shell.nodes {
        let mut inputs = Vec::new();
        node.operator.visit_child_links(&mut |child| match child {
            NativeChild::Node(index) => inputs.push(&carriers[*index]),
            NativeChild::MemoGroup { layout, .. } | NativeChild::Group { layout, .. } => {
                inputs.push(layout)
            }
        });
        let carrier = node.operator.carrier_layout_from_child_refs(&inputs);
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
        columns: SharedColumnStatistics,
        aliases: demand::BindingMap,
        output_columns: Box<[ColumnId]>,
        names: Arc<[String]>,
    }
    let mut completed = Vec::<Completed>::new();
    let mut scopes = HashMap::new();
    let mut resident_nodes = HashMap::new();
    let mut scan_bindings = demand::ScanBindings::new();
    for (index, node) in shell.nodes.iter_mut().enumerate() {
        if !memo.control().checkpoint()? {
            return Ok(None);
        }
        crate::work_partition::native_refresh_node();
        let mut layouts = Vec::new();
        let mut maximums = Vec::new();
        let mut columns = HashMap::new();
        let mut positional_columns = Vec::new();
        let mut input_aliases = Vec::new();
        let mut before = Vec::new();
        let mut input_facts = Vec::new();
        node.operator.visit_child_links(&mut |child| match child {
            NativeChild::Node(index) => before.push(original_layouts[*index].clone()),
            NativeChild::MemoGroup { layout, .. } | NativeChild::Group { layout, .. } => {
                before.push(layout.clone())
            }
        });
        let mut links = Vec::new();
        let mut operator = node.operator.clone().try_map_child_links(&mut |child| {
            let (stats, layout, maximum, reference) = match &child {
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
                    let input_columns = reference.column_statistics();
                    input_facts.push(ResidentInputFact::Memo(reference.facts.clone()));
                    for (binding, column) in layout.bindings().iter().zip(&input_columns) {
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
                    let mut input_columns = Vec::with_capacity(input.layout.len());
                    for binding in input.layout.bindings() {
                        let column = input.columns.get(binding).ok_or_else(|| {
                            paro_error::internal("native statistics output column missing")
                        })?;
                        columns.insert(*binding, column.clone());
                        input_columns.push(column.clone());
                    }
                    positional_columns.push(input_columns);
                    let facts = Arc::new(BoundRelationFacts::new(
                        BoundRelationFactValues {
                            cardinality: input.stats.estimated_cardinality,
                            maximum_cardinality: input.maximum,
                            unique_keys: input.stats.unique_keys.clone(),
                            ..BoundRelationFactValues::default()
                        },
                        input.layout.types().to_vec(),
                    ));
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
                    .with_facts(facts)?;
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
                    ))
                }
            };
            layouts.push(layout);
            maximums.push(maximum);
            links.push(child);
            Ok::<_, paro_error::ParoError>(Box::new(OwnedLogicalPlan {
                id: state.bind_context.next_plan_id(),
                stats,
                operator: LogicalOperator::BoundReference(reference),
            }))
        })?;
        let (local, inputs) = LogicalPlanNode::detach(OwnedLogicalPlan {
            id: node.id,
            stats: NodeStats::default(),
            operator,
        });
        let (local, aliases) = demand::apply(
            local,
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
        operator = local.assemble(inputs)?.into_operator();
        // Match settlement's scalar contract before looking up column domains.
        let statistics_partition = crate::work_partition::enter_b3(crate::work_partition::Bucket::Statistics);
        crate::expression::scalar_normalizer().visit_operator_expressions(&mut operator);
        let mut context =
            crate::context::OptimizationContext::new(session.clone(), state.bind_context.clone());
        context.cost_model = state.cost_model.clone();
        let input = Arc::new(columns);
        let mut propagator = StatisticsPropagator::with_statistics_map(input.as_ref().clone());
        let operator = propagator.propagate_operator(session.as_ref(), operator);
        if operator.op_type() != node.operator.op_type() {
            return Ok(None);
        }
        if matches!(&operator, LogicalOperator::Filter(filter)
            if filter.expressions.is_empty() && filter.projection_map.is_identity(layouts[0].len()))
        {
            // An input alias is a different settlement result, not an extra
            // zero-predicate physical operator licensed by this producer.
            return Ok(None);
        }
        context.column_stats = Arc::new(propagator.take_statistics_map());
        let plan = OwnedLogicalPlan {
            id: node.id,
            stats: NodeStats::default(),
            operator,
        };
        let (plan, layout, maximum) =
            StatisticsGathering::new().gather_local(plan, &layouts, &maximums, input, &mut context);
        if matches!(&plan.operator, LogicalOperator::SetOperation(_)) {
            let [left, right] = positional_columns.as_slice() else {
                return Err(paro_error::internal(
                    "native set-operation statistics arity changed",
                ));
            };
            crate::statistics::gathering::merge_set_operation_column_statistics(
                &layout,
                left,
                right,
                &mut context,
            );
        }
        drop(statistics_partition);
        let (_, stats, operator) = plan.into_parts();
        let mut links = links.into_iter();
        node.operator = operator.try_map_child_links(&mut |_| {
            links
                .next()
                .ok_or_else(|| paro_error::internal("native statistics changed child arity"))
        })?;
        node.stats = stats.clone();
        scopes.insert(node.id, context.column_stats.clone());
        let mut native_children = Vec::new();
        node.operator
            .visit_child_links(&mut |child| native_children.push(child.clone()));
        let child_names = native_children
            .iter()
            .map(|child| match child {
                NativeChild::Node(child) => completed
                    .get(*child)
                    .map(|completed| completed.names.as_ref())
                    .ok_or_else(|| paro_error::internal("native resident child name is missing")),
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
            native_children
                .iter()
                .map(|child| match child {
                    NativeChild::Node(child) => completed
                        .get(*child)
                        .map(|completed| completed.output_columns.clone())
                        .ok_or_else(|| {
                            paro_error::internal("native resident child columns are missing")
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
        let semantic_operator = node
            .operator
            .clone()
            .try_map_child_links(&mut |_| Ok::<_, std::convert::Infallible>(()))
            .expect("native resident semantic operator cannot fail");
        let child_name_refs = child_names.as_slice();
        let output_names: Arc<[String]> = semantic_operator
            .output_names_from_child_refs(child_name_refs)
            .into();
        let (output_columns, scalar_roots, operator_fingerprint, operator_encoding) = {
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
            let scalar_roots = if super::settlement::operator_has_no_scalar_payload(
                &semantic_operator,
            ) {
                Box::new([])
            } else {
                intern_operator_scalars(
                    &semantic_operator,
                    &output_columns,
                    &child_column_refs,
                    identity.binding_ids,
                    identity.columns,
                    identity.scalars,
                )?
            };
            let (operator_fingerprint, operator_encoding) =
                query_operator_identity(&semantic_operator, &scalar_roots, identity.scalars)?;
            (
                output_columns,
                scalar_roots,
                operator_fingerprint,
                operator_encoding,
            )
        };
        if resident_nodes
            .insert(
                node.id,
                ResidentNodeContract {
                    operator_fingerprint,
                    operator_encoding,
                    scalar_roots,
                    output_columns: output_columns.clone(),
                    output_layout: layout.clone(),
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
            stats,
            layout,
            maximum,
            columns: context.column_stats,
            aliases,
            output_columns,
            names: output_names,
        });
    }
    Ok(Some((shell, scopes, resident_nodes)))
}

#[cfg(test)]
#[path = "native_domain_tests.rs"]
mod tests;
