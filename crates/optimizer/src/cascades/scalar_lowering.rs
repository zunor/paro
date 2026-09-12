// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Canonical lowering from bound scalar expressions to optimizer-owned IR.
//!
//! Relational Memo expressions keep `ScalarExprId`s. The native DAG retains
//! executable call descriptors, while explicit binding domains preserve the
//! meaning of positional operands at the import/export boundary.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_external::routine::identity::{
    BuiltinIntrinsicId, BuiltinSemanticTag, RoutineCallIdentity,
};
use paro_planner::expression::{
    ComparisonType, ConjunctionType, Expression, ExpressionIdentity, ExpressionIterator,
};
use paro_planner::operator::join::JoinComparisonType;
use paro_planner::operator::{ColumnBinding, LogicalOperator};

use super::column::{ColumnCatalog, ColumnOrigin, ColumnVisibility};
use super::ids::{ColumnId, Fingerprint, ScalarExprId, StableFingerprintBuilder};
use super::scalar::{
    ComparisonOp, ScalarAggregate, ScalarArena, ScalarCast, ScalarFunction, ScalarKind,
    ScalarLiteral, ScalarLocalProperties, ScalarSpec, ScalarWindow, Volatility,
};

mod bindings;
mod fields;
pub(super) mod imports;
mod operator_export;
pub(crate) use bindings::BindingCatalog;
pub(crate) use operator_export::export_operator_scalars;

pub(crate) fn intern_operator_scalars<Child>(
    operator: &LogicalOperator<Child>,
    output_columns: &[ColumnId],
    child_columns: &[impl AsRef<[ColumnId]>],
    binding_ids: &mut BindingCatalog,
    columns: &mut ColumnCatalog,
    arena: &mut ScalarArena,
) -> Result<Box<[ScalarExprId]>> {
    use fields::{OperandRef, ReferenceScope};
    let default_references: std::borrow::Cow<'_, [ColumnId]> = match operator {
        LogicalOperator::SearchScan(search) => {
            get_reference_columns(&search.get, binding_ids, columns)?.into()
        }
        LogicalOperator::FullTextFilterScan(search) => {
            get_reference_columns(&search.get, binding_ids, columns)?.into()
        }
        _ if child_columns.is_empty() => output_columns.into(),
        _ if child_columns.len() == 1 => child_columns[0].as_ref().into(),
        _ => child_columns
            .iter()
            .flat_map(|columns| columns.as_ref().iter().copied())
            .collect::<Vec<_>>()
            .into(),
    };
    let left_columns = child_columns.first().map(AsRef::as_ref).unwrap_or(&[]);
    let right_columns = child_columns.get(1).map(AsRef::as_ref).unwrap_or(&[]);
    let mut reducer_columns = None;
    let mut roots = Vec::new();
    fields::visit_fields(operator, |operand| {
        let root = match operand {
            OperandRef::Expression(expression, scope) => {
                let references = match scope {
                    ReferenceScope::Input => default_references.as_ref(),
                    ReferenceScope::Output => output_columns,
                    ReferenceScope::Left => left_columns,
                    ReferenceScope::None => &[],
                    ReferenceScope::Reducers => {
                        if reducer_columns.is_none() {
                            let LogicalOperator::Aggregate(aggregate) = operator else {
                                return Err(paro_error::internal(
                                    "reducer scope has no aggregate owner",
                                ));
                            };
                            let reduction = aggregate.post_reduction.as_ref().ok_or_else(|| {
                                paro_error::internal("reducer scope has no reduction owner")
                            })?;
                            reducer_columns = Some(
                                reduction
                                    .reducers
                                    .iter()
                                    .enumerate()
                                    .map(|(ordinal, reducer)| {
                                        binding_ids.intern_reducer_output(
                                            reduction.reduction_index,
                                            ordinal,
                                            &reducer.return_type(),
                                            columns,
                                        )
                                    })
                                    .collect::<Result<Vec<_>>>()?,
                            );
                        }
                        reducer_columns
                            .as_deref()
                            .ok_or_else(|| paro_error::internal("reducer scope lost its columns"))?
                    }
                };
                intern_expression(expression, references, binding_ids, columns, arena)?
            }
            OperandRef::Comparison(condition) => {
                let left =
                    intern_expression(&condition.left, left_columns, binding_ids, columns, arena)?;
                let right = intern_expression(
                    &condition.right,
                    right_columns,
                    binding_ids,
                    columns,
                    arena,
                )?;
                intern_comparison(join_comparison(condition.comparison), left, right, arena)?
            }
            OperandRef::Window(expression) => {
                let mut children = Vec::new();
                ExpressionIterator::enumerate_window_children(expression, |child| {
                    children.push(child)
                });
                let children = children
                    .into_iter()
                    .map(|child| {
                        intern_expression(child, &default_references, binding_ids, columns, arena)
                    })
                    .collect::<Result<Box<[_]>>>()?;
                arena.intern(ScalarSpec {
                    kind: ScalarKind::Window {
                        function: ScalarWindow::from_bound(expression),
                    },
                    logical_type: expression.return_type(),
                    children,
                    local_properties: ScalarLocalProperties {
                        may_error: true,
                        ..Default::default()
                    },
                })?
            }
        };
        roots.push(root);
        Ok(())
    })?;
    if matches!(operator, LogicalOperator::Filter(_)) {
        // Logical identity has no preferred evaluation order within a pure,
        // total segment. Preserve fence positions and occurrence arity; the
        // physical Filter contract chooses execution order after search.
        let semantic_roots = semantic_operand_fingerprints(operator)?;
        sort_filter_scalar_roots(&mut roots, &semantic_roots, arena);
    }
    Ok(roots.into_boxed_slice())
}

/// Return a stable identity for the scalar operands of one logical shell.
///
/// `ScalarExprId` and the `ColumnId` embedded in a lowered scalar are local to
/// one Memo construction.  They are useful for arena lookup, but cannot be a
/// cross-Memo identity for a SeedPlan.  The bound expression/operand itself is
/// the semantic source of truth; references are positional or binding-based,
/// and therefore remain stable when a destination allocates a different
/// scalar/column numbering.
fn semantic_operand_fingerprints<Child>(
    operator: &LogicalOperator<Child>,
) -> Result<Vec<Fingerprint>> {
    use fields::{OperandRef, ReferenceScope};

    let mut fingerprints = Vec::new();
    fields::visit_fields(operator, |operand| {
        let mut builder = StableFingerprintBuilder::default();
        match operand {
            OperandRef::Expression(expression, scope) => {
                builder.write_u64(0);
                builder.write_u64(match scope {
                    ReferenceScope::Input => 0,
                    ReferenceScope::Output => 1,
                    ReferenceScope::Left => 2,
                    ReferenceScope::None => 3,
                    ReferenceScope::Reducers => 4,
                });
                builder.write_fingerprint(expression_fingerprint(expression));
            }
            OperandRef::Comparison(condition) => {
                builder.write_u64(1);
                builder.write_u64(match condition.comparison {
                    JoinComparisonType::Equal => 0,
                    JoinComparisonType::NotEqual => 1,
                    JoinComparisonType::LessThan => 2,
                    JoinComparisonType::LessThanOrEqual => 3,
                    JoinComparisonType::GreaterThan => 4,
                    JoinComparisonType::GreaterThanOrEqual => 5,
                    JoinComparisonType::DistinctFrom => 6,
                    JoinComparisonType::NotDistinctFrom => 7,
                });
                builder.write_fingerprint(expression_fingerprint(&condition.left));
                builder.write_fingerprint(expression_fingerprint(&condition.right));
            }
            OperandRef::Window(window) => {
                builder.write_u64(2);
                builder.write_fingerprint(window_fingerprint(window));
                builder.write_u64(window.partitions.len() as u64);
                for partition in &window.partitions {
                    builder.write_fingerprint(expression_fingerprint(partition));
                }
                builder.write_u64(window.orders.len() as u64);
                for order in &window.orders {
                    builder.write_u64(order.ascending as u64);
                    builder.write_u64(order.nulls_first as u64);
                    builder.write_fingerprint(expression_fingerprint(&order.expression));
                }
                builder.write_bytes(format!("{:?}", window.frame).as_bytes());
                builder.write_u64(window.ignore_nulls as u64);
            }
        }
        fingerprints.push(builder.finish());
        Ok(())
    })?;
    Ok(fingerprints)
}

/// Sort only reorderable Filter segments using semantic identities.  Fence
/// positions are taken from the lowered arena because they include execution
/// properties (volatility/error/side effects) that are not part of the
/// binding-level expression fingerprint.
fn sort_filter_scalar_roots(
    roots: &mut [ScalarExprId],
    semantic_roots: &[Fingerprint],
    arena: &ScalarArena,
) {
    debug_assert_eq!(roots.len(), semantic_roots.len());
    let mut start = 0;
    for end in 0..=roots.len() {
        if end == roots.len()
            || arena
                .get(roots[end])
                .is_none_or(|node| node.properties.is_evaluation_fence())
        {
            let mut segment = (start..end)
                .map(|index| (semantic_roots[index], roots[index]))
                .collect::<Vec<_>>();
            segment.sort_by_key(|(fingerprint, _)| *fingerprint);
            for (index, (_, root)) in (start..end).zip(segment) {
                roots[index] = root;
            }
            start = end + 1;
        }
    }
}

/// Stable scalar identities in the same canonical order used by the Memo
/// shell identity.  The arena is consulted only for the fence boundaries;
/// no arena-local fingerprint or ordinal escapes into the result.
pub(crate) fn semantic_scalar_root_fingerprints<Child>(
    operator: &LogicalOperator<Child>,
    roots: &[ScalarExprId],
    arena: &ScalarArena,
) -> Result<Box<[Fingerprint]>> {
    let mut fingerprints = semantic_operand_fingerprints(operator)?;
    if fingerprints.len() != roots.len() {
        return Err(paro_error::internal(
            "semantic scalar identity arity disagrees with lowered operator roots",
        ));
    }
    if matches!(operator, LogicalOperator::Filter(_)) {
        // The scalar arena keeps fence positions fixed while the Memo sorts
        // each reorderable segment. Apply that same segment normalization to
        // the semantic identities without exposing scalar IDs.
        let mut start = 0;
        for end in 0..=roots.len() {
            if end == roots.len()
                || arena
                    .get(roots[end])
                    .is_none_or(|node| node.properties.is_evaluation_fence())
            {
                fingerprints[start..end].sort_unstable();
                start = end + 1;
            }
        }
    }
    Ok(fingerprints.into_boxed_slice())
}

fn get_reference_columns(
    get: &paro_planner::operator::Get,
    binding_ids: &mut BindingCatalog,
    columns: &mut ColumnCatalog,
) -> Result<Vec<ColumnId>> {
    get.returned_types
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, logical_type)| {
            intern_column_binding(
                ColumnBinding::new(get.table_index, index),
                logical_type,
                binding_ids,
                columns,
            )
        })
        .collect()
}

pub(crate) fn intern_expression(
    expression: &Expression,
    reference_columns: &[ColumnId],
    binding_ids: &mut BindingCatalog,
    columns: &mut ColumnCatalog,
    arena: &mut ScalarArena,
) -> Result<ScalarExprId> {
    let domain = imports::BoundImportDomain(binding_ids.version(), columns.version());
    if let Some(root) = arena
        .bound_imports
        .lookup(domain, expression, reference_columns)
    {
        return Ok(root);
    }
    let root = lower_expression(expression, reference_columns, binding_ids, columns, arena)?;
    arena
        .bound_imports
        .insert(expression, reference_columns, root);
    Ok(root)
}

fn lower_expression(
    expression: &Expression,
    reference_columns: &[ColumnId],
    binding_ids: &mut BindingCatalog,
    columns: &mut ColumnCatalog,
    arena: &mut ScalarArena,
) -> Result<ScalarExprId> {
    let mut pending = Vec::<PendingConjunction>::new();
    // These identities are scoped to the borrowed immutable DAG and one
    // binding/reference namespace. They must never survive into another
    // lowering call or a catalog rollback.
    let lowered = RefCell::new(HashMap::new());
    let root = ExpressionIterator::try_fold_post_order_cached(
        expression,
        |node| lowered.borrow().get(&node.allocation_identity()).copied(),
        |node, children| {
            if let Expression::Conjunction(conjunction) = node {
                // Defer safe associative nodes until their maximal run is known.
                // Interning every prefix of a generated OR/AND chain eagerly
                // stores O(n²) child IDs even though the final DAG is one node.
                let safe = children.iter().all(|child| match child {
                    LoweredScalar::Pending(_) => true,
                    LoweredScalar::Interned(id) => arena
                        .get(*id)
                        .is_some_and(|node| node.properties.can_reorder_and_share()),
                });
                if safe {
                    let index = pending.len();
                    pending.push(PendingConjunction {
                        kind: match conjunction.conjunction_type {
                            ConjunctionType::And => ScalarKind::And,
                            ConjunctionType::Or => ScalarKind::Or,
                        },
                        children: children.into(),
                        resolved: None,
                    });
                    let result = LoweredScalar::Pending(index);
                    lowered
                        .borrow_mut()
                        .insert(node.allocation_identity(), result);
                    return Ok(result);
                }
            }
            let children = children
                .iter()
                .copied()
                .map(|child| resolve_lowered_scalar(child, &mut pending, arena))
                .collect::<Result<Vec<_>>>()?;
            let result = intern_expression_node(
                node,
                &children,
                reference_columns,
                binding_ids,
                columns,
                arena,
            )
            .map(LoweredScalar::Interned)?;
            lowered
                .borrow_mut()
                .insert(node.allocation_identity(), result);
            Ok(result)
        },
    )?;
    resolve_lowered_scalar(root, &mut pending, arena)
}

#[derive(Clone, Copy)]
enum LoweredScalar {
    Interned(ScalarExprId),
    Pending(usize),
}

struct PendingConjunction {
    kind: ScalarKind,
    children: Box<[LoweredScalar]>,
    resolved: Option<ScalarExprId>,
}

/// Local indexed construction storage avoids recursively owned pending ropes.
/// Every associative run is flattened once, including on error/drop paths.
fn resolve_lowered_scalar(
    root: LoweredScalar,
    pending: &mut [PendingConjunction],
    arena: &mut ScalarArena,
) -> Result<ScalarExprId> {
    if let LoweredScalar::Interned(id) = root {
        return Ok(id);
    }
    enum Task {
        Enter(LoweredScalar),
        Finish(usize, usize),
    }
    let mut tasks = vec![Task::Enter(root)];
    let mut completed = Vec::new();
    while let Some(task) = tasks.pop() {
        match task {
            Task::Enter(LoweredScalar::Interned(id)) => completed.push(id),
            Task::Enter(LoweredScalar::Pending(index)) => {
                if let Some(id) = pending[index].resolved {
                    completed.push(id);
                    continue;
                }
                let mut flatten = pending[index].children.to_vec();
                let mut leaves = Vec::new();
                let mut expanded = HashSet::new();
                while let Some(child) = flatten.pop() {
                    if let LoweredScalar::Pending(child_index) = child {
                        if pending[child_index].kind == pending[index].kind {
                            // Pending runs contain only reorderable,
                            // idempotent predicates. A shared run contributes
                            // its set of arms once, not once per DAG path.
                            if expanded.insert(child_index) {
                                flatten.extend(pending[child_index].children.iter().copied());
                            }
                            continue;
                        }
                    }
                    leaves.push(child);
                }
                tasks.push(Task::Finish(index, leaves.len()));
                tasks.extend(leaves.into_iter().map(Task::Enter));
            }
            Task::Finish(index, count) => {
                let start = completed
                    .len()
                    .checked_sub(count)
                    .ok_or_else(|| paro_error::internal("scalar run lost a completed input"))?;
                let children = completed.split_off(start);
                let id = arena.canonical_conjunction(pending[index].kind.clone(), children)?;
                pending[index].resolved = Some(id);
                completed.push(id);
            }
        }
    }
    if completed.len() != 1 {
        return Err(paro_error::internal("scalar run has no unique root"));
    }
    Ok(completed[0])
}

fn intern_expression_node(
    expression: &Expression,
    children: &[ScalarExprId],
    reference_columns: &[ColumnId],
    binding_ids: &mut BindingCatalog,
    columns: &mut ColumnCatalog,
    arena: &mut ScalarArena,
) -> Result<ScalarExprId> {
    match expression {
        Expression::ColumnRef(column) => {
            let column_id = intern_column_binding(
                column.binding,
                column.return_type.clone(),
                binding_ids,
                columns,
            )?;
            arena.intern(ScalarSpec {
                kind: if column.depth == 0 {
                    ScalarKind::Column(column_id)
                } else {
                    ScalarKind::CorrelatedColumn {
                        column: column_id,
                        depth: column.depth,
                    }
                },
                logical_type: column.return_type.clone(),
                children: Box::new([]),
                local_properties: ScalarLocalProperties::default(),
            })
        }
        Expression::Reference(reference) => {
            let column_id = reference_columns
                .get(reference.index)
                .copied()
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Query IR scalar reference {} is outside its {}-column input",
                        reference.index,
                        reference_columns.len()
                    ))
                })?;
            let descriptor = columns.get(column_id).ok_or_else(|| {
                paro_error::internal("Query IR reference resolves to an unknown column")
            })?;
            if descriptor.logical_type != reference.return_type {
                return Err(paro_error::internal(format!(
                    "Query IR reference {} type disagrees with its input domain: expected {:?}, got {:?}",
                    reference.index, descriptor.logical_type, reference.return_type
                )));
            }
            arena.intern(ScalarSpec {
                kind: ScalarKind::Column(column_id),
                logical_type: reference.return_type.clone(),
                children: Box::new([]),
                local_properties: ScalarLocalProperties::default(),
            })
        }
        Expression::Constant(constant) => arena.intern(ScalarSpec {
            kind: ScalarKind::Constant {
                value: ScalarLiteral::from_bound(constant),
            },
            logical_type: constant.return_type.clone(),
            children: Box::new([]),
            local_properties: ScalarLocalProperties::default(),
        }),
        Expression::Parameter(parameter) => arena.intern(ScalarSpec {
            kind: ScalarKind::Parameter(parameter.slot.index.index() as u32),
            logical_type: parameter.return_type(),
            children: Box::new([]),
            local_properties: ScalarLocalProperties::default(),
        }),
        Expression::Function(function) => arena.intern(ScalarSpec {
            kind: ScalarKind::Function {
                routine: ScalarFunction::from_bound(function),
            },
            logical_type: function.return_type.clone(),
            children: children.into(),
            local_properties: ScalarLocalProperties::default(),
        }),
        Expression::Cast(cast) => arena.intern(ScalarSpec {
            kind: ScalarKind::Cast {
                try_cast: cast.try_cast,
                binding: ScalarCast::new(cast.cast_info.clone()),
            },
            logical_type: cast.target_type.clone(),
            children: children.into(),
            local_properties: conservative_local_properties(expression),
        }),
        Expression::Conjunction(conjunction) => {
            let kind = match conjunction.conjunction_type {
                ConjunctionType::And => ScalarKind::And,
                ConjunctionType::Or => ScalarKind::Or,
            };
            arena.canonical_conjunction(kind, children.to_vec())
        }
        Expression::Comparison(comparison) => intern_comparison(
            comparison_op(comparison.comparison_type),
            children[0],
            children[1],
            arena,
        ),
        Expression::Case(case) => arena.intern(ScalarSpec {
            kind: ScalarKind::Case,
            logical_type: case.return_type.clone(),
            children: children.into(),
            local_properties: conservative_local_properties(expression),
        }),
        Expression::Operator(operator) => arena.intern(ScalarSpec {
            kind: ScalarKind::Operator {
                operator: operator.operator_type,
            },
            logical_type: operator.return_type.clone(),
            children: children.into(),
            local_properties: conservative_local_properties(expression),
        }),
        Expression::Aggregate(aggregate) => arena.intern(ScalarSpec {
            kind: ScalarKind::Aggregate {
                function: ScalarAggregate::from_bound(aggregate),
            },
            logical_type: aggregate.return_type.clone(),
            children: children.into(),
            local_properties: conservative_local_properties(expression),
        }),
        Expression::Window(window) => arena.intern(ScalarSpec {
            kind: ScalarKind::Window {
                function: ScalarWindow::from_bound(window),
            },
            logical_type: window.return_type(),
            children: children.into(),
            local_properties: conservative_local_properties(expression),
        }),
        Expression::Subquery(_) => Err(paro_error::internal(
            "unplanned scalar subquery reached the Query IR boundary",
        )),
    }
}

fn intern_comparison(
    op: ComparisonOp,
    left: ScalarExprId,
    right: ScalarExprId,
    arena: &mut ScalarArena,
) -> Result<ScalarExprId> {
    let left_type = &arena
        .get(left)
        .ok_or_else(|| paro_error::internal("comparison lost its left scalar"))?
        .logical_type;
    let right_type = &arena
        .get(right)
        .ok_or_else(|| paro_error::internal("comparison lost its right scalar"))?
        .logical_type;
    if left_type == right_type {
        arena.canonical_comparison(op, left, right)
    } else {
        arena.intern(ScalarSpec {
            kind: ScalarKind::Comparison(op),
            logical_type: LogicalType::Boolean,
            children: vec![left, right].into_boxed_slice(),
            local_properties: ScalarLocalProperties {
                may_error: true,
                ..Default::default()
            },
        })
    }
}

fn intern_column_binding(
    binding: ColumnBinding,
    logical_type: LogicalType,
    binding_ids: &mut BindingCatalog,
    columns: &mut ColumnCatalog,
) -> Result<ColumnId> {
    let type_domain = logical_type_fingerprint(&logical_type);
    if let Some(column) = binding_ids
        .get(binding.table_index, binding.column_index, &logical_type)
        .copied()
    {
        return Ok(column);
    }
    let column = columns.intern(
        logical_type,
        true,
        ColumnOrigin::Derived {
            key: typed_binding_fingerprint(binding, type_domain),
        },
        ColumnVisibility::Hidden,
        None,
    )?;
    binding_ids.insert(
        binding.table_index,
        binding.column_index,
        &columns
            .get(column)
            .expect("newly interned column must exist")
            .logical_type,
        column,
    )?;
    Ok(column)
}

fn conservative_local_properties(expression: &Expression) -> ScalarLocalProperties {
    let evaluation = expression.local_evaluation_properties();
    ScalarLocalProperties {
        volatility: if evaluation.can_share_evaluation() {
            Volatility::Immutable
        } else {
            Volatility::Volatile
        },
        may_error: !evaluation.is_infallible(),
        has_side_effects: !evaluation.can_share_evaluation(),
        depends_on_external_state: evaluation.is_reorder_fence(),
        deterministic: evaluation.can_share_evaluation() && !evaluation.is_reorder_fence(),
    }
}

fn comparison_op(comparison: ComparisonType) -> ComparisonOp {
    match comparison {
        ComparisonType::Equal => ComparisonOp::Equal,
        ComparisonType::NotEqual => ComparisonOp::NotEqual,
        ComparisonType::LessThan => ComparisonOp::Less,
        ComparisonType::LessThanOrEqual => ComparisonOp::LessOrEqual,
        ComparisonType::GreaterThan => ComparisonOp::Greater,
        ComparisonType::GreaterThanOrEqual => ComparisonOp::GreaterOrEqual,
        ComparisonType::DistinctFrom => ComparisonOp::DistinctFrom,
        ComparisonType::NotDistinctFrom => ComparisonOp::NotDistinctFrom,
    }
}

fn join_comparison(comparison: JoinComparisonType) -> ComparisonOp {
    match comparison {
        JoinComparisonType::Equal => ComparisonOp::Equal,
        JoinComparisonType::NotEqual => ComparisonOp::NotEqual,
        JoinComparisonType::LessThan => ComparisonOp::Less,
        JoinComparisonType::LessThanOrEqual => ComparisonOp::LessOrEqual,
        JoinComparisonType::GreaterThan => ComparisonOp::Greater,
        JoinComparisonType::GreaterThanOrEqual => ComparisonOp::GreaterOrEqual,
        JoinComparisonType::DistinctFrom => ComparisonOp::DistinctFrom,
        JoinComparisonType::NotDistinctFrom => ComparisonOp::NotDistinctFrom,
    }
}

pub(crate) fn function_fingerprint(
    function: &paro_planner::expression::FunctionExpression,
) -> Fingerprint {
    function_binding_fingerprint(
        &function.function,
        &function.return_type,
        function.routine_identity(),
    )
}

pub(crate) fn function_binding_fingerprint(
    function: &paro_function::scalar::BoundScalarFunction,
    return_type: &LogicalType,
    identity: Option<&RoutineCallIdentity>,
) -> Fingerprint {
    let mut builder = StableFingerprintBuilder::default();
    builder.write_u64(40);
    if let Some(identity) = identity {
        encode_routine_identity(&mut builder, identity);
    } else {
        builder.write_bytes(function.name.as_bytes());
    }
    encode_signature(&mut builder, &function.arguments, return_type);
    if let Some(data) = &function.bind_data {
        builder.write_u64(1);
        builder.write_u64(data.fingerprint());
    }
    builder.finish()
}

pub(crate) fn aggregate_fingerprint(
    aggregate: &paro_planner::expression::AggregateExpression,
) -> Fingerprint {
    aggregate_binding_fingerprint(
        &aggregate.function,
        &aggregate.return_type,
        aggregate.aggr_type,
        aggregate.filter.is_some(),
        aggregate
            .order_bys
            .iter()
            .map(|order| (order.ascending, order.nulls_first)),
        aggregate.bind_info.as_ref(),
    )
}

pub(crate) fn aggregate_binding_fingerprint(
    function: &paro_function::aggregate::AggregateFunction,
    return_type: &LogicalType,
    distinct: paro_planner::expression::AggregateType,
    has_filter: bool,
    orders: impl ExactSizeIterator<Item = (bool, bool)>,
    bind_info: Option<&std::sync::Arc<dyn paro_function::scalar::FunctionData>>,
) -> Fingerprint {
    let mut builder = StableFingerprintBuilder::default();
    builder.write_u64(41);
    builder.write_bytes(function.name.as_bytes());
    encode_signature(&mut builder, &function.arguments, return_type);
    builder.write_u64(distinct as u64);
    builder.write_u64(has_filter as u64);
    builder.write_u64(orders.len() as u64);
    for (ascending, nulls_first) in orders {
        builder.write_u64(ascending as u64);
        builder.write_u64(nulls_first as u64);
    }
    for (role, data) in [(0, function.bind_data.as_ref()), (1, bind_info)] {
        if let Some(data) = data {
            builder.write_u64(role);
            builder.write_u64(data.fingerprint());
        }
    }
    builder.finish()
}

pub(crate) fn window_fingerprint(
    window: &paro_planner::expression::WindowExpression,
) -> Fingerprint {
    ScalarWindow::from_bound(window).fingerprint()
}

pub(crate) fn encode_signature(
    builder: &mut StableFingerprintBuilder,
    arguments: &[LogicalType],
    return_type: &LogicalType,
) {
    builder.write_u64(arguments.len() as u64);
    for argument in arguments {
        super::scalar::encode_logical_type(builder, argument);
    }
    super::scalar::encode_logical_type(builder, return_type);
}

pub(crate) fn encode_routine_identity(
    builder: &mut StableFingerprintBuilder,
    identity: &RoutineCallIdentity,
) {
    match identity {
        RoutineCallIdentity::Catalog {
            routine_id,
            generation,
        } => {
            builder.write_u64(0);
            builder.write_u64(routine_id.raw());
            builder.write_u64(*generation);
        }
        RoutineCallIdentity::Builtin {
            intrinsic,
            semantic_tags,
        } => {
            builder.write_u64(1);
            encode_intrinsic(builder, intrinsic);
            builder.write_u64(semantic_tags.len() as u64);
            for tag in semantic_tags {
                encode_semantic_tag(builder, tag);
            }
        }
    }
}

fn encode_intrinsic(builder: &mut StableFingerprintBuilder, intrinsic: &BuiltinIntrinsicId) {
    let (tag, other) = match intrinsic {
        BuiltinIntrinsicId::Add => (0, None),
        BuiltinIntrinsicId::Subtract => (1, None),
        BuiltinIntrinsicId::Multiply => (2, None),
        BuiltinIntrinsicId::Divide => (3, None),
        BuiltinIntrinsicId::IntegerDivide => (4, None),
        BuiltinIntrinsicId::FullTextMatch => (5, None),
        BuiltinIntrinsicId::FullTextMatchInternal => (6, None),
        BuiltinIntrinsicId::Bm25 => (7, None),
        BuiltinIntrinsicId::Bm25ScoreInternal => (8, None),
        BuiltinIntrinsicId::TsRank => (9, None),
        BuiltinIntrinsicId::TsRankCd => (10, None),
        BuiltinIntrinsicId::ToTsVector => (11, None),
        BuiltinIntrinsicId::PlainToTsQuery => (12, None),
        BuiltinIntrinsicId::ToTsQuery => (13, None),
        BuiltinIntrinsicId::PhraseToTsQuery => (14, None),
        BuiltinIntrinsicId::WebSearchToTsQuery => (15, None),
        BuiltinIntrinsicId::L2Distance => (16, None),
        BuiltinIntrinsicId::L1Distance => (17, None),
        BuiltinIntrinsicId::CosineDistance => (18, None),
        BuiltinIntrinsicId::NegativeInnerProduct => (19, None),
        BuiltinIntrinsicId::SparseDistance => (20, None),
        BuiltinIntrinsicId::Other(value) => (21, Some(value.as_str())),
    };
    builder.write_u64(tag);
    if let Some(other) = other {
        builder.write_bytes(other.as_bytes());
    }
}

fn encode_semantic_tag(builder: &mut StableFingerprintBuilder, tag: &BuiltinSemanticTag) {
    let (tag, other) = match tag {
        BuiltinSemanticTag::Deterministic => (0, None),
        BuiltinSemanticTag::NoSideEffects => (1, None),
        BuiltinSemanticTag::Foldable => (2, None),
        BuiltinSemanticTag::SearchOptimized => (3, None),
        BuiltinSemanticTag::VectorOptimized => (4, None),
        BuiltinSemanticTag::Other(value) => (5, Some(value.as_str())),
    };
    builder.write_u64(tag);
    if let Some(other) = other {
        builder.write_bytes(other.as_bytes());
    }
}

pub(crate) fn value_fingerprint(value: &Value) -> Fingerprint {
    let mut builder = StableFingerprintBuilder::default();
    encode_value(&mut builder, value);
    builder.finish()
}

/// Stable structural identity for a bound expression used by planner-side
/// domain proofs.  This deliberately does not depend on arena-local
/// `ColumnId`s, insertion order, or pointer addresses.  Conjunction children
/// are normalized as a multiset because CTE domain equality treats nested
/// AND/OR arms as commutative and idempotent.
pub(crate) fn expression_fingerprint(expression: &Expression) -> Fingerprint {
    // Expression trees can be generated from very long IN/OR lists.  Keep
    // this identity path explicitly post-order just like logical-plan
    // staging; a query must not be able to exhaust the native stack merely by
    // asking the CTE domain interner for a fingerprint.
    let mut pending = vec![(expression, false)];
    let mut fingerprints = HashMap::<ExpressionIdentity, Fingerprint>::new();
    while let Some((current, visited)) = pending.pop() {
        if fingerprints.contains_key(&current.allocation_identity()) {
            continue;
        }
        if visited {
            let mut builder = StableFingerprintBuilder::default();
            encode_expression_node_fingerprint(&mut builder, current, &fingerprints);
            fingerprints.insert(current.allocation_identity(), builder.finish());
            continue;
        }
        pending.push((current, true));
        let mut children = Vec::new();
        if let Expression::Conjunction(conjunction) = current {
            // Fingerprint only maximal associative runs. Fingerprinting each
            // nested prefix and then flattening it again is quadratic on a
            // generated OR chain, even with an explicit traversal stack.
            let mut run = conjunction.children.iter().collect::<Vec<_>>();
            let mut expanded = HashSet::new();
            while let Some(child) = run.pop() {
                if !expanded.insert(child.allocation_identity()) {
                    continue;
                }
                if let Expression::Conjunction(nested) = child {
                    if nested.conjunction_type == conjunction.conjunction_type {
                        run.extend(nested.children.iter());
                        continue;
                    }
                }
                children.push(child);
            }
        } else {
            ExpressionIterator::enumerate_children(current, |child| children.push(child));
        }
        pending.extend(children.into_iter().rev().map(|child| (child, false)));
    }
    fingerprints[&expression.allocation_identity()]
}

fn expression_child_fingerprint(
    fingerprints: &HashMap<ExpressionIdentity, Fingerprint>,
    expression: &Expression,
) -> Fingerprint {
    *fingerprints
        .get(&expression.allocation_identity())
        .expect("expression post-order must fingerprint every child")
}

fn encode_expression_node_fingerprint(
    builder: &mut StableFingerprintBuilder,
    expression: &Expression,
    fingerprints: &HashMap<ExpressionIdentity, Fingerprint>,
) {
    let child_fp = |expression: &Expression| expression_child_fingerprint(fingerprints, expression);
    match expression {
        Expression::Constant(constant) => {
            builder.write_u64(0);
            builder.write_fingerprint(value_fingerprint(&constant.value));
        }
        Expression::ColumnRef(column) => {
            builder.write_u64(1);
            builder.write_u64(column.binding.table_index as u64);
            builder.write_u64(column.binding.column_index as u64);
            builder.write_u64(column.depth as u64);
        }
        Expression::Function(function) => {
            builder.write_u64(2);
            builder.write_fingerprint(function_fingerprint(function));
            builder.write_u64(function.children.len() as u64);
            for child in &function.children {
                builder.write_fingerprint(child_fp(child));
            }
        }
        Expression::Cast(cast) => {
            builder.write_u64(3);
            super::scalar::encode_logical_type(builder, &cast.target_type);
            builder.write_u64(cast.try_cast as u64);
            builder.write_fingerprint(child_fp(&cast.child));
        }
        Expression::Conjunction(conjunction) => {
            builder.write_u64(match conjunction.conjunction_type {
                ConjunctionType::And => 4,
                ConjunctionType::Or => 5,
            });
            // Domain proofs flatten nested conjunctions of the same kind and
            // compare their arms as an idempotent set.  Encode that exact
            // normalization here so the fingerprint remains a necessary
            // condition for the structural proof (never a source of false
            // negatives).
            let mut leaves = Vec::new();
            let mut pending = conjunction.children.iter().collect::<Vec<_>>();
            let mut expanded = HashSet::new();
            while let Some(child) = pending.pop() {
                if !expanded.insert(child.allocation_identity()) {
                    continue;
                }
                if let Expression::Conjunction(nested) = child {
                    if nested.conjunction_type == conjunction.conjunction_type {
                        pending.extend(nested.children.iter());
                        continue;
                    }
                }
                leaves.push(child);
            }
            let mut children = leaves.into_iter().map(child_fp).collect::<Vec<_>>();
            children.sort_unstable();
            children.dedup();
            builder.write_u64(children.len() as u64);
            for fingerprint in children {
                builder.write_fingerprint(fingerprint);
            }
        }
        Expression::Case(case) => {
            builder.write_u64(6);
            builder.write_fingerprint(child_fp(&case.check));
            builder.write_fingerprint(child_fp(&case.result_if_true));
            builder.write_fingerprint(child_fp(&case.result_if_false));
        }
        Expression::Comparison(comparison) => {
            builder.write_u64(7);
            builder.write_u64(comparison.comparison_type as u64);
            builder.write_fingerprint(child_fp(&comparison.left));
            builder.write_fingerprint(child_fp(&comparison.right));
        }
        Expression::Operator(operator) => {
            builder.write_u64(8);
            builder.write_u64(operator.operator_type as u64);
            builder.write_u64(operator.children.len() as u64);
            for child in &operator.children {
                builder.write_fingerprint(child_fp(child));
            }
        }
        Expression::Parameter(parameter) => {
            builder.write_u64(9);
            builder.write_u64(parameter.slot.index.index() as u64);
            super::scalar::encode_logical_type(builder, &parameter.slot.ty);
        }
        Expression::Reference(reference) => {
            builder.write_u64(10);
            builder.write_u64(reference.index as u64);
        }
        Expression::Aggregate(aggregate) => {
            builder.write_u64(11);
            builder.write_fingerprint(aggregate_fingerprint(aggregate));
            for child in &aggregate.children {
                builder.write_fingerprint(child_fp(child));
            }
            if let Some(filter) = &aggregate.filter {
                builder.write_u64(1);
                builder.write_fingerprint(child_fp(filter));
            } else {
                builder.write_u64(0);
            }
            for order in &aggregate.order_bys {
                builder.write_u64(order.ascending as u64);
                builder.write_u64(order.nulls_first as u64);
                builder.write_fingerprint(child_fp(&order.expression));
            }
        }
        Expression::Subquery(subquery) => {
            // Unplanned scalar subqueries are rejected at the Query IR
            // boundary. Keep a typed marker here so a malformed proof cannot
            // alias a scalar expression of another variant.
            builder.write_u64(12);
            builder.write_bytes(format!("{subquery:?}").as_bytes());
        }
        Expression::Window(window) => {
            builder.write_u64(13);
            builder.write_fingerprint(window_fingerprint(window));
            for partition in &window.partitions {
                builder.write_fingerprint(child_fp(partition));
            }
            for order in &window.orders {
                builder.write_u64(order.ascending as u64);
                builder.write_u64(order.nulls_first as u64);
                builder.write_fingerprint(child_fp(&order.expression));
            }
            encode_frame_bound_fingerprint(builder, &window.frame.start_bound, fingerprints);
            encode_frame_bound_fingerprint(builder, &window.frame.end_bound, fingerprints);
        }
    }
}

fn encode_frame_bound_fingerprint(
    builder: &mut StableFingerprintBuilder,
    bound: &paro_planner::expression::WindowFrameBound,
    fingerprints: &HashMap<ExpressionIdentity, Fingerprint>,
) {
    match bound {
        paro_planner::expression::WindowFrameBound::Unbounded => builder.write_u64(0),
        paro_planner::expression::WindowFrameBound::CurrentRow => builder.write_u64(1),
        paro_planner::expression::WindowFrameBound::Offset(expression) => {
            builder.write_u64(2);
            builder.write_fingerprint(expression_child_fingerprint(fingerprints, expression));
        }
    }
}

pub(crate) fn encode_value(builder: &mut StableFingerprintBuilder, value: &Value) {
    macro_rules! integer {
        ($tag:expr, $value:expr) => {{
            builder.write_u64($tag);
            builder.write_bytes(&$value.to_le_bytes());
        }};
    }
    // Nested values are user-visible constants and can be arbitrarily deep.
    // Encode them with an explicit work stack, preserving the exact order of
    // the previous recursive representation while making fingerprinting safe
    // for generated LIST/STRUCT/ARRAY literals.
    let mut pending = vec![value];
    while let Some(value) = pending.pop() {
        match value {
            Value::Null(ty) => {
                builder.write_u64(0);
                super::scalar::encode_logical_type(builder, ty);
            }
            Value::Boolean(value) => {
                builder.write_u64(1);
                builder.write_u64(*value as u64);
            }
            Value::TinyInt(value) => integer!(2, value),
            Value::SmallInt(value) => integer!(3, value),
            Value::Integer(value) => integer!(4, value),
            Value::BigInt(value) => integer!(5, value),
            Value::HugeInt(value) => integer!(6, value),
            Value::UTinyInt(value) => integer!(7, value),
            Value::USmallInt(value) => integer!(8, value),
            Value::UInteger(value) => integer!(9, value),
            Value::UBigInt(value) => integer!(10, value),
            Value::UHugeInt(value) => integer!(11, value),
            Value::Float(value) => integer!(12, value.to_bits()),
            Value::Double(value) => integer!(13, value.to_bits()),
            Value::Decimal(value, precision, scale) => {
                integer!(14, value);
                builder.write_u64(*precision as u64);
                builder.write_u64(*scale as u64);
            }
            Value::Varchar(value) => {
                builder.write_u64(15);
                builder.write_bytes(value.as_bytes());
            }
            Value::Blob(value) => {
                builder.write_u64(16);
                builder.write_bytes(value);
            }
            Value::Uuid(value) => integer!(17, value),
            Value::Date(value) => integer!(18, value),
            Value::Timestamp(value) => integer!(19, value),
            Value::TimestampTz(value) => integer!(20, value),
            Value::Time(value) => integer!(21, value),
            Value::Interval(months, days, micros) => {
                integer!(22, months);
                builder.write_bytes(&days.to_le_bytes());
                builder.write_bytes(&micros.to_le_bytes());
            }
            Value::List(values, ty) => {
                builder.write_u64(23);
                super::scalar::encode_logical_type(builder, ty);
                builder.write_u64(values.len() as u64);
                pending.extend(values.iter().rev());
            }
            Value::Struct(values, fields) => {
                builder.write_u64(24);
                builder.write_u64(fields.len() as u64);
                for (name, ty) in fields {
                    builder.write_bytes(name.as_bytes());
                    super::scalar::encode_logical_type(builder, ty);
                }
                pending.extend(values.iter().rev());
            }
            Value::Array(values, ty, size) => {
                builder.write_u64(25);
                super::scalar::encode_logical_type(builder, ty);
                builder.write_u64(*size as u64);
                pending.extend(values.iter().rev());
            }
        }
    }
}

pub(crate) fn logical_type_fingerprint(logical_type: &LogicalType) -> Fingerprint {
    let mut fingerprint = StableFingerprintBuilder::default();
    super::scalar::encode_logical_type(&mut fingerprint, logical_type);
    fingerprint.finish()
}

fn typed_binding_fingerprint(binding: ColumnBinding, type_domain: Fingerprint) -> Fingerprint {
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_u64(binding.table_index as u64);
    fingerprint.write_u64(binding.column_index as u64);
    fingerprint.write_fingerprint(type_domain);
    fingerprint.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_planner::expression::{CaseExpression, ConjunctionExpression, ConstantExpression};

    #[test]
    fn native_selectivity_matches_bound_evidence_without_exporting_scalars() {
        use paro_planner::expression::{
            ColumnRefExpression, ComparisonExpression, OperatorExpression, OperatorType,
        };
        use paro_storage::statistics::{BaseStatistics, ColumnStatistics, NumericStats};
        use std::sync::Arc;
        let model = crate::cost_model::CostModel::default();
        let binding = ColumnBinding::new(1, 0);
        let mut stats = ColumnStatistics::with_estimated_distinct(
            BaseStatistics::create_empty(LogicalType::Integer),
            Some(10),
        );
        NumericStats::set_guaranteed_min(stats.statistics_mut(), &Value::Integer(0));
        NumericStats::set_guaranteed_max(stats.statistics_mut(), &Value::Integer(9));
        let stats = HashMap::from([(binding, Arc::new(stats))]);
        let mut bindings = BindingCatalog::default();
        let mut columns = ColumnCatalog::default();
        let mut arena = ScalarArena::default();
        let mut cases = Vec::new();
        for depth in [0, 1] {
            for comparison in [
                ComparisonType::Equal,
                ComparisonType::NotEqual,
                ComparisonType::LessThan,
                ComparisonType::LessThanOrEqual,
                ComparisonType::GreaterThan,
                ComparisonType::GreaterThanOrEqual,
                ComparisonType::DistinctFrom,
                ComparisonType::NotDistinctFrom,
            ] {
                for value in [-1, 0, 4, 9, 10] {
                    let column = Expression::ColumnRef(
                        ColumnRefExpression::with_depth(binding, LogicalType::Integer, depth)
                            .into(),
                    );
                    let constant = Expression::Constant(
                        ConstantExpression::new(Value::Integer(value), LogicalType::Integer).into(),
                    );
                    for (left, right) in [(column.clone(), constant.clone()), (constant, column)] {
                        cases.push(Expression::Comparison(
                            ComparisonExpression::new(comparison, left, right).into(),
                        ));
                    }
                }
            }
        }
        for kind in [ConjunctionType::And, ConjunctionType::Or] {
            for indices in [[0, 31, 78], [3, 7, 15], [80, 83, 87]] {
                cases.push(Expression::Conjunction(
                    ConjunctionExpression::new(kind, indices.map(|i| cases[i].clone()).to_vec())
                        .into(),
                ));
            }
        }
        for operator in [
            OperatorType::Not,
            OperatorType::IsNull,
            OperatorType::IsNotNull,
        ] {
            cases.push(Expression::Operator(
                OperatorExpression::new(operator, vec![cases[0].clone()], LogicalType::Boolean)
                    .into(),
            ));
        }
        for expression in cases {
            let root = intern_expression(&expression, &[], &mut bindings, &mut columns, &mut arena)
                .unwrap();
            let before = arena.len();
            let native = model
                .estimate_native_selectivity(
                    root,
                    &arena,
                    &bindings,
                    |column| {
                        stats
                            .get(&bindings.relation_binding(column)?)
                            .map(|s| s.as_ref().into())
                    },
                    || Ok(true),
                )
                .unwrap()
                .unwrap();
            let bound = model.estimate_selectivity(&expression, &stats);
            assert_eq!(native.to_bits(), bound.to_bits(), "{expression:?}");
            assert_eq!(arena.len(), before);
        }
        let mut root = arena
            .intern(ScalarSpec {
                kind: ScalarKind::Constant {
                    value: ScalarLiteral::new(Value::Boolean(false), LogicalType::Boolean),
                },
                logical_type: LogicalType::Boolean,
                children: Box::new([]),
                local_properties: Default::default(),
            })
            .unwrap();
        for _ in 0..10_000 {
            // Retain a compact DAG even though it denotes exponentially many
            // paths. This deliberately does not use conjunction flattening.
            root = arena
                .intern(ScalarSpec {
                    kind: ScalarKind::Or,
                    logical_type: LogicalType::Boolean,
                    children: Box::new([root, root]),
                    local_properties: Default::default(),
                })
                .unwrap();
        }
        assert_eq!(
            model
                .estimate_native_selectivity(root, &arena, &bindings, |_| None, || Ok(true))
                .unwrap(),
            Some(0.0)
        );
    }

    #[test]
    fn native_selectivity_never_publishes_an_interrupted_dag_analysis() {
        let model = crate::cost_model::CostModel::default();
        let mut arena = ScalarArena::default();
        let bindings = BindingCatalog::default();
        for kind in [ScalarKind::And, ScalarKind::Or] {
            let mut root = arena
                .intern(ScalarSpec {
                    kind: ScalarKind::Constant {
                        value: ScalarLiteral::new(Value::Boolean(false), LogicalType::Boolean),
                    },
                    logical_type: LogicalType::Boolean,
                    children: Box::new([]),
                    local_properties: Default::default(),
                })
                .unwrap();
            for _ in 0..9 {
                root = arena
                    .intern(ScalarSpec {
                        kind: kind.clone(),
                        logical_type: LogicalType::Boolean,
                        children: Box::new([root, root]),
                        local_properties: Default::default(),
                    })
                    .unwrap();
            }
            let mut full_work = 0;
            assert_eq!(
                model
                    .estimate_native_selectivity(
                        root,
                        &arena,
                        &bindings,
                        |_| None,
                        || {
                            full_work += 1;
                            Ok(true)
                        }
                    )
                    .unwrap(),
                Some(0.0)
            );
            assert!(full_work < 110, "shared paths must not expand: {full_work}");
            for limit in 0..full_work {
                let mut reads = 0;
                let result = model
                    .estimate_native_selectivity(
                        root,
                        &arena,
                        &bindings,
                        |_| None,
                        || {
                            reads += 1;
                            Ok(reads <= limit)
                        },
                    )
                    .unwrap();
                assert!(result.is_none(), "partial estimate published at {limit}");
                assert_eq!(reads, limit + 1);
            }
            assert!(model
                .estimate_native_selectivity(
                    root,
                    &arena,
                    &bindings,
                    |_| None,
                    || { Err(paro_error::internal("injected cancellation")) }
                )
                .is_err());
        }
    }

    #[test]
    fn post_reduction_scalar_references_do_not_read_the_aggregate_input() {
        use paro_function::aggregate::distributive::{
            count::get_count_star_function, minmax::get_max_function,
        };
        use paro_planner::expression::{
            AggregateExpression, ColumnRefExpression, ComparisonExpression, ReferenceExpression,
        };
        use paro_planner::operator::{Aggregate, PostAggregateReduction};
        use paro_planner::plan::OwnedLogicalPlan;

        // Matching types make an ordinal error silent: all four domains have
        // a BIGINT in slot zero, but none denotes the same value.
        let ty = LogicalType::BigInt;
        let col = |table| {
            Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(table, 0), ty.clone()).into(),
            )
        };
        let (max, _) = get_max_function().bind(std::slice::from_ref(&ty)).unwrap();
        let aggregate = Aggregate::new(
            1,
            2,
            3,
            OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
            vec![Expression::Reference(
                ReferenceExpression::new(0, ty.clone()).into(),
            )],
            vec![],
            vec![Expression::Aggregate(
                AggregateExpression::new(get_count_star_function(), vec![], ty.clone()).into(),
            )],
            vec![],
        )
        .with_post_reduction(PostAggregateReduction {
            reduction_index: 4,
            reducers: vec![Expression::Aggregate(
                AggregateExpression::new(max, vec![col(2)], ty.clone()).into(),
            )],
            scalar_expressions: vec![Expression::Reference(
                ReferenceExpression::new(0, ty.clone()).into(),
            )],
            predicate: Expression::Comparison(
                ComparisonExpression::new(ComparisonType::Equal, col(2), col(4)).into(),
            ),
        });
        aggregate.verify_post_reduction().unwrap();
        let mut columns = ColumnCatalog::default();
        let mut bindings = BindingCatalog::default();
        let ids = (0..5)
            .map(|table| {
                intern_column_binding(
                    ColumnBinding::new(table, 0),
                    ty.clone(),
                    &mut bindings,
                    &mut columns,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let mut operator = LogicalOperator::Aggregate(Box::new(aggregate));
        let mut arena = ScalarArena::default();
        let roots = intern_operator_scalars(
            &operator,
            &ids[1..3],
            &[[ids[0]]],
            &mut bindings,
            &mut columns,
            &mut arena,
        )
        .unwrap();
        assert_eq!(roots.len(), 5);
        let refs = |root| -> std::collections::BTreeSet<ColumnId> {
            arena
                .get(root)
                .unwrap()
                .properties
                .local_columns()
                .collect()
        };
        assert_eq!(refs(roots[0]), [ids[0]].into());
        assert_eq!(refs(roots[2]), [ids[2]].into());
        let reducer_input = refs(roots[3]);
        assert_eq!(reducer_input.len(), 1);
        let private = *reducer_input.first().unwrap();
        assert!(!ids.contains(&private));
        assert_eq!(
            columns.get(private).unwrap().visibility,
            ColumnVisibility::Hidden
        );
        assert_eq!(refs(roots[4]), [ids[2], ids[4]].into());

        // Export must retain the private reducer coordinate, not turn every
        // native ColumnId back into a relational ColumnRef.
        export_operator_scalars(
            &mut operator,
            &roots,
            &arena,
            &bindings,
            |child, column| child == 0 && column == ids[0],
            || Ok(()),
        )
        .unwrap();
        let LogicalOperator::Aggregate(exported) = &operator else {
            unreachable!()
        };
        exported.verify_post_reduction().unwrap();
        assert!(
            matches!(&exported.post_reduction.as_ref().unwrap().scalar_expressions[0],
            Expression::Reference(reference) if reference.index == 0)
        );

        // A wider source cannot make a nonexistent reducer slot valid.
        let LogicalOperator::Aggregate(mut aggregate) = operator else {
            unreachable!()
        };
        aggregate
            .post_reduction
            .as_mut()
            .unwrap()
            .scalar_expressions[0] = Expression::Reference(ReferenceExpression::new(1, ty).into());
        let error = intern_operator_scalars(
            &LogicalOperator::Aggregate(aggregate),
            &ids[1..3],
            &[ids.as_slice()],
            &mut bindings,
            &mut columns,
            &mut arena,
        )
        .unwrap_err();
        assert!(error.to_string().contains("outside its 1-column input"));
    }

    #[test]
    fn reference_lowering_checks_the_resolved_column_type() {
        let mut columns = ColumnCatalog::default();
        let mut bindings = BindingCatalog::default();
        let id = intern_column_binding(
            ColumnBinding::new(0, 0),
            LogicalType::Integer,
            &mut bindings,
            &mut columns,
        )
        .unwrap();
        let error = intern_expression(
            &Expression::Reference(
                paro_planner::expression::ReferenceExpression::new(0, LogicalType::BigInt).into(),
            ),
            &[id],
            &mut bindings,
            &mut columns,
            &mut ScalarArena::default(),
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("type disagrees with its input domain"));
    }

    #[test]
    fn lowering_retains_lexical_depth_in_column_identity() {
        let mut arena = ScalarArena::default();
        let mut bindings = BindingCatalog::default();
        let mut columns = ColumnCatalog::default();
        let roots = (0..3)
            .map(|depth| {
                let expression = Expression::ColumnRef(
                    paro_planner::expression::ColumnRefExpression::with_depth(
                        ColumnBinding::new(0, 0),
                        LogicalType::Integer,
                        depth,
                    )
                    .into(),
                );
                intern_expression(&expression, &[], &mut bindings, &mut columns, &mut arena)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_ne!(roots[0], roots[1]);
        assert_ne!(roots[1], roots[2]);
        assert_ne!(
            arena.get(roots[0]).unwrap().fingerprint,
            arena.get(roots[1]).unwrap().fingerprint
        );
        assert!(matches!(
            arena.get(roots[2]).unwrap().kind,
            ScalarKind::CorrelatedColumn { depth: 2, .. }
        ));
    }

    #[test]
    fn shared_associative_dag_is_lowered_as_one_idempotent_domain() {
        let leaf = Expression::Constant(
            ConstantExpression::new(Value::Boolean(true), LogicalType::Boolean).into(),
        );
        let flat = Expression::Conjunction(
            ConjunctionExpression::new(ConjunctionType::Or, vec![leaf.clone()]).into(),
        );
        let mut expression = leaf;
        // Keep a finite upper bound even if a future change regresses DAG
        // pruning. The generic fact-fold oracle separately verifies visit
        // counts on a 2^50-path DAG with a hard callback budget.
        for _ in 0..18 {
            expression = Expression::Conjunction(
                ConjunctionExpression::new(
                    ConjunctionType::Or,
                    vec![expression.clone(), expression],
                )
                .into(),
            );
        }
        assert_eq!(
            expression_fingerprint(&expression),
            expression_fingerprint(&flat)
        );
        let mut arena = ScalarArena::default();
        let root = intern_expression(
            &expression,
            &[],
            &mut BindingCatalog::default(),
            &mut ColumnCatalog::default(),
            &mut arena,
        )
        .unwrap();
        assert_eq!(arena.len(), 2);
        assert_eq!(arena.get(root).unwrap().children.len(), 1);
    }

    #[test]
    fn sharing_an_effectful_expression_never_deduplicates_evaluation_edges() {
        use paro_planner::expression::{ComparisonExpression, FunctionExpression};
        let function = paro_function::scalar::math::get_random_function()
            .functions
            .into_iter()
            .next()
            .unwrap();
        let random = Expression::Function(
            FunctionExpression::new(function, vec![], LogicalType::Double).into(),
        );
        let mut expression = Expression::Comparison(
            ComparisonExpression::new(
                ComparisonType::GreaterThan,
                random,
                Expression::Constant(
                    ConstantExpression::new(Value::Double(0.0), LogicalType::Double).into(),
                ),
            )
            .into(),
        );
        for _ in 0..18 {
            expression = Expression::Conjunction(
                ConjunctionExpression::new(
                    ConjunctionType::Or,
                    vec![expression.clone(), expression],
                )
                .into(),
            );
        }
        let mut arena = ScalarArena::default();
        let mut root = intern_expression(
            &expression,
            &[],
            &mut BindingCatalog::default(),
            &mut ColumnCatalog::default(),
            &mut arena,
        )
        .unwrap();
        assert_eq!(arena.len(), 21);
        for _ in 0..18 {
            let node = arena.get(root).unwrap();
            assert!(!node.properties.can_reorder_and_share());
            assert_eq!(node.children.len(), 2);
            assert_eq!(node.children[0], node.children[1]);
            root = node.children[0];
        }
        assert_eq!(
            arena.get(root).unwrap().properties.volatility,
            Volatility::Volatile
        );
    }

    #[test]
    fn lowering_and_evaluation_properties_are_stack_safe() {
        std::thread::Builder::new()
            .stack_size(256 * 1024)
            .spawn(|| {
                let leaf = || {
                    Expression::Constant(
                        ConstantExpression::new(Value::Boolean(true), LogicalType::Boolean).into(),
                    )
                };
                let mut expression = leaf();
                for _ in 0..10_000 {
                    expression = Expression::Case(
                        CaseExpression::new(leaf(), leaf(), expression, LogicalType::Boolean)
                            .into(),
                    );
                }
                assert!(expression.evaluation_properties().is_infallible());
                let mut arena = ScalarArena::default();
                let id = intern_expression(
                    &expression,
                    &[],
                    &mut BindingCatalog::default(),
                    &mut ColumnCatalog::default(),
                    &mut arena,
                )
                .unwrap();
                assert!(!arena.get(id).unwrap().properties.may_error);
                // The last scalar owner is released iteratively too.
                drop(expression);
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn maximal_conjunction_fingerprint_preserves_associative_identity() {
        let leaf = |value| {
            Expression::Constant(
                ConstantExpression::new(Value::Boolean(value), LogicalType::Boolean).into(),
            )
        };
        let nested = Expression::Conjunction(
            ConjunctionExpression::new(
                ConjunctionType::Or,
                vec![
                    leaf(true),
                    Expression::Conjunction(
                        ConjunctionExpression::new(
                            ConjunctionType::Or,
                            vec![leaf(false), leaf(true)],
                        )
                        .into(),
                    ),
                ],
            )
            .into(),
        );
        let flat = Expression::Conjunction(
            ConjunctionExpression::new(ConjunctionType::Or, vec![leaf(false), leaf(true)]).into(),
        );
        assert_eq!(
            expression_fingerprint(&nested),
            expression_fingerprint(&flat)
        );
    }

    #[test]
    fn lowering_a_long_distinct_or_chain_keeps_only_the_maximal_run() {
        use paro_planner::expression::ColumnRefExpression;
        let column = |index| {
            Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(0, index), LogicalType::Boolean).into(),
            )
        };
        let mut expression = column(0);
        for index in 1..10_000 {
            expression = Expression::Conjunction(
                ConjunctionExpression::new(ConjunctionType::Or, vec![expression, column(index)])
                    .into(),
            );
        }
        let mut arena = ScalarArena::default();
        let root = intern_expression(
            &expression,
            &[],
            &mut BindingCatalog::default(),
            &mut ColumnCatalog::default(),
            &mut arena,
        )
        .unwrap();
        assert_eq!(arena.len(), 10_001);
        assert_eq!(arena.get(root).unwrap().children.len(), 10_000);
        assert_eq!(
            arena.get(root).unwrap().properties.column_references.len(),
            10_000
        );
        drop(expression);
    }

    #[test]
    fn fingerprint_handles_deep_conjunction_without_native_recursion() {
        let leaf = Expression::Constant(
            ConstantExpression::new(Value::Boolean(true), LogicalType::Boolean).into(),
        );
        let mut expression = leaf.clone();
        for _ in 0..10_000 {
            expression = Expression::Conjunction(
                ConjunctionExpression::new(ConjunctionType::Or, vec![expression, leaf.clone()])
                    .into(),
            );
        }
        let fingerprint = expression_fingerprint(&expression);
        assert_ne!(fingerprint, Fingerprint::default());
        drop(expression);
    }
}
