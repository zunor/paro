// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Native staging for the closed scan-prefix subset of late payload lowering.
//!
//! Row-id payload fetching remains on the authoritative owned rule.  This
//! adapter handles Projection through unary and unique-source Join paths to
//! Filter -> Get, whose
//! semantic witness is an exact ASCII membership predicate.  It therefore
//! avoids importing an owned tree without claiming that the full rule has
//! been migrated.

use paro_common::error::{self as paro_error, Result};
#[cfg(test)]
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::scalar::ScalarPredicateProjection;
use paro_planner::expression::Expression;
#[cfg(test)]
use paro_planner::expression::OperatorType;
use paro_planner::operator::{ColumnBinding, Filter, Get, Join, LogicalOperator, Projection};

use super::staging::{NativeChild, NativeShell};
use super::{boundary, Memo, PatternOperand, PlannerTransformState};

/// Try the exact scan-prefix part of LatePayloadFetch on the native shell.
///
/// A native miss deliberately returns None so the owned implementation can
/// still handle row-id paths, joins, and other shapes outside this contract.
pub(super) fn try_native_late_payload_prefix(
    binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
    facts: &boundary::BoundarySnapshot,
) -> Result<Option<NativeShell>> {
    let Some((shell, mut layouts)) =
        NativeShell::from_pattern_with_layouts(memo, state, binding, facts)?
    else {
        return Ok(None);
    };
    if super::native_shell_contains_control_boundary(&shell) {
        return Ok(None);
    }

    let root = shell.root;
    let original_layout = layouts
        .get(root)
        .cloned()
        .ok_or_else(|| paro_error::internal("native late-payload shell has no root layout"))?;
    let LogicalOperator::Projection(projection) = shell.root_operator().clone() else {
        return Ok(None);
    };
    let Some(source_table) = projection.expressions.iter().find_map(|expression| {
        let Expression::Function(function) = expression else {
            return None;
        };
        let Some(ScalarPredicateProjection::Utf8Substring {
            source_argument,
            start: 1,
            length: Some(_),
        }) = function.function.predicate_projection.as_ref()
        else {
            return None;
        };
        let Expression::ColumnRef(source) = function.children.get(*source_argument)? else {
            return None;
        };
        Some(source.binding.table_index)
    }) else {
        return Ok(None);
    };
    let source_counts = native_source_occurrences(&shell, source_table);
    if source_occurrence_at(&source_counts, &projection.child) != Some(1) {
        return Ok(None);
    }
    let mut cursor = projection.child.clone();
    let mut ancestors = Vec::new();
    let (filter_index, get_index, filter, mut get) = loop {
        let NativeChild::Node(index) = cursor else {
            return Ok(None);
        };
        let operator = &shell
            .nodes
            .get(index)
            .ok_or_else(|| paro_error::internal("native prefix path lost a node"))?
            .operator;
        match operator {
            LogicalOperator::Filter(filter) => {
                if let NativeChild::Node(child) = &filter.child {
                    if let LogicalOperator::Get(get) = &shell.nodes[*child].operator {
                        break (index, *child, filter.clone(), get.clone());
                    }
                }
                ancestors.push((index, 0));
                cursor = filter.child.clone();
            }
            LogicalOperator::Join(join) => {
                let (left, right) = match join {
                    Join::Comparison(join) => (&join.left, &join.right),
                    Join::Any(join) => (&join.left, &join.right),
                    Join::Cross(join) => (&join.left, &join.right),
                };
                let slot = match (
                    source_occurrence_at(&source_counts, left),
                    source_occurrence_at(&source_counts, right),
                ) {
                    (Some(1), Some(0)) => 0,
                    (Some(0), Some(1)) => 1,
                    _ => return Ok(None),
                };
                ancestors.push((index, slot));
                cursor = if slot == 0 { left } else { right }.clone();
            }
            operator => {
                let Some(child) = prefix_unary_child(operator) else {
                    return Ok(None);
                };
                ancestors.push((index, 0));
                cursor = child.clone();
            }
        }
    };

    let Some(candidate) = prove_prefix_candidate(&projection, &filter, &get) else {
        return Ok(None);
    };
    let source_type = get
        .column_types
        .get(candidate.source_binding.column_index)
        .cloned()
        .ok_or_else(|| paro_error::internal("native prefix source type is missing"))?;
    let source_column = get
        .stored_column(candidate.source_binding.column_index)
        .ok_or_else(|| paro_error::internal("native prefix source is not a stored column"))?;
    let derived_binding =
        get.append_matched_utf8_prefix(source_column, candidate.byte_width, source_type);

    let mut projection = projection;
    for output_index in candidate.output_indices {
        let expression = projection
            .expressions
            .get_mut(output_index)
            .ok_or_else(|| paro_error::internal("native prefix output ordinal is stale"))?;
        *expression = Expression::ColumnRef(
            paro_planner::expression::ColumnRefExpression::new(
                derived_binding,
                LogicalType::Varchar,
            )
            .into(),
        );
    }
    projection.returned_types = projection
        .expressions
        .iter()
        .map(Expression::return_type)
        .collect();

    let get_width_before = layouts
        .get(get_index)
        .ok_or_else(|| paro_error::internal("native prefix shell lost Get layout"))?
        .len();
    if derived_binding.column_index != get_width_before {
        return Err(paro_error::internal(
            "native prefix Get appended a non-suffix output",
        ));
    }
    let mut filter = filter;
    filter.projection_map.include(derived_binding.column_index);

    let mut nodes = shell.nodes.into_vec();
    nodes[get_index].operator = LogicalOperator::Get(get);
    nodes[get_index].source_proofs = Box::new([]);
    nodes[filter_index].operator = LogicalOperator::Filter(filter);
    nodes[filter_index].source_proofs = Box::new([]);
    let get_layout = nodes[get_index].operator.output_layout_from_child_refs(&[]);
    let mut child_layout = nodes[filter_index]
        .operator
        .output_layout_from_child_refs(&[&get_layout]);
    layouts[get_index] = get_layout;
    layouts[filter_index] = child_layout.clone();
    for (index, slot) in ancestors.into_iter().rev() {
        let ordinal = child_layout
            .bindings()
            .iter()
            .position(|binding| *binding == derived_binding)
            .ok_or_else(|| {
                paro_error::internal("native prefix ancestor lost the derived column")
            })?;
        let node = &mut nodes[index];
        let projection = match &mut node.operator {
            LogicalOperator::Join(Join::Comparison(join)) => Some(if slot == 0 {
                &mut join.left_projection_map
            } else {
                &mut join.right_projection_map
            }),
            LogicalOperator::Join(Join::Any(join)) => Some(if slot == 0 {
                &mut join.left_projection_map
            } else {
                &mut join.right_projection_map
            }),
            LogicalOperator::Join(Join::Cross(_)) => None,
            operator => {
                prefix_unary_child_mut(operator)
                    .ok_or_else(|| paro_error::internal("native prefix ancestor changed operator"))?
                    .1
            }
        };
        if let Some(projection) = projection {
            projection.include(ordinal);
        }
        let mut inputs = Vec::new();
        node.operator.visit_child_links(&mut |child| {
            inputs.push(match child {
                NativeChild::Node(index) => &layouts[*index],
                NativeChild::Group { layout, .. } | NativeChild::MemoGroup { layout, .. } => layout,
            })
        });
        child_layout = node.operator.output_layout_from_child_refs(&inputs);
        layouts[index] = child_layout.clone();
        node.source_proofs = Box::new([]);
    }
    nodes[root].operator = LogicalOperator::Projection(projection);
    nodes[root].source_proofs = Box::new([]);

    let (shell, result_layout) = super::compact_native_shell_with_layout(NativeShell {
        nodes: nodes.into_boxed_slice(),
        root,
    })?;
    if result_layout != original_layout {
        return Ok(None);
    }
    // This Projection now consumes a derived scan column. The existing
    // selective row-fetch contract rejects such a column, even when other
    // outputs are stored payload. The TopN-specific proof does not apply to
    // this root. Thus the owned prefix-then-rowid peer adds no further rewrite.
    Ok(Some(shell))
}

/// Count exact selected occurrences, not distinct node IDs. An opaque boundary
/// cannot prove the absence of another source and therefore fails closed.
fn native_source_occurrences(shell: &NativeShell, table: usize) -> Vec<Option<usize>> {
    let mut counts = Vec::with_capacity(shell.nodes.len());
    for node in &shell.nodes {
        let mut count = Some(usize::from(matches!(&node.operator,
            LogicalOperator::Get(get) if get.table_index == table)));
        node.operator.visit_child_links(&mut |child| {
            count = count
                .zip(source_occurrence_at(&counts, child))
                .map(|(left, right)| left.saturating_add(right).min(2));
        });
        counts.push(count);
    }
    counts
}

fn source_occurrence_at(counts: &[Option<usize>], child: &NativeChild) -> Option<usize> {
    let NativeChild::Node(index) = child else {
        return None;
    };
    counts.get(*index).copied().flatten()
}

struct PrefixCandidate {
    source_binding: ColumnBinding,
    byte_width: usize,
    output_indices: Vec<usize>,
}

fn prove_prefix_candidate(
    projection: &Projection<NativeChild>,
    filter: &Filter<NativeChild>,
    get: &Get,
) -> Option<PrefixCandidate> {
    let table = get
        .table
        .as_ref()
        .filter(|table| table.get_storage().is_some())?;
    let mut candidate: Option<PrefixCandidate> = None;
    for (output_index, expression) in projection.expressions.iter().enumerate() {
        let Expression::Function(function) = expression else {
            continue;
        };
        let Some(ScalarPredicateProjection::Utf8Substring {
            source_argument,
            start: 1,
            length: Some(length),
        }) = function.function.predicate_projection.as_ref()
        else {
            continue;
        };
        let byte_width = usize::try_from(*length).ok().filter(|width| *width > 0)?;
        let Expression::ColumnRef(source) = function.children.get(*source_argument)? else {
            return None;
        };
        if source.depth != 0
            || source.binding.table_index != get.table_index
            || source.return_type != LogicalType::Varchar
        {
            return None;
        }
        let catalog_column = get.stored_column(source.binding.column_index)?;
        if table
            .columns
            .get(catalog_column)
            .is_none_or(|definition| definition.logical_type != source.return_type)
        {
            return None;
        }
        if !filter.expressions.iter().any(|predicate| {
            prove_prefix_filter_expression(
                predicate,
                source.binding,
                &function.function,
                byte_width,
            )
        }) {
            return None;
        }
        match &mut candidate {
            Some(candidate)
                if candidate.source_binding == source.binding
                    && candidate.byte_width == byte_width =>
            {
                candidate.output_indices.push(output_index);
            }
            None => {
                candidate = Some(PrefixCandidate {
                    source_binding: source.binding,
                    byte_width,
                    output_indices: vec![output_index],
                });
            }
            Some(_) => return None,
        }
    }
    candidate
}

use crate::aggregate::late_payload::{
    prefix_unary_child, prefix_unary_child_mut, prove_prefix_filter_expression,
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use paro_catalog::entry::{
        CatalogObjectId, ColumnDefinition, CreateTableInfo, TableCatalogEntry,
    };
    use paro_function::scalar::string::get_substring_functions;
    use paro_function::scalar::ScalarBindInput;
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::{
        ColumnRefExpression, ConstantExpression, FunctionExpression, OperatorExpression,
    };
    use paro_planner::operator::Get;
    use paro_planner::plan::OwnedLogicalPlan;
    use paro_storage::table::table_factory::TableFactory;

    use super::super::{matching, PlannerTransformation};
    use crate::cascades::budget::{BudgetDimension, SearchBudget};
    use crate::cascades::planner::MemoBuilder;

    fn source(binding: ColumnBinding) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(binding, LogicalType::Varchar).into())
    }

    fn substring(input: Expression) -> Expression {
        let functions = get_substring_functions();
        let (function, types) = functions
            .bind(&[
                LogicalType::Varchar,
                LogicalType::BigInt,
                LogicalType::BigInt,
            ])
            .unwrap();
        let bound = function
            .bind(&ScalarBindInput::new(
                types,
                vec![None, Some(Value::BigInt(1)), Some(Value::BigInt(2))],
            ))
            .unwrap();
        Expression::Function(
            FunctionExpression::new(
                bound,
                vec![
                    input,
                    Expression::Constant(
                        ConstantExpression::new(Value::BigInt(1), LogicalType::BigInt).into(),
                    ),
                    Expression::Constant(
                        ConstantExpression::new(Value::BigInt(2), LogicalType::BigInt).into(),
                    ),
                ],
                LogicalType::Varchar,
            )
            .into(),
        )
    }

    fn source_table() -> Arc<TableCatalogEntry> {
        let columns = vec![ColumnDefinition::new(
            "name".to_string(),
            LogicalType::Varchar,
        )];
        let storage = Arc::new(
            TableFactory::default()
                .create_table(&[LogicalType::Varchar])
                .unwrap(),
        );
        Arc::new(
            TableCatalogEntry::from_info(
                CreateTableInfo::new(
                    "paro".into(),
                    "public".into(),
                    "prefix_source".into(),
                    columns,
                ),
                storage,
                CatalogObjectId::from_raw(91_021),
                0,
            )
            .unwrap(),
        )
    }

    fn production_plan(wrapper: usize) -> OwnedLogicalPlan {
        let context = BindContext::new();
        let binding = ColumnBinding::new(7, 0);
        let source_expression = source(binding);
        let predicate = Expression::Operator(
            OperatorExpression::new(
                OperatorType::In,
                vec![
                    substring(source_expression.clone()),
                    Expression::Constant(
                        ConstantExpression::new(
                            Value::Varchar("ab".to_string()),
                            LogicalType::Varchar,
                        )
                        .into(),
                    ),
                ],
                LogicalType::Boolean,
            )
            .into(),
        );
        let get = OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(Get::new(
            7,
            vec!["name".to_string()],
            vec![LogicalType::Varchar],
            source_table(),
        ))));
        let filter = OwnedLogicalPlan::new(
            &context,
            LogicalOperator::Filter(Filter::new(get, vec![predicate])),
        );
        let filter = match wrapper {
            0 => filter,
            1 => OwnedLogicalPlan::synthetic(LogicalOperator::Limit(Box::new(
                paro_planner::operator::Limit::new(filter, None, None),
            ))),
            2 => OwnedLogicalPlan::synthetic(LogicalOperator::Order(
                paro_planner::operator::Order::new(filter, vec![]),
            )),
            5 => OwnedLogicalPlan::synthetic(LogicalOperator::Window(
                paro_planner::operator::Window::new(9, vec![], filter),
            )),
            6 => OwnedLogicalPlan::synthetic(LogicalOperator::TopN(
                paro_planner::operator::TopN::new(filter, vec![], 3, 0),
            )),
            7 => OwnedLogicalPlan::synthetic(LogicalOperator::EmptyResult(
                paro_planner::operator::EmptyResult::new(filter),
            )),
            8..=13 => {
                let peer = OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(Get::new(
                    9,
                    vec!["peer".into()],
                    vec![LogicalType::Varchar],
                    source_table(),
                ))));
                let (left, right) = if wrapper % 2 == 0 {
                    (filter, peer)
                } else {
                    (peer, filter)
                };
                let join = match (wrapper - 8) / 2 {
                    0 => Join::Cross(paro_planner::operator::CrossProduct::new(left, right)),
                    1 => Join::comparison(
                        paro_planner::operator::JoinType::Inner,
                        left,
                        right,
                        vec![],
                    ),
                    _ => Join::any(
                        paro_planner::operator::JoinType::Inner,
                        left,
                        right,
                        Expression::Constant(
                            ConstantExpression::new(Value::Boolean(true), LogicalType::Boolean)
                                .into(),
                        ),
                    ),
                };
                OwnedLogicalPlan::synthetic(LogicalOperator::Join(join))
            }
            _ => OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(filter, vec![]))),
        };
        OwnedLogicalPlan::new(
            &context,
            LogicalOperator::Projection(Projection::new(
                8,
                filter,
                if wrapper == 4 {
                    vec![substring(source_expression.clone()), source_expression]
                } else {
                    vec![substring(source_expression)]
                },
            )),
        )
    }

    #[test]
    fn prefix_witness_requires_ascii_constants_and_same_kernel() {
        let binding = ColumnBinding::new(7, 3);
        let projected = substring(source(binding));
        let Expression::Function(projected_function) = &projected else {
            unreachable!()
        };
        let predicate = Expression::Operator(
            OperatorExpression::new(
                OperatorType::In,
                vec![
                    substring(source(binding)),
                    Expression::Constant(
                        ConstantExpression::new(
                            Value::Varchar("ab".to_string()),
                            LogicalType::Varchar,
                        )
                        .into(),
                    ),
                ],
                LogicalType::Boolean,
            )
            .into(),
        );
        assert!(prove_prefix_filter_expression(
            &predicate,
            binding,
            &projected_function.function,
            2,
        ));

        let non_ascii = Expression::Operator(
            OperatorExpression::new(
                OperatorType::In,
                vec![
                    substring(source(binding)),
                    Expression::Constant(
                        ConstantExpression::new(
                            Value::Varchar("é".to_string()),
                            LogicalType::Varchar,
                        )
                        .into(),
                    ),
                ],
                LogicalType::Boolean,
            )
            .into(),
        );
        assert!(!prove_prefix_filter_expression(
            &non_ascii,
            binding,
            &projected_function.function,
            2,
        ));
    }

    #[test]
    fn mixed_prefix_projection_cannot_chain_rowid_lowering() {
        use crate::transformation_rejection::{
            RejectionReasons, TransformationRejectionCounts, TransformationRejectionGuard as Guard,
        };
        let (mut plan, prefix_changed) =
            crate::aggregate::late_payload::rewrite_matched_prefix_node(production_plan(4))
                .unwrap();
        assert!(prefix_changed);
        let LogicalOperator::Projection(output) = &mut plan.operator else {
            unreachable!()
        };
        // Reach the column guard rather than accidentally passing this test
        // because the synthetic input had no cardinality evidence.
        output.child.stats.estimated_cardinality =
            Some(paro_planner::plan::CardinalityEstimate::exact(10));
        let mut reasons = Some(RejectionReasons::default());
        let (_, changed) = crate::aggregate::late_payload::rewrite_node_profiled(
            plan,
            &BindContext::new(),
            &crate::cost_model::CostModel::default(),
            &mut reasons,
        )
        .unwrap();
        assert!(!changed);
        let mut counts = TransformationRejectionCounts::default();
        counts.record(reasons.unwrap());
        assert!(counts
            .iter()
            .any(|(guard, count)| guard == Guard::SelectiveInvalidColumn && count == 1));
    }

    #[test]
    fn prefix_transport_keeps_topn_projected_output_visible() {
        let mut plan = production_plan(6);
        let LogicalOperator::Projection(output) = &mut plan.operator else {
            panic!("expected output projection")
        };
        let LogicalOperator::TopN(topn) = &mut output.child.operator else {
            panic!("expected TopN")
        };
        topn.projection_map = paro_planner::operator::ProjectionMap::new(vec![0]);
        let (rewritten, changed) =
            crate::aggregate::late_payload::rewrite_matched_prefix_node(plan).unwrap();
        assert!(changed);
        let LogicalOperator::Projection(output) = &rewritten.operator else {
            panic!("expected output projection")
        };
        let Expression::ColumnRef(column) = &output.expressions[0] else {
            panic!("expected derived prefix column")
        };
        assert_eq!(column.binding, ColumnBinding::new(7, 1));
        assert!(output.child.get_column_bindings().contains(&column.binding));
    }

    #[test]
    fn production_binding_builds_native_prefix_shell() {
        use crate::cascades::rules::TransformationRule;
        for wrapper in 0..14 {
            let mut input = MemoBuilder::build(
                production_plan(wrapper),
                BindContext::new(),
                SearchBudget::default(),
            )
            .unwrap();
            input.planner_state.write().unwrap().session =
                Some(paro_context::TestStatementContextBuilder::minimal().build());
            let state = input.planner_state.read().unwrap();
            let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
            let binding = matching::scoped_pattern_bindings(
                PlannerTransformation::LatePayloadFetch,
                input.root,
                expression,
                &input.memo,
                &state,
                None,
                BudgetDimension::RuleWorkPerGroup,
            )
            .unwrap()
            .bindings
            .first()
            .cloned()
            .expect("production prefix pattern should match");
            let mut context = super::super::TransformContext::new(&mut input.memo, input.root);
            let facts = super::super::boundary::BoundarySnapshot::read(
                &mut context,
                &state,
                &binding.root,
                BudgetDimension::RuleWorkPerGroup,
            )
            .unwrap()
            .expect("production binding should have boundary facts");
            let shell =
                try_native_late_payload_prefix(&binding.root, context.memo(), &state, &facts)
                    .unwrap()
                    .expect("production binding should take the native prefix path");
            assert_eq!(
                shell.root_layout().unwrap().len(),
                if wrapper == 4 { 2 } else { 1 }
            );
            let LogicalOperator::Projection(projection) = shell.root_operator() else {
                panic!("expected projection root")
            };
            assert!(matches!(
                projection.expressions.first(),
                Some(Expression::ColumnRef(column)) if column.binding.column_index == 1
            ));
            if wrapper == 8 {
                let mut duplicate = shell.clone();
                let NativeChild::Node(join_index) = projection.child else {
                    panic!("expected selected join node")
                };
                let LogicalOperator::Join(Join::Cross(join)) =
                    &mut duplicate.nodes[join_index].operator
                else {
                    panic!("expected cross join")
                };
                // Two edges to one selected node are two source occurrences,
                // even though both edges carry the identical native node ID.
                join.right = join.left.clone();
                let counts = native_source_occurrences(&duplicate, 7);
                assert_eq!(counts[join_index], Some(2));
            }
            drop(state);
            let rule = super::super::PlannerTransformationRule {
                transformation: PlannerTransformation::LatePayloadFetch,
                planner_state: input.planner_state.clone(),
            };
            let bridges = super::super::semantic_plan::owned_binding_instantiation_count();
            let outputs = rule.apply_binding(&binding, &mut context).unwrap();
            assert_eq!(outputs.len(), 1);
            assert_eq!(
                super::super::semantic_plan::owned_binding_instantiation_count(),
                bridges
            );
        }
    }
}
