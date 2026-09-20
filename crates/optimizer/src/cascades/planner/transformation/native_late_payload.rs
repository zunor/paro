// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Native staging for selected late payload lowering paths.
//!
//! Projection prefix witnesses, ordinary selective row fetch, and both TopN forms
//! use native selected nodes. Unsupported shapes still reach the owned rule;
//! a positive native result alone does not imply the entire rule is migrated.

use paro_common::error::{self as paro_error, Result};
#[cfg(test)]
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::expression::Expression;
#[cfg(test)]
use paro_planner::expression::OperatorType;
use paro_planner::operator::{Join, LogicalOperator};

use super::staging::{NativeChild, NativeShell};
use super::{boundary, Memo, PatternOperand, PlannerTransformState};

/// Try the supported LatePayloadFetch contracts on the native shell.
///
/// A native miss deliberately returns None so the owned implementation can
/// still handle shapes outside these contracts.
pub(super) fn try_native_late_payload_prefix(
    binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
    facts: &boundary::BoundarySnapshot,
) -> Result<Option<NativeShell>> {
    let Some((mut shell, mut layouts)) =
        NativeShell::from_pattern_with_layouts(memo, state, binding, facts)?
    else {
        return Ok(None);
    };
    if super::native_shell_contains_control_boundary(&shell) {
        return Ok(None);
    }
    if matches!(shell.root_operator(), LogicalOperator::TopN(_)) {
        super::native_topn_payload::restore_root_output(&mut shell, binding, memo, state)?;
        let aggregate_input = match shell.root_operator() {
            LogicalOperator::TopN(topn) => super::native_topn_payload::node(&topn.child)
                .and_then(|index| match &shell.nodes[index].operator {
                    LogicalOperator::Projection(output) => {
                        super::native_topn_payload::node(&output.child)
                    }
                    _ => None,
                })
                .is_some_and(|index| {
                    matches!(shell.nodes[index].operator, LogicalOperator::Aggregate(_))
                }),
            _ => false,
        };
        if aggregate_input {
            return super::native_aggregate_topn_payload::rewrite(shell, state);
        }
        return super::native_topn_payload::rewrite(shell, state);
    }

    let root = shell.root;
    let original_layout = layouts
        .get(root)
        .cloned()
        .ok_or_else(|| paro_error::internal("native late-payload shell has no root layout"))?;
    let LogicalOperator::Projection(projection) = shell.root_operator().clone() else {
        return Ok(None);
    };
    let mut paths = std::collections::HashMap::new();
    let candidate = crate::aggregate::late_payload::prove_prefix_outputs(
        &projection.expressions,
        |binding, kernel, byte_width| {
            let path = paths.entry(binding.table_index).or_insert_with(|| {
                native_prefix_path(&shell, &projection.child, binding.table_index)
            });
            let path = path.as_ref()?;
            let LogicalOperator::Get(get) = &shell.nodes[path.get].operator else {
                return None;
            };
            let table = get
                .table
                .as_ref()
                .filter(|table| table.get_storage().is_some())?;
            let column = get.stored_column(binding.column_index)?;
            if table
                .columns
                .get(column)
                .is_none_or(|definition| definition.logical_type != LogicalType::Varchar)
            {
                return None;
            }
            let Some(filter_index) = path.filter else {
                return Some(false);
            };
            let LogicalOperator::Filter(filter) = &shell.nodes[filter_index].operator else {
                return None;
            };
            Some(filter.expressions.iter().any(|predicate| {
                prove_prefix_filter_expression(predicate, binding, kernel, byte_width)
            }))
        },
    );
    let Some(candidate) = candidate else {
        return super::native_selective_payload::rewrite(shell, layouts, state);
    };
    let path = paths
        .remove(&candidate.source_binding.table_index)
        .flatten()
        .ok_or_else(|| paro_error::internal("native prefix witness lost its source path"))?;
    let filter_index = path
        .filter
        .ok_or_else(|| paro_error::internal("native prefix witness lost its filter"))?;
    let get_index = path.get;
    let ancestors = path.ancestors;
    let LogicalOperator::Filter(filter) = shell.nodes[filter_index].operator.clone() else {
        return Err(paro_error::internal("native prefix witness changed filter"));
    };
    let LogicalOperator::Get(mut get) = shell.nodes[get_index].operator.clone() else {
        return Err(paro_error::internal("native prefix witness changed source"));
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
    // The Get contract interns derived outputs: an exact existing prefix is
    // reused, otherwise one suffix is appended. Both need carrier exposure.
    if derived_binding.column_index > get_width_before {
        return Err(paro_error::internal(
            "native prefix Get returned an output beyond its append frontier",
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

struct PrefixPath {
    ancestors: Vec<(usize, usize)>,
    filter: Option<usize>,
    get: usize,
}

/// Resolve only selected nodes. A unique but unfiltered source is known not to
/// witness a prefix; an opaque/unsupported source path is missing evidence.
fn native_prefix_path(
    shell: &NativeShell,
    start: &NativeChild,
    table: usize,
) -> Option<PrefixPath> {
    let counts = native_source_occurrences(shell, table);
    if source_occurrence_at(&counts, start) != Some(1) {
        return None;
    }
    let mut cursor = start;
    let mut ancestors = Vec::new();
    loop {
        let NativeChild::Node(index) = cursor else {
            return None;
        };
        let operator = &shell.nodes.get(*index)?.operator;
        match operator {
            LogicalOperator::Get(_) => {
                return Some(PrefixPath {
                    ancestors,
                    filter: None,
                    get: *index,
                })
            }
            LogicalOperator::Filter(filter) => {
                if let NativeChild::Node(child) = &filter.child {
                    if matches!(shell.nodes.get(*child)?.operator, LogicalOperator::Get(_)) {
                        return Some(PrefixPath {
                            ancestors,
                            filter: Some(*index),
                            get: *child,
                        });
                    }
                }
                ancestors.push((*index, 0));
                cursor = &filter.child;
            }
            LogicalOperator::Join(join) => {
                let (left, right) = match join {
                    Join::Comparison(join) => (&join.left, &join.right),
                    Join::Any(join) => (&join.left, &join.right),
                    Join::Cross(join) => (&join.left, &join.right),
                };
                let slot = match (
                    source_occurrence_at(&counts, left),
                    source_occurrence_at(&counts, right),
                ) {
                    (Some(1), Some(0)) => 0,
                    (Some(0), Some(1)) => 1,
                    _ => return None,
                };
                ancestors.push((*index, slot));
                cursor = if slot == 0 { left } else { right };
            }
            operator => {
                cursor = prefix_unary_child(operator)?;
                ancestors.push((*index, 0));
            }
        }
    }
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
    use paro_planner::operator::{ColumnBinding, Filter, Get, Projection};
    use paro_planner::plan::OwnedLogicalPlan;
    use paro_storage::table::table_factory::TableFactory;

    use super::super::{matching, PlannerTransformation};
    use crate::cascades::budget::{BudgetDimension, SearchBudget};
    use crate::cascades::planner::MemoBuilder;

    fn source(binding: ColumnBinding) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(binding, LogicalType::Varchar).into())
    }

    fn substring(input: Expression) -> Expression {
        substring_width(input, 2)
    }

    fn substring_width(input: Expression, width: i64) -> Expression {
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
                vec![None, Some(Value::BigInt(1)), Some(Value::BigInt(width))],
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
                        ConstantExpression::new(Value::BigInt(width), LogicalType::BigInt).into(),
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
        let mut get = OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(Get::new(
            7,
            vec!["name".to_string()],
            vec![LogicalType::Varchar],
            source_table(),
        ))));
        if wrapper >= 16 {
            get.stats.estimated_cardinality =
                Some(paro_planner::plan::CardinalityEstimate::exact(100_000));
        }
        if wrapper == 19 || wrapper == 20 {
            let LogicalOperator::Get(get) = &mut get.operator else {
                unreachable!()
            };
            get.append_virtual_rowid("existing_rowid");
        }
        let mut filter = OwnedLogicalPlan::new(
            &context,
            LogicalOperator::Filter(Filter::new(get, vec![predicate])),
        );
        if wrapper >= 16 {
            filter.stats.estimated_cardinality =
                Some(paro_planner::plan::CardinalityEstimate::exact(100));
        }
        if wrapper == 20 {
            let LogicalOperator::Filter(filter) = &mut filter.operator else {
                unreachable!()
            };
            filter.projection_map = paro_planner::operator::ProjectionMap::new(vec![0]);
        }
        let filter = match wrapper {
            0 | 16 | 17 | 19 => filter,
            18 => {
                let function = paro_function::window::WindowFunction::row_number();
                let frame = paro_planner::expression::WindowFrame::get_default_frame(&function);
                let mut window = OwnedLogicalPlan::synthetic(LogicalOperator::Window(
                    paro_planner::operator::Window::new(
                        9,
                        vec![paro_planner::expression::WindowExpression::native(
                            function,
                            vec![],
                            vec![],
                            vec![],
                            frame,
                            false,
                        )],
                        filter,
                    ),
                ));
                window.stats.estimated_cardinality =
                    Some(paro_planner::plan::CardinalityEstimate::exact(100));
                window
            }
            20 => {
                let mut order = paro_planner::operator::Order::new(filter, vec![]);
                order.projection_map = paro_planner::operator::ProjectionMap::new(vec![0]);
                let mut order = OwnedLogicalPlan::synthetic(LogicalOperator::Order(order));
                order.stats.estimated_cardinality =
                    Some(paro_planner::plan::CardinalityEstimate::exact(100));
                order
            }
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
            8..=13 | 15 => {
                let peer = OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(Get::new(
                    9,
                    vec!["peer".into()],
                    vec![LogicalType::Varchar],
                    source_table(),
                ))));
                let (left, right) = if wrapper.is_multiple_of(2) {
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
                if wrapper == 16 || wrapper == 19 || wrapper == 20 {
                    vec![source_expression]
                } else if wrapper == 18 {
                    vec![
                        source_expression,
                        Expression::ColumnRef(
                            ColumnRefExpression::new(ColumnBinding::new(9, 0), LogicalType::BigInt)
                                .into(),
                        ),
                    ]
                } else if wrapper == 17 {
                    vec![source_expression.clone(), source_expression]
                } else if wrapper == 4 {
                    vec![substring(source_expression.clone()), source_expression]
                } else if wrapper == 14 {
                    vec![
                        substring(source_expression.clone()),
                        substring_width(source_expression, 1),
                    ]
                } else if wrapper == 15 {
                    vec![
                        substring(source(ColumnBinding::new(9, 0))),
                        substring(source_expression),
                    ]
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
    fn output_proof_distinguishes_nonmatching_missing_and_conflicting_witnesses() {
        let binding = ColumnBinding::new(7, 0);
        let expressions = [
            substring(source(binding)),
            substring_width(source(binding), 1),
        ];
        let candidate =
            crate::aggregate::late_payload::prove_prefix_outputs(&expressions, |_, _, width| {
                Some(width == 2)
            })
            .unwrap();
        assert_eq!(candidate.output_indices, [0]);
        assert!(
            crate::aggregate::late_payload::prove_prefix_outputs(&expressions, |_, _, _| None,)
                .is_none()
        );
        assert!(crate::aggregate::late_payload::prove_prefix_outputs(
            &expressions,
            |_, _, _| Some(true),
        )
        .is_none());
        let repeated = [substring(source(binding)), substring(source(binding))];
        assert_eq!(
            crate::aggregate::late_payload::prove_prefix_outputs(&repeated, |_, _, _| Some(true),)
                .unwrap()
                .output_indices,
            [0, 1]
        );
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
    fn production_prefix_reuses_existing_derived_scan_output() {
        use crate::cascades::rules::TransformationRule;
        let mut plan = production_plan(0);
        let LogicalOperator::Projection(output) = &mut plan.operator else { unreachable!() };
        let LogicalOperator::Filter(filter) = &mut output.child.operator else { unreachable!() };
        let LogicalOperator::Get(get) = &mut filter.child.operator else { unreachable!() };
        let reused = get.append_matched_utf8_prefix(0, 2, LogicalType::Varchar);
        assert_eq!(reused.column_index, 1);
        let mut input = MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
        input.planner_state.write().unwrap().session =
            Some(paro_context::TestStatementContextBuilder::minimal().build());
        let state = input.planner_state.read().unwrap();
        let expr = input.memo.group(input.root).unwrap().logical_exprs()[0];
        let bindings = matching::scoped_pattern_bindings(
            PlannerTransformation::LatePayloadFetch, input.root, expr, &input.memo,
            &state, None, BudgetDimension::RuleWorkPerGroup,
        ).unwrap();
        drop(state);
        let mut ctx = super::super::TransformContext::new(&mut input.memo, input.root);
        let rule = super::super::PlannerTransformationRule {
            transformation: PlannerTransformation::LatePayloadFetch,
            planner_state: input.planner_state.clone(),
        };
        let bridges = super::super::semantic_plan::owned_binding_instantiation_count();
        let outputs = rule.apply_binding(&bindings.bindings[0], &mut ctx).unwrap();
        assert_eq!(outputs.len(), 1);
        assert_eq!(bridges, super::super::semantic_plan::owned_binding_instantiation_count());
        let state = input.planner_state.read().unwrap();
        let payload = &state.payloads.logical[outputs[0].payload.index()];
        let LogicalOperator::Projection(output) = &payload.semantic_template.operator else {
            unreachable!()
        };
        let Expression::ColumnRef(column) = &output.expressions[0] else { unreachable!() };
        assert_eq!(column.binding, reused);
        drop(state);
        drop(outputs);
        ctx.rollback().unwrap();
    }

    #[test]
    fn production_binding_builds_native_prefix_shell() {
        use crate::cascades::rules::TransformationRule;
        for wrapper in 0..21 {
            if wrapper >= 16 {
                let (reference, changed) = crate::aggregate::late_payload::rewrite_node(
                    production_plan(wrapper),
                    &BindContext::new(),
                    &crate::cost_model::CostModel::default(),
                )
                .unwrap();
                assert!(changed);
                let LogicalOperator::Projection(output) = &reference.operator else {
                    unreachable!()
                };
                let LogicalOperator::RowFetch(fetch) = &output.child.operator else {
                    unreachable!()
                };
                assert_eq!(fetch.sources.len(), 1);
                assert_eq!(fetch.sources[0].needed_columns.as_ref(), &[0]);
            }
            if wrapper == 14 || wrapper == 15 {
                let (reference, changed) =
                    crate::aggregate::late_payload::rewrite_matched_prefix_node(production_plan(
                        wrapper,
                    ))
                    .unwrap();
                assert!(changed);
                let LogicalOperator::Projection(output) = &reference.operator else {
                    unreachable!()
                };
                assert!(matches!(
                    output.expressions[usize::from(wrapper == 14)],
                    Expression::Function(_)
                ));
            }
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
                if wrapper == 4 || wrapper == 14 || wrapper == 15 || wrapper == 17 || wrapper == 18
                {
                    2
                } else {
                    1
                }
            );
            let LogicalOperator::Projection(projection) = shell.root_operator() else {
                panic!("expected projection root")
            };
            if wrapper >= 16 {
                let NativeChild::Node(fetch_index) = projection.child else {
                    unreachable!()
                };
                let LogicalOperator::RowFetch(fetch) = &shell.nodes[fetch_index].operator else {
                    unreachable!()
                };
                assert_eq!(fetch.sources.len(), 1);
                assert_eq!(fetch.sources[0].needed_columns.as_ref(), &[0]);
                let Expression::ColumnRef(result) = &projection.expressions[0] else {
                    unreachable!()
                };
                assert_eq!(
                    result.binding,
                    ColumnBinding::new(fetch.sources[0].materialized_table_index, 0)
                );
                if wrapper == 18 {
                    let Expression::ColumnRef(ordinary) = &projection.expressions[1] else {
                        unreachable!()
                    };
                    assert_eq!(
                        ordinary.binding,
                        ColumnBinding::new(fetch.carrier_table_index, 0)
                    );
                    let NativeChild::Node(carrier_index) = fetch.child else {
                        unreachable!()
                    };
                    let LogicalOperator::Projection(carrier) = &shell.nodes[carrier_index].operator
                    else {
                        unreachable!()
                    };
                    assert_eq!(carrier.visible_count, 0);
                    assert!(
                        matches!(&carrier.expressions[0], Expression::ColumnRef(column)
                        if column.binding == ColumnBinding::new(9, 0))
                    );
                    assert_eq!(
                        carrier.returned_types,
                        vec![LogicalType::BigInt, LogicalType::BigInt]
                    );
                }
                if wrapper == 19 || wrapper == 20 {
                    let get = shell
                        .nodes
                        .iter()
                        .find_map(|node| match &node.operator {
                            LogicalOperator::Get(get) if get.table_index == 7 => Some(get),
                            _ => None,
                        })
                        .unwrap();
                    assert_eq!(
                        get.column_sources
                            .iter()
                            .filter(|source| matches!(
                                source,
                                paro_planner::operator::GetColumnSource::VirtualRowId
                            ))
                            .count(),
                        1
                    );
                    assert_eq!(get.returned_types.len(), 2);
                }
                if wrapper == 20 {
                    // Memo ingress expands output maps to the canonical
                    // layout; test that real representation, not an invented
                    // narrow map that bypasses binding construction.
                    for node in &shell.nodes {
                        match &node.operator {
                            LogicalOperator::Filter(filter) => assert_eq!(
                                filter.projection_map,
                                paro_planner::operator::ProjectionMap::all()
                            ),
                            LogicalOperator::Order(order) => assert_eq!(
                                order.projection_map,
                                paro_planner::operator::ProjectionMap::all()
                            ),
                            _ => {}
                        }
                    }
                }
                // The same selected shell must reject an upper bound with no
                // reduction, even if its expected output remains small.
                let (mut original, original_layouts) = NativeShell::from_pattern_with_layouts(
                    context.memo(),
                    &state,
                    &binding.root,
                    &facts,
                )
                .unwrap()
                .unwrap();
                let LogicalOperator::Projection(output) = original.root_operator() else {
                    unreachable!()
                };
                let NativeChild::Node(child) = output.child else {
                    unreachable!()
                };
                original.nodes[child]
                    .stats
                    .estimated_cardinality
                    .as_mut()
                    .unwrap()
                    .max = 100_000;
                assert!(super::super::native_selective_payload::rewrite(
                    original,
                    original_layouts,
                    &state
                )
                .unwrap()
                .is_none());
            } else {
                assert!(matches!(
                    projection.expressions.get(usize::from(wrapper == 15)),
                    Some(Expression::ColumnRef(column)) if column.binding.column_index == 1
                ));
            }
            if wrapper == 14 {
                assert!(matches!(projection.expressions[1], Expression::Function(_)));
            }
            if wrapper == 15 {
                assert!(matches!(projection.expressions[0], Expression::Function(_)));
            }
            if wrapper == 10 || wrapper == 11 {
                use crate::aggregate::late_payload::{prove_rowid_operator, RowIdPathPolicy};
                use paro_planner::operator::JoinType;
                for (join_type, allowed) in [
                    (JoinType::Inner, true),
                    (JoinType::Left, wrapper == 10),
                    (JoinType::Right, wrapper == 11),
                    (JoinType::Outer, false),
                ] {
                    let mut selected = shell.clone();
                    let NativeChild::Node(index) = projection.child else {
                        unreachable!()
                    };
                    let LogicalOperator::Join(Join::Comparison(join)) =
                        &mut selected.nodes[index].operator
                    else {
                        unreachable!()
                    };
                    join.join_type = join_type;
                    let counts = native_source_occurrences(&selected, 7);
                    let resolve = |child: &NativeChild| match child {
                        NativeChild::Node(index) => Some(&selected.nodes[*index].operator),
                        _ => None,
                    };
                    for policy in [RowIdPathPolicy::RowPreserving, RowIdPathPolicy::NonNull] {
                        assert_eq!(
                            prove_rowid_operator(
                                &selected.nodes[index].operator,
                                7,
                                policy,
                                &resolve,
                                &|child| source_occurrence_at(&counts, child),
                            )
                            .is_some(),
                            allowed
                        );
                    }
                }
            }
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
            let before_publish = (
                context.memo().group_count(),
                state.columns.len(),
                state.scalars.len(),
                state.binding_ids.checkpoint(),
                state.payloads.logical.len(),
            );
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
            if wrapper >= 16 {
                drop(outputs);
                context.rollback().unwrap();
                let state = input.planner_state.read().unwrap();
                assert_eq!(
                    (
                        input.memo.group_count(),
                        state.columns.len(),
                        state.scalars.len(),
                        state.binding_ids.checkpoint(),
                        state.payloads.logical.len()
                    ),
                    before_publish
                );
            }
        }
    }
}
