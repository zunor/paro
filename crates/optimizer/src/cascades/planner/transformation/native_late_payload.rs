// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Native staging for the closed scan-prefix subset of late payload lowering.
//!
//! Row-id payload fetching remains on the authoritative owned rule.  This
//! adapter handles Projection through Filter/Order/Limit to Filter -> Get, whose
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
use paro_planner::operator::{ColumnBinding, Filter, Get, LogicalOperator, Projection};

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
    prefix_only_complete: &mut bool,
) -> Result<Option<NativeShell>> {
    *prefix_only_complete = false;
    let Some((shell, layouts)) =
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
                ancestors.push(index);
                cursor = filter.child.clone();
            }
            LogicalOperator::Order(order) => {
                ancestors.push(index);
                cursor = order.child.clone();
            }
            LogicalOperator::Limit(limit) => {
                ancestors.push(index);
                cursor = limit.child.clone();
            }
            _ => return Ok(None),
        }
    };

    let Some(candidate) = prove_prefix_candidate(&projection, &filter, &get) else {
        return Ok(None);
    };
    // Row-id lowering needs at least one stored payload output. When every
    // output becomes a derived scan prefix, no output has a stored_column. Mixed
    // outputs must retain the owned peer's subsequent row-id opportunity.
    let only_derived_outputs = candidate.output_indices.len() == projection.expressions.len();
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
    for index in ancestors.into_iter().rev() {
        let ordinal = child_layout
            .bindings()
            .iter()
            .position(|binding| *binding == derived_binding)
            .ok_or_else(|| {
                paro_error::internal("native prefix ancestor lost the derived column")
            })?;
        let node = &mut nodes[index];
        match &mut node.operator {
            LogicalOperator::Filter(filter) => filter.projection_map.include(ordinal),
            LogicalOperator::Order(order) => order.projection_map.include(ordinal),
            LogicalOperator::Limit(_) => {}
            _ => {
                return Err(paro_error::internal(
                    "native prefix ancestor changed operator",
                ))
            }
        }
        child_layout = node
            .operator
            .output_layout_from_child_refs(&[&child_layout]);
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
    *prefix_only_complete = only_derived_outputs;
    Ok(Some(shell))
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

use crate::aggregate::late_payload::prove_prefix_filter_expression;

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
    fn production_binding_builds_native_prefix_shell() {
        use crate::cascades::rules::TransformationRule;
        for wrapper in 0..5 {
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
            let mut prefix_complete = false;
            let shell = try_native_late_payload_prefix(
                &binding.root,
                context.memo(),
                &state,
                &facts,
                &mut prefix_complete,
            )
            .unwrap()
            .expect("production binding should take the native prefix path");
            assert_eq!(prefix_complete, wrapper != 4);
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
            drop(state);
            let rule = super::super::PlannerTransformationRule {
                transformation: PlannerTransformation::LatePayloadFetch,
                planner_state: input.planner_state.clone(),
            };
            let bridges = super::super::semantic_plan::owned_binding_instantiation_count();
            let outputs = rule.apply_binding(&binding, &mut context).unwrap();
            assert_eq!(outputs.len(), if wrapper == 4 { 2 } else { 1 });
            assert_eq!(
                super::super::semantic_plan::owned_binding_instantiation_count(),
                bridges + usize::from(wrapper == 4)
            );
        }
    }
}
