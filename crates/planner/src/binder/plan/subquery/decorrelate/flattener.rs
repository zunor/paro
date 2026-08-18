// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Dependent-join decorrelation: flattener dispatch and per-shape flattening.

use crate::binder::bind::from::join_utils::{
    collect_table_bindings, extract_join_condition, get_expression_side, split_conjunction,
};
use crate::binder::context::BindShared;
use crate::binder::ir::OrderByNode;
use crate::binder::{Binder, CorrelatedColumnInfo};
use crate::expression::*;
use crate::operator::Window;
use crate::operator::{
    ColumnBinding, ComparisonJoin, CrossProduct, DelimGet, DependentJoin, DependentJoinKind,
    DistinctType, Filter, Join, JoinComparisonType, JoinCondition, JoinSide, JoinType,
    LogicalOperator, MarkJoinSemantics, MarkSubqueryKind, Projection,
};
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::scalar::cast::CastFunctionSet;
use paro_function::window::WindowFunction;
use std::sync::Arc;

use crate::binder::plan::subquery::{
    build_correlated_column_map, expression_has_correlated_columns_at_depth,
    operator_has_correlated_columns_at_depth, CorrelatedColumnMap, RewriteCorrelatedExpressions,
};
use crate::plan::LogicalPlan;

use super::helpers::{
    can_push_to_left_child, can_push_to_right_child, push_filter_to_child,
    should_eliminate_join_condition,
};

struct PushDownResult {
    plan: LogicalPlan,
    base_binding: ColumnBinding,
    visible_columns: Vec<usize>,
}

pub struct DependentJoinFlattener {
    shared: Arc<BindShared>,
    /// Bindings as referenced inside the dependent RHS before decorrelation.
    correlated_columns: Vec<CorrelatedColumnInfo>,
    /// Equivalent bindings exposed by the left input at the dependent-join boundary.
    ///
    /// These differ for clauses above a grouping boundary: the RHS still refers to the
    /// pre-aggregate group expression, while the join must read the aggregate's group output.
    outer_correlated_columns: Vec<CorrelatedColumnInfo>,
    delim_table_index: usize,
    delim_scan_allocated: bool,
    correlated_base_binding: Option<ColumnBinding>,
    cast_functions: Arc<CastFunctionSet>,
}

impl DependentJoinFlattener {
    pub fn new(
        shared: Arc<BindShared>,
        correlated_columns: Vec<CorrelatedColumnInfo>,
        delim_table_index: usize,
        cast_functions: Arc<CastFunctionSet>,
    ) -> Self {
        Self {
            shared,
            correlated_columns,
            outer_correlated_columns: Vec::new(),
            delim_table_index,
            delim_scan_allocated: false,
            correlated_base_binding: None,
            cast_functions,
        }
    }

    fn next_table_index(&self) -> usize {
        self.shared.generate_table_index()
    }

    pub fn flatten(
        &mut self,
        binder: &mut Binder,
        dependent_join: DependentJoin,
    ) -> Result<LogicalOperator> {
        let left = *dependent_join.left;
        let right = *dependent_join.right;
        let correlated_columns = dependent_join.correlated_columns;
        let kind = dependent_join.kind;

        if correlated_columns.is_empty() {
            return Ok(LogicalOperator::Join(Join::Cross(CrossProduct::new(
                left, right,
            ))));
        }

        self.correlated_columns = correlated_columns.clone();
        self.outer_correlated_columns =
            Self::bind_correlations_to_left_output(&left.operator, &correlated_columns)?;

        let PushDownResult {
            plan: rewritten_right,
            base_binding,
            visible_columns,
        } = self.push_down_dependent_join(binder, right, 0)?;
        self.correlated_base_binding = Some(base_binding);

        match kind {
            DependentJoinKind::Scalar => {
                self.flatten_scalar_subquery(left, rewritten_right, visible_columns)
            }
            DependentJoinKind::Mark {
                mark_index,
                subquery: MarkSubqueryKind::Exists,
            } => self.flatten_exists_subquery(binder, left, rewritten_right, mark_index, false),
            DependentJoinKind::Mark {
                mark_index,
                subquery: MarkSubqueryKind::NotExists,
            } => self.flatten_exists_subquery(binder, left, rewritten_right, mark_index, true),
            DependentJoinKind::Mark {
                mark_index,
                subquery: MarkSubqueryKind::Any(payload),
            } => self.flatten_any_subquery(
                binder,
                left,
                rewritten_right,
                payload.comparison_type,
                mark_index,
                &payload.expression_children,
                &payload.child_types,
                &payload.child_targets,
            ),
            DependentJoinKind::Mark {
                mark_index,
                subquery: MarkSubqueryKind::All(payload),
            } => self.flatten_all_subquery(
                binder,
                left,
                rewritten_right,
                payload.comparison_type,
                mark_index,
                &payload.expression_children,
                &payload.child_types,
                &payload.child_targets,
            ),
            DependentJoinKind::Lateral {
                join_type,
                join_condition,
            } => self.flatten_lateral_join(
                binder,
                join_type,
                left,
                rewritten_right,
                visible_columns,
                join_condition,
            ),
        }
    }

    fn flatten_scalar_subquery(
        &mut self,
        left: LogicalPlan,
        right: LogicalPlan,
        right_visible_columns: Vec<usize>,
    ) -> Result<LogicalOperator> {
        let right_types = right.types();
        if right_types.is_empty() {
            return Err(paro_error::internal(
                "Scalar subquery must return at least one column",
            ));
        }
        if right_visible_columns.len() != 1 {
            return Err(paro_error::internal(format!(
                "Scalar subquery must expose exactly one visible column after pushdown, got {}",
                right_visible_columns.len()
            )));
        }
        let scalar_child_index = right_visible_columns[0];
        if scalar_child_index >= right_types.len() {
            return Err(paro_error::internal(format!(
                "Scalar subquery visible column {} out of range for rhs width {}",
                scalar_child_index,
                right_types.len()
            )));
        }

        let conditions = self.create_correlated_join_conditions()?;

        // SINGLE preserves every outer row and its bindings while enforcing the
        // scalar-subquery cardinality contract. The former LEFT + GROUP BY all
        // outer columns + FIRST emulation both collapsed duplicate outer rows
        // and replaced their bindings with aggregate-group bindings.
        let mut join = ComparisonJoin::new(JoinType::Single, left, right, conditions);
        join.duplicate_eliminated_columns = self.duplicate_eliminated_columns();
        join.right_projection_map = right_visible_columns.into();
        Ok(LogicalOperator::Join(Join::Comparison(join)))
    }

    fn flatten_lateral_join(
        &mut self,
        binder: &mut Binder,
        join_type: JoinType,
        mut left: LogicalPlan,
        mut right: LogicalPlan,
        right_visible_columns: Vec<usize>,
        join_condition: Option<Expression>,
    ) -> Result<LogicalOperator> {
        let left_bindings = collect_table_bindings(&left.operator);
        let right_bindings = collect_table_bindings(&right.operator);
        let mut conditions = self.create_correlated_join_conditions()?;
        let mut arbitrary_expressions = Vec::new();

        if let Some(join_condition) = join_condition {
            for expr in split_conjunction(join_condition) {
                if should_eliminate_join_condition(&expr) {
                    continue;
                }

                match get_expression_side(&expr, &left_bindings, &right_bindings) {
                    JoinSide::Left if can_push_to_left_child(join_type) => {
                        push_filter_to_child(binder, &mut left, expr);
                    }
                    JoinSide::Right if can_push_to_right_child(join_type) => {
                        push_filter_to_child(binder, &mut right, expr);
                    }
                    _ => extract_join_condition(
                        expr,
                        &left_bindings,
                        &right_bindings,
                        &mut conditions,
                        &mut arbitrary_expressions,
                    ),
                }
            }
        }

        let mut join = ComparisonJoin::new(join_type, left, right, conditions);
        join.duplicate_eliminated_columns = self.duplicate_eliminated_columns();
        join.right_projection_map = right_visible_columns.into();
        let plan = LogicalOperator::Join(Join::Comparison(join));

        if arbitrary_expressions.is_empty() {
            return Ok(plan);
        }

        if join_type != JoinType::Inner {
            return Err(paro_error::syntax(
                "Join condition for non-inner LATERAL JOIN must be a comparison between the left and right side",
            ));
        }

        Ok(LogicalOperator::Filter(Filter::new(
            binder.wrap_plan(plan),
            arbitrary_expressions,
        )))
    }

    fn flatten_exists_subquery(
        &mut self,
        binder: &mut Binder,
        left: LogicalPlan,
        right: LogicalPlan,
        mark_index: usize,
        is_not_exists: bool,
    ) -> Result<LogicalOperator> {
        let (right, base_binding) = self.compact_mark_subquery_right(binder, right, &[])?;
        let conditions = self.create_correlated_join_conditions_for_base(base_binding)?;

        let mut mark_join = ComparisonJoin::new(JoinType::Mark, left, right, conditions);
        mark_join.mark_index = Some(mark_index);
        mark_join.mark_semantics = MarkJoinSemantics::TwoValued;
        mark_join.duplicate_eliminated_columns = self.duplicate_eliminated_columns();

        let join_op = LogicalOperator::Join(Join::Comparison(mark_join));
        let _ = is_not_exists;
        Ok(join_op)
    }

    fn flatten_any_subquery(
        &mut self,
        binder: &mut Binder,
        left: LogicalPlan,
        right: LogicalPlan,
        comparison_type: ComparisonType,
        mark_index: usize,
        expression_children: &[Expression],
        child_types: &[LogicalType],
        child_targets: &[LogicalType],
    ) -> Result<LogicalOperator> {
        if expression_children.is_empty() {
            return Err(paro_error::internal(
                "Planner must provide expression_children for correlated ANY/ALL subqueries",
            ));
        }
        if expression_children.len() != child_types.len()
            || expression_children.len() != child_targets.len()
        {
            return Err(paro_error::internal(
                "Planner must provide aligned child_types/child_targets metadata for correlated ANY/ALL subqueries",
            ));
        }

        let right_types = right.types();
        if right_types.is_empty() {
            return Err(paro_error::internal(
                "ANY subquery must return at least one column",
            ));
        }
        if right_types.len() < expression_children.len() {
            return Err(paro_error::internal(format!(
                "ANY subquery must expose at least {} payload columns, got {}",
                expression_children.len(),
                right_types.len()
            )));
        }

        let payload_positions = (0..expression_children.len()).collect::<Vec<_>>();
        let (right, base_binding) =
            self.compact_mark_subquery_right(binder, right, &payload_positions)?;

        let mut conditions = self.create_correlated_join_conditions_for_base(base_binding)?;
        let payload_condition_start = conditions.len();
        conditions.extend(self.create_any_join_conditions(
            &right,
            comparison_type,
            expression_children,
            child_types,
            child_targets,
        )?);

        let mut mark_join = ComparisonJoin::new(JoinType::Mark, left, right, conditions);
        mark_join.mark_index = Some(mark_index);
        mark_join.mark_semantics = MarkJoinSemantics::ThreeValuedFrom(payload_condition_start);
        mark_join.duplicate_eliminated_columns = self.duplicate_eliminated_columns();

        Ok(LogicalOperator::Join(Join::Comparison(mark_join)))
    }

    fn flatten_all_subquery(
        &mut self,
        binder: &mut Binder,
        left: LogicalPlan,
        right: LogicalPlan,
        comparison_type: ComparisonType,
        mark_index: usize,
        expression_children: &[Expression],
        child_types: &[LogicalType],
        child_targets: &[LogicalType],
    ) -> Result<LogicalOperator> {
        let inverted_comparison = match comparison_type {
            ComparisonType::Equal => ComparisonType::NotEqual,
            ComparisonType::NotEqual => ComparisonType::Equal,
            ComparisonType::LessThan => ComparisonType::GreaterThanOrEqual,
            ComparisonType::GreaterThan => ComparisonType::LessThanOrEqual,
            ComparisonType::LessThanOrEqual => ComparisonType::GreaterThan,
            ComparisonType::GreaterThanOrEqual => ComparisonType::LessThan,
            ComparisonType::DistinctFrom => ComparisonType::NotDistinctFrom,
            ComparisonType::NotDistinctFrom => ComparisonType::DistinctFrom,
        };

        let any_result = self.flatten_any_subquery(
            binder,
            left,
            right,
            inverted_comparison,
            mark_index,
            expression_children,
            child_types,
            child_targets,
        )?;

        let _ = mark_index;
        Ok(any_result)
    }

    fn bind_correlations_to_left_output(
        left: &LogicalOperator,
        correlated_columns: &[CorrelatedColumnInfo],
    ) -> Result<Vec<CorrelatedColumnInfo>> {
        let output_bindings = left.get_column_bindings();
        correlated_columns
            .iter()
            .map(|correlated| {
                let original = ColumnBinding::new(
                    correlated.table_index,
                    correlated.column_index,
                );
                if output_bindings.contains(&original) {
                    return Ok(correlated.clone());
                }

                if let LogicalOperator::Aggregate(aggregate) = left {
                    for (group_idx, group) in aggregate.groups.iter().enumerate() {
                        let Expression::ColumnRef(group_column) = group else {
                            continue;
                        };
                        if group_column.depth == 0 && group_column.binding == original {
                            let mut rebound = correlated.clone();
                            rebound.table_index = aggregate.group_index;
                            rebound.column_index = group_idx;
                            rebound.return_type = group.return_type();
                            return Ok(rebound);
                        }
                    }
                }

                Err(paro_error::internal(format!(
                    "correlated column {original:?} is not exposed by the dependent join left input; output bindings: {output_bindings:?}"
                )))
            })
            .collect()
    }

    fn create_correlated_join_conditions(&self) -> Result<Vec<JoinCondition>> {
        let base_binding = self.correlated_base_binding.ok_or_else(|| {
            paro_error::internal(
                "PushDownDependentJoin must establish a base binding before join conditions are built",
            )
        })?;
        self.create_correlated_join_conditions_for_base(base_binding)
    }

    fn create_correlated_join_conditions_for_base(
        &self,
        base_binding: ColumnBinding,
    ) -> Result<Vec<JoinCondition>> {
        let mut conditions = Vec::new();

        for (idx, corr) in self.outer_correlated_columns.iter().enumerate() {
            let left_expr = Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(corr.table_index, corr.column_index),
                corr.return_type.clone(),
            ));

            let right_expr = Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(base_binding.table_index, base_binding.column_index + idx),
                corr.return_type.clone(),
            ));

            conditions.push(JoinCondition::new(
                left_expr,
                right_expr,
                JoinComparisonType::NotDistinctFrom,
            ));
        }

        Ok(conditions)
    }

    fn compact_mark_subquery_right(
        &self,
        binder: &mut Binder,
        right: LogicalPlan,
        payload_positions: &[usize],
    ) -> Result<(LogicalPlan, ColumnBinding)> {
        let base_binding = self.correlated_base_binding.ok_or_else(|| {
            paro_error::internal(
                "PushDownDependentJoin must establish a base binding before mark-subquery compaction",
            )
        })?;
        let mut projection_indices = payload_positions.to_vec();
        projection_indices.extend(self.correlation_key_positions(&right, base_binding)?);

        let right_output_names = right.output_names();
        let mut output_names = payload_positions
            .iter()
            .map(|&idx| {
                right_output_names
                    .get(idx)
                    .cloned()
                    .unwrap_or_else(|| format!("col_{}", idx + 1))
            })
            .collect::<Vec<_>>();
        output_names.extend(self.internal_output_names());

        let projection_index = self.next_table_index();
        let compacted = self.build_reference_projection(
            binder,
            projection_index,
            right,
            &projection_indices,
            output_names,
        )?;

        Ok((
            compacted,
            ColumnBinding::new(projection_index, payload_positions.len()),
        ))
    }

    fn allocate_delim_table_index(&mut self) -> usize {
        if !self.delim_scan_allocated {
            self.delim_scan_allocated = true;
            self.delim_table_index
        } else {
            self.next_table_index()
        }
    }

    fn make_delim_get(&mut self) -> (LogicalOperator, ColumnBinding) {
        let table_index = self.allocate_delim_table_index();
        let chunk_types = self
            .correlated_columns
            .iter()
            .map(|corr| corr.return_type.clone())
            .collect();
        (
            LogicalOperator::DelimGet(DelimGet::new(table_index, chunk_types)),
            ColumnBinding::new(table_index, 0),
        )
    }

    fn attach_delim_cross_product(
        &mut self,
        binder: &mut Binder,
        plan: LogicalPlan,
    ) -> PushDownResult {
        let original_visible_count = plan.get_column_bindings().len();
        let (delim_get, base_binding) = self.make_delim_get();
        let delim_plan = binder.wrap_plan(delim_get);
        let join_op = LogicalOperator::Join(Join::Cross(CrossProduct::new(plan, delim_plan)));
        PushDownResult {
            plan: binder.wrap_plan(join_op),
            base_binding,
            visible_columns: Self::all_columns_visible(original_visible_count),
        }
    }

    fn delim_column_refs(&self, base_binding: ColumnBinding) -> Vec<Expression> {
        self.correlated_columns
            .iter()
            .enumerate()
            .map(|(idx, corr)| {
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(base_binding.table_index, base_binding.column_index + idx),
                    corr.return_type.clone(),
                ))
            })
            .collect()
    }

    fn duplicate_eliminated_columns(&self) -> Vec<Expression> {
        self.outer_correlated_columns
            .iter()
            .map(|corr| {
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(corr.table_index, corr.column_index),
                    corr.return_type.clone(),
                ))
            })
            .collect()
    }

    fn rewrite_order_nodes(
        &self,
        orders: Vec<OrderByNode>,
        base_binding: ColumnBinding,
        correlated_map: &CorrelatedColumnMap,
        lateral_depth: usize,
    ) -> Vec<OrderByNode> {
        let rewriter = RewriteCorrelatedExpressions::new_recursive(
            base_binding,
            correlated_map.clone(),
            lateral_depth,
        );

        orders
            .into_iter()
            .map(|mut order_node| {
                order_node.expression = rewriter.rewrite_expression(order_node.expression);
                order_node
            })
            .collect()
    }

    fn all_columns_visible(column_count: usize) -> Vec<usize> {
        (0..column_count).collect()
    }

    fn shift_visible_columns(visible_columns: &[usize], offset: usize) -> Vec<usize> {
        visible_columns.iter().map(|idx| idx + offset).collect()
    }

    fn apply_projection_map_to_visible_columns(
        visible_columns: &[usize],
        projection_map: &crate::operator::ProjectionMap,
    ) -> Vec<usize> {
        let Some(indices) = projection_map.as_columns() else {
            return visible_columns.to_vec();
        };
        indices
            .iter()
            .enumerate()
            .filter_map(|(output_idx, input_idx)| {
                visible_columns.contains(input_idx).then_some(output_idx)
            })
            .collect()
    }

    /// Projection-bearing pass-through operators are planned before
    /// decorrelation appends its internal keys. Preserve the operator's exact
    /// user projection while explicitly carrying those keys to the dependent
    /// join above it.
    fn carry_correlation_keys(
        &self,
        child: &LogicalPlan,
        base_binding: ColumnBinding,
        projection_map: &mut crate::operator::ProjectionMap,
    ) -> Result<()> {
        for index in self.correlation_key_positions(child, base_binding)? {
            projection_map.include(index);
        }
        Ok(())
    }

    fn internal_output_names(&self) -> Vec<String> {
        (0..self.correlated_columns.len())
            .map(|idx| format!("__corr_{}", idx + 1))
            .collect()
    }

    fn correlation_key_positions(
        &self,
        plan: &LogicalPlan,
        base_binding: ColumnBinding,
    ) -> Result<Vec<usize>> {
        let bindings = plan.get_column_bindings();
        (0..self.correlated_columns.len())
            .map(|offset| {
                let target = ColumnBinding::new(
                    base_binding.table_index,
                    base_binding.column_index + offset,
                );
                bindings.iter().position(|binding| *binding == target).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Failed to locate correlation key binding {:?} in child output bindings {:?}",
                        target, bindings
                    ))
                })
            })
            .collect()
    }

    fn build_reference_projection(
        &self,
        binder: &mut Binder,
        table_index: usize,
        child: LogicalPlan,
        indices: &[usize],
        output_names: Vec<String>,
    ) -> Result<LogicalPlan> {
        let child_bindings = child.get_column_bindings();
        let child_types = child.types();

        let expressions = indices
            .iter()
            .map(|&idx| {
                let binding = *child_bindings.get(idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Projection index {} out of range for child with {} columns",
                        idx,
                        child_bindings.len()
                    ))
                })?;
                let logical_type = child_types.get(idx).cloned().ok_or_else(|| {
                    paro_error::internal(format!(
                        "Projection type index {} out of range for child with {} types",
                        idx,
                        child_types.len()
                    ))
                })?;
                Ok(Expression::ColumnRef(ColumnRefExpression::new(
                    binding,
                    logical_type,
                )))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(binder.wrap_plan(LogicalOperator::Projection(
            Projection::new(table_index, child, expressions).with_visible_names(output_names),
        )))
    }

    fn normalize_setop_child(
        &mut self,
        binder: &mut Binder,
        child: PushDownResult,
    ) -> Result<PushDownResult> {
        let visible_count = child.visible_columns.len();
        let child_output_names = child.plan.output_names();
        let mut projection_indices = child.visible_columns.clone();
        let mut output_names = child
            .visible_columns
            .iter()
            .map(|&idx| {
                child_output_names
                    .get(idx)
                    .cloned()
                    .unwrap_or_else(|| format!("col_{}", idx + 1))
            })
            .collect::<Vec<_>>();

        projection_indices.extend(self.correlation_key_positions(&child.plan, child.base_binding)?);
        output_names.extend(self.internal_output_names());

        let projection_index = self.next_table_index();
        let normalized_plan = self.build_reference_projection(
            binder,
            projection_index,
            child.plan,
            &projection_indices,
            output_names,
        )?;

        Ok(PushDownResult {
            plan: normalized_plan,
            base_binding: ColumnBinding::new(projection_index, visible_count),
            visible_columns: Self::all_columns_visible(visible_count),
        })
    }

    fn leaf_pushdown_result(
        &mut self,
        binder: &mut Binder,
        id: crate::plan::PlanNodeId,
        stats: crate::plan::NodeStats,
        operator: LogicalOperator,
        lateral_depth: usize,
    ) -> Result<PushDownResult> {
        if operator_has_correlated_columns_at_depth(
            &operator,
            &self.correlated_columns,
            lateral_depth,
        ) {
            return Err(paro_error::not_implemented(format!(
                "Correlated subquery pushdown does not support {:?} in this context",
                operator.op_type()
            )));
        }

        Ok(self.attach_delim_cross_product(
            binder,
            LogicalPlan {
                id,
                stats,
                operator,
            },
        ))
    }

    fn extract_constant_limit_value(
        expr: Option<&Expression>,
        label: &str,
    ) -> Result<Option<usize>> {
        let Some(expr) = expr else {
            return Ok(None);
        };

        match expr {
            Expression::Constant(constant) => {
                let value = constant.value.as_i64().ok_or_else(|| {
                    paro_error::not_implemented(format!(
                        "Non-integer {label} not supported in correlated subquery"
                    ))
                })?;
                if value < 0 {
                    return Err(paro_error::not_implemented(format!(
                        "Negative {label} not supported in correlated subquery"
                    )));
                }
                Ok(Some(value as usize))
            }
            _ => Err(paro_error::not_implemented(format!(
                "Non-constant {label} not supported in correlated subquery"
            ))),
        }
    }

    fn build_partitioned_limit(
        &mut self,
        binder: &mut Binder,
        child: LogicalPlan,
        base_binding: ColumnBinding,
        visible_columns: Vec<usize>,
        orders: Vec<OrderByNode>,
        limit: Option<usize>,
        offset: usize,
    ) -> Result<PushDownResult> {
        let child_column_count = child.get_column_bindings().len();

        let row_number_function = WindowFunction::row_number();
        let row_number = WindowExpression::native(
            row_number_function.clone(),
            vec![],
            self.delim_column_refs(base_binding),
            orders
                .into_iter()
                .map(|order| OrderByExpression {
                    expression: order.expression,
                    ascending: order.ascending,
                    nulls_first: order.nulls_first,
                })
                .collect(),
            WindowFrame::get_default_frame(&row_number_function),
            false,
        );

        let window_index = self.next_table_index();
        let window = LogicalOperator::Window(Window::new(window_index, vec![row_number], child));
        let window_plan = binder.wrap_plan(window);
        let row_number_binding = ColumnBinding::new(window_index, 0);
        let row_number_ref = || {
            Expression::ColumnRef(ColumnRefExpression::new(
                row_number_binding,
                LogicalType::BigInt,
            ))
        };

        let mut filters = Vec::new();
        if let Some(limit) = limit {
            let upper_bound = offset.saturating_add(limit);
            filters.push(Expression::Comparison(ComparisonExpression::new(
                ComparisonType::LessThanOrEqual,
                row_number_ref(),
                Expression::Constant(ConstantExpression {
                    value: Value::BigInt(upper_bound as i64),
                    return_type: LogicalType::BigInt,
                }),
            )));
        }
        if offset > 0 {
            filters.push(Expression::Comparison(ComparisonExpression::new(
                ComparisonType::GreaterThan,
                row_number_ref(),
                Expression::Constant(ConstantExpression {
                    value: Value::BigInt(offset as i64),
                    return_type: LogicalType::BigInt,
                }),
            )));
        }

        let predicate = if filters.len() == 1 {
            filters.pop().expect("single filter")
        } else {
            Expression::Conjunction(ConjunctionExpression::new(ConjunctionType::And, filters))
        };
        let mut filter = Filter::new(window_plan, vec![predicate]);
        filter.projection_map = Self::all_columns_visible(child_column_count).into();

        Ok(PushDownResult {
            plan: binder.wrap_plan(LogicalOperator::Filter(filter)),
            base_binding,
            visible_columns,
        })
    }

    fn push_down_dependent_join(
        &mut self,
        binder: &mut Binder,
        plan: LogicalPlan,
        lateral_depth: usize,
    ) -> Result<PushDownResult> {
        self.push_down_dependent_join_internal(binder, plan, lateral_depth)
    }
    fn push_down_dependent_join_internal(
        &mut self,
        binder: &mut Binder,
        plan: LogicalPlan,
        lateral_depth: usize,
    ) -> Result<PushDownResult> {
        let LogicalPlan {
            id,
            stats,
            operator,
        } = plan;
        let correlated_map = build_correlated_column_map(&self.correlated_columns);

        match operator {
            LogicalOperator::Filter(mut filter) => {
                let PushDownResult {
                    plan: child,
                    base_binding,
                    visible_columns,
                } = self.push_down_dependent_join_internal(binder, *filter.child, lateral_depth)?;
                let rewriter = RewriteCorrelatedExpressions::new_recursive(
                    base_binding,
                    correlated_map.clone(),
                    lateral_depth,
                );
                filter.expressions = filter
                    .expressions
                    .into_iter()
                    .map(|expr| rewriter.rewrite_expression(expr))
                    .collect();
                let projection_map = filter.projection_map.clone();
                let projected_visible_columns = Self::apply_projection_map_to_visible_columns(
                    &visible_columns,
                    &projection_map,
                );
                self.carry_correlation_keys(&child, base_binding, &mut filter.projection_map)?;
                filter.child = Box::new(child);
                Ok(PushDownResult {
                    plan: LogicalPlan {
                        id,
                        stats,
                        operator: LogicalOperator::Filter(filter),
                    },
                    base_binding,
                    visible_columns: projected_visible_columns,
                })
            }
            LogicalOperator::Projection(mut proj) => {
                let PushDownResult {
                    plan: child,
                    base_binding,
                    ..
                } = self.push_down_dependent_join_internal(binder, *proj.child, lateral_depth)?;
                let rewriter = RewriteCorrelatedExpressions::new_recursive(
                    base_binding,
                    correlated_map.clone(),
                    lateral_depth,
                );
                let original_projection_count = proj.expressions.len();
                proj.expressions = proj
                    .expressions
                    .into_iter()
                    .map(|expr| rewriter.rewrite_expression(expr))
                    .collect();
                let delim_exprs = self.delim_column_refs(base_binding);
                let delim_offset = proj.expressions.len();
                let projection_index = proj.table_index;
                proj.expressions.extend(delim_exprs);
                proj.returned_types = proj
                    .expressions
                    .iter()
                    .map(|expr| expr.return_type())
                    .collect();
                proj.visible_names.extend(self.internal_output_names());
                proj.child = Box::new(child);
                Ok(PushDownResult {
                    plan: LogicalPlan {
                        id,
                        stats,
                        operator: LogicalOperator::Projection(proj),
                    },
                    base_binding: ColumnBinding::new(projection_index, delim_offset),
                    visible_columns: Self::all_columns_visible(original_projection_count),
                })
            }
            LogicalOperator::RowFetch(mut fetch) => {
                let PushDownResult {
                    plan: child,
                    base_binding,
                    visible_columns,
                } = self.push_down_dependent_join_internal(binder, *fetch.child, lateral_depth)?;
                let rewriter = RewriteCorrelatedExpressions::new_recursive(
                    base_binding,
                    correlated_map.clone(),
                    lateral_depth,
                );
                for source in &mut fetch.sources {
                    source.rowid = rewriter.rewrite_expression(source.rowid.clone());
                }
                fetch.child = Box::new(child);
                Ok(PushDownResult {
                    plan: LogicalPlan {
                        id,
                        stats,
                        operator: LogicalOperator::RowFetch(fetch),
                    },
                    base_binding,
                    visible_columns,
                })
            }
            LogicalOperator::ExternalProject(mut project) => {
                let PushDownResult {
                    plan: child,
                    base_binding,
                    visible_columns,
                } =
                    self.push_down_dependent_join_internal(binder, *project.child, lateral_depth)?;
                let rewriter = RewriteCorrelatedExpressions::new_recursive(
                    base_binding,
                    correlated_map.clone(),
                    lateral_depth,
                );
                project.expressions = project
                    .expressions
                    .into_iter()
                    .map(|mut expr| {
                        expr.expression = rewriter.rewrite_expression(expr.expression);
                        expr
                    })
                    .collect();
                project.child = Box::new(child);
                Ok(PushDownResult {
                    plan: LogicalPlan {
                        id,
                        stats,
                        operator: LogicalOperator::ExternalProject(project),
                    },
                    base_binding,
                    visible_columns,
                })
            }
            LogicalOperator::ExternalTable(mut table) => {
                if let Some(child) = table.child.take() {
                    let original_output_count = table.returned_types.len();
                    let table_index = table.table_index;
                    let PushDownResult {
                        plan: child,
                        base_binding,
                        ..
                    } = self.push_down_dependent_join_internal(binder, *child, lateral_depth)?;
                    let rewriter = RewriteCorrelatedExpressions::new_recursive(
                        base_binding,
                        correlated_map.clone(),
                        lateral_depth,
                    );
                    table.call_expression = rewriter.rewrite_expression(table.call_expression);
                    table.child = Some(Box::new(child));
                    for (name, return_type) in self.internal_output_names().into_iter().zip(
                        self.correlated_columns
                            .iter()
                            .map(|corr| corr.return_type.clone()),
                    ) {
                        table.output_columns.push(name);
                        table.returned_types.push(return_type);
                    }
                    Ok(PushDownResult {
                        plan: LogicalPlan {
                            id,
                            stats,
                            operator: LogicalOperator::ExternalTable(table),
                        },
                        base_binding: ColumnBinding::new(table_index, original_output_count),
                        visible_columns: Self::all_columns_visible(original_output_count),
                    })
                } else {
                    Ok(PushDownResult {
                        plan: LogicalPlan {
                            id,
                            stats,
                            operator: LogicalOperator::ExternalTable(table),
                        },
                        base_binding: ColumnBinding::new(0, 0),
                        visible_columns: Self::all_columns_visible(0),
                    })
                }
            }
            LogicalOperator::Aggregate(mut agg) => {
                let PushDownResult {
                    plan: child,
                    base_binding,
                    ..
                } = self.push_down_dependent_join_internal(binder, *agg.child, lateral_depth)?;
                let rewriter = RewriteCorrelatedExpressions::new_recursive(
                    base_binding,
                    correlated_map.clone(),
                    lateral_depth,
                );
                let original_group_count = agg.groups.len();
                let original_aggregate_count = agg.aggregates.len();
                let original_grouping_function_count = agg.grouping_functions.len();
                agg.groups = agg
                    .groups
                    .into_iter()
                    .map(|expr| rewriter.rewrite_expression(expr))
                    .collect();
                agg.aggregates = agg
                    .aggregates
                    .into_iter()
                    .map(|expr| rewriter.rewrite_expression(expr))
                    .collect();

                let delim_offset = agg.groups.len();
                let group_index = agg.group_index;
                for delim_expr in self.delim_column_refs(base_binding) {
                    let expr_index = agg.groups.len();
                    agg.groups.push(delim_expr);
                    for grouping_set in &mut agg.grouping_sets {
                        grouping_set.expressions.push(expr_index);
                    }
                }
                agg.child = Box::new(child);
                agg.recompute_returned_types();
                let aggregate_visible_start = agg.groups.len();
                let mut visible_columns = Self::all_columns_visible(original_group_count);
                visible_columns.extend(
                    aggregate_visible_start
                        ..aggregate_visible_start
                            + original_aggregate_count
                            + original_grouping_function_count,
                );
                Ok(PushDownResult {
                    plan: LogicalPlan {
                        id,
                        stats,
                        operator: LogicalOperator::Aggregate(agg),
                    },
                    base_binding: ColumnBinding::new(group_index, delim_offset),
                    visible_columns,
                })
            }
            LogicalOperator::Order(mut order) => {
                let PushDownResult {
                    plan: child,
                    base_binding,
                    visible_columns,
                } = self.push_down_dependent_join_internal(binder, *order.child, lateral_depth)?;
                order.orders = self.rewrite_order_nodes(
                    order.orders,
                    base_binding,
                    &correlated_map,
                    lateral_depth,
                );
                let projected_visible_columns = Self::apply_projection_map_to_visible_columns(
                    &visible_columns,
                    &order.projection_map,
                );
                self.carry_correlation_keys(&child, base_binding, &mut order.projection_map)?;
                order.child = Box::new(child);
                Ok(PushDownResult {
                    plan: LogicalPlan {
                        id,
                        stats,
                        operator: LogicalOperator::Order(order),
                    },
                    base_binding,
                    visible_columns: projected_visible_columns,
                })
            }
            LogicalOperator::TopN(topn) => {
                let PushDownResult {
                    plan: child,
                    base_binding,
                    visible_columns,
                } = self.push_down_dependent_join_internal(binder, *topn.child, lateral_depth)?;
                let rewritten_orders = self.rewrite_order_nodes(
                    topn.orders,
                    base_binding,
                    &correlated_map,
                    lateral_depth,
                );
                let mut result = self.build_partitioned_limit(
                    binder,
                    child,
                    base_binding,
                    visible_columns,
                    rewritten_orders,
                    Some(topn.limit),
                    topn.offset,
                )?;
                result.plan.id = id;
                result.plan.stats = stats;
                Ok(result)
            }
            LogicalOperator::Limit(limit) => {
                let limit_value =
                    Self::extract_constant_limit_value(limit.limit.as_ref(), "limit")?;
                let offset_value =
                    Self::extract_constant_limit_value(limit.offset.as_ref(), "offset")?
                        .unwrap_or(0);

                let lim_child = *limit.child;
                let (child, base_binding, visible_columns, orders) = match lim_child.operator {
                    LogicalOperator::Order(order) => {
                        let PushDownResult {
                            plan: child,
                            base_binding,
                            visible_columns,
                        } = self.push_down_dependent_join_internal(
                            binder,
                            *order.child,
                            lateral_depth,
                        )?;
                        let orders = self.rewrite_order_nodes(
                            order.orders,
                            base_binding,
                            &correlated_map,
                            lateral_depth,
                        );
                        (child, base_binding, visible_columns, orders)
                    }
                    _ => {
                        let PushDownResult {
                            plan: child,
                            base_binding,
                            visible_columns,
                        } = self.push_down_dependent_join_internal(
                            binder,
                            lim_child,
                            lateral_depth,
                        )?;
                        (child, base_binding, visible_columns, vec![])
                    }
                };

                let mut result = self.build_partitioned_limit(
                    binder,
                    child,
                    base_binding,
                    visible_columns,
                    orders,
                    limit_value,
                    offset_value,
                )?;
                result.plan.id = id;
                result.plan.stats = stats;
                Ok(result)
            }
            LogicalOperator::Distinct(mut distinct) => {
                let PushDownResult {
                    plan: child,
                    base_binding,
                    visible_columns,
                } =
                    self.push_down_dependent_join_internal(binder, *distinct.child, lateral_depth)?;
                let rewriter = RewriteCorrelatedExpressions::new_recursive(
                    base_binding,
                    correlated_map.clone(),
                    lateral_depth,
                );
                distinct.distinct_targets = distinct
                    .distinct_targets
                    .into_iter()
                    .map(|expr| rewriter.rewrite_expression(expr))
                    .collect();
                if let Some(order_by) = distinct.order_by.take() {
                    distinct.order_by = Some(self.rewrite_order_nodes(
                        order_by,
                        base_binding,
                        &correlated_map,
                        lateral_depth,
                    ));
                }
                if distinct.distinct_type == DistinctType::DistinctOn {
                    distinct
                        .distinct_targets
                        .extend(self.delim_column_refs(base_binding));
                }
                distinct.child = Box::new(child);
                Ok(PushDownResult {
                    plan: LogicalPlan {
                        id,
                        stats,
                        operator: LogicalOperator::Distinct(distinct),
                    },
                    base_binding,
                    visible_columns,
                })
            }
            LogicalOperator::Window(mut window) => {
                let PushDownResult {
                    plan: child,
                    base_binding,
                    visible_columns: child_visible_columns,
                } = self.push_down_dependent_join_internal(binder, *window.child, lateral_depth)?;
                let rewriter = RewriteCorrelatedExpressions::new_recursive(
                    base_binding,
                    correlated_map.clone(),
                    lateral_depth,
                );
                for expr in &mut window.expressions {
                    ExpressionIterator::enumerate_window_children_mut(expr, |child| {
                        *child = rewriter.rewrite_expression(child.clone());
                    });
                    expr.partitions.extend(self.delim_column_refs(base_binding));
                }
                let child_output_len = child.types().len();
                let mut visible_columns = child_visible_columns;
                visible_columns
                    .extend(child_output_len..child_output_len + window.expressions.len());
                window.child = Box::new(child);
                Ok(PushDownResult {
                    plan: LogicalPlan {
                        id,
                        stats,
                        operator: LogicalOperator::Window(window),
                    },
                    base_binding,
                    visible_columns,
                })
            }
            LogicalOperator::SetOperation(mut setop) => {
                let original_visible_count = setop.column_count;
                let left =
                    self.push_down_dependent_join_internal(binder, *setop.left, lateral_depth)?;
                let right =
                    self.push_down_dependent_join_internal(binder, *setop.right, lateral_depth)?;
                let left = self.normalize_setop_child(binder, left)?;
                let right = self.normalize_setop_child(binder, right)?;
                if left.visible_columns.len() != original_visible_count
                    || right.visible_columns.len() != original_visible_count
                {
                    return Err(paro_error::internal(format!(
                        "SetOperation expected {} visible columns but got left={} right={}",
                        original_visible_count,
                        left.visible_columns.len(),
                        right.visible_columns.len()
                    )));
                }
                let output_types = left.plan.types();
                let table_index = setop.table_index;
                setop.left = Box::new(left.plan);
                setop.right = Box::new(right.plan);
                setop.column_count = output_types.len();
                setop.types = output_types;
                Ok(PushDownResult {
                    plan: LogicalPlan {
                        id,
                        stats,
                        operator: LogicalOperator::SetOperation(setop),
                    },
                    base_binding: ColumnBinding::new(table_index, original_visible_count),
                    visible_columns: Self::all_columns_visible(original_visible_count),
                })
            }
            LogicalOperator::Join(Join::Cross(mut cross)) => {
                let left_has = operator_has_correlated_columns_at_depth(
                    &cross.left.operator,
                    &self.correlated_columns,
                    lateral_depth,
                );
                let right_has = operator_has_correlated_columns_at_depth(
                    &cross.right.operator,
                    &self.correlated_columns,
                    lateral_depth,
                );

                if !right_has {
                    let PushDownResult {
                        plan: left,
                        base_binding,
                        visible_columns: left_visible_columns,
                    } =
                        self.push_down_dependent_join_internal(binder, *cross.left, lateral_depth)?;
                    let right_output_len = cross.right.types().len();
                    cross.left = Box::new(left);
                    let mut visible_columns = left_visible_columns;
                    visible_columns.extend(Self::shift_visible_columns(
                        &Self::all_columns_visible(right_output_len),
                        cross.left.types().len(),
                    ));
                    return Ok(PushDownResult {
                        plan: LogicalPlan {
                            id,
                            stats,
                            operator: LogicalOperator::Join(Join::Cross(cross)),
                        },
                        base_binding,
                        visible_columns,
                    });
                }

                if !left_has {
                    let left_output_len = cross.left.types().len();
                    let PushDownResult {
                        plan: right,
                        base_binding,
                        visible_columns: right_visible_columns,
                    } = self.push_down_dependent_join_internal(
                        binder,
                        *cross.right,
                        lateral_depth,
                    )?;
                    cross.right = Box::new(right);
                    let mut visible_columns = Self::all_columns_visible(left_output_len);
                    visible_columns.extend(Self::shift_visible_columns(
                        &right_visible_columns,
                        left_output_len,
                    ));
                    return Ok(PushDownResult {
                        plan: LogicalPlan {
                            id,
                            stats,
                            operator: LogicalOperator::Join(Join::Cross(cross)),
                        },
                        base_binding,
                        visible_columns,
                    });
                }

                let PushDownResult {
                    plan: left,
                    base_binding: left_binding,
                    visible_columns: left_visible_columns,
                } = self.push_down_dependent_join_internal(binder, *cross.left, lateral_depth)?;
                let left_output_len = left.types().len();
                let PushDownResult {
                    plan: right,
                    visible_columns: right_visible_columns,
                    ..
                } = self.push_down_dependent_join_internal(binder, *cross.right, lateral_depth)?;
                cross.left = Box::new(left);
                cross.right = Box::new(right);
                let mut visible_columns = left_visible_columns;
                visible_columns.extend(Self::shift_visible_columns(
                    &right_visible_columns,
                    left_output_len,
                ));
                Ok(PushDownResult {
                    plan: LogicalPlan {
                        id,
                        stats,
                        operator: LogicalOperator::Join(Join::Cross(cross)),
                    },
                    base_binding: left_binding,
                    visible_columns,
                })
            }
            LogicalOperator::Join(Join::Comparison(mut join)) => {
                let left_has = operator_has_correlated_columns_at_depth(
                    &join.left.operator,
                    &self.correlated_columns,
                    lateral_depth,
                );
                let right_has = operator_has_correlated_columns_at_depth(
                    &join.right.operator,
                    &self.correlated_columns,
                    lateral_depth,
                );
                let condition_has = join.conditions.iter().any(|cond| {
                    expression_has_correlated_columns_at_depth(
                        &cond.left,
                        &self.correlated_columns,
                        lateral_depth,
                    ) || expression_has_correlated_columns_at_depth(
                        &cond.right,
                        &self.correlated_columns,
                        lateral_depth,
                    )
                });
                if condition_has {
                    return Err(paro_error::not_implemented(
                        "Correlated comparison join conditions are outside this pushdown refactor's supported scope",
                    ));
                }

                if !right_has {
                    let PushDownResult {
                        plan: left,
                        base_binding,
                        visible_columns: left_visible_columns,
                    } =
                        self.push_down_dependent_join_internal(binder, *join.left, lateral_depth)?;
                    let right_output_len = join.right.types().len();
                    join.left = Box::new(left);
                    let mut visible_columns = left_visible_columns;
                    visible_columns.extend(Self::shift_visible_columns(
                        &Self::all_columns_visible(right_output_len),
                        join.left.types().len(),
                    ));
                    return Ok(PushDownResult {
                        plan: LogicalPlan {
                            id,
                            stats,
                            operator: LogicalOperator::Join(Join::Comparison(join)),
                        },
                        base_binding,
                        visible_columns,
                    });
                }

                if !left_has {
                    let left_output_len = join.left.types().len();
                    let PushDownResult {
                        plan: right,
                        base_binding,
                        visible_columns: right_visible_columns,
                    } =
                        self.push_down_dependent_join_internal(binder, *join.right, lateral_depth)?;
                    join.right = Box::new(right);
                    let mut visible_columns = Self::all_columns_visible(left_output_len);
                    visible_columns.extend(Self::shift_visible_columns(
                        &right_visible_columns,
                        left_output_len,
                    ));
                    return Ok(PushDownResult {
                        plan: LogicalPlan {
                            id,
                            stats,
                            operator: LogicalOperator::Join(Join::Comparison(join)),
                        },
                        base_binding,
                        visible_columns,
                    });
                }

                let PushDownResult {
                    plan: left,
                    base_binding: left_binding,
                    visible_columns: left_visible_columns,
                } = self.push_down_dependent_join_internal(binder, *join.left, lateral_depth)?;
                let left_output_len = left.types().len();
                let PushDownResult {
                    plan: right,
                    visible_columns: right_visible_columns,
                    ..
                } = self.push_down_dependent_join_internal(binder, *join.right, lateral_depth)?;
                join.left = Box::new(left);
                join.right = Box::new(right);
                let mut visible_columns = left_visible_columns;
                visible_columns.extend(Self::shift_visible_columns(
                    &right_visible_columns,
                    left_output_len,
                ));
                Ok(PushDownResult {
                    plan: LogicalPlan {
                        id,
                        stats,
                        operator: LogicalOperator::Join(Join::Comparison(join)),
                    },
                    base_binding: left_binding,
                    visible_columns,
                })
            }
            LogicalOperator::Join(Join::Any(mut join)) => {
                let left_has = operator_has_correlated_columns_at_depth(
                    &join.left.operator,
                    &self.correlated_columns,
                    lateral_depth,
                );
                let right_has = operator_has_correlated_columns_at_depth(
                    &join.right.operator,
                    &self.correlated_columns,
                    lateral_depth,
                );
                if expression_has_correlated_columns_at_depth(
                    &join.condition,
                    &self.correlated_columns,
                    lateral_depth,
                ) {
                    return Err(paro_error::not_implemented(
                        "Correlated arbitrary ANY join conditions are outside this pushdown refactor's supported scope",
                    ));
                }

                if !right_has {
                    let PushDownResult {
                        plan: left,
                        base_binding,
                        visible_columns: left_visible_columns,
                    } =
                        self.push_down_dependent_join_internal(binder, *join.left, lateral_depth)?;
                    let right_output_len = join.right.types().len();
                    join.left = Box::new(left);
                    let mut visible_columns = left_visible_columns;
                    visible_columns.extend(Self::shift_visible_columns(
                        &Self::all_columns_visible(right_output_len),
                        join.left.types().len(),
                    ));
                    return Ok(PushDownResult {
                        plan: LogicalPlan {
                            id,
                            stats,
                            operator: LogicalOperator::Join(Join::Any(join)),
                        },
                        base_binding,
                        visible_columns,
                    });
                }

                if !left_has {
                    let left_output_len = join.left.types().len();
                    let PushDownResult {
                        plan: right,
                        base_binding,
                        visible_columns: right_visible_columns,
                    } =
                        self.push_down_dependent_join_internal(binder, *join.right, lateral_depth)?;
                    join.right = Box::new(right);
                    let mut visible_columns = Self::all_columns_visible(left_output_len);
                    visible_columns.extend(Self::shift_visible_columns(
                        &right_visible_columns,
                        left_output_len,
                    ));
                    return Ok(PushDownResult {
                        plan: LogicalPlan {
                            id,
                            stats,
                            operator: LogicalOperator::Join(Join::Any(join)),
                        },
                        base_binding,
                        visible_columns,
                    });
                }

                let PushDownResult {
                    plan: left,
                    base_binding: left_binding,
                    visible_columns: left_visible_columns,
                } = self.push_down_dependent_join_internal(binder, *join.left, lateral_depth)?;
                let left_output_len = left.types().len();
                let PushDownResult {
                    plan: right,
                    visible_columns: right_visible_columns,
                    ..
                } = self.push_down_dependent_join_internal(binder, *join.right, lateral_depth)?;
                join.left = Box::new(left);
                join.right = Box::new(right);
                let mut visible_columns = left_visible_columns;
                visible_columns.extend(Self::shift_visible_columns(
                    &right_visible_columns,
                    left_output_len,
                ));
                Ok(PushDownResult {
                    plan: LogicalPlan {
                        id,
                        stats,
                        operator: LogicalOperator::Join(Join::Any(join)),
                    },
                    base_binding: left_binding,
                    visible_columns,
                })
            }
            leaf @ (LogicalOperator::Get(_)
            | LogicalOperator::ExpressionGet(_)
            | LogicalOperator::DelimGet(_)
            | LogicalOperator::SearchScan(_)
            | LogicalOperator::FullTextFilterScan(_)
            | LogicalOperator::CTERef(_)
            | LogicalOperator::TableFunctionGet(_)
            | LogicalOperator::GraphMatch(_)
            | LogicalOperator::GraphScan(_)
            | LogicalOperator::DummyScan) => {
                self.leaf_pushdown_result(binder, id, stats, leaf, lateral_depth)
            }
            LogicalOperator::EmptyResult(mut empty) => {
                let PushDownResult {
                    plan: child,
                    base_binding,
                    visible_columns,
                } = self.push_down_dependent_join_internal(binder, *empty.child, lateral_depth)?;
                empty.child = Box::new(child);
                Ok(PushDownResult {
                    plan: LogicalPlan {
                        id,
                        stats,
                        operator: LogicalOperator::EmptyResult(empty),
                    },
                    base_binding,
                    visible_columns,
                })
            }
            LogicalOperator::GraphExpand(mut expand) => {
                let PushDownResult {
                    plan: child,
                    base_binding,
                    visible_columns,
                } = self.push_down_dependent_join_internal(binder, *expand.child, lateral_depth)?;
                expand.child = Box::new(child);
                Ok(PushDownResult {
                    plan: LogicalPlan {
                        id,
                        stats,
                        operator: LogicalOperator::GraphExpand(expand),
                    },
                    base_binding,
                    visible_columns,
                })
            }
            other @ (LogicalOperator::Alter(_)
            | LogicalOperator::CreateTable(_)
            | LogicalOperator::CreateRoutine(_)
            | LogicalOperator::CreateSequence(_)
            | LogicalOperator::CreateSchema(_)
            | LogicalOperator::CreateIndex(_)
            | LogicalOperator::CreateView(_)
            | LogicalOperator::Drop(_)
            | LogicalOperator::CreatePropertyGraph(_)
            | LogicalOperator::DropPropertyGraph(_)
            | LogicalOperator::RefreshPropertyGraph(_)
            | LogicalOperator::Insert(_)
            | LogicalOperator::Delete(_)
            | LogicalOperator::Update(_)
            | LogicalOperator::DependentJoin(_)
            | LogicalOperator::Explain(_)
            | LogicalOperator::MaterializedCTE(_)
            | LogicalOperator::RecursiveCTE(_)
            | LogicalOperator::CopyTo(_)) => Err(paro_error::not_implemented(format!(
                "Correlated subquery pushdown does not support {:?} in this context",
                other.op_type()
            ))),
        }
    }

    fn create_any_join_conditions(
        &self,
        right: &LogicalPlan,
        comparison_type: ComparisonType,
        expression_children: &[Expression],
        child_types: &[LogicalType],
        child_targets: &[LogicalType],
    ) -> Result<Vec<JoinCondition>> {
        let right_bindings = right.get_column_bindings();
        if right_bindings.len() < expression_children.len() {
            return Err(paro_error::internal(format!(
                "ANY subquery produced {} columns but planner expected at least {}",
                right_bindings.len(),
                expression_children.len()
            )));
        }

        let comparison = match comparison_type {
            ComparisonType::Equal => JoinComparisonType::Equal,
            ComparisonType::NotEqual => JoinComparisonType::NotEqual,
            ComparisonType::LessThan => JoinComparisonType::LessThan,
            ComparisonType::LessThanOrEqual => JoinComparisonType::LessThanOrEqual,
            ComparisonType::GreaterThan => JoinComparisonType::GreaterThan,
            ComparisonType::GreaterThanOrEqual => JoinComparisonType::GreaterThanOrEqual,
            ComparisonType::NotDistinctFrom => JoinComparisonType::NotDistinctFrom,
            ComparisonType::DistinctFrom => JoinComparisonType::DistinctFrom,
        };

        let mut conditions = Vec::with_capacity(expression_children.len());
        for child_idx in 0..expression_children.len() {
            let right_expr = Expression::ColumnRef(ColumnRefExpression::new(
                right_bindings[child_idx],
                child_types[child_idx].clone(),
            ));
            let right_expr = CastExpression::add_cast_if_needed(
                right_expr,
                child_targets[child_idx].clone(),
                self.cast_functions.as_ref(),
            )?;

            conditions.push(JoinCondition::new(
                expression_children[child_idx].clone(),
                right_expr,
                comparison,
            ));
        }

        Ok(conditions)
    }
}
