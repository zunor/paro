// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::{self as paro_error, Result};
use paro_external::routine::bound::BoundRoutineCallMeta;
use paro_external::routine::spec::RowSemantics;
use paro_planner::binder::bind::from::join_utils::{collect_table_bindings, get_expression_side};
use paro_planner::binder::context::BindContext;
use paro_planner::binder::ir::OrderByNode;
use paro_planner::expression::{ColumnRefExpression, Expression, ExpressionIterator};
use paro_planner::operator::external_project::{ExternalCostEstimate, ExternalProjectExpression};
use paro_planner::operator::{
    Aggregate, AnyJoin, ComparisonJoin, Distinct, Filter, Join, JoinSide, LogicalExternalProject,
    LogicalOperator, Order, Projection, TopN, Update, Window,
};
use paro_planner::plan::LogicalPlan;

#[derive(Debug)]
pub struct ExternalRoutineLoweringResult {
    pub plan: LogicalPlan,
    pub changed: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ExternalRoutineLoweringPass;

impl ExternalRoutineLoweringPass {
    pub fn name(self) -> &'static str {
        "ExternalRoutineLoweringPass"
    }

    pub fn lower(
        plan: LogicalPlan,
        bind_context: &BindContext,
    ) -> Result<ExternalRoutineLoweringResult> {
        let mut lowerer = ExternalRoutineLowerer::new(bind_context);
        let plan = lowerer.lower_plan(plan)?;
        lowerer.ensure_no_unlowered_external_routines(&plan)?;
        Ok(ExternalRoutineLoweringResult {
            plan,
            changed: lowerer.changed,
        })
    }
}

#[derive(Debug)]
struct ExternalRoutineLowerer<'a> {
    bind_context: &'a BindContext,
    changed: bool,
}

#[derive(Debug, Default)]
struct ReadyExternalLayer {
    calls: Vec<Expression>,
    reused_calls: usize,
}

#[derive(Debug)]
struct LayerMapping {
    expression: Expression,
    binding: ColumnRefExpression,
}

impl LayerMapping {
    fn replacement(&self) -> Expression {
        Expression::ColumnRef(self.binding.clone())
    }
}

/// Replaces calls collected for one external layer while preserving occurrence identity.
///
/// Shareable calls may repeatedly use the first matching binding. Volatile or side-effecting calls
/// consume their mappings in traversal order, so structurally equal occurrences remain distinct.
struct LayerExpressionRewriter<'a> {
    mappings: &'a [LayerMapping],
    consumed: Vec<bool>,
}

impl<'a> LayerExpressionRewriter<'a> {
    fn new(mappings: &'a [LayerMapping]) -> Self {
        Self {
            mappings,
            consumed: vec![false; mappings.len()],
        }
    }

    fn rewrite(&mut self, mut expression: Expression) -> Expression {
        if let Some(index) = self.mapping_index(&expression) {
            return self.mappings[index].replacement();
        }

        ExpressionIterator::enumerate_children_mut(&mut expression, |child| {
            *child = self.rewrite(child.clone());
        });
        expression
    }

    fn mapping_index(&mut self, expression: &Expression) -> Option<usize> {
        let shareable = expression.evaluation_properties().can_share_evaluation();
        let index = self
            .mappings
            .iter()
            .enumerate()
            .position(|(index, mapping)| {
                (shareable || !self.consumed[index]) && expression.equals(&mapping.expression)
            })?;
        if !shareable {
            self.consumed[index] = true;
        }
        Some(index)
    }
}

impl<'a> ExternalRoutineLowerer<'a> {
    fn new(bind_context: &'a BindContext) -> Self {
        Self {
            bind_context,
            changed: false,
        }
    }

    fn lower_plan(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        plan.try_map_post_order(|plan| self.lower_current_plan(plan))
    }

    fn lower_current_plan(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let LogicalPlan {
            id,
            stats,
            operator,
        } = plan;

        let operator = match operator {
            LogicalOperator::Projection(projection) => {
                LogicalOperator::Projection(self.lower_projection(projection)?)
            }
            LogicalOperator::Filter(filter) => LogicalOperator::Filter(self.lower_filter(filter)?),
            LogicalOperator::Order(order) => LogicalOperator::Order(self.lower_order(order)?),
            LogicalOperator::TopN(topn) => LogicalOperator::TopN(self.lower_topn(topn)?),
            LogicalOperator::Aggregate(aggregate) => {
                LogicalOperator::Aggregate(self.lower_aggregate(aggregate)?)
            }
            LogicalOperator::Window(window) => LogicalOperator::Window(self.lower_window(window)?),
            LogicalOperator::Distinct(distinct) => {
                LogicalOperator::Distinct(self.lower_distinct(distinct)?)
            }
            LogicalOperator::Update(update) => LogicalOperator::Update(self.lower_update(update)?),
            LogicalOperator::Join(join) => self.lower_join(join)?,
            LogicalOperator::Limit(limit) => {
                self.ensure_limit_is_native(&limit)?;
                LogicalOperator::Limit(limit)
            }
            LogicalOperator::ExternalProject(project) => LogicalOperator::ExternalProject(project),
            LogicalOperator::ExternalTable(table) => LogicalOperator::ExternalTable(table),
            other => other,
        };

        Ok(LogicalPlan {
            id,
            stats,
            operator,
        })
    }

    fn lower_projection(&mut self, mut projection: Projection) -> Result<Projection> {
        let child =
            self.lower_external_in_expression_vec(*projection.child, &mut projection.expressions)?;
        projection.child = Box::new(child);
        projection.returned_types = projection
            .expressions
            .iter()
            .map(Expression::return_type)
            .collect();
        Ok(projection)
    }

    fn lower_filter(&mut self, mut filter: Filter) -> Result<Filter> {
        let mut child = *filter.child;
        let mut native_predicates = Vec::new();
        let mut residual_predicates = Vec::new();

        for predicate in filter.expressions {
            if predicate.contains_external_routine() {
                residual_predicates.push(predicate);
            } else {
                native_predicates.push(predicate);
            }
        }

        if native_predicates.is_empty() {
            child = self.lower_external_in_expression_vec(child, &mut residual_predicates)?;
            filter.expressions = residual_predicates;
            filter.child = Box::new(child);
            return Ok(filter);
        }

        if residual_predicates.is_empty() {
            filter.expressions = native_predicates;
            filter.child = Box::new(child);
            return Ok(filter);
        }

        let mut native_filter = Filter::new(child, native_predicates);
        native_filter.projection_map = filter.projection_map.clone();
        let native_child =
            LogicalPlan::new(self.bind_context, LogicalOperator::Filter(native_filter));

        let lowered_child =
            self.lower_external_in_expression_vec(native_child, &mut residual_predicates)?;
        filter.expressions = residual_predicates;
        filter.child = Box::new(lowered_child);
        Ok(filter)
    }

    fn lower_order(&mut self, mut order: Order) -> Result<Order> {
        let mut expressions = take_order_expressions(&order.orders);
        let child = self.lower_external_in_expression_vec(*order.child, &mut expressions)?;
        restore_order_expressions(&mut order.orders, expressions);
        order.child = Box::new(child);
        Ok(order)
    }

    fn lower_topn(&mut self, mut topn: TopN) -> Result<TopN> {
        let mut expressions = take_order_expressions(&topn.orders);
        let child = self.lower_external_in_expression_vec(*topn.child, &mut expressions)?;
        restore_order_expressions(&mut topn.orders, expressions);
        topn.child = Box::new(child);
        Ok(topn)
    }

    fn lower_aggregate(&mut self, mut aggregate: Aggregate) -> Result<Aggregate> {
        let group_count = aggregate.groups.len();
        let mut expressions = aggregate.groups;
        expressions.extend(aggregate.aggregates);
        let child = self.lower_external_in_expression_vec(*aggregate.child, &mut expressions)?;
        let aggregates = expressions.split_off(group_count);
        aggregate.groups = expressions;
        aggregate.aggregates = aggregates;
        aggregate.child = Box::new(child);
        aggregate.recompute_returned_types_after_row_preserving_relocation();
        Ok(aggregate)
    }

    fn lower_window(&mut self, mut window: Window) -> Result<Window> {
        let mut expressions = window
            .expressions
            .into_iter()
            .map(Expression::Window)
            .collect::<Vec<_>>();
        let child = self.lower_external_in_expression_vec(*window.child, &mut expressions)?;
        window.expressions = expressions
            .into_iter()
            .map(|expr| match expr {
                Expression::Window(window_expr) => window_expr,
                other => unreachable!("window lowering produced non-window expression: {other:?}"),
            })
            .collect();
        window.child = Box::new(child);
        Ok(window)
    }

    fn lower_distinct(&mut self, mut distinct: Distinct) -> Result<Distinct> {
        let target_count = distinct.distinct_targets.len();
        let order_count = distinct.order_by.as_ref().map_or(0, Vec::len);
        let mut expressions = distinct.distinct_targets;
        if let Some(order_by) = &distinct.order_by {
            expressions.extend(order_by.iter().map(|order| order.expression.clone()));
        }

        let child = self.lower_external_in_expression_vec(*distinct.child, &mut expressions)?;
        let remaining = if order_count == 0 {
            Vec::new()
        } else {
            expressions.split_off(target_count)
        };
        distinct.distinct_targets = expressions;
        if let Some(order_by) = &mut distinct.order_by {
            restore_order_expressions(order_by, remaining);
        }
        distinct.child = Box::new(child);
        Ok(distinct)
    }

    fn lower_update(&mut self, mut update: Update) -> Result<Update> {
        let child =
            self.lower_external_in_expression_vec(*update.child, &mut update.expressions)?;
        update.child = Box::new(child);
        Ok(update)
    }

    fn lower_join(&mut self, join: Join) -> Result<LogicalOperator> {
        let join = match join {
            Join::Comparison(comparison) => {
                Join::Comparison(self.lower_comparison_join(comparison)?)
            }
            Join::Any(any) => Join::Any(Box::new(self.lower_any_join(*any)?)),
            Join::Cross(cross) => Join::Cross(cross),
        };
        Ok(LogicalOperator::Join(join))
    }

    fn lower_comparison_join(&mut self, mut join: ComparisonJoin) -> Result<ComparisonJoin> {
        loop {
            let left_bindings = collect_table_bindings(&join.left.operator);
            let right_bindings = collect_table_bindings(&join.right.operator);
            let mut left_layer = ReadyExternalLayer::default();
            let mut right_layer = ReadyExternalLayer::default();

            for condition in &join.conditions {
                self.collect_join_ready_layer(
                    &condition.left,
                    &left_bindings,
                    &right_bindings,
                    &mut left_layer,
                    &mut right_layer,
                )?;
                self.collect_join_ready_layer(
                    &condition.right,
                    &left_bindings,
                    &right_bindings,
                    &mut left_layer,
                    &mut right_layer,
                )?;
            }

            if left_layer.calls.is_empty() && right_layer.calls.is_empty() {
                break;
            }

            if !left_layer.calls.is_empty() {
                let (child, mappings) = self.wrap_external_layer(
                    *join.left,
                    left_layer.calls,
                    left_layer.reused_calls,
                )?;
                join.left = Box::new(child);
                let mut rewriter = LayerExpressionRewriter::new(&mappings);
                for condition in &mut join.conditions {
                    condition.left = rewriter.rewrite(condition.left.clone());
                    condition.right = rewriter.rewrite(condition.right.clone());
                }
            }

            if !right_layer.calls.is_empty() {
                let (child, mappings) = self.wrap_external_layer(
                    *join.right,
                    right_layer.calls,
                    right_layer.reused_calls,
                )?;
                join.right = Box::new(child);
                let mut rewriter = LayerExpressionRewriter::new(&mappings);
                for condition in &mut join.conditions {
                    condition.left = rewriter.rewrite(condition.left.clone());
                    condition.right = rewriter.rewrite(condition.right.clone());
                }
            }
        }

        Ok(join)
    }

    fn lower_any_join(&mut self, mut join: AnyJoin) -> Result<AnyJoin> {
        loop {
            let left_bindings = collect_table_bindings(&join.left.operator);
            let right_bindings = collect_table_bindings(&join.right.operator);
            let mut left_layer = ReadyExternalLayer::default();
            let mut right_layer = ReadyExternalLayer::default();
            self.collect_join_ready_layer(
                &join.condition,
                &left_bindings,
                &right_bindings,
                &mut left_layer,
                &mut right_layer,
            )?;

            if left_layer.calls.is_empty() && right_layer.calls.is_empty() {
                break;
            }

            if !left_layer.calls.is_empty() {
                let (child, mappings) = self.wrap_external_layer(
                    *join.left,
                    left_layer.calls,
                    left_layer.reused_calls,
                )?;
                join.left = Box::new(child);
                join.condition = LayerExpressionRewriter::new(&mappings).rewrite(join.condition);
            }

            if !right_layer.calls.is_empty() {
                let (child, mappings) = self.wrap_external_layer(
                    *join.right,
                    right_layer.calls,
                    right_layer.reused_calls,
                )?;
                join.right = Box::new(child);
                join.condition = LayerExpressionRewriter::new(&mappings).rewrite(join.condition);
            }
        }

        Ok(join)
    }

    fn ensure_limit_is_native(&self, limit: &paro_planner::operator::Limit) -> Result<()> {
        if limit
            .limit
            .as_ref()
            .is_some_and(Expression::contains_external_routine)
            || limit
                .offset
                .as_ref()
                .is_some_and(Expression::contains_external_routine)
        {
            return Err(paro_error::not_implemented(
                "External routines in LIMIT/OFFSET expressions are not supported by late lowering",
            ));
        }
        Ok(())
    }

    fn lower_external_in_expression_vec(
        &mut self,
        mut child: LogicalPlan,
        expressions: &mut Vec<Expression>,
    ) -> Result<LogicalPlan> {
        loop {
            let layer = self.collect_ready_layer(expressions.iter())?;
            if layer.calls.is_empty() {
                return Ok(child);
            }

            let (wrapped_child, mappings) =
                self.wrap_external_layer(child, layer.calls, layer.reused_calls)?;
            let mut rewriter = LayerExpressionRewriter::new(&mappings);
            for expr in expressions.iter_mut() {
                *expr = rewriter.rewrite(expr.clone());
            }
            child = wrapped_child;
        }
    }

    fn collect_ready_layer<'b>(
        &self,
        expressions: impl IntoIterator<Item = &'b Expression>,
    ) -> Result<ReadyExternalLayer> {
        let mut layer = ReadyExternalLayer::default();
        for expression in expressions {
            self.collect_ready_layer_from_expression(expression, &mut layer)?;
        }
        Ok(layer)
    }

    fn collect_ready_layer_from_expression(
        &self,
        expression: &Expression,
        layer: &mut ReadyExternalLayer,
    ) -> Result<()> {
        match expression {
            Expression::Function(function) if function.crosses_execution_boundary() => {
                let Some(meta) = function.routine_meta() else {
                    return Err(paro_error::internal(
                        "external routine is missing bound routine metadata",
                    ));
                };

                match meta.boundary.row_semantics {
                    RowSemantics::RowPreserving => {
                        if function
                            .children
                            .iter()
                            .any(Expression::contains_external_routine)
                        {
                            for child in &function.children {
                                self.collect_ready_layer_from_expression(child, layer)?;
                            }
                        } else {
                            push_unique_expression(
                                &mut layer.calls,
                                expression,
                                &mut layer.reused_calls,
                            );
                        }
                    }
                    RowSemantics::RelationExpanding => {
                        return Err(paro_error::not_implemented(
                            "Relation-expanding external routines must lower through LogicalExternalTable",
                        ));
                    }
                    RowSemantics::Aggregate => {
                        return Err(paro_error::not_implemented(
                            "Aggregate external routines must lower through a dedicated ExternalAggregate path",
                        ));
                    }
                    RowSemantics::Window => {
                        return Err(paro_error::not_implemented(
                            "Window external routines must lower through a dedicated ExternalWindow path",
                        ));
                    }
                }
            }
            _ => {
                let mut error = None;
                ExpressionIterator::enumerate_children(expression, |child| {
                    if error.is_none() {
                        error = self.collect_ready_layer_from_expression(child, layer).err();
                    }
                });
                if let Some(error) = error {
                    return Err(error);
                }
            }
        }

        Ok(())
    }

    fn collect_join_ready_layer(
        &self,
        expression: &Expression,
        left_bindings: &std::collections::HashSet<usize>,
        right_bindings: &std::collections::HashSet<usize>,
        left_layer: &mut ReadyExternalLayer,
        right_layer: &mut ReadyExternalLayer,
    ) -> Result<()> {
        match expression {
            Expression::Function(function) if function.crosses_execution_boundary() => {
                let Some(meta) = function.routine_meta() else {
                    return Err(paro_error::internal(
                        "external routine is missing bound routine metadata",
                    ));
                };

                match meta.boundary.row_semantics {
                    RowSemantics::RowPreserving => {
                        if function
                            .children
                            .iter()
                            .any(Expression::contains_external_routine)
                        {
                            for child in &function.children {
                                self.collect_join_ready_layer(
                                    child,
                                    left_bindings,
                                    right_bindings,
                                    left_layer,
                                    right_layer,
                                )?;
                            }
                        } else {
                            match get_expression_side(expression, left_bindings, right_bindings) {
                                JoinSide::Left | JoinSide::None => {
                                    push_unique_expression(
                                        &mut left_layer.calls,
                                        expression,
                                        &mut left_layer.reused_calls,
                                    );
                                }
                                JoinSide::Right => {
                                    push_unique_expression(
                                        &mut right_layer.calls,
                                        expression,
                                        &mut right_layer.reused_calls,
                                    );
                                }
                                JoinSide::Both => {
                                    return Err(paro_error::not_implemented(
                                        "External routines in JOIN conditions cannot depend on both sides of the join",
                                    ));
                                }
                            }
                        }
                    }
                    RowSemantics::RelationExpanding => {
                        return Err(paro_error::not_implemented(
                            "Relation-expanding external routines are not supported in JOIN predicates",
                        ));
                    }
                    RowSemantics::Aggregate => {
                        return Err(paro_error::not_implemented(
                            "Aggregate external routines must lower through a dedicated ExternalAggregate path",
                        ));
                    }
                    RowSemantics::Window => {
                        return Err(paro_error::not_implemented(
                            "Window external routines must lower through a dedicated ExternalWindow path",
                        ));
                    }
                }
            }
            _ => {
                let mut error = None;
                ExpressionIterator::enumerate_children(expression, |child| {
                    if error.is_none() {
                        error = self
                            .collect_join_ready_layer(
                                child,
                                left_bindings,
                                right_bindings,
                                left_layer,
                                right_layer,
                            )
                            .err();
                    }
                });
                if let Some(error) = error {
                    return Err(error);
                }
            }
        }

        Ok(())
    }

    fn wrap_external_layer(
        &mut self,
        child: LogicalPlan,
        calls: Vec<Expression>,
        reused_calls: usize,
    ) -> Result<(LogicalPlan, Vec<LayerMapping>)> {
        let project_index = self.bind_context.generate_table_index();
        let child_column_count = child.get_column_bindings().len();
        let expressions = calls
            .iter()
            .enumerate()
            .map(|(idx, expression)| {
                let routine_meta = external_routine_meta(expression)?.clone();
                Ok(ExternalProjectExpression {
                    output_name: format!("__ext_{project_index}_{idx}"),
                    expression: expression.clone(),
                    routine_meta,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let mappings = calls
            .into_iter()
            .enumerate()
            .map(|(idx, expression)| LayerMapping {
                binding: ColumnRefExpression::new(
                    paro_planner::operator::ColumnBinding::new(
                        project_index,
                        child_column_count + idx,
                    ),
                    expression.return_type(),
                ),
                expression,
            })
            .collect::<Vec<_>>();

        let cost = estimate_external_project_cost(&expressions, reused_calls);
        let project =
            LogicalExternalProject::new(project_index, child, expressions).with_cost(cost);
        self.changed = true;

        Ok((
            LogicalPlan::new(self.bind_context, LogicalOperator::ExternalProject(project)),
            mappings,
        ))
    }

    fn ensure_no_unlowered_external_routines(&self, plan: &LogicalPlan) -> Result<()> {
        plan.try_visit_pre_order(|plan| self.ensure_operator_is_lowered(&plan.operator))
    }

    fn ensure_operator_is_lowered(&self, operator: &LogicalOperator) -> Result<()> {
        match operator {
            LogicalOperator::ExternalProject(_) | LogicalOperator::ExternalTable(_) => Ok(()),
            LogicalOperator::Filter(filter) => {
                self.ensure_expressions_are_native("FILTER", filter.expressions.iter())
            }
            LogicalOperator::Projection(projection) => {
                self.ensure_expressions_are_native("PROJECTION", projection.expressions.iter())
            }
            LogicalOperator::Order(order) => self.ensure_expressions_are_native(
                "ORDER",
                order.orders.iter().map(|order| &order.expression),
            ),
            LogicalOperator::TopN(topn) => self.ensure_expressions_are_native(
                "TOPN",
                topn.orders.iter().map(|order| &order.expression),
            ),
            LogicalOperator::Aggregate(aggregate) => self.ensure_expressions_are_native(
                "AGGREGATE",
                aggregate.groups.iter().chain(aggregate.aggregates.iter()),
            ),
            LogicalOperator::Distinct(distinct) => self.ensure_expressions_are_native(
                "DISTINCT",
                distinct.distinct_targets.iter().chain(
                    distinct
                        .order_by
                        .iter()
                        .flat_map(|orders| orders.iter().map(|order| &order.expression)),
                ),
            ),
            LogicalOperator::Window(window) => {
                let expressions = window
                    .expressions
                    .iter()
                    .cloned()
                    .map(Expression::Window)
                    .collect::<Vec<_>>();
                self.ensure_expressions_are_native("WINDOW", expressions.iter())
            }
            LogicalOperator::Update(update) => {
                self.ensure_expressions_are_native("UPDATE", update.expressions.iter())
            }
            LogicalOperator::Join(join) => match join {
                Join::Comparison(comparison) => self.ensure_expressions_are_native(
                    "JOIN",
                    comparison
                        .conditions
                        .iter()
                        .flat_map(|condition| [&condition.left, &condition.right]),
                ),
                Join::Any(any) => self.ensure_expressions_are_native("JOIN", [&any.condition]),
                Join::Cross(_) => Ok(()),
            },
            LogicalOperator::Limit(limit) => self.ensure_expressions_are_native(
                "LIMIT",
                limit.limit.iter().chain(limit.offset.iter()),
            ),
            _ => Ok(()),
        }
    }

    fn ensure_expressions_are_native<'b>(
        &self,
        operator_name: &str,
        expressions: impl IntoIterator<Item = &'b Expression>,
    ) -> Result<()> {
        for expression in expressions {
            if expression.contains_external_routine() {
                return Err(paro_error::not_implemented(format!(
                    "External routine remained in {operator_name} after late lowering",
                )));
            }
        }
        Ok(())
    }
}

fn external_routine_meta(expression: &Expression) -> Result<&BoundRoutineCallMeta> {
    match expression {
        Expression::Function(function) if function.crosses_execution_boundary() => function
            .routine_meta()
            .ok_or_else(|| paro_error::internal("external routine is missing bound metadata")),
        _ => Err(paro_error::internal(
            "expected external function expression while building LogicalExternalProject",
        )),
    }
}

fn push_unique_expression(
    target: &mut Vec<Expression>,
    expression: &Expression,
    reused_calls: &mut usize,
) {
    if expression.evaluation_properties().can_share_evaluation()
        && target.iter().any(|existing| existing.equals(expression))
    {
        *reused_calls += 1;
    } else {
        target.push(expression.clone());
    }
}

fn estimate_external_project_cost(
    expressions: &[ExternalProjectExpression],
    reused_calls: usize,
) -> ExternalCostEstimate {
    let startup_cost = expressions
        .iter()
        .map(|expr| {
            if expr.routine_meta.boundary.may_block {
                4.0
            } else {
                1.0
            }
        })
        .sum::<f64>();
    let per_row_cost = expressions.len() as f64;
    let bytes_cost = expressions
        .iter()
        .map(|expr| estimate_type_width(expr.expression.return_type()) as f64)
        .sum::<f64>();
    let queue_risk = expressions
        .iter()
        .map(|expr| {
            if expr.routine_meta.boundary.may_block {
                1.0
            } else {
                0.2
            }
        })
        .sum::<f64>()
        + (reused_calls as f64 * 0.1);

    ExternalCostEstimate {
        startup_cost,
        per_row_cost,
        bytes_cost,
        queue_risk,
    }
}

fn estimate_type_width(ty: paro_common::types::LogicalType) -> usize {
    use paro_common::types::LogicalType;

    match ty {
        LogicalType::Boolean | LogicalType::TinyInt | LogicalType::UTinyInt => 1,
        LogicalType::SmallInt | LogicalType::USmallInt => 2,
        LogicalType::Integer | LogicalType::UInteger | LogicalType::Float => 4,
        LogicalType::BigInt
        | LogicalType::UBigInt
        | LogicalType::Double
        | LogicalType::Timestamp
        | LogicalType::Date
        | LogicalType::Time
        | LogicalType::Interval => 8,
        LogicalType::HugeInt
        | LogicalType::UHugeInt
        | LogicalType::Decimal {
            precision: _,
            scale: _,
        } => 16,
        _ => 32,
    }
}

fn take_order_expressions(orders: &[OrderByNode]) -> Vec<Expression> {
    orders
        .iter()
        .map(|order| order.expression.clone())
        .collect()
}

fn restore_order_expressions(orders: &mut [OrderByNode], expressions: Vec<Expression>) {
    for (order, expression) in orders.iter_mut().zip(expressions) {
        order.expression = expression;
    }
}

#[cfg(test)]
mod tests {
    use paro_common::chunk::Chunk;
    use paro_common::error::Result;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use paro_external::routine::bound::BoundRoutineCallMeta;
    use paro_external::routine::boundary::{ExecutionBoundary, PlacementClass};
    use paro_external::routine::identity::RoutineCallIdentity;
    use paro_external::routine::spec::{
        RoutineId, RoutineNullPolicy, RoutineSemantics, RoutineSideEffects, RoutineStability,
        RowSemantics,
    };
    use paro_function::scalar::{
        ExpressionState, FunctionSideEffects, FunctionStability, ScalarFunction,
    };
    use paro_planner::binder::context::BindContext;
    use paro_planner::binder::ir::OrderByNode;
    use paro_planner::expression::{
        ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression, Expression,
    };
    use paro_planner::operator::{
        Aggregate, ComparisonJoin, ExpressionGet, Filter, GroupInputMultiplicity, Join,
        JoinCondition, JoinType, LogicalOperator, Order, Projection, SingletonGroupProof,
    };
    use paro_planner::plan::LogicalPlan;

    use super::{ExternalRoutineLoweringPass, ExternalRoutineLoweringResult};

    fn noop_scalar_execute(
        _input: &Chunk,
        _state: &dyn ExpressionState,
        _result: &mut Vector,
    ) -> Result<()> {
        Ok(())
    }

    fn int_column(table_index: usize, column_index: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(
            paro_planner::operator::ColumnBinding::new(table_index, column_index),
            LogicalType::Integer,
        ))
    }

    fn bool_constant(value: bool) -> Expression {
        Expression::Constant(ConstantExpression::new(
            Value::Boolean(value),
            LogicalType::Boolean,
        ))
    }

    fn native_binary(
        name: &str,
        left: Expression,
        right: Expression,
        return_type: LogicalType,
    ) -> Expression {
        let function = ScalarFunction::new(
            name.to_string(),
            vec![left.return_type(), right.return_type()],
            return_type.clone(),
            noop_scalar_execute,
        );
        Expression::Function(paro_planner::expression::FunctionExpression::new(
            function,
            vec![left, right],
            return_type,
        ))
    }

    fn external_call(
        name: &str,
        arguments: Vec<Expression>,
        return_type: LogicalType,
    ) -> Expression {
        let function = ScalarFunction::new(
            name.to_string(),
            arguments.iter().map(Expression::return_type).collect(),
            return_type.clone(),
            noop_scalar_execute,
        );
        let semantics = RoutineSemantics {
            stability: RoutineStability::Stable,
            null_policy: RoutineNullPolicy::CalledOnNullInput,
            side_effects: RoutineSideEffects::None,
            row_semantics: RowSemantics::RowPreserving,
            may_block: true,
        };
        let meta = BoundRoutineCallMeta {
            identity: RoutineCallIdentity::Catalog {
                routine_id: RoutineId::from_raw(42),
                generation: 7,
            },
            semantics: semantics.clone(),
            boundary: ExecutionBoundary {
                placement: PlacementClass::External,
                may_block: true,
                row_semantics: RowSemantics::RowPreserving,
            },
            spec: None,
        };

        Expression::Function(
            paro_planner::expression::FunctionExpression::new(function, arguments, return_type)
                .with_routine_meta(meta),
        )
    }

    fn volatile_external_call(
        name: &str,
        arguments: Vec<Expression>,
        return_type: LogicalType,
    ) -> Expression {
        let mut expression = external_call(name, arguments, return_type);
        let Expression::Function(function) = &mut expression else {
            unreachable!();
        };
        function.function.stability = FunctionStability::Volatile;
        function.function.side_effects = FunctionSideEffects::HasSideEffects;
        let semantics = &mut function
            .routine_meta
            .as_mut()
            .expect("external routine metadata")
            .semantics;
        semantics.stability = RoutineStability::Volatile;
        semantics.side_effects = RoutineSideEffects::HasSideEffects;
        expression
    }

    fn expression_get(bind_context: &BindContext, table_index: usize) -> LogicalPlan {
        LogicalPlan::new(
            bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                table_index,
                vec![vec![Expression::Constant(ConstantExpression::new(
                    Value::Integer(1),
                    LogicalType::Integer,
                ))]],
                vec!["v".to_string()],
                vec![LogicalType::Integer],
            )),
        )
    }

    fn lower(plan: LogicalPlan, bind_context: &BindContext) -> ExternalRoutineLoweringResult {
        ExternalRoutineLoweringPass::lower(plan, bind_context)
            .expect("late lowering should succeed")
    }

    #[test]
    fn lowers_projection_with_external_cse() {
        let bind_context = BindContext::new();
        let child = expression_get(&bind_context, 1);
        let external = external_call("py_norm", vec![int_column(1, 0)], LogicalType::Integer);
        let projection = Projection::new(
            2,
            child,
            vec![
                external.clone(),
                native_binary("+", external.clone(), external, LogicalType::Integer),
            ],
        );
        let plan = LogicalPlan::new(&bind_context, LogicalOperator::Projection(projection));

        let lowered = lower(plan, &bind_context);
        assert!(lowered.changed);

        let LogicalOperator::Projection(projection) = lowered.plan.operator else {
            panic!("expected projection");
        };
        let LogicalOperator::ExternalProject(project) = projection.child.operator else {
            panic!("expected LogicalExternalProject under projection");
        };

        assert_eq!(project.expressions.len(), 1);
        match &projection.expressions[0] {
            Expression::ColumnRef(column_ref) => {
                assert_eq!(column_ref.binding.table_index, project.project_index);
            }
            other => panic!("expected temp ref, got {other:?}"),
        }
    }

    #[test]
    fn aggregate_lowering_preserves_singleton_group_proof() {
        let bind_context = BindContext::new();
        let child = expression_get(&bind_context, 1);
        let mut aggregate = Aggregate::new(
            2,
            3,
            4,
            child,
            vec![external_call(
                "py_group",
                vec![int_column(1, 0)],
                LogicalType::Integer,
            )],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        aggregate.group_input_multiplicity =
            GroupInputMultiplicity::AtMostOne(SingletonGroupProof::new([
                paro_planner::operator::ColumnBinding::new(1, 0),
            ]));
        let plan = LogicalPlan::new(&bind_context, LogicalOperator::Aggregate(aggregate));

        let lowered = lower(plan, &bind_context);
        assert!(lowered.changed);
        let LogicalOperator::Aggregate(aggregate) = lowered.plan.operator else {
            panic!("expected aggregate");
        };
        assert!(matches!(
            aggregate.group_input_multiplicity,
            GroupInputMultiplicity::AtMostOne(_)
        ));
    }

    #[test]
    fn lowers_identical_volatile_external_calls_independently() {
        let bind_context = BindContext::new();
        let child = expression_get(&bind_context, 1);
        let external =
            volatile_external_call("py_next", vec![int_column(1, 0)], LogicalType::Integer);
        let projection = Projection::new(2, child, vec![external.clone(), external]);
        let plan = LogicalPlan::new(&bind_context, LogicalOperator::Projection(projection));

        let lowered = lower(plan, &bind_context);
        let LogicalOperator::Projection(projection) = lowered.plan.operator else {
            panic!("expected projection");
        };
        let LogicalOperator::ExternalProject(project) = projection.child.operator else {
            panic!("expected external project");
        };

        assert_eq!(project.expressions.len(), 2);
        let [Expression::ColumnRef(first), Expression::ColumnRef(second)] =
            projection.expressions.as_slice()
        else {
            panic!("expected distinct external result references");
        };
        assert_ne!(first.binding, second.binding);
    }

    #[test]
    fn partitions_filter_predicates_around_external_project() {
        let bind_context = BindContext::new();
        let child = expression_get(&bind_context, 1);
        let native_predicate = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::GreaterThan,
            int_column(1, 0),
            Expression::Constant(ConstantExpression::new(
                Value::Integer(0),
                LogicalType::Integer,
            )),
        ));
        let external_predicate = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            external_call("py_check", vec![int_column(1, 0)], LogicalType::Boolean),
            bool_constant(true),
        ));
        let filter = Filter::new(child, vec![native_predicate, external_predicate]);
        let plan = LogicalPlan::new(&bind_context, LogicalOperator::Filter(filter));

        let lowered = lower(plan, &bind_context);
        let LogicalOperator::Filter(outer) = lowered.plan.operator else {
            panic!("expected outer filter");
        };
        let LogicalOperator::ExternalProject(project) = outer.child.operator else {
            panic!("expected external project between filters");
        };
        let LogicalOperator::Filter(inner) = project.child.operator else {
            panic!("expected native filter below external project");
        };

        assert_eq!(inner.expressions.len(), 1);
        assert_eq!(outer.expressions.len(), 1);
        assert!(matches!(
            project.expressions[0].expression,
            Expression::Function(_)
        ));
    }

    #[test]
    fn lowers_order_keys_through_external_project() {
        let bind_context = BindContext::new();
        let child = expression_get(&bind_context, 1);
        let order = Order::new(
            child,
            vec![OrderByNode {
                expression: external_call(
                    "py_sort_key",
                    vec![int_column(1, 0)],
                    LogicalType::Integer,
                ),
                ascending: true,
                nulls_first: false,
            }],
        );
        let plan = LogicalPlan::new(&bind_context, LogicalOperator::Order(order));

        let lowered = lower(plan, &bind_context);
        let LogicalOperator::Order(order) = lowered.plan.operator else {
            panic!("expected order");
        };
        let LogicalOperator::ExternalProject(project) = order.child.operator else {
            panic!("expected external project below order");
        };
        assert_eq!(project.expressions.len(), 1);
        assert!(matches!(
            order.orders[0].expression,
            Expression::ColumnRef(_)
        ));
    }

    #[test]
    fn lowers_join_side_local_external_calls() {
        let bind_context = BindContext::new();
        let left = expression_get(&bind_context, 1);
        let right = expression_get(&bind_context, 2);
        let join = ComparisonJoin::new(
            JoinType::Inner,
            left,
            right,
            vec![JoinCondition::equality(
                external_call("py_join_key", vec![int_column(1, 0)], LogicalType::Integer),
                int_column(2, 0),
            )],
        );
        let plan = LogicalPlan::new(&bind_context, LogicalOperator::Join(Join::Comparison(join)));

        let lowered = lower(plan, &bind_context);
        let LogicalOperator::Join(Join::Comparison(join)) = lowered.plan.operator else {
            panic!("expected comparison join");
        };
        let LogicalOperator::ExternalProject(project) = join.left.operator else {
            panic!("expected external project on left child");
        };
        assert_eq!(project.expressions.len(), 1);
        assert!(matches!(join.conditions[0].left, Expression::ColumnRef(_)));
    }

    #[test]
    fn rejects_cross_side_external_join_calls() {
        let bind_context = BindContext::new();
        let left = expression_get(&bind_context, 1);
        let right = expression_get(&bind_context, 2);
        let condition = external_call(
            "py_cross_join",
            vec![int_column(1, 0), int_column(2, 0)],
            LogicalType::Boolean,
        );
        let join = Join::any(JoinType::Inner, left, right, condition);
        let plan = LogicalPlan::new(&bind_context, LogicalOperator::Join(join));

        let error = ExternalRoutineLoweringPass::lower(plan, &bind_context)
            .expect_err("cross-side call must fail");
        assert!(error
            .to_string()
            .contains("cannot depend on both sides of the join"));
    }
}
