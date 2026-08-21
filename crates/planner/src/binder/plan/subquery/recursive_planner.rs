// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::binder::Binder;
use crate::expression::{ConstantExpression, ExpressionIterator, WindowExpression};
use crate::operator::{Join, LogicalOperator};
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;

pub struct RecursiveSubqueryPlanner<'a> {
    binder: &'a mut Binder,
}

impl<'a> RecursiveSubqueryPlanner<'a> {
    pub fn new(binder: &'a mut Binder) -> Self {
        Self { binder }
    }

    pub fn plan_root(&mut self, root: &mut LogicalOperator) -> Result<()> {
        let mut passes = 0usize;
        loop {
            passes += 1;
            if passes > 16 {
                return Err(paro_error::internal(
                    "RecursiveSubqueryPlanner exceeded 16 fixpoint passes".to_string(),
                ));
            }
            if !self.plan_one_pass(root)? {
                break;
            }
        }
        Ok(())
    }

    fn plan_one_pass(&mut self, op: &mut LogicalOperator) -> Result<bool> {
        match op {
            LogicalOperator::Filter(filter) => {
                let mut found = self.plan_one_pass(&mut filter.child.operator)?;
                found |= self.binder.plan_current_layer_subqueries_in_list(
                    &mut filter.expressions,
                    &mut filter.child.operator,
                )?;
                Ok(found)
            }
            LogicalOperator::Projection(proj) => {
                let mut found = self.plan_one_pass(&mut proj.child.operator)?;
                found |= self.binder.plan_current_layer_subqueries_in_list(
                    &mut proj.expressions,
                    &mut proj.child.operator,
                )?;
                Ok(found)
            }
            LogicalOperator::RowFetch(fetch) => {
                let mut found = self.plan_one_pass(&mut fetch.child.operator)?;
                for source in &mut fetch.sources {
                    found |= self.binder.plan_current_layer_subqueries(
                        &mut source.rowid,
                        &mut fetch.child.operator,
                    )?;
                }
                Ok(found)
            }
            LogicalOperator::ExternalProject(project) => {
                let mut found = self.plan_one_pass(&mut project.child.operator)?;
                for expr in &mut project.expressions {
                    found |= self.binder.plan_current_layer_subqueries(
                        &mut expr.expression,
                        &mut project.child.operator,
                    )?;
                }
                Ok(found)
            }
            LogicalOperator::ExternalTable(table) => {
                let mut found = false;
                if let Some(child) = &mut table.child {
                    found |= self.plan_one_pass(&mut child.operator)?;
                    found |= self.binder.plan_current_layer_subqueries(
                        &mut table.call_expression,
                        &mut child.operator,
                    )?;
                }
                Ok(found)
            }
            LogicalOperator::Limit(limit) => {
                let mut found = self.plan_one_pass(&mut limit.child.operator)?;
                if let Some(expr) = &mut limit.limit {
                    found |= self
                        .binder
                        .plan_current_layer_subqueries(expr, &mut limit.child.operator)?;
                }
                if let Some(expr) = &mut limit.offset {
                    found |= self
                        .binder
                        .plan_current_layer_subqueries(expr, &mut limit.child.operator)?;
                }
                Ok(found)
            }
            LogicalOperator::Order(order) => {
                let mut found = self.plan_one_pass(&mut order.child.operator)?;
                for item in &mut order.orders {
                    found |= self.binder.plan_current_layer_subqueries(
                        &mut item.expression,
                        &mut order.child.operator,
                    )?;
                }
                Ok(found)
            }
            LogicalOperator::TopN(topn) => {
                let mut found = self.plan_one_pass(&mut topn.child.operator)?;
                for item in &mut topn.orders {
                    found |= self.binder.plan_current_layer_subqueries(
                        &mut item.expression,
                        &mut topn.child.operator,
                    )?;
                }
                Ok(found)
            }
            LogicalOperator::Aggregate(agg) => {
                let mut found = self.plan_one_pass(&mut agg.child.operator)?;
                found |= self.binder.plan_current_layer_subqueries_in_list(
                    &mut agg.groups,
                    &mut agg.child.operator,
                )?;
                found |= self.binder.plan_current_layer_subqueries_in_list(
                    &mut agg.aggregates,
                    &mut agg.child.operator,
                )?;
                Ok(found)
            }
            LogicalOperator::Join(join) => {
                let mut found = match join {
                    Join::Comparison(comp) => {
                        let mut found = self.plan_one_pass(&mut comp.left.operator)?;
                        found |= self.plan_one_pass(&mut comp.right.operator)?;
                        found
                    }
                    Join::Any(any) => {
                        let mut found = self.plan_one_pass(&mut any.left.operator)?;
                        found |= self.plan_one_pass(&mut any.right.operator)?;
                        found
                    }
                    Join::Cross(cross) => {
                        let mut found = self.plan_one_pass(&mut cross.left.operator)?;
                        found |= self.plan_one_pass(&mut cross.right.operator)?;
                        found
                    }
                };
                found |= self.plan_join_expressions(op)?;
                Ok(found)
            }
            LogicalOperator::Distinct(distinct) => {
                let mut found = self.plan_one_pass(&mut distinct.child.operator)?;
                found |= self.binder.plan_current_layer_subqueries_in_list(
                    &mut distinct.distinct_targets,
                    &mut distinct.child.operator,
                )?;
                if let Some(order_by) = &mut distinct.order_by {
                    for item in order_by {
                        found |= self.binder.plan_current_layer_subqueries(
                            &mut item.expression,
                            &mut distinct.child.operator,
                        )?;
                    }
                }
                Ok(found)
            }
            LogicalOperator::Window(window) => {
                let mut found = self.plan_one_pass(&mut window.child.operator)?;
                for expr in &mut window.expressions {
                    found |= self.plan_window_expression(expr, &mut window.child.operator)?;
                }
                Ok(found)
            }
            LogicalOperator::MaterializedCTE(cte) => {
                let mut found = self.plan_one_pass(&mut cte.cte_query.operator)?;
                found |= self.plan_one_pass(&mut cte.child.operator)?;
                Ok(found)
            }
            LogicalOperator::RecursiveCTE(cte) => {
                let mut found = self.plan_one_pass(&mut cte.anchor.operator)?;
                found |= self.plan_one_pass(&mut cte.recursive.operator)?;
                Ok(found)
            }
            LogicalOperator::SetOperation(setop) => {
                let mut found = self.plan_one_pass(&mut setop.left.operator)?;
                found |= self.plan_one_pass(&mut setop.right.operator)?;
                Ok(found)
            }
            LogicalOperator::Insert(insert) => self.plan_one_pass(&mut insert.child.operator),
            LogicalOperator::Delete(delete) => self.plan_one_pass(&mut delete.child.operator),
            LogicalOperator::Update(update) => {
                let mut found = self.plan_one_pass(&mut update.child.operator)?;
                found |= self.binder.plan_current_layer_subqueries_in_list(
                    &mut update.expressions,
                    &mut update.child.operator,
                )?;
                Ok(found)
            }
            LogicalOperator::CopyTo(copy) => self.plan_one_pass(&mut copy.child.operator),
            LogicalOperator::Explain(explain) => self.plan_one_pass(&mut explain.child.operator),
            LogicalOperator::EmptyResult(empty) => self.plan_one_pass(&mut empty.child.operator),
            LogicalOperator::GraphExpand(expand) => self.plan_one_pass(&mut expand.child.operator),
            LogicalOperator::DependentJoin(dep) => {
                let mut found = self.plan_one_pass(&mut dep.left.operator)?;
                found |= self.plan_one_pass(&mut dep.right.operator)?;
                Ok(found)
            }
            LogicalOperator::TableFunctionGet(_)
            | LogicalOperator::SearchScan(_)
            | LogicalOperator::FullTextFilterScan(_)
            | LogicalOperator::ExpressionGet(_) => Ok(false),
            LogicalOperator::Get(_)
            | LogicalOperator::Alter(_)
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
            | LogicalOperator::DelimGet(_)
            | LogicalOperator::CTERef(_)
            | LogicalOperator::GraphMatch(_)
            | LogicalOperator::GraphScan(_)
            | LogicalOperator::DummyScan => Ok(false),
        }
    }

    fn plan_join_expressions(&mut self, op: &mut LogicalOperator) -> Result<bool> {
        match op {
            LogicalOperator::Join(Join::Comparison(comp)) => {
                let mut conditions = std::mem::take(&mut comp.conditions);
                let mut duplicate_eliminated_columns =
                    std::mem::take(&mut comp.duplicate_eliminated_columns);
                let mut found = false;
                for condition in &mut conditions {
                    found |= self
                        .binder
                        .plan_current_layer_subqueries(&mut condition.left, op)?;
                    found |= self
                        .binder
                        .plan_current_layer_subqueries(&mut condition.right, op)?;
                }
                for expr in &mut duplicate_eliminated_columns {
                    found |= self.binder.plan_current_layer_subqueries(expr, op)?;
                }
                let LogicalOperator::Join(Join::Comparison(comp)) = op else {
                    unreachable!();
                };
                comp.conditions = conditions;
                comp.duplicate_eliminated_columns = duplicate_eliminated_columns;
                Ok(found)
            }
            LogicalOperator::Join(Join::Any(any)) => {
                let mut condition = std::mem::replace(
                    &mut any.condition,
                    crate::expression::Expression::Constant(ConstantExpression {
                        value: Value::Boolean(true),
                        return_type: LogicalType::Boolean,
                    }),
                );
                let mut found = false;
                found |= self
                    .binder
                    .plan_current_layer_subqueries(&mut condition, op)?;
                let LogicalOperator::Join(Join::Any(any)) = op else {
                    unreachable!();
                };
                any.condition = condition;
                Ok(found)
            }
            LogicalOperator::Join(Join::Cross(_)) => Ok(false),
            _ => Ok(false),
        }
    }

    fn plan_window_expression(
        &mut self,
        expr: &mut WindowExpression,
        root: &mut LogicalOperator,
    ) -> Result<bool> {
        let mut found = false;
        let mut error = None;
        ExpressionIterator::enumerate_window_children_mut(expr, |child| {
            if error.is_some() {
                return;
            }
            match self.binder.plan_current_layer_subqueries(child, root) {
                Ok(child_found) => found |= child_found,
                Err(err) => error = Some(err),
            }
        });
        error.map_or(Ok(found), Err)
    }
}
