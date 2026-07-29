// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use paro_common::error::{self as paro_error, Result};
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{ColumnRefExpression, Expression, ExpressionIterator};
use paro_planner::operator::{Join, LogicalOperator};
use paro_planner::plan::LogicalPlan;

pub fn verify_logical_plan(_bind_context: &BindContext, plan: &LogicalPlan) -> Result<()> {
    verify_plan(_bind_context, &plan.operator)
}

fn verify_plan(_bind_context: &BindContext, plan: &LogicalOperator) -> Result<()> {
    let mut verifier = Verifier {
        seen_table_indices: HashSet::new(),
    };
    verifier.verify_operator(plan)
}

struct Verifier {
    seen_table_indices: HashSet<usize>,
}

impl Verifier {
    fn verify_operator(&mut self, op: &LogicalOperator) -> Result<()> {
        for idx in op.get_table_index() {
            if !self.seen_table_indices.insert(idx) {
                return Err(paro_error::internal(format!(
                    "Duplicate table index {} in logical plan",
                    idx
                )));
            }
        }

        self.verify_operator_invariants(op)?;
        self.verify_operator_expressions(op)?;

        for child in op.children() {
            self.verify_logical_plan(child)?;
        }

        Ok(())
    }

    fn verify_logical_plan(&mut self, plan: &LogicalPlan) -> Result<()> {
        self.verify_operator(&plan.operator)
    }

    fn verify_operator_invariants(&self, op: &LogicalOperator) -> Result<()> {
        let binding_len = op.get_column_bindings().len();
        let type_len = op.types().len();
        if binding_len != type_len {
            return Err(paro_error::internal(format!(
                "Operator {:?} output mismatch: {} bindings vs {} types",
                op.op_type(),
                binding_len,
                type_len
            )));
        }

        match op {
            LogicalOperator::Get(get) => {
                let len = get.returned_types.len();
                if get.names.len() != len
                    || get.column_ids.len() != len
                    || get.column_types.len() != len
                {
                    return Err(paro_error::internal(format!(
                        "Get output mismatch: returned_types={}, names={}, column_ids={}, column_types={}",
                        get.returned_types.len(),
                        get.names.len(),
                        get.column_ids.len(),
                        get.column_types.len()
                    )));
                }

                if let Some(table) = &get.table {
                    let table_col_count = table.columns.len();
                    for (idx, &col_id) in get.column_ids.iter().enumerate() {
                        let is_virtual_rowid = col_id == table_col_count
                            && matches!(
                                get.column_types.get(idx),
                                Some(paro_common::types::LogicalType::BigInt)
                            );
                        if col_id > table_col_count
                            || (col_id == table_col_count && !is_virtual_rowid)
                        {
                            return Err(paro_error::internal(format!(
                                "Get column id {} out of range (table columns={})",
                                col_id, table_col_count
                            )));
                        }
                    }
                }
            }
            LogicalOperator::Projection(proj) => {
                if proj.returned_types.len() != proj.expressions.len() {
                    return Err(paro_error::internal(format!(
                        "Projection returned_types mismatch: returned_types={}, expressions={}",
                        proj.returned_types.len(),
                        proj.expressions.len()
                    )));
                }
                for (i, expr) in proj.expressions.iter().enumerate() {
                    let ty = expr.return_type();
                    if proj.returned_types[i] != ty {
                        return Err(paro_error::internal(format!(
                            "Projection returned_types[{}] mismatch: cached={:?}, expr={:?}",
                            i, proj.returned_types[i], ty
                        )));
                    }
                }
            }
            LogicalOperator::Aggregate(agg) => {
                let expected =
                    agg.groups.len() + agg.aggregates.len() + agg.grouping_functions.len();
                if agg.returned_types.len() != expected {
                    return Err(paro_error::internal(format!(
                        "Aggregate returned_types mismatch: returned_types={}, expected={}",
                        agg.returned_types.len(),
                        expected
                    )));
                }
                for (i, expr) in agg.groups.iter().chain(agg.aggregates.iter()).enumerate() {
                    let ty: paro_common::types::LogicalType = expr.return_type();
                    if agg.returned_types[i] != ty {
                        return Err(paro_error::internal(format!(
                            "Aggregate returned_types[{}] mismatch: cached={:?}, expr={:?}",
                            i, agg.returned_types[i], ty
                        )));
                    }
                }
                for (i, _) in agg.grouping_functions.iter().enumerate() {
                    let idx = agg.groups.len() + agg.aggregates.len() + i;
                    if agg.returned_types[idx] != paro_common::types::LogicalType::BigInt {
                        return Err(paro_error::internal(format!(
                            "Aggregate grouping returned_types[{}] mismatch: cached={:?}, expected=BigInt",
                            idx, agg.returned_types[idx]
                        )));
                    }
                }
            }
            LogicalOperator::Join(join) => {
                self.verify_join_projection_maps(join)?;
            }
            LogicalOperator::SetOperation(setop) => {
                if setop.types.len() != setop.column_count {
                    return Err(paro_error::internal(format!(
                        "SetOperation column_count mismatch: column_count={}, types={}",
                        setop.column_count,
                        setop.types.len()
                    )));
                }
                let left_types = setop.left.types();
                let right_types = setop.right.types();
                if left_types.len() != setop.column_count || right_types.len() != setop.column_count
                {
                    return Err(paro_error::internal(format!(
                        "SetOperation arity mismatch: left={}, right={}, expected={}",
                        left_types.len(),
                        right_types.len(),
                        setop.column_count
                    )));
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn verify_join_projection_maps(&self, join: &Join) -> Result<()> {
        match join {
            Join::Comparison(cj) => {
                let left_len = cj.left.types().len();
                let right_len = cj.right.types().len();
                for &idx in &cj.left_projection_map {
                    if idx >= left_len {
                        return Err(paro_error::internal(format!(
                            "Join left_projection_map index {} out of range (left types={})",
                            idx, left_len
                        )));
                    }
                }
                for &idx in &cj.right_projection_map {
                    if idx >= right_len {
                        return Err(paro_error::internal(format!(
                            "Join right_projection_map index {} out of range (right types={})",
                            idx, right_len
                        )));
                    }
                }
                if matches!(
                    cj.join_type,
                    paro_planner::operator::JoinType::Semi | paro_planner::operator::JoinType::Anti
                ) && !cj.right_projection_map.is_empty()
                {
                    return Err(paro_error::internal(
                        "SEMI/ANTI join must not project right columns".to_string(),
                    ));
                }
            }
            Join::Any(aj) => {
                let left_len = aj.left.types().len();
                let right_len = aj.right.types().len();
                for &idx in &aj.left_projection_map {
                    if idx >= left_len {
                        return Err(paro_error::internal(format!(
                            "Join left_projection_map index {} out of range (left types={})",
                            idx, left_len
                        )));
                    }
                }
                for &idx in &aj.right_projection_map {
                    if idx >= right_len {
                        return Err(paro_error::internal(format!(
                            "Join right_projection_map index {} out of range (right types={})",
                            idx, right_len
                        )));
                    }
                }
                if matches!(
                    aj.join_type,
                    paro_planner::operator::JoinType::Semi | paro_planner::operator::JoinType::Anti
                ) && !aj.right_projection_map.is_empty()
                {
                    return Err(paro_error::internal(
                        "SEMI/ANTI join must not project right columns".to_string(),
                    ));
                }
            }
            Join::Cross(_) => {}
        }
        Ok(())
    }

    fn verify_operator_expressions(&self, op: &LogicalOperator) -> Result<()> {
        match op {
            LogicalOperator::Filter(filter) => {
                for expr in &filter.expressions {
                    self.verify_expression(expr)?;
                }
            }
            LogicalOperator::Projection(proj) => {
                for expr in &proj.expressions {
                    self.verify_expression(expr)?;
                }
            }
            LogicalOperator::Limit(limit) => {
                if let Some(expr) = &limit.limit {
                    self.verify_expression(expr)?;
                }
                if let Some(expr) = &limit.offset {
                    self.verify_expression(expr)?;
                }
            }
            LogicalOperator::Order(order) => {
                for node in &order.orders {
                    self.verify_expression(&node.expression)?;
                }
            }
            LogicalOperator::TopN(topn) => {
                for node in &topn.orders {
                    self.verify_expression(&node.expression)?;
                }
            }
            LogicalOperator::Aggregate(agg) => {
                for expr in &agg.groups {
                    self.verify_expression(expr)?;
                }
                for expr in &agg.aggregates {
                    self.verify_expression(expr)?;
                }
            }
            LogicalOperator::Join(join) => match join {
                Join::Comparison(cj) => {
                    for cond in &cj.conditions {
                        self.verify_expression(&cond.left)?;
                        self.verify_expression(&cond.right)?;
                    }
                }
                Join::Any(aj) => {
                    self.verify_expression(&aj.condition)?;
                }
                Join::Cross(_) => {}
            },
            LogicalOperator::DependentJoin(dj) => {
                if let Some(expr) = dj.join_condition() {
                    self.verify_expression(expr)?;
                }
                if let Some(payload) = dj.any_all_payload() {
                    for expr in &payload.expression_children {
                        self.verify_expression(expr)?;
                    }
                }
            }
            LogicalOperator::Distinct(distinct) => {
                for expr in &distinct.distinct_targets {
                    self.verify_expression(expr)?;
                }
                if let Some(orders) = &distinct.order_by {
                    for node in orders {
                        self.verify_expression(&node.expression)?;
                    }
                }
            }
            LogicalOperator::Window(window) => {
                let mut result = Ok(());
                for expression in &window.expressions {
                    ExpressionIterator::enumerate_window_children(expression, |child| {
                        if result.is_ok() {
                            result = self.verify_expression(child);
                        }
                    });
                }
                result?;
            }
            LogicalOperator::ExpressionGet(get) => {
                for row in &get.expressions {
                    for expr in row {
                        self.verify_expression(expr)?;
                    }
                }
            }
            LogicalOperator::CreateIndex(create_index) => {
                for expr in &create_index.expressions {
                    self.verify_expression(expr)?;
                }
                for expr in &create_index.unbound_expressions {
                    self.verify_expression(expr)?;
                }
            }
            LogicalOperator::Update(update) => {
                for expr in &update.expressions {
                    self.verify_expression(expr)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn verify_expression(&self, expr: &Expression) -> Result<()> {
        if let Expression::ColumnRef(column) = expr {
            return self.verify_column_ref(column);
        }

        let mut result = Ok(());
        ExpressionIterator::enumerate_children(expr, |child| {
            if result.is_ok() {
                result = self.verify_expression(child);
            }
        });
        result
    }

    fn verify_column_ref(&self, _col: &ColumnRefExpression) -> Result<()> {
        // With direct ColumnBinding storage, no additional verification needed
        // The binding is always valid by construction
        Ok(())
    }
}
