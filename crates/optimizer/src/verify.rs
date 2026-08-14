// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use paro_common::error::{self as paro_error, Result};
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{ColumnRefExpression, Expression, ExpressionIterator};
use paro_planner::operator::{ColumnBinding, Join, LogicalOperator};
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

#[derive(Debug)]
struct GraphProjectionScope {
    materialized_table_indices: HashSet<usize>,
    carrier_bindings: Vec<ColumnBinding>,
}

impl GraphProjectionScope {
    fn from_plan(plan: &LogicalPlan) -> Option<Self> {
        match &plan.operator {
            LogicalOperator::GraphScan(scan) => Some(Self {
                materialized_table_indices: HashSet::from([scan.table_index]),
                carrier_bindings: plan.get_column_bindings(),
            }),
            LogicalOperator::GraphExpand(expand) => {
                let mut scope = Self::from_plan(expand.child.as_ref())?;
                scope
                    .materialized_table_indices
                    .insert(expand.edge_table_index);
                scope
                    .materialized_table_indices
                    .insert(expand.target_table_index);
                scope.carrier_bindings = plan.get_column_bindings();
                Some(scope)
            }
            LogicalOperator::Filter(filter) => {
                let mut scope = Self::from_plan(filter.child.as_ref())?;
                scope.carrier_bindings = plan.get_column_bindings();
                Some(scope)
            }
            LogicalOperator::EmptyResult(empty) => {
                let mut scope = Self::from_plan(empty.child.as_ref())?;
                scope.carrier_bindings = plan.get_column_bindings();
                Some(scope)
            }
            _ => None,
        }
    }
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
                if let Some(fetch) = &proj.late_row_fetch {
                    let child_bindings = proj.child.get_column_bindings();
                    if fetch.sources.is_empty() {
                        return Err(paro_error::internal(
                            "Late row-fetch projection has no materialized source",
                        ));
                    }
                    if !child_bindings
                        .iter()
                        .any(|binding| binding.table_index == fetch.carrier_table_index)
                    {
                        return Err(paro_error::internal(format!(
                            "Late row-fetch carrier table {} is not produced by its child",
                            fetch.carrier_table_index
                        )));
                    }
                    let mut source_indices = HashSet::with_capacity(fetch.sources.len());
                    for source in &fetch.sources {
                        if !source_indices.insert(source.materialized_table_index) {
                            return Err(paro_error::internal(format!(
                                "Late row-fetch source table index {} is duplicated",
                                source.materialized_table_index
                            )));
                        }
                        let Expression::ColumnRef(rowid) = &source.rowid else {
                            return Err(paro_error::internal(
                                "Late row-fetch rowid carrier must be a direct column reference",
                            ));
                        };
                        if rowid.depth != 0
                            || rowid.binding.table_index != fetch.carrier_table_index
                            || rowid.return_type != paro_common::types::LogicalType::BigInt
                        {
                            return Err(paro_error::internal(
                                "Late row-fetch rowid must be a direct BIGINT carrier column",
                            ));
                        }
                        self.verify_expression_bindings(
                            &source.rowid,
                            &child_bindings,
                            "Late row-fetch rowid",
                        )?;
                    }
                    for expression in &proj.expressions {
                        self.verify_late_row_fetch_bindings(expression, fetch, &child_bindings)?;
                    }
                } else if let Some(scope) = GraphProjectionScope::from_plan(proj.child.as_ref()) {
                    for expression in &proj.expressions {
                        self.verify_graph_projection_bindings(expression, &scope)?;
                    }
                } else {
                    let child_bindings = proj.child.get_column_bindings();
                    for expression in &proj.expressions {
                        self.verify_expression_bindings(expression, &child_bindings, "Projection")?;
                    }
                }
            }
            LogicalOperator::Filter(filter) => {
                let child_bindings = filter.child.get_column_bindings();
                Self::verify_projection_map(
                    "Filter",
                    &filter.projection_map,
                    child_bindings.len(),
                )?;
                for expression in &filter.expressions {
                    self.verify_expression_bindings(expression, &child_bindings, "Filter")?;
                }
            }
            LogicalOperator::Order(order) => {
                Self::verify_projection_map(
                    "Order",
                    &order.projection_map,
                    order.child.types().len(),
                )?;
            }
            LogicalOperator::Aggregate(agg) => {
                agg.verify_post_reduction()?;
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
                if let Some((dependency_idx, _)) = agg
                    .group_dependencies
                    .iter()
                    .enumerate()
                    .find(|(_, dependency)| !dependency.is_valid_for(agg.groups.len()))
                {
                    return Err(paro_error::internal(format!(
                        "Aggregate group dependency {dependency_idx} is invalid for {} groups",
                        agg.groups.len()
                    )));
                }
                let child_bindings = agg.child.get_column_bindings();
                for expression in agg.groups.iter().chain(agg.aggregates.iter()) {
                    self.verify_expression_bindings(expression, &child_bindings, "Aggregate")?;
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

    fn verify_expression_bindings(
        &self,
        expression: &Expression,
        available: &[ColumnBinding],
        scope: &str,
    ) -> Result<()> {
        if let Expression::ColumnRef(column) = expression {
            if column.depth == 0 && !available.contains(&column.binding) {
                return Err(paro_error::internal(format!(
                    "{scope} expression references unavailable binding {:?}; input bindings: {available:?}",
                    column.binding
                )));
            }
            return Ok(());
        }

        let mut result = Ok(());
        ExpressionIterator::enumerate_children(expression, |child| {
            if result.is_ok() {
                result = self.verify_expression_bindings(child, available, scope);
            }
        });
        result
    }

    fn verify_graph_projection_bindings(
        &self,
        expression: &Expression,
        scope: &GraphProjectionScope,
    ) -> Result<()> {
        if let Expression::ColumnRef(column) = expression {
            if column.depth != 0 {
                return Ok(());
            }
            let binding = column.binding;
            let available = scope.carrier_bindings.contains(&binding)
                || scope
                    .materialized_table_indices
                    .contains(&binding.table_index);
            if !available {
                return Err(paro_error::internal(format!(
                    "Graph projection expression references unavailable binding {:?}; materialized tables: {:?}, carrier bindings: {:?}",
                    binding, scope.materialized_table_indices, scope.carrier_bindings
                )));
            }
            return Ok(());
        }

        let mut result = Ok(());
        ExpressionIterator::enumerate_children(expression, |child| {
            if result.is_ok() {
                result = self.verify_graph_projection_bindings(child, scope);
            }
        });
        result
    }

    fn verify_late_row_fetch_bindings(
        &self,
        expression: &Expression,
        fetch: &paro_planner::operator::projection::LateRowFetch,
        carrier_bindings: &[ColumnBinding],
    ) -> Result<()> {
        if let Expression::ColumnRef(column) = expression {
            if column.depth != 0 {
                return Ok(());
            }
            if column.binding.table_index == fetch.carrier_table_index {
                if carrier_bindings.contains(&column.binding) {
                    return Ok(());
                }
                return Err(paro_error::internal(format!(
                    "Late row-fetch expression references unavailable carrier binding {:?}",
                    column.binding
                )));
            }
            let Some(source) = fetch
                .sources
                .iter()
                .find(|source| source.materialized_table_index == column.binding.table_index)
            else {
                return Err(paro_error::internal(format!(
                    "Late row-fetch expression references unknown materialized binding {:?}",
                    column.binding
                )));
            };
            if column.binding.column_index >= source.table.columns.len() {
                return Err(paro_error::internal(format!(
                    "Late row-fetch catalog column {} is out of range for table {}",
                    column.binding.column_index, source.table.base.base.name
                )));
            }
            let catalog_type = &source.table.columns[column.binding.column_index].logical_type;
            if &column.return_type != catalog_type {
                return Err(paro_error::internal(format!(
                    "Late row-fetch catalog column {} type mismatch: expression={:?}, catalog={:?}",
                    column.binding.column_index, column.return_type, catalog_type
                )));
            }
            return Ok(());
        }

        let mut result = Ok(());
        ExpressionIterator::enumerate_children(expression, |child| {
            if result.is_ok() {
                result = self.verify_late_row_fetch_bindings(child, fetch, carrier_bindings);
            }
        });
        result
    }

    fn verify_join_projection_maps(&self, join: &Join) -> Result<()> {
        match join {
            Join::Comparison(cj) => {
                let left_len = cj.left.types().len();
                let right_len = cj.right.types().len();
                let left_bindings = cj.left.get_column_bindings();
                let right_bindings = cj.right.get_column_bindings();
                let duplicate_input = if cj.delim_flipped {
                    right_bindings.as_slice()
                } else {
                    left_bindings.as_slice()
                };
                for expression in &cj.duplicate_eliminated_columns {
                    self.verify_expression_bindings(
                        expression,
                        duplicate_input,
                        "comparison join duplicate-eliminated key",
                    )?;
                }
                for condition in &cj.conditions {
                    self.verify_expression_bindings(
                        &condition.left,
                        &left_bindings,
                        "comparison join probe key",
                    )?;
                    self.verify_expression_bindings(
                        &condition.right,
                        &right_bindings,
                        "comparison join build key",
                    )?;
                }
                Self::verify_projection_map(
                    "Join left_projection_map",
                    &cj.left_projection_map,
                    left_len,
                )?;
                Self::verify_projection_map(
                    "Join right_projection_map",
                    &cj.right_projection_map,
                    right_len,
                )?;
                if matches!(
                    cj.join_type,
                    paro_planner::operator::JoinType::Semi
                        | paro_planner::operator::JoinType::Anti
                        | paro_planner::operator::JoinType::Mark
                ) && !cj.right_projection_map.is_none()
                {
                    return Err(paro_error::internal(
                        "SEMI/ANTI/MARK join must not project right columns".to_string(),
                    ));
                }
                if matches!(
                    cj.join_type,
                    paro_planner::operator::JoinType::RightSemi
                        | paro_planner::operator::JoinType::RightAnti
                ) && !cj.left_projection_map.is_none()
                {
                    return Err(paro_error::internal(
                        "RIGHT SEMI/ANTI join must not project left columns".to_string(),
                    ));
                }
            }
            Join::Any(aj) => {
                let left_len = aj.left.types().len();
                let right_len = aj.right.types().len();
                let mut input_bindings = aj.left.get_column_bindings();
                input_bindings.extend(aj.right.get_column_bindings());
                self.verify_expression_bindings(
                    &aj.condition,
                    &input_bindings,
                    "ANY join condition",
                )?;
                Self::verify_projection_map(
                    "Join left_projection_map",
                    &aj.left_projection_map,
                    left_len,
                )?;
                Self::verify_projection_map(
                    "Join right_projection_map",
                    &aj.right_projection_map,
                    right_len,
                )?;
                if matches!(
                    aj.join_type,
                    paro_planner::operator::JoinType::Semi
                        | paro_planner::operator::JoinType::Anti
                        | paro_planner::operator::JoinType::Mark
                ) && !aj.right_projection_map.is_none()
                {
                    return Err(paro_error::internal(
                        "SEMI/ANTI/MARK join must not project right columns".to_string(),
                    ));
                }
                if matches!(
                    aj.join_type,
                    paro_planner::operator::JoinType::RightSemi
                        | paro_planner::operator::JoinType::RightAnti
                ) && !aj.left_projection_map.is_none()
                {
                    return Err(paro_error::internal(
                        "RIGHT SEMI/ANTI join must not project left columns".to_string(),
                    ));
                }
            }
            Join::Cross(_) => {}
        }
        Ok(())
    }

    fn verify_projection_map(
        label: &str,
        projection_map: &paro_planner::operator::ProjectionMap,
        child_width: usize,
    ) -> Result<()> {
        let Some(indices) = projection_map.as_columns() else {
            return Ok(());
        };
        let mut seen = HashSet::with_capacity(indices.len());
        for &index in indices {
            if index >= child_width {
                return Err(paro_error::internal(format!(
                    "{label} index {index} out of range (child columns={child_width})"
                )));
            }
            if !seen.insert(index) {
                return Err(paro_error::internal(format!(
                    "{label} contains duplicate child index {index}"
                )));
            }
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
                if let Some(reduction) = &agg.post_reduction {
                    for reducer in &reduction.reducers {
                        self.verify_expression(reducer)?;
                    }
                    for scalar in &reduction.scalar_expressions {
                        self.verify_expression(scalar)?;
                    }
                    self.verify_expression(&reduction.predicate)?;
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
                    expression.verify_bound_contract()?;
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
        if let Expression::Window(window) = expr {
            window.verify_bound_contract()?;
        }
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
