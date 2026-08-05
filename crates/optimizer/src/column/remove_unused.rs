// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Remove columns that are not required by ancestor operators.

use std::collections::HashMap;

use paro_context::StatementContext;
use paro_function::aggregate::distributive::count::get_count_star_function;
use paro_planner::binder::Binder;
use paro_planner::expression::{
    AggregateExpression, ColumnRefExpression, ConstantExpression, Expression, ExpressionIterator,
};
use paro_planner::operator::{ColumnBinding, LogicalOperator, Projection};
use paro_planner::plan::LogicalPlan;
use paro_planner::visitor::LogicalOperatorVisitor;

use crate::expression::binding_replacer::ReplacementBinding;

/// Information about a referenced column.
#[derive(Debug, Default, Clone)]
struct ReferencedColumn {
    /// The BoundColumnRefExpressions that reference this ColumnBinding.
    ///
    /// SAFETY: Pointers are only valid within a single visit_operator call.
    /// The expressions exist in LogicalOperator which we hold &mut to.
    bindings: Vec<*const ColumnRefExpression>,
    // Future: struct_extracts, child_columns, unique_paths
}

/// Remove unused columns with a single top-down traversal.
pub struct RemoveUnusedColumns<'a> {
    binder: &'a Binder,
    session: &'a StatementContext,
    column_references: HashMap<ColumnBinding, ReferencedColumn>,
    everything_referenced: bool,
    replacements: Vec<ReplacementBinding>,
}

impl<'a> RemoveUnusedColumns<'a> {
    fn new(binder: &'a Binder, session: &'a StatementContext, is_root: bool) -> Self {
        Self {
            binder,
            session,
            column_references: HashMap::new(),
            everything_referenced: is_root,
            replacements: Vec::new(),
        }
    }

    pub fn optimize(
        plan: &mut LogicalPlan,
        binder: &'a Binder,
        session: &'a StatementContext,
        is_root: bool,
    ) {
        let mut optimizer = Self::new(binder, session, is_root);
        optimizer.visit_logical_plan(plan);
    }

    #[inline]
    fn generate_table_index(&self) -> usize {
        self.binder.bind_context.generate_table_index()
    }

    /// Check if a column binding is referenced.
    /// Aligned with checking `column_references.find(binding) != column_references.end()`.
    #[inline]
    fn is_referenced(&self, binding: &ColumnBinding) -> bool {
        self.column_references.contains_key(binding)
    }

    /// Add a reference to a column.
    #[inline]
    fn add_binding(&mut self, col: &ColumnRefExpression) {
        let ptr = col as *const ColumnRefExpression;
        self.column_references
            .entry(col.binding)
            .or_default()
            .bindings
            .push(ptr);
    }

    /// Replace all bindings of a column with a new binding.
    /// Returns the number of bindings replaced.
    #[inline]
    fn replace_binding(&self, current_binding: ColumnBinding, new_binding: ColumnBinding) -> usize {
        let Some(col) = self.column_references.get(&current_binding) else {
            return 0;
        };

        for &ptr in &col.bindings {
            // SAFETY:
            // 1. Pointer was obtained from &ColumnRefExpression in the same visit_operator call
            // 2. The expression still exists in LogicalOperator (not moved or deallocated)
            // 3. We hold &mut LogicalOperator at the call site
            // 4. No other code modifies the expression between add_binding and replace_binding
            unsafe {
                let expr = &mut *(ptr as *mut ColumnRefExpression);
                expr.binding = new_binding;
            }
        }

        col.bindings.len()
    }

    /// Clear unused expressions from a list and remap bindings.
    /// Returns the new list with only referenced expressions.
    fn clear_unused_expressions(
        &mut self,
        expressions: &mut Vec<Expression>,
        output_names: Option<&mut Vec<String>>,
        table_idx: usize,
    ) {
        let mut new_expressions = Vec::new();
        let mut new_output_names = Vec::new();
        let mut new_col_idx = 0usize;

        for (old_idx, expr) in expressions.drain(..).enumerate() {
            let binding = ColumnBinding::new(table_idx, old_idx);
            if self.is_referenced(&binding)
                || self.everything_referenced
                || !expr.evaluation_properties().can_share_evaluation()
            {
                // Column is referenced, keep it
                if old_idx != new_col_idx {
                    // Column index changed, need to remap
                    // Use replace_binding to directly update parent's expressions via raw pointers
                    let new_binding = ColumnBinding::new(table_idx, new_col_idx);
                    self.replace_binding(binding, new_binding);
                }
                new_expressions.push(expr);
                if let Some(names) = output_names.as_ref() {
                    if let Some(name) = names.get(old_idx) {
                        new_output_names.push(name.clone());
                    }
                }
                new_col_idx += 1;
            }
        }

        *expressions = new_expressions;
        if let Some(names) = output_names {
            *names = new_output_names;
        }
    }

    /// Clear unused columns from a Get.
    fn remove_columns_from_get(&mut self, get: &mut paro_planner::operator::Get) {
        let mut new_column_ids = Vec::new();
        let mut new_column_types = Vec::new();
        let mut new_names = Vec::new();
        let mut new_col_idx = 0usize;

        for (old_idx, &col_id) in get.column_ids.iter().enumerate() {
            let binding = ColumnBinding::new(get.table_index, old_idx);
            if self.is_referenced(&binding) || self.everything_referenced {
                // Column is referenced, keep it
                if old_idx != new_col_idx {
                    // Column index changed, need to remap
                    let new_binding = ColumnBinding::new(get.table_index, new_col_idx);
                    self.replacements
                        .push(ReplacementBinding::new(binding, new_binding));
                }
                new_column_ids.push(col_id);
                new_column_types.push(get.column_types[old_idx].clone());
                if old_idx < get.names.len() {
                    new_names.push(get.names[old_idx].clone());
                }
                new_col_idx += 1;
            }
        }

        // Ensure at least one column
        if new_column_ids.is_empty() && !get.column_ids.is_empty() {
            new_column_ids.push(get.column_ids[0]);
            new_column_types.push(get.column_types[0].clone());
            if !get.names.is_empty() {
                new_names.push(get.names[0].clone());
            }
        }

        get.column_ids = new_column_ids;
        get.column_types = new_column_types.clone();
        get.returned_types = new_column_types;
        get.names = new_names;
    }
}

impl LogicalOperatorVisitor for RemoveUnusedColumns<'_> {
    fn visit_logical_plan(&mut self, plan: &mut LogicalPlan) {
        self.visit_operator(&mut plan.operator);

        // Window is cardinality-preserving and only appends computed columns. Once pruning has
        // removed every window expression, retaining the operator would enter the window runtime
        // for a pure pass-through. Preserve the child's plan identity and metadata when removing
        // that no-op boundary.
        if matches!(
            &plan.operator,
            LogicalOperator::Window(window) if window.expressions.is_empty()
        ) {
            let LogicalOperator::Window(window) =
                std::mem::replace(&mut plan.operator, LogicalOperator::DummyScan)
            else {
                unreachable!("empty-window guard must match the replaced operator");
            };
            *plan = *window.child;
        }
    }

    fn visit_operator(&mut self, op: &mut LogicalOperator) {
        match op {
            LogicalOperator::Aggregate(agg) => {
                // - Only aggregates are pruned
                if !self.everything_referenced {
                    // Clear unused aggregate expressions
                    // Aggregate bindings are local to `aggregate_index`.
                    let mut new_aggregates = Vec::new();
                    let mut new_agg_idx = 0usize;

                    for (old_agg_idx, agg_expr) in agg.aggregates.drain(..).enumerate() {
                        let binding = ColumnBinding::new(agg.aggregate_index, old_agg_idx);

                        if self.is_referenced(&binding)
                            || !agg_expr.evaluation_properties().can_share_evaluation()
                        {
                            if old_agg_idx != new_agg_idx {
                                // Binding changed, update via replace_binding
                                let new_binding =
                                    ColumnBinding::new(agg.aggregate_index, new_agg_idx);
                                self.replace_binding(binding, new_binding);
                            }
                            new_aggregates.push(agg_expr);
                            new_agg_idx += 1;
                        }
                    }
                    agg.aggregates = new_aggregates;

                    // If there are no aggregate expressions left, keep aggregate semantics
                    if agg.aggregates.is_empty() && agg.groups.is_empty() {
                        let count_star = get_count_star_function();
                        let return_type = count_star.return_type.clone();
                        agg.aggregates
                            .push(Expression::Aggregate(AggregateExpression::new(
                                count_star,
                                Vec::new(),
                                return_type,
                            )));
                    }

                    agg.recompute_returned_types();
                }

                // Create new instance for child, collect references from current expressions
                let mut child_optimizer =
                    RemoveUnusedColumns::new(self.binder, self.session, false);

                // Collect references from aggregate expressions
                for expr in &mut agg.groups {
                    child_optimizer.visit_expression(expr);
                }
                for expr in &mut agg.aggregates {
                    child_optimizer.visit_expression(expr);
                }

                // Recurse into child
                child_optimizer.visit_logical_plan(&mut agg.child);

                // Apply replacements from child
                for replacement in &child_optimizer.replacements {
                    child_optimizer
                        .replace_binding(replacement.old_binding, replacement.new_binding);
                }
            }
            LogicalOperator::Projection(proj) => {
                let carries_external_arguments = proj
                    .output_names
                    .iter()
                    .any(|name| name.starts_with("__external_arg_"));
                // Prune projection expressions if not at root
                if !self.everything_referenced && !carries_external_arguments {
                    self.clear_unused_expressions(
                        &mut proj.expressions,
                        Some(&mut proj.output_names),
                        proj.table_index,
                    );

                    // Ensure at least one expression
                    if proj.expressions.is_empty() {
                        proj.expressions
                            .push(Expression::Constant(ConstantExpression {
                                value: paro_common::runtime_value::Value::Integer(42),
                                return_type: paro_common::types::LogicalType::Integer,
                            }));
                        proj.output_names = vec!["42".to_string()];
                    }

                    // Update returned types
                    proj.returned_types =
                        proj.expressions.iter().map(|e| e.return_type()).collect();
                }

                // Create new instance for child, collect references from current expressions
                let mut child_optimizer =
                    RemoveUnusedColumns::new(self.binder, self.session, false);

                // Collect references from projection expressions (stores raw pointers)
                for expr in &mut proj.expressions {
                    child_optimizer.visit_expression(expr);
                }

                // Recurse into child
                child_optimizer.visit_logical_plan(&mut proj.child);

                // Use replace_binding to directly update bindings via raw pointers
                // No need for ColumnBindingReplacer traversal
                for replacement in &child_optimizer.replacements {
                    child_optimizer
                        .replace_binding(replacement.old_binding, replacement.new_binding);
                }
            }
            LogicalOperator::Filter(filter) => {
                // Filters don't produce new columns, just pass through
                let mut child_optimizer =
                    RemoveUnusedColumns::new(self.binder, self.session, self.everything_referenced);

                // Copy parent's column references - filter needs same columns as parent
                // This also transfers raw pointers to parent's expressions
                child_optimizer.column_references = std::mem::take(&mut self.column_references);

                // Also collect references from filter expressions
                for expr in &mut filter.expressions {
                    child_optimizer.visit_expression(expr);
                }

                // Recurse into child
                child_optimizer.visit_logical_plan(&mut filter.child);

                // Use replace_binding to directly update bindings via raw pointers
                // This updates both parent's and filter's expressions (since we inherited via mem::take)
                for replacement in &child_optimizer.replacements {
                    child_optimizer
                        .replace_binding(replacement.old_binding, replacement.new_binding);
                }
                // No need to propagate replacements up - already updated via raw pointers
            }
            LogicalOperator::Join(join) => {
                use paro_planner::operator::{JoinComparisonType, JoinType};

                // Joins don't produce new columns, pass through to both children
                let mut child_optimizer =
                    RemoveUnusedColumns::new(self.binder, self.session, self.everything_referenced);

                // Copy parent's column references
                child_optimizer.column_references = std::mem::take(&mut self.column_references);

                // For INNER JOIN with equality predicates, we can optimize:
                // Replace references to RHS with references to LHS to reduce columns extracted from hash table
                if let paro_planner::operator::Join::Comparison(cj) = join {
                    if !child_optimizer.everything_referenced && cj.join_type == JoinType::Inner {
                        for cond in &mut cj.conditions {
                            // Only for equality comparisons
                            if cond.comparison != JoinComparisonType::Equal {
                                continue;
                            }
                            // Both sides must be ColumnRef
                            let (lhs_binding, lhs_type) =
                                if let Expression::ColumnRef(ref lhs) = cond.left {
                                    (lhs.binding, lhs.return_type.clone())
                                } else {
                                    continue;
                                };
                            let rhs_binding = if let Expression::ColumnRef(ref rhs) = cond.right {
                                // Skip floating point types (+0 and -0 are equal but different)
                                if rhs.return_type.is_floating() || lhs_type.is_floating() {
                                    continue;
                                }
                                rhs.binding
                            } else {
                                continue;
                            };

                            // If there are references to RHS, redirect them to LHS
                            if let Some(rhs_col) =
                                child_optimizer.column_references.remove(&rhs_binding)
                            {
                                for &ptr in &rhs_col.bindings {
                                    // Update the binding to point to LHS
                                    unsafe {
                                        let expr = &mut *(ptr as *mut ColumnRefExpression);
                                        expr.binding = lhs_binding;
                                    }
                                    // Add this pointer to LHS's bindings
                                    child_optimizer.add_binding(unsafe { &*ptr });
                                }
                            }
                        }
                    }
                }

                // Collect references from join conditions
                match join {
                    paro_planner::operator::Join::Comparison(cj) => {
                        // Delim capture keys are evaluated against the captured
                        // child just like join conditions. Track them here so a
                        // pruned scan both retains the key and rewrites its
                        // binding to the compacted child layout.
                        for expr in &mut cj.duplicate_eliminated_columns {
                            child_optimizer.visit_expression(expr);
                        }
                        for cond in &mut cj.conditions {
                            child_optimizer.visit_expression(&mut cond.left);
                            child_optimizer.visit_expression(&mut cond.right);
                        }
                    }
                    paro_planner::operator::Join::Any(aj) => {
                        child_optimizer.visit_expression(&mut aj.condition);
                    }
                    paro_planner::operator::Join::Cross(_) => {}
                }

                let child_refs = child_optimizer.column_references.clone();

                let mut left_optimizer =
                    RemoveUnusedColumns::new(self.binder, self.session, self.everything_referenced);
                left_optimizer.column_references = child_refs.clone();
                left_optimizer.visit_logical_plan(join.left_mut());

                // Process right child with the same reference snapshot
                let mut right_optimizer =
                    RemoveUnusedColumns::new(self.binder, self.session, self.everything_referenced);
                right_optimizer.column_references = child_refs;
                right_optimizer.visit_logical_plan(join.right_mut());

                // Use replace_binding to directly update bindings via raw pointers
                for replacement in &left_optimizer.replacements {
                    left_optimizer
                        .replace_binding(replacement.old_binding, replacement.new_binding);
                }
                for replacement in &right_optimizer.replacements {
                    right_optimizer
                        .replace_binding(replacement.old_binding, replacement.new_binding);
                }
                // After replacing bindings, we may have duplicate conditions
                if let paro_planner::operator::Join::Comparison(cj) = join {
                    let mut unique_conditions = Vec::new();
                    for cond in cj.conditions.drain(..) {
                        let is_duplicate = unique_conditions.iter().any(
                            |existing: &paro_planner::operator::JoinCondition| {
                                cond.left.evaluation_properties().can_share_evaluation()
                                    && cond.right.evaluation_properties().can_share_evaluation()
                                    && existing.comparison == cond.comparison
                                    && existing.left.equals(&cond.left)
                                    && existing.right.equals(&cond.right)
                            },
                        );
                        if !is_duplicate {
                            unique_conditions.push(cond);
                        }
                    }
                    cj.conditions = unique_conditions;
                }
            }
            LogicalOperator::Get(get) => {
                // Remove unused columns from table scan
                self.remove_columns_from_get(get);
            }
            LogicalOperator::Order(order) => {
                // Order doesn't produce columns, pass through
                let mut child_optimizer =
                    RemoveUnusedColumns::new(self.binder, self.session, self.everything_referenced);

                // Copy parent's column references
                child_optimizer.column_references = std::mem::take(&mut self.column_references);

                // Collect references from order expressions
                for order_node in &mut order.orders {
                    child_optimizer.visit_expression(&mut order_node.expression);
                }

                // Recurse into child
                child_optimizer.visit_logical_plan(&mut order.child);

                // Use replace_binding to directly update bindings via raw pointers
                for replacement in &child_optimizer.replacements {
                    child_optimizer
                        .replace_binding(replacement.old_binding, replacement.new_binding);
                }
            }
            LogicalOperator::Limit(limit) => {
                // Limit doesn't produce columns, pass through
                let mut child_optimizer =
                    RemoveUnusedColumns::new(self.binder, self.session, self.everything_referenced);

                // Copy parent's column references
                child_optimizer.column_references = std::mem::take(&mut self.column_references);

                // Collect references from limit/offset expressions if they reference columns
                if let Some(ref mut expr) = limit.limit {
                    child_optimizer.visit_expression(expr);
                }
                if let Some(ref mut expr) = limit.offset {
                    child_optimizer.visit_expression(expr);
                }

                // Recurse into child
                child_optimizer.visit_logical_plan(&mut limit.child);

                // Use replace_binding to directly update bindings via raw pointers
                for replacement in &child_optimizer.replacements {
                    child_optimizer
                        .replace_binding(replacement.old_binding, replacement.new_binding);
                }
            }
            LogicalOperator::TopN(topn) => {
                // TopN doesn't produce columns, pass through
                let mut child_optimizer =
                    RemoveUnusedColumns::new(self.binder, self.session, self.everything_referenced);

                // Copy parent's column references
                child_optimizer.column_references = std::mem::take(&mut self.column_references);

                // Collect references from order expressions
                for order_node in &mut topn.orders {
                    child_optimizer.visit_expression(&mut order_node.expression);
                }

                // Recurse into child
                child_optimizer.visit_logical_plan(&mut topn.child);

                // Use replace_binding to directly update bindings via raw pointers
                for replacement in &child_optimizer.replacements {
                    child_optimizer
                        .replace_binding(replacement.old_binding, replacement.new_binding);
                }
            }
            LogicalOperator::SetOperation(setop) => {
                use paro_planner::operator::SetOpType;
                let layout_sensitive = [&setop.left, &setop.right].into_iter().any(|child| {
                    child
                        .output_names()
                        .iter()
                        .any(|name| name.starts_with("__corr_"))
                });
                if setop.setop_type == SetOpType::Union
                    && setop.setop_all
                    && !self.everything_referenced
                    && !layout_sensitive
                {
                    // 1. Collect and prune column indices using clear_unused_expressions pattern
                    let entries: Vec<usize> = (0..setop.column_count).collect();
                    let original_count = setop.column_count;

                    // Filter to only referenced columns and update bindings
                    let mut new_entries = Vec::new();
                    let mut new_col_idx = 0usize;
                    for old_col_idx in entries {
                        let binding = ColumnBinding::new(setop.table_index, old_col_idx);
                        if self.is_referenced(&binding) {
                            if old_col_idx != new_col_idx {
                                let new_binding =
                                    ColumnBinding::new(setop.table_index, new_col_idx);
                                self.replace_binding(binding, new_binding);
                            }
                            new_entries.push(old_col_idx);
                            new_col_idx += 1;
                        }
                    }

                    // Check if any pruning happened
                    if new_entries.len() < original_count {
                        // At least keep one column (for COUNT(*) case)
                        if new_entries.is_empty() {
                            new_entries.push(0);
                        }

                        // 2. Update column_count and types
                        setop.column_count = new_entries.len();
                        setop.types = new_entries
                            .iter()
                            .map(|&i| setop.types[i].clone())
                            .collect();

                        // 3. Insert Projection for each child
                        for child in [&mut setop.left, &mut setop.right] {
                            let child_bindings = child.get_column_bindings();
                            let child_types = child.types();

                            // Create projection expressions for referenced columns
                            let expressions: Vec<Expression> = new_entries
                                .iter()
                                .map(|&col_idx| {
                                    Expression::ColumnRef(ColumnRefExpression::new(
                                        child_bindings[col_idx],
                                        child_types[col_idx].clone(),
                                    ))
                                })
                                .collect();

                            // Create new Projection
                            let projection_index = self.generate_table_index();
                            let old_child = std::mem::replace(
                                child,
                                Box::new(LogicalPlan::synthetic(LogicalOperator::DummyScan)),
                            );
                            let projection =
                                Projection::new(projection_index, *old_child, expressions);
                            *child = Box::new(LogicalPlan::new(
                                &self.binder.bind_context,
                                LogicalOperator::Projection(projection),
                            ));

                            // Recursively process the inserted Projection
                            let mut child_optimizer =
                                RemoveUnusedColumns::new(self.binder, self.session, true);
                            child_optimizer.visit_logical_plan(child);
                        }
                        return;
                    }
                }

                // Default handling: UNION (non-ALL), EXCEPT, or INTERSECT
                // All columns are needed for comparison/deduplication
                for child in [&mut setop.left, &mut setop.right] {
                    let mut child_optimizer =
                        RemoveUnusedColumns::new(self.binder, self.session, true);
                    child_optimizer.visit_logical_plan(child);
                }
            }
            LogicalOperator::Window(window) => {
                if !self.everything_referenced {
                    let mut retained = Vec::new();
                    for (old_index, expression) in window.expressions.drain(..).enumerate() {
                        let old_binding = ColumnBinding::new(window.window_index, old_index);
                        if self.is_referenced(&old_binding)
                            || !expression.evaluation_properties().can_share_evaluation()
                        {
                            let new_index = retained.len();
                            if old_index != new_index {
                                self.replace_binding(
                                    old_binding,
                                    ColumnBinding::new(window.window_index, new_index),
                                );
                            }
                            retained.push(expression);
                        }
                    }
                    window.expressions = retained;
                }

                // Window is append-only: parent references to child columns pass through, while
                // every retained window expression contributes its own child requirements.
                let mut child_optimizer =
                    RemoveUnusedColumns::new(self.binder, self.session, self.everything_referenced);
                child_optimizer.column_references = std::mem::take(&mut self.column_references);
                for window_expression in &mut window.expressions {
                    ExpressionIterator::enumerate_window_children_mut(window_expression, |child| {
                        child_optimizer.visit_expression(child)
                    });
                }

                child_optimizer.visit_logical_plan(&mut window.child);

                for replacement in &child_optimizer.replacements {
                    child_optimizer
                        .replace_binding(replacement.old_binding, replacement.new_binding);
                }
            }
            LogicalOperator::Distinct(distinct) => {
                // - For DISTINCT ON, no need to implicitly reference everything
                // - For regular DISTINCT, all columns are used for comparison

                use paro_planner::operator::DistinctType;

                let new_everything_referenced = match distinct.distinct_type {
                    DistinctType::DistinctOn => {
                        // DISTINCT ON references specific columns, not everything
                        self.everything_referenced
                    }
                    DistinctType::Distinct => {
                        // Regular DISTINCT uses all columns for comparison
                        true
                    }
                };

                let mut child_optimizer =
                    RemoveUnusedColumns::new(self.binder, self.session, new_everything_referenced);

                // For DISTINCT ON, inherit parent's column references
                if distinct.distinct_type == DistinctType::DistinctOn {
                    child_optimizer.column_references = std::mem::take(&mut self.column_references);
                }

                // Collect references from distinct_targets and order_by
                for expr in &mut distinct.distinct_targets {
                    child_optimizer.visit_expression(expr);
                }
                if let Some(ref mut orders) = distinct.order_by {
                    for order in orders {
                        child_optimizer.visit_expression(&mut order.expression);
                    }
                }

                // Recurse into child
                child_optimizer.visit_logical_plan(&mut distinct.child);

                // Apply replacements from child
                for replacement in &child_optimizer.replacements {
                    child_optimizer
                        .replace_binding(replacement.old_binding, replacement.new_binding);
                }
            }
            LogicalOperator::Insert(insert) => {
                let mut child_optimizer = RemoveUnusedColumns::new(self.binder, self.session, true);
                child_optimizer.visit_logical_plan(&mut insert.child);
            }
            LogicalOperator::Update(update) => {
                let mut child_optimizer = RemoveUnusedColumns::new(self.binder, self.session, true);
                child_optimizer.visit_logical_plan(&mut update.child);
            }
            LogicalOperator::Delete(delete) => {
                let mut child_optimizer = RemoveUnusedColumns::new(self.binder, self.session, true);
                child_optimizer.visit_logical_plan(&mut delete.child);
            }
            LogicalOperator::MaterializedCTE(cte) => {
                // Producer side remains a materialization boundary: keep the CTE
                // definition schema intact unless a later pass rewrites all refs.
                let mut cte_query_optimizer =
                    RemoveUnusedColumns::new(self.binder, self.session, true);
                cte_query_optimizer.visit_logical_plan(&mut cte.cte_query);

                // Consumer-side pruning still follows the parent demand.
                let mut child_optimizer =
                    RemoveUnusedColumns::new(self.binder, self.session, self.everything_referenced);
                child_optimizer.column_references = std::mem::take(&mut self.column_references);
                child_optimizer.visit_logical_plan(&mut cte.child);

                for replacement in &child_optimizer.replacements {
                    child_optimizer
                        .replace_binding(replacement.old_binding, replacement.new_binding);
                }
            }
            LogicalOperator::RecursiveCTE(cte) => {
                // Recursive CTE producers currently keep their full schema.
                let mut anchor_optimizer =
                    RemoveUnusedColumns::new(self.binder, self.session, true);
                anchor_optimizer.visit_logical_plan(&mut cte.anchor);

                let mut recursive_optimizer =
                    RemoveUnusedColumns::new(self.binder, self.session, true);
                recursive_optimizer.visit_logical_plan(&mut cte.recursive);
            }
            LogicalOperator::CTERef(_) => {}
            LogicalOperator::ExpressionGet(expr_get) => {
                // ExpressionGet (VALUES clause): prune unused expressions
                if !self.everything_referenced {
                    // Each expression list in expr_get.expressions represents a row
                    // We need to prune columns (vertical pruning), not rows
                    // Track which column indices are referenced
                    let mut referenced_cols = vec![false; expr_get.types.len()];

                    for col_idx in 0..expr_get.types.len() {
                        let binding = ColumnBinding::new(expr_get.table_index, col_idx);
                        let preserves_evaluation = expr_get.expressions.iter().any(|row| {
                            row.get(col_idx).is_some_and(|expr| {
                                !expr.evaluation_properties().can_share_evaluation()
                            })
                        });
                        if self.is_referenced(&binding) || preserves_evaluation {
                            referenced_cols[col_idx] = true;
                        }
                    }

                    // If no columns are referenced, keep at least one
                    if !referenced_cols.iter().any(|&r| r) {
                        if !referenced_cols.is_empty() {
                            referenced_cols[0] = true;
                        }
                    }

                    // Prune columns from each expression list
                    for expr_list in &mut expr_get.expressions {
                        let mut new_list = Vec::new();
                        for (idx, keep) in referenced_cols.iter().enumerate() {
                            if *keep && idx < expr_list.len() {
                                new_list.push(expr_list[idx].clone());
                            }
                        }
                        *expr_list = new_list;
                    }

                    // Update types and handle binding replacements
                    let mut new_types = Vec::new();
                    let mut new_col_idx = 0usize;
                    for (old_col_idx, &keep) in referenced_cols.iter().enumerate() {
                        if keep {
                            if old_col_idx != new_col_idx {
                                // Column index changed, update bindings
                                let old_binding =
                                    ColumnBinding::new(expr_get.table_index, old_col_idx);
                                let new_binding =
                                    ColumnBinding::new(expr_get.table_index, new_col_idx);
                                self.replace_binding(old_binding, new_binding);
                            }
                            if old_col_idx < expr_get.types.len() {
                                new_types.push(expr_get.types[old_col_idx].clone());
                            }
                            new_col_idx += 1;
                        }
                    }
                    expr_get.types = new_types;
                }
            }
            LogicalOperator::DummyScan => {
                // Nothing to do
            }
            _ => {
                // Default: for other operators, visit expressions and children
                self.visit_operator_expressions(op);
                self.visit_operator_children(op);
            }
        }
    }

    fn visit_replace_column_ref(&mut self, expr: &mut ColumnRefExpression) -> Option<Expression> {
        self.add_binding(expr);

        None // Don't replace, just collect
    }
}

#[cfg(test)]
mod tests {
    use super::RemoveUnusedColumns;
    use paro_common::types::LogicalType;
    use paro_context::test_support::TestStatementContextBuilder;
    use paro_planner::binder::Binder;
    use paro_planner::expression::{ColumnRefExpression, Expression};
    use paro_planner::operator::{
        ColumnBinding, ComparisonJoin, ExpressionGet, Get, Join, JoinComparisonType, JoinCondition,
        JoinType, LogicalOperator, Projection,
    };
    use paro_planner::plan::LogicalPlan;

    fn int_column(table_index: usize, column_index: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(table_index, column_index),
            LogicalType::Integer,
        ))
    }

    fn binding(expression: &Expression) -> ColumnBinding {
        let Expression::ColumnRef(column) = expression else {
            panic!("expected column reference");
        };
        column.binding
    }

    #[test]
    fn delim_capture_key_tracks_pruned_child_binding() {
        let session = TestStatementContextBuilder::minimal().build();
        let binder = Binder::new(session.clone());
        let ctx = &binder.bind_context;
        let left = LogicalPlan::new(
            ctx,
            LogicalOperator::Get(Get::new_without_table(
                10,
                vec!["unused".into(), "key".into()],
                vec![LogicalType::Integer, LogicalType::Integer],
            )),
        );
        let right = LogicalPlan::new(
            ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                20,
                Vec::new(),
                vec!["value".into()],
                vec![LogicalType::Integer],
            )),
        );
        let mut join = ComparisonJoin::new(
            JoinType::Single,
            left,
            right,
            vec![JoinCondition::new(
                int_column(10, 1),
                int_column(20, 0),
                JoinComparisonType::NotDistinctFrom,
            )],
        );
        join.duplicate_eliminated_columns = vec![int_column(10, 1)];
        join.right_projection_map = vec![0];
        let joined = LogicalPlan::new(ctx, LogicalOperator::Join(Join::Comparison(join)));
        let mut plan = LogicalPlan::new(
            ctx,
            LogicalOperator::Projection(Projection::new(30, joined, vec![int_column(20, 0)])),
        );

        RemoveUnusedColumns::optimize(&mut plan, &binder, session.as_ref(), true);

        let LogicalOperator::Projection(projection) = &plan.operator else {
            panic!("expected root projection");
        };
        let LogicalOperator::Join(Join::Comparison(join)) = &projection.child.operator else {
            panic!("expected comparison join");
        };
        let LogicalOperator::Get(get) = &join.left.operator else {
            panic!("expected left get");
        };
        assert_eq!(get.column_ids, vec![1]);
        assert_eq!(
            binding(&join.duplicate_eliminated_columns[0]),
            ColumnBinding::new(10, 0)
        );
        assert_eq!(binding(&join.conditions[0].left), ColumnBinding::new(10, 0));
    }

    #[test]
    fn correlation_projection_prunes_unobserved_subquery_payload() {
        let session = TestStatementContextBuilder::minimal().build();
        let binder = Binder::new(session.clone());
        let ctx = &binder.bind_context;
        let scan = LogicalPlan::new(
            ctx,
            LogicalOperator::Get(Get::new_without_table(
                10,
                vec![
                    "payload_0".into(),
                    "correlation_key".into(),
                    "payload_1".into(),
                ],
                vec![
                    LogicalType::Integer,
                    LogicalType::Integer,
                    LogicalType::Integer,
                ],
            )),
        );
        let mut correlation_projection = Projection::new(
            20,
            scan,
            vec![int_column(10, 0), int_column(10, 2), int_column(10, 1)],
        );
        correlation_projection.output_names =
            vec!["payload_0".into(), "payload_1".into(), "__corr_1".into()];
        let correlation_projection =
            LogicalPlan::new(ctx, LogicalOperator::Projection(correlation_projection));
        let mut plan = LogicalPlan::new(
            ctx,
            LogicalOperator::Projection(Projection::new(
                30,
                correlation_projection,
                vec![int_column(20, 2)],
            )),
        );

        RemoveUnusedColumns::optimize(&mut plan, &binder, session.as_ref(), true);

        let LogicalOperator::Projection(root) = &plan.operator else {
            panic!("expected root projection");
        };
        assert_eq!(binding(&root.expressions[0]), ColumnBinding::new(20, 0));
        let LogicalOperator::Projection(correlation) = &root.child.operator else {
            panic!("expected correlation projection");
        };
        assert_eq!(correlation.output_names, vec!["__corr_1"]);
        assert_eq!(correlation.expressions.len(), 1);
        let LogicalOperator::Get(scan) = &correlation.child.operator else {
            panic!("expected scan");
        };
        assert_eq!(scan.names, vec!["correlation_key"]);
    }
}
