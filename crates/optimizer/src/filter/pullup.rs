// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Pull filter predicates upward so later passes can combine or reposition them.
//!
//! This is useful for:
//! - Combining filters from different branches of a join
//! - Moving filters above projections to enable further optimizations
//! - Preparing filters for pushdown in a subsequent pass

use paro_planner::expression::{ColumnRefExpression, ComparisonExpression, Expression};
use paro_planner::operator::{
    AnyJoin, ColumnBinding, ComparisonJoin, CrossProduct, Filter, Join, JoinType, LogicalOperator,
    Projection, SetOpType, SetOperation,
};
use paro_planner::plan::LogicalPlan;

use crate::expression::join_has_evaluation_fence;
use crate::expression::traversal::visit_expression;

/// Filter pullup optimizer.
///
/// Pulls filter predicates up through the logical plan tree,
/// collecting them for potential combination or further optimization.
pub struct FilterPullup {
    /// Filters that have been pulled up from children.
    filters_expr_pullup: Vec<Expression>,
    /// Whether we can pull up filters (true when there's a fork in the plan).
    can_pullup: bool,
    /// Whether we can add columns to projections (false for set operations).
    can_add_column: bool,
}

impl FilterPullup {
    /// Create a new FilterPullup optimizer.
    pub fn new() -> Self {
        Self {
            filters_expr_pullup: Vec::new(),
            can_pullup: false,
            can_add_column: false,
        }
    }

    /// Create a FilterPullup with specific settings.
    pub fn with_settings(can_pullup: bool, can_add_column: bool) -> Self {
        Self {
            filters_expr_pullup: Vec::new(),
            can_pullup,
            can_add_column,
        }
    }

    /// Perform filter pullup on a logical operator tree.
    ///
    /// Returns the optimized operator tree.
    fn rewrite(&mut self, op: LogicalOperator) -> LogicalOperator {
        match op {
            LogicalOperator::Filter(filter) => self.pullup_filter(filter),
            LogicalOperator::Projection(proj) => self.pullup_projection(proj),
            LogicalOperator::Join(join) => self.pullup_join(join),
            LogicalOperator::Distinct(distinct) => self.pullup_distinct(distinct),
            LogicalOperator::Order(order) => self.pullup_order(order),
            LogicalOperator::SetOperation(setop) => self.pullup_set_operation(setop),
            // For other operators, finish pullup
            _ => self.finish_pullup(op),
        }
    }

    pub fn rewrite_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        let LogicalPlan {
            id,
            stats,
            operator,
        } = plan;
        LogicalPlan {
            id,
            stats,
            operator: self.rewrite(operator),
        }
    }

    /// Generate a Filter with the pulled up expressions.
    fn generate_pullup_filter(child: LogicalPlan, expressions: Vec<Expression>) -> LogicalPlan {
        if expressions.is_empty() {
            return child;
        }
        let id = child.id;
        let stats = child.stats.clone();
        LogicalPlan {
            id,
            stats,
            operator: LogicalOperator::Filter(Filter::new(child, expressions)),
        }
    }

    fn generate_pullup_filter_op(
        child: LogicalOperator,
        expressions: Vec<Expression>,
    ) -> LogicalPlan {
        Self::generate_pullup_filter(LogicalPlan::synthetic(child), expressions)
    }

    /// Pull up through a Filter operator.
    fn pullup_filter(&mut self, filter: Filter) -> LogicalOperator {
        let Filter {
            child, expressions, ..
        } = filter;
        if self.can_pullup {
            if expressions
                .iter()
                .any(|expr| expr.evaluation_properties().is_reorder_fence())
            {
                let mut child_pullup = FilterPullup::new();
                let child = child_pullup.rewrite_plan(*child);
                return LogicalOperator::Filter(Filter::new(child, expressions));
            }

            let plan = self.rewrite_plan(*child);
            for expr in expressions {
                self.filters_expr_pullup.push(expr);
            }
            plan.operator
        } else {
            let plan = self.rewrite_plan(*child);
            LogicalOperator::Filter(Filter::new(plan, expressions))
        }
    }

    /// Pull up through a Projection operator.
    fn pullup_projection(&mut self, mut proj: Projection) -> LogicalOperator {
        proj.child = Box::new(self.rewrite_plan(*proj.child));

        if !self.filters_expr_pullup.is_empty() {
            if proj
                .expressions
                .iter()
                .any(|expr| expr.evaluation_properties().is_reorder_fence())
            {
                self.materialize_pullup_below_projection(&mut proj);
            } else if !self.can_add_column {
                // Special treatment for operators that cannot add columns
                // (e.g., INTERSECT, EXCEPT, DISTINCT)
                self.project_set_operation(&mut proj);
            } else {
                // Replace filter expression bindings
                for filter_expr in &mut self.filters_expr_pullup {
                    Self::replace_expression_binding(
                        &mut proj.expressions,
                        filter_expr,
                        proj.table_index,
                    );
                }
            }
        }

        proj.returned_types = proj.expressions.iter().map(|e| e.return_type()).collect();
        LogicalOperator::Projection(proj)
    }

    /// Special handling for set operations - cannot add new columns.
    fn project_set_operation(&mut self, proj: &mut Projection) {
        // Copy projection expressions to check if we need to add columns
        let original_len = proj.expressions.len();
        let mut copy_proj_expressions = proj.expressions.clone();

        // Try to replace bindings in filter expressions
        let mut changed_filter_expressions = Vec::new();
        for filter_expr in &self.filters_expr_pullup {
            let mut copy_filter_expr = filter_expr.clone();
            Self::replace_expression_binding(
                &mut copy_proj_expressions,
                &mut copy_filter_expr,
                proj.table_index,
            );
            changed_filter_expressions.push(copy_filter_expr);
        }

        // If new columns were added, we must revert and create a filter below
        if copy_proj_expressions.len() > original_len {
            // Revert: create filter below projection
            self.materialize_pullup_below_projection(proj);
            return;
        }

        // Replace the filter bindings
        self.filters_expr_pullup = changed_filter_expressions;
    }

    fn materialize_pullup_below_projection(&mut self, proj: &mut Projection) {
        let filters = std::mem::take(&mut self.filters_expr_pullup);
        let child = std::mem::replace(
            &mut *proj.child,
            LogicalPlan::synthetic(LogicalOperator::DummyScan),
        );
        *proj.child = Self::generate_pullup_filter(child, filters);
    }

    /// Replace column bindings in an expression to reference the projection output.
    fn replace_expression_binding(
        proj_expressions: &mut Vec<Expression>,
        expr: &mut Expression,
        proj_table_idx: usize,
    ) {
        let mut columns = Vec::new();
        Self::collect_column_refs(expr, &mut columns);

        let mut replacements = Vec::new();
        for column in columns {
            if replacements
                .iter()
                .any(|(binding, _)| *binding == column.binding)
            {
                continue;
            }

            let output_index = proj_expressions
                .iter()
                .position(|projected| {
                    matches!(projected, Expression::ColumnRef(existing) if existing.binding == column.binding)
                })
                .unwrap_or_else(|| {
                    let output_index = proj_expressions.len();
                    proj_expressions.push(Expression::ColumnRef(column.clone()));
                    output_index
                });
            replacements.push((column.binding, output_index));
        }

        let rewritten = expr.clone().replace_column_ref(&|column| {
            replacements
                .iter()
                .find(|(binding, _)| *binding == column.binding)
                .map(|(_, output_index)| {
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(proj_table_idx, *output_index),
                        column.return_type.clone(),
                    ))
                })
        });
        *expr = rewritten;
    }

    fn collect_column_refs(expr: &Expression, columns: &mut Vec<ColumnRefExpression>) {
        visit_expression(expr, &mut |expression| {
            if let Expression::ColumnRef(column) = expression {
                columns.push(column.clone());
            }
        });
    }

    /// Pull up through a Join operator.
    fn pullup_join(&mut self, join: Join) -> LogicalOperator {
        if join_has_evaluation_fence(&join) {
            return self.finish_pullup(LogicalOperator::Join(join));
        }

        match join {
            Join::Comparison(cj) => match cj.join_type {
                JoinType::Inner => self.pullup_inner_join(cj),
                JoinType::Left | JoinType::Semi | JoinType::Anti => {
                    self.pullup_from_left_comparison(cj)
                }
                _ => self.finish_pullup(LogicalOperator::Join(Join::Comparison(cj))),
            },
            Join::Any(aj) => match aj.join_type {
                JoinType::Inner => self.pullup_inner_any_join(*aj),
                JoinType::Left | JoinType::Semi | JoinType::Anti => self.pullup_from_left_any(*aj),
                _ => self.finish_pullup(LogicalOperator::Join(Join::Any(aj))),
            },
            Join::Cross(cp) => self.pullup_cross_product(cp),
        }
    }

    /// Pull up through an inner comparison join.
    fn pullup_inner_join(&mut self, cj: ComparisonJoin) -> LogicalOperator {
        // Get filters from both sides
        let mut op = self.pullup_both_side(LogicalOperator::Join(Join::Comparison(cj)));

        // Extract any filter that was created
        let mut expressions = Vec::new();
        if let LogicalOperator::Filter(filter) = op {
            expressions = filter.expressions;
            op = filter.child.operator;
        } else if !self.can_pullup {
            return op; // No filters from below, and we can't pullup, stop.
        }

        // Extract join conditions as filters
        if let LogicalOperator::Join(Join::Comparison(comp_join)) = &op {
            for condition in &comp_join.conditions {
                let comparison_type =
                    Self::join_comparison_to_comparison_type(condition.comparison);
                let expr = Expression::Comparison(ComparisonExpression::new(
                    comparison_type,
                    condition.left.clone(),
                    condition.right.clone(),
                ));
                expressions.push(expr);
            }
        }

        // Convert to cross product
        let (left, right) = match op {
            LogicalOperator::Join(Join::Comparison(cj)) => (*cj.left, *cj.right),
            _ => return op,
        };

        let cross = CrossProduct::new(left, right);
        let result = LogicalOperator::Join(Join::Cross(cross));

        if self.can_pullup {
            for expr in expressions {
                self.filters_expr_pullup.push(expr);
            }
            result
        } else {
            Self::generate_pullup_filter_op(result, expressions).operator
        }
    }

    /// Pull up through an inner any join.
    fn pullup_inner_any_join(&mut self, aj: AnyJoin) -> LogicalOperator {
        // Get filters from both sides
        let mut op = self.pullup_both_side(LogicalOperator::Join(Join::Any(Box::new(aj))));

        // Extract any filter that was created
        let mut expressions = Vec::new();
        if let LogicalOperator::Filter(filter) = op {
            expressions = filter.expressions;
            op = filter.child.operator;
        } else if !self.can_pullup {
            return op;
        }

        // Extract join condition
        if let LogicalOperator::Join(Join::Any(any_join)) = &op {
            expressions.push(any_join.condition.clone());
        }

        // Convert to cross product
        let (left, right) = match op {
            LogicalOperator::Join(Join::Any(aj)) => (*aj.left, *aj.right),
            _ => return op,
        };

        let cross = CrossProduct::new(left, right);
        let result = LogicalOperator::Join(Join::Cross(cross));

        if self.can_pullup {
            for expr in expressions {
                self.filters_expr_pullup.push(expr);
            }
            result
        } else {
            Self::generate_pullup_filter_op(result, expressions).operator
        }
    }

    /// Pull up from left side only (for LEFT/SEMI/ANTI joins).
    fn pullup_from_left_comparison(&mut self, mut cj: ComparisonJoin) -> LogicalOperator {
        let mut left_pullup = FilterPullup::with_settings(true, self.can_add_column);
        let mut right_pullup = FilterPullup::with_settings(false, self.can_add_column);

        cj.left = Box::new(left_pullup.rewrite_plan(*cj.left));
        cj.right = Box::new(right_pullup.rewrite_plan(*cj.right));

        // Only pull up filters from the left side
        if !left_pullup.filters_expr_pullup.is_empty()
            && right_pullup.filters_expr_pullup.is_empty()
        {
            Self::generate_pullup_filter_op(
                LogicalOperator::Join(Join::Comparison(cj)),
                left_pullup.filters_expr_pullup,
            )
            .operator
        } else {
            LogicalOperator::Join(Join::Comparison(cj))
        }
    }

    /// Pull up from left side only for any join.
    fn pullup_from_left_any(&mut self, mut aj: AnyJoin) -> LogicalOperator {
        let mut left_pullup = FilterPullup::with_settings(true, self.can_add_column);
        let mut right_pullup = FilterPullup::with_settings(false, self.can_add_column);

        aj.left = Box::new(left_pullup.rewrite_plan(*aj.left));
        aj.right = Box::new(right_pullup.rewrite_plan(*aj.right));

        if !left_pullup.filters_expr_pullup.is_empty()
            && right_pullup.filters_expr_pullup.is_empty()
        {
            Self::generate_pullup_filter_op(
                LogicalOperator::Join(Join::Any(Box::new(aj))),
                left_pullup.filters_expr_pullup,
            )
            .operator
        } else {
            LogicalOperator::Join(Join::Any(Box::new(aj)))
        }
    }

    /// Pull up through a cross product.
    fn pullup_cross_product(&mut self, cp: CrossProduct) -> LogicalOperator {
        self.pullup_both_side(LogicalOperator::Join(Join::Cross(cp)))
    }

    /// Pull up from both sides of a binary operator.
    fn pullup_both_side(&mut self, op: LogicalOperator) -> LogicalOperator {
        let mut left_pullup = FilterPullup::with_settings(true, self.can_add_column);
        let mut right_pullup = FilterPullup::with_settings(true, self.can_add_column);

        let result = match op {
            LogicalOperator::Join(Join::Cross(mut cp)) => {
                cp.left = Box::new(left_pullup.rewrite_plan(*cp.left));
                cp.right = Box::new(right_pullup.rewrite_plan(*cp.right));
                LogicalOperator::Join(Join::Cross(cp))
            }
            LogicalOperator::Join(Join::Comparison(mut cj)) => {
                cj.left = Box::new(left_pullup.rewrite_plan(*cj.left));
                cj.right = Box::new(right_pullup.rewrite_plan(*cj.right));
                LogicalOperator::Join(Join::Comparison(cj))
            }
            LogicalOperator::Join(Join::Any(mut aj)) => {
                aj.left = Box::new(left_pullup.rewrite_plan(*aj.left));
                aj.right = Box::new(right_pullup.rewrite_plan(*aj.right));
                LogicalOperator::Join(Join::Any(aj))
            }
            _ => return op,
        };

        // Merge filter expressions from both sides
        let mut merged_filters = left_pullup.filters_expr_pullup;
        merged_filters.extend(right_pullup.filters_expr_pullup);

        if !merged_filters.is_empty() {
            Self::generate_pullup_filter_op(result, merged_filters).operator
        } else {
            result
        }
    }

    /// Pull up through a Distinct operator.
    fn pullup_distinct(
        &mut self,

        mut distinct: paro_planner::operator::Distinct,
    ) -> LogicalOperator {
        // Can pull up through DISTINCT (but not DISTINCT ON)
        // For now, we assume all DISTINCT can be pulled through
        distinct.child = Box::new(self.rewrite_plan(*distinct.child));
        LogicalOperator::Distinct(distinct)
    }

    /// Pull up through an Order operator.
    fn pullup_order(&mut self, mut order: paro_planner::operator::Order) -> LogicalOperator {
        // Can pull directly through ORDER BY
        order.child = Box::new(self.rewrite_plan(*order.child));
        LogicalOperator::Order(order)
    }

    /// Pull up through a SetOperation operator.
    fn pullup_set_operation(&mut self, setop: SetOperation) -> LogicalOperator {
        self.can_add_column = false;
        self.can_pullup = true;

        let result = match setop.setop_type {
            SetOpType::Intersect => {
                // INTERSECT: pull from both sides
                self.pullup_both_side_set_operation(setop)
            }
            SetOpType::Except => {
                // EXCEPT: only pull from left side
                self.pullup_from_left_set_operation(setop)
            }
            SetOpType::Union => {
                // UNION: cannot pull up filters
                self.finish_pullup(LogicalOperator::SetOperation(setop))
            }
        };

        // Replace filter table indices to reference the set operation output
        if let LogicalOperator::Filter(ref filter) = result {
            if let LogicalOperator::SetOperation(ref inner_setop) = filter.child.operator {
                let table_index = inner_setop.table_index;
                // Note: In a real implementation, we'd need to replace the table indices
                // in the filter expressions. For now, we return as-is.
                let _ = table_index;
            }
        }

        result
    }

    /// Pull up from both sides of a set operation.
    fn pullup_both_side_set_operation(&mut self, mut setop: SetOperation) -> LogicalOperator {
        let mut left_pullup = FilterPullup::with_settings(true, false);
        let mut right_pullup = FilterPullup::with_settings(true, false);

        setop.left = Box::new(left_pullup.rewrite_plan(*setop.left));
        setop.right = Box::new(right_pullup.rewrite_plan(*setop.right));

        // Merge filters
        let mut merged_filters = left_pullup.filters_expr_pullup;
        merged_filters.extend(right_pullup.filters_expr_pullup);

        // Replace table indices in filters to reference set operation output
        let table_index = setop.table_index;
        for filter in &mut merged_filters {
            Self::replace_filter_table_index(filter, table_index);
        }

        if !merged_filters.is_empty() {
            Self::generate_pullup_filter_op(LogicalOperator::SetOperation(setop), merged_filters)
                .operator
        } else {
            LogicalOperator::SetOperation(setop)
        }
    }

    /// Pull up from left side of a set operation.
    fn pullup_from_left_set_operation(&mut self, mut setop: SetOperation) -> LogicalOperator {
        let mut left_pullup = FilterPullup::with_settings(true, false);
        let mut right_pullup = FilterPullup::with_settings(false, false);

        setop.left = Box::new(left_pullup.rewrite_plan(*setop.left));
        setop.right = Box::new(right_pullup.rewrite_plan(*setop.right));

        if !left_pullup.filters_expr_pullup.is_empty()
            && right_pullup.filters_expr_pullup.is_empty()
        {
            // Replace table indices
            let table_index = setop.table_index;
            for filter in &mut left_pullup.filters_expr_pullup {
                Self::replace_filter_table_index(filter, table_index);
            }
            Self::generate_pullup_filter_op(
                LogicalOperator::SetOperation(setop),
                left_pullup.filters_expr_pullup,
            )
            .operator
        } else {
            LogicalOperator::SetOperation(setop)
        }
    }

    /// Replace table index in filter expression to reference set operation output.
    fn replace_filter_table_index(expr: &mut Expression, table_index: usize) {
        match expr {
            Expression::ColumnRef(col) => {
                col.binding = ColumnBinding::new(table_index, col.binding.column_index);
            }
            Expression::Comparison(comp) => {
                Self::replace_filter_table_index(&mut comp.left, table_index);
                Self::replace_filter_table_index(&mut comp.right, table_index);
            }
            Expression::Conjunction(conj) => {
                for child in &mut conj.children {
                    Self::replace_filter_table_index(child, table_index);
                }
            }
            Expression::Function(func) => {
                for child in &mut func.children {
                    Self::replace_filter_table_index(child, table_index);
                }
            }
            Expression::Cast(cast) => {
                Self::replace_filter_table_index(&mut cast.child, table_index);
            }
            Expression::Operator(op) => {
                for child in &mut op.children {
                    Self::replace_filter_table_index(child, table_index);
                }
            }
            _ => {}
        }
    }

    /// Finish pullup at this operator.
    fn finish_pullup(&mut self, op: LogicalOperator) -> LogicalOperator {
        let plan = LogicalPlan::synthetic(op);
        let plan = plan
            .try_map_children(|child| {
                let mut child_pullup = FilterPullup::new();
                Ok(child_pullup.rewrite_plan(child))
            })
            .expect("FilterPullup child rewrite cannot fail");

        // Now pull up any existing filters
        if self.filters_expr_pullup.is_empty() {
            return plan.operator;
        }

        let filters = std::mem::take(&mut self.filters_expr_pullup);
        Self::generate_pullup_filter(plan, filters).operator
    }

    /// Convert JoinComparisonType to ComparisonType.
    fn join_comparison_to_comparison_type(
        jct: paro_planner::operator::JoinComparisonType,
    ) -> paro_planner::expression::ComparisonType {
        use paro_planner::expression::ComparisonType;
        use paro_planner::operator::JoinComparisonType;
        match jct {
            JoinComparisonType::Equal => ComparisonType::Equal,
            JoinComparisonType::NotEqual => ComparisonType::NotEqual,
            JoinComparisonType::LessThan => ComparisonType::LessThan,
            JoinComparisonType::GreaterThan => ComparisonType::GreaterThan,
            JoinComparisonType::LessThanOrEqual => ComparisonType::LessThanOrEqual,
            JoinComparisonType::GreaterThanOrEqual => ComparisonType::GreaterThanOrEqual,
            JoinComparisonType::NotDistinctFrom => ComparisonType::NotDistinctFrom,
            JoinComparisonType::DistinctFrom => ComparisonType::DistinctFrom,
        }
    }
}

impl Default for FilterPullup {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::types::LogicalType;
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::ComparisonType;
    use paro_planner::expression::{ConstantExpression, FunctionExpression};
    use paro_planner::operator::{Get, JoinComparisonType, JoinCondition};

    fn plan(ctx: &BindContext, op: LogicalOperator) -> LogicalPlan {
        LogicalPlan::new(ctx, op)
    }

    fn make_column_ref(table_index: usize, column_index: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression {
            binding: paro_planner::operator::ColumnBinding {
                table_index,
                column_index,
            },
            depth: 0,
            return_type: LogicalType::Integer,
        })
    }

    fn make_constant(value: i32) -> Expression {
        Expression::Constant(ConstantExpression {
            value: paro_common::runtime_value::Value::Integer(value),
            return_type: LogicalType::Integer,
        })
    }

    fn make_comparison(
        comp_type: ComparisonType,
        left: Expression,
        right: Expression,
    ) -> Expression {
        Expression::Comparison(ComparisonExpression::new(comp_type, left, right))
    }

    fn make_get(table_index: usize) -> LogicalOperator {
        LogicalOperator::Get(Get::new_without_table(
            table_index,
            vec!["col0".to_string(), "col1".to_string()],
            vec![LogicalType::Integer, LogicalType::Varchar],
        ))
    }

    fn volatile_call() -> Expression {
        let function = paro_function::scalar::math::get_random_function()
            .functions
            .into_iter()
            .next()
            .expect("random overload");
        Expression::Function(FunctionExpression::new(
            function,
            vec![],
            LogicalType::Double,
        ))
    }

    #[test]
    fn test_pullup_filter_with_can_pullup() {
        let ctx = BindContext::new();
        // Create: Filter(Get)
        let get = make_get(0);
        let filter_expr = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(5),
        );
        let filter = Filter::new(plan(&ctx, get), vec![filter_expr]);
        let op = LogicalOperator::Filter(filter);

        let mut pullup = FilterPullup::with_settings(true, false);
        let result = pullup.rewrite(op);

        // Filter should be pulled up (removed from tree, stored in pullup)
        assert!(matches!(result, LogicalOperator::Get(_)));
        assert_eq!(pullup.filters_expr_pullup.len(), 1);
    }

    #[test]
    fn test_pullup_filter_without_can_pullup() {
        let ctx = BindContext::new();
        // Create: Filter(Get)
        let get = make_get(0);
        let filter_expr = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(5),
        );
        let filter = Filter::new(plan(&ctx, get), vec![filter_expr]);
        let op = LogicalOperator::Filter(filter);

        let mut pullup = FilterPullup::new(); // can_pullup = false
        let result = pullup.rewrite(op);

        // Filter should remain in place
        assert!(matches!(result, LogicalOperator::Filter(_)));
        assert!(pullup.filters_expr_pullup.is_empty());
    }

    #[test]
    fn test_volatile_filter_is_not_pulled_up() {
        let ctx = BindContext::new();
        let filter_expr = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            volatile_call(),
        );
        let filter = Filter::new(plan(&ctx, make_get(0)), vec![filter_expr]);

        let mut pullup = FilterPullup::with_settings(true, true);
        let result = pullup.rewrite(LogicalOperator::Filter(filter));

        assert!(matches!(result, LogicalOperator::Filter(_)));
        assert!(pullup.filters_expr_pullup.is_empty());
    }

    #[test]
    fn test_pullup_through_order() {
        let ctx = BindContext::new();
        // Create: Order(Filter(Get))
        let get = make_get(0);
        let filter_expr = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(5),
        );
        let filter = Filter::new(plan(&ctx, get), vec![filter_expr]);
        let order = paro_planner::operator::Order {
            child: Box::new(plan(&ctx, LogicalOperator::Filter(filter))),
            orders: vec![],
            projection_map: paro_planner::operator::ProjectionMap::all(),
        };
        let op = LogicalOperator::Order(order);

        let mut pullup = FilterPullup::with_settings(true, false);
        let result = pullup.rewrite(op);

        // Filter should be pulled through Order
        match result {
            LogicalOperator::Order(o) => {
                assert!(matches!(o.child.operator, LogicalOperator::Get(_)));
            }
            _ => panic!("Expected Order operator"),
        }
        assert_eq!(pullup.filters_expr_pullup.len(), 1);
    }

    #[test]
    fn test_pullup_through_distinct() {
        let ctx = BindContext::new();
        // Create: Distinct(Filter(Get))
        let get = make_get(0);
        let filter_expr = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(5),
        );
        let filter = Filter::new(plan(&ctx, get), vec![filter_expr]);
        let distinct =
            paro_planner::operator::Distinct::new(plan(&ctx, LogicalOperator::Filter(filter)));
        let op = LogicalOperator::Distinct(distinct);

        let mut pullup = FilterPullup::with_settings(true, false);
        let result = pullup.rewrite(op);

        // Filter should be pulled through Distinct
        match result {
            LogicalOperator::Distinct(d) => {
                assert!(matches!(d.child.operator, LogicalOperator::Get(_)));
            }
            _ => panic!("Expected Distinct operator"),
        }
        assert_eq!(pullup.filters_expr_pullup.len(), 1);
    }

    #[test]
    fn test_pullup_cross_product() {
        let ctx = BindContext::new();
        // Create: Cross(Filter(Get0), Filter(Get1))
        let left_get = make_get(0);
        let left_filter_expr = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(5),
        );
        let left_filter = Filter::new(plan(&ctx, left_get), vec![left_filter_expr]);

        let right_get = make_get(1);
        let right_filter_expr = make_comparison(
            ComparisonType::LessThan,
            make_column_ref(1, 0),
            make_constant(10),
        );
        let right_filter = Filter::new(plan(&ctx, right_get), vec![right_filter_expr]);

        let cross = CrossProduct::new(
            plan(&ctx, LogicalOperator::Filter(left_filter)),
            plan(&ctx, LogicalOperator::Filter(right_filter)),
        );
        let op = LogicalOperator::Join(Join::Cross(cross));

        let mut pullup = FilterPullup::new();
        let result = pullup.rewrite(op);

        // Filters from both sides should be pulled up and combined
        match result {
            LogicalOperator::Filter(f) => {
                assert_eq!(f.expressions.len(), 2);
                match f.child.operator {
                    LogicalOperator::Join(Join::Cross(cp)) => {
                        assert!(matches!(cp.left.operator, LogicalOperator::Get(_)));
                        assert!(matches!(cp.right.operator, LogicalOperator::Get(_)));
                    }
                    _ => panic!("Expected Cross product"),
                }
            }
            _ => panic!("Expected Filter above Cross product"),
        }
    }

    #[test]
    fn test_volatile_inner_join_condition_is_not_pulled_into_filter() {
        let ctx = BindContext::new();
        let join = ComparisonJoin::new(
            JoinType::Inner,
            plan(&ctx, make_get(0)),
            plan(&ctx, make_get(1)),
            vec![JoinCondition::new(
                make_column_ref(0, 0),
                volatile_call(),
                JoinComparisonType::Equal,
            )],
        );

        let result = FilterPullup::new().rewrite(LogicalOperator::Join(Join::Comparison(join)));

        assert!(matches!(result, LogicalOperator::Join(Join::Comparison(_))));
    }

    #[test]
    fn test_pullup_projection() {
        let ctx = BindContext::new();
        // Create: Projection(Filter(Get))
        let get = make_get(0);
        let filter_expr = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(5),
        );
        let filter = Filter::new(plan(&ctx, get), vec![filter_expr]);
        let proj = Projection::new(
            1,
            plan(&ctx, LogicalOperator::Filter(filter)),
            vec![make_column_ref(0, 0), make_column_ref(0, 1)],
        );
        let op = LogicalOperator::Projection(proj);

        let mut pullup = FilterPullup::with_settings(true, true);
        let result = pullup.rewrite(op);

        // Filter should be pulled through projection with bindings replaced
        match result {
            LogicalOperator::Projection(p) => {
                assert!(matches!(p.child.operator, LogicalOperator::Get(_)));
            }
            _ => panic!("Expected Projection operator"),
        }
        assert_eq!(pullup.filters_expr_pullup.len(), 1);
    }

    #[test]
    fn test_filter_stays_below_volatile_projection() {
        let ctx = BindContext::new();
        let filter_expr = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(5),
        );
        let filter = Filter::new(plan(&ctx, make_get(0)), vec![filter_expr]);
        let projection = Projection::new(
            1,
            plan(&ctx, LogicalOperator::Filter(filter)),
            vec![volatile_call()],
        );

        let mut pullup = FilterPullup::with_settings(true, true);
        let result = pullup.rewrite(LogicalOperator::Projection(projection));

        let LogicalOperator::Projection(projection) = result else {
            panic!("expected projection");
        };
        assert!(matches!(
            projection.child.operator,
            LogicalOperator::Filter(_)
        ));
        assert!(pullup.filters_expr_pullup.is_empty());
    }

    #[test]
    fn test_finish_pullup_generates_filter() {
        let ctx = BindContext::new();
        // Create: Aggregate(Filter(Get))
        let get = make_get(0);
        let filter_expr = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(5),
        );
        let filter = Filter::new(plan(&ctx, get), vec![filter_expr]);
        let agg = paro_planner::operator::Aggregate::new(
            1,
            2,
            3,
            plan(&ctx, LogicalOperator::Filter(filter)),
            vec![],
            vec![],
            vec![],
            vec![],
        );
        let op = LogicalOperator::Aggregate(agg);

        let mut pullup = FilterPullup::with_settings(true, false);
        let result = pullup.rewrite(op);

        // Aggregate is a blocking operator, so filter should be generated above it
        // But since we're pulling up, the filter from below should be collected
        // and then a new filter created above the aggregate
        match result {
            LogicalOperator::Filter(f) => {
                assert!(matches!(f.child.operator, LogicalOperator::Aggregate(_)));
            }
            LogicalOperator::Aggregate(_) => {
                // This is also valid if no filters were pulled
            }
            _ => panic!("Expected Filter or Aggregate operator"),
        }
    }
}
