// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Push filter predicates as close to data sources as semantics allow.

use std::collections::HashSet;

use paro_planner::expression::ComparisonType;
use paro_planner::expression::ConjunctionType;
use paro_planner::expression::{
    ColumnRefExpression, ComparisonExpression, Expression, ExpressionIterator,
};
use paro_planner::operator::empty_result::EmptyResult;
use paro_planner::operator::Filter as PlannerFilter;
use paro_planner::operator::{
    AnyJoin, ComparisonJoin, CrossProduct, Join, JoinSide, JoinType, LogicalOperator, Projection,
};
use paro_planner::plan::LogicalPlan;

use crate::filter::combiner::{FilterCombiner, FilterResult};
/// A filter with its table bindings extracted.
#[derive(Debug)]
pub struct Filter {
    /// The table indices this filter references.
    pub bindings: HashSet<usize>,
    /// The filter expression.
    pub filter: Expression,
}

impl Filter {
    /// Create a new filter from an expression.
    pub fn new(filter: Expression) -> Self {
        let mut f = Self {
            bindings: HashSet::new(),
            filter,
        };
        f.extract_bindings();
        f
    }

    /// Extract table bindings from the filter expression.
    pub fn extract_bindings(&mut self) {
        self.bindings.clear();
        Self::extract_bindings_from_expr(&self.filter, &mut self.bindings);
    }

    /// Recursively extract table bindings from an expression.
    fn extract_bindings_from_expr(expr: &Expression, bindings: &mut HashSet<usize>) {
        if let Expression::ColumnRef(column) = expr {
            bindings.insert(column.binding.table_index);
            return;
        }

        ExpressionIterator::enumerate_children(expr, |child| {
            Self::extract_bindings_from_expr(child, bindings);
        });
    }
}

/// Filter pushdown optimizer.
///
/// Pushes filter predicates down through the logical plan tree,
/// as close to the data sources as possible.
pub struct FilterPushdown {
    /// The filter combiner for merging and simplifying filters.
    combiner: FilterCombiner,
    /// Current set of filters to push down.
    filters: Vec<Filter>,
}

impl FilterPushdown {
    /// Create a new FilterPushdown optimizer.
    pub fn new() -> Self {
        Self {
            combiner: FilterCombiner::new(),
            filters: Vec::new(),
        }
    }

    /// Perform filter pushdown on a logical operator tree.
    ///
    /// Returns the optimized operator tree.
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

    fn rewrite(&mut self, op: LogicalOperator) -> LogicalOperator {
        match op {
            LogicalOperator::Filter(filter) => self.pushdown_filter(filter),
            LogicalOperator::Projection(proj) => self.pushdown_projection(proj),
            LogicalOperator::Join(join) => self.pushdown_join(join),
            LogicalOperator::Aggregate(agg) => self.pushdown_aggregate(agg),
            LogicalOperator::Distinct(distinct) => self.pushdown_distinct(distinct),
            LogicalOperator::Order(order) => self.pushdown_order(order),
            LogicalOperator::Limit(limit) => self.pushdown_limit(limit),
            LogicalOperator::Window(window) => self.pushdown_window(window),
            LogicalOperator::Get(_) => self.finish_pushdown(op),
            LogicalOperator::TableFunctionGet(_) => self.finish_pushdown(op),
            LogicalOperator::EmptyResult(_) => self.finish_pushdown(op),
            LogicalOperator::MaterializedCTE(cte) => self.pushdown_materialized_cte(cte),
            LogicalOperator::RecursiveCTE(cte) => {
                self.finish_pushdown(LogicalOperator::RecursiveCTE(cte))
            }
            LogicalOperator::CTERef(_) => self.finish_pushdown(op),
            LogicalOperator::SetOperation(setop) => self.pushdown_set_operation(setop),
            // Policy default: unknown operators stop pushdown and keep the remaining filters above.
            _ => self.finish_pushdown(op),
        }
    }

    /// Add a filter expression to the pushdown set.
    ///
    /// Returns `FilterResult::Unsatisfiable` if the filter combination is impossible.
    pub fn add_filter(&mut self, expr: Expression) -> FilterResult {
        // First push any existing filters into the combiner
        if self.push_filters() == FilterResult::Unsatisfiable {
            return FilterResult::Unsatisfiable;
        }

        // Split AND predicates
        let expressions = Self::split_predicates(expr);

        // Push each predicate into the combiner
        for child_expr in expressions {
            if self.combiner.add_filter(child_expr) == FilterResult::Unsatisfiable {
                return FilterResult::Unsatisfiable;
            }
        }

        FilterResult::Success
    }

    /// Split an expression by AND predicates.
    fn split_predicates(expr: Expression) -> Vec<Expression> {
        let mut result = Vec::new();
        Self::split_predicates_recursive(expr, &mut result);
        result
    }

    /// Recursively split AND predicates.
    fn split_predicates_recursive(expr: Expression, result: &mut Vec<Expression>) {
        if let Expression::Conjunction(conj) = &expr {
            if conj.conjunction_type == ConjunctionType::And {
                for child in conj.children.clone() {
                    Self::split_predicates_recursive(child, result);
                }
                return;
            }
        }
        result.push(expr);
    }

    /// Push current filters into the combiner.
    fn push_filters(&mut self) -> FilterResult {
        for filter in self.filters.drain(..) {
            let result = self.combiner.add_filter(filter.filter);
            if result == FilterResult::Unsatisfiable {
                return FilterResult::Unsatisfiable;
            }
        }
        FilterResult::Success
    }

    /// Generate filters from the combiner.
    fn generate_filters(&mut self) {
        if !self.filters.is_empty() {
            return;
        }

        let generated = self.combiner.generate_filters();
        for filter_expr in generated {
            self.filters.push(Filter::new(filter_expr));
        }
    }

    /// Create a Filter with the remaining filters.
    fn push_final_filters(&mut self, op: LogicalOperator) -> LogicalOperator {
        if self.filters.is_empty() {
            return op;
        }

        let expressions: Vec<Expression> = self.filters.drain(..).map(|f| f.filter).collect();
        LogicalOperator::Filter(PlannerFilter::new(LogicalPlan::synthetic(op), expressions))
    }

    fn empty_result(plan: LogicalPlan) -> LogicalPlan {
        let id = plan.id;
        let stats = plan.stats.clone();
        LogicalPlan {
            id,
            stats,
            operator: LogicalOperator::EmptyResult(EmptyResult::new(plan)),
        }
    }

    /// Finish pushdown at this operator.
    ///
    /// Recursively pushes down into children, then adds any remaining filters.
    fn finish_pushdown(&mut self, op: LogicalOperator) -> LogicalOperator {
        let plan = LogicalPlan::synthetic(op);
        let plan = plan
            .try_map_children(|child| {
                let mut child_pushdown = FilterPushdown::new();
                Ok(child_pushdown.rewrite_plan(child))
            })
            .expect("FilterPushdown child rewrite cannot fail");

        // Add any remaining filters
        self.push_final_filters(plan.operator)
    }

    fn pushdown_materialized_cte(
        &mut self,
        mut cte: paro_planner::operator::MaterializedCTE,
    ) -> LogicalOperator {
        let mut cte_query_pushdown = FilterPushdown::new();
        cte.cte_query = Box::new(cte_query_pushdown.rewrite_plan(*cte.cte_query));

        // Caller-side filters may be pushed into the main-query child, but they
        // must never cross into the CTE producer subtree.
        cte.child = Box::new(self.rewrite_plan(*cte.child));
        LogicalOperator::MaterializedCTE(cte)
    }

    /// Push down through a Filter operator.
    fn pushdown_filter(&mut self, filter: PlannerFilter) -> LogicalOperator {
        // Add all filter expressions to the pushdown set
        for expr in filter.expressions {
            let result = self.add_filter(expr);
            if result == FilterResult::Unsatisfiable {
                // Filter is unsatisfiable - return empty result
                return Self::empty_result(*filter.child).operator;
            }
        }

        // Generate filters and continue pushing down
        self.generate_filters();
        self.rewrite_plan(*filter.child).operator
    }

    /// Push down through a Projection operator.
    fn pushdown_projection(&mut self, mut proj: Projection) -> LogicalOperator {
        // A projection over a graph operator chain is a GRAPH_TABLE COLUMNS projection.
        // Its expressions must be evaluated by PhysicalGraphProject after late materialization,
        // so pushing filters below it can bind predicates to the wrong (rowid/local-id) columns.
        if proj.child.is_graph_chain() {
            let mut child_pushdown = FilterPushdown::new();
            proj.child = Box::new(child_pushdown.rewrite_plan(*proj.child));
            return self.push_final_filters(LogicalOperator::Projection(proj));
        }

        let mut child_pushdown = FilterPushdown::new();
        let mut remaining_filters = Vec::new();

        for filter in self.filters.drain(..) {
            // Check if filter references volatile expressions
            if Self::is_volatile_through_projection(&proj, &filter.filter) {
                remaining_filters.push(filter.filter);
            } else {
                // Replace projection bindings in the filter
                let replaced = Self::replace_projection_bindings(&proj, filter.filter);
                let result = child_pushdown.add_filter(replaced);
                if result == FilterResult::Unsatisfiable {
                    return LogicalOperator::DummyScan;
                }
            }
        }

        // Generate filters for child
        child_pushdown.generate_filters();

        // Rewrite child
        proj.child = Box::new(child_pushdown.rewrite_plan(*proj.child));

        // Add remaining filters above projection
        let result = LogicalOperator::Projection(proj);
        if remaining_filters.is_empty() {
            result
        } else {
            LogicalOperator::Filter(PlannerFilter::new(
                LogicalPlan::synthetic(result),
                remaining_filters,
            ))
        }
    }

    /// Check if a filter is volatile through a projection.
    fn is_volatile_through_projection(proj: &Projection, expr: &Expression) -> bool {
        if expr.contains_external_routine() {
            return true;
        }
        projection_reference_crosses_execution_boundary(proj, expr)
    }

    /// Replace projection bindings in an expression.
    fn replace_projection_bindings(proj: &Projection, expr: Expression) -> Expression {
        expr.replace_column_ref(&|col: &ColumnRefExpression| {
            if col.binding.table_index == proj.table_index
                && col.binding.column_index < proj.expressions.len()
            {
                Some(proj.expressions[col.binding.column_index].clone())
            } else {
                None
            }
        })
    }

    /// Push down through a Join operator.
    fn pushdown_join(&mut self, join: Join) -> LogicalOperator {
        // Get table bindings for left and right sides
        let left_bindings = Self::get_table_bindings_plan(join.left());
        let right_bindings = Self::get_table_bindings_plan(join.right());

        match join {
            Join::Comparison(cj) => {
                if !cj.duplicate_eliminated_columns.is_empty() {
                    self.pushdown_delim_join(cj, left_bindings, right_bindings)
                } else {
                    match cj.join_type {
                        JoinType::Inner => {
                            self.pushdown_inner_join(cj, left_bindings, right_bindings)
                        }
                        JoinType::Left => {
                            self.pushdown_left_join(cj, left_bindings, right_bindings)
                        }
                        JoinType::Semi | JoinType::Anti => self.pushdown_semi_anti_join(cj),
                        _ => self.finish_pushdown(LogicalOperator::Join(Join::Comparison(cj))),
                    }
                }
            }
            Join::Any(aj) => match aj.join_type {
                JoinType::Inner => self.pushdown_inner_any_join(*aj, left_bindings, right_bindings),
                _ => self.finish_pushdown(LogicalOperator::Join(Join::Any(aj))),
            },
            Join::Cross(cp) => self.pushdown_cross_product(cp, left_bindings, right_bindings),
        }
    }

    /// Get table bindings from a plan subtree.
    fn get_table_bindings_plan(plan: &LogicalPlan) -> HashSet<usize> {
        let mut bindings = HashSet::new();
        Self::collect_table_bindings(&plan.operator, &mut bindings);
        bindings
    }

    /// Recursively collect table bindings.
    fn collect_table_bindings(op: &LogicalOperator, bindings: &mut HashSet<usize>) {
        for idx in op.get_table_index() {
            bindings.insert(idx);
        }
        for child in op.children() {
            Self::collect_table_bindings(&child.operator, bindings);
        }
    }

    /// Push down through an inner comparison join.
    fn pushdown_inner_join(
        &mut self,
        mut cj: ComparisonJoin,
        left_bindings: HashSet<usize>,
        right_bindings: HashSet<usize>,
    ) -> LogicalOperator {
        let range_condition_count = cj
            .conditions
            .iter()
            .filter(|condition| {
                matches!(
                    condition.comparison,
                    paro_planner::operator::JoinComparisonType::LessThan
                        | paro_planner::operator::JoinComparisonType::LessThanOrEqual
                        | paro_planner::operator::JoinComparisonType::GreaterThan
                        | paro_planner::operator::JoinComparisonType::GreaterThanOrEqual
                )
            })
            .count();
        let preserve_inner_comparison = (range_condition_count == 1 || range_condition_count == 2)
            && range_condition_count == cj.conditions.len();

        if !preserve_inner_comparison {
            // Inner join without range-only comparison handling: treat join predicates as filters
            // and fall through to the CrossProduct + Filter path.
            for condition in &cj.conditions {
                let comparison_type = match condition.comparison {
                    paro_planner::operator::JoinComparisonType::Equal => ComparisonType::Equal,
                    paro_planner::operator::JoinComparisonType::NotEqual => {
                        ComparisonType::NotEqual
                    }
                    paro_planner::operator::JoinComparisonType::LessThan => {
                        ComparisonType::LessThan
                    }
                    paro_planner::operator::JoinComparisonType::GreaterThan => {
                        ComparisonType::GreaterThan
                    }
                    paro_planner::operator::JoinComparisonType::LessThanOrEqual => {
                        ComparisonType::LessThanOrEqual
                    }
                    paro_planner::operator::JoinComparisonType::GreaterThanOrEqual => {
                        ComparisonType::GreaterThanOrEqual
                    }
                    paro_planner::operator::JoinComparisonType::NotDistinctFrom => {
                        ComparisonType::NotDistinctFrom
                    }
                    paro_planner::operator::JoinComparisonType::DistinctFrom => {
                        ComparisonType::DistinctFrom
                    }
                };
                let expr = Expression::Comparison(ComparisonExpression::new(
                    comparison_type,
                    condition.left.clone(),
                    condition.right.clone(),
                ));
                if self.add_filter(expr) == FilterResult::Unsatisfiable {
                    return LogicalOperator::DummyScan;
                }
            }

            self.generate_filters();

            let cross = CrossProduct::new(*cj.left, *cj.right);
            return self.pushdown_cross_product(cross, left_bindings, right_bindings);
        }

        let mut left_pushdown = FilterPushdown::new();
        let mut right_pushdown = FilterPushdown::new();
        let mut left_unsat = false;
        let mut right_unsat = false;
        let mut remaining_filters = Vec::new();

        for filter in self.filters.drain(..) {
            let side = Self::get_expression_side(&filter.filter, &left_bindings, &right_bindings);
            match side {
                JoinSide::Left => {
                    if !left_unsat
                        && left_pushdown.add_filter(filter.filter) == FilterResult::Unsatisfiable
                    {
                        left_unsat = true;
                    }
                }
                JoinSide::Right => {
                    if !right_unsat
                        && right_pushdown.add_filter(filter.filter) == FilterResult::Unsatisfiable
                    {
                        right_unsat = true;
                    }
                }
                JoinSide::Both | JoinSide::None => remaining_filters.push(filter.filter),
            }
        }

        let left = if left_unsat {
            Self::empty_result(*cj.left)
        } else {
            left_pushdown.generate_filters();
            left_pushdown.rewrite_plan(*cj.left)
        };
        let right = if right_unsat {
            Self::empty_result(*cj.right)
        } else {
            right_pushdown.generate_filters();
            right_pushdown.rewrite_plan(*cj.right)
        };

        cj.left = Box::new(left);
        cj.right = Box::new(right);

        let result = LogicalOperator::Join(Join::Comparison(cj));
        if remaining_filters.is_empty() {
            result
        } else {
            LogicalOperator::Filter(PlannerFilter::new(
                LogicalPlan::synthetic(result),
                remaining_filters,
            ))
        }
    }

    /// Push down through an inner any join.
    fn pushdown_inner_any_join(
        &mut self,
        aj: AnyJoin,
        left_bindings: HashSet<usize>,
        right_bindings: HashSet<usize>,
    ) -> LogicalOperator {
        // Add the condition as a filter
        if self.add_filter(aj.condition.clone()) == FilterResult::Unsatisfiable {
            return LogicalOperator::DummyScan;
        }

        self.generate_filters();

        // Convert to cross product and push down
        let cross = CrossProduct::new(*aj.left, *aj.right);
        self.pushdown_cross_product(cross, left_bindings, right_bindings)
    }

    /// Push down through a left join.
    fn pushdown_left_join(
        &mut self,
        mut cj: ComparisonJoin,
        left_bindings: HashSet<usize>,
        right_bindings: HashSet<usize>,
    ) -> LogicalOperator {
        // For left join, we can only push filters that reference only the left side
        let mut left_pushdown = FilterPushdown::new();
        let mut remaining_filters = Vec::new();

        for filter in self.filters.drain(..) {
            let side = Self::get_expression_side(&filter.filter, &left_bindings, &right_bindings);
            match side {
                JoinSide::Left => {
                    if left_pushdown.add_filter(filter.filter) == FilterResult::Unsatisfiable {
                        return LogicalOperator::DummyScan;
                    }
                }
                _ => {
                    remaining_filters.push(filter.filter);
                }
            }
        }

        left_pushdown.generate_filters();
        cj.left = Box::new(left_pushdown.rewrite_plan(*cj.left));

        // Recursively push down into right side (without filters)
        let mut right_pushdown = FilterPushdown::new();
        cj.right = Box::new(right_pushdown.rewrite_plan(*cj.right));

        let result = LogicalOperator::Join(Join::Comparison(cj));
        if remaining_filters.is_empty() {
            result
        } else {
            LogicalOperator::Filter(PlannerFilter::new(
                LogicalPlan::synthetic(result),
                remaining_filters,
            ))
        }
    }

    fn pushdown_delim_join(
        &mut self,
        mut cj: ComparisonJoin,
        left_bindings: HashSet<usize>,
        right_bindings: HashSet<usize>,
    ) -> LogicalOperator {
        let mut left_pushdown = FilterPushdown::new();
        let mut right_pushdown = FilterPushdown::new();
        let mut left_unsat = false;
        let mut right_unsat = false;
        let mut remaining_filters = Vec::new();

        for filter in self.filters.drain(..) {
            let side = Self::get_expression_side(&filter.filter, &left_bindings, &right_bindings);
            match side {
                JoinSide::Left => {
                    if !left_unsat
                        && left_pushdown.add_filter(filter.filter) == FilterResult::Unsatisfiable
                    {
                        left_unsat = true;
                    }
                }
                JoinSide::Right => {
                    if !right_unsat
                        && right_pushdown.add_filter(filter.filter) == FilterResult::Unsatisfiable
                    {
                        right_unsat = true;
                    }
                }
                JoinSide::Both | JoinSide::None => remaining_filters.push(filter.filter),
            }
        }

        let left = if left_unsat {
            Self::empty_result(*cj.left)
        } else {
            left_pushdown.generate_filters();
            left_pushdown.rewrite_plan(*cj.left)
        };
        let right = if right_unsat {
            Self::empty_result(*cj.right)
        } else {
            right_pushdown.generate_filters();
            right_pushdown.rewrite_plan(*cj.right)
        };

        cj.left = Box::new(left);
        cj.right = Box::new(right);

        let result = LogicalOperator::Join(Join::Comparison(cj));
        if remaining_filters.is_empty() {
            result
        } else {
            LogicalOperator::Filter(PlannerFilter::new(
                LogicalPlan::synthetic(result),
                remaining_filters,
            ))
        }
    }

    /// Push down through a semi/anti join.
    fn pushdown_semi_anti_join(&mut self, mut cj: ComparisonJoin) -> LogicalOperator {
        // For semi/anti joins, push all filters to the left side
        let mut left_pushdown = FilterPushdown::new();
        for filter in self.filters.drain(..) {
            if left_pushdown.add_filter(filter.filter) == FilterResult::Unsatisfiable {
                return LogicalOperator::DummyScan;
            }
        }

        left_pushdown.generate_filters();
        cj.left = Box::new(left_pushdown.rewrite_plan(*cj.left));

        // Recursively push down into right side
        let mut right_pushdown = FilterPushdown::new();
        cj.right = Box::new(right_pushdown.rewrite_plan(*cj.right));

        LogicalOperator::Join(Join::Comparison(cj))
    }

    /// Push down through a cross product.
    fn pushdown_cross_product(
        &mut self,
        cp: CrossProduct,
        left_bindings: HashSet<usize>,
        right_bindings: HashSet<usize>,
    ) -> LogicalOperator {
        let mut left_pushdown = FilterPushdown::new();
        let mut right_pushdown = FilterPushdown::new();
        let mut join_filters = Vec::new();

        for filter in self.filters.drain(..) {
            let side = Self::get_expression_side(&filter.filter, &left_bindings, &right_bindings);
            match side {
                JoinSide::Left => {
                    if left_pushdown.add_filter(filter.filter) == FilterResult::Unsatisfiable {
                        return LogicalOperator::DummyScan;
                    }
                }
                JoinSide::Right => {
                    if right_pushdown.add_filter(filter.filter) == FilterResult::Unsatisfiable {
                        return LogicalOperator::DummyScan;
                    }
                }
                JoinSide::Both | JoinSide::None => {
                    join_filters.push(filter.filter);
                }
            }
        }

        left_pushdown.generate_filters();
        right_pushdown.generate_filters();

        let left = left_pushdown.rewrite_plan(*cp.left);
        let right = right_pushdown.rewrite_plan(*cp.right);

        let result = LogicalOperator::Join(Join::Cross(CrossProduct::new(left, right)));

        if join_filters.is_empty() {
            result
        } else {
            LogicalOperator::Filter(PlannerFilter::new(
                LogicalPlan::synthetic(result),
                join_filters,
            ))
        }
    }

    /// Get which side of a join an expression references.
    fn get_expression_side(
        expr: &Expression,
        left_bindings: &HashSet<usize>,
        right_bindings: &HashSet<usize>,
    ) -> JoinSide {
        let mut bindings = HashSet::new();
        Filter::extract_bindings_from_expr(expr, &mut bindings);

        let mut side = JoinSide::None;
        for binding in bindings {
            let binding_side = JoinSide::get_side(binding, left_bindings, right_bindings);
            side = JoinSide::combine(side, binding_side);
        }
        side
    }

    /// Push down through an Aggregate operator.
    fn pushdown_aggregate(
        &mut self,
        mut agg: paro_planner::operator::Aggregate,
    ) -> LogicalOperator {
        let mut child_pushdown = FilterPushdown::new();
        let mut remaining_filters = Vec::new();

        // Get the aggregate's table index
        let agg_index = agg.aggregate_index;
        let groupings_index = agg.groupings_index;

        for filter in self.filters.drain(..) {
            // Check if filter references the aggregate output
            let references_aggregate =
                filter.bindings.contains(&agg_index) || filter.bindings.contains(&groupings_index);

            if references_aggregate {
                // Cannot push down filters that reference aggregate results
                remaining_filters.push(filter.filter);
            } else {
                // Can push down filters that only reference group-by columns
                if child_pushdown.add_filter(filter.filter) == FilterResult::Unsatisfiable {
                    return LogicalOperator::DummyScan;
                }
            }
        }

        child_pushdown.generate_filters();
        agg.child = Box::new(child_pushdown.rewrite_plan(*agg.child));

        let result = LogicalOperator::Aggregate(agg);
        if remaining_filters.is_empty() {
            result
        } else {
            LogicalOperator::Filter(PlannerFilter::new(
                LogicalPlan::synthetic(result),
                remaining_filters,
            ))
        }
    }

    /// Push down through a Distinct operator.
    fn pushdown_distinct(
        &mut self,
        mut distinct: paro_planner::operator::Distinct,
    ) -> LogicalOperator {
        // Distinct passes through all filters
        let mut child_pushdown = FilterPushdown::new();

        for filter in self.filters.drain(..) {
            if child_pushdown.add_filter(filter.filter) == FilterResult::Unsatisfiable {
                return LogicalOperator::DummyScan;
            }
        }

        child_pushdown.generate_filters();
        distinct.child = Box::new(child_pushdown.rewrite_plan(*distinct.child));

        LogicalOperator::Distinct(distinct)
    }

    /// Push down through an Order operator.
    fn pushdown_order(&mut self, mut order: paro_planner::operator::Order) -> LogicalOperator {
        // Order passes through all filters
        let mut child_pushdown = FilterPushdown::new();

        for filter in self.filters.drain(..) {
            if child_pushdown.add_filter(filter.filter) == FilterResult::Unsatisfiable {
                return LogicalOperator::DummyScan;
            }
        }

        child_pushdown.generate_filters();
        order.child = Box::new(child_pushdown.rewrite_plan(*order.child));

        LogicalOperator::Order(order)
    }

    /// Push down through a Limit operator.
    fn pushdown_limit(&mut self, mut limit: paro_planner::operator::Limit) -> LogicalOperator {
        // Limit passes through all filters
        // Note: This is safe because filters don't change the order of rows
        let mut child_pushdown = FilterPushdown::new();

        for filter in self.filters.drain(..) {
            if child_pushdown.add_filter(filter.filter) == FilterResult::Unsatisfiable {
                return LogicalOperator::DummyScan;
            }
        }

        child_pushdown.generate_filters();
        limit.child = Box::new(child_pushdown.rewrite_plan(*limit.child));

        LogicalOperator::Limit(limit)
    }

    /// Push down through a Window operator.
    fn pushdown_window(&mut self, mut window: paro_planner::operator::Window) -> LogicalOperator {
        let mut child_pushdown = FilterPushdown::new();
        let mut remaining_filters = Vec::new();

        // Get the window's table index
        let window_index = window.window_index;

        for filter in self.filters.drain(..) {
            // Check if filter references the window output
            let references_window = filter.bindings.contains(&window_index);

            if references_window {
                // Cannot push down filters that reference window results
                remaining_filters.push(filter.filter);
            } else {
                // Can push down filters that don't reference window results
                if child_pushdown.add_filter(filter.filter) == FilterResult::Unsatisfiable {
                    return LogicalOperator::DummyScan;
                }
            }
        }

        child_pushdown.generate_filters();
        window.child = Box::new(child_pushdown.rewrite_plan(*window.child));

        let result = LogicalOperator::Window(window);
        if remaining_filters.is_empty() {
            result
        } else {
            LogicalOperator::Filter(PlannerFilter::new(
                LogicalPlan::synthetic(result),
                remaining_filters,
            ))
        }
    }

    /// Push down through a SetOperation operator.
    fn pushdown_set_operation(
        &mut self,
        mut setop: paro_planner::operator::SetOperation,
    ) -> LogicalOperator {
        // For set operations, we cannot push filters through
        // because the semantics change (UNION removes duplicates, etc.)
        // Just finish pushdown here

        // Recursively push down into children
        let mut left_pushdown = FilterPushdown::new();
        let mut right_pushdown = FilterPushdown::new();

        setop.left = Box::new(left_pushdown.rewrite_plan(*setop.left));
        setop.right = Box::new(right_pushdown.rewrite_plan(*setop.right));

        let result = LogicalOperator::SetOperation(setop);
        self.push_final_filters(result)
    }
}

fn projection_reference_crosses_execution_boundary(proj: &Projection, expr: &Expression) -> bool {
    if let Expression::ColumnRef(column) = expr {
        return column.binding.table_index == proj.table_index
            && proj
                .expressions
                .get(column.binding.column_index)
                .is_some_and(Expression::contains_external_routine);
    }

    let mut crosses_boundary = false;
    ExpressionIterator::enumerate_children(expr, |child| {
        if !crosses_boundary {
            crosses_boundary = projection_reference_crosses_execution_boundary(proj, child);
        }
    });
    crosses_boundary
}

impl Default for FilterPushdown {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::chunk::Chunk;
    use paro_common::error::Result;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use paro_external::routine::boundary::PlacementClass;
    use paro_function::scalar::{ExpressionState, ScalarFunction};
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::{
        ComparisonExpression, ComparisonType, ConjunctionExpression, ConstantExpression,
        FunctionExpression, WindowExpression, WindowFrame, WindowFrameBound, WindowFrameType,
    };
    use paro_planner::operator::{
        ColumnBinding, ComparisonJoin, DelimGet, Get, JoinComparisonType, JoinCondition,
    };
    use paro_planner::plan::LogicalPlan;

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

    fn noop_scalar_execute(
        _input: &Chunk,
        _state: &dyn ExpressionState,
        _result: &mut Vector,
    ) -> Result<()> {
        Ok(())
    }

    fn external_call() -> Expression {
        let function = ScalarFunction::new(
            "external_test".to_string(),
            vec![],
            LogicalType::Integer,
            noop_scalar_execute,
        );
        let mut expression = FunctionExpression::new(function, vec![], LogicalType::Integer);
        expression
            .routine_meta
            .as_mut()
            .expect("builtin routine metadata")
            .boundary
            .placement = PlacementClass::External;
        Expression::Function(expression)
    }

    fn window_with_start_offset(offset: Expression) -> Expression {
        Expression::Window(WindowExpression {
            function: paro_function::window::WindowFunction::row_number(),
            children: vec![],
            partitions: vec![],
            orders: vec![],
            frame: WindowFrame {
                frame_type: WindowFrameType::Rows,
                start_bound: WindowFrameBound::Offset(Box::new(offset)),
                start_is_preceding: true,
                end_bound: WindowFrameBound::CurrentRow,
                end_is_preceding: false,
            },
            ignore_nulls: false,
            return_type: LogicalType::BigInt,
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

    fn make_delim_join(ctx: &BindContext, join_type: JoinType) -> LogicalOperator {
        let left = plan(ctx, make_get(0));
        let right = LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
            JoinType::Inner,
            plan(ctx, make_get(1)),
            plan(
                ctx,
                LogicalOperator::DelimGet(DelimGet::new(99, vec![LogicalType::Integer])),
            ),
            vec![JoinCondition::new(
                make_column_ref(1, 0),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(99, 0),
                    LogicalType::Integer,
                )),
                JoinComparisonType::Equal,
            )],
        )));

        let mut join = ComparisonJoin::new(
            join_type,
            left,
            plan(ctx, right),
            vec![JoinCondition::new(
                make_column_ref(0, 0),
                make_column_ref(1, 0),
                JoinComparisonType::Equal,
            )],
        );
        join.duplicate_eliminated_columns = vec![make_column_ref(0, 0)];
        LogicalOperator::Join(Join::Comparison(join))
    }

    fn contains_empty_result(p: &LogicalPlan) -> bool {
        if p.is_empty_result() {
            return true;
        }
        p.children()
            .iter()
            .any(|child| contains_empty_result(child))
    }

    #[test]
    fn test_filter_extract_bindings() {
        let expr = make_comparison(
            ComparisonType::Equal,
            make_column_ref(0, 0),
            make_constant(5),
        );

        let filter = Filter::new(expr);
        assert!(filter.bindings.contains(&0));
        assert_eq!(filter.bindings.len(), 1);
    }

    #[test]
    fn test_filter_extract_bindings_multiple() {
        let expr = make_comparison(
            ComparisonType::Equal,
            make_column_ref(0, 0),
            make_column_ref(1, 0),
        );

        let filter = Filter::new(expr);
        assert!(filter.bindings.contains(&0));
        assert!(filter.bindings.contains(&1));
        assert_eq!(filter.bindings.len(), 2);
    }

    #[test]
    fn test_filter_extract_bindings_visits_window_frame_offsets() {
        let filter = Filter::new(window_with_start_offset(make_column_ref(7, 0)));

        assert_eq!(filter.bindings, HashSet::from([7]));
    }

    #[test]
    fn test_external_routine_detection_visits_window_frame_offsets() {
        let expression = window_with_start_offset(external_call());

        assert!(expression.contains_external_routine());
    }

    #[test]
    fn test_projection_boundary_check_visits_window_frame_offsets() {
        let ctx = BindContext::new();
        let projection = Projection::new(7, plan(&ctx, make_get(0)), vec![external_call()]);
        let expression = window_with_start_offset(make_column_ref(7, 0));

        assert!(projection_reference_crosses_execution_boundary(
            &projection,
            &expression
        ));
    }

    #[test]
    fn test_pushdown_filter_through_filter() {
        let ctx = BindContext::new();
        let get = make_get(0);
        let filter_expr = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(5),
        );
        let filter = PlannerFilter::new(plan(&ctx, get), vec![filter_expr]);
        let op = LogicalOperator::Filter(filter);

        let mut pushdown = FilterPushdown::new();
        let result = pushdown.rewrite(op);

        // Filter should be pushed down to create Filter(Get)
        match result {
            LogicalOperator::Filter(f) => {
                assert!(matches!(f.child.operator, LogicalOperator::Get(_)));
            }
            _ => panic!("Expected Filter operator"),
        }
    }

    #[test]
    fn test_pushdown_through_projection() {
        let ctx = BindContext::new();
        // Create: Filter(Projection(Get))
        // Filter: proj.col0 > 5
        // Projection: [get.col0, get.col1]
        let get = make_get(0);
        let proj = Projection::new(
            1, // projection table index
            plan(&ctx, get),
            vec![make_column_ref(0, 0), make_column_ref(0, 1)],
        );

        let filter_expr = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(1, 0), // references projection output
            make_constant(5),
        );
        let filter = PlannerFilter::new(
            plan(&ctx, LogicalOperator::Projection(proj)),
            vec![filter_expr],
        );
        let op = LogicalOperator::Filter(filter);

        let mut pushdown = FilterPushdown::new();
        let result = pushdown.rewrite(op);

        // Filter should be pushed through projection
        // Result should be: Projection(Filter(Get))
        match result {
            LogicalOperator::Projection(p) => match p.child.operator {
                LogicalOperator::Filter(f) => {
                    assert!(matches!(f.child.operator, LogicalOperator::Get(_)));
                }
                _ => panic!("Expected Filter under Projection"),
            },
            _ => panic!("Expected Projection operator"),
        }
    }

    #[test]
    fn test_pushdown_through_cross_product() {
        let ctx = BindContext::new();
        // Create: Filter(Cross(Get0, Get1))
        // Filter: get0.col0 > 5 (only references left side)
        let left = make_get(0);
        let right = make_get(1);
        let cross = CrossProduct::new(plan(&ctx, left), plan(&ctx, right));

        let filter_expr = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0), // references left side only
            make_constant(5),
        );
        let filter = PlannerFilter::new(
            plan(&ctx, LogicalOperator::Join(Join::Cross(cross))),
            vec![filter_expr],
        );
        let op = LogicalOperator::Filter(filter);

        let mut pushdown = FilterPushdown::new();
        let result = pushdown.rewrite(op);

        // Filter should be pushed to left side
        // Result should be: Cross(Filter(Get0), Get1)
        match result {
            LogicalOperator::Join(Join::Cross(cp)) => {
                match cp.left.operator {
                    LogicalOperator::Filter(f) => {
                        assert!(matches!(f.child.operator, LogicalOperator::Get(_)));
                    }
                    _ => panic!("Expected Filter on left side"),
                }
                assert!(matches!(cp.right.operator, LogicalOperator::Get(_)));
            }
            _ => panic!("Expected Cross product"),
        }
    }

    #[test]
    fn test_pushdown_join_filter_stays_above() {
        let ctx = BindContext::new();
        // Create: Filter(Cross(Get0, Get1))
        // Filter: get0.col0 = get1.col0 (references both sides)
        let left = make_get(0);
        let right = make_get(1);
        let cross = CrossProduct::new(plan(&ctx, left), plan(&ctx, right));

        let filter_expr = make_comparison(
            ComparisonType::Equal,
            make_column_ref(0, 0), // references left
            make_column_ref(1, 0), // references right
        );
        let filter = PlannerFilter::new(
            plan(&ctx, LogicalOperator::Join(Join::Cross(cross))),
            vec![filter_expr],
        );
        let op = LogicalOperator::Filter(filter);

        let mut pushdown = FilterPushdown::new();
        let result = pushdown.rewrite(op);

        // Filter referencing both sides should stay above the join
        match result {
            LogicalOperator::Filter(f) => {
                assert!(matches!(
                    f.child.operator,
                    LogicalOperator::Join(Join::Cross(_))
                ));
            }
            _ => panic!("Expected Filter above Cross product"),
        }
    }

    #[test]
    fn test_pushdown_through_order() {
        let ctx = BindContext::new();
        let get = make_get(0);
        let order = paro_planner::operator::Order {
            child: Box::new(plan(&ctx, get)),
            orders: vec![],
            projection_map: Vec::new(),
        };

        let filter_expr = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(5),
        );
        let filter =
            PlannerFilter::new(plan(&ctx, LogicalOperator::Order(order)), vec![filter_expr]);
        let op = LogicalOperator::Filter(filter);

        let mut pushdown = FilterPushdown::new();
        let result = pushdown.rewrite(op);

        // Filter should be pushed through Order
        match result {
            LogicalOperator::Order(o) => match o.child.operator {
                LogicalOperator::Filter(f) => {
                    assert!(matches!(f.child.operator, LogicalOperator::Get(_)));
                }
                _ => panic!("Expected Filter under Order"),
            },
            _ => panic!("Expected Order operator"),
        }
    }

    #[test]
    fn test_pushdown_through_limit() {
        let ctx = BindContext::new();
        let get = make_get(0);
        let limit =
            paro_planner::operator::Limit::new(plan(&ctx, get), Some(make_constant(10)), None);

        let filter_expr = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(5),
        );
        let filter =
            PlannerFilter::new(plan(&ctx, LogicalOperator::Limit(limit)), vec![filter_expr]);
        let op = LogicalOperator::Filter(filter);

        let mut pushdown = FilterPushdown::new();
        let result = pushdown.rewrite(op);

        // Filter should be pushed through Limit
        match result {
            LogicalOperator::Limit(l) => match l.child.operator {
                LogicalOperator::Filter(f) => {
                    assert!(matches!(f.child.operator, LogicalOperator::Get(_)));
                }
                _ => panic!("Expected Filter under Limit"),
            },
            _ => panic!("Expected Limit operator"),
        }
    }

    #[test]
    fn test_pushdown_preserves_delim_join_shape() {
        let ctx = BindContext::new();
        let filter = PlannerFilter::new(
            plan(&ctx, make_delim_join(&ctx, JoinType::Inner)),
            vec![make_comparison(
                ComparisonType::GreaterThan,
                make_column_ref(0, 0),
                make_constant(5),
            )],
        );
        let result = FilterPushdown::new().rewrite(LogicalOperator::Filter(filter));

        match result {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert!(!join.duplicate_eliminated_columns.is_empty());
                assert!(matches!(join.left.operator, LogicalOperator::Filter(_)));
                assert!(!contains_empty_result(&join.right));
            }
            _ => panic!("expected delim comparison join"),
        }
    }

    #[test]
    fn test_pushdown_rhs_conflict_materializes_empty_result_inside_delim_subtree() {
        let ctx = BindContext::new();
        let left = plan(&ctx, make_get(0));
        let rhs_base = LogicalOperator::Filter(PlannerFilter::new(
            plan(&ctx, make_get(1)),
            vec![make_comparison(
                ComparisonType::Equal,
                make_column_ref(1, 0),
                make_constant(1),
            )],
        ));
        let rhs = LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
            JoinType::Inner,
            plan(&ctx, rhs_base),
            plan(
                &ctx,
                LogicalOperator::DelimGet(DelimGet::new(99, vec![LogicalType::Integer])),
            ),
            vec![JoinCondition::new(
                make_column_ref(1, 0),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(99, 0),
                    LogicalType::Integer,
                )),
                JoinComparisonType::Equal,
            )],
        )));
        let mut join = ComparisonJoin::new(
            JoinType::Mark,
            left,
            plan(&ctx, rhs),
            vec![JoinCondition::new(
                make_column_ref(0, 0),
                make_column_ref(1, 0),
                JoinComparisonType::Equal,
            )],
        );
        join.duplicate_eliminated_columns = vec![make_column_ref(0, 0)];

        let filter = PlannerFilter::new(
            plan(&ctx, LogicalOperator::Join(Join::Comparison(join))),
            vec![make_comparison(
                ComparisonType::Equal,
                make_column_ref(1, 0),
                make_constant(2),
            )],
        );
        let result = FilterPushdown::new().rewrite(LogicalOperator::Filter(filter));

        match result {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert!(contains_empty_result(&join.right));
            }
            _ => panic!("expected join with empty result in rhs subtree"),
        }
    }

    #[test]
    fn test_split_predicates() {
        // Create: a > 5 AND b < 10
        let left = make_comparison(
            ComparisonType::GreaterThan,
            make_column_ref(0, 0),
            make_constant(5),
        );
        let right = make_comparison(
            ComparisonType::LessThan,
            make_column_ref(0, 1),
            make_constant(10),
        );
        let and_expr = Expression::Conjunction(ConjunctionExpression {
            conjunction_type: ConjunctionType::And,
            children: vec![left, right],
        });

        let predicates = FilterPushdown::split_predicates(and_expr);
        assert_eq!(predicates.len(), 2);
    }

    #[test]
    fn test_get_expression_side() {
        let left_bindings: HashSet<usize> = [0].into_iter().collect();
        let right_bindings: HashSet<usize> = [1].into_iter().collect();

        // Expression referencing only left
        let left_expr = make_column_ref(0, 0);
        assert_eq!(
            FilterPushdown::get_expression_side(&left_expr, &left_bindings, &right_bindings),
            JoinSide::Left
        );

        // Expression referencing only right
        let right_expr = make_column_ref(1, 0);
        assert_eq!(
            FilterPushdown::get_expression_side(&right_expr, &left_bindings, &right_bindings),
            JoinSide::Right
        );

        // Expression referencing both
        let both_expr = make_comparison(
            ComparisonType::Equal,
            make_column_ref(0, 0),
            make_column_ref(1, 0),
        );
        assert_eq!(
            FilterPushdown::get_expression_side(&both_expr, &left_bindings, &right_bindings),
            JoinSide::Both
        );

        // Constant expression
        let const_expr = make_constant(5);
        assert_eq!(
            FilterPushdown::get_expression_side(&const_expr, &left_bindings, &right_bindings),
            JoinSide::None
        );
    }
}
