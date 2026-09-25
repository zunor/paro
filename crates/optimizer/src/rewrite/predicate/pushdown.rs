// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Push filter predicates as close to data sources as semantics allow.

use std::collections::{HashMap, HashSet};

use paro_common::runtime_value::Value;
use paro_planner::expression::{ColumnRefExpression, Expression};
use paro_planner::expression::{ComparisonType, ConjunctionType, OperatorType};
use paro_planner::logical::operator::empty_result::EmptyResult;
use paro_planner::logical::operator::Filter as PlannerFilter;
use paro_planner::logical::operator::{
    AnyJoin, ComparisonJoin, CrossProduct, Join, JoinComparisonType, JoinSide, JoinType,
    LogicalOperator, Projection,
};
use paro_planner::logical::plan::OwnedLogicalPlan;
use paro_planner::logical::visitor::LogicalOperatorVisitor;

use crate::rewrite::expr::binding_replacer::{ColumnBindingReplacer, ReplacementBinding};
use crate::rewrite::expr::join_has_evaluation_fence;
use crate::rewrite::expr::traversal::{
    associative_terms, expression_join_side, into_associative_terms, visit_expression,
};
use crate::rewrite::predicate::combiner::{FilterCombiner, FilterResult};
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
        let bindings = &mut self.bindings;
        visit_expression(&self.filter, &mut |expression| {
            if let Expression::ColumnRef(column) = expression {
                bindings.insert(column.binding.table_index);
            }
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
    pub fn rewrite_plan(&mut self, plan: OwnedLogicalPlan) -> OwnedLogicalPlan {
        let (id, stats, operator) = plan.into_parts();
        OwnedLogicalPlan {
            id,
            stats,
            operator: self.rewrite(operator),
        }
    }

    fn rewrite(&mut self, op: LogicalOperator) -> LogicalOperator {
        match op {
            LogicalOperator::Filter(filter) => self.pushdown_filter(filter),
            LogicalOperator::Projection(proj) => self.pushdown_projection(proj),
            LogicalOperator::RowFetch(mut fetch) => {
                let mut child_pushdown = FilterPushdown::new();
                fetch.child = Box::new(child_pushdown.rewrite_plan(*fetch.child));
                self.push_final_filters(LogicalOperator::RowFetch(fetch))
            }
            LogicalOperator::Join(join) => self.pushdown_join(join),
            LogicalOperator::Aggregate(agg) => self.pushdown_aggregate(*agg),
            LogicalOperator::Distinct(distinct) => self.pushdown_distinct(distinct),
            LogicalOperator::Order(order) => self.pushdown_order(order),
            LogicalOperator::Limit(limit) => self.pushdown_limit(*limit),
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
        into_associative_terms(expr, ConjunctionType::And)
    }

    /// Local predicate normalization shared by owned trees and operator shells.
    /// `None` leaves contradiction/empty-output settlement to its owner.
    pub(crate) fn normalize_predicates(
        predicates: impl IntoIterator<Item = Expression>,
    ) -> Option<Vec<Expression>> {
        let mut pushdown = Self::new();
        for predicate in predicates {
            if pushdown.add_filter(predicate) == FilterResult::Unsatisfiable {
                return None;
            }
        }
        pushdown.generate_filters();
        Some(
            pushdown
                .filters
                .into_iter()
                .map(|filter| filter.filter)
                .collect(),
        )
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
        LogicalOperator::Filter(PlannerFilter::new(
            OwnedLogicalPlan::synthetic(op),
            expressions,
        ))
    }

    fn empty_result(plan: OwnedLogicalPlan) -> OwnedLogicalPlan {
        let id = plan.id;
        let stats = plan.stats.clone();
        OwnedLogicalPlan {
            id,
            stats,
            operator: LogicalOperator::EmptyResult(EmptyResult::new(plan)),
        }
    }

    /// Finish pushdown at this operator.
    ///
    /// Recursively pushes down into children, then adds any remaining filters.
    fn finish_pushdown(&mut self, op: LogicalOperator) -> LogicalOperator {
        let plan = OwnedLogicalPlan::synthetic(op);
        let plan = plan
            .try_map_children(|child| {
                let mut child_pushdown = FilterPushdown::new();
                Ok(child_pushdown.rewrite_plan(child))
            })
            .expect("FilterPushdown child rewrite cannot fail");

        // Add any remaining filters
        self.push_final_filters(plan.into_operator())
    }

    fn pushdown_materialized_cte(
        &mut self,
        mut cte: paro_planner::logical::operator::MaterializedCTE,
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
        if filter
            .expressions
            .iter()
            .any(|expr| expr.evaluation_properties().is_reorder_fence())
        {
            // A fenced predicate is also a barrier for predicates arriving from an outer filter:
            // pushing those below it can change how often the fenced expression is evaluated.
            let mut child_pushdown = FilterPushdown::new();
            let child = child_pushdown.rewrite_plan(*filter.child);
            let result = LogicalOperator::Filter(PlannerFilter::new(child, filter.expressions));
            return self.push_final_filters(result);
        }

        for expr in filter.expressions {
            let result = self.add_filter(expr);
            if result == FilterResult::Unsatisfiable {
                // Filter is unsatisfiable - return empty result
                return Self::empty_result(*filter.child).into_operator();
            }
        }

        // Generate filters and continue pushing down
        self.generate_filters();
        self.rewrite_plan(*filter.child).into_operator()
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
            if Self::has_evaluation_fence_through_projection(&proj, &filter.filter) {
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
                OwnedLogicalPlan::synthetic(result),
                remaining_filters,
            ))
        }
    }

    /// Check whether moving a predicate below a projection could duplicate, eliminate, or reorder
    /// an observable evaluation.
    pub(crate) fn has_evaluation_fence_through_projection<Child>(
        proj: &Projection<Child>,
        expr: &Expression,
    ) -> bool {
        if expr.evaluation_properties().is_reorder_fence() {
            return true;
        }
        if proj
            .expressions
            .iter()
            .any(|projected| !projected.evaluation_properties().can_share_evaluation())
        {
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
        if join_has_evaluation_fence(&join) {
            return self.finish_pushdown(LogicalOperator::Join(join));
        }

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
                        JoinType::Mark => {
                            self.pushdown_mark_join(cj, left_bindings, right_bindings)
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

    /// Push down through a MARK join, lowering it to a SEMI join when its marker
    /// is consumed exclusively by a positive top-level filter.
    ///
    /// In a WHERE clause, retaining rows for which an IN/EXISTS marker is TRUE
    /// has exactly semi-join semantics: FALSE and UNKNOWN are both discarded.
    /// The rewrite is deliberately limited to a bare marker predicate and no
    /// other marker references, so it cannot erase observable expressions or
    /// alter three-valued logic in a compound predicate.
    fn pushdown_mark_join(
        &mut self,
        mut join: ComparisonJoin,
        left_bindings: HashSet<usize>,
        right_bindings: HashSet<usize>,
    ) -> LogicalOperator {
        let Some((marker_filter_index, expected)) = self.consumed_marker_truth_filter(&join) else {
            return self.pushdown_left_join(join, left_bindings, right_bindings);
        };
        if lower_mark_join_for_truth(&mut join, expected) {
            self.filters.remove(marker_filter_index);
            self.pushdown_semi_anti_join(join)
        } else {
            // MARK outputs the left schema plus one marker. Predicates over
            // left columns are safe below the join; predicates over the marker
            // must remain above it.
            self.pushdown_left_join(join, left_bindings, right_bindings)
        }
    }

    /// Get table bindings from a plan subtree.
    fn get_table_bindings_plan(plan: &OwnedLogicalPlan) -> HashSet<usize> {
        // Predicate routing is governed by the child's output contract, not by
        // every table index introduced somewhere below it. The latter includes
        // bindings hidden by projections and misses synthetic outputs such as a
        // MARK join's marker column.
        plan.get_column_bindings()
            .into_iter()
            .map(|binding| binding.table_index)
            .collect()
    }

    /// Push down through an inner comparison join.
    fn pushdown_inner_join(
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
            for implied in Self::derive_or_domain_filters(&filter.filter) {
                match Self::get_expression_side(&implied, &left_bindings, &right_bindings) {
                    JoinSide::Left => {
                        if !left_unsat
                            && left_pushdown.add_filter(implied) == FilterResult::Unsatisfiable
                        {
                            left_unsat = true;
                        }
                    }
                    JoinSide::Right => {
                        if !right_unsat
                            && right_pushdown.add_filter(implied) == FilterResult::Unsatisfiable
                        {
                            right_unsat = true;
                        }
                    }
                    JoinSide::Both | JoinSide::None => {}
                }
            }
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
                OwnedLogicalPlan::synthetic(result),
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
            // A MARK join's marker is produced by the join itself, not by
            // either child.  Treat it as an output barrier before classifying
            // child ownership: `inner_mark OR this_mark` otherwise appears to
            // be left-only and is pushed below the operator that creates
            // `this_mark`, leaving an invalid (and semantically impossible)
            // plan.  Bare truth tests have already taken the explicit
            // semi/anti lowering path in `pushdown_mark_join`.
            if Self::filter_references_join_marker(&cj, &filter) {
                remaining_filters.push(filter.filter);
                continue;
            }
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
                OwnedLogicalPlan::synthetic(result),
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
        self.lower_consumed_exists_marker(&mut cj);

        // Duplicate-elimination metadata changes correlation execution, not
        // the join's row-preservation semantics. Only an INNER delim join may
        // accept predicates on either child. SINGLE, LEFT and MARK preserve
        // the left row when the correlated side has no match, so a predicate
        // on a right/marker output must remain above that boundary. SEMI and
        // ANTI expose only left columns and can route every valid consumer
        // predicate to the preserved side.
        match cj.join_type {
            JoinType::Inner => self.pushdown_inner_delim_join(cj, left_bindings, right_bindings),
            JoinType::Semi | JoinType::Anti => self.pushdown_semi_anti_join(cj),
            _ => self.pushdown_left_join(cj, left_bindings, right_bindings),
        }
    }

    fn pushdown_inner_delim_join(
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
            // A delim MARK join still produces its marker at the join
            // boundary. Correlation metadata changes how the right side is
            // evaluated; it does not make the marker available to either
            // child.
            if Self::filter_references_join_marker(&cj, &filter) {
                remaining_filters.push(filter.filter);
                continue;
            }
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
                OwnedLogicalPlan::synthetic(result),
                remaining_filters,
            ))
        }
    }

    fn filter_references_join_marker(join: &ComparisonJoin, filter: &Filter) -> bool {
        join.mark_index
            .is_some_and(|mark_index| filter.bindings.contains(&mark_index))
    }

    /// Lower a correlated EXISTS marker as soon as its sole consumer is a
    /// top-level truth test.
    ///
    /// EXISTS is intrinsically two-valued, unlike IN/ANY. The dependent-join
    /// flattener records that distinction as `MarkJoinSemantics::TwoValued`,
    /// so both positive and negative consumers can become ordinary
    /// SEMI/ANTI joins without retaining a materialized marker column.
    fn lower_consumed_exists_marker(&mut self, join: &mut ComparisonJoin) {
        if join.join_type != JoinType::Mark
            || join.mark_semantics != paro_planner::logical::operator::MarkJoinSemantics::TwoValued
        {
            return;
        }
        let Some((filter_index, expected)) = self.consumed_marker_truth_filter(join) else {
            return;
        };

        if lower_mark_join_for_truth(join, expected) {
            self.filters.remove(filter_index);
        }
    }

    /// Return the sole top-level truth test that consumes a MARK result.
    ///
    /// The caller decides whether the marker's nullable semantics can be
    /// represented by a semi/anti join before removing the returned filter.
    fn consumed_marker_truth_filter(&self, join: &ComparisonJoin) -> Option<(usize, bool)> {
        let mark_index = join.mark_index?;
        let mark_binding = paro_planner::logical::operator::ColumnBinding::new(mark_index, 0);
        let mut marker_filters = self
            .filters
            .iter()
            .enumerate()
            .filter(|(_, filter)| filter.bindings.contains(&mark_index));
        let (filter_index, filter) = marker_filters.next()?;
        if marker_filters.next().is_some() {
            return None;
        }
        marker_truth_test(&filter.filter, mark_binding).map(|expected| (filter_index, expected))
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
            for implied in Self::derive_or_domain_filters(&filter.filter) {
                match Self::get_expression_side(&implied, &left_bindings, &right_bindings) {
                    JoinSide::Left => {
                        if left_pushdown.add_filter(implied) == FilterResult::Unsatisfiable {
                            return LogicalOperator::DummyScan;
                        }
                    }
                    JoinSide::Right => {
                        if right_pushdown.add_filter(implied) == FilterResult::Unsatisfiable {
                            return LogicalOperator::DummyScan;
                        }
                    }
                    JoinSide::Both | JoinSide::None => {}
                }
            }
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
                OwnedLogicalPlan::synthetic(result),
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
        expression_join_side(expr, &mut |expression| match expression {
            Expression::ColumnRef(column) => Some(JoinSide::get_side(
                column.binding.table_index,
                left_bindings,
                right_bindings,
            )),
            _ => None,
        })
    }

    /// Derive side-local domain predicates implied by every branch of an OR expression.
    ///
    /// `(a = 1 AND b = 2) OR (a = 2 AND b = 1)` implies both `a IN (1, 2)` and
    /// `b IN (1, 2)`. Keeping the original predicate preserves the correlation between domains;
    /// the implied predicates only reduce rows before the join.
    fn derive_or_domain_filters(expr: &Expression) -> Vec<Expression> {
        if expr.evaluation_properties().is_reorder_fence() {
            return Vec::new();
        }
        let branches = associative_terms(expr, ConjunctionType::Or);
        if branches.len() < 2 {
            return Vec::new();
        }

        let mut common_domains: Option<
            HashMap<paro_planner::logical::operator::ColumnBinding, Vec<Expression>>,
        > = None;
        for branch in branches {
            let mut branch_domains = HashMap::new();
            Self::collect_branch_equalities(branch, &mut branch_domains);
            if branch_domains.is_empty() {
                return Vec::new();
            }
            match &mut common_domains {
                None => common_domains = Some(branch_domains),
                Some(common) => {
                    common.retain(|binding, values| {
                        let Some(branch_values) = branch_domains.get(binding) else {
                            return false;
                        };
                        for value in branch_values {
                            if !values.iter().any(|existing| existing.equals(value)) {
                                values.push(value.clone());
                            }
                        }
                        true
                    });
                    if common.is_empty() {
                        return Vec::new();
                    }
                }
            }
        }

        let mut domains = common_domains
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();
        domains.sort_unstable_by_key(|(binding, _)| (binding.table_index, binding.column_index));
        domains
            .into_iter()
            .map(|(_, comparisons)| match comparisons.as_slice() {
                [comparison] => comparison.clone(),
                _ => Expression::Conjunction(
                    paro_planner::expression::ConjunctionExpression::new(
                        ConjunctionType::Or,
                        comparisons,
                    )
                    .into(),
                ),
            })
            .collect()
    }

    fn collect_branch_equalities(
        expr: &Expression,
        domains: &mut HashMap<paro_planner::logical::operator::ColumnBinding, Vec<Expression>>,
    ) {
        for term in associative_terms(expr, ConjunctionType::And) {
            let Expression::Comparison(comparison) = term else {
                continue;
            };
            if comparison.comparison_type != paro_planner::expression::ComparisonType::Equal {
                continue;
            }
            let (Expression::ColumnRef(column), Expression::Constant(_)) =
                (comparison.left.as_ref(), comparison.right.as_ref())
            else {
                let (Expression::Constant(constant), Expression::ColumnRef(column)) =
                    (comparison.left.as_ref(), comparison.right.as_ref())
                else {
                    continue;
                };
                let canonical = Expression::Comparison(
                    paro_planner::expression::ComparisonExpression::new(
                        paro_planner::expression::ComparisonType::Equal,
                        Expression::ColumnRef(column.clone()),
                        Expression::Constant(constant.clone()),
                    )
                    .into(),
                );
                domains.entry(column.binding).or_default().push(canonical);
                continue;
            };
            domains
                .entry(column.binding)
                .or_default()
                .push(term.clone());
        }
    }

    /// Push down through an Aggregate operator.
    fn pushdown_aggregate(
        &mut self,
        mut agg: paro_planner::logical::operator::Aggregate,
    ) -> LogicalOperator {
        let mut child_pushdown = FilterPushdown::new();
        let mut remaining_filters = Vec::new();

        // Get the aggregate's table index
        let agg_index = agg.aggregate_index;
        let groupings_index = agg.groupings_index;

        for mut filter in self.filters.drain(..) {
            // Check if filter references the aggregate output
            let references_aggregate =
                filter.bindings.contains(&agg_index) || filter.bindings.contains(&groupings_index);

            if references_aggregate {
                // Cannot push down filters that reference aggregate results
                remaining_filters.push(filter.filter);
            } else {
                // Group columns are outputs owned by `group_index`, not
                // pass-through child bindings. Substitute the defining GROUP
                // BY expression before crossing the aggregate boundary.
                if !Self::group_filter_can_move(&agg, &filter.filter) {
                    remaining_filters.push(filter.filter);
                    continue;
                }
                filter.filter = filter.filter.replace_column_ref(&|column| {
                    (column.depth == 0 && column.binding.table_index == agg.group_index)
                        .then(|| agg.groups.get(column.binding.column_index).cloned())
                        .flatten()
                });
                if child_pushdown.add_filter(filter.filter) == FilterResult::Unsatisfiable {
                    return LogicalOperator::DummyScan;
                }
            }
        }

        child_pushdown.generate_filters();
        agg.child = Box::new(child_pushdown.rewrite_plan(*agg.child));

        let result = LogicalOperator::Aggregate(Box::new(agg));
        if remaining_filters.is_empty() {
            result
        } else {
            LogicalOperator::Filter(PlannerFilter::new(
                OwnedLogicalPlan::synthetic(result),
                remaining_filters,
            ))
        }
    }

    /// The expression contract for crossing an aggregate output boundary.
    /// Callers that require an ordinary grouping domain check that separately.
    pub(crate) fn group_filter_can_move<Child>(
        agg: &paro_planner::logical::operator::Aggregate<Child>,
        predicate: &Expression,
    ) -> bool {
        let mut pushable = !predicate.evaluation_properties().is_reorder_fence();
        visit_expression(predicate, &mut |expression| {
            let Expression::ColumnRef(column) = expression else {
                return;
            };
            if column.depth != 0
                || column.binding.table_index != agg.group_index
                || agg
                    .groups
                    .get(column.binding.column_index)
                    .is_none_or(|group| !group.evaluation_properties().can_share_evaluation())
            {
                pushable = false;
            }
        });
        pushable
    }

    /// Push down through a Distinct operator.
    fn pushdown_distinct(
        &mut self,
        mut distinct: paro_planner::logical::operator::Distinct,
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
    fn pushdown_order(
        &mut self,
        mut order: paro_planner::logical::operator::Order,
    ) -> LogicalOperator {
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
    fn pushdown_limit(
        &mut self,
        mut limit: paro_planner::logical::operator::Limit,
    ) -> LogicalOperator {
        // LIMIT is a cardinality boundary: filtering its input can replace rows that the LIMIT
        // would otherwise have selected. Keep caller predicates above it while still optimizing
        // the child with an independent pushdown pass.
        let mut child_pushdown = FilterPushdown::new();
        limit.child = Box::new(child_pushdown.rewrite_plan(*limit.child));
        self.push_final_filters(LogicalOperator::Limit(Box::new(limit)))
    }

    /// Push down through a Window operator.
    fn pushdown_window(
        &mut self,
        mut window: paro_planner::logical::operator::Window,
    ) -> LogicalOperator {
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
                OwnedLogicalPlan::synthetic(result),
                remaining_filters,
            ))
        }
    }

    /// Push down through a SetOperation operator.
    fn pushdown_set_operation(
        &mut self,
        mut setop: paro_planner::logical::operator::SetOperation,
    ) -> LogicalOperator {
        // Selection distributes over UNION, INTERSECT, and EXCEPT, including
        // their ALL variants. Rebinding is ordinal because a set operation
        // owns a fresh output table index while each input keeps its own
        // bindings. Observable predicates remain above the boundary: moving
        // one to both inputs could change its evaluation count or error order.
        let mut left_pushdown = FilterPushdown::new();
        let mut right_pushdown = FilterPushdown::new();
        let left_bindings = setop.left.get_column_bindings();
        let right_bindings = setop.right.get_column_bindings();
        if left_bindings.len() != setop.column_count || right_bindings.len() != setop.column_count {
            return self.finish_pushdown(LogicalOperator::SetOperation(setop));
        }

        let mut remaining = Vec::new();
        for filter in self.filters.drain(..) {
            if filter.filter.evaluation_properties().is_reorder_fence()
                || filter
                    .bindings
                    .iter()
                    .any(|table_index| *table_index != setop.table_index)
            {
                remaining.push(filter.filter);
                continue;
            }

            let mut left = filter.filter.clone();
            let mut right = filter.filter;
            let mut left_replacer = ColumnBindingReplacer::new();
            let mut right_replacer = ColumnBindingReplacer::new();
            for (ordinal, (left_binding, right_binding)) in
                left_bindings.iter().zip(right_bindings.iter()).enumerate()
            {
                let output_binding =
                    paro_planner::logical::operator::ColumnBinding::new(setop.table_index, ordinal);
                left_replacer
                    .replacement_bindings
                    .push(ReplacementBinding::new(output_binding, *left_binding));
                right_replacer
                    .replacement_bindings
                    .push(ReplacementBinding::new(output_binding, *right_binding));
            }
            left_replacer.visit_expression(&mut left);
            right_replacer.visit_expression(&mut right);
            if left_pushdown.add_filter(left) == FilterResult::Unsatisfiable
                || right_pushdown.add_filter(right) == FilterResult::Unsatisfiable
            {
                return Self::empty_result(OwnedLogicalPlan::synthetic(
                    LogicalOperator::SetOperation(setop),
                ))
                .into_operator();
            }
        }

        left_pushdown.generate_filters();
        right_pushdown.generate_filters();

        setop.left = Box::new(left_pushdown.rewrite_plan(*setop.left));
        setop.right = Box::new(right_pushdown.rewrite_plan(*setop.right));

        let result = LogicalOperator::SetOperation(setop);
        if remaining.is_empty() {
            result
        } else {
            LogicalOperator::Filter(PlannerFilter::new(
                OwnedLogicalPlan::synthetic(result),
                remaining,
            ))
        }
    }
}

fn projection_reference_crosses_execution_boundary<Child>(
    proj: &Projection<Child>,
    expr: &Expression,
) -> bool {
    let mut crosses_boundary = false;
    visit_expression(expr, &mut |expression| {
        if crosses_boundary {
            return;
        }
        let Expression::ColumnRef(column) = expression else {
            return;
        };
        crosses_boundary = column.binding.table_index == proj.table_index
            && proj
                .expressions
                .get(column.binding.column_index)
                .is_some_and(Expression::contains_external_routine);
    });
    crosses_boundary
}

impl Default for FilterPushdown {
    fn default() -> Self {
        Self::new()
    }
}

fn marker_truth_test(
    expression: &Expression,
    marker: paro_planner::logical::operator::ColumnBinding,
) -> Option<bool> {
    match expression {
        Expression::ColumnRef(column) if column.depth == 0 && column.binding == marker => {
            Some(true)
        }
        Expression::Operator(operator)
            if operator.operator_type == OperatorType::Not
                && matches!(
                    operator.children.as_slice(),
                    [Expression::ColumnRef(column)]
                        if column.depth == 0 && column.binding == marker
                ) =>
        {
            Some(false)
        }
        Expression::Comparison(comparison) => {
            let (comparison_type, constant) =
                match (comparison.left.as_ref(), comparison.right.as_ref()) {
                    (Expression::ColumnRef(column), Expression::Constant(constant))
                        if column.depth == 0 && column.binding == marker =>
                    {
                        (comparison.comparison_type, constant)
                    }
                    (Expression::Constant(constant), Expression::ColumnRef(column))
                        if column.depth == 0 && column.binding == marker =>
                    {
                        (comparison.comparison_type, constant)
                    }
                    _ => return None,
                };
            let Value::Boolean(value) = constant.value else {
                return None;
            };
            match comparison_type {
                ComparisonType::Equal | ComparisonType::NotDistinctFrom => Some(value),
                ComparisonType::NotEqual | ComparisonType::DistinctFrom => Some(!value),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Replace an unobservable MARK result with its relational representation.
/// Returns `false` when nullable marker semantics cannot be represented by the
/// available semi/anti join modes.
fn lower_mark_join_for_truth(join: &mut ComparisonJoin, expected: bool) -> bool {
    if expected {
        join.join_type = JoinType::Semi;
        join.mark_index = None;
        join.mark_semantics = paro_planner::logical::operator::MarkJoinSemantics::NotMark;
        return true;
    }
    if join.mark_semantics == paro_planner::logical::operator::MarkJoinSemantics::TwoValued {
        // EXISTS is two-valued, so its negative truth test is an ordinary anti
        // join.
        join.join_type = JoinType::Anti;
        join.mark_index = None;
        join.mark_semantics = paro_planner::logical::operator::MarkJoinSemantics::NotMark;
        return true;
    }
    if join.mark_semantics == paro_planner::logical::operator::MarkJoinSemantics::ThreeValuedFrom(0)
        && join.conditions.len() == 1
        && join.conditions[0].comparison == JoinComparisonType::Equal
    {
        // A scalar NOT IN marker is observable only through this top-level
        // filter. Preserve UNKNOWN directly in a null-aware anti join.
        join.make_null_aware_anti();
        return true;
    }
    false
}

#[cfg(test)]
mod tests;
