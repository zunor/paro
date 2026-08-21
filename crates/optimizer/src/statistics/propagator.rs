// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Propagate column statistics through the logical plan.

use crate::filter::propagate_result::FilterPropagateResult;
use crate::filter::pushdown::FilterPushdown;
use crate::statistics::unique_keys::declared_unique_keys;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_context::StatementContext;
use paro_function::window::WindowFunctionType;
use paro_planner::expression::{
    ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression, Expression,
    WindowExpression, WindowInvocation,
};
use paro_planner::operator::{
    aggregate::GroupDependency, empty_result::EmptyResult, Aggregate, ColumnBinding, Filter, Join,
    JoinComparisonType, LogicalOperator,
};
use paro_planner::plan::LogicalPlan;
use paro_storage::statistics::{BaseStatistics, ColumnStatistics, NumericStats, StatsInfo};
use std::collections::HashMap;
use std::sync::Arc;

fn column_statistics_arc(base: BaseStatistics) -> Arc<ColumnStatistics> {
    Arc::new(ColumnStatistics::new(base))
}

fn window_output_statistics(expression: &WindowExpression) -> BaseStatistics {
    let return_type = expression.return_type();
    let mut statistics = BaseStatistics::create_unknown(return_type.clone());

    // Only publish bounds guaranteed by function semantics. Node cardinalities are estimates, not
    // correctness bounds, so they must never become window min/max values used for filter pruning.
    let WindowInvocation::Native { function, .. } = &expression.invocation else {
        return statistics;
    };
    match (function.function_type, &return_type) {
        (
            WindowFunctionType::RowNumber
            | WindowFunctionType::Rank
            | WindowFunctionType::DenseRank,
            LogicalType::BigInt,
        ) => {
            statistics.set(StatsInfo::CannotHaveNullValues);
            NumericStats::set_guaranteed_min(&mut statistics, &Value::BigInt(1));
            NumericStats::set_guaranteed_max(&mut statistics, &Value::BigInt(i64::MAX));
        }
        (WindowFunctionType::PercentRank | WindowFunctionType::CumeDist, LogicalType::Double) => {
            statistics.set(StatsInfo::CannotHaveNullValues);
            NumericStats::set_guaranteed_min(&mut statistics, &Value::Double(0.0));
            NumericStats::set_guaranteed_max(&mut statistics, &Value::Double(1.0));
        }
        (
            WindowFunctionType::RowNumber
            | WindowFunctionType::Rank
            | WindowFunctionType::DenseRank
            | WindowFunctionType::PercentRank
            | WindowFunctionType::CumeDist
            | WindowFunctionType::Ntile
            | WindowFunctionType::Lead
            | WindowFunctionType::Lag
            | WindowFunctionType::FirstValue
            | WindowFunctionType::LastValue
            | WindowFunctionType::NthValue,
            _,
        ) => {}
    }

    statistics
}

fn derive_group_dependencies(aggregate: &Aggregate) -> Vec<GroupDependency> {
    if aggregate.groups.len() < 2 || !aggregate.has_plain_grouping_domain() {
        return Vec::new();
    }

    let group_bindings = aggregate
        .groups
        .iter()
        .map(|group| match group {
            Expression::ColumnRef(column) if column.depth == 0 => Some(column.binding),
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut dependencies = Vec::new();
    collect_group_dependencies(
        aggregate.child.as_ref(),
        aggregate,
        &group_bindings,
        &mut dependencies,
    );
    dependencies
}

fn collect_group_dependencies(
    plan: &LogicalPlan,
    aggregate: &Aggregate,
    group_bindings: &[Option<ColumnBinding>],
    dependencies: &mut Vec<GroupDependency>,
) {
    if let LogicalOperator::Get(get) = &plan.operator {
        for key in declared_unique_keys(get) {
            let Some(determinants) = key
                .bindings
                .iter()
                .map(|binding| {
                    group_bindings
                        .iter()
                        .position(|candidate| candidate == &Some(*binding))
                })
                .collect::<Option<Vec<_>>>()
            else {
                continue;
            };
            if !key.is_unique_with_nulls_equal(|binding| {
                group_bindings
                    .iter()
                    .position(|candidate| candidate == &Some(binding))
                    .and_then(|group_idx| aggregate.group_stats.get(group_idx))
                    .and_then(Option::as_ref)
                    .is_some_and(|stats| !stats.can_have_null())
            }) {
                continue;
            }

            let dependents = group_bindings
                .iter()
                .enumerate()
                .filter_map(|(group_idx, binding)| {
                    binding
                        .filter(|binding| {
                            binding.table_index == get.table_index
                                && !key.bindings.contains(binding)
                        })
                        .map(|_| group_idx)
                })
                .collect::<Vec<_>>();
            if !dependents.is_empty() {
                dependencies.push(GroupDependency {
                    determinants: determinants.into_boxed_slice(),
                    dependents: dependents.into_boxed_slice(),
                });
            }
        }
    }
    for child in plan.children() {
        collect_group_dependencies(child, aggregate, group_bindings, dependencies);
    }
}

/// Propagates column statistics through the logical plan.
pub struct StatisticsPropagator {
    statistics_map: HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    cte_statistics: HashMap<usize, Vec<Arc<ColumnStatistics>>>,
}

impl StatisticsPropagator {
    pub fn new() -> Self {
        Self {
            statistics_map: HashMap::new(),
            cte_statistics: HashMap::new(),
        }
    }

    fn column_binding(col_ref: &ColumnRefExpression) -> ColumnBinding {
        col_ref.binding
    }

    /// Propagate statistics through a logical plan root.
    pub fn propagate(&mut self, ctx: Arc<StatementContext>, plan: LogicalPlan) -> LogicalPlan {
        self.propagate_plan(ctx.as_ref(), plan)
    }

    pub fn statistics_map(&self) -> &HashMap<ColumnBinding, Arc<ColumnStatistics>> {
        &self.statistics_map
    }

    pub fn take_statistics_map(self) -> HashMap<ColumnBinding, Arc<ColumnStatistics>> {
        self.statistics_map
    }

    /// Propagate comparison expression and return filter result
    pub fn propagate_comparison(
        &self,
        left_stats: &ColumnStatistics,
        right_stats: &ColumnStatistics,
        comparison: ComparisonType,
    ) -> FilterPropagateResult {
        let left = left_stats.statistics();
        let right = right_stats.statistics();
        // This propagation only supports numeric statistics.
        if !left.get_type().is_numeric() || !right.get_type().is_numeric() {
            return FilterPropagateResult::NoPruningPossible;
        }

        // Check if we have min/max statistics
        let (left_min, left_max) = match (left.min_value(), left.max_value()) {
            (Some(lo), Some(hi)) => (lo, hi),
            _ => return FilterPropagateResult::NoPruningPossible,
        };

        let (right_min, right_max) = match (right.min_value(), right.max_value()) {
            (Some(lo), Some(hi)) => (lo, hi),
            _ => return FilterPropagateResult::NoPruningPossible,
        };

        // Check if either side can have null
        let has_null = left.can_have_null() || right.can_have_null();

        match comparison {
            ComparisonType::Equal => {
                // l = r, if l.min > r.max or r.min > l.max equality is not possible
                if left_min > right_max || right_min > left_max {
                    if has_null {
                        FilterPropagateResult::FilterFalseOrNull
                    } else {
                        FilterPropagateResult::FilterAlwaysFalse
                    }
                } else {
                    FilterPropagateResult::NoPruningPossible
                }
            }
            ComparisonType::NotEqual => {
                // For now, we don't optimize != comparisons
                FilterPropagateResult::NoPruningPossible
            }
            ComparisonType::GreaterThan => {
                // l > r
                if left_min > right_max {
                    // if l.min > r.max, it is always true ONLY if neither side contains nulls
                    if has_null {
                        FilterPropagateResult::FilterTrueOrNull
                    } else {
                        FilterPropagateResult::FilterAlwaysTrue
                    }
                } else if right_min >= left_max {
                    // if r.min >= l.max, the filter is always false
                    if has_null {
                        FilterPropagateResult::FilterFalseOrNull
                    } else {
                        FilterPropagateResult::FilterAlwaysFalse
                    }
                } else {
                    FilterPropagateResult::NoPruningPossible
                }
            }
            ComparisonType::GreaterThanOrEqual => {
                // l >= r
                if left_min >= right_max {
                    // if l.min >= r.max, it is always true ONLY if neither side contains nulls
                    if has_null {
                        FilterPropagateResult::FilterTrueOrNull
                    } else {
                        FilterPropagateResult::FilterAlwaysTrue
                    }
                } else if right_min > left_max {
                    // if r.min > l.max, the filter is always false
                    if has_null {
                        FilterPropagateResult::FilterFalseOrNull
                    } else {
                        FilterPropagateResult::FilterAlwaysFalse
                    }
                } else {
                    FilterPropagateResult::NoPruningPossible
                }
            }
            ComparisonType::LessThan => {
                // l < r
                if left_max < right_min {
                    // if l.max < r.min, it is always true ONLY if neither side contains nulls
                    if has_null {
                        FilterPropagateResult::FilterTrueOrNull
                    } else {
                        FilterPropagateResult::FilterAlwaysTrue
                    }
                } else if left_min >= right_max {
                    // if l.min >= r.max, the filter is always false
                    if has_null {
                        FilterPropagateResult::FilterFalseOrNull
                    } else {
                        FilterPropagateResult::FilterAlwaysFalse
                    }
                } else {
                    FilterPropagateResult::NoPruningPossible
                }
            }
            ComparisonType::LessThanOrEqual => {
                // l <= r
                if left_max <= right_min {
                    // if l.max <= r.min, it is always true ONLY if neither side contains nulls
                    if has_null {
                        FilterPropagateResult::FilterTrueOrNull
                    } else {
                        FilterPropagateResult::FilterAlwaysTrue
                    }
                } else if left_min > right_max {
                    // if l.min > r.max, the filter is always false
                    if has_null {
                        FilterPropagateResult::FilterFalseOrNull
                    } else {
                        FilterPropagateResult::FilterAlwaysFalse
                    }
                } else {
                    FilterPropagateResult::NoPruningPossible
                }
            }
            ComparisonType::DistinctFrom | ComparisonType::NotDistinctFrom => {
                // For now, we don't optimize DISTINCT comparisons
                FilterPropagateResult::NoPruningPossible
            }
        }
    }

    /// Handle filter expression and return propagation result
    fn handle_filter(&mut self, expr: &mut Expression) -> FilterPropagateResult {
        // First propagate the expression
        self.propagate_expression(expr);

        // Check if the expression is a constant
        if let Expression::Constant(constant) = expr {
            match &constant.value {
                Value::Boolean(true) => return FilterPropagateResult::FilterAlwaysTrue,
                Value::Boolean(false) => return FilterPropagateResult::FilterAlwaysFalse,
                Value::Null(_) => return FilterPropagateResult::FilterFalseOrNull,
                _ => {}
            }
        }

        // Check if this is a comparison expression
        if let Expression::Comparison(comp) = expr {
            // Get statistics for left and right
            let left_stats = self.propagate_expression(&comp.left);
            let right_stats = self.propagate_expression(&comp.right);

            if let (Some(left_stats), Some(right_stats)) = (left_stats, right_stats) {
                let result = self.propagate_comparison(
                    left_stats.as_ref(),
                    right_stats.as_ref(),
                    comp.comparison_type,
                );

                // If the filter is always true or false, replace the expression with a constant
                match result {
                    FilterPropagateResult::FilterAlwaysTrue => {
                        *expr = Expression::Constant(ConstantExpression {
                            value: Value::Boolean(true),
                            return_type: LogicalType::Boolean,
                        });
                        return result;
                    }
                    FilterPropagateResult::FilterAlwaysFalse => {
                        *expr = Expression::Constant(ConstantExpression {
                            value: Value::Boolean(false),
                            return_type: LogicalType::Boolean,
                        });
                        return result;
                    }
                    _ => return result,
                }
            }
        }

        FilterPropagateResult::NoPruningPossible
    }

    fn propagate_plan(&mut self, ctx: &StatementContext, plan: LogicalPlan) -> LogicalPlan {
        let plan = plan.map_children(|child| self.propagate_plan(ctx, child));
        plan.map_operator(|operator| self.propagate_operator(ctx, operator))
    }

    /// Propagate statistics through an operator after all children have been propagated.
    fn propagate_operator(
        &mut self,
        ctx: &StatementContext,
        op: LogicalOperator,
    ) -> LogicalOperator {
        match op {
            LogicalOperator::Projection(proj) => {
                for (i, expr) in proj.expressions.iter().enumerate() {
                    if let Some(stats) = self.propagate_expression(expr) {
                        let binding = ColumnBinding {
                            table_index: proj.table_index,
                            column_index: i,
                        };
                        self.statistics_map.insert(binding, stats);
                    }
                }
                LogicalOperator::Projection(proj)
            }
            LogicalOperator::Filter(mut filter) => {
                let mut i = 0;
                while i < filter.expressions.len() {
                    let result = self.handle_filter(&mut filter.expressions[i]);

                    match result {
                        FilterPropagateResult::FilterAlwaysTrue => {
                            filter.expressions.remove(i);
                        }
                        FilterPropagateResult::FilterAlwaysFalse
                        | FilterPropagateResult::FilterFalseOrNull => {
                            filter.expressions.clear();
                            return LogicalOperator::EmptyResult(EmptyResult::new(
                                LogicalPlan::synthetic(LogicalOperator::Filter(filter)),
                            ));
                        }
                        _ => {
                            i += 1;
                        }
                    }
                }

                LogicalOperator::Filter(filter)
            }
            LogicalOperator::Aggregate(mut agg) => {
                for (i, expr) in agg.groups.iter().enumerate() {
                    if let Some(stats) = self.propagate_expression(expr) {
                        agg.group_stats[i] = Some(stats.statistics().clone());
                        let binding = ColumnBinding {
                            table_index: agg.group_index,
                            column_index: i,
                        };
                        self.statistics_map.insert(binding, stats);
                    }
                }

                for (i, expr) in agg.aggregates.iter().enumerate() {
                    if let Some(stats) = self.propagate_expression(expr) {
                        let binding = ColumnBinding {
                            table_index: agg.aggregate_index,
                            column_index: i,
                        };
                        self.statistics_map.insert(binding, stats);
                    }
                }

                // Dependencies are a derived annotation of the current child
                // and current statistics, never persistent optimizer state.
                agg.group_dependencies = derive_group_dependencies(&agg);

                LogicalOperator::Aggregate(agg)
            }
            LogicalOperator::Order(order) => {
                for order_by in &order.orders {
                    self.propagate_expression(&order_by.expression);
                }
                LogicalOperator::Order(order)
            }
            LogicalOperator::Limit(limit) => {
                if let Some(limit_val) = &limit.limit {
                    self.propagate_expression(limit_val);
                }
                if let Some(offset_val) = &limit.offset {
                    self.propagate_expression(offset_val);
                }
                LogicalOperator::Limit(limit)
            }
            LogicalOperator::TopN(topn) => {
                for order_by in &topn.orders {
                    self.propagate_expression(&order_by.expression);
                }
                LogicalOperator::TopN(topn)
            }
            LogicalOperator::Distinct(distinct) => LogicalOperator::Distinct(distinct),
            LogicalOperator::MaterializedCTE(cte) => {
                if let Some(stats) = self.capture_output_statistics(&cte.cte_query.operator) {
                    self.cte_statistics.insert(cte.cte_index, stats);
                }
                LogicalOperator::MaterializedCTE(cte)
            }
            LogicalOperator::RecursiveCTE(cte) => {
                if let Some(stats) = self.capture_output_statistics(&cte.anchor.operator) {
                    self.cte_statistics.insert(cte.cte_index, stats);
                }
                LogicalOperator::RecursiveCTE(cte)
            }
            LogicalOperator::CTERef(cte_ref) => {
                if let Some(stats) = self.cte_statistics.get(&cte_ref.cte_index).cloned() {
                    for (column_index, stats) in stats.into_iter().enumerate() {
                        self.statistics_map.insert(
                            ColumnBinding {
                                table_index: cte_ref.table_index,
                                column_index,
                            },
                            stats,
                        );
                    }
                }
                LogicalOperator::CTERef(cte_ref)
            }
            LogicalOperator::Get(get) => {
                if let Some(table) = &get.table {
                    let used_catalog = false;
                    let _txn = ctx.catalog_txn_view();
                    if !used_catalog {
                        if let Some(storage) = table.get_storage() {
                            for out_idx in 0..get.column_sources.len() {
                                if out_idx >= get.column_types.len() {
                                    break;
                                }
                                let Some(col_id) = get.stored_column(out_idx) else {
                                    continue;
                                };
                                if let Some(storage_stats) = storage.column_statistics(col_id) {
                                    let binding = ColumnBinding {
                                        table_index: get.table_index,
                                        column_index: out_idx,
                                    };
                                    self.statistics_map.insert(binding, Arc::new(storage_stats));
                                }
                            }
                        }
                    }
                }

                LogicalOperator::Get(get)
            }
            LogicalOperator::DummyScan => LogicalOperator::DummyScan,
            LogicalOperator::Join(join) => match join {
                Join::Comparison(mut cj) => {
                    let can_propagate = matches!(
                        cj.join_type,
                        paro_planner::operator::JoinType::Inner
                            | paro_planner::operator::JoinType::Semi
                    );

                    if can_propagate {
                        let mut left = *cj.left;
                        let mut right = *cj.right;

                        for condition in &cj.conditions {
                            let stats_left_before = self.propagate_expression(&condition.left);
                            let stats_right_before = self.propagate_expression(&condition.right);
                            let comparison_type =
                                Self::join_comparison_to_comparison(condition.comparison);

                            self.update_filter_statistics(
                                &condition.left,
                                &condition.right,
                                comparison_type,
                            );

                            let stats_left_after = self.propagate_expression(&condition.left);
                            let stats_right_after = self.propagate_expression(&condition.right);

                            if let (Some(before), Some(after)) =
                                (stats_left_before, stats_left_after)
                            {
                                if let Expression::ColumnRef(_) = &condition.left {
                                    left = Self::create_filter_from_join_stats(
                                        left,
                                        &condition.left,
                                        before.as_ref(),
                                        after.as_ref(),
                                    );
                                }
                            }

                            if let (Some(before), Some(after)) =
                                (stats_right_before, stats_right_after)
                            {
                                if let Expression::ColumnRef(_) = &condition.right {
                                    right = Self::create_filter_from_join_stats(
                                        right,
                                        &condition.right,
                                        before.as_ref(),
                                        after.as_ref(),
                                    );
                                }
                            }
                        }

                        cj.left = Box::new(left);
                        cj.right = Box::new(right);
                    } else {
                        for condition in &cj.conditions {
                            self.propagate_expression(&condition.left);
                            self.propagate_expression(&condition.right);
                        }
                    }

                    LogicalOperator::Join(Join::Comparison(cj))
                }
                Join::Any(aj) => {
                    self.propagate_expression(&aj.condition);
                    LogicalOperator::Join(Join::Any(aj))
                }
                Join::Cross(cross) => LogicalOperator::Join(Join::Cross(cross)),
            },
            LogicalOperator::Window(window) => {
                for (i, expression) in window.expressions.iter().enumerate() {
                    // Window output ranges depend on partition cardinality and frame semantics.
                    // Until those estimates are available, publish type-correct unknown statistics
                    // so downstream projections and CTEs retain a complete statistics chain.
                    let binding = ColumnBinding {
                        table_index: window.window_index,
                        column_index: i,
                    };
                    self.statistics_map.insert(
                        binding,
                        column_statistics_arc(window_output_statistics(expression)),
                    );
                }
                LogicalOperator::Window(window)
            }
            other => other,
        }
    }

    fn capture_output_statistics(
        &self,
        op: &LogicalOperator,
    ) -> Option<Vec<Arc<ColumnStatistics>>> {
        let bindings = op.get_column_bindings();
        let mut result = Vec::with_capacity(bindings.len());

        for binding in bindings {
            let stats = self.statistics_map.get(&binding)?.clone();
            result.push(stats);
        }

        Some(result)
    }

    /// Propagate statistics through an expression
    fn propagate_expression(&mut self, expr: &Expression) -> Option<Arc<ColumnStatistics>> {
        match expr {
            Expression::Constant(constant) => Some(column_statistics_arc(
                BaseStatistics::from_constant(&constant.value),
            )),
            Expression::ColumnRef(col_ref) => {
                let binding = Self::column_binding(col_ref);
                self.statistics_map.get(&binding).cloned()
            }
            Expression::Comparison(comp) => {
                self.propagate_expression(&comp.left);
                self.propagate_expression(&comp.right);

                Some(column_statistics_arc(BaseStatistics::new(
                    LogicalType::Boolean,
                )))
            }
            Expression::Conjunction(conj) => {
                for child in &conj.children {
                    self.propagate_expression(child);
                }

                Some(column_statistics_arc(BaseStatistics::new(
                    LogicalType::Boolean,
                )))
            }
            Expression::Function(func) => {
                let mut child_stats = Vec::with_capacity(func.children.len());
                for arg in &func.children {
                    if let Some(stats) = self.propagate_expression(arg) {
                        child_stats.push(stats);
                    }
                }
                if child_stats.len() == func.children.len() {
                    if let Some(statistics) = func.function.statistics {
                        let inputs = child_stats
                            .iter()
                            .map(|stats| stats.statistics())
                            .collect::<Vec<_>>();
                        if let Some(output) = statistics(&inputs) {
                            return Some(column_statistics_arc(output));
                        }
                    }
                }

                Some(column_statistics_arc(BaseStatistics::new(
                    func.return_type.clone(),
                )))
            }
            Expression::Aggregate(agg) => {
                for arg in &agg.children {
                    self.propagate_expression(arg);
                }
                if let Some(filter) = &agg.filter {
                    self.propagate_expression(filter);
                }
                for order in &agg.order_bys {
                    self.propagate_expression(&order.expression);
                }

                Some(column_statistics_arc(BaseStatistics::new(
                    agg.return_type.clone(),
                )))
            }
            Expression::Cast(cast) => {
                if let Some(child_stats) = self.propagate_expression(&cast.child) {
                    if Self::can_propagate_cast(&cast.child.return_type(), &cast.target_type) {
                        if let Some(casted_stats) = Self::try_propagate_cast(
                            child_stats.as_ref(),
                            &cast.child.return_type(),
                            &cast.target_type,
                        ) {
                            return Some(casted_stats);
                        }
                    }
                }

                Some(column_statistics_arc(BaseStatistics::new(
                    cast.target_type.clone(),
                )))
            }
            Expression::Operator(op) => {
                for child in &op.children {
                    self.propagate_expression(child);
                }

                Some(column_statistics_arc(BaseStatistics::new(
                    op.return_type.clone(),
                )))
            }
            Expression::Case(case) => {
                self.propagate_expression(&case.check);

                self.propagate_expression(&case.result_if_true);
                self.propagate_expression(&case.result_if_false);

                Some(column_statistics_arc(BaseStatistics::new(
                    case.return_type.clone(),
                )))
            }
            _ => Some(column_statistics_arc(BaseStatistics::new(
                expr.return_type(),
            ))),
        }
    }

    /// Check if we can propagate a cast between two types
    fn can_propagate_cast(source: &LogicalType, target: &LogicalType) -> bool {
        // For now, only allow propagating casts between numeric types
        match (source, target) {
            (LogicalType::TinyInt, LogicalType::SmallInt)
            | (LogicalType::TinyInt, LogicalType::Integer)
            | (LogicalType::TinyInt, LogicalType::BigInt)
            | (LogicalType::SmallInt, LogicalType::Integer)
            | (LogicalType::SmallInt, LogicalType::BigInt)
            | (LogicalType::Integer, LogicalType::BigInt) => true,
            _ => false,
        }
    }

    /// Try to propagate cast statistics
    fn try_propagate_cast(
        stats: &ColumnStatistics,
        source: &LogicalType,
        target: &LogicalType,
    ) -> Option<Arc<ColumnStatistics>> {
        if Self::can_propagate_cast(source, target) {
            let s = stats.statistics();
            let mut new_base = BaseStatistics::new(target.clone());
            new_base.copy_validity(s);
            if s.get_distinct_count() > 0 {
                new_base.set_distinct_count(s.get_distinct_count());
            }

            if let (Some(min), Some(max)) = (s.min_value(), s.max_value()) {
                NumericStats::set_guaranteed_min(&mut new_base, &min);
                NumericStats::set_guaranteed_max(&mut new_base, &max);
            }

            Some(Arc::new(ColumnStatistics::with_distinct(
                new_base,
                stats.distinct_stats().map(|distinct| distinct.copy()),
            )))
        } else {
            None
        }
    }

    /// Convert JoinComparisonType to ComparisonType
    fn join_comparison_to_comparison(join_comp: JoinComparisonType) -> ComparisonType {
        match join_comp {
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
    /// This modifies the statistics map to reflect the constraints imposed by the filter
    fn update_filter_statistics(
        &mut self,
        left: &Expression,
        right: &Expression,
        comparison: ComparisonType,
    ) {
        // This update only handles column-to-column comparisons.
        let (left_col, right_col) = match (left, right) {
            (Expression::ColumnRef(l), Expression::ColumnRef(r)) => (l, r),
            _ => return,
        };

        let left_binding = Self::column_binding(left_col);
        let right_binding = Self::column_binding(right_col);

        // Get current statistics
        let left_stats = self.statistics_map.get(&left_binding).cloned();
        let right_stats = self.statistics_map.get(&right_binding).cloned();

        if let (Some(left_arc), Some(right_arc)) = (left_stats, right_stats) {
            let mut left_col = left_arc.as_ref().clone();
            let mut right_col = right_arc.as_ref().clone();
            let left = left_col.statistics();
            let right = right_col.statistics();
            if !left.get_type().is_numeric() || !right.get_type().is_numeric() {
                return;
            }

            let left_min = left.min_value();
            let left_max = left.max_value();
            let right_min = right.min_value();
            let right_max = right.max_value();

            if let (Some(ln), Some(lx), Some(rn), Some(rx)) =
                (left_min, left_max, right_min, right_max)
            {
                match comparison {
                    ComparisonType::Equal => {
                        let new_min = if ln > rn { ln.clone() } else { rn.clone() };
                        let new_max = if lx < rx { lx.clone() } else { rx.clone() };

                        {
                            let lb = left_col.statistics_mut();
                            NumericStats::set_guaranteed_min(lb, &new_min);
                            NumericStats::set_guaranteed_max(lb, &new_max);
                        }
                        {
                            let rb = right_col.statistics_mut();
                            NumericStats::set_guaranteed_min(rb, &new_min);
                            NumericStats::set_guaranteed_max(rb, &new_max);
                        }

                        self.statistics_map.insert(left_binding, Arc::new(left_col));
                        self.statistics_map
                            .insert(right_binding, Arc::new(right_col));
                    }
                    ComparisonType::LessThan => {
                        if lx < rx {
                            let mut rc = right_col.clone();
                            NumericStats::set_guaranteed_min(rc.statistics_mut(), &ln);
                            NumericStats::set_guaranteed_max(rc.statistics_mut(), &rx);
                            self.statistics_map.insert(right_binding, Arc::new(rc));
                        }
                        if rn > ln {
                            let mut lc = left_col.clone();
                            NumericStats::set_guaranteed_min(lc.statistics_mut(), &ln);
                            NumericStats::set_guaranteed_max(lc.statistics_mut(), &rx);
                            self.statistics_map.insert(left_binding, Arc::new(lc));
                        }
                    }
                    ComparisonType::LessThanOrEqual => {
                        if lx <= rx {
                            let mut rc = right_col.clone();
                            NumericStats::set_guaranteed_min(rc.statistics_mut(), &ln);
                            NumericStats::set_guaranteed_max(rc.statistics_mut(), &rx);
                            self.statistics_map.insert(right_binding, Arc::new(rc));
                        }
                        if rn >= ln {
                            let mut lc = left_col.clone();
                            NumericStats::set_guaranteed_min(lc.statistics_mut(), &ln);
                            NumericStats::set_guaranteed_max(lc.statistics_mut(), &rx);
                            self.statistics_map.insert(left_binding, Arc::new(lc));
                        }
                    }
                    ComparisonType::GreaterThan => {
                        if ln > rn {
                            let mut rc = right_col.clone();
                            NumericStats::set_guaranteed_min(rc.statistics_mut(), &rn);
                            NumericStats::set_guaranteed_max(rc.statistics_mut(), &lx);
                            self.statistics_map.insert(right_binding, Arc::new(rc));
                        }
                        if rx < lx {
                            let mut lc = left_col.clone();
                            NumericStats::set_guaranteed_min(lc.statistics_mut(), &rn);
                            NumericStats::set_guaranteed_max(lc.statistics_mut(), &lx);
                            self.statistics_map.insert(left_binding, Arc::new(lc));
                        }
                    }
                    ComparisonType::GreaterThanOrEqual => {
                        if ln >= rn {
                            let mut rc = right_col.clone();
                            NumericStats::set_guaranteed_min(rc.statistics_mut(), &rn);
                            NumericStats::set_guaranteed_max(rc.statistics_mut(), &lx);
                            self.statistics_map.insert(right_binding, Arc::new(rc));
                        }
                        if rx <= lx {
                            let mut lc = left_col.clone();
                            NumericStats::set_guaranteed_min(lc.statistics_mut(), &rn);
                            NumericStats::set_guaranteed_max(lc.statistics_mut(), &lx);
                            self.statistics_map.insert(left_binding, Arc::new(lc));
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    /// Create a filter from join statistics changes
    /// If the statistics of a column have been narrowed by the join condition,
    /// we can push down a filter to the child operator
    fn create_filter_from_join_stats(
        plan: LogicalPlan,
        expr: &Expression,
        stats_before: &ColumnStatistics,
        stats_after: &ColumnStatistics,
    ) -> LogicalPlan {
        // Only handle column refs with numeric types
        let col_ref = match expr {
            Expression::ColumnRef(c) => c,
            _ => return plan,
        };

        if !expr.return_type().is_numeric() {
            return plan;
        }

        let before = stats_before.statistics();
        let after = stats_after.statistics();
        let (min_before, max_before) = match (before.min_value(), before.max_value()) {
            (Some(lo), Some(hi)) => (lo, hi),
            _ => return plan,
        };

        let (min_after, max_after) = match (after.min_value(), after.max_value()) {
            (Some(lo), Some(hi)) => (lo, hi),
            _ => return plan,
        };

        // Create filter expressions if the range has been narrowed
        let mut filter_exprs = Vec::new();

        // If min increased, add >= filter
        if min_after > min_before {
            let left = Expression::ColumnRef(col_ref.clone());
            let right = Expression::Constant(ConstantExpression {
                value: min_after.clone(),
                return_type: expr.return_type(),
            });
            filter_exprs.push(Expression::Comparison(ComparisonExpression {
                left: Box::new(left),
                right: Box::new(right),
                comparison_type: ComparisonType::GreaterThanOrEqual,
            }));
        }

        // If max decreased, add <= filter
        if max_after < max_before {
            let left = Expression::ColumnRef(col_ref.clone());
            let right = Expression::Constant(ConstantExpression {
                value: max_after.clone(),
                return_type: expr.return_type(),
            });
            filter_exprs.push(Expression::Comparison(ComparisonExpression {
                left: Box::new(left),
                right: Box::new(right),
                comparison_type: ComparisonType::LessThanOrEqual,
            }));
        }

        if filter_exprs.is_empty() {
            return plan;
        }

        let filter_plan =
            LogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(plan, filter_exprs)));
        let mut pushdown = FilterPushdown::new();
        pushdown.rewrite_plan(filter_plan)
    }
}

impl Default for StatisticsPropagator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_catalog::entry::{
        CatalogObjectId, ColumnDefinition, Constraint, CreateTableInfo, TableCatalogEntry,
    };
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_function::aggregate::distributive::count::get_count_star_function;
    use paro_function::window::WindowFunction;
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::{AggregateExpression, WindowExpression, WindowFrame};
    use paro_planner::operator::{Aggregate, ExpressionGet, Get, Projection, Window};
    use paro_storage::statistics::StringStats;
    use paro_storage::table::table_factory::TableFactory;

    use super::*;

    fn make_test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn keyed_group_aggregate(constraint: Constraint) -> Aggregate {
        let types = vec![
            LogicalType::BigInt,
            LogicalType::Varchar,
            LogicalType::Varchar,
        ];
        let storage = Arc::new(TableFactory::default().create_table(&types).unwrap());
        let info = CreateTableInfo::new(
            "paro".to_string(),
            "public".to_string(),
            "customer".to_string(),
            vec![
                ColumnDefinition::new("key".to_string(), LogicalType::BigInt),
                ColumnDefinition::new("name".to_string(), LogicalType::Varchar),
                ColumnDefinition::new("comment".to_string(), LogicalType::Varchar),
            ],
        )
        .with_constraints(vec![constraint]);
        let table = Arc::new(
            TableCatalogEntry::from_info(info, storage, CatalogObjectId::from_raw(20_001), 0)
                .unwrap(),
        );
        let child = LogicalPlan::synthetic(LogicalOperator::Get(Get::new(
            7,
            vec!["key".to_string(), "name".to_string(), "comment".to_string()],
            types.clone(),
            table,
        )));
        let groups = types
            .into_iter()
            .enumerate()
            .map(|(column_index, ty)| {
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(7, column_index),
                    ty,
                ))
            })
            .collect();
        let count = Expression::Aggregate(AggregateExpression::new(
            get_count_star_function(),
            Vec::new(),
            LogicalType::BigInt,
        ));
        Aggregate::new(8, 9, 10, child, groups, Vec::new(), vec![count], Vec::new())
    }

    #[test]
    fn primary_key_proves_group_dependencies_without_runtime_statistics() {
        let aggregate = keyed_group_aggregate(Constraint::primary_key(vec![0]));
        let plan = LogicalPlan::synthetic(LogicalOperator::Aggregate(aggregate));
        let propagated = StatisticsPropagator::new().propagate(make_test_session(), plan);
        let LogicalOperator::Aggregate(aggregate) = propagated.operator else {
            panic!("expected aggregate root");
        };
        assert_eq!(
            aggregate.group_dependencies,
            [GroupDependency {
                determinants: Box::new([0]),
                dependents: Box::new([1, 2]),
            }]
        );
    }

    #[test]
    fn nullable_unique_key_is_not_a_group_determinant() {
        let mut aggregate = keyed_group_aggregate(Constraint::unique(vec![0]));
        aggregate.group_stats[0] = Some(NumericStats::create_unknown(LogicalType::BigInt));
        assert!(derive_group_dependencies(&aggregate).is_empty());

        aggregate.group_stats[0] = Some(NumericStats::create_empty(LogicalType::BigInt));
        assert_eq!(derive_group_dependencies(&aggregate).len(), 1);
    }

    #[test]
    fn false_filter_becomes_schema_preserving_empty_result() {
        let bind_context = BindContext::new();
        let child = LogicalPlan::new(
            &bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                7,
                vec![vec![Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(7, 0),
                    LogicalType::Integer,
                ))]],
                vec!["quota".to_string()],
                vec![LogicalType::Integer],
            )),
        );
        let filter = LogicalPlan::new(
            &bind_context,
            LogicalOperator::Filter(Filter::new(
                child,
                vec![Expression::Constant(ConstantExpression::new(
                    Value::Boolean(false),
                    LogicalType::Boolean,
                ))],
            )),
        );

        let optimized = StatisticsPropagator::new().propagate(make_test_session(), filter);

        match optimized.operator {
            LogicalOperator::EmptyResult(empty) => {
                assert_eq!(empty.get_types(), vec![LogicalType::Integer]);
                assert_eq!(empty.child.output_names(), vec!["quota".to_string()]);
            }
            other => panic!("expected schema-preserving EmptyResult, got {other:?}"),
        }
    }

    #[test]
    fn window_outputs_keep_statistics_available_to_parent_projections() {
        let bind_context = BindContext::new();
        let input = LogicalPlan::new(
            &bind_context,
            LogicalOperator::Projection(Projection::new(
                7,
                LogicalPlan::new(&bind_context, LogicalOperator::DummyScan),
                vec![Expression::Constant(ConstantExpression::new(
                    Value::Integer(11),
                    LogicalType::Integer,
                ))],
            )),
        );
        let function = WindowFunction::row_number();
        let window = LogicalPlan::new(
            &bind_context,
            LogicalOperator::Window(Window::new(
                20,
                vec![WindowExpression::native(
                    function.clone(),
                    Vec::new(),
                    vec![Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(7, 0),
                        LogicalType::Integer,
                    ))],
                    Vec::new(),
                    WindowFrame::get_default_frame(&function),
                    false,
                )],
                input,
            )),
        );
        let root = LogicalPlan::new(
            &bind_context,
            LogicalOperator::Projection(Projection::new(
                30,
                window,
                vec![Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(20, 0),
                    LogicalType::BigInt,
                ))],
            )),
        );

        let mut propagator = StatisticsPropagator::new();
        propagator.propagate(make_test_session(), root);

        for binding in [ColumnBinding::new(20, 0), ColumnBinding::new(30, 0)] {
            let stats = propagator
                .statistics_map()
                .get(&binding)
                .unwrap_or_else(|| panic!("missing statistics for {binding:?}"));
            assert_eq!(stats.statistics().get_type(), &LogicalType::BigInt);
            assert!(!stats.statistics().can_have_null());
            assert!(stats.statistics().can_have_no_null());
            assert_eq!(stats.statistics().min_value(), Some(Value::BigInt(1)));
            assert_eq!(
                stats.statistics().max_value(),
                Some(Value::BigInt(i64::MAX))
            );
        }
    }

    #[test]
    fn aggregate_retains_group_statistics_for_physical_planning() {
        let bind_context = BindContext::new();
        let input = LogicalPlan::new(
            &bind_context,
            LogicalOperator::Projection(Projection::new(
                7,
                LogicalPlan::new(&bind_context, LogicalOperator::DummyScan),
                vec![Expression::Constant(ConstantExpression::new(
                    Value::Varchar("R".to_string()),
                    LogicalType::Varchar,
                ))],
            )),
        );
        let aggregate = LogicalPlan::new(
            &bind_context,
            LogicalOperator::Aggregate(Aggregate::new(
                20,
                21,
                22,
                input,
                vec![Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(7, 0),
                    LogicalType::Varchar,
                ))],
                vec![paro_planner::binder::ir::GroupingSet {
                    expressions: vec![0],
                }],
                Vec::new(),
                Vec::new(),
            )),
        );

        let optimized = StatisticsPropagator::new().propagate(make_test_session(), aggregate);
        let LogicalOperator::Aggregate(aggregate) = optimized.operator else {
            panic!("expected aggregate");
        };
        let stats = aggregate.group_stats[0].as_ref().expect("group statistics");
        assert_eq!(stats.min_value(), Some(Value::Varchar("R".to_string())));
        assert_eq!(StringStats::max_string_length(stats), Some(1));
    }

    #[test]
    fn window_statistics_only_publish_function_intrinsic_facts() {
        let cume_dist = WindowFunction::cume_dist();
        let cume_dist = WindowExpression::native(
            cume_dist.clone(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            WindowFrame::get_default_frame(&cume_dist),
            false,
        );
        let cume_dist_stats = window_output_statistics(&cume_dist);
        assert!(!cume_dist_stats.can_have_null());
        assert_eq!(cume_dist_stats.min_value(), Some(Value::Double(0.0)));
        assert_eq!(cume_dist_stats.max_value(), Some(Value::Double(1.0)));

        let ntile = WindowFunction::ntile();
        let ntile = WindowExpression::native(
            ntile.clone(),
            vec![Expression::Constant(ConstantExpression::new(
                Value::BigInt(4),
                LogicalType::BigInt,
            ))],
            Vec::new(),
            Vec::new(),
            WindowFrame::get_default_frame(&ntile),
            false,
        );
        let ntile_stats = window_output_statistics(&ntile);
        assert!(ntile_stats.can_have_null());
        assert!(ntile_stats.can_have_no_null());
        assert_eq!(ntile_stats.min_value(), None);
        assert_eq!(ntile_stats.max_value(), None);
    }
}
