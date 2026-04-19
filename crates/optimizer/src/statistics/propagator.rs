// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Propagate column statistics through the logical plan.

use crate::filter::propagate_result::FilterPropagateResult;
use crate::filter::pushdown::FilterPushdown;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_context::StatementContext;
use paro_planner::expression::{
    ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression, Expression,
};
use paro_planner::operator::{ColumnBinding, Filter, Join, JoinComparisonType, LogicalOperator};
use paro_planner::plan::LogicalPlan;
use paro_storage::statistics::{BaseStatistics, ColumnStatistics, NumericStats, StringStats};
use std::collections::HashMap;
use std::sync::Arc;

fn column_statistics_arc(base: BaseStatistics) -> Arc<ColumnStatistics> {
    Arc::new(ColumnStatistics::new(base))
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
                            filter.child =
                                Box::new(LogicalPlan::synthetic(LogicalOperator::DummyScan));
                            return LogicalOperator::Filter(filter);
                        }
                        _ => {
                            i += 1;
                        }
                    }
                }

                LogicalOperator::Filter(filter)
            }
            LogicalOperator::Aggregate(agg) => {
                for (i, expr) in agg.groups.iter().enumerate() {
                    if let Some(stats) = self.propagate_expression(expr) {
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
                            for (out_idx, &col_id) in get.column_ids.iter().enumerate() {
                                if out_idx >= get.column_types.len() {
                                    break;
                                }
                                if let Some(storage_stats) = storage.column_statistics(col_id) {
                                    let mut base =
                                        BaseStatistics::new(get.column_types[out_idx].clone());
                                    base.copy_validity(&storage_stats);
                                    let distinct = storage_stats.get_distinct_count();
                                    if distinct > 0 {
                                        base.set_distinct_count(distinct);
                                    }

                                    if let (Some(min), Some(max)) =
                                        (storage_stats.min_value(), storage_stats.max_value())
                                    {
                                        match (&min, &max) {
                                            (Value::Varchar(min_s), Value::Varchar(max_s)) => {
                                                StringStats::set_min(&mut base, min_s);
                                                StringStats::set_max(&mut base, max_s);
                                            }
                                            _ => {
                                                if !min.is_null() && !max.is_null() {
                                                    NumericStats::set_min(&mut base, &min);
                                                    NumericStats::set_max(&mut base, &max);
                                                }
                                            }
                                        }
                                    }

                                    let binding = ColumnBinding {
                                        table_index: get.table_index,
                                        column_index: out_idx,
                                    };
                                    self.statistics_map
                                        .insert(binding, column_statistics_arc(base));
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
                for (i, _expr) in window.expressions.iter().enumerate() {
                    let _binding = ColumnBinding {
                        table_index: window.window_index,
                        column_index: i,
                    };
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
                for arg in &func.children {
                    self.propagate_expression(arg);
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
                NumericStats::set_min(&mut new_base, &min);
                NumericStats::set_max(&mut new_base, &max);
            }

            Some(column_statistics_arc(new_base))
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
                            NumericStats::set_min(lb, &new_min);
                            NumericStats::set_max(lb, &new_max);
                        }
                        {
                            let rb = right_col.statistics_mut();
                            NumericStats::set_min(rb, &new_min);
                            NumericStats::set_max(rb, &new_max);
                        }

                        self.statistics_map.insert(left_binding, Arc::new(left_col));
                        self.statistics_map
                            .insert(right_binding, Arc::new(right_col));
                    }
                    ComparisonType::LessThan => {
                        if lx < rx {
                            let mut rc = right_col.clone();
                            NumericStats::set_min(rc.statistics_mut(), &ln);
                            NumericStats::set_max(rc.statistics_mut(), &rx);
                            self.statistics_map.insert(right_binding, Arc::new(rc));
                        }
                        if rn > ln {
                            let mut lc = left_col.clone();
                            NumericStats::set_min(lc.statistics_mut(), &ln);
                            NumericStats::set_max(lc.statistics_mut(), &rx);
                            self.statistics_map.insert(left_binding, Arc::new(lc));
                        }
                    }
                    ComparisonType::LessThanOrEqual => {
                        if lx <= rx {
                            let mut rc = right_col.clone();
                            NumericStats::set_min(rc.statistics_mut(), &ln);
                            NumericStats::set_max(rc.statistics_mut(), &rx);
                            self.statistics_map.insert(right_binding, Arc::new(rc));
                        }
                        if rn >= ln {
                            let mut lc = left_col.clone();
                            NumericStats::set_min(lc.statistics_mut(), &ln);
                            NumericStats::set_max(lc.statistics_mut(), &rx);
                            self.statistics_map.insert(left_binding, Arc::new(lc));
                        }
                    }
                    ComparisonType::GreaterThan => {
                        if ln > rn {
                            let mut rc = right_col.clone();
                            NumericStats::set_min(rc.statistics_mut(), &rn);
                            NumericStats::set_max(rc.statistics_mut(), &lx);
                            self.statistics_map.insert(right_binding, Arc::new(rc));
                        }
                        if rx < lx {
                            let mut lc = left_col.clone();
                            NumericStats::set_min(lc.statistics_mut(), &rn);
                            NumericStats::set_max(lc.statistics_mut(), &lx);
                            self.statistics_map.insert(left_binding, Arc::new(lc));
                        }
                    }
                    ComparisonType::GreaterThanOrEqual => {
                        if ln >= rn {
                            let mut rc = right_col.clone();
                            NumericStats::set_min(rc.statistics_mut(), &rn);
                            NumericStats::set_max(rc.statistics_mut(), &lx);
                            self.statistics_map.insert(right_binding, Arc::new(rc));
                        }
                        if rx <= lx {
                            let mut lc = left_col.clone();
                            NumericStats::set_min(lc.statistics_mut(), &rn);
                            NumericStats::set_max(lc.statistics_mut(), &lx);
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
