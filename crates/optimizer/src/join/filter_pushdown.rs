// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Join filter pushdown optimizer.
//!
//! This pass derives conservative min/max filters from build-side table
//! statistics of INNER equality joins and injects them into probe-side
//! `Get` runtime filters. Physical planning then pushes them into
//! scan predicates to prune `segment / scan partition`.

use std::sync::Arc;

use paro_catalog::entry::TableCatalogEntry;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_context::StatementContext;
use paro_planner::expression::{
    ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression, Expression,
};
use paro_planner::operator::{ColumnBinding, Join, JoinComparisonType, JoinType, LogicalOperator};
use paro_planner::plan::LogicalPlan;

#[derive(Debug, Clone)]
struct ResolvedGetBinding {
    table_index: usize,
    column_index: usize,
    physical_column_id: usize,
    logical_type: LogicalType,
    table: Option<Arc<TableCatalogEntry>>,
}

/// Push build-side min/max runtime filters into probe-side scans.
pub struct JoinFilterPushdown {
    session: Arc<StatementContext>,
}

impl JoinFilterPushdown {
    pub fn new(session: Arc<StatementContext>) -> Self {
        Self { session }
    }

    #[cfg(test)]
    fn optimize(&mut self, plan: LogicalOperator) -> LogicalOperator {
        self.optimize_plan(LogicalPlan::synthetic(plan)).operator
    }

    pub fn optimize_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        self.optimize_recursive_plan(plan)
    }

    fn optimize_recursive_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        let plan = plan.map_children(|child| self.optimize_recursive_plan(child));
        plan.map_operator(|operator| self.optimize_operator(operator))
    }

    fn optimize_operator(&mut self, plan: LogicalOperator) -> LogicalOperator {
        match plan {
            LogicalOperator::Join(join) => match join {
                Join::Comparison(mut comp) => {
                    self.try_pushdown_join_filters(&mut comp);
                    LogicalOperator::Join(Join::Comparison(comp))
                }
                other => LogicalOperator::Join(other),
            },
            other => other,
        }
    }

    fn try_pushdown_join_filters(&self, join: &mut paro_planner::operator::ComparisonJoin) {
        // Conservative scope for correctness:
        // - INNER only
        // - equality only
        // - both sides resolvable to base Get columns
        if join.join_type != JoinType::Inner {
            return;
        }

        for cond in &join.conditions {
            if cond.comparison != JoinComparisonType::Equal {
                continue;
            }
            let Some(probe_binding) = Self::extract_column_binding(&cond.left) else {
                continue;
            };
            let Some(build_binding) = Self::extract_column_binding(&cond.right) else {
                continue;
            };

            let Some(probe_target) = Self::resolve_get_binding(&join.left.operator, probe_binding)
            else {
                continue;
            };
            let Some(build_target) = Self::resolve_get_binding(&join.right.operator, build_binding)
            else {
                continue;
            };

            let Some((build_min, build_max)) = self.read_build_min_max(&build_target) else {
                continue;
            };

            let cast_min = Self::cast_value(build_min, &probe_target.logical_type);
            let cast_max = Self::cast_value(build_max, &probe_target.logical_type);
            if cast_min.is_none() && cast_max.is_none() {
                continue;
            }

            let runtime_filters =
                Self::build_runtime_filter_expressions(&probe_target, cast_min, cast_max);
            if runtime_filters.is_empty() {
                continue;
            }

            let _ = Self::append_runtime_filters(
                &mut join.left.operator,
                &probe_target,
                &runtime_filters,
            );
        }
    }

    fn extract_column_binding(expr: &Expression) -> Option<ColumnBinding> {
        match expr {
            Expression::ColumnRef(col_ref) if col_ref.depth == 0 => Some(col_ref.binding),
            _ => None,
        }
    }

    fn resolve_get_binding(
        op: &LogicalOperator,
        binding: ColumnBinding,
    ) -> Option<ResolvedGetBinding> {
        match op {
            LogicalOperator::Get(get) => {
                if binding.table_index != get.table_index {
                    return None;
                }
                let physical_column_id = *get.column_ids.get(binding.column_index)?;
                let logical_type = get.column_types.get(binding.column_index)?.clone();
                Some(ResolvedGetBinding {
                    table_index: get.table_index,
                    column_index: binding.column_index,
                    physical_column_id,
                    logical_type,
                    table: get.table.clone(),
                })
            }
            LogicalOperator::Projection(proj) => {
                if binding.table_index != proj.table_index {
                    return None;
                }
                let mapped_expr = proj.expressions.get(binding.column_index)?;
                let mapped_binding = Self::extract_column_binding(mapped_expr)?;
                Self::resolve_get_binding(&proj.child.operator, mapped_binding)
            }
            LogicalOperator::Filter(filter) => {
                Self::resolve_get_binding(&filter.child.operator, binding)
            }
            LogicalOperator::Limit(limit) => {
                Self::resolve_get_binding(&limit.child.operator, binding)
            }
            LogicalOperator::Order(order) => {
                Self::resolve_get_binding(&order.child.operator, binding)
            }
            LogicalOperator::TopN(topn) => Self::resolve_get_binding(&topn.child.operator, binding),
            LogicalOperator::Distinct(distinct) => {
                Self::resolve_get_binding(&distinct.child.operator, binding)
            }
            LogicalOperator::EmptyResult(empty) => {
                Self::resolve_get_binding(&empty.child.operator, binding)
            }
            _ => None,
        }
    }

    fn read_build_min_max(
        &self,
        build_target: &ResolvedGetBinding,
    ) -> Option<(Option<Value>, Option<Value>)> {
        let table = build_target.table.as_ref()?;
        let storage = table.get_storage()?;
        if build_target.physical_column_id >= storage.types().len() {
            return None;
        }

        let visible_version =
            i64::try_from(self.session.transaction_visible_version()).unwrap_or(i64::MAX);
        let segments = storage.collect_segments(visible_version).ok()?;

        let mut min_value: Option<Value> = None;
        let mut max_value: Option<Value> = None;

        for (_rowset, segment) in segments {
            let Some(seg_stats) = segment.statistics() else {
                continue;
            };
            let Some(col_stats) = seg_stats.column(build_target.physical_column_id as u32) else {
                continue;
            };
            let base_stats = col_stats.stats.statistics();

            if let Some(candidate_min) = base_stats.min_value() {
                Self::update_min(&mut min_value, candidate_min);
            }
            if let Some(candidate_max) = base_stats.max_value() {
                Self::update_max(&mut max_value, candidate_max);
            }
        }

        if min_value.is_none() && max_value.is_none() {
            None
        } else {
            Some((min_value, max_value))
        }
    }

    fn update_min(current: &mut Option<Value>, candidate: Value) {
        if candidate.is_null() {
            return;
        }
        match current {
            None => *current = Some(candidate),
            Some(existing) => {
                if candidate < existing.clone() {
                    *existing = candidate;
                }
            }
        }
    }

    fn update_max(current: &mut Option<Value>, candidate: Value) {
        if candidate.is_null() {
            return;
        }
        match current {
            None => *current = Some(candidate),
            Some(existing) => {
                if candidate > existing.clone() {
                    *existing = candidate;
                }
            }
        }
    }

    fn cast_value(value: Option<Value>, target_type: &LogicalType) -> Option<Value> {
        let value = value?;
        if value.logical_type() == *target_type {
            return Some(value);
        }
        value.cast(target_type).ok()
    }

    fn build_runtime_filter_expressions(
        probe_target: &ResolvedGetBinding,
        min_value: Option<Value>,
        max_value: Option<Value>,
    ) -> Vec<Expression> {
        let mut filters = Vec::new();
        let binding = ColumnBinding::new(probe_target.table_index, probe_target.column_index);
        let column_type = probe_target.logical_type.clone();

        if let (Some(min), Some(max)) = (&min_value, &max_value) {
            if min == max {
                filters.push(Expression::Comparison(ComparisonExpression::new(
                    ComparisonType::Equal,
                    Expression::ColumnRef(ColumnRefExpression::new(binding, column_type.clone())),
                    Expression::Constant(ConstantExpression::new(min.clone(), column_type.clone())),
                )));
                return filters;
            }
        }

        if let Some(min) = min_value {
            filters.push(Expression::Comparison(ComparisonExpression::new(
                ComparisonType::GreaterThanOrEqual,
                Expression::ColumnRef(ColumnRefExpression::new(binding, column_type.clone())),
                Expression::Constant(ConstantExpression::new(min, column_type.clone())),
            )));
        }
        if let Some(max) = max_value {
            filters.push(Expression::Comparison(ComparisonExpression::new(
                ComparisonType::LessThanOrEqual,
                Expression::ColumnRef(ColumnRefExpression::new(binding, column_type.clone())),
                Expression::Constant(ConstantExpression::new(max, column_type)),
            )));
        }

        filters
    }

    fn append_runtime_filters(
        op: &mut LogicalOperator,
        probe_target: &ResolvedGetBinding,
        runtime_filters: &[Expression],
    ) -> bool {
        match op {
            LogicalOperator::Get(get) => {
                if get.table_index != probe_target.table_index {
                    return false;
                }
                let mut appended = false;
                for runtime_filter in runtime_filters {
                    if get
                        .runtime_filter_expressions
                        .iter()
                        .any(|existing| existing.equals(runtime_filter))
                    {
                        continue;
                    }
                    get.runtime_filter_expressions.push(runtime_filter.clone());
                    appended = true;
                }
                appended
            }
            LogicalOperator::Projection(proj) => Self::append_runtime_filters(
                &mut proj.child.operator,
                probe_target,
                runtime_filters,
            ),
            LogicalOperator::Filter(filter) => Self::append_runtime_filters(
                &mut filter.child.operator,
                probe_target,
                runtime_filters,
            ),
            LogicalOperator::Limit(limit) => Self::append_runtime_filters(
                &mut limit.child.operator,
                probe_target,
                runtime_filters,
            ),
            LogicalOperator::Order(order) => Self::append_runtime_filters(
                &mut order.child.operator,
                probe_target,
                runtime_filters,
            ),
            LogicalOperator::TopN(topn) => Self::append_runtime_filters(
                &mut topn.child.operator,
                probe_target,
                runtime_filters,
            ),
            LogicalOperator::Distinct(distinct) => Self::append_runtime_filters(
                &mut distinct.child.operator,
                probe_target,
                runtime_filters,
            ),
            LogicalOperator::EmptyResult(empty) => Self::append_runtime_filters(
                &mut empty.child.operator,
                probe_target,
                runtime_filters,
            ),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::JoinFilterPushdown;
    use std::sync::Arc;

    use paro_catalog::entry::{ColumnDefinition, TableCatalogEntry};
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_planner::expression::{ColumnRefExpression, ComparisonType, Expression};
    use paro_planner::operator::{
        ColumnBinding, Get, Join, JoinComparisonType, JoinCondition, JoinType, LogicalOperator,
    };
    use paro_planner::plan::LogicalPlan;
    use paro_storage::table::table_factory::TableFactory;
    use paro_storage::table::table_handle::TableHandle;

    fn create_storage(types: &[LogicalType]) -> TableHandle {
        TableFactory::default().create_table(types).unwrap()
    }

    fn make_test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn create_table(name: &str, values: &[i32]) -> Arc<TableCatalogEntry> {
        let storage = Arc::new(create_storage(&[LogicalType::Integer]));
        let mut chunk = paro_common::test_utils::test_chunk_with_capacity(
            &[LogicalType::Integer],
            values.len(),
        );
        for (idx, value) in values.iter().enumerate() {
            chunk
                .column_mut(0)
                .expect("column")
                .set_value(idx, &Value::Integer(*value));
        }
        chunk.set_cardinality(values.len());
        storage.append(&chunk).expect("append");

        Arc::new(TableCatalogEntry::new(
            "paro".to_string(),
            "public".to_string(),
            name.to_string(),
            vec![ColumnDefinition::new("k".to_string(), LogicalType::Integer)],
            storage,
            0,
        ))
    }

    fn create_get(table_index: usize, table: Arc<TableCatalogEntry>) -> LogicalOperator {
        LogicalOperator::Get(Get::new(
            table_index,
            vec!["k".to_string()],
            vec![LogicalType::Integer],
            table,
        ))
    }

    fn create_inner_join(
        join_type: JoinType,
        probe: LogicalOperator,
        build: LogicalOperator,
    ) -> LogicalOperator {
        LogicalOperator::Join(Join::comparison(
            join_type,
            LogicalPlan::synthetic(probe),
            LogicalPlan::synthetic(build),
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(0, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(1, 0),
                    LogicalType::Integer,
                )),
                JoinComparisonType::Equal,
            )],
        ))
    }

    fn extract_runtime_comparison_values(get: &Get) -> Vec<(ComparisonType, Value)> {
        let mut result = Vec::new();
        for expr in &get.runtime_filter_expressions {
            let Expression::Comparison(cmp) = expr else {
                continue;
            };
            let Expression::Constant(constant) = cmp.right.as_ref() else {
                continue;
            };
            result.push((cmp.comparison_type, constant.value.clone()));
        }
        result
    }

    #[test]
    fn inner_join_pushes_build_min_max_into_probe_get() {
        let probe = create_get(0, create_table("probe", &[1, 2, 3, 4]));
        let build = create_get(1, create_table("build", &[10, 30, 20]));
        let plan = create_inner_join(JoinType::Inner, probe, build);

        let mut optimizer = JoinFilterPushdown::new(make_test_session());
        let optimized = optimizer.optimize(plan);

        let LogicalOperator::Join(Join::Comparison(join)) = optimized else {
            panic!("expected comparison join");
        };
        let LogicalOperator::Get(get) = &join.left.operator else {
            panic!("expected probe side get");
        };

        let comparisons = extract_runtime_comparison_values(get);
        assert!(comparisons.contains(&(ComparisonType::GreaterThanOrEqual, Value::Integer(10))));
        assert!(comparisons.contains(&(ComparisonType::LessThanOrEqual, Value::Integer(30))));
    }

    #[test]
    fn non_inner_join_does_not_push_runtime_filters() {
        let probe = create_get(0, create_table("probe_left", &[1, 2, 3]));
        let build = create_get(1, create_table("build_left", &[10, 30, 20]));
        let plan = create_inner_join(JoinType::Left, probe, build);

        let mut optimizer = JoinFilterPushdown::new(make_test_session());
        let optimized = optimizer.optimize(plan);

        let LogicalOperator::Join(Join::Comparison(join)) = optimized else {
            panic!("expected comparison join");
        };
        let LogicalOperator::Get(get) = &join.left.operator else {
            panic!("expected probe side get");
        };

        assert!(get.runtime_filter_expressions.is_empty());
    }
}
