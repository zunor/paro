//! Aggregate Statistics Execution
//!
//! Rewrites eligible ungrouped aggregates to `ExpressionGet` using
//! table/column statistics instead of scanning storage.

use std::sync::Arc;

use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::expression::{AggregateType, ConstantExpression, Expression};
use paro_planner::operator::{Aggregate, ColumnBinding, ExpressionGet, LogicalOperator};
use paro_planner::plan::LogicalPlan;
use paro_storage::statistics::{ColumnStatistics, NumericStats, StringStats};
use paro_storage::table::table_handle::TableHandle;

/// Aggregate type that can be executed using table statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatisticsAggregate {
    CountStar,
    Min,
    Max,
}

/// Result of one statistics-executed aggregate.
#[derive(Debug, Clone, PartialEq)]
pub struct AggregateResult {
    pub aggregate_type: StatisticsAggregate,
    pub value: Value,
}

#[derive(Debug, Clone)]
struct SimpleScanInfo {
    storage: Arc<TableHandle>,
}

/// Optimizer pass for executing simple aggregates directly from storage stats.
pub struct AggregateStatisticsExecutor;

impl AggregateStatisticsExecutor {
    pub fn new() -> Self {
        Self
    }

    pub fn optimize(&mut self, plan: LogicalPlan) -> LogicalPlan {
        self.optimize_plan(plan)
    }

    pub fn can_execute(&self, aggregate: &Aggregate) -> bool {
        aggregate.groups.is_empty()
            && aggregate.grouping_functions.is_empty()
            && aggregate
                .aggregates
                .iter()
                .all(|expr| Self::is_supported_aggregate(expr))
    }

    pub fn try_execute(&self, aggregate: &Aggregate) -> Option<Vec<AggregateResult>> {
        if !self.can_execute(aggregate) {
            return None;
        }
        let scan = Self::resolve_simple_scan(aggregate.child.as_ref())?;
        aggregate
            .aggregates
            .iter()
            .map(|expr| Self::execute_aggregate(expr, aggregate.child.as_ref(), &scan))
            .collect()
    }

    fn optimize_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        let plan = plan.map_children(|child| self.optimize_plan(child));

        let LogicalPlan {
            id,
            stats,
            operator,
        } = plan;

        if let LogicalOperator::Aggregate(agg) = &operator {
            if let Some(results) = self.try_execute(agg) {
                let row = agg
                    .aggregates
                    .iter()
                    .zip(results.iter())
                    .map(|(expr, result)| {
                        Expression::Constant(ConstantExpression::new(
                            result.value.clone(),
                            expr.return_type(),
                        ))
                    })
                    .collect::<Vec<_>>();
                let names = (0..agg.aggregates.len())
                    .map(|idx| format!("agg_{}", idx))
                    .collect();
                let types = agg
                    .aggregates
                    .iter()
                    .map(|expr| expr.return_type())
                    .collect();
                return LogicalPlan {
                    id,
                    stats,
                    operator: LogicalOperator::ExpressionGet(ExpressionGet::new(
                        agg.aggregate_index,
                        vec![row],
                        names,
                        types,
                    )),
                };
            }
        }

        LogicalPlan {
            id,
            stats,
            operator,
        }
    }

    fn is_supported_aggregate(expr: &Expression) -> bool {
        let Expression::Aggregate(agg) = expr else {
            return false;
        };
        if agg.filter.is_some()
            || !agg.order_bys.is_empty()
            || agg.aggr_type != AggregateType::NonDistinct
        {
            return false;
        }
        let name = agg.function.name.to_lowercase();
        matches!(name.as_str(), "count_star" | "min" | "max" | "count")
    }

    fn execute_aggregate(
        expr: &Expression,
        child: &LogicalPlan,
        scan: &SimpleScanInfo,
    ) -> Option<AggregateResult> {
        let Expression::Aggregate(agg) = expr else {
            return None;
        };
        if agg.filter.is_some()
            || !agg.order_bys.is_empty()
            || agg.aggr_type != AggregateType::NonDistinct
        {
            return None;
        }

        let name = agg.function.name.to_lowercase();
        match name.as_str() {
            "count_star" => {
                if !agg.children.is_empty() {
                    return None;
                }
                Some(AggregateResult {
                    aggregate_type: StatisticsAggregate::CountStar,
                    value: Self::visible_rows_value(scan),
                })
            }
            "count" => {
                if !agg.children.is_empty() {
                    return None;
                }
                Some(AggregateResult {
                    aggregate_type: StatisticsAggregate::CountStar,
                    value: Self::visible_rows_value(scan),
                })
            }
            "min" | "max" => {
                if agg.children.len() != 1 {
                    return None;
                }
                let Expression::ColumnRef(col_ref) = &agg.children[0] else {
                    return None;
                };
                let stats = Self::resolve_column_statistics(&child.operator, col_ref.binding)?;
                let value = Self::extract_min_max(stats.as_ref(), name == "min", &agg.return_type)?;
                Some(AggregateResult {
                    aggregate_type: if name == "min" {
                        StatisticsAggregate::Min
                    } else {
                        StatisticsAggregate::Max
                    },
                    value,
                })
            }
            _ => None,
        }
    }

    fn visible_rows_value(scan: &SimpleScanInfo) -> Value {
        let visible_rows = scan
            .storage
            .tablet()
            .statistics()
            .map(|stats| {
                stats
                    .num_rows
                    .saturating_sub(stats.delete_stats.num_deleted_rows) as usize
            })
            .unwrap_or_else(|_| {
                scan.storage
                    .total_rows()
                    .saturating_sub(scan.storage.deleted_row_count())
            });
        let count = i64::try_from(visible_rows).unwrap_or(i64::MAX);
        Value::BigInt(count)
    }

    fn resolve_simple_scan(plan: &LogicalPlan) -> Option<SimpleScanInfo> {
        let mut current = &plan.operator;
        loop {
            match current {
                LogicalOperator::Projection(proj) => {
                    current = &proj.child.operator;
                }
                LogicalOperator::Get(get) => {
                    let table = get.table.as_ref()?;
                    let storage = table.get_storage()?.clone();
                    return Some(SimpleScanInfo { storage });
                }
                _ => return None,
            }
        }
    }

    fn resolve_column_statistics(
        op: &LogicalOperator,
        binding: ColumnBinding,
    ) -> Option<Arc<ColumnStatistics>> {
        let mut current = op;
        let mut current_binding = binding;
        loop {
            match current {
                LogicalOperator::Projection(proj) => {
                    if current_binding.table_index != proj.table_index {
                        return None;
                    }
                    let expr = proj.expressions.get(current_binding.column_index)?;
                    let Expression::ColumnRef(col_ref) = expr else {
                        return None;
                    };
                    if col_ref.depth != 0 {
                        return None;
                    }
                    current_binding = col_ref.binding;
                    current = &proj.child.operator;
                }
                LogicalOperator::Get(get) => {
                    if current_binding.table_index != get.table_index {
                        return None;
                    }
                    let column_id = *get.column_ids.get(current_binding.column_index)?;
                    let table = get.table.as_ref()?;
                    let storage = table.get_storage()?;
                    let base = storage.column_statistics(column_id)?;
                    return Some(Arc::new(ColumnStatistics::new(base)));
                }
                _ => return None,
            }
        }
    }

    fn extract_min_max(
        stats: &ColumnStatistics,
        take_min: bool,
        return_type: &LogicalType,
    ) -> Option<Value> {
        let base = stats.statistics();
        if !base.can_have_no_null() {
            return Some(Value::Null(return_type.clone()));
        }

        let value = if base.get_type().is_numeric() || base.get_type().is_temporal() {
            if !NumericStats::has_min_max(base) {
                return None;
            }
            if take_min {
                NumericStats::min(base)?
            } else {
                NumericStats::max(base)?
            }
        } else if matches!(
            base.get_type(),
            LogicalType::Varchar | LogicalType::StringLiteral
        ) {
            let min = StringStats::min(base);
            let max = StringStats::max(base);
            if min > max {
                return None;
            }
            if take_min {
                Value::Varchar(min)
            } else {
                Value::Varchar(max)
            }
        } else {
            return None;
        };

        Some(value.cast(return_type).unwrap_or(value))
    }
}

impl Default for AggregateStatisticsExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use paro_function::aggregate::{AggregateFunction, AggregateInputData};
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::{AggregateExpression, ColumnRefExpression, Expression};
    use paro_storage::statistics::BaseStatistics;

    unsafe fn noop_initialize(_state: *mut u8) {}

    unsafe fn noop_update(
        _inputs: &[&Vector],
        _input_data: &AggregateInputData,
        _states: &Vector,
        _count: usize,
    ) {
    }

    unsafe fn noop_combine(
        _source: &Vector,
        _target: &Vector,
        _input_data: &AggregateInputData,
        _count: usize,
    ) {
    }

    unsafe fn noop_finalize(
        _states: &Vector,
        _input_data: &AggregateInputData,
        _result: &mut Vector,
        _count: usize,
    ) {
    }

    fn aggregate_expr(
        name: &str,
        children: Vec<Expression>,
        return_type: LogicalType,
    ) -> Expression {
        let function = AggregateFunction {
            name: name.to_string(),
            arguments: children.iter().map(|child| child.return_type()).collect(),
            return_type: return_type.clone(),
            state_size: 8,
            initialize: noop_initialize,
            update: noop_update,
            combine: noop_combine,
            finalize: noop_finalize,
            simple_update: None,
            destructor: None,
            varargs: None,
            bind_data: None,
        };
        Expression::Aggregate(AggregateExpression::new(function, children, return_type))
    }

    #[test]
    fn can_execute_for_simple_ungrouped_aggregate() {
        let bind_context = BindContext::new();
        let expr = aggregate_expr("count_star", vec![], LogicalType::BigInt);
        let agg = Aggregate::new(
            0,
            1,
            2,
            LogicalPlan::new(&bind_context, LogicalOperator::DummyScan),
            vec![],
            vec![],
            vec![expr],
            vec![],
        );

        let executor = AggregateStatisticsExecutor::new();
        assert!(executor.can_execute(&agg));
    }

    #[test]
    fn cannot_execute_when_group_exists() {
        let bind_context = BindContext::new();
        let expr = aggregate_expr("count_star", vec![], LogicalType::BigInt);
        let group_expr = Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(0, 0),
            LogicalType::Integer,
        ));
        let agg = Aggregate::new(
            0,
            1,
            2,
            LogicalPlan::new(&bind_context, LogicalOperator::DummyScan),
            vec![group_expr],
            vec![],
            vec![expr],
            vec![],
        );

        let executor = AggregateStatisticsExecutor::new();
        assert!(!executor.can_execute(&agg));
    }

    #[test]
    fn extract_numeric_min_max() {
        let mut base = BaseStatistics::new(LogicalType::Integer);
        base.set_has_no_null_fast();
        NumericStats::set_min(&mut base, &Value::Integer(10));
        NumericStats::set_max(&mut base, &Value::Integer(42));
        let stats = ColumnStatistics::new(base);

        assert_eq!(
            AggregateStatisticsExecutor::extract_min_max(&stats, true, &LogicalType::Integer),
            Some(Value::Integer(10))
        );
        assert_eq!(
            AggregateStatisticsExecutor::extract_min_max(&stats, false, &LogicalType::Integer),
            Some(Value::Integer(42))
        );
    }
}
