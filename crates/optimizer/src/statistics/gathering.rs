// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_parser::ast::PathQuantifier;
use paro_planner::expression::{
    ComparisonExpression, ComparisonType, ConstantExpression, Expression,
};
use paro_planner::operator::{
    ColumnBinding, Filter, FullTextFilterScan, Get, GraphExpand, GraphScan, Join,
    JoinComparisonType, JoinCondition, JoinType, LogicalOperator, SearchScan, SetOpType,
};
use paro_planner::plan::{CardinalityEstimate, CardinalityProvenance, LogicalPlan};
use paro_storage::index::graph::GraphStatsProvider;
use paro_storage::statistics::{BaseStatistics, ColumnStatistics};

use crate::context::OptimizationContext;
use crate::statistics::aggregate_filter::estimate_grouped_sum_filter_selectivity;

#[derive(Default)]
pub struct StatisticsGathering {
    cte_cardinality: HashMap<usize, CardinalityEstimate>,
    cte_output_stats: HashMap<usize, Vec<Arc<ColumnStatistics>>>,
}

impl StatisticsGathering {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn gather(
        &mut self,
        plan: LogicalPlan,
        ctx: &mut OptimizationContext,
    ) -> Result<LogicalPlan> {
        let mut plan = plan.try_map_children(|child| self.gather(child, ctx))?;
        if plan.stats.cardinality_provenance != CardinalityProvenance::JoinGraph {
            plan.stats.estimated_cardinality = self.estimate_plan_cardinality(&plan, ctx);
            plan.stats.cardinality_provenance = CardinalityProvenance::Statistics;
        }
        self.update_output_column_stats(&plan, ctx);
        Ok(plan)
    }

    fn estimate_plan_cardinality(
        &mut self,
        plan: &LogicalPlan,
        ctx: &mut OptimizationContext,
    ) -> Option<CardinalityEstimate> {
        match &plan.operator {
            LogicalOperator::DummyScan => None,
            LogicalOperator::Get(get) => Some(CardinalityEstimate::exact(
                self.get_storage_rows(get, ctx) as u64,
            )),
            LogicalOperator::ExpressionGet(get) => {
                Some(CardinalityEstimate::exact(get.expressions.len() as u64))
            }
            LogicalOperator::DelimGet(_) => Some(CardinalityEstimate::exact(1)),
            LogicalOperator::TableFunctionGet(_) => Some(CardinalityEstimate::exact(100)),
            LogicalOperator::Projection(proj) => proj.child.stats.estimated_cardinality,
            LogicalOperator::ExternalProject(project) => project.child.stats.estimated_cardinality,
            LogicalOperator::ExternalTable(table) => table
                .child
                .as_ref()
                .and_then(|child| child.stats.estimated_cardinality)
                .or(Some(CardinalityEstimate::exact(100))),
            LogicalOperator::Order(order) => order.child.stats.estimated_cardinality,
            LogicalOperator::Window(window) => window.child.stats.estimated_cardinality,
            LogicalOperator::Distinct(distinct) => {
                self.estimate_distinct_cardinality(distinct.child.as_ref(), ctx)
            }
            LogicalOperator::Filter(filter) => self.estimate_filter_cardinality(filter, ctx),
            LogicalOperator::Limit(limit) => {
                let child = limit.child.stats.estimated_cardinality?;
                Some(apply_limit_estimate(
                    child,
                    limit.limit.as_ref().and_then(extract_constant_usize),
                    limit
                        .offset
                        .as_ref()
                        .and_then(extract_constant_usize)
                        .unwrap_or(0),
                ))
            }
            LogicalOperator::TopN(topn) => {
                let child = topn.child.stats.estimated_cardinality?;
                Some(apply_limit_estimate(child, Some(topn.limit), topn.offset))
            }
            LogicalOperator::Aggregate(agg) => {
                let child = agg.child.stats.estimated_cardinality?;
                if agg.groups.is_empty() {
                    return Some(CardinalityEstimate::exact(1));
                }
                let mut expected = 1u64;
                let mut saw_known = false;
                for group in &agg.groups {
                    let distinct = estimate_group_distinct(group, ctx, child.expected);
                    saw_known |= distinct != fallback_group_distinct(child.expected);
                    expected = saturating_mul_u64(expected, distinct.max(1)).min(child.expected);
                }
                if !agg.grouping_sets.is_empty() {
                    expected = saturating_mul_u64(expected, agg.grouping_sets.len() as u64)
                        .min(child.expected.max(1));
                }
                if !saw_known {
                    expected = expected.min(child.expected.max(1));
                }
                Some(CardinalityEstimate {
                    min: expected.saturating_div(2).max(1).min(expected),
                    expected: expected.max(1).min(child.expected.max(1)),
                    max: expected.saturating_mul(2).min(child.max.max(1)),
                })
            }
            LogicalOperator::Join(join) => self.estimate_join_cardinality(join, ctx),
            LogicalOperator::DependentJoin(join) => {
                let left = join.left.stats.estimated_cardinality?;
                let right = join.right.stats.estimated_cardinality?;
                Some(product_estimate(left, right))
            }
            LogicalOperator::SetOperation(setop) => {
                let left = setop.left.stats.estimated_cardinality?;
                let right = setop.right.stats.estimated_cardinality?;
                Some(match (setop.setop_type, setop.setop_all) {
                    (SetOpType::Union, true) => sum_estimate(left, right),
                    (SetOpType::Union, false) => CardinalityEstimate {
                        min: left.expected.max(right.expected),
                        expected: left
                            .expected
                            .max(right.expected)
                            .saturating_add(left.expected.min(right.expected) / 2),
                        max: left.max.saturating_add(right.max),
                    },
                    (SetOpType::Intersect, _) => CardinalityEstimate {
                        min: 0,
                        expected: left.expected.min(right.expected),
                        max: left.max.min(right.max),
                    },
                    (SetOpType::Except, _) => CardinalityEstimate {
                        min: 0,
                        expected: left.expected,
                        max: left.max,
                    },
                })
            }
            LogicalOperator::EmptyResult(_) => Some(CardinalityEstimate::exact(0)),
            LogicalOperator::MaterializedCTE(cte) => {
                if let Some(cardinality) = cte.cte_query.stats.estimated_cardinality {
                    self.cte_cardinality.insert(cte.cte_index, cardinality);
                }
                self.cte_output_stats.insert(
                    cte.cte_index,
                    collect_output_stats(cte.cte_query.as_ref(), ctx),
                );
                cte.child.stats.estimated_cardinality
            }
            LogicalOperator::RecursiveCTE(cte) => {
                let anchor = cte.anchor.stats.estimated_cardinality?;
                let recursive = cte.recursive.stats.estimated_cardinality.unwrap_or(anchor);
                let estimate = CardinalityEstimate {
                    min: anchor.min,
                    expected: anchor.expected.max(recursive.expected),
                    max: anchor.max.saturating_add(recursive.max),
                };
                self.cte_cardinality.insert(cte.cte_index, estimate);
                self.cte_output_stats.insert(
                    cte.cte_index,
                    collect_output_stats(cte.anchor.as_ref(), ctx),
                );
                Some(estimate)
            }
            LogicalOperator::CTERef(cte_ref) => {
                self.cte_cardinality.get(&cte_ref.cte_index).copied()
            }
            LogicalOperator::SearchScan(search) => {
                Some(self.estimate_search_scan_cardinality(search, ctx))
            }
            LogicalOperator::FullTextFilterScan(scan) => {
                Some(self.estimate_fulltext_filter_cardinality(scan, ctx))
            }
            LogicalOperator::GraphMatch(_) => None,
            LogicalOperator::GraphScan(scan) => {
                Some(self.estimate_graph_scan_cardinality(scan, ctx))
            }
            LogicalOperator::GraphExpand(expand) => {
                Some(self.estimate_graph_expand_cardinality(expand, ctx))
            }
            LogicalOperator::Explain(explain) => explain.child.stats.estimated_cardinality,
            LogicalOperator::Insert(_)
            | LogicalOperator::Delete(_)
            | LogicalOperator::Update(_) => Some(CardinalityEstimate::exact(1)),
            LogicalOperator::CopyTo(_) => Some(CardinalityEstimate::exact(1)),
            LogicalOperator::Alter(_)
            | LogicalOperator::CreateTable(_)
            | LogicalOperator::CreateRoutine(_)
            | LogicalOperator::CreateSequence(_)
            | LogicalOperator::CreateSchema(_)
            | LogicalOperator::CreateIndex(_)
            | LogicalOperator::CreateView(_)
            | LogicalOperator::CreatePropertyGraph(_)
            | LogicalOperator::DropPropertyGraph(_)
            | LogicalOperator::RefreshPropertyGraph(_)
            | LogicalOperator::Drop(_) => None,
        }
    }

    fn estimate_filter_cardinality(
        &self,
        filter: &Filter,
        ctx: &OptimizationContext,
    ) -> Option<CardinalityEstimate> {
        let child = filter.child.stats.estimated_cardinality?;
        if let Some(selectivity) =
            estimate_grouped_sum_filter_selectivity(filter, &ctx.column_stats)
        {
            return Some(
                ctx.cost_model
                    .estimate_cardinality_from_selectivity(child.expected, selectivity),
            );
        }
        let child_bindings = filter.child.get_column_bindings();
        Some(ctx.cost_model.estimate_filter_cardinality_with_positions(
            child.expected,
            &filter.expressions,
            &ctx.column_stats,
            &child_bindings,
        ))
    }

    fn estimate_distinct_cardinality(
        &self,
        child: &LogicalPlan,
        ctx: &OptimizationContext,
    ) -> Option<CardinalityEstimate> {
        let child_est = child.stats.estimated_cardinality?;
        let stats = collect_output_stats(child, ctx);
        let mut expected = 1u64;
        let mut saw_distinct = false;
        for stat in stats {
            let distinct = stat.get_distinct_count() as u64;
            if distinct > 0 {
                expected = saturating_mul_u64(expected, distinct).min(child_est.expected.max(1));
                saw_distinct = true;
            }
        }
        if !saw_distinct {
            expected = fallback_group_distinct(child_est.expected);
        }
        Some(CardinalityEstimate {
            min: expected.saturating_div(2),
            expected: expected.min(child_est.expected.max(1)),
            max: expected.saturating_mul(2).min(child_est.max.max(1)),
        })
    }

    fn estimate_join_cardinality(
        &self,
        join: &Join,
        ctx: &OptimizationContext,
    ) -> Option<CardinalityEstimate> {
        match join {
            Join::Cross(cross) => Some(product_estimate(
                cross.left.stats.estimated_cardinality?,
                cross.right.stats.estimated_cardinality?,
            )),
            Join::Any(any) => {
                let left = any.left.stats.estimated_cardinality?;
                let right = any.right.stats.estimated_cardinality?;
                let selectivity = ctx
                    .cost_model
                    .estimate_selectivity(&any.condition, &ctx.column_stats);
                Some(adjust_join_estimate(
                    apply_selectivity(product_estimate(left, right), selectivity),
                    left,
                    right,
                    any.join_type,
                ))
            }
            Join::Comparison(cmp) => {
                let left = cmp.left.stats.estimated_cardinality?;
                let right = cmp.right.stats.estimated_cardinality?;
                let left_bindings = cmp.left.get_column_bindings();
                let right_bindings = cmp.right.get_column_bindings();
                let selectivity = estimate_comparison_join_selectivity(
                    &cmp.conditions,
                    &left_bindings,
                    &right_bindings,
                    ctx,
                );
                Some(adjust_join_estimate(
                    apply_selectivity(product_estimate(left, right), selectivity),
                    left,
                    right,
                    cmp.join_type,
                ))
            }
        }
    }

    fn estimate_search_scan_cardinality(
        &self,
        search: &SearchScan,
        ctx: &OptimizationContext,
    ) -> CardinalityEstimate {
        let base_rows = self.get_storage_rows(&search.get, ctx) as u64;
        let mut expressions = Vec::new();
        expressions.extend(search.absorbed_predicates.iter().cloned());
        expressions.extend(search.residual_predicates.iter().cloned());
        expressions.extend(search.get.runtime_filter_expressions.iter().cloned());
        let filtered =
            ctx.cost_model
                .estimate_filter_cardinality(base_rows, &expressions, &ctx.column_stats);
        apply_limit_estimate(filtered, Some(search.limit), 0)
    }

    fn estimate_fulltext_filter_cardinality(
        &self,
        scan: &FullTextFilterScan,
        ctx: &OptimizationContext,
    ) -> CardinalityEstimate {
        let base_rows = self.get_storage_rows(&scan.get, ctx) as u64;
        let mut expressions = vec![scan.match_expression.clone()];
        expressions.extend(scan.other_predicates.iter().cloned());
        expressions.extend(scan.residual_predicates.iter().cloned());
        expressions.extend(scan.get.runtime_filter_expressions.iter().cloned());
        ctx.cost_model
            .estimate_filter_cardinality(base_rows, &expressions, &ctx.column_stats)
    }

    fn estimate_graph_scan_cardinality(
        &self,
        scan: &GraphScan,
        ctx: &mut OptimizationContext,
    ) -> CardinalityEstimate {
        let base = ctx
            .graph_stats
            .get(&scan.graph_name)
            .and_then(|stats| stats.vertex_count(&scan.label))
            .unwrap_or(if scan.filter.is_some() { 100 } else { 1000 });
        let expected = if let Some(filter) = &scan.filter {
            let selectivity = ctx
                .cost_model
                .estimate_selectivity(filter, &ctx.column_stats);
            ((base as f64) * selectivity).ceil() as u64
        } else {
            base
        }
        .max(1);
        CardinalityEstimate {
            min: if scan.filter.is_some() {
                expected.saturating_div(2)
            } else {
                expected
            },
            expected,
            max: base.max(expected),
        }
    }

    fn estimate_graph_expand_cardinality(
        &self,
        expand: &GraphExpand,
        ctx: &mut OptimizationContext,
    ) -> CardinalityEstimate {
        let child = expand
            .child
            .stats
            .estimated_cardinality
            .unwrap_or(CardinalityEstimate::exact(1000));
        let stats = graph_name_for_plan(expand.child.as_ref())
            .and_then(|graph_name| ctx.graph_stats.get(graph_name));
        let (min_hops, max_hops) = quantifier_bounds(expand.quantifier.as_ref());
        let hop_multiplier = hop_multiplier(min_hops, max_hops);
        let factor = stats
            .as_ref()
            .map(|stats| {
                estimate_expand_factor(
                    stats.as_ref(),
                    &expand.source_label,
                    &expand.edge_info.label,
                    &expand.target_label,
                    expand.direction,
                )
            })
            .unwrap_or(4.0);
        let expected =
            ((child.expected.max(1) as f64) * factor.max(0.01) * hop_multiplier).ceil() as u64;
        CardinalityEstimate {
            min: expected.saturating_div(2).max(1),
            expected: expected.max(1),
            max: expected.saturating_mul(2),
        }
    }

    fn update_output_column_stats(&mut self, plan: &LogicalPlan, ctx: &mut OptimizationContext) {
        let output_stats = match &plan.operator {
            LogicalOperator::Get(get) => self.get_output_stats(get, ctx),
            LogicalOperator::Projection(proj) => proj
                .expressions
                .iter()
                .map(|expr| expression_statistics(expr, ctx))
                .collect(),
            LogicalOperator::ExternalProject(project) => {
                let mut stats = collect_output_stats(project.child.as_ref(), ctx);
                stats.extend(
                    project
                        .expressions
                        .iter()
                        .map(|expr| expression_statistics(&expr.expression, ctx)),
                );
                stats
            }
            LogicalOperator::ExternalTable(table) => unknown_stats_for_types(&table.returned_types),
            LogicalOperator::Aggregate(agg) => {
                let mut stats = Vec::new();
                stats.extend(
                    agg.groups
                        .iter()
                        .map(|expr| expression_statistics(expr, ctx)),
                );
                stats.extend(
                    agg.aggregates
                        .iter()
                        .map(|expr| aggregate_expression_statistics(expr, ctx)),
                );
                stats.extend(
                    agg.grouping_functions
                        .iter()
                        .map(|_| ColumnStatistics::create_unknown(LogicalType::BigInt)),
                );
                stats
            }
            LogicalOperator::SetOperation(setop) => merge_setop_output_stats(
                setop.left.as_ref(),
                setop.right.as_ref(),
                &setop.types,
                ctx,
            ),
            LogicalOperator::RecursiveCTE(cte) => self
                .cte_output_stats
                .get(&cte.cte_index)
                .cloned()
                .unwrap_or_else(|| unknown_stats_for_types(&cte.column_types)),
            LogicalOperator::CTERef(cte_ref) => self
                .cte_output_stats
                .get(&cte_ref.cte_index)
                .cloned()
                .unwrap_or_else(|| unknown_stats_for_types(&cte_ref.column_types)),
            LogicalOperator::SearchScan(search) => search
                .projections
                .iter()
                .map(|expr| expression_statistics(expr, ctx))
                .collect(),
            LogicalOperator::FullTextFilterScan(scan) => self.get_output_stats(&scan.get, ctx),
            _ => collect_output_stats(plan, ctx),
        };

        for (binding, stats) in plan.get_column_bindings().into_iter().zip(output_stats) {
            ctx.column_stats.insert(binding, stats);
        }
    }

    fn get_output_stats(
        &self,
        get: &Get,
        _ctx: &OptimizationContext,
    ) -> Vec<Arc<ColumnStatistics>> {
        let Some(table) = &get.table else {
            return unknown_stats_for_types(&get.returned_types);
        };
        let Some(storage) = table.get_storage() else {
            return unknown_stats_for_types(&get.returned_types);
        };
        get.column_ids
            .iter()
            .enumerate()
            .map(|(idx, &column_id)| {
                storage
                    .column_statistics(column_id)
                    .map(Arc::new)
                    .unwrap_or_else(|| {
                        ColumnStatistics::create_unknown(
                            get.returned_types
                                .get(idx)
                                .cloned()
                                .unwrap_or(LogicalType::Integer),
                        )
                    })
            })
            .collect()
    }

    fn get_storage_rows(&self, get: &Get, ctx: &OptimizationContext) -> usize {
        get.table
            .as_ref()
            .and_then(|table| table.get_storage())
            .map(|storage| storage.total_rows().max(1))
            .unwrap_or_else(|| default_table_cardinality(ctx))
    }
}

/// Estimate a comparison join without manufacturing independence between
/// marginal statistics of one composite relation pair.
///
/// Two equality keys between the same aliases are commonly a composite key.
/// Multiplying their individual NDV selectivities can underestimate the join
/// by orders of magnitude unless joint-domain statistics prove independence.
/// Keep the strongest equality domain for each concrete alias pair, while
/// conditions connecting different pairs and non-equality residuals remain
/// independent factors. This matches the correlation contract used by the
/// join-order estimator and keeps post-reorder statistics from reversing a
/// sound build/probe decision.
fn estimate_comparison_join_selectivity(
    conditions: &[JoinCondition],
    left_bindings: &[ColumnBinding],
    right_bindings: &[ColumnBinding],
    ctx: &OptimizationContext,
) -> f64 {
    correlate_join_condition_selectivities(conditions.iter().map(|condition| {
        (
            equality_relation_pair(condition, left_bindings, right_bindings),
            estimate_join_condition_selectivity(condition, left_bindings, right_bindings, ctx),
        )
    }))
}

fn correlate_join_condition_selectivities(
    conditions: impl IntoIterator<Item = (Option<(usize, usize)>, f64)>,
) -> f64 {
    let mut equality_by_relation_pair = HashMap::<(usize, usize), f64>::new();
    let mut independent_selectivity = 1.0;

    for (relation_pair, selectivity) in conditions {
        if let Some(pair) = relation_pair {
            equality_by_relation_pair
                .entry(pair)
                .and_modify(|strongest| *strongest = strongest.min(selectivity))
                .or_insert(selectivity);
        } else {
            independent_selectivity *= selectivity;
        }
    }

    equality_by_relation_pair
        .values()
        .fold(independent_selectivity, |product, selectivity| {
            product * selectivity
        })
        .clamp(0.0, 1.0)
}

fn equality_relation_pair(
    condition: &JoinCondition,
    left_bindings: &[ColumnBinding],
    right_bindings: &[ColumnBinding],
) -> Option<(usize, usize)> {
    if !matches!(
        condition.comparison,
        JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
    ) {
        return None;
    }
    let left = expression_binding(&condition.left, left_bindings)?;
    let right = expression_binding(&condition.right, right_bindings)?;
    let pair = (left.table_index, right.table_index);
    Some(if pair.0 <= pair.1 {
        pair
    } else {
        (pair.1, pair.0)
    })
}

fn expression_binding(
    expression: &Expression,
    positional_bindings: &[ColumnBinding],
) -> Option<ColumnBinding> {
    match expression {
        Expression::ColumnRef(column) => Some(column.binding),
        Expression::Reference(reference) => positional_bindings.get(reference.index).copied(),
        // A cast preserves column lineage for correlation purposes. It changes
        // the comparison domain, whose selectivity is still estimated by the
        // ordinary expression model, but not which aliases form the pair.
        Expression::Cast(cast) => expression_binding(cast.child.as_ref(), positional_bindings),
        _ => None,
    }
}

fn collect_output_stats(
    plan: &LogicalPlan,
    ctx: &impl ColumnStatsView,
) -> Vec<Arc<ColumnStatistics>> {
    plan.operator
        .types()
        .into_iter()
        .zip(plan.get_column_bindings())
        .map(|(ty, binding)| {
            ctx.get_stat(&binding)
                .unwrap_or_else(|| ColumnStatistics::create_unknown(ty))
        })
        .collect()
}

fn merge_setop_output_stats(
    left: &LogicalPlan,
    right: &LogicalPlan,
    types: &[LogicalType],
    ctx: &impl ColumnStatsView,
) -> Vec<Arc<ColumnStatistics>> {
    let left_bindings = left.get_column_bindings();
    let right_bindings = right.get_column_bindings();
    types
        .iter()
        .enumerate()
        .map(|(idx, ty)| {
            let left_stats = left_bindings
                .get(idx)
                .and_then(|binding| ctx.get_stat(binding));
            let right_stats = right_bindings
                .get(idx)
                .and_then(|binding| ctx.get_stat(binding));
            merge_column_statistics(left_stats, right_stats, ty.clone())
        })
        .collect()
}

fn merge_column_statistics(
    left: Option<Arc<ColumnStatistics>>,
    right: Option<Arc<ColumnStatistics>>,
    ty: LogicalType,
) -> Arc<ColumnStatistics> {
    match (left, right) {
        (Some(left), Some(right)) => {
            let mut merged = left.copy();
            merged.merge(right.as_ref());
            Arc::new(merged)
        }
        (Some(left), None) => left,
        (None, Some(right)) => right,
        (None, None) => ColumnStatistics::create_unknown(ty),
    }
}

fn aggregate_expression_statistics(
    expr: &Expression,
    ctx: &impl ColumnStatsView,
) -> Arc<ColumnStatistics> {
    let Expression::Aggregate(agg) = expr else {
        return expression_statistics(expr, ctx);
    };

    let name = agg.function.name.to_ascii_lowercase();
    match name.as_str() {
        "count" | "count_star" => Arc::new(ColumnStatistics::new(BaseStatistics::new(
            LogicalType::BigInt,
        ))),
        "min" | "max" if agg.children.len() == 1 => expression_statistics(&agg.children[0], ctx),
        _ => ColumnStatistics::create_unknown(agg.return_type.clone()),
    }
}

fn expression_statistics(expr: &Expression, ctx: &impl ColumnStatsView) -> Arc<ColumnStatistics> {
    match expr {
        Expression::ColumnRef(col_ref) => ctx
            .get_stat(&col_ref.binding)
            .unwrap_or_else(|| ColumnStatistics::create_unknown(col_ref.return_type.clone())),
        Expression::Constant(constant) => Arc::new(ColumnStatistics::new(
            BaseStatistics::from_constant(&constant.value),
        )),
        Expression::Cast(cast) => ColumnStatistics::create_unknown(cast.target_type.clone()),
        Expression::Reference(reference) => {
            ColumnStatistics::create_unknown(reference.return_type.clone())
        }
        _ => ColumnStatistics::create_unknown(expr.return_type()),
    }
}

fn estimate_group_distinct(expr: &Expression, ctx: &impl ColumnStatsView, child_rows: u64) -> u64 {
    match expr {
        Expression::ColumnRef(col_ref) => ctx
            .get_stat(&col_ref.binding)
            .map(|stats| stats.get_distinct_count() as u64)
            .filter(|count| *count > 0)
            .unwrap_or_else(|| fallback_group_distinct(child_rows)),
        Expression::Constant(_) => 1,
        _ => fallback_group_distinct(child_rows),
    }
}

fn fallback_group_distinct(child_rows: u64) -> u64 {
    ((child_rows.max(1) as f64).sqrt().ceil() as u64).max(1)
}

fn default_table_cardinality(ctx: &OptimizationContext) -> usize {
    match ctx.session.get_setting("default_table_cardinality") {
        Some(Value::BigInt(v)) if *v > 0 => *v as usize,
        Some(Value::Integer(v)) if *v > 0 => *v as usize,
        _ => 1000,
    }
}

fn apply_limit_estimate(
    estimate: CardinalityEstimate,
    limit: Option<usize>,
    offset: usize,
) -> CardinalityEstimate {
    fn apply_one(value: u64, limit: Option<u64>, offset: u64) -> u64 {
        let after_offset = value.saturating_sub(offset);
        match limit {
            Some(limit) => after_offset.min(limit),
            None => after_offset,
        }
    }

    let limit = limit.map(|v| v as u64);
    let offset = offset as u64;
    let min = apply_one(estimate.min, limit, offset);
    let expected = apply_one(estimate.expected, limit, offset);
    let max = apply_one(estimate.max, limit, offset).max(expected);
    CardinalityEstimate { min, expected, max }
}

fn product_estimate(left: CardinalityEstimate, right: CardinalityEstimate) -> CardinalityEstimate {
    CardinalityEstimate {
        min: saturating_mul_u64(left.min, right.min),
        expected: saturating_mul_u64(left.expected, right.expected),
        max: saturating_mul_u64(left.max, right.max),
    }
}

fn sum_estimate(left: CardinalityEstimate, right: CardinalityEstimate) -> CardinalityEstimate {
    CardinalityEstimate {
        min: left.min.saturating_add(right.min),
        expected: left.expected.saturating_add(right.expected),
        max: left.max.saturating_add(right.max),
    }
}

fn apply_selectivity(estimate: CardinalityEstimate, selectivity: f64) -> CardinalityEstimate {
    fn apply_one(value: u64, selectivity: f64) -> u64 {
        ((value as f64) * selectivity).round() as u64
    }

    CardinalityEstimate {
        min: apply_one(estimate.min, selectivity * 0.5)
            .min(apply_one(estimate.expected, selectivity)),
        expected: apply_one(estimate.expected, selectivity),
        max: apply_one(estimate.max, (selectivity * 1.5).clamp(0.0, 1.0))
            .max(apply_one(estimate.expected, selectivity)),
    }
}

fn adjust_join_estimate(
    inner: CardinalityEstimate,
    left: CardinalityEstimate,
    right: CardinalityEstimate,
    join_type: JoinType,
) -> CardinalityEstimate {
    match join_type {
        JoinType::Inner => inner,
        JoinType::Left => CardinalityEstimate {
            min: left.min,
            expected: inner.expected.max(left.expected),
            max: inner.max.max(left.max),
        },
        JoinType::Right => CardinalityEstimate {
            min: right.min,
            expected: inner.expected.max(right.expected),
            max: inner.max.max(right.max),
        },
        JoinType::Outer => CardinalityEstimate {
            min: left.min.max(right.min),
            expected: inner.expected.max(left.expected.max(right.expected)),
            max: inner.max.max(left.max.saturating_add(right.max)),
        },
        JoinType::Semi | JoinType::Mark | JoinType::Single => CardinalityEstimate {
            min: 0,
            expected: inner.expected.min(left.expected),
            max: left.max,
        },
        JoinType::Anti => {
            let semi = inner.expected.min(left.expected);
            CardinalityEstimate {
                min: 0,
                expected: left.expected.saturating_sub(semi),
                max: left.max,
            }
        }
        JoinType::RightSemi => CardinalityEstimate {
            min: 0,
            expected: inner.expected.min(right.expected),
            max: right.max,
        },
        JoinType::RightAnti => {
            let semi = inner.expected.min(right.expected);
            CardinalityEstimate {
                min: 0,
                expected: right.expected.saturating_sub(semi),
                max: right.max,
            }
        }
        JoinType::Invalid => inner,
    }
}

fn estimate_join_condition_selectivity(
    condition: &JoinCondition,
    left_bindings: &[ColumnBinding],
    right_bindings: &[ColumnBinding],
    ctx: &OptimizationContext,
) -> f64 {
    match condition.comparison {
        JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom => {
            if let (Some(left), Some(right)) = (
                expression_binding(&condition.left, left_bindings),
                expression_binding(&condition.right, right_bindings),
            ) {
                let left_distinct = ctx
                    .column_stats
                    .get(&left)
                    .map(|stats| stats.get_distinct_count())
                    .unwrap_or(0);
                let right_distinct = ctx
                    .column_stats
                    .get(&right)
                    .map(|stats| stats.get_distinct_count())
                    .unwrap_or(0);
                if left_distinct > 0 && right_distinct > 0 {
                    return (1.0 / left_distinct.max(right_distinct) as f64).clamp(0.0, 1.0);
                }
            }
        }
        _ => {}
    }

    let expr = Expression::Comparison(ComparisonExpression {
        left: Box::new(condition.left.clone()),
        right: Box::new(condition.right.clone()),
        comparison_type: join_comparison_to_comparison(condition.comparison),
    });
    ctx.cost_model
        .estimate_selectivity(&expr, &ctx.column_stats)
}

fn join_comparison_to_comparison(comparison: JoinComparisonType) -> ComparisonType {
    match comparison {
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

fn extract_constant_usize(expr: &Expression) -> Option<usize> {
    let Expression::Constant(ConstantExpression { value, .. }) = expr else {
        return None;
    };
    match value {
        Value::TinyInt(v) if *v >= 0 => Some(*v as usize),
        Value::SmallInt(v) if *v >= 0 => Some(*v as usize),
        Value::Integer(v) if *v >= 0 => Some(*v as usize),
        Value::BigInt(v) if *v >= 0 => Some(*v as usize),
        Value::UTinyInt(v) => Some(*v as usize),
        Value::USmallInt(v) => Some(*v as usize),
        Value::UInteger(v) => Some(*v as usize),
        Value::UBigInt(v) => usize::try_from(*v).ok(),
        _ => None,
    }
}

fn unknown_stats_for_types(types: &[LogicalType]) -> Vec<Arc<ColumnStatistics>> {
    types
        .iter()
        .cloned()
        .map(ColumnStatistics::create_unknown)
        .collect()
}

fn saturating_mul_u64(left: u64, right: u64) -> u64 {
    let product = (left as u128) * (right as u128);
    product.min(u64::MAX as u128) as u64
}

fn quantifier_bounds(quantifier: Option<&PathQuantifier>) -> (u64, u64) {
    match quantifier {
        None => (1, 1),
        Some(PathQuantifier::Plus) => (1, 4),
        Some(PathQuantifier::Star) => (0, 4),
        Some(PathQuantifier::Bounded { lower, upper }) => (*lower, upper.unwrap_or(4).min(4)),
    }
}

fn hop_multiplier(min_hops: u64, max_hops: u64) -> f64 {
    if min_hops == 1 && max_hops == 1 {
        1.0
    } else {
        max_hops.max(min_hops.max(1)) as f64
    }
}

fn estimate_expand_factor(
    stats: &dyn GraphStatsProvider,
    source_label: &str,
    edge_label: &str,
    target_label: &str,
    direction: paro_planner::operator::ExpandDirection,
) -> f64 {
    use paro_planner::operator::ExpandDirection;

    match direction {
        ExpandDirection::Forward => {
            estimate_pattern_factor(stats, source_label, edge_label, target_label)
        }
        ExpandDirection::Backward => {
            estimate_pattern_factor(stats, target_label, edge_label, source_label)
        }
        ExpandDirection::Both => {
            estimate_pattern_factor(stats, source_label, edge_label, target_label)
                + estimate_pattern_factor(stats, target_label, edge_label, source_label)
        }
    }
}

fn estimate_pattern_factor(
    stats: &dyn GraphStatsProvider,
    source_label: &str,
    edge_label: &str,
    target_label: &str,
) -> f64 {
    let source_count = stats.vertex_count(source_label).unwrap_or(1).max(1) as f64;
    stats
        .pattern_step_count(source_label, edge_label, target_label)
        .map(|count| (count as f64 / source_count).max(1.0 / source_count))
        .or_else(|| stats.avg_degree(source_label))
        .unwrap_or(1.0)
}

fn graph_name_for_plan(plan: &LogicalPlan) -> Option<&str> {
    match &plan.operator {
        LogicalOperator::GraphScan(scan) => Some(scan.graph_name.as_str()),
        LogicalOperator::GraphExpand(expand) => graph_name_for_plan(expand.child.as_ref()),
        LogicalOperator::Filter(filter) => graph_name_for_plan(filter.child.as_ref()),
        LogicalOperator::EmptyResult(empty) => graph_name_for_plan(empty.child.as_ref()),
        _ => None,
    }
}

trait ColumnStatsView {
    fn get_stat(&self, binding: &ColumnBinding) -> Option<Arc<ColumnStatistics>>;
}

impl ColumnStatsView for OptimizationContext {
    fn get_stat(&self, binding: &ColumnBinding) -> Option<Arc<ColumnStatistics>> {
        self.column_stats.get(binding).cloned()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_common::runtime_value::Value;
    use paro_context::test_support::TestStatementContextBuilder;
    use paro_context::StatementContext;
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::ColumnRefExpression;
    use paro_planner::operator::{ExpressionGet, Limit, Projection};

    use super::*;
    use crate::context::OptimizationContext;

    fn make_test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn column_ref(table_index: usize, column_index: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression {
            binding: ColumnBinding::new(table_index, column_index),
            depth: 0,
            return_type: LogicalType::BigInt,
        })
    }

    fn equality(
        left_table: usize,
        left_column: usize,
        right_table: usize,
        right_column: usize,
    ) -> JoinCondition {
        JoinCondition::new(
            column_ref(left_table, left_column),
            column_ref(right_table, right_column),
            JoinComparisonType::Equal,
        )
    }

    fn values_relation(bind_context: &BindContext, table_index: usize, rows: usize) -> LogicalPlan {
        LogicalPlan::new(
            bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                table_index,
                vec![
                    vec![Expression::Constant(ConstantExpression::new(
                        Value::BigInt(1),
                        LogicalType::BigInt,
                    ))];
                    rows
                ],
                vec!["v".to_string()],
                vec![LogicalType::BigInt],
            )),
        )
    }

    #[test]
    fn statistics_gathering_sets_expression_get_and_limit_cardinality() {
        let bind_context = BindContext::new();
        let session = make_test_session();
        let mut ctx = OptimizationContext::new(session, bind_context.clone());

        let child = LogicalPlan::new(
            &bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                1,
                vec![
                    vec![Expression::Constant(ConstantExpression::new(
                        Value::BigInt(1),
                        LogicalType::BigInt,
                    ))];
                    10
                ],
                vec!["v".to_string()],
                vec![LogicalType::BigInt],
            )),
        );
        let projection = LogicalPlan::new(
            &bind_context,
            LogicalOperator::Projection(Projection::new(
                2,
                child,
                vec![Expression::ColumnRef(ColumnRefExpression {
                    binding: ColumnBinding::new(1, 0),
                    depth: 0,
                    return_type: LogicalType::BigInt,
                })],
            )),
        );
        let plan = LogicalPlan::new(
            &bind_context,
            LogicalOperator::Limit(Limit::new(
                projection,
                Some(Expression::Constant(ConstantExpression::new(
                    Value::BigInt(3),
                    LogicalType::BigInt,
                ))),
                None,
            )),
        );

        let gathered = StatisticsGathering::new()
            .gather(plan, &mut ctx)
            .expect("gather should succeed");

        assert_eq!(
            gathered.stats.estimated_cardinality,
            Some(CardinalityEstimate::exact(3))
        );
        let LogicalOperator::Limit(limit) = &gathered.operator else {
            panic!("expected limit");
        };
        assert_eq!(
            limit.child.stats.estimated_cardinality,
            Some(CardinalityEstimate::exact(10))
        );
        assert!(ctx.column_stats.contains_key(&ColumnBinding::new(2, 0)));
    }

    #[test]
    fn statistics_gathering_preserves_join_graph_cardinality_provenance() {
        let bind_context = BindContext::new();
        let session = make_test_session();
        let mut ctx = OptimizationContext::new(session, bind_context.clone());
        let join = paro_planner::operator::ComparisonJoin::new(
            JoinType::Inner,
            values_relation(&bind_context, 1, 10),
            values_relation(&bind_context, 2, 20),
            vec![equality(1, 0, 2, 0)],
        );
        let mut plan =
            LogicalPlan::new(&bind_context, LogicalOperator::Join(Join::Comparison(join)));
        plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(73));
        plan.stats.cardinality_provenance = CardinalityProvenance::JoinGraph;

        let gathered = StatisticsGathering::new()
            .gather(plan, &mut ctx)
            .expect("gather should succeed");

        assert_eq!(
            gathered.stats.estimated_cardinality,
            Some(CardinalityEstimate::exact(73))
        );
        assert_eq!(
            gathered.stats.cardinality_provenance,
            CardinalityProvenance::JoinGraph
        );
    }

    #[test]
    fn composite_equalities_share_one_marginal_domain_per_relation_pair() {
        let conditions = [equality(0, 0, 1, 0), equality(0, 1, 1, 1)];
        let pair = equality_relation_pair(&conditions[0], &[], &[]);
        assert_eq!(pair, equality_relation_pair(&conditions[1], &[], &[]));
        let selectivity = correlate_join_condition_selectivities([
            (pair, 1.0 / 200_000.0),
            (pair, 1.0 / 10_000.0),
        ]);
        assert_eq!(selectivity, 1.0 / 200_000.0);
    }

    #[test]
    fn equalities_between_different_relation_pairs_remain_independent() {
        let first_pair = equality_relation_pair(&equality(0, 0, 1, 0), &[], &[]);
        let second_pair = equality_relation_pair(&equality(2, 0, 3, 0), &[], &[]);
        assert_ne!(first_pair, second_pair);
        let selectivity = correlate_join_condition_selectivities([
            (first_pair, 1.0 / 200_000.0),
            (first_pair, 1.0 / 10_000.0),
            (second_pair, 1.0 / 25.0),
        ]);
        assert_eq!(selectivity, 1.0 / 200_000.0 / 25.0);
    }

    #[test]
    fn physical_references_recover_their_relation_pair_from_join_inputs() {
        let condition = JoinCondition::new(
            Expression::Reference(paro_planner::expression::ReferenceExpression::new(
                1,
                LogicalType::BigInt,
            )),
            Expression::Reference(paro_planner::expression::ReferenceExpression::new(
                0,
                LogicalType::BigInt,
            )),
            JoinComparisonType::Equal,
        );
        assert_eq!(
            equality_relation_pair(
                &condition,
                &[ColumnBinding::new(4, 0), ColumnBinding::new(2, 1)],
                &[ColumnBinding::new(7, 3)]
            ),
            Some((2, 7))
        );
    }
}
