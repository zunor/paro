// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_parser::ast::PathQuantifier;
use paro_planner::expression::{ComparisonExpression, ComparisonType, ConjunctionType, Expression};
use paro_planner::operator::{
    BoundReference, ColumnBinding, Filter, FullTextFilterScan, Get, GraphExpand, GraphScan, Join,
    JoinComparisonType, JoinCondition, JoinType, LogicalOperator, LogicalOutputLayout, SearchScan,
    SetOpType,
};
use paro_planner::plan::{
    CardinalityEstimate, CardinalityProvenance, LogicalPlanPostOrderFolder, NodeStats,
    OwnedLogicalPlan,
};
use paro_storage::index::graph::GraphStatsProvider;
use paro_storage::statistics::{BaseStatistics, ColumnStatistics};

use crate::context::{GraphStatsCache, OptimizationContext, SharedColumnStatistics};
use crate::statistics::aggregate_filter::estimate_grouped_sum_distribution;

fn external_table_cardinality<Child: LocalChildFacts>(
    table: &paro_planner::operator::LogicalExternalTable<Child>,
) -> Option<CardinalityEstimate> {
    let declared_rows = table.call.spec.as_ref().and_then(|spec| {
        let paro_external::routine::spec::RoutineExecutionContract::Table(contract) =
            &spec.execution_contract
        else {
            return None;
        };
        contract.rows_hint
    });
    let per_invocation = match declared_rows {
        Some(expected) => CardinalityEstimate {
            min: 0,
            expected,
            // ROWS is an estimate, not an unearned hard bound. Keep a broad
            // finite interval until a versioned routine profile supplies one.
            max: expected.saturating_mul(16).max(expected),
        },
        None => CardinalityEstimate {
            min: 0,
            expected: 100,
            max: 1_000_000_000,
        },
    };
    let invocations = table
        .child
        .as_ref()
        .and_then(|child| child.estimated_cardinality())
        .unwrap_or_else(|| CardinalityEstimate::exact(1));
    Some(CardinalityEstimate {
        min: invocations.min.saturating_mul(per_invocation.min),
        expected: invocations.expected.saturating_mul(per_invocation.expected),
        max: invocations.max.saturating_mul(per_invocation.max),
    })
}

#[derive(Default)]
pub struct StatisticsGathering {
    cte_cardinality: HashMap<usize, CardinalityEstimate>,
    cte_output_stats: HashMap<usize, Vec<Arc<ColumnStatistics>>>,
    delim_cardinality: HashMap<usize, CardinalityEstimate>,
    delim_output_stats: HashMap<usize, Vec<Arc<ColumnStatistics>>>,
}

/// Set-operation inputs are positional domains, even when both branches use
/// identical bindings. A shared binding map cannot represent both snapshots.
pub(crate) fn merge_set_operation_column_statistics(
    output: &LogicalOutputLayout,
    left: &[Arc<ColumnStatistics>],
    right: &[Arc<ColumnStatistics>],
    context: &mut OptimizationContext,
) {
    for (ordinal, binding) in output.bindings().iter().enumerate() {
        if let (Some(left), Some(right)) = (left.get(ordinal), right.get(ordinal)) {
            let mut merged = left.as_ref().copy();
            merged.merge(right.as_ref());
            context
                .column_stats_mut()
                .insert(*binding, Arc::new(merged));
        }
    }
}

struct StatisticsGatherFolder<'a> {
    gathering: &'a mut StatisticsGathering,
    context: &'a mut OptimizationContext,
}

struct GatheredNodeProperties {
    layout: LogicalOutputLayout,
    maximum_cardinality: Option<u64>,
}

/// Estimation reads the completed inputs, not domains published by this
/// operator's simplification. In particular, `x = c` has an input NDV and a
/// different, singleton output NDV. Keeping the view separate prevents local
/// settlement from feeding the output proof back into input selectivity.
struct CardinalityInputs<'a> {
    column_stats: &'a SharedColumnStatistics,
    cost_model: &'a crate::cost_model::CostModel,
    session: &'a paro_context::StatementContext,
    graph_stats: &'a mut GraphStatsCache,
}

/// Facts needed by local cardinality estimation.  The ordinary post-order
/// path supplies owned child plans; native relation construction supplies
/// immutable BoundReferences.  Keeping this small interface at the estimator
/// boundary lets both paths use the same formulas without manufacturing an
/// OwnedLogicalPlan solely to expose a child's row estimate.
trait LocalChildFacts {
    fn estimated_cardinality(&self) -> Option<CardinalityEstimate>;

    fn unique_keys(&self) -> Vec<Vec<ColumnBinding>> {
        Vec::new()
    }

    fn graph_name(&self) -> Option<&str> {
        None
    }
}

impl LocalChildFacts for Box<OwnedLogicalPlan> {
    fn estimated_cardinality(&self) -> Option<CardinalityEstimate> {
        self.stats.estimated_cardinality
    }

    fn graph_name(&self) -> Option<&str> {
        graph_name_for_plan(self.as_ref())
    }

    fn unique_keys(&self) -> Vec<Vec<ColumnBinding>> {
        crate::statistics::unique_keys::proven_unique_keys(self.as_ref())
    }
}

impl LocalChildFacts for BoundReference {
    fn estimated_cardinality(&self) -> Option<CardinalityEstimate> {
        self.facts.cardinality
    }

    fn unique_keys(&self) -> Vec<Vec<ColumnBinding>> {
        self.facts
            .unique_keys
            .iter()
            .map(|key| key.columns.iter().map(|column| column.binding).collect())
            .collect()
    }
}

impl LogicalPlanPostOrderFolder<GatheredNodeProperties> for StatisticsGatherFolder<'_> {
    fn child_completed(
        &mut self,
        parent_skeleton: &paro_planner::plan::arena::LogicalPlanNode<()>,
        completed_children: &[Box<OwnedLogicalPlan>],
        completed_properties: &[GatheredNodeProperties],
        remaining_children: &[Box<OwnedLogicalPlan>],
    ) -> Result<()> {
        self.gathering.publish_completed_first_child(
            parent_skeleton,
            completed_children,
            completed_properties,
            remaining_children,
            self.context,
        );
        Ok(())
    }

    fn fold(
        &mut self,
        plan: OwnedLogicalPlan,
        child_properties: Vec<GatheredNodeProperties>,
    ) -> Result<(OwnedLogicalPlan, GatheredNodeProperties)> {
        let (child_layouts, child_maximum_cardinalities): (Vec<_>, Vec<_>) = child_properties
            .into_iter()
            .map(|properties| (properties.layout, properties.maximum_cardinality))
            .unzip();
        let (plan, output_layout, maximum_cardinality) = self.gathering.gather_local(
            plan,
            &child_layouts,
            &child_maximum_cardinalities,
            self.context.column_stats.clone(),
            self.context,
        );
        Ok((
            plan,
            GatheredNodeProperties {
                layout: output_layout,
                maximum_cardinality,
            },
        ))
    }
}

impl StatisticsGathering {
    pub fn new() -> Self {
        Self::default()
    }

    /// Derive one operator from completed input facts. This entry point never
    /// visits descendants and is used by incremental arena settlement.
    pub(crate) fn gather_local(
        &mut self,
        mut plan: OwnedLogicalPlan,
        child_layouts: &[LogicalOutputLayout],
        child_maximum_cardinalities: &[Option<u64>],
        input_column_stats: SharedColumnStatistics,
        ctx: &mut OptimizationContext,
    ) -> (OwnedLogicalPlan, LogicalOutputLayout, Option<u64>) {
        if plan.stats.cardinality_provenance != CardinalityProvenance::JoinGraph {
            let mut inputs = CardinalityInputs {
                column_stats: &input_column_stats,
                cost_model: &ctx.cost_model,
                session: &ctx.session,
                graph_stats: &mut ctx.graph_stats,
            };
            plan.stats.estimated_cardinality = self.estimate_plan_cardinality(
                &plan.operator,
                &plan.stats,
                child_layouts,
                &mut inputs,
            );
            plan.stats.cardinality_provenance = CardinalityProvenance::Statistics;
        }
        // The recursive baseline can use the current map as its input view.
        // Release that read before publishing outputs: retaining the extra
        // Arc would detach the entire accumulated map at every node.
        drop(input_column_stats);
        let output = plan.operator.output_layout_from_children(child_layouts);
        let maximum = crate::statistics::cardinality_bound::derive_maximum_cardinality(
            &plan.operator,
            child_maximum_cardinalities,
        );
        plan.stats.unique_keys = crate::statistics::unique_keys::derive_local_unique_keys(
            &plan.operator,
            &output,
            child_layouts,
        );
        self.update_output_column_stats(
            &plan.operator,
            &plan.stats,
            &output,
            child_layouts,
            maximum,
            ctx,
        );
        (plan, output, maximum)
    }

    /// Gather one native operator directly from immutable child references.
    ///
    /// Native transformation producers already have the exact child facts and
    /// layouts that the ordinary arena folder would expose after detaching
    /// and reassembling an OwnedLogicalPlan.  This entry point applies the
    /// same relation-property equations in place, so that transient owned
    /// plans, child adapters, and their second lowering pass never enter the
    /// production lifetime.  It deliberately shares the estimator and output
    /// statistic update below; this is not a second statistics algorithm.
    pub(crate) fn gather_native_local(
        &mut self,
        operator: LogicalOperator<BoundReference>,
        mut stats: NodeStats,
        child_layouts: &[LogicalOutputLayout],
        child_maximum_cardinalities: &[Option<u64>],
        input_column_stats: SharedColumnStatistics,
        ctx: &mut OptimizationContext,
    ) -> (
        NodeStats,
        LogicalOperator<BoundReference>,
        LogicalOutputLayout,
        Option<u64>,
    ) {
        if stats.cardinality_provenance != CardinalityProvenance::JoinGraph {
            let mut inputs = CardinalityInputs {
                column_stats: &input_column_stats,
                cost_model: &ctx.cost_model,
                session: &ctx.session,
                graph_stats: &mut ctx.graph_stats,
            };
            stats.estimated_cardinality =
                self.estimate_plan_cardinality(&operator, &stats, child_layouts, &mut inputs);
            stats.cardinality_provenance = CardinalityProvenance::Statistics;
        }
        // `input_column_stats` is the immutable input view.  The estimator
        // only reads it; output facts are published into the context's
        // copy-on-write map below.
        drop(input_column_stats);
        let output = operator.output_layout_from_children(child_layouts);
        let maximum = crate::statistics::cardinality_bound::derive_maximum_cardinality(
            &operator,
            child_maximum_cardinalities,
        );
        let mut child_keys = Vec::new();
        operator.visit_child_links(&mut |child| {
            child_keys.push(child.facts.unique_keys.as_slice());
        });
        stats.unique_keys = crate::statistics::unique_keys::derive_unique_keys_from_facts(
            &operator,
            &output,
            &child_layouts.iter().collect::<Vec<_>>(),
            &child_keys,
        );
        self.update_output_column_stats(&operator, &stats, &output, child_layouts, maximum, ctx);
        (stats, operator, output, maximum)
    }

    pub(crate) fn bind_cte_domain(
        &mut self,
        index: usize,
        cardinality: Option<CardinalityEstimate>,
        columns: Vec<Arc<ColumnStatistics>>,
    ) {
        if let Some(cardinality) = cardinality {
            self.cte_cardinality.insert(index, cardinality);
        }
        self.cte_output_stats.insert(index, columns);
    }

    pub fn gather(
        &mut self,
        plan: OwnedLogicalPlan,
        ctx: &mut OptimizationContext,
    ) -> Result<OwnedLogicalPlan> {
        let mut folder = StatisticsGatherFolder {
            gathering: self,
            context: ctx,
        };
        plan.try_fold_post_order_with(&mut folder)
            .map(|(plan, _properties)| plan)
    }

    /// Some consumers require producer statistics while their sibling is
    /// still being gathered. Preserve that depth-first publication boundary
    /// explicitly instead of relying on native call-stack sequencing.
    fn publish_completed_first_child(
        &mut self,
        parent_skeleton: &paro_planner::plan::arena::LogicalPlanNode<()>,
        completed_children: &[Box<OwnedLogicalPlan>],
        completed_properties: &[GatheredNodeProperties],
        remaining_children: &[Box<OwnedLogicalPlan>],
        ctx: &OptimizationContext,
    ) {
        if completed_children.len() != 1 {
            return;
        }
        let first = &completed_children[0];
        let Some(first_layout) = completed_properties
            .first()
            .map(|properties| &properties.layout)
        else {
            return;
        };
        match &parent_skeleton.operator {
            LogicalOperator::MaterializedCTE(cte) => {
                self.publish_cte_statistics(cte.cte_index, first, first_layout, ctx);
            }
            LogicalOperator::RecursiveCTE(cte) => {
                self.publish_cte_statistics(cte.cte_index, first, first_layout, ctx);
            }
            LogicalOperator::Join(Join::Comparison(join))
                if !join.duplicate_eliminated_columns.is_empty() =>
            {
                if let Some(right) = remaining_children.first() {
                    self.publish_delim_statistics(
                        first,
                        &join.duplicate_eliminated_columns,
                        right,
                        ctx,
                    );
                }
            }
            _ => {}
        }
    }

    fn publish_cte_statistics(
        &mut self,
        cte_index: usize,
        producer: &OwnedLogicalPlan,
        producer_layout: &LogicalOutputLayout,
        ctx: &OptimizationContext,
    ) {
        if let Some(cardinality) = producer.stats.estimated_cardinality {
            self.cte_cardinality.insert(cte_index, cardinality);
        }
        self.cte_output_stats.insert(
            cte_index,
            collect_output_stats_for_layout(producer_layout, ctx),
        );
    }

    fn publish_delim_statistics(
        &mut self,
        outer: &OwnedLogicalPlan,
        duplicate_eliminated_columns: &[Expression],
        dependent: &OwnedLogicalPlan,
        ctx: &OptimizationContext,
    ) {
        let Some(outer_cardinality) = outer.stats.estimated_cardinality else {
            return;
        };
        let stats = duplicate_eliminated_columns
            .iter()
            .map(|expression| expression_statistics(expression, ctx))
            .collect::<Vec<_>>();
        let mut expected = 1u64;
        let mut all_known = true;
        for stat in &stats {
            let distinct = stat.distinct_evidence().point;
            if distinct == 0 {
                all_known = false;
                break;
            }
            expected =
                saturating_mul_u64(expected, distinct).min(outer_cardinality.expected.max(1));
        }
        if !all_known {
            expected = fallback_group_distinct(outer_cardinality.expected);
        }
        let estimate = CardinalityEstimate {
            min: expected.saturating_div(2),
            expected,
            max: expected
                .saturating_mul(2)
                .min(outer_cardinality.max.max(expected)),
        };
        let indices = collect_delim_indices(dependent);
        for table_index in indices {
            self.delim_cardinality.insert(table_index, estimate);
            self.delim_output_stats.insert(table_index, stats.clone());
        }
    }

    fn estimate_plan_cardinality<Child: LocalChildFacts>(
        &mut self,
        operator: &LogicalOperator<Child>,
        current_stats: &NodeStats,
        child_layouts: &[LogicalOutputLayout],
        ctx: &mut CardinalityInputs<'_>,
    ) -> Option<CardinalityEstimate> {
        match operator {
            // DUMMY_SCAN is the one-row, zero-column identity relation. It is
            // not an unknown table: treating it as unknown inflates lateral
            // argument plans before an external table multiplies by its
            // per-invocation row estimate.
            LogicalOperator::DummyScan => Some(CardinalityEstimate::exact(1)),
            LogicalOperator::BoundReference(reference) => reference.facts.cardinality,
            LogicalOperator::Get(get) => Some(CardinalityEstimate::exact(
                self.get_storage_rows(get, ctx) as u64,
            )),
            LogicalOperator::ExpressionGet(get) => {
                Some(CardinalityEstimate::exact(get.expressions.len() as u64))
            }
            LogicalOperator::DelimGet(delim) => self
                .delim_cardinality
                .get(&delim.table_index)
                .copied()
                .or_else(|| Some(CardinalityEstimate::exact(1))),
            LogicalOperator::TableFunctionGet(_) => Some(CardinalityEstimate::exact(100)),
            LogicalOperator::Projection(proj) => proj.child.estimated_cardinality(),
            LogicalOperator::RowFetch(fetch) => fetch.child.estimated_cardinality(),
            LogicalOperator::ExternalProject(project) => project.child.estimated_cardinality(),
            LogicalOperator::ExternalTable(table) => external_table_cardinality(table),
            LogicalOperator::Order(order) => order.child.estimated_cardinality(),
            LogicalOperator::Window(window) => window.child.estimated_cardinality(),
            LogicalOperator::Distinct(distinct) => {
                self.estimate_distinct_cardinality(&distinct.child, child_layouts.first()?, ctx)
            }
            LogicalOperator::Filter(filter) => {
                self.estimate_filter_cardinality(filter, child_layouts.first()?, ctx)
            }
            LogicalOperator::Limit(limit) => {
                let child = limit.child.estimated_cardinality()?;
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
                let child = topn.child.estimated_cardinality()?;
                Some(apply_limit_estimate(child, Some(topn.limit), topn.offset))
            }
            LogicalOperator::Aggregate(agg) => {
                let child = agg.child.estimated_cardinality()?;
                if agg.groups.is_empty() {
                    // A scalar aggregate emits one row per grouping domain,
                    // including on empty input.  Plain aggregation has one
                    // implicit domain; explicit GROUPING SETS has one domain
                    // for each set (duplicates are semantically observable).
                    return Some(CardinalityEstimate::exact(
                        if agg.grouping_sets.is_empty() {
                            1
                        } else {
                            agg.grouping_sets.len() as u64
                        },
                    ));
                }
                let mut expected = 1u64;
                let mut saw_known = false;
                let mut distinct_upper = Some(1u64);
                for group in &agg.groups {
                    let (distinct, upper) =
                        estimate_group_distinct(group, ctx, child.expected, child.max);
                    saw_known |= upper.is_some();
                    expected = saturating_mul_u64(expected, distinct.max(1)).min(child.expected);
                    distinct_upper = distinct_upper
                        .zip(upper)
                        .map(|(current, upper)| saturating_mul_u64(current, upper).min(child.max));
                }
                if !saw_known {
                    expected = expected.min(child.expected);
                }
                let base_expected = expected.min(child.expected);
                let (non_empty_domains, empty_domains) = if agg.grouping_sets.is_empty() {
                    (1u64, 0u64)
                } else {
                    agg.grouping_sets.iter().fold(
                        (0u64, 0u64),
                        |(non_empty, empty), grouping_set| {
                            if grouping_set.expressions.is_empty() {
                                (non_empty, empty.saturating_add(1))
                            } else {
                                (non_empty.saturating_add(1), empty)
                            }
                        },
                    )
                };
                let expected = base_expected
                    .saturating_mul(non_empty_domains)
                    .saturating_add(empty_domains);
                let max = distinct_upper
                    .unwrap_or(child.max)
                    .saturating_mul(non_empty_domains)
                    .saturating_add(empty_domains)
                    .max(expected);
                let groups = CardinalityEstimate {
                    min: base_expected
                        .saturating_div(2)
                        .min(expected)
                        .saturating_mul(non_empty_domains)
                        .saturating_add(empty_domains)
                        .min(expected),
                    expected,
                    max,
                };
                if let Some(reduction) = &agg.post_reduction {
                    // The post-reduction predicate is an aggregate-owned
                    // HAVING filter. Its hidden scalar has no catalog column
                    // statistics, but the cost model still supplies the same
                    // comparison fallback that an explicit Filter used before
                    // the topology-preserving rewrite. Do not expose the full
                    // pre-predicate group count to later join/order costing.
                    let selectivity = ctx
                        .cost_model
                        .estimate_selectivity(&reduction.predicate, ctx.column_stats);
                    Some(
                        ctx.cost_model
                            .apply_selectivity_to_cardinality(groups, selectivity),
                    )
                } else {
                    Some(groups)
                }
            }
            LogicalOperator::Join(join) => self.estimate_join_cardinality(
                join,
                child_layouts.first()?,
                child_layouts.get(1)?,
                ctx,
            ),
            LogicalOperator::DependentJoin(join) => {
                let left = join.left.estimated_cardinality()?;
                let right = join.right.estimated_cardinality()?;
                Some(product_estimate(left, right))
            }
            LogicalOperator::SetOperation(setop) => {
                let left = setop.left.estimated_cardinality()?;
                let right = setop.right.estimated_cardinality()?;
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
                if let Some(cardinality) = cte.cte_query.estimated_cardinality() {
                    self.cte_cardinality.insert(cte.cte_index, cardinality);
                }
                self.cte_output_stats.insert(
                    cte.cte_index,
                    collect_output_stats_for_layout(child_layouts.first()?, ctx),
                );
                cte.child.estimated_cardinality()
            }
            LogicalOperator::RecursiveCTE(cte) => {
                let anchor = cte.anchor.estimated_cardinality()?;
                let recursive = cte.recursive.estimated_cardinality().unwrap_or(anchor);
                let estimate = CardinalityEstimate {
                    min: anchor.min,
                    expected: anchor.expected.max(recursive.expected),
                    max: anchor.max.saturating_add(recursive.max),
                };
                self.cte_cardinality.insert(cte.cte_index, estimate);
                self.cte_output_stats.insert(
                    cte.cte_index,
                    collect_output_stats_for_layout(child_layouts.first()?, ctx),
                );
                Some(estimate)
            }
            LogicalOperator::CTERef(cte_ref) => {
                // A transformation may optimize an inner shared-plan region
                // independently from an owner in an enclosing Memo group.
                // Alpha-renaming preserves the owner's cardinality summary on
                // the reference; use it only when this estimator instance has
                // no locally published producer. Cardinality is an estimate,
                // never a correctness proof.
                self.cte_cardinality
                    .get(&cte_ref.cte_index)
                    .copied()
                    .or(current_stats.estimated_cardinality)
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
            LogicalOperator::Explain(explain) => explain.child.estimated_cardinality(),
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

    fn estimate_filter_cardinality<Child: LocalChildFacts>(
        &self,
        filter: &Filter<Child>,
        child_layout: &LogicalOutputLayout,
        ctx: &CardinalityInputs<'_>,
    ) -> Option<CardinalityEstimate> {
        let child = filter.child.estimated_cardinality()?;
        Some(ctx.cost_model.estimate_filter_cardinality_with_positions(
            child.expected,
            &filter.expressions,
            ctx.column_stats,
            child_layout.bindings(),
        ))
    }

    fn estimate_distinct_cardinality<Child: LocalChildFacts>(
        &self,
        child: &Child,
        child_layout: &LogicalOutputLayout,
        ctx: &CardinalityInputs<'_>,
    ) -> Option<CardinalityEstimate> {
        let child_est = child.estimated_cardinality()?;
        let stats = collect_output_stats_for_layout(child_layout, ctx);
        let mut expected = 1u64;
        let mut saw_distinct = false;
        for stat in stats {
            let distinct = stat.distinct_evidence().point;
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

    fn estimate_join_cardinality<Child: LocalChildFacts>(
        &self,
        join: &Join<Child>,
        left_layout: &LogicalOutputLayout,
        right_layout: &LogicalOutputLayout,
        ctx: &CardinalityInputs<'_>,
    ) -> Option<CardinalityEstimate> {
        match join {
            Join::Cross(cross) => Some(product_estimate(
                cross.left.estimated_cardinality()?,
                cross.right.estimated_cardinality()?,
            )),
            Join::Any(any) => {
                let left = any.left.estimated_cardinality()?;
                let right = any.right.estimated_cardinality()?;
                let selectivity = ctx
                    .cost_model
                    .estimate_selectivity(&any.condition, ctx.column_stats);
                Some(adjust_join_estimate(
                    apply_selectivity(product_estimate(left, right), selectivity),
                    left,
                    right,
                    any.join_type,
                ))
            }
            Join::Comparison(cmp) => {
                let left = cmp.left.estimated_cardinality()?;
                let right = cmp.right.estimated_cardinality()?;
                if let Some(estimate) = estimate_same_domain_semi_join(
                    cmp,
                    left,
                    right,
                    left_layout.bindings(),
                    right_layout.bindings(),
                    ctx,
                ) {
                    return Some(estimate);
                }
                if let Some(inner) =
                    estimate_unique_dimension_join(cmp, left, right, left_layout, right_layout, ctx)
                {
                    return Some(adjust_join_estimate(inner, left, right, cmp.join_type));
                }
                let selectivity = estimate_comparison_join_selectivity(
                    &cmp.conditions,
                    left_layout.bindings(),
                    right_layout.bindings(),
                    left.expected,
                    right.expected,
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
        ctx: &CardinalityInputs<'_>,
    ) -> CardinalityEstimate {
        let base_rows = self.get_storage_rows(&search.get, ctx) as u64;
        let mut expressions = Vec::new();
        expressions.extend(search.absorbed_predicates.iter().cloned());
        expressions.extend(search.residual_predicates.iter().cloned());
        expressions.extend(search.get.runtime_filter_expressions.iter().cloned());
        let filtered =
            ctx.cost_model
                .estimate_filter_cardinality(base_rows, &expressions, ctx.column_stats);
        apply_limit_estimate(filtered, Some(search.limit), 0)
    }

    fn estimate_fulltext_filter_cardinality(
        &self,
        scan: &FullTextFilterScan,
        ctx: &CardinalityInputs<'_>,
    ) -> CardinalityEstimate {
        let base_rows = self.get_storage_rows(&scan.get, ctx) as u64;
        let mut expressions = vec![scan.match_expression.clone()];
        expressions.extend(scan.other_predicates.iter().cloned());
        expressions.extend(scan.residual_predicates.iter().cloned());
        expressions.extend(scan.get.runtime_filter_expressions.iter().cloned());
        ctx.cost_model
            .estimate_filter_cardinality(base_rows, &expressions, ctx.column_stats)
    }

    fn estimate_graph_scan_cardinality(
        &self,
        scan: &GraphScan,
        ctx: &mut CardinalityInputs<'_>,
    ) -> CardinalityEstimate {
        let base = ctx
            .graph_stats
            .get(&scan.graph_name)
            .and_then(|stats| stats.vertex_count(&scan.label))
            .unwrap_or(if scan.filter.is_some() { 100 } else { 1000 });
        let expected = if let Some(filter) = &scan.filter {
            let selectivity = ctx
                .cost_model
                .estimate_selectivity(filter, ctx.column_stats);
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

    fn estimate_graph_expand_cardinality<Child: LocalChildFacts>(
        &self,
        expand: &GraphExpand<Child>,
        ctx: &mut CardinalityInputs<'_>,
    ) -> CardinalityEstimate {
        let child = expand
            .child
            .estimated_cardinality()
            .unwrap_or(CardinalityEstimate::exact(1000));
        let stats = expand
            .child
            .graph_name()
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

    fn update_output_column_stats<Child: LocalChildFacts>(
        &mut self,
        operator: &LogicalOperator<Child>,
        node_stats: &NodeStats,
        output_layout: &LogicalOutputLayout,
        child_layouts: &[LogicalOutputLayout],
        guaranteed_output_rows: Option<u64>,
        ctx: &mut OptimizationContext,
    ) {
        let output_stats = match operator {
            LogicalOperator::Get(get) => self.get_output_stats(get, ctx),
            LogicalOperator::BoundReference(reference) => reference.column_statistics(),
            LogicalOperator::Projection(proj) => proj
                .expressions
                .iter()
                .map(|expr| expression_statistics(expr, ctx))
                .collect(),
            LogicalOperator::Filter(filter) => filter_output_stats(
                filter,
                child_layouts
                    .first()
                    .expect("statistics fold completed filter child layout"),
                ctx,
            ),
            LogicalOperator::ExternalProject(project) => {
                let mut stats = collect_output_stats_for_layout(
                    child_layouts
                        .first()
                        .expect("statistics fold completed external-project child layout"),
                    ctx,
                );
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
                stats.extend(agg.aggregates.iter().map(|expr| {
                    let output = aggregate_expression_statistics(expr, ctx, guaranteed_output_rows);
                    let distribution = if agg.post_reduction.is_none()
                        && agg.grouping_sets.is_empty()
                        && !agg.groups.is_empty()
                    {
                        agg.child
                            .estimated_cardinality()
                            .zip(node_stats.estimated_cardinality)
                            .zip(child_layouts.first())
                            .and_then(|((input, groups), layout)| {
                                estimate_grouped_sum_distribution(
                                    expr,
                                    input.expected,
                                    groups.expected,
                                    layout.bindings(),
                                    &ctx.column_stats,
                                )
                            })
                    } else {
                        None
                    };
                    if distribution.is_some() {
                        Arc::new(
                            output
                                .as_ref()
                                .copy()
                                .with_estimated_numeric_distribution(distribution),
                        )
                    } else {
                        output
                    }
                }));
                stats.extend(
                    agg.grouping_functions
                        .iter()
                        .map(|_| ColumnStatistics::create_unknown(LogicalType::BigInt)),
                );
                stats
            }
            LogicalOperator::SetOperation(setop) => merge_setop_output_stats(
                child_layouts
                    .first()
                    .expect("statistics fold completed set-operation left layout"),
                child_layouts
                    .get(1)
                    .expect("statistics fold completed set-operation right layout"),
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
            LogicalOperator::DelimGet(delim) => self
                .delim_output_stats
                .get(&delim.table_index)
                .cloned()
                .unwrap_or_else(|| unknown_stats_for_types(&delim.chunk_types)),
            LogicalOperator::SearchScan(search) => search
                .projections
                .iter()
                .map(|expr| expression_statistics(expr, ctx))
                .collect(),
            LogicalOperator::FullTextFilterScan(scan) => project_column_statistics(
                self.get_output_stats(&scan.get, ctx),
                &scan.projection_map,
            ),
            _ => collect_output_stats_for_layout(output_layout, ctx),
        };

        // A conditional or multiplicity-changing boundary cannot retain an
        // unconditional value distribution without an explicit conditioning
        // model. Value bounds and NDV remain separate and are not erased.
        let preserves_distribution = matches!(
            operator,
            LogicalOperator::Get(_)
                | LogicalOperator::BoundReference(_)
                | LogicalOperator::Projection(_)
                | LogicalOperator::RowFetch(_)
                | LogicalOperator::ExternalProject(_)
                | LogicalOperator::Order(_)
                | LogicalOperator::Aggregate(_)
                | LogicalOperator::SetOperation(_)
                | LogicalOperator::MaterializedCTE(_)
                | LogicalOperator::CTERef(_)
                | LogicalOperator::DelimGet(_)
        );
        for (binding, mut stats) in output_layout.bindings().iter().copied().zip(output_stats) {
            if !preserves_distribution && stats.estimated_numeric_distribution().is_some() {
                stats = Arc::new(
                    stats
                        .as_ref()
                        .copy()
                        .with_estimated_numeric_distribution(None),
                );
            }
            ctx.column_stats_mut().insert(binding, stats);
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
        get.returned_types
            .iter()
            .enumerate()
            .map(|(idx, return_type)| {
                get.stored_column(idx)
                    .and_then(|column_id| storage.column_statistics(column_id))
                    .map(Arc::new)
                    .unwrap_or_else(|| ColumnStatistics::create_unknown(return_type.clone()))
            })
            .collect()
    }

    fn get_storage_rows(&self, get: &Get, ctx: &CardinalityInputs<'_>) -> usize {
        get.table
            .as_ref()
            .and_then(|table| table.get_storage())
            .and_then(|storage| storage.total_rows().ok())
            .map(|rows| rows.max(1))
            .unwrap_or_else(|| default_table_cardinality(Some(ctx.session)))
    }
}

/// Estimate a semi join between two filtered views of the same statistical
/// key domain without charging build-side duplicates as new matches.
///
/// Base column statistics intentionally survive relational filters, so two
/// alpha-renamed scans of the same key retain the same NDV and range. For a
/// semi join that is exactly the useful signal: rows on the demand side are a
/// sample of the preserved key distribution, and repeated demand keys do not
/// multiply the output. The estimate remains deliberately uncertain; this is
/// a costing interval, never a correctness bound.
fn estimate_same_domain_semi_join<Child>(
    join: &paro_planner::operator::ComparisonJoin<Child>,
    left: CardinalityEstimate,
    right: CardinalityEstimate,
    left_bindings: &[ColumnBinding],
    right_bindings: &[ColumnBinding],
    ctx: &CardinalityInputs<'_>,
) -> Option<CardinalityEstimate> {
    let (preserved, demand, preserved_bindings, demand_bindings) = match join.join_type {
        JoinType::Semi => (left, right, left_bindings, right_bindings),
        JoinType::RightSemi => (right, left, right_bindings, left_bindings),
        _ => return None,
    };
    let [condition] = join.conditions.as_slice() else {
        return None;
    };
    if condition.comparison != JoinComparisonType::Equal {
        return None;
    }
    let preserved_key = expression_binding(&condition.left, preserved_bindings)
        .or_else(|| expression_binding(&condition.right, preserved_bindings))?;
    let demand_key = expression_binding(&condition.right, demand_bindings)
        .or_else(|| expression_binding(&condition.left, demand_bindings))?;
    let preserved_stats = ctx.column_stats.get(&preserved_key)?;
    let demand_stats = ctx.column_stats.get(&demand_key)?;
    let preserved_distinct = preserved_stats.distinct_evidence().point;
    if preserved_distinct == 0
        || preserved_distinct != demand_stats.distinct_evidence().point
        || preserved_stats.get_type() != demand_stats.get_type()
        || preserved_stats.statistics().min_value() != demand_stats.statistics().min_value()
        || preserved_stats.statistics().max_value() != demand_stats.statistics().max_value()
    {
        return None;
    }

    let expected = preserved.expected.min(demand.expected);
    Some(CardinalityEstimate {
        min: 0,
        expected,
        // `demand.max` already carries the estimator's uncertainty envelope.
        // Applying another arbitrary factor here double-counts uncertainty
        // and makes a duplicate-insensitive semi join look riskier than the
        // unfiltered relation it replaces.
        max: preserved.max.min(demand.max).max(expected),
    })
}

/// Estimate an equality lookup into a declared unique relation against the
/// key domain actually present on the fact side.
///
/// A filtered date/customer dimension retains base-column NDV statistics even
/// though its row count is selective. Dividing by that historical dimension
/// NDV can underestimate a foreign-key-shaped join by orders of magnitude.
/// Uniqueness proves at most one match per fact row; the expected match ratio
/// is therefore `selected_dimension_rows / fact_key_domain`, capped at one.
fn estimate_unique_dimension_join<Child: LocalChildFacts>(
    join: &paro_planner::operator::ComparisonJoin<Child>,
    left: CardinalityEstimate,
    right: CardinalityEstimate,
    left_layout: &LogicalOutputLayout,
    right_layout: &LogicalOutputLayout,
    ctx: &CardinalityInputs<'_>,
) -> Option<CardinalityEstimate> {
    let [condition] = join.conditions.as_slice() else {
        return None;
    };
    if condition.comparison != JoinComparisonType::Equal {
        return None;
    }
    let left_key = expression_binding(&condition.left, left_layout.bindings())?;
    let right_key = expression_binding(&condition.right, right_layout.bindings())?;

    if plan_has_single_column_unique_key(&join.right, right_key) {
        return unique_lookup_estimate(left, right, left_key, ctx);
    }
    if plan_has_single_column_unique_key(&join.left, left_key) {
        return unique_lookup_estimate(right, left, right_key, ctx);
    }
    None
}

fn plan_has_single_column_unique_key<Child: LocalChildFacts>(
    child: &Child,
    binding: ColumnBinding,
) -> bool {
    child
        .unique_keys()
        .iter()
        .any(|key| key.len() == 1 && key[0] == binding)
}

fn unique_lookup_estimate(
    fact: CardinalityEstimate,
    dimension: CardinalityEstimate,
    fact_key: ColumnBinding,
    ctx: &CardinalityInputs<'_>,
) -> Option<CardinalityEstimate> {
    let domain = ctx.column_stats.get(&fact_key)?.distinct_evidence().point;
    if domain == 0 {
        return None;
    }
    let scale = |rows: u64, selected: u64| {
        ((rows as u128).saturating_mul(selected as u128) / domain as u128).min(rows as u128) as u64
    };
    let expected = scale(fact.expected, dimension.expected);
    Some(CardinalityEstimate {
        min: scale(fact.min, dimension.min).min(expected),
        expected,
        max: scale(fact.max, dimension.max).max(expected),
    })
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
    left_rows: u64,
    right_rows: u64,
    ctx: &CardinalityInputs<'_>,
) -> f64 {
    correlate_join_condition_selectivities(conditions.iter().map(|condition| {
        (
            equality_relation_pair(condition, left_bindings, right_bindings),
            estimate_join_condition_selectivity(
                condition,
                left_bindings,
                right_bindings,
                left_rows,
                right_rows,
                ctx,
            ),
        )
    }))
}

fn correlate_join_condition_selectivities(
    conditions: impl IntoIterator<Item = (Option<(usize, usize)>, f64)>,
) -> f64 {
    // This map participates in a floating-point reduction. Iterating a HashMap
    // would make the estimate (and potentially the winning physical plan)
    // depend on the process hash seed.
    let mut equality_by_relation_pair = BTreeMap::<(usize, usize), f64>::new();
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

fn collect_output_stats_for_layout(
    layout: &LogicalOutputLayout,
    ctx: &impl ColumnStatsView,
) -> Vec<Arc<ColumnStatistics>> {
    layout
        .types()
        .iter()
        .cloned()
        .zip(layout.bindings().iter().copied())
        .map(|(ty, binding)| {
            ctx.get_stat(&binding)
                .unwrap_or_else(|| ColumnStatistics::create_unknown(ty))
        })
        .collect()
}

fn filter_output_stats<Child>(
    filter: &Filter<Child>,
    child_layout: &LogicalOutputLayout,
    ctx: &impl ColumnStatsView,
) -> Vec<Arc<ColumnStatistics>> {
    let mut child_output = collect_output_stats_for_layout(child_layout, ctx);

    fn refine(
        expression: &Expression,
        bindings: &[ColumnBinding],
        output: &mut [Arc<ColumnStatistics>],
    ) {
        if let Expression::Conjunction(conjunction) = expression {
            if conjunction.conjunction_type == ConjunctionType::And {
                for child in &conjunction.children {
                    refine(child, bindings, output);
                }
                return;
            }
        }

        let Some((binding, values)) = finite_equality_domain(expression) else {
            return;
        };
        let Some(index) = bindings.iter().position(|candidate| *candidate == binding) else {
            return;
        };
        let Some((first, rest)) = values.split_first() else {
            return;
        };
        let mut domain = BaseStatistics::from_constant(first);
        for value in rest {
            domain.merge(&BaseStatistics::from_constant(value));
        }
        let Some(statistics) = output.get_mut(index) else {
            return;
        };
        *statistics = Arc::new(
            ColumnStatistics::with_estimated_distinct(domain, Some(values.len()))
                .with_guaranteed_distinct_upper(values.len() as u64),
        );
    }

    for expression in &filter.expressions {
        refine(expression, child_layout.bindings(), &mut child_output);
    }

    filter
        .projection_map
        .to_indices(child_layout.len())
        .into_iter()
        .filter_map(|child_index| {
            child_layout
                .types()
                .get(child_index)
                .cloned()
                .map(|output_type| {
                    child_output
                        .get(child_index)
                        .cloned()
                        .unwrap_or_else(|| ColumnStatistics::create_unknown(output_type))
                })
        })
        .collect()
}

/// Extract a finite value domain proven by an equality predicate.
///
/// OR is accepted only when every branch constrains the same column. The
/// resulting bound follows from the predicate itself and remains valid after
/// DML, unlike a min/max range observed in one table snapshot.
fn finite_equality_domain(expression: &Expression) -> Option<(ColumnBinding, Vec<Value>)> {
    match expression {
        Expression::Comparison(comparison)
            if matches!(
                comparison.comparison_type,
                ComparisonType::Equal | ComparisonType::NotDistinctFrom
            ) =>
        {
            let (column, constant) = match (comparison.left.as_ref(), comparison.right.as_ref()) {
                (Expression::ColumnRef(column), Expression::Constant(constant))
                | (Expression::Constant(constant), Expression::ColumnRef(column))
                    if column.depth == 0 && !constant.value.is_null() =>
                {
                    (column, constant)
                }
                _ => return None,
            };
            Some((column.binding, vec![constant.value.clone()]))
        }
        Expression::Conjunction(conjunction)
            if conjunction.conjunction_type == ConjunctionType::Or
                && !conjunction.children.is_empty() =>
        {
            let mut binding = None;
            let mut values = Vec::new();
            for child in &conjunction.children {
                let (child_binding, child_values) = finite_equality_domain(child)?;
                if binding.is_some_and(|binding| binding != child_binding) {
                    return None;
                }
                binding = Some(child_binding);
                for value in child_values {
                    if !values.contains(&value) {
                        values.push(value);
                    }
                }
            }
            Some((binding?, values))
        }
        _ => None,
    }
}

fn merge_setop_output_stats(
    left_layout: &LogicalOutputLayout,
    right_layout: &LogicalOutputLayout,
    types: &[LogicalType],
    ctx: &impl ColumnStatsView,
) -> Vec<Arc<ColumnStatistics>> {
    types
        .iter()
        .enumerate()
        .map(|(idx, ty)| {
            let left_stats = left_layout
                .bindings()
                .get(idx)
                .and_then(|binding| ctx.get_stat(binding));
            let right_stats = right_layout
                .bindings()
                .get(idx)
                .and_then(|binding| ctx.get_stat(binding));
            merge_column_statistics(left_stats, right_stats, ty.clone())
        })
        .collect()
}

fn project_column_statistics(
    statistics: Vec<Arc<ColumnStatistics>>,
    projection: &paro_planner::operator::ProjectionMap,
) -> Vec<Arc<ColumnStatistics>> {
    projection
        .to_indices(statistics.len())
        .into_iter()
        .filter_map(|index| statistics.get(index).cloned())
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
    guaranteed_output_rows: Option<u64>,
) -> Arc<ColumnStatistics> {
    let Expression::Aggregate(agg) = expr else {
        return expression_statistics(expr, ctx);
    };

    match agg.function.name.to_ascii_lowercase().as_str() {
        "count" | "count_star" => Arc::new(ColumnStatistics::new(BaseStatistics::new(
            LogicalType::BigInt,
        ))),
        _ if agg.function.preserves_input_domain() && agg.children.len() == 1 => {
            // These aggregates can only publish a value drawn from their
            // input domain. Cap its NDV where the result is produced so every
            // downstream consumer observes self-consistent column statistics.
            let mut statistics = expression_statistics(&agg.children[0], ctx).as_ref().copy();
            if let Some(output_rows) = guaranteed_output_rows {
                statistics = statistics.with_guaranteed_distinct_upper(output_rows);
            }
            Arc::new(statistics)
        }
        _ => ColumnStatistics::create_unknown(agg.return_type.clone()),
    }
}

fn expression_statistics(expr: &Expression, ctx: &impl ColumnStatsView) -> Arc<ColumnStatistics> {
    match expr {
        Expression::ColumnRef(col_ref) => ctx
            .get_stat(&col_ref.binding)
            .unwrap_or_else(|| ColumnStatistics::create_unknown(col_ref.return_type.clone())),
        Expression::Constant(constant) => Arc::new(
            ColumnStatistics::new(BaseStatistics::from_constant(&constant.value))
                .with_guaranteed_distinct_upper(1),
        ),
        Expression::Cast(cast) => ColumnStatistics::create_unknown(cast.target_type.clone()),
        Expression::Reference(reference) => {
            ColumnStatistics::create_unknown(reference.return_type.clone())
        }
        _ => ColumnStatistics::create_unknown(expr.return_type()),
    }
}

fn estimate_group_distinct(
    expr: &Expression,
    ctx: &impl ColumnStatsView,
    child_expected_rows: u64,
    child_max_rows: u64,
) -> (u64, Option<u64>) {
    match expr {
        Expression::ColumnRef(col_ref) => {
            let statistics = ctx.get_stat(&col_ref.binding);
            let guaranteed_upper = statistics
                .as_ref()
                .and_then(|stats| stats.guaranteed_distinct_upper())
                .map(|upper| upper.min(child_max_rows));
            let distinct = statistics
                .as_ref()
                .map(|stats| stats.distinct_evidence().point)
                .filter(|count| *count > 0);
            match (distinct, guaranteed_upper) {
                (Some(distinct), Some(upper)) => (distinct.min(upper), Some(upper)),
                (None, Some(upper)) => (upper.min(child_expected_rows), Some(upper)),
                // HLL is an estimate rather than a semantic bound. A 2x
                // envelope remains conservative for planning while avoiding
                // the useless input-cardinality upper bound that made a
                // proven preaggregation look riskier than its unreduced join.
                (Some(distinct), None) => (
                    distinct,
                    Some(distinct.saturating_mul(2).min(child_max_rows).max(distinct)),
                ),
                (None, None) => (fallback_group_distinct(child_expected_rows), None),
            }
        }
        Expression::Constant(_) => (1, Some(1)),
        _ => (fallback_group_distinct(child_expected_rows), None),
    }
}

fn fallback_group_distinct(child_rows: u64) -> u64 {
    ((child_rows.max(1) as f64).sqrt().ceil() as u64).max(1)
}

pub(crate) fn default_table_cardinality(session: Option<&paro_context::StatementContext>) -> usize {
    match session.and_then(|session| session.get_setting("default_table_cardinality")) {
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
        JoinType::Semi => CardinalityEstimate {
            min: 0,
            expected: inner.expected.min(left.expected),
            max: left.max,
        },
        // MARK and SINGLE joins append one derived value to every preserved
        // row.  Whether the right side finds zero or one match changes that
        // value, never the number of output rows.  Treating them like SEMI
        // joins collapses dependent-subquery cardinalities to the (often tiny)
        // correlated aggregate branch and poisons every cost decision above
        // the control region.
        JoinType::Mark | JoinType::Single => left,
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
    left_rows: u64,
    right_rows: u64,
    ctx: &CardinalityInputs<'_>,
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
                    .map(|stats| stats.distinct_evidence().point)
                    .unwrap_or(0);
                let right_distinct = ctx
                    .column_stats
                    .get(&right)
                    .map(|stats| stats.distinct_evidence().point)
                    .unwrap_or(0);
                if left_distinct > 0 && right_distinct > 0 {
                    // A filtered relation cannot expose more distinct values
                    // than rows. Column statistics retain their base-table
                    // NDV through predicates on correlated columns (for
                    // example `d_year` filtering `d_date_sk`), so cap each
                    // marginal by the cardinality of the side that owns it.
                    let side_rows = |binding: ColumnBinding| {
                        if left_bindings.contains(&binding) {
                            left_rows
                        } else if right_bindings.contains(&binding) {
                            right_rows
                        } else {
                            u64::MAX
                        }
                    };
                    let left_domain = left_distinct.min(side_rows(left));
                    let right_domain = right_distinct.min(side_rows(right));
                    return (1.0 / left_domain.max(right_domain).max(1) as f64).clamp(0.0, 1.0);
                }
            }
        }
        _ => {}
    }

    let expr = Expression::Comparison(
        ComparisonExpression::new(
            join_comparison_to_comparison(condition.comparison),
            condition.left.clone(),
            condition.right.clone(),
        )
        .into(),
    );
    ctx.cost_model.estimate_selectivity(&expr, ctx.column_stats)
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
    let Expression::Constant(constant) = expr else {
        return None;
    };
    match &constant.value {
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

fn collect_delim_indices(plan: &OwnedLogicalPlan) -> BTreeSet<usize> {
    let mut indices = BTreeSet::new();
    plan.try_visit_pre_order(|plan| {
        if let LogicalOperator::DelimGet(delim) = &plan.operator {
            indices.insert(delim.table_index);
        }
        Ok(())
    })
    .expect("delimiter-index collection has no fallible operation");
    indices
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

fn graph_name_for_plan(mut plan: &OwnedLogicalPlan) -> Option<&str> {
    loop {
        plan = match &plan.operator {
            LogicalOperator::GraphScan(scan) => return Some(scan.graph_name.as_str()),
            LogicalOperator::GraphExpand(expand) => expand.child.as_ref(),
            LogicalOperator::Filter(filter) => filter.child.as_ref(),
            LogicalOperator::EmptyResult(empty) => empty.child.as_ref(),
            _ => return None,
        };
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

impl ColumnStatsView for CardinalityInputs<'_> {
    fn get_stat(&self, binding: &ColumnBinding) -> Option<Arc<ColumnStatistics>> {
        self.column_stats.get(binding).cloned()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_catalog::entry::{EdgeTableInfo, VertexTableInfo};
    use paro_common::runtime_value::Value;
    use paro_context::test_support::TestStatementContextBuilder;
    use paro_context::StatementContext;
    use paro_planner::binder::context::BindContext;
    use paro_planner::binder::ir::{CTEMaterialize, GroupingSet};
    use paro_planner::expression::{ColumnRefExpression, ComparisonExpression, ConstantExpression};
    use paro_planner::operator::graph_expand::ExpandDirection;
    use paro_planner::operator::{
        Aggregate, CTERef, DelimGet, ExpressionGet, Filter, GraphExpand, GraphScan, Limit,
        MaterializedCTE, Projection,
    };
    use paro_storage::index::graph::GraphStatistics;

    use super::*;
    use crate::context::{GraphStatsCache, GraphStatsLoader, OptimizationContext};

    fn make_test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn column_ref(table_index: usize, column_index: usize) -> Expression {
        Expression::ColumnRef(
            ColumnRefExpression {
                binding: ColumnBinding::new(table_index, column_index),
                depth: 0,
                return_type: LogicalType::BigInt,
            }
            .into(),
        )
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

    /// Independent two-gather baseline: compare every relation property and
    /// serialized column evidence, including producer-before-consumer state.
    fn assert_query_settlement(plan: &OwnedLogicalPlan, context: &OptimizationContext) {
        use crate::statistics::propagator::StatisticsPropagator;
        use paro_planner::binder::deep_copy::duplicate_plan_preserving_indices;
        let duplicate =
            || duplicate_plan_preserving_indices(plan, context.bind_context.shared().as_ref());
        let mut old_context = context.fork_for_candidate(Arc::new(HashMap::new()));
        let mut new_context = context.fork_for_candidate(Arc::new(HashMap::new()));
        let old = StatisticsGathering::new()
            .gather(duplicate(), &mut old_context)
            .unwrap();
        let mut propagation = StatisticsPropagator::new();
        let old = propagation.propagate(old_context.session.clone(), old);
        old_context.column_stats = Arc::new(propagation.take_statistics_map());
        let old = StatisticsGathering::new()
            .gather(old, &mut old_context)
            .unwrap();
        let new =
            crate::statistics::settle_query_properties(duplicate(), &mut new_context).unwrap();
        let mut pending = vec![(&old, &new)];
        while let Some((old, new)) = pending.pop() {
            assert_eq!(
                std::mem::discriminant(&old.operator),
                std::mem::discriminant(&new.operator)
            );
            assert_eq!(old.stats, new.stats);
            assert_eq!(old.output_layout(), new.output_layout());
            let old_children = old.children();
            let new_children = new.children();
            assert_eq!(old_children.len(), new_children.len());
            pending.extend(old_children.into_iter().zip(new_children));
        }
        let columns = |context: &OptimizationContext| {
            context
                .column_stats
                .iter()
                .map(|(binding, column)| (*binding, column.to_bytes().unwrap()))
                .collect::<BTreeMap<_, _>>()
        };
        assert_eq!(columns(&old_context), columns(&new_context));
    }

    fn values_relation(
        bind_context: &BindContext,
        table_index: usize,
        rows: usize,
    ) -> OwnedLogicalPlan {
        OwnedLogicalPlan::new(
            bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                table_index,
                vec![
                    vec![Expression::Constant(
                        ConstantExpression::new(Value::BigInt(1), LogicalType::BigInt,).into()
                    )];
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

        let child = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                1,
                vec![
                    vec![Expression::Constant(
                        ConstantExpression::new(Value::BigInt(1), LogicalType::BigInt,).into()
                    )];
                    10
                ],
                vec!["v".to_string()],
                vec![LogicalType::BigInt],
            )),
        );
        let projection = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::Projection(Projection::new(
                2,
                child,
                vec![Expression::ColumnRef(
                    ColumnRefExpression {
                        binding: ColumnBinding::new(1, 0),
                        depth: 0,
                        return_type: LogicalType::BigInt,
                    }
                    .into(),
                )],
            )),
        );
        let plan = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::Limit(Box::new(Limit::new(
                projection,
                Some(Expression::Constant(
                    ConstantExpression::new(Value::BigInt(3), LogicalType::BigInt).into(),
                )),
                None,
            ))),
        );

        assert_query_settlement(&plan, &ctx);
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
    fn statistics_gathering_uses_a_heap_stack_for_deep_plans() {
        std::thread::Builder::new()
            .name("deep-statistics-gathering".to_string())
            .stack_size(512 * 1024)
            .spawn(|| {
                const DEPTH: usize = 10_000;
                let bind_context = BindContext::new();
                let session = make_test_session();
                let mut ctx = OptimizationContext::new(session, bind_context.clone());
                let mut plan = OwnedLogicalPlan::new(
                    &bind_context,
                    LogicalOperator::ExpressionGet(ExpressionGet::new(
                        17,
                        vec![vec![Expression::Constant(
                            ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into(),
                        )]],
                        vec!["v".to_string()],
                        vec![LogicalType::Integer],
                    )),
                );
                for _ in 0..DEPTH {
                    plan = OwnedLogicalPlan::new(
                        &bind_context,
                        LogicalOperator::Limit(Box::new(Limit::new(plan, None, None))),
                    );
                }

                let plan = StatisticsGathering::new()
                    .gather(plan, &mut ctx)
                    .expect("deep gathering should not consume the native stack");
                assert!(ctx.column_stats.contains_key(&ColumnBinding::new(17, 0)));
                let mut current = &plan;
                for _ in 0..DEPTH {
                    let LogicalOperator::Limit(limit) = &current.operator else {
                        panic!("expected the deep limit chain to remain intact");
                    };
                    current = &limit.child;
                }
                assert!(matches!(
                    &current.operator,
                    LogicalOperator::ExpressionGet(_)
                ));
            })
            .expect("deep statistics thread should start")
            .join()
            .expect("deep statistics gathering should complete");
    }

    #[test]
    fn delimiter_collection_uses_a_heap_stack_for_a_deep_dependent_rhs() {
        std::thread::Builder::new()
            .name("deep-delimiter-collection".to_string())
            .stack_size(512 * 1024)
            .spawn(|| {
                const DEPTH: usize = 10_000;
                let bind_context = BindContext::new();
                let session = make_test_session();
                let mut ctx = OptimizationContext::new(session, bind_context.clone());
                let outer = values_relation(&bind_context, 1, 35);
                let mut dependent = OwnedLogicalPlan::new(
                    &bind_context,
                    LogicalOperator::DelimGet(DelimGet::new(9, vec![LogicalType::BigInt])),
                );
                for _ in 0..DEPTH {
                    dependent = OwnedLogicalPlan::new(
                        &bind_context,
                        LogicalOperator::Limit(Box::new(Limit::new(dependent, None, None))),
                    );
                }
                let mut join = paro_planner::operator::ComparisonJoin::new(
                    JoinType::Inner,
                    outer,
                    dependent,
                    vec![equality(1, 0, 9, 0)],
                );
                join.duplicate_eliminated_columns = vec![column_ref(1, 0)];
                let plan = OwnedLogicalPlan::new(
                    &bind_context,
                    LogicalOperator::Join(Join::Comparison(join)),
                );

                let gathered = StatisticsGathering::new()
                    .gather(plan, &mut ctx)
                    .expect("deep delimiter gathering should succeed");
                let LogicalOperator::Join(Join::Comparison(join)) = &gathered.operator else {
                    panic!("expected delimiter join root");
                };
                let mut dependent = join.right.as_ref();
                for _ in 0..DEPTH {
                    let LogicalOperator::Limit(limit) = &dependent.operator else {
                        panic!("expected deep dependent RHS to retain its limit chain");
                    };
                    dependent = &limit.child;
                }
                assert!(matches!(&dependent.operator, LogicalOperator::DelimGet(_)));
                let estimate = dependent
                    .stats
                    .estimated_cardinality
                    .expect("deep DelimGet should inherit the outer key domain");
                assert!(estimate.expected > 1 && estimate.expected < 35);
                assert!(ctx.column_stats.contains_key(&ColumnBinding::new(9, 0)));
            })
            .expect("deep delimiter thread should start")
            .join()
            .expect("deep delimiter collection should complete");
    }

    #[test]
    fn graph_name_lookup_uses_a_heap_stack_for_a_deep_graph_chain() {
        std::thread::Builder::new()
            .name("deep-graph-name-lookup".to_string())
            .stack_size(512 * 1024)
            .spawn(|| {
                const DEPTH: usize = 10_000;

                struct StaticGraphStatsLoader;

                impl GraphStatsLoader for StaticGraphStatsLoader {
                    fn load(&self, graph_name: &str) -> Option<Arc<GraphStatistics>> {
                        (graph_name == "g").then(|| {
                            Arc::new(
                                GraphStatistics::default()
                                    .with_vertex_count("v", 10)
                                    .with_pattern_step_count("v", "e", "v", 10),
                            )
                        })
                    }
                }

                let bind_context = BindContext::new();
                let mut ctx = OptimizationContext::new(make_test_session(), bind_context.clone());
                ctx.graph_stats = GraphStatsCache::with_loader(Arc::new(StaticGraphStatsLoader));
                let mut child = OwnedLogicalPlan::new(
                    &bind_context,
                    LogicalOperator::GraphScan(Box::new(GraphScan::new(
                        VertexTableInfo {
                            table_name: "vertices".to_string(),
                            table_oid: 1,
                            key_column_ids: vec![0],
                            label: "v".to_string(),
                            property_column_ids: Vec::new(),
                        },
                        None,
                        0,
                        3,
                        "v".to_string(),
                        "g".to_string(),
                        "public".to_string(),
                    ))),
                );
                for _ in 0..DEPTH {
                    child = OwnedLogicalPlan::new(
                        &bind_context,
                        LogicalOperator::Filter(Filter::new(child, Vec::new())),
                    );
                }
                let plan = OwnedLogicalPlan::new(
                    &bind_context,
                    LogicalOperator::GraphExpand(Box::new(GraphExpand::new(
                        EdgeTableInfo {
                            table_name: "edges".to_string(),
                            table_oid: 2,
                            key_column_ids: vec![0],
                            source_key_column_ids: vec![0],
                            source_vertex_table: "vertices".to_string(),
                            source_ref_column_ids: vec![0],
                            destination_key_column_ids: vec![0],
                            destination_vertex_table: "vertices".to_string(),
                            destination_ref_column_ids: vec![0],
                            label: "e".to_string(),
                            property_column_ids: Vec::new(),
                        },
                        ExpandDirection::Forward,
                        "v".to_string(),
                        0,
                        1,
                        2,
                        3,
                        "v".to_string(),
                        1,
                        1,
                        "vertices".to_string(),
                        child,
                    ))),
                );

                let gathered = StatisticsGathering::new()
                    .gather(plan, &mut ctx)
                    .expect("deep graph gathering should succeed");
                assert_eq!(
                    gathered.stats.estimated_cardinality,
                    Some(CardinalityEstimate {
                        min: 5,
                        expected: 10,
                        max: 20,
                    })
                );

                let LogicalOperator::GraphExpand(expand) = &gathered.operator else {
                    panic!("expected graph-expand root");
                };
                let mut child = expand.child.as_ref();
                for _ in 0..DEPTH {
                    let LogicalOperator::Filter(filter) = &child.operator else {
                        panic!("expected deep graph chain to retain its filters");
                    };
                    child = &filter.child;
                }
                let LogicalOperator::GraphScan(scan) = &child.operator else {
                    panic!("expected graph-scan leaf");
                };
                assert_eq!(scan.graph_name, "g");
                assert_eq!(
                    child.stats.estimated_cardinality,
                    Some(CardinalityEstimate::exact(10))
                );
            })
            .expect("deep graph-name thread should start")
            .join()
            .expect("deep graph-name lookup should complete");
    }

    #[test]
    fn materialized_cte_publishes_producer_cardinality_before_its_consumer() {
        let bind_context = BindContext::new();
        let session = make_test_session();
        let mut ctx = OptimizationContext::new(session, bind_context.clone());
        let producer = values_relation(&bind_context, 1, 37);
        let consumer = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::CTERef(CTERef::new(
                9,
                2,
                "shared".to_string(),
                vec!["v".to_string()],
                vec![LogicalType::BigInt],
            )),
        );
        let plan = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::MaterializedCTE(MaterializedCTE::new(
                9,
                "shared".to_string(),
                vec!["v".to_string()],
                vec![LogicalType::BigInt],
                CTEMaterialize::Materialized,
                producer,
                consumer,
            )),
        );

        assert_query_settlement(&plan, &ctx);
        let gathered = StatisticsGathering::new()
            .gather(plan, &mut ctx)
            .expect("gather should succeed");
        let LogicalOperator::MaterializedCTE(cte) = &gathered.operator else {
            panic!("expected materialized CTE");
        };

        assert_eq!(
            cte.child.stats.estimated_cardinality,
            Some(CardinalityEstimate::exact(37))
        );
        assert_eq!(
            gathered.stats.estimated_cardinality,
            Some(CardinalityEstimate::exact(37))
        );
    }

    #[test]
    fn detached_cte_reference_retains_its_owner_cardinality_summary() {
        let bind_context = BindContext::new();
        let session = make_test_session();
        let mut ctx = OptimizationContext::new(session, bind_context.clone());
        let mut reference = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::CTERef(CTERef::new(
                9,
                2,
                "shared".to_string(),
                vec!["v".to_string()],
                vec![LogicalType::BigInt],
            )),
        );
        reference.stats.estimated_cardinality = Some(CardinalityEstimate::exact(37));

        assert_query_settlement(&reference, &ctx);
        let gathered = StatisticsGathering::new()
            .gather(reference, &mut ctx)
            .expect("detached reference should retain its summary");

        assert_eq!(
            gathered.stats.estimated_cardinality,
            Some(CardinalityEstimate::exact(37))
        );
    }

    #[test]
    fn delim_join_publishes_outer_key_domain_before_its_consumer() {
        let bind_context = BindContext::new();
        let session = make_test_session();
        let mut ctx = OptimizationContext::new(session, bind_context.clone());
        let outer = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                1,
                (0..35)
                    .map(|value| {
                        vec![Expression::Constant(
                            ConstantExpression::new(Value::BigInt(value % 7), LogicalType::BigInt)
                                .into(),
                        )]
                    })
                    .collect(),
                vec!["key".to_string()],
                vec![LogicalType::BigInt],
            )),
        );
        let delim = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::DelimGet(DelimGet::new(9, vec![LogicalType::BigInt])),
        );
        let mut join = paro_planner::operator::ComparisonJoin::new(
            JoinType::Inner,
            outer,
            delim,
            vec![equality(1, 0, 9, 0)],
        );
        join.duplicate_eliminated_columns = vec![column_ref(1, 0)];
        let plan =
            OwnedLogicalPlan::new(&bind_context, LogicalOperator::Join(Join::Comparison(join)));

        assert_query_settlement(&plan, &ctx);
        let gathered = StatisticsGathering::new()
            .gather(plan, &mut ctx)
            .expect("gather should succeed");
        let LogicalOperator::Join(Join::Comparison(join)) = &gathered.operator else {
            panic!("expected delim join")
        };
        let estimate = join
            .right
            .stats
            .estimated_cardinality
            .expect("DelimGet should inherit the outer key domain");
        assert!(estimate.expected > 1 && estimate.expected < 35);
        assert_eq!(estimate.max, estimate.expected * 2);
        assert!(ctx.column_stats.contains_key(&ColumnBinding::new(9, 0)));
    }

    #[test]
    fn dummy_scan_is_the_exact_one_row_relation() {
        let bind_context = BindContext::new();
        let session = make_test_session();
        let mut ctx = OptimizationContext::new(session, bind_context.clone());
        let plan = OwnedLogicalPlan::new(&bind_context, LogicalOperator::DummyScan);

        assert_query_settlement(&plan, &ctx);
        let gathered = StatisticsGathering::new()
            .gather(plan, &mut ctx)
            .expect("gather should succeed");

        assert_eq!(
            gathered.stats.estimated_cardinality,
            Some(CardinalityEstimate::exact(1))
        );
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
            OwnedLogicalPlan::new(&bind_context, LogicalOperator::Join(Join::Comparison(join)));
        plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(73));
        plan.stats.cardinality_provenance = CardinalityProvenance::JoinGraph;

        assert_query_settlement(&plan, &ctx);
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
    fn grouped_aggregate_over_proven_empty_input_is_empty() {
        let bind_context = BindContext::new();
        let session = make_test_session();
        let mut ctx = OptimizationContext::new(session, bind_context.clone());
        let child = values_relation(&bind_context, 1, 0);
        let aggregate = Aggregate::new(
            2,
            3,
            4,
            child,
            vec![column_ref(1, 0)],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        let plan = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::Aggregate(Box::new(aggregate)),
        );

        assert_query_settlement(&plan, &ctx);
        let gathered = StatisticsGathering::new()
            .gather(plan, &mut ctx)
            .expect("gather should succeed");

        assert_eq!(
            gathered.stats.estimated_cardinality,
            Some(CardinalityEstimate::exact(0))
        );
    }

    #[test]
    fn known_group_distinct_count_has_a_finite_uncertainty_envelope() {
        let bind_context = BindContext::new();
        let session = make_test_session();
        let mut ctx = OptimizationContext::new(session, bind_context);
        let binding = ColumnBinding::new(1, 0);
        let base = BaseStatistics::new(LogicalType::BigInt);
        let mut stats = ColumnStatistics::new(base);
        stats.update_distinct_statistics(&[11, 29], 2);
        ctx.column_stats_mut().insert(binding, Arc::new(stats));

        assert_eq!(
            estimate_group_distinct(&column_ref(1, 0), &ctx, 4_096, 4_096),
            (2, Some(4))
        );
    }

    #[test]
    fn singleton_range_is_exact_without_forging_an_hll_observation() {
        let bind_context = BindContext::new();
        let session = make_test_session();
        let mut ctx = OptimizationContext::new(session, bind_context);
        let binding = ColumnBinding::new(1, 0);
        ctx.column_stats_mut().insert(
            binding,
            Arc::new(
                ColumnStatistics::new(BaseStatistics::from_constant(&Value::BigInt(2001)))
                    .with_guaranteed_distinct_upper(1),
            ),
        );

        assert_eq!(
            estimate_group_distinct(&column_ref(1, 0), &ctx, 4_096, 4_096),
            (1, Some(1))
        );
    }

    #[test]
    fn value_selecting_aggregate_preserves_its_input_domain_statistics() {
        let bind_context = BindContext::new();
        let session = make_test_session();
        let mut ctx = OptimizationContext::new(session, bind_context);
        let binding = ColumnBinding::new(1, 0);
        let mut input = ColumnStatistics::new(BaseStatistics::from_constant(&Value::BigInt(7)));
        input.update_distinct_statistics(&[11, 29, 47], 3);
        ctx.column_stats_mut().insert(binding, Arc::new(input));
        let (function, target_types) =
            paro_function::aggregate::distributive::first_last::get_first_function()
                .bind(&[LogicalType::BigInt])
                .expect("bind FIRST");
        assert_eq!(target_types, vec![LogicalType::BigInt]);
        let return_type = function.return_type.clone();
        let expression = Expression::Aggregate(
            paro_planner::expression::AggregateExpression::new(
                function,
                vec![column_ref(1, 0)],
                return_type,
            )
            .into(),
        );

        let unconstrained = aggregate_expression_statistics(&expression, &ctx, None);
        assert_eq!(unconstrained.distinct_evidence().point, 3);
        assert_eq!(unconstrained.guaranteed_distinct_upper(), None);

        let output = aggregate_expression_statistics(&expression, &ctx, Some(1));

        assert_eq!(output.distinct_evidence().point, 1);
        assert_eq!(output.statistics().min_value(), Some(Value::BigInt(7)));
        assert_eq!(output.statistics().max_value(), Some(Value::BigInt(7)));
    }

    #[test]
    fn filter_output_replaces_storage_hll_with_exact_equality_domain() {
        let bind_context = BindContext::new();
        let session = make_test_session();
        let mut ctx = OptimizationContext::new(session, bind_context.clone());
        let binding = ColumnBinding::new(1, 0);
        let mut storage =
            ColumnStatistics::new(BaseStatistics::create_unknown(LogicalType::BigInt));
        storage.update_distinct_statistics(&[11, 29, 47], 3);
        ctx.column_stats_mut().insert(binding, Arc::new(storage));
        let filter = Filter::new(
            values_relation(&bind_context, 1, 4),
            vec![Expression::Comparison(
                ComparisonExpression::new(
                    ComparisonType::Equal,
                    column_ref(1, 0),
                    Expression::Constant(
                        ConstantExpression::new(Value::BigInt(2001), LogicalType::BigInt).into(),
                    ),
                )
                .into(),
            )],
        );

        let output = filter_output_stats(&filter, &filter.child.output_layout(), &ctx);
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].distinct_evidence().point, 1);
        assert_eq!(
            output[0].statistics().min_value(),
            Some(Value::BigInt(2001))
        );
        ctx.column_stats_mut().insert(binding, output[0].clone());
        assert_eq!(
            estimate_group_distinct(&column_ref(1, 0), &ctx, 4, 4),
            (1, Some(1))
        );
    }

    #[test]
    fn filter_output_stats_follow_non_identity_projection_order() {
        let bind_context = BindContext::new();
        let session = make_test_session();
        let mut ctx = OptimizationContext::new(session, bind_context.clone());
        let first = ColumnBinding::new(1, 0);
        let third = ColumnBinding::new(1, 2);
        ctx.column_stats_mut().insert(
            first,
            Arc::new(ColumnStatistics::new(BaseStatistics::from_constant(
                &Value::BigInt(7),
            ))),
        );
        ctx.column_stats_mut().insert(
            third,
            Arc::new(ColumnStatistics::new(BaseStatistics::from_constant(
                &Value::BigInt(99),
            ))),
        );
        let child = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                1,
                vec![vec![
                    Expression::Constant(
                        ConstantExpression::new(Value::BigInt(7), LogicalType::BigInt).into(),
                    ),
                    Expression::Constant(
                        ConstantExpression::new(Value::BigInt(8), LogicalType::BigInt).into(),
                    ),
                    Expression::Constant(
                        ConstantExpression::new(Value::BigInt(99), LogicalType::BigInt).into(),
                    ),
                ]],
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
                vec![LogicalType::BigInt; 3],
            )),
        );
        let mut filter = Filter::new(
            child,
            vec![Expression::Comparison(
                ComparisonExpression::new(
                    ComparisonType::Equal,
                    column_ref(1, 0),
                    Expression::Constant(
                        ConstantExpression::new(Value::BigInt(2001), LogicalType::BigInt).into(),
                    ),
                )
                .into(),
            )],
        );
        filter.projection_map = vec![2, 0].into();

        let output = filter_output_stats(&filter, &filter.child.output_layout(), &ctx);

        assert_eq!(output.len(), 2);
        assert_eq!(output[0].statistics().min_value(), Some(Value::BigInt(99)));
        assert_eq!(
            output[1].statistics().min_value(),
            Some(Value::BigInt(2001))
        );
        assert_eq!(output[1].guaranteed_distinct_upper(), Some(1));
    }

    #[test]
    fn finite_equality_disjunction_publishes_a_plan_invariant_domain() {
        let bind_context = BindContext::new();
        let session = make_test_session();
        let mut ctx = OptimizationContext::new(session, bind_context.clone());
        let binding = ColumnBinding::new(1, 0);
        let mut storage =
            ColumnStatistics::new(BaseStatistics::create_unknown(LogicalType::BigInt));
        storage.update_distinct_statistics(&[11, 29, 47], 3);
        ctx.column_stats_mut().insert(binding, Arc::new(storage));
        let equality = |year| {
            Expression::Comparison(
                ComparisonExpression::new(
                    ComparisonType::Equal,
                    column_ref(1, 0),
                    Expression::Constant(
                        ConstantExpression::new(Value::BigInt(year), LogicalType::BigInt).into(),
                    ),
                )
                .into(),
            )
        };
        let filter = Filter::new(
            values_relation(&bind_context, 1, 4),
            vec![Expression::Conjunction(
                paro_planner::expression::ConjunctionExpression::new(
                    ConjunctionType::Or,
                    vec![equality(2001), equality(2002), equality(2001)],
                )
                .into(),
            )],
        );

        let output = filter_output_stats(&filter, &filter.child.output_layout(), &ctx);
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].distinct_evidence().point, 2);
        assert_eq!(output[0].guaranteed_distinct_upper(), Some(2));
        assert_eq!(
            output[0].statistics().min_value(),
            Some(Value::BigInt(2001))
        );
        assert_eq!(
            output[0].statistics().max_value(),
            Some(Value::BigInt(2002))
        );
        ctx.column_stats_mut().insert(binding, output[0].clone());
        assert_eq!(
            estimate_group_distinct(&column_ref(1, 0), &ctx, 4, 4),
            (2, Some(2))
        );
    }

    #[test]
    fn same_domain_semi_join_does_not_count_duplicate_demand_rows_twice() {
        let bind_context = BindContext::new();
        let session = make_test_session();
        let mut ctx = OptimizationContext::new(session, bind_context.clone());
        let mut shared = ColumnStatistics::new(BaseStatistics::from_constant(&Value::BigInt(7)));
        shared.update_distinct_statistics(&[11, 29, 47], 3);
        let shared = Arc::new(shared);
        ctx.column_stats_mut()
            .insert(ColumnBinding::new(1, 0), shared.clone());
        ctx.column_stats_mut()
            .insert(ColumnBinding::new(2, 0), shared);

        let join = paro_planner::operator::ComparisonJoin::new(
            JoinType::Semi,
            values_relation(&bind_context, 1, 1),
            values_relation(&bind_context, 2, 1),
            vec![equality(1, 0, 2, 0)],
        );
        let estimate = estimate_same_domain_semi_join(
            &join,
            CardinalityEstimate {
                min: 36_000,
                expected: 73_049,
                max: 90_000,
            },
            CardinalityEstimate {
                min: 362,
                expected: 724,
                max: 1_086,
            },
            &[ColumnBinding::new(1, 0)],
            &[ColumnBinding::new(2, 0)],
            &CardinalityInputs {
                column_stats: &ctx.column_stats,
                cost_model: &ctx.cost_model,
                session: &ctx.session,
                graph_stats: &mut ctx.graph_stats,
            },
        )
        .expect("same-domain semi join estimate");

        assert_eq!(estimate.expected, 724);
        assert_eq!(estimate.max, 1_086);
    }

    #[test]
    fn empty_grouping_set_emits_one_row_over_empty_input() {
        let bind_context = BindContext::new();
        let session = make_test_session();
        let mut ctx = OptimizationContext::new(session, bind_context.clone());
        let child = values_relation(&bind_context, 1, 0);
        let aggregate = Aggregate::new(
            2,
            3,
            4,
            child,
            vec![column_ref(1, 0)],
            vec![GroupingSet {
                expressions: Vec::new(),
            }],
            Vec::new(),
            Vec::new(),
        );
        let plan = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::Aggregate(Box::new(aggregate)),
        );

        assert_query_settlement(&plan, &ctx);
        let gathered = StatisticsGathering::new()
            .gather(plan, &mut ctx)
            .expect("gather should succeed");

        assert_eq!(
            gathered.stats.estimated_cardinality,
            Some(CardinalityEstimate::exact(1))
        );
    }

    #[test]
    fn mark_and_single_joins_preserve_left_cardinality() {
        let left = CardinalityEstimate::exact(73);
        let right = CardinalityEstimate::exact(2);
        let inner = CardinalityEstimate::exact(1);

        assert_eq!(
            adjust_join_estimate(inner, left, right, JoinType::Mark),
            left
        );
        assert_eq!(
            adjust_join_estimate(inner, left, right, JoinType::Single),
            left
        );

        // This is the correlated-aggregate failure shape: the dependent side
        // currently estimates to zero, but MARK/SINGLE still emit one derived
        // value for every preserved row.
        let empty = CardinalityEstimate::exact(0);
        assert_eq!(
            adjust_join_estimate(empty, left, empty, JoinType::Mark),
            left
        );
        assert_eq!(
            adjust_join_estimate(empty, left, empty, JoinType::Single),
            left
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
            Expression::Reference(
                paro_planner::expression::ReferenceExpression::new(1, LogicalType::BigInt).into(),
            ),
            Expression::Reference(
                paro_planner::expression::ReferenceExpression::new(0, LogicalType::BigInt).into(),
            ),
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
