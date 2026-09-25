// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_parser::ast::PathQuantifier;
use paro_planner::expression::{ComparisonExpression, ComparisonType, ConjunctionType, Expression};
use paro_planner::logical::operator::{
    ColumnBinding, Filter, FullTextFilterScan, Get, GraphExpand, GraphScan, Join,
    JoinComparisonType, JoinCondition, JoinType, LogicalOperator, LogicalOutputLayout, SearchScan,
    SetOpType, SubplanRef,
};
use paro_planner::logical::plan::{
    CardinalityEstimate, CardinalityProvenance, LogicalPlanPostOrderFolder, NodeStats,
    OwnedLogicalPlan,
};
use paro_storage::index::graph::GraphStatsProvider;
use paro_storage::statistics::{BaseStatistics, ColumnStatistics};

use crate::context::{GraphStatsCache, OptimizationContext, SharedColumnStatistics};
use crate::estimate::aggregate_filter::estimate_grouped_sum_distribution;

fn external_table_cardinality<Child: LocalChildFacts>(
    table: &paro_planner::logical::operator::LogicalExternalTable<Child>,
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
    proofs: crate::estimate::relation_proofs::RelationProofs,
    cte_cardinality: HashMap<usize, CardinalityEstimate>,
    cte_output_stats: HashMap<usize, BTreeMap<usize, Arc<ColumnStatistics>>>,
    delim_cardinality: HashMap<usize, CardinalityEstimate>,
    delim_output_stats: HashMap<usize, Vec<Arc<ColumnStatistics>>>,
}

struct StatisticsGatherFolder<'a> {
    gathering: &'a mut StatisticsGathering,
    context: &'a mut OptimizationContext,
}

fn cte_columns(
    layout: &LogicalOutputLayout,
    correspondence: Option<&[paro_planner::logical::operator::cte::CteOutputColumn]>,
    context: &impl ColumnStatsView,
) -> BTreeMap<usize, Arc<ColumnStatistics>> {
    let values = collect_output_stats_for_layout(layout, context);
    match correspondence {
        None => values.into_iter().enumerate().collect(),
        Some(columns) => columns
            .iter()
            .filter_map(|column| {
                let ordinal = layout
                    .bindings()
                    .iter()
                    .position(|b| *b == column.binding)?;
                Some((column.definition.0, values.get(ordinal)?.clone()))
            })
            .collect(),
    }
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
    column_stats: &'a dyn crate::estimate::ColumnStatisticsLookup,
    cost_model: &'a crate::estimate::selectivity::SelectivityModel,
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
    fn finite_domains(&self) -> &paro_planner::logical::plan::finite_domain::FiniteDomains;

    fn unique_keys(&self) -> Vec<Vec<ColumnBinding>> {
        Vec::new()
    }

    fn graph_name(&self) -> Option<&str> {
        None
    }
}

impl LocalChildFacts for Box<OwnedLogicalPlan> {
    fn finite_domains(&self) -> &paro_planner::logical::plan::finite_domain::FiniteDomains {
        &self.stats.finite_domains
    }
    fn estimated_cardinality(&self) -> Option<CardinalityEstimate> {
        self.stats.estimated_cardinality
    }

    fn graph_name(&self) -> Option<&str> {
        graph_name_for_plan(self.as_ref())
    }

    fn unique_keys(&self) -> Vec<Vec<ColumnBinding>> {
        crate::estimate::unique_keys::proven_unique_keys(self.as_ref())
    }
}

impl LocalChildFacts for SubplanRef {
    fn finite_domains(&self) -> &paro_planner::logical::plan::finite_domain::FiniteDomains {
        &self.facts.finite_domains
    }
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
        parent_skeleton: &paro_planner::logical::plan::arena::LogicalPlanNode<()>,
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

    /// Read-only relation estimate over completed inputs. Unlike settlement,
    /// this does not propagate expressions, derive proofs or publish column
    /// statistics. Callers own the eligibility of delaying those operations.
    pub(crate) fn estimate_native_cardinality(
        &mut self,
        operator: &LogicalOperator<SubplanRef>,
        child_layouts: &[LogicalOutputLayout],
        columns: &dyn crate::estimate::ColumnStatisticsLookup,
        context: &mut OptimizationContext,
    ) -> Option<CardinalityEstimate> {
        self.estimate_plan_cardinality(
            operator,
            &NodeStats::default(),
            child_layouts,
            &mut CardinalityInputs {
                column_stats: columns,
                cost_model: &context.cost_model,
                session: &context.session,
                graph_stats: &mut context.graph_stats,
            },
        )
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
        if plan.stats.cardinality_provenance != CardinalityProvenance::JoinGraph
            || matches!(plan.operator, LogicalOperator::Filter(_))
        {
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
        let maximum = crate::estimate::cardinality_bound::derive_maximum_cardinality(
            &plan.operator,
            child_maximum_cardinalities,
        );
        let inputs = plan
            .operator
            .children()
            .into_iter()
            .map(|c| crate::estimate::relation_proofs::Input {
                keys: &c.stats.unique_keys,
                domains: &c.stats.finite_domains,
            })
            .collect::<Vec<_>>();
        self.proofs.derive(
            &plan.operator,
            &output,
            child_layouts,
            &inputs,
            &mut plan.stats,
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
        operator: LogicalOperator<SubplanRef>,
        mut stats: NodeStats,
        child_layouts: &[LogicalOutputLayout],
        child_maximum_cardinalities: &[Option<u64>],
        input_column_stats: SharedColumnStatistics,
        ctx: &mut OptimizationContext,
    ) -> (
        NodeStats,
        LogicalOperator<SubplanRef>,
        LogicalOutputLayout,
        Option<u64>,
    ) {
        if stats.cardinality_provenance != CardinalityProvenance::JoinGraph
            || matches!(operator, LogicalOperator::Filter(_))
        {
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
        let maximum = crate::estimate::cardinality_bound::derive_maximum_cardinality(
            &operator,
            child_maximum_cardinalities,
        );
        let mut inputs = Vec::new();
        operator.visit_child_links(&mut |child| {
            inputs.push(crate::estimate::relation_proofs::Input {
                keys: &child.facts.unique_keys,
                domains: &child.facts.finite_domains,
            });
        });
        self.proofs
            .derive(&operator, &output, child_layouts, &inputs, &mut stats);
        self.update_output_column_stats(&operator, &stats, &output, child_layouts, maximum, ctx);
        (stats, operator, output, maximum)
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
        parent_skeleton: &paro_planner::logical::plan::arena::LogicalPlanNode<()>,
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
                self.proofs
                    .publish(cte.cte_index, &cte.output_columns, &first.stats);
                self.publish_cte_statistics(
                    cte.cte_index,
                    first,
                    first_layout,
                    Some(&cte.output_columns),
                    ctx,
                );
            }
            LogicalOperator::RecursiveCTE(cte) => {
                self.publish_cte_statistics(cte.cte_index, first, first_layout, None, ctx);
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
        correspondence: Option<&[paro_planner::logical::operator::cte::CteOutputColumn]>,
        ctx: &OptimizationContext,
    ) {
        if let Some(cardinality) = producer.stats.estimated_cardinality {
            self.cte_cardinality.insert(cte_index, cardinality);
        }
        self.cte_output_stats
            .insert(cte_index, cte_columns(producer_layout, correspondence, ctx));
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
            LogicalOperator::SubplanRef(reference) => reference.facts.cardinality,
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
                    cte_columns(child_layouts.first()?, Some(&cte.output_columns), ctx),
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
                    cte_columns(child_layouts.first()?, None, ctx),
                );
                Some(estimate)
            }
            LogicalOperator::CTERef(cte_ref) => {
                // A transformation may optimize an inner shared-plan region
                // independently from an enclosing CTE owner.
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
        if let Some(FiniteFilterSelectivity {
            fraction,
            impossible,
            residuals,
        }) = finite_filter_fraction(&filter.expressions, filter.child.finite_domains())
        {
            if impossible {
                return Some(CardinalityEstimate::exact(0));
            }
            if fraction == 1.0 && residuals.is_empty() {
                return Some(child);
            }
            let residual = ctx.cost_model.estimate_filter_cardinality_with_positions(
                child.expected,
                &residuals,
                ctx.column_stats,
                child_layout.bindings(),
            );
            // A permitted value set is not a frequency histogram. Uniform
            // weighting is only the point prior; never scale the upper
            // envelope by that fraction or advertise an exact row count.
            return Some(CardinalityEstimate {
                min: 0,
                expected: ((residual.expected as f64 * fraction).round() as u64)
                    .min(child.expected),
                max: child.max,
            });
        }
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
                    return Some(adjust_join_estimate(
                        cap_unique_join(inner, cmp, left, right, left_layout, right_layout),
                        left,
                        right,
                        cmp.join_type,
                    ));
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
                    cap_unique_join(
                        apply_selectivity(product_estimate(left, right), selectivity),
                        cmp,
                        left,
                        right,
                        left_layout,
                        right_layout,
                    ),
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
            LogicalOperator::SubplanRef(reference) => reference.column_statistics(),
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
                .map(|columns| {
                    cte.column_types
                        .iter()
                        .enumerate()
                        .map(|(i, ty)| {
                            columns
                                .get(&i)
                                .cloned()
                                .unwrap_or_else(|| ColumnStatistics::create_unknown(ty.clone()))
                        })
                        .collect()
                })
                .unwrap_or_else(|| unknown_stats_for_types(&cte.column_types)),
            LogicalOperator::CTERef(cte_ref) => self
                .cte_output_stats
                .get(&cte_ref.cte_index)
                .map(|columns| {
                    cte_ref
                        .definition_columns
                        .iter()
                        .zip(&cte_ref.column_types)
                        .map(|(definition, ty)| {
                            columns
                                .get(&definition.0)
                                .cloned()
                                .unwrap_or_else(|| ColumnStatistics::create_unknown(ty.clone()))
                        })
                        .collect()
                })
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
                | LogicalOperator::SubplanRef(_)
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

struct FiniteFilterSelectivity {
    fraction: f64,
    impossible: bool,
    residuals: Vec<Expression>,
}

fn finite_filter_fraction(
    expressions: &[Expression],
    domains: &paro_planner::logical::plan::finite_domain::FiniteDomains,
) -> Option<FiniteFilterSelectivity> {
    use paro_planner::logical::plan::finite_domain::DomainValue;
    if domains.is_empty() {
        return None;
    }
    let mut selected = BTreeMap::<ColumnBinding, BTreeSet<DomainValue>>::new();
    let mut residuals = Vec::new();
    let mut pending = expressions.iter().collect::<Vec<_>>();
    while let Some(expression) = pending.pop() {
        if let Expression::Conjunction(c) = expression {
            if c.conjunction_type == ConjunctionType::And {
                pending.extend(&c.children);
                continue;
            }
        }
        let constraint = finite_equality_domain(expression).and_then(|(binding, values)| {
            domains.get(&binding)?;
            let values = values
                .iter()
                .map(DomainValue::from_value)
                .collect::<Option<BTreeSet<_>>>()?;
            Some((binding, values))
        });
        if let Some((binding, values)) = constraint {
            selected
                .entry(binding)
                .and_modify(|old| *old = old.intersection(&values).cloned().collect())
                .or_insert(values);
        } else {
            residuals.push(expression.clone());
        }
    }
    if selected.is_empty() {
        return None;
    }
    let mut impossible = false;
    let fraction = selected.iter().fold(1.0, |fraction, (binding, values)| {
        let known = &domains[binding];
        impossible |= known.is_disjoint(values);
        fraction
            * if known.is_empty() {
                0.0
            } else {
                known.intersection(values).count() as f64 / known.len() as f64
            }
    });
    Some(FiniteFilterSelectivity {
        fraction,
        impossible,
        residuals,
    })
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
mod join;
use join::*;

mod output;
pub(crate) use output::finite_equality_domain;
use output::*;

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

mod graph;
use graph::*;

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
mod tests;
