// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Unified pipeline runner for logical-plan rewrites.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use paro_common::error::{self as paro_error, Result};
use paro_common::logging::targets;
use paro_context::StatementContext;
use paro_planner::binder::Binder;
use paro_planner::operator::LogicalOperator;
use paro_planner::plan::LogicalPlan;
use tracing::debug;

use crate::context::OptimizationContext;
use crate::external::lowering::ExternalRoutineLoweringPass;
use crate::optimizer_type::OptimizerType;
use crate::pipeline_passes::{
    AggregateJoinPreaggregationPass, AggregateJoinSubsumptionPass, AggregatePostReductionPass,
    BuildProbeSidePass, ColumnLifetimePass, CommonAggregatePass, CorrelatedPartitionAggregatePass,
    CteFilterPusherPass, CteInliningPass, DelimJoinEliminationPass, EmptyResultPullupPass,
    ExpressionRewriterPass, FilterPullupPass, FilterPushdownPass, GraphMatchDecomposePass,
    GraphPredicatePushdownPass, GraphStartSelectionPass, InClausePass, JoinEliminationPass,
    JoinFilterPushdownPass, JoinOrderPass, LatePayloadFetchPass, LimitPushdownPass,
    MixedJoinPredicatePass, ReorderFilterPass, SearchOptimizationPass, SegmentPrunerPass,
    StatisticsGatheringPass, StatisticsPropagationPass, TopNPass, UnusedColumnsPass,
};
use crate::profiler::publish_optimizer_profile_snapshot;
use crate::rewriter::Rewriter;
use crate::statistics::gathering::StatisticsGathering;
use crate::verify::verify_logical_plan;

pub struct Optimizer {
    pipeline: Vec<Box<dyn Rewriter>>,
    disabled: HashSet<OptimizerType>,
    ctx: OptimizationContext,
}

impl Optimizer {
    pub fn new(binder: Binder, session: Arc<StatementContext>) -> Self {
        Self::with_disabled(binder, session, HashSet::new())
    }

    pub fn with_disabled(
        binder: Binder,
        session: Arc<StatementContext>,
        disabled: HashSet<OptimizerType>,
    ) -> Self {
        let ctx = OptimizationContext::new(session, binder.bind_context.clone());
        let pipeline = Self::build_pipeline(binder);
        Self {
            pipeline,
            disabled,
            ctx,
        }
    }

    pub fn optimizer_disabled(&self, opt_type: OptimizerType) -> bool {
        self.disabled.contains(&opt_type)
    }

    pub fn disable_optimizer(&mut self, opt_type: OptimizerType) {
        self.disabled.insert(opt_type);
    }

    pub fn enable_optimizer(&mut self, opt_type: OptimizerType) {
        self.disabled.remove(&opt_type);
    }

    pub fn optimize(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let started_at = Instant::now();
        debug!(
            target: targets::OPTIMIZER,
            disabled_optimizers = self.disabled.len(),
            pipeline_len = self.pipeline.len(),
            "Optimization started"
        );

        let optimized = self.optimize_plan(plan)?;
        let lowering = ExternalRoutineLoweringPass::lower(optimized, &self.ctx.bind_context)?;
        let mut optimized = lowering.plan;

        if lowering.changed {
            self.ctx.column_stats.clear();
            optimized = StatisticsGathering::new().gather(optimized, &mut self.ctx)?;
            if self.ctx.verify_enabled {
                verify_logical_plan(&self.ctx.bind_context, &optimized)?;
            }
        }

        debug!(
            target: targets::OPTIMIZER,
            elapsed_ms = started_at.elapsed().as_millis(),
            external_lowering = lowering.changed,
            "Optimization completed"
        );
        publish_optimizer_profile_snapshot(
            self.ctx
                .profiler
                .snapshot(&self.pipeline_types(), &self.disabled),
        );
        Ok(optimized)
    }

    fn optimize_plan(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        if matches!(plan.operator, LogicalOperator::Explain(_)) {
            return self.optimize_explain(plan);
        }
        if Self::should_skip_optimization(&plan) {
            return Ok(plan);
        }

        if self.ctx.verify_enabled {
            verify_logical_plan(&self.ctx.bind_context, &plan)?;
        }

        let mut current = plan;
        for pass in &mut self.pipeline {
            let opt_type = pass.optimizer_type();
            if self.disabled.contains(&opt_type) {
                continue;
            }

            let started_at = Instant::now();
            let rewrite_result = pass.rewrite(current, &mut self.ctx);
            self.ctx.profiler.record(opt_type, started_at.elapsed());
            current = rewrite_result?;

            if self.ctx.verify_enabled {
                verify_logical_plan(&self.ctx.bind_context, &current).map_err(|error| {
                    paro_error::internal(format!(
                        "Logical plan invariant failed after optimizer pass {opt_type}: {}",
                        error.message()
                    ))
                })?;
            }
        }

        Ok(current)
    }

    fn optimize_explain(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        if !matches!(plan.operator, LogicalOperator::Explain(_)) {
            unreachable!("optimize_explain requires a LogicalOperator::Explain root")
        }

        let optimized = plan.try_map_children(|child| self.optimize_plan(child))?;
        if self.ctx.verify_enabled {
            verify_logical_plan(&self.ctx.bind_context, &optimized)?;
        }
        Ok(optimized)
    }

    fn should_skip_optimization(plan: &LogicalPlan) -> bool {
        matches!(plan.operator, LogicalOperator::DummyScan)
    }

    fn pipeline_types(&self) -> Vec<OptimizerType> {
        self.pipeline
            .iter()
            .map(|pass| pass.optimizer_type())
            .collect()
    }

    fn build_pipeline(binder: Binder) -> Vec<Box<dyn Rewriter>> {
        let unused_columns_binder = binder.clone();
        let late_payload_binder = binder.clone();
        vec![
            Box::new(GraphStartSelectionPass),
            Box::new(GraphMatchDecomposePass),
            Box::new(GraphPredicatePushdownPass),
            Box::new(ExpressionRewriterPass),
            Box::new(CommonAggregatePass),
            Box::new(CteInliningPass),
            Box::new(FilterPullupPass),
            Box::new(FilterPushdownPass),
            Box::new(CteFilterPusherPass),
            // Second inlining pass: filters were pushed into CTE bodies; inline again when beneficial.
            Box::new(CteInliningPass),
            Box::new(DelimJoinEliminationPass),
            Box::new(EmptyResultPullupPass),
            // Canonicalize comma joins and mixed predicates before semantic
            // join rewrites and cost-based ordering inspect the graph.
            Box::new(MixedJoinPredicatePass),
            // Reuse a detail stream for a correlated full-partition
            // aggregate while the canonical delim shape and declared-key
            // join graph are still explicit.
            Box::new(CorrelatedPartitionAggregatePass),
            // Fold a scalar aggregate over a provably alpha-equivalent source
            // into the grouped aggregate before either branch receives stats.
            Box::new(AggregatePostReductionPass),
            // Planning statistics: collect base-column bounds and distinct
            // counts before join ordering consumes them as cost inputs.
            Box::new(StatisticsGatheringPass),
            Box::new(ReorderFilterPass),
            Box::new(JoinEliminationPass),
            // Bound a multiplicative nullable side to one row per equality key
            // before join ordering costs the resulting graph.
            Box::new(AggregateJoinPreaggregationPass),
            // Remove redundant detail scans while their semantic join edge is
            // still explicit, before cost-based ordering sees the graph.
            Box::new(AggregateJoinSubsumptionPass),
            // The rewrite changes both row production and the
            // statistics-visible HAVING shape. Re-derive cost inputs so join
            // ordering optimizes the reduced graph rather than a stale tree.
            Box::new(StatisticsGatheringPass),
            Box::new(JoinOrderPass),
            Box::new(UnusedColumnsPass {
                binder: unused_columns_binder,
            }),
            Box::new(BuildProbeSidePass),
            Box::new(JoinFilterPushdownPass),
            Box::new(TopNPass),
            Box::new(LimitPushdownPass),
            Box::new(InClausePass),
            Box::new(SearchOptimizationPass),
            Box::new(SegmentPrunerPass),
            // Final annotations: structural planning above may replace nodes
            // and preserve their old parents, so derive cardinality and output
            // statistics again over the settled tree. This is a separate
            // lifecycle phase from the cost inputs gathered before join order.
            Box::new(StatisticsGatheringPass),
            Box::new(StatisticsPropagationPass),
            // Replace functionally-dependent wide aggregate payload with a
            // stable rowid and fetch it only after a bounded TopN. Dependency
            // proofs are populated by statistics propagation immediately
            // above; pruning is performed atomically inside this pass.
            Box::new(LatePayloadFetchPass {
                binder: late_payload_binder,
            }),
            // Projection maps are positional annotations over the final logical
            // layout. Derive them only after every structural rewrite (most
            // notably build/probe-side flips) has settled that layout. This
            // terminal pass may reduce output widths, but it must not reorder
            // retained columns or invalidate operator/cardinality statistics:
            // statistics propagated above are operator-level, never indexed by
            // the pre-pruning output position.
            Box::new(ColumnLifetimePass),
        ]
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_catalog::catalog::Catalog;
    use paro_catalog::entry::{
        AggregateFunctionCatalogEntry, ColumnDefinition, OnCreateConflict,
        ScalarFunctionCatalogEntry, TableCatalogEntry,
    };
    use paro_catalog::mvcc::CatalogSnapshot;
    use paro_catalog::search_path::CatalogSearchEntry;
    use paro_common::types::LogicalType;
    use paro_context::{test_support::TestStatementContextBuilder, QueryResources};
    use paro_function::aggregate::distributive::minmax::{get_max_function, get_min_function};
    use paro_function::aggregate::distributive::sum::get_sum_function;
    use paro_function::scalar::cast::{
        date_casts, decimal_casts, BindCastInput, BoundCastInfo, CastFunctionSet,
    };
    use paro_function::scalar::ScalarFunctionSet;
    use paro_planner::operator::{Join, LogicalOperator};
    use paro_planner::planner::Planner;
    use paro_storage::table::table_factory::TableFactory;

    use super::Optimizer;
    use crate::optimizer_type::OptimizerType;

    #[test]
    fn column_lifetime_is_the_terminal_optimizer_pass() {
        let session = TestStatementContextBuilder::minimal().build();
        let planner = Planner::new(session.clone());
        let optimizer = Optimizer::new(planner.binder.clone(), session);

        assert_eq!(
            optimizer.pipeline_types().last(),
            Some(&OptimizerType::ColumnLifetime)
        );
    }

    #[test]
    fn correlated_having_retains_delim_capture_key_through_optimization() {
        let session = TestStatementContextBuilder::minimal().build();
        let mut planner = Planner::new(session.clone());
        let statement = paro_parser::parse_one(
            "SELECT o.grp \
             FROM (VALUES (1, 10), (2, 10), (3, 20)) AS o(id, grp) \
             GROUP BY o.grp \
             HAVING EXISTS( \
                 SELECT 1 \
                 FROM (VALUES (10), (30)) AS d(grp) \
                 WHERE d.grp = o.grp \
             )",
        )
        .expect("parse correlated HAVING")
        .stmt;
        planner
            .create_plan(statement)
            .expect("plan correlated HAVING");
        let plan = planner.take_plan().expect("logical plan");
        let mut optimizer = Optimizer::new(planner.binder.clone(), session);

        optimizer
            .optimize(plan)
            .expect("optimize correlated HAVING without losing delim keys");
    }

    #[test]
    fn consumed_correlated_exists_lowers_to_direct_existence_join() {
        let session = TestStatementContextBuilder::minimal().build();
        let mut planner = Planner::new(session.clone());
        let statement = paro_parser::parse_one(
            "SELECT o.k \
             FROM (VALUES (1, 10), (1, 20), (2, 30)) AS o(k, s) \
             WHERE EXISTS ( \
                 SELECT 1 \
                 FROM (VALUES (1, 10), (1, 20), (2, 30)) AS i(k, s) \
                 WHERE i.k = o.k AND i.s <> o.s \
             )",
        )
        .expect("parse correlated EXISTS")
        .stmt;
        planner
            .create_plan(statement)
            .expect("plan correlated EXISTS");
        let plan = planner.take_plan().expect("logical plan");
        let mut optimizer = Optimizer::new(planner.binder.clone(), session);
        let optimized = optimizer
            .optimize(plan)
            .expect("optimize correlated EXISTS");

        fn contains_delim_join(plan: &paro_planner::plan::LogicalPlan) -> bool {
            if matches!(
                &plan.operator,
                LogicalOperator::Join(Join::Comparison(join))
                    if !join.duplicate_eliminated_columns.is_empty()
            ) {
                return true;
            }
            plan.children()
                .iter()
                .any(|child| contains_delim_join(child))
        }

        assert!(
            !contains_delim_join(&optimized),
            "consumed EXISTS should not retain a delimiter control region: {optimized:#?}"
        );
    }

    #[test]
    fn tpch_q11_uses_one_partsupp_source_with_post_aggregate_reduction() {
        std::thread::Builder::new()
            .name("tpch-q11-optimizer-test".to_string())
            .stack_size(8 * 1024 * 1024)
            .spawn(tpch_q11_uses_one_partsupp_source_with_post_aggregate_reduction_inner)
            .expect("spawn Q11 optimizer test")
            .join()
            .expect("Q11 optimizer test thread");
    }

    fn tpch_q11_uses_one_partsupp_source_with_post_aggregate_reduction_inner() {
        let mut session = TestStatementContextBuilder::minimal()
            .with_current_database("paro")
            .with_search_path(vec![
                CatalogSearchEntry::schema_only("pg_catalog"),
                CatalogSearchEntry::schema_only("public"),
            ])
            .with_visible_version(u64::MAX)
            .build();
        let mut cast_functions = CastFunctionSet::new();
        cast_functions.register_bind_function(decimal_casts::bind_decimal_casts);
        let context = Arc::get_mut(&mut session).expect("fresh test statement context");
        context.services = Arc::new(QueryResources {
            infra: context.services.infra.clone(),
            cast_functions: Arc::new(cast_functions),
            graph_index: context.services.graph_index.clone(),
            python_runtime: context.services.python_runtime.clone(),
            governance: context.services.governance.clone(),
            plan_cache: context.services.plan_cache.clone(),
            connection_info: context.services.connection_info.clone(),
        });
        let catalog = session.catalog();
        catalog.initialize(false);
        let transaction = CatalogSnapshot::permanent_writer(u64::MAX);
        let schema = catalog
            .get_schema(&transaction, "public")
            .expect("public schema");

        for operator in ["*", "=", ">"] {
            let mut set = ScalarFunctionSet::new(operator.to_string());
            if operator == "*" {
                paro_function::scalar::operators::arithmetic::register_arithmetic_functions(
                    &mut set,
                );
            } else {
                paro_function::scalar::operators::comparison::register_comparison_functions(
                    &mut set,
                );
            }
            let entry = Arc::new(ScalarFunctionCatalogEntry::new(
                "paro".to_string(),
                "public".to_string(),
                set,
                schema.object_id_allocator().allocate(),
                0,
            ));
            schema
                .create_scalar_function(&transaction, entry, OnCreateConflict::ReplaceOnConflict)
                .expect("install scalar operator");
        }
        let sum = Arc::new(AggregateFunctionCatalogEntry::new(
            "paro".to_string(),
            "public".to_string(),
            get_sum_function(),
            schema.object_id_allocator().allocate(),
            0,
        ));
        schema
            .create_aggregate_function(&transaction, sum, OnCreateConflict::ReplaceOnConflict)
            .expect("install SUM");

        for (name, columns) in [
            (
                "partsupp",
                vec![
                    ("ps_partkey", LogicalType::BigInt),
                    ("ps_suppkey", LogicalType::BigInt),
                    ("ps_availqty", LogicalType::BigInt),
                    (
                        "ps_supplycost",
                        LogicalType::Decimal {
                            precision: 15,
                            scale: 2,
                        },
                    ),
                ],
            ),
            (
                "supplier",
                vec![
                    ("s_suppkey", LogicalType::BigInt),
                    ("s_nationkey", LogicalType::Integer),
                ],
            ),
            (
                "nation",
                vec![
                    ("n_nationkey", LogicalType::Integer),
                    ("n_name", LogicalType::Varchar),
                ],
            ),
        ] {
            let types = columns.iter().map(|(_, ty)| ty.clone()).collect::<Vec<_>>();
            let storage = Arc::new(
                TableFactory::default()
                    .create_table(&types)
                    .expect("test table storage"),
            );
            let entry = Arc::new(TableCatalogEntry::new(
                "paro".to_string(),
                "public".to_string(),
                name.to_string(),
                columns
                    .into_iter()
                    .map(|(column, ty)| ColumnDefinition::new(column.to_string(), ty))
                    .collect(),
                storage,
                schema.object_id_allocator().allocate(),
                0,
            ));
            schema
                .create_table(&transaction, entry, OnCreateConflict::ErrorOnConflict)
                .expect("install TPC-H table");
        }

        let query = include_str!("../../../benchmark/workloads/tpch/sql/q11.sql");
        let statement = paro_parser::parse_one(query).expect("parse Q11").stmt;
        let mut planner = Planner::new(session.clone());
        planner.create_plan(statement).expect("plan Q11");
        let plan = planner.take_plan().expect("logical Q11 plan");
        let mut optimizer = Optimizer::new(planner.binder.clone(), session);
        let optimized = optimizer.optimize(plan).expect("optimize Q11");

        fn inspect(plan: &paro_planner::plan::LogicalPlan) -> (usize, usize) {
            let mut partsupp_gets = usize::from(matches!(
                &plan.operator,
                LogicalOperator::Get(get)
                    if get.table.as_ref().is_some_and(|table| table.base.base.name == "partsupp")
            ));
            let mut reductions = usize::from(matches!(
                &plan.operator,
                LogicalOperator::Aggregate(aggregate) if aggregate.post_reduction.is_some()
            ));
            for child in plan.children() {
                let (child_gets, child_reductions) = inspect(child);
                partsupp_gets += child_gets;
                reductions += child_reductions;
            }
            (partsupp_gets, reductions)
        }

        let (partsupp_gets, reductions) = inspect(&optimized);
        assert_eq!(
            partsupp_gets, 1,
            "Q11 must scan partsupp once: {optimized:#?}"
        );
        assert_eq!(
            reductions, 1,
            "Q11 must carry one post reduction: {optimized:#?}"
        );
    }

    fn bind_test_literal_casts(
        input: &BindCastInput,
        source: &LogicalType,
        target: &LogicalType,
    ) -> paro_common::error::Result<Option<BoundCastInfo>> {
        match source {
            LogicalType::IntegerLiteral(_) => input
                .get_cast_function(&LogicalType::BigInt, target)
                .map(Some),
            LogicalType::StringLiteral => input
                .get_cast_function(&LogicalType::Varchar, target)
                .map(Some),
            LogicalType::Null => Ok(Some(BoundCastInfo::null(target))),
            _ => Ok(None),
        }
    }

    #[test]
    fn tpch_q15_folds_default_two_consumer_cte_into_grouped_max_reduction() {
        let mut session = TestStatementContextBuilder::minimal()
            .with_current_database("paro")
            .with_search_path(vec![
                CatalogSearchEntry::schema_only("pg_catalog"),
                CatalogSearchEntry::schema_only("public"),
            ])
            .with_visible_version(u64::MAX)
            .build();
        let mut cast_functions = CastFunctionSet::new();
        cast_functions.register_cast(
            LogicalType::Varchar,
            LogicalType::Date,
            BoundCastInfo::varlen(date_casts::varchar_to_date),
        );
        cast_functions.register_bind_function(decimal_casts::bind_decimal_casts);
        cast_functions.register_bind_function(bind_test_literal_casts);
        let context = Arc::get_mut(&mut session).expect("fresh test statement context");
        context.services = Arc::new(QueryResources {
            infra: context.services.infra.clone(),
            cast_functions: Arc::new(cast_functions),
            graph_index: context.services.graph_index.clone(),
            python_runtime: context.services.python_runtime.clone(),
            governance: context.services.governance.clone(),
            plan_cache: context.services.plan_cache.clone(),
            connection_info: context.services.connection_info.clone(),
        });
        let catalog = session.catalog();
        catalog.initialize(false);
        let transaction = CatalogSnapshot::permanent_writer(u64::MAX);
        let schema = catalog
            .get_schema(&transaction, "public")
            .expect("public schema");

        for operator in ["*", "-", "=", ">=", "<"] {
            let mut set = ScalarFunctionSet::new(operator.to_string());
            if matches!(operator, "*" | "-") {
                paro_function::scalar::operators::arithmetic::register_arithmetic_functions(
                    &mut set,
                );
            } else {
                paro_function::scalar::operators::comparison::register_comparison_functions(
                    &mut set,
                );
            }
            let entry = Arc::new(ScalarFunctionCatalogEntry::new(
                "paro".to_string(),
                "public".to_string(),
                set,
                schema.object_id_allocator().allocate(),
                0,
            ));
            schema
                .create_scalar_function(&transaction, entry, OnCreateConflict::ReplaceOnConflict)
                .expect("install scalar operator");
        }
        for function in [get_sum_function(), get_max_function(), get_min_function()] {
            let entry = Arc::new(AggregateFunctionCatalogEntry::new(
                "paro".to_string(),
                "public".to_string(),
                function,
                schema.object_id_allocator().allocate(),
                0,
            ));
            schema
                .create_aggregate_function(&transaction, entry, OnCreateConflict::ReplaceOnConflict)
                .expect("install aggregate");
        }
        for (name, columns) in [
            (
                "lineitem",
                vec![
                    ("l_suppkey", LogicalType::BigInt),
                    (
                        "l_extendedprice",
                        LogicalType::Decimal {
                            precision: 15,
                            scale: 2,
                        },
                    ),
                    (
                        "l_discount",
                        LogicalType::Decimal {
                            precision: 15,
                            scale: 2,
                        },
                    ),
                    ("l_shipdate", LogicalType::Date),
                ],
            ),
            (
                "supplier",
                vec![
                    ("s_suppkey", LogicalType::BigInt),
                    ("s_name", LogicalType::Varchar),
                    ("s_address", LogicalType::Varchar),
                    ("s_phone", LogicalType::Varchar),
                ],
            ),
        ] {
            let types = columns.iter().map(|(_, ty)| ty.clone()).collect::<Vec<_>>();
            let storage = Arc::new(
                TableFactory::default()
                    .create_table(&types)
                    .expect("test table storage"),
            );
            let entry = Arc::new(TableCatalogEntry::new(
                "paro".to_string(),
                "public".to_string(),
                name.to_string(),
                columns
                    .into_iter()
                    .map(|(column, ty)| ColumnDefinition::new(column.to_string(), ty))
                    .collect(),
                storage,
                schema.object_id_allocator().allocate(),
                0,
            ));
            schema
                .create_table(&transaction, entry, OnCreateConflict::ErrorOnConflict)
                .expect("install TPC-H table");
        }

        let query = include_str!("../../../benchmark/workloads/tpch/sql/q15.sql");
        let statement = paro_parser::parse_one(query).expect("parse Q15").stmt;
        let mut planner = Planner::new(session.clone());
        planner.create_plan(statement).expect("plan Q15");
        let plan = planner.take_plan().expect("logical Q15 plan");
        let mut optimizer = Optimizer::new(planner.binder.clone(), session.clone());
        let optimized = optimizer.optimize(plan).expect("optimize Q15");
        crate::verify::verify_logical_plan(&planner.binder.bind_context, &optimized)
            .expect("verify optimized Q15");

        fn inspect(plan: &paro_planner::plan::LogicalPlan) -> (usize, usize, usize, usize) {
            let materialized =
                usize::from(matches!(plan.operator, LogicalOperator::MaterializedCTE(_)));
            let references = usize::from(matches!(plan.operator, LogicalOperator::CTERef(_)));
            let lineitem = usize::from(matches!(
                &plan.operator,
                LogicalOperator::Get(get)
                    if get.table.as_ref().is_some_and(|table| table.base.base.name == "lineitem")
            ));
            let reductions = usize::from(matches!(
                &plan.operator,
                LogicalOperator::Aggregate(aggregate) if aggregate.post_reduction.is_some()
            ));
            plan.children().into_iter().fold(
                (materialized, references, lineitem, reductions),
                |totals, child| {
                    let child = inspect(child);
                    (
                        totals.0 + child.0,
                        totals.1 + child.1,
                        totals.2 + child.2,
                        totals.3 + child.3,
                    )
                },
            )
        }

        assert_eq!(inspect(&optimized), (0, 0, 1, 1));
        fn reduction(
            plan: &paro_planner::plan::LogicalPlan,
        ) -> Option<&paro_planner::operator::PostAggregateReduction> {
            if let LogicalOperator::Aggregate(aggregate) = &plan.operator {
                if aggregate.post_reduction.is_some() {
                    return aggregate.post_reduction.as_ref();
                }
            }
            plan.children().into_iter().find_map(reduction)
        }
        let reduction = reduction(&optimized).expect("Q15 post reduction");
        assert!(matches!(
            reduction.reducers.as_slice(),
            [paro_planner::expression::Expression::Aggregate(max)] if max.function.name == "max"
        ));
        assert!(matches!(
            reduction.predicate,
            paro_planner::expression::Expression::Comparison(ref comparison)
                if comparison.comparison_type
                    == paro_planner::expression::ComparisonType::Equal
        ));

        let explicitly_materialized =
            query.replacen("WITH revenue AS (", "WITH revenue AS MATERIALIZED (", 1);
        let statement = paro_parser::parse_one(&explicitly_materialized)
            .expect("parse explicitly materialized Q15")
            .stmt;
        let mut planner = Planner::new(session.clone());
        planner
            .create_plan(statement)
            .expect("plan explicitly materialized Q15");
        let plan = planner.take_plan().expect("logical explicit Q15 plan");
        let mut optimizer = Optimizer::new(planner.binder.clone(), session.clone());
        let explicit = optimizer.optimize(plan).expect("optimize explicit Q15");
        assert_eq!(inspect(&explicit).0, 1, "explicit MATERIALIZED is a fence");
        assert_eq!(inspect(&explicit).3, 0);

        let third_consumer = query.replacen(
            "ORDER BY",
            "AND supplier_no >= (SELECT min(supplier_no) FROM revenue) ORDER BY",
            1,
        );
        let statement = paro_parser::parse_one(&third_consumer)
            .expect("parse three-consumer Q15")
            .stmt;
        let mut planner = Planner::new(session.clone());
        planner
            .create_plan(statement)
            .expect("plan three-consumer Q15");
        let plan = planner
            .take_plan()
            .expect("logical three-consumer Q15 plan");
        let mut optimizer = Optimizer::new(planner.binder.clone(), session.clone());
        let third = optimizer
            .optimize(plan)
            .expect("optimize three-consumer Q15");
        assert_eq!(inspect(&third).0, 1, "a third consumer prevents fusion");
        assert_eq!(inspect(&third).3, 0);

        let statement = paro_parser::parse_one(query)
            .expect("parse volatile Q15")
            .stmt;
        let mut planner = Planner::new(session.clone());
        planner.create_plan(statement).expect("plan volatile Q15");
        let mut plan = planner.take_plan().expect("logical volatile Q15 plan");
        let LogicalOperator::MaterializedCTE(cte) = &mut plan.operator else {
            panic!("Q15 CTE wrapper");
        };
        let LogicalOperator::Projection(definition) = &mut cte.cte_query.operator else {
            panic!("Q15 CTE projection");
        };
        let LogicalOperator::Aggregate(grouped) = &mut definition.child.operator else {
            panic!("Q15 grouped aggregate");
        };
        let original_group = std::mem::replace(
            &mut grouped.groups[0],
            paro_planner::expression::Expression::Constant(
                paro_planner::expression::ConstantExpression::new(
                    paro_common::runtime_value::Value::BigInt(0),
                    LogicalType::BigInt,
                ),
            ),
        );
        let volatile = paro_function::scalar::ScalarFunction::new(
            "volatile_identity".to_string(),
            vec![LogicalType::BigInt],
            LogicalType::BigInt,
            |_chunk, _state, _result| Ok(()),
        )
        .with_stability(paro_function::scalar::FunctionStability::Volatile);
        grouped.groups[0] = paro_planner::expression::Expression::Function(
            paro_planner::expression::FunctionExpression::new(
                volatile,
                vec![original_group],
                LogicalType::BigInt,
            ),
        );
        grouped.recompute_returned_types();
        let mut optimizer = Optimizer::new(planner.binder.clone(), session);
        let volatile = optimizer.optimize(plan).expect("optimize volatile Q15");
        assert_eq!(
            inspect(&volatile).0,
            1,
            "volatile definitions remain fenced"
        );
        assert_eq!(inspect(&volatile).3, 0);
    }
}
