use std::sync::Arc;

use paro_catalog::catalog::Catalog;
use paro_catalog::entry::{
    AggregateFunctionCatalogEntry, CatalogEntryEnum, ColumnDefinition, Constraint, CreateTableInfo,
    OnCreateConflict, ScalarFunctionCatalogEntry, TableCatalogEntry,
};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_catalog::search_path::CatalogSearchEntry;
use paro_common::types::LogicalType;
use paro_context::{test_support::TestStatementContextBuilder, QueryResources};
use paro_function::aggregate::distributive::{
    avg::get_avg_function,
    count::{get_count_function, get_count_star_function},
    minmax::get_min_function,
    sum::get_sum_function,
};
use paro_function::aggregate::AggregateFunctionSet;
use paro_function::scalar::cast::{
    date_casts, decimal_casts, numeric_casts, BindCastInput, BoundCastInfo, CastFunctionSet,
};
use paro_function::scalar::string::get_substring_functions;
use paro_function::scalar::ScalarFunctionSet;
use paro_planner::expression::{ColumnRefExpression, Expression};
use paro_planner::operator::LogicalOperator;
use paro_planner::operator::{ColumnBinding, Projection};
use paro_planner::planner::Planner;
use paro_storage::table::table_factory::TableFactory;

use super::partition_aggregate::CorrelatedPartitionAggregate;
use crate::optimizer::Optimizer;

#[test]
fn tpch_q02_and_q17_reuse_the_detail_source() {
    std::thread::Builder::new()
        .name("tpch-correlated-partition-test".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(assert_tpch_rewrites)
        .expect("spawn optimizer test")
        .join()
        .expect("optimizer test");
}

#[test]
fn tpch_q20_pulls_unique_correlated_sum_into_grouped_join() {
    std::thread::Builder::new()
        .name("tpch-q20-grouped-correlation-test".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let session = setup_session();
            let statement = paro_parser::parse_one(include_str!(
                "../../../../benchmark/workloads/tpch/sql/q20.sql"
            ))
            .expect("parse q20")
            .stmt;
            let mut planner = Planner::new(session.clone());
            planner.create_plan(statement).expect("plan q20");
            let planned = planner.take_plan().expect("logical q20");
            let mut optimizer = Optimizer::new(planner.binder.clone(), session);
            let optimized = optimizer.optimize(planned).expect("optimize q20");
            let inspection = inspect_plan(&optimized);

            assert_eq!(inspection.delim_joins, 0, "{optimized:#?}");
            assert_eq!(inspection.aggregates, 1, "{optimized:#?}");
            assert_eq!(inspection.aggregate_groups, vec![3], "{optimized:#?}");
            assert_eq!(inspection.group_dependencies, 0, "{optimized:#?}");
            assert_eq!(inspection.gets_named("partsupp"), 1, "{optimized:#?}");
            assert_eq!(inspection.gets_named("lineitem"), 1, "{optimized:#?}");
        })
        .expect("spawn q20 optimizer test")
        .join()
        .expect("q20 optimizer test");
}

#[test]
fn grouped_join_uses_empty_input_contract_instead_of_function_name() {
    let optimized = optimize_sql(
        "SELECT ps.ps_partkey \
         FROM partsupp AS ps \
         WHERE ps.ps_availqty > ( \
             SELECT min(l.l_quantity) \
             FROM lineitem AS l \
             WHERE l.l_partkey = ps.ps_partkey \
               AND l.l_suppkey = ps.ps_suppkey)",
    );
    let inspection = inspect_plan(&optimized);

    assert_eq!(inspection.delim_joins, 0, "{optimized:#?}");
    assert_eq!(inspection.gets_named("partsupp"), 1, "{optimized:#?}");
    assert_eq!(inspection.gets_named("lineitem"), 1, "{optimized:#?}");
}

#[test]
fn grouped_join_rejects_count_empty_input_contract() {
    let optimized = optimize_sql(
        "SELECT ps.ps_partkey \
         FROM partsupp AS ps \
         WHERE ps.ps_availqty > ( \
             SELECT count(l.l_quantity) \
             FROM lineitem AS l \
             WHERE l.l_partkey = ps.ps_partkey \
               AND l.l_suppkey = ps.ps_suppkey)",
    );
    let inspection = inspect_plan(&optimized);

    assert_eq!(inspection.delim_joins, 1, "{optimized:#?}");
}

#[test]
fn grouped_join_rejects_non_null_rejecting_scalar_predicate() {
    let optimized = optimize_sql(
        "SELECT ps.ps_partkey \
         FROM partsupp AS ps \
         WHERE ps.ps_availqty IS NOT DISTINCT FROM ( \
             SELECT sum(l.l_quantity) \
             FROM lineitem AS l \
             WHERE l.l_partkey = ps.ps_partkey \
               AND l.l_suppkey = ps.ps_suppkey)",
    );
    let inspection = inspect_plan(&optimized);

    assert_eq!(inspection.delim_joins, 1, "{optimized:#?}");
}

#[test]
fn grouped_join_rejects_correlation_that_does_not_cover_outer_unique_key() {
    let optimized = optimize_sql(
        "SELECT l.l_orderkey \
         FROM lineitem AS l \
         WHERE l.l_quantity > ( \
             SELECT sum(ps.ps_availqty) \
             FROM partsupp AS ps \
             WHERE ps.ps_partkey = l.l_partkey)",
    );
    let inspection = inspect_plan(&optimized);

    assert_eq!(inspection.delim_joins, 1, "{optimized:#?}");
}

#[test]
fn grouped_join_preserves_real_values_before_a_live_late_ordinal() {
    let optimized = optimize_sql(
        "SELECT ps.ps_comment \
         FROM partsupp AS ps \
         WHERE ps.ps_availqty > ( \
             SELECT sum(l.l_quantity) \
             FROM lineitem AS l \
             WHERE l.l_partkey = ps.ps_partkey \
               AND l.l_suppkey = ps.ps_suppkey)",
    );
    let inspection = inspect_plan(&optimized);

    assert_eq!(inspection.delim_joins, 0, "{optimized:#?}");
    // comment is ordinal 4. Reconstructing that original binding requires a
    // real, contiguous 0..=4 prefix; no invisible slot may be replaced by a
    // synthesized NULL.
    assert_eq!(inspection.aggregate_groups, vec![5], "{optimized:#?}");
    assert_eq!(inspection.group_dependencies, 0, "{optimized:#?}");
}

#[test]
fn grouped_join_with_no_visible_outer_binding_avoids_zero_width_projection() {
    let rewritten = optimize_sql(
        "SELECT 1 \
         FROM partsupp AS ps \
         WHERE ps.ps_availqty > ( \
             SELECT sum(l.l_quantity) \
             FROM lineitem AS l \
             WHERE l.l_partkey = ps.ps_partkey \
               AND l.l_suppkey = ps.ps_suppkey)",
    );
    let inspection = inspect_plan(&rewritten);

    assert_eq!(inspection.delim_joins, 0, "{rewritten:#?}");
    assert_eq!(inspection.zero_width_projections, 0, "{rewritten:#?}");
}

fn assert_tpch_rewrites() {
    let session = setup_session();
    for (query, sql) in [
        (
            "q02",
            include_str!("../../../../benchmark/workloads/tpch/sql/q02.sql"),
        ),
        (
            "q17",
            include_str!("../../../../benchmark/workloads/tpch/sql/q17.sql"),
        ),
    ] {
        let statement = paro_parser::parse_one(sql).expect("parse").stmt;
        let mut planner = Planner::new(session.clone());
        planner.create_plan(statement).expect("plan");
        let planned = planner.take_plan().expect("logical plan");
        let mut optimizer = Optimizer::new(planner.binder.clone(), session.clone());
        let optimized = optimizer.optimize(planned).expect("optimize");
        let inspection = inspect_plan(&optimized);
        assert_eq!(inspection.windows, 1, "{query}: {optimized:#?}");
        assert_eq!(inspection.delim_joins, 0, "{query}: {optimized:#?}");
        assert_eq!(inspection.gets_named("part"), 1, "{query}: {optimized:#?}");
        let common = if query == "q02" {
            "partsupp"
        } else {
            "lineitem"
        };
        assert_eq!(inspection.gets_named(common), 1, "{query}: {optimized:#?}");
        // This structural fixture has no physical rows. Sparse fetch carries
        // a real vector/reader startup cost, so the cost model must preserve
        // the eager payload plan here; SF1 performance coverage separately
        // proves that Q2's populated relation crosses the fetch frontier.
        assert_eq!(inspection.late_fetches, 0, "{query}: {optimized:#?}");
        assert_eq!(inspection.late_fetch_sources, 0, "{query}: {optimized:#?}");
    }
}

pub(super) fn setup_session() -> Arc<paro_context::StatementContext> {
    let mut session = TestStatementContextBuilder::minimal()
        .with_current_database("paro")
        .with_search_path(vec![
            CatalogSearchEntry::schema_only("pg_catalog"),
            CatalogSearchEntry::schema_only("public"),
        ])
        .with_visible_version(u64::MAX)
        .build();
    let mut casts = CastFunctionSet::new();
    casts.register_cast(
        LogicalType::BigInt,
        LogicalType::Integer,
        BoundCastInfo::fixed(numeric_casts::int64_to_int32),
    );
    casts.register_cast(
        LogicalType::Integer,
        LogicalType::BigInt,
        BoundCastInfo::fixed(numeric_casts::int32_to_int64),
    );
    casts.register_cast(
        LogicalType::Varchar,
        LogicalType::Date,
        BoundCastInfo::varlen(date_casts::varchar_to_date),
    );
    casts.register_bind_function(decimal_casts::bind_decimal_casts);
    casts.register_bind_function(bind_literals);
    let context = Arc::get_mut(&mut session).expect("fresh context");
    context.services = Arc::new(QueryResources {
        infra: context.services.infra.clone(),
        cast_functions: Arc::new(casts),
        graph_index: context.services.graph_index.clone(),
        python_runtime: context.services.python_runtime.clone(),
        governance: context.services.governance.clone(),
        connection_info: context.services.connection_info.clone(),
    });

    let catalog = session.catalog();
    catalog.initialize(false);
    let transaction = CatalogSnapshot::permanent_writer(u64::MAX);
    let schema = catalog
        .get_schema(&transaction, "public")
        .expect("public schema");
    for operator in ["=", "<", "-", "*", "/"] {
        let mut set = ScalarFunctionSet::new(operator.to_string());
        if matches!(operator, "-" | "*" | "/") {
            paro_function::scalar::operators::arithmetic::register_arithmetic_functions(&mut set);
        } else {
            paro_function::scalar::operators::comparison::register_comparison_functions(&mut set);
        }
        schema
            .create_scalar_function(
                &transaction,
                Arc::new(ScalarFunctionCatalogEntry::new(
                    "paro".to_string(),
                    "public".to_string(),
                    set,
                    schema.object_id_allocator().allocate(),
                    0,
                )),
                OnCreateConflict::ReplaceOnConflict,
            )
            .expect("install scalar");
    }
    schema
        .create_scalar_function(
            &transaction,
            Arc::new(ScalarFunctionCatalogEntry::new(
                "paro".to_string(),
                "public".to_string(),
                get_substring_functions(),
                schema.object_id_allocator().allocate(),
                0,
            )),
            OnCreateConflict::ReplaceOnConflict,
        )
        .expect("install substring");
    for function in [
        get_min_function(),
        get_avg_function(),
        get_sum_function(),
        get_count_function(),
    ] {
        schema
            .create_aggregate_function(
                &transaction,
                Arc::new(AggregateFunctionCatalogEntry::new(
                    "paro".to_string(),
                    "public".to_string(),
                    function,
                    schema.object_id_allocator().allocate(),
                    0,
                )),
                OnCreateConflict::ReplaceOnConflict,
            )
            .expect("install aggregate");
    }
    let mut count_star = AggregateFunctionSet::new("count_star".to_string());
    count_star.add_function(get_count_star_function());
    schema
        .create_aggregate_function(
            &transaction,
            Arc::new(AggregateFunctionCatalogEntry::new(
                "paro".to_string(),
                "public".to_string(),
                count_star,
                schema.object_id_allocator().allocate(),
                0,
            )),
            OnCreateConflict::ReplaceOnConflict,
        )
        .expect("install count_star");

    let decimal = LogicalType::Decimal {
        precision: 15,
        scale: 2,
    };
    install_table(
        &schema,
        &transaction,
        "part",
        vec![
            ("p_partkey", LogicalType::BigInt),
            ("p_name", LogicalType::Varchar),
            ("p_mfgr", LogicalType::Varchar),
            ("p_brand", LogicalType::Varchar),
            ("p_type", LogicalType::Varchar),
            ("p_size", LogicalType::Integer),
            ("p_container", LogicalType::Varchar),
            ("p_retailprice", decimal.clone()),
            ("p_comment", LogicalType::Varchar),
        ],
        vec![0],
    );
    install_table(
        &schema,
        &transaction,
        "partsupp",
        vec![
            ("ps_partkey", LogicalType::BigInt),
            ("ps_suppkey", LogicalType::BigInt),
            ("ps_availqty", LogicalType::BigInt),
            ("ps_supplycost", decimal.clone()),
            ("ps_comment", LogicalType::Varchar),
        ],
        vec![0, 1],
    );
    install_table(
        &schema,
        &transaction,
        "supplier",
        vec![
            ("s_suppkey", LogicalType::BigInt),
            ("s_name", LogicalType::Varchar),
            ("s_address", LogicalType::Varchar),
            ("s_nationkey", LogicalType::Integer),
            ("s_phone", LogicalType::Varchar),
            ("s_acctbal", decimal.clone()),
            ("s_comment", LogicalType::Varchar),
        ],
        vec![0],
    );
    install_table(
        &schema,
        &transaction,
        "nation",
        vec![
            ("n_nationkey", LogicalType::Integer),
            ("n_name", LogicalType::Varchar),
            ("n_regionkey", LogicalType::Integer),
            ("n_comment", LogicalType::Varchar),
        ],
        vec![0],
    );
    install_table(
        &schema,
        &transaction,
        "region",
        vec![
            ("r_regionkey", LogicalType::Integer),
            ("r_name", LogicalType::Varchar),
            ("r_comment", LogicalType::Varchar),
        ],
        vec![0],
    );
    install_table(
        &schema,
        &transaction,
        "lineitem",
        vec![
            ("l_orderkey", LogicalType::BigInt),
            ("l_partkey", LogicalType::BigInt),
            ("l_suppkey", LogicalType::BigInt),
            ("l_linenumber", LogicalType::BigInt),
            ("l_quantity", decimal.clone()),
            ("l_extendedprice", decimal.clone()),
            ("l_discount", decimal.clone()),
            ("l_tax", decimal.clone()),
            ("l_returnflag", LogicalType::Varchar),
            ("l_linestatus", LogicalType::Varchar),
            ("l_shipdate", LogicalType::Date),
            ("l_commitdate", LogicalType::Date),
            ("l_receiptdate", LogicalType::Date),
            ("l_shipinstruct", LogicalType::Varchar),
            ("l_shipmode", LogicalType::Varchar),
            ("l_comment", LogicalType::Varchar),
        ],
        vec![0, 3],
    );
    install_table(
        &schema,
        &transaction,
        "customer",
        vec![
            ("c_custkey", LogicalType::BigInt),
            ("c_name", LogicalType::Varchar),
            ("c_address", LogicalType::Varchar),
            ("c_nationkey", LogicalType::Integer),
            ("c_phone", LogicalType::Varchar),
            ("c_acctbal", decimal.clone()),
            ("c_mktsegment", LogicalType::Varchar),
            ("c_comment", LogicalType::Varchar),
        ],
        vec![0],
    );
    install_table(
        &schema,
        &transaction,
        "orders",
        vec![
            ("o_orderkey", LogicalType::BigInt),
            ("o_custkey", LogicalType::BigInt),
            ("o_orderstatus", LogicalType::Varchar),
            ("o_totalprice", decimal.clone()),
            ("o_orderdate", LogicalType::Date),
            ("o_orderpriority", LogicalType::Varchar),
            ("o_clerk", LogicalType::Varchar),
            ("o_shippriority", LogicalType::Integer),
            ("o_comment", LogicalType::Varchar),
        ],
        vec![0],
    );
    session
}

fn bind_literals(
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

fn install_table(
    schema: &paro_catalog::entry::SchemaEntry,
    transaction: &CatalogSnapshot,
    name: &str,
    columns: Vec<(&str, LogicalType)>,
    unique: Vec<usize>,
) {
    install_table_with_constraint(
        schema,
        transaction,
        name,
        columns,
        Constraint::unique(unique),
    );
}

fn install_table_with_constraint(
    schema: &paro_catalog::entry::SchemaEntry,
    transaction: &CatalogSnapshot,
    name: &str,
    columns: Vec<(&str, LogicalType)>,
    constraint: Constraint,
) {
    let definitions = columns
        .into_iter()
        .map(|(name, ty)| ColumnDefinition::new(name.to_string(), ty))
        .collect::<Vec<_>>();
    let storage = Arc::new(
        TableFactory::default()
            .create_table(
                &definitions
                    .iter()
                    .map(|c| c.logical_type.clone())
                    .collect::<Vec<_>>(),
            )
            .expect("storage"),
    );
    let info = CreateTableInfo::new(
        "paro".to_string(),
        "public".to_string(),
        name.to_string(),
        definitions,
    )
    .with_constraints(vec![constraint]);
    let table =
        TableCatalogEntry::from_info(info, storage, schema.object_id_allocator().allocate(), 0)
            .expect("table entry");
    schema
        .create_table(
            transaction,
            Arc::new(table),
            OnCreateConflict::ErrorOnConflict,
        )
        .expect("install table");
}

#[derive(Default)]
struct PlanInspection {
    windows: usize,
    aggregates: usize,
    aggregate_groups: Vec<usize>,
    group_dependencies: usize,
    delim_joins: usize,
    late_fetches: usize,
    late_fetch_sources: usize,
    zero_width_projections: usize,
    gets: std::collections::HashMap<String, usize>,
}

impl PlanInspection {
    fn gets_named(&self, name: &str) -> usize {
        self.gets.get(name).copied().unwrap_or(0)
    }
}

fn inspect_plan(plan: &paro_planner::plan::LogicalPlan) -> PlanInspection {
    fn visit(plan: &paro_planner::plan::LogicalPlan, result: &mut PlanInspection) {
        match &plan.operator {
            LogicalOperator::RowFetch(fetch) => {
                result.late_fetches += 1;
                result.late_fetch_sources += fetch.sources.len();
            }
            LogicalOperator::Window(_) => result.windows += 1,
            LogicalOperator::Projection(projection) if projection.expressions.is_empty() => {
                result.zero_width_projections += 1;
            }
            LogicalOperator::Aggregate(aggregate) => {
                result.aggregates += 1;
                result.aggregate_groups.push(aggregate.groups.len());
                result.group_dependencies += aggregate.group_dependencies.len();
            }
            LogicalOperator::Join(paro_planner::operator::Join::Comparison(join))
                if !join.duplicate_eliminated_columns.is_empty() =>
            {
                result.delim_joins += 1;
            }
            LogicalOperator::Get(get) => {
                if let Some(table) = &get.table {
                    *result.gets.entry(table.base.base.name.clone()).or_default() += 1;
                }
            }
            _ => {}
        }
        for child in plan.children() {
            visit(child, result);
        }
    }

    let mut result = PlanInspection::default();
    visit(plan, &mut result);
    result
}

#[test]
fn small_customer_payload_declines_late_fetch_without_losing_bindings() {
    let session = setup_session();
    for (table_name, values) in [
        (
            "customer",
            vec![
                paro_common::runtime_value::Value::BigInt(1),
                paro_common::runtime_value::Value::Varchar("Customer#1".to_string()),
                paro_common::runtime_value::Value::Varchar("Address".to_string()),
                paro_common::runtime_value::Value::Integer(1),
                paro_common::runtime_value::Value::Varchar("Phone".to_string()),
                paro_common::runtime_value::Value::Decimal(100, 15, 2),
                paro_common::runtime_value::Value::Varchar("BUILDING".to_string()),
                paro_common::runtime_value::Value::Varchar("Comment".to_string()),
            ],
        ),
        (
            "orders",
            vec![
                paro_common::runtime_value::Value::BigInt(1),
                paro_common::runtime_value::Value::BigInt(1),
                paro_common::runtime_value::Value::Varchar("O".to_string()),
                paro_common::runtime_value::Value::Decimal(100, 15, 2),
                paro_common::runtime_value::Value::Date(8_674),
                paro_common::runtime_value::Value::Varchar("1-URGENT".to_string()),
                paro_common::runtime_value::Value::Varchar("Clerk#1".to_string()),
                paro_common::runtime_value::Value::Integer(0),
                paro_common::runtime_value::Value::Varchar("Comment".to_string()),
            ],
        ),
        (
            "nation",
            vec![
                paro_common::runtime_value::Value::Integer(1),
                paro_common::runtime_value::Value::Varchar("NATION".to_string()),
                paro_common::runtime_value::Value::Integer(1),
                paro_common::runtime_value::Value::Varchar("Comment".to_string()),
            ],
        ),
        (
            "lineitem",
            vec![
                paro_common::runtime_value::Value::BigInt(1),
                paro_common::runtime_value::Value::BigInt(1),
                paro_common::runtime_value::Value::BigInt(1),
                paro_common::runtime_value::Value::BigInt(1),
                paro_common::runtime_value::Value::Decimal(100, 15, 2),
                paro_common::runtime_value::Value::Decimal(100, 15, 2),
                paro_common::runtime_value::Value::Decimal(5, 15, 2),
                paro_common::runtime_value::Value::Decimal(0, 15, 2),
                paro_common::runtime_value::Value::Varchar("R".to_string()),
                paro_common::runtime_value::Value::Varchar("F".to_string()),
                paro_common::runtime_value::Value::Date(8_674),
                paro_common::runtime_value::Value::Date(8_674),
                paro_common::runtime_value::Value::Date(8_674),
                paro_common::runtime_value::Value::Varchar("DELIVER IN PERSON".to_string()),
                paro_common::runtime_value::Value::Varchar("AIR".to_string()),
                paro_common::runtime_value::Value::Varchar("Comment".to_string()),
            ],
        ),
    ] {
        append_catalog_row(&session, table_name, values);
    }
    let statement = paro_parser::parse_one(include_str!(
        "../../../../benchmark/workloads/tpch/sql/q10.sql"
    ))
    .expect("parse q10 shape")
    .stmt;
    let mut planner = Planner::new(session.clone());
    planner.create_plan(statement).expect("plan q10 shape");
    let planned = planner.take_plan().expect("logical q10 shape");
    let mut optimizer = Optimizer::new(planner.binder.clone(), session);
    let optimized = optimizer.optimize(planned).expect("optimize q10 shape");
    assert_eq!(
        inspect_plan(&optimized).late_fetches,
        0,
        "the one-row fixture must not force an unprofitable late fetch: {optimized:#?}"
    );
    crate::verify::verify_logical_plan(&planner.binder.bind_context, &optimized)
        .expect("verify q10 late payload bindings");
}

fn append_catalog_row(
    session: &paro_context::StatementContext,
    table_name: &str,
    values: Vec<paro_common::runtime_value::Value>,
) {
    let transaction = CatalogSnapshot::permanent_writer(u64::MAX);
    let table = session
        .catalog()
        .get_schema(&transaction, "public")
        .expect("public schema")
        .get_table(
            transaction.transaction_id,
            transaction.start_time,
            table_name,
        )
        .expect("test table");
    let CatalogEntryEnum::Table(table) = table.as_ref() else {
        panic!("expected table entry")
    };
    let storage = table.get_storage().expect("test storage");
    let mut vectors = values
        .iter()
        .zip(storage.types())
        .map(|(value, ty)| {
            let mut vector = paro_common::test_utils::test_vector_with_capacity(ty.clone(), 1);
            vector.set_value(0, value);
            vector.set_count(1);
            vector
        })
        .collect::<Vec<_>>();
    let mut chunk = paro_common::chunk::Chunk::from_vectors(
        std::mem::take(&mut vectors),
        paro_common::test_utils::test_allocator(),
    );
    chunk.try_set_cardinality(1).expect("row cardinality");
    storage.append(&chunk).expect("append test row");
}

#[test]
fn unique_dimension_key_other_than_partition_key_does_not_rewrite() {
    let session = setup_session();
    let sql = "SELECT l.l_partkey \
               FROM lineitem AS l, part AS p \
               WHERE p.p_partkey = l.l_suppkey \
                 AND p.p_brand = 'Brand#23' \
                 AND l.l_quantity < ( \
                     SELECT avg(i.l_quantity) \
                     FROM lineitem AS i \
                     WHERE i.l_partkey = l.l_partkey)";
    let statement = paro_parser::parse_one(sql)
        .expect("parse negative case")
        .stmt;
    let mut planner = Planner::new(session.clone());
    planner.create_plan(statement).expect("plan negative case");
    let planned = planner.take_plan().expect("logical negative plan");
    let mut optimizer = Optimizer::new(planner.binder.clone(), session);
    let optimized = optimizer.optimize(planned).expect("optimize negative case");
    let inspection = inspect_plan(&optimized);

    assert_eq!(
        inspection.windows, 0,
        "unsafe partial partition: {optimized:#?}"
    );
    assert_eq!(inspection.gets_named("lineitem"), 2, "{optimized:#?}");
}

#[test]
fn nullable_correlation_without_keyed_dimension_does_not_rewrite() {
    let optimized = optimize_sql(
        "SELECT l.l_partkey \
         FROM lineitem AS l \
         WHERE l.l_quantity < ( \
             SELECT avg(i.l_quantity) \
             FROM lineitem AS i \
             WHERE i.l_partkey = l.l_partkey)",
    );
    let inspection = inspect_plan(&optimized);

    assert_eq!(
        inspection.windows, 0,
        "nullable correlation: {optimized:#?}"
    );
    assert_eq!(inspection.gets_named("lineitem"), 2, "{optimized:#?}");
}

#[test]
fn extra_dimension_residual_does_not_rewrite() {
    let optimized = optimize_sql(
        "SELECT l.l_partkey \
         FROM lineitem AS l, part AS p \
         WHERE p.p_partkey = l.l_partkey \
           AND p.p_brand IS NOT DISTINCT FROM l.l_returnflag \
           AND l.l_quantity < ( \
               SELECT avg(i.l_quantity) \
               FROM lineitem AS i \
               WHERE i.l_partkey = l.l_partkey)",
    );
    let inspection = inspect_plan(&optimized);

    assert_eq!(inspection.windows, 0, "dimension residual: {optimized:#?}");
    assert_eq!(inspection.gets_named("lineitem"), 2, "{optimized:#?}");
}

#[test]
fn scalar_binding_visible_above_filter_does_not_rewrite() {
    let session = setup_session();
    let statement = paro_parser::parse_one(
        "SELECT l.l_partkey \
         FROM lineitem AS l, part AS p \
         WHERE p.p_partkey = l.l_partkey \
           AND p.p_brand = 'Brand#23' \
           AND l.l_quantity < ( \
               SELECT avg(i.l_quantity) \
               FROM lineitem AS i \
               WHERE i.l_partkey = l.l_partkey)",
    )
    .expect("parse binding escape")
    .stmt;
    let mut planner = Planner::new(session);
    planner.create_plan(statement).expect("plan binding escape");
    let mut plan = planner.take_plan().expect("logical binding escape plan");

    let scalar_binding = find_single_scalar_binding(&plan).expect("correlated scalar binding");
    let scalar_type = find_binding_type(&plan, scalar_binding).expect("scalar type");
    let parent_index = planner.binder.bind_context.generate_table_index();
    plan = paro_planner::plan::LogicalPlan::new(
        &planner.binder.bind_context,
        LogicalOperator::Projection(Projection::new(
            parent_index,
            plan,
            vec![Expression::ColumnRef(ColumnRefExpression::new(
                scalar_binding,
                scalar_type,
            ))],
        )),
    );

    let rewritten = CorrelatedPartitionAggregate::new(planner.binder.bind_context.clone())
        .optimize_plan(plan)
        .expect("rewrite binding-escape plan");
    let inspection = inspect_plan(&rewritten);
    assert_eq!(
        inspection.windows, 0,
        "escaping scalar binding: {rewritten:#?}"
    );
    assert_eq!(inspection.delim_joins, 1, "{rewritten:#?}");
}

fn find_single_scalar_binding(plan: &paro_planner::plan::LogicalPlan) -> Option<ColumnBinding> {
    if let LogicalOperator::Join(paro_planner::operator::Join::Comparison(join)) = &plan.operator {
        if join.join_type == paro_planner::operator::JoinType::Single {
            return join.right.get_column_bindings().first().copied();
        }
    }
    plan.children()
        .into_iter()
        .find_map(find_single_scalar_binding)
}

fn find_binding_type(
    plan: &paro_planner::plan::LogicalPlan,
    binding: ColumnBinding,
) -> Option<LogicalType> {
    plan.get_column_bindings()
        .into_iter()
        .zip(plan.types())
        .find_map(|(candidate, ty)| (candidate == binding).then_some(ty))
        .or_else(|| {
            plan.children()
                .into_iter()
                .find_map(|child| find_binding_type(child, binding))
        })
}

fn optimize_sql(sql: &str) -> paro_planner::plan::LogicalPlan {
    let session = setup_session();
    let statement = paro_parser::parse_one(sql)
        .expect("parse negative case")
        .stmt;
    let mut planner = Planner::new(session.clone());
    planner.create_plan(statement).expect("plan negative case");
    let planned = planner.take_plan().expect("logical negative plan");
    let mut optimizer = Optimizer::new(planner.binder.clone(), session);
    optimizer.optimize(planned).expect("optimize negative case")
}
