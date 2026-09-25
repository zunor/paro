// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_catalog::catalog::Catalog;
use paro_catalog::entry::{
    AggregateFunctionCatalogEntry, ColumnDefinition, Constraint, CreateTableInfo, OnCreateConflict,
    ScalarFunctionCatalogEntry, TableCatalogEntry,
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
use paro_storage::table::table_factory::TableFactory;

pub(crate) fn setup_session() -> Arc<paro_context::StatementContext> {
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
