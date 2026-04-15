use std::sync::Arc;

use paro_catalog::catalog::Catalog;
use paro_catalog::entry::{ColumnDefinition, OnCreateConflict, TableCatalogEntry};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_catalog::search_path::CatalogSearchEntry;
use paro_common::types::LogicalType;
use paro_context::{test_support::TestStatementContextBuilder, QueryResources, StatementContext};
use paro_function::scalar::cast::CastFunctionSet;
use paro_instance::builtin::casts::BuiltinCasts;
use paro_storage::table::table_factory::TableFactory;

use crate::binder::Binder;

pub(crate) fn test_session(search_path: Vec<CatalogSearchEntry>) -> Arc<StatementContext> {
    let base = TestStatementContextBuilder::minimal()
        .with_current_database("paro")
        .with_search_path(search_path)
        .build();
    base.catalog().initialize(false);

    let mut cast_functions = CastFunctionSet::new();
    BuiltinCasts::register_all(&mut cast_functions);

    Arc::new(StatementContext {
        env: base.env.clone(),
        txn: base.txn.clone(),
        ddl: base.ddl.clone(),
        settings: base.settings.clone(),
        options: base.options.clone(),
        databases: base.databases.clone(),
        limits: base.limits.clone(),
        cancellation: base.cancellation.clone(),
        services: Arc::new(QueryResources {
            infra: base.services.infra.clone(),
            cast_functions: Arc::new(cast_functions),
            graph_index: base.services.graph_index.clone(),
            governance: base.services.governance.clone(),
            plan_cache: base.services.plan_cache.clone(),
            connection_info: base.services.connection_info.clone(),
        }),
        graph_registry: base.graph_registry.clone(),
        session_metadata: base.session_metadata.clone(),
    })
}

pub(crate) fn test_binder() -> Binder {
    Binder::new(test_session(Vec::new()))
}

pub(crate) fn test_binder_with_search_path(search_path: Vec<CatalogSearchEntry>) -> Binder {
    Binder::new(test_session(search_path))
}

pub(crate) fn test_binder_with_public_table(
    table_name: &str,
    columns: &[(&str, LogicalType)],
) -> Binder {
    let binder = test_binder();
    let catalog = binder.catalog();
    let txn = CatalogSnapshot::permanent_writer(u64::MAX);
    let schema = catalog
        .get_schema(&txn, "public")
        .expect("public schema should exist in test catalog");
    let storage_types = columns
        .iter()
        .map(|(_name, ty)| ty.clone())
        .collect::<Vec<_>>();
    let storage = Arc::new(
        TableFactory::default()
            .create_table(&storage_types)
            .expect("create test table storage"),
    );
    let entry = Arc::new(TableCatalogEntry::new(
        catalog.name().to_string(),
        "public".to_string(),
        table_name.to_string(),
        columns
            .iter()
            .map(|(name, ty)| ColumnDefinition::new((*name).to_string(), ty.clone()))
            .collect(),
        storage,
        0,
    ));
    schema
        .create_table(&txn, entry, OnCreateConflict::ErrorOnConflict)
        .expect("install test table into catalog");
    binder
}
