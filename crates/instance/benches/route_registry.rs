// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use divan::{black_box, Bencher};
use paro_catalog::catalog::Catalog;
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::ColumnDefinition;
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::ddl::{DdlObjectKey, DdlObjectKind};
use paro_common::types::LogicalType;
use paro_instance::RouteRegistry;
use paro_storage::table::table_factory::TableFactory;

const BOOTSTRAP_TABLE_COUNTS: [usize; 2] = [64, 256];
const INCREMENTAL_UPDATE_ARGS: [(usize, usize); 3] = [(64, 1), (64, 8), (256, 8)];

fn main() {
    divan::main();
}

struct RouteRegistryBenchState {
    catalog: Arc<ParoCatalog>,
    keys: Vec<DdlObjectKey>,
    base_registry: RouteRegistry,
}

impl RouteRegistryBenchState {
    fn new(table_count: usize) -> Self {
        let catalog = Arc::new(ParoCatalog::new("bench".to_string()));
        catalog.initialize(false);

        let committed = CatalogSnapshot::permanent_writer(u64::MAX);
        let mut keys = Vec::with_capacity(table_count);
        for idx in 0..table_count {
            let table_name = format!("route_registry_bench_{idx}");
            let storage = Arc::new(
                TableFactory::default()
                    .create_table(&[LogicalType::Integer])
                    .expect("create benchmark table storage"),
            );
            catalog
                .create_table_in_snapshot(
                    &committed,
                    "public",
                    &table_name,
                    vec![ColumnDefinition {
                        name: "id".to_string(),
                        logical_type: LogicalType::Integer,
                        not_null: false,
                        default_value: None,
                        comment: None,
                    }],
                    storage,
                )
                .expect("register benchmark table");
            keys.push(DdlObjectKey::new(
                catalog.name(),
                Some("public".to_string()),
                table_name,
                DdlObjectKind::Table,
            ));
        }

        let base_registry =
            RouteRegistry::from_catalog(&catalog).expect("bootstrap route registry");
        Self {
            catalog,
            keys,
            base_registry,
        }
    }
}

#[divan::bench(args = BOOTSTRAP_TABLE_COUNTS, sample_count = 10)]
fn route_registry_bootstrap_benchmark(bencher: Bencher, table_count: usize) {
    let state = RouteRegistryBenchState::new(table_count);
    bencher.counter(table_count).bench_local(|| {
        let registry = RouteRegistry::from_catalog(&state.catalog).unwrap();
        black_box(registry);
    });
}

#[divan::bench(args = INCREMENTAL_UPDATE_ARGS, sample_count = 10)]
fn route_registry_incremental_update_benchmark(
    bencher: Bencher,
    (table_count, changed_objects): (usize, usize),
) {
    let state = RouteRegistryBenchState::new(table_count);
    let selected = state
        .keys
        .iter()
        .take(changed_objects)
        .cloned()
        .collect::<Vec<_>>();
    bencher.counter(changed_objects).bench_local(|| {
        let mut registry = state.base_registry.clone();
        for key in &selected {
            registry
                .sync_table_from_catalog(&state.catalog, key)
                .unwrap();
        }
        black_box(registry);
    });
}
