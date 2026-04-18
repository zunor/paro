// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Registration for built-in table functions.

use paro_catalog::collection::InstallMode;
use paro_catalog::entry::{CatalogEntryEnum, CatalogType, SchemaEntry, TableFunctionCatalogEntry};
use paro_function::table::range::{create_generate_series_function_set, create_range_function_set};
use paro_function::table::read_csv::create_read_csv_function_set;
use paro_function::table::read_ndjson::create_read_ndjson_function_set;
use paro_function::table::repeat::{create_repeat_function_set, create_repeat_row_function_set};
use paro_function::table::system::{
    create_paro_columns_function_set, create_paro_databases_function_set,
    create_paro_indexes_function_set, create_paro_logs_function_set,
    create_paro_optimizers_function_set, create_paro_schemas_function_set,
    create_paro_storage_info_function_set, create_paro_tables_function_set,
    create_paro_views_function_set, create_paro_wal_metrics_function_set,
};
use paro_function::table::unnest::create_unnest_function_set;
use paro_function::table::TableFunctionSet;
use std::sync::Arc;

/// Registers built-in table functions.
pub struct BuiltinTableFunctions;

impl BuiltinTableFunctions {
    /// Register all built-in table functions into the given schema.
    ///
    /// This should be called during Catalog initialization after the schema
    /// is created.
    ///
    /// # Arguments
    /// * `schema` - The schema to register functions into (typically "public")
    pub fn register_all(schema: &SchemaEntry) {
        Self::register_range_functions(schema);
        Self::register_generate_series_functions(schema);
        Self::register_unnest_functions(schema);
        Self::register_repeat_functions(schema);
        Self::register_read_csv_functions(schema);
        Self::register_read_ndjson_functions(schema);

        Self::register_system_functions(schema);
    }

    /// Returns every built-in table function set without registering it.
    pub fn get_all_table_function_sets() -> Vec<TableFunctionSet> {
        vec![
            // Core table functions
            create_range_function_set(),
            create_generate_series_function_set(),
            create_unnest_function_set(),
            create_repeat_function_set(),
            create_repeat_row_function_set(),
            create_read_csv_function_set(),
            create_read_ndjson_function_set(),
            // System table functions
            create_paro_databases_function_set(),
            create_paro_schemas_function_set(),
            create_paro_tables_function_set(),
            create_paro_columns_function_set(),
            create_paro_views_function_set(),
            create_paro_indexes_function_set(),
            create_paro_logs_function_set(),
            create_paro_optimizers_function_set(),
            create_paro_storage_info_function_set(),
            create_paro_wal_metrics_function_set(),
        ]
    }

    /// Register range table functions.
    ///
    /// Variants:
    /// - `range(end)` - Generate [0, end)
    /// - `range(start, end)` - Generate [start, end)
    /// - `range(start, end, step)` - Generate [start, end) with step
    fn register_range_functions(schema: &SchemaEntry) {
        let set = create_range_function_set();
        Self::register_set(schema, set);
    }

    /// Register generate_series table functions.
    ///
    /// Variants:
    /// - `generate_series(start, end)` - Generate [start, end] (inclusive)
    /// - `generate_series(start, end, step)` - Generate [start, end] with step
    fn register_generate_series_functions(schema: &SchemaEntry) {
        let set = create_generate_series_function_set();
        Self::register_set(schema, set);
    }

    /// Register unnest table function.
    ///
    /// Variants:
    /// - `unnest(list)` - Expand a list into rows
    fn register_unnest_functions(schema: &SchemaEntry) {
        let set = create_unnest_function_set();
        Self::register_set(schema, set);
    }

    /// Register repeat table functions.
    ///
    /// Variants:
    /// - `repeat(value, count)` - Repeat a value count times
    /// - `repeat_row(value1, value2, ..., num_rows => count)` - Repeat a row count times
    fn register_repeat_functions(schema: &SchemaEntry) {
        let repeat_set = create_repeat_function_set();
        Self::register_set(schema, repeat_set);

        let repeat_row_set = create_repeat_row_function_set();
        Self::register_set(schema, repeat_row_set);
    }

    /// Register read_csv table function.
    fn register_read_csv_functions(schema: &SchemaEntry) {
        let set = create_read_csv_function_set();
        Self::register_set(schema, set);
    }

    /// Register read_ndjson table function.
    fn register_read_ndjson_functions(schema: &SchemaEntry) {
        let set = create_read_ndjson_function_set();
        Self::register_set(schema, set);
    }

    /// Register system table functions.
    ///
    /// System functions provide access to catalog metadata:
    /// - `paro_logs()` - Query in-memory logs
    fn register_system_functions(schema: &SchemaEntry) {
        let paro_databases_set = create_paro_databases_function_set();
        Self::register_set(schema, paro_databases_set);

        let paro_schemas_set = create_paro_schemas_function_set();
        Self::register_set(schema, paro_schemas_set);

        let paro_tables_set = create_paro_tables_function_set();
        Self::register_set(schema, paro_tables_set);

        let paro_columns_set = create_paro_columns_function_set();
        Self::register_set(schema, paro_columns_set);

        let paro_views_set = create_paro_views_function_set();
        Self::register_set(schema, paro_views_set);

        let paro_indexes_set = create_paro_indexes_function_set();
        Self::register_set(schema, paro_indexes_set);

        // paro_logs() - Query in-memory logs
        let paro_logs_set = create_paro_logs_function_set();
        Self::register_set(schema, paro_logs_set);

        let paro_optimizers_set = create_paro_optimizers_function_set();
        Self::register_set(schema, paro_optimizers_set);

        let paro_storage_info_set = create_paro_storage_info_function_set();
        Self::register_set(schema, paro_storage_info_set);

        let paro_wal_metrics_set = create_paro_wal_metrics_function_set();
        Self::register_set(schema, paro_wal_metrics_set);
    }

    /// Register a table function set into the schema.
    ///
    /// Table functions are registered in the `table_functions` CatalogCollection,
    /// which is separate from the `functions` CatalogCollection used for scalar
    /// and aggregate functions.
    fn register_set(schema: &SchemaEntry, set: TableFunctionSet) {
        let entry = Arc::new(TableFunctionCatalogEntry::new(
            schema.base.catalog.clone(),
            schema.base.name.clone(),
            set,
            0, // timestamp 0 = permanent/committed
        ));
        let _ = schema
            .collection(CatalogType::TableFunction)
            .expect("table function collection")
            .install_committed(
                Arc::new(CatalogEntryEnum::TableFunction(entry)),
                InstallMode::RejectExisting,
            );
    }
}
