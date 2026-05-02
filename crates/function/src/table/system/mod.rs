// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! System Table Functions
//!
//!
//!
//! ## Overview
//! System table functions provide access to catalog metadata.
//! These functions return information about schemas, tables, columns, views, etc.
//!
//! ## Supported Functions
//! - `paro_databases()` - List all databases ✅
//! - `paro_schemas()` - List all schemas ✅
//! - `paro_tables()` - List all tables ✅
//! - `paro_columns()` - List all columns ✅
//! - `paro_views()` - List all views ✅
//! - `paro_indexes()` - List all indexes ✅
//! - `paro_logs()` - Query in-memory logs ✅
//! - `paro_property_graphs()` - List all property graphs ✅
//! - `paro_graph_statistics(graph_name)` - Graph statistics ✅
//! - `paro_optimizers()` - Optimizer profile snapshot ✅
//! - `paro_storage_info(table)` - Segment/column storage observability ✅
//! - `paro_wal_metrics()` - WAL/apply queue observability ✅
//! - `paro_transaction_metrics()` - Transaction pipeline observability ✅
//! - `paro_commit_frontiers()` - Commit frontier observability ✅
//! - `paro_commit_poison()` - Commit poison/admission observability ✅
//!
//! ## Dependencies Check
//! - Catalog: ✅ `paro_catalog`
//! - TableFunction: ✅ `crate::table`

mod memory_runtime;
pub mod paro_columns;
pub mod paro_commit_frontiers;
pub mod paro_commit_poison;
pub mod paro_databases;
pub mod paro_graph_statistics;
pub mod paro_indexes;
pub mod paro_logs;
pub mod paro_memory;
pub mod paro_optimizers;
pub mod paro_pg_cursors;
pub mod paro_pg_prepared_statements;
pub mod paro_pg_settings;
pub mod paro_property_graphs;
pub mod paro_schemas;
pub mod paro_storage_info;
pub mod paro_tables;
pub mod paro_temporary_files;
pub mod paro_transaction_metrics;
pub mod paro_views;
pub mod paro_wal_metrics;
pub mod pragma_database_size;

pub use memory_runtime::{get_system_buffer_manager, register_system_buffer_manager};
pub use paro_columns::create_paro_columns_function_set;
pub use paro_commit_frontiers::create_paro_commit_frontiers_function_set;
pub use paro_commit_poison::create_paro_commit_poison_function_set;
pub use paro_databases::create_paro_databases_function_set;
pub use paro_graph_statistics::create_paro_graph_statistics_function_set;
pub use paro_indexes::create_paro_indexes_function_set;
pub use paro_logs::{create_paro_logs_function_set, get_log_storage, register_log_storage};
pub use paro_memory::create_paro_memory_function_set;
pub use paro_optimizers::create_paro_optimizers_function_set;
pub use paro_pg_cursors::create_paro_pg_cursors_function_set;
pub use paro_pg_prepared_statements::create_paro_pg_prepared_statements_function_set;
pub use paro_pg_settings::create_paro_pg_settings_function_set;
pub use paro_property_graphs::create_paro_property_graphs_function_set;
pub use paro_schemas::create_paro_schemas_function_set;
pub use paro_storage_info::create_paro_storage_info_function_set;
pub use paro_tables::create_paro_tables_function_set;
pub use paro_temporary_files::create_paro_temporary_files_function_set;
pub use paro_transaction_metrics::create_paro_transaction_metrics_function_set;
pub use paro_views::create_paro_views_function_set;
pub use paro_wal_metrics::create_paro_wal_metrics_function_set;
pub use pragma_database_size::create_pragma_database_size_function_set;
