// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::{CatalogEntryEnum, CatalogType};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::logging::targets;
use std::collections::HashMap;
use std::sync::Arc;

/// Per-table consistency result emitted after WAL recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryTableConsistencyReport {
    pub schema_name: String,
    pub table_name: String,
    pub has_storage: bool,
    pub tablet_id: Option<u64>,
    pub rowset_count: Option<usize>,
    pub max_version: Option<i64>,
    pub catalog_index_count: usize,
    pub runtime_index_count: Option<usize>,
    pub version_graph_ok: bool,
    pub primary_index_reconciled: bool,
    pub errors: Vec<String>,
}

/// Aggregated recovery consistency report across catalog/table runtime state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryConsistencyReport {
    pub schema_count: usize,
    pub table_count: usize,
    pub catalog_index_count: usize,
    pub consistent_tables: usize,
    pub inconsistent_tables: usize,
    pub all_consistent: bool,
    pub tables: Vec<RecoveryTableConsistencyReport>,
}

/// Build a post-recovery consistency report for rowset/version/index/catalog reconciliation.
pub fn build_recovery_consistency_report(catalog: &Arc<ParoCatalog>) -> RecoveryConsistencyReport {
    let txn = CatalogSnapshot::read_only(u64::MAX);
    let schemas = catalog
        .get_schema_collection()
        .scan(txn.transaction_id, txn.start_time);

    let mut schema_count = 0usize;
    let mut table_count = 0usize;
    let mut catalog_index_count = 0usize;
    let mut tables = Vec::new();
    let mut consistent_tables = 0usize;
    let mut inconsistent_tables = 0usize;

    for schema_entry in schemas {
        let CatalogEntryEnum::Schema(schema) = schema_entry.as_ref() else {
            continue;
        };
        schema_count += 1;

        let mut index_count_by_table: HashMap<String, usize> = HashMap::new();
        for index_entry in schema
            .collection(CatalogType::Index)
            .expect("index collection")
            .scan(txn.transaction_id, txn.start_time)
        {
            let CatalogEntryEnum::Index(index) = index_entry.as_ref() else {
                continue;
            };
            catalog_index_count += 1;
            *index_count_by_table
                .entry(index.table_name.clone())
                .or_insert(0usize) += 1;
        }

        let table_entries = schema
            .collection(CatalogType::Table)
            .expect("table collection")
            .scan(txn.transaction_id, txn.start_time);
        for table_entry in table_entries {
            let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
                continue;
            };
            table_count += 1;

            let mut report = RecoveryTableConsistencyReport {
                schema_name: schema.base.name.clone(),
                table_name: table.base.base.name.clone(),
                has_storage: false,
                tablet_id: None,
                rowset_count: None,
                max_version: None,
                catalog_index_count: index_count_by_table
                    .get(&table.base.base.name)
                    .copied()
                    .unwrap_or(0),
                runtime_index_count: None,
                version_graph_ok: true,
                primary_index_reconciled: true,
                errors: Vec::new(),
            };

            if let Some(storage) = table.get_storage() {
                report.has_storage = true;
                report.tablet_id = Some(storage.tablet_id());
                report.rowset_count = Some(storage.rowset_count());
                report.max_version = Some(storage.max_version());
                let runtime_index_count = storage.recovery_index_count();
                report.runtime_index_count = Some(runtime_index_count);

                if let Err(err) = storage.validate_version_graph() {
                    report.version_graph_ok = false;
                    report
                        .errors
                        .push(format!("version graph check failed: {}", err));
                }

                if let Err(err) = storage.reconcile_primary_index_row_count() {
                    report.primary_index_reconciled = false;
                    report
                        .errors
                        .push(format!("primary index reconcile failed: {}", err));
                }

                if report.catalog_index_count != runtime_index_count {
                    report.errors.push(format!(
                        "index count mismatch: catalog={} runtime={}",
                        report.catalog_index_count, runtime_index_count
                    ));
                }
            } else {
                report
                    .errors
                    .push("table storage missing after WAL recovery".to_string());
            }

            if report.errors.is_empty() {
                consistent_tables += 1;
            } else {
                inconsistent_tables += 1;
            }

            tables.push(report);
        }
    }

    RecoveryConsistencyReport {
        schema_count,
        table_count,
        catalog_index_count,
        consistent_tables,
        inconsistent_tables,
        all_consistent: inconsistent_tables == 0,
        tables,
    }
}

pub(crate) fn log_recovery_consistency_report(report: &RecoveryConsistencyReport) {
    tracing::info!(
        target: targets::INSTANCE,
        schema_count = report.schema_count,
        table_count = report.table_count,
        catalog_index_count = report.catalog_index_count,
        consistent_tables = report.consistent_tables,
        inconsistent_tables = report.inconsistent_tables,
        all_consistent = report.all_consistent,
        "WAL recovery consistency report"
    );

    for table in &report.tables {
        if table.errors.is_empty() {
            tracing::debug!(
                target: targets::INSTANCE,
                schema = %table.schema_name,
                table = %table.table_name,
                tablet_id = table.tablet_id,
                rowset_count = table.rowset_count,
                max_version = table.max_version,
                catalog_index_count = table.catalog_index_count,
                runtime_index_count = table.runtime_index_count,
                "WAL recovery consistency table check passed"
            );
        } else {
            tracing::warn!(
                target: targets::INSTANCE,
                schema = %table.schema_name,
                table = %table.table_name,
                tablet_id = table.tablet_id,
                rowset_count = table.rowset_count,
                max_version = table.max_version,
                catalog_index_count = table.catalog_index_count,
                runtime_index_count = table.runtime_index_count,
                errors = ?table.errors,
                "WAL recovery consistency table check failed"
            );
        }
    }
}
