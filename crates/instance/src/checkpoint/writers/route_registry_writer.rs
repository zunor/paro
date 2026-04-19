// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::checkpoint::view::CheckpointView;
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::{CatalogEntry, CatalogEntryEnum, CatalogType};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::checkpoint::{RouteRegistryBundle, RouteRegistryEntry};

/// Snapshots catalog-visible table routing metadata into a stable checkpoint
/// bundle.
#[derive(Debug, Default)]
pub struct RouteRegistryWriter;

impl RouteRegistryWriter {
    pub fn serialize_view(
        catalog: &ParoCatalog,
        view: &CheckpointView,
    ) -> anyhow::Result<RouteRegistryBundle> {
        let txn = CatalogSnapshot::read_only(view.catalog_snapshot_ts);
        let mut entries = Vec::new();

        for schema_entry in catalog
            .get_schema_collection()
            .scan(txn.transaction_id, txn.start_time)
        {
            let CatalogEntryEnum::Schema(schema) = schema_entry.as_ref() else {
                continue;
            };
            let Some(tables) = schema.collection(CatalogType::Table) else {
                continue;
            };
            for table_entry in tables.scan(txn.transaction_id, txn.start_time) {
                let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
                    continue;
                };
                let descriptor = table
                    .get_storage_descriptor()
                    .cloned()
                    .or_else(|| {
                        table
                            .get_storage()
                            .and_then(|storage| storage.to_descriptor().ok())
                    })
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "table {}.{} is visible at checkpoint but has no storage descriptor",
                            schema.base.name,
                            table.name()
                        )
                    })?;

                entries.push(RouteRegistryEntry {
                    schema_name: schema.base.name.clone(),
                    table_name: table.name().to_string(),
                    table_object_id: table.object_id().raw(),
                    tablet_id: descriptor.tablet_id,
                    storage_descriptor: descriptor.serialize().map_err(anyhow::Error::from)?,
                });
            }
        }

        entries.sort_by(|left, right| {
            left.schema_name
                .cmp(&right.schema_name)
                .then(left.table_name.cmp(&right.table_name))
                .then(left.tablet_id.cmp(&right.tablet_id))
        });

        Ok(RouteRegistryBundle { entries })
    }
}

#[cfg(test)]
mod tests {
    use super::RouteRegistryWriter;
    use crate::checkpoint::view::{CheckpointCut, CheckpointView};
    use paro_catalog::catalog::Catalog;
    use paro_catalog::collection::InstallMode;
    use paro_catalog::database_catalog::ParoCatalog;
    use paro_catalog::entry::{
        CatalogEntryEnum, CatalogType, ColumnDefinition, CreateTableInfo, TableCatalogEntry,
    };
    use paro_catalog::mvcc::CatalogSnapshot;
    use paro_common::checkpoint::{CheckpointFrontier, RecoverySummary};
    use paro_common::types::LogicalType;
    use paro_storage::table::table_factory::TableFactory;
    use std::sync::Arc;

    fn aligned_view() -> CheckpointView {
        CheckpointView::new(
            CheckpointCut {
                target_lsn: 4,
                issued_at_micros: 1,
            },
            CheckpointFrontier {
                checkpoint_lsn: 4,
                checkpoint_commit_id: 10,
                checkpoint_maintenance_id: 0,
            },
            RecoverySummary {
                max_lsn: 4,
                max_commit_id: 10,
                max_maintenance_id: 0,
                max_catalog_commit_id: 10,
                max_seen_object_id: 200,
            },
            11,
        )
        .expect("checkpoint view should be aligned")
    }

    #[test]
    fn serialize_view_excludes_post_cut_tables_from_route_registry() {
        let catalog = ParoCatalog::new("test_db".to_string());
        catalog.initialize(false);
        let read_txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog
            .get_schema(&read_txn, "public")
            .expect("public schema");
        let tables = schema
            .collection(CatalogType::Table)
            .expect("table collection");

        let before_cut_storage = Arc::new(
            TableFactory::default()
                .create_table(&[LogicalType::Integer])
                .expect("create before-cut table"),
        );
        tables
            .install_replayed(
                10,
                Arc::new(CatalogEntryEnum::Table(Arc::new(
                    TableCatalogEntry::from_info(
                        CreateTableInfo::new(
                            catalog.name().to_string(),
                            "public".to_string(),
                            "before_cut".to_string(),
                            vec![ColumnDefinition::new(
                                "id".to_string(),
                                LogicalType::Integer,
                            )],
                        ),
                        before_cut_storage.clone(),
                        10,
                    ),
                ))),
                InstallMode::RejectExisting,
            )
            .expect("install before-cut table");

        let after_cut_storage = Arc::new(
            TableFactory::default()
                .create_table(&[LogicalType::Integer])
                .expect("create after-cut table"),
        );
        tables
            .install_replayed(
                11,
                Arc::new(CatalogEntryEnum::Table(Arc::new(
                    TableCatalogEntry::from_info(
                        CreateTableInfo::new(
                            catalog.name().to_string(),
                            "public".to_string(),
                            "after_cut".to_string(),
                            vec![ColumnDefinition::new(
                                "id".to_string(),
                                LogicalType::Integer,
                            )],
                        ),
                        after_cut_storage,
                        11,
                    ),
                ))),
                InstallMode::RejectExisting,
            )
            .expect("install after-cut table");

        let bundle = RouteRegistryWriter::serialize_view(&catalog, &aligned_view())
            .expect("serialize route registry bundle");

        assert_eq!(bundle.entries.len(), 1);
        assert_eq!(bundle.entries[0].table_name, "before_cut");
        assert_eq!(bundle.entries[0].tablet_id, before_cut_storage.tablet_id());
    }
}
