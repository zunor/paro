// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::replay_handler::CatalogReplayHandler;
use paro_catalog::entry::{CatalogEntryEnum, CatalogType};
use paro_common::logging::targets;
use paro_common::types::LogicalType;
use paro_storage::table::table_factory::TableFactory;
use paro_storage::table::table_handle::TableHandle;
use std::collections::HashMap;
use std::sync::Arc;

type RowIdDeleteReplayGroup = (String, String, Arc<TableHandle>, Vec<(u64, u32, u32)>);

impl<'a> CatalogReplayHandler<'a> {
    pub(super) fn find_storage_by_tablet_id(
        &self,
        tablet_id: u64,
    ) -> Option<(String, String, Arc<TableHandle>)> {
        let schemas = self
            .catalog
            .get_schema_collection()
            .scan(self.transaction.transaction_id, self.transaction.start_time);

        for schema_entry in schemas {
            let CatalogEntryEnum::Schema(schema) = schema_entry.as_ref() else {
                continue;
            };

            let tables = schema
                .collection(CatalogType::Table)
                .expect("table collection")
                .scan(self.transaction.transaction_id, self.transaction.start_time);

            for table_entry in tables {
                let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
                    continue;
                };

                let descriptor = table
                    .get_storage_descriptor()
                    .cloned()
                    .or_else(|| table.get_storage().and_then(|s| s.to_descriptor().ok()));

                let Some(descriptor) = descriptor else {
                    continue;
                };

                if descriptor.tablet_id != tablet_id {
                    continue;
                }

                if let Some(storage) = table.get_storage() {
                    return Some((
                        schema.base.name.clone(),
                        table.base.base.name.clone(),
                        Arc::clone(storage),
                    ));
                }

                let column_types: Vec<LogicalType> = table
                    .columns
                    .iter()
                    .map(|c| c.logical_type.clone())
                    .collect();
                match TableFactory::default().open_from_descriptor(&column_types, &descriptor) {
                    Ok(storage) => {
                        return Some((
                            schema.base.name.clone(),
                            table.base.base.name.clone(),
                            Arc::new(storage),
                        ));
                    }
                    Err(err) => {
                        tracing::warn!(
                            target: targets::INSTANCE,
                            schema = %schema.base.name,
                            table = %table.base.base.name,
                            tablet_id = tablet_id,
                            error = %err,
                            "Failed to restore descriptor-only storage during RowsetCommit replay"
                        );
                    }
                }
            }
        }

        None
    }

    pub(super) fn find_storage_by_rowset_id(
        &self,
        rowset_id: u64,
    ) -> Option<(String, String, Arc<TableHandle>)> {
        let schemas = self
            .catalog
            .get_schema_collection()
            .scan(self.transaction.transaction_id, self.transaction.start_time);

        for schema_entry in schemas {
            let CatalogEntryEnum::Schema(schema) = schema_entry.as_ref() else {
                continue;
            };

            let tables = schema
                .collection(CatalogType::Table)
                .expect("table collection")
                .scan(self.transaction.transaction_id, self.transaction.start_time);

            for table_entry in tables {
                let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
                    continue;
                };

                let descriptor = table
                    .get_storage_descriptor()
                    .cloned()
                    .or_else(|| table.get_storage().and_then(|s| s.to_descriptor().ok()));

                let Some(descriptor) = descriptor else {
                    continue;
                };

                let storage = if let Some(storage) = table.get_storage() {
                    Arc::clone(storage)
                } else {
                    let column_types: Vec<LogicalType> = table
                        .columns
                        .iter()
                        .map(|c| c.logical_type.clone())
                        .collect();
                    match TableFactory::default().open_from_descriptor(&column_types, &descriptor) {
                        Ok(storage) => Arc::new(storage),
                        Err(err) => {
                            tracing::warn!(
                                target: targets::INSTANCE,
                                schema = %schema.base.name,
                                table = %table.base.base.name,
                                rowset_id = rowset_id,
                                error = %err,
                                "Failed to restore descriptor-only storage during RowIdDelete replay"
                            );
                            continue;
                        }
                    }
                };

                if storage.tablet().find_rowset_by_id(rowset_id).is_some() {
                    return Some((
                        schema.base.name.clone(),
                        table.base.base.name.clone(),
                        storage,
                    ));
                }
            }
        }

        None
    }

    pub(super) fn replay_primary_delete(
        &mut self,
        keys: &[Vec<u8>],
    ) -> paro_common::error::Result<()> {
        tracing::debug!(
            target: targets::INSTANCE,
            keys = keys.len(),
            "Skipping PRIMARY DELETE in instance replay (tablet WAL owns this path)"
        );
        Ok(())
    }

    pub(super) fn replay_row_id_delete(
        &mut self,
        locations: &[(u64, u32, u32)],
    ) -> paro_common::error::Result<()> {
        let mut grouped: HashMap<u64, RowIdDeleteReplayGroup> = HashMap::new();

        for &location @ (rowset_id, _, _) in locations {
            let Some((schema_name, table_name, storage)) =
                self.find_storage_by_rowset_id(rowset_id)
            else {
                tracing::warn!(
                    target: targets::INSTANCE,
                    rowset_id = rowset_id,
                    "RowIdDelete replay skipped (rowset not mapped in catalog/runtime storage)"
                );
                continue;
            };

            grouped
                .entry(storage.tablet_id())
                .or_insert_with(|| (schema_name, table_name, Arc::clone(&storage), Vec::new()))
                .3
                .push(location);
        }

        for (_, (schema_name, table_name, storage, tablet_locations)) in grouped {
            storage.replay_row_id_delete(&tablet_locations)?;
            tracing::info!(
                target: targets::INSTANCE,
                schema = %schema_name,
                table = %table_name,
                tablet_id = storage.tablet_id(),
                locations = tablet_locations.len(),
                "Replayed RowIdDelete"
            );
        }

        Ok(())
    }

    pub(super) fn replay_rowset_commit(
        &mut self,
        tablet_id: u64,
        rowset_id: u64,
        start_version: i64,
        end_version: i64,
        rowset_path: &str,
    ) -> paro_common::error::Result<()> {
        let Some((schema_name, table_name, storage)) = self.find_storage_by_tablet_id(tablet_id)
        else {
            tracing::debug!(
                target: targets::INSTANCE,
                tablet_id = tablet_id,
                rowset_id = rowset_id,
                "RowsetCommit replay skipped (tablet not mapped in catalog)"
            );
            return Ok(());
        };

        storage.replay_rowset_commit(rowset_id, start_version, end_version, rowset_path)?;
        tracing::info!(
            target: targets::INSTANCE,
            schema = %schema_name,
            table = %table_name,
            tablet_id = tablet_id,
            rowset_id = rowset_id,
            start_version = start_version,
            end_version = end_version,
            "Replayed RowsetCommit"
        );
        Ok(())
    }
}
