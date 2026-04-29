// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::recovery::replay_handler::CatalogReplayHandler;
use paro_catalog::collection::InstallMode;
use paro_catalog::entry::CatalogObjectId;
use paro_catalog::entry::{
    CatalogEntryEnum, CatalogType, ColumnDefinition, ConstraintType, CreateTableInfo,
    OnCreateConflict, TableCatalogEntry,
};
use paro_common::ddl::CreateTablePayload;
use paro_common::error as paro_error;
use paro_common::logging::targets;
use paro_common::types::LogicalType;
use paro_journal::wal::wal_entry::{ColumnInfo, TableConstraintInfo, WalConstraintType};
use paro_storage::table::table_factory::TableFactory;
use std::sync::Arc;

impl<'a> CatalogReplayHandler<'a> {
    pub(in crate::recovery) fn replay_create_table(
        &mut self,
        schema_name: &str,
        table_name: &str,
        payload: &CreateTablePayload,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        let schema = self.ensure_schema(schema_name, commit_id)?;

        if schema
            .get_table(
                self.transaction.transaction_id,
                self.transaction.start_time,
                table_name,
            )
            .is_some()
        {
            tracing::debug!(
                target: targets::INSTANCE,
                schema = schema_name,
                table = table_name,
                "Table already exists, skipping"
            );
            return Ok(());
        }

        let constraints = payload
            .constraints
            .iter()
            .map(|constraint| {
                let constraint_type = match constraint.constraint_type.as_str() {
                    "not_null" => WalConstraintType::NotNull,
                    "unique" => WalConstraintType::Unique,
                    "primary_key" => WalConstraintType::PrimaryKey,
                    "foreign_key" => WalConstraintType::ForeignKey,
                    "check" => WalConstraintType::Check,
                    other => {
                        return Err(paro_error::serialization_error(format!(
                            "unknown catalog txn constraint type: {other}"
                        )))
                    }
                };
                Ok(TableConstraintInfo {
                    constraint_type: constraint_type as u8,
                    columns: constraint.columns.clone(),
                    expression: constraint.expression.clone(),
                    referenced_table: constraint.referenced_table.clone(),
                    referenced_columns: constraint.referenced_columns.clone(),
                })
            })
            .collect::<paro_common::error::Result<Vec<_>>>()?;
        let columns = payload
            .columns
            .iter()
            .map(|column| {
                ColumnInfo::new(
                    column.name.clone(),
                    column.logical_type.clone(),
                    column.nullable,
                )
            })
            .collect::<Vec<_>>();

        let storage_descriptor = payload
            .storage
            .as_ref()
            .map(Self::table_storage_descriptor_from_typed)
            .transpose()?;
        let descriptor = storage_descriptor.as_ref().ok_or_else(|| {
            paro_error::serialization_error(format!(
                "CREATE TABLE {}.{} missing storage descriptor in WAL replay",
                schema_name, table_name
            ))
        })?;

        let mut column_defs: Vec<ColumnDefinition> = columns
            .iter()
            .map(|col| {
                let mut def = ColumnDefinition::new(col.name.clone(), col.logical_type.clone());
                if !col.nullable {
                    def = def.with_not_null();
                }
                def
            })
            .collect();

        let column_types: Vec<LogicalType> =
            columns.iter().map(|col| col.logical_type.clone()).collect();

        let constraints = Self::decode_constraints(&constraints, columns.len())?;
        for constraint in &constraints {
            if matches!(
                constraint.constraint_type,
                ConstraintType::NotNull | ConstraintType::PrimaryKey
            ) {
                for &column_idx in &constraint.columns {
                    if let Some(column_def) = column_defs.get_mut(column_idx) {
                        column_def.not_null = true;
                    }
                }
            }
        }

        let storage = Arc::new(
            TableFactory::new(self.tablet_meta_manager.clone())
                .open_from_descriptor(&column_types, descriptor)?,
        );
        let info = CreateTableInfo::new(
            self.catalog.name().to_string(),
            schema_name.to_string(),
            table_name.to_string(),
            column_defs,
        )
        .with_constraints(constraints)
        .with_on_conflict(OnCreateConflict::IgnoreOnConflict);
        self.observe_object_id(payload.object_id);
        let entry = Arc::new(CatalogEntryEnum::Table(Arc::new(
            TableCatalogEntry::from_info_with_object_id(
                info,
                storage,
                CatalogObjectId::from_raw(payload.object_id),
                0,
            ),
        )));
        let table_collection = schema
            .collection(CatalogType::Table)
            .expect("table collection");
        self.install_replayed_entry(
            table_collection,
            commit_id,
            entry,
            InstallMode::RejectExisting,
        )?;

        tracing::info!(
            target: targets::INSTANCE,
            schema = schema_name,
            table = table_name,
            columns = columns.len(),
            tablet_id = descriptor.tablet_id,
            "Replayed CREATE TABLE"
        );
        Ok(())
    }

    pub(in crate::recovery) fn replay_drop_table(
        &mut self,
        schema_name: &str,
        table_name: &str,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        let schema = match self.catalog.get_schema(&self.transaction, schema_name) {
            Ok(schema) => schema,
            Err(_) => {
                tracing::debug!(
                    target: targets::INSTANCE,
                    schema = schema_name,
                    table = table_name,
                    "DROP TABLE replay skipped: schema not found"
                );
                return Ok(());
            }
        };
        match schema
            .collection(CatalogType::Table)
            .expect("table collection")
            .stage_drop(&self.transaction, table_name)?
        {
            Some(handle) => {
                self.publish_catalog_handle(handle, commit_id)?;
                tracing::info!(
                    target: targets::INSTANCE,
                    schema = schema_name,
                    table = table_name,
                    "Replayed DROP TABLE"
                );
                Ok(())
            }
            None => {
                tracing::debug!(
                    target: targets::INSTANCE,
                    schema = schema_name,
                    table = table_name,
                    "DROP TABLE replay skipped (already absent)"
                );
                Ok(())
            }
        }
    }
}
