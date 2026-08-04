// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::recovery::replay_handler::CatalogReplayHandler;
use paro_catalog::collection::InstallMode;
use paro_catalog::entry::CatalogObjectId;
use paro_catalog::entry::{CatalogEntryEnum, CreateSchemaInfo};
use paro_common::ddl::CreateSchemaPayload;
use paro_common::logging::targets;
use std::sync::Arc;

impl<'a> CatalogReplayHandler<'a> {
    pub(in crate::recovery) fn replay_create_schema(
        &mut self,
        schema_name: &str,
        payload: &CreateSchemaPayload,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        if self
            .catalog
            .get_schema(&self.transaction, schema_name)
            .is_ok()
        {
            tracing::debug!(
                target: targets::INSTANCE,
                schema = schema_name,
                "Schema already exists, skipping"
            );
            return Ok(());
        }

        let info = CreateSchemaInfo {
            catalog: self.catalog.name().to_string(),
            name: schema_name.to_string(),
            internal: false,
            on_conflict: paro_catalog::entry::OnCreateConflict::IgnoreOnConflict,
        };
        self.observe_object_id(payload.object_id);
        let entry = Arc::new(CatalogEntryEnum::Schema(Arc::new(
            paro_catalog::entry::SchemaEntry::from_info_with_object_id(
                &info,
                CatalogObjectId::from_raw(payload.object_id),
                Arc::clone(self.catalog.object_id_allocator()),
                self.catalog.gc_epoch_handle(),
                0,
            ),
        )));
        self.install_replayed_entry(
            self.catalog.get_schema_collection(),
            commit_id,
            entry,
            InstallMode::RejectExisting,
        )?;
        tracing::debug!(
            target: targets::INSTANCE,
            schema = schema_name,
            "Replayed CREATE SCHEMA"
        );
        Ok(())
    }

    pub(in crate::recovery) fn replay_drop_schema(
        &mut self,
        schema_name: &str,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        match self
            .catalog
            .get_schema_collection()
            .stage_drop(&self.transaction, schema_name)?
        {
            Some(handle) => {
                self.publish_catalog_handle(handle, commit_id)?;
                tracing::info!(
                    target: targets::INSTANCE,
                    schema = schema_name,
                    "Replayed DROP SCHEMA"
                );
                Ok(())
            }
            None => {
                tracing::debug!(
                    target: targets::INSTANCE,
                    schema = schema_name,
                    "DROP SCHEMA replay skipped: already absent"
                );
                Ok(())
            }
        }
    }
}
