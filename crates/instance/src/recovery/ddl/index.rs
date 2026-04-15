use crate::recovery::replay_handler::CatalogReplayHandler;
use paro_catalog::collection::InstallMode;
use paro_catalog::entry::CatalogObjectId;
use paro_catalog::entry::{
    CatalogEntryEnum, CatalogType, CreateIndexInfo, IndexBuildState, IndexCatalogEntry, IndexType,
    LogicalIndex, OnCreateConflict,
};
use paro_common::ddl::CreateIndexPayload;
use paro_common::logging::targets;
use std::sync::Arc;

impl<'a> CatalogReplayHandler<'a> {
    pub(in crate::recovery) fn replay_create_index(
        &mut self,
        schema_name: &str,
        index_name: &str,
        payload: &CreateIndexPayload,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        let schema = self.ensure_schema(schema_name, commit_id)?;

        let table_entry = match schema.get_table(
            self.transaction.transaction_id,
            self.transaction.start_time,
            &payload.table_name,
        ) {
            Some(entry) => entry,
            None => {
                tracing::warn!(
                    target: targets::INSTANCE,
                    schema = schema_name,
                    table = %payload.table_name,
                    index = index_name,
                    "CREATE INDEX replay skipped: table not found"
                );
                return Ok(());
            }
        };

        let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
            tracing::warn!(
                target: targets::INSTANCE,
                schema = schema_name,
                table = %payload.table_name,
                index = index_name,
                "CREATE INDEX replay skipped: target entry is not a table"
            );
            return Ok(());
        };

        let column_ids = payload
            .column_ids
            .iter()
            .copied()
            .map(LogicalIndex::new)
            .collect::<Vec<_>>();
        let index_type = IndexType::from_str(&payload.index_type);
        let constraint_type = if payload.is_unique {
            paro_storage::index::IndexConstraintType::Unique
        } else {
            paro_storage::index::IndexConstraintType::None
        };

        let (build_state, failure_reason) = if index_type.requires_runtime_build() {
            (
                IndexBuildState::Failed,
                Some(
                    "WAL replay restored index metadata only; rerun CREATE INDEX to rebuild runtime data"
                        .to_string(),
                ),
            )
        } else if index_type == IndexType::FullText {
            (
                IndexBuildState::Building,
                Some(
                    "WAL replay restored fulltext metadata; waiting for coverage validation"
                        .to_string(),
                ),
            )
        } else {
            (IndexBuildState::Ready, None)
        };

        let mut info = CreateIndexInfo::new(
            schema_name.to_string(),
            payload.table_name.clone(),
            index_name.to_string(),
            column_ids,
            payload.column_types.clone(),
        )
        .with_catalog(self.catalog.name().to_string())
        .with_index_type(index_type)
        .with_build_state(build_state);
        if index_type == IndexType::FullText {
            let column_id = payload
                .column_ids
                .first()
                .copied()
                .map(LogicalIndex::new)
                .ok_or_else(|| {
                    paro_common::error::serialization_error(
                        "FullText WAL index missing source column id",
                    )
                })?;
            let config = payload
                .fulltext_config
                .clone()
                .unwrap_or_else(|| "simple".to_string());
            info = info.with_fulltext_options(column_id, config);
        }
        info.constraint_type = constraint_type;
        info.on_conflict = OnCreateConflict::IgnoreOnConflict;
        info.if_not_exists = true;

        if let Some(reason) = failure_reason {
            info = info.with_failure_reason(reason);
        }

        self.observe_object_id(payload.object_id);
        let index_entry = Arc::new(IndexCatalogEntry::with_object_id(
            info,
            table.base.base.object_id.raw(),
            0,
            self.catalog.name().to_string(),
            CatalogObjectId::from_raw(payload.object_id),
        ));
        let index_collection = schema
            .collection(CatalogType::Index)
            .expect("index collection");
        self.install_replayed_entry(
            index_collection,
            commit_id,
            Arc::new(CatalogEntryEnum::Index(index_entry)),
            InstallMode::RejectExisting,
        )?;

        tracing::info!(
            target: targets::INSTANCE,
            schema = schema_name,
            table = %payload.table_name,
            index = index_name,
            index_type = %payload.index_type,
            build_state = ?build_state,
            "Replayed CREATE INDEX metadata"
        );
        Ok(())
    }

    pub(in crate::recovery) fn replay_drop_index(
        &mut self,
        schema_name: &str,
        index_name: &str,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        let schema = match self.catalog.get_schema(&self.transaction, schema_name) {
            Ok(schema) => schema,
            Err(_) => {
                tracing::debug!(
                    target: targets::INSTANCE,
                    schema = schema_name,
                    index = index_name,
                    "DROP INDEX replay skipped: schema not found"
                );
                return Ok(());
            }
        };

        if let Some(handle) = schema
            .collection(CatalogType::Index)
            .expect("index collection")
            .stage_drop(&self.transaction, index_name)?
        {
            self.publish_catalog_handle(handle, commit_id)?;
            tracing::info!(
                target: targets::INSTANCE,
                schema = schema_name,
                index = index_name,
                "Replayed DROP INDEX"
            );
        } else {
            tracing::debug!(
                target: targets::INSTANCE,
                schema = schema_name,
                index = index_name,
                "DROP INDEX replay skipped: already absent"
            );
        }
        Ok(())
    }
}
