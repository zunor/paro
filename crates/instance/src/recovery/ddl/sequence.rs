use crate::recovery::replay_handler::CatalogReplayHandler;
use paro_catalog::collection::InstallMode;
use paro_catalog::entry::CatalogObjectId;
use paro_catalog::entry::{
    CatalogEntryEnum, CatalogType, CreateSequenceInfo, SequenceCatalogEntry,
};
use paro_common::ddl::CreateSequencePayload;
use paro_common::logging::targets;
use std::sync::Arc;

impl<'a> CatalogReplayHandler<'a> {
    pub(in crate::recovery) fn replay_create_sequence(
        &mut self,
        schema_name: &str,
        sequence_name: &str,
        payload: &CreateSequencePayload,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        let schema = self.ensure_schema(schema_name, commit_id)?;
        let mut info = CreateSequenceInfo::new(schema_name.to_string(), sequence_name.to_string())
            .with_catalog(self.catalog.name().to_string())
            .with_increment(payload.increment)
            .with_min_value(payload.min_value)
            .with_max_value(payload.max_value)
            .with_start_value(payload.start_value)
            .with_if_not_exists();
        if payload.cycle {
            info = info.with_cycle();
        }
        self.observe_object_id(payload.object_id);
        let entry = Arc::new(CatalogEntryEnum::Sequence(Arc::new(
            SequenceCatalogEntry::with_object_id(
                info,
                0,
                self.catalog.name().to_string(),
                CatalogObjectId::from_raw(payload.object_id),
            )?,
        )));
        let sequence_collection = schema
            .collection(CatalogType::Sequence)
            .expect("sequence collection");
        self.install_replayed_entry(
            sequence_collection,
            commit_id,
            entry,
            InstallMode::RejectExisting,
        )?;
        tracing::info!(
            target: targets::INSTANCE,
            schema = schema_name,
            sequence = sequence_name,
            "Replayed CREATE SEQUENCE"
        );
        Ok(())
    }

    pub(in crate::recovery) fn replay_drop_sequence(
        &mut self,
        schema_name: &str,
        sequence_name: &str,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        let schema = match self.catalog.get_schema(&self.transaction, schema_name) {
            Ok(schema) => schema,
            Err(_) => {
                tracing::debug!(
                    target: targets::INSTANCE,
                    schema = schema_name,
                    sequence = sequence_name,
                    "DROP SEQUENCE replay skipped: schema not found"
                );
                return Ok(());
            }
        };
        if let Some(handle) = schema
            .collection(CatalogType::Sequence)
            .expect("sequence collection")
            .stage_drop(&self.transaction, sequence_name)?
        {
            self.publish_catalog_handle(handle, commit_id)?;
            tracing::info!(
                target: targets::INSTANCE,
                schema = schema_name,
                sequence = sequence_name,
                "Replayed DROP SEQUENCE"
            );
        } else {
            tracing::debug!(
                target: targets::INSTANCE,
                schema = schema_name,
                sequence = sequence_name,
                "DROP SEQUENCE replay skipped: already absent"
            );
        }
        Ok(())
    }
}
