// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::recovery::replay_handler::CatalogReplayHandler;
use paro_catalog::entry::CatalogObjectId;
use paro_catalog::entry::{
    CatalogEntryEnum, CatalogType, RoutineCatalogEntry, StoredRoutineOverload,
};
use paro_common::ddl::{CreateRoutinePayload, DropRoutinePayload};
use paro_common::error as paro_error;
use paro_common::logging::targets;
use paro_routine::RoutineSpec;
use std::sync::Arc;

impl<'a> CatalogReplayHandler<'a> {
    pub(in crate::recovery) fn replay_create_routine(
        &mut self,
        schema_name: &str,
        routine_name: &str,
        payload: &CreateRoutinePayload,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        let schema = self.ensure_schema(schema_name, commit_id)?;
        let spec: RoutineSpec = serde_json::from_str(&payload.spec_json).map_err(|error| {
            paro_error::serialization_error(format!(
                "failed to decode CREATE FUNCTION spec for {}.{}: {}",
                schema_name, routine_name, error
            ))
        })?;
        self.observe_object_id(payload.object_id);
        self.observe_object_id(payload.routine_id);

        let collection = schema
            .collection(CatalogType::Routine)
            .expect("routine collection");
        let existing = schema.get_routine(
            self.transaction.transaction_id,
            self.transaction.start_time,
            routine_name,
        );

        let handle = if let Some(existing_entry) = existing {
            let existing_routine = existing_entry
                .as_routine()
                .ok_or_else(|| paro_error::wrong_object_type("routine", routine_name))?;
            let mut overloads = existing_routine.overloads().to_vec();
            if let Some(index) = overloads.iter().position(|overload| {
                overload
                    .spec
                    .signature()
                    .exact_match(&spec.signature().argument_types)
            }) {
                overloads[index] = StoredRoutineOverload {
                    spec,
                    sql: payload.sql.clone(),
                };
            } else {
                overloads.push(StoredRoutineOverload {
                    spec,
                    sql: payload.sql.clone(),
                });
            }

            let replacement = Arc::new(CatalogEntryEnum::Routine(Arc::new(
                RoutineCatalogEntry::with_overloads(
                    self.catalog.name().to_string(),
                    schema_name.to_string(),
                    routine_name.to_string(),
                    existing_routine.base.base.object_id,
                    0,
                    overloads,
                ),
            )));
            collection.stage_replace(&self.transaction, routine_name, replacement)?
        } else {
            let entry = Arc::new(CatalogEntryEnum::Routine(Arc::new(
                RoutineCatalogEntry::with_overloads(
                    self.catalog.name().to_string(),
                    schema_name.to_string(),
                    routine_name.to_string(),
                    CatalogObjectId::from_raw(payload.object_id),
                    0,
                    vec![StoredRoutineOverload {
                        spec,
                        sql: payload.sql.clone(),
                    }],
                ),
            )));
            collection.stage_create(&self.transaction, routine_name, entry)?
        };

        if let Some(handle) = handle {
            self.publish_catalog_handle(handle, commit_id)?;
            tracing::info!(
                target: targets::INSTANCE,
                schema = schema_name,
                routine = routine_name,
                "Replayed CREATE FUNCTION"
            );
        } else {
            tracing::debug!(
                target: targets::INSTANCE,
                schema = schema_name,
                routine = routine_name,
                "CREATE FUNCTION replay skipped: no catalog mutation produced"
            );
        }

        Ok(())
    }

    pub(in crate::recovery) fn replay_drop_routine(
        &mut self,
        schema_name: &str,
        routine_name: &str,
        payload: &DropRoutinePayload,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        let schema = match self.catalog.get_schema(&self.transaction, schema_name) {
            Ok(schema) => schema,
            Err(_) => {
                tracing::debug!(
                    target: targets::INSTANCE,
                    schema = schema_name,
                    routine = routine_name,
                    "DROP FUNCTION replay skipped: schema not found"
                );
                return Ok(());
            }
        };

        let Some(existing_entry) = schema.get_routine(
            self.transaction.transaction_id,
            self.transaction.start_time,
            routine_name,
        ) else {
            tracing::debug!(
                target: targets::INSTANCE,
                schema = schema_name,
                routine = routine_name,
                "DROP FUNCTION replay skipped: routine already absent"
            );
            return Ok(());
        };
        let existing_routine = existing_entry
            .as_routine()
            .ok_or_else(|| paro_error::wrong_object_type("routine", routine_name))?;

        let Some(index) = existing_routine
            .overloads()
            .iter()
            .position(|overload| overload.spec.signature().exact_match(&payload.arg_types))
        else {
            tracing::debug!(
                target: targets::INSTANCE,
                schema = schema_name,
                routine = routine_name,
                "DROP FUNCTION replay skipped: signature already absent"
            );
            return Ok(());
        };

        let handle = if existing_routine.overloads().len() == 1 {
            schema
                .collection(CatalogType::Routine)
                .expect("routine collection")
                .stage_drop(&self.transaction, routine_name)?
        } else {
            let mut overloads = existing_routine.overloads().to_vec();
            overloads.remove(index);
            let replacement = Arc::new(CatalogEntryEnum::Routine(Arc::new(
                RoutineCatalogEntry::with_overloads(
                    self.catalog.name().to_string(),
                    schema_name.to_string(),
                    routine_name.to_string(),
                    existing_routine.base.base.object_id,
                    0,
                    overloads,
                ),
            )));
            schema
                .collection(CatalogType::Routine)
                .expect("routine collection")
                .stage_replace(&self.transaction, routine_name, replacement)?
        };

        if let Some(handle) = handle {
            self.publish_catalog_handle(handle, commit_id)?;
            tracing::info!(
                target: targets::INSTANCE,
                schema = schema_name,
                routine = routine_name,
                "Replayed DROP FUNCTION"
            );
        }

        Ok(())
    }
}
