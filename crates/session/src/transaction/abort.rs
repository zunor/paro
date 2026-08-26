// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::session_transaction::FrozenTransaction;
use crate::session::Session;
use paro_common::effect::StagedArtifactDescriptor;
use paro_common::error::{ParoError, Result};
use paro_storage::transaction::manager::TransactionManager;
use std::path::PathBuf;
use std::sync::Arc;

pub struct AbortPipeline {
    manager: Arc<TransactionManager>,
    frozen: FrozenTransaction,
}

impl AbortPipeline {
    pub fn new(manager: Arc<TransactionManager>, frozen: FrozenTransaction) -> Self {
        Self { manager, frozen }
    }

    pub fn execute(self) -> Result<()> {
        self.manager
            .rollback_transaction(Arc::clone(&self.frozen.active))?;

        for mut change in self.frozen.ddl_changes.into_iter().rev() {
            if let Some(delta) = change.dependencies.take() {
                delta.discard();
            }
            if let Some(handle) = change.catalog.take() {
                handle.discard()?;
            }
            Self::discard_staged_artifacts(&change.staged_artifacts);
        }

        Ok(())
    }

    fn discard_staged_artifacts(descriptors: &[StagedArtifactDescriptor]) {
        for descriptor in descriptors.iter().rev() {
            let path = match descriptor {
                StagedArtifactDescriptor::PropertyGraphBuild { staging, .. } => {
                    Self::path_from_components(&staging.path_components)
                }
                StagedArtifactDescriptor::BulkLoadRowset(artifact) => {
                    Self::path_from_components(&artifact.staging.path_components)
                }
                StagedArtifactDescriptor::SearchGenerationBuild(_artifact) => {
                    // The transient staged-generation owner resolves the
                    // tablet-relative ref and removes its workspace. Crash
                    // leftovers are handled by startup staging GC.
                    continue;
                }
            };
            if !path.exists() {
                continue;
            }
            let _ = std::fs::remove_dir_all(path);
        }
    }

    fn path_from_components(components: &[String]) -> PathBuf {
        let mut iter = components.iter();
        let Some(first) = iter.next() else {
            return PathBuf::new();
        };
        let mut path = PathBuf::from(first);
        for component in iter {
            path.push(component);
        }
        path
    }
}

impl Session {
    pub(crate) fn rollback_via_pipeline(&mut self, cause: Option<&ParoError>) -> Result<()> {
        let frozen = self
            .transaction
            .freeze_for(crate::transaction::session_transaction::FreezeIntent::Rollback)?;
        let pipeline = AbortPipeline::new(
            Arc::clone(self.current_database.transaction_manager()),
            frozen,
        );
        pipeline.execute()?;
        self.on_transaction_rollback_prepared();
        self.notify_transaction_rollback(cause);
        self.current_database.maybe_gc_catalog();
        crate::utility::settings::reconcile_effective_settings(self)?;
        self.refresh_session_metadata();
        Ok(())
    }

    pub(crate) fn rollback_auto_transaction(&mut self, cause: Option<&ParoError>) -> Result<()> {
        self.rollback_via_pipeline(cause)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestSessionBuilder;
    use crate::transaction::ddl_changes::PreparedCatalogOp;
    use paro_catalog::dependency::DependencyDelta;
    use paro_catalog::entry::{CatalogObjectId, CatalogObjectRef, CatalogType, DependencyType};
    use paro_common::ddl::{
        CreateSchemaPayload, DdlChange, DdlChangeRecord, DdlObjectKey, DdlObjectKind,
    };
    use paro_context::DdlExecutionProfile;

    fn staged_dependency_change(catalog_name: &str) -> PreparedCatalogOp {
        let schema_id = CatalogObjectId::from_raw(91_001);
        let view_id = CatalogObjectId::from_raw(91_002);
        let mut dependencies = DependencyDelta::new();
        dependencies.add_object(CatalogObjectRef::schema(
            schema_id,
            catalog_name.to_string(),
            "rollback_schema".to_string(),
        ));
        dependencies.add_object(CatalogObjectRef::in_schema(
            view_id,
            CatalogType::View,
            catalog_name.to_string(),
            Some(schema_id),
            "rollback_schema".to_string(),
            "rollback_view".to_string(),
        ));
        dependencies.add_dependency(view_id, schema_id, DependencyType::OwnedBy);

        PreparedCatalogOp {
            record: DdlChangeRecord {
                key: DdlObjectKey::new(
                    catalog_name,
                    None::<String>,
                    "rollback_schema",
                    DdlObjectKind::Schema,
                ),
                change: DdlChange::CreateSchema(CreateSchemaPayload {
                    object_id: schema_id.raw(),
                    if_not_exists: false,
                }),
            },
            profile: DdlExecutionProfile::metadata_only(),
            catalog: None,
            dependencies: Some(dependencies),
            dml_targets: Vec::new(),
            staged_artifacts: Vec::new(),
            storage_ops: Vec::new(),
            runtime_transitions: Vec::new(),
            cleanups: Vec::new(),
            post_commit_hooks: Vec::new(),
            transient_runtime: None,
        }
    }

    #[test]
    fn abort_pipeline_rolls_back_frozen_transaction() {
        let mut session = TestSessionBuilder::minimal().build();
        session.begin_explicit_transaction().unwrap();

        let frozen = session.transaction.freeze().unwrap();
        AbortPipeline::new(
            Arc::clone(session.current_database.transaction_manager()),
            frozen,
        )
        .execute()
        .unwrap();

        assert!(!session.transaction.has_active_transaction());
        assert!(session.transaction.is_auto_commit());
    }

    #[test]
    fn rollback_transaction_discards_dependency_delta() {
        let mut session = TestSessionBuilder::minimal().build();
        session.begin_explicit_transaction().unwrap();

        let catalog_name = session.current_database.catalog().name().to_string();
        session
            .transaction
            .ddl_changes()
            .lock()
            .unwrap()
            .record(staged_dependency_change(&catalog_name));

        assert!(!session
            .current_database
            .catalog()
            .dependency_graph()
            .contains_object(CatalogObjectId::from_raw(91_002)));

        session.rollback_transaction().unwrap();

        let graph = session.current_database.catalog().dependency_graph();
        assert!(!graph.contains_object(CatalogObjectId::from_raw(91_001)));
        assert!(!graph.contains_object(CatalogObjectId::from_raw(91_002)));
        assert!(graph
            .incident_edges_of(CatalogObjectId::from_raw(91_001))
            .is_empty());
    }
}
