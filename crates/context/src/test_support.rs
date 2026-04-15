// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::{
    AttachedDatabaseDirectory, EffectiveSettings, ExecutionResources, QueryResources,
    RuntimeLimits, SessionMetadataRows, StatementAuthContext, StatementCancellation,
    StatementContext, StatementEnvironment, StatementOptions, StatementView,
};
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::search_path::CatalogSearchEntry;
use paro_common::identity::DatabaseType;
use paro_function::scalar::cast::CastFunctionSet;
use paro_scheduler::scheduler::TaskScheduler;
use paro_storage::buffer::{BufferPool, StandardBufferManager, TemporaryMemoryManager};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct StaticSessionMetadata {
    rows: RwLock<SessionMetadataRows>,
}

impl crate::SessionMetadataProvider for StaticSessionMetadata {
    fn current_settings(&self) -> Vec<crate::SettingRow> {
        self.rows.read().unwrap().settings.clone()
    }

    fn current_prepared_statements(&self) -> Vec<crate::PreparedStatementSummary> {
        self.rows.read().unwrap().prepared_statements.clone()
    }

    fn current_cursors(&self) -> Vec<crate::CursorSummary> {
        self.rows.read().unwrap().cursors.clone()
    }
}

#[derive(Default)]
struct EmptyGraphManager;

impl crate::GraphIndexProvider for EmptyGraphManager {
    fn snapshot(
        &self,
        _id: &paro_common::identity::GraphId,
    ) -> Option<paro_storage::index::graph::GraphReadSnapshot> {
        None
    }
}

impl crate::GraphRegistry for EmptyGraphManager {
    fn register_generation(
        &self,
        _id: &paro_common::identity::GraphId,
        _generation: paro_storage::index::graph::GraphStorageGeneration,
    ) {
    }

    fn unregister(&self, _id: &paro_common::identity::GraphId) {}

    fn publish_generation(
        &self,
        _id: &paro_common::identity::GraphId,
        _generation: paro_storage::index::graph::GraphStorageGeneration,
    ) -> paro_common::error::Result<()> {
        Ok(())
    }
}

pub struct TestStatementContextBuilder {
    settings: HashMap<String, paro_common::runtime_value::Value>,
    limits: Option<RuntimeLimits>,
    search_path: Vec<CatalogSearchEntry>,
    current_database: String,
    current_schema: String,
    current_user: String,
    visible_version: u64,
}

impl TestStatementContextBuilder {
    pub fn minimal() -> Self {
        Self {
            settings: HashMap::new(),
            limits: None,
            search_path: Vec::new(),
            current_database: "test".to_string(),
            current_schema: "public".to_string(),
            current_user: "paro".to_string(),
            visible_version: 0,
        }
    }

    pub fn with_setting(
        mut self,
        key: impl Into<String>,
        value: paro_common::runtime_value::Value,
    ) -> Self {
        self.settings.insert(key.into(), value);
        self
    }

    pub fn with_limits(mut self, limits: RuntimeLimits) -> Self {
        self.limits = Some(limits);
        self
    }

    pub fn with_search_path(mut self, search_path: Vec<CatalogSearchEntry>) -> Self {
        self.search_path = search_path;
        self
    }

    pub fn with_current_database(mut self, current_database: impl Into<String>) -> Self {
        self.current_database = current_database.into();
        self
    }

    pub fn with_current_schema(mut self, current_schema: impl Into<String>) -> Self {
        self.current_schema = current_schema.into();
        self
    }

    pub fn with_current_user(mut self, current_user: impl Into<String>) -> Self {
        self.current_user = current_user.into();
        self
    }

    pub fn with_visible_version(mut self, visible_version: u64) -> Self {
        self.visible_version = visible_version;
        self
    }

    pub fn build(self) -> Arc<StatementContext> {
        let buffer_pool = BufferPool::new_arc(64 * 1024 * 1024);
        let buffer_manager = Arc::new(StandardBufferManager::new_with_pool(
            buffer_pool.clone(),
            paro_storage::buffer::DEFAULT_BLOCK_ALLOC_SIZE,
            8,
        ));
        let scheduler = Arc::new(TaskScheduler::new());
        let _ = scheduler.set_threads(1);
        let temporary_memory_manager = Arc::new(TemporaryMemoryManager::with_buffer_pool(
            Arc::downgrade(&buffer_pool),
        ));
        let execution_resources = Arc::new(ExecutionResources {
            scheduler,
            buffer_pool,
            buffer_manager,
            temporary_memory_manager,
        });
        let graph_manager = Arc::new(EmptyGraphManager);
        let settings = Arc::new(EffectiveSettings::new(self.settings));
        let limits = self.limits.unwrap_or(RuntimeLimits {
            max_threads: 1,
            max_memory: 64 * 1024 * 1024,
            use_temporary_directory: false,
            temporary_directory: String::new(),
            max_temp_directory_size: None,
            force_external: false,
        });
        let catalog = Arc::new(ParoCatalog::new(self.current_database.clone()));
        Arc::new(StatementContext {
            env: StatementEnvironment {
                current_database: self.current_database.clone(),
                current_schema: self.current_schema,
                current_user: self.current_user,
                search_path: self.search_path,
                auth: StatementAuthContext::default(),
            },
            txn: StatementView {
                visible_version: self.visible_version,
                ..StatementView::default()
            },
            ddl: None,
            settings,
            options: StatementOptions::default(),
            databases: Arc::new(AttachedDatabaseDirectory::new(
                0,
                Some(self.current_database.clone()),
                vec![crate::AttachedDatabaseSnapshot {
                    identity: crate::DatabaseSnapshotIdentity {
                        id: 1,
                        name: self.current_database,
                        path: ":memory:".to_string(),
                        db_type: DatabaseType::ReadWrite,
                    },
                    catalog,
                    tablet_meta: None,
                }],
            )),
            limits,
            cancellation: StatementCancellation::new(CancellationToken::new(), None),
            services: Arc::new(QueryResources {
                infra: execution_resources,
                cast_functions: Arc::new(CastFunctionSet::new()),
                graph_index: graph_manager.clone(),
                governance: crate::QueryResourceGovernance::default(),
                plan_cache: None,
                connection_info: None,
            }),
            graph_registry: graph_manager,
            session_metadata: Arc::new(StaticSessionMetadata::default()),
        })
    }
}
