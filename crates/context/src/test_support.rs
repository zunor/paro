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
use paro_external::runtime::host::PythonRuntimeProvider;
use paro_function::scalar::cast::CastFunctionSet;
use paro_scheduler::scheduler::TaskScheduler;
use paro_storage::buffer::{BufferPool, StandardBufferManager};
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
    can_create_routine: Option<bool>,
    can_create_elevated_routine: Option<bool>,
    python_runtime: Option<Arc<dyn PythonRuntimeProvider>>,
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
            can_create_routine: None,
            can_create_elevated_routine: None,
            python_runtime: None,
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

    pub fn with_routine_creation_privilege(mut self, allowed: bool) -> Self {
        self.can_create_routine = Some(allowed);
        self
    }

    pub fn with_elevated_routine_creation_privilege(mut self, allowed: bool) -> Self {
        self.can_create_elevated_routine = Some(allowed);
        self
    }

    pub fn with_python_runtime(mut self, python_runtime: Arc<dyn PythonRuntimeProvider>) -> Self {
        self.python_runtime = Some(python_runtime);
        self
    }

    pub fn with_visible_version(mut self, visible_version: u64) -> Self {
        self.visible_version = visible_version;
        self
    }

    pub fn build(self) -> Arc<StatementContext> {
        let current_user = self.current_user;
        let can_create_routine = self
            .can_create_routine
            .unwrap_or_else(|| current_user.eq_ignore_ascii_case("paro"));
        let can_create_elevated_routine = self
            .can_create_elevated_routine
            .unwrap_or(can_create_routine);
        let limits = self.limits.unwrap_or(RuntimeLimits {
            max_threads: 1,
            max_memory: 64 * 1024 * 1024,
            use_temporary_directory: false,
            temporary_directory: String::new(),
            max_temp_directory_size: None,
            force_external: false,
            rowset_scan_pushdown: true,
            parallel_scheduler: false,
        });
        let buffer_pool = BufferPool::new_arc(64 * 1024 * 1024);
        if limits.use_temporary_directory && !limits.temporary_directory.is_empty() {
            buffer_pool
                .set_temporary_directory(limits.temporary_directory.clone())
                .expect("set test temporary directory");
        }
        let buffer_manager = Arc::new(StandardBufferManager::new_with_pool(
            buffer_pool.clone(),
            paro_storage::buffer::DEFAULT_BLOCK_ALLOC_SIZE,
            8,
        ));
        let scheduler = Arc::new(TaskScheduler::new());
        let _ = scheduler.set_threads(1);
        let execution_resources = Arc::new(ExecutionResources {
            scheduler,
            buffer_pool,
            buffer_manager,
            query_memory_coordinator: None,
        });
        let graph_manager = Arc::new(EmptyGraphManager);
        let settings = Arc::new(EffectiveSettings::new(self.settings));
        let catalog = Arc::new(ParoCatalog::new(self.current_database.clone()));
        Arc::new(StatementContext {
            env: StatementEnvironment {
                current_database: self.current_database.clone(),
                current_schema: self.current_schema,
                current_user: current_user.clone(),
                search_path: self.search_path,
                auth: StatementAuthContext {
                    authenticated_user: Some(current_user),
                    can_create_routine,
                    can_create_elevated_routine,
                    ..StatementAuthContext::default()
                },
            },
            txn: StatementView {
                transaction: crate::TransactionView::autocommit(paro_transaction::ReadTs::new(
                    self.visible_version,
                )),
                ..StatementView::default()
            },
            ddl: None,
            settings,
            options: StatementOptions::default(),
            input: crate::StatementInput::default(),
            time: crate::StatementTimeContext::capture(None),
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
                    catalog_epoch: catalog.gc_epoch(),
                    catalog,
                    tablet_meta: None,
                    wal_metrics: crate::AttachedDatabaseWalMetricsSnapshot::default(),
                    transaction_metrics: crate::AttachedDatabaseTransactionMetricsSnapshot::default(
                    ),
                    commit_frontier: crate::AttachedDatabaseCommitFrontierSnapshot::default(),
                    commit_poison: crate::AttachedDatabaseCommitPoisonSnapshot::default(),
                }],
            )),
            limits,
            cancellation: StatementCancellation::new(CancellationToken::new(), None),
            services: Arc::new(QueryResources {
                infra: execution_resources,
                cast_functions: Arc::new(CastFunctionSet::new()),
                graph_index: graph_manager.clone(),
                python_runtime: self.python_runtime,
                governance: crate::QueryResourceGovernance::default(),
                plan_cache: None,
                connection_info: None,
            }),
            graph_registry: graph_manager,
            session_metadata: Arc::new(StaticSessionMetadata::default()),
        })
    }
}
