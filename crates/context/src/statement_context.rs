// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::{
    AttachedDatabaseDirectory, AttachedDatabaseSnapshot, DdlApplyContext, EffectiveSettings,
    QueryResources, RuntimeLimits, SessionMetadataProvider, StatementAuthContext,
    StatementCancellation, StatementEnvironment, StatementExecutionTracker, StatementOptions,
    StatementView, TxnAdmissionState, WriteGuard,
};
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::mvcc::CatalogSnapshot;
use paro_catalog::search_path::CatalogSearchEntry;
use paro_common::allocator::{Allocator, BufferAllocator, MemoryTag};
use paro_common::identity::GraphId;
use paro_common::runtime_value::Value;
use paro_external_runtime::host::PythonRuntimeStatus;
use paro_storage::meta::TabletMetaManager;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompileEnvironmentKey {
    pub current_database: String,
    pub current_schema: String,
    pub search_path: Vec<CatalogSearchEntry>,
    pub visible_generation: u64,
    pub settings_fingerprint: u64,
}

#[derive(Clone)]
pub struct StatementContext {
    pub env: StatementEnvironment,
    pub txn: StatementView,
    pub ddl: Option<Arc<dyn DdlApplyContext>>,
    pub settings: Arc<EffectiveSettings>,
    pub options: StatementOptions,
    pub databases: Arc<AttachedDatabaseDirectory>,
    pub limits: RuntimeLimits,
    pub cancellation: StatementCancellation,
    pub execution_tracker: Option<Arc<dyn StatementExecutionTracker>>,
    pub services: Arc<QueryResources>,
    pub graph_registry: Arc<dyn crate::GraphRegistry>,
    pub session_metadata: Arc<dyn SessionMetadataProvider>,
}

impl std::fmt::Debug for StatementContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatementContext")
            .field("env", &self.env)
            .field("txn", &self.txn)
            .field("options", &self.options)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl StatementContext {
    pub fn current_database(&self) -> &str {
        &self.env.current_database
    }

    pub fn current_schema(&self) -> &str {
        &self.env.current_schema
    }

    pub fn current_user(&self) -> &str {
        &self.env.current_user
    }

    pub fn search_path(&self) -> &[CatalogSearchEntry] {
        &self.env.search_path
    }

    pub fn auth(&self) -> &StatementAuthContext {
        &self.env.auth
    }

    pub fn is_interrupted(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    pub fn bind_execution_coordinator(
        &self,
        coordinator: Arc<paro_scheduler::coordinator::EventCoordinator>,
    ) {
        if let Some(tracker) = &self.execution_tracker {
            tracker.bind_coordinator(coordinator);
        }
    }

    pub fn catalog(&self) -> Arc<ParoCatalog> {
        self.current_database_snapshot()
            .map(|snapshot| snapshot.catalog.clone())
            .expect("current database snapshot must exist")
    }

    pub fn catalog_txn_view(&self) -> CatalogSnapshot {
        if self.txn.active.is_some() {
            CatalogSnapshot::writer(self.txn.id, self.txn.start_time)
        } else {
            CatalogSnapshot::read_only(u64::MAX)
        }
    }

    pub fn get_setting(&self, key: &str) -> Option<&Value> {
        self.settings.get(key)
    }

    pub fn cast_functions(&self) -> Arc<paro_function::scalar::cast::CastFunctionSet> {
        self.services.cast_functions.clone()
    }

    pub fn query_governance(&self) -> &crate::QueryResourceGovernance {
        &self.services.governance
    }

    pub fn python_runtime_status(&self) -> Option<PythonRuntimeStatus> {
        self.services
            .python_runtime
            .as_ref()
            .map(|provider| provider.status())
    }

    pub fn ensure_python_runtime_ready_for_ddl(&self) -> paro_common::error::Result<()> {
        if let Some(provider) = &self.services.python_runtime {
            provider.ensure_ready_for_ddl()
        } else {
            Ok(())
        }
    }

    pub fn ensure_python_runtime_ready_for_execution(&self) -> paro_common::error::Result<()> {
        if let Some(provider) = &self.services.python_runtime {
            provider.ensure_ready_for_execution()
        } else {
            Ok(())
        }
    }

    pub fn shared_plan_cache(&self) -> Option<&Arc<dyn crate::SharedPlanCacheHandle>> {
        self.services.plan_cache.as_ref()
    }

    pub fn connection_info_provider(&self) -> Option<&Arc<dyn crate::ConnectionInfoProvider>> {
        self.services.connection_info.as_ref()
    }

    pub fn buffer_pool(&self) -> &Arc<paro_storage::buffer::BufferPool> {
        &self.services.infra.buffer_pool
    }

    pub fn buffer_manager(&self) -> &Arc<dyn paro_storage::buffer::BufferManager> {
        &self.services.infra.buffer_manager
    }

    pub fn buffer_allocator(&self) -> Arc<dyn Allocator> {
        self.services.infra.buffer_manager.get_buffer_allocator()
    }

    pub fn query_memory_coordinator(&self) -> Option<&Arc<dyn crate::QueryMemoryCoordinator>> {
        self.services.infra.query_memory_coordinator.as_ref()
    }

    pub fn scheduler(&self) -> &Arc<paro_scheduler::scheduler::TaskScheduler> {
        &self.services.infra.scheduler
    }

    pub fn number_of_threads(&self) -> usize {
        self.limits.max_threads
    }

    pub fn transaction_id(&self) -> u64 {
        self.txn.id
    }

    pub fn transaction_start_time(&self) -> u64 {
        self.txn.start_time
    }

    pub fn transaction_visible_version(&self) -> u64 {
        self.txn.visible_version
    }

    pub fn active_transaction(&self) -> Option<Arc<paro_storage::transaction::txn::Transaction>> {
        self.txn.active.clone()
    }

    pub fn ddl(&self) -> Option<Arc<dyn DdlApplyContext>> {
        self.ddl.clone()
    }

    pub fn write_guard(&self) -> Option<Arc<WriteGuard>> {
        self.txn.write_guard.clone()
    }

    pub fn txn_admission(&self) -> Option<Arc<TxnAdmissionState>> {
        self.txn.admission.clone()
    }

    pub fn metadata_provider(&self) -> Option<Arc<TabletMetaManager>> {
        self.current_metadata_provider()
    }

    pub fn session_metadata_provider(&self) -> Arc<dyn SessionMetadataProvider> {
        self.session_metadata.clone()
    }

    pub fn database(&self, name: &str) -> Option<&AttachedDatabaseSnapshot> {
        self.databases.get(name)
    }

    pub fn iter_databases(&self) -> impl Iterator<Item = &AttachedDatabaseSnapshot> {
        self.databases.iter()
    }

    pub fn current_database_snapshot(&self) -> Option<&AttachedDatabaseSnapshot> {
        self.databases.current_database_snapshot()
    }

    pub fn current_metadata_provider(&self) -> Option<Arc<TabletMetaManager>> {
        self.current_database_snapshot()
            .and_then(|snapshot| snapshot.tablet_meta.clone())
    }

    pub fn statement_format(&self) -> Option<&str> {
        self.options.statement_format.as_deref()
    }

    pub fn allocator(&self, tag: MemoryTag) -> Arc<dyn Allocator> {
        if tag == MemoryTag::Allocator {
            self.buffer_allocator()
        } else {
            Arc::new(BufferAllocator::new(
                self.buffer_manager().clone() as Arc<dyn paro_common::allocator::BufferManager>,
                tag,
            ))
        }
    }

    pub fn compile_environment_key(&self) -> CompileEnvironmentKey {
        CompileEnvironmentKey {
            current_database: self.env.current_database.clone(),
            current_schema: self.env.current_schema.clone(),
            search_path: self.env.search_path.clone(),
            visible_generation: self.databases.visible_generation,
            settings_fingerprint: self.settings.fingerprint(),
        }
    }

    pub fn graph_id(
        &self,
        catalog: impl Into<String>,
        schema: impl Into<String>,
        graph_name: impl Into<String>,
    ) -> GraphId {
        GraphId::new(catalog, schema, graph_name)
    }
}
