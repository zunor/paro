// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Instance-level runtime for shared resources and managed databases.

pub use paro_context::StatementCancelReason;
use std::sync::Arc;

pub mod builder;
pub mod builtin;
pub mod config;
pub mod database;
pub mod file_system;
pub mod lifecycle;
pub mod metadata;
pub mod recovery;
pub mod runtime;
pub mod storage_manager;
pub mod valid_checker;

#[cfg(test)]
mod tests;

pub use builder::InstanceBuilder;
pub use config::{BootConfig, InstanceConfig, InstanceConfigOptions};
pub use database::handle::{
    AccessMode, AttachOptions, AttachVisibility, DatabaseCloseAction, DatabaseHandle, DbState,
    RecoveryMode,
};
pub use database::hooks::{
    FullTextRecoveryHook, GraphProjectionRecoveryHook, RecoveryHook, RecoveryHookContext,
    RecoveryHookIssue, RecoveryHookIssueKind, RecoveryHookResult, VectorIndexRecoveryHook,
};
pub use database::identity::{DatabaseIdentity, DatabaseType, RESERVED_NAMES};
pub use database::opener::{
    DatabaseOpenContext, DatabaseOpenError, DatabaseOpenIntent, DatabaseOpenRequest,
    DatabaseOpenResult, RecoveryReport,
};
pub use database::registry::{
    AlterDatabaseInfo, AlterDatabaseType, AttachInfo, DatabaseFilePathManager, DatabaseRegistry,
    InsertDatabasePathResult, OnCreateConflict, OnEntryNotFound, SYSTEM_CATALOG, TEMP_CATALOG,
};
pub use database::state::DatabaseState;
pub use database::storage::{
    DatabaseStorage, InMemoryDatabaseStorage, DEFAULT_CHECKPOINT_WAL_SIZE,
};
pub use database::storage_identity::{
    DatabaseStorageIdentity, DATABASE_STORAGE_IDENTITY_FORMAT_VERSION,
    DATABASE_STORAGE_IDENTITY_KEY,
};
pub use database::wal_observability::WalLifecycleMetricsSnapshot;
pub use database::{DatabaseDdlContext, InstanceWalLifecycleMetrics, ManagedDatabaseService};
pub use file_system::{DatabaseFileSystem, FileSystem, InMemoryFileSystem, LocalFileSystem};
pub use lifecycle::admission::AdmissionController;
pub use lifecycle::ddl_lock::{InstanceDdlGuard, InstanceDdlLock, InstanceDdlOwner};
pub use lifecycle::shutdown::{
    InstanceQuiesceProof, InstanceShutdownDisposition, InstanceShutdownMode, InstanceShutdownReport,
};
pub use lifecycle::startup_report::{
    DatabaseStartupEntry, DatabaseStartupStatus, InstanceStartupDisposition, StartupIssue,
    StartupIssueKind, StartupPolicy, StartupReport, StartupReportCounts,
};
pub use metadata::instance_catalog::{
    DatabaseRecord, DatabaseRecordState, InstanceCatalog, FIRST_MANAGED_DATABASE_ID,
    INSTANCE_CATALOG_FORMAT_VERSION,
};
pub use metadata::instance_catalog_store::{InstanceCatalogStore, INSTANCE_CATALOG_KEY};
pub use metadata::instance_layout::InstanceLayout;
pub use metadata::instance_run_state::{
    InstanceLifecycleState, InstanceRunState, InstanceRunStateStore,
    INSTANCE_RUN_STATE_FORMAT_VERSION, INSTANCE_RUN_STATE_KEY,
};
pub use metadata::InstanceMetadata;
pub use recovery::consistency_report::{
    build_recovery_consistency_report, RecoveryConsistencyReport, RecoveryTableConsistencyReport,
};
pub use recovery::replay_handler::{
    needs_recovery, recover_database, recover_database_with_checkpoint, CatalogReplayHandler,
};
pub use runtime::connection_registry::{ConnectionHandle, ConnectionId, ConnectionRegistry};
pub use runtime::object_cache::{ObjectCache, ObjectCacheEntry};
pub use runtime::runtime_tuning::{RuntimeTuning, RuntimeTuningSnapshot};
pub use runtime::session_registry::{
    RegistryKey, SessionExecutionHandle, SessionExecutionRegistry,
};
pub use runtime::shutdown_reason::ConnectionShutdownReason;
pub use runtime::InstanceRuntime;
pub use storage_manager::{CheckpointOptions, DatabaseSize, MetadataBlockInfo, StorageManager};
pub use valid_checker::ValidChecker;

/// Shared runtime for instance-wide resources and managed databases.
///
/// Persistent state is rooted at `InstanceConfigOptions::instance_root`.
/// Managed databases are published through `DatabaseRegistry`.
pub struct Instance {
    /// Immutable boot-time configuration shared by the whole instance.
    pub(crate) boot_config: Arc<BootConfig>,
    /// Durable metadata owned by the instance.
    pub(crate) metadata: InstanceMetadata,
    /// Runtime-only shared resources.
    pub(crate) runtime: InstanceRuntime,
    /// Lifecycle control-plane state.
    pub(crate) lifecycle: lifecycle::InstanceLifecycle,
    /// Managed database registry and orchestration.
    pub(crate) database_service: ManagedDatabaseService,
}
