// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::database::opener::DatabaseOpenContext;
use crate::file_system::DatabaseFileSystem;
use crate::{BootConfig, Instance};
use paro_execution::memory_runtime::{MemoryArbitrator, SystemReserve};
use paro_external::runtime::host::{ExternalRuntimeHost, PythonRuntimeStatus};
use paro_function::scalar::cast::CastFunctionSet;
use paro_scheduler::scheduler::TaskScheduler;
use paro_storage::buffer::{BufferManager, BufferPool, PageCache};
use paro_transaction::{CommitDrainWakePool, CommitDrainWakePoolOptions};
use std::sync::Arc;

pub mod connection_registry;
pub mod copy_stdin;
pub mod object_cache;
pub mod runtime_tuning;
pub mod session_registry;
pub mod shutdown_reason;

use self::connection_registry::ConnectionRegistry;
use self::copy_stdin::CopyStdinMetrics;
use self::object_cache::ObjectCache;
use self::runtime_tuning::RuntimeTuning;
use self::session_registry::SessionExecutionRegistry;

/// Runtime-only instance resources shared across sessions and managed databases.
#[derive(Debug)]
pub struct InstanceRuntime {
    buffer_pool: Arc<BufferPool>,
    page_cache: Arc<PageCache>,
    buffer_manager: Arc<dyn BufferManager>,
    scheduler: Arc<TaskScheduler>,
    memory_arbitrator: Arc<MemoryArbitrator>,
    system_reserve: Arc<SystemReserve>,
    connection_registry: Arc<ConnectionRegistry>,
    copy_stdin_metrics: Arc<CopyStdinMetrics>,
    session_registry: Arc<SessionExecutionRegistry>,
    object_cache: Arc<ObjectCache>,
    db_file_system: Arc<DatabaseFileSystem>,
    python_runtime: Arc<ExternalRuntimeHost>,
    commit_drain_wake_pool: Arc<CommitDrainWakePool>,
    tuning: RuntimeTuning,
}

pub(crate) struct InstanceRuntimeResources {
    pub(crate) buffer_pool: Arc<BufferPool>,
    pub(crate) buffer_manager: Arc<dyn BufferManager>,
    pub(crate) scheduler: Arc<TaskScheduler>,
    pub(crate) memory_arbitrator: Arc<MemoryArbitrator>,
    pub(crate) system_reserve: Arc<SystemReserve>,
    pub(crate) connection_registry: Arc<ConnectionRegistry>,
    pub(crate) session_registry: Arc<SessionExecutionRegistry>,
    pub(crate) object_cache: Arc<ObjectCache>,
    pub(crate) db_file_system: Arc<DatabaseFileSystem>,
    pub(crate) python_runtime: Arc<ExternalRuntimeHost>,
}

impl InstanceRuntime {
    pub(crate) fn new(resources: InstanceRuntimeResources, tuning: RuntimeTuning) -> Self {
        let InstanceRuntimeResources {
            buffer_pool,
            buffer_manager,
            scheduler,
            memory_arbitrator,
            system_reserve,
            connection_registry,
            session_registry,
            object_cache,
            db_file_system,
            python_runtime,
        } = resources;
        let page_cache = Arc::new(PageCache::new(Arc::clone(&buffer_pool)));
        Self {
            buffer_pool,
            page_cache,
            buffer_manager,
            scheduler,
            memory_arbitrator,
            system_reserve,
            connection_registry,
            copy_stdin_metrics: Arc::new(CopyStdinMetrics::default()),
            session_registry,
            object_cache,
            db_file_system,
            python_runtime,
            commit_drain_wake_pool: Arc::new(CommitDrainWakePool::new(
                CommitDrainWakePoolOptions::default(),
            )),
            tuning,
        }
    }

    pub fn buffer_pool(&self) -> &Arc<BufferPool> {
        &self.buffer_pool
    }

    pub fn page_cache(&self) -> &Arc<PageCache> {
        &self.page_cache
    }

    pub fn buffer_manager(&self) -> &Arc<dyn BufferManager> {
        &self.buffer_manager
    }

    pub fn scheduler(&self) -> &Arc<TaskScheduler> {
        &self.scheduler
    }

    pub fn memory_arbitrator(&self) -> &Arc<MemoryArbitrator> {
        &self.memory_arbitrator
    }

    pub fn system_reserve(&self) -> &Arc<SystemReserve> {
        &self.system_reserve
    }

    pub fn connection_registry(&self) -> &Arc<ConnectionRegistry> {
        &self.connection_registry
    }

    pub fn copy_stdin_metrics(&self) -> &Arc<CopyStdinMetrics> {
        &self.copy_stdin_metrics
    }

    pub fn session_registry(&self) -> &Arc<SessionExecutionRegistry> {
        &self.session_registry
    }

    pub fn object_cache(&self) -> &Arc<ObjectCache> {
        &self.object_cache
    }

    pub fn db_file_system(&self) -> &Arc<DatabaseFileSystem> {
        &self.db_file_system
    }

    pub fn python_runtime(&self) -> &Arc<ExternalRuntimeHost> {
        &self.python_runtime
    }

    pub fn python_runtime_status(&self) -> PythonRuntimeStatus {
        self.python_runtime.status()
    }

    pub fn tuning(&self) -> &RuntimeTuning {
        &self.tuning
    }

    pub fn database_open_context(
        &self,
        checkpoint: crate::config::CheckpointConfigOptions,
    ) -> DatabaseOpenContext {
        DatabaseOpenContext {
            buffer_pool: Arc::clone(&self.buffer_pool),
            buffer_manager: Arc::clone(&self.buffer_manager),
            scheduler: Arc::clone(&self.scheduler),
            commit_drain_wake_pool: Arc::clone(&self.commit_drain_wake_pool),
            checkpoint,
        }
    }

    pub fn set_memory_limit(&self, limit: usize) -> paro_common::error::Result<()> {
        self.buffer_manager.set_memory_limit(limit)?;
        self.tuning.set_maximum_memory(limit);
        self.memory_arbitrator.set_buffer_pool_limit(limit);
        Ok(())
    }

    pub fn set_temporary_directory(&self, path: String) -> paro_common::error::Result<()> {
        self.buffer_manager.set_temporary_directory(path.clone())?;
        self.tuning.set_temporary_directory(path);
        Ok(())
    }

    pub fn set_max_temp_directory_size(
        &self,
        limit: Option<usize>,
    ) -> paro_common::error::Result<()> {
        self.buffer_manager.set_swap_limit(limit)?;
        self.tuning.set_max_temp_directory_size(limit);
        Ok(())
    }

    pub fn set_threads(&self, threads: usize) -> paro_common::error::Result<()> {
        self.scheduler.set_threads(threads)?;
        self.tuning.set_maximum_threads(Some(threads));
        Ok(())
    }
}

impl Instance {
    pub fn get_buffer_pool(&self) -> &Arc<BufferPool> {
        self.runtime.buffer_pool()
    }

    pub fn get_page_cache(&self) -> &Arc<PageCache> {
        self.runtime.page_cache()
    }

    pub fn get_buffer_manager(&self) -> &Arc<dyn BufferManager> {
        self.runtime.buffer_manager()
    }

    pub fn get_memory_arbitrator(&self) -> &Arc<MemoryArbitrator> {
        self.runtime.memory_arbitrator()
    }

    pub fn get_system_reserve(&self) -> &Arc<SystemReserve> {
        self.runtime.system_reserve()
    }

    pub fn get_scheduler(&self) -> &Arc<TaskScheduler> {
        self.runtime.scheduler()
    }

    pub fn get_connection_registry(&self) -> &Arc<ConnectionRegistry> {
        self.runtime.connection_registry()
    }

    pub fn copy_stdin_metrics(&self) -> &Arc<CopyStdinMetrics> {
        self.runtime.copy_stdin_metrics()
    }

    pub fn get_session_registry(&self) -> &Arc<SessionExecutionRegistry> {
        self.runtime.session_registry()
    }

    pub fn get_object_cache(&self) -> &Arc<ObjectCache> {
        self.runtime.object_cache()
    }

    pub fn get_file_system(&self) -> &Arc<DatabaseFileSystem> {
        self.runtime.db_file_system()
    }

    pub fn python_runtime(&self) -> &Arc<ExternalRuntimeHost> {
        self.runtime.python_runtime()
    }

    pub fn python_runtime_status(&self) -> PythonRuntimeStatus {
        self.runtime.python_runtime_status()
    }

    pub fn boot_config(&self) -> &Arc<BootConfig> {
        &self.boot_config
    }

    pub fn runtime_tuning(&self) -> &RuntimeTuning {
        self.runtime.tuning()
    }

    pub fn cast_functions(&self) -> &Arc<CastFunctionSet> {
        &self.boot_config.cast_functions
    }

    pub fn set_memory_limit(&self, limit: usize) -> paro_common::error::Result<()> {
        self.lifecycle.admission.check(None)?;
        self.runtime.set_memory_limit(limit)
    }

    pub fn set_temporary_directory(&self, path: String) -> paro_common::error::Result<()> {
        self.lifecycle.admission.check(None)?;
        self.runtime.set_temporary_directory(path)
    }

    pub fn set_max_temp_directory_size(
        &self,
        limit: Option<usize>,
    ) -> paro_common::error::Result<()> {
        self.lifecycle.admission.check(None)?;
        self.runtime.set_max_temp_directory_size(limit)
    }

    pub fn set_threads(&self, threads: usize) -> paro_common::error::Result<()> {
        self.lifecycle.admission.check(None)?;
        self.runtime.set_threads(threads)
    }

    pub fn number_of_threads(&self) -> usize {
        self.runtime.scheduler().number_of_threads().max(0) as usize
    }

    pub fn is_in_memory(&self) -> bool {
        self.boot_config.is_in_memory()
    }
}
