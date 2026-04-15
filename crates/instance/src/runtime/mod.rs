// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::database::opener::DatabaseOpenContext;
use crate::file_system::DatabaseFileSystem;
use crate::{BootConfig, Instance};
use paro_function::scalar::cast::CastFunctionSet;
use paro_scheduler::scheduler::TaskScheduler;
use paro_storage::buffer::{
    BufferManager, BufferPool, TemporaryMemoryConfig, TemporaryMemoryManager,
};
use std::sync::Arc;

pub mod connection_manager;
pub mod object_cache;
pub mod runtime_tuning;

use self::connection_manager::ConnectionManager;
use self::object_cache::ObjectCache;
use self::runtime_tuning::RuntimeTuning;

/// Runtime-only instance resources shared across sessions and managed databases.
#[derive(Debug)]
pub struct InstanceRuntime {
    buffer_pool: Arc<BufferPool>,
    buffer_manager: Arc<dyn BufferManager>,
    scheduler: Arc<TaskScheduler>,
    temporary_memory_manager: Arc<TemporaryMemoryManager>,
    connection_manager: Arc<ConnectionManager>,
    object_cache: Arc<ObjectCache>,
    db_file_system: Arc<DatabaseFileSystem>,
    tuning: RuntimeTuning,
}

pub(crate) struct InstanceRuntimeResources {
    pub(crate) buffer_pool: Arc<BufferPool>,
    pub(crate) buffer_manager: Arc<dyn BufferManager>,
    pub(crate) scheduler: Arc<TaskScheduler>,
    pub(crate) temporary_memory_manager: Arc<TemporaryMemoryManager>,
    pub(crate) connection_manager: Arc<ConnectionManager>,
    pub(crate) object_cache: Arc<ObjectCache>,
    pub(crate) db_file_system: Arc<DatabaseFileSystem>,
}

impl InstanceRuntime {
    pub(crate) fn new(resources: InstanceRuntimeResources, tuning: RuntimeTuning) -> Self {
        let InstanceRuntimeResources {
            buffer_pool,
            buffer_manager,
            scheduler,
            temporary_memory_manager,
            connection_manager,
            object_cache,
            db_file_system,
        } = resources;
        Self {
            buffer_pool,
            buffer_manager,
            scheduler,
            temporary_memory_manager,
            connection_manager,
            object_cache,
            db_file_system,
            tuning,
        }
    }

    pub fn buffer_pool(&self) -> &Arc<BufferPool> {
        &self.buffer_pool
    }

    pub fn buffer_manager(&self) -> &Arc<dyn BufferManager> {
        &self.buffer_manager
    }

    pub fn scheduler(&self) -> &Arc<TaskScheduler> {
        &self.scheduler
    }

    pub fn temporary_memory_manager(&self) -> &Arc<TemporaryMemoryManager> {
        &self.temporary_memory_manager
    }

    pub fn connection_manager(&self) -> &Arc<ConnectionManager> {
        &self.connection_manager
    }

    pub fn object_cache(&self) -> &Arc<ObjectCache> {
        &self.object_cache
    }

    pub fn db_file_system(&self) -> &Arc<DatabaseFileSystem> {
        &self.db_file_system
    }

    pub fn tuning(&self) -> &RuntimeTuning {
        &self.tuning
    }

    pub fn database_open_context(&self, checkpoint_wal_size: u64) -> DatabaseOpenContext {
        DatabaseOpenContext {
            buffer_pool: Arc::clone(&self.buffer_pool),
            buffer_manager: Arc::clone(&self.buffer_manager),
            scheduler: Arc::clone(&self.scheduler),
            checkpoint_wal_size,
        }
    }

    pub fn refresh_temporary_memory_configuration(
        &self,
        session_threads: Option<usize>,
        force_external: bool,
    ) {
        let options = self.tuning.snapshot();
        let memory_limit = self.buffer_manager.get_max_memory();
        let num_threads = session_threads.unwrap_or_else(|| options.effective_max_threads());
        let num_connections = self.connection_manager.get_active_connection_count().max(1);
        let has_temp_dir =
            options.use_temporary_directory && self.buffer_pool.has_temporary_directory();

        self.temporary_memory_manager
            .update_configuration(TemporaryMemoryConfig {
                memory_limit,
                has_temporary_directory: has_temp_dir,
                num_threads,
                num_connections,
                query_max_memory: memory_limit,
                force_external,
            });
    }

    pub fn set_memory_limit(&self, limit: usize) -> paro_common::error::Result<()> {
        self.buffer_manager.set_memory_limit(limit)?;
        self.tuning.set_maximum_memory(limit);
        self.refresh_temporary_memory_configuration(None, false);
        Ok(())
    }

    pub fn set_temporary_directory(&self, path: String) -> paro_common::error::Result<()> {
        self.buffer_manager.set_temporary_directory(path.clone())?;
        self.tuning.set_temporary_directory(path);
        self.refresh_temporary_memory_configuration(None, false);
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
        self.refresh_temporary_memory_configuration(None, false);
        Ok(())
    }
}

impl Instance {
    pub fn get_buffer_pool(&self) -> &Arc<BufferPool> {
        self.runtime.buffer_pool()
    }

    pub fn get_buffer_manager(&self) -> &Arc<dyn BufferManager> {
        self.runtime.buffer_manager()
    }

    pub fn get_temporary_memory_manager(&self) -> &Arc<TemporaryMemoryManager> {
        self.runtime.temporary_memory_manager()
    }

    pub fn get_scheduler(&self) -> &Arc<TaskScheduler> {
        self.runtime.scheduler()
    }

    pub fn get_connection_manager(&self) -> &Arc<ConnectionManager> {
        self.runtime.connection_manager()
    }

    pub fn get_object_cache(&self) -> &Arc<ObjectCache> {
        self.runtime.object_cache()
    }

    pub fn get_file_system(&self) -> &Arc<DatabaseFileSystem> {
        self.runtime.db_file_system()
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

    pub fn refresh_temporary_memory_configuration(
        &self,
        session_threads: Option<usize>,
        force_external: bool,
    ) {
        self.runtime
            .refresh_temporary_memory_configuration(session_threads, force_external);
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
