// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::runtime::connection_registry::ConnectionRegistry;
use crate::runtime::object_cache::ObjectCache;
use crate::runtime::runtime_tuning::RuntimeTuning;
use crate::runtime::session_registry::SessionExecutionRegistry;
use crate::runtime::{InstanceRuntime, InstanceRuntimeResources};
use crate::{BootConfig, DatabaseFileSystem, InstanceConfig};
use paro_execution::memory_runtime::{MemoryArbitrator, SystemReserve};
use paro_external::runtime::host::ExternalRuntimeHost;
use paro_scheduler::scheduler::TaskScheduler;
use paro_storage::buffer::{BufferManager, StandardBufferManager};
use std::sync::Arc;

pub(crate) struct RuntimeResources {
    buffer_manager: Arc<dyn BufferManager>,
    scheduler: Arc<TaskScheduler>,
    memory_arbitrator: Arc<MemoryArbitrator>,
    system_reserve: Arc<SystemReserve>,
    connection_registry: Arc<ConnectionRegistry>,
    session_registry: Arc<SessionExecutionRegistry>,
    object_cache: Arc<ObjectCache>,
    db_file_system: Arc<DatabaseFileSystem>,
    python_runtime: Arc<ExternalRuntimeHost>,
}

impl RuntimeResources {
    pub(crate) fn build(
        boot_config: &Arc<BootConfig>,
        db_file_system: Arc<DatabaseFileSystem>,
    ) -> Self {
        let buffer_manager = boot_config
            .buffer_manager_override
            .clone()
            .unwrap_or_else(|| {
                Arc::new(StandardBufferManager::new_with_pool(
                    Arc::clone(&boot_config.buffer_pool),
                    ::paro_storage::buffer::DEFAULT_BLOCK_ALLOC_SIZE,
                    8,
                ))
            });
        let scheduler = Arc::new(TaskScheduler::new());
        let _ = scheduler.set_thread_affinity_mode(boot_config.pin_threads);
        let runtime_threads = boot_config.effective_max_threads();
        let _ = scheduler.set_threads(runtime_threads);
        paro_storage::index::hnsw::configure_hnsw_build_threads(runtime_threads);

        let memory_arbitrator = Arc::new(MemoryArbitrator::new(boot_config.initial_maximum_memory));
        let system_reserve = Arc::new(SystemReserve::new(memory_arbitrator.clone()));

        Self {
            buffer_manager,
            scheduler,
            memory_arbitrator,
            system_reserve,
            connection_registry: Arc::new(ConnectionRegistry::new()),
            session_registry: Arc::new(SessionExecutionRegistry::new()),
            object_cache: Arc::new(ObjectCache::new()),
            db_file_system,
            python_runtime: Arc::new(ExternalRuntimeHost::new()),
        }
    }

    pub(crate) fn into_runtime(
        self,
        config: &InstanceConfig,
        boot_config: &Arc<BootConfig>,
    ) -> InstanceRuntime {
        InstanceRuntime::new(
            InstanceRuntimeResources {
                buffer_pool: Arc::clone(&boot_config.buffer_pool),
                buffer_manager: self.buffer_manager,
                scheduler: self.scheduler,
                memory_arbitrator: self.memory_arbitrator,
                system_reserve: self.system_reserve,
                connection_registry: self.connection_registry,
                session_registry: self.session_registry,
                object_cache: self.object_cache,
                db_file_system: self.db_file_system,
                python_runtime: self.python_runtime,
            },
            RuntimeTuning::from_options(&config.options),
        )
    }
}
