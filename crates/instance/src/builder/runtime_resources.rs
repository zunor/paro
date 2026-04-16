// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::runtime::connection_registry::ConnectionRegistry;
use crate::runtime::object_cache::ObjectCache;
use crate::runtime::runtime_tuning::RuntimeTuning;
use crate::runtime::session_registry::SessionExecutionRegistry;
use crate::runtime::{InstanceRuntime, InstanceRuntimeResources};
use crate::{BootConfig, DatabaseFileSystem, InstanceConfig};
use paro_function::register_system_buffer_manager;
use paro_scheduler::scheduler::TaskScheduler;
use paro_storage::buffer::{BufferManager, StandardBufferManager, TemporaryMemoryManager};
use std::sync::Arc;

pub(crate) struct RuntimeResources {
    buffer_manager: Arc<dyn BufferManager>,
    scheduler: Arc<TaskScheduler>,
    temporary_memory_manager: Arc<TemporaryMemoryManager>,
    connection_registry: Arc<ConnectionRegistry>,
    session_registry: Arc<SessionExecutionRegistry>,
    object_cache: Arc<ObjectCache>,
    db_file_system: Arc<DatabaseFileSystem>,
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
        register_system_buffer_manager(Arc::clone(&buffer_manager));

        let scheduler = Arc::new(TaskScheduler::new());
        let _ = scheduler.set_thread_affinity_mode(boot_config.pin_threads);
        let _ = scheduler.set_threads(boot_config.effective_max_threads());

        Self {
            buffer_manager,
            scheduler,
            temporary_memory_manager: Arc::new(TemporaryMemoryManager::with_buffer_pool(
                Arc::downgrade(&boot_config.buffer_pool),
            )),
            connection_registry: Arc::new(ConnectionRegistry::new()),
            session_registry: Arc::new(SessionExecutionRegistry::new()),
            object_cache: Arc::new(ObjectCache::new()),
            db_file_system,
        }
    }

    pub(crate) fn into_runtime(
        self,
        config: &InstanceConfig,
        boot_config: &Arc<BootConfig>,
    ) -> InstanceRuntime {
        let runtime = InstanceRuntime::new(
            InstanceRuntimeResources {
                buffer_pool: Arc::clone(&boot_config.buffer_pool),
                buffer_manager: self.buffer_manager,
                scheduler: self.scheduler,
                temporary_memory_manager: self.temporary_memory_manager,
                connection_registry: self.connection_registry,
                session_registry: self.session_registry,
                object_cache: self.object_cache,
                db_file_system: self.db_file_system,
            },
            RuntimeTuning::from_options(&config.options),
        );
        runtime.refresh_temporary_memory_configuration(None, false);
        runtime
    }
}
