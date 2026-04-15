// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_scheduler::scheduler::TaskScheduler;
use paro_storage::buffer::{BufferManager, BufferPool, TemporaryMemoryManager};
use std::sync::Arc;

#[derive(Clone)]
pub struct ExecutionResources {
    pub scheduler: Arc<TaskScheduler>,
    pub buffer_pool: Arc<BufferPool>,
    pub buffer_manager: Arc<dyn BufferManager>,
    pub temporary_memory_manager: Arc<TemporaryMemoryManager>,
}

impl std::fmt::Debug for ExecutionResources {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionResources").finish_non_exhaustive()
    }
}
