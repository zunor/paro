// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Per-task execution context passed to physical operators.

use crate::pipeline::pipeline::Pipeline;
use crate::thread_context::ThreadContext;
use paro_common::allocator::{Allocator, ArenaAllocator, BufferAllocator, MemoryTag};
use paro_common::error::Result;
use paro_context::StatementContext;
use std::sync::Arc;

/// Execution context for operator execution.
///
/// The context holds shared statement state, the current thread-local state, and
/// an optional reference to the active pipeline.
pub struct ExecutionContext<'a> {
    /// The session context (Arc for flexible lifetime management).
    pub session: Arc<StatementContext>,

    /// The thread-local context.
    pub thread: &'a ThreadContext,

    /// Reference to the pipeline (optional).
    pub pipeline: Option<&'a Pipeline>,
}

impl<'a> ExecutionContext<'a> {
    /// Create a new ExecutionContext.
    ///
    /// # Arguments
    ///
    /// * `session` - Arc to the session context
    /// * `thread` - Reference to the thread context
    /// * `pipeline` - Optional reference to the pipeline
    pub fn new(
        session: Arc<StatementContext>,
        thread: &'a ThreadContext,
        pipeline: Option<&'a Pipeline>,
    ) -> Self {
        Self {
            session,
            thread,
            pipeline,
        }
    }

    /// Get the buffer pool.
    #[inline]
    pub fn buffer_pool(&self) -> &Arc<paro_storage::buffer::BufferPool> {
        self.session.buffer_pool()
    }

    /// Get the temporary memory manager.
    #[inline]
    pub fn temporary_memory_manager(&self) -> &Arc<paro_storage::buffer::TemporaryMemoryManager> {
        self.session.temporary_memory_manager()
    }

    /// Get the number of threads.
    #[inline]
    pub fn num_threads(&self) -> usize {
        self.session.number_of_threads()
    }

    /// Get the metadata provider.
    #[inline]
    pub fn metadata_provider(&self) -> Option<Arc<paro_storage::meta::TabletMetaManager>> {
        self.session.metadata_provider()
    }

    /// Get the thread ID.
    #[inline]
    pub fn thread_id(&self) -> usize {
        self.thread.thread_id()
    }

    /// Check if the query has been interrupted.
    #[inline]
    pub fn is_interrupted(&self) -> bool {
        self.session.is_interrupted()
    }

    #[inline]
    pub fn check_cancelled(&self) -> Result<()> {
        self.session.cancellation.check()
    }

    /// Get the current database name.
    #[inline]
    pub fn current_database(&self) -> &str {
        self.session.current_database()
    }

    /// Get the current schema name.
    #[inline]
    pub fn current_schema(&self) -> &str {
        self.session.current_schema()
    }

    /// Get the current user name.
    #[inline]
    pub fn current_user(&self) -> &str {
        self.session.current_user()
    }

    /// Get the catalog.
    #[inline]
    pub fn catalog(&self) -> Arc<paro_catalog::database_catalog::ParoCatalog> {
        self.session.catalog()
    }

    /// Get the catalog transaction.
    #[inline]
    pub fn catalog_txn_view(&self) -> paro_catalog::mvcc::CatalogSnapshot {
        self.session.catalog_txn_view()
    }

    /// Get an allocator for the given memory tag.
    #[inline]
    pub fn allocator(&self, tag: MemoryTag) -> Arc<dyn Allocator> {
        if tag == MemoryTag::Allocator {
            self.session.buffer_allocator()
        } else {
            Arc::new(BufferAllocator::new(
                self.session.buffer_manager().clone()
                    as Arc<dyn paro_common::allocator::BufferManager>,
                tag,
            ))
        }
    }

    /// Create a new ArenaAllocator for batch allocation.
    #[inline]
    pub fn arena_allocator(&self) -> ArenaAllocator {
        ArenaAllocator::new(self.allocator(MemoryTag::Allocator))
    }

    /// Get the transaction ID.
    ///
    #[inline]
    pub fn transaction_id(&self) -> u64 {
        self.session.transaction_id()
    }

    /// Get the transaction start time.
    ///
    #[inline]
    pub fn transaction_start_time(&self) -> u64 {
        self.session.transaction_start_time()
    }

    /// Get the transaction visible version (MVCC snapshot).
    #[inline]
    pub fn transaction_visible_version(&self) -> u64 {
        self.session.transaction_visible_version()
    }

    /// Get the active transaction handle if present.
    #[inline]
    pub fn active_transaction(
        &self,
    ) -> Option<std::sync::Arc<paro_storage::transaction::txn::Transaction>> {
        self.session.active_transaction()
    }
}
