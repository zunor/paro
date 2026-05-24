// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Query executor for typed runtime programs.

use std::sync::Arc;
use std::time::Instant;

use paro_common::allocator::{BufferAllocator, MemoryTag};
use paro_common::error::Result;
use paro_common::logging::targets;
use paro_common::types::LogicalType;
use paro_context::{QueryMemoryBudgetSpec, QueryMemoryTarget, StatementContext};
use paro_scheduler::scheduler::TaskScheduler;
use tracing::debug;

use crate::memory_runtime::QueryMemoryPool;
use crate::query_executor::compiled::{CompiledExecutable, CompiledStatement};
use crate::query_executor::program_executor;
use crate::runtime::ParameterBindings;

use super::stream::ResultHandler;

/// Executor holds a StatementContext Arc to avoid lifetime pollution.
pub struct Executor {
    /// Session context for accessing cluster and other resources.
    session: Arc<StatementContext>,
}

impl Executor {
    /// Create a new Executor for a query.
    pub fn new(session: Arc<StatementContext>) -> Self {
        Self { session }
    }

    /// Get the session context.
    pub fn session_context(&self) -> &StatementContext {
        self.session.as_ref()
    }

    /// Get the task scheduler.
    #[inline]
    pub fn task_scheduler(&self) -> &Arc<TaskScheduler> {
        self.session.scheduler()
    }

    /// Execute a typed runtime program and return a streaming result handler.
    pub fn execute(&self, compiled: CompiledStatement) -> Result<ResultHandler> {
        let result_names = compiled.result_names();
        let result_types = compiled.result_types();
        let is_query = !result_names.is_empty();
        let started_at = Instant::now();
        debug!(
            target: targets::EXECUTOR,
            is_query,
            result_columns = result_names.len(),
            "Execution started"
        );

        let allocator = Arc::new(BufferAllocator::new(
            self.session.buffer_pool().clone(),
            MemoryTag::Allocator,
        )) as Arc<dyn paro_common::allocator::Allocator>;

        let query_memory_pool = self.create_query_memory_pool();
        let parameter_bindings = compiled.parameter_bindings.clone();
        let CompiledExecutable::Program(program) = compiled.executable;
        let handler = self.execute_program(
            program,
            result_names,
            result_types,
            parameter_bindings,
            allocator,
            query_memory_pool,
        )?;
        debug!(
            target: targets::EXECUTOR,
            is_query,
            elapsed_ms = started_at.elapsed().as_millis(),
            "Execution pipelines completed"
        );
        Ok(handler)
    }

    fn execute_program(
        &self,
        program: crate::pipeline::StatementProgram,
        result_names: Vec<String>,
        result_types: Vec<LogicalType>,
        parameter_bindings: ParameterBindings,
        allocator: Arc<dyn paro_common::allocator::Allocator>,
        query_memory_pool: Arc<QueryMemoryPool>,
    ) -> Result<ResultHandler> {
        let params = Arc::new(parameter_bindings);
        let execution = if result_types.is_empty() {
            program_executor::execute_program(
                self.session.clone(),
                &program,
                params,
                query_memory_pool.clone(),
                allocator.clone(),
            )?
        } else {
            program_executor::start_program(
                self.session.clone(),
                &program,
                params,
                query_memory_pool.clone(),
                allocator.clone(),
            )?
        };
        ResultHandler::from_program_execution(
            result_names,
            result_types,
            execution,
            allocator,
            Some(query_memory_pool),
        )
    }

    fn create_query_memory_pool(&self) -> Arc<QueryMemoryPool> {
        let governance = self.session.query_governance();
        let Some(coordinator) = self.session.query_memory_coordinator() else {
            let requested_bytes = governance.memory_quota.unwrap_or(usize::MAX / 4).max(1);
            return Arc::new(QueryMemoryPool::new(requested_bytes));
        };

        let requested_bytes = governance
            .memory_quota
            .unwrap_or_else(|| coordinator.available_for_queries())
            .max(1);
        let pool = Arc::new(QueryMemoryPool::new(requested_bytes));
        let query_id = coordinator.next_query_id();
        let spec = QueryMemoryBudgetSpec::new(
            query_id,
            governance.query_group.clone(),
            requested_bytes,
            governance.memory_quota,
        );
        let target: Arc<dyn QueryMemoryTarget> = pool.clone();
        let registration = coordinator
            .clone()
            .register_query(spec, Arc::downgrade(&target));
        pool.attach_registration(registration);
        pool
    }
}
