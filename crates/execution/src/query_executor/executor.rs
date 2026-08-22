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
use crate::query_executor::compiled::{CompiledExecutable, ExecutionRequest};
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
    pub fn execute(&self, request: ExecutionRequest) -> Result<ResultHandler> {
        let (compiled, parameter_bindings) = request.into_parts();
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

        let handler = match compiled.executable() {
            CompiledExecutable::Program(program) => {
                let query_memory_pool = self.create_query_memory_pool();
                self.execute_program(
                    program,
                    result_names,
                    result_types,
                    parameter_bindings,
                    allocator,
                    query_memory_pool,
                )?
            }
            CompiledExecutable::DirectDenseTopK(executable) => {
                let chunk = crate::query_executor::direct_dense_topk::execute(
                    &self.session,
                    executable,
                    parameter_bindings.as_ref(),
                    allocator.clone(),
                )?;
                ResultHandler::from_direct_chunk(
                    result_names,
                    result_types,
                    chunk,
                    allocator,
                    self.session.cancellation.child_execution_attempt(),
                )?
            }
        };
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
        program: &crate::pipeline::StatementProgram,
        result_names: Vec<String>,
        result_types: Vec<LogicalType>,
        params: Arc<ParameterBindings>,
        allocator: Arc<dyn paro_common::allocator::Allocator>,
        query_memory_pool: Arc<QueryMemoryPool>,
    ) -> Result<ResultHandler> {
        let execution = if result_types.is_empty() {
            program_executor::execute_program(
                self.session.clone(),
                program,
                params,
                query_memory_pool.clone(),
                allocator.clone(),
            )?
        } else {
            program_executor::start_program(
                self.session.clone(),
                program,
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
        // Workload governance and the process buffer pool may tighten a
        // session limit, but neither may loosen the statement-scoped ceiling
        // captured by the front end. A zero physical limit means unbounded.
        let configured_limit = governance
            .memory_quota
            .map(|quota| quota.min(self.session.limits.max_memory))
            .unwrap_or(self.session.limits.max_memory);
        let physical_limit = self.session.buffer_manager().get_max_memory();
        let hard_limit_bytes = if physical_limit == 0 {
            configured_limit
        } else {
            configured_limit.min(physical_limit)
        }
        .max(1);
        let Some(coordinator) = self.session.query_memory_coordinator() else {
            return Arc::new(QueryMemoryPool::new(hard_limit_bytes));
        };

        let pool = Arc::new(QueryMemoryPool::new(hard_limit_bytes));
        let query_id = coordinator.next_query_id();
        let spec = QueryMemoryBudgetSpec::new(
            query_id,
            governance.query_group.clone(),
            hard_limit_bytes,
            Some(hard_limit_bytes),
        );
        let target: Arc<dyn QueryMemoryTarget> = pool.clone();
        let registration = coordinator
            .clone()
            .register_query(spec, Arc::downgrade(&target));
        pool.attach_registration(registration);
        pool
    }
}

#[cfg(test)]
mod tests {
    use super::Executor;
    use paro_context::{RuntimeLimits, TestStatementContextBuilder};
    use std::sync::Arc;

    #[test]
    fn query_pool_honors_statement_memory_limit_without_coordinator() {
        let context = TestStatementContextBuilder::minimal()
            .with_limits(RuntimeLimits {
                max_memory: 4_096,
                ..RuntimeLimits::default()
            })
            .build();

        let pool = Executor::new(context).create_query_memory_pool();

        assert_eq!(pool.capacity_bytes(), 4_096);
    }

    #[test]
    fn query_pool_cannot_exceed_physical_limit_without_coordinator() {
        let context = TestStatementContextBuilder::minimal()
            .with_limits(RuntimeLimits {
                max_memory: 128 * 1024 * 1024,
                ..RuntimeLimits::default()
            })
            .build();

        let physical_limit = context.buffer_manager().get_max_memory();
        let pool = Executor::new(context).create_query_memory_pool();

        assert_eq!(physical_limit, 64 * 1024 * 1024);
        assert_eq!(pool.capacity_bytes(), physical_limit);
    }

    #[test]
    fn registered_query_pool_cannot_exceed_statement_memory_limit() {
        let mut context = TestStatementContextBuilder::minimal()
            .with_limits(RuntimeLimits {
                max_memory: 4_096,
                ..RuntimeLimits::default()
            })
            .build();
        let arbitrator = Arc::new(crate::memory_runtime::MemoryArbitrator::new(16_384));
        let context_mut = Arc::make_mut(&mut context);
        let services = Arc::make_mut(&mut context_mut.services);
        services.governance.memory_quota = Some(8_192);
        Arc::make_mut(&mut services.infra).query_memory_coordinator = Some(arbitrator);

        let pool = Executor::new(context).create_query_memory_pool();

        assert_eq!(pool.capacity_bytes(), 4_096);
        assert!(pool.registered_query_id().is_some());
    }
}
