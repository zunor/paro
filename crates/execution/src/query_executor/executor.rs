// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Query executor for typed runtime programs.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use paro_common::allocator::{BufferAllocator, MemoryTag};
use paro_common::error::Result;
use paro_common::logging::targets;
use paro_common::types::LogicalType;
use paro_context::{
    AdmissionFallback, AdmissionResult, ExecutionReceiptHandle, ExecutionReceiptStart,
    MemoryCompletionReceipt, QueryMemoryBudgetSpec, QueryMemoryTarget, ResourceReceipt,
    StatementContext,
};
use paro_scheduler::scheduler::TaskScheduler;
use tracing::debug;

use crate::memory_runtime::{ExecutionLease, QueryMemoryPool};
use crate::pipeline::{AdmissionSelection, StatementProgram};
use crate::query_executor::compiled::{CompiledStatement, ExecutionRequest};
use crate::query_executor::program_executor;
use crate::runtime::ParameterBindings;

use super::stream::ResultHandler;

static NEXT_STANDALONE_EXECUTION_ID: AtomicU64 = AtomicU64::new(1_u64 << 63);

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
        let statement_trace = self.session.statement_trace();
        if let Some(trace) = &statement_trace {
            trace.record_event("execution", "executor_entry");
        }
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
        // Portfolio admission models configured capacity. Runtime readiness is
        // an execution capability with its own typed diagnostics; collapsing
        // the two here turns a precise unavailable/misconfigured error into a
        // misleading "no physical variant" planning failure.
        let external_worker_slots = self.session.python_execution_slot_limit();
        if let Some(trace) = &statement_trace {
            trace.record_event("admission", "admission_entry");
        }
        let admission_started = Instant::now();
        let admitted = self.admit_program(&compiled, &query_memory_pool, external_worker_slots);
        if let Some(trace) = &statement_trace {
            trace.record_span("admission", "lower_and_admit", admission_started);
        }
        let (program, execution_lease, selection, fallback) = match admitted {
            Ok(admitted) => admitted,
            Err(error) => {
                let admission = if error.sqlstate().is_resource_error() {
                    AdmissionResult::Infeasible
                } else {
                    AdmissionResult::Failed
                };
                let receipt = self.session.diagnostics.begin_execution_receipt(ExecutionReceiptStart {
                    artifact_identity: compiled.artifact_identity(),
                    expected_class: compiled.expected_grant_class(),
                    actual_class: None,
                    actual_fingerprint: None,
                    resources: None,
                    admission,
                    fallback: None,
                });
                receipt.record_error(error.to_string());
                drop(receipt);
                if let Some(trace) = &statement_trace {
                    trace.record_event("admission", "admission_error");
                }
                return Err(error);
            }
        };
        let receipt = self.session.diagnostics.begin_execution_receipt(ExecutionReceiptStart {
            artifact_identity: compiled.artifact_identity(),
            expected_class: compiled.expected_grant_class(),
            actual_class: selection.map(|selection| selection.resources.class.0),
            actual_fingerprint: selection.map(|selection| fingerprint_words(selection.physical_fingerprint)),
            resources: selection.map(|selection| resource_receipt(selection.resources)),
            admission: AdmissionResult::Selected,
            fallback,
        });
        if let Some(lease) = execution_lease {
            query_memory_pool.install_execution_lease(lease)?;
            if let Some(trace) = &statement_trace {
                trace.record_event("admission", "resource_grant_published");
            }
        }
        if let Some(trace) = &statement_trace {
            trace.record_event("execution", "pipeline_dispatch_entry");
        }
        let handler = match self.execute_program(
            &program,
            result_names,
            result_types,
            parameter_bindings,
            allocator,
            query_memory_pool,
            Some(receipt.clone()),
        ) {
            Ok(handler) => handler,
            Err(error) => {
                receipt.fail(error.to_string());
                return Err(error);
            }
        };
        receipt.image_ready();
        if let Some(trace) = &statement_trace {
            trace.record_event("execution", "result_handler_ready");
            trace.record_event("execution", "executor_return");
        }
        debug!(
            target: targets::EXECUTOR,
            is_query,
            elapsed_ms = started_at.elapsed().as_millis(),
            "Execution pipelines completed"
        );
        Ok(handler)
    }

    /// Resolve one immutable portfolio and acquire every selected resource.
    ///
    /// External capacity is acquired before memory so a worker race cannot
    /// strand a capacity floor. A memory race drops the external lease and
    /// monotonically retries a lower operating point. The returned lease is
    /// published exactly once before physical runtime construction.
    fn admit_program(
        &self,
        compiled: &CompiledStatement,
        query_memory_pool: &Arc<QueryMemoryPool>,
        available_external_worker_slots: u16,
    ) -> Result<(
        StatementProgram,
        Option<ExecutionLease>,
        Option<AdmissionSelection>,
        Option<AdmissionFallback>,
    )> {
        let available_parallel_tasks =
            u16::try_from(self.session.number_of_threads()).unwrap_or(u16::MAX);
        let mut memory_ceiling =
            u64::try_from(query_memory_pool.capacity_bytes()).unwrap_or(u64::MAX);
        let mut external_ceiling = available_external_worker_slots;
        let mut admission_attempt = 0_u64;
        let mut fallback = None;
        loop {
            admission_attempt = admission_attempt.saturating_add(1);
            if let Some(trace) = self.session.statement_trace() {
                trace.record_value("admission", "admission_attempt", admission_attempt);
            }
            let (program, selection) = compiled.program().admit_for_execution_with_selection(
                memory_ceiling,
                available_parallel_tasks,
                external_ceiling,
                &|plan| super::compiled::physical_plan_dependencies_available(plan, &self.session),
            )?;
            let Some(resources) = program.execution_resources() else {
                if let Some(trace) = self.session.statement_trace() {
                    trace.record_event("admission", "resource_contract_absent");
                }
                return Ok((program, None, selection, fallback));
            };
            if let Some(trace) = self.session.statement_trace() {
                trace.record_value(
                    "admission",
                    "working_set_memory_bytes",
                    resources.working_set_memory_bytes,
                );
                trace.record_value(
                    "admission",
                    "max_parallel_tasks",
                    u64::from(resources.max_parallel_tasks),
                );
                trace.record_value(
                    "admission",
                    "external_worker_slots",
                    u64::from(resources.external_worker_slots),
                );
            }
            let external_workers = if resources.external_worker_slots == 0 {
                None
            } else {
                let query_id = query_memory_pool
                    .registered_query_id()
                    .unwrap_or_else(|| NEXT_STANDALONE_EXECUTION_ID.fetch_add(1, Ordering::AcqRel));
                match self
                    .session
                    .try_acquire_python_worker_slots(query_id, resources.external_worker_slots)?
                {
                    Some(lease) => Some(lease),
                    None => {
                        if let Some(trace) = self.session.statement_trace() {
                            trace.record_event("admission", "external_capacity_retry");
                        }
                        external_ceiling = 0;
                        fallback = Some(AdmissionFallback::ExternalCapacity);
                        continue;
                    }
                }
            };
            let working_set =
                usize::try_from(resources.working_set_memory_bytes).unwrap_or(usize::MAX);
            if query_memory_pool.try_reserve_minimum_capacity(working_set)? {
                return Ok((
                    program,
                    Some(ExecutionLease::new(resources, external_workers)?),
                    selection,
                    fallback,
                ));
            }
            drop(external_workers);
            if resources.working_set_memory_bytes == 0 {
                return Err(paro_common::error::out_of_memory(
                    "unable to reserve a zero-byte physical operating point after admission changed",
                ));
            }
            memory_ceiling = memory_ceiling.min(resources.working_set_memory_bytes - 1);
            fallback = Some(AdmissionFallback::LowerResourceClass);
            if let Some(trace) = self.session.statement_trace() {
                trace.record_event("admission", "memory_capacity_retry");
            }
        }
    }

    fn execute_program(
        &self,
        program: &crate::pipeline::StatementProgram,
        result_names: Vec<String>,
        result_types: Vec<LogicalType>,
        params: Arc<ParameterBindings>,
        allocator: Arc<dyn paro_common::allocator::Allocator>,
        query_memory_pool: Arc<QueryMemoryPool>,
        receipt: Option<ExecutionReceiptHandle>,
    ) -> Result<ResultHandler> {
        let pipeline_started = Instant::now();
        let execution =
            if result_types.is_empty() && !self.session.input.requires_background_execution() {
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
        if let Some(trace) = self.session.statement_trace() {
            trace.record_span("execution", "pipeline_initialized", pipeline_started);
        }
        ResultHandler::from_program_execution_with_receipt(
            result_names,
            result_types,
            execution,
            allocator,
            Some(query_memory_pool),
            receipt,
        )
    }

    fn create_query_memory_pool(&self) -> Arc<QueryMemoryPool> {
        let governance = self.session.query_governance();
        // Workload governance and the process buffer pool may tighten a
        // session limit, but neither may loosen the statement-scoped ceiling
        // captured by the front end. A zero physical limit means unbounded.
        let statement_limit = if self.session.limits.max_memory > 0 {
            self.session.limits.max_memory
        } else {
            usize::MAX
        };
        let configured_limit = governance
            .memory_quota
            .map(|quota| quota.min(statement_limit))
            .unwrap_or(statement_limit);
        let physical_limit = self.session.buffer_manager().get_max_memory();
        let hard_limit_bytes = if physical_limit == 0 {
            configured_limit
        } else {
            configured_limit.min(physical_limit)
        };
        if hard_limit_bytes == usize::MAX {
            return Arc::new(QueryMemoryPool::unbounded());
        }
        let hard_limit_bytes = hard_limit_bytes.max(1);
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

fn fingerprint_words(fingerprint: paro_optimizer::physical::Fingerprint) -> [u64; 2] {
    [(fingerprint.0 >> 64) as u64, fingerprint.0 as u64]
}

fn resource_receipt(
    resources: paro_optimizer::physical::ExecutionResourceContract,
) -> ResourceReceipt {
    let memory_completion = match resources.memory_completion {
        paro_optimizer::physical::MemoryCompletion::Guaranteed => MemoryCompletionReceipt::Guaranteed,
        completion => match completion.uncapped_memory_demand() {
            Some(paro_optimizer::physical::UncappedMemoryDemand::KnownBytes(bytes)) =>
                MemoryCompletionReceipt::RuntimeCappedKnown { uncapped_memory_bytes: bytes },
            Some(paro_optimizer::physical::UncappedMemoryDemand::Unbounded) | None =>
                MemoryCompletionReceipt::RuntimeCappedUnbounded,
        },
    };
    ResourceReceipt {
        class: resources.class.0,
        minimum_memory_bytes: resources.minimum_memory_bytes,
        working_set_memory_bytes: resources.working_set_memory_bytes,
        memory_ceiling_bytes: resources.memory_ceiling_bytes,
        memory_completion,
        max_parallel_tasks: resources.max_parallel_tasks,
        external_worker_slots: resources.external_worker_slots,
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
    fn zero_statement_limit_uses_physical_capacity_instead_of_one_byte() {
        let context = TestStatementContextBuilder::minimal()
            .with_limits(RuntimeLimits::default())
            .build();
        let physical_limit = context.buffer_manager().get_max_memory();

        let pool = Executor::new(context).create_query_memory_pool();

        assert!(physical_limit > 1);
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
