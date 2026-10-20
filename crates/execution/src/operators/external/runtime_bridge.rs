// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_external::routine::identity::RoutineCallIdentity;
use paro_external::routine::spec::RoutineSpec;
use paro_external::runtime::dispatch::policy::ExternalDispatchPolicy;
use paro_planner::logical::operator::external_project::ExternalProjectExpression;
use paro_planner::physical::ExternalRoutineDescriptor;

#[cfg(test)]
use crate::expression_executor::executor::{ExpressionExecutor, VectorKernelInput};

use crate::memory_runtime::OperatorMemoryScope;
use crate::runtime::QueryRuntimeContext;

use super::batching::SubmissionBatchPolicy;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeBridgeExplainInfo {
    pub language: String,
    pub backend: String,
    pub env_artifact_id: Option<String>,
    pub artifact_validation_state: String,
}

impl RuntimeBridgeExplainInfo {
    pub fn default_python_flight() -> Self {
        Self {
            language: "python".to_string(),
            backend: "arrow-flight".to_string(),
            env_artifact_id: None,
            artifact_validation_state: "pending-runtime-bind".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RuntimeWarmState {
    #[default]
    Warm,
    Cold,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeBridgeMetrics {
    pub worker_acquire_time_us: u64,
    pub queue_wait_us: u64,
    pub execution_time_us: u64,
    pub encode_decode_time_us: u64,
    pub data_plane_bytes: u64,
    pub cache_hit: bool,
    pub warm_state: RuntimeWarmState,
    pub retired_count: u64,
    pub output_rows: u64,
    pub output_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct RuntimeBridgeResponse {
    pub output_batches: Vec<Chunk>,
    pub metrics: RuntimeBridgeMetrics,
}

pub struct ProjectSubmission<'a> {
    pub batch_id: u64,
    pub input: &'a Chunk,
    pub expressions: &'a [ExternalProjectExpression],
    pub routines: &'a [ExternalRoutineDescriptor],
    pub force_tail_flush: bool,
    pub batch_policy: &'a SubmissionBatchPolicy,
}

impl fmt::Debug for ProjectSubmission<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectSubmission")
            .field("batch_id", &self.batch_id)
            .field("input_rows", &self.input.size())
            .field("expression_count", &self.expressions.len())
            .field("force_tail_flush", &self.force_tail_flush)
            .finish()
    }
}

pub struct TableSubmission<'a> {
    pub batch_id: u64,
    pub input: &'a Chunk,
    pub routine: &'a ExternalRoutineDescriptor,
    pub output_types: &'a [LogicalType],
    pub lateral: bool,
    pub parameterized: bool,
}

impl fmt::Debug for TableSubmission<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableSubmission")
            .field("batch_id", &self.batch_id)
            .field("input_rows", &self.input.size())
            .field("output_types", &self.output_types)
            .field("lateral", &self.lateral)
            .field("parameterized", &self.parameterized)
            .finish()
    }
}

pub trait ExternalProjectExecutor: Send + Sync + fmt::Debug {
    fn execute(
        &self,
        ctx: &QueryRuntimeContext,
        submission: &ProjectSubmission<'_>,
        memory: &OperatorMemoryScope<'_>,
    ) -> Result<RuntimeBridgeResponse>;
}

pub trait ExternalTableExecutor: Send + Sync + fmt::Debug {
    fn execute(
        &self,
        ctx: &QueryRuntimeContext,
        submission: &TableSubmission<'_>,
        memory: &OperatorMemoryScope<'_>,
    ) -> Result<RuntimeBridgeResponse>;
}

#[cfg(test)]
#[derive(Debug, Default)]
pub struct TestProjectExecutor;

#[cfg(test)]
impl ExternalProjectExecutor for TestProjectExecutor {
    fn execute(
        &self,
        ctx: &QueryRuntimeContext,
        submission: &ProjectSubmission<'_>,
        memory: &OperatorMemoryScope<'_>,
    ) -> Result<RuntimeBridgeResponse> {
        ctx.session.ensure_python_runtime_ready_for_execution()?;
        let started_at = Instant::now();
        let expressions = submission
            .expressions
            .iter()
            .map(|expression| expression.expression.clone())
            .collect::<Vec<_>>();
        let mut executor = ExpressionExecutor::with_expressions(&expressions);
        let mut generated = Chunk::try_new(memory.accounted_allocator_for(
            MemoryTag::ExternalRuntimeHost,
            paro_common::memory::MemoryAccountingClass::NonRevocable,
        ))?;
        executor.execute_all_kernel(
            VectorKernelInput::from_chunk(submission.input),
            ctx,
            &mut generated,
        )?;

        let elapsed_us = started_at.elapsed().as_micros() as u64;
        let output_bytes = SubmissionBatchPolicy::estimate_chunk_bytes(&generated);
        let input_bytes = SubmissionBatchPolicy::estimate_chunk_bytes(submission.input);
        Ok(RuntimeBridgeResponse {
            output_batches: vec![generated],
            metrics: RuntimeBridgeMetrics {
                worker_acquire_time_us: 0,
                queue_wait_us: 0,
                execution_time_us: elapsed_us.max(1),
                encode_decode_time_us: 0,
                data_plane_bytes: input_bytes.saturating_add(output_bytes),
                cache_hit: false,
                warm_state: RuntimeWarmState::Warm,
                retired_count: 0,
                output_rows: submission.input.size() as u64,
                output_bytes,
            },
        })
    }
}

#[derive(Debug, Default)]
pub struct PythonFlightProjectExecutor;

impl ExternalProjectExecutor for PythonFlightProjectExecutor {
    fn execute(
        &self,
        ctx: &QueryRuntimeContext,
        submission: &ProjectSubmission<'_>,
        memory: &OperatorMemoryScope<'_>,
    ) -> Result<RuntimeBridgeResponse> {
        ctx.session.ensure_python_runtime_ready_for_execution()?;
        let started_at = Instant::now();
        let allocator = memory.accounted_allocator_for(
            MemoryTag::ExternalRuntimeHost,
            paro_common::memory::MemoryAccountingClass::NonRevocable,
        );
        let mut vectors = Vec::with_capacity(submission.expressions.len());
        let mut total_data_plane_bytes =
            SubmissionBatchPolicy::estimate_chunk_bytes(submission.input);

        for expression in submission.expressions {
            vectors.push(super::expression::evaluate(
                &expression.expression,
                submission.input,
                ctx,
                memory,
            )?);
        }

        let generated = Chunk::from_vectors(vectors, allocator);
        total_data_plane_bytes = total_data_plane_bytes
            .saturating_add(SubmissionBatchPolicy::estimate_chunk_bytes(&generated));
        let elapsed_us = started_at.elapsed().as_micros() as u64;
        Ok(RuntimeBridgeResponse {
            output_batches: vec![generated.clone()],
            metrics: RuntimeBridgeMetrics {
                worker_acquire_time_us: 0,
                queue_wait_us: 0,
                execution_time_us: elapsed_us.max(1),
                encode_decode_time_us: elapsed_us.max(1),
                data_plane_bytes: total_data_plane_bytes,
                cache_hit: false,
                warm_state: RuntimeWarmState::Cold,
                retired_count: 0,
                output_rows: generated.size() as u64,
                output_bytes: SubmissionBatchPolicy::estimate_chunk_bytes(&generated),
            },
        })
    }
}

#[derive(Debug, Default)]
pub struct PythonFlightTableExecutor;

impl ExternalTableExecutor for PythonFlightTableExecutor {
    fn execute(
        &self,
        ctx: &QueryRuntimeContext,
        submission: &TableSubmission<'_>,
        memory: &OperatorMemoryScope<'_>,
    ) -> Result<RuntimeBridgeResponse> {
        ctx.session.ensure_python_runtime_ready_for_execution()?;
        let started_at = Instant::now();
        let spec = submission.routine.spec.as_ref().ok_or_else(|| {
            paro_error::internal("external table routine is missing executable spec")
        })?;
        let allocator = memory.accounted_allocator_for(
            MemoryTag::ExternalRuntimeHost,
            paro_common::memory::MemoryAccountingClass::NonRevocable,
        );
        let arguments = Chunk::try_from_arc_vectors_with_cardinality(
            submission
                .input
                .view()
                .columns()
                .iter()
                .take(spec.arguments.len())
                .cloned()
                .collect(),
            submission.input.size(),
            allocator.clone(),
        )?;
        let vectors = execute_python(ctx, spec, &arguments, memory)?;
        let output = Chunk::from_vectors(vectors, allocator);
        let elapsed_us = started_at.elapsed().as_micros() as u64;
        let input_bytes = SubmissionBatchPolicy::estimate_chunk_bytes(submission.input);
        let output_bytes = SubmissionBatchPolicy::estimate_chunk_bytes(&output);
        Ok(RuntimeBridgeResponse {
            output_batches: vec![output.clone()],
            metrics: RuntimeBridgeMetrics {
                worker_acquire_time_us: 0,
                queue_wait_us: 0,
                execution_time_us: elapsed_us.max(1),
                encode_decode_time_us: elapsed_us.max(1),
                data_plane_bytes: input_bytes.saturating_add(output_bytes),
                cache_hit: false,
                warm_state: RuntimeWarmState::Cold,
                retired_count: 0,
                output_rows: output.size() as u64,
                output_bytes,
            },
        })
    }
}

#[derive(Debug)]
pub struct ExternalRuntimeBridge {
    explain: RuntimeBridgeExplainInfo,
    dispatch_policy: ExternalDispatchPolicy,
    project_executor: Arc<dyn ExternalProjectExecutor>,
    table_executor: Arc<dyn ExternalTableExecutor>,
}

impl ExternalRuntimeBridge {
    pub fn new(
        explain: RuntimeBridgeExplainInfo,
        dispatch_policy: ExternalDispatchPolicy,
        project_executor: Arc<dyn ExternalProjectExecutor>,
        table_executor: Arc<dyn ExternalTableExecutor>,
    ) -> Self {
        Self {
            explain,
            dispatch_policy,
            project_executor,
            table_executor,
        }
    }

    pub fn default_bridge() -> Self {
        Self::new(
            RuntimeBridgeExplainInfo::default_python_flight(),
            ExternalDispatchPolicy::default(),
            Arc::new(PythonFlightProjectExecutor),
            Arc::new(PythonFlightTableExecutor),
        )
    }

    pub fn explain(&self) -> &RuntimeBridgeExplainInfo {
        &self.explain
    }

    pub fn dispatch_policy(&self) -> &ExternalDispatchPolicy {
        &self.dispatch_policy
    }

    pub fn execute_project(
        &self,
        ctx: &QueryRuntimeContext,
        submission: &ProjectSubmission<'_>,
        memory: &OperatorMemoryScope<'_>,
    ) -> Result<RuntimeBridgeResponse> {
        self.dispatch(ctx, || {
            self.project_executor.execute(ctx, submission, memory)
        })
    }

    pub fn execute_table(
        &self,
        ctx: &QueryRuntimeContext,
        submission: &TableSubmission<'_>,
        memory: &OperatorMemoryScope<'_>,
    ) -> Result<RuntimeBridgeResponse> {
        self.dispatch(ctx, || self.table_executor.execute(ctx, submission, memory))
    }

    fn dispatch(
        &self,
        ctx: &QueryRuntimeContext,
        execute: impl FnOnce() -> Result<RuntimeBridgeResponse>,
    ) -> Result<RuntimeBridgeResponse> {
        let gate = ctx.memory.external_dispatch_gate().ok_or_else(|| {
            paro_error::internal("external runtime dispatch requires an admitted worker-slot lease")
        })?;
        let waiting_since = Instant::now();
        let _permit = loop {
            ctx.cancellation.check()?;
            if let Some(permit) = gate.try_acquire() {
                break permit;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        };
        let wait_us = waiting_since.elapsed().as_micros().min(u64::MAX as u128) as u64;
        let mut response = execute()?;
        response.metrics.worker_acquire_time_us = response
            .metrics
            .worker_acquire_time_us
            .saturating_add(wait_us);
        response.metrics.queue_wait_us = response.metrics.queue_wait_us.saturating_add(wait_us);
        Ok(response)
    }
}

pub fn format_identity_label(identity: &RoutineCallIdentity, fallback_label: &str) -> String {
    match identity {
        RoutineCallIdentity::Catalog {
            routine_id,
            generation,
        } => format!("{fallback_label}[{}@{}]", routine_id.raw(), generation),
        RoutineCallIdentity::Builtin { intrinsic, .. } => format!("{intrinsic:?}"),
    }
}

pub(super) fn execute_python(
    ctx: &QueryRuntimeContext,
    spec: &RoutineSpec,
    input: &Chunk,
    memory: &OperatorMemoryScope<'_>,
) -> Result<Vec<paro_common::vector::Vector>> {
    use paro_external::python::{codec, definition::Definition, TRANSPORT_WORKING_SET_BYTES};
    let (definition, input_schema, output_schema) = Definition::from_spec(spec)?;
    let allocator = memory.accounted_allocator_for(
        MemoryTag::ExternalRuntimeHost,
        paro_common::memory::MemoryAccountingClass::NonRevocable,
    );
    if input.is_empty() {
        return match &spec.return_type {
            paro_external::routine::spec::RoutineReturn::Scalar(ty) => {
                Ok(vec![paro_common::vector::Vector::try_new(
                    ty.clone(),
                    0,
                    allocator,
                )?])
            }
            paro_external::routine::spec::RoutineReturn::Table(fields) => fields
                .iter()
                .map(|field| {
                    paro_common::vector::Vector::try_new(
                        field.data_type.clone(),
                        0,
                        allocator.clone(),
                    )
                })
                .collect(),
        };
    }
    if definition.strict {
        use paro_common::vector::{SelectionVector, Vector};
        let selected = (0..input.size())
            .filter(|row| {
                input
                    .view()
                    .columns()
                    .iter()
                    .all(|column| !column.is_null(*row))
            })
            .map(|row| row as u32)
            .collect::<Vec<_>>();
        if selected.len() != input.size() {
            let selection =
                SelectionVector::try_from_owned_indices(selected.clone(), allocator.clone())?;
            let mut active = Chunk::try_new(allocator.clone())?;
            active.try_reference_selection(input, &[], selection.into())?;
            let mut values = execute_python(ctx, spec, &active, memory)?;
            if let paro_external::routine::spec::RoutineReturn::Scalar(ty) = &spec.return_type {
                let mut output = Vector::try_new(ty.clone(), input.size(), allocator)?;
                output.try_set_count(input.size())?;
                for row in 0..input.size() {
                    output.try_set_null(row, true)?;
                }
                for (source, target) in selected.iter().enumerate() {
                    output.try_copy_range(*target as usize, &values[0], source, 1)?;
                }
                values[0] = output;
            }
            return Ok(values);
        }
    }
    // Arrow builders, wire frames and decoder buffers live outside the native
    // allocator. Charge their bounded working set to the admitted operator.
    let _transport_memory = memory.retain_external_allocation_handle(
        MemoryTag::ExternalRuntimeHost,
        paro_common::memory::MemoryAccountingClass::NonRevocable,
        TRANSPORT_WORKING_SET_BYTES,
    )?;
    let batch = codec::encode(input, input_schema)?;
    let result = ctx.session.python_client().and_then(|client| {
        client.execute(definition, batch, output_schema, allocator, || {
            ctx.cancellation.check()
        })
    });
    if let Err(error) = &result {
        if error.is(paro_common::error::codes::external_routine::WORKER_FAILURE) {
            ctx.session
                .observe_python_runtime_failure(&error.to_string());
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::{
        ExternalProjectExecutor, ExternalRoutineDescriptor, ExternalRuntimeBridge,
        ProjectSubmission, PythonFlightTableExecutor, RuntimeBridgeExplainInfo,
        TestProjectExecutor,
    };
    use crate::memory_runtime::{
        ExecutionLease, LocalMemoryGrant, OperatorMemoryAccount, OperatorMemoryScope,
        QueryMemoryPool,
    };
    use crate::operators::external::batching::SubmissionBatchPolicy;
    use crate::runtime::{ParameterBindings, QueryOutputPort, QueryRuntimeContext};
    use paro_common::allocator::{Allocator, DefaultAllocator, MemoryTag};
    use paro_common::chunk::Chunk;
    use paro_common::memory::{MemoryAccountingClass, MemoryOwner};
    use paro_common::types::LogicalType;

    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_external::routine::bound::BoundRoutineCallMeta;
    use paro_external::routine::boundary::{ExecutionBoundary, PlacementClass};
    use paro_external::routine::identity::RoutineCallIdentity;
    use paro_external::routine::spec::{
        RoutineNullPolicy, RoutineSemantics, RoutineSideEffects, RoutineStability, RowSemantics,
    };
    use paro_external::runtime::dispatch::policy::ExternalDispatchPolicy;
    use paro_external::runtime::host::{
        ExternalRuntimeHost, PythonRuntimeProbe, PythonRuntimeProbeResult, PythonRuntimeProvider,
    };
    use paro_function::scalar::ScalarFunction;
    use paro_planner::expression::{Expression, FunctionExpression, ReferenceExpression};
    use paro_planner::logical::operator::external_project::ExternalProjectExpression;
    use paro_planner::physical::{ExecutionResourceContract, ResourceGrantClassId};
    use std::sync::Arc;

    #[derive(Debug)]
    struct DisabledProbe;

    impl PythonRuntimeProbe for DisabledProbe {
        fn probe(&self) -> PythonRuntimeProbeResult {
            PythonRuntimeProbeResult::disabled_by_config("Python runtime is disabled by test")
        }
    }

    fn query_ctx(session: Arc<StatementContext>) -> QueryRuntimeContext {
        QueryRuntimeContext::new(
            session,
            Arc::new(ParameterBindings::empty()),
            Arc::new(QueryMemoryPool::unbounded()),
            QueryOutputPort::discarding(),
        )
    }

    fn test_ctx() -> QueryRuntimeContext {
        let session: Arc<StatementContext> = TestStatementContextBuilder::minimal().build();
        query_ctx(session)
    }

    fn disabled_runtime_ctx() -> QueryRuntimeContext {
        let runtime = Arc::new(ExternalRuntimeHost::new().with_probe(Arc::new(DisabledProbe)));
        let session: Arc<StatementContext> = TestStatementContextBuilder::minimal()
            .with_python_runtime(runtime)
            .build();
        query_ctx(session)
    }

    fn add_one_expr() -> ExternalProjectExpression {
        fn add_one(
            input: &Chunk,
            _ctx: &dyn paro_function::scalar::FunctionExecContext,
            result: &mut paro_common::vector::Vector,
        ) -> paro_common::error::Result<()> {
            let column = input.column(0).expect("input column");
            for row_idx in 0..input.size() {
                let value = column.get_i32(row_idx).expect("non-null");
                result.set_i32(row_idx, value + 1);
            }
            Ok(())
        }

        let semantics = RoutineSemantics {
            stability: RoutineStability::Immutable,
            null_policy: RoutineNullPolicy::CalledOnNullInput,
            side_effects: RoutineSideEffects::None,
            row_semantics: RowSemantics::RowPreserving,
            may_block: false,
        };
        let expr = Expression::Function(
            FunctionExpression::new(
                ScalarFunction::new(
                    "add_one".to_string(),
                    vec![LogicalType::Integer],
                    LogicalType::Integer,
                    add_one,
                ),
                vec![Expression::Reference(
                    ReferenceExpression::new(0, LogicalType::Integer).into(),
                )],
                LogicalType::Integer,
            )
            .with_routine_meta(BoundRoutineCallMeta {
                identity: RoutineCallIdentity::Catalog {
                    routine_id: paro_external::routine::spec::RoutineId::from_raw(42),
                    generation: 3,
                },
                semantics: semantics.clone(),
                boundary: ExecutionBoundary {
                    placement: PlacementClass::External,
                    may_block: false,
                    row_semantics: RowSemantics::RowPreserving,
                },
                spec: None,
            })
            .into(),
        );

        ExternalProjectExpression {
            output_name: "__ext".to_string(),
            expression: expr,
            routine_meta: BoundRoutineCallMeta {
                identity: RoutineCallIdentity::Catalog {
                    routine_id: paro_external::routine::spec::RoutineId::from_raw(42),
                    generation: 3,
                },
                semantics,
                boundary: ExecutionBoundary {
                    placement: PlacementClass::External,
                    may_block: false,
                    row_semantics: RowSemantics::RowPreserving,
                },
                spec: None,
            },
        }
    }

    fn test_operator_memory_scope() -> OperatorMemoryScope<'static> {
        const TEST_OPERATOR_GRANT_BYTES: usize = 1024 * 1024 * 1024;

        let pool = Arc::new(QueryMemoryPool::unbounded());
        let account = Arc::new(OperatorMemoryAccount::new(pool));
        let owner: Arc<dyn MemoryOwner> = account;
        let allocator: Arc<dyn Allocator> = Arc::new(DefaultAllocator::new());
        let grant = LocalMemoryGrant::new(
            owner,
            TEST_OPERATOR_GRANT_BYTES,
            MemoryTag::Allocator,
            MemoryAccountingClass::NonRevocable,
            allocator,
        )
        .expect("test operator memory grant should be available");

        OperatorMemoryScope::new(Box::leak(Box::new(grant)))
    }

    #[test]
    fn runtime_bridge_requires_and_consumes_an_external_worker_lease() {
        let ctx = test_ctx();
        let input = Chunk::from_vectors(
            vec![paro_common::test_utils::test_i32_vector_with_allocator(
                &[1, 2, 3],
                paro_common::test_utils::test_allocator(),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let expression = add_one_expr();
        let routines = vec![ExternalRoutineDescriptor {
            label: "py_add_one".to_string(),
            identity: expression.routine_meta.identity.clone(),
            semantics: expression.routine_meta.semantics.clone(),
            spec: expression.routine_meta.spec.clone(),
        }];
        let submission = ProjectSubmission {
            batch_id: 7,
            input: &input,
            expressions: std::slice::from_ref(&expression),
            routines: &routines,
            force_tail_flush: false,
            batch_policy: &SubmissionBatchPolicy {
                target_batch_bytes: 1024,
                max_accumulation_bytes: 1024,
                min_batch_rows: 1,
                max_batch_rows: 1024,
                emission_chunk_rows: 2048,
            },
        };

        let bridge = ExternalRuntimeBridge::new(
            RuntimeBridgeExplainInfo::default_python_flight(),
            ExternalDispatchPolicy::default(),
            Arc::new(TestProjectExecutor),
            Arc::new(PythonFlightTableExecutor),
        );
        let error = bridge
            .execute_project(&ctx, &submission, &test_operator_memory_scope())
            .expect_err("dispatch without an admitted worker slot must fail");
        assert!(error.to_string().contains("worker-slot lease"));

        let host = ExternalRuntimeHost::ready_stub();
        let lease = host
            .try_acquire_execution_slots(1, 1)
            .unwrap()
            .expect("test query should acquire a worker slot");
        ctx.memory
            .install_execution_lease(
                ExecutionLease::new(
                    ExecutionResourceContract {
                        class: ResourceGrantClassId::new(0),
                        minimum_memory_bytes: 0,
                        working_set_memory_bytes: 0,
                        memory_ceiling_bytes: u64::try_from(ctx.memory.capacity_bytes())
                            .unwrap_or(u64::MAX),
                        memory_completion: paro_planner::physical::MemoryCompletion::Guaranteed,
                        max_parallel_tasks: 1,
                        external_worker_slots: 1,
                    },
                    Some(lease),
                )
                .unwrap(),
            )
            .unwrap();
        let response = bridge
            .execute_project(&ctx, &submission, &test_operator_memory_scope())
            .expect("project bridge should succeed");

        let output = response.output_batches.first().expect("generated output");
        assert_eq!(
            output.get_value(0, 0),
            Some(paro_common::runtime_value::Value::Integer(2))
        );
        assert_eq!(
            output.get_value(0, 2),
            Some(paro_common::runtime_value::Value::Integer(4))
        );
    }

    #[test]
    fn evaluating_project_executor_reports_runtime_unavailable_before_execution() {
        let ctx = disabled_runtime_ctx();
        let input = Chunk::from_vectors(
            vec![paro_common::test_utils::test_i32_vector_with_allocator(
                &[1, 2, 3],
                paro_common::test_utils::test_allocator(),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let expression = add_one_expr();
        let routines = vec![ExternalRoutineDescriptor {
            label: "py_add_one".to_string(),
            identity: expression.routine_meta.identity.clone(),
            semantics: expression.routine_meta.semantics.clone(),
            spec: expression.routine_meta.spec.clone(),
        }];
        let submission = ProjectSubmission {
            batch_id: 7,
            input: &input,
            expressions: std::slice::from_ref(&expression),
            routines: &routines,
            force_tail_flush: false,
            batch_policy: &SubmissionBatchPolicy {
                target_batch_bytes: 1024,
                max_accumulation_bytes: 1024,
                min_batch_rows: 1,
                max_batch_rows: 1024,
                emission_chunk_rows: 2048,
            },
        };

        let err = TestProjectExecutor
            .execute(&ctx, &submission, &test_operator_memory_scope())
            .expect_err("runtime availability must fail before execution");
        assert!(err.to_string().contains("Python runtime"));
    }
}
