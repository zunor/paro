// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_external_runtime::dispatch::policy::ExternalDispatchPolicy;
use paro_planner::operator::external_project::ExternalProjectExpression;
use paro_routine::{RoutineCallIdentity, RoutineSemantics};

use crate::execution_context::ExecutionContext;
use crate::expression_executor::executor::ExpressionExecutor;

use super::batching::SubmissionBatchPolicy;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalRoutineDescriptor {
    pub label: String,
    pub identity: RoutineCallIdentity,
    pub semantics: RoutineSemantics,
}

impl ExternalRoutineDescriptor {
    pub fn identity_label(&self) -> String {
        format_identity_label(&self.identity, &self.label)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeBridgeExplainInfo {
    pub language: String,
    pub backend: String,
    pub env_artifact_id: Option<String>,
    pub artifact_validation_state: String,
}

impl RuntimeBridgeExplainInfo {
    pub fn default_python_process() -> Self {
        Self {
            language: "python".to_string(),
            backend: "process".to_string(),
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
    pub kernel_time_us: u64,
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

#[derive(Debug, Clone)]
pub enum RuntimeBridgeOutcome {
    Ready(RuntimeBridgeResponse),
    Blocked(RuntimeBridgeResponse),
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

pub trait ProjectBridgeKernel: Send + Sync + fmt::Debug {
    fn execute(
        &self,
        ctx: &ExecutionContext,
        submission: &ProjectSubmission<'_>,
    ) -> Result<RuntimeBridgeOutcome>;
}

pub trait TableBridgeKernel: Send + Sync + fmt::Debug {
    fn execute(
        &self,
        ctx: &ExecutionContext,
        submission: &TableSubmission<'_>,
    ) -> Result<RuntimeBridgeOutcome>;
}

#[derive(Debug, Default)]
pub struct EvaluatingProjectKernel;

impl ProjectBridgeKernel for EvaluatingProjectKernel {
    fn execute(
        &self,
        ctx: &ExecutionContext,
        submission: &ProjectSubmission<'_>,
    ) -> Result<RuntimeBridgeOutcome> {
        ctx.session.ensure_python_runtime_ready_for_execution()?;
        let started_at = Instant::now();
        let expressions = submission
            .expressions
            .iter()
            .map(|expression| expression.expression.clone())
            .collect::<Vec<_>>();
        let mut executor = ExpressionExecutor::with_expressions(&expressions);
        let mut generated = Chunk::with_allocator(ctx.allocator(MemoryTag::Extension));
        executor.execute_all_into(submission.input, ctx, &mut generated)?;

        let elapsed_us = started_at.elapsed().as_micros() as u64;
        let output_bytes = SubmissionBatchPolicy::estimate_chunk_bytes(&generated);
        let input_bytes = SubmissionBatchPolicy::estimate_chunk_bytes(submission.input);
        Ok(RuntimeBridgeOutcome::Ready(RuntimeBridgeResponse {
            output_batches: vec![generated],
            metrics: RuntimeBridgeMetrics {
                worker_acquire_time_us: 10,
                queue_wait_us: 0,
                kernel_time_us: elapsed_us.max(1),
                encode_decode_time_us: 10,
                data_plane_bytes: input_bytes.saturating_add(output_bytes),
                cache_hit: false,
                warm_state: RuntimeWarmState::Warm,
                retired_count: 0,
                output_rows: submission.input.size() as u64,
                output_bytes,
            },
        }))
    }
}

#[derive(Debug)]
pub struct UnboundTableKernel;

impl TableBridgeKernel for UnboundTableKernel {
    fn execute(
        &self,
        ctx: &ExecutionContext,
        submission: &TableSubmission<'_>,
    ) -> Result<RuntimeBridgeOutcome> {
        ctx.session.ensure_python_runtime_ready_for_execution()?;
        Err(paro_error::not_implemented(format!(
            "external table routine '{}' reached physical execution before a runtime bridge was bound",
            submission.routine.label
        )))
    }
}

#[derive(Debug)]
pub struct ExternalRuntimeBridge {
    explain: RuntimeBridgeExplainInfo,
    dispatch_policy: ExternalDispatchPolicy,
    project_kernel: Arc<dyn ProjectBridgeKernel>,
    table_kernel: Arc<dyn TableBridgeKernel>,
}

impl ExternalRuntimeBridge {
    pub fn new(
        explain: RuntimeBridgeExplainInfo,
        dispatch_policy: ExternalDispatchPolicy,
        project_kernel: Arc<dyn ProjectBridgeKernel>,
        table_kernel: Arc<dyn TableBridgeKernel>,
    ) -> Self {
        Self {
            explain,
            dispatch_policy,
            project_kernel,
            table_kernel,
        }
    }

    pub fn default_bridge() -> Self {
        Self::new(
            RuntimeBridgeExplainInfo::default_python_process(),
            ExternalDispatchPolicy::default(),
            Arc::new(EvaluatingProjectKernel),
            Arc::new(UnboundTableKernel),
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
        ctx: &ExecutionContext,
        submission: &ProjectSubmission<'_>,
    ) -> Result<RuntimeBridgeOutcome> {
        self.project_kernel.execute(ctx, submission)
    }

    pub fn execute_table(
        &self,
        ctx: &ExecutionContext,
        submission: &TableSubmission<'_>,
    ) -> Result<RuntimeBridgeOutcome> {
        self.table_kernel.execute(ctx, submission)
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

#[cfg(test)]
mod tests {
    use super::{
        EvaluatingProjectKernel, ExternalRoutineDescriptor, ProjectBridgeKernel, ProjectSubmission,
    };
    use crate::execution_context::ExecutionContext;
    use crate::operator::external::batching::SubmissionBatchPolicy;
    use crate::thread_context::ThreadContext;
    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_external_runtime::host::{
        ExternalRuntimeHost, PythonRuntimeProbe, PythonRuntimeProbeResult,
    };
    use paro_function::scalar::ScalarFunction;
    use paro_planner::expression::{Expression, FunctionExpression, ReferenceExpression};
    use paro_planner::operator::external_project::ExternalProjectExpression;
    use paro_routine::{
        BoundRoutineCallMeta, ExecutionBoundary, PlacementClass, RoutineCallIdentity,
        RoutineNullPolicy, RoutineSemantics, RoutineSideEffects, RoutineStability, RowSemantics,
    };
    use std::sync::Arc;

    #[derive(Debug)]
    struct DisabledProbe;

    impl PythonRuntimeProbe for DisabledProbe {
        fn probe(&self) -> PythonRuntimeProbeResult {
            PythonRuntimeProbeResult::disabled_by_config("Python runtime is disabled by test")
        }
    }

    fn test_ctx() -> ExecutionContext<'static> {
        let session: Arc<StatementContext> = TestStatementContextBuilder::minimal().build();
        let thread = Box::leak(Box::new(ThreadContext::single_threaded()));
        ExecutionContext::new(session, thread, None)
    }

    fn disabled_runtime_ctx() -> ExecutionContext<'static> {
        let runtime = Arc::new(ExternalRuntimeHost::new().with_probe(Arc::new(DisabledProbe)));
        let session: Arc<StatementContext> = TestStatementContextBuilder::minimal()
            .with_python_runtime(runtime)
            .build();
        let thread = Box::leak(Box::new(ThreadContext::single_threaded()));
        ExecutionContext::new(session, thread, None)
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
                vec![Expression::Reference(ReferenceExpression::new(
                    0,
                    LogicalType::Integer,
                ))],
                LogicalType::Integer,
            )
            .with_routine_meta(BoundRoutineCallMeta {
                identity: RoutineCallIdentity::Catalog {
                    routine_id: paro_routine::RoutineId::from_raw(42),
                    generation: 3,
                },
                semantics: semantics.clone(),
                boundary: ExecutionBoundary {
                    placement: PlacementClass::External,
                    may_block: false,
                    row_semantics: RowSemantics::RowPreserving,
                },
                spec: None,
            }),
        );

        ExternalProjectExpression {
            output_name: "__ext".to_string(),
            expression: expr,
            routine_meta: BoundRoutineCallMeta {
                identity: RoutineCallIdentity::Catalog {
                    routine_id: paro_routine::RoutineId::from_raw(42),
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

    #[test]
    fn evaluating_project_kernel_executes_generated_columns() {
        let ctx = test_ctx();
        let input = Chunk::from_vectors(vec![Vector::from_i32(&[1, 2, 3])]);
        let expression = add_one_expr();
        let routines = vec![ExternalRoutineDescriptor {
            label: "py_add_one".to_string(),
            identity: expression.routine_meta.identity.clone(),
            semantics: expression.routine_meta.semantics.clone(),
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

        let response = match EvaluatingProjectKernel
            .execute(&ctx, &submission)
            .expect("project bridge should succeed")
        {
            super::RuntimeBridgeOutcome::Ready(response) => response,
            super::RuntimeBridgeOutcome::Blocked(_) => panic!("project bridge should be ready"),
        };

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
    fn evaluating_project_kernel_reports_runtime_unavailable_before_execution() {
        let ctx = disabled_runtime_ctx();
        let input = Chunk::from_vectors(vec![Vector::from_i32(&[1, 2, 3])]);
        let expression = add_one_expr();
        let routines = vec![ExternalRoutineDescriptor {
            label: "py_add_one".to_string(),
            identity: expression.routine_meta.identity.clone(),
            semantics: expression.routine_meta.semantics.clone(),
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

        let err = EvaluatingProjectKernel
            .execute(&ctx, &submission)
            .expect_err("runtime availability must fail before execution");
        assert!(err.to_string().contains("Python runtime"));
    }
}
