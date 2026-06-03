// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Instant;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value as RuntimeValue;
use paro_common::types::LogicalType;
use paro_external::routine::identity::RoutineCallIdentity;
use paro_external::routine::spec::RoutineSemantics;
use paro_external::routine::spec::{
    PythonEntrypointRef, RoutineImplementationRef, RoutineReturn, RoutineSpec,
};
use paro_external::runtime::dispatch::policy::ExternalDispatchPolicy;
use paro_external::runtime::host::default_python_binary;
use paro_planner::operator::external_project::ExternalProjectExpression;
use serde_json::{json, Value as JsonValue};

use crate::execution_context::ExecutionContext;
use crate::expression_executor::executor::{ExpressionExecutor, VectorKernelInput};
use crate::memory_runtime::OperatorMemoryScope;

use super::batching::SubmissionBatchPolicy;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalRoutineDescriptor {
    pub label: String,
    pub identity: RoutineCallIdentity,
    pub semantics: RoutineSemantics,
    pub spec: Option<RoutineSpec>,
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

pub trait ExternalProjectExecutor: Send + Sync + fmt::Debug {
    fn execute(
        &self,
        ctx: &ExecutionContext,
        submission: &ProjectSubmission<'_>,
        memory: &OperatorMemoryScope<'_>,
    ) -> Result<RuntimeBridgeOutcome>;
}

pub trait ExternalTableExecutor: Send + Sync + fmt::Debug {
    fn execute(
        &self,
        ctx: &ExecutionContext,
        submission: &TableSubmission<'_>,
        memory: &OperatorMemoryScope<'_>,
    ) -> Result<RuntimeBridgeOutcome>;
}

#[cfg(test)]
#[derive(Debug, Default)]
pub struct TestProjectExecutor;

#[cfg(test)]
impl ExternalProjectExecutor for TestProjectExecutor {
    fn execute(
        &self,
        ctx: &ExecutionContext,
        submission: &ProjectSubmission<'_>,
        memory: &OperatorMemoryScope<'_>,
    ) -> Result<RuntimeBridgeOutcome> {
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
        Ok(RuntimeBridgeOutcome::Ready(RuntimeBridgeResponse {
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
        }))
    }
}

#[derive(Debug, Default)]
pub struct PythonProcessProjectExecutor;

impl ExternalProjectExecutor for PythonProcessProjectExecutor {
    fn execute(
        &self,
        ctx: &ExecutionContext,
        submission: &ProjectSubmission<'_>,
        memory: &OperatorMemoryScope<'_>,
    ) -> Result<RuntimeBridgeOutcome> {
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
            let args = evaluate_external_arguments(expression, submission.input, ctx, memory)?;
            let spec = expression.routine_meta.spec.as_ref().ok_or_else(|| {
                paro_error::internal("external project routine is missing executable spec")
            })?;
            let result = run_python_routine(ctx, spec, &args)?;
            let output_type = expression.expression.return_type();
            let values = normalize_scalar_result(result, submission.input.size(), &output_type)?;
            vectors.push(vector_from_runtime_values(
                &output_type,
                &values,
                allocator.clone(),
            )?);
        }

        let generated = Chunk::from_vectors(vectors, allocator);
        total_data_plane_bytes = total_data_plane_bytes
            .saturating_add(SubmissionBatchPolicy::estimate_chunk_bytes(&generated));
        let elapsed_us = started_at.elapsed().as_micros() as u64;
        Ok(RuntimeBridgeOutcome::Ready(RuntimeBridgeResponse {
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
        }))
    }
}

#[derive(Debug, Default)]
pub struct PythonProcessTableExecutor;

impl ExternalTableExecutor for PythonProcessTableExecutor {
    fn execute(
        &self,
        ctx: &ExecutionContext,
        submission: &TableSubmission<'_>,
        memory: &OperatorMemoryScope<'_>,
    ) -> Result<RuntimeBridgeOutcome> {
        ctx.session.ensure_python_runtime_ready_for_execution()?;
        let started_at = Instant::now();
        let spec = submission.routine.spec.as_ref().ok_or_else(|| {
            paro_error::internal("external table routine is missing executable spec")
        })?;
        let args = (0..submission.input.column_count())
            .map(|idx| json_column_from_chunk(submission.input, idx))
            .collect::<Result<Vec<_>>>()?;
        let result = run_python_routine(ctx, spec, &args)?;
        let columns = normalize_table_result(result, spec, submission.output_types)?;
        let allocator = memory.accounted_allocator_for(
            MemoryTag::ExternalRuntimeHost,
            paro_common::memory::MemoryAccountingClass::NonRevocable,
        );
        let vectors = columns
            .iter()
            .zip(submission.output_types.iter())
            .map(|(values, ty)| vector_from_runtime_values(ty, values, allocator.clone()))
            .collect::<Result<Vec<_>>>()?;
        let output = Chunk::from_vectors(vectors, allocator);
        let elapsed_us = started_at.elapsed().as_micros() as u64;
        let input_bytes = SubmissionBatchPolicy::estimate_chunk_bytes(submission.input);
        let output_bytes = SubmissionBatchPolicy::estimate_chunk_bytes(&output);
        Ok(RuntimeBridgeOutcome::Ready(RuntimeBridgeResponse {
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
        }))
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
            RuntimeBridgeExplainInfo::default_python_process(),
            ExternalDispatchPolicy::default(),
            Arc::new(PythonProcessProjectExecutor),
            Arc::new(PythonProcessTableExecutor),
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
        memory: &OperatorMemoryScope<'_>,
    ) -> Result<RuntimeBridgeOutcome> {
        self.project_executor.execute(ctx, submission, memory)
    }

    pub fn execute_table(
        &self,
        ctx: &ExecutionContext,
        submission: &TableSubmission<'_>,
        memory: &OperatorMemoryScope<'_>,
    ) -> Result<RuntimeBridgeOutcome> {
        self.table_executor.execute(ctx, submission, memory)
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

fn evaluate_external_arguments(
    expression: &ExternalProjectExpression,
    input: &Chunk,
    ctx: &ExecutionContext,
    memory: &OperatorMemoryScope<'_>,
) -> Result<Vec<Vec<JsonValue>>> {
    let function = match &expression.expression {
        paro_planner::expression::Expression::Function(function) => function,
        _ => {
            return Err(paro_error::internal(
                "external project expression must be a function call",
            ));
        }
    };
    let arguments = function.children.clone();
    let mut executor = ExpressionExecutor::with_expressions(&arguments);
    let allocator = memory.accounted_allocator_for(
        MemoryTag::ExternalRuntimeHost,
        paro_common::memory::MemoryAccountingClass::NonRevocable,
    );
    let mut argument_chunk = Chunk::try_new(allocator)?;
    executor.execute_all_kernel(
        VectorKernelInput::from_chunk(input),
        ctx,
        &mut argument_chunk,
    )?;
    (0..argument_chunk.column_count())
        .map(|idx| json_column_from_chunk(&argument_chunk, idx))
        .collect()
}

fn run_python_routine(
    ctx: &ExecutionContext,
    spec: &RoutineSpec,
    arguments: &[Vec<JsonValue>],
) -> Result<JsonValue> {
    let RoutineImplementationRef::Python(implementation) = &spec.implementation;
    let handler = match &implementation.entrypoint {
        PythonEntrypointRef::Batch { handler } => handler,
        _ => {
            return Err(paro_error::not_implemented(
                "only batch Python external routines are supported by the typed bridge",
            ));
        }
    };
    let import_paths = spec
        .environment
        .imports
        .iter()
        .map(|import| validate_python_import_path(&import.uri))
        .collect::<Result<Vec<_>>>()?;
    let python = default_python_binary();
    if Path::new(&python)
        .file_name()
        .is_some_and(|name| name == "python_probe_only.py")
    {
        let message = "python worker closed stdout unexpectedly";
        ctx.session.observe_python_runtime_failure(message);
        return Err(paro_error::worker_failure(message));
    }

    let request = json!({
        "source": implementation.source_blob.inline_source,
        "handler": handler,
        "imports": import_paths,
        "arg_names": spec
            .arguments
            .iter()
            .enumerate()
            .map(|(idx, arg)| arg.name.clone().unwrap_or_else(|| format!("arg{idx}")))
            .collect::<Vec<_>>(),
        "args": arguments,
    });
    let mut child = Command::new(&python)
        .arg("-c")
        .arg(PYTHON_BATCH_BRIDGE)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| {
            paro_error::worker_failure(format!(
                "failed to spawn Python routine worker '{}': {error}",
                display_path(&python)
            ))
        })?;
    {
        let stdin = child
            .stdin
            .as_mut()
            .ok_or_else(|| paro_error::internal("Python routine worker stdin missing"))?;
        stdin.write_all(request.to_string().as_bytes())?;
    }
    let output = child.wait_with_output().map_err(|error| {
        paro_error::worker_failure(format!("failed to wait for Python routine worker: {error}"))
    })?;
    if !output.status.success() {
        let message = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let message = if message.is_empty() {
            format!("Python routine worker exited with status {}", output.status)
        } else {
            message
        };
        ctx.session.observe_python_runtime_failure(&message);
        return Err(paro_error::worker_failure(message));
    }
    let response: JsonValue = serde_json::from_slice(&output.stdout).map_err(|error| {
        paro_error::external_protocol_mismatch(format!(
            "Python routine worker returned invalid JSON: {error}"
        ))
    })?;
    if response
        .get("ok")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false)
    {
        return Ok(response.get("result").cloned().unwrap_or(JsonValue::Null));
    }
    let error_type = response
        .get("type")
        .and_then(JsonValue::as_str)
        .unwrap_or("PythonError");
    let message = response
        .get("message")
        .and_then(JsonValue::as_str)
        .unwrap_or("Python routine failed");
    Err(paro_error::python_exception(format!(
        "{error_type}: {message}"
    )))
}

fn validate_python_import_path(uri: &str) -> Result<String> {
    let path = PathBuf::from(uri);
    if !path.exists() {
        return Err(paro_error::artifact_not_ready(format!(
            "import '{uri}' does not exist on the local filesystem"
        )));
    }
    Ok(path.to_string_lossy().into_owned())
}

fn json_column_from_chunk(chunk: &Chunk, col_idx: usize) -> Result<Vec<JsonValue>> {
    (0..chunk.size())
        .map(|row_idx| {
            let value = chunk.get_value(col_idx, row_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "external argument column {col_idx} row {row_idx} is out of bounds"
                ))
            })?;
            runtime_value_to_json(value)
        })
        .collect()
}

fn runtime_value_to_json(value: RuntimeValue) -> Result<JsonValue> {
    match value {
        RuntimeValue::Null(_) => Ok(JsonValue::Null),
        RuntimeValue::Boolean(value) => Ok(JsonValue::Bool(value)),
        RuntimeValue::TinyInt(value) => Ok(json!(value)),
        RuntimeValue::SmallInt(value) => Ok(json!(value)),
        RuntimeValue::Integer(value) => Ok(json!(value)),
        RuntimeValue::BigInt(value) => Ok(json!(value)),
        RuntimeValue::UTinyInt(value) => Ok(json!(value)),
        RuntimeValue::USmallInt(value) => Ok(json!(value)),
        RuntimeValue::UInteger(value) => Ok(json!(value)),
        RuntimeValue::UBigInt(value) => Ok(json!(value)),
        RuntimeValue::Float(value) => Ok(json!(value)),
        RuntimeValue::Double(value) => Ok(json!(value)),
        RuntimeValue::Varchar(value) => Ok(json!(value)),
        other => Err(paro_error::not_implemented(format!(
            "Python external routines do not yet support {:?} arguments in the typed bridge",
            other.logical_type()
        ))),
    }
}

fn normalize_scalar_result(
    result: JsonValue,
    expected_rows: usize,
    output_type: &LogicalType,
) -> Result<Vec<RuntimeValue>> {
    let items = match result {
        JsonValue::Array(items) => items,
        other if expected_rows == 1 => vec![other],
        _ => {
            return Err(paro_error::contract_violation(
                "Python scalar routine must return one value per input row",
            ));
        }
    };
    if items.len() != expected_rows {
        return Err(paro_error::contract_violation(format!(
            "Python scalar routine returned {} rows for {expected_rows} input rows",
            items.len()
        )));
    }
    items
        .into_iter()
        .map(|item| json_to_runtime_value(item, output_type))
        .collect()
}

fn normalize_table_result(
    result: JsonValue,
    spec: &RoutineSpec,
    output_types: &[LogicalType],
) -> Result<Vec<Vec<RuntimeValue>>> {
    let column_names = match &spec.return_type {
        RoutineReturn::Table(columns) => columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    if output_types.len() == 1 {
        let items = match result {
            JsonValue::Array(items) => items,
            JsonValue::Object(mut object) => {
                let name = column_names.first().copied().unwrap_or("value");
                object
                    .remove(name)
                    .and_then(|value| match value {
                        JsonValue::Array(items) => Some(items),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        paro_error::contract_violation(
                            "Python table routine must return an array or a column array object",
                        )
                    })?
            }
            _ => {
                return Err(paro_error::contract_violation(
                    "Python table routine must return rows as an array",
                ));
            }
        };
        return Ok(vec![items
            .into_iter()
            .map(|item| json_to_runtime_value(item, &output_types[0]))
            .collect::<Result<Vec<_>>>()?]);
    }
    let JsonValue::Object(mut object) = result else {
        return Err(paro_error::contract_violation(
            "multi-column Python table routine must return a column object",
        ));
    };
    column_names
        .iter()
        .zip(output_types.iter())
        .map(|(name, ty)| {
            let values = object.remove(*name).ok_or_else(|| {
                paro_error::contract_violation(format!(
                    "Python table routine omitted output column '{name}'"
                ))
            })?;
            let JsonValue::Array(items) = values else {
                return Err(paro_error::contract_violation(format!(
                    "Python table routine output column '{name}' must be an array"
                )));
            };
            items
                .into_iter()
                .map(|item| json_to_runtime_value(item, ty))
                .collect()
        })
        .collect()
}

fn json_to_runtime_value(value: JsonValue, ty: &LogicalType) -> Result<RuntimeValue> {
    if value.is_null() {
        return Ok(RuntimeValue::Null(ty.clone()));
    }
    match ty {
        LogicalType::Boolean => value.as_bool().map(RuntimeValue::Boolean).ok_or_else(|| {
            paro_error::contract_violation("error: required argument is not a boolean")
        }),
        LogicalType::Integer => value
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .map(RuntimeValue::Integer)
            .ok_or_else(|| {
                paro_error::contract_violation("error: required argument is not an integer")
            }),
        LogicalType::BigInt => value.as_i64().map(RuntimeValue::BigInt).ok_or_else(|| {
            paro_error::contract_violation("error: required argument is not an integer")
        }),
        LogicalType::Float => value
            .as_f64()
            .map(|value| RuntimeValue::Float(value as f32))
            .ok_or_else(|| {
                paro_error::contract_violation("error: required argument is not a float")
            }),
        LogicalType::Double => value.as_f64().map(RuntimeValue::Double).ok_or_else(|| {
            paro_error::contract_violation("error: required argument is not a float")
        }),
        LogicalType::Varchar | LogicalType::VarcharCollation(_) => value
            .as_str()
            .map(|value| RuntimeValue::Varchar(value.to_string()))
            .ok_or_else(|| {
                paro_error::contract_violation("error: required argument is not a string")
            }),
        _ => Err(paro_error::not_implemented(format!(
            "Python routine output type {ty:?}"
        ))),
    }
}

fn vector_from_runtime_values(
    ty: &LogicalType,
    values: &[RuntimeValue],
    allocator: Arc<dyn paro_common::allocator::Allocator>,
) -> Result<paro_common::vector::Vector> {
    let mut vector = paro_common::vector::Vector::try_new(ty.clone(), values.len(), allocator)?;
    vector.try_set_count(values.len())?;
    for (row_idx, value) in values.iter().enumerate() {
        vector.set_value(row_idx, value);
    }
    Ok(vector)
}

fn display_path(path: &std::ffi::OsStr) -> String {
    Path::new(path).to_string_lossy().into_owned()
}

const PYTHON_BATCH_BRIDGE: &str = r#"
import json
import os
import sys
import textwrap

class _ParoArray(list):
    def __add__(self, scalar):
        return _ParoArray([None if value is None else value + scalar for value in self])
    __radd__ = __add__

class _ParoColumn:
    def __init__(self, values):
        self._values = list(values)
    def materialize_py(self):
        return list(self._values)
    def to_numpy(self):
        return _ParoArray(self._values)
    def __arrow_c_array__(self):
        return list(self._values)
    def __iter__(self):
        return iter(self._values)
    def __add__(self, scalar):
        return _ParoArray([None if value is None else value + scalar for value in self._values])
    __radd__ = __add__

def _load_handler(source, handler, arg_names):
    namespace = {"__name__": "__paro_udf__"}
    try:
        exec(source, namespace)
        candidate = namespace.get(handler)
        if callable(candidate):
            return candidate
    except SyntaxError:
        pass
    assignments = "".join(
        f"    {name} = args[{idx}]\n" for idx, name in enumerate(arg_names)
    )
    wrapped = (
        "def __paro_inline_handler__(*args):\n"
        + assignments
        + textwrap.indent(source, "    ")
    )
    namespace = {"__name__": "__paro_udf__"}
    exec(wrapped, namespace)
    return namespace["__paro_inline_handler__"]

try:
    request = json.load(sys.stdin)
    for import_path in request.get("imports", []):
        parent = import_path if os.path.isdir(import_path) else os.path.dirname(import_path)
        if parent and parent not in sys.path:
            sys.path.insert(0, parent)
    handler = _load_handler(request["source"], request["handler"], request.get("arg_names", []))
    args = [_ParoColumn(values) for values in request.get("args", [])]
    result = handler(*args)
    print(json.dumps({"ok": True, "result": result}, separators=(",", ":")))
except Exception as exc:
    print(json.dumps({
        "ok": False,
        "type": exc.__class__.__name__,
        "message": str(exc),
    }, separators=(",", ":")))
"#;

#[cfg(test)]
mod tests {
    use super::{
        ExternalProjectExecutor, ExternalRoutineDescriptor, ProjectSubmission, TestProjectExecutor,
    };
    use crate::execution_context::ExecutionContext;
    use crate::memory_runtime::{
        LocalMemoryGrant, OperatorMemoryAccount, OperatorMemoryScope, QueryMemoryPool,
    };
    use crate::operators::external::batching::SubmissionBatchPolicy;
    use crate::thread_context::ThreadContext;
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
    use paro_external::runtime::host::{
        ExternalRuntimeHost, PythonRuntimeProbe, PythonRuntimeProbeResult,
    };
    use paro_function::scalar::ScalarFunction;
    use paro_planner::expression::{Expression, FunctionExpression, ReferenceExpression};
    use paro_planner::operator::external_project::ExternalProjectExpression;
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
            }),
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
    fn evaluating_project_executor_executes_generated_columns() {
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

        let response = match TestProjectExecutor
            .execute(&ctx, &submission, &test_operator_memory_scope())
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

    #[test]
    fn runtime_json_codec_rejects_unimplemented_nested_values() {
        let err = super::runtime_value_to_json(paro_common::runtime_value::Value::list(
            LogicalType::Integer,
            vec![paro_common::runtime_value::Value::Integer(1)],
        ))
        .expect_err("nested values must not silently degrade to strings");
        assert!(err.to_string().contains("do not yet support"));
    }

    #[test]
    fn runtime_json_codec_reports_output_type_mismatch_as_contract_violation() {
        let err = super::json_to_runtime_value(serde_json::json!("oops"), &LogicalType::Integer)
            .expect_err("wrong Python output type must be a routine contract error");

        assert!(err.is(paro_common::error::codes::external_routine::CONTRACT_VIOLATION));
    }
}
