// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use parking_lot::Mutex;
use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_context::StatementContext;
use paro_external_abi::{
    AbiLogicalType, BufferLease, ColumnBatchLease, ColumnDescriptor, ColumnEncoding, ColumnLayout,
    ColumnPopulationMode, LeaseOwnership, LeaseState, OffsetWidth,
};
use paro_external_runtime::artifact::resolve::{ArtifactResolver, ResolveInputs};
use paro_external_runtime::artifact::validate::ArtifactValidator;
use paro_external_runtime::backend::selector::{
    BackendAvailability, BackendKind, BackendSelection, BackendSelector,
};
use paro_external_runtime::control::header::{ControlHeader, ControlMessageKind};
use paro_external_runtime::dispatch::policy::ExternalDispatchPolicy;
use paro_external_runtime::protocol::messages::PythonExceptionPayload;
use paro_planner::expression::Expression;
use paro_planner::operator::external_project::ExternalProjectExpression;
use paro_routine::{
    ArtifactCapabilities, ArtifactValidationState, BoundRoutineCallMeta, DeclaredEnvSpec,
    PythonEntrypointRef, PythonImplementationRef, RoutineImplementationRef, RoutineReturn,
    RoutineSpec, RuntimeContract, TransportKind,
};
use serde_json::{json, Value as JsonValue};

use crate::execution_context::ExecutionContext;
use crate::expression_executor::executor::ExpressionExecutor;
use crate::operator::external::batching::SubmissionBatchPolicy;
use crate::operator::external::runtime_bridge::{
    ExternalRoutineDescriptor, ExternalRuntimeBridge, ProjectBridgeKernel, ProjectSubmission,
    RuntimeBridgeExplainInfo, RuntimeBridgeMetrics, RuntimeBridgeOutcome, RuntimeBridgeResponse,
    RuntimeWarmState, TableBridgeKernel, TableSubmission,
};

const DEFAULT_PYTHON_BIN: &str = "python3";
const PYTHON_BIN_ENV: &str = "PARO_PYTHON_BIN";

static SUBINTERPRETER_SUPPORT_CACHE: OnceLock<Mutex<HashMap<OsString, bool>>> = OnceLock::new();

pub fn build_project_runtime_bridge(
    session: &Arc<StatementContext>,
    routines: &[ExternalRoutineDescriptor],
    expressions: &[ExternalProjectExpression],
) -> Result<ExternalRuntimeBridge> {
    if expressions
        .iter()
        .any(|expression| expression.routine_meta.spec.is_none())
    {
        return Ok(ExternalRuntimeBridge::default_bridge());
    }
    let prepared = expressions
        .iter()
        .zip(routines.iter())
        .map(|(expression, descriptor)| {
            PreparedProjectRoutine::prepare(session, expression, descriptor)
        })
        .collect::<Result<Vec<_>>>()?;
    let explain = explain_info(prepared.iter().map(|routine| &routine.base));
    let worker = Arc::new(PythonWorkerClient::new());
    Ok(ExternalRuntimeBridge::new(
        explain,
        ExternalDispatchPolicy::default(),
        Arc::new(PythonProcessProjectKernel { prepared, worker }),
        Arc::new(PythonProcessTableKernel::unbound()),
    ))
}

pub fn build_table_runtime_bridge(
    session: &Arc<StatementContext>,
    routine_meta: &BoundRoutineCallMeta,
    routine: &ExternalRoutineDescriptor,
    output_types: &[LogicalType],
) -> Result<ExternalRuntimeBridge> {
    if routine_meta.spec.is_none() {
        return Ok(ExternalRuntimeBridge::default_bridge());
    }
    let prepared = PreparedTableRoutine::prepare(session, routine_meta, routine, output_types)?;
    let explain = explain_info(std::iter::once(&prepared.base));
    let worker = Arc::new(PythonWorkerClient::new());
    Ok(ExternalRuntimeBridge::new(
        explain,
        ExternalDispatchPolicy::default(),
        Arc::new(PythonProcessProjectKernel::unbound()),
        Arc::new(PythonProcessTableKernel { prepared, worker }),
    ))
}

fn explain_info(
    routines: impl IntoIterator<Item = impl AsRef<PreparedRoutineBase>>,
) -> RuntimeBridgeExplainInfo {
    let routines = routines
        .into_iter()
        .map(|routine| routine.as_ref().clone())
        .collect::<Vec<_>>();
    let artifact_id = match routines.first() {
        Some(first)
            if routines.iter().all(|entry| {
                entry.artifact_validation.artifact_id == first.artifact_validation.artifact_id
            }) =>
        {
            Some(first.artifact_validation.artifact_id.clone())
        }
        Some(_) => Some("mixed-artifacts".to_string()),
        None => None,
    };
    let artifact_validation_state = routines
        .iter()
        .map(|entry| entry.artifact_validation.state.clone())
        .next()
        .unwrap_or_else(|| "pending-runtime-bind".to_string());
    let backend = summarize_selected_backends(&routines);
    RuntimeBridgeExplainInfo {
        language: "python".to_string(),
        backend,
        env_artifact_id: artifact_id,
        artifact_validation_state,
    }
}

fn summarize_selected_backends(routines: &[PreparedRoutineBase]) -> String {
    match routines.first() {
        Some(first)
            if routines.iter().all(|entry| {
                entry.runtime_binding.explain_label() == first.runtime_binding.explain_label()
            }) =>
        {
            first.runtime_binding.explain_label()
        }
        Some(_) => format!(
            "mixed({})",
            routines
                .iter()
                .map(|entry| entry.runtime_binding.explain_label())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        None => "process".to_string(),
    }
}

#[derive(Debug, Clone)]
struct PreparedArtifactValidation {
    artifact_id: String,
    state: String,
}

#[derive(Debug, Clone)]
struct PreparedRoutineBase {
    descriptor: ExternalRoutineDescriptor,
    module_path: PathBuf,
    handler: String,
    search_paths: Vec<String>,
    cache_key: String,
    artifact_validation: PreparedArtifactValidation,
    runtime_binding: PreparedRuntimeBinding,
}

impl AsRef<PreparedRoutineBase> for PreparedRoutineBase {
    fn as_ref(&self) -> &PreparedRoutineBase {
        self
    }
}

#[derive(Debug, Clone)]
struct PreparedRuntimeBinding {
    selection: BackendSelection,
    capability_profile_label: Option<String>,
    compiled_kernel_kind: Option<String>,
    subinterpreter_policy: Option<paro_routine::SubInterpreterPolicy>,
}

impl PreparedRuntimeBinding {
    fn backend_label(&self) -> &'static str {
        match self.selection.backend {
            BackendKind::Process => "process",
            BackendKind::SubInterpreter => "subinterpreter",
            BackendKind::Sandbox => "sandbox",
            BackendKind::CompiledKernel => "compiled_kernel",
            BackendKind::Remote => "remote",
        }
    }

    fn transport_label(&self) -> &'static str {
        match self.selection.transport {
            TransportKind::LocalShm => "local_shm",
            TransportKind::LocalIoUring => "local_io_uring",
            TransportKind::Remote => "remote",
        }
    }

    fn explain_label(&self) -> String {
        let backend = match self.selection.backend {
            BackendKind::Sandbox => self
                .selection
                .sandbox_runtime
                .map(|runtime| runtime.label().to_string())
                .unwrap_or_else(|| "sandbox".to_string()),
            BackendKind::CompiledKernel => self
                .compiled_kernel_kind
                .as_ref()
                .map(|kind| format!("compiled_kernel[{kind}]"))
                .unwrap_or_else(|| "compiled_kernel".to_string()),
            _ => self.backend_label().to_string(),
        };
        format!("{backend}@{}", self.transport_label())
    }
}

#[derive(Debug, Clone)]
struct PreparedProjectRoutine {
    base: PreparedRoutineBase,
    argument_names: Vec<String>,
    output_name: String,
    output_type: LogicalType,
    argument_expressions: Vec<Expression>,
}

impl PreparedProjectRoutine {
    fn prepare(
        session: &Arc<StatementContext>,
        expression: &ExternalProjectExpression,
        descriptor: &ExternalRoutineDescriptor,
    ) -> Result<Self> {
        let Expression::Function(function) = &expression.expression else {
            return Err(paro_error::internal(
                "external project expression must lower from a function call".to_string(),
            ));
        };
        let base = PreparedRoutineBase::prepare(session, &expression.routine_meta, descriptor)?;
        Ok(Self {
            argument_names: argument_names(base_spec(&expression.routine_meta)?)?,
            output_name: expression.output_name.clone(),
            output_type: expression.expression.return_type(),
            argument_expressions: function.children.clone(),
            base,
        })
    }
}

#[derive(Debug, Clone)]
struct PreparedTableRoutine {
    base: PreparedRoutineBase,
    output_names: Vec<String>,
    output_types: Vec<LogicalType>,
}

impl PreparedTableRoutine {
    fn prepare(
        session: &Arc<StatementContext>,
        routine_meta: &BoundRoutineCallMeta,
        descriptor: &ExternalRoutineDescriptor,
        output_types: &[LogicalType],
    ) -> Result<Self> {
        let spec = base_spec(routine_meta)?;
        let output_names = match &spec.return_type {
            RoutineReturn::Table(columns) => {
                columns.iter().map(|column| column.name.clone()).collect()
            }
            RoutineReturn::Scalar(_) => {
                return Err(paro_error::internal(
                    "external table routine requires RETURNS TABLE".to_string(),
                ))
            }
        };
        Ok(Self {
            base: PreparedRoutineBase::prepare(session, routine_meta, descriptor)?,
            output_names,
            output_types: output_types.to_vec(),
        })
    }
}

impl PreparedRoutineBase {
    fn prepare(
        session: &Arc<StatementContext>,
        routine_meta: &BoundRoutineCallMeta,
        descriptor: &ExternalRoutineDescriptor,
    ) -> Result<Self> {
        let spec = base_spec(routine_meta)?;
        let (implementation, handler) = python_implementation(spec)?;
        let artifact = materialize_artifact(session, spec, implementation, &handler)?;
        let runtime_binding = bind_runtime(session, spec, &artifact)?;
        Ok(Self {
            descriptor: descriptor.clone(),
            module_path: artifact.module_path,
            handler,
            search_paths: artifact.search_paths,
            cache_key: implementation.source_blob.id.clone(),
            artifact_validation: artifact.validation,
            runtime_binding,
        })
    }
}

fn base_spec(routine_meta: &BoundRoutineCallMeta) -> Result<&RoutineSpec> {
    routine_meta.spec.as_ref().ok_or_else(|| {
        paro_error::artifact_not_ready(
            "external routine reached execution without a bound RoutineSpec snapshot",
        )
    })
}

fn python_implementation(spec: &RoutineSpec) -> Result<(&PythonImplementationRef, String)> {
    let RoutineImplementationRef::Python(implementation) = &spec.implementation;
    let handler = match &implementation.entrypoint {
        PythonEntrypointRef::Batch { handler } => handler.clone(),
        _ => {
            return Err(paro_error::not_implemented(format!(
                "routine '{}' uses a non-batch Python entrypoint",
                spec.name
            )))
        }
    };
    Ok((implementation, handler))
}

fn argument_names(spec: &RoutineSpec) -> Result<Vec<String>> {
    Ok(spec
        .arguments
        .iter()
        .enumerate()
        .map(|(index, argument)| {
            argument
                .name
                .clone()
                .unwrap_or_else(|| format!("arg_{}", index + 1))
        })
        .collect())
}

#[derive(Debug, Clone)]
struct MaterializedPythonArtifact {
    module_path: PathBuf,
    search_paths: Vec<String>,
    validation: PreparedArtifactValidation,
    capabilities: ArtifactCapabilities,
    compiled_kernel_kind: Option<String>,
}

fn materialize_artifact(
    session: &Arc<StatementContext>,
    spec: &RoutineSpec,
    implementation: &PythonImplementationRef,
    handler: &str,
) -> Result<MaterializedPythonArtifact> {
    let runtime_contract = runtime_contract();
    let resolver = ArtifactResolver;
    let plan = resolver.resolve(
        &ResolveInputs {
            tenant_or_security_domain: format!(
                "{}/{}",
                session.current_database(),
                spec.permissions
                    .capability_profile
                    .profile_name
                    .clone()
                    .unwrap_or_else(|| "default".to_string())
            ),
            runtime_selector: format!("{:?}", implementation.runtime),
            env: spec.environment.clone(),
        },
        runtime_contract.clone(),
    );

    let artifact_root = artifact_root(&plan.artifact_id);
    let inline_root = artifact_root.join("inline");
    let imports_root = artifact_root.join("imports");
    fs::create_dir_all(&inline_root).map_err(io_error)?;
    fs::create_dir_all(&imports_root).map_err(io_error)?;

    let source = render_inline_source(spec, handler, &implementation.source_blob.inline_source)?;
    let module_path = inline_root.join(format!(
        "{}.py",
        sanitize_path_component(&implementation.source_blob.id)
    ));
    fs::write(&module_path, &source).map_err(io_error)?;

    materialize_imports(&spec.environment, &imports_root)?;
    let validator = ArtifactValidator;
    let validation = validator.to_validation_state(validator.validate(
        handler,
        &runtime_contract,
        &runtime_contract,
    ));
    let validation_label = match validation {
        ArtifactValidationState::Pending => "pending".to_string(),
        ArtifactValidationState::Ready {
            validated_handler,
            protocol_version,
        } => format!("ready({validated_handler}@v{protocol_version})"),
        ArtifactValidationState::Failed { reason } => {
            return Err(paro_error::artifact_not_ready(reason));
        }
    };
    let (capabilities, compiled_kernel_kind) = derive_artifact_capabilities(spec, &source);

    Ok(MaterializedPythonArtifact {
        module_path,
        search_paths: vec![imports_root.to_string_lossy().to_string()],
        validation: PreparedArtifactValidation {
            artifact_id: plan.artifact_id,
            state: validation_label,
        },
        capabilities,
        compiled_kernel_kind,
    })
}

fn bind_runtime(
    _session: &Arc<StatementContext>,
    spec: &RoutineSpec,
    artifact: &MaterializedPythonArtifact,
) -> Result<PreparedRuntimeBinding> {
    if let Some(reason) = compiled_kernel_requirement_error(
        &spec.permissions.capability_profile,
        &artifact.capabilities,
    ) {
        return Err(paro_error::artifact_not_ready(reason));
    }
    let selector = BackendSelector;
    let selection = selector
        .select(
            paro_routine::BackendSelectionInput {
                capability_profile: spec.permissions.capability_profile.clone(),
                artifact_capabilities: artifact.capabilities.clone(),
                runtime_contract: runtime_contract(),
                minimum_isolation: spec.permissions.capability_profile.minimum_isolation(),
                trusted_backend_preference: spec
                    .permissions
                    .capability_profile
                    .trusted_backend_preference(),
            },
            &BackendAvailability {
                local_process_ready: true,
                sandbox_ready: false,
                restricted_wasm_ready: false,
                mediated_sandbox_ready: false,
                microvm_ready: false,
                subinterpreter_ready: python_subinterpreter_supported(),
                compiled_kernel_ready: true,
                remote_ready: false,
                local_io_uring_ready: false,
            },
        )
        .map_err(|error| paro_error::artifact_not_ready(error.detail))?;
    let subinterpreter_policy = (selection.backend == BackendKind::SubInterpreter)
        .then(|| spec.permissions.capability_profile.subinterpreter.clone());
    Ok(PreparedRuntimeBinding {
        selection,
        capability_profile_label: spec.permissions.capability_profile.profile_name.clone(),
        compiled_kernel_kind: artifact.compiled_kernel_kind.clone(),
        subinterpreter_policy,
    })
}

fn python_binary() -> OsString {
    env::var_os(PYTHON_BIN_ENV).unwrap_or_else(|| OsString::from(DEFAULT_PYTHON_BIN))
}

fn python_subinterpreter_supported() -> bool {
    let python_bin = python_binary();
    let cache = SUBINTERPRETER_SUPPORT_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(cached) = cache.lock().get(&python_bin).copied() {
        return cached;
    }
    let supported = Command::new(&python_bin)
        .arg("-c")
        .arg("from concurrent import interpreters")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success());
    cache.lock().insert(python_bin, supported);
    supported
}

fn compiled_kernel_requirement_error(
    profile: &paro_routine::CapabilityProfile,
    capabilities: &ArtifactCapabilities,
) -> Option<String> {
    if profile.trusted_backend_preference()
        != paro_routine::TrustedBackendPreference::CompiledKernel
    {
        return None;
    }

    let label = profile.profile_name.as_deref().unwrap_or("compiled_kernel");
    if !capabilities.supports_compiled_kernel_backend {
        return Some(format!(
            "capability profile `{label}` requires a registered compiled kernel candidate"
        ));
    }
    if !profile.allows_compiled_native_extensions()
        && profile.allows_native_jit_compiled_kernel()
        && !capabilities.supports_native_jit_backend
    {
        return Some(format!(
            "capability profile `{label}` requires a zero-dependency native-jit compiled kernel candidate"
        ));
    }
    None
}

fn derive_artifact_capabilities(
    spec: &RoutineSpec,
    source: &str,
) -> (ArtifactCapabilities, Option<String>) {
    let compiled_kernel_kind = detect_compiled_kernel_kind(source);
    let lower = source.to_ascii_lowercase();
    (
        ArtifactCapabilities {
            supports_process_backend: true,
            supports_subinterpreter_backend: true,
            supports_subinterpreter_import_policy: true,
            supports_compiled_kernel_backend: compiled_kernel_kind.is_some(),
            supports_native_jit_backend: compiled_kernel_kind.as_deref() == Some("jit"),
            supports_hpy_universal_abi: compiled_kernel_kind.as_deref() == Some("hpy"),
            supports_free_threaded_python: false,
            supports_arrow_c_stream_adapter: true,
            supports_arrow_py_capsule_protocol: true,
            supports_kernel_fusion: matches!(
                spec.semantics.row_semantics,
                paro_routine::RowSemantics::RowPreserving
            ) && spec.semantics.side_effects
                == paro_routine::RoutineSideEffects::None,
            supports_restricted_wasm_backend: spec
                .permissions
                .capability_profile
                .native_extension_policy
                == paro_routine::CapabilityPolicy::Deny
                && spec.environment.packages.is_empty()
                && !lower.contains("subprocess")
                && !lower.contains("socket")
                && !lower.contains("ctypes")
                && !lower.contains("cffi")
                && !lower.contains("threading")
                && !lower.contains("multiprocessing")
                && !lower.contains("os.fork"),
            supports_mediated_sandbox_backend: true,
            supports_microvm_backend: true,
            supports_remote_transport: false,
            validated_native_extensions: spec
                .permissions
                .capability_profile
                .native_extension_policy
                != paro_routine::CapabilityPolicy::Deny
                || spec.environment.packages.is_empty(),
            requires_gpu: lower.contains("cuda") || lower.contains("gpu"),
        },
        compiled_kernel_kind,
    )
}

fn detect_compiled_kernel_kind(source: &str) -> Option<String> {
    let lower = source.to_ascii_lowercase();
    if !(lower.contains("@register_compiled_kernel")
        || lower.contains("register_compiled_kernel(")
        || lower.contains("@register_native_jit_kernel")
        || lower.contains("register_native_jit_kernel("))
    {
        return None;
    }
    if lower.contains("@register_native_jit_kernel")
        || lower.contains("register_native_jit_kernel(")
    {
        return Some("jit".to_string());
    }
    for kind in ["numba", "hpy", "pyo3", "aot", "jit"] {
        let single = format!("kind='{kind}'");
        let double = format!("kind=\"{kind}\"");
        if lower.contains(&single) || lower.contains(&double) {
            return Some(kind.to_string());
        }
    }
    Some("compiled".to_string())
}

fn artifact_root(artifact_id: &str) -> PathBuf {
    env::temp_dir()
        .join("paro-python-udf")
        .join(sanitize_path_component(artifact_id))
}

fn render_inline_source(spec: &RoutineSpec, handler: &str, inline_source: &str) -> Result<String> {
    let handler_name = if handler.contains('.') {
        return Err(paro_error::artifact_not_ready(format!(
            "handler '{}' must be a top-level function name when CREATE FUNCTION stores a body snippet",
            handler
        )));
    } else {
        handler.to_string()
    };

    if inline_source.contains(&format!("def {handler_name}(")) {
        return Ok(inline_source.to_string());
    }

    let signature = spec
        .arguments
        .iter()
        .enumerate()
        .map(|(index, argument)| {
            argument
                .name
                .clone()
                .unwrap_or_else(|| format!("arg_{}", index + 1))
        })
        .collect::<Vec<_>>();
    let mut rendered = format!(
        "from __future__ import annotations\n\n\ndef {handler_name}(ctx, {}):\n",
        signature.join(", ")
    );
    let body = inline_source.trim_end();
    if body.is_empty() {
        rendered.push_str("    pass\n");
    } else {
        for line in body.lines() {
            rendered.push_str("    ");
            rendered.push_str(line);
            rendered.push('\n');
        }
    }
    Ok(rendered)
}

fn materialize_imports(env_spec: &DeclaredEnvSpec, imports_root: &Path) -> Result<()> {
    for import in &env_spec.imports {
        let source = resolve_import_path(&import.uri)?;
        let destination =
            imports_root.join(source.file_name().ok_or_else(|| {
                paro_error::artifact_not_ready("import path is missing a filename")
            })?);
        if source.is_dir() {
            copy_dir_recursive(&source, &destination)?;
        } else {
            fs::copy(&source, &destination).map_err(io_error)?;
        }
    }
    Ok(())
}

fn resolve_import_path(uri: &str) -> Result<PathBuf> {
    let path = if let Some(rest) = uri.strip_prefix("file://") {
        PathBuf::from(rest)
    } else {
        PathBuf::from(uri)
    };
    if path.exists() {
        Ok(path)
    } else {
        Err(paro_error::artifact_not_ready(format!(
            "import '{}' does not exist on the local filesystem",
            uri
        )))
    }
}

fn copy_dir_recursive(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination).map_err(io_error)?;
    for entry in fs::read_dir(source).map_err(io_error)? {
        let entry = entry.map_err(io_error)?;
        let source_path = entry.path();
        let target_path = destination.join(entry.file_name());
        if source_path.is_dir() {
            copy_dir_recursive(&source_path, &target_path)?;
        } else {
            fs::copy(&source_path, &target_path).map_err(io_error)?;
        }
    }
    Ok(())
}

#[derive(Debug)]
struct PythonProcessProjectKernel {
    prepared: Vec<PreparedProjectRoutine>,
    worker: Arc<PythonWorkerClient>,
}

impl PythonProcessProjectKernel {
    fn unbound() -> Self {
        Self {
            prepared: Vec::new(),
            worker: Arc::new(PythonWorkerClient::new()),
        }
    }
}

impl ProjectBridgeKernel for PythonProcessProjectKernel {
    fn execute(
        &self,
        ctx: &ExecutionContext,
        submission: &ProjectSubmission<'_>,
    ) -> Result<RuntimeBridgeOutcome> {
        if self.prepared.is_empty() {
            return Err(paro_error::internal(
                "python project kernel was not bound to a prepared routine set".to_string(),
            ));
        }

        let started_at = Instant::now();
        let mut metrics = RuntimeBridgeMetrics::default();
        let mut generated_columns = Vec::with_capacity(self.prepared.len());

        for routine in &self.prepared {
            let input_chunk = evaluate_argument_chunk(
                submission.input,
                ctx,
                &routine.argument_expressions,
                &routine.argument_names,
            )?;
            let input_bytes = SubmissionBatchPolicy::estimate_chunk_bytes(&input_chunk);
            let result = self.worker.invoke(
                ctx.session.clone(),
                &routine.base,
                submission.batch_id,
                &input_chunk,
                Some(input_chunk.size()),
                std::slice::from_ref(&routine.output_type),
                std::slice::from_ref(&routine.output_name),
            )?;
            metrics.worker_acquire_time_us = metrics
                .worker_acquire_time_us
                .saturating_add(result.metrics.worker_acquire_time_us);
            metrics.kernel_time_us = metrics
                .kernel_time_us
                .saturating_add(result.metrics.kernel_time_us);
            metrics.encode_decode_time_us = metrics
                .encode_decode_time_us
                .saturating_add(result.metrics.encode_decode_time_us);
            metrics.data_plane_bytes = metrics
                .data_plane_bytes
                .saturating_add(input_bytes)
                .saturating_add(result.metrics.output_bytes);
            metrics.output_bytes = metrics
                .output_bytes
                .saturating_add(result.metrics.output_bytes);
            metrics.output_rows = result.metrics.output_rows;
            metrics.warm_state = merge_warm_state(metrics.warm_state, result.metrics.warm_state);
            generated_columns.push(
                result
                    .chunk
                    .column(0)
                    .expect("scalar routine must return one column")
                    .as_ref()
                    .clone(),
            );
        }

        metrics.output_rows = submission.input.size() as u64;
        metrics.queue_wait_us = 0;
        metrics.retired_count = 0;
        metrics.cache_hit = false;
        metrics.kernel_time_us = metrics
            .kernel_time_us
            .max(started_at.elapsed().as_micros() as u64);

        Ok(RuntimeBridgeOutcome::Ready(RuntimeBridgeResponse {
            output_batches: vec![Chunk::from_vectors(generated_columns)],
            metrics,
        }))
    }
}

#[derive(Debug)]
struct PythonProcessTableKernel {
    prepared: PreparedTableRoutine,
    worker: Arc<PythonWorkerClient>,
}

impl PythonProcessTableKernel {
    fn unbound() -> Self {
        Self {
            prepared: PreparedTableRoutine {
                base: PreparedRoutineBase {
                    descriptor: ExternalRoutineDescriptor {
                        label: "__unbound__".to_string(),
                        identity: paro_routine::RoutineCallIdentity::Catalog {
                            routine_id: paro_routine::RoutineId::from_raw(0),
                            generation: 0,
                        },
                        semantics: paro_routine::RoutineSemantics {
                            stability: paro_routine::RoutineStability::Volatile,
                            null_policy: paro_routine::RoutineNullPolicy::CalledOnNullInput,
                            side_effects: paro_routine::RoutineSideEffects::HasSideEffects,
                            row_semantics: paro_routine::RowSemantics::RelationExpanding,
                            may_block: true,
                        },
                    },
                    module_path: PathBuf::new(),
                    handler: "batch".to_string(),
                    search_paths: Vec::new(),
                    cache_key: "__unbound__".to_string(),
                    artifact_validation: PreparedArtifactValidation {
                        artifact_id: "unbound".to_string(),
                        state: "unbound".to_string(),
                    },
                    runtime_binding: PreparedRuntimeBinding {
                        selection: BackendSelection {
                            backend: BackendKind::Process,
                            isolation:
                                paro_external_runtime::backend::selector::IsolationLevel::Process,
                            transport: TransportKind::LocalShm,
                            sandbox_runtime: None,
                            input: paro_routine::BackendSelectionInput {
                                capability_profile:
                                    paro_routine::CapabilityProfile::process_default(),
                                artifact_capabilities: ArtifactCapabilities {
                                    supports_process_backend: true,
                                    supports_subinterpreter_backend: false,
                                    supports_subinterpreter_import_policy: false,
                                    supports_compiled_kernel_backend: false,
                                    supports_native_jit_backend: false,
                                    supports_hpy_universal_abi: false,
                                    supports_free_threaded_python: false,
                                    supports_arrow_c_stream_adapter: true,
                                    supports_arrow_py_capsule_protocol: true,
                                    supports_kernel_fusion: false,
                                    supports_restricted_wasm_backend: false,
                                    supports_mediated_sandbox_backend: false,
                                    supports_microvm_backend: false,
                                    supports_remote_transport: false,
                                    validated_native_extensions: false,
                                    requires_gpu: false,
                                },
                                runtime_contract: runtime_contract(),
                                minimum_isolation: paro_routine::MinimumIsolation::Process,
                                trusted_backend_preference:
                                    paro_routine::TrustedBackendPreference::Automatic,
                            },
                        },
                        capability_profile_label: None,
                        compiled_kernel_kind: None,
                        subinterpreter_policy: None,
                    },
                },
                output_names: Vec::new(),
                output_types: Vec::new(),
            },
            worker: Arc::new(PythonWorkerClient::new()),
        }
    }
}

impl TableBridgeKernel for PythonProcessTableKernel {
    fn execute(
        &self,
        ctx: &ExecutionContext,
        submission: &TableSubmission<'_>,
    ) -> Result<RuntimeBridgeOutcome> {
        if self.prepared.output_types.is_empty() {
            return Err(paro_error::internal(
                "python table kernel was not bound to a prepared routine".to_string(),
            ));
        }
        let input_bytes = SubmissionBatchPolicy::estimate_chunk_bytes(submission.input);
        let result = self.worker.invoke(
            ctx.session.clone(),
            &self.prepared.base,
            submission.batch_id,
            submission.input,
            if matches!(
                self.prepared.base.descriptor.semantics.row_semantics,
                paro_routine::RowSemantics::RowPreserving
            ) {
                Some(submission.input.size())
            } else {
                None
            },
            &self.prepared.output_types,
            &self.prepared.output_names,
        )?;
        let mut metrics = result.metrics;
        metrics.data_plane_bytes = input_bytes.saturating_add(metrics.output_bytes);
        Ok(RuntimeBridgeOutcome::Ready(RuntimeBridgeResponse {
            output_batches: vec![result.chunk],
            metrics,
        }))
    }
}

fn evaluate_argument_chunk(
    input: &Chunk,
    ctx: &ExecutionContext,
    expressions: &[Expression],
    _argument_names: &[String],
) -> Result<Chunk> {
    if expressions.is_empty() {
        return Ok(Chunk::new());
    }
    let mut executor = ExpressionExecutor::with_expressions(expressions);
    let mut chunk = Chunk::with_allocator(ctx.allocator(MemoryTag::Extension));
    executor.execute_all_into(input, ctx, &mut chunk)?;
    Ok(chunk)
}

#[derive(Debug)]
struct WorkerInvokeResult {
    chunk: Chunk,
    metrics: RuntimeBridgeMetrics,
}

#[derive(Debug, Default)]
struct PythonWorkerClient {
    process: Mutex<Option<PythonWorkerProcess>>,
}

impl PythonWorkerClient {
    fn new() -> Self {
        Self {
            process: Mutex::new(None),
        }
    }

    fn invoke(
        &self,
        session: Arc<StatementContext>,
        routine: &PreparedRoutineBase,
        batch_id: u64,
        input: &Chunk,
        expected_output_rows: Option<usize>,
        output_types: &[LogicalType],
        output_names: &[String],
    ) -> Result<WorkerInvokeResult> {
        session.ensure_python_runtime_ready_for_execution()?;

        let started_at = Instant::now();
        let mut guard = self.process.lock();
        let warm_state = if guard.is_some() {
            RuntimeWarmState::Warm
        } else {
            match PythonWorkerProcess::spawn() {
                Ok(process) => *guard = Some(process),
                Err(error) => {
                    observe_worker_failure(&session, &error);
                    return Err(error);
                }
            }
            RuntimeWarmState::Cold
        };
        let process = guard
            .as_mut()
            .expect("python worker process should be initialized");

        let request = WorkerRequest::from_chunk(
            batch_id,
            routine,
            input,
            expected_output_rows,
            output_types,
            output_names,
            session.transaction_id(),
        )?;
        let response = match process.exchange(&request) {
            Ok(response) => response,
            Err(error) => {
                *guard = None;
                observe_worker_failure(&session, &error);
                return Err(error);
            }
        };
        if response.header.batch_id != batch_id {
            *guard = None;
            let error = paro_error::external_protocol_mismatch(format!(
                "worker response batch_id {} does not match request {}",
                response.header.batch_id, batch_id
            ));
            observe_worker_failure(&session, &error);
            return Err(error);
        }

        let chunk = match decode_response_chunk(&response.payload, output_types) {
            Ok(chunk) => chunk,
            Err(error) => {
                *guard = None;
                observe_worker_failure(&session, &error);
                return Err(error);
            }
        };
        let output_rows = chunk.size() as u64;
        let output_bytes = SubmissionBatchPolicy::estimate_chunk_bytes(&chunk);
        Ok(WorkerInvokeResult {
            chunk,
            metrics: RuntimeBridgeMetrics {
                worker_acquire_time_us: 1,
                queue_wait_us: 0,
                kernel_time_us: started_at.elapsed().as_micros() as u64,
                encode_decode_time_us: started_at.elapsed().as_micros() as u64,
                data_plane_bytes: 0,
                cache_hit: false,
                warm_state,
                retired_count: 0,
                output_rows,
                output_bytes,
            },
        })
    }
}

fn observe_worker_failure(session: &StatementContext, error: &paro_common::error::ParoError) {
    if !should_mark_runtime_degraded(error) {
        return;
    }
    if let Some(provider) = session.services.python_runtime.as_ref() {
        provider.observe_worker_failure(&error.to_string());
    }
}

fn should_mark_runtime_degraded(error: &paro_common::error::ParoError) -> bool {
    error.is(paro_common::error::codes::external_routine::WORKER_FAILURE)
        || error.is(paro_common::error::codes::external_routine::PROTOCOL_MISMATCH)
        || error.is(paro_common::error::codes::external_routine::EPOCH_MISMATCH)
}

#[derive(Debug)]
struct PythonWorkerProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl PythonWorkerProcess {
    fn spawn() -> Result<Self> {
        let python_bin =
            env::var_os(PYTHON_BIN_ENV).unwrap_or_else(|| OsString::from(DEFAULT_PYTHON_BIN));
        let pythonpath = env::join_paths([
            repo_root().join("python/paro_udf/src"),
            repo_root().join("runtimes/python-worker/src"),
        ])
        .map_err(|error| {
            paro_error::worker_failure(format!("failed to build PYTHONPATH: {error}"))
        })?;

        let mut child = Command::new(python_bin)
            .arg("-m")
            .arg("paro_runtime_worker")
            .current_dir(repo_root())
            .env("PYTHONUNBUFFERED", "1")
            .env("PYTHONPATH", pythonpath)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|error| {
                paro_error::worker_failure(format!("failed to spawn python worker: {error}"))
            })?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| paro_error::worker_failure("python worker stdin is unavailable"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| paro_error::worker_failure("python worker stdout is unavailable"))?;
        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
        })
    }

    fn exchange(&mut self, request: &WorkerRequest) -> Result<WorkerResponse> {
        let frame = json!({
            "header": encode_hex(&request.header),
            "payload": request.payload,
        });
        let serialized = serde_json::to_string(&frame).map_err(|error| {
            paro_error::worker_failure(format!("failed to encode worker request: {error}"))
        })?;
        writeln!(self.stdin, "{serialized}")
            .and_then(|_| self.stdin.flush())
            .map_err(|error| {
                paro_error::worker_failure(format!("failed to write worker request: {error}"))
            })?;

        let mut line = String::new();
        self.stdout.read_line(&mut line).map_err(|error| {
            paro_error::worker_failure(format!("failed to read worker response: {error}"))
        })?;
        if line.trim().is_empty() {
            let status = self.child.try_wait().map_err(|error| {
                paro_error::worker_failure(format!("failed to query worker status: {error}"))
            })?;
            let status = match status {
                Some(status) => Some(status),
                None => Some(self.child.wait().map_err(|error| {
                    paro_error::worker_failure(format!(
                        "failed to wait for worker exit after stdout closed: {error}"
                    ))
                })?),
            };
            return Err(paro_error::worker_failure(format!(
                "python worker closed stdout unexpectedly (status: {status:?})"
            )));
        }

        let payload: JsonValue = serde_json::from_str(&line).map_err(|error| {
            paro_error::worker_failure(format!("failed to decode worker response: {error}"))
        })?;
        let header_hex = payload
            .get("header")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| {
                paro_error::external_protocol_mismatch(
                    "worker response is missing a control header",
                )
            })?;
        let header = ControlHeader::decode(&decode_hex(header_hex)?)
            .map_err(|error| paro_error::external_protocol_mismatch(error.to_string()))?;
        let body = payload
            .get("payload")
            .cloned()
            .unwrap_or_else(|| JsonValue::Object(Default::default()));

        match header
            .kind()
            .map_err(|error| paro_error::external_protocol_mismatch(error.to_string()))?
        {
            ControlMessageKind::Complete => Ok(WorkerResponse {
                header,
                payload: body,
            }),
            ControlMessageKind::Error => {
                let error =
                    serde_json::from_value::<PythonExceptionPayload>(body).map_err(|error| {
                        paro_error::external_protocol_mismatch(format!(
                            "failed to decode Python exception payload: {error}"
                        ))
                    })?;
                Err(paro_error::python_exception(format!(
                    "{}: {}",
                    error.exception_type, error.message
                ))
                .detail(error.formatted_traceback)
                .hint(format!("module={} handler={}", error.module, error.handler)))
            }
            other => Err(paro_error::external_protocol_mismatch(format!(
                "unexpected worker response kind {other:?}"
            ))),
        }
    }
}

#[derive(Debug)]
struct WorkerRequest {
    header: [u8; 32],
    payload: JsonValue,
}

impl WorkerRequest {
    fn from_chunk(
        batch_id: u64,
        routine: &PreparedRoutineBase,
        input: &Chunk,
        expected_output_rows: Option<usize>,
        output_types: &[LogicalType],
        output_names: &[String],
        query_epoch: u64,
    ) -> Result<Self> {
        let encoded = encode_chunk_to_abi(input, query_epoch)?;
        let payload = json!({
            "module_path": routine.module_path.to_string_lossy(),
            "handler": routine.handler,
            "cache_key": routine.cache_key,
            "search_paths": routine.search_paths,
            "lease": serde_json::to_value(&encoded.lease).map_err(json_error)?,
            "buffers": encoded.buffers.iter().map(|buffer| encode_hex(buffer)).collect::<Vec<_>>(),
            "context": {
                "query_id": query_epoch.to_string(),
                "routine_identity": routine.descriptor.identity_label(),
                "capability_profile": routine.runtime_binding.capability_profile_label,
                "execution_backend": routine.runtime_binding.backend_label(),
                "output_row_hint": expected_output_rows,
                "metadata": {
                    "routine_label": routine.descriptor.label,
                    "transport": routine.runtime_binding.transport_label(),
                    "compiled_kernel_kind": routine.runtime_binding.compiled_kernel_kind,
                    "subinterpreter_policy": routine.runtime_binding.subinterpreter_policy,
                },
            },
            "expected_row_count": expected_output_rows,
            "return_types": output_types.iter().map(logical_type_to_json).collect::<Result<Vec<_>>>()?,
            "output_names": output_names,
            "returns_nullable": true,
            "ownership": {
                "owner_worker_epoch": 0_u64,
                "owner_host_epoch": 0_u64,
                "owner_query_epoch": query_epoch,
            },
            "completion_fence": 0_u64,
        });
        let payload_len = serde_json::to_vec(&payload).map_err(json_error)?.len() as u32;
        Ok(Self {
            header: ControlHeader::new(
                ControlMessageKind::Submit,
                batch_id,
                encoded.lease.lease_id,
                payload_len,
            )
            .encode(),
            payload,
        })
    }
}

#[derive(Debug)]
struct WorkerResponse {
    header: ControlHeader,
    payload: JsonValue,
}

#[derive(Debug)]
struct EncodedAbiChunk {
    lease: ColumnBatchLease,
    buffers: Vec<Vec<u8>>,
}

fn encode_chunk_to_abi(input: &Chunk, query_epoch: u64) -> Result<EncodedAbiChunk> {
    let mut buffers = Vec::new();
    let mut columns = Vec::with_capacity(input.column_count());
    for index in 0..input.column_count() {
        let column = input
            .column(index)
            .ok_or_else(|| paro_error::contract_violation("missing input chunk column"))?;
        columns.push(encode_vector(
            column,
            input.size(),
            &format!("c{}", index),
            &mut buffers,
        )?);
    }

    let lease = ColumnBatchLease {
        version: 1,
        lease_id: 1,
        row_count: input.size() as u32,
        state: LeaseState::Committed,
        ownership: LeaseOwnership {
            owner_worker_epoch: 0,
            owner_host_epoch: 0,
            owner_query_epoch: query_epoch,
        },
        completion_fence: 0,
        payload_checksum: None,
        columns,
    };
    Ok(EncodedAbiChunk { lease, buffers })
}

fn encode_vector(
    column: &Vector,
    row_count: usize,
    name: &str,
    buffers: &mut Vec<Vec<u8>>,
) -> Result<ColumnDescriptor> {
    let logical_type = logical_type_to_abi(column.logical_type()).ok_or_else(|| {
        paro_error::contract_violation(format!(
            "logical type '{}' is not supported by the Python worker bridge yet",
            column.logical_type()
        ))
    })?;
    let null_mask = (0..row_count)
        .map(|row| column.is_null(row))
        .collect::<Vec<_>>();
    let validity = if null_mask.iter().any(|is_null| *is_null) {
        let bitmap = pack_validity_bitmap(&null_mask);
        let buffer_index = buffers.len() as u16;
        buffers.push(bitmap);
        Some(BufferLease::host(
            buffer_index,
            0,
            buffers.last().expect("validity").len() as u64,
            1,
        ))
    } else {
        None
    };

    match logical_type {
        AbiLogicalType::Varchar
        | AbiLogicalType::Blob
        | AbiLogicalType::Json
        | AbiLogicalType::Jsonb => {
            let mut offsets = Vec::with_capacity((row_count + 1) * 4);
            let mut data = Vec::new();
            let mut current = 0_u32;
            offsets.extend_from_slice(&current.to_le_bytes());
            for row in 0..row_count {
                let bytes = if column.is_null(row) {
                    Vec::new()
                } else {
                    match column.get_value(row) {
                        Value::Varchar(value) => value.into_bytes(),
                        Value::Blob(value) => value,
                        other => {
                            return Err(paro_error::contract_violation(format!(
                                "expected varlen value for '{}', got {other:?}",
                                column.logical_type()
                            )))
                        }
                    }
                };
                data.extend_from_slice(&bytes);
                current = current.saturating_add(bytes.len() as u32);
                offsets.extend_from_slice(&current.to_le_bytes());
            }
            let offsets_index = buffers.len() as u16;
            buffers.push(offsets);
            let data_index = buffers.len() as u16;
            buffers.push(data);
            Ok(ColumnDescriptor {
                name: name.to_string(),
                logical_type,
                encoding: ColumnEncoding::Flat,
                population_mode: ColumnPopulationMode::Eager,
                nullable: validity.is_some(),
                validity,
                layout: ColumnLayout::VarLen {
                    offsets: BufferLease::host(
                        offsets_index,
                        0,
                        buffers[offsets_index as usize].len() as u64,
                        4,
                    ),
                    data: BufferLease::host(
                        data_index,
                        0,
                        buffers[data_index as usize].len() as u64,
                        1,
                    ),
                    offset_width: OffsetWidth::U32,
                },
                children: Vec::new(),
            })
        }
        _ => {
            let stride = logical_type
                .fixed_width_bytes()
                .ok_or_else(|| paro_error::contract_violation("missing fixed-width stride"))?;
            let mut values = Vec::with_capacity(row_count * stride as usize);
            for row in 0..row_count {
                encode_fixed_width_value(&mut values, &column.get_value(row), &logical_type)?;
            }
            let buffer_index = buffers.len() as u16;
            buffers.push(values);
            Ok(ColumnDescriptor {
                name: name.to_string(),
                logical_type,
                encoding: ColumnEncoding::Flat,
                population_mode: ColumnPopulationMode::Eager,
                nullable: validity.is_some(),
                validity,
                layout: ColumnLayout::FixedWidth {
                    values: BufferLease::host(
                        buffer_index,
                        0,
                        buffers[buffer_index as usize].len() as u64,
                        stride,
                    ),
                    stride,
                },
                children: Vec::new(),
            })
        }
    }
}

fn decode_response_chunk(payload: &JsonValue, expected_types: &[LogicalType]) -> Result<Chunk> {
    let state = payload
        .get("state")
        .and_then(JsonValue::as_str)
        .unwrap_or("Unknown");
    if state != "Finished" {
        return Err(paro_error::contract_violation(format!(
            "python worker returned unexpected terminal state '{state}'"
        )));
    }
    let lease_value = payload.get("lease").cloned().ok_or_else(|| {
        paro_error::external_protocol_mismatch("worker response is missing lease metadata")
    })?;
    let lease: ColumnBatchLease = serde_json::from_value(lease_value).map_err(json_error)?;
    if lease.state != LeaseState::Committed {
        return Err(paro_error::contract_violation(format!(
            "worker returned lease in state {:?}",
            lease.state
        )));
    }
    let buffer_values = payload
        .get("buffers")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| {
            paro_error::external_protocol_mismatch("worker response is missing buffers")
        })?;
    let buffers = buffer_values
        .iter()
        .map(|value| {
            let hex = value.as_str().ok_or_else(|| {
                paro_error::external_protocol_mismatch("worker buffer payload must be hex strings")
            })?;
            decode_hex(hex)
        })
        .collect::<Result<Vec<_>>>()?;

    if lease.columns.len() != expected_types.len() {
        return Err(paro_error::contract_violation(format!(
            "worker returned {} columns, expected {}",
            lease.columns.len(),
            expected_types.len()
        )));
    }

    let mut vectors = Vec::with_capacity(expected_types.len());
    for (descriptor, expected_type) in lease.columns.iter().zip(expected_types.iter()) {
        let mut vector = Vector::with_capacity_and_allocator(
            expected_type.clone(),
            lease.row_count as usize,
            Arc::new(paro_common::allocator::default_allocator()),
        );
        decode_descriptor_into_vector(
            descriptor,
            &buffers,
            lease.row_count as usize,
            &mut vector,
            expected_type,
        )?;
        vector.set_len(lease.row_count as usize);
        vectors.push(vector);
    }
    Ok(Chunk::from_vectors(vectors))
}

fn decode_descriptor_into_vector(
    descriptor: &ColumnDescriptor,
    buffers: &[Vec<u8>],
    row_count: usize,
    vector: &mut Vector,
    expected_type: &LogicalType,
) -> Result<()> {
    let expected_abi = logical_type_to_abi(expected_type).ok_or_else(|| {
        paro_error::contract_violation(format!("unsupported output type '{expected_type}'"))
    })?;
    if descriptor.logical_type != expected_abi {
        return Err(paro_error::contract_violation(format!(
            "worker returned logical type {:?}, expected {:?}",
            descriptor.logical_type, expected_abi
        )));
    }

    let null_mask = if let Some(lease) = descriptor.validity.as_ref() {
        unpack_validity_bitmap(buffer_from_lease(buffers, lease)?, row_count)
    } else {
        vec![false; row_count]
    };

    match &descriptor.layout {
        ColumnLayout::FixedWidth { values, .. } => {
            let buffer = buffer_from_lease(buffers, values)?;
            for row in 0..row_count {
                if null_mask[row] {
                    vector.validity_mut().set_null(row);
                    continue;
                }
                let value = decode_fixed_width_value(&descriptor.logical_type, buffer, row)?;
                vector.set_value(row, &value);
            }
        }
        ColumnLayout::VarLen { offsets, data, .. } => {
            let offsets_buffer = buffer_from_lease(buffers, offsets)?;
            let data_buffer = buffer_from_lease(buffers, data)?;
            for row in 0..row_count {
                if null_mask[row] {
                    vector.validity_mut().set_null(row);
                    continue;
                }
                let start = read_u32(offsets_buffer, row * 4)? as usize;
                let end = read_u32(offsets_buffer, (row + 1) * 4)? as usize;
                let value = match descriptor.logical_type {
                    AbiLogicalType::Varchar | AbiLogicalType::Json => Value::Varchar(
                        String::from_utf8(data_buffer[start..end].to_vec()).map_err(|error| {
                            paro_error::contract_violation(format!(
                                "worker returned invalid UTF-8 output: {error}"
                            ))
                        })?,
                    ),
                    AbiLogicalType::Blob | AbiLogicalType::Jsonb => {
                        Value::Blob(data_buffer[start..end].to_vec())
                    }
                    _ => {
                        return Err(paro_error::contract_violation(format!(
                            "varlen output is not supported for {:?}",
                            descriptor.logical_type
                        )))
                    }
                };
                vector.set_value(row, &value);
            }
        }
        ColumnLayout::Constant { value } => {
            let scalar = scalar_value_to_runtime(value)?;
            for row in 0..row_count {
                if null_mask[row] {
                    vector.validity_mut().set_null(row);
                } else {
                    vector.set_value(row, &scalar);
                }
            }
        }
        ColumnLayout::Sequence { start, step } => {
            for row in 0..row_count {
                if null_mask[row] {
                    vector.validity_mut().set_null(row);
                    continue;
                }
                let scalar = match descriptor.logical_type {
                    AbiLogicalType::Int32 => Value::Integer((*start + *step * row as i64) as i32),
                    AbiLogicalType::Int64 => Value::BigInt(*start + *step * row as i64),
                    _ => {
                        return Err(paro_error::contract_violation(format!(
                            "sequence output is unsupported for {:?}",
                            descriptor.logical_type
                        )))
                    }
                };
                vector.set_value(row, &scalar);
            }
        }
        other => {
            return Err(paro_error::contract_violation(format!(
                "worker returned unsupported layout {other:?}"
            )))
        }
    }

    Ok(())
}

fn buffer_from_lease<'a>(buffers: &'a [Vec<u8>], lease: &BufferLease) -> Result<&'a [u8]> {
    let buffer = buffers.get(lease.buffer_index as usize).ok_or_else(|| {
        paro_error::external_protocol_mismatch(format!(
            "buffer index {} is out of range",
            lease.buffer_index
        ))
    })?;
    let start = lease.offset as usize;
    let end = start.saturating_add(lease.len as usize);
    buffer.get(start..end).ok_or_else(|| {
        paro_error::external_protocol_mismatch(format!(
            "buffer slice [{}..{}) exceeds buffer length {}",
            start,
            end,
            buffer.len()
        ))
    })
}

fn decode_fixed_width_value(
    logical_type: &AbiLogicalType,
    buffer: &[u8],
    row: usize,
) -> Result<Value> {
    let offset = row
        .checked_mul(logical_type.fixed_width_bytes().unwrap_or_default() as usize)
        .ok_or_else(|| paro_error::contract_violation("fixed-width row offset overflow"))?;
    match logical_type {
        AbiLogicalType::Boolean => Ok(Value::Boolean(buffer[offset] != 0)),
        AbiLogicalType::Int8 => Ok(Value::TinyInt(buffer[offset] as i8)),
        AbiLogicalType::UInt8 => Ok(Value::UTinyInt(buffer[offset])),
        AbiLogicalType::Int16 => Ok(Value::SmallInt(i16::from_le_bytes(
            buffer[offset..offset + 2].try_into().expect("i16 bytes"),
        ))),
        AbiLogicalType::UInt16 => Ok(Value::USmallInt(u16::from_le_bytes(
            buffer[offset..offset + 2].try_into().expect("u16 bytes"),
        ))),
        AbiLogicalType::Int32 => Ok(Value::Integer(i32::from_le_bytes(
            buffer[offset..offset + 4].try_into().expect("i32 bytes"),
        ))),
        AbiLogicalType::Date => Ok(Value::Date(i32::from_le_bytes(
            buffer[offset..offset + 4].try_into().expect("date bytes"),
        ))),
        AbiLogicalType::UInt32 => Ok(Value::UInteger(u32::from_le_bytes(
            buffer[offset..offset + 4].try_into().expect("u32 bytes"),
        ))),
        AbiLogicalType::Int64 => Ok(Value::BigInt(i64::from_le_bytes(
            buffer[offset..offset + 8].try_into().expect("i64 bytes"),
        ))),
        AbiLogicalType::Time => Ok(Value::Time(i64::from_le_bytes(
            buffer[offset..offset + 8].try_into().expect("time bytes"),
        ))),
        AbiLogicalType::Timestamp => Ok(Value::Timestamp(i64::from_le_bytes(
            buffer[offset..offset + 8]
                .try_into()
                .expect("timestamp bytes"),
        ))),
        AbiLogicalType::TimestampTz => Ok(Value::TimestampTz(i64::from_le_bytes(
            buffer[offset..offset + 8]
                .try_into()
                .expect("timestamptz bytes"),
        ))),
        AbiLogicalType::UInt64 => Ok(Value::UBigInt(u64::from_le_bytes(
            buffer[offset..offset + 8].try_into().expect("u64 bytes"),
        ))),
        AbiLogicalType::Float32 => Ok(Value::Float(f32::from_le_bytes(
            buffer[offset..offset + 4].try_into().expect("f32 bytes"),
        ))),
        AbiLogicalType::Float64 => Ok(Value::Double(f64::from_le_bytes(
            buffer[offset..offset + 8].try_into().expect("f64 bytes"),
        ))),
        other => Err(paro_error::contract_violation(format!(
            "fixed-width decode is not implemented for {other:?}"
        ))),
    }
}

fn encode_fixed_width_value(
    buffer: &mut Vec<u8>,
    value: &Value,
    logical_type: &AbiLogicalType,
) -> Result<()> {
    match logical_type {
        AbiLogicalType::Boolean => buffer.push(if matches!(value, Value::Boolean(true)) {
            1
        } else {
            0
        }),
        AbiLogicalType::Int8 => buffer.push(match value {
            Value::TinyInt(v) => *v as u8,
            Value::Null(_) => 0,
            other => return Err(type_mismatch(other, logical_type)),
        }),
        AbiLogicalType::UInt8 => buffer.push(match value {
            Value::UTinyInt(v) => *v,
            Value::Null(_) => 0,
            other => return Err(type_mismatch(other, logical_type)),
        }),
        AbiLogicalType::Int16 => buffer.extend_from_slice(&match value {
            Value::SmallInt(v) => v.to_le_bytes(),
            Value::Null(_) => 0_i16.to_le_bytes(),
            other => return Err(type_mismatch(other, logical_type)),
        }),
        AbiLogicalType::UInt16 => buffer.extend_from_slice(&match value {
            Value::USmallInt(v) => v.to_le_bytes(),
            Value::Null(_) => 0_u16.to_le_bytes(),
            other => return Err(type_mismatch(other, logical_type)),
        }),
        AbiLogicalType::Int32 | AbiLogicalType::Date => buffer.extend_from_slice(&match value {
            Value::Integer(v) => v.to_le_bytes(),
            Value::Date(v) => v.to_le_bytes(),
            Value::Null(_) => 0_i32.to_le_bytes(),
            other => return Err(type_mismatch(other, logical_type)),
        }),
        AbiLogicalType::UInt32 => buffer.extend_from_slice(&match value {
            Value::UInteger(v) => v.to_le_bytes(),
            Value::Null(_) => 0_u32.to_le_bytes(),
            other => return Err(type_mismatch(other, logical_type)),
        }),
        AbiLogicalType::Int64
        | AbiLogicalType::Time
        | AbiLogicalType::Timestamp
        | AbiLogicalType::TimestampTz => {
            let bytes = match value {
                Value::BigInt(v) => v.to_le_bytes(),
                Value::Time(v) => v.to_le_bytes(),
                Value::Timestamp(v) => v.to_le_bytes(),
                Value::TimestampTz(v) => v.to_le_bytes(),
                Value::Null(_) => 0_i64.to_le_bytes(),
                other => return Err(type_mismatch(other, logical_type)),
            };
            buffer.extend_from_slice(&bytes);
        }
        AbiLogicalType::UInt64 => buffer.extend_from_slice(&match value {
            Value::UBigInt(v) => v.to_le_bytes(),
            Value::Null(_) => 0_u64.to_le_bytes(),
            other => return Err(type_mismatch(other, logical_type)),
        }),
        AbiLogicalType::Float32 => buffer.extend_from_slice(&match value {
            Value::Float(v) => v.to_le_bytes(),
            Value::Null(_) => 0_f32.to_le_bytes(),
            other => return Err(type_mismatch(other, logical_type)),
        }),
        AbiLogicalType::Float64 => buffer.extend_from_slice(&match value {
            Value::Double(v) => v.to_le_bytes(),
            Value::Null(_) => 0_f64.to_le_bytes(),
            other => return Err(type_mismatch(other, logical_type)),
        }),
        other => {
            return Err(paro_error::contract_violation(format!(
                "fixed-width encode is not implemented for {other:?}"
            )))
        }
    }
    Ok(())
}

fn logical_type_to_json(logical_type: &LogicalType) -> Result<JsonValue> {
    serde_json::to_value(logical_type_to_abi(logical_type).ok_or_else(|| {
        paro_error::contract_violation(format!("unsupported logical type '{logical_type}'"))
    })?)
    .map_err(json_error)
}

fn logical_type_to_abi(logical_type: &LogicalType) -> Option<AbiLogicalType> {
    match logical_type {
        LogicalType::Boolean => Some(AbiLogicalType::Boolean),
        LogicalType::TinyInt => Some(AbiLogicalType::Int8),
        LogicalType::UTinyInt => Some(AbiLogicalType::UInt8),
        LogicalType::SmallInt => Some(AbiLogicalType::Int16),
        LogicalType::USmallInt => Some(AbiLogicalType::UInt16),
        LogicalType::Integer => Some(AbiLogicalType::Int32),
        LogicalType::UInteger => Some(AbiLogicalType::UInt32),
        LogicalType::BigInt => Some(AbiLogicalType::Int64),
        LogicalType::UBigInt => Some(AbiLogicalType::UInt64),
        LogicalType::Float => Some(AbiLogicalType::Float32),
        LogicalType::Double => Some(AbiLogicalType::Float64),
        LogicalType::Date => Some(AbiLogicalType::Date),
        LogicalType::Time => Some(AbiLogicalType::Time),
        LogicalType::Timestamp => Some(AbiLogicalType::Timestamp),
        LogicalType::TimestampTz => Some(AbiLogicalType::TimestampTz),
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery => Some(AbiLogicalType::Varchar),
        LogicalType::Blob => Some(AbiLogicalType::Blob),
        LogicalType::Json => Some(AbiLogicalType::Json),
        LogicalType::Jsonb => Some(AbiLogicalType::Jsonb),
        _ => None,
    }
}

fn scalar_value_to_runtime(value: &paro_external_abi::ScalarValueRef) -> Result<Value> {
    match value {
        paro_external_abi::ScalarValueRef::Null => Ok(Value::Null(LogicalType::Unknown)),
        paro_external_abi::ScalarValueRef::Boolean(v) => Ok(Value::Boolean(*v)),
        paro_external_abi::ScalarValueRef::Int32(v) => Ok(Value::Integer(*v)),
        paro_external_abi::ScalarValueRef::Int64(v) => Ok(Value::BigInt(*v)),
        paro_external_abi::ScalarValueRef::UInt32(v) => Ok(Value::UInteger(*v)),
        paro_external_abi::ScalarValueRef::UInt64(v) => Ok(Value::UBigInt(*v)),
        paro_external_abi::ScalarValueRef::Utf8(v) => Ok(Value::Varchar(v.clone())),
        paro_external_abi::ScalarValueRef::Binary(v) => Ok(Value::Blob(v.clone())),
        other => Err(paro_error::contract_violation(format!(
            "constant output is not implemented for {other:?}"
        ))),
    }
}

fn type_mismatch(value: &Value, logical_type: &AbiLogicalType) -> paro_common::error::ParoError {
    paro_error::contract_violation(format!(
        "value {value:?} does not match worker ABI logical type {logical_type:?}"
    ))
}

fn read_u32(buffer: &[u8], offset: usize) -> Result<u32> {
    let bytes = buffer
        .get(offset..offset + 4)
        .ok_or_else(|| paro_error::external_protocol_mismatch("offset buffer is truncated"))?;
    Ok(u32::from_le_bytes(bytes.try_into().expect("u32 bytes")))
}

fn pack_validity_bitmap(null_mask: &[bool]) -> Vec<u8> {
    let mut bitmap = vec![0_u8; (null_mask.len() + 7) / 8];
    for (index, is_null) in null_mask.iter().enumerate() {
        if !is_null {
            bitmap[index / 8] |= 1 << (index % 8);
        }
    }
    bitmap
}

fn unpack_validity_bitmap(buffer: &[u8], length: usize) -> Vec<bool> {
    (0..length)
        .map(|index| (buffer[index / 8] & (1 << (index % 8))) == 0)
        .collect()
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

fn decode_hex(hex: &str) -> Result<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return Err(paro_error::external_protocol_mismatch(
            "hex payload has an odd length",
        ));
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let chars = hex.as_bytes();
    let mut index = 0;
    while index < chars.len() {
        let hi = decode_hex_nibble(chars[index])?;
        let lo = decode_hex_nibble(chars[index + 1])?;
        bytes.push((hi << 4) | lo);
        index += 2;
    }
    Ok(bytes)
}

fn decode_hex_nibble(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(paro_error::external_protocol_mismatch(format!(
            "invalid hex digit '{}'",
            byte as char
        ))),
    }
}

fn sanitize_path_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn merge_warm_state(left: RuntimeWarmState, right: RuntimeWarmState) -> RuntimeWarmState {
    if matches!(left, RuntimeWarmState::Cold) || matches!(right, RuntimeWarmState::Cold) {
        RuntimeWarmState::Cold
    } else {
        RuntimeWarmState::Warm
    }
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("execution crate parent")
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

fn runtime_contract() -> RuntimeContract {
    RuntimeContract {
        sdk_version: env!("CARGO_PKG_VERSION").to_string(),
        worker_protocol_version: 1,
        abi_version: 1,
        supported_transports: vec![TransportKind::LocalShm],
    }
}

fn io_error(error: std::io::Error) -> paro_common::error::ParoError {
    paro_error::artifact_not_ready(error.to_string())
}

fn json_error(error: serde_json::Error) -> paro_common::error::ParoError {
    paro_error::external_protocol_mismatch(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{build_project_runtime_bridge, build_table_runtime_bridge};
    use crate::execution_context::ExecutionContext;
    use crate::operator::external::batching::SubmissionBatchPolicy;
    use crate::operator::external::runtime_bridge::{
        ExternalRoutineDescriptor, ExternalRuntimeBridge, ProjectSubmission, RuntimeBridgeOutcome,
        TableSubmission,
    };
    use crate::thread_context::ThreadContext;
    use paro_common::chunk::Chunk;
    use paro_common::error::codes;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_external_runtime::host::ExternalRuntimeHost;
    use paro_function::scalar::ScalarFunction;
    use paro_planner::expression::{Expression, FunctionExpression, ReferenceExpression};
    use paro_planner::operator::external_project::ExternalProjectExpression;
    use paro_routine::{
        BoundRoutineCallMeta, CapabilityProfile, CapabilityProfilePreset, DeclaredEnvSpec,
        ExecutionBoundary, ImportRef, PermissionSpec, PlacementClass, PythonEntrypointRef,
        PythonImplementationRef, PythonRuntimeSelector, RoutineArgument, RoutineCallIdentity,
        RoutineExecutionContract, RoutineFamily, RoutineIdentity, RoutineImplementationRef,
        RoutineNullPolicy, RoutineOwner, RoutineReturn, RoutineSecurityMode, RoutineSemantics,
        RoutineSideEffects, RoutineSpec, RoutineStability, RoutineTableColumn, RowSemantics,
        ScalarRoutineContract, SourceBlobRef, TableRoutineContract,
    };
    use std::fs;
    use std::sync::Arc;

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal()
            .with_python_runtime(Arc::new(ExternalRuntimeHost::ready_stub()))
            .build()
    }

    fn test_ctx() -> ExecutionContext<'static> {
        let session = test_session();
        let thread = Box::leak(Box::new(ThreadContext::single_threaded()));
        ExecutionContext::new(session, thread, None)
    }

    fn scalar_spec(name: &str, inline_source: &str, imports: Vec<ImportRef>) -> RoutineSpec {
        scalar_spec_with_profile(
            name,
            inline_source,
            imports,
            CapabilityProfile::process_default(),
        )
    }

    fn scalar_spec_with_profile(
        name: &str,
        inline_source: &str,
        imports: Vec<ImportRef>,
        capability_profile: CapabilityProfile,
    ) -> RoutineSpec {
        RoutineSpec {
            identity: RoutineIdentity {
                id: paro_routine::RoutineId::from_raw(77),
                generation: 3,
            },
            name: name.to_string(),
            schema: "public".to_string(),
            owner: RoutineOwner {
                principal: "paro".to_string(),
            },
            arguments: vec![RoutineArgument {
                name: Some("a".to_string()),
                data_type: LogicalType::Integer,
            }],
            family: RoutineFamily::ScalarBatch,
            return_type: RoutineReturn::Scalar(LogicalType::Integer),
            execution_contract: RoutineExecutionContract::Scalar(ScalarRoutineContract),
            semantics: RoutineSemantics {
                stability: RoutineStability::Immutable,
                null_policy: RoutineNullPolicy::CalledOnNullInput,
                side_effects: RoutineSideEffects::None,
                row_semantics: RowSemantics::RowPreserving,
                may_block: false,
            },
            implementation: RoutineImplementationRef::Python(PythonImplementationRef {
                source_blob: SourceBlobRef {
                    id: format!("blob:{name}"),
                    inline_source: inline_source.to_string(),
                },
                entrypoint: PythonEntrypointRef::Batch {
                    handler: "batch".to_string(),
                },
                runtime: PythonRuntimeSelector::SystemDefault,
            }),
            environment: DeclaredEnvSpec {
                runtime: PythonRuntimeSelector::SystemDefault,
                packages: Vec::new(),
                imports,
            },
            permissions: PermissionSpec {
                security_mode: RoutineSecurityMode::Invoker,
                capability_profile,
            },
        }
    }

    fn trusted_subinterpreter_profile() -> CapabilityProfile {
        CapabilityProfile::from_preset(CapabilityProfilePreset::TrustedSubInterpreter)
    }

    fn compiled_kernel_profile() -> CapabilityProfile {
        CapabilityProfile::from_preset(CapabilityProfilePreset::CompiledKernel)
    }

    fn compiled_jit_profile() -> CapabilityProfile {
        CapabilityProfile::from_preset(CapabilityProfilePreset::CompiledJit)
    }

    fn table_spec(name: &str, inline_source: &str) -> RoutineSpec {
        RoutineSpec {
            identity: RoutineIdentity {
                id: paro_routine::RoutineId::from_raw(88),
                generation: 5,
            },
            name: name.to_string(),
            schema: "public".to_string(),
            owner: RoutineOwner {
                principal: "paro".to_string(),
            },
            arguments: vec![RoutineArgument {
                name: Some("a".to_string()),
                data_type: LogicalType::Integer,
            }],
            family: RoutineFamily::TableBatch,
            return_type: RoutineReturn::Table(vec![RoutineTableColumn {
                name: "value".to_string(),
                data_type: LogicalType::Integer,
            }]),
            execution_contract: RoutineExecutionContract::Table(TableRoutineContract {
                rows_hint: Some(4),
            }),
            semantics: RoutineSemantics {
                stability: RoutineStability::Stable,
                null_policy: RoutineNullPolicy::CalledOnNullInput,
                side_effects: RoutineSideEffects::None,
                row_semantics: RowSemantics::RelationExpanding,
                may_block: false,
            },
            implementation: RoutineImplementationRef::Python(PythonImplementationRef {
                source_blob: SourceBlobRef {
                    id: format!("blob:{name}"),
                    inline_source: inline_source.to_string(),
                },
                entrypoint: PythonEntrypointRef::Batch {
                    handler: "batch".to_string(),
                },
                runtime: PythonRuntimeSelector::SystemDefault,
            }),
            environment: DeclaredEnvSpec::empty(PythonRuntimeSelector::SystemDefault),
            permissions: PermissionSpec {
                security_mode: RoutineSecurityMode::Invoker,
                capability_profile: CapabilityProfile::process_default(),
            },
        }
    }

    fn scalar_expression(spec: RoutineSpec) -> ExternalProjectExpression {
        fn placeholder(
            _input: &Chunk,
            _ctx: &dyn paro_function::scalar::FunctionExecContext,
            _result: &mut paro_common::vector::Vector,
        ) -> paro_common::error::Result<()> {
            Err(paro_common::error::internal(
                "placeholder scalar should never execute",
            ))
        }

        let routine_meta = BoundRoutineCallMeta {
            identity: RoutineCallIdentity::Catalog {
                routine_id: spec.identity.id,
                generation: spec.identity.generation,
            },
            semantics: spec.semantics.clone(),
            boundary: ExecutionBoundary {
                placement: PlacementClass::External,
                may_block: false,
                row_semantics: RowSemantics::RowPreserving,
            },
            spec: Some(spec.clone()),
        };
        ExternalProjectExpression {
            output_name: "value".to_string(),
            expression: Expression::Function(
                FunctionExpression::new(
                    ScalarFunction::new(
                        spec.name.clone(),
                        vec![LogicalType::Integer],
                        LogicalType::Integer,
                        placeholder,
                    ),
                    vec![Expression::Reference(ReferenceExpression::new(
                        0,
                        LogicalType::Integer,
                    ))],
                    LogicalType::Integer,
                )
                .with_routine_meta(routine_meta.clone()),
            ),
            routine_meta,
        }
    }

    fn table_meta(spec: RoutineSpec) -> (RoutineSpec, BoundRoutineCallMeta) {
        let routine_meta = BoundRoutineCallMeta {
            identity: RoutineCallIdentity::Catalog {
                routine_id: spec.identity.id,
                generation: spec.identity.generation,
            },
            semantics: spec.semantics.clone(),
            boundary: ExecutionBoundary {
                placement: PlacementClass::External,
                may_block: false,
                row_semantics: RowSemantics::RelationExpanding,
            },
            spec: Some(spec.clone()),
        };
        (spec, routine_meta)
    }

    fn integer_input(values: &[i32]) -> Chunk {
        Chunk::from_vectors(vec![Vector::from_i32(values)])
    }

    fn batch_policy(bridge: &ExternalRuntimeBridge) -> SubmissionBatchPolicy {
        SubmissionBatchPolicy::from_dispatch_policy(bridge.dispatch_policy())
    }

    #[test]
    fn project_bridge_executes_python_worker_from_body_snippet() {
        let ctx = test_ctx();
        let expression = scalar_expression(scalar_spec(
            "py_add_one",
            "return [value + 1 for value in a.materialize_py()]",
            Vec::new(),
        ));
        let descriptor = ExternalRoutineDescriptor {
            label: "py_add_one".to_string(),
            identity: expression.routine_meta.identity.clone(),
            semantics: expression.routine_meta.semantics.clone(),
        };
        let bridge = build_project_runtime_bridge(
            &ctx.session,
            std::slice::from_ref(&descriptor),
            std::slice::from_ref(&expression),
        )
        .expect("build bridge");
        let batch_policy = batch_policy(&bridge);

        let input = integer_input(&[1, 2, 3]);
        let submission = ProjectSubmission {
            batch_id: 1,
            input: &input,
            expressions: std::slice::from_ref(&expression),
            routines: std::slice::from_ref(&descriptor),
            force_tail_flush: false,
            batch_policy: &batch_policy,
        };
        let RuntimeBridgeOutcome::Ready(response) = bridge
            .execute_project(&ctx, &submission)
            .expect("execute project")
        else {
            panic!("project bridge must not block in unit tests");
        };
        let output = response.output_batches.first().expect("output batch");
        assert_eq!(output.size(), 3);
        assert_eq!(output.column(0).unwrap().get_i32(0), Some(2));
        assert_eq!(output.column(0).unwrap().get_i32(1), Some(3));
        assert_eq!(output.column(0).unwrap().get_i32(2), Some(4));
        assert_eq!(bridge.explain().language, "python");
        assert!(bridge
            .explain()
            .artifact_validation_state
            .contains("ready("));
    }

    #[test]
    fn project_bridge_uses_trusted_subinterpreter_backend_when_profile_requests_it() {
        let ctx = test_ctx();
        let subinterpreter_supported = super::python_subinterpreter_supported();
        let expression = scalar_expression(scalar_spec_with_profile(
            "py_subinterp_double",
            r#"
from paro_udf import batch_udf

@batch_udf(return_type="int32")
def batch(ctx, a):
    return [value * 2 for value in a.materialize_py()]
"#,
            Vec::new(),
            trusted_subinterpreter_profile(),
        ));
        let descriptor = ExternalRoutineDescriptor {
            label: "py_subinterp_double".to_string(),
            identity: expression.routine_meta.identity.clone(),
            semantics: expression.routine_meta.semantics.clone(),
        };
        let bridge = build_project_runtime_bridge(
            &ctx.session,
            std::slice::from_ref(&descriptor),
            std::slice::from_ref(&expression),
        )
        .expect("build bridge");
        if subinterpreter_supported {
            assert!(bridge.explain().backend.contains("subinterpreter"));
        } else {
            assert!(bridge.explain().backend.contains("process"));
        }

        let batch_policy = batch_policy(&bridge);
        let input = integer_input(&[3, 4]);
        let submission = ProjectSubmission {
            batch_id: 9,
            input: &input,
            expressions: std::slice::from_ref(&expression),
            routines: std::slice::from_ref(&descriptor),
            force_tail_flush: false,
            batch_policy: &batch_policy,
        };
        let RuntimeBridgeOutcome::Ready(response) = bridge
            .execute_project(&ctx, &submission)
            .expect("execute project")
        else {
            panic!("project bridge must not block");
        };
        let output = response.output_batches.first().expect("output");
        assert_eq!(output.column(0).unwrap().get_i32(0), Some(6));
        assert_eq!(output.column(0).unwrap().get_i32(1), Some(8));
    }

    #[test]
    fn project_bridge_uses_compiled_kernel_candidate_when_profile_requests_it() {
        let ctx = test_ctx();
        let expression = scalar_expression(scalar_spec_with_profile(
            "py_compiled_score",
            r#"
from paro_udf import batch_udf, register_compiled_kernel

def compiled_batch(ctx, a):
    return [value + 100 for value in a.materialize_py()]

@register_compiled_kernel(kind="numba", entrypoint="compiled_batch")
@batch_udf(return_type="int32")
def batch(ctx, a):
    return [value + 1 for value in a.materialize_py()]
"#,
            Vec::new(),
            compiled_kernel_profile(),
        ));
        let descriptor = ExternalRoutineDescriptor {
            label: "py_compiled_score".to_string(),
            identity: expression.routine_meta.identity.clone(),
            semantics: expression.routine_meta.semantics.clone(),
        };
        let bridge = build_project_runtime_bridge(
            &ctx.session,
            std::slice::from_ref(&descriptor),
            std::slice::from_ref(&expression),
        )
        .expect("build bridge");
        assert!(bridge.explain().backend.contains("compiled_kernel"));

        let batch_policy = batch_policy(&bridge);
        let input = integer_input(&[1, 2]);
        let submission = ProjectSubmission {
            batch_id: 10,
            input: &input,
            expressions: std::slice::from_ref(&expression),
            routines: std::slice::from_ref(&descriptor),
            force_tail_flush: false,
            batch_policy: &batch_policy,
        };
        let RuntimeBridgeOutcome::Ready(response) = bridge
            .execute_project(&ctx, &submission)
            .expect("execute project")
        else {
            panic!("project bridge must not block");
        };
        let output = response.output_batches.first().expect("output");
        assert_eq!(output.column(0).unwrap().get_i32(0), Some(101));
        assert_eq!(output.column(0).unwrap().get_i32(1), Some(102));
    }

    #[test]
    fn project_bridge_uses_native_jit_candidate_without_native_extension_permission() {
        let ctx = test_ctx();
        let expression = scalar_expression(scalar_spec_with_profile(
            "py_compiled_jit_score",
            r#"
from paro_udf import batch_udf, register_native_jit_kernel

def compiled_batch(ctx, a):
    return [value + 200 for value in a.materialize_py()]

@register_native_jit_kernel(entrypoint="compiled_batch")
@batch_udf(return_type="int32")
def batch(ctx, a):
    return [value + 1 for value in a.materialize_py()]
"#,
            Vec::new(),
            compiled_jit_profile(),
        ));
        let descriptor = ExternalRoutineDescriptor {
            label: "py_compiled_jit_score".to_string(),
            identity: expression.routine_meta.identity.clone(),
            semantics: expression.routine_meta.semantics.clone(),
        };
        let bridge = build_project_runtime_bridge(
            &ctx.session,
            std::slice::from_ref(&descriptor),
            std::slice::from_ref(&expression),
        )
        .expect("build bridge");
        assert!(bridge.explain().backend.contains("compiled_kernel[jit]"));

        let batch_policy = batch_policy(&bridge);
        let input = integer_input(&[1, 2]);
        let submission = ProjectSubmission {
            batch_id: 11,
            input: &input,
            expressions: std::slice::from_ref(&expression),
            routines: std::slice::from_ref(&descriptor),
            force_tail_flush: false,
            batch_policy: &batch_policy,
        };
        let RuntimeBridgeOutcome::Ready(response) = bridge
            .execute_project(&ctx, &submission)
            .expect("execute project")
        else {
            panic!("project bridge must not block");
        };
        let output = response.output_batches.first().expect("output");
        assert_eq!(output.column(0).unwrap().get_i32(0), Some(201));
        assert_eq!(output.column(0).unwrap().get_i32(1), Some(202));
    }

    #[test]
    fn project_bridge_materializes_imports() {
        let helper_dir = std::env::temp_dir().join("paro-python-udf-tests");
        fs::create_dir_all(&helper_dir).expect("create helper dir");
        let helper = helper_dir.join("basic_math.py");
        fs::write(
            &helper,
            "def shift(values):\n    return [value + 10 for value in values]\n",
        )
        .expect("write helper");

        let ctx = test_ctx();
        let expression = scalar_expression(scalar_spec(
            "py_arrow_identity",
            "from basic_math import shift\nvalues = shift(a.materialize_py())\nreturn values",
            vec![ImportRef {
                uri: helper.to_string_lossy().to_string(),
                expected_digest: None,
                expected_size: None,
            }],
        ));
        let descriptor = ExternalRoutineDescriptor {
            label: "py_arrow_identity".to_string(),
            identity: expression.routine_meta.identity.clone(),
            semantics: expression.routine_meta.semantics.clone(),
        };
        let bridge = build_project_runtime_bridge(
            &ctx.session,
            std::slice::from_ref(&descriptor),
            std::slice::from_ref(&expression),
        )
        .expect("build bridge");
        let batch_policy = batch_policy(&bridge);
        let input = integer_input(&[5, 6]);
        let submission = ProjectSubmission {
            batch_id: 2,
            input: &input,
            expressions: std::slice::from_ref(&expression),
            routines: std::slice::from_ref(&descriptor),
            force_tail_flush: false,
            batch_policy: &batch_policy,
        };
        let RuntimeBridgeOutcome::Ready(response) = bridge
            .execute_project(&ctx, &submission)
            .expect("execute project")
        else {
            panic!("project bridge must not block");
        };
        let output = response.output_batches.first().expect("output");
        assert_eq!(output.column(0).unwrap().get_i32(0), Some(15));
        assert_eq!(output.column(0).unwrap().get_i32(1), Some(16));
    }

    #[test]
    fn project_bridge_uses_fixed_width_numpy_fast_path_when_numpy_is_imported() {
        let helper_dir = std::env::temp_dir().join("paro-python-udf-tests-fastpath");
        fs::create_dir_all(&helper_dir).expect("create helper dir");
        let helper = helper_dir.join("numpy.py");
        fs::write(
            &helper,
            r#"
import struct

class FakeArray(list):
    def __add__(self, scalar):
        return FakeArray([item + scalar for item in self])

def frombuffer(buffer, *, dtype, count):
    if dtype != "int32":
        raise AssertionError(f"unexpected dtype {dtype}")
    raw = memoryview(buffer).tobytes()
    return FakeArray([
        struct.unpack_from("<i", raw, index * 4)[0]
        for index in range(count)
    ])

def array(*_args, **_kwargs):
    raise AssertionError("fixed-width fast path unexpectedly fell back to numpy.array()")

def asarray(*_args, **_kwargs):
    raise AssertionError("fixed-width fast path unexpectedly fell back to numpy.asarray()")
"#,
        )
        .expect("write fake numpy module");

        let ctx = test_ctx();
        let expression = scalar_expression(scalar_spec(
            "py_numpy_fast_path",
            "import numpy\nreturn a.to_numpy() + 7",
            vec![ImportRef {
                uri: helper.to_string_lossy().to_string(),
                expected_digest: None,
                expected_size: None,
            }],
        ));
        let descriptor = ExternalRoutineDescriptor {
            label: "py_numpy_fast_path".to_string(),
            identity: expression.routine_meta.identity.clone(),
            semantics: expression.routine_meta.semantics.clone(),
        };
        let bridge = build_project_runtime_bridge(
            &ctx.session,
            std::slice::from_ref(&descriptor),
            std::slice::from_ref(&expression),
        )
        .expect("build bridge");
        let batch_policy = batch_policy(&bridge);
        let input = integer_input(&[4, 9]);
        let submission = ProjectSubmission {
            batch_id: 5,
            input: &input,
            expressions: std::slice::from_ref(&expression),
            routines: std::slice::from_ref(&descriptor),
            force_tail_flush: false,
            batch_policy: &batch_policy,
        };

        let RuntimeBridgeOutcome::Ready(response) = bridge
            .execute_project(&ctx, &submission)
            .expect("execute project")
        else {
            panic!("project bridge must not block");
        };
        let output = response.output_batches.first().expect("output");
        assert_eq!(output.column(0).unwrap().get_i32(0), Some(11));
        assert_eq!(output.column(0).unwrap().get_i32(1), Some(16));
    }

    #[test]
    fn project_bridge_accepts_arrow_capsule_results() {
        let ctx = test_ctx();
        let expression = scalar_expression(scalar_spec(
            "py_arrow_capsule",
            "return a.__arrow_c_array__()",
            Vec::new(),
        ));
        let descriptor = ExternalRoutineDescriptor {
            label: "py_arrow_capsule".to_string(),
            identity: expression.routine_meta.identity.clone(),
            semantics: expression.routine_meta.semantics.clone(),
        };
        let bridge = build_project_runtime_bridge(
            &ctx.session,
            std::slice::from_ref(&descriptor),
            std::slice::from_ref(&expression),
        )
        .expect("build bridge");
        let batch_policy = batch_policy(&bridge);
        let input = integer_input(&[8, 13]);
        let submission = ProjectSubmission {
            batch_id: 6,
            input: &input,
            expressions: std::slice::from_ref(&expression),
            routines: std::slice::from_ref(&descriptor),
            force_tail_flush: false,
            batch_policy: &batch_policy,
        };

        let RuntimeBridgeOutcome::Ready(response) = bridge
            .execute_project(&ctx, &submission)
            .expect("execute project")
        else {
            panic!("project bridge must not block");
        };
        let output = response.output_batches.first().expect("output");
        assert_eq!(output.column(0).unwrap().get_i32(0), Some(8));
        assert_eq!(output.column(0).unwrap().get_i32(1), Some(13));
    }

    #[test]
    fn project_bridge_surfaces_python_exceptions() {
        let ctx = test_ctx();
        let expression = scalar_expression(scalar_spec(
            "py_fail",
            "raise ValueError('boom from worker')",
            Vec::new(),
        ));
        let descriptor = ExternalRoutineDescriptor {
            label: "py_fail".to_string(),
            identity: expression.routine_meta.identity.clone(),
            semantics: expression.routine_meta.semantics.clone(),
        };
        let bridge = build_project_runtime_bridge(
            &ctx.session,
            std::slice::from_ref(&descriptor),
            std::slice::from_ref(&expression),
        )
        .expect("build bridge");
        let batch_policy = batch_policy(&bridge);
        let input = integer_input(&[1]);
        let submission = ProjectSubmission {
            batch_id: 3,
            input: &input,
            expressions: std::slice::from_ref(&expression),
            routines: std::slice::from_ref(&descriptor),
            force_tail_flush: false,
            batch_policy: &batch_policy,
        };
        let error = bridge
            .execute_project(&ctx, &submission)
            .expect_err("worker exception should surface");
        assert!(error.is(codes::external_routine::PYTHON_EXCEPTION));
        assert!(error.to_string().contains("boom from worker"));
    }

    #[test]
    fn table_bridge_executes_python_worker() {
        let ctx = test_ctx();
        let (_spec, meta) = table_meta(table_spec(
            "py_expand",
            "return [value * 3 for value in a.materialize_py()]",
        ));
        let descriptor = ExternalRoutineDescriptor {
            label: "py_expand".to_string(),
            identity: meta.identity.clone(),
            semantics: meta.semantics.clone(),
        };
        let bridge =
            build_table_runtime_bridge(&ctx.session, &meta, &descriptor, &[LogicalType::Integer])
                .expect("build table bridge");
        let input = integer_input(&[2, 4]);
        let submission = TableSubmission {
            batch_id: 4,
            input: &input,
            routine: &descriptor,
            output_types: &[LogicalType::Integer],
            lateral: false,
            parameterized: false,
        };
        let RuntimeBridgeOutcome::Ready(response) = bridge
            .execute_table(&ctx, &submission)
            .expect("execute table")
        else {
            panic!("table bridge must not block");
        };
        let output = response.output_batches.first().expect("output chunk");
        assert_eq!(output.size(), 2);
        assert_eq!(output.column(0).unwrap().get_i32(0), Some(6));
        assert_eq!(output.column(0).unwrap().get_i32(1), Some(12));
    }

    #[test]
    fn table_bridge_supports_relation_expanding_output() {
        let ctx = test_ctx();
        let (_spec, meta) = table_meta(table_spec(
            "py_expand_rows",
            "output = []\nfor value in a.materialize_py():\n    output.extend((value, value + 100))\nreturn output",
        ));
        let descriptor = ExternalRoutineDescriptor {
            label: "py_expand_rows".to_string(),
            identity: meta.identity.clone(),
            semantics: meta.semantics.clone(),
        };
        let bridge =
            build_table_runtime_bridge(&ctx.session, &meta, &descriptor, &[LogicalType::Integer])
                .expect("build table bridge");
        let input = integer_input(&[1, 2]);
        let submission = TableSubmission {
            batch_id: 7,
            input: &input,
            routine: &descriptor,
            output_types: &[LogicalType::Integer],
            lateral: false,
            parameterized: false,
        };
        let RuntimeBridgeOutcome::Ready(response) = bridge
            .execute_table(&ctx, &submission)
            .expect("execute table")
        else {
            panic!("table bridge must not block");
        };
        let output = response.output_batches.first().expect("output chunk");
        assert_eq!(output.size(), 4);
        assert_eq!(output.column(0).unwrap().get_i32(0), Some(1));
        assert_eq!(output.column(0).unwrap().get_i32(1), Some(101));
        assert_eq!(output.column(0).unwrap().get_i32(2), Some(2));
        assert_eq!(output.column(0).unwrap().get_i32(3), Some(102));
    }
}
