// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use parking_lot::Mutex;
use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{AccountedBytesMut, AccountedVec, MemoryAccountingClass};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_context::StatementContext;
use paro_external::abi::descriptor::ColumnDescriptor;
use paro_external::abi::encoding::{ColumnEncoding, ColumnPopulationMode};
use paro_external::abi::layout::{BufferLease, ColumnLayout, OffsetWidth};
use paro_external::abi::lease::{ColumnBatchLease, LeaseOwnership, LeaseState};
use paro_external::abi::types::AbiLogicalType;
use paro_external::routine::artifact::{
    ArtifactCapabilities, ArtifactValidationState, RuntimeContract, TransportKind,
};
use paro_external::routine::bound::BoundRoutineCallMeta;
use paro_external::routine::env::DeclaredEnvSpec;
use paro_external::routine::spec::{
    PythonEntrypointRef, PythonImplementationRef, RoutineImplementationRef, RoutineReturn,
    RoutineSpec,
};
use paro_external::runtime::artifact::resolve::{ArtifactResolver, ResolveInputs};
use paro_external::runtime::artifact::validate::ArtifactValidator;
use paro_external::runtime::backend::selector::{
    BackendAvailability, BackendKind, BackendSelection, BackendSelector,
};
use paro_external::runtime::control::header::CONTROL_HEADER_SIZE;
use paro_external::runtime::control::header::{ControlHeader, ControlMessageKind};
use paro_external::runtime::dispatch::policy::ExternalDispatchPolicy;
use paro_external::runtime::protocol::messages::PythonExceptionPayload;
use paro_planner::expression::Expression;
use paro_planner::operator::external_project::ExternalProjectExpression;
use serde_json::{json, Value as JsonValue};

use crate::execution_context::ExecutionContext;
use crate::expression_executor::executor::ExpressionExecutor;
use crate::memory_runtime::{OperatorMemoryScope, RetainedMemoryHandle};
use crate::operator::external::batching::SubmissionBatchPolicy;
use crate::operator::external::runtime_bridge::{
    ExternalRoutineDescriptor, ExternalRuntimeBridge, ProjectBridgeKernel, ProjectSubmission,
    RuntimeBridgeExplainInfo, RuntimeBridgeMetrics, RuntimeBridgeOutcome, RuntimeBridgeResponse,
    RuntimeWarmState, TableBridgeKernel, TableSubmission,
};

const DEFAULT_PYTHON_BIN: &str = "python3";
const PYTHON_BIN_ENV: &str = "PARO_PYTHON_BIN";
const PYTHON_BRIDGE_TAG: MemoryTag = MemoryTag::ExternalRuntimeHost;
const PYTHON_BRIDGE_CLASS: MemoryAccountingClass = MemoryAccountingClass::NonRevocable;
const PYTHON_WORKER_FRAME_MAGIC: &[u8; 8] = b"PAROFRM1";

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
    subinterpreter_policy: Option<paro_external::routine::capability::SubInterpreterPolicy>,
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
            paro_external::routine::artifact::BackendSelectionInput {
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
    profile: &paro_external::routine::capability::CapabilityProfile,
    capabilities: &ArtifactCapabilities,
) -> Option<String> {
    if profile.trusted_backend_preference()
        != paro_external::routine::artifact::TrustedBackendPreference::CompiledKernel
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
                paro_external::routine::spec::RowSemantics::RowPreserving
            ) && spec.semantics.side_effects
                == paro_external::routine::spec::RoutineSideEffects::None,
            supports_restricted_wasm_backend: spec
                .permissions
                .capability_profile
                .native_extension_policy
                == paro_external::routine::capability::CapabilityPolicy::Deny
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
                != paro_external::routine::capability::CapabilityPolicy::Deny
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
        memory: &OperatorMemoryScope<'_>,
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
                memory,
            )?;
            let input_bytes = SubmissionBatchPolicy::estimate_chunk_bytes(&input_chunk);
            let result = self.worker.invoke(
                ctx.session.clone(),
                memory,
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
            output_batches: vec![Chunk::from_vectors(
                generated_columns,
                memory.accounted_allocator_for(PYTHON_BRIDGE_TAG, PYTHON_BRIDGE_CLASS),
            )],
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
                        identity: paro_external::routine::identity::RoutineCallIdentity::Catalog {
                            routine_id: paro_external::routine::spec::RoutineId::from_raw(0),
                            generation: 0,
                        },
                        semantics: paro_external::routine::spec::RoutineSemantics {
                            stability: paro_external::routine::spec::RoutineStability::Volatile,
                            null_policy: paro_external::routine::spec::RoutineNullPolicy::CalledOnNullInput,
                            side_effects: paro_external::routine::spec::RoutineSideEffects::HasSideEffects,
                            row_semantics: paro_external::routine::spec::RowSemantics::RelationExpanding,
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
                                paro_external::runtime::backend::selector::IsolationLevel::Process,
                            transport: TransportKind::LocalShm,
                            sandbox_runtime: None,
                            input: paro_external::routine::artifact::BackendSelectionInput {
                                capability_profile:
                                    paro_external::routine::capability::CapabilityProfile::process_default(),
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
                                minimum_isolation: paro_external::routine::artifact::MinimumIsolation::Process,
                                trusted_backend_preference:
                                    paro_external::routine::artifact::TrustedBackendPreference::Automatic,
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
        memory: &OperatorMemoryScope<'_>,
    ) -> Result<RuntimeBridgeOutcome> {
        if self.prepared.output_types.is_empty() {
            return Err(paro_error::internal(
                "python table kernel was not bound to a prepared routine".to_string(),
            ));
        }
        let input_bytes = SubmissionBatchPolicy::estimate_chunk_bytes(submission.input);
        let result = self.worker.invoke(
            ctx.session.clone(),
            memory,
            &self.prepared.base,
            submission.batch_id,
            submission.input,
            if matches!(
                self.prepared.base.descriptor.semantics.row_semantics,
                paro_external::routine::spec::RowSemantics::RowPreserving
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
    memory: &OperatorMemoryScope<'_>,
) -> Result<Chunk> {
    let allocator = memory.accounted_allocator_for(PYTHON_BRIDGE_TAG, PYTHON_BRIDGE_CLASS);
    if expressions.is_empty() {
        return Chunk::try_new(allocator);
    }
    let mut executor = ExpressionExecutor::with_expressions(expressions);
    let mut chunk = Chunk::try_new(allocator)?;
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
        memory: &OperatorMemoryScope<'_>,
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
            memory,
        )?;
        let response = match process.exchange(&request, memory) {
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

        let chunk = match decode_response_chunk(
            &response.payload,
            response.buffers.as_slice(),
            output_types,
            memory,
        ) {
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

    fn exchange(
        &mut self,
        request: &WorkerRequest,
        memory: &OperatorMemoryScope<'_>,
    ) -> Result<WorkerResponse> {
        self.write_frame(request, memory)?;
        let response = self.read_frame(memory)?;

        match response
            .header
            .kind()
            .map_err(|error| paro_error::external_protocol_mismatch(error.to_string()))?
        {
            ControlMessageKind::Complete => Ok(response),
            ControlMessageKind::Error => {
                let error = serde_json::from_value::<PythonExceptionPayload>(response.payload)
                    .map_err(|error| {
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

    fn write_frame(
        &mut self,
        request: &WorkerRequest,
        memory: &OperatorMemoryScope<'_>,
    ) -> Result<()> {
        let payload = serde_json::to_vec(&request.payload).map_err(|error| {
            paro_error::worker_failure(format!("failed to encode worker request: {error}"))
        })?;
        let payload_len = u32::try_from(payload.len()).map_err(|_| {
            paro_error::worker_failure("worker request payload exceeds u32 frame length")
        })?;
        let buffer_count = u32::try_from(request.buffers.len()).map_err(|_| {
            paro_error::worker_failure("worker request has too many buffers for binary frame")
        })?;
        let _payload_memory = retain_bridge_bytes(memory, payload.capacity())?;
        self.stdin
            .write_all(PYTHON_WORKER_FRAME_MAGIC)
            .and_then(|_| self.stdin.write_all(&request.header))
            .and_then(|_| self.stdin.write_all(&payload_len.to_le_bytes()))
            .and_then(|_| self.stdin.write_all(&buffer_count.to_le_bytes()))
            .map_err(write_worker_frame_error)?;
        for buffer in request.buffers.iter() {
            let len = u64::try_from(buffer.len()).map_err(|_| {
                paro_error::worker_failure("worker request buffer exceeds u64 frame length")
            })?;
            self.stdin
                .write_all(&len.to_le_bytes())
                .map_err(write_worker_frame_error)?;
        }
        self.stdin
            .write_all(&payload)
            .map_err(write_worker_frame_error)?;
        for buffer in request.buffers.iter() {
            self.stdin
                .write_all(buffer.as_slice())
                .map_err(write_worker_frame_error)?;
        }
        self.stdin.flush().map_err(write_worker_frame_error)?;
        Ok(())
    }

    fn read_frame(&mut self, memory: &OperatorMemoryScope<'_>) -> Result<WorkerResponse> {
        let mut magic = [0_u8; 8];
        if let Err(error) = self.stdout.read_exact(&mut magic) {
            return Err(self.read_worker_frame_error(error));
        }
        if &magic != PYTHON_WORKER_FRAME_MAGIC {
            return Err(paro_error::external_protocol_mismatch(
                "worker response has invalid binary frame magic",
            ));
        }

        let mut header_bytes = [0_u8; CONTROL_HEADER_SIZE];
        self.stdout
            .read_exact(&mut header_bytes)
            .map_err(|error| self.read_worker_frame_error(error))?;
        let header = ControlHeader::decode(&header_bytes)
            .map_err(|error| paro_error::external_protocol_mismatch(error.to_string()))?;
        let payload_len = self.read_u32()?;
        let buffer_count = self.read_u32()?;
        if header.payload_len != payload_len {
            return Err(paro_error::external_protocol_mismatch(format!(
                "worker response payload length {} does not match frame length {}",
                header.payload_len, payload_len
            )));
        }

        let mut buffer_lengths = Vec::with_capacity(buffer_count as usize);
        for _ in 0..buffer_count {
            let len = usize::try_from(self.read_u64()?).map_err(|_| {
                paro_error::external_protocol_mismatch(
                    "worker response buffer length does not fit host usize",
                )
            })?;
            buffer_lengths.push(len);
        }

        let mut payload_bytes = bridge_bytes_with_capacity(memory, payload_len as usize)?;
        payload_bytes.try_resize(payload_len as usize, 0)?;
        self.stdout
            .read_exact(payload_bytes.as_mut_slice())
            .map_err(|error| self.read_worker_frame_error(error))?;
        let payload: JsonValue = if payload_bytes.is_empty() {
            JsonValue::Object(Default::default())
        } else {
            serde_json::from_slice(payload_bytes.as_slice()).map_err(|error| {
                paro_error::external_protocol_mismatch(format!(
                    "failed to decode worker response: {error}"
                ))
            })?
        };
        let payload_memory = retain_bridge_bytes(memory, payload_len as usize)?;

        let mut buffers = AccountedVec::new_with_accounting(
            memory.split_sub_grant(0)?,
            PYTHON_BRIDGE_TAG,
            PYTHON_BRIDGE_CLASS,
        );
        for len in buffer_lengths {
            let mut buffer = bridge_bytes_with_capacity(memory, len)?;
            buffer.try_resize(len, 0)?;
            self.stdout
                .read_exact(buffer.as_mut_slice())
                .map_err(|error| self.read_worker_frame_error(error))?;
            buffers.try_push(buffer)?;
        }

        Ok(WorkerResponse {
            header,
            payload,
            buffers,
            _control_memory: vec![payload_memory],
        })
    }

    fn read_u32(&mut self) -> Result<u32> {
        let mut bytes = [0_u8; 4];
        self.stdout
            .read_exact(&mut bytes)
            .map_err(|error| self.read_worker_frame_error(error))?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64> {
        let mut bytes = [0_u8; 8];
        self.stdout
            .read_exact(&mut bytes)
            .map_err(|error| self.read_worker_frame_error(error))?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_worker_frame_error(&mut self, error: std::io::Error) -> paro_common::error::ParoError {
        let _ = self.child.try_wait();
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            return paro_error::worker_failure("python worker closed stdout unexpectedly");
        }
        paro_error::worker_failure(format!("failed to read worker binary frame: {error}"))
    }
}

fn write_worker_frame_error(error: std::io::Error) -> paro_common::error::ParoError {
    paro_error::worker_failure(format!("failed to write worker binary frame: {error}"))
}

#[derive(Debug)]
struct WorkerRequest {
    header: [u8; 32],
    payload: JsonValue,
    buffers: AccountedVec<AccountedBytesMut>,
    _control_memory: Vec<RetainedMemoryHandle>,
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
        memory: &OperatorMemoryScope<'_>,
    ) -> Result<Self> {
        let encoded = encode_chunk_to_abi(input, query_epoch, memory)?;
        let mut control_memory = Vec::new();
        let payload = json!({
            "module_path": routine.module_path.to_string_lossy(),
            "handler": routine.handler,
            "cache_key": routine.cache_key,
            "search_paths": routine.search_paths,
            "lease": serde_json::to_value(&encoded.lease).map_err(json_error)?,
            "buffer_count": encoded.buffers.len(),
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
        let payload_len = u32::try_from(serde_json::to_vec(&payload).map_err(json_error)?.len())
            .map_err(|_| paro_error::worker_failure("worker request payload exceeds u32 length"))?;
        control_memory.push(retain_bridge_bytes(memory, payload_len as usize)?);
        Ok(Self {
            header: ControlHeader::new(
                ControlMessageKind::Submit,
                batch_id,
                encoded.lease.lease_id,
                payload_len,
            )
            .encode(),
            payload,
            buffers: encoded.buffers,
            _control_memory: control_memory,
        })
    }
}

#[derive(Debug)]
struct WorkerResponse {
    header: ControlHeader,
    payload: JsonValue,
    buffers: AccountedVec<AccountedBytesMut>,
    _control_memory: Vec<RetainedMemoryHandle>,
}

#[derive(Debug)]
struct EncodedAbiChunk {
    lease: ColumnBatchLease,
    buffers: AccountedVec<AccountedBytesMut>,
}

fn encode_chunk_to_abi(
    input: &Chunk,
    query_epoch: u64,
    memory: &OperatorMemoryScope<'_>,
) -> Result<EncodedAbiChunk> {
    let mut buffers = AccountedVec::new_with_accounting(
        memory.split_sub_grant(0)?,
        PYTHON_BRIDGE_TAG,
        PYTHON_BRIDGE_CLASS,
    );
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
            memory,
        )?);
    }

    let lease = ColumnBatchLease {
        version: 1,
        lease_id: 1,
        row_count: u32::try_from(input.size()).map_err(|_| {
            paro_error::contract_violation("worker ABI input row count exceeds u32")
        })?,
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
    buffers: &mut AccountedVec<AccountedBytesMut>,
    memory: &OperatorMemoryScope<'_>,
) -> Result<ColumnDescriptor> {
    let logical_type = logical_type_to_abi(column.logical_type()).ok_or_else(|| {
        paro_error::contract_violation(format!(
            "logical type '{}' is not supported by the Python worker bridge yet",
            column.logical_type()
        ))
    })?;
    let has_null = (0..row_count).any(|row| column.is_null(row));
    let validity = if has_null {
        let bitmap = pack_validity_bitmap(column, row_count, memory)?;
        let buffer_index = next_buffer_index(buffers)?;
        let bitmap_len = bitmap.len();
        buffers.try_push(bitmap)?;
        Some(BufferLease::host(buffer_index, 0, bitmap_len as u64, 1))
    } else {
        None
    };

    match logical_type {
        AbiLogicalType::Varchar
        | AbiLogicalType::Blob
        | AbiLogicalType::Json
        | AbiLogicalType::Jsonb => {
            let mut offsets = bridge_bytes_with_capacity(memory, (row_count + 1) * 4)?;
            let mut data = bridge_bytes_with_capacity(memory, row_count * 8)?;
            let mut current = 0_u32;
            offsets.try_extend_from_slice(&current.to_le_bytes())?;
            for row in 0..row_count {
                if !column.is_null(row) {
                    match column.get_value(row) {
                        Value::Varchar(value) => {
                            let value_len = u32::try_from(value.len()).map_err(|_| {
                                paro_error::contract_violation(
                                    "worker ABI varchar value exceeds U32 offset range",
                                )
                            })?;
                            data.try_extend_from_slice(value.as_bytes())?;
                            current = current.checked_add(value_len).ok_or_else(|| {
                                paro_error::contract_violation(
                                    "worker ABI varlen payload exceeds U32 offset range",
                                )
                            })?;
                        }
                        Value::Blob(value) => {
                            let value_len = u32::try_from(value.len()).map_err(|_| {
                                paro_error::contract_violation(
                                    "worker ABI blob value exceeds U32 offset range",
                                )
                            })?;
                            data.try_extend_from_slice(&value)?;
                            current = current.checked_add(value_len).ok_or_else(|| {
                                paro_error::contract_violation(
                                    "worker ABI varlen payload exceeds U32 offset range",
                                )
                            })?;
                        }
                        other => {
                            return Err(paro_error::contract_violation(format!(
                                "expected varlen value for '{}', got {other:?}",
                                column.logical_type()
                            )))
                        }
                    }
                }
                offsets.try_extend_from_slice(&current.to_le_bytes())?;
            }
            let offsets_index = next_buffer_index(buffers)?;
            let offsets_len = offsets.len();
            buffers.try_push(offsets)?;
            let data_index = next_buffer_index(buffers)?;
            let data_len = data.len();
            buffers.try_push(data)?;
            Ok(ColumnDescriptor {
                name: name.to_string(),
                logical_type,
                encoding: ColumnEncoding::Flat,
                population_mode: ColumnPopulationMode::Eager,
                nullable: validity.is_some(),
                validity,
                layout: ColumnLayout::VarLen {
                    offsets: BufferLease::host(offsets_index, 0, offsets_len as u64, 4),
                    data: BufferLease::host(data_index, 0, data_len as u64, 1),
                    offset_width: OffsetWidth::U32,
                },
                children: Vec::new(),
            })
        }
        _ => {
            let stride = logical_type
                .fixed_width_bytes()
                .ok_or_else(|| paro_error::contract_violation("missing fixed-width stride"))?;
            let mut values = bridge_bytes_with_capacity(memory, row_count * stride as usize)?;
            for row in 0..row_count {
                encode_fixed_width_value(&mut values, &column.get_value(row), &logical_type)?;
            }
            let buffer_index = next_buffer_index(buffers)?;
            let values_len = values.len();
            buffers.try_push(values)?;
            Ok(ColumnDescriptor {
                name: name.to_string(),
                logical_type,
                encoding: ColumnEncoding::Flat,
                population_mode: ColumnPopulationMode::Eager,
                nullable: validity.is_some(),
                validity,
                layout: ColumnLayout::FixedWidth {
                    values: BufferLease::host(buffer_index, 0, values_len as u64, stride),
                    stride,
                },
                children: Vec::new(),
            })
        }
    }
}

fn decode_response_chunk(
    payload: &JsonValue,
    buffers: &[AccountedBytesMut],
    expected_types: &[LogicalType],
    memory: &OperatorMemoryScope<'_>,
) -> Result<Chunk> {
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
    if lease.columns.len() != expected_types.len() {
        return Err(paro_error::contract_violation(format!(
            "worker returned {} columns, expected {}",
            lease.columns.len(),
            expected_types.len()
        )));
    }

    let allocator = memory.accounted_allocator_for(PYTHON_BRIDGE_TAG, PYTHON_BRIDGE_CLASS);
    let mut vectors = Vec::with_capacity(expected_types.len());
    for (descriptor, expected_type) in lease.columns.iter().zip(expected_types.iter()) {
        let mut vector = Vector::try_new(
            expected_type.clone(),
            lease.row_count as usize,
            allocator.clone(),
        )?;
        decode_descriptor_into_vector(
            descriptor,
            buffers,
            lease.row_count as usize,
            &mut vector,
            expected_type,
        )?;
        vector.set_len(lease.row_count as usize);
        vectors.push(vector);
    }
    Ok(Chunk::from_vectors(vectors, allocator))
}

fn decode_descriptor_into_vector(
    descriptor: &ColumnDescriptor,
    buffers: &[AccountedBytesMut],
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
    descriptor.validate().map_err(|error| {
        paro_error::external_protocol_mismatch(format!(
            "worker returned invalid column descriptor: {error}"
        ))
    })?;

    let validity = if let Some(lease) = descriptor.validity.as_ref() {
        let validity = buffer_from_lease(buffers, lease)?;
        validate_validity_bitmap(validity, row_count, &descriptor.name)?;
        Some(validity)
    } else {
        None
    };

    match &descriptor.layout {
        ColumnLayout::FixedWidth { values, stride } => {
            validate_fixed_width_layout(&descriptor.logical_type, *stride, row_count)?;
            let buffer = buffer_from_lease(buffers, values)?;
            validate_fixed_width_buffer(&descriptor.logical_type, buffer, row_count)?;
            for row in 0..row_count {
                if validity_is_null(validity, row)? {
                    vector.validity_mut().set_null(row);
                    continue;
                }
                let value = decode_fixed_width_value(&descriptor.logical_type, buffer, row)?;
                vector.set_value(row, &value);
            }
        }
        ColumnLayout::VarLen {
            offsets,
            data,
            offset_width,
        } => {
            if !matches!(offset_width, OffsetWidth::U32) {
                return Err(paro_error::external_protocol_mismatch(
                    "worker varlen output must use U32 offsets",
                ));
            }
            let offsets_buffer = buffer_from_lease(buffers, offsets)?;
            let data_buffer = buffer_from_lease(buffers, data)?;
            validate_varlen_offsets_buffer(offsets_buffer, row_count)?;
            let mut start = read_u32(offsets_buffer, 0)? as usize;
            if start > data_buffer.len() {
                return Err(paro_error::external_protocol_mismatch(format!(
                    "worker varlen first offset {start} exceeds data buffer length {}",
                    data_buffer.len()
                )));
            }
            for row in 0..row_count {
                let end = read_u32(offsets_buffer, (row + 1) * 4)? as usize;
                validate_varlen_range(start, end, data_buffer.len(), row)?;
                if validity_is_null(validity, row)? {
                    vector.validity_mut().set_null(row);
                    start = end;
                    continue;
                }
                let bytes = data_buffer.get(start..end).ok_or_else(|| {
                    paro_error::external_protocol_mismatch(format!(
                        "worker varlen row {row} range [{start}..{end}) exceeds data buffer length {}",
                        data_buffer.len()
                    ))
                })?;
                match descriptor.logical_type {
                    AbiLogicalType::Varchar | AbiLogicalType::Json => {
                        let value = std::str::from_utf8(bytes).map_err(|error| {
                            paro_error::contract_violation(format!(
                                "worker returned invalid UTF-8 output: {error}"
                            ))
                        })?;
                        vector.set_string(row, value);
                    }
                    AbiLogicalType::Blob | AbiLogicalType::Jsonb => vector.set_blob(row, bytes),
                    _ => {
                        return Err(paro_error::contract_violation(format!(
                            "varlen output is not supported for {:?}",
                            descriptor.logical_type
                        )))
                    }
                };
                start = end;
            }
        }
        ColumnLayout::Constant { value } => {
            let scalar = scalar_value_to_runtime(value)?;
            for row in 0..row_count {
                if validity_is_null(validity, row)? {
                    vector.validity_mut().set_null(row);
                } else {
                    vector.set_value(row, &scalar);
                }
            }
        }
        ColumnLayout::Sequence { start, step } => {
            for row in 0..row_count {
                if validity_is_null(validity, row)? {
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

fn buffer_from_lease<'a>(
    buffers: &'a [AccountedBytesMut],
    lease: &BufferLease,
) -> Result<&'a [u8]> {
    let buffer = buffers.get(lease.buffer_index as usize).ok_or_else(|| {
        paro_error::external_protocol_mismatch(format!(
            "buffer index {} is out of range",
            lease.buffer_index
        ))
    })?;
    let start = usize::try_from(lease.offset).map_err(|_| {
        paro_error::external_protocol_mismatch(format!(
            "buffer lease offset {} does not fit host usize",
            lease.offset
        ))
    })?;
    let len = usize::try_from(lease.len).map_err(|_| {
        paro_error::external_protocol_mismatch(format!(
            "buffer lease length {} does not fit host usize",
            lease.len
        ))
    })?;
    let end = start.checked_add(len).ok_or_else(|| {
        paro_error::external_protocol_mismatch(format!(
            "buffer slice [{}..+{}) overflows host usize",
            start, len
        ))
    })?;
    buffer.as_slice().get(start..end).ok_or_else(|| {
        paro_error::external_protocol_mismatch(format!(
            "buffer slice [{}..{}) exceeds buffer length {}",
            start,
            end,
            buffer.len()
        ))
    })
}

fn validate_validity_bitmap(validity: &[u8], row_count: usize, column_name: &str) -> Result<()> {
    let required = row_count.div_ceil(8);
    if validity.len() < required {
        return Err(paro_error::external_protocol_mismatch(format!(
            "worker validity bitmap for column '{column_name}' has {} bytes, expected at least {required}",
            validity.len()
        )));
    }
    Ok(())
}

fn validity_is_null(validity: Option<&[u8]>, index: usize) -> Result<bool> {
    let Some(buffer) = validity else {
        return Ok(false);
    };
    let byte = buffer.get(index / 8).ok_or_else(|| {
        paro_error::external_protocol_mismatch(format!(
            "worker validity bitmap is truncated at row {index}"
        ))
    })?;
    Ok((byte & (1 << (index % 8))) == 0)
}

fn validate_fixed_width_layout(
    logical_type: &AbiLogicalType,
    stride: u32,
    row_count: usize,
) -> Result<()> {
    let expected = logical_type.fixed_width_bytes().ok_or_else(|| {
        paro_error::external_protocol_mismatch(format!(
            "worker fixed-width layout is invalid for {logical_type:?}"
        ))
    })?;
    if stride != expected {
        return Err(paro_error::external_protocol_mismatch(format!(
            "worker fixed-width stride {stride} does not match {logical_type:?} width {expected}"
        )));
    }
    row_count.checked_mul(stride as usize).ok_or_else(|| {
        paro_error::external_protocol_mismatch("worker fixed-width buffer length overflow")
    })?;
    Ok(())
}

fn validate_fixed_width_buffer(
    logical_type: &AbiLogicalType,
    buffer: &[u8],
    row_count: usize,
) -> Result<()> {
    let stride = logical_type.fixed_width_bytes().ok_or_else(|| {
        paro_error::external_protocol_mismatch(format!(
            "worker fixed-width layout is invalid for {logical_type:?}"
        ))
    })? as usize;
    let required = row_count.checked_mul(stride).ok_or_else(|| {
        paro_error::external_protocol_mismatch("worker fixed-width buffer length overflow")
    })?;
    if buffer.len() < required {
        return Err(paro_error::external_protocol_mismatch(format!(
            "worker fixed-width buffer has {} bytes, expected at least {required}",
            buffer.len()
        )));
    }
    Ok(())
}

fn validate_varlen_offsets_buffer(offsets: &[u8], row_count: usize) -> Result<()> {
    let required = row_count
        .checked_add(1)
        .and_then(|count| count.checked_mul(4))
        .ok_or_else(|| {
            paro_error::external_protocol_mismatch("worker varlen offsets length overflow")
        })?;
    if offsets.len() < required {
        return Err(paro_error::external_protocol_mismatch(format!(
            "worker varlen offsets buffer has {} bytes, expected at least {required}",
            offsets.len()
        )));
    }
    Ok(())
}

fn validate_varlen_range(start: usize, end: usize, data_len: usize, row: usize) -> Result<()> {
    if end < start {
        return Err(paro_error::external_protocol_mismatch(format!(
            "worker varlen offsets are not monotonic at row {row}: {start} > {end}"
        )));
    }
    if end > data_len {
        return Err(paro_error::external_protocol_mismatch(format!(
            "worker varlen row {row} end offset {end} exceeds data buffer length {data_len}"
        )));
    }
    Ok(())
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
        AbiLogicalType::Boolean => Ok(Value::Boolean(read_fixed_bytes(buffer, offset, 1)?[0] != 0)),
        AbiLogicalType::Int8 => Ok(Value::TinyInt(
            read_fixed_bytes(buffer, offset, 1)?[0] as i8,
        )),
        AbiLogicalType::UInt8 => Ok(Value::UTinyInt(read_fixed_bytes(buffer, offset, 1)?[0])),
        AbiLogicalType::Int16 => Ok(Value::SmallInt(i16::from_le_bytes(read_fixed_array::<2>(
            buffer, offset,
        )?))),
        AbiLogicalType::UInt16 => Ok(Value::USmallInt(u16::from_le_bytes(read_fixed_array::<2>(
            buffer, offset,
        )?))),
        AbiLogicalType::Int32 => Ok(Value::Integer(i32::from_le_bytes(read_fixed_array::<4>(
            buffer, offset,
        )?))),
        AbiLogicalType::Date => Ok(Value::Date(i32::from_le_bytes(read_fixed_array::<4>(
            buffer, offset,
        )?))),
        AbiLogicalType::UInt32 => Ok(Value::UInteger(u32::from_le_bytes(read_fixed_array::<4>(
            buffer, offset,
        )?))),
        AbiLogicalType::Int64 => Ok(Value::BigInt(i64::from_le_bytes(read_fixed_array::<8>(
            buffer, offset,
        )?))),
        AbiLogicalType::Time => Ok(Value::Time(i64::from_le_bytes(read_fixed_array::<8>(
            buffer, offset,
        )?))),
        AbiLogicalType::Timestamp => Ok(Value::Timestamp(i64::from_le_bytes(
            read_fixed_array::<8>(buffer, offset)?,
        ))),
        AbiLogicalType::TimestampTz => Ok(Value::TimestampTz(i64::from_le_bytes(
            read_fixed_array::<8>(buffer, offset)?,
        ))),
        AbiLogicalType::UInt64 => Ok(Value::UBigInt(u64::from_le_bytes(read_fixed_array::<8>(
            buffer, offset,
        )?))),
        AbiLogicalType::Float32 => Ok(Value::Float(f32::from_le_bytes(read_fixed_array::<4>(
            buffer, offset,
        )?))),
        AbiLogicalType::Float64 => Ok(Value::Double(f64::from_le_bytes(read_fixed_array::<8>(
            buffer, offset,
        )?))),
        other => Err(paro_error::contract_violation(format!(
            "fixed-width decode is not implemented for {other:?}"
        ))),
    }
}

fn read_fixed_array<const N: usize>(buffer: &[u8], offset: usize) -> Result<[u8; N]> {
    Ok(read_fixed_bytes(buffer, offset, N)?
        .try_into()
        .expect("slice length is fixed by read_fixed_bytes"))
}

fn read_fixed_bytes(buffer: &[u8], offset: usize, len: usize) -> Result<&[u8]> {
    let end = offset.checked_add(len).ok_or_else(|| {
        paro_error::external_protocol_mismatch("worker fixed-width byte offset overflow")
    })?;
    buffer.get(offset..end).ok_or_else(|| {
        paro_error::external_protocol_mismatch(format!(
            "worker fixed-width range [{offset}..{end}) exceeds buffer length {}",
            buffer.len()
        ))
    })
}

fn encode_fixed_width_value(
    buffer: &mut AccountedBytesMut,
    value: &Value,
    logical_type: &AbiLogicalType,
) -> Result<()> {
    match logical_type {
        AbiLogicalType::Boolean => buffer.try_push(if matches!(value, Value::Boolean(true)) {
            1
        } else {
            0
        })?,
        AbiLogicalType::Int8 => buffer.try_push(match value {
            Value::TinyInt(v) => *v as u8,
            Value::Null(_) => 0,
            other => return Err(type_mismatch(other, logical_type)),
        })?,
        AbiLogicalType::UInt8 => buffer.try_push(match value {
            Value::UTinyInt(v) => *v,
            Value::Null(_) => 0,
            other => return Err(type_mismatch(other, logical_type)),
        })?,
        AbiLogicalType::Int16 => buffer.try_extend_from_slice(&match value {
            Value::SmallInt(v) => v.to_le_bytes(),
            Value::Null(_) => 0_i16.to_le_bytes(),
            other => return Err(type_mismatch(other, logical_type)),
        })?,
        AbiLogicalType::UInt16 => buffer.try_extend_from_slice(&match value {
            Value::USmallInt(v) => v.to_le_bytes(),
            Value::Null(_) => 0_u16.to_le_bytes(),
            other => return Err(type_mismatch(other, logical_type)),
        })?,
        AbiLogicalType::Int32 | AbiLogicalType::Date => {
            buffer.try_extend_from_slice(&match value {
                Value::Integer(v) => v.to_le_bytes(),
                Value::Date(v) => v.to_le_bytes(),
                Value::Null(_) => 0_i32.to_le_bytes(),
                other => return Err(type_mismatch(other, logical_type)),
            })?
        }
        AbiLogicalType::UInt32 => buffer.try_extend_from_slice(&match value {
            Value::UInteger(v) => v.to_le_bytes(),
            Value::Null(_) => 0_u32.to_le_bytes(),
            other => return Err(type_mismatch(other, logical_type)),
        })?,
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
            buffer.try_extend_from_slice(&bytes)?;
        }
        AbiLogicalType::UInt64 => buffer.try_extend_from_slice(&match value {
            Value::UBigInt(v) => v.to_le_bytes(),
            Value::Null(_) => 0_u64.to_le_bytes(),
            other => return Err(type_mismatch(other, logical_type)),
        })?,
        AbiLogicalType::Float32 => buffer.try_extend_from_slice(&match value {
            Value::Float(v) => v.to_le_bytes(),
            Value::Null(_) => 0_f32.to_le_bytes(),
            other => return Err(type_mismatch(other, logical_type)),
        })?,
        AbiLogicalType::Float64 => buffer.try_extend_from_slice(&match value {
            Value::Double(v) => v.to_le_bytes(),
            Value::Null(_) => 0_f64.to_le_bytes(),
            other => return Err(type_mismatch(other, logical_type)),
        })?,
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

fn scalar_value_to_runtime(value: &paro_external::abi::layout::ScalarValueRef) -> Result<Value> {
    match value {
        paro_external::abi::layout::ScalarValueRef::Null => Ok(Value::Null(LogicalType::Unknown)),
        paro_external::abi::layout::ScalarValueRef::Boolean(v) => Ok(Value::Boolean(*v)),
        paro_external::abi::layout::ScalarValueRef::Int32(v) => Ok(Value::Integer(*v)),
        paro_external::abi::layout::ScalarValueRef::Int64(v) => Ok(Value::BigInt(*v)),
        paro_external::abi::layout::ScalarValueRef::UInt32(v) => Ok(Value::UInteger(*v)),
        paro_external::abi::layout::ScalarValueRef::UInt64(v) => Ok(Value::UBigInt(*v)),
        paro_external::abi::layout::ScalarValueRef::Utf8(v) => Ok(Value::Varchar(v.clone())),
        paro_external::abi::layout::ScalarValueRef::Binary(v) => Ok(Value::Blob(v.clone())),
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
    let end = offset.checked_add(4).ok_or_else(|| {
        paro_error::external_protocol_mismatch("offset buffer index overflows host usize")
    })?;
    let bytes = buffer
        .get(offset..end)
        .ok_or_else(|| paro_error::external_protocol_mismatch("offset buffer is truncated"))?;
    Ok(u32::from_le_bytes(bytes.try_into().expect("u32 bytes")))
}

fn next_buffer_index(buffers: &AccountedVec<AccountedBytesMut>) -> Result<u16> {
    u16::try_from(buffers.len())
        .map_err(|_| paro_error::contract_violation("worker ABI buffer index exceeds u16"))
}

fn pack_validity_bitmap(
    column: &Vector,
    length: usize,
    memory: &OperatorMemoryScope<'_>,
) -> Result<AccountedBytesMut> {
    let mut bitmap = bridge_bytes_with_capacity(memory, (length + 7) / 8)?;
    bitmap.try_resize((length + 7) / 8, 0)?;
    for index in 0..length {
        if !column.is_null(index) {
            bitmap.as_mut_slice()[index / 8] |= 1 << (index % 8);
        }
    }
    Ok(bitmap)
}

fn bridge_bytes_with_capacity(
    memory: &OperatorMemoryScope<'_>,
    capacity: usize,
) -> Result<AccountedBytesMut> {
    let mut bytes = AccountedBytesMut::new_with_accounting(
        memory.split_sub_grant(0)?,
        PYTHON_BRIDGE_TAG,
        PYTHON_BRIDGE_CLASS,
    );
    bytes.try_reserve(capacity)?;
    Ok(bytes)
}

fn retain_bridge_bytes(
    memory: &OperatorMemoryScope<'_>,
    bytes: usize,
) -> Result<RetainedMemoryHandle> {
    Ok(RetainedMemoryHandle::new(
        memory.retain_external_allocation_handle(PYTHON_BRIDGE_TAG, PYTHON_BRIDGE_CLASS, bytes)?,
    ))
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
    use crate::memory_runtime::OperatorMemoryScope;
    use crate::operator::external::batching::SubmissionBatchPolicy;
    use crate::operator::external::runtime_bridge::{
        ExternalRoutineDescriptor, ExternalRuntimeBridge, ProjectSubmission, RuntimeBridgeOutcome,
        TableSubmission,
    };
    use crate::thread_context::ThreadContext;
    use paro_common::chunk::Chunk;
    use paro_common::error::codes;
    use paro_common::memory::{AccountedBytesMut, AccountedVec};
    use paro_common::types::LogicalType;

    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_external::abi::descriptor::ColumnDescriptor;
    use paro_external::abi::encoding::{ColumnEncoding, ColumnPopulationMode};
    use paro_external::abi::layout::{BufferLease, ColumnLayout, OffsetWidth};
    use paro_external::abi::lease::{LeaseOwnership, LeaseState};
    use paro_external::abi::types::AbiLogicalType;
    use paro_external::routine::bound::BoundRoutineCallMeta;
    use paro_external::routine::boundary::{ExecutionBoundary, PlacementClass};
    use paro_external::routine::capability::{CapabilityProfile, CapabilityProfilePreset};
    use paro_external::routine::env::{DeclaredEnvSpec, ImportRef, PythonRuntimeSelector};
    use paro_external::routine::identity::RoutineCallIdentity;
    use paro_external::routine::permission::{PermissionSpec, RoutineSecurityMode};
    use paro_external::routine::spec::{
        PythonEntrypointRef, PythonImplementationRef, RoutineArgument, RoutineExecutionContract,
        RoutineFamily, RoutineIdentity, RoutineImplementationRef, RoutineNullPolicy, RoutineOwner,
        RoutineReturn, RoutineSemantics, RoutineSideEffects, RoutineSpec, RoutineStability,
        RoutineTableColumn, RowSemantics, ScalarRoutineContract, SourceBlobRef,
        TableRoutineContract,
    };
    use paro_external::runtime::host::ExternalRuntimeHost;
    use paro_function::scalar::ScalarFunction;
    use paro_planner::expression::{Expression, FunctionExpression, ReferenceExpression};
    use paro_planner::operator::external_project::ExternalProjectExpression;
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
                id: paro_external::routine::spec::RoutineId::from_raw(77),
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
                id: paro_external::routine::spec::RoutineId::from_raw(88),
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
        Chunk::from_vectors(
            vec![paro_common::test_utils::test_i32_vector_with_allocator(
                values,
                paro_common::test_utils::test_allocator(),
            )],
            paro_common::test_utils::test_allocator(),
        )
    }

    fn response_buffers(
        memory: &OperatorMemoryScope<'_>,
        parts: &[&[u8]],
    ) -> AccountedVec<AccountedBytesMut> {
        let mut buffers = AccountedVec::new_with_accounting(
            memory.split_sub_grant(0).expect("test grant"),
            super::PYTHON_BRIDGE_TAG,
            super::PYTHON_BRIDGE_CLASS,
        );
        for part in parts {
            let mut buffer = AccountedBytesMut::new_with_accounting(
                memory.split_sub_grant(0).expect("test grant"),
                super::PYTHON_BRIDGE_TAG,
                super::PYTHON_BRIDGE_CLASS,
            );
            buffer.try_extend_from_slice(part).expect("test buffer");
            buffers.try_push(buffer).expect("test buffer vec");
        }
        buffers
    }

    fn response_payload(row_count: u32, descriptor: ColumnDescriptor) -> serde_json::Value {
        let lease = paro_external::abi::lease::ColumnBatchLease {
            version: 1,
            lease_id: 1,
            row_count,
            state: LeaseState::Committed,
            ownership: LeaseOwnership {
                owner_worker_epoch: 0,
                owner_host_epoch: 0,
                owner_query_epoch: 1,
            },
            completion_fence: 0,
            payload_checksum: None,
            columns: vec![descriptor],
        };
        serde_json::json!({
            "state": "Finished",
            "lease": lease,
        })
    }

    fn varchar_descriptor(offset_len: u64, data_len: u64) -> ColumnDescriptor {
        ColumnDescriptor {
            name: "value".to_string(),
            logical_type: AbiLogicalType::Varchar,
            encoding: ColumnEncoding::Flat,
            population_mode: ColumnPopulationMode::Eager,
            nullable: false,
            validity: None,
            layout: ColumnLayout::VarLen {
                offsets: BufferLease::host(0, 0, offset_len, 4),
                data: BufferLease::host(1, 0, data_len, 1),
                offset_width: OffsetWidth::U32,
            },
            children: Vec::new(),
        }
    }

    fn int32_descriptor(values_len: u64, validity: Option<BufferLease>) -> ColumnDescriptor {
        ColumnDescriptor {
            name: "value".to_string(),
            logical_type: AbiLogicalType::Int32,
            encoding: ColumnEncoding::Flat,
            population_mode: ColumnPopulationMode::Eager,
            nullable: validity.is_some(),
            validity,
            layout: ColumnLayout::FixedWidth {
                values: BufferLease::host(0, 0, values_len, 4),
                stride: 4,
            },
            children: Vec::new(),
        }
    }

    fn assert_decode_protocol_error(
        payload: serde_json::Value,
        buffers: &[AccountedBytesMut],
        expected_types: &[LogicalType],
        memory: &OperatorMemoryScope<'_>,
        needle: &str,
    ) {
        let error =
            super::decode_response_chunk(&payload, buffers, expected_types, memory).unwrap_err();
        assert!(
            error.to_string().contains(needle),
            "expected '{needle}' in error: {error}"
        );
    }

    fn batch_policy(bridge: &ExternalRuntimeBridge) -> SubmissionBatchPolicy {
        SubmissionBatchPolicy::from_dispatch_policy(bridge.dispatch_policy())
    }

    #[test]
    fn response_decode_rejects_varlen_offset_past_data_buffer() {
        let memory = crate::operator::state::test_operator_memory_scope();
        let offsets = [0_u32.to_le_bytes(), 5_u32.to_le_bytes()].concat();
        let buffers = response_buffers(&memory, &[&offsets, b"abc"]);
        let payload = response_payload(1, varchar_descriptor(offsets.len() as u64, 3));

        assert_decode_protocol_error(
            payload,
            buffers.as_slice(),
            &[LogicalType::Varchar],
            &memory,
            "exceeds data buffer",
        );
    }

    #[test]
    fn response_decode_rejects_non_monotonic_varlen_offsets() {
        let memory = crate::operator::state::test_operator_memory_scope();
        let offsets = [2_u32.to_le_bytes(), 1_u32.to_le_bytes()].concat();
        let buffers = response_buffers(&memory, &[&offsets, b"abc"]);
        let payload = response_payload(1, varchar_descriptor(offsets.len() as u64, 3));

        assert_decode_protocol_error(
            payload,
            buffers.as_slice(),
            &[LogicalType::Varchar],
            &memory,
            "not monotonic",
        );
    }

    #[test]
    fn response_decode_rejects_truncated_fixed_width_buffer() {
        let memory = crate::operator::state::test_operator_memory_scope();
        let buffers = response_buffers(&memory, &[&[1, 0, 0]]);
        let payload = response_payload(1, int32_descriptor(3, None));

        assert_decode_protocol_error(
            payload,
            buffers.as_slice(),
            &[LogicalType::Integer],
            &memory,
            "fixed-width buffer",
        );
    }

    #[test]
    fn response_decode_rejects_truncated_validity_bitmap() {
        let memory = crate::operator::state::test_operator_memory_scope();
        let values = 7_i32.to_le_bytes();
        let buffers = response_buffers(&memory, &[&values, &[]]);
        let descriptor = int32_descriptor(4, Some(BufferLease::host(1, 0, 0, 1)));
        let payload = response_payload(1, descriptor);

        assert_decode_protocol_error(
            payload,
            buffers.as_slice(),
            &[LogicalType::Integer],
            &memory,
            "validity bitmap",
        );
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
            .execute_project(
                &ctx,
                &submission,
                &crate::operator::state::test_operator_memory_scope(),
            )
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
            .execute_project(
                &ctx,
                &submission,
                &crate::operator::state::test_operator_memory_scope(),
            )
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
            .execute_project(
                &ctx,
                &submission,
                &crate::operator::state::test_operator_memory_scope(),
            )
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
            .execute_project(
                &ctx,
                &submission,
                &crate::operator::state::test_operator_memory_scope(),
            )
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
            .execute_project(
                &ctx,
                &submission,
                &crate::operator::state::test_operator_memory_scope(),
            )
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
            .execute_project(
                &ctx,
                &submission,
                &crate::operator::state::test_operator_memory_scope(),
            )
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
            .execute_project(
                &ctx,
                &submission,
                &crate::operator::state::test_operator_memory_scope(),
            )
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
            .execute_project(
                &ctx,
                &submission,
                &crate::operator::state::test_operator_memory_scope(),
            )
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
            .execute_table(
                &ctx,
                &submission,
                &crate::operator::state::test_operator_memory_scope(),
            )
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
            .execute_table(
                &ctx,
                &submission,
                &crate::operator::state::test_operator_memory_scope(),
            )
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
