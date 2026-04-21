// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::codes;
use paro_external_abi::{
    AbiLogicalType, BufferLease, ColumnDescriptor, ColumnEncoding, ColumnLayout,
    ColumnPopulationMode, LeaseOwnership,
};
use paro_external_runtime::artifact::cache::{ArtifactCache, ArtifactCacheKey};
use paro_external_runtime::artifact::gc::ArtifactGcPolicy;
use paro_external_runtime::artifact::materialize::ArtifactMaterializer;
use paro_external_runtime::artifact::resolve::{ArtifactResolver, ResolveInputs};
use paro_external_runtime::artifact::validate::ArtifactValidator;
use paro_external_runtime::backend::sandbox::SandboxRuntimeKind;
use paro_external_runtime::backend::selector::{BackendAvailability, BackendKind, BackendSelector};
use paro_external_runtime::control::cancel::{
    CancelAction, CancelEscalation, CancelEscalationPolicy, CancelStage,
};
use paro_external_runtime::control::header::{ControlHeader, ControlMessageKind};
use paro_external_runtime::control::notifier::{ControlNotifier, NotifierAvailability};
use paro_external_runtime::control::retry::{RetryFailureKind, RetryPolicy};
use paro_external_runtime::data_plane::arena::{
    ArenaBacking, ArenaConfig, ArenaKind, ArenaNamespace, SharedArena,
};
use paro_external_runtime::dispatch::permits::{PermitBudget, PermitPool};
use paro_external_runtime::dispatch::policy::ExternalDispatchPolicy;
use paro_external_runtime::error::ExternalRoutineErrorKind;
use paro_external_runtime::host::{
    ExternalRuntimeHost, PythonRuntimeAvailability, PythonRuntimeProbe, PythonRuntimeProbeResult,
    PythonRuntimeProvider, PythonRuntimeStartupPolicy,
};
use paro_external_runtime::isolation::process_enforcer::{
    ProcessIsolationEnforcer, ProcessPlatformCapabilities, SeccompDefaultAction,
};
use paro_external_runtime::isolation::resource_limits::{
    NetworkAllowRule, NetworkPolicy, ResourceLimits,
};
use paro_external_runtime::metrics::autotuning::{Autotuner, PerfObservation};
use paro_external_runtime::metrics::profile_store::{InMemoryProfileStore, RoutinePerfProfileKey};
use paro_external_runtime::protocol::messages::{KernelFusionPlan, PythonExceptionPayload};
use paro_external_runtime::worker::lifecycle::WorkerLifecycleState;
use paro_external_runtime::worker::pool::{WorkerAcquireDecision, WorkerPool, WorkerShardKey};
use paro_external_runtime::worker::recovery::{recover_epoch_mismatch, WorkerRecoveryAction};
use paro_external_runtime::worker::template::{
    TemplateStrategy, WorkerTemplate, WorkerTemplateRegistry,
};
use paro_routine::{
    ArtifactCapabilities, ArtifactValidationState, BackendSelectionInput, CapabilityPolicy,
    CapabilityProfile, CapabilityProfilePreset, DeclaredEnvSpec, ImportRef, MinimumIsolation,
    PackageRequirement, PythonRuntimeSelector, ResolvedEnvArtifact, RoutineNullPolicy,
    RoutineSemantics, RoutineSideEffects, RoutineStability, RowSemantics, RuntimeContract,
    TransportKind, TrustedBackendPreference,
};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct ScriptedProbe {
    responses: Mutex<Vec<PythonRuntimeProbeResult>>,
}

impl ScriptedProbe {
    fn new(responses: Vec<PythonRuntimeProbeResult>) -> Self {
        Self {
            responses: Mutex::new(responses),
        }
    }
}

impl PythonRuntimeProbe for ScriptedProbe {
    fn probe(&self) -> PythonRuntimeProbeResult {
        self.responses.lock().expect("probe responses").remove(0)
    }
}

fn sample_descriptor() -> ColumnDescriptor {
    ColumnDescriptor {
        name: "numbers".to_string(),
        logical_type: AbiLogicalType::Int64,
        encoding: ColumnEncoding::Flat,
        population_mode: ColumnPopulationMode::Eager,
        nullable: false,
        validity: None,
        layout: ColumnLayout::FixedWidth {
            values: BufferLease::host(0, 0, 32, 8),
            stride: 8,
        },
        children: Vec::new(),
    }
}

fn runtime_contract() -> RuntimeContract {
    RuntimeContract {
        sdk_version: "0.1.0".to_string(),
        worker_protocol_version: 1,
        abi_version: 1,
        supported_transports: vec![TransportKind::LocalShm, TransportKind::LocalIoUring],
    }
}

fn capability_profile() -> CapabilityProfile {
    let mut profile =
        CapabilityProfile::from_preset(CapabilityProfilePreset::TrustedSubInterpreter);
    profile.native_extension_policy = CapabilityPolicy::Allow;
    profile.subprocess_policy = CapabilityPolicy::Allow;
    profile.shared_memory_policy = CapabilityPolicy::Allow;
    profile
}

fn semantics(stability: RoutineStability, side_effects: RoutineSideEffects) -> RoutineSemantics {
    RoutineSemantics {
        stability,
        null_policy: RoutineNullPolicy::CalledOnNullInput,
        side_effects,
        row_semantics: RowSemantics::RowPreserving,
        may_block: false,
    }
}

#[test]
fn control_plane_prefers_io_uring_when_available() {
    let header = ControlHeader::new(ControlMessageKind::Submit, 7, 9, 64);
    let decoded = ControlHeader::decode(&header.encode()).expect("decode");
    assert_eq!(decoded.kind().expect("kind"), ControlMessageKind::Submit);

    let notifier = ControlNotifier::choose(
        TransportKind::LocalIoUring,
        &NotifierAvailability {
            io_uring_available: true,
            eventfd_available: true,
            shared_ring_available: true,
        },
    )
    .expect("notifier");
    assert_eq!(
        notifier.kind,
        paro_external_runtime::control::notifier::NotifierKind::IoUring
    );
}

#[test]
fn runtime_host_defaults_to_lazy_best_effort_and_not_probed() {
    let host = ExternalRuntimeHost::new();
    let status = host.status();
    assert_eq!(
        status.startup_policy,
        PythonRuntimeStartupPolicy::LazyBestEffort
    );
    assert_eq!(status.availability, PythonRuntimeAvailability::NotProbed);
    assert_eq!(
        host.startup_policy(),
        PythonRuntimeStartupPolicy::LazyBestEffort
    );
}

#[test]
fn runtime_host_probes_on_demand_for_ddl_and_execution() {
    let probe = ScriptedProbe::new(vec![PythonRuntimeProbeResult::ready()]);
    let host = ExternalRuntimeHost::new().with_probe(Arc::new(probe));
    host.ensure_ready_for_ddl()
        .expect("ddl probe should succeed");

    let status = host.status();
    assert_eq!(status.availability, PythonRuntimeAvailability::Ready);
    host.ensure_ready_for_execution()
        .expect("execution should reuse ready state");
}

#[test]
fn runtime_host_reports_binary_missing_and_degraded_recovery() {
    let probe = ScriptedProbe::new(vec![
        PythonRuntimeProbeResult::binary_missing("python3 missing from PATH"),
        PythonRuntimeProbeResult::ready(),
    ]);
    let host = ExternalRuntimeHost::new()
        .with_probe(Arc::new(probe))
        .with_reprobe_window(
            std::time::Duration::from_millis(1),
            std::time::Duration::from_millis(1),
        );

    let err = host
        .ensure_ready_for_ddl()
        .expect_err("binary missing must reject python DDL");
    assert!(err.is(codes::external_routine::PYTHON_RUNTIME_UNAVAILABLE));
    assert_eq!(
        host.availability(),
        PythonRuntimeAvailability::BinaryMissing
    );

    let degraded = host.observe_worker_failure("all workers crashed");
    assert!(matches!(
        degraded.availability,
        PythonRuntimeAvailability::Degraded { .. }
    ));
    std::thread::sleep(std::time::Duration::from_millis(2));
    let recovered = host
        .maybe_reprobe()
        .expect("degraded runtime should retry automatically");
    assert_eq!(recovered.availability, PythonRuntimeAvailability::Ready);
}

#[test]
fn arena_reclaims_query_epoch_and_reuses_release_slots() {
    let mut arena = SharedArena::new(ArenaConfig {
        namespace: ArenaNamespace {
            tenant: "tenant".to_string(),
            security_domain: "domain".to_string(),
            arena_name: "output".to_string(),
        },
        kind: ArenaKind::Output,
        backing: ArenaBacking::MemfdSealed,
        premap_bytes: 1024,
        buffer_count: 1,
        reclaim_policy: Default::default(),
    });
    let ownership = LeaseOwnership {
        owner_worker_epoch: 2,
        owner_host_epoch: 3,
        owner_query_epoch: 4,
    };
    let (allocation, _) = arena.reserve(64, ownership).expect("reserve");
    arena.begin_write(allocation.lease_id).expect("begin");
    arena
        .commit(allocation.lease_id, 10, Some(1), vec![sample_descriptor()])
        .expect("commit");
    assert_eq!(arena.reclaim_query_epoch(4), 1);

    let (reused, _) = arena.reserve(64, ownership).expect("reserve again");
    assert_eq!(allocation.offset, reused.offset);
}

#[test]
fn artifact_plane_builds_ready_cache_entry() {
    let resolver = ArtifactResolver;
    let materializer = ArtifactMaterializer;
    let validator = ArtifactValidator;
    let inputs = ResolveInputs {
        tenant_or_security_domain: "tenant/domain".to_string(),
        runtime_selector: "system".to_string(),
        env: DeclaredEnvSpec {
            runtime: PythonRuntimeSelector::Version("3.12".to_string()),
            packages: vec![PackageRequirement {
                spec: "numpy==2.0.0".to_string(),
                source: None,
            }],
            imports: vec![ImportRef {
                uri: "file:///tmp/udf.py".to_string(),
                expected_digest: None,
                expected_size: None,
            }],
        },
    };

    let contract = runtime_contract();
    let plan = resolver.resolve(&inputs, contract.clone());
    let root = materializer.materialize(&plan, "/tmp/artifacts", Some("/tmp/templates"));
    assert!(root.filesystem_root.contains(&plan.artifact_id));

    let report = validator
        .validate("module:handler", &contract, &contract)
        .expect("validate");
    let artifact = ResolvedEnvArtifact {
        id: plan.artifact_id.clone(),
        platform: "linux-x86_64".to_string(),
        python_abi: "cp312".to_string(),
        env_fingerprint: plan.packages_fingerprint.clone(),
        wheel_lock_digest: plan.packages_fingerprint.clone(),
        imports_digest: plan.imports_fingerprint.clone(),
        build_recipe_digest: plan.imports_fingerprint.clone(),
        runtime_contract: contract,
        validation: ArtifactValidationState::Ready {
            validated_handler: report.validated_handler,
            protocol_version: report.protocol_version,
        },
        filesystem_root: root.filesystem_root,
        capabilities: ArtifactCapabilities {
            supports_process_backend: true,
            supports_subinterpreter_backend: true,
            supports_subinterpreter_import_policy: true,
            supports_compiled_kernel_backend: true,
            supports_native_jit_backend: false,
            supports_hpy_universal_abi: true,
            supports_free_threaded_python: false,
            supports_arrow_c_stream_adapter: true,
            supports_arrow_py_capsule_protocol: true,
            supports_kernel_fusion: true,
            supports_restricted_wasm_backend: true,
            supports_mediated_sandbox_backend: true,
            supports_microvm_backend: true,
            supports_remote_transport: false,
            validated_native_extensions: true,
            requires_gpu: false,
        },
    };
    let key = ArtifactCacheKey {
        tenant_or_security_domain: "tenant/domain".to_string(),
        artifact_id: artifact.id.clone(),
    };
    let mut cache = ArtifactCache::default();
    cache.insert(key.clone(), artifact, 1024, 100);
    cache.mark_draining(&key);
    assert!(cache.contains(&key));
    let swept = cache.sweep(
        &ArtifactGcPolicy {
            min_unused_age_ms: 10,
            ..ArtifactGcPolicy::default()
        },
        1_000,
    );
    assert_eq!(swept, vec![key]);
}

#[test]
fn backend_selector_and_worker_pool_respect_generation_boundaries() {
    let input = BackendSelectionInput {
        capability_profile: capability_profile(),
        artifact_capabilities: ArtifactCapabilities {
            supports_process_backend: true,
            supports_subinterpreter_backend: true,
            supports_subinterpreter_import_policy: true,
            supports_compiled_kernel_backend: true,
            supports_native_jit_backend: false,
            supports_hpy_universal_abi: true,
            supports_free_threaded_python: false,
            supports_arrow_c_stream_adapter: true,
            supports_arrow_py_capsule_protocol: true,
            supports_kernel_fusion: true,
            supports_restricted_wasm_backend: true,
            supports_mediated_sandbox_backend: true,
            supports_microvm_backend: true,
            supports_remote_transport: false,
            validated_native_extensions: true,
            requires_gpu: false,
        },
        runtime_contract: runtime_contract(),
        minimum_isolation: MinimumIsolation::Trusted,
        trusted_backend_preference: TrustedBackendPreference::CompiledKernel,
    };
    let selector = BackendSelector;
    let selection = selector
        .select(
            input,
            &BackendAvailability {
                local_process_ready: true,
                sandbox_ready: true,
                restricted_wasm_ready: true,
                mediated_sandbox_ready: true,
                microvm_ready: true,
                subinterpreter_ready: true,
                compiled_kernel_ready: true,
                remote_ready: false,
                local_io_uring_ready: true,
            },
        )
        .expect("selection");
    assert_eq!(selection.backend, BackendKind::CompiledKernel);
    assert_eq!(selection.transport, TransportKind::LocalIoUring);

    let mut pool = WorkerPool::default();
    let old_shard = WorkerShardKey {
        tenant_or_security_domain: "tenant/domain".to_string(),
        env_artifact_id: "artifact-a".to_string(),
        routine_generation: 1,
        backend_kind: "process".to_string(),
        runtime_contract: "wp1-abi1".to_string(),
    };
    let new_shard = WorkerShardKey {
        routine_generation: 2,
        ..old_shard.clone()
    };
    let worker_id = pool.register_worker(old_shard.clone(), WorkerLifecycleState::Idle, 10);
    assert_eq!(
        pool.acquire(&old_shard, 20),
        WorkerAcquireDecision::Reused(worker_id)
    );
    pool.release(worker_id, 21).expect("release");
    pool.start_generation_drain(&new_shard);
    assert_eq!(pool.state(worker_id), Some(WorkerLifecycleState::Draining));
    assert_eq!(pool.hard_retire_contract("wp1-abi1"), 1);
    assert_eq!(
        pool.state(worker_id),
        Some(WorkerLifecycleState::HardRetired)
    );
}

#[test]
fn backend_selector_defaults_to_process_when_trusted_lane_is_not_requested() {
    let input = BackendSelectionInput {
        capability_profile: CapabilityProfile::process_default(),
        artifact_capabilities: ArtifactCapabilities {
            supports_process_backend: true,
            supports_subinterpreter_backend: true,
            supports_subinterpreter_import_policy: true,
            supports_compiled_kernel_backend: true,
            supports_native_jit_backend: false,
            supports_hpy_universal_abi: true,
            supports_free_threaded_python: false,
            supports_arrow_c_stream_adapter: true,
            supports_arrow_py_capsule_protocol: true,
            supports_kernel_fusion: true,
            supports_restricted_wasm_backend: true,
            supports_mediated_sandbox_backend: true,
            supports_microvm_backend: true,
            supports_remote_transport: false,
            validated_native_extensions: true,
            requires_gpu: false,
        },
        runtime_contract: runtime_contract(),
        minimum_isolation: MinimumIsolation::Process,
        trusted_backend_preference: TrustedBackendPreference::Automatic,
    };

    let selection = BackendSelector
        .select(
            input,
            &BackendAvailability {
                local_process_ready: true,
                sandbox_ready: true,
                restricted_wasm_ready: true,
                mediated_sandbox_ready: true,
                microvm_ready: true,
                subinterpreter_ready: true,
                compiled_kernel_ready: true,
                remote_ready: false,
                local_io_uring_ready: false,
            },
        )
        .expect("selection");

    assert_eq!(selection.backend, BackendKind::Process);
}

#[test]
fn backend_selector_requires_sandbox_when_profile_demands_it() {
    let input = BackendSelectionInput {
        capability_profile: CapabilityProfile::from_preset(CapabilityProfilePreset::SandboxProcess),
        artifact_capabilities: ArtifactCapabilities {
            supports_process_backend: true,
            supports_subinterpreter_backend: true,
            supports_subinterpreter_import_policy: true,
            supports_compiled_kernel_backend: true,
            supports_native_jit_backend: false,
            supports_hpy_universal_abi: true,
            supports_free_threaded_python: false,
            supports_arrow_c_stream_adapter: true,
            supports_arrow_py_capsule_protocol: true,
            supports_kernel_fusion: true,
            supports_restricted_wasm_backend: true,
            supports_mediated_sandbox_backend: true,
            supports_microvm_backend: true,
            supports_remote_transport: false,
            validated_native_extensions: true,
            requires_gpu: false,
        },
        runtime_contract: runtime_contract(),
        minimum_isolation: MinimumIsolation::Sandboxed,
        trusted_backend_preference: TrustedBackendPreference::Automatic,
    };

    let selection = BackendSelector
        .select(
            input,
            &BackendAvailability {
                local_process_ready: true,
                sandbox_ready: true,
                restricted_wasm_ready: true,
                mediated_sandbox_ready: true,
                microvm_ready: true,
                subinterpreter_ready: true,
                compiled_kernel_ready: true,
                remote_ready: false,
                local_io_uring_ready: false,
            },
        )
        .expect("selection");

    assert_eq!(selection.backend, BackendKind::Sandbox);
    assert_eq!(
        selection.sandbox_runtime,
        Some(SandboxRuntimeKind::NamespaceProcess)
    );
}

#[test]
fn sandbox_selector_prefers_restricted_wasm_for_restricted_profiles() {
    let profile = CapabilityProfile::from_preset(CapabilityProfilePreset::SandboxWasm);
    let input = BackendSelectionInput {
        capability_profile: profile.clone(),
        artifact_capabilities: ArtifactCapabilities {
            supports_process_backend: true,
            supports_subinterpreter_backend: true,
            supports_subinterpreter_import_policy: true,
            supports_compiled_kernel_backend: false,
            supports_native_jit_backend: false,
            supports_hpy_universal_abi: false,
            supports_free_threaded_python: false,
            supports_arrow_c_stream_adapter: true,
            supports_arrow_py_capsule_protocol: true,
            supports_kernel_fusion: true,
            supports_restricted_wasm_backend: true,
            supports_mediated_sandbox_backend: true,
            supports_microvm_backend: true,
            supports_remote_transport: false,
            validated_native_extensions: false,
            requires_gpu: false,
        },
        runtime_contract: runtime_contract(),
        minimum_isolation: profile.minimum_isolation(),
        trusted_backend_preference: profile.trusted_backend_preference(),
    };

    let selection = BackendSelector
        .select(
            input,
            &BackendAvailability {
                local_process_ready: true,
                sandbox_ready: true,
                restricted_wasm_ready: true,
                mediated_sandbox_ready: true,
                microvm_ready: true,
                subinterpreter_ready: true,
                compiled_kernel_ready: true,
                remote_ready: false,
                local_io_uring_ready: false,
            },
        )
        .expect("selection");

    assert_eq!(selection.backend, BackendKind::Sandbox);
    assert_eq!(
        selection.sandbox_runtime,
        Some(SandboxRuntimeKind::RestrictedWasm)
    );
}

#[test]
fn sandbox_selector_does_not_require_namespace_process_for_restricted_wasm() {
    let profile = CapabilityProfile::from_preset(CapabilityProfilePreset::SandboxWasm);
    let input = BackendSelectionInput {
        capability_profile: profile.clone(),
        artifact_capabilities: ArtifactCapabilities {
            supports_process_backend: true,
            supports_subinterpreter_backend: true,
            supports_subinterpreter_import_policy: true,
            supports_compiled_kernel_backend: false,
            supports_native_jit_backend: false,
            supports_hpy_universal_abi: false,
            supports_free_threaded_python: false,
            supports_arrow_c_stream_adapter: true,
            supports_arrow_py_capsule_protocol: true,
            supports_kernel_fusion: true,
            supports_restricted_wasm_backend: true,
            supports_mediated_sandbox_backend: true,
            supports_microvm_backend: true,
            supports_remote_transport: false,
            validated_native_extensions: false,
            requires_gpu: false,
        },
        runtime_contract: runtime_contract(),
        minimum_isolation: profile.minimum_isolation(),
        trusted_backend_preference: profile.trusted_backend_preference(),
    };

    let selection = BackendSelector
        .select(
            input,
            &BackendAvailability {
                local_process_ready: true,
                sandbox_ready: false,
                restricted_wasm_ready: true,
                mediated_sandbox_ready: true,
                microvm_ready: true,
                subinterpreter_ready: true,
                compiled_kernel_ready: true,
                remote_ready: false,
                local_io_uring_ready: false,
            },
        )
        .expect("selection");

    assert_eq!(selection.backend, BackendKind::Sandbox);
    assert_eq!(
        selection.sandbox_runtime,
        Some(SandboxRuntimeKind::RestrictedWasm)
    );
}

#[test]
fn compiled_jit_profile_selects_zero_dependency_native_jit_lane() {
    let profile = CapabilityProfile::from_preset(CapabilityProfilePreset::CompiledJit);
    let input = BackendSelectionInput {
        capability_profile: profile.clone(),
        artifact_capabilities: ArtifactCapabilities {
            supports_process_backend: true,
            supports_subinterpreter_backend: true,
            supports_subinterpreter_import_policy: true,
            supports_compiled_kernel_backend: true,
            supports_native_jit_backend: true,
            supports_hpy_universal_abi: false,
            supports_free_threaded_python: false,
            supports_arrow_c_stream_adapter: true,
            supports_arrow_py_capsule_protocol: true,
            supports_kernel_fusion: true,
            supports_restricted_wasm_backend: false,
            supports_mediated_sandbox_backend: false,
            supports_microvm_backend: false,
            supports_remote_transport: false,
            validated_native_extensions: false,
            requires_gpu: false,
        },
        runtime_contract: runtime_contract(),
        minimum_isolation: profile.minimum_isolation(),
        trusted_backend_preference: profile.trusted_backend_preference(),
    };

    let selection = BackendSelector
        .select(
            input,
            &BackendAvailability {
                local_process_ready: true,
                sandbox_ready: false,
                restricted_wasm_ready: false,
                mediated_sandbox_ready: false,
                microvm_ready: false,
                subinterpreter_ready: true,
                compiled_kernel_ready: true,
                remote_ready: false,
                local_io_uring_ready: true,
            },
        )
        .expect("selection");

    assert_eq!(selection.backend, BackendKind::CompiledKernel);
    assert_eq!(selection.transport, TransportKind::LocalIoUring);
}

#[test]
fn kernel_fusion_plan_requires_one_backend_family() {
    let plan = KernelFusionPlan::row_preserving_chain(
        "module",
        ["normalize", "score"],
        BackendKind::Process,
    );
    assert!(plan.is_chain_eligible());

    let mut mixed = plan.clone();
    mixed.stages[1].backend = BackendKind::CompiledKernel;
    assert!(!mixed.is_chain_eligible());
}

#[test]
fn permit_pool_and_autotuning_respect_bootstrap_window() {
    let mut permits = PermitPool::new(PermitBudget {
        global_external_permits: 2,
        per_shard_permits: 1,
        per_query_permits: 1,
    });
    let ticket = permits
        .try_acquire("shard-a", 1)
        .expect("first permit available");
    assert!(permits.try_acquire("shard-a", 1).is_none());
    permits.release(ticket);
    assert!(permits.try_acquire("shard-a", 1).is_some());

    let mut store = InMemoryProfileStore::default();
    let key = RoutinePerfProfileKey {
        tenant_or_security_domain: "tenant/domain".to_string(),
        env_artifact_id: "artifact-a".to_string(),
        routine_generation: 2,
        backend_kind: "process".to_string(),
        runtime_contract: "wp1-abi1".to_string(),
        shape_class: "small-fixed".to_string(),
    };
    let profile = store.get_or_default(&key);
    let autotuner = Autotuner;
    let policy = paro_external_runtime::metrics::autotuning::AutotuningPolicy::default();
    for _ in 0..policy.bootstrap.total_batches() {
        autotuner.observe(
            profile,
            PerfObservation {
                target_batch_bytes: 128 * 1024,
                queue_wait_us: 80,
                kernel_time_us: 100,
                output_expansion_factor: 1.0,
                warm_hit: true,
            },
            &policy,
        );
    }
    assert!(profile.preferred_target_batch_bytes.is_none());

    autotuner.observe(
        profile,
        PerfObservation {
            target_batch_bytes: 256 * 1024,
            queue_wait_us: 40,
            kernel_time_us: 60,
            output_expansion_factor: 1.2,
            warm_hit: true,
        },
        &policy,
    );
    assert_eq!(profile.preferred_target_batch_bytes, Some(256 * 1024));

    let batch_rows = ExternalDispatchPolicy::default().suggest_batch_rows(128, 0);
    assert!(batch_rows >= 1);
}

#[test]
fn process_isolation_enforcer_builds_process_launch_spec() {
    let limits = ResourceLimits {
        max_memory_bytes: Some(256 * 1024 * 1024),
        max_cpu_time_ms: Some(5_000),
        max_wall_time_ms: Some(10_000),
        max_tmp_bytes: Some(32 * 1024 * 1024),
        network_policy: NetworkPolicy::allowlist([NetworkAllowRule {
            host_pattern: "pypi.internal".to_string(),
            port: Some(443),
        }]),
        filesystem_policy:
            paro_external_runtime::isolation::resource_limits::FilesystemPolicy::sandbox_default(
                "/tmp/artifacts",
            ),
    };
    let enforcer = ProcessIsolationEnforcer::new(ProcessPlatformCapabilities::linux_default());
    let launch = enforcer
        .enforce_profile(&limits, &capability_profile())
        .expect("launch spec");

    assert_eq!(launch.rlimits.memory_bytes, Some(256 * 1024 * 1024));
    assert_eq!(launch.cgroup.cpu_max_ms, Some(5_000));
    assert_eq!(launch.seccomp.default_action, SeccompDefaultAction::Trap);
    assert!(launch
        .seccomp
        .denied_syscalls
        .contains(&"socket".to_string()));
    assert_eq!(
        launch.env.get("PARO_EXTERNAL_ROUTINE_ARTIFACT_ROOT"),
        Some(&"/tmp/artifacts".to_string())
    );

    let snapshot = enforcer.snapshot();
    assert_eq!(snapshot.launch_spec, Some(launch));
}

#[test]
fn process_isolation_enforcer_rejects_invalid_network_and_capability_profiles() {
    let enforcer = ProcessIsolationEnforcer::default();
    let invalid_limits = ResourceLimits {
        max_memory_bytes: Some(64),
        max_cpu_time_ms: None,
        max_wall_time_ms: None,
        max_tmp_bytes: None,
        network_policy: NetworkPolicy::allowlist(std::iter::empty::<NetworkAllowRule>()),
        filesystem_policy:
            paro_external_runtime::isolation::resource_limits::FilesystemPolicy::sandbox_default(
                "/tmp/artifacts",
            ),
    };
    let err = enforcer
        .build_launch_spec(&invalid_limits, &capability_profile())
        .expect_err("missing allowlist rules must fail");
    assert!(err.is(codes::data::INVALID_PARAMETER_VALUE));

    let mut disallowed = capability_profile();
    disallowed.shared_memory_policy = CapabilityPolicy::Deny;
    let err = enforcer
        .build_launch_spec(
            &ResourceLimits::sandbox_default("/tmp/artifacts"),
            &disallowed,
        )
        .expect_err("process backend must require shared memory");
    assert!(err.is(codes::external_routine::SANDBOX_VIOLATION));
}

#[test]
fn retry_policy_only_retries_dispatch_failures_before_start() {
    let lifecycle = paro_external_runtime::control::state_machine::SubmissionLifecycle::default();
    let policy = RetryPolicy::default();

    let dispatch = policy.decide(
        &lifecycle,
        &semantics(RoutineStability::Stable, RoutineSideEffects::None),
        RetryFailureKind::WorkerCrash,
    );
    assert!(dispatch.transparent);

    let volatile = policy.decide(
        &lifecycle,
        &semantics(
            RoutineStability::Volatile,
            RoutineSideEffects::HasSideEffects,
        ),
        RetryFailureKind::WorkerCrash,
    );
    assert!(!volatile.transparent);

    let python = policy.decide(
        &lifecycle,
        &semantics(RoutineStability::Stable, RoutineSideEffects::None),
        RetryFailureKind::PythonException,
    );
    assert!(!python.transparent);
}

#[test]
fn cancel_escalation_follows_cancel_interrupt_kill_retire_path() {
    let policy = CancelEscalationPolicy {
        python_interrupt_grace_ms: 10,
        force_terminate_grace_ms: 20,
    };
    let mut escalation = CancelEscalation::new(9, 100);
    assert_eq!(
        escalation.next_action(100, &policy),
        CancelAction::SendWorkerCancel
    );

    escalation
        .advance_to(CancelStage::WorkerMarkedCancelled, 105)
        .expect("worker marked cancelled");
    assert_eq!(escalation.next_action(110, &policy), CancelAction::None);
    assert_eq!(
        escalation.next_action(116, &policy),
        CancelAction::InterruptPythonSafepoint
    );

    escalation
        .advance_to(CancelStage::PythonInterrupted, 117)
        .expect("python interrupted");
    assert_eq!(
        escalation.next_action(138, &policy),
        CancelAction::ForceTerminateWorker
    );

    escalation
        .advance_to(CancelStage::ForceTerminate, 139)
        .expect("force terminate");
    assert_eq!(
        escalation.next_action(139, &policy),
        CancelAction::RetireWorker
    );
}

#[test]
fn external_routine_errors_map_to_distinct_sqlstates() {
    let python = ExternalRoutineErrorKind::PythonException(PythonExceptionPayload {
        exception_type: "ValueError".to_string(),
        message: "boom".to_string(),
        formatted_traceback: "Traceback...".to_string(),
        module: "sample".to_string(),
        handler: "explode".to_string(),
        batch_id: 7,
        truncated: false,
    })
    .to_paro_error();
    assert!(python.is(codes::external_routine::PYTHON_EXCEPTION));

    let contract = ExternalRoutineErrorKind::HostContractViolation {
        message: "shape mismatch".to_string(),
    }
    .to_paro_error();
    assert!(contract.is(codes::external_routine::CONTRACT_VIOLATION));

    let protocol = ExternalRoutineErrorKind::ProtocolMismatch {
        message: "worker protocol 2 != host protocol 1".to_string(),
    }
    .to_paro_error();
    assert!(protocol.is(codes::external_routine::PROTOCOL_MISMATCH));

    let sandbox = ExternalRoutineErrorKind::SandboxViolation {
        message: "network access denied".to_string(),
    }
    .to_paro_error();
    assert!(sandbox.is(codes::external_routine::SANDBOX_VIOLATION));

    let timeout = ExternalRoutineErrorKind::StatementTimeout.to_paro_error();
    assert!(timeout.is(codes::operator::STATEMENT_TIMEOUT));
}

#[test]
fn epoch_mismatch_recovery_reclaims_leases_and_hard_retires_worker() {
    let mut arena = SharedArena::new(ArenaConfig {
        namespace: ArenaNamespace {
            tenant: "tenant".to_string(),
            security_domain: "domain".to_string(),
            arena_name: "output".to_string(),
        },
        kind: ArenaKind::Output,
        backing: ArenaBacking::MemfdSealed,
        premap_bytes: 1024,
        buffer_count: 1,
        reclaim_policy: Default::default(),
    });
    let ownership = LeaseOwnership {
        owner_worker_epoch: 2,
        owner_host_epoch: 7,
        owner_query_epoch: 3,
    };
    let (allocation, _) = arena.reserve(64, ownership).expect("reserve");
    arena.begin_write(allocation.lease_id).expect("begin");
    arena
        .commit(allocation.lease_id, 10, Some(1), vec![sample_descriptor()])
        .expect("commit");

    let mut pool = WorkerPool::default();
    let shard = WorkerShardKey {
        tenant_or_security_domain: "tenant/domain".to_string(),
        env_artifact_id: "artifact-a".to_string(),
        routine_generation: 1,
        backend_kind: "process".to_string(),
        runtime_contract: "wp1-abi1".to_string(),
    };
    let worker_id = pool.register_worker(shard, WorkerLifecycleState::Idle, 10);

    let recovery =
        recover_epoch_mismatch(&mut arena, &mut pool, worker_id, 7, 3).expect("recovery");
    assert_eq!(
        recovery.action,
        WorkerRecoveryAction::HardRetireAndReHandshake
    );
    assert_eq!(recovery.reclaimed_query_leases, 1);
    assert_eq!(
        pool.state(worker_id),
        Some(WorkerLifecycleState::HardRetired)
    );
}

#[test]
fn template_registry_keeps_security_domains_isolated() {
    let mut registry = WorkerTemplateRegistry::default();
    let primary = WorkerShardKey {
        tenant_or_security_domain: "tenant/domain-a".to_string(),
        env_artifact_id: "artifact-a".to_string(),
        routine_generation: 3,
        backend_kind: "process".to_string(),
        runtime_contract: "wp1-abi1".to_string(),
    };
    let other_domain = WorkerShardKey {
        tenant_or_security_domain: "tenant/domain-b".to_string(),
        ..primary.clone()
    };
    registry.insert(WorkerTemplate {
        shard_key: primary.clone(),
        strategy: TemplateStrategy::ForkTemplate,
        template_epoch: 1,
        ready: true,
    });

    assert!(registry.contains(&primary));
    assert!(!registry.contains(&other_domain));
    assert!(registry.get(&other_domain).is_none());
}
