// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use crate::backend::sandbox::{
    SandboxAvailability, SandboxRuntimeKind, SandboxedPythonProcessBackend,
};
use paro_routine::{
    BackendSelectionInput, CapabilityPolicy, MinimumIsolation, TransportKind,
    TrustedBackendPreference,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackendKind {
    Process,
    SubInterpreter,
    Sandbox,
    CompiledKernel,
    Remote,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationLevel {
    Trusted,
    Process,
    Sandboxed,
    Remote,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendSelection {
    pub backend: BackendKind,
    pub isolation: IsolationLevel,
    pub transport: TransportKind,
    pub input: BackendSelectionInput,
    pub sandbox_runtime: Option<SandboxRuntimeKind>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendSelectionError {
    pub minimum_isolation: MinimumIsolation,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendAvailability {
    pub local_process_ready: bool,
    pub sandbox_ready: bool,
    pub restricted_wasm_ready: bool,
    pub mediated_sandbox_ready: bool,
    pub microvm_ready: bool,
    pub subinterpreter_ready: bool,
    pub compiled_kernel_ready: bool,
    pub remote_ready: bool,
    pub local_io_uring_ready: bool,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BackendSelector;

impl BackendSelector {
    pub fn select(
        &self,
        input: BackendSelectionInput,
        availability: &BackendAvailability,
    ) -> Result<BackendSelection, BackendSelectionError> {
        let remote = remote_candidate(&input, availability);
        let sandbox = sandbox_candidate(&input, availability);
        let process = process_candidate(&input, availability);
        let compiled = compiled_candidate(&input, availability);
        let subinterpreter = subinterpreter_candidate(&input, availability);

        match input.minimum_isolation {
            MinimumIsolation::Remote => {
                return remote.ok_or_else(|| selection_error(&input, availability))
            }
            MinimumIsolation::Sandboxed => {
                return sandbox
                    .or(remote)
                    .ok_or_else(|| selection_error(&input, availability))
            }
            MinimumIsolation::Process => {
                return process
                    .or(sandbox)
                    .or(remote)
                    .ok_or_else(|| selection_error(&input, availability))
            }
            MinimumIsolation::Trusted => {}
        }

        match input.trusted_backend_preference {
            TrustedBackendPreference::CompiledKernel => compiled
                .or(subinterpreter)
                .or(process)
                .or(sandbox)
                .or(remote),
            TrustedBackendPreference::SubInterpreter => subinterpreter
                .or(compiled)
                .or(process)
                .or(sandbox)
                .or(remote),
            TrustedBackendPreference::Automatic => compiled
                .or(subinterpreter)
                .or(process)
                .or(sandbox)
                .or(remote),
        }
        .ok_or_else(|| selection_error(&input, availability))
    }
}

fn remote_candidate(
    input: &BackendSelectionInput,
    availability: &BackendAvailability,
) -> Option<BackendSelection> {
    let remote_supported = input.artifact_capabilities.supports_remote_transport
        && input
            .runtime_contract
            .supported_transports
            .iter()
            .any(|transport| matches!(transport, TransportKind::Remote));

    (remote_supported && availability.remote_ready).then(|| BackendSelection {
        backend: BackendKind::Remote,
        isolation: IsolationLevel::Remote,
        transport: TransportKind::Remote,
        input: input.clone(),
        sandbox_runtime: None,
    })
}

fn compiled_candidate(
    input: &BackendSelectionInput,
    availability: &BackendAvailability,
) -> Option<BackendSelection> {
    let supports_native_jit = input.artifact_capabilities.supports_native_jit_backend
        && input.capability_profile.allows_native_jit_compiled_kernel();
    let supports_native_extension_lane =
        input.capability_profile.allows_compiled_native_extensions();
    (input.artifact_capabilities.supports_compiled_kernel_backend
        && availability.compiled_kernel_ready
        && input.capability_profile.allows_trusted_fast_lane()
        && (supports_native_jit || supports_native_extension_lane))
        .then(|| BackendSelection {
            backend: BackendKind::CompiledKernel,
            isolation: IsolationLevel::Trusted,
            transport: choose_local_transport(input, availability),
            input: input.clone(),
            sandbox_runtime: None,
        })
}

fn subinterpreter_candidate(
    input: &BackendSelectionInput,
    availability: &BackendAvailability,
) -> Option<BackendSelection> {
    (input.artifact_capabilities.supports_subinterpreter_backend
        && availability.subinterpreter_ready
        && input.capability_profile.allows_trusted_fast_lane()
        && input.capability_profile.thread_policy == CapabilityPolicy::Allow)
        .then(|| BackendSelection {
            backend: BackendKind::SubInterpreter,
            isolation: IsolationLevel::Trusted,
            transport: choose_local_transport(input, availability),
            input: input.clone(),
            sandbox_runtime: None,
        })
}

fn process_candidate(
    input: &BackendSelectionInput,
    availability: &BackendAvailability,
) -> Option<BackendSelection> {
    (availability.local_process_ready && input.artifact_capabilities.supports_process_backend).then(
        || BackendSelection {
            backend: BackendKind::Process,
            isolation: IsolationLevel::Process,
            transport: choose_local_transport(input, availability),
            input: input.clone(),
            sandbox_runtime: None,
        },
    )
}

fn sandbox_candidate(
    input: &BackendSelectionInput,
    availability: &BackendAvailability,
) -> Option<BackendSelection> {
    let sandbox_runtime = SandboxedPythonProcessBackend::select_target(
        &input.capability_profile,
        &input.artifact_capabilities,
        &SandboxAvailability {
            namespace_ready: availability.sandbox_ready,
            restricted_wasm_ready: availability.restricted_wasm_ready,
            mediated_ready: availability.mediated_sandbox_ready,
            microvm_snapshot_ready: availability.microvm_ready,
        },
    )?;
    Some(BackendSelection {
        backend: BackendKind::Sandbox,
        isolation: IsolationLevel::Sandboxed,
        transport: TransportKind::LocalShm,
        input: input.clone(),
        sandbox_runtime: Some(sandbox_runtime.runtime),
    })
}

fn selection_error(
    input: &BackendSelectionInput,
    availability: &BackendAvailability,
) -> BackendSelectionError {
    BackendSelectionError {
        minimum_isolation: input.minimum_isolation,
        detail: format!(
            "no backend satisfies minimum isolation {:?} (process_ready={}, namespace_sandbox_ready={}, restricted_wasm_ready={}, mediated_sandbox_ready={}, microvm_ready={}, subinterpreter_ready={}, compiled_ready={}, remote_ready={})",
            input.minimum_isolation,
            availability.local_process_ready,
            availability.sandbox_ready,
            availability.restricted_wasm_ready,
            availability.mediated_sandbox_ready,
            availability.microvm_ready,
            availability.subinterpreter_ready,
            availability.compiled_kernel_ready,
            availability.remote_ready
        ),
    }
}

fn choose_local_transport(
    input: &BackendSelectionInput,
    availability: &BackendAvailability,
) -> TransportKind {
    if availability.local_io_uring_ready
        && input
            .runtime_contract
            .supported_transports
            .iter()
            .any(|transport| matches!(transport, TransportKind::LocalIoUring))
    {
        TransportKind::LocalIoUring
    } else {
        TransportKind::LocalShm
    }
}
