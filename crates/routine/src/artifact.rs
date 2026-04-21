// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::capability::CapabilityProfile;
use serde::{Deserialize, Serialize};

pub type ResolvedEnvArtifactId = String;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedEnvArtifact {
    pub id: ResolvedEnvArtifactId,
    pub platform: String,
    pub python_abi: String,
    pub env_fingerprint: String,
    pub wheel_lock_digest: String,
    pub imports_digest: String,
    pub build_recipe_digest: String,
    pub runtime_contract: RuntimeContract,
    pub validation: ArtifactValidationState,
    pub filesystem_root: String,
    pub capabilities: ArtifactCapabilities,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeContract {
    pub sdk_version: String,
    pub worker_protocol_version: u16,
    pub abi_version: u16,
    pub supported_transports: Vec<TransportKind>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactValidationState {
    Pending,
    Ready {
        validated_handler: String,
        protocol_version: u16,
    },
    Failed {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactCapabilities {
    pub supports_process_backend: bool,
    pub supports_subinterpreter_backend: bool,
    pub supports_subinterpreter_import_policy: bool,
    pub supports_compiled_kernel_backend: bool,
    pub supports_native_jit_backend: bool,
    pub supports_hpy_universal_abi: bool,
    pub supports_free_threaded_python: bool,
    pub supports_arrow_c_stream_adapter: bool,
    pub supports_arrow_py_capsule_protocol: bool,
    pub supports_kernel_fusion: bool,
    pub supports_restricted_wasm_backend: bool,
    pub supports_mediated_sandbox_backend: bool,
    pub supports_microvm_backend: bool,
    pub supports_remote_transport: bool,
    pub validated_native_extensions: bool,
    pub requires_gpu: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MinimumIsolation {
    Trusted,
    Process,
    Sandboxed,
    Remote,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrustedBackendPreference {
    Automatic,
    SubInterpreter,
    CompiledKernel,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransportKind {
    LocalShm,
    LocalIoUring,
    Remote,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendSelectionInput {
    pub capability_profile: CapabilityProfile,
    pub artifact_capabilities: ArtifactCapabilities,
    pub runtime_contract: RuntimeContract,
    pub minimum_isolation: MinimumIsolation,
    pub trusted_backend_preference: TrustedBackendPreference,
}
