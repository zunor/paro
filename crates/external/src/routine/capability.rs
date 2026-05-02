// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use super::artifact::{MinimumIsolation, TrustedBackendPreference};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CapabilityPolicy {
    #[default]
    Deny,
    Allow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SandboxBackendPreference {
    #[default]
    Auto,
    NamespaceProcess,
    RestrictedWasm,
    Mediated,
    MicroVm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RestrictedSdkProfile {
    #[default]
    Disabled,
    Columnar,
    NumericOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SandboxProfile {
    pub preferred_backend: SandboxBackendPreference,
    pub restricted_sdk: RestrictedSdkProfile,
    pub allow_cold_boot: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SubInterpreterImportPolicy {
    #[default]
    InheritWorkerPaths,
    ArtifactAndStdlibOnly,
    AllowList,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SubInterpreterExtensionPolicy {
    #[default]
    AllowValidatedOnly,
    AllowAll,
    DenyAll,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SubInterpreterGilPolicy {
    #[default]
    Shared,
    Dedicated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SubInterpreterPolicy {
    pub import_policy: SubInterpreterImportPolicy,
    pub allowed_modules: Vec<String>,
    pub extension_modules: SubInterpreterExtensionPolicy,
    pub gil: SubInterpreterGilPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum NativeJitPolicy {
    Disabled,
    #[default]
    Observe,
    Preferred,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CompiledKernelPolicy {
    pub native_jit: NativeJitPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapabilityProfilePreset {
    TrustedSubInterpreter,
    CompiledKernel,
    CompiledJit,
    SandboxProcess,
    SandboxWasm,
    SandboxMediated,
    SandboxMicroVm,
    Remote,
}

impl CapabilityProfilePreset {
    pub const fn label(self) -> &'static str {
        match self {
            CapabilityProfilePreset::TrustedSubInterpreter => "trusted_subinterpreter",
            CapabilityProfilePreset::CompiledKernel => "compiled_kernel",
            CapabilityProfilePreset::CompiledJit => "compiled_jit",
            CapabilityProfilePreset::SandboxProcess => "sandbox_process",
            CapabilityProfilePreset::SandboxWasm => "sandbox_wasm",
            CapabilityProfilePreset::SandboxMediated => "sandbox_mediated",
            CapabilityProfilePreset::SandboxMicroVm => "sandbox_microvm",
            CapabilityProfilePreset::Remote => "remote",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "trusted_subinterpreter" => Some(CapabilityProfilePreset::TrustedSubInterpreter),
            "compiled_kernel" => Some(CapabilityProfilePreset::CompiledKernel),
            "compiled_jit" => Some(CapabilityProfilePreset::CompiledJit),
            "sandbox_process" => Some(CapabilityProfilePreset::SandboxProcess),
            "sandbox_wasm" => Some(CapabilityProfilePreset::SandboxWasm),
            "sandbox_mediated" => Some(CapabilityProfilePreset::SandboxMediated),
            "sandbox_microvm" => Some(CapabilityProfilePreset::SandboxMicroVm),
            "remote" => Some(CapabilityProfilePreset::Remote),
            _ => None,
        }
    }

    pub const fn supported_names() -> &'static [&'static str] {
        &[
            "trusted_subinterpreter",
            "compiled_kernel",
            "compiled_jit",
            "sandbox_process",
            "sandbox_wasm",
            "sandbox_mediated",
            "sandbox_microvm",
            "remote",
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityProfile {
    pub profile_name: Option<String>,
    pub native_extension_policy: CapabilityPolicy,
    pub subprocess_policy: CapabilityPolicy,
    pub thread_policy: CapabilityPolicy,
    pub outbound_ipc_policy: CapabilityPolicy,
    pub shared_memory_policy: CapabilityPolicy,
    pub gpu_policy: CapabilityPolicy,
    pub minimum_isolation: MinimumIsolation,
    pub trusted_backend_preference: TrustedBackendPreference,
    pub sandbox: SandboxProfile,
    pub subinterpreter: SubInterpreterPolicy,
    pub compiled_kernel: CompiledKernelPolicy,
}

impl CapabilityProfile {
    pub fn process_default() -> Self {
        Self {
            profile_name: None,
            native_extension_policy: CapabilityPolicy::Deny,
            subprocess_policy: CapabilityPolicy::Deny,
            thread_policy: CapabilityPolicy::Deny,
            outbound_ipc_policy: CapabilityPolicy::Deny,
            shared_memory_policy: CapabilityPolicy::Deny,
            gpu_policy: CapabilityPolicy::Deny,
            minimum_isolation: MinimumIsolation::Process,
            trusted_backend_preference: TrustedBackendPreference::Automatic,
            sandbox: SandboxProfile::default(),
            subinterpreter: SubInterpreterPolicy::default(),
            compiled_kernel: CompiledKernelPolicy::default(),
        }
    }

    pub fn from_preset(preset: CapabilityProfilePreset) -> Self {
        let mut profile = Self {
            profile_name: Some(preset.label().to_string()),
            ..Self::process_default()
        };

        match preset {
            CapabilityProfilePreset::TrustedSubInterpreter => {
                profile.minimum_isolation = MinimumIsolation::Trusted;
                profile.trusted_backend_preference = TrustedBackendPreference::SubInterpreter;
                profile.thread_policy = CapabilityPolicy::Allow;
                profile.subinterpreter.import_policy =
                    SubInterpreterImportPolicy::ArtifactAndStdlibOnly;
            }
            CapabilityProfilePreset::CompiledKernel => {
                profile.minimum_isolation = MinimumIsolation::Trusted;
                profile.trusted_backend_preference = TrustedBackendPreference::CompiledKernel;
                profile.native_extension_policy = CapabilityPolicy::Allow;
                profile.thread_policy = CapabilityPolicy::Allow;
            }
            CapabilityProfilePreset::CompiledJit => {
                profile.minimum_isolation = MinimumIsolation::Trusted;
                profile.trusted_backend_preference = TrustedBackendPreference::CompiledKernel;
                profile.thread_policy = CapabilityPolicy::Allow;
                profile.compiled_kernel.native_jit = NativeJitPolicy::Preferred;
            }
            CapabilityProfilePreset::SandboxProcess => {
                profile.minimum_isolation = MinimumIsolation::Sandboxed;
                profile.sandbox.preferred_backend = SandboxBackendPreference::NamespaceProcess;
            }
            CapabilityProfilePreset::SandboxWasm => {
                profile.minimum_isolation = MinimumIsolation::Sandboxed;
                profile.sandbox.preferred_backend = SandboxBackendPreference::RestrictedWasm;
                profile.sandbox.restricted_sdk = RestrictedSdkProfile::Columnar;
            }
            CapabilityProfilePreset::SandboxMediated => {
                profile.minimum_isolation = MinimumIsolation::Sandboxed;
                profile.sandbox.preferred_backend = SandboxBackendPreference::Mediated;
            }
            CapabilityProfilePreset::SandboxMicroVm => {
                profile.minimum_isolation = MinimumIsolation::Sandboxed;
                profile.sandbox.preferred_backend = SandboxBackendPreference::MicroVm;
            }
            CapabilityProfilePreset::Remote => {
                profile.minimum_isolation = MinimumIsolation::Remote;
            }
        }

        profile
    }

    pub fn resolve_preset(name: &str) -> Option<Self> {
        CapabilityProfilePreset::parse(name).map(Self::from_preset)
    }

    pub fn is_high_risk_override(&self) -> bool {
        self.profile_name.is_some()
            || self.native_extension_policy == CapabilityPolicy::Allow
            || self.subprocess_policy == CapabilityPolicy::Allow
            || self.thread_policy == CapabilityPolicy::Allow
            || self.outbound_ipc_policy == CapabilityPolicy::Allow
            || self.shared_memory_policy == CapabilityPolicy::Allow
            || self.gpu_policy == CapabilityPolicy::Allow
            || self.minimum_isolation != MinimumIsolation::Process
            || self.trusted_backend_preference != TrustedBackendPreference::Automatic
            || self.sandbox != SandboxProfile::default()
            || self.subinterpreter != SubInterpreterPolicy::default()
            || self.compiled_kernel != CompiledKernelPolicy::default()
    }

    pub fn minimum_isolation(&self) -> MinimumIsolation {
        self.minimum_isolation
    }

    pub fn trusted_backend_preference(&self) -> TrustedBackendPreference {
        self.trusted_backend_preference
    }

    pub fn allows_trusted_fast_lane(&self) -> bool {
        self.minimum_isolation == MinimumIsolation::Trusted
            || self.trusted_backend_preference != TrustedBackendPreference::Automatic
    }

    pub fn allows_compiled_native_extensions(&self) -> bool {
        self.native_extension_policy == CapabilityPolicy::Allow
    }

    pub fn allows_native_jit_compiled_kernel(&self) -> bool {
        self.compiled_kernel.native_jit != NativeJitPolicy::Disabled
    }
}
