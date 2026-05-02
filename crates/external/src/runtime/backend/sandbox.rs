// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::routine::artifact::ArtifactCapabilities;
use crate::routine::capability::{
    CapabilityProfile, RestrictedSdkProfile, SandboxBackendPreference,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SandboxRuntimeKind {
    NamespaceProcess,
    RestrictedWasm,
    Mediated,
    MicroVmSnapshot,
}

impl SandboxRuntimeKind {
    pub fn label(self) -> &'static str {
        match self {
            SandboxRuntimeKind::NamespaceProcess => "sandbox_process",
            SandboxRuntimeKind::RestrictedWasm => "sandbox_wasm",
            SandboxRuntimeKind::Mediated => "sandbox_mediated",
            SandboxRuntimeKind::MicroVmSnapshot => "sandbox_microvm",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxSelection {
    pub runtime: SandboxRuntimeKind,
    pub restricted_sdk: RestrictedSdkProfile,
    pub cold_boot_allowed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SandboxAvailability {
    pub namespace_ready: bool,
    pub restricted_wasm_ready: bool,
    pub mediated_ready: bool,
    pub microvm_snapshot_ready: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxedPythonProcessBackend;

impl SandboxedPythonProcessBackend {
    pub fn select_target(
        profile: &CapabilityProfile,
        capabilities: &ArtifactCapabilities,
        availability: &SandboxAvailability,
    ) -> Option<SandboxSelection> {
        let preferred = preferred_candidates(profile);
        preferred.into_iter().find_map(|runtime| {
            self::runtime_supported(runtime, capabilities, availability).then_some(
                SandboxSelection {
                    runtime,
                    restricted_sdk: profile.sandbox.restricted_sdk,
                    cold_boot_allowed: profile.sandbox.allow_cold_boot,
                },
            )
        })
    }
}

fn preferred_candidates(profile: &CapabilityProfile) -> Vec<SandboxRuntimeKind> {
    match profile.sandbox.preferred_backend {
        SandboxBackendPreference::NamespaceProcess => {
            vec![SandboxRuntimeKind::NamespaceProcess]
        }
        SandboxBackendPreference::RestrictedWasm => vec![
            SandboxRuntimeKind::RestrictedWasm,
            SandboxRuntimeKind::Mediated,
            SandboxRuntimeKind::MicroVmSnapshot,
            SandboxRuntimeKind::NamespaceProcess,
        ],
        SandboxBackendPreference::Mediated => vec![
            SandboxRuntimeKind::Mediated,
            SandboxRuntimeKind::MicroVmSnapshot,
            SandboxRuntimeKind::NamespaceProcess,
        ],
        SandboxBackendPreference::MicroVm => vec![
            SandboxRuntimeKind::MicroVmSnapshot,
            SandboxRuntimeKind::Mediated,
            SandboxRuntimeKind::NamespaceProcess,
        ],
        SandboxBackendPreference::Auto => {
            if profile.sandbox.restricted_sdk != RestrictedSdkProfile::Disabled {
                vec![
                    SandboxRuntimeKind::RestrictedWasm,
                    SandboxRuntimeKind::Mediated,
                    SandboxRuntimeKind::MicroVmSnapshot,
                    SandboxRuntimeKind::NamespaceProcess,
                ]
            } else {
                vec![
                    SandboxRuntimeKind::Mediated,
                    SandboxRuntimeKind::MicroVmSnapshot,
                    SandboxRuntimeKind::NamespaceProcess,
                ]
            }
        }
    }
}

fn runtime_supported(
    runtime: SandboxRuntimeKind,
    capabilities: &ArtifactCapabilities,
    availability: &SandboxAvailability,
) -> bool {
    match runtime {
        SandboxRuntimeKind::NamespaceProcess => availability.namespace_ready,
        SandboxRuntimeKind::RestrictedWasm => {
            availability.restricted_wasm_ready && capabilities.supports_restricted_wasm_backend
        }
        SandboxRuntimeKind::Mediated => {
            availability.mediated_ready && capabilities.supports_mediated_sandbox_backend
        }
        SandboxRuntimeKind::MicroVmSnapshot => {
            availability.microvm_snapshot_ready && capabilities.supports_microvm_backend
        }
    }
}
