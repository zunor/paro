// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use paro_common::error::{self as paro_error, Result};
use paro_routine::{CapabilityPolicy, CapabilityProfile};
use serde::{Deserialize, Serialize};

use crate::isolation::enforcer::IsolationEnforcer;
use crate::isolation::resource_limits::{
    FilesystemPolicy, NetworkPolicy, NetworkPolicyMode, ResourceLimits,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProcessPlatformCapabilities {
    pub cgroup_v2_available: bool,
    pub seccomp_available: bool,
    pub namespace_available: bool,
}

impl ProcessPlatformCapabilities {
    pub fn linux_default() -> Self {
        Self {
            cgroup_v2_available: true,
            seccomp_available: true,
            namespace_available: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RlimitPlan {
    pub memory_bytes: Option<u64>,
    pub cpu_time_ms: Option<u64>,
    pub tmp_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CgroupPlan {
    pub memory_max_bytes: Option<u64>,
    pub cpu_max_ms: Option<u64>,
    pub tmp_max_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SeccompDefaultAction {
    Allow,
    Trap,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeccompPolicy {
    pub default_action: SeccompDefaultAction,
    pub denied_syscalls: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespacePolicy {
    pub mount_namespace: bool,
    pub network_namespace: bool,
    pub pid_namespace: bool,
    pub ipc_namespace: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityEnforcementPlan {
    pub disable_native_extensions: bool,
    pub disable_subprocess: bool,
    pub disable_threads: bool,
    pub disable_outbound_ipc: bool,
    pub shared_memory_required: bool,
    pub disable_gpu: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessLaunchSpec {
    pub rlimits: RlimitPlan,
    pub cgroup: CgroupPlan,
    pub seccomp: SeccompPolicy,
    pub namespaces: NamespacePolicy,
    pub network_policy: NetworkPolicy,
    pub filesystem_policy: FilesystemPolicy,
    pub capabilities: CapabilityEnforcementPlan,
    pub env: BTreeMap<String, String>,
    pub wall_time_deadline_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AppliedIsolationProfile {
    pub limits: Option<ResourceLimits>,
    pub network_policy: Option<NetworkPolicy>,
    pub filesystem_policy: Option<FilesystemPolicy>,
    pub capability_profile: Option<CapabilityProfile>,
    pub launch_spec: Option<ProcessLaunchSpec>,
}

#[derive(Debug, Clone)]
pub struct ProcessIsolationEnforcer {
    platform: ProcessPlatformCapabilities,
    applied: Arc<Mutex<AppliedIsolationProfile>>,
}

impl Default for ProcessIsolationEnforcer {
    fn default() -> Self {
        Self::new(ProcessPlatformCapabilities::linux_default())
    }
}

impl ProcessIsolationEnforcer {
    pub fn new(platform: ProcessPlatformCapabilities) -> Self {
        Self {
            platform,
            applied: Arc::new(Mutex::new(AppliedIsolationProfile::default())),
        }
    }

    pub fn snapshot(&self) -> AppliedIsolationProfile {
        self.applied
            .lock()
            .expect("process isolation state")
            .clone()
    }

    pub fn build_launch_spec(
        &self,
        limits: &ResourceLimits,
        profile: &CapabilityProfile,
    ) -> Result<ProcessLaunchSpec> {
        validate_resource_limits(limits)?;
        validate_network_policy(&limits.network_policy)?;
        validate_filesystem_policy(&limits.filesystem_policy)?;
        validate_process_backend_capabilities(profile)?;

        let mut denied_syscalls = Vec::new();
        if profile.subprocess_policy == CapabilityPolicy::Deny {
            denied_syscalls.extend(
                ["clone", "clone3", "fork", "vfork", "execve", "execveat"]
                    .into_iter()
                    .map(str::to_string),
            );
        }
        if profile.thread_policy == CapabilityPolicy::Deny {
            denied_syscalls.extend(["clone", "clone3"].into_iter().map(str::to_string));
        }
        if profile.outbound_ipc_policy == CapabilityPolicy::Deny
            || limits.network_policy.mode == NetworkPolicyMode::DefaultDeny
        {
            denied_syscalls.extend(
                [
                    "socket",
                    "socketpair",
                    "connect",
                    "accept",
                    "accept4",
                    "listen",
                    "bind",
                ]
                .into_iter()
                .map(str::to_string),
            );
        }
        if profile.shared_memory_policy == CapabilityPolicy::Deny {
            denied_syscalls.extend(
                [
                    "memfd_create",
                    "shmget",
                    "shmat",
                    "shmctl",
                    "eventfd2",
                    "io_uring_setup",
                ]
                .into_iter()
                .map(str::to_string),
            );
        }
        denied_syscalls.sort();
        denied_syscalls.dedup();

        let mut env = BTreeMap::new();
        env.insert(
            "PARO_EXTERNAL_ROUTINE_NETWORK_MODE".to_string(),
            match limits.network_policy.mode {
                NetworkPolicyMode::DefaultDeny => "deny".to_string(),
                NetworkPolicyMode::AllowList => "allowlist".to_string(),
                NetworkPolicyMode::AllowAll => "allow".to_string(),
            },
        );
        if !limits.network_policy.allowlist.is_empty() {
            env.insert(
                "PARO_EXTERNAL_ROUTINE_NETWORK_ALLOWLIST".to_string(),
                limits
                    .network_policy
                    .allowlist
                    .iter()
                    .map(|rule| match rule.port {
                        Some(port) => format!("{}:{port}", rule.host_pattern),
                        None => rule.host_pattern.clone(),
                    })
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }
        if profile.native_extension_policy == CapabilityPolicy::Deny {
            env.insert(
                "PARO_EXTERNAL_ROUTINE_DISABLE_NATIVE_EXTENSIONS".to_string(),
                "1".to_string(),
            );
        }
        if profile.subprocess_policy == CapabilityPolicy::Deny {
            env.insert(
                "PARO_EXTERNAL_ROUTINE_DISABLE_SUBPROCESS".to_string(),
                "1".to_string(),
            );
        }
        if profile.thread_policy == CapabilityPolicy::Deny {
            env.insert(
                "PARO_EXTERNAL_ROUTINE_DISABLE_THREADS".to_string(),
                "1".to_string(),
            );
        }
        if profile.outbound_ipc_policy == CapabilityPolicy::Deny {
            env.insert(
                "PARO_EXTERNAL_ROUTINE_DISABLE_OUTBOUND_IPC".to_string(),
                "1".to_string(),
            );
        }
        if profile.gpu_policy == CapabilityPolicy::Deny {
            env.insert(
                "PARO_EXTERNAL_ROUTINE_DISABLE_GPU".to_string(),
                "1".to_string(),
            );
        }
        env.insert(
            "PARO_EXTERNAL_ROUTINE_ARTIFACT_ROOT".to_string(),
            limits.filesystem_policy.artifact_root.clone(),
        );
        if let Some(tmp_root) = &limits.filesystem_policy.writable_tmp_root {
            env.insert(
                "PARO_EXTERNAL_ROUTINE_TMP_ROOT".to_string(),
                tmp_root.clone(),
            );
        }

        Ok(ProcessLaunchSpec {
            rlimits: RlimitPlan {
                memory_bytes: limits.max_memory_bytes,
                cpu_time_ms: limits.max_cpu_time_ms,
                tmp_bytes: limits.max_tmp_bytes,
            },
            cgroup: CgroupPlan {
                memory_max_bytes: self
                    .platform
                    .cgroup_v2_available
                    .then_some(limits.max_memory_bytes)
                    .flatten(),
                cpu_max_ms: self
                    .platform
                    .cgroup_v2_available
                    .then_some(limits.max_cpu_time_ms)
                    .flatten(),
                tmp_max_bytes: self
                    .platform
                    .cgroup_v2_available
                    .then_some(limits.max_tmp_bytes)
                    .flatten(),
            },
            seccomp: SeccompPolicy {
                default_action: if self.platform.seccomp_available {
                    SeccompDefaultAction::Trap
                } else {
                    SeccompDefaultAction::Allow
                },
                denied_syscalls,
            },
            namespaces: NamespacePolicy {
                mount_namespace: self.platform.namespace_available,
                network_namespace: self.platform.namespace_available,
                pid_namespace: self.platform.namespace_available,
                ipc_namespace: self.platform.namespace_available,
            },
            network_policy: limits.network_policy.clone(),
            filesystem_policy: limits.filesystem_policy.clone(),
            capabilities: CapabilityEnforcementPlan {
                disable_native_extensions: profile.native_extension_policy
                    == CapabilityPolicy::Deny,
                disable_subprocess: profile.subprocess_policy == CapabilityPolicy::Deny,
                disable_threads: profile.thread_policy == CapabilityPolicy::Deny,
                disable_outbound_ipc: profile.outbound_ipc_policy == CapabilityPolicy::Deny,
                shared_memory_required: profile.shared_memory_policy == CapabilityPolicy::Allow,
                disable_gpu: profile.gpu_policy == CapabilityPolicy::Deny,
            },
            env,
            wall_time_deadline_ms: limits.max_wall_time_ms,
        })
    }

    pub fn enforce_profile(
        &self,
        limits: &ResourceLimits,
        profile: &CapabilityProfile,
    ) -> Result<ProcessLaunchSpec> {
        let launch_spec = self.build_launch_spec(limits, profile)?;
        self.apply_limits(limits)?;
        self.apply_network_policy(&limits.network_policy)?;
        self.apply_filesystem_policy(&limits.filesystem_policy)?;
        self.apply_capability_profile(profile)?;
        self.applied
            .lock()
            .expect("process isolation state")
            .launch_spec = Some(launch_spec.clone());
        Ok(launch_spec)
    }
}

impl IsolationEnforcer for ProcessIsolationEnforcer {
    fn apply_limits(&self, limits: &ResourceLimits) -> Result<()> {
        validate_resource_limits(limits)?;
        self.applied.lock().expect("process isolation state").limits = Some(limits.clone());
        Ok(())
    }

    fn apply_network_policy(&self, policy: &NetworkPolicy) -> Result<()> {
        validate_network_policy(policy)?;
        self.applied
            .lock()
            .expect("process isolation state")
            .network_policy = Some(policy.clone());
        Ok(())
    }

    fn apply_filesystem_policy(&self, policy: &FilesystemPolicy) -> Result<()> {
        validate_filesystem_policy(policy)?;
        self.applied
            .lock()
            .expect("process isolation state")
            .filesystem_policy = Some(policy.clone());
        Ok(())
    }

    fn apply_capability_profile(&self, profile: &CapabilityProfile) -> Result<()> {
        self.applied
            .lock()
            .expect("process isolation state")
            .capability_profile = Some(profile.clone());
        Ok(())
    }
}

fn validate_resource_limits(limits: &ResourceLimits) -> Result<()> {
    for (label, value) in [
        ("max_memory_bytes", limits.max_memory_bytes),
        ("max_cpu_time_ms", limits.max_cpu_time_ms),
        ("max_wall_time_ms", limits.max_wall_time_ms),
        ("max_tmp_bytes", limits.max_tmp_bytes),
    ] {
        if matches!(value, Some(0)) {
            return Err(paro_error::invalid_parameter(format!(
                "{label} must be greater than zero when configured"
            )));
        }
    }
    Ok(())
}

fn validate_network_policy(policy: &NetworkPolicy) -> Result<()> {
    if policy.mode == NetworkPolicyMode::AllowList && policy.allowlist.is_empty() {
        return Err(paro_error::invalid_parameter(
            "network allowlist mode requires at least one allow rule",
        ));
    }
    if policy
        .allowlist
        .iter()
        .any(|rule| rule.host_pattern.trim().is_empty())
    {
        return Err(paro_error::invalid_parameter(
            "network allowlist rules require a non-empty host pattern",
        ));
    }
    Ok(())
}

fn validate_filesystem_policy(policy: &FilesystemPolicy) -> Result<()> {
    if policy.artifact_root.trim().is_empty() {
        return Err(paro_error::invalid_parameter(
            "filesystem policy requires a non-empty artifact root",
        ));
    }
    if policy
        .readonly_roots()
        .into_iter()
        .any(|path| path.trim().is_empty())
    {
        return Err(paro_error::invalid_parameter(
            "filesystem readonly roots must not contain empty paths",
        ));
    }
    if policy
        .writable_roots()
        .into_iter()
        .any(|path| path.trim().is_empty())
    {
        return Err(paro_error::invalid_parameter(
            "filesystem writable roots must not contain empty paths",
        ));
    }
    Ok(())
}

fn validate_process_backend_capabilities(profile: &CapabilityProfile) -> Result<()> {
    if profile.shared_memory_policy == CapabilityPolicy::Deny {
        return Err(paro_error::sandbox_violation(
            "process backend requires shared-memory capability for host/worker data exchange",
        ));
    }
    Ok(())
}
