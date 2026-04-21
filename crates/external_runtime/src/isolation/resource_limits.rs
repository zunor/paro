// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ResourceLimits {
    pub max_memory_bytes: Option<u64>,
    pub max_cpu_time_ms: Option<u64>,
    pub max_wall_time_ms: Option<u64>,
    pub max_tmp_bytes: Option<u64>,
    pub network_policy: NetworkPolicy,
    pub filesystem_policy: FilesystemPolicy,
}

impl ResourceLimits {
    pub fn sandbox_default(artifact_root: impl Into<String>) -> Self {
        Self {
            max_memory_bytes: None,
            max_cpu_time_ms: None,
            max_wall_time_ms: None,
            max_tmp_bytes: None,
            network_policy: NetworkPolicy::default_deny(),
            filesystem_policy: FilesystemPolicy::sandbox_default(artifact_root),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum NetworkPolicyMode {
    #[default]
    DefaultDeny,
    AllowList,
    AllowAll,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkAllowRule {
    pub host_pattern: String,
    pub port: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct NetworkPolicy {
    pub mode: NetworkPolicyMode,
    pub allowlist: Vec<NetworkAllowRule>,
}

impl NetworkPolicy {
    pub fn default_deny() -> Self {
        Self::default()
    }

    pub fn allowlist(rules: impl IntoIterator<Item = NetworkAllowRule>) -> Self {
        Self {
            mode: NetworkPolicyMode::AllowList,
            allowlist: rules.into_iter().collect(),
        }
    }

    pub fn allow_all() -> Self {
        Self {
            mode: NetworkPolicyMode::AllowAll,
            allowlist: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FilesystemPolicy {
    pub artifact_root: String,
    pub runtime_readonly_roots: Vec<String>,
    pub writable_tmp_root: Option<String>,
    pub writable_roots: Vec<String>,
}

impl FilesystemPolicy {
    pub fn sandbox_default(artifact_root: impl Into<String>) -> Self {
        Self {
            artifact_root: artifact_root.into(),
            runtime_readonly_roots: vec![
                "/usr/lib".to_string(),
                "/usr/lib64".to_string(),
                "/usr/local/lib".to_string(),
            ],
            writable_tmp_root: Some("/tmp/paro-python-worker".to_string()),
            writable_roots: Vec::new(),
        }
    }

    pub fn readonly_roots(&self) -> Vec<String> {
        let mut roots = Vec::with_capacity(1 + self.runtime_readonly_roots.len());
        if !self.artifact_root.is_empty() {
            roots.push(self.artifact_root.clone());
        }
        roots.extend(self.runtime_readonly_roots.iter().cloned());
        roots
    }

    pub fn writable_roots(&self) -> Vec<String> {
        let mut roots = Vec::with_capacity(
            self.writable_roots.len() + usize::from(self.writable_tmp_root.is_some()),
        );
        if let Some(tmp_root) = &self.writable_tmp_root {
            roots.push(tmp_root.clone());
        }
        roots.extend(self.writable_roots.iter().cloned());
        roots
    }
}
