// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeclaredEnvSpec {
    pub runtime: PythonRuntimeSelector,
    pub packages: Vec<PackageRequirement>,
    pub imports: Vec<ImportRef>,
}

impl DeclaredEnvSpec {
    pub fn empty(runtime: PythonRuntimeSelector) -> Self {
        Self {
            runtime,
            packages: Vec::new(),
            imports: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageRequirement {
    pub spec: String,
    pub source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportRef {
    pub uri: String,
    pub expected_digest: Option<String>,
    pub expected_size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PythonRuntimeSelector {
    SystemDefault,
    Version(String),
}
