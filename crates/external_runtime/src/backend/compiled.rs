// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompiledRuntimeKind {
    Generic,
    Numba,
    Hpy,
    Pyo3,
    Aot,
    NativeJit,
}

impl CompiledRuntimeKind {
    pub fn label(self) -> &'static str {
        match self {
            CompiledRuntimeKind::Generic => "compiled",
            CompiledRuntimeKind::Numba => "numba",
            CompiledRuntimeKind::Hpy => "hpy",
            CompiledRuntimeKind::Pyo3 => "pyo3",
            CompiledRuntimeKind::Aot => "aot",
            CompiledRuntimeKind::NativeJit => "jit",
        }
    }

    pub fn from_declared_kind(kind: Option<&str>) -> Self {
        match kind.map(str::to_ascii_lowercase).as_deref() {
            Some("numba") => CompiledRuntimeKind::Numba,
            Some("hpy") => CompiledRuntimeKind::Hpy,
            Some("pyo3") => CompiledRuntimeKind::Pyo3,
            Some("aot") => CompiledRuntimeKind::Aot,
            Some("jit") | Some("native_jit") => CompiledRuntimeKind::NativeJit,
            Some(_) => CompiledRuntimeKind::Generic,
            None => CompiledRuntimeKind::Generic,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledKernelBackend;
