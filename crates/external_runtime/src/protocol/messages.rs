// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use crate::backend::selector::BackendKind;
use crate::control::header::ControlHeader;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SidebandSchemaKind {
    Artifact,
    DataPlane,
    Error,
}

impl SidebandSchemaKind {
    pub fn file_name(self) -> &'static str {
        match self {
            SidebandSchemaKind::Artifact => "artifact.fbs",
            SidebandSchemaKind::DataPlane => "data_plane.fbs",
            SidebandSchemaKind::Error => "error.fbs",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolBindings {
    pub control_header: ControlHeader,
    pub rust_sideband_dir: &'static str,
    pub python_sideband_dir: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PythonExceptionPayload {
    pub exception_type: String,
    pub message: String,
    pub formatted_traceback: String,
    pub module: String,
    pub handler: String,
    pub batch_id: u64,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KernelFusionMode {
    RowPreservingChain,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KernelFusionStage {
    pub module: String,
    pub handler: String,
    pub backend: BackendKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KernelFusionPlan {
    pub mode: KernelFusionMode,
    pub stages: Vec<KernelFusionStage>,
}

impl KernelFusionPlan {
    pub fn row_preserving_chain<M, I, H>(module: M, handlers: I, backend: BackendKind) -> Self
    where
        M: Into<String>,
        I: IntoIterator<Item = H>,
        H: Into<String>,
    {
        let module = module.into();
        Self {
            mode: KernelFusionMode::RowPreservingChain,
            stages: handlers
                .into_iter()
                .map(|handler| KernelFusionStage {
                    module: module.clone(),
                    handler: handler.into(),
                    backend,
                })
                .collect(),
        }
    }

    pub fn is_chain_eligible(&self) -> bool {
        !self.stages.is_empty()
            && self
                .stages
                .windows(2)
                .all(|window| window[0].backend == window[1].backend)
    }
}

impl ProtocolBindings {
    pub fn schema_path(&self, kind: SidebandSchemaKind) -> String {
        format!("runtimes/protocol/sideband/{}", kind.file_name())
    }
}

impl Default for ProtocolBindings {
    fn default() -> Self {
        Self {
            control_header: ControlHeader::default(),
            rust_sideband_dir: "runtimes/protocol/generated/rust",
            python_sideband_dir: "runtimes/protocol/generated/python",
        }
    }
}
