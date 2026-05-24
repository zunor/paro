// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Pipeline construction, scheduling, and execution primitives.
//!
//! The high-level query coordinator lives in `crate::query_executor`.

pub mod graph;
pub mod handles;
pub mod lowerer;
pub mod program;
pub mod properties;

pub use program::{
    ControlRegionKind, ControlRegionProgram, ExtensionOperatorFactory, ExtensionSinkSpec,
    ExtensionSourceSpec, ExtensionTransformSpec, OperatorRuntimeRegistry, PipelineIdMap,
    PipelineProgram, PipelineProgramBuilder, PipelineProgramIndex, PipelineProgramSet,
    StatementProgram, UtilityProgram,
};
