// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Expression execution, compiled state, and runtime adapters for scalar functions.

mod comparison;
mod execution_state;
mod like_pattern;
mod predicate;
mod program;

pub mod executor;
pub(crate) mod rows;
pub mod physical {
    pub use super::program::{
        expression_fingerprint, expression_list_fingerprints, ExpressionBackend,
        ExpressionProgramCache, ExpressionProgramVersion, ExpressionScratchLayout,
        ExpressionScratchSlot, PhysicalExpressionProgram,
    };
}
pub mod state;

pub(crate) use comparison::compile_comparison_dispatch;
