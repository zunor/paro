// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Query optimizer passes and supporting infrastructure.

pub(crate) mod binding;
pub mod cascades;
pub mod context;
pub mod cost;
pub mod optimizer;
pub mod physical;
pub(crate) mod statement;
pub(crate) mod verify;

pub use optimizer::{OptimizedStatement, Optimizer};

pub mod estimate;
pub mod region;
pub mod rewrite;

pub mod diagnostics;
