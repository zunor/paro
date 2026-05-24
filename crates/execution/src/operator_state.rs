// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Compatibility state traits for the standalone sorting kernel.
//!
//! The typed operator runtime uses the role-specific state containers in
//! `runtime::state`, `runtime::scratch`, and breaker handles. This module stays
//! crate-private so the old state-trait model cannot re-enter
//! `runtime/`, `pipeline/`, or `physical/` hot paths. Its remaining owner is the
//! sorting kernel, which still has a self-contained source/sink API while the
//! typed runtime talks to sorting through breaker roles.

use std::any::Any;
use std::fmt;

/// Thread-local state for sorting sources.
pub trait LocalSourceState: Send + fmt::Debug {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Shared state for sorting sources.
pub trait GlobalSourceState: Send + Sync + fmt::Debug {
    fn as_any(&self) -> &dyn Any;

    fn max_threads(&self) -> usize {
        1
    }
}

/// Thread-local state for sorting sinks.
pub trait LocalSinkState: Send + fmt::Debug {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Shared state for sorting sinks.
pub trait GlobalSinkState: Send + Sync + fmt::Debug {
    fn as_any(&self) -> &dyn Any;
}
