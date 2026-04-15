// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::fmt::Debug;

/// Reusable executor-local state for scalar functions.
pub trait FunctionLocalState: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn as_any_mut(&mut self) -> &mut dyn Any;
}
