// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_function::table::CopyStdinSource;
use std::fmt;
use std::sync::Arc;

/// External inputs whose lifetime is exactly one statement execution.
///
/// Inputs live outside the parsed SQL and compiled plan. This keeps protocol
/// resources out of reusable plans while still making ownership explicit to
/// execution components that consume them.
#[derive(Clone, Default)]
pub struct StatementInput {
    copy_stdin: Option<Arc<dyn CopyStdinSource>>,
}

impl StatementInput {
    pub fn copy_from_stdin(source: Arc<dyn CopyStdinSource>) -> Self {
        Self {
            copy_stdin: Some(source),
        }
    }

    pub fn copy_stdin_source(&self) -> Option<Arc<dyn CopyStdinSource>> {
        self.copy_stdin.clone()
    }

    pub fn requires_background_execution(&self) -> bool {
        self.copy_stdin
            .as_ref()
            .is_some_and(|source| source.requires_background_execution())
    }
}

impl fmt::Debug for StatementInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StatementInput")
            .field("has_copy_stdin", &self.copy_stdin.is_some())
            .finish()
    }
}
