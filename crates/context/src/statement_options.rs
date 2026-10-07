// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExplainOutputType {
    All,
    Optimized,
    #[default]
    PhysicalOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum StatementSource {
    #[default]
    SimpleQuery,
    PreparedSql,
    ExtendedQuery,
    Internal,
}

#[derive(Debug, Clone, Default)]
pub struct StatementOptions {
    /// Request-owned observer, absent on ordinary compilation. Never a search hint.
    pub compile_capture: Option<std::sync::Arc<crate::compile_diagnostics::CompileCapture>>,
    pub statement_format: Option<String>,
    pub explain_output: Option<ExplainOutputType>,
    pub source: StatementSource,
}
