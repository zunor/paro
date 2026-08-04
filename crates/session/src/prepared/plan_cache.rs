// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_compiler::{compile_statement, compile_statement_with_parameter_types};
use paro_context::StatementContext;
use paro_execution::query_executor::compiled::CompiledStatement;
use paro_parser::ast::Statement;
use std::sync::Arc;

pub fn build_generic_plan(
    ctx: Arc<StatementContext>,
    stmt: Statement,
    parameter_types: &[Option<LogicalType>],
) -> Result<CompiledStatement> {
    if parameter_types.is_empty() {
        return compile_statement(ctx, stmt);
    }
    if parameter_types.iter().any(Option::is_none) {
        return Err(paro_common::error::syntax(
            "cannot build a reusable plan until every parameter type is known".to_string(),
        ));
    }
    let parameter_types = parameter_types
        .iter()
        .map(|ty| ty.clone().expect("parameter types checked"))
        .collect::<Vec<_>>();
    compile_statement_with_parameter_types(ctx, stmt, &parameter_types)
}
