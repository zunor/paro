use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_compiler::compile_statement;
use paro_context::StatementContext;
use paro_execution::query_executor::compiled::CompiledStatement;
use paro_parser::ast::Statement;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PlanCacheMode {
    #[default]
    Auto,
    ForceGeneric,
    ForceCustom,
}

pub fn build_generic_plan(
    ctx: Arc<StatementContext>,
    stmt: Statement,
    parameter_types: &[Option<LogicalType>],
) -> Result<CompiledStatement> {
    let mut compiled = compile_statement(ctx, stmt)?;
    compiled.parameter_types = parameter_types
        .iter()
        .map(|ty| ty.clone().unwrap_or(LogicalType::Unknown))
        .collect();
    Ok(compiled)
}
