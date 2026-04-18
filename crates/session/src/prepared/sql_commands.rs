// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};
use paro_common::logging::targets;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_compiler::compile_statement;
use paro_context::{StatementContext, StatementOptions, StatementSource};
use paro_execution::query_executor::compiled::{CompiledStatement, ResultColumnDesc};
use paro_execution::query_executor::executor::Executor;
use paro_parser::ast::{
    CursorScrollMode, DeallocateStmt, Expr, FetchStmt, Literal, PrepareStmt, Statement,
    UnaryOperator,
};
use paro_planner::binder::bind::type_name::bind_logical_type;
use tracing::{debug, error};

use crate::completion::StatementCompletion;
use crate::copy_protocol::ProtocolResultSink;
use crate::dispatch::{dispatch_statement, FrontendRoute, PreparedCommand};
use crate::prepared::materialization::materialize_compiled_statement;
use crate::prepared::parameters::{
    bind_expr_arguments, bind_value_arguments, placeholder_count, render_probe_statement,
};
use crate::prepared::plan_cache::build_generic_plan;
use crate::prepared::portal::{
    bind_value_types, CursorHoldability, ExecutionCursorHandle, FormatCode, PortalCursor,
    PortalExecutionState, ScrollMode,
};
use crate::prepared::store::{
    PortalEntry, PortalKind, PortalStatementRef, PreparedStatementEntry, PreparedStatementSource,
};
use crate::Session;

pub(crate) async fn execute_prepared_command<S: ProtocolResultSink>(
    session: &mut Session,
    cmd: PreparedCommand,
    sink: &mut S,
) -> Result<()> {
    match cmd {
        PreparedCommand::Prepare(stmt) => execute_prepare(session, stmt, sink).await,
        PreparedCommand::Execute(stmt) => {
            execute_execute(session, stmt.name.name, stmt.args, sink).await
        }
        PreparedCommand::Deallocate(stmt) => execute_deallocate(session, stmt, sink).await,
        PreparedCommand::DeclareCursor(stmt) => execute_declare_cursor(session, stmt, sink).await,
        PreparedCommand::Fetch(stmt) => execute_fetch(session, stmt, false, sink).await,
        PreparedCommand::Move(stmt) => execute_fetch(session, stmt, true, sink).await,
        PreparedCommand::CloseCursor(stmt) => {
            execute_close_cursor(session, stmt.name.map(|n| n.name), sink).await
        }
    }
}

async fn execute_prepare<S: ProtocolResultSink>(
    session: &mut Session,
    stmt: PrepareStmt,
    sink: &mut S,
) -> Result<()> {
    validate_preparable_statement(&stmt.statement)?;
    if session
        .state
        .has_prepared_statement(stmt.name.name.as_str())
    {
        return Err(paro_error::catalog(format!(
            "prepared statement \"{}\" already exists",
            stmt.name.name
        )));
    }

    let require_new_transaction =
        session.transaction.is_auto_commit() && !session.transaction.has_active_transaction();
    if require_new_transaction {
        session.begin_transaction_internal()?;
    }

    let placeholder_count = placeholder_count(&stmt.statement);
    let parameter_types = stmt
        .parameter_types
        .iter()
        .map(bind_logical_type)
        .map(|res| res.map(Some))
        .collect::<Result<Vec<_>>>()?;
    if !parameter_types.is_empty() && parameter_types.len() != placeholder_count {
        if require_new_transaction {
            let _ = session.rollback_auto_transaction(None);
        }
        return Err(paro_error::syntax(format!(
            "expected {placeholder_count} parameters, got {}",
            parameter_types.len()
        )));
    }

    let raw_stmt = (*stmt.statement).clone();
    let snapshot = session.freeze_statement_context(
        StatementOptions {
            source: StatementSource::PreparedSql,
            ..StatementOptions::default()
        },
        session
            .current_statement_cancellation()
            .expect("PREPARE requires an active statement scope"),
    );
    let parameter_types = if parameter_types.is_empty() {
        vec![None; placeholder_count]
    } else {
        parameter_types
    };
    let (result_schema, generic_plan) = match render_probe_statement(&raw_stmt, &parameter_types) {
        Ok(Some(probe_stmt)) => {
            match build_generic_plan(snapshot.clone(), probe_stmt, &parameter_types) {
                Ok(plan) => (plan.result_schema.clone(), None),
                Err(err) => {
                    if require_new_transaction {
                        let _ = session.rollback_auto_transaction(Some(&err));
                    }
                    return Err(err);
                }
            }
        }
        Ok(None) => {
            match build_generic_plan(snapshot.clone(), raw_stmt.clone(), &parameter_types) {
                Ok(plan) => (plan.result_schema.clone(), Some(plan)),
                Err(err) => {
                    if require_new_transaction {
                        let _ = session.rollback_auto_transaction(Some(&err));
                    }
                    return Err(err);
                }
            }
        }
        Err(err) => {
            if require_new_transaction {
                let _ = session.rollback_auto_transaction(Some(&err));
            }
            return Err(err);
        }
    };

    let entry = PreparedStatementEntry {
        name: stmt.name.name,
        source_sql: raw_stmt.to_string(),
        raw_stmt,
        parameter_types,
        result_schema,
        plan_cache_mode: crate::prepared::plan_cache::PlanCacheMode::Auto,
        generic_plan,
        custom_plan_executions: 0,
        dependency_epoch: session.transaction_visible_version(),
        compile_environment: snapshot.compile_environment_key(),
        source: PreparedStatementSource::Sql,
    };

    session.state.add_prepared_statement(entry);
    if require_new_transaction {
        session.commit_auto_transaction()?;
    }
    session.refresh_session_metadata();
    sink.finish_result(&StatementCompletion::Prepare).await?;
    Ok(())
}

#[allow(clippy::vec_box)]
async fn execute_execute<S: ProtocolResultSink>(
    session: &mut Session,
    name: String,
    args: Vec<Box<Expr>>,
    sink: &mut S,
) -> Result<()> {
    let entry = session
        .state
        .get_prepared_statement(&name)
        .cloned()
        .ok_or_else(|| {
            paro_error::catalog(format!("prepared statement \"{name}\" does not exist"))
        })?;

    let require_new_transaction =
        session.transaction.is_auto_commit() && !session.transaction.has_active_transaction();

    if require_new_transaction {
        session.begin_transaction_internal()?;
    }

    let bound_params = match evaluate_execute_args(session, &args).await {
        Ok(params) => params,
        Err(err) => {
            if require_new_transaction {
                let _ = session.rollback_auto_transaction(Some(&err));
            }
            return Err(err);
        }
    };

    let snapshot = session.freeze_statement_context(
        StatementOptions {
            source: StatementSource::PreparedSql,
            ..StatementOptions::default()
        },
        session
            .current_statement_cancellation()
            .expect("EXECUTE requires an active statement scope"),
    );

    let compiled =
        match select_execute_plan(session, &name, &entry, snapshot.clone(), &bound_params) {
            Ok(compiled) => compiled,
            Err(err) => {
                if require_new_transaction {
                    let _ = session.rollback_auto_transaction(Some(&err));
                }
                return Err(err);
            }
        };

    match run_compiled_statement(
        session,
        snapshot,
        compiled,
        Some(&entry.raw_stmt),
        None,
        sink,
    )
    .await
    {
        Ok(()) => {
            if require_new_transaction {
                session.commit_auto_transaction()?;
            }
            session.refresh_session_metadata();
            Ok(())
        }
        Err(err) => {
            if require_new_transaction {
                let _ = session.rollback_auto_transaction(Some(&err));
            }
            Err(err)
        }
    }
}

async fn execute_deallocate<S: ProtocolResultSink>(
    session: &mut Session,
    stmt: DeallocateStmt,
    sink: &mut S,
) -> Result<()> {
    let completion = match stmt.name {
        Some(name) => {
            if session
                .state
                .remove_prepared_statement(name.name.as_str())
                .is_none()
            {
                return Err(paro_error::catalog(format!(
                    "prepared statement \"{}\" does not exist",
                    name.name
                )));
            }
            StatementCompletion::Deallocate { all: false }
        }
        None => {
            session.state.clear_prepared_statements();
            StatementCompletion::Deallocate { all: true }
        }
    };

    session.refresh_session_metadata();
    sink.finish_result(&completion).await?;
    Ok(())
}

async fn execute_declare_cursor<S: ProtocolResultSink>(
    session: &mut Session,
    stmt: paro_parser::ast::DeclareCursorStmt,
    sink: &mut S,
) -> Result<()> {
    validate_cursor_query(&stmt.query)?;
    if !(stmt.hold || session.is_in_explicit_block() || session.is_in_implicit_block()) {
        return Err(paro_error::invalid_transaction_state(
            "DECLARE CURSOR can only be used in transaction blocks".to_string(),
        ));
    }
    if session.state.has_portal(stmt.name.name.as_str()) {
        return Err(paro_error::catalog(format!(
            "cursor \"{}\" already exists",
            stmt.name.name
        )));
    }

    let require_new_transaction =
        session.transaction.is_auto_commit() && !session.transaction.has_active_transaction();
    if require_new_transaction {
        session.begin_transaction_internal()?;
    }

    let snapshot = session.freeze_statement_context(
        StatementOptions {
            source: StatementSource::PreparedSql,
            ..StatementOptions::default()
        },
        session
            .current_statement_cancellation()
            .expect("DECLARE CURSOR requires an active statement scope"),
    );
    let compiled = match compile_statement(snapshot.clone(), (*stmt.query).clone()) {
        Ok(compiled) => compiled,
        Err(err) => {
            if require_new_transaction {
                let _ = session.rollback_auto_transaction(Some(&err));
            }
            return Err(err);
        }
    };

    if !compiled.is_query() {
        if require_new_transaction {
            let _ = session.rollback_auto_transaction(None);
        }
        return Err(paro_error::syntax(
            "DECLARE CURSOR requires a query that returns rows".to_string(),
        ));
    }

    let result_schema = compiled.result_schema.clone();
    let compiled_for_portal = compiled.clone();
    let materialized =
        match materialize_compiled_statement(session, snapshot.clone(), compiled).await {
            Ok(materialized) => materialized,
            Err(err) => {
                if require_new_transaction {
                    let _ = session.rollback_auto_transaction(Some(&err));
                }
                return Err(err);
            }
        };

    let portal = PortalEntry {
        name: stmt.name.name,
        statement_ref: PortalStatementRef::None,
        source_sql: stmt.query.to_string(),
        raw_stmt: (*stmt.query).clone(),
        bound_params: Vec::new(),
        holdability: if stmt.hold {
            CursorHoldability::WithHold
        } else {
            CursorHoldability::WithoutHold
        },
        scroll_mode: scroll_mode_from_ast(stmt.scroll),
        result_formats: vec![FormatCode::Text; result_schema.len().max(1)],
        result_schema,
        kind: PortalKind::Compiled(Box::new(compiled_for_portal)),
        execution_state: PortalExecutionState::Active(PortalCursor {
            position: -1,
            execution: ExecutionCursorHandle::materialized(materialized),
        }),
        completion: None,
        dependency_epoch: session.transaction_visible_version(),
        created_generation: 0,
        transaction_owned: session.has_active_transaction(),
    };
    session.state.add_portal(portal);

    if require_new_transaction {
        session.commit_auto_transaction()?;
    }

    session.refresh_session_metadata();
    sink.finish_result(&StatementCompletion::DeclareCursor)
        .await?;
    Ok(())
}

async fn execute_fetch<S: ProtocolResultSink>(
    session: &mut Session,
    stmt: FetchStmt,
    move_only: bool,
    sink: &mut S,
) -> Result<()> {
    session.check_active_statement_cancellation()?;
    let portal = session
        .state
        .get_portal_mut(stmt.cursor.name.as_str())
        .ok_or_else(|| paro_error::catalog(format!("cursor \"{}\" does not exist", stmt.cursor)))?;

    let (outcome, result_schema) = match &mut portal.execution_state {
        PortalExecutionState::Active(cursor) => {
            let outcome = cursor
                .execution
                .fetch(
                    cursor.position,
                    &stmt.direction,
                    portal.scroll_mode,
                    move_only,
                )
                .map_err(paro_error::syntax)?;
            cursor.position = outcome.new_position;
            (outcome, portal.result_schema.clone())
        }
        PortalExecutionState::Ready => {
            return Err(paro_error::syntax(
                "cursor has not been initialized".to_string(),
            ))
        }
        PortalExecutionState::Exhausted { .. } => {
            return Err(paro_error::syntax("cursor is exhausted".to_string()))
        }
    };

    if !move_only {
        let (names, types) = split_result_schema(&result_schema);
        sink.start_result(&names, &types).await?;
        for chunk in &outcome.rows {
            sink.push_chunk(chunk).await?;
        }
        sink.finish_result(&StatementCompletion::Fetch {
            rows: outcome.moved_rows,
        })
        .await?;
    } else {
        sink.finish_result(&StatementCompletion::Move {
            rows: outcome.moved_rows,
        })
        .await?;
    }

    session.refresh_session_metadata();
    Ok(())
}

async fn execute_close_cursor<S: ProtocolResultSink>(
    session: &mut Session,
    name: Option<String>,
    sink: &mut S,
) -> Result<()> {
    let completion = match name {
        Some(name) => {
            if session.state.remove_portal(name.as_str()).is_none() {
                return Err(paro_error::catalog(format!(
                    "cursor \"{name}\" does not exist"
                )));
            }
            StatementCompletion::CloseCursor { all: false }
        }
        None => {
            session.state.clear_portals();
            StatementCompletion::CloseCursor { all: true }
        }
    };

    session.refresh_session_metadata();
    sink.finish_result(&completion).await?;
    Ok(())
}

async fn run_compiled_statement<S: ProtocolResultSink>(
    session: &mut Session,
    ctx: Arc<StatementContext>,
    compiled: CompiledStatement,
    stmt: Option<&Statement>,
    completion_override: Option<StatementCompletion>,
    sink: &mut S,
) -> Result<()> {
    let statement_completion = completion_override.clone().unwrap_or_else(|| {
        stmt.map(crate::completion_infer::initial_statement_completion)
            .unwrap_or(StatementCompletion::Empty)
    });
    let executor = Executor::new(ctx);
    session.set_executor(executor);
    debug!(
        target: targets::EXECUTOR,
        session_id = session.id,
        statement_completion = %statement_completion,
        has_result_set = compiled.is_query(),
        "Prepared executor initialized"
    );

    let result = session.get_executor().execute(compiled.clone());
    match result {
        Ok(mut stream) => {
            let result_names = compiled.result_names();
            let result_types = compiled.result_types();
            let emits_rows = compiled.is_query() && !matches!(stmt, Some(Statement::Copy(_)));
            let mut rows = 0usize;
            if emits_rows {
                sink.start_result(&result_names, &result_types).await?;
                while let Some(chunk) = stream.fetch()? {
                    rows += chunk.len();
                    sink.push_chunk(chunk).await?;
                }
            } else {
                while let Some(chunk) = stream.fetch()? {
                    if chunk.len() > 0 && chunk.column_count() > 0 {
                        if let Some(col) = chunk.column(0) {
                            let value = col.get_value(0);
                            if let Value::BigInt(count) = value {
                                rows = count as usize;
                            }
                        }
                    }
                }
            }
            let completion = completion_override.unwrap_or_else(|| {
                stmt.map(|stmt| crate::completion_infer::infer_statement_completion(stmt, rows))
                    .unwrap_or(StatementCompletion::Empty)
            });
            sink.finish_result(&completion).await?;
            Ok(())
        }
        Err(err) => {
            error!(
                target: targets::EXECUTOR,
                session_id = session.id,
                statement_completion = %statement_completion,
                error = %err,
                stage = "prepared",
                "Prepared execution failed"
            );
            Err(err)
        }
    }
}

fn select_execute_plan(
    session: &mut Session,
    name: &str,
    entry: &PreparedStatementEntry,
    ctx: Arc<StatementContext>,
    bound_params: &[Value],
) -> Result<CompiledStatement> {
    let compile_environment = ctx.compile_environment_key();
    let compile_environment_changed = entry.compile_environment != compile_environment;
    let resolved_parameter_types = resolve_parameter_types(&entry.parameter_types, bound_params)?;
    let bound_stmt =
        bind_value_arguments(&entry.raw_stmt, bound_params, &resolved_parameter_types)?;

    if !resolved_parameter_types.is_empty() {
        let mut compiled = build_generic_plan(ctx, bound_stmt, &resolved_parameter_types)?;
        compiled.parameter_types = resolved_parameter_types
            .iter()
            .map(|ty| ty.clone().unwrap_or(LogicalType::Unknown))
            .collect();
        if let Some(stored) = session.state.get_prepared_statement_mut(name) {
            stored.result_schema = compiled.result_schema.clone();
            stored.parameter_types = resolved_parameter_types;
            stored.custom_plan_executions = stored.custom_plan_executions.saturating_add(1);
            stored.compile_environment = compile_environment;
        }
        return Ok(compiled);
    }

    match entry.plan_cache_mode {
        crate::prepared::plan_cache::PlanCacheMode::ForceCustom => {
            let mut compiled =
                build_generic_plan(ctx, bound_stmt.clone(), &resolved_parameter_types)?;
            compiled.parameter_types = resolved_parameter_types
                .iter()
                .map(|ty| ty.clone().unwrap_or(LogicalType::Unknown))
                .collect();
            if let Some(stored) = session.state.get_prepared_statement_mut(name) {
                stored.custom_plan_executions = stored.custom_plan_executions.saturating_add(1);
                stored.parameter_types = resolved_parameter_types;
                stored.compile_environment = compile_environment;
            }
            Ok(compiled)
        }
        crate::prepared::plan_cache::PlanCacheMode::Auto
        | crate::prepared::plan_cache::PlanCacheMode::ForceGeneric => {
            if !compile_environment_changed {
                if let Some(plan) = entry.generic_plan.clone() {
                    if let Some(stored) = session.state.get_prepared_statement_mut(name) {
                        stored.parameter_types = resolved_parameter_types;
                    }
                    return Ok(plan);
                }
            }

            let mut compiled = build_generic_plan(ctx, bound_stmt, &resolved_parameter_types)?;
            compiled.parameter_types = resolved_parameter_types
                .iter()
                .map(|ty| ty.clone().unwrap_or(LogicalType::Unknown))
                .collect();
            if let Some(stored) = session.state.get_prepared_statement_mut(name) {
                stored.result_schema = compiled.result_schema.clone();
                stored.generic_plan = Some(compiled.clone());
                stored.parameter_types = resolved_parameter_types;
                stored.compile_environment = compile_environment;
            }
            Ok(compiled)
        }
    }
}

fn validate_preparable_statement(stmt: &Statement) -> Result<()> {
    if matches!(
        stmt,
        Statement::Explain { .. }
            | Statement::ExplainAnalyze { .. }
            | Statement::Copy(_)
            | Statement::StatementWithSettings { .. }
    ) {
        return Err(paro_error::syntax(
            "statement type is not supported by PREPARE".to_string(),
        ));
    }

    match dispatch_statement(stmt.clone()) {
        FrontendRoute::Query(_) => Ok(()),
        _ => Err(paro_error::syntax(
            "PREPARE only supports plannable query statements".to_string(),
        )),
    }
}

fn validate_cursor_query(stmt: &Statement) -> Result<()> {
    match dispatch_statement(stmt.clone()) {
        FrontendRoute::Query(_) => Ok(()),
        _ => Err(paro_error::syntax(
            "DECLARE CURSOR only supports query statements".to_string(),
        )),
    }
}

fn resolve_parameter_types(
    declared: &[Option<LogicalType>],
    bound_params: &[Value],
) -> Result<Vec<Option<LogicalType>>> {
    if declared.len() != bound_params.len() {
        return Err(paro_error::syntax(format!(
            "expected {} parameters, got {}",
            declared.len(),
            bound_params.len()
        )));
    }

    let inferred = bind_value_types(bound_params);
    declared
        .iter()
        .zip(inferred)
        .map(|(declared, inferred)| match (declared, inferred) {
            (Some(ty), _) => Ok(Some(ty.clone())),
            (None, Some(ty)) => Ok(Some(ty)),
            (None, None) => Err(paro_error::syntax(
                "could not infer parameter type".to_string(),
            )),
        })
        .collect()
}

fn evaluate_constant_expr(expr: &Expr) -> Result<Value> {
    match expr {
        Expr::Literal { value, .. } => literal_to_value(value),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
            ..
        } => evaluate_constant_expr(expr),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
            ..
        } => negate_value(evaluate_constant_expr(expr)?),
        _ => Err(paro_error::syntax(
            "EXECUTE only supports literal parameters for now".to_string(),
        )),
    }
}

fn literal_to_value(literal: &Literal) -> Result<Value> {
    match literal {
        Literal::UInt64(value) => match i64::try_from(*value) {
            Ok(v) => Ok(Value::BigInt(v)),
            Err(_) => Ok(Value::UBigInt(*value)),
        },
        Literal::Float64(value) => Ok(Value::Double(*value)),
        Literal::Decimal256 {
            value,
            precision,
            scale,
        } => Ok(Value::Decimal(value.as_i128(), *precision, *scale)),
        Literal::String(value) => Ok(Value::Varchar(value.clone())),
        Literal::Boolean(value) => Ok(Value::Boolean(*value)),
        Literal::Null => Ok(Value::Null(LogicalType::Unknown)),
    }
}

fn negate_value(value: Value) -> Result<Value> {
    match value {
        Value::UBigInt(v) if v <= i64::MAX as u64 => Ok(Value::BigInt(-(v as i64))),
        Value::BigInt(v) => Ok(Value::BigInt(-v)),
        Value::Integer(v) => Ok(Value::Integer(-v)),
        Value::SmallInt(v) => Ok(Value::SmallInt(-v)),
        Value::TinyInt(v) => Ok(Value::TinyInt(-v)),
        Value::Double(v) => Ok(Value::Double(-v)),
        Value::Float(v) => Ok(Value::Float(-v)),
        Value::Decimal(v, precision, scale) => Ok(Value::Decimal(-v, precision, scale)),
        other => Err(paro_error::syntax(format!(
            "cannot apply unary minus to parameter value {other:?}"
        ))),
    }
}

fn scroll_mode_from_ast(mode: CursorScrollMode) -> ScrollMode {
    match mode {
        CursorScrollMode::Scroll => ScrollMode::Scroll,
        CursorScrollMode::NoScroll => ScrollMode::NoScroll,
        CursorScrollMode::Unspecified => ScrollMode::Scroll,
    }
}

fn split_result_schema(schema: &[ResultColumnDesc]) -> (Vec<String>, Vec<LogicalType>) {
    (
        schema.iter().map(|col| col.name.clone()).collect(),
        schema.iter().map(|col| col.logical_type.clone()).collect(),
    )
}

pub(crate) async fn evaluate_execute_args(
    session: &Session,
    args: &[Box<Expr>],
) -> Result<Vec<Value>> {
    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        match evaluate_constant_expr(arg) {
            Ok(value) => values.push(value),
            Err(_) => values.push(evaluate_scalar_expr(session, arg).await?),
        }
    }
    Ok(values)
}

pub(crate) fn bind_execute_explain_target(
    stmt: &Statement,
    args: &[Box<Expr>],
) -> Result<Statement> {
    let exprs = args.iter().map(|arg| (**arg).clone()).collect::<Vec<_>>();
    bind_expr_arguments(stmt, &exprs)
}

async fn evaluate_scalar_expr(session: &Session, expr: &Expr) -> Result<Value> {
    let sql = format!("SELECT {expr}");
    let stmt = paro_parser::parse_one(&sql)
        .map_err(|err| paro_error::from_parser(err.to_string()))?
        .stmt;
    let ctx = session.freeze_statement_context(
        StatementOptions {
            source: StatementSource::PreparedSql,
            ..StatementOptions::default()
        },
        session
            .current_statement_execution_attempt()
            .unwrap_or_else(|| session.compile_scope_cancellation()),
    );
    let compiled = compile_statement(ctx.clone(), stmt)?;
    let executor = Executor::new(ctx);
    let mut stream = executor.execute(compiled)?;

    let mut result = None;
    while let Some(chunk) = stream.fetch()? {
        if chunk.len() == 0 || chunk.column_count() == 0 {
            continue;
        }
        if result.is_some() {
            return Err(paro_error::internal(
                "EXECUTE parameter expression returned multiple rows".to_string(),
            ));
        }
        let column = chunk.column(0).expect("scalar query must have a column");
        result = Some(column.get_value(0));
    }

    result.ok_or_else(|| {
        paro_error::internal("EXECUTE parameter expression returned no rows".to_string())
    })
}
