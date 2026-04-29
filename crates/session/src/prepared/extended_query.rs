// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Session-owned extended query protocol handling shared by SQL prepared state and pgwire.

use async_trait::async_trait;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, ParoError, Result};
use paro_common::logging::targets;
use paro_common::runtime_value::Value;
use paro_common::types::{logical_type_from_pg_oid, LogicalType};
use paro_compiler::{compile_statement, compile_statement_with_parameters};
use paro_context::{StatementContext, StatementOptions, StatementSource};
use paro_execution::query_executor::compiled::{CompiledStatement, ResultColumnDesc};
use paro_execution::query_executor::executor::Executor;
use paro_parser::ast::{Statement, VariableShowStmt};
use std::sync::Arc;
use tracing::error;

use crate::completion::StatementCompletion;
use crate::completion_infer::infer_statement_completion;
use crate::copy_protocol::{CopyProtocolSink, CopyProtocolSource};
use crate::dispatch::{
    classify_statement, dispatch_statement, utility_command_from_statement, FrontendRoute,
    StatementClass,
};
use crate::prepared::binary_codec::{
    decode_binary_param, is_binary_recv_supported, is_binary_send_supported,
};
use crate::prepared::materialization::materialize_compiled_statement;
use crate::prepared::parameters::{
    bind_value_arguments, parse_text_parameter_value, placeholder_count, typed_null_parameter_env,
    typed_parameter_env_from_values,
};
use crate::prepared::portal::{
    CursorHoldability, ExecutionCursorHandle, FormatCode, PortalCursor, PortalExecutionState,
    PortalSnapshotRetention, ScrollMode,
};
use crate::prepared::store::{
    PortalEntry, PortalKind, PortalStatementRef, PreparedStatementEntry, PreparedStatementSource,
};
use crate::prepared::typed_parameters::TypedParameterEnv;
use crate::transaction::is_allowed_in_failed_transaction;
use crate::utility::execute_utility_command;
use crate::Session;

#[derive(Debug, Clone)]
pub enum ExtendedQueryMessage {
    Parse(ParseMessage),
    Bind(BindMessage),
    Describe(DescribeTarget),
    Execute(ExecutePortalMessage),
    Close(CloseTarget),
    Flush,
    Sync,
}

#[derive(Debug, Clone)]
pub struct ParseMessage {
    pub name: Option<String>,
    pub query: String,
    pub type_oids: Vec<u32>,
}

#[derive(Debug, Clone)]
pub struct BindMessage {
    pub portal_name: Option<String>,
    pub statement_name: Option<String>,
    pub parameter_format_codes: Vec<i16>,
    pub parameters: Vec<Option<Vec<u8>>>,
    pub result_column_format_codes: Vec<i16>,
}

#[derive(Debug, Clone)]
pub enum DescribeTarget {
    Statement(Option<String>),
    Portal(Option<String>),
}

#[derive(Debug, Clone)]
pub struct ExecutePortalMessage {
    pub name: Option<String>,
    pub max_rows: i32,
}

#[derive(Debug, Clone)]
pub enum CloseTarget {
    Statement(Option<String>),
    Portal(Option<String>),
}

#[async_trait]
pub trait ExtendedQueryResponder: Send {
    async fn send_parse_complete(&mut self) -> Result<()>;
    async fn send_bind_complete(&mut self) -> Result<()>;
    async fn send_parameter_description(
        &mut self,
        parameter_types: &[Option<LogicalType>],
    ) -> Result<()>;
    async fn send_row_description(
        &mut self,
        schema: &[ResultColumnDesc],
        format_codes: &[FormatCode],
    ) -> Result<()>;
    async fn send_data_chunk(
        &mut self,
        chunk: &Chunk,
        schema: &[ResultColumnDesc],
        format_codes: &[FormatCode],
    ) -> Result<()>;
    async fn send_command_complete(&mut self, completion: &StatementCompletion) -> Result<()>;
    async fn send_close_complete(&mut self) -> Result<()>;
    async fn send_no_data(&mut self) -> Result<()>;
    async fn send_empty_query_response(&mut self) -> Result<()>;
    async fn send_portal_suspended(&mut self) -> Result<()>;
    async fn send_error(&mut self, err: &ParoError) -> Result<()>;
    async fn flush(&mut self) -> Result<()>;

    fn create_copy_out_sink(
        &mut self,
        _options: &paro_function::copy::CopyOptions,
    ) -> Result<Box<dyn CopyProtocolSink + '_>> {
        Err(paro_error::not_supported(
            "COPY TO STDOUT is not available in this context",
        ))
    }

    fn create_copy_in_source(&mut self) -> Result<Box<dyn CopyProtocolSource + '_>> {
        Err(paro_error::not_supported(
            "COPY FROM STDIN is not available in this context",
        ))
    }
}

pub(crate) async fn execute_extended_query_message<R: ExtendedQueryResponder>(
    session: &mut Session,
    message: ExtendedQueryMessage,
    responder: &mut R,
) -> Result<()> {
    match message {
        ExtendedQueryMessage::Parse(message) => execute_parse(session, message, responder).await,
        ExtendedQueryMessage::Bind(message) => execute_bind(session, message, responder).await,
        ExtendedQueryMessage::Describe(target) => {
            execute_describe(session, target, responder).await
        }
        ExtendedQueryMessage::Execute(message) => execute_portal(session, message, responder).await,
        ExtendedQueryMessage::Close(target) => execute_close(session, target, responder).await,
        ExtendedQueryMessage::Flush => responder.flush().await,
        ExtendedQueryMessage::Sync => Ok(()),
    }
}

async fn execute_parse<R: ExtendedQueryResponder>(
    session: &mut Session,
    message: ParseMessage,
    responder: &mut R,
) -> Result<()> {
    let statements = paro_parser::parse(&message.query)
        .map_err(|error| paro_error::from_parser(error.to_string()))?;
    if statements.len() != 1 {
        return Err(paro_error::protocol_violation(
            "Parse expects exactly one SQL statement".to_string(),
        ));
    }

    let raw_stmt = statements
        .into_iter()
        .next()
        .expect("statement length checked")
        .stmt;
    let route = dispatch_statement(raw_stmt.clone());
    if matches!(classify_statement(&raw_stmt), StatementClass::Prepared) {
        return Err(paro_error::not_supported(
            "extended query protocol does not parse SQL prepared/cursor commands yet",
        ));
    }

    let parameter_types = resolve_parse_parameter_types(&raw_stmt, &message.type_oids)?;
    let (result_schema, generic_plan) = if is_client_copy(&raw_stmt) {
        (Vec::new(), None)
    } else {
        build_parse_artifacts(
            session,
            &message.query,
            &raw_stmt,
            &route,
            &message.type_oids,
        )?
    };

    let entry = PreparedStatementEntry {
        name: message.name.clone().unwrap_or_default(),
        source_sql: message.query,
        raw_stmt,
        parameter_types,
        result_schema,
        plan_cache_mode: crate::prepared::plan_cache::PlanCacheMode::Auto,
        generic_plan,
        custom_plan_executions: 0,
        dependency_epoch: session.transaction_visible_version(),
        compile_environment: session.compile_environment_key(),
        source: PreparedStatementSource::Protocol,
    };

    match message.name {
        Some(name) => {
            if session.state.has_prepared_statement(&name) {
                return Err(paro_error::catalog(format!(
                    "prepared statement \"{name}\" already exists",
                )));
            }
            session.state.add_prepared_statement(entry);
        }
        None => {
            session.state.set_unnamed_prepared_statement(entry);
        }
    }

    session.refresh_session_metadata();
    responder.send_parse_complete().await
}

async fn execute_bind<R: ExtendedQueryResponder>(
    session: &mut Session,
    message: BindMessage,
    responder: &mut R,
) -> Result<()> {
    let statement = statement_entry(session, message.statement_name.as_deref())?.clone();
    let parameter_env = decode_bind_parameters(
        &statement.parameter_types,
        &message.parameter_format_codes,
        &message.parameters,
    )?;
    let resolved_parameter_types = parameter_env.logical_types();
    let result_formats = validate_result_formats(
        &message.result_column_format_codes,
        &statement.result_schema,
    )?;
    let kind = match classify_statement(&statement.raw_stmt) {
        StatementClass::Prepared => {
            return Err(paro_error::not_supported(
                "extended query protocol does not execute SQL prepared/cursor commands yet",
            ))
        }
        StatementClass::Utility => {
            let bound_stmt = bind_value_arguments(
                &statement.raw_stmt,
                &parameter_env.values(),
                &resolved_parameter_types,
            )?;
            PortalKind::Utility(Box::new(utility_command_from_statement(bound_stmt)))
        }
        StatementClass::Query if is_client_copy(&statement.raw_stmt) => PortalKind::ClientCopy {
            stmt: Box::new(statement.raw_stmt.clone()),
            parameter_env: parameter_env.clone(),
        },
        StatementClass::Query => PortalKind::Query {
            parameter_env: parameter_env.clone(),
        },
    };

    if let Some(name) = message.portal_name.as_deref() {
        if session.state.has_portal(name) {
            return Err(paro_error::catalog(format!(
                "portal \"{name}\" already exists",
            )));
        }
    }

    let portal = PortalEntry {
        name: message.portal_name.clone().unwrap_or_default(),
        statement_ref: match message.statement_name.clone() {
            Some(name) => PortalStatementRef::Named(name),
            None => PortalStatementRef::Unnamed,
        },
        source_sql: statement.source_sql.clone(),
        raw_stmt: statement.raw_stmt.clone(),
        bound_params: parameter_env.values(),
        holdability: CursorHoldability::WithoutHold,
        scroll_mode: ScrollMode::Scroll,
        result_formats,
        result_schema: statement.result_schema.clone(),
        kind,
        execution_state: PortalExecutionState::Ready,
        snapshot_retention: None,
        completion: None,
        dependency_epoch: statement.dependency_epoch,
        created_generation: 0,
        transaction_owned: session.has_active_transaction(),
    };

    match message.portal_name {
        Some(_) => {
            session.state.add_portal(portal);
        }
        None => {
            session.state.set_unnamed_portal(portal);
        }
    }

    if let Some(stored) =
        named_or_unnamed_statement_entry_mut(session, message.statement_name.as_deref())
    {
        stored.parameter_types = resolved_parameter_types;
    }

    session.refresh_session_metadata();
    responder.send_bind_complete().await
}

async fn execute_describe<R: ExtendedQueryResponder>(
    session: &mut Session,
    target: DescribeTarget,
    responder: &mut R,
) -> Result<()> {
    match target {
        DescribeTarget::Statement(name) => {
            let statement = statement_entry(session, name.as_deref())?;
            responder
                .send_parameter_description(&statement.parameter_types)
                .await?;
            if statement.result_schema.is_empty() {
                responder.send_no_data().await
            } else {
                let format_codes = vec![FormatCode::Text; statement.result_schema.len()];
                responder
                    .send_row_description(&statement.result_schema, &format_codes)
                    .await
            }
        }
        DescribeTarget::Portal(name) => {
            let portal = portal_entry(session, name.as_deref())?;
            if portal.result_schema.is_empty() {
                responder.send_no_data().await
            } else {
                responder
                    .send_row_description(&portal.result_schema, &portal.result_formats)
                    .await
            }
        }
    }
}

async fn execute_portal<R: ExtendedQueryResponder>(
    session: &mut Session,
    message: ExecutePortalMessage,
    responder: &mut R,
) -> Result<()> {
    let mut portal = portal_entry(session, message.name.as_deref())?.clone();

    if session.is_transaction_failed() && !is_allowed_in_failed_transaction(&portal.raw_stmt) {
        return Err(paro_error::transaction_aborted());
    }

    let query_str = portal.source_sql.clone();
    session.begin_statement_scope(&query_str);

    let portal_kind = portal.kind.clone();
    if should_begin_implicit_transaction_for_portal(session, &portal_kind) {
        session.begin_implicit_transaction_block()?;
    }

    let result = match portal_kind {
        PortalKind::Compiled(compiled) => {
            execute_query_portal(
                session,
                &mut portal,
                Some(*compiled),
                None,
                &message,
                responder,
            )
            .await
        }
        PortalKind::Query { parameter_env } => {
            execute_query_portal(
                session,
                &mut portal,
                None,
                Some(parameter_env),
                &message,
                responder,
            )
            .await
        }
        PortalKind::Utility(cmd) => {
            execute_utility_portal(session, &mut portal, *cmd, responder).await
        }
        PortalKind::ClientCopy {
            stmt,
            parameter_env,
        } => {
            execute_client_copy_portal(session, &mut portal, *stmt, parameter_env, responder).await
        }
    };

    match &result {
        Ok(PortalProgress::Complete(completion)) => {
            session.finish_statement_scope(true);
            if !completion.is_transaction_control() {
                session.command_counter_increment();
            }
            overwrite_portal_entry(session, message.name.as_deref(), portal);
            session.refresh_session_metadata();
        }
        Ok(PortalProgress::Suspended) => {
            session.finish_statement_scope(true);
            overwrite_portal_entry(session, message.name.as_deref(), portal);
            session.refresh_session_metadata();
        }
        Err(error) => session.finish_statement_scope_with_error(error),
    }

    result.map(|_| ())
}

async fn execute_close<R: ExtendedQueryResponder>(
    session: &mut Session,
    target: CloseTarget,
    responder: &mut R,
) -> Result<()> {
    match target {
        CloseTarget::Statement(name) => match name.as_deref() {
            Some(name) => {
                let _ = session.state.remove_prepared_statement(name);
            }
            None => {
                let _ = session.state.remove_unnamed_prepared_statement();
            }
        },
        CloseTarget::Portal(name) => match name.as_deref() {
            Some(name) => {
                let _ = session.state.remove_portal(name);
            }
            None => {
                let _ = session.state.remove_unnamed_portal();
            }
        },
    }

    session.refresh_session_metadata();
    responder.send_close_complete().await
}

fn build_parse_artifacts(
    session: &Session,
    sql: &str,
    stmt: &Statement,
    route: &FrontendRoute,
    type_oids: &[u32],
) -> Result<(Vec<ResultColumnDesc>, Option<CompiledStatement>)> {
    match route {
        FrontendRoute::Query(_) => {
            let parameter_types = resolve_parse_parameter_types(stmt, type_oids)?;
            let snapshot = session.freeze_statement_context(
                StatementOptions {
                    source: StatementSource::ExtendedQuery,
                    ..StatementOptions::default()
                },
                session.compile_scope_cancellation(),
            );
            if parameter_types.is_empty() {
                let compiled = compile_statement(snapshot, stmt.clone())?;
                Ok((compiled.result_schema.clone(), Some(compiled)))
            } else {
                let compiled = build_query_plan(
                    snapshot,
                    stmt.clone(),
                    &typed_null_parameter_env(&parameter_types),
                )?;
                Ok((compiled.result_schema.clone(), None))
            }
        }
        FrontendRoute::Utility(cmd) => Ok((utility_result_schema(cmd), None)),
        FrontendRoute::Prepared(_) => Err(paro_error::not_supported(format!(
            "extended query Parse does not support statement \"{sql}\"",
        ))),
    }
}

fn utility_result_schema(cmd: &crate::dispatch::UtilityCommand) -> Vec<ResultColumnDesc> {
    match cmd {
        crate::dispatch::UtilityCommand::VariableShow(stmt) => describe_variable_show(stmt),
        _ => Vec::new(),
    }
}

fn build_query_plan(
    snapshot: Arc<StatementContext>,
    stmt: Statement,
    parameter_env: &TypedParameterEnv,
) -> Result<CompiledStatement> {
    let mut compiled = compile_statement_with_parameters(snapshot, stmt, parameter_env)?;
    compiled.parameter_types = parameter_env
        .logical_types()
        .into_iter()
        .map(|ty| ty.unwrap_or(LogicalType::Unknown))
        .collect();
    Ok(compiled)
}

fn resolve_parse_parameter_types(
    stmt: &Statement,
    type_oids: &[u32],
) -> Result<Vec<Option<LogicalType>>> {
    let placeholder_count = placeholder_count(stmt);
    if type_oids.len() > placeholder_count {
        return Err(paro_error::protocol_violation(format!(
            "Parse specified {} parameter types, but statement has {placeholder_count} parameters",
            type_oids.len()
        )));
    }

    let mut parameter_types = vec![None; placeholder_count];
    for (index, oid) in type_oids.iter().enumerate() {
        parameter_types[index] = logical_type_from_pg_oid(*oid)?;
    }
    Ok(parameter_types)
}

fn decode_bind_parameters(
    parameter_types: &[Option<LogicalType>],
    format_codes: &[i16],
    parameters: &[Option<Vec<u8>>],
) -> Result<TypedParameterEnv> {
    if parameter_types.len() != parameters.len() {
        return Err(paro_error::protocol_violation(format!(
            "Bind supplied {} parameters, but statement expects {}",
            parameters.len(),
            parameter_types.len()
        )));
    }

    let normalized_formats =
        validate_parameter_formats(parameter_types, format_codes, parameters.len())?;
    let mut bound = Vec::with_capacity(parameters.len());
    for (idx, value) in parameters.iter().enumerate() {
        match normalized_formats[idx] {
            FormatCode::Text => bound.push(parse_text_parameter_value(
                value.as_deref(),
                parameter_types.get(idx).and_then(|ty| ty.as_ref()),
            )?),
            FormatCode::Binary => match value {
                Some(bytes) => bound.push(decode_binary_param(
                    bytes,
                    parameter_types[idx]
                        .as_ref()
                        .expect("validated known binary type"),
                )?),
                None => bound.push(Value::Null(
                    parameter_types[idx].clone().unwrap_or(LogicalType::Unknown),
                )),
            },
        }
    }
    typed_parameter_env_from_values(parameter_types, &bound)
}

fn validate_parameter_formats(
    declared: &[Option<LogicalType>],
    raw_format_codes: &[i16],
    count: usize,
) -> Result<Vec<FormatCode>> {
    let formats = expand_format_codes(raw_format_codes, count, "parameter")?;
    for (idx, format) in formats.iter().enumerate() {
        if !matches!(format, FormatCode::Binary) {
            continue;
        }
        let Some(logical_type) = declared.get(idx).and_then(|ty| ty.as_ref()) else {
            return Err(paro_error::protocol_violation(format!(
                "binary parameter ${} requires a known type",
                idx + 1,
            )));
        };
        if !is_binary_recv_supported(logical_type) {
            return Err(paro_error::not_implemented(format!(
                "binary parameter format not supported for type {logical_type}",
            )));
        }
    }
    Ok(formats)
}

async fn execute_query_portal<R: ExtendedQueryResponder>(
    session: &mut Session,
    portal: &mut PortalEntry,
    compiled: Option<CompiledStatement>,
    parameter_env: Option<TypedParameterEnv>,
    message: &ExecutePortalMessage,
    responder: &mut R,
) -> Result<PortalProgress> {
    if let Some(compiled) = compiled.as_ref() {
        if !compiled.is_query() {
            return execute_non_row_query_portal(session, portal, compiled.clone(), responder)
                .await;
        }
    }

    if matches!(portal.execution_state, PortalExecutionState::Ready) {
        let snapshot = session.freeze_statement_context(
            StatementOptions {
                source: StatementSource::ExtendedQuery,
                ..StatementOptions::default()
            },
            session
                .current_statement_cancellation()
                .expect("portal execution requires an active statement scope"),
        );
        let compiled = match compiled {
            Some(compiled) => compiled,
            None => build_query_plan(
                snapshot.clone(),
                portal.raw_stmt.clone(),
                parameter_env
                    .as_ref()
                    .expect("query portal must carry bound parameters"),
            )?,
        };
        if !compiled.is_query() {
            return execute_non_row_query_portal(session, portal, compiled, responder).await;
        }
        let materialized =
            materialize_compiled_statement(session, snapshot.clone(), compiled).await?;
        portal.execution_state = PortalExecutionState::Active(PortalCursor {
            position: -1,
            execution: ExecutionCursorHandle::materialized(materialized),
        });
        portal.snapshot_retention = Some(PortalSnapshotRetention::materialized(
            snapshot.transaction_view().effective_read_ts(),
        ));
    }

    match &mut portal.execution_state {
        PortalExecutionState::Active(cursor) => {
            session.check_active_statement_cancellation()?;
            let direction = if message.max_rows <= 0 {
                paro_parser::ast::FetchDirection::ForwardAll
            } else {
                paro_parser::ast::FetchDirection::ForwardCount(message.max_rows as i64)
            };
            let outcome = cursor
                .execution
                .fetch(cursor.position, &direction, portal.scroll_mode, false)
                .map_err(paro_error::syntax)?;
            cursor.position = outcome.new_position;

            for chunk in &outcome.rows {
                responder
                    .send_data_chunk(chunk, &portal.result_schema, &portal.result_formats)
                    .await?;
            }

            if outcome.at_end {
                portal.execution_state = PortalExecutionState::Exhausted {
                    position: outcome.new_position,
                };
                let completion = infer_statement_completion(&portal.raw_stmt, outcome.moved_rows);
                responder.send_command_complete(&completion).await?;
                Ok(PortalProgress::Complete(completion))
            } else {
                responder.send_portal_suspended().await?;
                Ok(PortalProgress::Suspended)
            }
        }
        PortalExecutionState::Exhausted { .. } => {
            let completion = infer_statement_completion(&portal.raw_stmt, 0);
            responder.send_command_complete(&completion).await?;
            Ok(PortalProgress::Complete(completion))
        }
        PortalExecutionState::Ready => Err(paro_error::internal(
            "portal was not materialized".to_string(),
        )),
    }
}

async fn execute_non_row_query_portal<R: ExtendedQueryResponder>(
    session: &mut Session,
    portal: &mut PortalEntry,
    compiled: CompiledStatement,
    responder: &mut R,
) -> Result<PortalProgress> {
    if let Some(completion) = portal.completion.clone() {
        responder.send_command_complete(&completion).await?;
        return Ok(PortalProgress::Complete(completion));
    }

    let completion = run_non_row_compiled_statement(session, &portal.raw_stmt, compiled)?;
    portal.execution_state = PortalExecutionState::Exhausted { position: 0 };
    portal.completion = Some(completion.clone());
    responder.send_command_complete(&completion).await?;
    Ok(PortalProgress::Complete(completion))
}

async fn execute_utility_portal<R: ExtendedQueryResponder>(
    session: &mut Session,
    portal: &mut PortalEntry,
    cmd: crate::dispatch::UtilityCommand,
    responder: &mut R,
) -> Result<PortalProgress> {
    if let Some(completion) = portal.completion.clone() {
        responder.send_command_complete(&completion).await?;
        return Ok(PortalProgress::Complete(completion));
    }

    if cmd.starts_explicit_transaction() && session.is_in_implicit_block() {
        session.end_implicit_transaction_block()?;
    }

    let mut sink = ResponderSink::new(responder, &portal.result_schema, &portal.result_formats);
    execute_utility_command(session, cmd, &mut sink).await?;
    let completion = sink
        .last_completion()
        .cloned()
        .unwrap_or(StatementCompletion::Empty);
    portal.execution_state = PortalExecutionState::Exhausted { position: 0 };
    portal.completion = Some(completion.clone());
    Ok(PortalProgress::Complete(completion))
}

fn run_non_row_compiled_statement(
    session: &mut Session,
    stmt: &Statement,
    compiled: CompiledStatement,
) -> Result<StatementCompletion> {
    let snapshot = session.freeze_statement_context(
        StatementOptions {
            source: StatementSource::ExtendedQuery,
            ..StatementOptions::default()
        },
        session
            .current_statement_cancellation()
            .expect("portal execution requires an active statement scope"),
    );
    let executor = Executor::new(snapshot);
    session.set_executor(executor);

    let mut stream = session.get_executor().execute(compiled).map_err(|err| {
        error!(
            target: targets::EXECUTOR,
            session_id = session.id,
            error = %err,
            stage = "extended_query",
            "Extended query execution failed"
        );
        err
    })?;

    let mut rows = 0usize;
    while let Some(chunk) = stream.fetch()? {
        if chunk.len() > 0 && chunk.column_count() > 0 {
            if let Some(col) = chunk.column(0) {
                let value = col.get_value(0);
                if let paro_common::runtime_value::Value::BigInt(count) = value {
                    rows = count as usize;
                }
            }
        }
    }

    Ok(infer_statement_completion(stmt, rows))
}

fn validate_result_formats(
    raw_format_codes: &[i16],
    schema: &[ResultColumnDesc],
) -> Result<Vec<FormatCode>> {
    let formats = expand_format_codes(raw_format_codes, schema.len(), "result column")?;
    for (idx, (format, column)) in formats.iter().zip(schema).enumerate() {
        if matches!(format, FormatCode::Binary) && !is_binary_send_supported(&column.logical_type) {
            return Err(paro_error::not_implemented(format!(
                "binary result format not supported for column {} (type {})",
                idx + 1,
                column.logical_type,
            )));
        }
    }
    Ok(formats)
}

fn expand_format_codes(format_codes: &[i16], count: usize, label: &str) -> Result<Vec<FormatCode>> {
    let codes = match format_codes {
        [] => vec![FormatCode::Text; count],
        [single] => vec![decode_format_code(*single)?; count],
        many if count == 0 => many
            .iter()
            .map(|code| decode_format_code(*code))
            .collect::<Result<Vec<_>>>()?,
        many if many.len() == count => many
            .iter()
            .map(|code| decode_format_code(*code))
            .collect::<Result<Vec<_>>>()?,
        many => {
            return Err(paro_error::protocol_violation(format!(
                "{label} format code count {} does not match expected count {count}",
                many.len()
            )))
        }
    };
    Ok(codes)
}

fn decode_format_code(code: i16) -> Result<FormatCode> {
    match code {
        0 => Ok(FormatCode::Text),
        1 => Ok(FormatCode::Binary),
        other => Err(paro_error::protocol_violation(format!(
            "unsupported format code {other}",
        ))),
    }
}

fn statement_entry<'a>(
    session: &'a Session,
    name: Option<&str>,
) -> Result<&'a PreparedStatementEntry> {
    match name {
        Some(name) => session.state.get_prepared_statement(name).ok_or_else(|| {
            paro_error::catalog(format!("prepared statement \"{name}\" does not exist"))
        }),
        None => session.state.unnamed_prepared_statement().ok_or_else(|| {
            paro_error::catalog("unnamed prepared statement does not exist".to_string())
        }),
    }
}

fn portal_entry<'a>(session: &'a Session, name: Option<&str>) -> Result<&'a PortalEntry> {
    match name {
        Some(name) => session
            .state
            .get_portal(name)
            .ok_or_else(|| paro_error::catalog(format!("portal \"{name}\" does not exist"))),
        None => session
            .state
            .unnamed_portal()
            .ok_or_else(|| paro_error::catalog("unnamed portal does not exist".to_string())),
    }
}

fn overwrite_portal_entry(session: &mut Session, name: Option<&str>, portal: PortalEntry) {
    match name {
        Some(name) => {
            if let Some(existing) = session.state.get_portal_mut(name) {
                *existing = portal;
            }
        }
        None => {
            if let Some(existing) = session.state.unnamed_portal_mut() {
                *existing = portal;
            }
        }
    }
}

fn named_or_unnamed_statement_entry_mut<'a>(
    session: &'a mut Session,
    name: Option<&str>,
) -> Option<&'a mut PreparedStatementEntry> {
    match name {
        Some(name) => session.state.get_prepared_statement_mut(name),
        None => session.state.unnamed_prepared_statement_mut(),
    }
}

fn should_begin_implicit_transaction_for_portal(session: &Session, kind: &PortalKind) -> bool {
    !session.has_active_transaction()
        && session.is_auto_commit()
        && matches!(
            kind,
            PortalKind::Compiled(_) | PortalKind::Query { .. } | PortalKind::ClientCopy { .. }
        )
}

async fn execute_client_copy_portal<R: ExtendedQueryResponder>(
    session: &mut Session,
    portal: &mut PortalEntry,
    stmt: Statement,
    parameter_env: TypedParameterEnv,
    responder: &mut R,
) -> Result<PortalProgress> {
    if let Some(completion) = portal.completion.clone() {
        responder.send_command_complete(&completion).await?;
        return Ok(PortalProgress::Complete(completion));
    }

    let Statement::Copy(copy_stmt) = stmt else {
        return Err(paro_error::internal(
            "client COPY portal missing COPY statement".to_string(),
        ));
    };

    let completion = match (&copy_stmt.direction, &copy_stmt.source) {
        (paro_parser::ast::CopyDirection::To, paro_parser::ast::CopySource::Stdout) => {
            let options = paro_function::copy::CopyOptions::from_ast(&copy_stmt.options)?;
            let query_stmt = crate::execute::build_copy_to_query_statement(&copy_stmt)?;
            {
                let mut copy_sink = responder.create_copy_out_sink(&options)?;
                session
                    .execute_copy_to_core(
                        query_stmt,
                        Some(&parameter_env),
                        None,
                        StatementSource::ExtendedQuery,
                        &mut *copy_sink,
                    )
                    .await?
            }
        }
        (paro_parser::ast::CopyDirection::From, paro_parser::ast::CopySource::Stdin) => {
            let mut copy_source = responder.create_copy_in_source()?;
            session
                .execute_copy_from_core(
                    &copy_stmt,
                    Some(&parameter_env),
                    None,
                    StatementSource::ExtendedQuery,
                    &mut *copy_source,
                )
                .await?
        }
        _ => unreachable!("file-backed COPY should not use client COPY portal kind"),
    };

    portal.execution_state = PortalExecutionState::Exhausted { position: 0 };
    portal.completion = Some(completion.clone());
    responder.send_command_complete(&completion).await?;
    Ok(PortalProgress::Complete(completion))
}

fn is_client_copy(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Copy(copy)
            if matches!(
                (&copy.direction, &copy.source),
                (paro_parser::ast::CopyDirection::To, paro_parser::ast::CopySource::Stdout)
                    | (paro_parser::ast::CopyDirection::From, paro_parser::ast::CopySource::Stdin)
            )
    )
}

enum PortalProgress {
    Complete(StatementCompletion),
    Suspended,
}

struct ResponderSink<'a, R> {
    responder: &'a mut R,
    schema: &'a [ResultColumnDesc],
    format_codes: &'a [FormatCode],
    completion: Option<StatementCompletion>,
}

impl<'a, R> ResponderSink<'a, R> {
    fn new(
        responder: &'a mut R,
        schema: &'a [ResultColumnDesc],
        format_codes: &'a [FormatCode],
    ) -> Self {
        Self {
            responder,
            schema,
            format_codes,
            completion: None,
        }
    }

    fn last_completion(&self) -> Option<&StatementCompletion> {
        self.completion.as_ref()
    }
}

#[async_trait]
impl<R: ExtendedQueryResponder> crate::result::sink::ResultSink for ResponderSink<'_, R> {
    async fn start_result(&mut self, _names: &[String], _types: &[LogicalType]) -> Result<()> {
        Ok(())
    }

    async fn push_chunk(&mut self, chunk: &Chunk) -> Result<()> {
        self.responder
            .send_data_chunk(chunk, self.schema, self.format_codes)
            .await
    }

    async fn finish_result(&mut self, completion: &StatementCompletion) -> Result<()> {
        self.completion = Some(completion.clone());
        self.responder.send_command_complete(completion).await
    }

    async fn error(&mut self, err: &ParoError) -> Result<()> {
        self.responder.send_error(err).await
    }
}

fn describe_variable_show(stmt: &VariableShowStmt) -> Vec<ResultColumnDesc> {
    crate::utility::settings::describe_variable_show(stmt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::UtilityCommand;
    use crate::result::collecting_sink::CollectingSink;
    use async_trait::async_trait;
    use paro_common::runtime_value::Value;
    use paro_common::types::pg_oid::{INT4OID, NUMERICOID};
    use tokio_util::bytes::Bytes;

    #[derive(Default)]
    struct TestResponder {
        events: Vec<String>,
        rows: Vec<Vec<String>>,
        copy_out_rows: usize,
        copy_in_spec: Option<crate::CopyInSpec>,
        copy_in_payload: Vec<Bytes>,
        last_row_formats: Vec<FormatCode>,
    }

    #[async_trait]
    impl ExtendedQueryResponder for TestResponder {
        async fn send_parse_complete(&mut self) -> Result<()> {
            self.events.push("parse_complete".to_string());
            Ok(())
        }

        async fn send_bind_complete(&mut self) -> Result<()> {
            self.events.push("bind_complete".to_string());
            Ok(())
        }

        async fn send_parameter_description(
            &mut self,
            parameter_types: &[Option<LogicalType>],
        ) -> Result<()> {
            self.events
                .push(format!("param_desc:{}", parameter_types.len()));
            Ok(())
        }

        async fn send_row_description(
            &mut self,
            schema: &[ResultColumnDesc],
            format_codes: &[FormatCode],
        ) -> Result<()> {
            self.events.push(format!("row_desc:{}", schema.len()));
            self.last_row_formats = format_codes.to_vec();
            Ok(())
        }

        async fn send_data_chunk(
            &mut self,
            chunk: &Chunk,
            _schema: &[ResultColumnDesc],
            _format_codes: &[FormatCode],
        ) -> Result<()> {
            for row_idx in 0..chunk.len() {
                let mut row = Vec::new();
                for col_idx in 0..chunk.column_count() {
                    let vector = chunk.column(col_idx).expect("column exists");
                    row.push(vector.get_value(row_idx).to_string());
                }
                self.rows.push(row);
            }
            Ok(())
        }

        async fn send_command_complete(&mut self, completion: &StatementCompletion) -> Result<()> {
            self.events.push(format!("complete:{completion}"));
            Ok(())
        }

        async fn send_close_complete(&mut self) -> Result<()> {
            self.events.push("close_complete".to_string());
            Ok(())
        }

        async fn send_no_data(&mut self) -> Result<()> {
            self.events.push("no_data".to_string());
            Ok(())
        }

        async fn send_empty_query_response(&mut self) -> Result<()> {
            self.events.push("empty".to_string());
            Ok(())
        }

        async fn send_portal_suspended(&mut self) -> Result<()> {
            self.events.push("portal_suspended".to_string());
            Ok(())
        }

        async fn send_error(&mut self, err: &ParoError) -> Result<()> {
            self.events.push(format!("error:{}", err.message()));
            Ok(())
        }

        async fn flush(&mut self) -> Result<()> {
            self.events.push("flush".to_string());
            Ok(())
        }

        fn create_copy_out_sink(
            &mut self,
            _options: &paro_function::copy::CopyOptions,
        ) -> Result<Box<dyn CopyProtocolSink + '_>> {
            self.events.push("copy_out_sink".to_string());
            Ok(Box::new(TestCopyOutSink { responder: self }))
        }

        fn create_copy_in_source(&mut self) -> Result<Box<dyn CopyProtocolSource + '_>> {
            self.events.push("copy_in_source".to_string());
            Ok(Box::new(TestCopyInSource { responder: self }))
        }
    }

    struct TestCopyOutSink<'a> {
        responder: &'a mut TestResponder,
    }

    #[async_trait]
    impl CopyProtocolSink for TestCopyOutSink<'_> {
        async fn start_copy_out(
            &mut self,
            _names: &[String],
            _types: &[LogicalType],
        ) -> Result<()> {
            self.responder.events.push("copy_out_start".to_string());
            Ok(())
        }

        async fn push_copy_rows(&mut self, chunk: &Chunk) -> Result<()> {
            self.responder.copy_out_rows += chunk.len();
            Ok(())
        }

        async fn finish_copy_out(&mut self) -> Result<()> {
            self.responder.events.push("copy_out_done".to_string());
            Ok(())
        }
    }

    struct TestCopyInSource<'a> {
        responder: &'a mut TestResponder,
    }

    #[async_trait]
    impl CopyProtocolSource for TestCopyInSource<'_> {
        async fn begin_copy_in(&mut self, spec: &crate::CopyInSpec) -> Result<()> {
            self.responder.copy_in_spec = Some(spec.clone());
            Ok(())
        }

        async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
            if self.responder.copy_in_payload.is_empty() {
                return Ok(None);
            }
            Ok(Some(self.responder.copy_in_payload.remove(0)))
        }
    }

    async fn exec_simple_ok(session: &mut Session, sink: &mut CollectingSink, sql: &str) {
        let result = session.execute_simple_query(sql, sink).await;
        assert!(
            result.is_ok(),
            "simple query should succeed: {sql}: {result:?}"
        );
        assert!(
            !sink.has_errors(),
            "simple query should not emit errors: {:?}",
            sink.errors()
        );
    }

    async fn run_named_statement_and_portal_support_row_limited_execute() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = Session::new(1, instance);
        let mut responder = TestResponder::default();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Parse(ParseMessage {
                name: Some("s1".to_string()),
                query:
                    "SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3) t ORDER BY v"
                        .to_string(),
                type_oids: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Bind(BindMessage {
                portal_name: Some("p1".to_string()),
                statement_name: Some("s1".to_string()),
                parameter_format_codes: Vec::new(),
                parameters: Vec::new(),
                result_column_format_codes: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Execute(ExecutePortalMessage {
                name: Some("p1".to_string()),
                max_rows: 2,
            }),
            &mut responder,
        )
        .await
        .unwrap();

        assert_eq!(
            responder.rows,
            vec![vec!["1".to_string()], vec!["2".to_string()]]
        );
        assert!(responder
            .events
            .iter()
            .any(|event| event == "portal_suspended"));

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Execute(ExecutePortalMessage {
                name: Some("p1".to_string()),
                max_rows: 0,
            }),
            &mut responder,
        )
        .await
        .unwrap();

        assert_eq!(responder.rows.len(), 3);
        assert!(responder
            .events
            .iter()
            .any(|event| event == "complete:SELECT 1"));
    }

    #[test]
    fn named_statement_and_portal_support_row_limited_execute() {
        std::thread::Builder::new()
            .name("session-row-limited-execute".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build current-thread runtime");
                runtime.block_on(run_named_statement_and_portal_support_row_limited_execute());
            })
            .expect("spawn large-stack test thread")
            .join()
            .expect("join large-stack test thread");
    }

    #[tokio::test]
    async fn utility_show_uses_protocol_responder() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = Session::new(1, instance);
        session
            .config
            .set_setting("application_name", Value::Varchar("proto".to_string()));
        crate::utility::settings::reconcile_effective_settings(&mut session).unwrap();

        let mut responder = TestResponder::default();
        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Parse(ParseMessage {
                name: Some("show_stmt".to_string()),
                query: "SHOW application_name".to_string(),
                type_oids: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();

        let statement = statement_entry(&session, Some("show_stmt")).unwrap();
        assert_eq!(
            statement.result_schema,
            utility_result_schema(&UtilityCommand::VariableShow(
                match statement.raw_stmt.clone() {
                    Statement::VariableShow(stmt) => stmt,
                    other => panic!("expected show statement, got {other:?}"),
                }
            ))
        );
    }

    #[tokio::test]
    async fn protocol_bind_defers_query_snapshot_until_execute() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = Session::new(1, instance);
        let mut responder = TestResponder::default();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Parse(ParseMessage {
                name: Some("s1".to_string()),
                query: "SELECT ? + 1".to_string(),
                type_oids: vec![INT4OID],
            }),
            &mut responder,
        )
        .await
        .unwrap();
        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Bind(BindMessage {
                portal_name: Some("p1".to_string()),
                statement_name: Some("s1".to_string()),
                parameter_format_codes: Vec::new(),
                parameters: vec![Some(b"41".to_vec())],
                result_column_format_codes: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();

        let portal = portal_entry(&session, Some("p1")).unwrap();
        assert!(matches!(portal.kind, PortalKind::Query { .. }));
        assert!(matches!(
            portal.execution_state,
            PortalExecutionState::Ready
        ));

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Execute(ExecutePortalMessage {
                name: Some("p1".to_string()),
                max_rows: 0,
            }),
            &mut responder,
        )
        .await
        .unwrap();

        assert_eq!(responder.rows, vec![vec!["42".to_string()]]);
    }

    #[tokio::test]
    async fn unnamed_statement_and_portal_support_describe_close_and_flush() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = Session::new(1, instance);
        session
            .config
            .set_setting("application_name", Value::Varchar("proto".to_string()));
        crate::utility::settings::reconcile_effective_settings(&mut session).unwrap();

        let mut responder = TestResponder::default();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Parse(ParseMessage {
                name: None,
                query: "SHOW application_name".to_string(),
                type_oids: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();
        assert!(session.state.unnamed_prepared_statement().is_some());

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Describe(DescribeTarget::Statement(None)),
            &mut responder,
        )
        .await
        .unwrap();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Bind(BindMessage {
                portal_name: None,
                statement_name: None,
                parameter_format_codes: Vec::new(),
                parameters: Vec::new(),
                result_column_format_codes: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();
        assert!(session.state.unnamed_portal().is_some());

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Describe(DescribeTarget::Portal(None)),
            &mut responder,
        )
        .await
        .unwrap();

        execute_extended_query_message(&mut session, ExtendedQueryMessage::Flush, &mut responder)
            .await
            .unwrap();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Close(CloseTarget::Portal(None)),
            &mut responder,
        )
        .await
        .unwrap();
        assert!(session.state.unnamed_portal().is_none());

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Close(CloseTarget::Statement(None)),
            &mut responder,
        )
        .await
        .unwrap();
        assert!(session.state.unnamed_prepared_statement().is_none());

        assert_eq!(
            responder.events,
            vec![
                "parse_complete".to_string(),
                "param_desc:0".to_string(),
                "row_desc:1".to_string(),
                "bind_complete".to_string(),
                "row_desc:1".to_string(),
                "flush".to_string(),
                "close_complete".to_string(),
                "close_complete".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn extended_copy_to_stdout_executes_once_and_caches_completion() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = Session::new(1, instance);
        let mut sink = CollectingSink::new();
        exec_simple_ok(&mut session, &mut sink, "CREATE TABLE ext_copy_out (v INT)").await;
        exec_simple_ok(
            &mut session,
            &mut sink,
            "INSERT INTO ext_copy_out VALUES (1), (2)",
        )
        .await;

        let mut responder = TestResponder::default();
        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Parse(ParseMessage {
                name: Some("copy_stmt".to_string()),
                query: "COPY ext_copy_out TO STDOUT WITH (FORMAT csv)".to_string(),
                type_oids: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();
        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Bind(BindMessage {
                portal_name: Some("copy_portal".to_string()),
                statement_name: Some("copy_stmt".to_string()),
                parameter_format_codes: Vec::new(),
                parameters: Vec::new(),
                result_column_format_codes: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();
        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Execute(ExecutePortalMessage {
                name: Some("copy_portal".to_string()),
                max_rows: 0,
            }),
            &mut responder,
        )
        .await
        .unwrap();
        assert_eq!(responder.copy_out_rows, 2);
        assert!(responder
            .events
            .iter()
            .any(|event| event == "copy_out_done"));
        assert!(responder
            .events
            .iter()
            .any(|event| event == "complete:COPY 2"));

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Execute(ExecutePortalMessage {
                name: Some("copy_portal".to_string()),
                max_rows: 0,
            }),
            &mut responder,
        )
        .await
        .unwrap();
        assert_eq!(responder.copy_out_rows, 2);
    }

    #[tokio::test]
    async fn extended_copy_from_stdin_uses_copy_source_and_reports_real_column_count() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = Session::new(1, instance);
        let mut sink = CollectingSink::new();
        exec_simple_ok(&mut session, &mut sink, "CREATE TABLE ext_copy_in (v INT)").await;

        let mut responder = TestResponder {
            copy_in_payload: vec![Bytes::from("1\n2\n")],
            ..Default::default()
        };
        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Parse(ParseMessage {
                name: Some("copy_in_stmt".to_string()),
                query: "COPY ext_copy_in FROM STDIN WITH (FORMAT csv)".to_string(),
                type_oids: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();
        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Bind(BindMessage {
                portal_name: Some("copy_in_portal".to_string()),
                statement_name: Some("copy_in_stmt".to_string()),
                parameter_format_codes: Vec::new(),
                parameters: Vec::new(),
                result_column_format_codes: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();
        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Execute(ExecutePortalMessage {
                name: Some("copy_in_portal".to_string()),
                max_rows: 0,
            }),
            &mut responder,
        )
        .await
        .unwrap();

        assert_eq!(
            responder.copy_in_spec,
            Some(crate::CopyInSpec {
                overall_format: 0,
                column_formats: vec![0],
            })
        );
        assert!(responder
            .events
            .iter()
            .any(|event| event == "complete:COPY 2"));
        if session.is_in_implicit_block() {
            session.end_implicit_transaction_block().unwrap();
        }

        sink.clear();
        exec_simple_ok(
            &mut session,
            &mut sink,
            "SELECT v FROM ext_copy_in ORDER BY v",
        )
        .await;
        let result = sink.assert_single_result();
        assert_eq!(result.total_rows(), 2);
    }

    #[tokio::test]
    async fn bind_supports_text_parameters_and_updates_statement_types() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = Session::new(1, instance);
        let mut responder = TestResponder::default();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Parse(ParseMessage {
                name: Some("s1".to_string()),
                query: "SELECT ? + 1".to_string(),
                type_oids: vec![INT4OID],
            }),
            &mut responder,
        )
        .await
        .unwrap();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Bind(BindMessage {
                portal_name: Some("p1".to_string()),
                statement_name: Some("s1".to_string()),
                parameter_format_codes: vec![0],
                parameters: vec![Some(b"41".to_vec())],
                result_column_format_codes: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Execute(ExecutePortalMessage {
                name: Some("p1".to_string()),
                max_rows: 0,
            }),
            &mut responder,
        )
        .await
        .unwrap();

        assert_eq!(responder.rows, vec![vec!["42".to_string()]]);
        assert_eq!(
            statement_entry(&session, Some("s1"))
                .unwrap()
                .parameter_types,
            vec![Some(LogicalType::Integer)]
        );
    }

    #[tokio::test]
    async fn bind_supports_binary_parameters_for_known_types() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = Session::new(1, instance);
        let mut responder = TestResponder::default();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Parse(ParseMessage {
                name: Some("s1".to_string()),
                query: "SELECT ? + 1".to_string(),
                type_oids: vec![INT4OID],
            }),
            &mut responder,
        )
        .await
        .unwrap();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Bind(BindMessage {
                portal_name: Some("p1".to_string()),
                statement_name: Some("s1".to_string()),
                parameter_format_codes: vec![1],
                parameters: vec![Some(41_i32.to_be_bytes().to_vec())],
                result_column_format_codes: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Execute(ExecutePortalMessage {
                name: Some("p1".to_string()),
                max_rows: 0,
            }),
            &mut responder,
        )
        .await
        .unwrap();

        assert_eq!(responder.rows, vec![vec!["42".to_string()]]);
    }

    #[tokio::test]
    async fn describe_portal_uses_bound_result_formats() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = Session::new(1, instance);
        let mut responder = TestResponder::default();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Parse(ParseMessage {
                name: Some("s1".to_string()),
                query: "SELECT ?".to_string(),
                type_oids: vec![INT4OID],
            }),
            &mut responder,
        )
        .await
        .unwrap();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Bind(BindMessage {
                portal_name: Some("p1".to_string()),
                statement_name: Some("s1".to_string()),
                parameter_format_codes: vec![0],
                parameters: vec![Some(b"1".to_vec())],
                result_column_format_codes: vec![1],
            }),
            &mut responder,
        )
        .await
        .unwrap();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Describe(DescribeTarget::Portal(Some("p1".to_string()))),
            &mut responder,
        )
        .await
        .unwrap();

        assert_eq!(responder.last_row_formats, vec![FormatCode::Binary]);
    }

    #[tokio::test]
    async fn repeated_binds_with_typed_parameters_do_not_leak_old_values() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = Session::new(1, instance);
        let mut responder = TestResponder::default();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Parse(ParseMessage {
                name: Some("s1".to_string()),
                query: "SELECT ? + 1".to_string(),
                type_oids: vec![INT4OID],
            }),
            &mut responder,
        )
        .await
        .unwrap();

        assert!(statement_entry(&session, Some("s1"))
            .unwrap()
            .generic_plan
            .is_none());

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Bind(BindMessage {
                portal_name: Some("p1".to_string()),
                statement_name: Some("s1".to_string()),
                parameter_format_codes: vec![0],
                parameters: vec![Some(b"1".to_vec())],
                result_column_format_codes: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();
        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Execute(ExecutePortalMessage {
                name: Some("p1".to_string()),
                max_rows: 0,
            }),
            &mut responder,
        )
        .await
        .unwrap();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Bind(BindMessage {
                portal_name: Some("p2".to_string()),
                statement_name: Some("s1".to_string()),
                parameter_format_codes: vec![0],
                parameters: vec![Some(b"41".to_vec())],
                result_column_format_codes: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();
        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Execute(ExecutePortalMessage {
                name: Some("p2".to_string()),
                max_rows: 0,
            }),
            &mut responder,
        )
        .await
        .unwrap();

        assert_eq!(
            responder.rows,
            vec![vec!["2".to_string()], vec!["42".to_string()]]
        );
    }

    #[tokio::test]
    async fn close_statement_cascades_to_bound_portals() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = Session::new(1, instance);
        let mut responder = TestResponder::default();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Parse(ParseMessage {
                name: Some("s1".to_string()),
                query: "SELECT 1".to_string(),
                type_oids: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Bind(BindMessage {
                portal_name: Some("p1".to_string()),
                statement_name: Some("s1".to_string()),
                parameter_format_codes: Vec::new(),
                parameters: Vec::new(),
                result_column_format_codes: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();
        assert!(session.state.get_portal("p1").is_some());

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Close(CloseTarget::Statement(Some("s1".to_string()))),
            &mut responder,
        )
        .await
        .unwrap();

        assert!(session.state.get_prepared_statement("s1").is_none());
        assert!(session.state.get_portal("p1").is_none());
    }

    #[tokio::test]
    async fn bind_validates_binary_parameter_and_result_formats() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = Session::new(1, instance);
        let mut responder = TestResponder::default();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Parse(ParseMessage {
                name: Some("s1".to_string()),
                query: "SELECT ?".to_string(),
                type_oids: vec![NUMERICOID],
            }),
            &mut responder,
        )
        .await
        .unwrap();

        let err = execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Bind(BindMessage {
                portal_name: Some("p1".to_string()),
                statement_name: Some("s1".to_string()),
                parameter_format_codes: vec![1],
                parameters: vec![Some(1_i64.to_be_bytes().to_vec())],
                result_column_format_codes: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap_err();
        assert!(err
            .message()
            .contains("binary parameter format not supported"));

        let err = execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Bind(BindMessage {
                portal_name: Some("p2".to_string()),
                statement_name: Some("s1".to_string()),
                parameter_format_codes: Vec::new(),
                parameters: vec![Some(b"1.25".to_vec())],
                result_column_format_codes: vec![1],
            }),
            &mut responder,
        )
        .await
        .unwrap_err();
        assert!(err.message().contains("binary result format not supported"));
    }

    #[tokio::test]
    async fn simple_query_clears_protocol_unnamed_objects() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = Session::new(1, instance);
        let mut responder = TestResponder::default();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Parse(ParseMessage {
                name: None,
                query: "SELECT 1".to_string(),
                type_oids: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();
        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Bind(BindMessage {
                portal_name: None,
                statement_name: None,
                parameter_format_codes: Vec::new(),
                parameters: Vec::new(),
                result_column_format_codes: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();

        assert!(session.state.unnamed_prepared_statement().is_some());
        assert!(session.state.unnamed_portal().is_some());

        let mut sink = CollectingSink::new();
        session
            .execute_simple_query("SELECT 1", &mut sink)
            .await
            .unwrap();

        assert!(session.state.unnamed_prepared_statement().is_none());
        assert!(session.state.unnamed_portal().is_none());
    }

    #[tokio::test]
    async fn create_database_runs_before_implicit_transaction_but_not_inside_one() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = Session::new(1, instance);
        let mut responder = TestResponder::default();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Parse(ParseMessage {
                name: Some("create_stmt".to_string()),
                query: "CREATE DATABASE ext_created".to_string(),
                type_oids: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();
        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Bind(BindMessage {
                portal_name: Some("create_portal".to_string()),
                statement_name: Some("create_stmt".to_string()),
                parameter_format_codes: Vec::new(),
                parameters: Vec::new(),
                result_column_format_codes: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();
        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Execute(ExecutePortalMessage {
                name: Some("create_portal".to_string()),
                max_rows: 0,
            }),
            &mut responder,
        )
        .await
        .unwrap();

        assert!(session
            .instance
            .database_registry()
            .get_database("ext_created")
            .is_some());
        assert!(!session.has_active_transaction());

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Parse(ParseMessage {
                name: Some("query_stmt".to_string()),
                query: "SELECT 1".to_string(),
                type_oids: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();
        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Bind(BindMessage {
                portal_name: Some("query_portal".to_string()),
                statement_name: Some("query_stmt".to_string()),
                parameter_format_codes: Vec::new(),
                parameters: Vec::new(),
                result_column_format_codes: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();
        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Execute(ExecutePortalMessage {
                name: Some("query_portal".to_string()),
                max_rows: 0,
            }),
            &mut responder,
        )
        .await
        .unwrap();
        assert!(session.is_in_implicit_block());

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Parse(ParseMessage {
                name: Some("create_in_txn_stmt".to_string()),
                query: "CREATE DATABASE ext_blocked".to_string(),
                type_oids: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();
        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Bind(BindMessage {
                portal_name: Some("create_in_txn_portal".to_string()),
                statement_name: Some("create_in_txn_stmt".to_string()),
                parameter_format_codes: Vec::new(),
                parameters: Vec::new(),
                result_column_format_codes: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();

        let err = execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Execute(ExecutePortalMessage {
                name: Some("create_in_txn_portal".to_string()),
                max_rows: 0,
            }),
            &mut responder,
        )
        .await
        .unwrap_err();
        assert!(err
            .message()
            .contains("cannot run inside a transaction block"));
    }
}
