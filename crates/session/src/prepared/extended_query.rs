// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Session-owned extended query protocol handling shared by SQL prepared state and pgwire.

use async_trait::async_trait;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, ParoError, Result};
use paro_common::logging::targets;
use paro_common::runtime_value::Value;
use paro_common::types::{logical_type_from_pg_oid, LogicalType};
use paro_compiler::compile_statement_with_parameter_types;
use paro_context::{
    statement_fingerprint, StatementCancellation, StatementContext, StatementOptions,
    StatementSource, StatementTrace,
};
use paro_execution::query_executor::compiled::{
    CompiledStatement, ExecutionRequest, ResultColumnDesc,
};
use paro_execution::query_executor::executor::Executor;
use paro_parser::ast::{Expr, Statement, VariableShowStmt};
use paro_parser::StatementVisitor;
use paro_planner::binder::bind::type_name::bind_logical_type;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error};

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
    bind_value_arguments, parse_text_parameter_value, placeholder_count,
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
        _cancellation: &StatementCancellation,
        _options: &paro_function::copy::CopyOptions,
    ) -> Result<Box<dyn CopyProtocolSink + '_>> {
        Err(paro_error::not_supported(
            "COPY TO STDOUT is not available in this context",
        ))
    }

    fn create_copy_in_source(
        &mut self,
        _cancellation: &StatementCancellation,
    ) -> Result<Box<dyn CopyProtocolSource + '_>> {
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
    let parse_started = Instant::now();
    let statement_trace = session.new_statement_trace(&message.query, 0, parse_started);
    if let Some(trace) = &statement_trace {
        trace.record_event("protocol", "parse_entry");
    }
    let result = execute_parse_inner(session, message, responder, statement_trace.clone()).await;
    if let Some(trace) = statement_trace {
        trace.record_span("protocol", "parse_operation", parse_started);
        if result.is_err() {
            trace.record_event("protocol", "parse_error");
            trace.record_event("lifecycle", "statement_error");
            session.publish_statement_trace(trace.snapshot());
        }
    }
    result
}

async fn execute_parse_inner<R: ExtendedQueryResponder>(
    session: &mut Session,
    message: ParseMessage,
    responder: &mut R,
    statement_trace: Option<Arc<StatementTrace>>,
) -> Result<()> {
    // The unnamed statement is replaced on every Parse, but an exact,
    // parameter-free repeat does not need to rebuild even its immutable AST.
    // Check the byte-identical SQL and full compile environment before
    // entering the parser; parameterized statements retain the ordinary type
    // resolution path below.
    if let Some(entry) = reusable_parameter_free_unnamed_entry(session, &message) {
        let mut entry = entry;
        entry.statement_trace = statement_trace.clone();
        if let Some(trace) = &statement_trace {
            trace.record_event("compile", "plan_cache_hit");
            trace.record_event("protocol", "parse_reused_image");
        }
        session.state.set_unnamed_prepared_statement(entry);
        let result = responder.send_parse_complete().await;
        if result.is_ok() {
            if let Some(trace) = &statement_trace {
                trace.record_event("protocol", "parse_complete_sent");
            }
        }
        return result;
    }

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
    let reusable_unnamed = message
        .name
        .is_none()
        .then(|| {
            reusable_unnamed_parse_artifacts(session, &message.query, &raw_stmt, &parameter_types)
        })
        .flatten();
    let (result_schema, generic_plan) = if is_client_copy(&raw_stmt) {
        (Vec::new(), None)
    } else if let Some(artifacts) = reusable_unnamed {
        artifacts
    } else {
        build_parse_artifacts(
            session,
            &message.query,
            &raw_stmt,
            &route,
            &message.type_oids,
            statement_trace.clone(),
        )?
    };

    let entry = PreparedStatementEntry {
        name: message.name.clone().unwrap_or_default(),
        source_sql: message.query.into(),
        raw_stmt: Arc::new(raw_stmt),
        parameter_types,
        result_schema,
        generic_plan,
        generic_plan_uses: 0,
        source: PreparedStatementSource::Protocol,
        statement_trace: statement_trace.clone(),
    };

    let is_named_statement = message.name.is_some();
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

    if is_named_statement {
        session.refresh_prepared_statement_metadata();
    }
    let result = responder.send_parse_complete().await;
    if result.is_ok() {
        if let Some(trace) = &statement_trace {
            trace.record_event("protocol", "parse_complete_sent");
        }
    }
    result
}

fn reusable_parameter_free_unnamed_entry(
    session: &Session,
    message: &ParseMessage,
) -> Option<PreparedStatementEntry> {
    if message.name.is_some() || !message.type_oids.is_empty() {
        return None;
    }
    let previous = reusable_unnamed_statement_image(session, &message.query)?;
    if !previous.parameter_types.is_empty() {
        return None;
    }
    let mut entry = previous.clone();
    entry.generic_plan_uses = 0;
    debug!(
        target: targets::QUERY,
        sql_bytes = message.query.len(),
        "Repeated parameter-free unnamed Parse reused immutable statement image"
    );
    Some(entry)
}

/// Return the one immutable unnamed statement image that is legal to reuse.
///
/// Parsing a byte-identical SQL string is deterministic: parse behavior has no
/// session input. Binding and planning do, so the canonical compile-environment
/// key is checked here before either the pre-parse or post-parse reuse path can
/// observe the image. More-specific callers may additionally constrain
/// parameter types or compare the parsed AST, but cannot weaken this guard.
fn reusable_unnamed_statement_image<'a>(
    session: &'a Session,
    sql: &str,
) -> Option<&'a PreparedStatementEntry> {
    let previous = session.state.unnamed_prepared_statement()?;
    let plan = previous.generic_plan.as_ref()?;
    (previous.source == PreparedStatementSource::Protocol
        && previous.source_sql.as_ref() == sql
        && plan.compile_environment() == &session.compile_environment_key())
        .then_some(previous)
}

/// Reuse the immutable image behind a repeated unnamed Parse.
///
/// Drivers commonly use an unnamed Parse/Bind/Execute cycle even when they
/// deliberately disable server-side named statements. PostgreSQL semantics
/// replace the unnamed statement on every Parse, but do not require rebuilding
/// an identical value-independent plan. The compile environment contains the
/// visible catalog generation/epochs and all plan-affecting settings, so any
/// schema, search-path, database, or setting change declines the reuse.
fn reusable_unnamed_parse_artifacts(
    session: &Session,
    sql: &str,
    stmt: &Statement,
    parameter_types: &[Option<LogicalType>],
) -> Option<(Vec<ResultColumnDesc>, Option<CompiledStatement>)> {
    let previous = reusable_unnamed_statement_image(session, sql)?;
    if previous.raw_stmt.as_ref() != stmt || previous.parameter_types != parameter_types {
        return None;
    }
    let plan = previous.generic_plan.as_ref()?;
    debug!(
        target: targets::QUERY,
        sql_bytes = sql.len(),
        "Repeated unnamed Parse reused immutable generic plan"
    );
    Some((previous.result_schema.clone(), Some(plan.clone())))
}

async fn execute_bind<R: ExtendedQueryResponder>(
    session: &mut Session,
    message: BindMessage,
    responder: &mut R,
) -> Result<()> {
    let statement = statement_entry(session, message.statement_name.as_deref())?.clone();
    let statement_trace = take_statement_trace(session, message.statement_name.as_deref())
        .or_else(|| session.new_statement_trace(statement.source_sql.as_ref(), 0, Instant::now()));
    if let Some(trace) = &statement_trace {
        trace.record_event("protocol", "bind_entry");
        if statement.statement_trace.is_none() {
            trace.record_event("compile", "prepared_image_reused");
        }
    }
    let result = execute_bind_inner(
        session,
        message,
        responder,
        statement,
        statement_trace.clone(),
    )
    .await;
    if result.is_err() {
        if let Some(trace) = statement_trace {
            trace.record_event("protocol", "bind_error");
            trace.record_event("lifecycle", "statement_error");
            session.publish_statement_trace(trace.snapshot());
        }
    }
    result
}

async fn execute_bind_inner<R: ExtendedQueryResponder>(
    session: &mut Session,
    message: BindMessage,
    responder: &mut R,
    statement: PreparedStatementEntry,
    statement_trace: Option<Arc<StatementTrace>>,
) -> Result<()> {
    let is_named_statement = message.statement_name.is_some();
    let is_named_portal = message.portal_name.is_some();
    let parameter_env = decode_bind_parameters(
        &statement.parameter_types,
        &message.parameter_format_codes,
        &message.parameters,
    )?;
    let resolved_parameter_types = parameter_env.logical_types();
    let mut cached_query_plan = None;
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
            stmt: Box::new(statement.raw_stmt.as_ref().clone()),
            parameter_env: parameter_env.clone(),
        },
        StatementClass::Query => {
            let plan = select_protocol_query_plan(
                session,
                &statement,
                &parameter_env,
                statement_trace.clone(),
            )?;
            let execution = ExecutionRequest::from_typed_env(plan.clone(), &parameter_env)?;
            cached_query_plan = Some(plan);
            PortalKind::Query(execution)
        }
    };

    let portal_result_schema = cached_query_plan
        .as_ref()
        .map(|plan| plan.result_schema().to_vec())
        .unwrap_or_else(|| statement.result_schema.clone());
    let result_formats =
        validate_result_formats(&message.result_column_format_codes, &portal_result_schema)?;

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
        holdability: CursorHoldability::WithoutHold,
        // Protocol portals are forward-only. SQL DECLARE CURSOR is the path that
        // opts into scrollability and materialization.
        scroll_mode: ScrollMode::NoScroll,
        result_formats: result_formats.into(),
        result_schema: portal_result_schema.into(),
        kind,
        execution_state: PortalExecutionState::Ready,
        snapshot_retention: None,
        completion: None,
        created_generation: 0,
        transaction_owned: session.has_active_transaction(),
        statement_trace: statement_trace.clone(),
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
        if let Some(plan) = cached_query_plan {
            stored.result_schema = plan.result_schema().to_vec();
            stored.generic_plan = Some(plan);
            stored.generic_plan_uses = stored.generic_plan_uses.saturating_add(1);
        }
    }

    if is_named_statement {
        session.refresh_prepared_statement_metadata();
    }
    if is_named_portal {
        session.refresh_cursor_metadata();
    }
    let result = responder.send_bind_complete().await;
    if result.is_ok() {
        if let Some(trace) = &statement_trace {
            trace.record_event("protocol", "bind_complete_sent");
        }
    }
    result
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
    let statement_trace = portal
        .statement_trace
        .take()
        .or_else(|| session.new_statement_trace(portal.source_sql.as_ref(), 0, Instant::now()));
    if let Some(trace) = &statement_trace {
        trace.record_event("protocol", "execute_entry");
        trace.record_value(
            "protocol",
            "portal_max_rows",
            message.max_rows.max(0) as u64,
        );
    }

    if session.is_transaction_failed() && !is_allowed_in_failed_transaction(&portal.raw_stmt) {
        let error = paro_error::transaction_aborted();
        if let Some(trace) = statement_trace {
            trace.record_event("protocol", "execute_rejected");
            trace.record_event("lifecycle", "statement_error");
            session.publish_statement_trace(trace.snapshot());
        }
        return Err(error);
    }

    let query_str = portal.source_sql.clone();
    let portal_kind = portal.kind.clone();
    let trace_for_scope = statement_trace.clone();
    let result = session
        .run_in_statement_scope_with_trace_and_publish(
            &query_str,
            trace_for_scope,
            false,
            async |session| {
                if should_begin_implicit_transaction_for_portal(session, &portal_kind) {
                    session.begin_implicit_transaction_block()?;
                }

                match portal_kind {
                    PortalKind::Query(execution) => {
                        execute_query_portal(session, &mut portal, execution, &message, responder)
                            .await
                    }
                    PortalKind::Materialized => Err(paro_error::internal(
                        "materialized cursor cannot enter extended query execution".to_string(),
                    )),
                    PortalKind::Utility(cmd) => {
                        execute_utility_portal(session, &mut portal, *cmd, responder).await
                    }
                    PortalKind::ClientCopy {
                        stmt,
                        parameter_env,
                    } => {
                        execute_client_copy_portal(
                            session,
                            &mut portal,
                            *stmt,
                            parameter_env,
                            responder,
                        )
                        .await
                    }
                }
            },
        )
        .await;

    match &result {
        Ok(PortalProgress::Complete(completion)) => {
            if !completion.is_transaction_control() {
                session.command_counter_increment();
            }
            if let Some(trace) = &statement_trace {
                trace.record_event("protocol", "portal_complete");
                session.defer_protocol_statement_trace(trace.clone());
            }
        }
        Ok(PortalProgress::Suspended) => {
            if let Some(trace) = &statement_trace {
                trace.record_event("protocol", "portal_suspended");
            }
            portal.statement_trace = statement_trace;
        }
        Err(_) => {}
    }

    if result.is_ok() {
        overwrite_portal_entry(session, message.name.as_deref(), portal);
    }
    if message.name.is_some() && result.is_ok() {
        session.refresh_cursor_metadata();
    }

    result.map(|_| ())
}

async fn execute_close<R: ExtendedQueryResponder>(
    session: &mut Session,
    target: CloseTarget,
    responder: &mut R,
) -> Result<()> {
    let mut refresh_prepared = false;
    let mut refresh_cursors = false;
    match target {
        CloseTarget::Statement(name) => match name.as_deref() {
            Some(name) => {
                let _ = session.state.remove_prepared_statement(name);
                refresh_prepared = true;
                refresh_cursors = true;
            }
            None => {
                let _ = session.state.remove_unnamed_prepared_statement();
            }
        },
        CloseTarget::Portal(name) => match name.as_deref() {
            Some(name) => {
                let _ = session.state.remove_portal(name);
                refresh_cursors = true;
            }
            None => {
                let _ = session.state.remove_unnamed_portal();
            }
        },
    }

    if refresh_prepared {
        session.refresh_prepared_statement_metadata();
    }
    if refresh_cursors {
        session.refresh_cursor_metadata();
    }
    responder.send_close_complete().await
}

fn build_parse_artifacts(
    session: &Session,
    sql: &str,
    stmt: &Statement,
    route: &FrontendRoute,
    type_oids: &[u32],
    statement_trace: Option<Arc<StatementTrace>>,
) -> Result<(Vec<ResultColumnDesc>, Option<CompiledStatement>)> {
    match route {
        FrontendRoute::Query(_) => {
            let parameter_types = resolve_parse_parameter_types(stmt, type_oids)?;
            let snapshot = session.freeze_statement_context_with_trace(
                StatementOptions {
                    source: StatementSource::ExtendedQuery,
                    ..StatementOptions::default()
                },
                session.compile_scope_cancellation(),
                statement_trace.clone(),
            );
            let share_across_sessions = !session.transaction.has_active_transaction();
            if parameter_types.is_empty() {
                let compiled = build_query_plan(
                    session,
                    snapshot,
                    stmt.clone(),
                    &[],
                    share_across_sessions,
                    statement_fingerprint(sql),
                )?;
                Ok((compiled.result_schema().to_vec(), Some(compiled)))
            } else {
                let parameter_types = parameter_types
                    .iter()
                    .map(|ty| ty.clone().unwrap_or(LogicalType::Unknown))
                    .collect::<Vec<_>>();
                let compiled = build_query_plan(
                    session,
                    snapshot,
                    stmt.clone(),
                    &parameter_types,
                    share_across_sessions,
                    statement_fingerprint(sql),
                )?;
                let generic_plan = parameter_types
                    .iter()
                    .all(|ty| !matches!(ty, LogicalType::Unknown))
                    .then_some(compiled.clone());
                Ok((compiled.result_schema().to_vec(), generic_plan))
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
    session: &Session,
    snapshot: Arc<StatementContext>,
    stmt: Statement,
    parameter_types: &[LogicalType],
    share_across_sessions: bool,
    cache_query_fingerprint: u64,
) -> Result<CompiledStatement> {
    let statement_trace = snapshot.statement_trace();
    if share_across_sessions {
        if let Some(plan) =
            session.reusable_instance_query_plan(&stmt, parameter_types, snapshot.as_ref())
        {
            session.record_statement_cache_decision(cache_query_fingerprint, true);
            if let Some(trace) = &statement_trace {
                trace.record_event("compile", "plan_cache_hit");
            }
            return Ok(plan);
        }
    }
    let cache_occurrence = share_across_sessions.then(||
        session.record_statement_cache_decision(cache_query_fingerprint, false)).flatten();
    if let Some(trace) = &statement_trace {
        trace.record_event("compile", "plan_cache_miss");
        trace.record_event("compile", "compiler_call_entry");
    }
    let compiled =
        compile_statement_with_parameter_types(snapshot.clone(), stmt.clone(), parameter_types);
    if let Some(trace) = &statement_trace {
        trace.record_event("compile", "compiler_call_return");
    }
    let plan = compiled?;
    if let (Some(occurrence), Some(work)) = (cache_occurrence, plan.compile_work()) {
        snapshot.diagnostics.publish_compile_work(cache_query_fingerprint, occurrence, work);
    }
    if share_across_sessions {
        session.publish_instance_query_plan(
            stmt,
            parameter_types.to_vec(),
            snapshot.as_ref(),
            plan.clone(),
        );
        if let Some(trace) = &statement_trace {
            trace.record_event("compile", "plan_cache_publish");
        }
    }
    Ok(plan)
}

fn select_protocol_query_plan(
    session: &Session,
    statement: &PreparedStatementEntry,
    parameter_env: &TypedParameterEnv,
    statement_trace: Option<Arc<StatementTrace>>,
) -> Result<CompiledStatement> {
    let parameter_types = parameter_env
        .logical_types()
        .into_iter()
        .map(|ty| ty.unwrap_or(LogicalType::Unknown))
        .collect::<Vec<_>>();
    let compile_environment = session.compile_environment_key();
    if let Some(plan) = statement.generic_plan.as_ref() {
        if plan.compile_environment() == &compile_environment
            && plan.parameter_types() == parameter_types
        {
            if let Some(trace) = &statement_trace {
                trace.record_event("compile", "prepared_plan_cache_hit");
            }
            return Ok(plan.clone());
        }
    }

    let snapshot = session.freeze_statement_context_with_trace(
        StatementOptions {
            source: StatementSource::ExtendedQuery,
            ..StatementOptions::default()
        },
        session.compile_scope_cancellation(),
        statement_trace,
    );
    build_query_plan(
        session,
        snapshot,
        statement.raw_stmt.as_ref().clone(),
        &parameter_types,
        !session.transaction.has_active_transaction(),
        statement_fingerprint(statement.source_sql.as_ref()),
    )
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
    for (index, inferred) in infer_cast_parameter_types(stmt, placeholder_count)?
        .into_iter()
        .enumerate()
    {
        if parameter_types[index].is_none() {
            parameter_types[index] = inferred;
        }
    }
    Ok(parameter_types)
}

fn infer_cast_parameter_types(
    stmt: &Statement,
    parameter_count: usize,
) -> Result<Vec<Option<LogicalType>>> {
    let mut hints = Vec::new();
    let mut visitor = StatementVisitor::new(
        |expr| match expr {
            Expr::Cast {
                expr, target_type, ..
            }
            | Expr::TryCast {
                expr, target_type, ..
            } => {
                if let Expr::Parameter { index, .. } = expr.as_ref() {
                    hints.push((*index, target_type.clone()));
                }
            }
            _ => {}
        },
        |_| {},
    );
    visitor.visit(stmt);

    let mut inferred = vec![None; parameter_count];
    for (index, target_type) in hints {
        let logical_type = bind_logical_type(&target_type)?;
        let Some(slot) = inferred.get_mut(index) else {
            return Err(paro_error::protocol_violation(format!(
                "parameter ${} exceeds the statement parameter signature",
                index + 1
            )));
        };
        if slot
            .as_ref()
            .is_some_and(|existing| existing != &logical_type)
        {
            return Err(paro_error::type_mismatch(format!(
                "parameter ${} has conflicting cast targets",
                index + 1
            )));
        }
        *slot = Some(logical_type);
    }
    Ok(inferred)
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
    execution: ExecutionRequest,
    message: &ExecutePortalMessage,
    responder: &mut R,
) -> Result<PortalProgress> {
    if !matches!(portal.execution_state, PortalExecutionState::Ready) {
        if !execution.statement().is_query() {
            return execute_non_row_query_portal(session, portal, None, execution, responder).await;
        }
    } else {
        let snapshot_started = Instant::now();
        let snapshot = session.freeze_statement_context(
            StatementOptions {
                source: StatementSource::ExtendedQuery,
                ..StatementOptions::default()
            },
            session
                .current_statement_cancellation()
                .expect("portal execution requires an active statement scope"),
        );
        if let Some(trace) = active_statement_trace(session) {
            trace.record_span("frontend", "statement_snapshot", snapshot_started);
        }
        let execution = revalidate_portal_execution(session, snapshot.clone(), portal, execution)?;
        portal.kind = PortalKind::Query(execution.clone());
        if !execution.statement().is_query() {
            return execute_non_row_query_portal(
                session,
                portal,
                Some(snapshot),
                execution,
                responder,
            )
            .await;
        }
        if message.max_rows <= 0 && matches!(portal.scroll_mode, ScrollMode::NoScroll) {
            let execution_started = Instant::now();
            let executor = Executor::new(snapshot);
            session.set_executor(executor);
            let mut stream = session.get_executor().execute(execution)?;
            let mut row_count = 0usize;
            let mut first_page = false;
            let fetch_started = Instant::now();
            while let Some(chunk) = stream.fetch()? {
                let chunk_rows = chunk.size();
                row_count = row_count.saturating_add(chunk_rows);
                if !first_page {
                    first_page = true;
                    if let Some(trace) = active_statement_trace(session) {
                        trace.record_span("execution", "first_page_ready", fetch_started);
                    }
                }
                responder
                    .send_data_chunk(chunk, &portal.result_schema, &portal.result_formats)
                    .await?;
                if let Some(trace) = active_statement_trace(session) {
                    trace.record_value("protocol", "result_chunk_delivered", chunk_rows as u64);
                }
            }
            if !first_page {
                if let Some(trace) = active_statement_trace(session) {
                    trace.record_span("execution", "first_page_empty", fetch_started);
                }
            }
            if let Some(trace) = active_statement_trace(session) {
                trace.record_span("execution", "fetch_drain", fetch_started);
                trace.record_value("execution", "rows_returned", row_count as u64);
                trace.record_span("execution", "portal_execution", execution_started);
            }
            portal.execution_state = PortalExecutionState::Exhausted {
                position: row_count as i64,
            };
            portal.snapshot_retention = None;
            let completion = infer_statement_completion(&portal.raw_stmt, row_count);
            responder.send_command_complete(&completion).await?;
            if let Some(trace) = active_statement_trace(session) {
                trace.record_event("protocol", "command_complete_sent");
            }
            return Ok(PortalProgress::Complete(completion));
        }
        let materialization_started = Instant::now();
        let materialized =
            materialize_compiled_statement(session, snapshot.clone(), execution).await?;
        if let Some(trace) = active_statement_trace(session) {
            trace.record_span(
                "execution",
                "portal_materialization",
                materialization_started,
            );
        }
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

            let mut first_page = false;
            for chunk in &outcome.rows {
                if !first_page {
                    first_page = true;
                    if let Some(trace) = active_statement_trace(session) {
                        trace.record_event("execution", "first_page_ready");
                    }
                }
                responder
                    .send_data_chunk(chunk, &portal.result_schema, &portal.result_formats)
                    .await?;
                if let Some(trace) = active_statement_trace(session) {
                    trace.record_value("protocol", "result_chunk_delivered", chunk.size() as u64);
                }
            }
            if let Some(trace) = active_statement_trace(session) {
                trace.record_value("execution", "rows_returned", outcome.moved_rows as u64);
            }

            if outcome.at_end {
                portal.execution_state = PortalExecutionState::Exhausted {
                    position: outcome.new_position,
                };
                let completion = infer_statement_completion(&portal.raw_stmt, outcome.moved_rows);
                responder.send_command_complete(&completion).await?;
                if let Some(trace) = active_statement_trace(session) {
                    trace.record_event("protocol", "command_complete_sent");
                }
                Ok(PortalProgress::Complete(completion))
            } else {
                responder.send_portal_suspended().await?;
                if let Some(trace) = active_statement_trace(session) {
                    trace.record_event("protocol", "portal_suspended");
                }
                Ok(PortalProgress::Suspended)
            }
        }
        PortalExecutionState::Exhausted { .. } => {
            let completion = infer_statement_completion(&portal.raw_stmt, 0);
            responder.send_command_complete(&completion).await?;
            if let Some(trace) = active_statement_trace(session) {
                trace.record_event("protocol", "command_complete_sent");
            }
            Ok(PortalProgress::Complete(completion))
        }
        PortalExecutionState::Ready => Err(paro_error::internal(
            "portal was not materialized".to_string(),
        )),
    }
}

/// Paro acquires a portal's data snapshot at first Execute, rather than Bind.
/// Revalidate against that same snapshot so catalog bindings cannot lag behind it.
fn revalidate_portal_execution(
    session: &Session,
    snapshot: Arc<StatementContext>,
    portal: &PortalEntry,
    execution: ExecutionRequest,
) -> Result<ExecutionRequest> {
    if execution.statement().compile_environment() == &snapshot.compile_environment_key()
        && execution
            .statement()
            .dynamic_dependencies_available(snapshot.as_ref())
    {
        return Ok(execution);
    }

    let parameter_types = execution.statement().parameter_types().to_vec();
    let plan = build_query_plan(
        session,
        snapshot,
        portal.raw_stmt.as_ref().clone(),
        &parameter_types,
        false,
        statement_fingerprint(portal.source_sql.as_ref()),
    )?;
    if plan.result_schema() != portal.result_schema.as_ref() {
        return Err(ParoError::new(paro_error::ErrorData::new(
            paro_error::Severity::Error,
            paro_error::codes::feature::FEATURE_NOT_SUPPORTED,
            "cached plan must not change result type",
        )));
    }
    execution.with_statement(plan)
}

async fn execute_non_row_query_portal<R: ExtendedQueryResponder>(
    session: &mut Session,
    portal: &mut PortalEntry,
    snapshot: Option<Arc<StatementContext>>,
    execution: ExecutionRequest,
    responder: &mut R,
) -> Result<PortalProgress> {
    if let Some(completion) = portal.completion.clone() {
        responder.send_command_complete(&completion).await?;
        if let Some(trace) = active_statement_trace(session) {
            trace.record_event("protocol", "command_complete_sent");
        }
        return Ok(PortalProgress::Complete(completion));
    }

    let snapshot = snapshot.ok_or_else(|| {
        paro_error::internal("ready portal execution requires a statement snapshot".to_string())
    })?;
    let completion =
        run_non_row_compiled_statement(session, snapshot, &portal.raw_stmt, execution)?;
    portal.execution_state = PortalExecutionState::Exhausted { position: 0 };
    portal.completion = Some(completion.clone());
    responder.send_command_complete(&completion).await?;
    if let Some(trace) = active_statement_trace(session) {
        trace.record_event("protocol", "command_complete_sent");
    }
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
        if let Some(trace) = active_statement_trace(session) {
            trace.record_event("protocol", "command_complete_sent");
        }
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
    if let Some(trace) = active_statement_trace(session) {
        trace.record_event("protocol", "command_complete_sent");
    }
    Ok(PortalProgress::Complete(completion))
}

fn run_non_row_compiled_statement(
    session: &mut Session,
    snapshot: Arc<StatementContext>,
    stmt: &Statement,
    execution: ExecutionRequest,
) -> Result<StatementCompletion> {
    let executor = Executor::new(snapshot);
    session.set_executor(executor);

    let mut stream = session.get_executor().execute(execution).map_err(|err| {
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

fn take_statement_trace(session: &mut Session, name: Option<&str>) -> Option<Arc<StatementTrace>> {
    match name {
        Some(name) => session
            .state
            .get_prepared_statement_mut(name)
            .and_then(|entry| entry.statement_trace.take()),
        None => session
            .state
            .unnamed_prepared_statement_mut()
            .and_then(|entry| entry.statement_trace.take()),
    }
}

fn active_statement_trace(session: &Session) -> Option<Arc<StatementTrace>> {
    session
        .active_query()
        .and_then(|query| query.statement_trace())
        .cloned()
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
            PortalKind::Query(_) | PortalKind::Materialized | PortalKind::ClientCopy { .. }
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
        if let Some(trace) = active_statement_trace(session) {
            trace.record_event("protocol", "command_complete_sent");
        }
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
                let cancellation = session
                    .current_statement_cancellation()
                    .expect("COPY TO STDOUT requires an active statement scope");
                let mut copy_sink = responder.create_copy_out_sink(&cancellation, &options)?;
                session
                    .execute_copy_to_core(
                        query_stmt,
                        Some(&parameter_env),
                        None,
                        StatementSource::ExtendedQuery,
                        cancellation,
                        &mut *copy_sink,
                    )
                    .await?
            }
        }
        (paro_parser::ast::CopyDirection::From, paro_parser::ast::CopySource::Stdin) => {
            let cancellation = session
                .current_statement_cancellation()
                .expect("COPY FROM STDIN requires an active statement scope");
            let mut copy_source = responder.create_copy_in_source(&cancellation)?;
            session
                .execute_copy_from_core(
                    &copy_stmt,
                    Some(&parameter_env),
                    None,
                    StatementSource::ExtendedQuery,
                    cancellation,
                    &mut *copy_source,
                )
                .await?
        }
        _ => unreachable!("file-backed COPY should not use client COPY portal kind"),
    };

    portal.execution_state = PortalExecutionState::Exhausted { position: 0 };
    portal.completion = Some(completion.clone());
    responder.send_command_complete(&completion).await?;
    if let Some(trace) = active_statement_trace(session) {
        trace.record_event("protocol", "command_complete_sent");
    }
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
            _cancellation: &StatementCancellation,
            _options: &paro_function::copy::CopyOptions,
        ) -> Result<Box<dyn CopyProtocolSink + '_>> {
            self.events.push("copy_out_sink".to_string());
            Ok(Box::new(TestCopyOutSink { responder: self }))
        }

        fn create_copy_in_source(
            &mut self,
            _cancellation: &StatementCancellation,
        ) -> Result<Box<dyn CopyProtocolSource + '_>> {
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

        async fn abort(&mut self) -> Result<()> {
            self.responder.copy_in_payload.clear();
            Ok(())
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
                match statement.raw_stmt.as_ref().clone() {
                    Statement::VariableShow(stmt) => stmt,
                    other => panic!("expected show statement, got {other:?}"),
                }
            ))
        );
    }

    #[tokio::test]
    async fn portal_progress_cannot_resurrect_a_removed_portal() {
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

        let detached_progress = portal_entry(&session, Some("p1")).unwrap().clone();
        session.state.remove_portal("p1");
        overwrite_portal_entry(&mut session, Some("p1"), detached_progress);
        assert!(session.state.get_portal("p1").is_none());
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
                query: "SELECT $1 + 1".to_string(),
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
        assert!(matches!(portal.kind, PortalKind::Query(_)));
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
    async fn protocol_execute_replans_after_catalog_change_between_bind_and_execute() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = Session::new(1, instance.clone());
        let mut ddl_session = Session::new(2, instance);
        let mut sink = CollectingSink::new();
        let mut responder = TestResponder::default();

        exec_simple_ok(
            &mut ddl_session,
            &mut sink,
            "CREATE TABLE portal_replan_t (v INT)",
        )
        .await;
        sink.clear();
        exec_simple_ok(
            &mut ddl_session,
            &mut sink,
            "INSERT INTO portal_replan_t VALUES (1)",
        )
        .await;

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Parse(ParseMessage {
                name: Some("s1".to_string()),
                query: "SELECT v FROM portal_replan_t".to_string(),
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

        sink.clear();
        exec_simple_ok(&mut ddl_session, &mut sink, "DROP TABLE portal_replan_t").await;
        sink.clear();
        exec_simple_ok(
            &mut ddl_session,
            &mut sink,
            "CREATE TABLE portal_replan_t (v INT)",
        )
        .await;
        sink.clear();
        exec_simple_ok(
            &mut ddl_session,
            &mut sink,
            "INSERT INTO portal_replan_t VALUES (2)",
        )
        .await;

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

        assert_eq!(responder.rows, vec![vec!["2".to_string()]]);
    }

    #[tokio::test]
    async fn protocol_execute_replans_non_row_statement_after_catalog_change() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = Session::new(1, instance.clone());
        let mut ddl_session = Session::new(2, instance);
        let mut sink = CollectingSink::new();
        let mut responder = TestResponder::default();

        exec_simple_ok(
            &mut ddl_session,
            &mut sink,
            "CREATE TABLE portal_insert_t (v INT)",
        )
        .await;
        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Parse(ParseMessage {
                name: Some("s1".to_string()),
                query: "INSERT INTO portal_insert_t VALUES ($1)".to_string(),
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
                parameters: vec![Some(b"7".to_vec())],
                result_column_format_codes: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();

        sink.clear();
        exec_simple_ok(&mut ddl_session, &mut sink, "DROP TABLE portal_insert_t").await;
        sink.clear();
        exec_simple_ok(
            &mut ddl_session,
            &mut sink,
            "CREATE TABLE portal_insert_t (v INT)",
        )
        .await;

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

        sink.clear();
        exec_simple_ok(&mut session, &mut sink, "SELECT v FROM portal_insert_t").await;
        let result = sink.assert_single_result();
        let value = result.chunks[0]
            .column(0)
            .expect("result column")
            .get_value(0);
        assert_eq!(value, Value::Integer(7));
    }

    #[tokio::test]
    async fn protocol_execute_rejects_result_type_change_after_bind() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = Session::new(1, instance.clone());
        let mut ddl_session = Session::new(2, instance);
        let mut sink = CollectingSink::new();
        let mut responder = TestResponder::default();

        exec_simple_ok(
            &mut ddl_session,
            &mut sink,
            "CREATE TABLE portal_schema_t (v INT)",
        )
        .await;
        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Parse(ParseMessage {
                name: Some("s1".to_string()),
                query: "SELECT * FROM portal_schema_t".to_string(),
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

        sink.clear();
        exec_simple_ok(&mut ddl_session, &mut sink, "DROP TABLE portal_schema_t").await;
        sink.clear();
        exec_simple_ok(
            &mut ddl_session,
            &mut sink,
            "CREATE TABLE portal_schema_t (v INT, extra INT)",
        )
        .await;

        let error = execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Execute(ExecutePortalMessage {
                name: Some("p1".to_string()),
                max_rows: 0,
            }),
            &mut responder,
        )
        .await
        .expect_err("result schema changes must invalidate a bound portal");

        assert_eq!(error.message(), "cached plan must not change result type");
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
                query: "SELECT $1 + 1".to_string(),
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
    async fn first_bind_specializes_unknown_signature_then_caches_the_plan() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = Session::new(1, instance);
        let mut responder = TestResponder::default();

        execute_extended_query_message(
            &mut session,
            ExtendedQueryMessage::Parse(ParseMessage {
                name: Some("s1".to_string()),
                query: "SELECT $1".to_string(),
                type_oids: Vec::new(),
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
                parameters: vec![Some(b"41".to_vec())],
                result_column_format_codes: Vec::new(),
            }),
            &mut responder,
        )
        .await
        .unwrap();

        let statement = statement_entry(&session, Some("s1")).unwrap();
        let cached = statement
            .generic_plan
            .as_ref()
            .expect("first typed Bind should publish a reusable plan");
        assert_eq!(cached.parameter_types().len(), 1);
        assert_ne!(cached.parameter_types()[0], LogicalType::Unknown);
        let PortalKind::Query(execution) = &portal_entry(&session, Some("p1")).unwrap().kind else {
            panic!("expected query portal");
        };
        assert!(execution.statement().shares_image_with(cached));

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

        assert_eq!(responder.rows, vec![vec!["41".to_string()]]);
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
                query: "SELECT $1 + 1".to_string(),
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
                query: "SELECT $1".to_string(),
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
                query: "SELECT $1 + 1".to_string(),
                type_oids: vec![INT4OID],
            }),
            &mut responder,
        )
        .await
        .unwrap();

        let prepared_plan = statement_entry(&session, Some("s1"))
            .unwrap()
            .generic_plan
            .clone()
            .expect("typed Parse should cache a reusable plan");

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
        let PortalKind::Query(first_execution) = &portal_entry(&session, Some("p1")).unwrap().kind
        else {
            panic!("expected query portal");
        };
        assert!(first_execution
            .statement()
            .shares_image_with(&prepared_plan));
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
        let PortalKind::Query(second_execution) = &portal_entry(&session, Some("p2")).unwrap().kind
        else {
            panic!("expected query portal");
        };
        assert!(second_execution
            .statement()
            .shares_image_with(&prepared_plan));
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
        assert_eq!(
            statement_entry(&session, Some("s1"))
                .unwrap()
                .generic_plan_uses,
            2
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
                query: "SELECT $1".to_string(),
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

        execute_extended_query_message(
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
        .unwrap();
        assert_eq!(
            session
                .state
                .get_portal("p2")
                .expect("numeric binary portal")
                .result_formats
                .as_ref(),
            [FormatCode::Binary]
        );
    }

    #[tokio::test]
    async fn repeated_unnamed_parse_reuses_only_the_same_compile_environment() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = Session::new(1, instance);
        let mut responder = TestResponder::default();
        let parse = || {
            ExtendedQueryMessage::Parse(ParseMessage {
                name: None,
                query: "SELECT 1".to_string(),
                type_oids: Vec::new(),
            })
        };

        execute_extended_query_message(&mut session, parse(), &mut responder)
            .await
            .unwrap();
        let first = session
            .state
            .unnamed_prepared_statement()
            .and_then(|statement| statement.generic_plan.clone())
            .expect("first unnamed Parse compiles a generic plan");
        let first_ast = session
            .state
            .unnamed_prepared_statement()
            .map(|statement| statement.raw_stmt.clone())
            .expect("first unnamed Parse retains its AST");

        execute_extended_query_message(&mut session, parse(), &mut responder)
            .await
            .unwrap();
        let second = session
            .state
            .unnamed_prepared_statement()
            .and_then(|statement| statement.generic_plan.clone())
            .expect("repeated unnamed Parse keeps a generic plan");
        assert!(second.shares_image_with(&first));
        assert!(Arc::ptr_eq(
            &first_ast,
            &session
                .state
                .unnamed_prepared_statement()
                .expect("repeated unnamed Parse retains its entry")
                .raw_stmt
        ));

        session.config.set_setting("threads", Value::Integer(2));
        crate::utility::settings::reconcile_effective_settings(&mut session).unwrap();
        execute_extended_query_message(&mut session, parse(), &mut responder)
            .await
            .unwrap();
        let changed_environment = session
            .state
            .unnamed_prepared_statement()
            .and_then(|statement| statement.generic_plan.clone())
            .expect("changed environment recompiles a generic plan");
        assert!(!changed_environment.shares_image_with(&second));
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
    async fn query_plan_cache_is_instance_wide_and_environment_exact() {
        let instance = paro_instance::Instance::new_in_memory();
        let mut session = Session::new(1, instance.clone());
        session
            .config
            .set_setting("optimizer_verify", Value::Boolean(true));
        crate::utility::settings::reconcile_effective_settings(&mut session).unwrap();

        let mut first_sink = CollectingSink::new();
        exec_simple_ok(&mut session, &mut first_sink, "SELECT 1").await;
        let after_first = instance.plan_cache().metrics();
        assert_eq!(after_first.entries, 1);
        assert_eq!(after_first.hits, 0);
        assert_eq!(after_first.misses, 1);

        let mut second_sink = CollectingSink::new();
        exec_simple_ok(&mut session, &mut second_sink, "SELECT 1").await;
        assert_eq!(instance.plan_cache().metrics().hits, 1);

        let mut peer = Session::new(2, instance.clone());
        peer.config
            .set_setting("optimizer_verify", Value::Boolean(true));
        crate::utility::settings::reconcile_effective_settings(&mut peer).unwrap();
        assert_eq!(session.effective_settings(), peer.effective_settings());
        assert_eq!(
            session.compile_environment_key(),
            peer.compile_environment_key()
        );
        assert_eq!(
            session.freeze_query_context().env,
            peer.freeze_query_context().env
        );
        let mut peer_sink = CollectingSink::new();
        exec_simple_ok(&mut peer, &mut peer_sink, "SELECT 1").await;
        let after_peer = instance.plan_cache().metrics();
        assert_eq!(after_peer.hits, 2, "{after_peer:?}");

        let mut unverified_peer = Session::new(3, instance.clone());
        unverified_peer
            .config
            .set_setting("optimizer_verify", Value::Boolean(false));
        crate::utility::settings::reconcile_effective_settings(&mut unverified_peer).unwrap();
        assert_eq!(
            session.compile_environment_key(),
            unverified_peer.compile_environment_key(),
            "verification observes a compiled image and cannot change its identity"
        );
        let mut unverified_sink = CollectingSink::new();
        exec_simple_ok(&mut unverified_peer, &mut unverified_sink, "SELECT 1").await;
        assert_eq!(instance.plan_cache().metrics().hits, 3);

        session.config.set_setting("threads", Value::Integer(2));
        crate::utility::settings::reconcile_effective_settings(&mut session).unwrap();
        let mut changed_sink = CollectingSink::new();
        exec_simple_ok(&mut session, &mut changed_sink, "SELECT 1").await;
        let changed = instance.plan_cache().metrics();
        assert_eq!(changed.hits, 3);
        assert_eq!(changed.misses, 2);
        assert_eq!(changed.entries, 2);
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
