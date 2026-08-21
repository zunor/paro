// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! SQL execution entry points for simple-query front-end routing.

use crate::completion::StatementCompletion;
use crate::completion_infer::{infer_statement_completion, initial_statement_completion};
use crate::copy_protocol::{CopyInSpec, CopyProtocolSink, CopyProtocolSource, ProtocolResultSink};
use crate::dispatch::{dispatch_statement, FrontendRoute, PreparedCommand, UtilityCommand};
use crate::prepared::extended_query::{
    execute_extended_query_message as execute_protocol_message, ExtendedQueryMessage,
    ExtendedQueryResponder,
};
use crate::prepared::sql_commands::{bind_execute_explain_target, execute_prepared_command};
use crate::prepared::typed_parameters::TypedParameterEnv;
use crate::result::sink::ResultSink;
use crate::transaction::is_allowed_in_failed_transaction;
use crate::utility::execute_utility_command;
use crate::Session;
use async_trait::async_trait;
use paro_common::error::{self as paro_error, Result};
use paro_common::logging::targets;
use paro_common::types::LogicalType;
use paro_compiler::{compile_statement, compile_statement_with_parameter_types};
use paro_context::{StatementCancellation, StatementInput, StatementOptions, StatementSource};
use paro_execution::query_executor::compiled::ExecutionRequest;
use paro_execution::query_executor::executor::Executor;
use paro_function::table::CopyStdinSource;
use paro_instance::{CopyStdinMetrics, CopyStdinRejectReason};
use paro_parser::ast::{
    CopyDirection, CopySource, CopyStmt, CopyTarget, ExecuteStmt, Expr, Identifier, Literal, Query,
    SetExpr, SetValues, Settings, Statement, TableReference,
};
use paro_transaction::{
    IsolationLevel, LockMode, LockRequest, LockResource, ReadTrackingPolicy, ReadWritePromotion,
    TableId,
};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error};

#[derive(Default)]
struct QueryPipelineOptions {
    statement_format: Option<String>,
    source: StatementSource,
    input: StatementInput,
    completion_override: Option<StatementCompletion>,
    reuse_simple_query_plan: bool,
}

impl Session {
    /// Execute a SQL string using the Simple Query protocol with a ResultSink.
    pub async fn execute_simple_query<S: ProtocolResultSink>(
        &mut self,
        sql: &str,
        sink: &mut S,
    ) -> Result<()> {
        self.clear_protocol_unnamed_objects();

        let statements = match paro_parser::parse(sql) {
            Ok(stmts) => stmts,
            Err(e) => {
                let err = paro_error::from_parser(e.to_string());
                sink.error(&err).await?;
                return Err(err);
            }
        };

        if statements.is_empty() {
            return Ok(());
        }

        let stmt_count = statements.len();
        let use_implicit = stmt_count > 1 && self.is_auto_commit();
        debug!(
            target: targets::SESSION,
            session_id = self.id,
            statement_count = stmt_count,
            implicit_transaction = use_implicit,
            "Simple query execution started"
        );
        if use_implicit {
            if let Err(e) = self.begin_implicit_transaction_block() {
                sink.error(&e).await?;
                return Err(e);
            }
        }

        let mut final_result = Ok(());
        for (stmt_index, stmt_with_format) in statements.into_iter().enumerate() {
            let stmt = stmt_with_format.stmt;
            let statement_format = stmt_with_format.format;
            match self
                .execute_statement(stmt, statement_format, stmt_index, sink)
                .await
            {
                Ok(()) => {}
                Err(e) => {
                    sink.error(&e).await?;

                    if self.is_in_implicit_block() {
                        let _ = self.rollback_implicit_transaction();
                        final_result = Err(e);
                        break;
                    } else if self.is_in_explicit_block() {
                        self.set_transaction_failed();
                        final_result = Err(e);
                        break;
                    } else {
                        final_result = Err(e);
                        break;
                    }
                }
            }
        }

        if final_result.is_ok() && self.is_in_implicit_block() {
            if let Err(e) = self.end_implicit_transaction_block() {
                sink.error(&e).await?;
                return Err(e);
            }
        }

        final_result
    }

    /// Execute one extended-query protocol message against the shared prepared/portal store.
    pub async fn execute_extended_query_message<R: ExtendedQueryResponder>(
        &mut self,
        message: ExtendedQueryMessage,
        responder: &mut R,
    ) -> Result<()> {
        execute_protocol_message(self, message, responder).await
    }

    async fn execute_statement<S: ProtocolResultSink>(
        &mut self,
        stmt: Statement,
        statement_format: Option<String>,
        stmt_index: usize,
        sink: &mut S,
    ) -> Result<()> {
        if self.is_transaction_failed() && !is_allowed_in_failed_transaction(&stmt) {
            return Err(paro_error::transaction_aborted());
        }

        let initial_completion = initial_statement_completion(&stmt);
        debug!(
            target: targets::SESSION,
            session_id = self.id,
            statement_index = stmt_index,
            statement_completion = %initial_completion,
            "Statement execution started"
        );

        let query_str = stmt.to_string();
        let route = dispatch_statement(stmt.clone());
        debug!(
            target: targets::SESSION,
            session_id = self.id,
            statement_index = stmt_index,
            statement_completion = %initial_completion,
            route = route.label(),
            "Statement routed"
        );

        let result = self
            .run_in_statement_scope(&query_str, async move |session| {
                session
                    .execute_frontend_route(route, statement_format, sink)
                    .await
            })
            .await;

        debug!(
            target: targets::SESSION,
            session_id = self.id,
            statement_index = stmt_index,
            statement_completion = %initial_completion,
            success = result.is_ok(),
            "Statement execution finished"
        );

        if result.is_ok() && !initial_completion.is_transaction_control() {
            self.command_counter_increment();
        }

        result
    }

    async fn execute_frontend_route<S: ProtocolResultSink>(
        &mut self,
        route: FrontendRoute,
        statement_format: Option<String>,
        sink: &mut S,
    ) -> Result<()> {
        match route {
            FrontendRoute::Query(stmt) => {
                self.execute_query_statement(*stmt, statement_format, None, sink)
                    .await
            }
            FrontendRoute::Prepared(cmd) => self.execute_prepared_command(*cmd, sink).await,
            FrontendRoute::Utility(cmd) => {
                self.execute_utility_statement(*cmd, statement_format, sink)
                    .await
            }
        }
    }

    async fn execute_prepared_command<S: ProtocolResultSink>(
        &mut self,
        cmd: PreparedCommand,
        sink: &mut S,
    ) -> Result<()> {
        execute_prepared_command(self, cmd, sink).await
    }

    async fn execute_utility_statement<S: ProtocolResultSink>(
        &mut self,
        cmd: UtilityCommand,
        statement_format: Option<String>,
        sink: &mut S,
    ) -> Result<()> {
        if cmd.starts_explicit_transaction() && self.is_in_implicit_block() {
            self.end_implicit_transaction_block()?;
        }

        let _ = statement_format;
        execute_utility_command(self, cmd, sink).await
    }

    async fn execute_query_statement<S: ProtocolResultSink>(
        &mut self,
        stmt: Statement,
        statement_format: Option<String>,
        completion_override: Option<StatementCompletion>,
        sink: &mut S,
    ) -> Result<()> {
        let stmt = self.rewrite_query_statement(stmt)?;

        if let Statement::Copy(copy_stmt) = &stmt {
            match (&copy_stmt.direction, &copy_stmt.source) {
                (CopyDirection::To, CopySource::Stdout) => {
                    let query_stmt = build_copy_to_query_statement(copy_stmt)?;
                    let options = paro_function::copy::CopyOptions::from_ast(&copy_stmt.options)?;
                    return self
                        .execute_copy_to_frontend(
                            query_stmt,
                            statement_format,
                            StatementSource::SimpleQuery,
                            &options,
                            sink,
                        )
                        .await;
                }
                (CopyDirection::From, CopySource::Stdin) => {
                    return self
                        .execute_copy_from_frontend(
                            copy_stmt,
                            statement_format,
                            StatementSource::SimpleQuery,
                            sink,
                        )
                        .await;
                }
                _ => {}
            }
        }

        self.execute_query_pipeline(stmt, statement_format, completion_override, sink)
            .await
    }

    async fn execute_query_pipeline<S: ProtocolResultSink>(
        &mut self,
        stmt: Statement,
        statement_format: Option<String>,
        completion_override: Option<StatementCompletion>,
        sink: &mut S,
    ) -> Result<()> {
        self.execute_query_pipeline_with_parameters(
            stmt,
            None,
            QueryPipelineOptions {
                statement_format,
                source: StatementSource::SimpleQuery,
                completion_override,
                reuse_simple_query_plan: true,
                ..QueryPipelineOptions::default()
            },
            sink,
        )
        .await
    }

    async fn execute_query_pipeline_with_parameters<S: ProtocolResultSink>(
        &mut self,
        stmt: Statement,
        parameter_env: Option<&TypedParameterEnv>,
        options: QueryPipelineOptions,
        sink: &mut S,
    ) -> Result<()> {
        let QueryPipelineOptions {
            statement_format,
            source,
            input,
            completion_override,
            reuse_simple_query_plan,
        } = options;
        let require_new_transaction =
            self.transaction.is_auto_commit() && !self.transaction.has_active_transaction();

        if require_new_transaction {
            self.begin_transaction_internal()?;
        }
        let read_tracking_selection =
            read_tracking_selection_for_statement(&stmt, self.transaction.isolation_level())?;
        let read_tracking_policy = read_tracking_selection.policy;
        if self.transaction.is_read_only() && read_tracking_selection.requires_read_write {
            let err = paro_error::read_only_transaction();
            if require_new_transaction {
                let _ = self.rollback_auto_transaction(Some(&err));
            }
            return Err(err);
        }
        self.current_database
            .transaction_manager()
            .record_read_tracking_selection(
                read_tracking_selection.policy,
                read_tracking_selection.had_user_hint,
                read_tracking_selection.escalated,
            );
        let promotion = self
            .transaction
            .prepare_statement_read_tracking_for_database_with_access(
                self.current_database.transaction_manager(),
                read_tracking_policy,
                read_tracking_selection.requires_read_write,
                paro_transaction::DatabaseId::new(self.current_database.id()),
                self.current_database.name(),
            )?;
        if promotion == ReadWritePromotion::MustRestartUserVisible {
            return Err(paro_error::serialization_failure(
                "safe snapshot transaction must be restarted before executing a write",
            ));
        }
        if statement_has_for_update(&stmt) {
            if let Err(err) = self.acquire_select_for_update_locks(&stmt) {
                if require_new_transaction {
                    let _ = self.rollback_auto_transaction(Some(&err));
                }
                return Err(err);
            }
        }

        let statement_completion = initial_statement_completion(&stmt);
        let started_at = Instant::now();
        let simple_plan_cache_eligible = reuse_simple_query_plan
            && parameter_env.is_none()
            && matches!(stmt, Statement::Query(_));
        let cached_statement_format = statement_format.clone();

        let ctx = self.freeze_statement_context_with_input(
            StatementOptions {
                statement_format,
                source,
                ..StatementOptions::default()
            },
            self.current_statement_cancellation()
                .expect("query pipeline requires an active statement scope"),
            input,
        );

        debug!(
            target: targets::QUERY,
            session_id = self.id,
            statement_completion = %statement_completion,
            "Statement compilation started"
        );
        let compile_environment = ctx.compile_environment_key();
        let cached_plan = simple_plan_cache_eligible.then(|| {
            self.state.reusable_simple_query_plan(
                &stmt,
                cached_statement_format.as_deref(),
                &compile_environment,
            )
        });
        let cached_plan = cached_plan.flatten();
        let compile_result = if let Some(plan) = cached_plan.as_ref() {
            debug!(
                target: targets::QUERY,
                session_id = self.id,
                "Repeated Simple Query reused immutable plan"
            );
            Ok(plan.clone())
        } else {
            match parameter_env {
                Some(parameter_env) => {
                    let parameter_types = parameter_env
                        .iter()
                        .map(|parameter| parameter.logical_type.clone())
                        .collect::<Vec<_>>();
                    compile_statement_with_parameter_types(
                        ctx.clone(),
                        stmt.clone(),
                        &parameter_types,
                    )
                }
                None => compile_statement(ctx.clone(), stmt.clone()),
            }
        };
        let compiled = match compile_result {
            Ok(c) => c,
            Err(e) => {
                error!(
                    target: targets::QUERY,
                    session_id = self.id,
                    statement_completion = %statement_completion,
                    error = %e,
                    stage = "compile",
                    "Statement compilation failed"
                );
                if require_new_transaction {
                    let _ = self.rollback_auto_transaction(Some(&e));
                }
                return Err(e);
            }
        };
        if simple_plan_cache_eligible && cached_plan.is_none() {
            self.state.publish_simple_query_plan(
                stmt.clone(),
                cached_statement_format,
                compiled.clone(),
            );
        }
        debug!(
            target: targets::QUERY,
            session_id = self.id,
            statement_completion = %statement_completion,
            result_columns = compiled.result_schema().len(),
            "Statement compilation completed"
        );

        let executor = Executor::new(ctx);
        self.set_executor(executor);
        debug!(
            target: targets::EXECUTOR,
            session_id = self.id,
            statement_completion = %statement_completion,
            has_result_set = compiled.is_query(),
            "Query executor initialized"
        );

        let execution = match parameter_env {
            Some(parameter_env) => {
                ExecutionRequest::from_typed_env(compiled.clone(), parameter_env)?
            }
            None => ExecutionRequest::unparameterized(compiled.clone())?,
        };
        let result = self.get_executor().execute(execution);

        match result {
            Ok(mut stream) => {
                let result_names = compiled.result_names();
                let result_types = compiled.result_types();
                debug!(
                    target: targets::EXECUTOR,
                    session_id = self.id,
                    statement_completion = %statement_completion,
                    has_result_set = compiled.is_query(),
                    "Query execution started"
                );

                let mut rows = 0usize;
                let emits_rows = compiled.is_query() && !matches!(stmt, Statement::Copy(_));
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
                                if let paro_common::runtime_value::Value::BigInt(count) = value {
                                    rows = count as usize;
                                }
                            }
                        }
                    }
                }

                let completion =
                    completion_override.unwrap_or_else(|| infer_statement_completion(&stmt, rows));
                sink.finish_result(&completion).await?;

                debug!(
                    target: targets::EXECUTOR,
                    session_id = self.id,
                    statement_completion = %completion,
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "Query execution completed"
                );

                if require_new_transaction {
                    self.commit_auto_transaction()?;
                }

                Ok(())
            }
            Err(e) => {
                error!(
                    target: targets::EXECUTOR,
                    session_id = self.id,
                    statement_completion = %statement_completion,
                    error = %e,
                    stage = "pipeline",
                    "Query execution failed"
                );
                if require_new_transaction {
                    let _ = self.rollback_auto_transaction(Some(&e));
                }
                Err(e)
            }
        }
    }

    async fn execute_copy_to_frontend<S: ProtocolResultSink>(
        &mut self,
        query_stmt: Statement,
        statement_format: Option<String>,
        source: StatementSource,
        options: &paro_function::copy::CopyOptions,
        sink: &mut S,
    ) -> Result<()> {
        let require_new_transaction =
            self.transaction.is_auto_commit() && !self.transaction.has_active_transaction();

        if require_new_transaction {
            self.begin_transaction_internal()?;
        }

        let result: Result<()> = async {
            let completion = {
                let cancellation = self
                    .current_statement_cancellation()
                    .expect("COPY TO STDOUT requires an active statement scope");
                let mut protocol_sink = sink.create_copy_out_sink(&cancellation, options)?;
                self.execute_copy_to_core(
                    query_stmt,
                    None,
                    statement_format.clone(),
                    source,
                    cancellation,
                    &mut *protocol_sink,
                )
                .await?
            };
            if require_new_transaction {
                self.commit_auto_transaction()?;
            }
            sink.finish_result(&completion).await?;
            Ok(())
        }
        .await;

        if let Err(ref err) = result {
            if require_new_transaction {
                let _ = self.rollback_auto_transaction(Some(err));
            }
        }

        result
    }

    async fn execute_copy_from_frontend<S: ProtocolResultSink>(
        &mut self,
        copy_stmt: &CopyStmt,
        statement_format: Option<String>,
        source: StatementSource,
        sink: &mut S,
    ) -> Result<()> {
        let require_new_transaction =
            self.transaction.is_auto_commit() && !self.transaction.has_active_transaction();

        if require_new_transaction {
            self.begin_transaction_internal()?;
        }

        let result: Result<()> = async {
            let completion = {
                let cancellation = self
                    .current_statement_cancellation()
                    .expect("COPY FROM STDIN requires an active statement scope");
                let mut protocol_source = sink.create_copy_in_source(&cancellation)?;
                self.execute_copy_from_core(
                    copy_stmt,
                    None,
                    statement_format.clone(),
                    source,
                    cancellation,
                    &mut *protocol_source,
                )
                .await?
            };
            if require_new_transaction {
                self.commit_auto_transaction()?;
            }
            sink.finish_result(&completion).await?;
            Ok(())
        }
        .await;

        if let Err(ref err) = result {
            if require_new_transaction {
                let _ = self.rollback_auto_transaction(Some(err));
            }
        }

        result
    }

    pub(crate) async fn execute_copy_to_core(
        &mut self,
        query_stmt: Statement,
        parameter_env: Option<&TypedParameterEnv>,
        statement_format: Option<String>,
        source: StatementSource,
        cancellation: StatementCancellation,
        protocol_sink: &mut dyn CopyProtocolSink,
    ) -> Result<StatementCompletion> {
        let statement_completion = StatementCompletion::Copy { rows: 0 };
        let started_at = Instant::now();

        let ctx = self.freeze_statement_context(
            StatementOptions {
                statement_format,
                source,
                ..StatementOptions::default()
            },
            cancellation,
        );

        debug!(
            target: targets::QUERY,
            session_id = self.id,
            statement_completion = %statement_completion,
            "Statement compilation started"
        );
        let compiled = match parameter_env {
            Some(parameter_env) => {
                let parameter_types = parameter_env
                    .iter()
                    .map(|parameter| parameter.logical_type.clone())
                    .collect::<Vec<_>>();
                compile_statement_with_parameter_types(ctx.clone(), query_stmt, &parameter_types)?
            }
            None => compile_statement(ctx.clone(), query_stmt)?,
        };
        debug!(
            target: targets::QUERY,
            session_id = self.id,
            statement_completion = %statement_completion,
            result_columns = compiled.result_schema().len(),
            "Statement compilation completed"
        );

        let result_names = compiled.result_names();
        let result_types = compiled.result_types();
        let executor = Executor::new(ctx);
        self.set_executor(executor);
        debug!(
            target: targets::EXECUTOR,
            session_id = self.id,
            statement_completion = %statement_completion,
            has_result_set = compiled.is_query(),
            "Query executor initialized"
        );

        let execution = match parameter_env {
            Some(parameter_env) => ExecutionRequest::from_typed_env(compiled, parameter_env)?,
            None => ExecutionRequest::unparameterized(compiled)?,
        };
        let mut stream = self.get_executor().execute(execution).map_err(|e| {
            error!(
                target: targets::EXECUTOR,
                session_id = self.id,
                statement_completion = %statement_completion,
                error = %e,
                stage = "pipeline",
                "Query execution failed"
            );
            e
        })?;

        debug!(
            target: targets::EXECUTOR,
            session_id = self.id,
            statement_completion = %statement_completion,
            "Query execution started"
        );

        let mut rows = 0usize;
        let output_names = result_names
            .iter()
            .map(|name| normalize_copy_output_name(name))
            .collect::<Vec<_>>();
        protocol_sink
            .start_copy_out(&output_names, &result_types)
            .await?;

        while let Some(chunk) = stream.fetch()? {
            rows += chunk.len();
            protocol_sink.push_copy_rows(chunk).await?;
        }

        protocol_sink.finish_copy_out().await?;
        let completion = StatementCompletion::Copy { rows };

        debug!(
            target: targets::EXECUTOR,
            session_id = self.id,
            statement_completion = %completion,
            elapsed_ms = started_at.elapsed().as_millis(),
            "Query execution completed"
        );

        Ok(completion)
    }

    pub(crate) async fn execute_copy_from_core(
        &mut self,
        copy_stmt: &CopyStmt,
        parameter_env: Option<&TypedParameterEnv>,
        statement_format: Option<String>,
        source: StatementSource,
        cancellation: StatementCancellation,
        protocol_source: &mut dyn CopyProtocolSource,
    ) -> Result<StatementCompletion> {
        validate_copy_from_options(copy_stmt)?;
        let column_count = resolve_copy_from_target_columns(self, copy_stmt)?.len();
        let spec = CopyInSpec {
            overall_format: 0,
            column_formats: vec![0; column_count],
        };
        protocol_source.begin_copy_in(&spec).await?;

        let payload = FramedCopySourceBridge::new(
            self.copy_stdin_memory_limit(),
            self.instance.copy_stdin_metrics().clone(),
        )
        .collect(protocol_source, cancellation)
        .await?;
        let input = StatementInput::copy_from_stdin(Arc::new(payload));

        let mut sink = CompletionCaptureSink::default();
        let result = self
            .execute_query_pipeline_with_parameters(
                Statement::Copy(copy_stmt.clone()),
                parameter_env,
                QueryPipelineOptions {
                    statement_format,
                    source,
                    input,
                    completion_override: None,
                    reuse_simple_query_plan: false,
                },
                &mut sink,
            )
            .await;

        result?;
        sink.into_completion()
    }

    fn rewrite_query_statement(&mut self, stmt: Statement) -> Result<Statement> {
        match stmt {
            Statement::Explain {
                kind,
                options,
                query,
            } => Ok(Statement::Explain {
                kind,
                options,
                query: Box::new(self.rewrite_explain_target(*query)?),
            }),
            Statement::ExplainAnalyze {
                partial,
                graphical,
                query,
            } => Ok(Statement::ExplainAnalyze {
                partial,
                graphical,
                query: Box::new(self.rewrite_explain_target(*query)?),
            }),
            Statement::StatementWithSettings { settings, stmt } => {
                Ok(Statement::StatementWithSettings {
                    settings,
                    stmt: Box::new(self.rewrite_query_statement(*stmt)?),
                })
            }
            other => Ok(other),
        }
    }

    fn rewrite_explain_target(&mut self, stmt: Statement) -> Result<Statement> {
        match stmt {
            Statement::Execute(execute) => self.resolve_explain_execute(execute),
            other => Ok(other),
        }
    }

    fn resolve_explain_execute(&mut self, execute: ExecuteStmt) -> Result<Statement> {
        let Some(entry) = self
            .state
            .get_prepared_statement(execute.name.name.as_str())
        else {
            return Err(paro_error::catalog(format!(
                "prepared statement \"{}\" does not exist",
                execute.name.name
            )));
        };

        bind_execute_explain_target(&entry.raw_stmt, &execute.args)
    }
}

pub(crate) fn build_copy_to_query_statement(copy_stmt: &CopyStmt) -> Result<Statement> {
    if copy_stmt.where_clause.is_some() {
        return Err(paro_error::syntax(
            "COPY TO does not support WHERE clause".to_string(),
        ));
    }

    match &copy_stmt.target {
        CopyTarget::Query(query) => Ok(Statement::Query(query.clone())),
        CopyTarget::Table { name, columns } => {
            let projection = columns
                .as_ref()
                .map(|cols| {
                    cols.iter()
                        .map(|column| column.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_else(|| "*".to_string());
            let sql = format!("SELECT {projection} FROM {name}");
            let parsed =
                paro_parser::parse(&sql).map_err(|e| paro_error::from_parser(e.to_string()))?;
            Ok(parsed
                .into_iter()
                .next()
                .expect("synthetic COPY TO query must parse")
                .stmt)
        }
    }
}

fn normalize_copy_output_name(name: &str) -> String {
    name.rsplit('.').next().unwrap_or(name).to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadTrackingSelection {
    policy: ReadTrackingPolicy,
    had_user_hint: bool,
    escalated: bool,
    requires_read_write: bool,
}

impl ReadTrackingSelection {
    const fn new(policy: ReadTrackingPolicy, requires_read_write: bool) -> Self {
        Self {
            policy,
            had_user_hint: false,
            escalated: false,
            requires_read_write,
        }
    }

    const fn with_hint(mut self, policy: ReadTrackingPolicy) -> Self {
        self.policy = policy;
        self.had_user_hint = true;
        self
    }

    const fn conservative_for_write(mut self) -> Self {
        self.policy = ReadTrackingPolicy::RangeCritical;
        self.requires_read_write = true;
        self.escalated = true;
        self
    }
}

fn read_tracking_selection_for_statement(
    stmt: &Statement,
    isolation: IsolationLevel,
) -> Result<ReadTrackingSelection> {
    let _is_serializable = isolation == IsolationLevel::Serializable;
    match stmt {
        Statement::StatementWithSettings { settings, stmt } => {
            let mut selection = read_tracking_selection_for_statement(stmt, isolation)?;
            if let Some(policy) = settings
                .as_ref()
                .map(read_tracking_hint_from_settings)
                .transpose()?
                .flatten()
            {
                selection = selection.with_hint(policy);
                if selection.requires_read_write && policy != ReadTrackingPolicy::RangeCritical {
                    selection = selection.conservative_for_write();
                }
            }
            Ok(selection)
        }
        _ => Ok(default_read_tracking_selection_for_statement(stmt)),
    }
}

fn read_tracking_hint_from_settings(settings: &Settings) -> Result<Option<ReadTrackingPolicy>> {
    let SetValues::Expr(values) = &settings.values else {
        return Ok(None);
    };

    for (identifier, value) in settings.identifiers.iter().zip(values.iter()) {
        if !is_read_tracking_setting(identifier) {
            continue;
        }
        let value = read_tracking_hint_value(value)?;
        let Some(policy) = ReadTrackingPolicy::from_user_hint(&value) else {
            return Err(paro_error::invalid_input(format!(
                "unsupported read_tracking_policy hint '{}'; expected one of safe_snapshot_preferred, analytical_scan, point_critical, range_critical",
                value
            )));
        };
        return Ok(Some(policy));
    }

    Ok(None)
}

fn is_read_tracking_setting(identifier: &Identifier) -> bool {
    matches!(
        identifier
            .name
            .to_ascii_lowercase()
            .replace('-', "_")
            .as_str(),
        "read_tracking_policy" | "transaction_read_tracking_policy" | "read_tracking"
    )
}

fn read_tracking_hint_value(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Literal {
            value: Literal::String(value),
            ..
        } => Ok(value.clone()),
        Expr::ColumnRef { column, .. } => Ok(column.to_string()),
        Expr::Literal {
            value: Literal::Boolean(true),
            ..
        } => Ok("safe_snapshot_preferred".to_string()),
        Expr::Literal {
            value: Literal::Boolean(false),
            ..
        } => Ok("analytical_scan".to_string()),
        other => Err(paro_error::invalid_input(format!(
            "read_tracking_policy hint must be a string or identifier, got {other}"
        ))),
    }
}

fn default_read_tracking_selection_for_statement(stmt: &Statement) -> ReadTrackingSelection {
    match stmt {
        Statement::Query(query) if query_contains_for_update(query) => {
            ReadTrackingSelection::new(ReadTrackingPolicy::RangeCritical, true)
        }
        Statement::Query(_) | Statement::Explain { .. } | Statement::ExplainAnalyze { .. } => {
            ReadTrackingSelection::new(ReadTrackingPolicy::SafeSnapshotPreferred, false)
        }
        Statement::ReportIssue(_)
        | Statement::VariableShow(_)
        | Statement::ShowSettings { .. }
        | Statement::ShowProcessList { .. }
        | Statement::ShowMetrics { .. }
        | Statement::ShowEngines { .. }
        | Statement::ShowFunctions { .. }
        | Statement::ShowUserFunctions { .. }
        | Statement::ShowTableFunctions { .. }
        | Statement::ShowIndexes { .. }
        | Statement::ShowLocks(_)
        | Statement::ShowVariables { .. }
        | Statement::SetRole { .. }
        | Statement::SetSecondaryRoles { .. }
        | Statement::ShowDatabases(_)
        | Statement::ShowCreateDatabase(_)
        | Statement::UseDatabase { .. }
        | Statement::ConnectTo(_)
        | Statement::ShowOnlineNodes(_)
        | Statement::UseWarehouse(_)
        | Statement::ShowWarehouses(_)
        | Statement::InspectWarehouse(_)
        | Statement::ShowWorkloadGroups(_)
        | Statement::ShowSchemas(_)
        | Statement::ShowDropSchemas(_)
        | Statement::ShowCreateSchema(_)
        | Statement::UseSchema { .. }
        | Statement::ShowTables(_)
        | Statement::ShowCreateTable(_)
        | Statement::DescribeTable(_)
        | Statement::ShowTablesStatus(_)
        | Statement::ShowDropTables(_)
        | Statement::ExistsTable(_)
        | Statement::ShowStatistics(_)
        | Statement::ShowCreateDictionary(_)
        | Statement::ShowDictionaries(_)
        | Statement::ShowColumns(_)
        | Statement::ShowViews(_)
        | Statement::DescribeView(_)
        | Statement::ShowStreams(_)
        | Statement::DescribeStream(_)
        | Statement::ShowVirtualColumns(_)
        | Statement::ShowUsers { .. }
        | Statement::DescribeUser { .. }
        | Statement::ShowRoles { .. }
        | Statement::ShowGrants { .. }
        | Statement::ShowObjectPrivileges(_)
        | Statement::ShowGrantsOfRole(_)
        | Statement::DescRowAccessPolicy(_)
        | Statement::ShowTags(_)
        | Statement::ShowStages { .. }
        | Statement::DescribeStage { .. }
        | Statement::ListStage { .. }
        | Statement::DescribeConnection(_)
        | Statement::ShowConnections(_)
        | Statement::ShowFileFormats
        | Statement::Presign(_)
        | Statement::DescDatamaskPolicy(_)
        | Statement::DescNetworkPolicy(_)
        | Statement::ShowNetworkPolicies
        | Statement::DescPasswordPolicy(_)
        | Statement::ShowPasswordPolicies { .. }
        | Statement::DescribeTask(_)
        | Statement::ShowTasks(_)
        | Statement::DescribePipe(_)
        | Statement::DescribeNotification(_)
        | Statement::ShowProcedures { .. }
        | Statement::DescProcedure(_)
        | Statement::ShowSequences { .. }
        | Statement::DescSequence { .. }
        | Statement::SetStmt { .. }
        | Statement::UnSetStmt { .. }
        | Statement::SetPriority { .. } => {
            ReadTrackingSelection::new(ReadTrackingPolicy::SafeSnapshotPreferred, false)
        }
        Statement::Copy(copy) if copy.direction == CopyDirection::To => {
            ReadTrackingSelection::new(ReadTrackingPolicy::SafeSnapshotPreferred, false)
        }
        Statement::Copy(copy) if copy.direction == CopyDirection::From => {
            ReadTrackingSelection::new(ReadTrackingPolicy::RangeCritical, true)
        }
        _ => ReadTrackingSelection::new(ReadTrackingPolicy::RangeCritical, true),
    }
}

impl Session {
    fn acquire_select_for_update_locks(&self, stmt: &Statement) -> Result<()> {
        let active = self.active_transaction().ok_or_else(|| {
            paro_error::invalid_transaction_state(
                "SELECT FOR UPDATE requires an active transaction".to_string(),
            )
        })?;
        let mut requests = Vec::new();
        collect_for_update_lock_requests(self, stmt, &mut requests)?;
        if requests.is_empty() {
            return Ok(());
        }
        active.acquire_lock_requests(requests)
    }
}

fn statement_has_for_update(stmt: &Statement) -> bool {
    match stmt {
        Statement::StatementWithSettings { stmt, .. } => statement_has_for_update(stmt),
        Statement::Query(query) => query_contains_for_update(query),
        _ => false,
    }
}

fn query_contains_for_update(query: &Query) -> bool {
    query.locking.is_some() || set_expr_contains_for_update(&query.body)
}

fn set_expr_contains_for_update(expr: &SetExpr) -> bool {
    match expr {
        SetExpr::Select(_) | SetExpr::Values { .. } => false,
        SetExpr::Query(query) => query_contains_for_update(query),
        SetExpr::SetOperation(operation) => {
            set_expr_contains_for_update(&operation.left)
                || set_expr_contains_for_update(&operation.right)
        }
    }
}

fn collect_for_update_lock_requests(
    session: &Session,
    stmt: &Statement,
    out: &mut Vec<LockRequest>,
) -> Result<()> {
    match stmt {
        Statement::StatementWithSettings { stmt, .. } => {
            collect_for_update_lock_requests(session, stmt, out)
        }
        Statement::Query(query) => collect_query_for_update_locks(session, query, out),
        _ => Ok(()),
    }
}

fn collect_query_for_update_locks(
    session: &Session,
    query: &Query,
    out: &mut Vec<LockRequest>,
) -> Result<()> {
    if query.locking.is_some() {
        collect_set_expr_table_locks(session, &query.body, out)?;
    }
    collect_nested_query_locks(session, &query.body, out)
}

fn collect_nested_query_locks(
    session: &Session,
    expr: &SetExpr,
    out: &mut Vec<LockRequest>,
) -> Result<()> {
    match expr {
        SetExpr::Select(select) => {
            for table in &select.from {
                collect_nested_table_locks(session, table, out)?;
            }
            Ok(())
        }
        SetExpr::Query(query) => collect_query_for_update_locks(session, query, out),
        SetExpr::SetOperation(operation) => {
            collect_nested_query_locks(session, &operation.left, out)?;
            collect_nested_query_locks(session, &operation.right, out)
        }
        SetExpr::Values { .. } => Ok(()),
    }
}

fn collect_set_expr_table_locks(
    session: &Session,
    expr: &SetExpr,
    out: &mut Vec<LockRequest>,
) -> Result<()> {
    match expr {
        SetExpr::Select(select) => {
            for table in &select.from {
                collect_table_reference_locks(session, table, out)?;
            }
            Ok(())
        }
        SetExpr::Query(query) => collect_set_expr_table_locks(session, &query.body, out),
        SetExpr::SetOperation(operation) => {
            collect_set_expr_table_locks(session, &operation.left, out)?;
            collect_set_expr_table_locks(session, &operation.right, out)
        }
        SetExpr::Values { .. } => Ok(()),
    }
}

fn collect_nested_table_locks(
    session: &Session,
    table: &TableReference,
    out: &mut Vec<LockRequest>,
) -> Result<()> {
    match table {
        TableReference::Subquery { subquery, .. } => {
            collect_query_for_update_locks(session, subquery, out)
        }
        TableReference::Join { join, .. } => {
            collect_nested_table_locks(session, &join.left, out)?;
            collect_nested_table_locks(session, &join.right, out)
        }
        _ => Ok(()),
    }
}

fn collect_table_reference_locks(
    session: &Session,
    table: &TableReference,
    out: &mut Vec<LockRequest>,
) -> Result<()> {
    match table {
        TableReference::Table {
            database,
            schema,
            table,
            ..
        } => {
            let table_id =
                resolve_for_update_table_id(session, database.as_ref(), schema.as_ref(), table)?;
            let request = LockRequest::new(
                LockResource::Table {
                    namespace: session
                        .current_database
                        .transaction_manager()
                        .lock_namespace(),
                    table_id,
                },
                LockMode::X,
            );
            if !out
                .iter()
                .any(|existing| existing.resource == request.resource)
            {
                out.push(request);
            }
            Ok(())
        }
        TableReference::Subquery { subquery, .. } => {
            collect_set_expr_table_locks(session, &subquery.body, out)
        }
        TableReference::Join { join, .. } => {
            collect_table_reference_locks(session, &join.left, out)?;
            collect_table_reference_locks(session, &join.right, out)
        }
        TableReference::TableFunction { .. }
        | TableReference::Location { .. }
        | TableReference::GraphTable { .. } => Err(paro_error::not_supported(
            "SELECT FOR UPDATE only supports base tables and subqueries",
        )),
    }
}

fn resolve_for_update_table_id(
    session: &Session,
    database: Option<&Identifier>,
    schema: Option<&Identifier>,
    table: &Identifier,
) -> Result<TableId> {
    let catalog_txn = session.catalog_txn_view();
    let table_name = table.name.clone();
    let entry = match (database, schema) {
        (Some(database), Some(schema)) => {
            if !database
                .name
                .eq_ignore_ascii_case(session.current_database.name())
            {
                return Err(paro_error::not_implemented(format!(
                    "SELECT FOR UPDATE cross-database lock: {}.{}",
                    database.name, schema.name
                )));
            }
            session
                .current_database
                .catalog()
                .get_table(&catalog_txn, &schema.name, &table_name)?
        }
        (None, Some(schema)) => {
            session
                .current_database
                .catalog()
                .get_table(&catalog_txn, &schema.name, &table_name)?
        }
        (None, None) => {
            let mut found = None;
            for search_entry in session.search_path().get() {
                let catalog_name = if search_entry.catalog.is_empty() {
                    session.current_database.name()
                } else {
                    search_entry.catalog.as_str()
                };
                if !catalog_name.eq_ignore_ascii_case(session.current_database.name()) {
                    continue;
                }
                if let Ok(entry) = session.current_database.catalog().get_table(
                    &catalog_txn,
                    &search_entry.schema,
                    &table_name,
                ) {
                    found = Some(entry);
                    break;
                }
            }
            found.ok_or_else(|| paro_error::table_not_found(&table_name))?
        }
        (Some(_), None) => {
            return Err(paro_error::catalog(
                "Invalid table reference: database provided without schema",
            ))
        }
    };

    let table = entry
        .as_table()
        .ok_or_else(|| paro_error::wrong_object_type("table", &table_name))?;
    if let Some(descriptor) = table.get_storage_descriptor() {
        return Ok(TableId::new(descriptor.table_id));
    }
    let storage = table.get_storage().ok_or_else(|| {
        paro_error::internal(format!(
            "SELECT FOR UPDATE target {table_name} has no storage"
        ))
    })?;
    Ok(TableId::new(storage.table_id()))
}

fn validate_copy_from_options(copy_stmt: &CopyStmt) -> Result<()> {
    let options = paro_function::copy::CopyOptions::from_ast(&copy_stmt.options)?;
    match options.format {
        paro_function::copy::CopyFormat::Binary => Err(paro_error::not_implemented(
            "COPY FROM STDIN BINARY is not supported yet",
        )),
        paro_function::copy::CopyFormat::Csv
        | paro_function::copy::CopyFormat::Text
        | paro_function::copy::CopyFormat::Ndjson => Ok(()),
    }
}

fn resolve_copy_from_target_columns(
    session: &Session,
    copy_stmt: &CopyStmt,
) -> Result<Vec<String>> {
    let CopyTarget::Table { name, columns } = &copy_stmt.target else {
        return Err(paro_error::not_supported(
            "COPY FROM STDIN only supports table targets",
        ));
    };

    let schema_name = name
        .schema
        .as_ref()
        .map(|schema| schema.name.as_str())
        .unwrap_or("public");
    let snapshot = session.catalog_txn_view();
    let table_entry =
        session
            .current_database
            .catalog()
            .get_table(&snapshot, schema_name, &name.table.name)?;
    let table = table_entry
        .as_table()
        .ok_or_else(|| paro_error::catalog(format!("\"{}\" is not a table", name.table.name)))?;

    match columns {
        Some(columns) => columns
            .iter()
            .map(|column| {
                let matched = table
                    .columns
                    .iter()
                    .find(|def| def.name.eq_ignore_ascii_case(&column.name))
                    .ok_or_else(|| {
                        paro_error::catalog(format!(
                            "column \"{}\" does not exist in table \"{}\"",
                            column.name, name.table.name
                        ))
                    })?;
                Ok(matched.name.clone())
            })
            .collect(),
        None => Ok(table
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect()),
    }
}

#[derive(Default)]
struct CompletionCaptureSink {
    completion: Option<StatementCompletion>,
}

impl CompletionCaptureSink {
    fn into_completion(self) -> Result<StatementCompletion> {
        self.completion.ok_or_else(|| {
            paro_error::internal("COPY execution completed without a command tag".to_string())
        })
    }
}

#[async_trait]
impl ResultSink for CompletionCaptureSink {
    async fn start_result(&mut self, _names: &[String], _types: &[LogicalType]) -> Result<()> {
        Ok(())
    }

    async fn push_chunk(&mut self, _chunk: &paro_common::chunk::Chunk) -> Result<()> {
        Ok(())
    }

    async fn finish_result(&mut self, completion: &StatementCompletion) -> Result<()> {
        self.completion = Some(completion.clone());
        Ok(())
    }
}

impl ProtocolResultSink for CompletionCaptureSink {}

struct FramedCopySourceBridge {
    buffer: Vec<u8>,
    limit: usize,
    memory: CopyStdinPayloadMemoryTracker,
}

#[derive(Debug)]
struct BufferedCopyStdinPayload {
    data: Vec<u8>,
    _memory: CopyStdinPayloadMemoryTracker,
}

impl CopyStdinSource for BufferedCopyStdinPayload {
    fn as_bytes(&self) -> &[u8] {
        self.data.as_slice()
    }
}

#[derive(Debug)]
/// Keeps resident-memory accounting attached to the COPY payload as ownership
/// moves from socket collection into the statement execution context.
struct CopyStdinPayloadMemoryTracker {
    tracked_bytes: usize,
    metrics: Arc<CopyStdinMetrics>,
}

impl CopyStdinPayloadMemoryTracker {
    fn new(metrics: Arc<CopyStdinMetrics>) -> Self {
        Self {
            tracked_bytes: 0,
            metrics,
        }
    }

    fn observe(&mut self, new_bytes: usize) {
        self.metrics
            .observe_buffer_bytes(self.tracked_bytes, new_bytes);
        self.tracked_bytes = new_bytes;
    }

    fn record_rejection(&self, reason: CopyStdinRejectReason) {
        self.metrics.record_rejection(reason);
    }
}

impl Drop for CopyStdinPayloadMemoryTracker {
    fn drop(&mut self) {
        self.metrics.finish_buffering(self.tracked_bytes);
    }
}

impl FramedCopySourceBridge {
    fn new(limit: usize, metrics: Arc<CopyStdinMetrics>) -> Self {
        Self {
            buffer: Vec::new(),
            limit,
            memory: CopyStdinPayloadMemoryTracker::new(metrics),
        }
    }

    async fn collect(
        mut self,
        source: &mut dyn CopyProtocolSource,
        cancellation: StatementCancellation,
    ) -> Result<BufferedCopyStdinPayload> {
        while let Some(chunk) = source.next_chunk().await? {
            cancellation.check()?;
            let new_len = self.buffer.len().checked_add(chunk.len()).ok_or_else(|| {
                self.memory
                    .record_rejection(CopyStdinRejectReason::TotalLimit);
                paro_error::configuration_limit_exceeded("COPY FROM STDIN payload overflow")
            })?;
            if new_len > self.limit {
                self.memory
                    .record_rejection(CopyStdinRejectReason::TotalLimit);
                return Err(paro_error::configuration_limit_exceeded(
                    "COPY FROM STDIN payload exceeds memory limit",
                ));
            }
            self.buffer.try_reserve(chunk.len()).map_err(|_| {
                paro_error::out_of_memory("COPY FROM STDIN payload allocation failed")
            })?;
            self.buffer.extend_from_slice(&chunk);
            self.memory.observe(new_len);
            cancellation.check()?;
        }
        Ok(BufferedCopyStdinPayload {
            data: self.buffer,
            _memory: self.memory,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::bytes::Bytes;

    fn parse_one(sql: &str) -> Statement {
        paro_parser::parse(sql)
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .stmt
    }

    #[test]
    fn read_tracking_hint_selects_exact_read_only_policy() {
        let stmt = parse_one("SETTINGS (read_tracking_policy = 'range_critical') SELECT 1");
        let selection =
            read_tracking_selection_for_statement(&stmt, IsolationLevel::Serializable).unwrap();

        assert_eq!(selection.policy, ReadTrackingPolicy::RangeCritical);
        assert!(selection.had_user_hint);
        assert!(!selection.escalated);
        assert!(!selection.requires_read_write);
    }

    #[test]
    fn read_tracking_hint_cannot_downgrade_write_statement() {
        let stmt =
            parse_one("SETTINGS (read_tracking_policy = 'safe_snapshot_preferred') SELECT * FROM t FOR UPDATE");
        let selection =
            read_tracking_selection_for_statement(&stmt, IsolationLevel::Serializable).unwrap();

        assert_eq!(selection.policy, ReadTrackingPolicy::RangeCritical);
        assert!(selection.had_user_hint);
        assert!(selection.escalated);
        assert!(selection.requires_read_write);
    }

    #[test]
    fn read_tracking_hint_rejects_unknown_policy() {
        let stmt = parse_one("SETTINGS (read_tracking_policy = 'fast') SELECT 1");
        let err = read_tracking_selection_for_statement(&stmt, IsolationLevel::Serializable)
            .expect_err("unknown hint must fail");

        assert!(err.message().contains("unsupported read_tracking_policy"));
    }

    struct TestCopySource {
        chunks: Vec<Bytes>,
    }

    #[async_trait]
    impl CopyProtocolSource for TestCopySource {
        async fn begin_copy_in(&mut self, _spec: &CopyInSpec) -> Result<()> {
            Ok(())
        }

        async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
            if self.chunks.is_empty() {
                Ok(None)
            } else {
                Ok(Some(self.chunks.remove(0)))
            }
        }
    }

    #[tokio::test]
    async fn copy_stdin_payload_metrics_span_collection_and_execution() {
        let metrics = Arc::new(CopyStdinMetrics::default());
        let mut source = TestCopySource {
            chunks: vec![Bytes::from_static(b"12"), Bytes::from_static(b"345")],
        };

        let payload = FramedCopySourceBridge::new(16, metrics.clone())
            .collect(
                &mut source,
                StatementCancellation::new(tokio_util::sync::CancellationToken::new(), None),
            )
            .await
            .unwrap();
        assert_eq!(payload.data, b"12345");

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.current_buffer_bytes, 5);
        assert_eq!(snapshot.peak_buffer_bytes, 5);
        assert_eq!(snapshot.rejected_total, 0);

        let input = StatementInput::copy_from_stdin(Arc::new(payload));
        assert_eq!(metrics.snapshot().current_buffer_bytes, 5);
        drop(input);
        assert_eq!(metrics.snapshot().current_buffer_bytes, 0);
    }

    #[tokio::test]
    async fn framed_copy_source_bridge_records_total_limit_rejections() {
        let metrics = Arc::new(CopyStdinMetrics::default());
        let mut source = TestCopySource {
            chunks: vec![Bytes::from_static(b"12"), Bytes::from_static(b"345")],
        };

        let err = FramedCopySourceBridge::new(4, metrics.clone())
            .collect(
                &mut source,
                StatementCancellation::new(tokio_util::sync::CancellationToken::new(), None),
            )
            .await
            .expect_err("payload should exceed limit");
        assert!(err
            .message()
            .contains("COPY FROM STDIN payload exceeds memory limit"));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.current_buffer_bytes, 0);
        assert_eq!(snapshot.peak_buffer_bytes, 2);
        assert_eq!(snapshot.rejected_total, 1);
        assert_eq!(snapshot.rejected_total_limit, 1);
        assert_eq!(snapshot.rejected_frame_limit, 0);
    }
}
