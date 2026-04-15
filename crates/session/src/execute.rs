//! SQL execution entry points for simple-query front-end routing.

use crate::completion::StatementCompletion;
use crate::completion_infer::{infer_statement_completion, initial_statement_completion};
use crate::copy_metrics::{copy_stdin_metrics, CopyStdinRejectReason};
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
use paro_compiler::{compile_statement, compile_statement_with_parameters};
use paro_context::{StatementOptions, StatementSource};
use paro_execution::query_executor::executor::Executor;
use paro_parser::ast::{CopyDirection, CopySource, CopyStmt, CopyTarget, ExecuteStmt, Statement};
use std::time::Instant;
use tracing::{debug, error};

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
        self.begin_query_internal(&query_str);

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
            .execute_frontend_route(route, statement_format, sink)
            .await;

        match &result {
            Ok(()) => self.end_query_internal(true),
            Err(e) => self.end_query_internal_with_error(e),
        }

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
            statement_format,
            StatementSource::SimpleQuery,
            completion_override,
            sink,
        )
        .await
    }

    async fn execute_query_pipeline_with_parameters<S: ProtocolResultSink>(
        &mut self,
        stmt: Statement,
        parameter_env: Option<&TypedParameterEnv>,
        statement_format: Option<String>,
        source: StatementSource,
        completion_override: Option<StatementCompletion>,
        sink: &mut S,
    ) -> Result<()> {
        let require_new_transaction =
            self.transaction.is_auto_commit() && !self.transaction.has_active_transaction();

        if require_new_transaction {
            self.begin_transaction_internal()?;
        }

        let statement_completion = initial_statement_completion(&stmt);
        let started_at = Instant::now();

        let ctx = self.freeze_statement_context(StatementOptions {
            statement_format,
            source,
            ..StatementOptions::default()
        });

        debug!(
            target: targets::QUERY,
            session_id = self.id,
            statement_completion = %statement_completion,
            "Statement compilation started"
        );
        let compile_result = match parameter_env {
            Some(parameter_env) => {
                compile_statement_with_parameters(ctx.clone(), stmt.clone(), parameter_env)
            }
            None => compile_statement(ctx.clone(), stmt.clone()),
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
        debug!(
            target: targets::QUERY,
            session_id = self.id,
            statement_completion = %statement_completion,
            result_columns = compiled.result_schema.len(),
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

        let result = self.get_executor().execute(compiled.clone());

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
                let mut protocol_sink = sink.create_copy_out_sink(options)?;
                self.execute_copy_to_core(
                    query_stmt,
                    None,
                    statement_format.clone(),
                    source,
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
                let mut protocol_source = sink.create_copy_in_source()?;
                self.execute_copy_from_core(
                    copy_stmt,
                    None,
                    statement_format.clone(),
                    source,
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
        protocol_sink: &mut dyn CopyProtocolSink,
    ) -> Result<StatementCompletion> {
        let statement_completion = StatementCompletion::Copy { rows: 0 };
        let started_at = Instant::now();

        let ctx = self.freeze_statement_context(StatementOptions {
            statement_format,
            source,
            ..StatementOptions::default()
        });

        debug!(
            target: targets::QUERY,
            session_id = self.id,
            statement_completion = %statement_completion,
            "Statement compilation started"
        );
        let compiled = match parameter_env {
            Some(parameter_env) => {
                compile_statement_with_parameters(ctx.clone(), query_stmt, parameter_env)?
            }
            None => compile_statement(ctx.clone(), query_stmt)?,
        };
        debug!(
            target: targets::QUERY,
            session_id = self.id,
            statement_completion = %statement_completion,
            result_columns = compiled.result_schema.len(),
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

        let mut stream = self.get_executor().execute(compiled).map_err(|e| {
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
        protocol_source: &mut dyn CopyProtocolSource,
    ) -> Result<StatementCompletion> {
        validate_copy_from_options(copy_stmt)?;
        let column_count = resolve_copy_from_target_columns(self, copy_stmt)?.len();
        let spec = CopyInSpec {
            overall_format: 0,
            column_formats: vec![0; column_count],
        };
        protocol_source.begin_copy_in(&spec).await?;

        let payload = FramedCopySourceBridge::new(self.copy_stdin_memory_limit())
            .collect(protocol_source)
            .await?;
        let virtual_path = paro_function::table::read_csv::register_copy_stdin_payload(payload);
        let mut rewritten = copy_stmt.clone();
        rewritten.source = CopySource::File(virtual_path.clone());

        let mut sink = CompletionCaptureSink::default();
        let result = self
            .execute_query_pipeline_with_parameters(
                Statement::Copy(rewritten),
                parameter_env,
                statement_format,
                source,
                None,
                &mut sink,
            )
            .await;
        paro_function::table::read_csv::unregister_copy_stdin_payload(&virtual_path);

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

fn validate_copy_from_options(copy_stmt: &CopyStmt) -> Result<()> {
    let options = paro_function::copy::CopyOptions::from_ast(&copy_stmt.options)?;
    match options.format {
        paro_function::copy::CopyFormat::Binary => Err(paro_error::not_implemented(
            "COPY FROM STDIN BINARY is not supported yet",
        )),
        paro_function::copy::CopyFormat::Ndjson => Err(paro_error::not_implemented(
            "COPY FROM STDIN NDJSON is not supported yet",
        )),
        paro_function::copy::CopyFormat::Csv | paro_function::copy::CopyFormat::Text => Ok(()),
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
    tracked_bytes: usize,
}

impl FramedCopySourceBridge {
    fn new(limit: usize) -> Self {
        Self {
            buffer: Vec::new(),
            limit,
            tracked_bytes: 0,
        }
    }

    async fn collect(mut self, source: &mut dyn CopyProtocolSource) -> Result<Vec<u8>> {
        while let Some(chunk) = source.next_chunk().await? {
            let new_len = self.buffer.len().checked_add(chunk.len()).ok_or_else(|| {
                copy_stdin_metrics().record_rejection(CopyStdinRejectReason::TotalLimit);
                paro_error::configuration_limit_exceeded("COPY FROM STDIN payload overflow")
            })?;
            if new_len > self.limit {
                copy_stdin_metrics().record_rejection(CopyStdinRejectReason::TotalLimit);
                return Err(paro_error::configuration_limit_exceeded(
                    "COPY FROM STDIN payload exceeds memory limit",
                ));
            }
            self.buffer.try_reserve(chunk.len()).map_err(|_| {
                paro_error::out_of_memory("COPY FROM STDIN payload allocation failed")
            })?;
            self.buffer.extend_from_slice(&chunk);
            copy_stdin_metrics().observe_buffer_bytes(self.tracked_bytes, new_len);
            self.tracked_bytes = new_len;
        }
        Ok(std::mem::take(&mut self.buffer))
    }
}

impl Drop for FramedCopySourceBridge {
    fn drop(&mut self) {
        copy_stdin_metrics().finish_buffering(self.tracked_bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use tokio_util::bytes::Bytes;

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
    #[serial]
    async fn framed_copy_source_bridge_updates_metrics_and_resets_current_bytes() {
        copy_stdin_metrics().reset_for_tests();
        let mut source = TestCopySource {
            chunks: vec![Bytes::from_static(b"12"), Bytes::from_static(b"345")],
        };

        let payload = FramedCopySourceBridge::new(16)
            .collect(&mut source)
            .await
            .unwrap();
        assert_eq!(payload, b"12345");

        let metrics = copy_stdin_metrics().snapshot();
        assert_eq!(metrics.current_buffer_bytes, 0);
        assert_eq!(metrics.peak_buffer_bytes, 5);
        assert_eq!(metrics.rejected_total, 0);
    }

    #[tokio::test]
    #[serial]
    async fn framed_copy_source_bridge_records_total_limit_rejections() {
        copy_stdin_metrics().reset_for_tests();
        let mut source = TestCopySource {
            chunks: vec![Bytes::from_static(b"12"), Bytes::from_static(b"345")],
        };

        let err = FramedCopySourceBridge::new(4)
            .collect(&mut source)
            .await
            .expect_err("payload should exceed limit");
        assert!(err
            .message()
            .contains("COPY FROM STDIN payload exceeds memory limit"));

        let metrics = copy_stdin_metrics().snapshot();
        assert_eq!(metrics.current_buffer_bytes, 0);
        assert_eq!(metrics.peak_buffer_bytes, 2);
        assert_eq!(metrics.rejected_total, 1);
        assert_eq!(metrics.rejected_total_limit, 1);
        assert_eq!(metrics.rejected_frame_limit, 0);
    }
}
