// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! PgWire COPY protocol adapters.
//!
//! This module keeps pgwire message choreography in the server crate while the
//! session crate owns COPY routing and execution decisions.

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, ParoError, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::copy::{CopyFormat, CopyOptions, ForceQuoteOption};
use paro_session::{
    ActiveStatementControl, CopyInSpec, CopyProtocolSink, CopyProtocolSource, ResultSink,
    SessionExecutionControl, StatementCompletion,
};
use pgwire::messages::copy::{CopyData, CopyDone, CopyInResponse, CopyOutResponse};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio_util::bytes::Bytes;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use crate::connection::PgCodec;
use crate::protocol::result::{
    build_error_response, format_pg_array, value_to_pg_text, PgWireResultSink,
};

const COPY_TEXT_FORMAT_CODE: i8 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFrontendMode {
    SimpleQuery,
    ExtendedQuery,
}

pub struct PgWireCopyOutSink<'a> {
    socket: &'a mut Framed<TcpStream, PgCodec>,
    active_statement: Arc<ActiveStatementControl>,
    options: CopyOptions,
    names: Vec<String>,
    force_quote_columns: Vec<bool>,
    col_count: usize,
}

impl<'a> PgWireCopyOutSink<'a> {
    pub fn new(
        socket: &'a mut Framed<TcpStream, PgCodec>,
        active_statement: Arc<ActiveStatementControl>,
        options: CopyOptions,
    ) -> Result<Self> {
        if matches!(options.format, CopyFormat::Binary) {
            return Err(paro_error::not_implemented(
                "COPY TO STDOUT BINARY is not supported yet",
            ));
        }
        if matches!(options.format, CopyFormat::Ndjson) {
            return Err(paro_error::not_implemented(
                "COPY TO STDOUT NDJSON is not supported yet",
            ));
        }

        let delimiter = options
            .delimiter()
            .expect("CSV/TEXT options always have a delimiter");

        if delimiter.is_empty() || delimiter.chars().count() != 1 {
            return Err(paro_error::invalid_parameter(
                "COPY option delimiter must be a single character",
            ));
        }

        Ok(Self {
            socket,
            active_statement,
            options,
            names: Vec::new(),
            force_quote_columns: Vec::new(),
            col_count: 0,
        })
    }

    fn delimiter(&self) -> String {
        self.options
            .delimiter()
            .expect("COPY OUT only supports CSV/TEXT formats")
            .to_string()
    }

    fn null_string(&self) -> String {
        self.options
            .null_string()
            .expect("COPY OUT only supports CSV/TEXT formats")
            .to_string()
    }

    fn quote_char(&self) -> Option<char> {
        self.options.quote()
    }

    fn escape_char(&self) -> Option<char> {
        self.options.escape()
    }

    async fn send_copy_data_line(&mut self, line: String) -> Result<()> {
        self.send_backend_message(PgWireBackendMessage::CopyData(CopyData::new(Bytes::from(
            line,
        ))))
        .await
    }

    /// Send one complete COPY frame after observing statement cancellation.
    ///
    /// Once `Framed::send` starts, it must run to completion: dropping that
    /// future while it is flushing can leave an encoded COPY frame pending and
    /// prevent the subsequent error response from making progress. A client
    /// that issued a cancel request resumes reading the original connection;
    /// the in-flight frame can then drain, and the next frame boundary reports
    /// the cancellation without corrupting the protocol stream.
    async fn send_backend_message(&mut self, message: PgWireBackendMessage) -> Result<()> {
        self.active_statement.cancellation().check()?;
        self.socket
            .send(message)
            .await
            .map_err(|e| paro_error::internal(e.to_string()))
    }

    fn append_csv_field(&self, out: &mut String, field: &str, force_quote: bool) {
        let quote = self.quote_char();
        let escape = self.escape_char().or(quote);
        let needs_quote = self.should_quote_csv(field, force_quote);

        if needs_quote {
            let quote_char = quote.unwrap_or('"');
            let escape_char = escape.unwrap_or(quote_char);
            out.push(quote_char);
            for ch in field.chars() {
                if ch == quote_char || ch == escape_char {
                    out.push(escape_char);
                }
                out.push(ch);
            }
            out.push(quote_char);
        } else {
            out.push_str(field);
        }
    }

    fn should_quote_csv(&self, field: &str, force_quote: bool) -> bool {
        if force_quote {
            return true;
        }

        let quote = match self.quote_char() {
            Some(q) => q,
            None => return false,
        };

        let delimiter = self.delimiter();
        if field.contains('\n') || field.contains('\r') {
            return true;
        }
        if field.contains(&delimiter) {
            return true;
        }

        field.contains(quote)
    }

    fn append_text_field(&self, out: &mut String, field: &str) {
        let delimiter = self.delimiter().chars().next().unwrap_or('\t');
        for ch in field.chars() {
            match ch {
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                '\u{0008}' => out.push_str("\\b"),
                '\u{000c}' => out.push_str("\\f"),
                '\u{000b}' => out.push_str("\\v"),
                other if other == delimiter => {
                    out.push('\\');
                    out.push(other);
                }
                other => out.push(other),
            }
        }
    }

    fn value_to_text(&self, value: &Value) -> String {
        match value {
            Value::List(values, _) | Value::Array(values, _, _) => format_pg_array(values),
            _ => value_to_pg_text(value),
        }
    }

    fn serialize_row(&self, chunk: &Chunk, row: usize) -> String {
        let mut line = String::new();
        let delimiter = self.delimiter();
        let null_string = self.null_string();

        for col in 0..self.col_count {
            if col > 0 {
                line.push_str(&delimiter);
            }

            let value = chunk.column(col).map(|v| v.get_value(row));
            let is_null = chunk.column(col).map(|v| v.is_null(row)).unwrap_or(true);
            let field = if is_null {
                null_string.clone()
            } else {
                self.value_to_text(value.as_ref().unwrap())
            };

            if is_null {
                line.push_str(&field);
                continue;
            }

            match self.options.format {
                CopyFormat::Csv => {
                    let force_quote = self.force_quote_columns.get(col).copied().unwrap_or(false);
                    self.append_csv_field(&mut line, &field, force_quote);
                }
                CopyFormat::Text => {
                    self.append_text_field(&mut line, &field);
                }
                CopyFormat::Binary => {
                    line.push_str(&field);
                }
                CopyFormat::Ndjson => {
                    line.push_str(&field);
                }
            }
        }

        line.push('\n');
        line
    }

    fn serialize_header(&self) -> String {
        let mut line = String::new();
        let delimiter = self.delimiter();

        for (idx, name) in self.names.iter().enumerate() {
            if idx > 0 {
                line.push_str(&delimiter);
            }

            match self.options.format {
                CopyFormat::Csv => self.append_csv_field(&mut line, name, false),
                CopyFormat::Text => self.append_text_field(&mut line, name),
                CopyFormat::Binary => line.push_str(name),
                CopyFormat::Ndjson => line.push_str(name),
            }
        }

        line.push('\n');
        line
    }
}

fn normalize_copy_header_name(name: &str) -> String {
    name.rsplit('.').next().unwrap_or(name).to_string()
}

#[async_trait]
impl<'a> CopyProtocolSink for PgWireCopyOutSink<'a> {
    async fn start_copy_out(&mut self, names: &[String], types: &[LogicalType]) -> Result<()> {
        self.col_count = names.len();
        let normalized_names = names
            .iter()
            .map(|name| normalize_copy_header_name(name))
            .collect::<Vec<_>>();
        self.force_quote_columns = resolve_force_quote_columns(
            &self.options.force_quote,
            &normalized_names,
            self.options.format,
        )?;
        self.names = normalized_names;

        let _ = types;

        self.send_backend_message(PgWireBackendMessage::CopyOutResponse(CopyOutResponse::new(
            COPY_TEXT_FORMAT_CODE,
            self.col_count as i16,
            vec![0; self.col_count],
        )))
        .await?;

        if self.options.header() {
            let header = self.serialize_header();
            self.send_copy_data_line(header).await?;
        }

        Ok(())
    }

    async fn push_copy_rows(&mut self, chunk: &Chunk) -> Result<()> {
        for row in 0..chunk.size() {
            let line = self.serialize_row(chunk, row);
            self.send_copy_data_line(line).await?;
        }
        Ok(())
    }

    async fn finish_copy_out(&mut self) -> Result<()> {
        self.send_backend_message(PgWireBackendMessage::CopyDone(CopyDone::new()))
            .await
    }
}

#[async_trait]
impl<'a> ResultSink for PgWireCopyOutSink<'a> {
    async fn start_result(&mut self, names: &[String], types: &[LogicalType]) -> Result<()> {
        self.start_copy_out(names, types).await
    }

    async fn push_chunk(&mut self, chunk: &Chunk) -> Result<()> {
        self.push_copy_rows(chunk).await
    }

    async fn finish_result(&mut self, completion: &StatementCompletion) -> Result<()> {
        let _ = completion;
        self.finish_copy_out().await
    }

    async fn error(&mut self, err: &ParoError) -> Result<()> {
        self.socket
            .send(PgWireBackendMessage::ErrorResponse(build_error_response(
                err,
            )))
            .await
            .map_err(|e| paro_error::internal(e.to_string()))?;
        Ok(())
    }
}

pub struct PgWireCopyInSink<'a> {
    sink: PgWireResultSink<'a>,
    receiver: CopyFrontendReceiver<'a>,
}

impl<'a> PgWireCopyInSink<'a> {
    pub fn new(
        socket: &'a mut Framed<TcpStream, PgCodec>,
        execution_control: Arc<SessionExecutionControl>,
        drain_token: CancellationToken,
        force_close_token: CancellationToken,
        pending_frontend_messages: Arc<Mutex<VecDeque<PgWireFrontendMessage>>>,
        mode: CopyFrontendMode,
    ) -> Self {
        Self {
            sink: PgWireResultSink::new(socket),
            receiver: CopyFrontendReceiver::new(
                execution_control,
                drain_token,
                force_close_token,
                pending_frontend_messages,
                mode,
            ),
        }
    }

    async fn send_copy_in_response(&mut self, spec: &CopyInSpec) -> Result<()> {
        self.sink
            .socket_mut()
            .send(PgWireBackendMessage::CopyInResponse(CopyInResponse::new(
                spec.overall_format,
                spec.column_formats.len() as i16,
                spec.column_formats.clone(),
            )))
            .await
            .map_err(|e| paro_error::internal(e.to_string()))
    }
}

struct CopyFrontendReceiver<'a> {
    execution_control: Arc<SessionExecutionControl>,
    drain_token: CancellationToken,
    force_close_token: CancellationToken,
    pending_frontend_messages: Arc<Mutex<VecDeque<PgWireFrontendMessage>>>,
    mode: CopyFrontendMode,
    _marker: std::marker::PhantomData<&'a mut ()>,
}

impl<'a> CopyFrontendReceiver<'a> {
    fn new(
        execution_control: Arc<SessionExecutionControl>,
        drain_token: CancellationToken,
        force_close_token: CancellationToken,
        pending_frontend_messages: Arc<Mutex<VecDeque<PgWireFrontendMessage>>>,
        mode: CopyFrontendMode,
    ) -> Self {
        Self {
            execution_control,
            drain_token,
            force_close_token,
            pending_frontend_messages,
            mode,
            _marker: std::marker::PhantomData,
        }
    }

    fn active_statement(&self) -> Result<std::sync::Arc<paro_session::ActiveStatementControl>> {
        self.execution_control.active_statement().ok_or_else(|| {
            paro_error::internal("COPY FROM STDIN requires an active statement scope")
        })
    }

    async fn next_message(
        &self,
        socket: &mut Framed<TcpStream, PgCodec>,
    ) -> Result<PgWireFrontendMessage> {
        let active_statement = self.active_statement()?;
        let cancellation = active_statement.cancellation();
        let statement_token = active_statement.statement_token();

        tokio::select! {
            biased;
            _ = self.force_close_token.cancelled() => {
                Err(paro_error::internal(
                    "connection force-closed during COPY FROM STDIN".to_string(),
                ))
            }
            _ = self.drain_token.cancelled() => {
                Err(paro_error::internal(
                    "connection drained during COPY FROM STDIN".to_string(),
                ))
            }
            _ = statement_token.cancelled() => {
                cancellation.check()?;
                Err(paro_error::query_canceled())
            }
            message = socket.next() => {
                match message {
                    Some(Ok(message)) => Ok(message),
                    Some(Err(error)) => Err(paro_error::internal(error.to_string())),
                    None => Err(paro_error::internal(
                        "connection closed during COPY FROM STDIN".to_string(),
                    )),
                }
            }
        }
    }

    async fn drain_until_copy_terminator(&self, socket: &mut Framed<TcpStream, PgCodec>) {
        loop {
            tokio::select! {
                biased;
                _ = self.force_close_token.cancelled() => break,
                _ = self.drain_token.cancelled() => break,
                message = socket.next() => {
                    match message {
                        Some(Ok(PgWireFrontendMessage::CopyData(_)))
                        | Some(Ok(PgWireFrontendMessage::Flush(_)))
                        => continue,
                        Some(Ok(message @ PgWireFrontendMessage::Sync(_)))
                        | Some(Ok(message @ PgWireFrontendMessage::Terminate(_))) => {
                            if matches!(self.mode, CopyFrontendMode::ExtendedQuery) {
                                self.pending_frontend_messages
                                    .lock()
                                    .expect("pending frontend queue")
                                    .push_back(message);
                            }
                            break;
                        }
                        Some(Ok(PgWireFrontendMessage::CopyDone(_)))
                        | Some(Ok(PgWireFrontendMessage::CopyFail(_)))
                        | Some(Ok(_))
                        | Some(Err(_))
                        | None => break,
                    }
                }
            }
        }
    }
}

#[async_trait]
impl<'a> CopyProtocolSource for PgWireCopyInSink<'a> {
    async fn begin_copy_in(&mut self, spec: &CopyInSpec) -> Result<()> {
        self.sink.socket_mut().codec_mut().enter_copy_data_mode();
        self.send_copy_in_response(spec).await
    }

    async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        loop {
            let msg = match self.receiver.next_message(self.sink.socket_mut()).await {
                Ok(msg) => msg,
                Err(err) => {
                    self.receiver
                        .drain_until_copy_terminator(self.sink.socket_mut())
                        .await;
                    return Err(err);
                }
            };

            match msg {
                PgWireFrontendMessage::CopyData(copy_data) => {
                    return Ok(Some(copy_data.data));
                }
                PgWireFrontendMessage::CopyDone(_) => return Ok(None),
                PgWireFrontendMessage::CopyFail(fail) => {
                    let message = if fail.message.is_empty() {
                        "COPY from stdin aborted by client".to_string()
                    } else {
                        format!("COPY from stdin aborted by client: {}", fail.message)
                    };
                    return Err(paro_error::query_canceled_message(message));
                }
                PgWireFrontendMessage::Flush(_) | PgWireFrontendMessage::Sync(_) => {
                    continue;
                }
                PgWireFrontendMessage::Terminate(_) => {
                    return Err(paro_error::internal(
                        "connection terminated during COPY FROM STDIN".to_string(),
                    ))
                }
                _ => {
                    let err = paro_error::protocol_violation(
                        "unexpected frontend message during COPY FROM STDIN".to_string(),
                    );
                    self.receiver
                        .drain_until_copy_terminator(self.sink.socket_mut())
                        .await;
                    return Err(err);
                }
            }
        }
    }
}

impl Drop for PgWireCopyInSink<'_> {
    fn drop(&mut self) {
        self.sink.socket_mut().codec_mut().leave_copy_data_mode();
    }
}

#[async_trait]
impl<'a> ResultSink for PgWireCopyInSink<'a> {
    async fn start_result(&mut self, names: &[String], types: &[LogicalType]) -> Result<()> {
        self.sink.start_result(names, types).await
    }

    async fn push_chunk(&mut self, chunk: &Chunk) -> Result<()> {
        self.sink.push_chunk(chunk).await
    }

    async fn finish_result(&mut self, completion: &StatementCompletion) -> Result<()> {
        self.sink.finish_result(completion).await
    }

    async fn error(&mut self, err: &ParoError) -> Result<()> {
        self.sink.error(err).await
    }
}

fn resolve_force_quote_columns(
    option: &ForceQuoteOption,
    names: &[String],
    format: CopyFormat,
) -> Result<Vec<bool>> {
    if matches!(format, CopyFormat::Text) && !matches!(option, ForceQuoteOption::None) {
        return Err(paro_error::invalid_parameter(
            "FORCE_QUOTE is only supported for CSV format",
        ));
    }

    let mut result = vec![false; names.len()];
    match option {
        ForceQuoteOption::None => {}
        ForceQuoteOption::All => {
            result.fill(true);
        }
        ForceQuoteOption::Columns(columns) => {
            for col in columns {
                if let Some(idx) = names.iter().position(|name| name.eq_ignore_ascii_case(col)) {
                    result[idx] = true;
                } else {
                    return Err(paro_error::invalid_parameter(format!(
                        "FORCE_QUOTE column '{}' not found in COPY output",
                        col
                    )));
                }
            }
        }
    }

    Ok(result)
}

pub fn create_copy_out_sink<'a>(
    socket: &'a mut Framed<TcpStream, PgCodec>,
    execution_control: Arc<SessionExecutionControl>,
    options: &CopyOptions,
) -> Result<Box<dyn CopyProtocolSink + 'a>> {
    let active_statement = execution_control
        .active_statement()
        .ok_or_else(|| paro_error::internal("COPY TO STDOUT requires an active statement scope"))?;
    Ok(Box::new(PgWireCopyOutSink::new(
        socket,
        active_statement,
        options.clone(),
    )?))
}

pub fn create_copy_in_source<'a>(
    socket: &'a mut Framed<TcpStream, PgCodec>,
    execution_control: Arc<SessionExecutionControl>,
    drain_token: CancellationToken,
    force_close_token: CancellationToken,
    pending_frontend_messages: Arc<Mutex<VecDeque<PgWireFrontendMessage>>>,
    mode: CopyFrontendMode,
) -> Result<Box<dyn CopyProtocolSource + 'a>> {
    Ok(Box::new(PgWireCopyInSink::new(
        socket,
        execution_control,
        drain_token,
        force_close_token,
        pending_frontend_messages,
        mode,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_force_quote_columns_for_csv() {
        let names = vec!["id".to_string(), "name".to_string()];
        let all =
            resolve_force_quote_columns(&ForceQuoteOption::All, &names, CopyFormat::Csv).unwrap();
        assert_eq!(all, vec![true, true]);

        let cols = resolve_force_quote_columns(
            &ForceQuoteOption::Columns(vec!["name".to_string()]),
            &names,
            CopyFormat::Csv,
        )
        .unwrap();
        assert_eq!(cols, vec![false, true]);
    }
}
