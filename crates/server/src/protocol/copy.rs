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
    CopyInSpec, CopyProtocolSink, CopyProtocolSource, ResultSink, StatementCancellation,
    StatementCompletion,
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
const COPY_DATA_TARGET_BYTES: usize = 64 * 1024;
const COPY_CANCEL_FLUSH_GRACE: std::time::Duration = std::time::Duration::from_millis(100);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFrontendMode {
    SimpleQuery,
    ExtendedQuery,
}

pub struct PgWireCopyOutSink<'a> {
    socket: &'a mut Framed<TcpStream, PgCodec>,
    cancellation: StatementCancellation,
    drain_token: CancellationToken,
    force_close_token: CancellationToken,
    options: CopyOptions,
    delimiter: String,
    null_string: String,
    names: Vec<String>,
    force_quote_columns: Vec<bool>,
    col_count: usize,
    copy_buffer: String,
}

impl<'a> PgWireCopyOutSink<'a> {
    pub fn new(
        socket: &'a mut Framed<TcpStream, PgCodec>,
        cancellation: StatementCancellation,
        drain_token: CancellationToken,
        force_close_token: CancellationToken,
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
            cancellation,
            drain_token,
            force_close_token,
            delimiter: delimiter.to_string(),
            null_string: options
                .null_string()
                .expect("COPY OUT only supports CSV/TEXT formats")
                .to_string(),
            options,
            names: Vec::new(),
            force_quote_columns: Vec::new(),
            col_count: 0,
            copy_buffer: String::with_capacity(COPY_DATA_TARGET_BYTES),
        })
    }

    fn quote_char(&self) -> Option<char> {
        self.options.quote()
    }

    fn escape_char(&self) -> Option<char> {
        self.options.escape()
    }

    fn abandon_connection(&self, reason: &'static str) -> ParoError {
        // An interrupted flush leaves a valid prefix in the codec buffer, but
        // an ErrorResponse cannot overtake it. Closing this connection is the
        // only progress-preserving outcome when the peer is not reading.
        self.force_close_token.cancel();
        paro_error::connection_failure(reason)
    }

    /// Send one COPY frame under the connection and statement lifetimes.
    ///
    /// Cancellation observed before encoding a frame is returned normally so
    /// the connection loop can send a terminal ErrorResponse. Cancellation
    /// while a frame is blocked first gets a short flush grace, because a
    /// normal PostgreSQL cancel client resumes reading the original socket. If
    /// the peer still applies backpressure, the connection is abandoned: the
    /// queued frame remains byte-valid, but no terminal response can overtake
    /// it and a non-reading peer cannot observe that response anyway.
    async fn send_cancellable_frame(&mut self, message: PgWireBackendMessage) -> Result<()> {
        if self.force_close_token.is_cancelled() {
            return Err(self.abandon_connection("connection force-closed during COPY TO STDOUT"));
        }
        if self.drain_token.is_cancelled() {
            return Err(self.abandon_connection("connection drained during COPY TO STDOUT"));
        }
        self.cancellation.check()?;

        if let Err(error) = self.socket.feed(message).await {
            self.force_close_token.cancel();
            return Err(paro_error::connection_failure(error.to_string()));
        }

        enum FlushOutcome {
            Flushed(std::io::Result<()>),
            ForceClosed,
            Drained,
            StatementCancelled,
        }

        let outcome = tokio::select! {
            biased;
            _ = self.force_close_token.cancelled() => FlushOutcome::ForceClosed,
            _ = self.drain_token.cancelled() => FlushOutcome::Drained,
            _ = self.cancellation.cancelled() => FlushOutcome::StatementCancelled,
            result = self.socket.flush() => FlushOutcome::Flushed(result),
        };

        match outcome {
            FlushOutcome::Flushed(Ok(())) => Ok(()),
            FlushOutcome::Flushed(Err(error)) => {
                self.force_close_token.cancel();
                Err(paro_error::connection_failure(error.to_string()))
            }
            FlushOutcome::ForceClosed => {
                Err(self.abandon_connection("connection force-closed during COPY TO STDOUT"))
            }
            FlushOutcome::Drained => {
                Err(self.abandon_connection("connection drained during COPY TO STDOUT"))
            }
            FlushOutcome::StatementCancelled => {
                match tokio::time::timeout(COPY_CANCEL_FLUSH_GRACE, self.socket.flush()).await {
                    Ok(Ok(())) => self.cancellation.check(),
                    Ok(Err(error)) => {
                        self.force_close_token.cancel();
                        Err(paro_error::connection_failure(error.to_string()))
                    }
                    Err(_) => Err(self
                        .abandon_connection("statement cancelled while flushing COPY TO STDOUT")),
                }
            }
        }
    }

    async fn flush_copy_buffer(&mut self) -> Result<()> {
        if self.copy_buffer.is_empty() {
            return Ok(());
        }
        let payload = std::mem::replace(
            &mut self.copy_buffer,
            String::with_capacity(COPY_DATA_TARGET_BYTES),
        );
        self.send_cancellable_frame(PgWireBackendMessage::CopyData(CopyData::new(Bytes::from(
            payload,
        ))))
        .await
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

        if field.contains('\n') || field.contains('\r') {
            return true;
        }
        if field.contains(&self.delimiter) {
            return true;
        }

        field.contains(quote)
    }

    fn append_text_field(&self, out: &mut String, field: &str) {
        let delimiter = self.delimiter.chars().next().unwrap_or('\t');
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

    fn append_serialized_row(&self, chunk: &Chunk, row: usize, out: &mut String) {
        for col in 0..self.col_count {
            if col > 0 {
                out.push_str(&self.delimiter);
            }

            let value = chunk.column(col).map(|v| v.get_value(row));
            let is_null = chunk.column(col).map(|v| v.is_null(row)).unwrap_or(true);

            if is_null {
                out.push_str(&self.null_string);
                continue;
            }
            let field = self.value_to_text(value.as_ref().unwrap());

            match self.options.format {
                CopyFormat::Csv => {
                    let force_quote = self.force_quote_columns.get(col).copied().unwrap_or(false);
                    self.append_csv_field(out, &field, force_quote);
                }
                CopyFormat::Text => {
                    self.append_text_field(out, &field);
                }
                CopyFormat::Binary => {
                    out.push_str(&field);
                }
                CopyFormat::Ndjson => {
                    out.push_str(&field);
                }
            }
        }

        out.push('\n');
    }

    fn append_serialized_header(&self, out: &mut String) {
        for (idx, name) in self.names.iter().enumerate() {
            if idx > 0 {
                out.push_str(&self.delimiter);
            }

            match self.options.format {
                CopyFormat::Csv => self.append_csv_field(out, name, false),
                CopyFormat::Text => self.append_text_field(out, name),
                CopyFormat::Binary => out.push_str(name),
                CopyFormat::Ndjson => out.push_str(name),
            }
        }

        out.push('\n');
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

        self.send_cancellable_frame(PgWireBackendMessage::CopyOutResponse(CopyOutResponse::new(
            COPY_TEXT_FORMAT_CODE,
            self.col_count as i16,
            vec![0; self.col_count],
        )))
        .await?;

        if self.options.header() {
            let mut buffer = std::mem::take(&mut self.copy_buffer);
            self.append_serialized_header(&mut buffer);
            self.copy_buffer = buffer;
        }

        Ok(())
    }

    async fn push_copy_rows(&mut self, chunk: &Chunk) -> Result<()> {
        let mut buffer = std::mem::take(&mut self.copy_buffer);
        for row in 0..chunk.size() {
            self.append_serialized_row(chunk, row, &mut buffer);
            if buffer.len() >= COPY_DATA_TARGET_BYTES {
                self.copy_buffer = buffer;
                self.flush_copy_buffer().await?;
                buffer = std::mem::take(&mut self.copy_buffer);
            }
        }
        self.copy_buffer = buffer;
        Ok(())
    }

    async fn finish_copy_out(&mut self) -> Result<()> {
        self.flush_copy_buffer().await?;
        self.send_cancellable_frame(PgWireBackendMessage::CopyDone(CopyDone::new()))
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
        // Terminal errors must bypass statement cancellation. Otherwise the
        // cancellation that caused this callback would suppress its own
        // ErrorResponse and leave a reading client waiting forever.
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
        cancellation: StatementCancellation,
        drain_token: CancellationToken,
        force_close_token: CancellationToken,
        pending_frontend_messages: Arc<Mutex<VecDeque<PgWireFrontendMessage>>>,
        mode: CopyFrontendMode,
    ) -> Self {
        Self {
            sink: PgWireResultSink::new(socket),
            receiver: CopyFrontendReceiver::new(
                cancellation,
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
    cancellation: StatementCancellation,
    drain_token: CancellationToken,
    force_close_token: CancellationToken,
    pending_frontend_messages: Arc<Mutex<VecDeque<PgWireFrontendMessage>>>,
    mode: CopyFrontendMode,
    _marker: std::marker::PhantomData<&'a mut ()>,
}

impl<'a> CopyFrontendReceiver<'a> {
    fn new(
        cancellation: StatementCancellation,
        drain_token: CancellationToken,
        force_close_token: CancellationToken,
        pending_frontend_messages: Arc<Mutex<VecDeque<PgWireFrontendMessage>>>,
        mode: CopyFrontendMode,
    ) -> Self {
        Self {
            cancellation,
            drain_token,
            force_close_token,
            pending_frontend_messages,
            mode,
            _marker: std::marker::PhantomData,
        }
    }

    async fn next_message(
        &self,
        socket: &mut Framed<TcpStream, PgCodec>,
    ) -> Result<PgWireFrontendMessage> {
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
            _ = self.cancellation.cancelled() => {
                self.cancellation.check()?;
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
    cancellation: &StatementCancellation,
    drain_token: CancellationToken,
    force_close_token: CancellationToken,
    options: &CopyOptions,
) -> Result<Box<dyn CopyProtocolSink + 'a>> {
    Ok(Box::new(PgWireCopyOutSink::new(
        socket,
        cancellation.clone(),
        drain_token,
        force_close_token,
        options.clone(),
    )?))
}

pub fn create_copy_in_source<'a>(
    socket: &'a mut Framed<TcpStream, PgCodec>,
    cancellation: &StatementCancellation,
    drain_token: CancellationToken,
    force_close_token: CancellationToken,
    pending_frontend_messages: Arc<Mutex<VecDeque<PgWireFrontendMessage>>>,
    mode: CopyFrontendMode,
) -> Result<Box<dyn CopyProtocolSource + 'a>> {
    Ok(Box::new(PgWireCopyInSink::new(
        socket,
        cancellation.clone(),
        drain_token,
        force_close_token,
        pending_frontend_messages,
        mode,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_instance::CopyStdinMetrics;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    async fn connected_pg_streams() -> (Framed<TcpStream, PgCodec>, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = TcpStream::connect(address);
        let accept = listener.accept();
        let (client, accepted) = tokio::join!(client, accept);
        let (server, _) = accepted.unwrap();
        (
            Framed::new(
                server,
                PgCodec::new(
                    crate::connection::PgFrontendMessageLimits::new(1024 * 1024),
                    Arc::new(CopyStdinMetrics::default()),
                ),
            ),
            client.unwrap(),
        )
    }

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

    #[tokio::test]
    async fn cancellation_before_copy_done_is_reported_without_closing_connection() {
        let (mut server, mut client) = connected_pg_streams().await;
        let statement_token = CancellationToken::new();
        let cancellation = StatementCancellation::new(statement_token.clone(), None);
        let drain_token = CancellationToken::new();
        let force_close_token = CancellationToken::new();
        let mut sink = PgWireCopyOutSink::new(
            &mut server,
            cancellation,
            drain_token,
            force_close_token.clone(),
            CopyOptions::default(),
        )
        .unwrap();

        statement_token.cancel();
        let error = sink
            .finish_copy_out()
            .await
            .expect_err("cancellation at the final frame boundary must win over CopyDone");
        assert!(error.is_query_canceled());
        assert!(!force_close_token.is_cancelled());
        assert!(
            tokio::time::timeout(Duration::from_millis(50), client.read_u8())
                .await
                .is_err(),
            "the cancelled finish must not put CopyDone on the wire"
        );
    }
}
