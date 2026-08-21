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
use tokio_util::bytes::{Bytes, BytesMut};
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use crate::connection::PgCodec;
use crate::protocol::result::PgWireResultSink;
use crate::protocol::value_format::TextVectorEncoder;

const COPY_TEXT_FORMAT_CODE: i8 = 0;
const COPY_DATA_TARGET_BYTES: usize = 64 * 1024;
const COPY_DATA_BUFFER_BYTES: usize = COPY_DATA_TARGET_BYTES * 2;
const COPY_CANCEL_STALL_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(100);

#[derive(Debug, Clone, Copy)]
struct CopyFlushPolicy {
    stalled_write_timeout: std::time::Duration,
}

impl Default for CopyFlushPolicy {
    fn default() -> Self {
        Self {
            stalled_write_timeout: COPY_CANCEL_STALL_TIMEOUT,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFrontendMode {
    SimpleQuery,
    ExtendedQuery,
}

#[derive(Debug, Clone, Copy)]
enum CopyFieldEncoding {
    Csv { quote: char, escape: char },
    Text,
}

struct CopyRowEncoding {
    field_encoding: CopyFieldEncoding,
    delimiter: char,
    null_bytes: Box<[u8]>,
    force_quote_columns: Box<[bool]>,
}

impl CopyRowEncoding {
    fn from_options(options: &CopyOptions) -> Result<Self> {
        let delimiter_text = options
            .delimiter()
            .expect("CSV/TEXT options always have a delimiter");
        let mut delimiters = delimiter_text.chars();
        let delimiter = delimiters.next().ok_or_else(|| {
            paro_error::invalid_parameter("COPY option delimiter must be a single character")
        })?;
        if delimiters.next().is_some() {
            return Err(paro_error::invalid_parameter(
                "COPY option delimiter must be a single character",
            ));
        }

        let field_encoding = match options.format {
            CopyFormat::Csv => {
                let quote = options.quote().expect("CSV always has an effective quote");
                CopyFieldEncoding::Csv {
                    quote,
                    escape: options.escape().unwrap_or(quote),
                }
            }
            CopyFormat::Text => CopyFieldEncoding::Text,
            CopyFormat::Binary => {
                return Err(paro_error::not_implemented(
                    "COPY TO STDOUT BINARY is not supported yet",
                ))
            }
            CopyFormat::Ndjson => {
                return Err(paro_error::not_implemented(
                    "COPY TO STDOUT NDJSON is not supported yet",
                ))
            }
        };

        Ok(Self {
            field_encoding,
            delimiter,
            null_bytes: options
                .null_string()
                .expect("COPY OUT only supports CSV/TEXT formats")
                .as_bytes()
                .into(),
            force_quote_columns: Box::new([]),
        })
    }

    fn set_force_quote_columns(&mut self, columns: Vec<bool>) {
        self.force_quote_columns = columns.into_boxed_slice();
    }

    #[inline]
    fn append_char(out: &mut BytesMut, value: char) {
        let mut encoded = [0_u8; 4];
        out.extend_from_slice(value.encode_utf8(&mut encoded).as_bytes());
    }

    #[inline]
    fn append_delimiter(&self, out: &mut BytesMut) {
        Self::append_char(out, self.delimiter);
    }

    fn append_field(&self, out: &mut BytesMut, field: &[u8], column: usize) -> Result<()> {
        let field = std::str::from_utf8(field)
            .map_err(|_| paro_error::data_corrupted("COPY text output contains invalid UTF-8"))?;
        match self.field_encoding {
            CopyFieldEncoding::Csv { quote, escape } => {
                self.append_csv_field(
                    out,
                    field,
                    self.force_quote_columns
                        .get(column)
                        .copied()
                        .unwrap_or(false),
                    quote,
                    escape,
                );
            }
            CopyFieldEncoding::Text => self.append_text_field(out, field),
        }
        Ok(())
    }

    fn append_csv_field(
        &self,
        out: &mut BytesMut,
        field: &str,
        force_quote: bool,
        quote: char,
        escape: char,
    ) {
        let needs_quote = force_quote
            || field.contains('\n')
            || field.contains('\r')
            || field.contains(self.delimiter)
            || field.contains(quote);
        if !needs_quote {
            out.extend_from_slice(field.as_bytes());
            return;
        }

        Self::append_char(out, quote);
        for ch in field.chars() {
            if ch == quote || ch == escape {
                Self::append_char(out, escape);
            }
            Self::append_char(out, ch);
        }
        Self::append_char(out, quote);
    }

    fn append_text_field(&self, out: &mut BytesMut, field: &str) {
        for ch in field.chars() {
            match ch {
                '\\' => out.extend_from_slice(b"\\\\"),
                '\n' => out.extend_from_slice(b"\\n"),
                '\r' => out.extend_from_slice(b"\\r"),
                '\t' => out.extend_from_slice(b"\\t"),
                '\u{0008}' => out.extend_from_slice(b"\\b"),
                '\u{000c}' => out.extend_from_slice(b"\\f"),
                '\u{000b}' => out.extend_from_slice(b"\\v"),
                other if other == self.delimiter => {
                    out.extend_from_slice(b"\\");
                    Self::append_char(out, other);
                }
                other => Self::append_char(out, other),
            }
        }
    }

    fn append_header(&self, out: &mut BytesMut, names: &[String]) -> Result<()> {
        for (column, name) in names.iter().enumerate() {
            if column != 0 {
                self.append_delimiter(out);
            }
            match self.field_encoding {
                CopyFieldEncoding::Csv { quote, escape } => {
                    self.append_csv_field(out, name, false, quote, escape)
                }
                CopyFieldEncoding::Text => self.append_text_field(out, name),
            }
        }
        out.extend_from_slice(b"\n");
        Ok(())
    }
}

struct CopyColumnWriter<'a> {
    encoder: TextVectorEncoder<'a>,
    scratch: BytesMut,
}

struct CopyRowEncoder<'a> {
    columns: Vec<CopyColumnWriter<'a>>,
}

impl<'a> CopyRowEncoder<'a> {
    fn try_new(chunk: &'a Chunk, output_types: &[LogicalType]) -> Result<Self> {
        if chunk.column_count() != output_types.len() {
            return Err(paro_error::internal(format!(
                "COPY output column count changed from {} to {}",
                output_types.len(),
                chunk.column_count()
            )));
        }

        let columns = chunk
            .data
            .iter()
            .zip(output_types)
            .map(|(vector, expected_type)| {
                if vector.logical_type().physical_type() != expected_type.physical_type() {
                    return Err(paro_error::internal(format!(
                        "COPY output type changed from {expected_type} to {}",
                        vector.logical_type()
                    )));
                }
                Ok(CopyColumnWriter {
                    encoder: TextVectorEncoder::try_new(vector, chunk.size())?,
                    scratch: BytesMut::with_capacity(64),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { columns })
    }

    fn append_row(
        &mut self,
        out: &mut BytesMut,
        row: usize,
        encoding: &CopyRowEncoding,
    ) -> Result<()> {
        for (column_index, column) in self.columns.iter_mut().enumerate() {
            if column_index != 0 {
                encoding.append_delimiter(out);
            }
            if column.encoder.is_null(row) {
                out.extend_from_slice(&encoding.null_bytes);
                continue;
            }
            column
                .encoder
                .with_non_null_bytes(&mut column.scratch, row, |field| {
                    encoding.append_field(out, field, column_index)
                })?;
        }
        out.extend_from_slice(b"\n");
        Ok(())
    }
}

pub struct PgWireCopyOutSink<'a> {
    socket: &'a mut Framed<TcpStream, PgCodec>,
    cancellation: StatementCancellation,
    force_close_token: CancellationToken,
    flush_policy: CopyFlushPolicy,
    header: bool,
    format: CopyFormat,
    force_quote: ForceQuoteOption,
    row_encoding: CopyRowEncoding,
    names: Vec<String>,
    output_types: Vec<LogicalType>,
    copy_buffer: BytesMut,
}

impl<'a> PgWireCopyOutSink<'a> {
    pub fn new(
        socket: &'a mut Framed<TcpStream, PgCodec>,
        cancellation: StatementCancellation,
        force_close_token: CancellationToken,
        options: CopyOptions,
    ) -> Result<Self> {
        Self::with_flush_policy(
            socket,
            cancellation,
            force_close_token,
            options,
            CopyFlushPolicy::default(),
        )
    }

    fn with_flush_policy(
        socket: &'a mut Framed<TcpStream, PgCodec>,
        cancellation: StatementCancellation,
        force_close_token: CancellationToken,
        options: CopyOptions,
        flush_policy: CopyFlushPolicy,
    ) -> Result<Self> {
        let row_encoding = CopyRowEncoding::from_options(&options)?;

        Ok(Self {
            socket,
            cancellation,
            force_close_token,
            flush_policy,
            header: options.header(),
            format: options.format,
            force_quote: options.force_quote,
            row_encoding,
            names: Vec::new(),
            output_types: Vec::new(),
            copy_buffer: BytesMut::with_capacity(COPY_DATA_BUFFER_BYTES),
        })
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
    /// normal PostgreSQL cancel client resumes reading the original socket.
    /// Each observed write-buffer reduction renews that grace; only a full
    /// interval with zero progress abandons the connection. The queued frame
    /// remains byte-valid, but no terminal response can overtake it and a
    /// non-reading peer cannot observe that response anyway.
    async fn send_cancellable_frame(&mut self, message: PgWireBackendMessage) -> Result<()> {
        if self.force_close_token.is_cancelled() {
            return Err(self.abandon_connection("connection force-closed during COPY TO STDOUT"));
        }
        self.cancellation.check()?;

        // Every successful call flushes the codec completely; every failure
        // terminates this sink. Therefore `feed` never inherits a buffer above
        // Framed's backpressure boundary and cannot park in `poll_ready`.
        debug_assert!(self.socket.write_buffer().is_empty());
        if let Err(error) = self.socket.feed(message).await {
            self.force_close_token.cancel();
            return Err(paro_error::connection_failure(error.to_string()));
        }

        enum FlushOutcome {
            Flushed(std::io::Result<()>),
            ForceClosed,
            StatementCancelled,
        }

        let outcome = tokio::select! {
            biased;
            _ = self.force_close_token.cancelled() => FlushOutcome::ForceClosed,
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
            FlushOutcome::StatementCancelled => self.flush_cancelled_frame().await,
        }
    }

    async fn flush_cancelled_frame(&mut self) -> Result<()> {
        let mut remaining = self.socket.write_buffer().len();
        loop {
            match tokio::time::timeout(self.flush_policy.stalled_write_timeout, self.socket.flush())
                .await
            {
                Ok(Ok(())) => return self.cancellation.check(),
                Ok(Err(error)) => {
                    self.force_close_token.cancel();
                    return Err(paro_error::connection_failure(error.to_string()));
                }
                Err(_) => {
                    let current = self.socket.write_buffer().len();
                    if current < remaining {
                        remaining = current;
                        continue;
                    }
                    return Err(self
                        .abandon_connection("statement cancelled while flushing COPY TO STDOUT"));
                }
            }
        }
    }

    async fn flush_copy_buffer(&mut self) -> Result<()> {
        if self.copy_buffer.is_empty() {
            return Ok(());
        }
        let payload = std::mem::take(&mut self.copy_buffer).freeze();
        let reclaim = payload.clone();
        self.send_cancellable_frame(PgWireBackendMessage::CopyData(CopyData::new(payload)))
            .await?;

        // PgCodec copies the payload into its write buffer during `feed` and
        // retains no source reference. Recover the allocation after the frame
        // flush instead of allocating another 128 KiB staging buffer.
        self.copy_buffer = match reclaim.try_into_mut() {
            Ok(mut buffer) => {
                buffer.clear();
                buffer
            }
            Err(_) => BytesMut::with_capacity(COPY_DATA_BUFFER_BYTES),
        };
        if self.copy_buffer.capacity() < COPY_DATA_BUFFER_BYTES {
            self.copy_buffer
                .reserve(COPY_DATA_BUFFER_BYTES - self.copy_buffer.capacity());
        }
        Ok(())
    }
}

fn normalize_copy_header_name(name: &str) -> String {
    name.rsplit('.').next().unwrap_or(name).to_string()
}

#[async_trait]
impl<'a> CopyProtocolSink for PgWireCopyOutSink<'a> {
    async fn start_copy_out(&mut self, names: &[String], types: &[LogicalType]) -> Result<()> {
        if names.len() != types.len() {
            return Err(paro_error::internal(format!(
                "COPY output has {} names for {} columns",
                names.len(),
                types.len()
            )));
        }
        let normalized_names = names
            .iter()
            .map(|name| normalize_copy_header_name(name))
            .collect::<Vec<_>>();
        self.row_encoding
            .set_force_quote_columns(resolve_force_quote_columns(
                &self.force_quote,
                &normalized_names,
                self.format,
            )?);
        self.names = normalized_names;
        self.output_types = types.to_vec();

        self.send_cancellable_frame(PgWireBackendMessage::CopyOutResponse(CopyOutResponse::new(
            COPY_TEXT_FORMAT_CODE,
            self.output_types.len() as i16,
            vec![0; self.output_types.len()],
        )))
        .await?;

        if self.header {
            self.row_encoding
                .append_header(&mut self.copy_buffer, &self.names)?;
        }

        Ok(())
    }

    async fn push_copy_rows(&mut self, chunk: &Chunk) -> Result<()> {
        let mut row = 0;
        while row < chunk.size() {
            // Decoded vector views contain raw pointers and deliberately do
            // not cross an await. Rebuild them only when a frame boundary
            // splits this input chunk.
            {
                let mut encoder = CopyRowEncoder::try_new(chunk, &self.output_types)?;
                while row < chunk.size() && self.copy_buffer.len() < COPY_DATA_TARGET_BYTES {
                    encoder.append_row(&mut self.copy_buffer, row, &self.row_encoding)?;
                    row += 1;
                }
            }
            if self.copy_buffer.len() >= COPY_DATA_TARGET_BYTES {
                self.flush_copy_buffer().await?;
            }
        }
        Ok(())
    }

    async fn finish_copy_out(&mut self) -> Result<()> {
        self.flush_copy_buffer().await?;
        self.send_cancellable_frame(PgWireBackendMessage::CopyDone(CopyDone::new()))
            .await
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
    force_close_token: CancellationToken,
    options: &CopyOptions,
) -> Result<Box<dyn CopyProtocolSink + 'a>> {
    Ok(Box::new(PgWireCopyOutSink::new(
        socket,
        cancellation.clone(),
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
        let force_close_token = CancellationToken::new();
        let mut sink = PgWireCopyOutSink::new(
            &mut server,
            cancellation,
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

    #[tokio::test]
    async fn stalled_cancelled_flush_escalates_to_connection_close() {
        let (mut server, _client) = connected_pg_streams().await;
        let statement_token = CancellationToken::new();
        let cancellation = StatementCancellation::new(statement_token.clone(), None);
        let force_close_token = CancellationToken::new();
        let mut sink = PgWireCopyOutSink::with_flush_policy(
            &mut server,
            cancellation,
            force_close_token.clone(),
            CopyOptions::default(),
            CopyFlushPolicy {
                stalled_write_timeout: Duration::from_millis(10),
            },
        )
        .unwrap();
        let cancel_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            statement_token.cancel();
        });

        let error = tokio::time::timeout(
            Duration::from_secs(2),
            sink.send_cancellable_frame(PgWireBackendMessage::CopyData(CopyData::new(
                Bytes::from(vec![b'x'; 16 * 1024 * 1024]),
            ))),
        )
        .await
        .expect("a cancelled non-reading peer must not pin the COPY task")
        .expect_err("a stalled cancelled flush must abandon its connection");
        cancel_task.await.unwrap();
        assert!(error.is_connection_error());
        assert!(
            force_close_token.is_cancelled(),
            "abandoning COPY must suppress the outer protocol epilogue"
        );
    }

    #[tokio::test]
    async fn successful_copy_frame_reclaims_its_staging_allocation() {
        let (mut server, _client) = connected_pg_streams().await;
        let cancellation = StatementCancellation::new(CancellationToken::new(), None);
        let mut sink = PgWireCopyOutSink::new(
            &mut server,
            cancellation,
            CancellationToken::new(),
            CopyOptions::default(),
        )
        .unwrap();
        sink.copy_buffer
            .extend_from_slice(&vec![b'x'; COPY_DATA_TARGET_BYTES]);
        let allocation = sink.copy_buffer.as_ptr();

        sink.flush_copy_buffer().await.unwrap();
        assert_eq!(sink.copy_buffer.as_ptr(), allocation);
        assert!(sink.copy_buffer.is_empty());
        assert!(sink.copy_buffer.capacity() >= COPY_DATA_BUFFER_BYTES);
    }
}
