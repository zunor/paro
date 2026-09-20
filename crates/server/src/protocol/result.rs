// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Result sinks and shared row/error encoding helpers for PostgreSQL wire messages.

use async_trait::async_trait;
use futures::SinkExt;
use paro_common::chunk::Chunk;
use paro_common::error::{ParoError, Result};
use paro_common::types::LogicalType;
use paro_execution::query_executor::compiled::ResultColumnDesc;
use paro_session::{FormatCode, ProtocolResultSink, ResultSink, StatementCompletion};
use pgwire::messages::data::{FieldDescription, RowDescription};
use pgwire::messages::response::CommandComplete;
use pgwire::messages::PgWireBackendMessage;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::connection::PgCodec;

use super::data_row::{encode_chunk_rows, encode_text_chunk_rows};

const FORMAT_CODE_TEXT: i16 = 0;
const NO_TABLE_ID: i32 = 0;
const NO_COLUMN_ID: i16 = 0;
/// Bound buffered extended-query output without forcing a syscall per row or
/// per operator chunk. Reaching the threshold is also a cooperative yield
/// point for large result streams.
const RESULT_STREAM_FLUSH_BYTES: usize = 128 * 1024;

#[inline]
fn should_flush_result_buffer(buffered_bytes: usize) -> bool {
    buffered_bytes >= RESULT_STREAM_FLUSH_BYTES
}

pub struct PgWireResultSink<'a> {
    socket: &'a mut Framed<TcpStream, PgCodec>,
    col_count: usize,
}

impl<'a> PgWireResultSink<'a> {
    pub fn new(socket: &'a mut Framed<TcpStream, PgCodec>) -> Self {
        Self {
            socket,
            col_count: 0,
        }
    }

    pub(crate) fn socket_mut(&mut self) -> &mut Framed<TcpStream, PgCodec> {
        self.socket
    }
}

#[async_trait]
impl<'a> ResultSink for PgWireResultSink<'a> {
    async fn start_result(&mut self, names: &[String], types: &[LogicalType]) -> Result<()> {
        self.col_count = names.len();

        let fields = names
            .iter()
            .zip(types)
            .map(|(name, logical_type)| field_description(name.clone(), logical_type))
            .collect::<Vec<_>>();

        let result = self
            .socket
            .send(PgWireBackendMessage::RowDescription(RowDescription::new(
                fields,
            )))
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()));
        observe_pending_output(self.socket);
        result?;

        Ok(())
    }

    async fn push_chunk(&mut self, chunk: &Chunk) -> Result<()> {
        send_text_chunk_rows(self.socket, chunk, self.col_count).await
    }

    async fn push_diagnostic_chunk(
        &mut self,
        chunk: &Chunk,
        owner: std::sync::Arc<dyn paro_common::vector::VectorLifetimeOwner>,
    ) -> Result<()> {
        // Register the owner before flushing: once these bytes enter Framed's
        // write buffer, the request future is no longer their lifetime owner.
        let bytes = append_text_chunk_rows(self.socket, chunk, self.col_count)?;
        self.socket
            .codec_mut()
            .retain_pending_output_owner(bytes, owner);
        let result = self
            .socket
            .flush()
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()));
        observe_pending_output(self.socket);
        result
    }

    async fn finish_result(&mut self, completion: &StatementCompletion) -> Result<()> {
        let result = self
            .socket
            .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                completion.to_command_complete(),
            )))
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()));
        observe_pending_output(self.socket);
        result?;

        self.col_count = 0;
        Ok(())
    }

    async fn error(&mut self, err: &ParoError) -> Result<()> {
        let result = self
            .socket
            .send(PgWireBackendMessage::ErrorResponse(build_error_response(
                err,
            )))
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()));
        observe_pending_output(self.socket);
        result?;
        Ok(())
    }
}

impl<'a> ProtocolResultSink for PgWireResultSink<'a> {}

pub(crate) fn field_description(name: String, logical_type: &LogicalType) -> FieldDescription {
    field_description_with_format(name, logical_type, FORMAT_CODE_TEXT)
}

pub(crate) fn field_description_with_format(
    name: String,
    logical_type: &LogicalType,
    format_code: i16,
) -> FieldDescription {
    let descriptor = logical_type.pg_descriptor();
    FieldDescription::new(
        name,
        NO_TABLE_ID,
        NO_COLUMN_ID,
        descriptor.oid,
        descriptor.type_size,
        descriptor.type_modifier,
        format_code,
    )
}

pub(crate) async fn send_text_chunk_rows(
    socket: &mut Framed<TcpStream, PgCodec>,
    chunk: &Chunk,
    col_count: usize,
) -> Result<()> {
    append_text_chunk_rows(socket, chunk, col_count)?;
    flush_result_buffer_if_needed(socket).await?;
    Ok(())
}

pub(crate) async fn send_chunk_rows(
    socket: &mut Framed<TcpStream, PgCodec>,
    chunk: &Chunk,
    schema: &[ResultColumnDesc],
    format_codes: &[FormatCode],
) -> Result<()> {
    let encoded = encode_chunk_rows(chunk, schema, format_codes)?.into_inner();
    socket.write_buffer_mut().unsplit(encoded);
    flush_result_buffer_if_needed(socket).await?;
    Ok(())
}

fn append_text_chunk_rows(
    socket: &mut Framed<TcpStream, PgCodec>,
    chunk: &Chunk,
    col_count: usize,
) -> Result<usize> {
    let encoded = encode_text_chunk_rows(chunk, col_count)?.into_inner();
    let bytes = encoded.len();
    socket.write_buffer_mut().unsplit(encoded);
    Ok(bytes)
}

async fn flush_result_buffer_if_needed(socket: &mut Framed<TcpStream, PgCodec>) -> Result<()> {
    if should_flush_result_buffer(socket.write_buffer().len()) {
        let result = socket
            .flush()
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()));
        observe_pending_output(socket);
        result?;
    }
    Ok(())
}

pub(crate) fn observe_pending_output(socket: &mut Framed<TcpStream, PgCodec>) {
    let buffered_bytes = socket.write_buffer().len();
    socket.codec_mut().observe_pending_output(buffered_bytes);
}

pub(crate) fn build_error_response(err: &ParoError) -> pgwire::messages::response::ErrorResponse {
    let data = err.data();
    build_error_response_fields(ErrorResponseFields {
        severity: data.severity.as_str(),
        sqlstate: data.sqlstate.as_str(),
        message: data.message.as_ref(),
        detail: data.detail.as_deref(),
        hint: data.hint.as_deref(),
        position: data.position,
        schema_name: data.schema_name.as_deref(),
        table_name: data.table_name.as_deref(),
        column_name: data.column_name.as_deref(),
        datatype_name: data.datatype_name.as_deref(),
        constraint_name: data.constraint_name.as_deref(),
    })
}

pub(crate) fn build_error_response_message(
    severity: &str,
    sqlstate: &str,
    message: &str,
) -> pgwire::messages::response::ErrorResponse {
    build_error_response_fields(ErrorResponseFields {
        severity,
        sqlstate,
        message,
        detail: None,
        hint: None,
        position: None,
        schema_name: None,
        table_name: None,
        column_name: None,
        datatype_name: None,
        constraint_name: None,
    })
}

struct ErrorResponseFields<'a> {
    severity: &'a str,
    sqlstate: &'a str,
    message: &'a str,
    detail: Option<&'a str>,
    hint: Option<&'a str>,
    position: Option<u32>,
    schema_name: Option<&'a str>,
    table_name: Option<&'a str>,
    column_name: Option<&'a str>,
    datatype_name: Option<&'a str>,
    constraint_name: Option<&'a str>,
}

fn build_error_response_fields(
    fields_in: ErrorResponseFields<'_>,
) -> pgwire::messages::response::ErrorResponse {
    let mut fields: Vec<(u8, String)> = Vec::with_capacity(16);
    fields.push((b'S', fields_in.severity.to_string()));
    fields.push((b'C', fields_in.sqlstate.to_string()));
    fields.push((b'M', fields_in.message.to_string()));

    if let Some(detail) = fields_in.detail {
        fields.push((b'D', detail.to_string()));
    }
    if let Some(hint) = fields_in.hint {
        fields.push((b'H', hint.to_string()));
    }
    if let Some(position) = fields_in.position {
        fields.push((b'P', position.to_string()));
    }
    if let Some(schema) = fields_in.schema_name {
        fields.push((b's', schema.to_string()));
    }
    if let Some(table) = fields_in.table_name {
        fields.push((b't', table.to_string()));
    }
    if let Some(column) = fields_in.column_name {
        fields.push((b'c', column.to_string()));
    }
    if let Some(datatype) = fields_in.datatype_name {
        fields.push((b'd', datatype.to_string()));
    }
    if let Some(constraint) = fields_in.constraint_name {
        fields.push((b'n', constraint.to_string()));
    }

    pgwire::messages::response::ErrorResponse::new(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::value_format::value_to_pg_text;
    use paro_common::runtime_value::Value;
    use paro_common::types::pg_oid::{INT2OID, INT4OID, NUMERICOID};
    use paro_common::vector::Vector;
    use paro_context::compile_diagnostics::{CompileCapture, MAX_CAPTURES};
    use paro_session::FormatCode;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::time::{timeout, Duration};

    async fn connected_socket() -> (Framed<TcpStream, PgCodec>, TcpStream) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_socket = tokio::net::TcpSocket::new_v4().unwrap();
        peer_socket.set_recv_buffer_size(4096).unwrap();
        let peer = peer_socket
            .connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let codec = PgCodec::new(
            crate::connection::PgFrontendMessageLimits::new(1 << 20),
            Arc::new(paro_instance::CopyStdinMetrics::default()),
        );
        (Framed::new(socket, codec), peer)
    }

    async fn fill_kernel_send_buffer(socket: &Framed<TcpStream, PgCodec>) {
        let block = [b'x'; 64 * 1024];
        let mut wrote = 0usize;
        let mut blocked = false;
        for _ in 0..20 {
            loop {
                match socket.get_ref().try_write(&block) {
                    Ok(n) => wrote += n,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        blocked = true;
                        break;
                    }
                    Err(error) => panic!("unexpected fill error after {wrote} bytes: {error}"),
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            blocked && wrote > 0,
            "the test must create real TCP backpressure"
        );
    }

    fn diagnostic_chunk(session: &paro_session::Session) -> Chunk {
        let allocator = session.buffer_allocator();
        let vector = Vector::try_from_strings(&[&"d".repeat(100_000)], allocator.clone()).unwrap();
        Chunk::from_vectors(vec![vector], allocator)
    }

    #[test]
    fn field_descriptions_use_pg_descriptor_metadata() {
        let int_field = field_description("i".to_string(), &LogicalType::TinyInt);
        assert_eq!(int_field.type_id, INT2OID);
        assert_eq!(int_field.type_size, 2);
        assert_eq!(int_field.type_modifier, -1);

        let numeric_field = field_description("n".to_string(), &LogicalType::HugeInt);
        assert_eq!(numeric_field.type_id, NUMERICOID);
        assert_eq!(numeric_field.type_size, -1);

        let literal_field = field_description("lit".to_string(), &LogicalType::IntegerLiteral(1));
        assert_eq!(literal_field.type_id, INT4OID);
        assert_eq!(literal_field.type_size, 4);
    }

    #[test]
    fn value_to_pg_text_formats_large_integers_as_decimal() {
        assert_eq!(
            value_to_pg_text(&Value::HugeInt(i128::MAX)),
            i128::MAX.to_string()
        );
        assert_eq!(
            value_to_pg_text(&Value::UBigInt(u64::MAX)),
            u64::MAX.to_string()
        );
        assert_eq!(
            value_to_pg_text(&Value::UHugeInt(u128::MAX)),
            u128::MAX.to_string()
        );
    }

    #[test]
    fn row_descriptions_preserve_requested_format_codes() {
        let field = field_description_with_format("v".to_string(), &LogicalType::Integer, 1);
        assert_eq!(field.format_code, 1);
        assert_eq!(
            field_description_with_format("v".to_string(), &LogicalType::Integer, 0).format_code,
            match FormatCode::Text {
                FormatCode::Text => 0,
                FormatCode::Binary => 1,
            }
        );
    }

    #[test]
    fn result_stream_buffer_policy_is_bounded() {
        assert!(!should_flush_result_buffer(RESULT_STREAM_FLUSH_BYTES - 1));
        assert!(should_flush_result_buffer(RESULT_STREAM_FLUSH_BYTES));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn diagnostic_lease_follows_pending_framed_bytes_until_drain() {
        let (mut socket, mut peer) = connected_socket().await;
        let session = paro_session::Session::new(94, paro_instance::Instance::new_in_memory());
        let chunk = diagnostic_chunk(&session);
        let capture = CompileCapture::try_start().unwrap().seal();
        let weak = Arc::downgrade(&capture);
        let pending = {
            let mut sink = PgWireResultSink::new(&mut socket);
            sink.start_result(&["QUERY PLAN".into()], &[LogicalType::Varchar])
                .await
                .unwrap();
            fill_kernel_send_buffer(sink.socket_mut()).await;
            timeout(
                Duration::from_millis(30),
                sink.push_diagnostic_chunk(&chunk, capture.clone()),
            )
            .await
        };
        assert!(
            pending.is_err(),
            "push must observe real transport backpressure"
        );
        drop(capture);
        assert!(weak.upgrade().is_some());
        assert!(!socket.write_buffer().is_empty());
        assert!(socket.codec().pending_output_bytes() > 0);

        let reader = tokio::spawn(async move {
            let mut buffer = [0_u8; 64 * 1024];
            loop {
                let read = peer.read(&mut buffer).await.unwrap();
                if read == 0 {
                    break;
                }
            }
        });
        socket.flush().await.unwrap();
        observe_pending_output(&mut socket);
        assert_eq!(socket.codec().pending_output_bytes(), 0);
        assert!(weak.upgrade().is_none());
        drop(socket);
        let _ = reader.await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn diagnostic_leases_are_scoped_per_connection_and_drop_with_buffers() {
        let (mut socket_a, _peer_a) = connected_socket().await;
        let (mut socket_b, _peer_b) = connected_socket().await;
        let session_a = paro_session::Session::new(95, paro_instance::Instance::new_in_memory());
        let session_b = paro_session::Session::new(96, paro_instance::Instance::new_in_memory());
        let chunk_a = diagnostic_chunk(&session_a);
        let chunk_b = diagnostic_chunk(&session_b);
        let capture_a = CompileCapture::try_start().unwrap().seal();
        let capture_b = CompileCapture::try_start().unwrap().seal();
        let weak_a = Arc::downgrade(&capture_a);
        let weak_b = Arc::downgrade(&capture_b);
        {
            let mut sink_a = PgWireResultSink::new(&mut socket_a);
            sink_a
                .start_result(&["QUERY PLAN".into()], &[LogicalType::Varchar])
                .await
                .unwrap();
            fill_kernel_send_buffer(sink_a.socket_mut()).await;
            assert!(timeout(
                Duration::from_millis(30),
                sink_a.push_diagnostic_chunk(&chunk_a, capture_a.clone()),
            )
            .await
            .is_err());
        }
        {
            let mut sink_b = PgWireResultSink::new(&mut socket_b);
            sink_b
                .start_result(&["QUERY PLAN".into()], &[LogicalType::Varchar])
                .await
                .unwrap();
            fill_kernel_send_buffer(sink_b.socket_mut()).await;
            assert!(timeout(
                Duration::from_millis(30),
                sink_b.push_diagnostic_chunk(&chunk_b, capture_b.clone()),
            )
            .await
            .is_err());
        }
        drop(capture_a);
        drop(capture_b);
        assert!(weak_a.upgrade().is_some());
        assert!(weak_b.upgrade().is_some());

        let mut additional = Vec::new();
        while let Some(capture) = CompileCapture::try_start() {
            additional.push(capture);
        }
        assert_eq!(additional.len(), MAX_CAPTURES - 2);
        drop(additional);
        drop(socket_a);
        assert!(weak_a.upgrade().is_none());
        let mut one_more = Vec::new();
        while let Some(capture) = CompileCapture::try_start() {
            one_more.push(capture);
        }
        assert_eq!(one_more.len(), MAX_CAPTURES - 1);
        drop(one_more);
        drop(socket_b);
        assert!(weak_b.upgrade().is_none());
        let all_available = (0..MAX_CAPTURES)
            .map(|_| CompileCapture::try_start().expect("all leases released"))
            .collect::<Vec<_>>();
        drop(all_available);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn diagnostic_lease_releases_when_failed_buffer_is_dropped() {
        let (mut socket, peer) = connected_socket().await;
        let session = paro_session::Session::new(97, paro_instance::Instance::new_in_memory());
        let chunk = diagnostic_chunk(&session);
        let capture = CompileCapture::try_start().unwrap().seal();
        let weak = Arc::downgrade(&capture);
        let error = {
            let mut sink = PgWireResultSink::new(&mut socket);
            sink.start_result(&["QUERY PLAN".into()], &[LogicalType::Varchar])
                .await
                .unwrap();
            sink.socket_mut().get_mut().shutdown().await.unwrap();
            sink.push_diagnostic_chunk(&chunk, capture.clone())
                .await
                .unwrap_err()
        };
        assert!(!error.to_string().is_empty());
        drop(capture);
        assert!(weak.upgrade().is_some());
        drop(socket);
        assert!(weak.upgrade().is_none());
        drop(peer);
    }
}
