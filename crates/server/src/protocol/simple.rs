// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Unified simple-query protocol sink with optional COPY adapters.

use async_trait::async_trait;
use paro_common::chunk::Chunk;
use paro_common::error::{ParoError, Result};
use paro_common::types::LogicalType;
use paro_function::copy::CopyOptions;
use paro_session::{
    CopyProtocolSink, CopyProtocolSource, ProtocolResultSink, ResultSink, StatementCancellation,
    StatementCompletion,
};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use crate::connection::PgCodec;

use super::copy::{CopyFrontendMode, create_copy_in_source, create_copy_out_sink};
use super::result::PgWireResultSink;

pub struct ProtocolSink<'a> {
    result_sink: PgWireResultSink<'a>,
    drain_token: CancellationToken,
    force_close_token: CancellationToken,
    pending_frontend_messages: Arc<Mutex<VecDeque<pgwire::messages::PgWireFrontendMessage>>>,
    transport_failure: Option<ParoError>,
}

impl<'a> ProtocolSink<'a> {
    pub fn new(
        socket: &'a mut Framed<TcpStream, PgCodec>,
        drain_token: CancellationToken,
        force_close_token: CancellationToken,
        pending_frontend_messages: Arc<Mutex<VecDeque<pgwire::messages::PgWireFrontendMessage>>>,
    ) -> Self {
        Self {
            result_sink: PgWireResultSink::new(socket),
            drain_token,
            force_close_token,
            pending_frontend_messages,
            transport_failure: None,
        }
    }

    pub fn transport_failure(&self) -> Option<ParoError> {
        self.transport_failure.clone()
    }

    fn ensure_transport_available(&self) -> Result<()> {
        match &self.transport_failure {
            Some(err) => Err(err.clone()),
            None => Ok(()),
        }
    }

    fn remember_transport_failure<T>(&mut self, result: Result<T>) -> Result<T> {
        result.inspect_err(|err| {
            if self.transport_failure.is_none() {
                self.transport_failure = Some(err.clone());
            }
        })
    }
}

#[async_trait]
impl ResultSink for ProtocolSink<'_> {
    async fn push_diagnostic_chunk(
        &mut self,
        chunk: &Chunk,
        owner: std::sync::Arc<dyn paro_common::vector::VectorLifetimeOwner>,
    ) -> Result<()> {
        self.ensure_transport_available()?;
        let result = self.result_sink.push_diagnostic_chunk(chunk, owner).await;
        self.remember_transport_failure(result)
    }
    async fn start_result(&mut self, names: &[String], types: &[LogicalType]) -> Result<()> {
        self.ensure_transport_available()?;
        let result = self.result_sink.start_result(names, types).await;
        self.remember_transport_failure(result)
    }

    async fn push_chunk(&mut self, chunk: &Chunk) -> Result<()> {
        self.ensure_transport_available()?;
        let result = self.result_sink.push_chunk(chunk).await;
        self.remember_transport_failure(result)
    }

    async fn finish_result(&mut self, completion: &StatementCompletion) -> Result<()> {
        self.ensure_transport_available()?;
        let result = self.result_sink.finish_result(completion).await;
        self.remember_transport_failure(result)
    }

    async fn error(&mut self, _err: &ParoError) -> Result<()> {
        // Simple-query terminal ErrorResponse/ReadyForQuery ownership lives in `connection.rs`
        // so it can make the final protocol-state decision in one place. This sink therefore
        // only reports transport availability and leaves user-visible error emission to the
        // outer connection loop.
        self.ensure_transport_available()
    }
}

impl ProtocolResultSink for ProtocolSink<'_> {
    fn create_copy_out_sink(
        &mut self,
        cancellation: &StatementCancellation,
        options: &CopyOptions,
    ) -> Result<Box<dyn CopyProtocolSink + '_>> {
        self.ensure_transport_available()?;
        create_copy_out_sink(
            self.result_sink.socket_mut(),
            cancellation,
            self.force_close_token.clone(),
            options,
        )
    }

    fn create_copy_in_source(
        &mut self,
        cancellation: &StatementCancellation,
    ) -> Result<Box<dyn CopyProtocolSource + '_>> {
        self.ensure_transport_available()?;
        create_copy_in_source(
            self.result_sink.socket_mut(),
            cancellation,
            self.drain_token.clone(),
            self.force_close_token.clone(),
            Arc::clone(&self.pending_frontend_messages),
            CopyFrontendMode::SimpleQuery,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn diagnostic_flush_failure_is_terminal_for_protocol_sink() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let codec = PgCodec::new(
            crate::connection::PgFrontendMessageLimits::new(1 << 20),
            Arc::new(paro_instance::CopyStdinMetrics::default()),
        );
        let mut socket = Framed::new(socket, codec);
        // A locally closed writer deterministically exercises the real framed
        // socket flush error, without depending on when a remote RST arrives.
        socket.get_mut().shutdown().await.unwrap();
        let mut sink = ProtocolSink::new(
            &mut socket,
            CancellationToken::new(),
            CancellationToken::new(),
            Arc::new(Mutex::new(VecDeque::new())),
        );
        let instance = paro_instance::Instance::new_in_memory();
        let session = paro_session::Session::new(99, instance);
        let allocator = session.buffer_allocator();
        let vector =
            paro_common::vector::Vector::try_from_strings(&["diagnostic"], allocator.clone())
                .unwrap();
        let chunk = Chunk::from_vectors(vec![vector], allocator);
        let owner = paro_context::compile_diagnostics::CompileCapture::try_start().unwrap();
        // Feed a frame before sending the diagnostic so even a zero-column
        // chunk cannot turn this into a no-op flush.
        sink.result_sink
            .socket_mut()
            .write_buffer_mut()
            .extend_from_slice(b"pending");
        let first = sink.push_diagnostic_chunk(&chunk, owner).await.unwrap_err();
        assert_eq!(
            sink.transport_failure().unwrap().to_string(),
            first.to_string()
        );
        assert_eq!(
            sink.error(&first).await.unwrap_err().to_string(),
            first.to_string()
        );
        assert_eq!(
            sink.finish_result(&StatementCompletion::Explain)
                .await
                .unwrap_err()
                .to_string(),
            first.to_string()
        );
        drop(peer);
    }
}
