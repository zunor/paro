// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Unified simple-query protocol sink with optional COPY adapters.

use async_trait::async_trait;
use paro_common::chunk::Chunk;
use paro_common::error::{ParoError, Result};
use paro_common::types::LogicalType;
use paro_function::copy::CopyOptions;
use paro_session::{
    CopyProtocolSink, CopyProtocolSource, ProtocolResultSink, ResultSink, SessionExecutionControl,
    StatementCompletion,
};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use crate::connection::PgCodec;

use super::copy::{create_copy_in_source, create_copy_out_sink, CopyFrontendMode};
use super::result::PgWireResultSink;

pub struct ProtocolSink<'a> {
    result_sink: PgWireResultSink<'a>,
    execution_control: Arc<SessionExecutionControl>,
    drain_token: CancellationToken,
    force_close_token: CancellationToken,
    pending_frontend_messages: Arc<Mutex<VecDeque<pgwire::messages::PgWireFrontendMessage>>>,
    transport_failure: Option<ParoError>,
}

impl<'a> ProtocolSink<'a> {
    pub fn new(
        socket: &'a mut Framed<TcpStream, PgCodec>,
        execution_control: Arc<SessionExecutionControl>,
        drain_token: CancellationToken,
        force_close_token: CancellationToken,
        pending_frontend_messages: Arc<Mutex<VecDeque<pgwire::messages::PgWireFrontendMessage>>>,
    ) -> Self {
        Self {
            result_sink: PgWireResultSink::new(socket),
            execution_control,
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
        options: &CopyOptions,
    ) -> Result<Box<dyn CopyProtocolSink + '_>> {
        self.ensure_transport_available()?;
        create_copy_out_sink(self.result_sink.socket_mut(), options)
    }

    fn create_copy_in_source(&mut self) -> Result<Box<dyn CopyProtocolSource + '_>> {
        self.ensure_transport_available()?;
        create_copy_in_source(
            self.result_sink.socket_mut(),
            Arc::clone(&self.execution_control),
            self.drain_token.clone(),
            self.force_close_token.clone(),
            Arc::clone(&self.pending_frontend_messages),
            CopyFrontendMode::SimpleQuery,
        )
    }
}
