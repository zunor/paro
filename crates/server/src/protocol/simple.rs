//! Unified simple-query protocol sink with optional COPY adapters.

use async_trait::async_trait;
use futures::SinkExt;
use paro_common::chunk::Chunk;
use paro_common::error::{ParoError, Result};
use paro_common::types::LogicalType;
use paro_function::copy::CopyOptions;
use paro_session::{
    CopyProtocolSink, CopyProtocolSource, ProtocolResultSink, ResultSink, StatementCompletion,
};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::client_connection::PgCodec;

use super::copy::{create_copy_in_source, create_copy_out_sink};
use super::result::{build_error_response, PgWireResultSink};

pub struct ProtocolSink<'a> {
    result_sink: PgWireResultSink<'a>,
    error_was_sent: bool,
    transport_failure: Option<ParoError>,
}

impl<'a> ProtocolSink<'a> {
    pub fn new(socket: &'a mut Framed<TcpStream, PgCodec>) -> Self {
        Self {
            result_sink: PgWireResultSink::new(socket),
            error_was_sent: false,
            transport_failure: None,
        }
    }

    pub fn error_was_sent(&self) -> bool {
        self.error_was_sent
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

    async fn error(&mut self, err: &ParoError) -> Result<()> {
        self.ensure_transport_available()?;
        let result = self
            .result_sink
            .socket_mut()
            .send(pgwire::messages::PgWireBackendMessage::ErrorResponse(
                build_error_response(err),
            ))
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()));
        self.remember_transport_failure(result)?;
        self.error_was_sent = true;
        Ok(())
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
        create_copy_in_source(self.result_sink.socket_mut())
    }
}
