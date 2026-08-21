// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Extended-query protocol responder implementations.

use async_trait::async_trait;
use futures::SinkExt;
use paro_common::chunk::Chunk;
use paro_common::error::{ParoError, Result};
use paro_common::types::LogicalType;
use paro_execution::query_executor::compiled::ResultColumnDesc;
use paro_function::copy::CopyOptions;
use paro_session::{
    CopyProtocolSink, CopyProtocolSource, ExtendedQueryResponder, FormatCode,
    StatementCancellation, StatementCompletion,
};
use pgwire::messages::data::{NoData, ParameterDescription, RowDescription};
use pgwire::messages::extendedquery::{
    BindComplete, CloseComplete, ParseComplete, PortalSuspended,
};
use pgwire::messages::response::EmptyQueryResponse;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use crate::connection::PgCodec;

use super::copy::{create_copy_in_source, create_copy_out_sink, CopyFrontendMode};
use super::result::{build_error_response, field_description_with_format, send_chunk_rows};

pub struct PgWireExtendedQueryResponder<'a> {
    socket: &'a mut Framed<TcpStream, PgCodec>,
    drain_token: CancellationToken,
    force_close_token: CancellationToken,
    pending_frontend_messages: Arc<Mutex<VecDeque<PgWireFrontendMessage>>>,
}

impl<'a> PgWireExtendedQueryResponder<'a> {
    pub fn new(
        socket: &'a mut Framed<TcpStream, PgCodec>,
        drain_token: CancellationToken,
        force_close_token: CancellationToken,
        pending_frontend_messages: Arc<Mutex<VecDeque<PgWireFrontendMessage>>>,
    ) -> Self {
        Self {
            socket,
            drain_token,
            force_close_token,
            pending_frontend_messages,
        }
    }
}

#[async_trait]
impl ExtendedQueryResponder for PgWireExtendedQueryResponder<'_> {
    async fn send_parse_complete(&mut self) -> Result<()> {
        self.socket
            .send(PgWireBackendMessage::ParseComplete(ParseComplete::new()))
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()))?;
        Ok(())
    }

    async fn send_bind_complete(&mut self) -> Result<()> {
        self.socket
            .send(PgWireBackendMessage::BindComplete(BindComplete::new()))
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()))?;
        Ok(())
    }

    async fn send_parameter_description(
        &mut self,
        parameter_types: &[Option<LogicalType>],
    ) -> Result<()> {
        let type_oids = parameter_types
            .iter()
            .map(|ty| ty.as_ref().map(|ty| ty.pg_descriptor().oid).unwrap_or(0))
            .collect::<Vec<_>>();
        self.socket
            .send(PgWireBackendMessage::ParameterDescription(
                ParameterDescription::new(type_oids),
            ))
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()))?;
        Ok(())
    }

    async fn send_row_description(
        &mut self,
        schema: &[ResultColumnDesc],
        format_codes: &[FormatCode],
    ) -> Result<()> {
        let fields = schema
            .iter()
            .enumerate()
            .map(|(idx, column)| {
                let format_code = match format_codes.get(idx).unwrap_or(&FormatCode::Text) {
                    FormatCode::Text => 0,
                    FormatCode::Binary => 1,
                };
                field_description_with_format(
                    column.name.clone(),
                    &column.logical_type,
                    format_code,
                )
            })
            .collect::<Vec<_>>();

        self.socket
            .send(PgWireBackendMessage::RowDescription(RowDescription::new(
                fields,
            )))
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()))?;
        Ok(())
    }

    async fn send_data_chunk(
        &mut self,
        chunk: &Chunk,
        schema: &[ResultColumnDesc],
        format_codes: &[FormatCode],
    ) -> Result<()> {
        send_chunk_rows(self.socket, chunk, schema, format_codes).await
    }

    async fn send_command_complete(&mut self, completion: &StatementCompletion) -> Result<()> {
        self.socket
            .send(PgWireBackendMessage::CommandComplete(
                pgwire::messages::response::CommandComplete::new(completion.to_command_complete()),
            ))
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()))?;
        Ok(())
    }

    async fn send_close_complete(&mut self) -> Result<()> {
        self.socket
            .send(PgWireBackendMessage::CloseComplete(CloseComplete::new()))
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()))?;
        Ok(())
    }

    async fn send_no_data(&mut self) -> Result<()> {
        self.socket
            .send(PgWireBackendMessage::NoData(NoData::new()))
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()))?;
        Ok(())
    }

    async fn send_empty_query_response(&mut self) -> Result<()> {
        self.socket
            .send(PgWireBackendMessage::EmptyQueryResponse(
                EmptyQueryResponse::new(),
            ))
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()))?;
        Ok(())
    }

    async fn send_portal_suspended(&mut self) -> Result<()> {
        self.socket
            .send(PgWireBackendMessage::PortalSuspended(PortalSuspended::new()))
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()))?;
        Ok(())
    }

    async fn send_error(&mut self, err: &ParoError) -> Result<()> {
        self.socket
            .send(PgWireBackendMessage::ErrorResponse(build_error_response(
                err,
            )))
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()))?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.socket
            .flush()
            .await
            .map_err(|e| paro_common::error::internal(e.to_string()))
    }

    fn create_copy_out_sink(
        &mut self,
        cancellation: &StatementCancellation,
        options: &CopyOptions,
    ) -> Result<Box<dyn CopyProtocolSink + '_>> {
        create_copy_out_sink(
            self.socket,
            cancellation,
            self.drain_token.clone(),
            self.force_close_token.clone(),
            options,
        )
    }

    fn create_copy_in_source(
        &mut self,
        cancellation: &StatementCancellation,
    ) -> Result<Box<dyn CopyProtocolSource + '_>> {
        create_copy_in_source(
            self.socket,
            cancellation,
            self.drain_token.clone(),
            self.force_close_token.clone(),
            Arc::clone(&self.pending_frontend_messages),
            CopyFrontendMode::ExtendedQuery,
        )
    }
}
