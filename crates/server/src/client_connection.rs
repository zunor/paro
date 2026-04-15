// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Per-client PostgreSQL wire-protocol loop.

use paro_common::logging::targets;
use paro_instance::{Instance, ManagedConnection};
use std::sync::Arc;
use std::time::Instant;

use futures::{SinkExt, StreamExt};
use paro_common::runtime_value::Value;
use paro_session::{
    copy_stdin_metrics, BindMessage, CloseTarget, CopyStdinRejectReason, DescribeTarget,
    ExecutePortalMessage, ExtendedQueryMessage, ExtendedQueryResponder, ParseMessage, Session,
    TransactionState,
};
use pgwire::error::PgWireError;
use pgwire::messages::copy::{
    MESSAGE_TYPE_BYTE_COPY_DATA, MESSAGE_TYPE_BYTE_COPY_DONE, MESSAGE_TYPE_BYTE_COPY_FAIL,
};
use pgwire::messages::extendedquery::{MESSAGE_TYPE_BYTE_FLUSH, MESSAGE_TYPE_BYTE_SYNC};
use pgwire::messages::response::{ReadyForQuery, TransactionStatus};
use pgwire::messages::startup::{Authentication, ParameterStatus};
use pgwire::messages::terminate::MESSAGE_TYPE_BYTE_TERMINATE;
use pgwire::messages::{
    DecodeContext, PgWireBackendMessage, PgWireFrontendMessage, ProtocolVersion,
    SslNegotiationMetaMessage,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder, Framed};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::connection_control::{ServerConnectionControl, ServerLimits};
use crate::protocol::extended::PgWireExtendedQueryResponder;
use crate::protocol::result::build_error_response_message;
use crate::protocol::simple::ProtocolSink;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FrontendProtocolState {
    Ready,
    SkippingUntilSync,
}

#[derive(Debug)]
enum DispatchResult {
    Continue { send_ready_for_query: bool },
    Terminate,
}

const DEFAULT_NORMAL_FRONTEND_MESSAGE_LIMIT: usize = 64 * 1024 * 1024;
const DEFAULT_SMALL_FRONTEND_MESSAGE_LIMIT: usize = 10 * 1024;
const MAX_PG_FRONTEND_PACKET_BYTES: usize = 0x3fffffff - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgFrontendMessageLimits {
    startup_packet_bytes: usize,
    normal_message_bytes: usize,
    small_message_bytes: usize,
    copy_data_bytes: usize,
}

impl PgFrontendMessageLimits {
    pub fn new(copy_stdin_memory_limit: usize) -> Self {
        Self {
            startup_packet_bytes: DEFAULT_NORMAL_FRONTEND_MESSAGE_LIMIT,
            normal_message_bytes: DEFAULT_NORMAL_FRONTEND_MESSAGE_LIMIT,
            small_message_bytes: DEFAULT_SMALL_FRONTEND_MESSAGE_LIMIT,
            // Frontend message length includes the 4-byte length header.
            copy_data_bytes: copy_stdin_memory_limit
                .saturating_add(4)
                .min(MAX_PG_FRONTEND_PACKET_BYTES),
        }
    }
}

/// A client connection managing the protocol loop and session state machine.
pub(crate) struct Connection {
    /// Unique connection/session ID.
    id: u64,
    /// Peer address captured at accept time for stable connection logs.
    peer_addr: String,
    /// Framed socket with pgwire codec.
    socket: Framed<TcpStream, PgCodec>,
    /// Reference to the global database instance root.
    instance: Arc<Instance>,
    /// Shared lifecycle/control-plane state owned by the server.
    control: Arc<ServerConnectionControl>,
    /// Shared limit state needed during pre-auth handshake.
    limits: Arc<ServerLimits>,
    /// Memory cap for transitional COPY FROM STDIN buffering.
    copy_stdin_memory_limit: usize,
    /// Server-side token that asks the connection to drain and stop accepting new work.
    drain_token: CancellationToken,
    /// Server-side token that asks the connection to force-close.
    force_close_token: CancellationToken,
    /// The active session. Initialized after handshake.
    session: Option<Session>,
    /// Whether this connection has been mirrored into Instance::ConnectionManager.
    mirrored_in_connection_manager: bool,
    /// Protocol state for extended query error recovery.
    protocol_state: FrontendProtocolState,
    /// Whether we have an open extended-query pipeline waiting for `Sync`.
    extended_query_pipeline_open: bool,
}

pub(crate) struct ConnectionInit {
    pub(crate) id: u64,
    pub(crate) peer_addr: String,
    pub(crate) tcp: TcpStream,
    pub(crate) instance: Arc<Instance>,
    pub(crate) control: Arc<ServerConnectionControl>,
    pub(crate) limits: Arc<ServerLimits>,
    pub(crate) frontend_message_limits: PgFrontendMessageLimits,
    pub(crate) copy_stdin_memory_limit: usize,
    pub(crate) drain_token: CancellationToken,
    pub(crate) force_close_token: CancellationToken,
}

impl Connection {
    /// Create a new connection.
    pub(crate) fn new(init: ConnectionInit) -> Self {
        let ConnectionInit {
            id,
            peer_addr,
            tcp,
            instance,
            control,
            limits,
            frontend_message_limits,
            copy_stdin_memory_limit,
            drain_token,
            force_close_token,
        } = init;
        let _ = tcp.set_nodelay(true);
        Self {
            id,
            peer_addr,
            socket: Framed::new(tcp, PgCodec::new(frontend_message_limits)),
            instance,
            control,
            limits,
            copy_stdin_memory_limit,
            drain_token,
            force_close_token,
            session: None,
            mirrored_in_connection_manager: false,
            protocol_state: FrontendProtocolState::Ready,
            extended_query_pipeline_open: false,
        }
    }

    /// Run the connection loop.
    pub(crate) async fn run(mut self) {
        let connection_span = tracing::info_span!(
            "connection",
            session_id = self.id,
            peer_addr = %self.peer_addr
        );
        let start = Instant::now();

        tracing::info!(
            target: targets::CONNECTION,
            session_id = self.id,
            peer_addr = %self.peer_addr,
            "Connection started"
        );

        let result = self.run_inner().instrument(connection_span).await;
        self.cleanup();

        match result {
            Ok(()) => {
                tracing::info!(
                    target: targets::CONNECTION,
                    session_id = self.id,
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    "Connection closed"
                );
            }
            Err(error) => {
                tracing::warn!(
                    target: targets::CONNECTION,
                    session_id = self.id,
                    error = %error,
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    "Connection terminated with error"
                );
            }
        }
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        // 1. Handshake (Resolve Database & Auth)
        self.handshake().await?;
        if self.session.is_none() {
            return Ok(());
        }
        if let Some(session) = self.session.as_ref() {
            tracing::info!(
                target: targets::CONNECTION,
                database = %session.current_database.name(),
                "Connection ready"
            );
        }

        // 2. Main Protocol Loop
        loop {
            // Wait for next frontend message
            let msg = match self.recv_frontend_message().await? {
                Some(msg) => msg,
                None => return Ok(()),
            };

            // Dispatch message to handler
            self.control.set_in_flight(true);
            let dispatch_result = self.dispatch(msg).await;
            self.control.set_in_flight(false);
            match dispatch_result {
                Ok(DispatchResult::Continue {
                    send_ready_for_query,
                }) => {
                    if send_ready_for_query {
                        self.send_ready_for_query().await?;
                    }
                }
                Ok(DispatchResult::Terminate) => break,
                Err(error) => return Err(error),
            }

            if self.drain_token.is_cancelled() {
                tracing::info!(
                    target: targets::CONNECTION,
                    session_id = self.id,
                    "Connection drained after completing the current frontend work"
                );
                break;
            }
            if self.force_close_token.is_cancelled() {
                self.force_close_session();
                break;
            }
        }

        Ok(())
    }

    /// Handle PG handshake (SSL negotiation and Startup message).
    async fn handshake(&mut self) -> anyhow::Result<()> {
        let deadline = Instant::now() + self.limits.startup_timeout();

        loop {
            let Some(message) = self.recv_frontend_message_before(deadline).await? else {
                return Ok(());
            };

            match message {
                PgWireFrontendMessage::Startup(start) => {
                    if self.limits.post_startup_limit_reached() {
                        tracing::warn!(
                            target: targets::CONNECTION,
                            session_id = self.id,
                            tracked_connections = self.limits.tracked_connections(),
                            max_connections = self.limits.max_connections(),
                            "Startup rejected because max_connections was exceeded during handshake"
                        );
                        self.send_error_response("53300", "sorry, too many clients already")
                            .await?;
                        return Ok(());
                    }

                    let db_name = start
                        .parameters
                        .get("database")
                        .map(|s| s.as_str())
                        .unwrap_or("postgres");
                    let user_name = start
                        .parameters
                        .get("user")
                        .map(|s| s.as_str())
                        .unwrap_or("unknown");

                    tracing::debug!(
                        target: targets::CONNECTION,
                        user = %user_name,
                        database = %db_name,
                        "Startup message received"
                    );

                    // Resolve the database from the instance
                    match self.instance.database_registry().get_database(db_name) {
                        Some(db) => {
                            if !db.is_ready() {
                                let err_msg = format!(
                                    "database \"{}\" is NOT ready (state: {:?})",
                                    db_name,
                                    db.state()
                                );
                                tracing::warn!(
                                    target: targets::CONNECTION,
                                    database = %db_name,
                                    error = %err_msg,
                                    "Startup rejected"
                                );
                                self.send_error_response("57P03", &err_msg).await?;
                                return Err(anyhow::anyhow!(err_msg));
                            }
                        }
                        None => {
                            let err_msg = format!("database \"{}\" does not exist", db_name);
                            tracing::warn!(
                                target: targets::CONNECTION,
                                database = %db_name,
                                error = %err_msg,
                                "Startup rejected"
                            );
                            self.send_error_response("3D000", &err_msg).await?;
                            return Err(anyhow::anyhow!(err_msg));
                        }
                    };

                    // Initialize the session with the startup identity.
                    let mut session = Session::with_user(self.id, self.instance.clone(), user_name);
                    session.set_copy_stdin_memory_limit(self.copy_stdin_memory_limit);
                    // Set the current database context
                    session.set_current_database(db_name)?;
                    if let Some(application_name) = start.parameters.get("application_name") {
                        session.set_session_setting(
                            "application_name",
                            Value::Varchar(application_name.clone()),
                        )?;
                    }

                    self.session = Some(session);
                    self.register_connection_manager_mirror();

                    // No auth implemented yet, just say OK
                    self.socket
                        .send(PgWireBackendMessage::Authentication(Authentication::Ok))
                        .await?;

                    for (name, value) in self
                        .session
                        .as_ref()
                        .expect("session must exist after startup initialization")
                        .startup_parameters()
                    {
                        self.socket
                            .send(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                                name.to_string(),
                                value,
                            )))
                            .await?;
                    }

                    // Send ReadyForQuery to complete the handshake
                    self.send_ready_for_query().await?;

                    tracing::info!(
                        target: targets::CONNECTION,
                        user = %user_name,
                        database = %db_name,
                        "Startup completed"
                    );

                    return Ok(());
                }
                PgWireFrontendMessage::CancelRequest(cancel) => {
                    tracing::warn!(
                        target: targets::CONNECTION,
                        session_id = self.id,
                        cancel_pid = cancel.pid,
                        cancel_secret = ?cancel.secret_key,
                        "CancelRequest received during startup but cancellation is not implemented yet"
                    );
                    return Ok(());
                }
                PgWireFrontendMessage::PasswordMessageFamily(_) => {
                    tracing::debug!(
                        target: targets::CONNECTION,
                        session_id = self.id,
                        "Ignoring authentication message during unauthenticated startup"
                    );
                }
                PgWireFrontendMessage::SslNegotiation(SslNegotiationMetaMessage::PostgresSsl(
                    _,
                )) => {
                    // Reject SSL for now. This remains the future TLS upgrade entry point.
                    tracing::debug!(
                        target: targets::CONNECTION,
                        "SSL negotiation rejected"
                    );
                    self.socket.get_mut().write_all(b"N").await?;
                }
                PgWireFrontendMessage::SslNegotiation(SslNegotiationMetaMessage::None) => {
                    // Client didn't ask for SSL, continue waiting for Startup
                }
                msg => {
                    return Err(anyhow::anyhow!(
                        "unexpected startup message: {}",
                        Self::frontend_message_name(&msg)
                    ));
                }
            }
        }
    }

    async fn recv_frontend_message(&mut self) -> anyhow::Result<Option<PgWireFrontendMessage>> {
        tokio::select! {
            biased;
            _ = self.force_close_token.cancelled() => {
                self.force_close_session();
                Ok(None)
            }
            _ = self.drain_token.cancelled() => {
                // Once server shutdown starts draining, we prefer to stop accepting frontend
                // work immediately even if a full message became ready in the same poll turn.
                // That can drop an already-decoded message, but the connection is about to
                // close anyway and the shutdown contract is "stop taking new work".
                Ok(None)
            }
            message = self.socket.next() => {
                match message {
                    Some(Ok(message)) => Ok(Some(message)),
                    Some(Err(error)) => {
                        self.handle_frontend_decode_error(&error).await?;
                        Err(error.into())
                    }
                    None => Ok(None),
                }
            }
        }
    }

    async fn recv_frontend_message_before(
        &mut self,
        deadline: Instant,
    ) -> anyhow::Result<Option<PgWireFrontendMessage>> {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            tracing::warn!(
                target: targets::CONNECTION,
                session_id = self.id,
                timeout_ms = self.limits.startup_timeout().as_millis() as u64,
                "Connection closed because startup handshake timed out"
            );
            return Ok(None);
        };

        match timeout(remaining, self.recv_frontend_message()).await {
            Ok(result) => result,
            Err(_) => {
                tracing::warn!(
                    target: targets::CONNECTION,
                    session_id = self.id,
                    timeout_ms = self.limits.startup_timeout().as_millis() as u64,
                    "Connection closed because startup handshake timed out"
                );
                Ok(None)
            }
        }
    }

    async fn dispatch(&mut self, msg: PgWireFrontendMessage) -> anyhow::Result<DispatchResult> {
        if self.protocol_state == FrontendProtocolState::SkippingUntilSync {
            return self.dispatch_while_skipping(msg).await;
        }

        match msg {
            PgWireFrontendMessage::Query(q) => {
                self.execute_protocol_query(&q.query).await?;
                Ok(DispatchResult::Continue {
                    send_ready_for_query: true,
                })
            }
            PgWireFrontendMessage::Parse(parse) => {
                self.handle_extended_query_message(ExtendedQueryMessage::Parse(ParseMessage {
                    name: parse.name,
                    query: parse.query,
                    type_oids: parse.type_oids,
                }))
                .await?;
                Ok(DispatchResult::Continue {
                    send_ready_for_query: false,
                })
            }
            PgWireFrontendMessage::Bind(bind) => {
                self.handle_extended_query_message(ExtendedQueryMessage::Bind(BindMessage {
                    portal_name: bind.portal_name,
                    statement_name: bind.statement_name,
                    parameter_format_codes: bind.parameter_format_codes,
                    parameters: bind
                        .parameters
                        .into_iter()
                        .map(|param| param.map(|bytes| bytes.to_vec()))
                        .collect(),
                    result_column_format_codes: bind.result_column_format_codes,
                }))
                .await?;
                Ok(DispatchResult::Continue {
                    send_ready_for_query: false,
                })
            }
            PgWireFrontendMessage::Describe(describe) => {
                let target = match describe.target_type {
                    pgwire::messages::extendedquery::TARGET_TYPE_BYTE_STATEMENT => {
                        DescribeTarget::Statement(describe.name)
                    }
                    pgwire::messages::extendedquery::TARGET_TYPE_BYTE_PORTAL => {
                        DescribeTarget::Portal(describe.name)
                    }
                    _ => {
                        self.handle_extended_query_protocol_error(
                            paro_common::error::protocol_violation(
                                "Describe target type must be statement or portal",
                            ),
                        )
                        .await?;
                        return Ok(DispatchResult::Continue {
                            send_ready_for_query: false,
                        });
                    }
                };
                self.handle_extended_query_message(ExtendedQueryMessage::Describe(target))
                    .await?;
                Ok(DispatchResult::Continue {
                    send_ready_for_query: false,
                })
            }
            PgWireFrontendMessage::Execute(execute) => {
                self.handle_extended_query_message(ExtendedQueryMessage::Execute(
                    ExecutePortalMessage {
                        name: execute.name,
                        max_rows: execute.max_rows,
                    },
                ))
                .await?;
                Ok(DispatchResult::Continue {
                    send_ready_for_query: false,
                })
            }
            PgWireFrontendMessage::Close(close) => {
                let target = match close.target_type {
                    pgwire::messages::extendedquery::TARGET_TYPE_BYTE_STATEMENT => {
                        CloseTarget::Statement(close.name)
                    }
                    pgwire::messages::extendedquery::TARGET_TYPE_BYTE_PORTAL => {
                        CloseTarget::Portal(close.name)
                    }
                    _ => {
                        self.handle_extended_query_protocol_error(
                            paro_common::error::protocol_violation(
                                "Close target type must be statement or portal",
                            ),
                        )
                        .await?;
                        return Ok(DispatchResult::Continue {
                            send_ready_for_query: false,
                        });
                    }
                };
                self.handle_extended_query_message(ExtendedQueryMessage::Close(target))
                    .await?;
                Ok(DispatchResult::Continue {
                    send_ready_for_query: false,
                })
            }
            PgWireFrontendMessage::Flush(_) => {
                self.handle_extended_query_message(ExtendedQueryMessage::Flush)
                    .await?;
                Ok(DispatchResult::Continue {
                    send_ready_for_query: false,
                })
            }
            PgWireFrontendMessage::Sync(_) => {
                self.finish_extended_query_pipeline().await?;
                self.send_ready_for_query().await?;
                Ok(DispatchResult::Continue {
                    send_ready_for_query: false,
                })
            }
            PgWireFrontendMessage::Terminate(_) => {
                tracing::debug!(target: targets::CONNECTION, "Terminate requested");
                Ok(DispatchResult::Terminate)
            }
            PgWireFrontendMessage::CopyData(_)
            | PgWireFrontendMessage::CopyFail(_)
            | PgWireFrontendMessage::CopyDone(_)
            | PgWireFrontendMessage::PortalSuspended(_) => {
                self.send_error_response(
                    "08P01",
                    "unexpected frontend message outside COPY or extended query",
                )
                .await?;
                Ok(DispatchResult::Continue {
                    send_ready_for_query: false,
                })
            }
            PgWireFrontendMessage::SslNegotiation(_) | PgWireFrontendMessage::Startup(_) => {
                Ok(DispatchResult::Continue {
                    send_ready_for_query: false,
                })
            }
            PgWireFrontendMessage::CancelRequest(cancel) => {
                tracing::warn!(
                    target: targets::CONNECTION,
                    session_id = self.id,
                    cancel_pid = cancel.pid,
                    cancel_secret = ?cancel.secret_key,
                    "CancelRequest reached normal dispatch path unexpectedly"
                );
                Ok(DispatchResult::Continue {
                    send_ready_for_query: false,
                })
            }
            PgWireFrontendMessage::PasswordMessageFamily(_) => {
                self.send_error_response("08P01", "unexpected authentication message")
                    .await?;
                Ok(DispatchResult::Continue {
                    send_ready_for_query: false,
                })
            }
        }
    }

    async fn dispatch_while_skipping(
        &mut self,
        msg: PgWireFrontendMessage,
    ) -> anyhow::Result<DispatchResult> {
        match msg {
            PgWireFrontendMessage::Flush(_) => {
                self.socket.flush().await?;
                Ok(DispatchResult::Continue {
                    send_ready_for_query: false,
                })
            }
            PgWireFrontendMessage::Sync(_) => {
                tracing::debug!(
                    target: targets::CONNECTION,
                    "Sync received while skipping until sync"
                );
                self.protocol_state = FrontendProtocolState::Ready;
                self.finish_extended_query_pipeline().await?;
                self.send_ready_for_query().await?;
                Ok(DispatchResult::Continue {
                    send_ready_for_query: false,
                })
            }
            PgWireFrontendMessage::Terminate(_) => Ok(DispatchResult::Terminate),
            other => {
                tracing::debug!(
                    target: targets::CONNECTION,
                    message_type = Self::frontend_message_name(&other),
                    "Skipping frontend message until Sync"
                );
                Ok(DispatchResult::Continue {
                    send_ready_for_query: false,
                })
            }
        }
    }

    fn enter_skip_until_sync(&mut self) {
        self.protocol_state = FrontendProtocolState::SkippingUntilSync;
    }

    async fn handle_extended_query_message(
        &mut self,
        message: ExtendedQueryMessage,
    ) -> anyhow::Result<()> {
        self.begin_extended_query_pipeline();

        let session = self.session.as_mut().expect("session must be initialized");
        let mut responder = PgWireExtendedQueryResponder::new(&mut self.socket);
        match session
            .execute_extended_query_message(message, &mut responder)
            .await
        {
            Ok(()) => Ok(()),
            Err(err) => self.handle_extended_query_protocol_error(err).await,
        }
    }

    fn begin_extended_query_pipeline(&mut self) {
        if self.extended_query_pipeline_open {
            return;
        }

        self.extended_query_pipeline_open = true;
    }

    async fn finish_extended_query_pipeline(&mut self) -> anyhow::Result<()> {
        if !self.extended_query_pipeline_open {
            return Ok(());
        }

        self.extended_query_pipeline_open = false;
        let session = self.session.as_mut().expect("session must be initialized");
        if session.is_in_implicit_block() {
            if session.is_transaction_failed() {
                session.rollback_implicit_transaction()?;
            } else {
                session.end_implicit_transaction_block()?;
            }
        }
        Ok(())
    }

    async fn handle_extended_query_protocol_error(
        &mut self,
        err: paro_common::error::ParoError,
    ) -> anyhow::Result<()> {
        let session = self.session.as_mut().expect("session must be initialized");
        if session.is_in_implicit_block() {
            let _ = session.rollback_implicit_transaction();
        } else if session.is_in_explicit_block() {
            session.set_transaction_failed();
        }

        self.extended_query_pipeline_open = false;
        self.enter_skip_until_sync();

        let mut responder = PgWireExtendedQueryResponder::new(&mut self.socket);
        responder.send_error(&err).await?;
        Ok(())
    }

    async fn execute_protocol_query(&mut self, sql: &str) -> anyhow::Result<()> {
        let session = self.session.as_mut().expect("session must be initialized");
        let mut sink = ProtocolSink::new(&mut self.socket);

        match session.execute_simple_query(sql, &mut sink).await {
            Ok(()) => Ok(()),
            Err(err) if sink.error_was_sent() => {
                tracing::debug!(
                    target: targets::CONNECTION,
                    session_id = self.id,
                    error = %err,
                    "Simple query returned after sending ErrorResponse"
                );
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn send_ready_for_query(&mut self) -> anyhow::Result<()> {
        // Determine transaction status from session state
        let status = match self.session.as_ref() {
            Some(session) => match session.transaction_state() {
                TransactionState::Idle => TransactionStatus::Idle,
                TransactionState::InTransaction => TransactionStatus::Transaction,
                TransactionState::Failed => TransactionStatus::Error,
            },
            None => TransactionStatus::Idle,
        };

        self.socket
            .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                status,
            )))
            .await?;
        Ok(())
    }

    async fn send_error_response(&mut self, sqlstate: &str, message: &str) -> anyhow::Result<()> {
        self.socket
            .send(PgWireBackendMessage::ErrorResponse(
                build_error_response_message("ERROR", sqlstate, message),
            ))
            .await?;
        Ok(())
    }

    async fn handle_frontend_decode_error(&mut self, error: &PgWireError) -> anyhow::Result<()> {
        match error {
            PgWireError::MessageTooLarge(max, actual) => {
                self.send_error_response(
                    "08P01",
                    &format!(
                        "frontend message length {} exceeds protocol limit {}",
                        actual, max
                    ),
                )
                .await?;
            }
            PgWireError::InvalidMessageType(message_type) => {
                self.send_error_response(
                    "08P01",
                    &format!("invalid frontend message type {}", *message_type as char),
                )
                .await?;
            }
            _ => {}
        }
        Ok(())
    }

    fn frontend_message_name(msg: &PgWireFrontendMessage) -> &'static str {
        match msg {
            PgWireFrontendMessage::Startup(_) => "Startup",
            PgWireFrontendMessage::SslNegotiation(SslNegotiationMetaMessage::PostgresSsl(_)) => {
                "SslNegotiation(PostgresSsl)"
            }
            PgWireFrontendMessage::SslNegotiation(SslNegotiationMetaMessage::None) => {
                "SslNegotiation(None)"
            }
            PgWireFrontendMessage::Query(_) => "Query",
            PgWireFrontendMessage::Parse(_) => "Parse",
            PgWireFrontendMessage::Bind(_) => "Bind",
            PgWireFrontendMessage::Describe(_) => "Describe",
            PgWireFrontendMessage::Execute(_) => "Execute",
            PgWireFrontendMessage::Close(_) => "Close",
            PgWireFrontendMessage::Flush(_) => "Flush",
            PgWireFrontendMessage::Sync(_) => "Sync",
            PgWireFrontendMessage::Terminate(_) => "Terminate",
            PgWireFrontendMessage::CopyData(_) => "CopyData",
            PgWireFrontendMessage::CopyDone(_) => "CopyDone",
            PgWireFrontendMessage::CopyFail(_) => "CopyFail",
            PgWireFrontendMessage::CancelRequest(_) => "CancelRequest",
            PgWireFrontendMessage::PasswordMessageFamily(_) => "PasswordMessageFamily",
            _ => "Other",
        }
    }

    fn register_connection_manager_mirror(&mut self) {
        if self.mirrored_in_connection_manager {
            return;
        }

        self.control.mark_handshake_complete();
        let connection: Arc<dyn ManagedConnection> = self.control.clone();
        self.instance
            .get_connection_manager()
            .add_connection(connection);
        self.mirrored_in_connection_manager = true;
    }

    fn force_close_session(&mut self) {
        self.control.request_force_close();
        if let Some(session) = self.session.as_ref() {
            session.interrupt();
        }
    }

    fn cleanup(&mut self) {
        self.control.deactivate();
        if self.mirrored_in_connection_manager {
            self.instance
                .get_connection_manager()
                .remove_connection(self.id);
            self.mirrored_in_connection_manager = false;
        }
    }
}

/// PostgreSQL wire protocol codec for Tokio framed streams.
///
/// This codec handles encoding/decoding of postgres frontend/backend messages.
pub struct PgCodec {
    ctx: DecodeContext,
    limits: PgFrontendMessageLimits,
    copy_data_mode: bool,
}

impl Default for PgCodec {
    fn default() -> Self {
        Self::new(PgFrontendMessageLimits::new(MAX_PG_FRONTEND_PACKET_BYTES))
    }
}

impl PgCodec {
    /// Create a new PgCodec with default protocol version (3.0).
    pub fn new(limits: PgFrontendMessageLimits) -> Self {
        Self {
            ctx: DecodeContext::new(ProtocolVersion::PROTOCOL3_0),
            limits,
            copy_data_mode: false,
        }
    }

    pub fn enter_copy_data_mode(&mut self) {
        self.copy_data_mode = true;
    }

    pub fn leave_copy_data_mode(&mut self) {
        self.copy_data_mode = false;
    }

    fn enforce_frontend_message_limit(&self, src: &BytesMut) -> Result<(), PgWireError> {
        let Some((message_length, limit, rejected_reason)) = self.peek_frontend_message_limit(src)
        else {
            return Ok(());
        };

        if message_length > limit {
            if let Some(reason) = rejected_reason {
                copy_stdin_metrics().record_rejection(reason);
            }
            return Err(PgWireError::MessageTooLarge(limit, message_length));
        }

        Ok(())
    }

    fn peek_frontend_message_limit(
        &self,
        src: &BytesMut,
    ) -> Option<(usize, usize, Option<CopyStdinRejectReason>)> {
        if self.ctx.awaiting_ssl || self.ctx.awaiting_startup {
            if src.len() < 4 {
                return None;
            }
            let length = i32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
            return Some((length, self.limits.startup_packet_bytes, None));
        }

        if src.len() < 5 {
            return None;
        }

        let message_type = src[0];
        let message_length = i32::from_be_bytes([src[1], src[2], src[3], src[4]]) as usize;
        let (limit, rejected_reason) = match message_type {
            MESSAGE_TYPE_BYTE_COPY_DATA if self.copy_data_mode => (
                self.limits.copy_data_bytes,
                Some(CopyStdinRejectReason::FrameLimit),
            ),
            MESSAGE_TYPE_BYTE_COPY_DATA
            | MESSAGE_TYPE_BYTE_COPY_DONE
            | MESSAGE_TYPE_BYTE_COPY_FAIL
            | MESSAGE_TYPE_BYTE_FLUSH
            | MESSAGE_TYPE_BYTE_SYNC
            | MESSAGE_TYPE_BYTE_TERMINATE => (self.limits.small_message_bytes, None),
            _ => (self.limits.normal_message_bytes, None),
        };

        Some((message_length, limit, rejected_reason))
    }
}

impl Decoder for PgCodec {
    type Item = PgWireFrontendMessage;
    type Error = pgwire::error::PgWireError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.enforce_frontend_message_limit(src)?;
        let result = PgWireFrontendMessage::decode(src, &self.ctx)?;
        if let Some(ref msg) = result {
            match msg {
                PgWireFrontendMessage::SslNegotiation(SslNegotiationMetaMessage::None) => {
                    self.ctx.awaiting_ssl = false;
                }
                PgWireFrontendMessage::Startup(_) => {
                    self.ctx.awaiting_ssl = false;
                    self.ctx.awaiting_startup = false;
                }
                _ => {}
            }
        }
        Ok(result)
    }
}

impl Encoder<PgWireBackendMessage> for PgCodec {
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: PgWireBackendMessage,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        item.encode(dst).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgwire::messages::copy::CopyData;
    use pgwire::messages::simplequery::Query;
    use pgwire::messages::startup::Startup;
    use pgwire::messages::Message;
    use serial_test::serial;
    use tokio_util::bytes::{Bytes, BytesMut};

    fn encode_frontend_message(message: PgWireFrontendMessage) -> BytesMut {
        let mut buf = BytesMut::new();
        message.encode(&mut buf).unwrap();
        buf
    }

    #[test]
    fn codec_rejects_oversized_query_messages_before_decode() {
        let mut codec = PgCodec::new(PgFrontendMessageLimits {
            startup_packet_bytes: 1024,
            normal_message_bytes: 16,
            small_message_bytes: 8,
            copy_data_bytes: 32,
        });
        codec.ctx.awaiting_ssl = false;
        codec.ctx.awaiting_startup = false;

        let mut buf = encode_frontend_message(PgWireFrontendMessage::Query(Query::new(
            "SELECT 1234567890".to_string(),
        )));
        let err = codec
            .decode(&mut buf)
            .expect_err("query should exceed normal limit");
        assert!(matches!(err, PgWireError::MessageTooLarge(16, _)));
    }

    #[test]
    fn codec_allows_copy_frames_within_copy_mode_limit() {
        let mut codec = PgCodec::new(PgFrontendMessageLimits {
            startup_packet_bytes: 1024,
            normal_message_bytes: 16,
            small_message_bytes: 8,
            copy_data_bytes: 32,
        });
        codec.ctx.awaiting_ssl = false;
        codec.ctx.awaiting_startup = false;
        codec.enter_copy_data_mode();

        let mut buf = encode_frontend_message(PgWireFrontendMessage::CopyData(CopyData::new(
            Bytes::from_static(b"12345678901234567890"),
        )));
        let decoded = codec.decode(&mut buf).expect("decode should succeed");
        assert!(matches!(decoded, Some(PgWireFrontendMessage::CopyData(_))));
    }

    #[test]
    fn copy_message_limit_includes_length_header_overhead() {
        let limits = PgFrontendMessageLimits::new(20);
        assert_eq!(limits.copy_data_bytes, 24);
    }

    #[test]
    #[serial]
    fn codec_rejects_oversized_copy_frames_and_records_metric() {
        copy_stdin_metrics().reset_for_tests();

        let mut codec = PgCodec::new(PgFrontendMessageLimits {
            startup_packet_bytes: 1024,
            normal_message_bytes: 16,
            small_message_bytes: 8,
            copy_data_bytes: 12,
        });
        codec.ctx.awaiting_ssl = false;
        codec.ctx.awaiting_startup = false;
        codec.enter_copy_data_mode();

        let mut buf = encode_frontend_message(PgWireFrontendMessage::CopyData(CopyData::new(
            Bytes::from_static(b"12345678901234567890"),
        )));
        let err = codec
            .decode(&mut buf)
            .expect_err("copy frame should exceed copy-data limit");
        assert!(matches!(err, PgWireError::MessageTooLarge(12, _)));

        let metrics = copy_stdin_metrics().snapshot();
        assert_eq!(metrics.rejected_total, 1);
        assert_eq!(metrics.rejected_total_limit, 0);
        assert_eq!(metrics.rejected_frame_limit, 1);
    }

    #[test]
    fn codec_rejects_oversized_startup_packets() {
        let mut codec = PgCodec::new(PgFrontendMessageLimits {
            startup_packet_bytes: 16,
            normal_message_bytes: 64,
            small_message_bytes: 8,
            copy_data_bytes: 32,
        });
        let mut buf = BytesMut::new();
        let mut startup = Startup::new();
        startup
            .parameters
            .insert("user".to_string(), "alice".repeat(32));
        startup
            .parameters
            .insert("database".to_string(), "postgres".to_string());
        startup.encode(&mut buf).unwrap();
        let err = codec
            .decode(&mut buf)
            .expect_err("startup packet should exceed startup limit");
        assert!(matches!(err, PgWireError::MessageTooLarge(16, _)));
    }
}
