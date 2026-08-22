// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Server - TCP accept loop.

use paro_common::config::ParoConfig;
use paro_common::logging::targets;
use paro_instance::{Instance, InstanceConfig, InstanceShutdownMode, InstanceShutdownReport};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Notify};
use tokio::time::{sleep, timeout, Instant};
use tokio_util::sync::CancellationToken;

use crate::connection::{Connection, ConnectionInit, PgFrontendMessageLimits};
use crate::connection_control::{ServerConnectionControl, ServerConnectionHandle, ServerLimits};

const CONNECTION_DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(25);
const FORCE_CLOSE_GRACE_PERIOD: Duration = Duration::from_millis(100);
const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const PRE_AUTH_CANCEL_PEEK_TIMEOUT: Duration = Duration::from_millis(100);

/// Server configuration (derived from ParoConfig).
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub addr: String,
    pub data_dir: String,
    pub max_connections: usize,
    pub buffer_pool_size: usize,
    pub startup_timeout: Duration,
    pub copy_stdin_memory_limit: usize,
    pub frontend_message_limits: PgFrontendMessageLimits,
}

impl From<&ParoConfig> for ServerConfig {
    fn from(config: &ParoConfig) -> Self {
        let copy_stdin_memory_limit = config
            .server
            .effective_copy_stdin_memory_limit(config.cluster.max_memory);
        Self {
            addr: config.server.address(),
            data_dir: config.storage.data_dir.to_string_lossy().to_string(),
            max_connections: config.server.max_connections,
            buffer_pool_size: config.storage.buffer_pool.size,
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            frontend_message_limits: PgFrontendMessageLimits::new(copy_stdin_memory_limit),
            copy_stdin_memory_limit,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServerShutdownReport {
    pub connections_drained: usize,
    pub connections_forced: usize,
    pub instance_shutdown_report: InstanceShutdownReport,
}

/// Paro database server.
pub struct Server {
    config: ServerConfig,
    /// The database instance root. Owned by the server.
    instance: Arc<Instance>,
    shutdown_token: CancellationToken,
    accept_loop_running: AtomicBool,
    accept_loop_stopped: Notify,
    connections_changed: Notify,
    listener_addr: Mutex<Option<SocketAddr>>,
    connections: Mutex<HashMap<u64, ServerConnectionHandle>>,
    limits: Arc<ServerLimits>,
}

impl Server {
    /// Initialize a new server from ParoConfig.
    pub async fn from_config(config: &ParoConfig) -> anyhow::Result<Self> {
        let server_config = ServerConfig::from(config);
        let max_connections = server_config.max_connections;
        let startup_timeout = server_config.startup_timeout;
        if config.server.tls.is_some() && config.server.allow_plaintext {
            tracing::warn!(
                target: targets::SERVER,
                "TLS is configured but not yet implemented; running in plaintext mode"
            );
        }

        // `ParoConfig.cluster` is the TOML section name; map it into runtime `InstanceConfig`.
        let mut instance_config = InstanceConfig::from(config);
        instance_config.options.instance_root = server_config.data_dir.clone();

        // Always create a persistent instance (in-memory mode is only for testing).
        let instance = Instance::new_persistent(&server_config.data_dir, instance_config)?;

        tracing::info!(
            target: targets::INSTANCE,
            data_dir = %server_config.data_dir,
            "Persistent instance initialized"
        );

        Ok(Self {
            config: server_config,
            instance,
            shutdown_token: CancellationToken::new(),
            accept_loop_running: AtomicBool::new(false),
            accept_loop_stopped: Notify::new(),
            connections_changed: Notify::new(),
            listener_addr: Mutex::new(None),
            connections: Mutex::new(HashMap::new()),
            limits: Arc::new(ServerLimits::new(max_connections, startup_timeout)),
        })
    }

    /// Run the server - accept connections and spawn handlers.
    pub async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        self.accept_loop_running.store(true, Ordering::Release);

        let listener = match TcpListener::bind(&self.config.addr).await {
            Ok(listener) => listener,
            Err(error) => {
                self.accept_loop_running.store(false, Ordering::Release);
                self.accept_loop_stopped.notify_waiters();
                tracing::error!(
                    target: targets::SERVER,
                    addr = %self.config.addr,
                    error = %error,
                    "Failed to bind server listener"
                );
                return Err(error.into());
            }
        };

        *self.listener_addr.lock().expect("listener addr poisoned") =
            Some(listener.local_addr().map_err(anyhow::Error::from)?);

        tracing::info!(
            target: targets::SERVER,
            addr = %self.config.addr,
            data_dir = %self.config.data_dir,
            max_connections = self.config.max_connections,
            buffer_pool_size = self.config.buffer_pool_size,
            copy_stdin_memory_limit = self.config.copy_stdin_memory_limit,
            "Server listener started"
        );

        let result = loop {
            let accepted = tokio::select! {
                biased;
                _ = self.shutdown_token.cancelled() => {
                    tracing::info!(
                        target: targets::SERVER,
                        addr = %self.config.addr,
                        "Server listener shutting down"
                    );
                    break Ok(());
                }
                accepted = listener.accept() => accepted,
            };

            let (socket, peer) = match accepted {
                Ok(accepted) => accepted,
                Err(error) => break Err(error.into()),
            };

            if self.limits.pre_auth_limit_reached()
                && !socket_looks_like_cancel_request(&socket).await
            {
                tracing::warn!(
                    target: targets::SERVER,
                    peer_addr = %peer,
                    max_connections = self.config.max_connections,
                    "Connection rejected before handshake because max_connections was reached"
                );
                drop(socket);
                continue;
            }

            if self.shutdown_token.is_cancelled() {
                tracing::info!(
                    target: targets::SERVER,
                    addr = %self.config.addr,
                    peer_addr = %peer,
                    "Dropping accepted socket because shutdown was requested before the connection task was registered"
                );
                drop(socket);
                break Ok(());
            }

            let connection_id = self
                .instance
                .get_connection_registry()
                .assign_connection_id();

            tracing::info!(
                target: targets::CONNECTION,
                session_id = connection_id,
                peer_addr = %peer,
                "Connection accepted"
            );

            let control = Arc::new(ServerConnectionControl::new(
                connection_id,
                peer.to_string(),
            ));
            let drain_token = CancellationToken::new();
            let force_close_token = CancellationToken::new();
            let instance = Arc::clone(&self.instance);
            let peer_addr = peer.to_string();
            let server = Arc::clone(&self);
            let connection_control = Arc::clone(&control);
            let limits = Arc::clone(&self.limits);
            let frontend_message_limits = self.config.frontend_message_limits;
            let copy_stdin_memory_limit = self.config.copy_stdin_memory_limit;
            let connection_drain_token = drain_token.clone();
            let connection_force_close_token = force_close_token.clone();

            // Register the connection before its task starts running so shutdown never
            // observes an accepted-but-untracked socket.
            let (start_tx, start_rx) = oneshot::channel();
            let join_handle = tokio::spawn(async move {
                let _ = start_rx.await;
                let conn = Connection::new(ConnectionInit {
                    id: connection_id,
                    peer_addr,
                    tcp: socket,
                    instance,
                    control: connection_control,
                    limits,
                    frontend_message_limits,
                    copy_stdin_memory_limit,
                    drain_token: connection_drain_token,
                    force_close_token: connection_force_close_token,
                });
                conn.run().await;
                // `Connection::cleanup()` removes the ConnectionRegistry observability mirror.
                // This unregister removes the authoritative server-owned drain handle.
                server.unregister_connection(connection_id);
            });

            self.register_connection(ServerConnectionHandle {
                connection_id,
                control,
                join_handle,
                drain_token,
                force_close_token,
            });
            let _ = start_tx.send(());
        };

        self.accept_loop_running.store(false, Ordering::Release);
        self.accept_loop_stopped.notify_waiters();
        result
    }

    pub fn request_shutdown(&self) {
        self.shutdown_token.cancel();
    }

    pub async fn shutdown(
        self: &Arc<Self>,
        drain_timeout: Duration,
    ) -> anyhow::Result<ServerShutdownReport> {
        self.request_shutdown();
        self.wait_for_accept_loop_stop().await;

        let initial_connection_count = self.connection_count();
        self.broadcast_drain();

        let drained_before_deadline = self.wait_for_connections_to_exit(drain_timeout).await;
        if !drained_before_deadline {
            let remaining_at_deadline = self.connection_count();
            tracing::warn!(
                target: targets::SERVER,
                tracked_connections = remaining_at_deadline,
                "Server shutdown reached the drain deadline; force-closing remaining connections"
            );
            self.broadcast_force_close();
            let _ = self
                .wait_for_connections_to_exit(FORCE_CLOSE_GRACE_PERIOD)
                .await;
        }

        let remaining_handles = self.take_all_connections();
        let forced = remaining_handles.len();
        let drained = initial_connection_count.saturating_sub(forced);
        for handle in &remaining_handles {
            handle.control.request_force_close();
            handle.control.deactivate();
            self.instance
                .get_session_registry()
                .unregister(handle.connection_id);
            self.instance
                .get_connection_registry()
                .remove_connection(handle.connection_id);
            handle.force_close_token.cancel();
            handle.join_handle.abort();
        }
        for handle in remaining_handles {
            let _ = handle.join_handle.await;
        }

        let instance = Arc::clone(&self.instance);
        let instance_shutdown_report = tokio::task::spawn_blocking(move || {
            if forced != 0 {
                return instance.shutdown_dirty(InstanceShutdownMode::TryCheckpoint);
            }

            match instance.verify_quiesced_for_clean_shutdown() {
                Ok(proof) => instance.shutdown_clean(InstanceShutdownMode::Checkpoint, proof),
                Err(err) => {
                    tracing::warn!(
                        target: targets::SERVER,
                        error = %err,
                        "Clean shutdown proof unavailable after server drain; falling back to dirty instance shutdown"
                    );
                    instance.shutdown_dirty(InstanceShutdownMode::TryCheckpoint)
                }
            }
        })
        .await
        .map_err(|err| anyhow::anyhow!("server shutdown worker failed: {}", err))??;

        tracing::info!(
            target: targets::SERVER,
            drained_connections = drained,
            forced_connections = forced,
            instance_disposition = ?instance_shutdown_report.disposition,
            clean_shutdown_persisted = instance_shutdown_report.clean_shutdown_persisted,
            "Server shutdown completed"
        );

        Ok(ServerShutdownReport {
            connections_drained: drained,
            connections_forced: forced,
            instance_shutdown_report,
        })
    }

    pub fn local_addr(&self) -> Option<SocketAddr> {
        *self.listener_addr.lock().expect("listener addr poisoned")
    }

    fn register_connection(&self, handle: ServerConnectionHandle) {
        self.connections
            .lock()
            .expect("server connection registry")
            .insert(handle.connection_id, handle);
        self.limits.on_connection_registered();
        self.connections_changed.notify_waiters();
    }

    fn unregister_connection(&self, connection_id: u64) {
        if self
            .connections
            .lock()
            .expect("server connection registry")
            .remove(&connection_id)
            .is_some()
        {
            self.limits.on_connection_unregistered();
            self.connections_changed.notify_waiters();
        }
    }

    fn connection_count(&self) -> usize {
        self.connections
            .lock()
            .expect("server connection registry")
            .len()
    }

    fn broadcast_drain(&self) {
        let connections = self.connections.lock().expect("server connection registry");
        for handle in connections.values() {
            // Keep the control-plane booleans and async cancellation tokens in sync:
            // the former is for observability, the latter is what the connection task polls.
            handle.control.request_drain();
            handle.drain_token.cancel();
        }
    }

    fn broadcast_force_close(&self) {
        let connections = self.connections.lock().expect("server connection registry");
        for handle in connections.values() {
            // See `broadcast_drain()`: control state is descriptive, token cancellation is
            // the protocol-level mechanism that makes the task break out.
            handle.control.request_force_close();
            handle.force_close_token.cancel();
        }
    }

    fn take_all_connections(&self) -> Vec<ServerConnectionHandle> {
        let drained = self
            .connections
            .lock()
            .expect("server connection registry")
            .drain()
            .map(|(_, handle)| handle)
            .collect::<Vec<_>>();
        self.limits.reset_tracked_connections();
        drained
    }

    async fn wait_for_accept_loop_stop(&self) {
        loop {
            let notified = self.accept_loop_stopped.notified();
            if !self.accept_loop_running.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    async fn wait_for_connections_to_exit(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.connection_count() == 0 {
                return true;
            }

            let now = Instant::now();
            if now >= deadline {
                return self.connection_count() == 0;
            }

            let notified = self.connections_changed.notified();
            tokio::pin!(notified);
            tokio::select! {
                _ = &mut notified => {}
                _ = sleep((deadline - now).min(CONNECTION_DRAIN_POLL_INTERVAL)) => {}
            }
        }
    }
}

async fn socket_looks_like_cancel_request(socket: &TcpStream) -> bool {
    let deadline = Instant::now() + PRE_AUTH_CANCEL_PEEK_TIMEOUT;
    let mut header = [0_u8; 8];

    loop {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return false;
        };

        match timeout(remaining, socket.readable()).await {
            Ok(Ok(())) => match socket.peek(&mut header).await {
                Ok(read) if read >= header.len() => {
                    return pgwire::messages::cancel::CancelRequest::is_cancel_request_packet(
                        &header[..read],
                    );
                }
                Ok(_) => continue,
                Err(_) => return false,
            },
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_instance::InstanceShutdownDisposition;
    use paro_instance::{InstanceLayout, InstanceLifecycleState, InstanceRunStateStore};
    use paro_storage::meta::{FileMetadataStore, MetadataStore};
    use pgwire::messages::cancel::CancelRequest;
    use pgwire::messages::copy::{CopyData, CopyDone};
    use pgwire::messages::extendedquery::{Bind, Execute, Flush, Parse, Sync};
    use pgwire::messages::simplequery::Query;
    use pgwire::messages::startup::SecretKey;
    use pgwire::messages::PgWireFrontendMessage;
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::task::JoinHandle;
    use tokio_util::bytes::Bytes;

    async fn wait_for_listener(server: &Arc<Server>) -> SocketAddr {
        let start = Instant::now();
        loop {
            if let Some(addr) = server.local_addr() {
                return addr;
            }
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "listener should publish a bound address"
            );
            sleep(Duration::from_millis(10)).await;
        }
    }

    async fn spawn_test_server(
        max_connections: usize,
    ) -> (Arc<Server>, JoinHandle<anyhow::Result<()>>, SocketAddr) {
        let data_dir = tempfile::Builder::new()
            .prefix("paro-server-test-")
            .tempdir()
            .expect("create tempdir")
            .keep();
        let mut config = ParoConfig::default();
        config.server.host = "127.0.0.1".to_string();
        config.server.port = 0;
        config.server.max_connections = max_connections;
        config.storage.data_dir = data_dir;

        let server = Arc::new(Server::from_config(&config).await.expect("build server"));
        let run_task = tokio::spawn(Arc::clone(&server).run());
        let addr = wait_for_listener(&server).await;
        (server, run_task, addr)
    }

    fn encode_startup_packet(user: &str, database: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&196_608_i32.to_be_bytes());
        for (key, value) in [("user", user), ("database", database)] {
            body.extend_from_slice(key.as_bytes());
            body.push(0);
            body.extend_from_slice(value.as_bytes());
            body.push(0);
        }
        body.push(0);

        let mut packet = Vec::with_capacity(body.len() + 4);
        packet.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        packet.extend_from_slice(&body);
        packet
    }

    fn encode_frontend_message(message: PgWireFrontendMessage) -> Vec<u8> {
        let mut buf = tokio_util::bytes::BytesMut::new();
        message.encode(&mut buf).expect("encode frontend message");
        buf.to_vec()
    }

    fn encode_cancel_request(pid: i32, secret_key: i32) -> Vec<u8> {
        encode_frontend_message(PgWireFrontendMessage::CancelRequest(CancelRequest::new(
            pid,
            SecretKey::I32(secret_key),
        )))
    }

    fn encode_copy_fail_message(message: &str) -> Vec<u8> {
        let mut buf = Vec::with_capacity(message.len() + 6);
        buf.push(b'f');
        buf.extend_from_slice(&((message.len() + 5) as i32).to_be_bytes());
        buf.extend_from_slice(message.as_bytes());
        buf.push(0);
        buf
    }

    async fn read_backend_message(stream: &mut TcpStream) -> (u8, Vec<u8>) {
        let mut tag = [0_u8; 1];
        stream.read_exact(&mut tag).await.expect("message type");
        let mut len = [0_u8; 4];
        stream.read_exact(&mut len).await.expect("message length");
        let length = i32::from_be_bytes(len) as usize;
        let mut payload = vec![0_u8; length - 4];
        stream
            .read_exact(&mut payload)
            .await
            .expect("message payload");
        (tag[0], payload)
    }

    async fn read_backend_message_timeout(
        stream: &mut TcpStream,
        timeout_duration: Duration,
    ) -> Option<(u8, Vec<u8>)> {
        tokio::time::timeout(timeout_duration, read_backend_message(stream))
            .await
            .ok()
    }

    async fn read_messages_until_ready(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
        const MAX_MESSAGES: usize = 4_096;
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut messages = Vec::new();
        for _ in 0..MAX_MESSAGES {
            let mut message = tokio::time::timeout_at(deadline, read_backend_message(stream))
                .await
                .expect("backend message before ReadyForQuery");
            let ready = message.0 == b'Z';
            // COPY payloads can be arbitrarily large. Tests using this generic
            // control-message helper only need the tag, so retain no row data.
            if message.0 == b'd' {
                message.1.clear();
            }
            messages.push(message);
            if ready {
                return messages;
            }
        }
        panic!("backend emitted more than {MAX_MESSAGES} messages before ReadyForQuery");
    }

    async fn read_available_messages(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
        let mut messages = Vec::new();
        while let Some(message) = read_backend_message_timeout(stream, Duration::from_secs(1)).await
        {
            messages.push(message);
        }
        messages
    }

    async fn complete_startup(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
        stream
            .write_all(&encode_startup_packet("alice", "postgres"))
            .await
            .expect("write startup packet");
        read_messages_until_ready(stream).await
    }

    async fn run_simple_query_roundtrip(stream: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
        stream
            .write_all(&encode_frontend_message(PgWireFrontendMessage::Query(
                Query::new(sql.to_string()),
            )))
            .await
            .expect("write query");
        read_messages_until_ready(stream).await
    }

    fn command_complete_tag(payload: &[u8]) -> Option<String> {
        payload
            .iter()
            .position(|byte| *byte == 0)
            .and_then(|end| String::from_utf8(payload[..end].to_vec()).ok())
    }

    fn parameter_status(payload: &[u8]) -> Option<(String, String)> {
        let mut index = 0;
        let name_end = payload[index..]
            .iter()
            .position(|byte| *byte == 0)
            .map(|offset| index + offset)?;
        let name = String::from_utf8(payload[index..name_end].to_vec()).ok()?;
        index = name_end + 1;
        let value_end = payload[index..]
            .iter()
            .position(|byte| *byte == 0)
            .map(|offset| index + offset)?;
        let value = String::from_utf8(payload[index..value_end].to_vec()).ok()?;
        Some((name, value))
    }

    fn backend_key_data(payload: &[u8]) -> Option<(i32, i32)> {
        let pid = i32::from_be_bytes(payload.get(0..4)?.try_into().ok()?);
        let secret = i32::from_be_bytes(payload.get(4..8)?.try_into().ok()?);
        Some((pid, secret))
    }

    fn error_field(payload: &[u8], tag: u8) -> Option<String> {
        let mut index = 0;
        while index < payload.len() {
            let field_tag = payload[index];
            if field_tag == 0 {
                return None;
            }
            index += 1;
            let end = payload[index..]
                .iter()
                .position(|byte| *byte == 0)
                .map(|offset| index + offset)?;
            let value = String::from_utf8(payload[index..end].to_vec()).ok()?;
            if field_tag == tag {
                return Some(value);
            }
            index = end + 1;
        }
        None
    }

    fn ready_for_query_status(payload: &[u8]) -> Option<char> {
        payload.first().copied().map(char::from)
    }

    fn test_server_with_in_memory_instance() -> Arc<Server> {
        Arc::new(Server {
            config: ServerConfig {
                addr: "127.0.0.1:0".to_string(),
                data_dir: ":memory:".to_string(),
                max_connections: 0,
                buffer_pool_size: 0,
                startup_timeout: DEFAULT_STARTUP_TIMEOUT,
                copy_stdin_memory_limit: 256 * 1024 * 1024,
                frontend_message_limits: PgFrontendMessageLimits::new(256 * 1024 * 1024),
            },
            instance: Instance::new_in_memory(),
            shutdown_token: CancellationToken::new(),
            accept_loop_running: AtomicBool::new(false),
            accept_loop_stopped: Notify::new(),
            connections_changed: Notify::new(),
            listener_addr: Mutex::new(None),
            connections: Mutex::new(HashMap::new()),
            limits: Arc::new(ServerLimits::new(0, DEFAULT_STARTUP_TIMEOUT)),
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_shutdown_without_clients_persists_clean_run_state() {
        let dir = tempdir().expect("tempdir");
        let mut config = ParoConfig::default();
        config.server.host = "127.0.0.1".to_string();
        config.server.port = 0;
        config.storage.data_dir = dir.path().to_path_buf();

        let server = Arc::new(Server::from_config(&config).await.expect("build server"));
        let mut run_task = Some(tokio::spawn(Arc::clone(&server).run()));

        let start = Instant::now();
        while server.local_addr().is_none() {
            if run_task.as_ref().is_some_and(|task| task.is_finished()) {
                let result = run_task
                    .take()
                    .expect("accept loop task should exist")
                    .await
                    .expect("accept loop task should join");
                panic!(
                    "accept loop exited before publishing a bound address: {:?}",
                    result
                );
            }
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "listener should publish a bound address"
            );
            sleep(Duration::from_millis(10)).await;
        }

        let shutdown_report = server
            .shutdown(Duration::from_secs(5))
            .await
            .expect("shutdown server");
        assert_eq!(shutdown_report.connections_drained, 0);
        assert_eq!(shutdown_report.connections_forced, 0);
        assert!(
            shutdown_report
                .instance_shutdown_report
                .clean_shutdown_persisted
        );

        if let Some(run_task) = run_task {
            run_task
                .await
                .expect("accept loop task should join")
                .expect("accept loop should exit cleanly");
        }

        let layout = InstanceLayout::new(dir.path());
        let meta_store: Arc<dyn MetadataStore> =
            Arc::new(FileMetadataStore::new(layout.meta_dir()).expect("open meta store"));
        let store = InstanceRunStateStore::with_store(meta_store);
        let run_state = store
            .load()
            .expect("load run_state")
            .expect("run_state should exist");
        assert_eq!(run_state.state, InstanceLifecycleState::Clean);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn server_shutdown_drains_registered_connections_before_clean_instance_shutdown() {
        let server = test_server_with_in_memory_instance();
        let drain_token = CancellationToken::new();
        let force_close_token = CancellationToken::new();
        let control = Arc::new(ServerConnectionControl::new(1, "127.0.0.1:15432"));
        let drain_waiter = drain_token.clone();
        let server_for_task = Arc::clone(&server);
        let join_handle = tokio::spawn(async move {
            let _force_close = force_close_token;
            drain_waiter.cancelled().await;
            server_for_task.unregister_connection(1);
        });

        server.register_connection(ServerConnectionHandle {
            connection_id: 1,
            control,
            join_handle,
            drain_token,
            force_close_token: CancellationToken::new(),
        });

        let shutdown_report = server
            .shutdown(Duration::from_millis(200))
            .await
            .expect("shutdown should succeed");
        assert_eq!(shutdown_report.connections_drained, 1);
        assert_eq!(shutdown_report.connections_forced, 0);
        assert_eq!(
            shutdown_report.instance_shutdown_report.disposition,
            InstanceShutdownDisposition::Clean
        );
        assert!(
            shutdown_report
                .instance_shutdown_report
                .clean_shutdown_persisted
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn server_shutdown_force_closes_stalled_connections_and_uses_dirty_instance_shutdown() {
        let server = test_server_with_in_memory_instance();
        let control = Arc::new(ServerConnectionControl::new(7, "127.0.0.1:15433"));
        let join_handle = tokio::spawn(async move {
            std::future::pending::<()>().await;
        });

        server.register_connection(ServerConnectionHandle {
            connection_id: 7,
            control,
            join_handle,
            drain_token: CancellationToken::new(),
            force_close_token: CancellationToken::new(),
        });

        let shutdown_report = server
            .shutdown(Duration::from_millis(20))
            .await
            .expect("shutdown should succeed");
        assert_eq!(shutdown_report.connections_drained, 0);
        assert_eq!(shutdown_report.connections_forced, 1);
        assert_eq!(
            shutdown_report.instance_shutdown_report.disposition,
            InstanceShutdownDisposition::Dirty
        );
        assert!(
            !shutdown_report
                .instance_shutdown_report
                .clean_shutdown_persisted
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn max_connections_pre_auth_rejects_extra_tcp_connections() {
        let (server, run_task, addr) = spawn_test_server(1).await;
        let _first = TcpStream::connect(addr).await.expect("first connection");

        let mut second = TcpStream::connect(addr).await.expect("second connection");
        let mut buf = [0_u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(2), second.read(&mut buf))
            .await
            .expect("second connection should resolve");
        assert_eq!(read.expect("read from second connection"), 0);

        server
            .shutdown(Duration::from_secs(5))
            .await
            .expect("shutdown server");
        run_task
            .await
            .expect("accept loop join")
            .expect("accept loop exit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_reports_sqlstate_53300_when_limit_is_exceeded_during_handshake() {
        let (server, run_task, addr) = spawn_test_server(1).await;
        let mut client = TcpStream::connect(addr).await.expect("client connection");

        let start = Instant::now();
        while server.connection_count() != 1 {
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "server should register accepted connection"
            );
            sleep(Duration::from_millis(10)).await;
        }

        // Simulate the accept/startup race where another connection becomes visible after
        // this socket was accepted but before its Startup packet is processed.
        server.limits.on_connection_registered();

        client
            .write_all(&encode_startup_packet("alice", "postgres"))
            .await
            .expect("write startup packet");
        let (tag, payload) = read_backend_message(&mut client).await;
        assert_eq!(tag, b'E');
        assert_eq!(error_field(&payload, b'C').as_deref(), Some("53300"));
        assert_eq!(
            error_field(&payload, b'M').as_deref(),
            Some("sorry, too many clients already")
        );

        server.limits.on_connection_unregistered();
        server
            .shutdown(Duration::from_secs(5))
            .await
            .expect("shutdown server");
        run_task
            .await
            .expect("accept loop join")
            .expect("accept loop exit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_advertises_parameter_statuses_expected_by_pg_clients() {
        let (server, run_task, addr) = spawn_test_server(4).await;
        let mut client = TcpStream::connect(addr).await.expect("client connection");

        let messages = complete_startup(&mut client).await;
        let parameter_statuses = messages
            .iter()
            .filter(|(tag, _)| *tag == b'S')
            .filter_map(|(_, payload)| parameter_status(payload))
            .collect::<std::collections::HashMap<_, _>>();

        assert_eq!(
            parameter_statuses
                .get("server_version_num")
                .map(String::as_str),
            Some("150000")
        );
        assert_eq!(
            parameter_statuses
                .get("standard_conforming_strings")
                .map(String::as_str),
            Some("on")
        );
        assert_eq!(
            parameter_statuses
                .get("application_name")
                .map(String::as_str),
            Some("")
        );
        assert!(messages
            .iter()
            .any(|(tag, payload)| { *tag == b'K' && backend_key_data(payload).is_some() }));
        assert!(messages.iter().any(|(tag, _)| *tag == b'Z'));

        server
            .shutdown(Duration::from_secs(5))
            .await
            .expect("shutdown server");
        run_task
            .await
            .expect("accept loop join")
            .expect("accept loop exit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancel_request_bypasses_pre_auth_limit_and_leaves_idle_target_usable() {
        let (server, run_task, addr) = spawn_test_server(1).await;
        let mut target = TcpStream::connect(addr).await.expect("target connection");
        let startup_messages = complete_startup(&mut target).await;
        let (pid, secret) = startup_messages
            .iter()
            .find(|(tag, _)| *tag == b'K')
            .and_then(|(_, payload)| backend_key_data(payload))
            .expect("startup should include backend key data");

        let mut cancel = TcpStream::connect(addr).await.expect("cancel connection");
        cancel
            .write_all(&encode_cancel_request(pid, secret))
            .await
            .expect("write cancel request");

        let mut eof = [0_u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(2), cancel.read(&mut eof))
            .await
            .expect("cancel connection should close quickly")
            .expect("read cancel EOF");
        assert_eq!(
            read, 0,
            "CancelRequest connections should close without a response"
        );

        let roundtrip = run_simple_query_roundtrip(&mut target, "SELECT 1").await;
        assert!(
            !roundtrip.iter().any(|(tag, _)| *tag == b'E'),
            "idle target should remain usable after CancelRequest"
        );
        assert_eq!(roundtrip.last().map(|(tag, _)| *tag), Some(b'Z'));

        server
            .shutdown(Duration::from_secs(5))
            .await
            .expect("shutdown server");
        run_task
            .await
            .expect("accept loop join")
            .expect("accept loop exit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn driver_style_startup_probe_queries_succeed() {
        let (server, run_task, addr) = spawn_test_server(4).await;
        let mut client = TcpStream::connect(addr).await.expect("client connection");
        complete_startup(&mut client).await;

        let probe_queries = [
            ("psql", "SELECT 1"),
            ("pgjdbc", "SELECT 1"),
            ("node-postgres", "SELECT 1"),
            ("asyncpg", "SELECT 1"),
        ];

        for (driver, sql) in probe_queries {
            let messages = run_simple_query_roundtrip(&mut client, sql).await;
            assert!(
                !messages.iter().any(|(tag, _)| *tag == b'E'),
                "{driver} probe should succeed: {sql}"
            );
            assert!(
                messages.iter().any(|(tag, _)| *tag == b'C'),
                "{driver} probe should emit CommandComplete"
            );
            assert_eq!(
                messages.last().map(|(tag, _)| *tag),
                Some(b'Z'),
                "{driver} probe should end with ReadyForQuery"
            );
        }

        server
            .shutdown(Duration::from_secs(5))
            .await
            .expect("shutdown server");
        run_task
            .await
            .expect("accept loop join")
            .expect("accept loop exit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn extended_query_flushes_before_waiting_for_more_frontend_input() {
        let (server, run_task, addr) = spawn_test_server(4).await;
        let mut client = TcpStream::connect(addr).await.expect("client connection");
        complete_startup(&mut client).await;

        client
            .write_all(&encode_frontend_message(PgWireFrontendMessage::Parse(
                Parse::new(
                    Some("standalone_parse".to_string()),
                    "SELECT 1".to_string(),
                    Vec::new(),
                ),
            )))
            .await
            .expect("write standalone Parse");
        let parse_complete = read_backend_message_timeout(&mut client, Duration::from_secs(2))
            .await
            .expect("ParseComplete must not wait for Sync or Flush");
        assert_eq!(parse_complete.0, b'1');

        client
            .write_all(&encode_frontend_message(PgWireFrontendMessage::Execute(
                Execute::new(Some("missing_portal".to_string()), 0),
            )))
            .await
            .expect("write invalid Execute");
        let error = read_backend_message_timeout(&mut client, Duration::from_secs(2))
            .await
            .expect("ErrorResponse must not wait for Sync or Flush");
        assert_eq!(error.0, b'E');

        client
            .write_all(&encode_frontend_message(PgWireFrontendMessage::Sync(
                Sync::new(),
            )))
            .await
            .expect("write recovery Sync");
        let recovered = read_messages_until_ready(&mut client).await;
        assert_eq!(recovered.last().map(|(tag, _)| *tag), Some(b'Z'));

        server
            .shutdown(Duration::from_secs(5))
            .await
            .expect("shutdown server");
        run_task
            .await
            .expect("accept loop join")
            .expect("accept loop exit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn extended_copy_pipeline_waits_for_sync_before_ready_for_query() {
        let (server, run_task, addr) = spawn_test_server(4).await;
        let mut client = TcpStream::connect(addr).await.expect("client connection");
        complete_startup(&mut client).await;
        run_simple_query_roundtrip(&mut client, "CREATE TABLE copy_pipeline_t (v INT)").await;

        for message in [
            PgWireFrontendMessage::Parse(Parse::new(
                Some("copy_stmt".to_string()),
                "COPY copy_pipeline_t FROM STDIN WITH (FORMAT csv)".to_string(),
                Vec::new(),
            )),
            PgWireFrontendMessage::Bind(Bind::new(
                Some("copy_portal".to_string()),
                Some("copy_stmt".to_string()),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )),
            PgWireFrontendMessage::Execute(Execute::new(Some("copy_portal".to_string()), 0)),
            PgWireFrontendMessage::CopyData(CopyData::new(Bytes::from_static(b"1\n2\n"))),
            PgWireFrontendMessage::CopyDone(CopyDone::new()),
            PgWireFrontendMessage::Parse(Parse::new(
                Some("select_stmt".to_string()),
                "SELECT v FROM copy_pipeline_t ORDER BY v".to_string(),
                Vec::new(),
            )),
            PgWireFrontendMessage::Bind(Bind::new(
                Some("select_portal".to_string()),
                Some("select_stmt".to_string()),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )),
            PgWireFrontendMessage::Execute(Execute::new(Some("select_portal".to_string()), 0)),
            PgWireFrontendMessage::Flush(Flush::new()),
        ] {
            client
                .write_all(&encode_frontend_message(message))
                .await
                .expect("write frontend message");
        }

        let before_sync = read_available_messages(&mut client).await;
        let before_sync_summary = before_sync
            .iter()
            .map(|(tag, payload)| match *tag {
                b'C' => format!("C:{}", command_complete_tag(payload).unwrap_or_default()),
                other => (other as char).to_string(),
            })
            .collect::<Vec<_>>();
        assert!(before_sync.iter().any(|(tag, _)| *tag == b'G'));
        assert!(
            before_sync.iter().any(|(tag, payload)| {
                *tag == b'C' && command_complete_tag(payload).as_deref() == Some("COPY 2")
            }),
            "COPY completion should be emitted before Sync"
        );
        assert!(
            before_sync.iter().any(|(tag, payload)| {
                *tag == b'C'
                    && command_complete_tag(payload)
                        .map(|tag| tag.starts_with("SELECT "))
                        .unwrap_or(false)
            }),
            "follow-up Execute should complete before Sync: {:?}",
            before_sync_summary
        );
        assert!(
            !before_sync.iter().any(|(tag, _)| *tag == b'Z'),
            "ReadyForQuery must wait for Sync"
        );

        client
            .write_all(&encode_frontend_message(PgWireFrontendMessage::Sync(
                Sync::new(),
            )))
            .await
            .expect("write sync");
        let after_sync = read_messages_until_ready(&mut client).await;
        assert_eq!(after_sync.last().map(|(tag, _)| *tag), Some(b'Z'));

        server
            .shutdown(Duration::from_secs(5))
            .await
            .expect("shutdown server");
        run_task
            .await
            .expect("accept loop join")
            .expect("accept loop exit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn extended_copy_copyfail_skips_until_sync_and_ignores_followup_copydata() {
        let (server, run_task, addr) = spawn_test_server(4).await;
        let mut client = TcpStream::connect(addr).await.expect("client connection");
        complete_startup(&mut client).await;
        run_simple_query_roundtrip(&mut client, "CREATE TABLE copy_fail_t (v INT)").await;

        for message in [
            PgWireFrontendMessage::Parse(Parse::new(
                Some("copy_stmt".to_string()),
                "COPY copy_fail_t FROM STDIN WITH (FORMAT csv)".to_string(),
                Vec::new(),
            )),
            PgWireFrontendMessage::Bind(Bind::new(
                Some("copy_portal".to_string()),
                Some("copy_stmt".to_string()),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )),
            PgWireFrontendMessage::Execute(Execute::new(Some("copy_portal".to_string()), 0)),
        ] {
            client
                .write_all(&encode_frontend_message(message))
                .await
                .expect("write copy setup message");
        }
        let setup_messages = read_available_messages(&mut client).await;
        assert!(setup_messages.iter().any(|(tag, _)| *tag == b'G'));

        client
            .write_all(&encode_copy_fail_message("client aborted"))
            .await
            .expect("write copy fail");
        for message in [
            PgWireFrontendMessage::CopyData(CopyData::new(Bytes::from_static(b"9\n"))),
            PgWireFrontendMessage::Sync(Sync::new()),
        ] {
            client
                .write_all(&encode_frontend_message(message))
                .await
                .expect("write copy fail sequence");
        }

        let messages = read_messages_until_ready(&mut client).await;
        assert_eq!(
            messages.iter().filter(|(tag, _)| *tag == b'E').count(),
            1,
            "COPY fail should surface one ErrorResponse"
        );
        assert!(
            !messages.iter().any(|(tag, payload)| {
                *tag == b'C' && command_complete_tag(payload).as_deref() == Some("COPY 1")
            }),
            "ignored CopyData must not produce a COPY completion"
        );

        let probe =
            run_simple_query_roundtrip(&mut client, "SELECT COUNT(*) FROM copy_fail_t").await;
        assert!(
            !probe.iter().any(|(tag, _)| *tag == b'E'),
            "connection should recover after Sync"
        );

        server
            .shutdown(Duration::from_secs(5))
            .await
            .expect("shutdown server");
        run_task
            .await
            .expect("accept loop join")
            .expect("accept loop exit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn extended_copy_protocol_errors_skip_until_sync_and_ignore_followup_copydata() {
        let (server, run_task, addr) = spawn_test_server(4).await;
        let mut client = TcpStream::connect(addr).await.expect("client connection");
        complete_startup(&mut client).await;
        run_simple_query_roundtrip(&mut client, "CREATE TABLE copy_skip_t (v INT)").await;

        for message in [
            PgWireFrontendMessage::Parse(Parse::new(
                Some("copy_stmt".to_string()),
                "COPY copy_skip_t FROM STDIN WITH (FORMAT csv)".to_string(),
                Vec::new(),
            )),
            PgWireFrontendMessage::Bind(Bind::new(
                Some("copy_portal".to_string()),
                Some("copy_stmt".to_string()),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )),
            PgWireFrontendMessage::Execute(Execute::new(Some("copy_portal".to_string()), 0)),
        ] {
            client
                .write_all(&encode_frontend_message(message))
                .await
                .expect("write copy setup message");
        }
        let setup_messages = read_available_messages(&mut client).await;
        assert!(setup_messages.iter().any(|(tag, _)| *tag == b'G'));

        for message in [
            PgWireFrontendMessage::Parse(Parse::new(
                Some("oops".to_string()),
                "SELECT 1".to_string(),
                Vec::new(),
            )),
            PgWireFrontendMessage::CopyData(CopyData::new(Bytes::from_static(b"7\n"))),
            PgWireFrontendMessage::Sync(Sync::new()),
        ] {
            client
                .write_all(&encode_frontend_message(message))
                .await
                .expect("write protocol violation sequence");
        }

        let messages = read_messages_until_ready(&mut client).await;
        assert_eq!(
            messages.iter().filter(|(tag, _)| *tag == b'E').count(),
            1,
            "protocol violation should surface one ErrorResponse"
        );
        assert!(
            !messages.iter().any(|(tag, _)| *tag == b'1'),
            "ParseComplete from the skipped message must not be sent"
        );

        let probe =
            run_simple_query_roundtrip(&mut client, "SELECT COUNT(*) FROM copy_skip_t").await;
        assert!(
            !probe.iter().any(|(tag, _)| *tag == b'E'),
            "connection should recover after Sync"
        );

        server
            .shutdown(Duration::from_secs(5))
            .await
            .expect("shutdown server");
        run_task
            .await
            .expect("accept loop join")
            .expect("accept loop exit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn simple_query_cancel_skips_remaining_statements_in_batch() {
        let (server, run_task, addr) = spawn_test_server(4).await;
        let mut client = TcpStream::connect(addr).await.expect("client connection");
        let startup_messages = complete_startup(&mut client).await;
        let (pid, secret) = startup_messages
            .iter()
            .find(|(tag, _)| *tag == b'K')
            .and_then(|(_, payload)| backend_key_data(payload))
            .expect("startup should include backend key data");

        run_simple_query_roundtrip(&mut client, "CREATE TABLE simple_copy_cancel_t (v INT)").await;

        client
            .write_all(&encode_frontend_message(PgWireFrontendMessage::Query(
                Query::new(
                    "COPY simple_copy_cancel_t FROM STDIN WITH (FORMAT csv); SELECT 42".to_string(),
                ),
            )))
            .await
            .expect("write simple COPY batch");

        let setup_messages = read_available_messages(&mut client).await;
        assert!(setup_messages.iter().any(|(tag, _)| *tag == b'G'));
        assert!(!setup_messages.iter().any(|(tag, _)| *tag == b'Z'));

        let mut cancel = TcpStream::connect(addr).await.expect("cancel connection");
        cancel
            .write_all(&encode_cancel_request(pid, secret))
            .await
            .expect("write cancel request");
        let mut eof = [0_u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(2), cancel.read(&mut eof))
            .await
            .expect("cancel connection should close")
            .expect("read cancel EOF");
        assert_eq!(read, 0);

        client
            .write_all(&encode_frontend_message(PgWireFrontendMessage::CopyDone(
                CopyDone::new(),
            )))
            .await
            .expect("write copy done");

        let messages = read_messages_until_ready(&mut client).await;
        assert_eq!(
            messages
                .iter()
                .filter(|(tag, payload)| {
                    *tag == b'E' && error_field(payload, b'C').as_deref() == Some("57014")
                })
                .count(),
            1,
            "cancelled batch should surface one query_canceled error"
        );
        assert!(
            !messages.iter().any(|(tag, payload)| {
                *tag == b'C'
                    && command_complete_tag(payload)
                        .map(|tag| tag == "COPY 0" || tag.starts_with("SELECT "))
                        .unwrap_or(false)
            }),
            "remaining simple-query statements must be skipped after cancel"
        );
        assert_eq!(
            messages
                .last()
                .and_then(|(_, payload)| ready_for_query_status(payload)),
            Some('I')
        );

        let probe =
            run_simple_query_roundtrip(&mut client, "SELECT COUNT(*) FROM simple_copy_cancel_t")
                .await;
        assert!(
            probe.iter().any(|(tag, payload)| {
                *tag == b'D'
                    || (*tag == b'C'
                        && command_complete_tag(payload).as_deref() == Some("SELECT 1"))
            }),
            "connection should remain usable after cancel"
        );

        server
            .shutdown(Duration::from_secs(5))
            .await
            .expect("shutdown server");
        run_task
            .await
            .expect("accept loop join")
            .expect("accept loop exit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn extended_query_cancel_marks_explicit_transaction_failed_until_sync() {
        let (server, run_task, addr) = spawn_test_server(4).await;
        let mut client = TcpStream::connect(addr).await.expect("client connection");
        let startup_messages = complete_startup(&mut client).await;
        let (pid, secret) = startup_messages
            .iter()
            .find(|(tag, _)| *tag == b'K')
            .and_then(|(_, payload)| backend_key_data(payload))
            .expect("startup should include backend key data");

        run_simple_query_roundtrip(&mut client, "CREATE TABLE ext_cancel_t (v INT)").await;
        let begin_messages = run_simple_query_roundtrip(&mut client, "BEGIN").await;
        assert_eq!(
            begin_messages
                .last()
                .and_then(|(_, payload)| ready_for_query_status(payload)),
            Some('T')
        );

        for message in [
            PgWireFrontendMessage::Parse(Parse::new(
                Some("copy_stmt".to_string()),
                "COPY ext_cancel_t FROM STDIN WITH (FORMAT csv)".to_string(),
                Vec::new(),
            )),
            PgWireFrontendMessage::Bind(Bind::new(
                Some("copy_portal".to_string()),
                Some("copy_stmt".to_string()),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )),
            PgWireFrontendMessage::Execute(Execute::new(Some("copy_portal".to_string()), 0)),
        ] {
            client
                .write_all(&encode_frontend_message(message))
                .await
                .expect("write extended COPY setup");
        }

        let setup_messages = read_available_messages(&mut client).await;
        assert!(setup_messages.iter().any(|(tag, _)| *tag == b'G'));

        let mut cancel = TcpStream::connect(addr).await.expect("cancel connection");
        cancel
            .write_all(&encode_cancel_request(pid, secret))
            .await
            .expect("write cancel request");
        let mut eof = [0_u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(2), cancel.read(&mut eof))
            .await
            .expect("cancel connection should close")
            .expect("read cancel EOF");
        assert_eq!(read, 0);

        for message in [
            PgWireFrontendMessage::CopyDone(CopyDone::new()),
            PgWireFrontendMessage::Sync(Sync::new()),
        ] {
            client
                .write_all(&encode_frontend_message(message))
                .await
                .expect("write cancel recovery sequence");
        }

        let messages = read_messages_until_ready(&mut client).await;
        assert_eq!(
            messages
                .iter()
                .filter(|(tag, payload)| {
                    *tag == b'E' && error_field(payload, b'C').as_deref() == Some("57014")
                })
                .count(),
            1
        );
        assert_eq!(
            messages
                .last()
                .and_then(|(_, payload)| ready_for_query_status(payload)),
            Some('E'),
            "explicit transaction should remain aborted after cancel until Sync"
        );

        let aborted = run_simple_query_roundtrip(&mut client, "SELECT 1").await;
        assert_eq!(
            aborted
                .iter()
                .filter(|(tag, payload)| {
                    *tag == b'E' && error_field(payload, b'C').as_deref() == Some("25P02")
                })
                .count(),
            1,
            "statements inside the failed transaction should return transaction_aborted"
        );
        assert_eq!(
            aborted
                .last()
                .and_then(|(_, payload)| ready_for_query_status(payload)),
            Some('E')
        );

        let rollback_messages = run_simple_query_roundtrip(&mut client, "ROLLBACK").await;
        assert_eq!(
            rollback_messages
                .last()
                .and_then(|(_, payload)| ready_for_query_status(payload)),
            Some('I')
        );

        server
            .shutdown(Duration::from_secs(5))
            .await
            .expect("shutdown server");
        run_task
            .await
            .expect("accept loop join")
            .expect("accept loop exit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_to_stdout_cancel_returns_query_canceled_and_recovers_connection() {
        let (server, run_task, addr) = spawn_test_server(4).await;
        let mut client = TcpStream::connect(addr).await.expect("client connection");
        let startup_messages = complete_startup(&mut client).await;
        let (pid, secret) = startup_messages
            .iter()
            .find(|(tag, _)| *tag == b'K')
            .and_then(|(_, payload)| backend_key_data(payload))
            .expect("startup should include backend key data");

        client
            .write_all(&encode_frontend_message(PgWireFrontendMessage::Query(
                // A large streaming source cannot finish into an ordinary TCP
                // send buffer before the cancel connection is opened. Unlike
                // a materialized test table, it also has constant-time cleanup.
                Query::new(
                    "COPY (SELECT * FROM range(0, 1000000000)) TO STDOUT WITH (FORMAT csv)"
                        .to_string(),
                ),
            )))
            .await
            .expect("write copy out query");

        let mut saw_copy_out_response = false;
        loop {
            let (tag, _payload) = read_backend_message(&mut client).await;
            match tag {
                b'H' => saw_copy_out_response = true,
                b'd' => break,
                other => panic!(
                    "expected CopyOutResponse/CopyData before cancel, saw tag {}",
                    other as char
                ),
            }
        }

        assert!(
            saw_copy_out_response,
            "COPY TO STDOUT should enter copy-out mode"
        );

        let mut cancel = TcpStream::connect(addr).await.expect("cancel connection");
        cancel
            .write_all(&encode_cancel_request(pid, secret))
            .await
            .expect("write cancel request");
        let mut eof = [0_u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(2), cancel.read(&mut eof))
            .await
            .expect("cancel connection should close")
            .expect("read cancel EOF");
        assert_eq!(read, 0);

        let messages = read_messages_until_ready(&mut client).await;
        assert_eq!(
            messages
                .iter()
                .filter(|(tag, payload)| {
                    *tag == b'E' && error_field(payload, b'C').as_deref() == Some("57014")
                })
                .count(),
            1,
            "cancelled COPY TO STDOUT should surface one query_canceled error"
        );
        assert!(
            !messages.iter().any(|(tag, payload)| {
                *tag == b'C'
                    && command_complete_tag(payload)
                        .map(|tag| tag.starts_with("COPY "))
                        .unwrap_or(false)
            }),
            "cancelled COPY TO STDOUT must not emit a COPY command completion"
        );
        assert_eq!(
            messages
                .last()
                .and_then(|(_, payload)| ready_for_query_status(payload)),
            Some('I')
        );

        let probe = run_simple_query_roundtrip(&mut client, "SELECT 1").await;
        assert!(
            !probe.iter().any(|(tag, _)| *tag == b'E'),
            "connection should remain usable after COPY TO STDOUT cancel"
        );

        server
            .shutdown(Duration::from_secs(5))
            .await
            .expect("shutdown server");
        run_task
            .await
            .expect("accept loop join")
            .expect("accept loop exit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_to_stdout_batches_rows_into_bounded_frames() {
        let (server, run_task, addr) = spawn_test_server(4).await;
        let mut client = TcpStream::connect(addr).await.expect("client connection");
        complete_startup(&mut client).await;

        let messages = run_simple_query_roundtrip(
            &mut client,
            "COPY (SELECT * FROM range(0, 10000)) TO STDOUT WITH (FORMAT csv)",
        )
        .await;
        let copy_frames = messages.iter().filter(|(tag, _)| *tag == b'd').count();
        assert!(
            (1..10).contains(&copy_frames),
            "10k rows should be batched into a small number of COPY frames, saw {copy_frames}"
        );
        assert!(messages.iter().any(|(tag, payload)| {
            *tag == b'C' && command_complete_tag(payload).is_some_and(|tag| tag == "COPY 10000")
        }));

        server
            .shutdown(Duration::from_secs(5))
            .await
            .expect("shutdown server");
        run_task
            .await
            .expect("accept loop join")
            .expect("accept loop exit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_to_stdout_vector_encoding_preserves_csv_bytes() {
        let (server, run_task, addr) = spawn_test_server(4).await;
        let mut client = TcpStream::connect(addr).await.expect("client connection");
        complete_startup(&mut client).await;
        client
            .write_all(&encode_frontend_message(PgWireFrontendMessage::Query(
                Query::new(
                    "COPY (SELECT CAST(42 AS BIGINT) AS id, 'a,b\"c' AS text, CAST(NULL AS VARCHAR) AS missing) TO STDOUT WITH (FORMAT csv, HEADER true)"
                        .to_string(),
                ),
            )))
            .await
            .expect("write COPY query");

        let mut csv = Vec::new();
        loop {
            let (tag, payload) = read_backend_message(&mut client).await;
            if tag == b'd' {
                csv.extend_from_slice(&payload);
            }
            if tag == b'Z' {
                break;
            }
        }
        assert_eq!(
            csv, b"id,text,missing\n42,\"a,b\"\"c\",\n",
            "direct vector encoders must preserve PostgreSQL CSV semantics"
        );

        server
            .shutdown(Duration::from_secs(5))
            .await
            .expect("shutdown server");
        run_task
            .await
            .expect("accept loop join")
            .expect("accept loop exit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drain_allows_copy_to_stdout_to_finish_before_closing_connection() {
        let (server, run_task, addr) = spawn_test_server(4).await;
        let mut client = TcpStream::connect(addr).await.expect("client connection");
        complete_startup(&mut client).await;
        client
            .write_all(&encode_frontend_message(PgWireFrontendMessage::Query(
                Query::new(
                    "COPY (SELECT * FROM range(0, 100000)) TO STDOUT WITH (FORMAT csv)".to_string(),
                ),
            )))
            .await
            .expect("write COPY query");
        loop {
            let (tag, _) = read_backend_message(&mut client).await;
            if tag == b'd' {
                break;
            }
            assert_eq!(tag, b'H');
        }

        server.broadcast_drain();
        let messages = read_messages_until_ready(&mut client).await;
        assert!(messages.iter().any(|(tag, payload)| {
            *tag == b'C' && command_complete_tag(payload).is_some_and(|tag| tag == "COPY 100000")
        }));
        assert_eq!(
            messages
                .last()
                .and_then(|(_, payload)| ready_for_query_status(payload)),
            Some('I')
        );
        let mut eof = [0_u8; 1];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), client.read(&mut eof))
                .await
                .expect("drained connection should close after COPY completion")
                .expect("read drained connection"),
            0
        );

        server
            .shutdown(Duration::from_secs(5))
            .await
            .expect("shutdown server");
        run_task
            .await
            .expect("accept loop join")
            .expect("accept loop exit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn statement_timeout_cancels_copy_to_stdout_and_recovers_connection() {
        let (server, run_task, addr) = spawn_test_server(4).await;
        let mut client = TcpStream::connect(addr).await.expect("client connection");
        complete_startup(&mut client).await;
        run_simple_query_roundtrip(&mut client, "SET statement_timeout = 50").await;

        client
            .write_all(&encode_frontend_message(PgWireFrontendMessage::Query(
                Query::new(
                    "COPY (SELECT * FROM range(0, 1000000000)) TO STDOUT WITH (FORMAT csv)"
                        .to_string(),
                ),
            )))
            .await
            .expect("write COPY query");

        let messages = read_messages_until_ready(&mut client).await;
        assert_eq!(
            messages
                .iter()
                .filter(|(tag, payload)| {
                    *tag == b'E'
                        && error_field(payload, b'M').as_deref()
                            == Some("canceling statement due to statement timeout")
                })
                .count(),
            1
        );
        assert!(!messages.iter().any(|(tag, payload)| {
            *tag == b'C'
                && command_complete_tag(payload).is_some_and(|tag| tag.starts_with("COPY "))
        }));
        assert_eq!(
            messages
                .last()
                .and_then(|(_, payload)| ready_for_query_status(payload)),
            Some('I')
        );

        let probe = run_simple_query_roundtrip(&mut client, "SELECT 1").await;
        assert!(!probe.iter().any(|(tag, _)| *tag == b'E'));

        server
            .shutdown(Duration::from_secs(5))
            .await
            .expect("shutdown server");
        run_task
            .await
            .expect("accept loop join")
            .expect("accept loop exit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn statement_timeout_cancels_copy_from_stdin_and_recovers_connection() {
        let (server, run_task, addr) = spawn_test_server(4).await;
        let mut client = TcpStream::connect(addr).await.expect("client connection");
        complete_startup(&mut client).await;

        run_simple_query_roundtrip(&mut client, "CREATE TABLE timeout_copy_t (v INT)").await;
        run_simple_query_roundtrip(&mut client, "SET statement_timeout = 50").await;

        client
            .write_all(&encode_frontend_message(PgWireFrontendMessage::Query(
                Query::new("COPY timeout_copy_t FROM STDIN WITH (FORMAT csv)".to_string()),
            )))
            .await
            .expect("write COPY query");

        let setup_messages = read_available_messages(&mut client).await;
        assert!(setup_messages.iter().any(|(tag, _)| *tag == b'G'));

        sleep(Duration::from_millis(120)).await;
        client
            .write_all(&encode_frontend_message(PgWireFrontendMessage::CopyDone(
                CopyDone::new(),
            )))
            .await
            .expect("write copy done after timeout");

        let messages = read_messages_until_ready(&mut client).await;
        assert_eq!(
            messages
                .iter()
                .filter(|(tag, payload)| {
                    *tag == b'E'
                        && error_field(payload, b'M').as_deref()
                            == Some("canceling statement due to statement timeout")
                })
                .count(),
            1,
            "statement_timeout should surface its dedicated error message"
        );
        assert_eq!(
            messages
                .last()
                .and_then(|(_, payload)| ready_for_query_status(payload)),
            Some('I')
        );

        let probe = run_simple_query_roundtrip(&mut client, "SELECT 1").await;
        assert!(
            !probe.iter().any(|(tag, _)| *tag == b'E'),
            "connection should stay usable after statement timeout"
        );

        server
            .shutdown(Duration::from_secs(5))
            .await
            .expect("shutdown server");
        run_task
            .await
            .expect("accept loop join")
            .expect("accept loop exit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn force_close_does_not_leak_terminal_query_error_or_ready_for_query() {
        let (server, run_task, addr) = spawn_test_server(4).await;
        let mut client = TcpStream::connect(addr).await.expect("client connection");
        complete_startup(&mut client).await;

        run_simple_query_roundtrip(&mut client, "CREATE TABLE force_close_t (v INT)").await;
        client
            .write_all(&encode_frontend_message(PgWireFrontendMessage::Query(
                Query::new("COPY force_close_t FROM STDIN WITH (FORMAT csv)".to_string()),
            )))
            .await
            .expect("write COPY query");

        let setup_messages = read_available_messages(&mut client).await;
        assert!(setup_messages.iter().any(|(tag, _)| *tag == b'G'));

        server.broadcast_force_close();

        let mut buf = [0_u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .expect("force-closed connection should resolve read")
            .expect("read after force close");
        assert_eq!(
            read, 0,
            "force close should terminate the socket without protocol epilogue"
        );

        server
            .shutdown(Duration::from_secs(5))
            .await
            .expect("shutdown server");
        run_task
            .await
            .expect("accept loop join")
            .expect("accept loop exit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn force_close_interrupts_backpressured_copy_to_stdout() {
        let (server, run_task, addr) = spawn_test_server(4).await;
        let mut client = TcpStream::connect(addr).await.expect("client connection");
        complete_startup(&mut client).await;

        client
            .write_all(&encode_frontend_message(PgWireFrontendMessage::Query(
                Query::new(
                    "COPY (SELECT * FROM range(0, 1000000000)) TO STDOUT WITH (FORMAT csv)"
                        .to_string(),
                ),
            )))
            .await
            .expect("write COPY query");
        loop {
            let (tag, _) = read_backend_message(&mut client).await;
            if tag == b'd' {
                break;
            }
            assert_eq!(tag, b'H');
        }

        // Stop consuming rows long enough for the server-side socket to apply
        // backpressure, then verify the lifecycle token interrupts that send.
        sleep(Duration::from_millis(50)).await;
        server.broadcast_force_close();
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut buffer = [0_u8; 64 * 1024];
            loop {
                if client
                    .read(&mut buffer)
                    .await
                    .expect("read after force close")
                    == 0
                {
                    break;
                }
            }
        })
        .await
        .expect("backpressured COPY TO STDOUT should release the connection promptly");

        server
            .shutdown(Duration::from_secs(5))
            .await
            .expect("shutdown server");
        run_task
            .await
            .expect("accept loop join")
            .expect("accept loop exit");
    }
}
