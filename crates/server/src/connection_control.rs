// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Server-owned connection control-plane state and limit tracking.

use paro_instance::{ConnectionId, ManagedConnection};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub(crate) struct ServerLimits {
    max_connections: usize,
    startup_timeout: Duration,
    tracked_connections: AtomicUsize,
}

impl ServerLimits {
    pub(crate) fn new(max_connections: usize, startup_timeout: Duration) -> Self {
        Self {
            max_connections,
            startup_timeout,
            tracked_connections: AtomicUsize::new(0),
        }
    }

    pub(crate) fn startup_timeout(&self) -> Duration {
        self.startup_timeout
    }

    pub(crate) fn max_connections(&self) -> usize {
        self.max_connections
    }

    pub(crate) fn pre_auth_limit_reached(&self) -> bool {
        self.max_connections != 0 && self.tracked_connections() >= self.max_connections
    }

    pub(crate) fn post_startup_limit_reached(&self) -> bool {
        self.max_connections != 0 && self.tracked_connections() > self.max_connections
    }

    pub(crate) fn tracked_connections(&self) -> usize {
        self.tracked_connections.load(Ordering::Acquire)
    }

    pub(crate) fn on_connection_registered(&self) {
        self.tracked_connections.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn on_connection_unregistered(&self) {
        let _ =
            self.tracked_connections
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    Some(count.saturating_sub(1))
                });
    }

    pub(crate) fn reset_tracked_connections(&self) {
        self.tracked_connections.store(0, Ordering::Release);
    }
}

#[derive(Debug)]
pub(crate) struct ServerConnectionControl {
    connection_id: ConnectionId,
    peer_addr: String,
    active: AtomicBool,
    draining: AtomicBool,
    force_close_requested: AtomicBool,
    handshake_complete: AtomicBool,
    in_flight: AtomicBool,
}

impl ServerConnectionControl {
    pub(crate) fn new(connection_id: ConnectionId, peer_addr: impl Into<String>) -> Self {
        Self {
            connection_id,
            peer_addr: peer_addr.into(),
            active: AtomicBool::new(true),
            draining: AtomicBool::new(false),
            force_close_requested: AtomicBool::new(false),
            handshake_complete: AtomicBool::new(false),
            in_flight: AtomicBool::new(false),
        }
    }

    pub(crate) fn mark_handshake_complete(&self) {
        self.handshake_complete.store(true, Ordering::Release);
    }

    pub(crate) fn set_in_flight(&self, in_flight: bool) {
        self.in_flight.store(in_flight, Ordering::Release);
    }

    pub(crate) fn request_drain(&self) {
        self.draining.store(true, Ordering::Release);
    }

    pub(crate) fn request_force_close(&self) {
        self.force_close_requested.store(true, Ordering::Release);
        self.request_drain();
    }

    pub(crate) fn deactivate(&self) {
        self.active.store(false, Ordering::Release);
        self.in_flight.store(false, Ordering::Release);
    }
}

impl ManagedConnection for ServerConnectionControl {
    fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    fn description(&self) -> String {
        format!(
            "client_connection(id={}, peer_addr={}, handshake_complete={}, draining={}, force_close_requested={}, in_flight={})",
            self.connection_id,
            self.peer_addr,
            self.handshake_complete.load(Ordering::Acquire),
            self.draining.load(Ordering::Acquire),
            self.force_close_requested.load(Ordering::Acquire),
            self.in_flight.load(Ordering::Acquire),
        )
    }
}

#[derive(Debug)]
pub(crate) struct ServerConnectionHandle {
    pub(crate) connection_id: ConnectionId,
    pub(crate) control: Arc<ServerConnectionControl>,
    pub(crate) join_handle: JoinHandle<()>,
    pub(crate) drain_token: CancellationToken,
    pub(crate) force_close_token: CancellationToken,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn description_tracks_shutdown_state_transitions() {
        let control = ServerConnectionControl::new(7, "127.0.0.1:5432");
        assert!(control.is_active());
        assert!(
            control.description().contains("handshake_complete=false"),
            "control should start before handshake completion"
        );

        control.mark_handshake_complete();
        control.set_in_flight(true);
        control.request_force_close();
        let description = control.description();
        assert!(
            description.contains("handshake_complete=true"),
            "description should surface handshake completion"
        );
        assert!(
            description.contains("draining=true"),
            "request_force_close should also mark the connection draining"
        );
        assert!(
            description.contains("force_close_requested=true"),
            "description should surface force-close state"
        );
        assert!(
            description.contains("in_flight=true"),
            "description should surface in-flight work state"
        );

        control.deactivate();
        assert!(
            !control.is_active(),
            "deactivate should mark the control inactive for ConnectionManager cleanup"
        );
    }

    #[test]
    fn server_limits_use_two_stage_connection_checks() {
        let limits = ServerLimits::new(2, Duration::from_secs(30));
        assert!(!limits.pre_auth_limit_reached());
        limits.on_connection_registered();
        assert!(!limits.pre_auth_limit_reached());
        limits.on_connection_registered();
        assert!(limits.pre_auth_limit_reached());
        assert!(!limits.post_startup_limit_reached());
        limits.on_connection_registered();
        assert!(limits.post_startup_limit_reached());
        limits.on_connection_unregistered();
        assert!(!limits.post_startup_limit_reached());
    }
}
