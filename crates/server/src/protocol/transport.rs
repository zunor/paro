// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! The connection-owned backend transport boundary.
//!
//! `Framed` does not expose a callback when a particular encoded segment is
//! written.  The codec therefore owns diagnostic reservations until the whole
//! write buffer is empty.  All backend writes and flushes go through this
//! module so that the observation cannot be accidentally omitted by a protocol
//! adapter.

use futures::SinkExt;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use crate::connection::PgCodec;

pub(crate) type BackendTransport = Framed<TcpStream, PgCodec>;

/// The grace interval used when a cancelled statement has already queued
/// protocol bytes.  Progress during an interval renews the grace period; a
/// full interval without progress closes the connection instead of retaining
/// a cancelled request and its output forever.
pub(crate) const CANCELLED_OUTPUT_STALL_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainOutcome {
    Drained,
    ForceClosed,
    Stalled,
}

/// Observe the aggregate `Framed` write buffer after one transport operation.
///
/// The codec deliberately releases all pending owners only when this buffer
/// is empty.  This is conservative for mixed protocol output and keeps the
/// byte stream valid without per-frame offset bookkeeping.
pub(crate) fn observe_pending_output(socket: &mut BackendTransport) {
    let buffered_bytes = socket.write_buffer().len();
    socket.codec_mut().observe_pending_output(buffered_bytes);
}

pub(crate) async fn feed(
    socket: &mut BackendTransport,
    message: pgwire::messages::PgWireBackendMessage,
) -> std::io::Result<()> {
    let result = socket.feed(message).await;
    observe_pending_output(socket);
    result
}

pub(crate) async fn send(
    socket: &mut BackendTransport,
    message: pgwire::messages::PgWireBackendMessage,
) -> std::io::Result<()> {
    let result = socket.send(message).await;
    observe_pending_output(socket);
    result
}

pub(crate) async fn flush(socket: &mut BackendTransport) -> std::io::Result<()> {
    let result = socket.flush().await;
    observe_pending_output(socket);
    result
}

/// Flush pending backend bytes while observing force-close and no-progress.
///
/// A reduction in the buffered byte count renews the stall grace interval.
/// If the peer stops consuming bytes, or the connection is force-closed, the
/// caller must drop the `Framed` connection; no second unbounded flush is
/// attempted.  Errors are returned separately because they are transport
/// failures, not a normal cancellation outcome.
pub(crate) async fn drain_pending_output(
    socket: &mut BackendTransport,
    force_close_token: &CancellationToken,
    stalled_write_timeout: Duration,
) -> Result<DrainOutcome, std::io::Error> {
    let mut remaining = socket.write_buffer().len();
    if remaining == 0 {
        observe_pending_output(socket);
        return Ok(DrainOutcome::Drained);
    }

    loop {
        let outcome = tokio::select! {
            biased;
            _ = force_close_token.cancelled() => {
                DrainOutcome::ForceClosed
            }
            result = tokio::time::timeout(stalled_write_timeout, socket.flush()) => {
                match result {
                    Ok(Ok(())) => {
                        let current = socket.write_buffer().len();
                        if current == 0 {
                            DrainOutcome::Drained
                        } else if current < remaining {
                            remaining = current;
                            observe_pending_output(socket);
                            continue;
                        } else {
                            DrainOutcome::Stalled
                        }
                    }
                    Ok(Err(error)) => {
                        observe_pending_output(socket);
                        return Err(error);
                    }
                    Err(_) => {
                        let current = socket.write_buffer().len();
                        if current == 0 {
                            observe_pending_output(socket);
                            return Ok(DrainOutcome::Drained);
                        } else if current < remaining {
                            remaining = current;
                            observe_pending_output(socket);
                            continue;
                        }
                        DrainOutcome::Stalled
                    }
                }
            }
        };

        observe_pending_output(socket);
        return Ok(outcome);
    }
}
