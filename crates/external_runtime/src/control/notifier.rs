// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use paro_routine::TransportKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NotifierKind {
    EventFd,
    IoUring,
    SharedRing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlNotifier {
    pub kind: NotifierKind,
    pub label: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotifierAvailability {
    pub io_uring_available: bool,
    pub eventfd_available: bool,
    pub shared_ring_available: bool,
}

impl Default for NotifierAvailability {
    fn default() -> Self {
        Self {
            io_uring_available: cfg!(target_os = "linux"),
            eventfd_available: cfg!(target_os = "linux"),
            shared_ring_available: true,
        }
    }
}

impl ControlNotifier {
    pub fn choose(transport: TransportKind, availability: &NotifierAvailability) -> Option<Self> {
        match transport {
            TransportKind::LocalIoUring if availability.io_uring_available => Some(Self {
                kind: NotifierKind::IoUring,
                label: "local-io-uring",
            }),
            TransportKind::LocalShm if availability.shared_ring_available => Some(Self {
                kind: NotifierKind::SharedRing,
                label: "local-shared-ring",
            }),
            TransportKind::LocalShm if availability.eventfd_available => Some(Self {
                kind: NotifierKind::EventFd,
                label: "local-eventfd",
            }),
            TransportKind::Remote => None,
            _ => None,
        }
    }
}
