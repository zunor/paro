// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_scheduler::coordinator::EventCoordinator;
use std::sync::Arc;

/// Session-layer hook used to publish the currently active coordinator without
/// introducing a dependency cycle from execution back to session.
pub trait StatementExecutionTracker: Send + Sync {
    fn bind_coordinator(&self, coordinator: Arc<EventCoordinator>);
}
