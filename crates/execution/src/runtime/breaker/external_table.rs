// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Breaker handle for cardinality-changing external table routines.

use std::sync::Arc;

use paro_common::error::Result;

use crate::operators::external::table_state::{ExternalTableFlowControl, ExternalTableSharedState};
use crate::runtime::context::OperatorCleanupContext;

use super::cleanup::{CleanupReason, CleanupState, CleanupStatus, RuntimeCleanup};
use super::registry::BreakerHandleMetadata;

#[derive(Debug)]
pub struct ExternalTableHandle {
    metadata: BreakerHandleMetadata,
    shared: Arc<ExternalTableSharedState>,
    cleanup: CleanupState,
}

impl ExternalTableHandle {
    pub fn new(metadata: BreakerHandleMetadata) -> Self {
        Self {
            metadata,
            shared: Arc::new(ExternalTableSharedState::new(
                ExternalTableFlowControl::default(),
            )),
            cleanup: CleanupState::default(),
        }
    }

    pub fn metadata(&self) -> &BreakerHandleMetadata {
        &self.metadata
    }

    pub fn shared(&self) -> &Arc<ExternalTableSharedState> {
        &self.shared
    }

    pub fn cleanup_status(&self) -> CleanupStatus {
        self.cleanup.status()
    }
}

impl RuntimeCleanup for ExternalTableHandle {
    fn cleanup(&self, _ctx: &mut OperatorCleanupContext, reason: CleanupReason) -> Result<()> {
        self.shared.mark_finalized();
        self.cleanup.mark(reason);
        Ok(())
    }
}
