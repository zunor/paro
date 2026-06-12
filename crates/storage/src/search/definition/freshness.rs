// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Freshness policy helpers shared by registry capability resolution.

use crate::search::capability::{
    SearchCapability, SearchCapabilityState, SearchNotQueryableReason,
};

pub(crate) fn capability_needs_required_freshness_wait(capability: &SearchCapability) -> bool {
    matches!(
        capability.capability_state(),
        SearchCapabilityState::NotQueryable {
            reason: SearchNotQueryableReason::FreshnessRequired
        }
    )
}
