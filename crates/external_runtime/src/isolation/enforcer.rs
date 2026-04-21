// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::Result;
use paro_routine::CapabilityProfile;

use crate::isolation::resource_limits::{FilesystemPolicy, NetworkPolicy, ResourceLimits};

pub trait IsolationEnforcer {
    fn apply_limits(&self, limits: &ResourceLimits) -> Result<()>;
    fn apply_network_policy(&self, policy: &NetworkPolicy) -> Result<()>;
    fn apply_filesystem_policy(&self, policy: &FilesystemPolicy) -> Result<()>;
    fn apply_capability_profile(&self, profile: &CapabilityProfile) -> Result<()>;

    fn enforce_all(&self, limits: &ResourceLimits, profile: &CapabilityProfile) -> Result<()> {
        self.apply_limits(limits)?;
        self.apply_network_policy(&limits.network_policy)?;
        self.apply_filesystem_policy(&limits.filesystem_policy)?;
        self.apply_capability_profile(profile)
    }
}
