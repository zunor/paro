// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use crate::capability::CapabilityProfile;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionSpec {
    pub security_mode: RoutineSecurityMode,
    pub capability_profile: CapabilityProfile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutineSecurityMode {
    Invoker,
    Definer,
}
