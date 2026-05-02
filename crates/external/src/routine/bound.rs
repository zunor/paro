// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use super::boundary::ExecutionBoundary;
use super::identity::RoutineCallIdentity;
use super::spec::{RoutineSemantics, RoutineSpec};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundRoutineCallMeta {
    pub identity: RoutineCallIdentity,
    pub semantics: RoutineSemantics,
    pub boundary: ExecutionBoundary,
    pub spec: Option<RoutineSpec>,
}
