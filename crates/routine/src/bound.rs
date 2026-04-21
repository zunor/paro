// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use crate::boundary::ExecutionBoundary;
use crate::identity::RoutineCallIdentity;
use crate::spec::{RoutineSemantics, RoutineSpec};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundRoutineCallMeta {
    pub identity: RoutineCallIdentity,
    pub semantics: RoutineSemantics,
    pub boundary: ExecutionBoundary,
    pub spec: Option<RoutineSpec>,
}
