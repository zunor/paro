// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Fixed-capacity exclusive optimizer wall-time accounting. Phases and work
//! categories are two projections of the same interval, not additive timers.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(usize)]
pub enum WorkKind {
    Normalization,
    RegionPlanning,
    PhysicalSelection,
    PhysicalLowering,
    Unclassified,
}

impl WorkKind {
    pub const ALL: [Self; 5] = [
        Self::Normalization,
        Self::RegionPlanning,
        Self::PhysicalSelection,
        Self::PhysicalLowering,
        Self::Unclassified,
    ];
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkEntry {
    pub kind: WorkKind,
    pub exclusive_ns: u64,
    pub entries: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptimizerWork {
    pub total_ns: u64,
    pub buckets: [WorkEntry; WorkKind::ALL.len()],
}

impl OptimizerWork {
    pub fn is_closed(&self) -> bool {
        self.buckets
            .iter()
            .enumerate()
            .all(|(i, row)| row.kind == WorkKind::ALL[i])
            && self
                .buckets
                .iter()
                .try_fold(0u64, |sum, row| sum.checked_add(row.exclusive_ns))
                == Some(self.total_ns)
    }
}
