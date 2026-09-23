// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Fixed-capacity exclusive optimizer wall-time accounting. Phases and work
//! categories are two projections of the same interval, not additive timers.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(usize)]
pub enum WorkKind {
    Pre,
    Agenda,
    Match,
    Apply,
    Insert,
    Schedule,
    Recipe,
    Subproblem,
    Kernel,
    Admission,
    Publish,
    Quality,
    Finish,
    QualityEvidence,
    QualityDomain,
    QualityProduction,
    QualityFreeze,
    QualityReads,
    NativeConstruct,
    Statistics,
    OwnedRewrite,
    Settlement,
    Staging,
    SemanticGuard,
    Rollback,
    Encoding,
    Dependencies,
    PhaseTransition,
    PhysicalLowering,
    Unclassified,
}

impl WorkKind {
    pub const ALL: [Self; 30] = [
        Self::Pre,
        Self::Agenda,
        Self::Match,
        Self::Apply,
        Self::Insert,
        Self::Schedule,
        Self::Recipe,
        Self::Subproblem,
        Self::Kernel,
        Self::Admission,
        Self::Publish,
        Self::Quality,
        Self::Finish,
        Self::QualityEvidence,
        Self::QualityDomain,
        Self::QualityProduction,
        Self::QualityFreeze,
        Self::QualityReads,
        Self::NativeConstruct,
        Self::Statistics,
        Self::OwnedRewrite,
        Self::Settlement,
        Self::Staging,
        Self::SemanticGuard,
        Self::Rollback,
        Self::Encoding,
        Self::Dependencies,
        Self::PhaseTransition,
        Self::PhysicalLowering,
        Self::Unclassified,
    ];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(usize)]
pub enum WorkPhase {
    OutsideSearch,
    Mandatory,
    Optional,
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
    pub outside_search_ns: u64,
    pub mandatory_ns: u64,
    pub optional_ns: u64,
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
            && [self.outside_search_ns, self.mandatory_ns, self.optional_ns]
                .into_iter()
                .try_fold(0u64, |sum, ns| sum.checked_add(ns))
                == Some(self.total_ns)
    }
}
