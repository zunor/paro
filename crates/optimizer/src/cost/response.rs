// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable physical response facts shared by planning strategies.
use paro_planner::physical::{Fingerprint, SearchCost};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GrantDependencyDescriptor {
    /// Neither memory-class decisions nor worker capacity affect this local
    /// implementation. Children can still make the enclosing group sensitive.
    Invariant,
    /// Memory independent, but source supply/latency depends on the worker
    /// capacity. Equal capacities may share a winner across memory classes.
    Parallelism,
    /// The complete operating point is required (including memory/spill).
    Sensitive,
}

/// Query-local identity of one base row source. Binder table indexes are
/// unique across aliases, so self joins remain distinct while equivalent Memo
/// expressions retain the same source identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct WorkSourceId(pub usize);

/// Stable identity of one survivor-domain proof. It names the build domain,
/// probe-key mapping, equality/NULL semantics, and statistics snapshot used
/// to derive a source-local retention bound. Reusing the same proof cannot
/// shrink the source domain twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DomainProofId(pub Fingerprint);

/// Stable identity of one physical predicate evaluation. This is deliberately
/// separate from [`DomainProofId`]: two operators may evaluate the same domain
/// proof, while replaying one operator during search must not charge it twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct EvaluationOccurrenceId(pub Fingerprint);

/// Source-local work retention derived for one runtime-filter installation.
/// Keeping the ratio beside its source preserves a unique-key proof on one
/// lineage even when another lineage of the same join key is non-unique.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SidewaysFilterSource {
    pub source: WorkSourceId,
    /// Identity of the exact domain proof. Replaying the same proof is
    /// idempotent; different identities are conservatively correlated unless
    /// a future joint-domain proof explicitly relates them.
    pub domain: DomainProofId,
    /// Physical evaluation which publishes `domain` to this source.
    pub evaluation: EvaluationOccurrenceId,
    pub expected_retained_ppm: u32,
    pub upper_retained_ppm: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceRetentionProof {
    pub domain: DomainProofId,
    pub expected_retained_ppm: u32,
    /// Absolute survivor bound relative to the immutable base source, never a
    /// conditional selectivity relative to the preceding filter.
    pub upper_retained_ppm: u32,
}

/// A disjoint portion of a winner's work proven to belong to one base source.
/// The contained cost is work-only: memory and external-resource contracts
/// remain on the complete winner and are never weakened by selectivity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceFilterWork {
    pub domain: DomainProofId,
    pub evaluation: EvaluationOccurrenceId,
    /// Immutable input-row domain on which this evaluation is charged.  This
    /// is intentionally carried next to the occurrence rather than inferred
    /// from `SourceWork::cost`: the latter already contains survivor
    /// reductions and therefore changes with join composition order.
    pub evaluation_rows: u64,
    pub expected_retained_ppm: u32,
    pub upper_retained_ppm: u32,
    /// Cost of evaluating this predicate against the unfiltered source.  It is
    /// allocated once from the operator-local full-source term and then only
    /// scaled by preceding *distinct* domain proofs.  It must never be derived
    /// from a lane's already-retained `cost`.
    pub full_apply_cost: SearchCost,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceWork(Arc<SourceWorkData>);

impl SourceWork {
    pub(crate) fn snapshot(&self) -> &SourceWorkData {
        &self.0
    }

    /// Diagnostic-only allocation identity, valid while the snapshot is
    /// borrowed. This is never a semantic identity or retained cache key.
    pub(crate) fn payload_identity(&self) -> usize {
        Arc::as_ptr(&self.0) as usize
    }

    pub(crate) fn retained_payload_bytes(&self) -> usize {
        std::mem::size_of::<SourceWorkData>()
            + std::mem::size_of_val(self.retentions.as_ref())
            + std::mem::size_of_val(self.filters.as_ref())
    }

    #[cfg(test)]
    pub(crate) fn shares_payload(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl From<SourceWorkData> for SourceWork {
    fn from(data: SourceWorkData) -> Self {
        Self(Arc::new(data))
    }
}

impl std::ops::Deref for SourceWork {
    type Target = SourceWorkData;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An immutable source response once published. Streaming/branch composition
/// shares the complete snapshot; only a filter which changes this source
/// constructs a new one. No mutable access to a published snapshot is exposed.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceWorkData {
    pub source: WorkSourceId,
    /// Immutable number of rows in this physical source lane before runtime
    /// predicates. Predicate work is attributed by this row domain, never by
    /// byte cost or by a cost already reduced by an earlier predicate.
    pub source_rows: u64,
    /// Work of the unfiltered source. Every survivor proof is interpreted in
    /// this immutable domain so correlated filters cannot multiply hard bounds.
    pub base_cost: SearchCost,
    /// Base-source access work after every selected runtime filter.
    pub cost: SearchCost,
    /// Unique survivor proofs already applied to the base domain.
    pub retentions: Box<[SourceRetentionProof]>,
    /// Runtime predicates already attached to this source.
    pub filters: Box<[SourceFilterWork]>,
    /// Jointly ordered evaluation work currently present in the winner cost.
    pub filter_apply_cost: SearchCost,
    /// Complete source-pipeline work after retention and predicate ordering,
    /// folded once at the pipeline operating point.
    pub phased_cost: SearchCost,
    pub phase_tasks: u16,
}
