// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable physical response facts shared by planning strategies.

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
