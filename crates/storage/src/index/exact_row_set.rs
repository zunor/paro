// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Exact segment-local row-set representations.
//!
//! Search operators need both exact membership tests and, occasionally, an
//! iterator for an exact distance scan. Requiring every access path to first
//! materialize a `RoaringBitmap` makes broad low-cardinality predicates scale
//! with the number of matching rows even though graph search inspects only a
//! few thousand candidates. This interface keeps exactness while allowing an
//! index-native membership representation.

use std::sync::Arc;

use paro_common::error::Result;
use roaring::RoaringBitmap;

/// Borrowed admission kernel selected once at the graph-search boundary.
///
/// Exact row sets remain extensible as owned query objects, but HNSW must not
/// pay one virtual call for every inspected edge. Matching on this enum before
/// entering the graph loop lets the compiler monomorphize the hot membership
/// test for each physical representation.
#[derive(Debug, Clone, Copy)]
pub enum ExactRowAdmission<'a> {
    Roaring(&'a RoaringBitmap),
    Ordinal {
        row_ordinals: &'a [u16],
        accepted_ordinals: &'a [u64],
        accepts_null: bool,
    },
}

impl ExactRowAdmission<'_> {
    #[inline(always)]
    pub fn contains(self, row_id: u32) -> bool {
        match self {
            Self::Roaring(bitmap) => bitmap.contains(row_id),
            Self::Ordinal {
                row_ordinals,
                accepted_ordinals,
                accepts_null,
            } => row_ordinals.get(row_id as usize).is_some_and(|ordinal| {
                if *ordinal == u16::MAX {
                    return accepts_null;
                }
                accepted_ordinals
                    .get(*ordinal as usize / 64)
                    .is_some_and(|word| word & (1_u64 << (*ordinal % 64)) != 0)
            }),
        }
    }
}

/// Exact row membership over one immutable segment-local domain.
pub trait ExactRowSet: std::fmt::Debug + Send + Sync {
    /// Number of matching rows.
    fn len(&self) -> u64;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether a segment-local row belongs to the set.
    fn contains(&self, row_id: u32) -> bool;

    /// Number of row-id slots in the segment-local domain.
    fn domain_len(&self) -> usize;

    /// Materialize the set when an exact sequential scan is actually chosen.
    fn materialize(&self) -> RoaringBitmap;

    /// Visit matching row ids without requiring a union bitmap. Implementors
    /// backed by disjoint inverted postings can stream those postings directly
    /// into a distance scanner.
    fn try_for_each(&self, visitor: &mut dyn FnMut(u32) -> Result<()>) -> Result<()>;

    /// Query-owned retained bytes. Shared index storage is accounted by its
    /// index owner and must not be charged once per query.
    fn query_retained_bytes(&self) -> usize;

    /// Resolve the representation-specific graph admission kernel. This is
    /// deliberately separate from [`Self::contains`]: generic consumers may
    /// use the trait method, while HNSW resolves dynamic dispatch once.
    fn admission(&self) -> ExactRowAdmission<'_>;
}

impl ExactRowSet for RoaringBitmap {
    fn len(&self) -> u64 {
        RoaringBitmap::len(self)
    }

    fn contains(&self, row_id: u32) -> bool {
        RoaringBitmap::contains(self, row_id)
    }

    fn domain_len(&self) -> usize {
        self.max().map_or(0, |row_id| row_id as usize + 1)
    }

    fn materialize(&self) -> RoaringBitmap {
        self.clone()
    }

    fn try_for_each(&self, visitor: &mut dyn FnMut(u32) -> Result<()>) -> Result<()> {
        for row_id in self.iter() {
            visitor(row_id)?;
        }
        Ok(())
    }

    fn query_retained_bytes(&self) -> usize {
        const CONTAINER_METADATA_BYTES: usize = 32;
        let stats = self.statistics();
        let payload = stats
            .n_bytes_array_containers
            .saturating_add(stats.n_bytes_run_containers)
            .saturating_add(stats.n_bytes_bitset_containers) as usize;
        std::mem::size_of::<RoaringBitmap>()
            .saturating_add(payload)
            .saturating_add(stats.n_containers as usize * CONTAINER_METADATA_BYTES)
    }

    fn admission(&self) -> ExactRowAdmission<'_> {
        ExactRowAdmission::Roaring(self)
    }
}

/// Low-cardinality exact membership encoded as one dictionary ordinal per
/// row plus a query-specific accepted-ordinal bit set.
#[derive(Debug)]
pub struct OrdinalRowSet {
    row_ordinals: Arc<[u16]>,
    accepted_ordinals: Box<[u64]>,
    accepts_null: bool,
    cardinality: u64,
    /// Accepted dictionary postings are disjoint by the bitmap artifact
    /// completeness contract. Keeping shared decoded postings lets exact scans
    /// enumerate candidates without deserializing or unioning them per query.
    postings: Box<[Arc<RoaringBitmap>]>,
}

impl OrdinalRowSet {
    pub(crate) fn new(
        row_ordinals: Arc<[u16]>,
        accepted_ordinals: Box<[u64]>,
        accepts_null: bool,
        cardinality: u64,
        postings: Box<[Arc<RoaringBitmap>]>,
    ) -> Self {
        Self {
            row_ordinals,
            accepted_ordinals,
            accepts_null,
            cardinality,
            postings,
        }
    }

    fn accepts_ordinal(&self, ordinal: u16) -> bool {
        if ordinal == u16::MAX {
            return self.accepts_null;
        }
        self.accepted_ordinals
            .get(ordinal as usize / 64)
            .is_some_and(|word| word & (1_u64 << (ordinal % 64)) != 0)
    }
}

impl ExactRowSet for OrdinalRowSet {
    fn len(&self) -> u64 {
        self.cardinality
    }

    fn contains(&self, row_id: u32) -> bool {
        self.row_ordinals
            .get(row_id as usize)
            .is_some_and(|ordinal| self.accepts_ordinal(*ordinal))
    }

    fn domain_len(&self) -> usize {
        self.row_ordinals.len()
    }

    fn materialize(&self) -> RoaringBitmap {
        let mut bitmap = RoaringBitmap::new();
        for posting in &self.postings {
            bitmap |= posting.as_ref();
        }
        bitmap
    }

    fn try_for_each(&self, visitor: &mut dyn FnMut(u32) -> Result<()>) -> Result<()> {
        for posting in &self.postings {
            for row_id in posting.iter() {
                visitor(row_id)?;
            }
        }
        Ok(())
    }

    fn query_retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.accepted_ordinals.len() * std::mem::size_of::<u64>())
            .saturating_add(self.postings.len() * std::mem::size_of::<Arc<RoaringBitmap>>())
    }

    fn admission(&self) -> ExactRowAdmission<'_> {
        ExactRowAdmission::Ordinal {
            row_ordinals: &self.row_ordinals,
            accepted_ordinals: &self.accepted_ordinals,
            accepts_null: self.accepts_null,
        }
    }
}
