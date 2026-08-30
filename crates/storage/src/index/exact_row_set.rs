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

use std::ops::Range;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use paro_common::error::Result;
use roaring::RoaringBitmap;

use crate::index::PartitionDirectory;
use crate::tablet::ColumnId;

/// One immutable scalar-dictionary posting. `ordinal` is the physical
/// dictionary identity shared by the complete bitmap artifact and any
/// covering vector-scan layout built from that artifact.
#[derive(Debug, Clone)]
pub struct ExactOrdinalPosting {
    ordinal: u16,
    scalar_key: ExactScalarKey,
    rows: Arc<RoaringBitmap>,
}

/// Canonical scalar identity carried from a complete bitmap dictionary into
/// generation-owned covering scans. Unlike a posting hash, this proves which
/// SQL value an ordinal represents even when different segments assign that
/// value different local ordinal numbers.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ExactScalarKey {
    Value(Bytes),
    Null,
}

impl ExactOrdinalPosting {
    #[cfg(test)]
    pub(crate) fn new(ordinal: u16, rows: Arc<RoaringBitmap>) -> Self {
        let scalar_key = if ordinal == u16::MAX {
            ExactScalarKey::Null
        } else {
            ExactScalarKey::Value(Bytes::copy_from_slice(&ordinal.to_le_bytes()))
        };
        Self {
            ordinal,
            scalar_key,
            rows,
        }
    }

    pub(crate) fn from_index(
        ordinal: u16,
        scalar_key: ExactScalarKey,
        rows: Arc<RoaringBitmap>,
    ) -> Self {
        Self {
            ordinal,
            scalar_key,
            rows,
        }
    }

    pub fn ordinal(&self) -> u16 {
        self.ordinal
    }

    pub fn rows(&self) -> &RoaringBitmap {
        &self.rows
    }

    pub fn scalar_key(&self) -> &ExactScalarKey {
        &self.scalar_key
    }
}

/// Borrowed admission kernel selected once at the graph-search boundary.
///
/// Exact row sets remain extensible as owned query objects, but HNSW must not
/// pay one virtual call for every inspected edge. Matching on this enum before
/// entering the graph loop lets the compiler monomorphize the hot membership
/// test for each physical representation.
#[derive(Debug)]
pub enum ExactRowAdmission<'a> {
    Roaring(&'a RoaringBitmap),
    Ordinal {
        row_ordinals: &'a [u16],
        accepted_ordinals: &'a [u64],
        accepts_null: bool,
    },
    Dense(u32),
    Partitioned(PartitionExactRowAdmission<'a>),
}

/// Borrowed physical partitions of an exact row set.
///
/// `OrdinalPostings` is stronger than a generic iterator: every bitmap owns
/// a disjoint subset of the segment-local row-id domain. Exact distance scans
/// may therefore score the postings independently and merge only their Top-K
/// heaps, without materializing or sorting a query-sized candidate array.
#[derive(Debug, Clone, Copy)]
pub enum ExactRowPartitions<'a> {
    /// One complete contiguous local domain. Exact scoring can stream the
    /// corresponding base-vector range without gathering point ids.
    Dense(u32),
    Single(&'a RoaringBitmap),
    /// Query selection over an index-owned immutable posting catalog. The
    /// accepted-ordinal bit set belongs to the query; posting payloads do not.
    OrdinalSelection(&'a OrdinalRowSet),
    /// Canonical concatenation of immutable segment-local row sets.
    ///
    /// Keeping the partition identity here is essential: generation-owned
    /// search artifacts can validate the shifted union of segment postings
    /// against their generation-wide covering layout without first
    /// materializing a query-sized bitmap.
    Partitioned(&'a PartitionExactRowSet),
}

impl ExactRowPartitions<'_> {
    pub fn len(self) -> usize {
        match self {
            Self::Dense(_) => 1,
            Self::Single(_) => 1,
            Self::OrdinalSelection(row_set) => row_set.selected_posting_count(),
            Self::Partitioned(row_set) => row_set
                .physical_parts()
                .map(|(_, part)| part.physical_partitions().len())
                .sum(),
        }
    }

    pub fn is_empty(self) -> bool {
        self.len() == 0
    }
}

impl ExactRowAdmission<'_> {
    #[inline(always)]
    pub fn contains(&self, row_id: u32) -> bool {
        match self {
            Self::Roaring(bitmap) => bitmap.contains(row_id),
            Self::Ordinal {
                row_ordinals,
                accepted_ordinals,
                accepts_null,
            } => row_ordinals.get(row_id as usize).is_some_and(|ordinal| {
                if *ordinal == u16::MAX {
                    return *accepts_null;
                }
                accepted_ordinals
                    .get(*ordinal as usize / 64)
                    .is_some_and(|word| word & (1_u64 << (*ordinal % 64)) != 0)
            }),
            Self::Dense(domain_len) => row_id < *domain_len,
            Self::Partitioned(admission) => admission.contains(row_id),
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

    /// Visit matching row ids in caller-sized batches. The default preserves
    /// extensibility, while index-native implementations override it so exact
    /// scans pay dynamic dispatch once per batch rather than once per row.
    fn try_for_each_batch(
        &self,
        batch: &mut [u32],
        visitor: &mut dyn FnMut(&[u32]) -> Result<()>,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let mut len = 0usize;
        self.try_for_each(&mut |row_id| {
            batch[len] = row_id;
            len += 1;
            if len == batch.len() {
                visitor(batch)?;
                len = 0;
            }
            Ok(())
        })?;
        if len != 0 {
            visitor(&batch[..len])?;
        }
        Ok(())
    }

    /// Append deterministic witnesses for the physical partitions that make
    /// up this exact row set. Predicate-local graphs use these rows as direct
    /// entry seeds, avoiding the assumption that an unfiltered HNSW beam will
    /// happen to discover every scalar partition selected by a predicate.
    ///
    /// Implementations should spread a bounded result across their disjoint
    /// partitions. A representation without partition metadata may return a
    /// single admitted row; correctness never depends on these witnesses
    /// because exact admission and the exact fallback remain authoritative.
    fn append_partition_seeds(&self, limit: usize, seeds: &mut Vec<u32>);

    /// Resolve disjoint physical partitions for exact distance execution.
    /// The returned representation borrows immutable index storage; callers
    /// must not charge its retained bytes to the query.
    fn physical_partitions(&self) -> ExactRowPartitions<'_>;

    /// Query-owned retained bytes. Shared index storage is accounted by its
    /// index owner and must not be charged once per query.
    fn query_retained_bytes(&self) -> usize;

    /// Resolve the representation-specific graph admission kernel. This is
    /// deliberately separate from [`Self::contains`]: generic consumers may
    /// use the trait method, while HNSW resolves dynamic dispatch once.
    fn admission(&self) -> ExactRowAdmission<'_>;
}

/// Exact all-row membership without materializing a dense bitmap.
#[derive(Debug)]
pub struct DenseRowSet {
    domain_len: u32,
    materialized: OnceLock<RoaringBitmap>,
}

impl DenseRowSet {
    pub fn new(domain_len: u32) -> Self {
        Self {
            domain_len,
            materialized: OnceLock::new(),
        }
    }

    fn materialized(&self) -> &RoaringBitmap {
        self.materialized.get_or_init(|| {
            let mut rows = RoaringBitmap::new();
            rows.insert_range(0..self.domain_len);
            rows
        })
    }
}

impl ExactRowSet for DenseRowSet {
    fn len(&self) -> u64 {
        u64::from(self.domain_len)
    }

    fn contains(&self, row_id: u32) -> bool {
        row_id < self.domain_len
    }

    fn domain_len(&self) -> usize {
        self.domain_len as usize
    }

    fn materialize(&self) -> RoaringBitmap {
        self.materialized().clone()
    }

    fn try_for_each(&self, visitor: &mut dyn FnMut(u32) -> Result<()>) -> Result<()> {
        for row_id in 0..self.domain_len {
            visitor(row_id)?;
        }
        Ok(())
    }

    fn append_partition_seeds(&self, limit: usize, seeds: &mut Vec<u32>) {
        if limit == 0 || self.domain_len == 0 {
            return;
        }
        let count = (self.domain_len as usize).min(limit);
        for slot in 0..count {
            seeds.push(((slot as u64 * u64::from(self.domain_len)) / count as u64) as u32);
        }
    }

    fn physical_partitions(&self) -> ExactRowPartitions<'_> {
        ExactRowPartitions::Dense(self.domain_len)
    }

    fn query_retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn admission(&self) -> ExactRowAdmission<'_> {
        ExactRowAdmission::Dense(self.domain_len)
    }
}

#[derive(Debug)]
struct PartitionExactRowSetPart {
    range: Range<u32>,
    row_set: Arc<dyn ExactRowSet>,
}

/// Query-compiled admission kernel for a partition row set.
///
/// Each child trait object is resolved to its concrete admission enum once at
/// the search boundary. Candidate probes then use the parent's block directory
/// and an enum match; they never binary-search segment ranges or call an
/// `ExactRowSet` virtual method in the graph loop.
#[derive(Debug)]
pub struct PartitionExactRowAdmission<'a> {
    row_set: &'a PartitionExactRowSet,
    parts: Box<[ExactRowAdmission<'a>]>,
}

impl PartitionExactRowAdmission<'_> {
    #[inline(always)]
    pub(crate) fn contains(&self, row_id: u32) -> bool {
        let Some((part_index, local_row_id)) = self.row_set.part_index_for(row_id) else {
            return false;
        };
        self.parts
            .get(part_index)
            .is_some_and(|part| part.contains(local_row_id))
    }
}

/// Exact membership over a canonical concatenation of segment-local domains.
///
/// Graph navigation resolves membership through the constituent row sets
/// without building a query-wide bitmap. Exact-scan paths materialize the
/// shifted union lazily and at most once.
#[derive(Debug)]
pub struct PartitionExactRowSet {
    parts: Box<[PartitionExactRowSetPart]>,
    part_directory: Arc<PartitionDirectory>,
    directory_owned_by_query: bool,
    cardinality: u64,
    domain_len: u32,
    materialized: OnceLock<RoaringBitmap>,
}

impl PartitionExactRowSet {
    pub fn try_new(parts: Vec<(Range<u32>, Arc<dyn ExactRowSet>)>) -> Result<Self> {
        Self::try_new_inner(parts, None)
    }

    /// Bind query-specific exact row sets to the immutable physical layout
    /// already owned by a generation artifact. Partition boundaries are an
    /// artifact property; rebuilding their routing directory for every query
    /// would make query state pay for immutable generation metadata.
    pub(crate) fn try_new_with_directory(
        parts: Vec<(Range<u32>, Arc<dyn ExactRowSet>)>,
        part_directory: Arc<PartitionDirectory>,
    ) -> Result<Self> {
        Self::try_new_inner(parts, Some(part_directory))
    }

    fn try_new_inner(
        parts: Vec<(Range<u32>, Arc<dyn ExactRowSet>)>,
        part_directory: Option<Arc<PartitionDirectory>>,
    ) -> Result<Self> {
        if parts.is_empty() {
            return Err(paro_common::error::invalid_input(
                "partition exact row set must contain at least one domain",
            ));
        }
        let mut expected_start = 0u32;
        let mut cardinality = 0u64;
        let mut validated = Vec::with_capacity(parts.len());
        for (range, row_set) in parts {
            if range.start != expected_start || range.start >= range.end {
                return Err(paro_common::error::invalid_input(
                    "partition exact row-set domains must be non-empty and contiguous",
                ));
            }
            let domain_rows = (range.end - range.start) as usize;
            if row_set.domain_len() > domain_rows {
                return Err(paro_common::error::data_corrupted(format!(
                    "segment exact row-set domain {} exceeds partition span {domain_rows}",
                    row_set.domain_len()
                )));
            }
            cardinality = cardinality.checked_add(row_set.len()).ok_or_else(|| {
                paro_common::error::out_of_range("partition exact row-set cardinality overflow")
            })?;
            expected_start = range.end;
            validated.push(PartitionExactRowSetPart { range, row_set });
        }
        let parts = validated.into_boxed_slice();
        let directory_owned_by_query = part_directory.is_none();
        let part_directory = match part_directory {
            Some(directory) => {
                if !directory.matches_partition_ends(parts.iter().map(|part| part.range.end)) {
                    return Err(paro_common::error::data_corrupted(
                        "partition exact row-set domains differ from artifact coverage",
                    ));
                }
                directory
            }
            None => Arc::new(PartitionDirectory::try_new(
                parts.iter().map(|part| part.range.end),
            )?),
        };
        Ok(Self {
            parts,
            part_directory,
            directory_owned_by_query,
            cardinality,
            domain_len: expected_start,
            materialized: OnceLock::new(),
        })
    }

    #[inline(always)]
    fn part_index_for(&self, row_id: u32) -> Option<(usize, u32)> {
        let position = self.part_directory.part_for(row_id)?;
        let part = self.parts.get(position)?;
        debug_assert!(part.range.contains(&row_id));
        Some((position, row_id - part.range.start))
    }

    fn materialized(&self) -> &RoaringBitmap {
        self.materialized.get_or_init(|| {
            let mut rows = RoaringBitmap::new();
            for part in &self.parts {
                for row_id in part.row_set.materialize() {
                    rows.insert(part.range.start + row_id);
                }
            }
            rows
        })
    }

    pub(crate) fn physical_parts(
        &self,
    ) -> impl Iterator<Item = (Range<u32>, &dyn ExactRowSet)> + '_ {
        self.parts
            .iter()
            .map(|part| (part.range.clone(), part.row_set.as_ref()))
    }
}

impl ExactRowSet for PartitionExactRowSet {
    fn len(&self) -> u64 {
        self.cardinality
    }

    fn contains(&self, row_id: u32) -> bool {
        self.part_index_for(row_id)
            .is_some_and(|(part_index, local_row_id)| {
                self.parts[part_index].row_set.contains(local_row_id)
            })
    }

    fn domain_len(&self) -> usize {
        self.domain_len as usize
    }

    fn materialize(&self) -> RoaringBitmap {
        self.materialized().clone()
    }

    fn try_for_each(&self, visitor: &mut dyn FnMut(u32) -> Result<()>) -> Result<()> {
        for part in &self.parts {
            part.row_set
                .try_for_each(&mut |row_id| visitor(part.range.start + row_id))?;
        }
        Ok(())
    }

    fn append_partition_seeds(&self, limit: usize, seeds: &mut Vec<u32>) {
        if limit == 0 {
            return;
        }
        let per_part = limit.div_ceil(self.parts.len()).max(1);
        for part in &self.parts {
            if seeds.len() >= limit {
                break;
            }
            let mut local = Vec::new();
            part.row_set
                .append_partition_seeds(per_part.min(limit - seeds.len()), &mut local);
            seeds.extend(local.into_iter().map(|row_id| part.range.start + row_id));
        }
        seeds.truncate(limit);
    }

    fn physical_partitions(&self) -> ExactRowPartitions<'_> {
        ExactRowPartitions::Partitioned(self)
    }

    fn query_retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(
                self.parts.len().saturating_mul(
                    std::mem::size_of::<PartitionExactRowSetPart>()
                        .saturating_add(std::mem::size_of::<ExactRowAdmission<'_>>()),
                ),
            )
            .saturating_add(if self.directory_owned_by_query {
                self.part_directory.allocated_bytes()
            } else {
                0
            })
    }

    fn admission(&self) -> ExactRowAdmission<'_> {
        ExactRowAdmission::Partitioned(PartitionExactRowAdmission {
            row_set: self,
            parts: self
                .parts
                .iter()
                .map(|part| part.row_set.admission())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }
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

    fn try_for_each_batch(
        &self,
        batch: &mut [u32],
        visitor: &mut dyn FnMut(&[u32]) -> Result<()>,
    ) -> Result<()> {
        visit_row_ids_in_batches(self.iter(), batch, visitor)
    }

    fn append_partition_seeds(&self, limit: usize, seeds: &mut Vec<u32>) {
        if limit == 0 || self.is_empty() {
            return;
        }
        let count = self.len().min(limit as u64);
        for slot in 0..count {
            let rank = slot.saturating_mul(self.len()) / count;
            if let Some(row_id) = self.select(u32::try_from(rank).unwrap_or(u32::MAX)) {
                seeds.push(row_id);
            }
        }
    }

    fn physical_partitions(&self) -> ExactRowPartitions<'_> {
        ExactRowPartitions::Single(self)
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
    column_id: ColumnId,
    row_ordinals: Arc<[u16]>,
    accepted_ordinals: Box<[u64]>,
    accepts_null: bool,
    cardinality: u64,
    /// Accepted dictionary postings are disjoint by the bitmap artifact
    /// completeness contract. Keeping shared decoded postings lets exact scans
    /// enumerate candidates without deserializing or unioning them per query.
    posting_catalog: Arc<[ExactOrdinalPosting]>,
}

impl OrdinalRowSet {
    #[cfg(test)]
    pub(crate) fn new(
        column_id: ColumnId,
        row_ordinals: Arc<[u16]>,
        accepted_ordinals: Box<[u64]>,
        accepts_null: bool,
        cardinality: u64,
        postings: Box<[ExactOrdinalPosting]>,
    ) -> Self {
        Self::new_shared(
            column_id,
            row_ordinals,
            accepted_ordinals,
            accepts_null,
            cardinality,
            Arc::from(postings),
        )
    }

    pub(crate) fn new_shared(
        column_id: ColumnId,
        row_ordinals: Arc<[u16]>,
        accepted_ordinals: Box<[u64]>,
        accepts_null: bool,
        cardinality: u64,
        posting_catalog: Arc<[ExactOrdinalPosting]>,
    ) -> Self {
        Self {
            column_id,
            row_ordinals,
            accepted_ordinals,
            accepts_null,
            cardinality,
            posting_catalog,
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

    pub(crate) fn column_id(&self) -> ColumnId {
        self.column_id
    }

    pub(crate) fn selected_postings(
        &self,
    ) -> impl Iterator<Item = &ExactOrdinalPosting> + Clone + '_ {
        self.posting_catalog
            .iter()
            .filter(|posting| self.accepts_ordinal(posting.ordinal()))
    }

    pub(crate) fn selected_posting_count(&self) -> usize {
        self.selected_postings().count()
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
        for posting in self.selected_postings() {
            bitmap |= posting.rows();
        }
        bitmap
    }

    fn try_for_each(&self, visitor: &mut dyn FnMut(u32) -> Result<()>) -> Result<()> {
        for posting in self.selected_postings() {
            for row_id in posting.rows().iter() {
                visitor(row_id)?;
            }
        }
        Ok(())
    }

    fn try_for_each_batch(
        &self,
        batch: &mut [u32],
        visitor: &mut dyn FnMut(&[u32]) -> Result<()>,
    ) -> Result<()> {
        let row_ids = self
            .selected_postings()
            .flat_map(|posting| posting.rows().iter());
        visit_row_ids_in_batches(row_ids, batch, visitor)
    }

    fn append_partition_seeds(&self, limit: usize, seeds: &mut Vec<u32>) {
        let posting_count = self.selected_posting_count();
        if limit == 0 || posting_count == 0 {
            return;
        }
        let count = posting_count.min(limit);
        let mut postings = self.selected_postings();
        let mut next_slot = 0usize;
        for slot in 0..count {
            let partition = slot.saturating_mul(posting_count) / count;
            let Some(posting) = postings.nth(partition.saturating_sub(next_slot)) else {
                break;
            };
            next_slot = partition.saturating_add(1);
            if let Some(row_id) = posting.rows().iter().next() {
                seeds.push(row_id);
            }
        }
    }

    fn physical_partitions(&self) -> ExactRowPartitions<'_> {
        ExactRowPartitions::OrdinalSelection(self)
    }

    fn query_retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.accepted_ordinals.len() * std::mem::size_of::<u64>())
    }

    fn admission(&self) -> ExactRowAdmission<'_> {
        ExactRowAdmission::Ordinal {
            row_ordinals: &self.row_ordinals,
            accepted_ordinals: &self.accepted_ordinals,
            accepts_null: self.accepts_null,
        }
    }
}

fn visit_row_ids_in_batches(
    row_ids: impl Iterator<Item = u32>,
    batch: &mut [u32],
    visitor: &mut dyn FnMut(&[u32]) -> Result<()>,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut len = 0usize;
    for row_id in row_ids {
        batch[len] = row_id;
        len += 1;
        if len == batch.len() {
            visitor(batch)?;
            len = 0;
        }
    }
    if len != 0 {
        visitor(&batch[..len])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinal_row_set_batches_across_posting_boundaries() {
        let first = Arc::new(RoaringBitmap::from_iter([1, 3, 5]));
        let second = Arc::new(RoaringBitmap::from_iter([8, 13]));
        let rows = OrdinalRowSet::new(
            7,
            Arc::from([0_u16; 16]),
            vec![0b11].into_boxed_slice(),
            false,
            5,
            vec![
                ExactOrdinalPosting::new(0, first),
                ExactOrdinalPosting::new(1, second),
            ]
            .into_boxed_slice(),
        );
        let mut batch = [0; 4];
        let mut batches = Vec::new();
        rows.try_for_each_batch(&mut batch, &mut |rows| {
            batches.push(rows.to_vec());
            Ok(())
        })
        .unwrap();

        assert_eq!(batches, vec![vec![1, 3, 5, 8], vec![13]]);
    }

    #[test]
    fn ordinal_partition_seeds_cover_disjoint_postings_and_sample_when_bounded() {
        let postings = [
            ExactOrdinalPosting::new(0, Arc::new(RoaringBitmap::from_iter([1, 2]))),
            ExactOrdinalPosting::new(1, Arc::new(RoaringBitmap::from_iter([10, 11]))),
            ExactOrdinalPosting::new(2, Arc::new(RoaringBitmap::from_iter([20, 21]))),
            ExactOrdinalPosting::new(3, Arc::new(RoaringBitmap::from_iter([30, 31]))),
        ];
        let rows = OrdinalRowSet::new(
            7,
            Arc::from([0_u16; 32]),
            vec![0b1111].into_boxed_slice(),
            false,
            8,
            postings.into(),
        );
        let mut all = Vec::new();
        rows.append_partition_seeds(8, &mut all);
        assert_eq!(all, vec![1, 10, 20, 30]);

        let mut sampled = Vec::new();
        rows.append_partition_seeds(2, &mut sampled);
        assert_eq!(sampled, vec![1, 20]);
    }

    #[test]
    fn roaring_row_set_batch_iteration_accepts_empty_scratch() {
        let rows = RoaringBitmap::from_iter([2, 4]);
        rows.try_for_each_batch(&mut [], &mut |_| {
            panic!("empty scratch must not invoke the visitor")
        })
        .unwrap();
    }

    #[test]
    fn partition_row_set_translates_segment_local_domains_without_unioning() {
        let first: Arc<dyn ExactRowSet> = Arc::new(RoaringBitmap::from_iter([0_u32, 3, 7]));
        let second: Arc<dyn ExactRowSet> = Arc::new(DenseRowSet::new(4));
        let rows = PartitionExactRowSet::try_new(vec![(0..8, first), (8..12, second)]).unwrap();

        assert_eq!(rows.len(), 7);
        assert_eq!(rows.domain_len(), 12);
        assert!(rows.contains(0));
        assert!(rows.contains(7));
        assert!(rows.contains(8));
        assert!(rows.contains(11));
        assert!(!rows.contains(4));
        assert!(!rows.contains(12));
        assert!(rows.admission().contains(11));
        assert!(matches!(
            rows.physical_partitions(),
            ExactRowPartitions::Partitioned(_)
        ));
        assert!(rows.materialized.get().is_none());

        let mut visited = Vec::new();
        rows.try_for_each(&mut |row_id| {
            visited.push(row_id);
            Ok(())
        })
        .unwrap();
        assert_eq!(visited, vec![0, 3, 7, 8, 9, 10, 11]);
        assert_eq!(
            rows.materialize(),
            RoaringBitmap::from_iter([0_u32, 3, 7, 8, 9, 10, 11])
        );
    }

    #[test]
    fn partition_row_set_rejects_gaps_and_oversized_local_domains() {
        let dense: Arc<dyn ExactRowSet> = Arc::new(DenseRowSet::new(4));
        assert!(PartitionExactRowSet::try_new(vec![(1..5, Arc::clone(&dense))]).is_err());
        assert!(PartitionExactRowSet::try_new(vec![(0..3, dense)]).is_err());
    }

    #[test]
    fn partition_admission_directory_uses_direct_blocks_and_boundary_fallbacks() {
        let first: Arc<dyn ExactRowSet> = Arc::new(DenseRowSet::new(6_000));
        let second: Arc<dyn ExactRowSet> = Arc::new(DenseRowSet::new(6_000));
        let rows = PartitionExactRowSet::try_new(vec![(0..6_000, first), (6_000..12_000, second)])
            .unwrap();

        assert_eq!(rows.part_index_for(4_095), Some((0, 4_095)));
        assert_eq!(rows.part_index_for(5_999), Some((0, 5_999)));
        assert_eq!(rows.part_index_for(6_000), Some((1, 0)));
        assert_eq!(rows.part_index_for(11_999), Some((1, 5_999)));
        assert_eq!(rows.part_index_for(12_000), None);

        let admission = rows.admission();
        assert!(admission.contains(5_999));
        assert!(admission.contains(6_000));
        assert!(!admission.contains(12_000));
    }
}
