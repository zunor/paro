// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Covering vector layout for exact scalar-filter scans.
//!
//! A configured HNSW filter column partitions every immutable segment into
//! SQL-ordered scalar blocks. Persisting `(row_id, vector)` in that block order
//! turns an exact posting scan from random base-column gathers into bounded,
//! sequential artifact reads. The layout remains an HNSW artifact: table pages
//! keep the SQL values and physical row order supplied by the writer.

use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use memmap2::{Mmap, MmapOptions};
use paro_common::error::{self as paro_error, Result};

use crate::index::{ExactOrdinalPosting, ExactScalarKey};

use super::artifact_integrity::ArtifactIntegrity;
use super::{PointOffset, VectorStorage};

const PREDICATE_SCAN_MAGIC: [u8; 4] = *b"HPSC";
const PREDICATE_SCAN_VERSION: u32 = 3;
const PREDICATE_SCAN_HEADER_LEN: usize = 64;
const COLUMN_HEADER_LEN: usize = 32;
const ORDINAL_RANGE_LEN: usize = 20;
const BLOCK_HEADER_LEN: usize = 24;
const NO_BLOCK: u32 = u32::MAX;
pub(crate) const PREDICATE_SCAN_BUILD_STREAM_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct PredicateScanBuildBlock {
    pub(crate) dictionary_ordinals: Box<[u32]>,
    pub(crate) dictionary_values: Box<[Option<Bytes>]>,
    pub(crate) ordinal_row_counts: Box<[u32]>,
    pub(crate) ordinal_fingerprints: Box<[u64]>,
    pub(crate) row_ids: Box<[PointOffset]>,
}

#[derive(Debug)]
pub(crate) struct PredicateScanBuildColumn {
    pub(crate) column_id: u32,
    pub(crate) blocks: Vec<PredicateScanBuildBlock>,
}

#[derive(Debug)]
enum PredicateScanBacking {
    Bytes(Bytes),
    Mmap {
        mmap: Arc<Mmap>,
        offset: usize,
        len: usize,
    },
}

impl PredicateScanBacking {
    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Bytes(bytes) => bytes,
            Self::Mmap { mmap, offset, len } => &mmap[*offset..*offset + *len],
        }
    }
}

#[derive(Debug)]
enum PredicateScanBlock {
    Build {
        row_ids: Box<[PointOffset]>,
    },
    Encoded {
        rows: usize,
        row_ids_offset: usize,
        vectors_offset: usize,
    },
}

impl PredicateScanBlock {
    fn rows(&self) -> usize {
        match self {
            Self::Build { row_ids } => row_ids.len(),
            Self::Encoded { rows, .. } => *rows,
        }
    }
}

struct PredicateScanBuildSource(Arc<dyn VectorStorage>);

impl std::fmt::Debug for PredicateScanBuildSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PredicateScanBuildSource")
            .field("point_count", &self.0.num_vectors())
            .field("dimension", &self.0.vector_dim())
            .finish()
    }
}

#[derive(Debug)]
struct PredicateScanColumn {
    column_id: u32,
    /// Canonical encoded dictionary keys in generation ordinal order.
    ordinal_keys: Box<[Bytes]>,
    ordinal_ranges: Box<[PredicateScanRange]>,
    null_range: PredicateScanRange,
    blocks: Box<[PredicateScanBlock]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PredicateScanRange {
    block_id: u32,
    row_start: u32,
    row_count: u32,
    fingerprint: u64,
}

impl PredicateScanRange {
    const MISSING: Self = Self {
        block_id: NO_BLOCK,
        row_start: 0,
        row_count: 0,
        fingerprint: 0,
    };

    const fn is_missing(self) -> bool {
        self.block_id == NO_BLOCK
    }
}

#[derive(Debug)]
pub struct PredicateScanLayout {
    dimension: usize,
    point_count: usize,
    columns: Box<[PredicateScanColumn]>,
    backing: Option<PredicateScanBacking>,
    /// Canonical generation vectors retained only by a newly built layout.
    /// Publication streams rows from this source into the immutable covering
    /// image with a bounded scratch buffer. Opened artifacts use `backing`
    /// instead and never retain this source.
    build_source: Option<PredicateScanBuildSource>,
    integrity: Option<PredicateIntegrityRange>,
    serialized_len: usize,
}

#[derive(Debug, Clone)]
struct PredicateIntegrityRange {
    verifier: Arc<ArtifactIntegrity>,
    artifact_offset: usize,
}

impl PredicateIntegrityRange {
    #[inline(always)]
    fn verify(&self, local_offset: usize, len: usize) -> Result<()> {
        if self.verifier.is_fully_verified() {
            return Ok(());
        }
        self.verifier.verify_range(
            self.artifact_offset
                .checked_add(local_offset)
                .ok_or_else(|| {
                    paro_error::data_corrupted("predicate scan integrity range overflow")
                })?,
            len,
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PredicateScanRangeRef<'a> {
    row_ids: &'a [PointOffset],
    vectors: &'a [f32],
}

impl<'a> PredicateScanRangeRef<'a> {
    pub(crate) fn row_ids(self) -> &'a [PointOffset] {
        self.row_ids
    }

    pub(crate) fn vectors(self) -> &'a [f32] {
        self.vectors
    }

    fn restrict_to_point_range(
        self,
        dimension: usize,
        point_range: std::ops::Range<PointOffset>,
    ) -> Result<Self> {
        let first = self
            .row_ids
            .partition_point(|&row_id| row_id < point_range.start);
        let end = self
            .row_ids
            .partition_point(|&row_id| row_id < point_range.end);
        let row_ids = self.row_ids.get(first..end).ok_or_else(|| {
            paro_error::data_corrupted("predicate scan point-range slice exceeds row ids")
        })?;
        let vector_start = first.checked_mul(dimension).ok_or_else(|| {
            paro_error::data_corrupted("predicate scan point-range vector offset overflow")
        })?;
        let vector_end = end.checked_mul(dimension).ok_or_else(|| {
            paro_error::data_corrupted("predicate scan point-range vector offset overflow")
        })?;
        let vectors = self.vectors.get(vector_start..vector_end).ok_or_else(|| {
            paro_error::data_corrupted("predicate scan point-range slice exceeds vectors")
        })?;
        Ok(Self { row_ids, vectors })
    }
}

impl PredicateScanLayout {
    pub(crate) fn has_column(&self, column_id: u32) -> bool {
        self.columns
            .binary_search_by_key(&column_id, |column| column.column_id)
            .is_ok()
    }

    /// Deep semantic verification used before publication and by explicit
    /// index-integrity tooling. Normal mmap open validates only metadata and
    /// range bounds so it does not fault every covering-vector page.
    pub(crate) fn verify_integrity(&self) -> Result<()> {
        for column in &self.columns {
            let mut covered = roaring::RoaringBitmap::new();
            for block_id in 0..column.blocks.len() {
                for &row_id in self.block_row_ids(column, block_id)? {
                    if row_id as usize >= self.point_count || !covered.insert(row_id) {
                        return Err(paro_error::data_corrupted(format!(
                            "predicate scan column {} has invalid or duplicate row id {row_id}",
                            column.column_id
                        )));
                    }
                }
            }
            for (ordinal, range) in column.ordinal_ranges.iter().copied().enumerate().chain(
                (!column.null_range.is_missing())
                    .then_some((usize::from(u16::MAX), column.null_range)),
            ) {
                let row_ids = self.range_row_ids(column, range)?;
                if row_ids.windows(2).any(|rows| rows[0] >= rows[1]) {
                    return Err(paro_error::data_corrupted(format!(
                        "predicate scan column {} ordinal {ordinal} is not a strictly ordered posting",
                        column.column_id
                    )));
                }
                let fingerprint = crate::index::bitmap::posting_fingerprint_rows(
                    row_ids.len() as u64,
                    row_ids.iter().copied(),
                );
                if fingerprint != range.fingerprint {
                    return Err(paro_error::data_corrupted(format!(
                        "predicate scan column {} ordinal {ordinal} payload differs from its posting fingerprint",
                        column.column_id
                    )));
                }
            }
            if covered.len() != self.point_count as u64 {
                return Err(paro_error::data_corrupted(format!(
                    "predicate scan column {} covers {} of {} points",
                    column.column_id,
                    covered.len(),
                    self.point_count
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn validate_contract(
        &self,
        dimension: usize,
        point_count: usize,
        filter_columns: &[u32],
    ) -> Result<()> {
        if self.dimension != dimension {
            return Err(paro_error::data_corrupted(format!(
                "predicate scan dimension {} differs from vector dimension {dimension}",
                self.dimension
            )));
        }
        if self.point_count != point_count {
            return Err(paro_error::data_corrupted(format!(
                "predicate scan point count {} differs from graph cardinality {point_count}",
                self.point_count
            )));
        }
        if self
            .columns
            .windows(2)
            .any(|columns| columns[0].column_id >= columns[1].column_id)
        {
            return Err(paro_error::data_corrupted(
                "predicate scan columns must be strictly increasing",
            ));
        }
        for column in &self.columns {
            if filter_columns.binary_search(&column.column_id).is_err() {
                return Err(paro_error::data_corrupted(format!(
                    "predicate scan column {} is absent from the HNSW filter-topology contract",
                    column.column_id
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn from_build_columns(
        dimension: usize,
        point_count: usize,
        columns: Vec<PredicateScanBuildColumn>,
        vector_storage: Arc<dyn VectorStorage>,
    ) -> Result<Self> {
        if dimension == 0 && point_count != 0 {
            return Err(paro_error::invalid_input(
                "predicate scan layout dimension must be positive",
            ));
        }
        u32::try_from(dimension).map_err(|_| {
            paro_error::configuration_limit_exceeded(
                "predicate scan dimension exceeds the durable u32 width",
            )
        })?;
        u32::try_from(point_count).map_err(|_| {
            paro_error::configuration_limit_exceeded(
                "predicate scan point count exceeds the durable u32 width",
            )
        })?;
        u32::try_from(columns.len()).map_err(|_| {
            paro_error::configuration_limit_exceeded(
                "predicate scan column count exceeds the durable u32 width",
            )
        })?;
        if vector_storage.vector_dim() != dimension || vector_storage.num_vectors() != point_count {
            return Err(paro_error::data_corrupted(format!(
                "predicate scan build source shape {}x{} differs from layout {}x{}",
                vector_storage.num_vectors(),
                vector_storage.vector_dim(),
                point_count,
                dimension
            )));
        }
        let mut built_columns = Vec::with_capacity(columns.len());
        for column in columns {
            u32::try_from(column.blocks.len()).map_err(|_| {
                paro_error::configuration_limit_exceeded(
                    "predicate scan block count exceeds the durable u32 width",
                )
            })?;
            let mut max_ordinal = None;
            for block in column.blocks.iter() {
                if block.dictionary_ordinals.len() != block.ordinal_row_counts.len()
                    || block.dictionary_ordinals.len() != block.dictionary_values.len()
                    || block.dictionary_ordinals.len() != block.ordinal_fingerprints.len()
                    || block.ordinal_row_counts.contains(&0)
                    || block
                        .ordinal_row_counts
                        .iter()
                        .try_fold(0usize, |rows, count| rows.checked_add(*count as usize))
                        != Some(block.row_ids.len())
                {
                    return Err(paro_error::data_corrupted(format!(
                        "predicate scan column {} has inconsistent ordinal runs",
                        column.column_id
                    )));
                }
                for &ordinal in block.dictionary_ordinals.iter() {
                    if ordinal != u32::MAX {
                        max_ordinal =
                            Some(max_ordinal.map_or(ordinal, |max: u32| max.max(ordinal)));
                    }
                }
            }
            // Native ordinal admission uses u16 and is deliberately absent for
            // higher-cardinality bitmap dictionaries. Such columns retain the
            // predicate graph but do not need a covering exact-scan layout.
            if max_ordinal.is_some_and(|ordinal| ordinal >= u16::MAX as u32) {
                continue;
            }
            let mut ordinal_ranges = vec![
                PredicateScanRange::MISSING;
                max_ordinal.map_or(0, |value| value + 1) as usize
            ];
            let mut ordinal_keys = vec![None; ordinal_ranges.len()];
            let mut null_range = PredicateScanRange::MISSING;
            let mut blocks = Vec::with_capacity(column.blocks.len());
            let mut covered = roaring::RoaringBitmap::new();
            for (block_id, block) in column.blocks.into_iter().enumerate() {
                let block_id = u32::try_from(block_id).map_err(|_| {
                    paro_error::configuration_limit_exceeded(
                        "predicate scan block id exceeds the durable u32 width",
                    )
                })?;
                if block.row_ids.is_empty() {
                    return Err(paro_error::data_corrupted(format!(
                        "predicate scan column {} contains an empty block",
                        column.column_id
                    )));
                }
                let mut rows = roaring::RoaringBitmap::new();
                for &row_id in block.row_ids.iter() {
                    if row_id as usize >= point_count {
                        return Err(paro_error::data_corrupted(format!(
                            "predicate scan column {} row id exceeds point count {point_count}",
                            column.column_id
                        )));
                    }
                    if !rows.insert(row_id) {
                        return Err(paro_error::data_corrupted(format!(
                            "predicate scan column {} repeats a row inside one block",
                            column.column_id
                        )));
                    }
                }
                if !covered.is_disjoint(&rows) {
                    return Err(paro_error::data_corrupted(format!(
                        "predicate scan column {} repeats a row across blocks",
                        column.column_id
                    )));
                }
                covered |= rows;
                let mut row_start = 0u32;
                for (((ordinal, scalar_key), row_count), fingerprint) in block
                    .dictionary_ordinals
                    .iter()
                    .copied()
                    .zip(block.dictionary_values.iter())
                    .zip(block.ordinal_row_counts.iter().copied())
                    .zip(block.ordinal_fingerprints.iter().copied())
                {
                    let range = PredicateScanRange {
                        block_id,
                        row_start,
                        row_count,
                        fingerprint,
                    };
                    let slot = if ordinal == u32::MAX {
                        if scalar_key.is_some() {
                            return Err(paro_error::data_corrupted(
                                "predicate scan NULL ordinal carries a scalar key",
                            ));
                        }
                        &mut null_range
                    } else {
                        let scalar_key = scalar_key.as_ref().ok_or_else(|| {
                            paro_error::data_corrupted(
                                "predicate scan non-NULL ordinal is missing its scalar key",
                            )
                        })?;
                        let key_slot = ordinal_keys.get_mut(ordinal as usize).ok_or_else(|| {
                            paro_error::data_corrupted("predicate scan scalar-key map overflow")
                        })?;
                        if key_slot.replace(scalar_key.clone()).is_some() {
                            return Err(paro_error::data_corrupted(format!(
                                "predicate scan column {} repeats scalar key for ordinal {ordinal}",
                                column.column_id
                            )));
                        }
                        ordinal_ranges.get_mut(ordinal as usize).ok_or_else(|| {
                            paro_error::data_corrupted("predicate scan ordinal map overflow")
                        })?
                    };
                    if !slot.is_missing() {
                        return Err(paro_error::data_corrupted(format!(
                            "predicate scan column {} repeats dictionary ordinal {ordinal}",
                            column.column_id
                        )));
                    }
                    *slot = range;
                    row_start = row_start.checked_add(row_count).ok_or_else(|| {
                        paro_error::data_corrupted("predicate scan ordinal range overflow")
                    })?;
                }
                blocks.push(PredicateScanBlock::Build {
                    row_ids: block.row_ids,
                });
            }
            if covered.len() != point_count as u64 {
                return Err(paro_error::data_corrupted(format!(
                    "predicate scan column {} covers {} of {point_count} points",
                    column.column_id,
                    covered.len()
                )));
            }
            if ordinal_ranges.iter().any(|range| range.is_missing()) {
                return Err(paro_error::data_corrupted(format!(
                    "predicate scan column {} has a hole in its ordinal map",
                    column.column_id
                )));
            }
            let ordinal_keys = ordinal_keys
                .into_iter()
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    paro_error::data_corrupted(format!(
                        "predicate scan column {} has a hole in its scalar-key map",
                        column.column_id
                    ))
                })?;
            if ordinal_keys
                .windows(2)
                .any(|keys| keys[0].as_ref() >= keys[1].as_ref())
            {
                return Err(paro_error::data_corrupted(format!(
                    "predicate scan column {} scalar keys are not strictly ordered",
                    column.column_id
                )));
            }
            built_columns.push(PredicateScanColumn {
                column_id: column.column_id,
                ordinal_keys: ordinal_keys.into_boxed_slice(),
                ordinal_ranges: ordinal_ranges.into_boxed_slice(),
                null_range,
                blocks: blocks.into_boxed_slice(),
            });
        }
        if built_columns
            .windows(2)
            .any(|columns| columns[0].column_id >= columns[1].column_id)
        {
            return Err(paro_error::data_corrupted(
                "predicate scan columns must be strictly increasing",
            ));
        }
        let serialized_len = Self::encoded_len(dimension, &built_columns)?;
        Ok(Self {
            dimension,
            point_count,
            columns: built_columns.into_boxed_slice(),
            backing: None,
            build_source: Some(PredicateScanBuildSource(vector_storage)),
            integrity: None,
            serialized_len,
        })
    }

    pub(crate) fn serialized_size_bytes(&self) -> usize {
        self.serialized_len
    }

    /// Resolve one physical segment's scalar postings into generation-owned
    /// covering ranges. A generation range may contain the same scalar value
    /// from several segments, so it is sliced structurally by the immutable
    /// point-id span before cardinality is compared.
    pub(crate) fn selected_ranges_for_partition<'a, 'p>(
        &'a self,
        column_id: u32,
        point_range: std::ops::Range<PointOffset>,
        postings: impl IntoIterator<Item = &'p ExactOrdinalPosting>,
    ) -> Result<Option<Vec<PredicateScanRangeRef<'a>>>> {
        let Some(column) = self
            .columns
            .iter()
            .find(|column| column.column_id == column_id)
        else {
            return Ok(None);
        };
        let mut ranges = Vec::new();
        for posting in postings {
            let range = Self::range_for_scalar_key(column, posting.scalar_key());
            if range.is_missing() {
                return Ok(None);
            }
            let range = self
                .range_ref(column, range)?
                .restrict_to_point_range(self.dimension, point_range.clone())?;
            if range.row_ids().len() as u64 != posting.rows().len() {
                return Err(paro_error::data_corrupted(format!(
                    "predicate scan ordinal {} covers {} rows in point range {:?}, scalar posting covers {}",
                    posting.ordinal(),
                    range.row_ids().len(),
                    point_range,
                    posting.rows().len()
                )));
            }
            ranges.push(range);
        }
        ranges.sort_unstable_by_key(|range| range.row_ids().first().copied().unwrap_or(u32::MAX));
        Ok(Some(ranges))
    }

    fn range_for_scalar_key(
        column: &PredicateScanColumn,
        scalar_key: &ExactScalarKey,
    ) -> PredicateScanRange {
        match scalar_key {
            ExactScalarKey::Null => column.null_range,
            ExactScalarKey::Value(value) => column
                .ordinal_keys
                .binary_search_by(|candidate| candidate.as_ref().cmp(value.as_ref()))
                .ok()
                .and_then(|ordinal| column.ordinal_ranges.get(ordinal).copied())
                .unwrap_or(PredicateScanRange::MISSING),
        }
    }

    fn range_ref<'a>(
        &'a self,
        column: &'a PredicateScanColumn,
        range: PredicateScanRange,
    ) -> Result<PredicateScanRangeRef<'a>> {
        let block = self.block_ref(column, range.block_id as usize)?;
        let start = range.row_start as usize;
        let end = start
            .checked_add(range.row_count as usize)
            .ok_or_else(|| paro_error::data_corrupted("predicate scan ordinal range overflow"))?;
        let row_ids = block.row_ids().get(start..end).ok_or_else(|| {
            paro_error::data_corrupted("predicate scan ordinal row range exceeds its block")
        })?;
        let vector_start = start.checked_mul(self.dimension).ok_or_else(|| {
            paro_error::data_corrupted("predicate scan ordinal vector range overflow")
        })?;
        let vector_end = end.checked_mul(self.dimension).ok_or_else(|| {
            paro_error::data_corrupted("predicate scan ordinal vector range overflow")
        })?;
        let vectors = block
            .vectors()
            .get(vector_start..vector_end)
            .ok_or_else(|| {
                paro_error::data_corrupted("predicate scan ordinal vector range exceeds its block")
            })?;
        Ok(PredicateScanRangeRef { row_ids, vectors })
    }

    fn range_row_ids<'a>(
        &'a self,
        column: &'a PredicateScanColumn,
        range: PredicateScanRange,
    ) -> Result<&'a [PointOffset]> {
        let row_ids = self.block_row_ids(column, range.block_id as usize)?;
        let start = range.row_start as usize;
        let end = start
            .checked_add(range.row_count as usize)
            .ok_or_else(|| paro_error::data_corrupted("predicate scan ordinal range overflow"))?;
        row_ids.get(start..end).ok_or_else(|| {
            paro_error::data_corrupted("predicate scan ordinal row range exceeds its block")
        })
    }

    fn block_row_ids<'a>(
        &'a self,
        column: &'a PredicateScanColumn,
        block_id: usize,
    ) -> Result<&'a [PointOffset]> {
        let block = column.blocks.get(block_id).ok_or_else(|| {
            paro_error::data_corrupted("predicate scan ordinal references a missing block")
        })?;
        match block {
            PredicateScanBlock::Build { row_ids } => Ok(row_ids),
            PredicateScanBlock::Encoded {
                rows,
                row_ids_offset,
                ..
            } => {
                let bytes = self
                    .backing
                    .as_ref()
                    .expect("encoded predicate scan block retains its backing")
                    .as_bytes();
                let row_ids_end = row_ids_offset
                    .checked_add(
                        rows.checked_mul(std::mem::size_of::<PointOffset>())
                            .ok_or_else(|| {
                                paro_error::data_corrupted("predicate scan row-id range overflow")
                            })?,
                    )
                    .ok_or_else(|| {
                        paro_error::data_corrupted("predicate scan row-id range overflow")
                    })?;
                let row_bytes = bytes.get(*row_ids_offset..row_ids_end).ok_or_else(|| {
                    paro_error::data_corrupted("predicate scan row-id range exceeds backing")
                })?;
                if let Some(integrity) = &self.integrity {
                    integrity.verify(*row_ids_offset, row_ids_end - *row_ids_offset)?;
                }
                #[cfg(target_endian = "little")]
                {
                    bytemuck::try_cast_slice(row_bytes).map_err(|_| {
                        paro_error::data_corrupted("predicate scan row-id payload is unaligned")
                    })
                }
                #[cfg(not(target_endian = "little"))]
                {
                    let _ = row_bytes;
                    Err(paro_error::not_supported(
                        "predicate scan native views require little-endian storage",
                    ))
                }
            }
        }
    }

    fn block_ref<'a>(
        &'a self,
        column: &'a PredicateScanColumn,
        block_id: usize,
    ) -> Result<PredicateScanRangeRef<'a>> {
        let block = column.blocks.get(block_id).ok_or_else(|| {
            paro_error::data_corrupted("predicate scan ordinal references a missing block")
        })?;
        match block {
            PredicateScanBlock::Build { .. } => Err(paro_error::internal(
                "newly built predicate scan layout must be published before query access",
            )),
            PredicateScanBlock::Encoded {
                rows,
                row_ids_offset,
                vectors_offset,
            } => {
                let bytes = self
                    .backing
                    .as_ref()
                    .expect("encoded predicate scan block retains its backing")
                    .as_bytes();
                let row_ids_end = row_ids_offset
                    .checked_add(
                        rows.checked_mul(std::mem::size_of::<PointOffset>())
                            .ok_or_else(|| {
                                paro_error::data_corrupted("predicate scan row-id range overflow")
                            })?,
                    )
                    .ok_or_else(|| {
                        paro_error::data_corrupted("predicate scan row-id range overflow")
                    })?;
                let vectors_end = vectors_offset
                    .checked_add(
                        rows.checked_mul(self.dimension)
                            .and_then(|values| values.checked_mul(std::mem::size_of::<f32>()))
                            .ok_or_else(|| {
                                paro_error::data_corrupted("predicate scan vector range overflow")
                            })?,
                    )
                    .ok_or_else(|| {
                        paro_error::data_corrupted("predicate scan vector range overflow")
                    })?;
                let row_bytes = bytes.get(*row_ids_offset..row_ids_end).ok_or_else(|| {
                    paro_error::data_corrupted("predicate scan row-id range exceeds backing")
                })?;
                let vector_bytes = bytes.get(*vectors_offset..vectors_end).ok_or_else(|| {
                    paro_error::data_corrupted("predicate scan vector range exceeds backing")
                })?;
                if let Some(integrity) = &self.integrity {
                    integrity.verify(*row_ids_offset, row_ids_end - *row_ids_offset)?;
                    integrity.verify(*vectors_offset, vectors_end - *vectors_offset)?;
                }
                #[cfg(target_endian = "little")]
                {
                    let row_ids = bytemuck::try_cast_slice(row_bytes).map_err(|_| {
                        paro_error::data_corrupted("predicate scan row-id payload is unaligned")
                    })?;
                    let vectors = bytemuck::try_cast_slice(vector_bytes).map_err(|_| {
                        paro_error::data_corrupted("predicate scan vector payload is unaligned")
                    })?;
                    Ok(PredicateScanRangeRef { row_ids, vectors })
                }
                #[cfg(not(target_endian = "little"))]
                {
                    let _ = (row_bytes, vector_bytes);
                    Err(paro_error::not_supported(
                        "predicate scan native views require little-endian storage",
                    ))
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn serialize(&self) -> Result<Vec<u8>> {
        let mut data = Vec::with_capacity(self.serialized_len);
        self.serialize_into(&mut data)?;
        Ok(data)
    }

    pub(crate) fn serialize_into<W: Write>(&self, mut writer: W) -> Result<()> {
        let metadata_len = Self::metadata_len(&self.columns)?;
        let mut metadata = vec![0; PREDICATE_SCAN_HEADER_LEN];
        metadata[0..4].copy_from_slice(&PREDICATE_SCAN_MAGIC);
        metadata[4..8].copy_from_slice(&PREDICATE_SCAN_VERSION.to_le_bytes());
        metadata[8..12].copy_from_slice(&(PREDICATE_SCAN_HEADER_LEN as u32).to_le_bytes());
        metadata[12..16].copy_from_slice(
            &u32::try_from(self.dimension)
                .map_err(|_| paro_error::out_of_range("predicate scan dimension exceeds u32"))?
                .to_le_bytes(),
        );
        metadata[16..20].copy_from_slice(
            &u32::try_from(self.columns.len())
                .map_err(|_| paro_error::out_of_range("predicate scan column count exceeds u32"))?
                .to_le_bytes(),
        );
        metadata[24..32].copy_from_slice(&(self.point_count as u64).to_le_bytes());

        metadata[32..40].copy_from_slice(&(metadata_len as u64).to_le_bytes());
        metadata[40..48].copy_from_slice(&(self.serialized_len as u64).to_le_bytes());

        let mut payload_offset = metadata_len;
        for column in &self.columns {
            metadata.extend_from_slice(&column.column_id.to_le_bytes());
            metadata.extend_from_slice(
                &u32::try_from(column.ordinal_ranges.len())
                    .map_err(|_| {
                        paro_error::out_of_range("predicate scan ordinal count exceeds u32")
                    })?
                    .to_le_bytes(),
            );
            metadata.extend_from_slice(
                &u32::try_from(column.blocks.len())
                    .map_err(|_| {
                        paro_error::out_of_range("predicate scan block count exceeds u32")
                    })?
                    .to_le_bytes(),
            );
            metadata.extend_from_slice(&column.null_range.block_id.to_le_bytes());
            metadata.extend_from_slice(&column.null_range.row_start.to_le_bytes());
            metadata.extend_from_slice(&column.null_range.row_count.to_le_bytes());
            metadata.extend_from_slice(&column.null_range.fingerprint.to_le_bytes());
            for key in column.ordinal_keys.iter() {
                metadata.extend_from_slice(
                    &u32::try_from(key.len())
                        .map_err(|_| {
                            paro_error::out_of_range("predicate scan scalar key length exceeds u32")
                        })?
                        .to_le_bytes(),
                );
                metadata.extend_from_slice(key);
            }
            for range in column.ordinal_ranges.iter() {
                metadata.extend_from_slice(&range.block_id.to_le_bytes());
                metadata.extend_from_slice(&range.row_start.to_le_bytes());
                metadata.extend_from_slice(&range.row_count.to_le_bytes());
                metadata.extend_from_slice(&range.fingerprint.to_le_bytes());
            }
            for block in column.blocks.iter() {
                let rows = block.rows();
                let row_ids_offset = payload_offset;
                let row_bytes = rows
                    .checked_mul(std::mem::size_of::<PointOffset>())
                    .ok_or_else(|| paro_error::out_of_range("predicate scan row payload"))?;
                let vector_bytes = rows
                    .checked_mul(self.dimension)
                    .and_then(|values| values.checked_mul(std::mem::size_of::<f32>()))
                    .ok_or_else(|| paro_error::out_of_range("predicate scan vector payload"))?;
                let vectors_offset = row_ids_offset
                    .checked_add(row_bytes)
                    .ok_or_else(|| paro_error::out_of_range("predicate scan row payload"))?;
                payload_offset = vectors_offset
                    .checked_add(vector_bytes)
                    .ok_or_else(|| paro_error::out_of_range("predicate scan vector payload"))?;
                metadata.extend_from_slice(
                    &u32::try_from(rows)
                        .map_err(|_| {
                            paro_error::out_of_range("predicate scan block rows exceed u32")
                        })?
                        .to_le_bytes(),
                );
                metadata.extend_from_slice(&0_u32.to_le_bytes());
                metadata.extend_from_slice(&(row_ids_offset as u64).to_le_bytes());
                metadata.extend_from_slice(&(vectors_offset as u64).to_le_bytes());
            }
        }
        metadata.resize(metadata_len, 0);
        writer.write_all(&metadata)?;
        let mut written = metadata.len();
        let mut build_batch = Vec::new();
        for column in &self.columns {
            for (block_id, block) in column.blocks.iter().enumerate() {
                let row_ids = match block {
                    PredicateScanBlock::Build { row_ids } => row_ids.as_ref(),
                    PredicateScanBlock::Encoded { .. } => {
                        self.block_ref(column, block_id)?.row_ids()
                    }
                };
                #[cfg(target_endian = "little")]
                {
                    writer.write_all(bytemuck::cast_slice(row_ids))?;
                }
                #[cfg(not(target_endian = "little"))]
                {
                    for row_id in row_ids {
                        writer.write_all(&row_id.to_le_bytes())?;
                    }
                }
                self.write_block_vectors(&mut writer, column, block_id, block, &mut build_batch)?;
                let block_vector_bytes = row_ids
                    .len()
                    .checked_mul(self.dimension)
                    .and_then(|values| values.checked_mul(std::mem::size_of::<f32>()))
                    .ok_or_else(|| paro_error::out_of_range("predicate scan length overflow"))?;
                let block_row_bytes = row_ids
                    .len()
                    .checked_mul(std::mem::size_of::<PointOffset>())
                    .ok_or_else(|| paro_error::out_of_range("predicate scan length overflow"))?;
                written = written
                    .checked_add(block_row_bytes)
                    .and_then(|bytes| bytes.checked_add(block_vector_bytes))
                    .ok_or_else(|| paro_error::out_of_range("predicate scan length overflow"))?;
            }
        }
        if written != self.serialized_len {
            return Err(paro_error::internal(format!(
                "predicate scan encoder produced {} bytes, expected {}",
                written, self.serialized_len
            )));
        }
        Ok(())
    }

    fn write_block_vectors<W: Write>(
        &self,
        writer: &mut W,
        column: &PredicateScanColumn,
        block_id: usize,
        block: &PredicateScanBlock,
        build_batch: &mut Vec<f32>,
    ) -> Result<()> {
        match block {
            PredicateScanBlock::Encoded { .. } => {
                let vectors = self.block_ref(column, block_id)?.vectors();
                #[cfg(target_endian = "little")]
                writer.write_all(bytemuck::cast_slice(vectors))?;
                #[cfg(not(target_endian = "little"))]
                for value in vectors {
                    writer.write_all(&value.to_le_bytes())?;
                }
            }
            PredicateScanBlock::Build { row_ids } => {
                let source = &self
                    .build_source
                    .as_ref()
                    .expect("build predicate scan layout retains its vector source")
                    .0;
                let rows_per_batch = PREDICATE_SCAN_BUILD_STREAM_BYTES
                    .checked_div(
                        self.dimension
                            .checked_mul(std::mem::size_of::<f32>())
                            .ok_or_else(|| {
                                paro_error::out_of_range("predicate scan build row width overflow")
                            })?,
                    )
                    .unwrap_or(0)
                    .max(1);
                let batch_values = rows_per_batch.checked_mul(self.dimension).ok_or_else(|| {
                    paro_error::out_of_range("predicate scan build batch overflow")
                })?;
                build_batch.clear();
                if build_batch.capacity() < batch_values {
                    build_batch.try_reserve_exact(batch_values).map_err(|_| {
                        paro_error::configuration_limit_exceeded(
                            "predicate scan build stream buffer exceeds available memory",
                        )
                    })?;
                }
                for points in row_ids.chunks(rows_per_batch) {
                    build_batch.clear();
                    for &point in points {
                        build_batch.extend_from_slice(source.get_vector(point));
                    }
                    #[cfg(target_endian = "little")]
                    writer.write_all(bytemuck::cast_slice(build_batch))?;
                    #[cfg(not(target_endian = "little"))]
                    for value in build_batch.iter() {
                        writer.write_all(&value.to_le_bytes())?;
                    }
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn deserialize_bytes(bytes: Bytes) -> Result<Self> {
        Self::deserialize_backing(PredicateScanBacking::Bytes(bytes), None)
    }

    pub(crate) fn deserialize_bytes_with_integrity(
        bytes: Bytes,
        verifier: Arc<ArtifactIntegrity>,
        artifact_offset: usize,
    ) -> Result<Self> {
        Self::deserialize_backing(
            PredicateScanBacking::Bytes(bytes),
            Some(PredicateIntegrityRange {
                verifier,
                artifact_offset,
            }),
        )
    }

    pub(crate) fn deserialize_mmap_range(
        mmap: Arc<Mmap>,
        offset: usize,
        len: usize,
    ) -> Result<Self> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| paro_error::data_corrupted("predicate scan mmap range overflow"))?;
        if end > mmap.len() {
            return Err(paro_error::data_corrupted(
                "predicate scan mmap range exceeds backing",
            ));
        }
        Self::deserialize_backing(PredicateScanBacking::Mmap { mmap, offset, len }, None)
    }

    pub(crate) fn deserialize_mmap_range_with_integrity(
        mmap: Arc<Mmap>,
        offset: usize,
        len: usize,
        verifier: Arc<ArtifactIntegrity>,
        artifact_offset: usize,
    ) -> Result<Self> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| paro_error::data_corrupted("predicate scan mmap range overflow"))?;
        if end > mmap.len() {
            return Err(paro_error::data_corrupted(
                "predicate scan mmap range exceeds backing",
            ));
        }
        Self::deserialize_backing(
            PredicateScanBacking::Mmap { mmap, offset, len },
            Some(PredicateIntegrityRange {
                verifier,
                artifact_offset,
            }),
        )
    }

    pub(crate) fn save(&self, path: &Path) -> Result<()> {
        let mut file = std::fs::File::create(path).map_err(paro_error::io)?;
        self.serialize_into(&mut file)?;
        file.sync_all().map_err(paro_error::io)
    }

    pub(crate) fn load_mmap(path: &Path) -> Result<Self> {
        let file = std::fs::File::open(path).map_err(paro_error::io)?;
        let mmap = Arc::new(unsafe { MmapOptions::new().map(&file).map_err(paro_error::io)? });
        let len = mmap.len();
        Self::deserialize_mmap_range(mmap, 0, len)
    }

    fn deserialize_backing(
        backing: PredicateScanBacking,
        integrity: Option<PredicateIntegrityRange>,
    ) -> Result<Self> {
        let bytes = backing.as_bytes();
        if let Some(integrity) = &integrity {
            integrity.verify(0, PREDICATE_SCAN_HEADER_LEN)?;
        }
        if bytes.len() < PREDICATE_SCAN_HEADER_LEN || bytes[0..4] != PREDICATE_SCAN_MAGIC {
            return Err(paro_error::data_corrupted(
                "predicate scan artifact has an invalid header",
            ));
        }
        let read_u32_at = |offset: usize| {
            u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("u32 width"))
        };
        let read_u64_at = |offset: usize| {
            u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("u64 width"))
        };
        if read_u32_at(4) != PREDICATE_SCAN_VERSION
            || read_u32_at(8) as usize != PREDICATE_SCAN_HEADER_LEN
        {
            return Err(paro_error::data_corrupted(
                "predicate scan artifact version is not supported",
            ));
        }
        if bytes[20..24].iter().any(|byte| *byte != 0)
            || bytes[48..64].iter().any(|byte| *byte != 0)
        {
            return Err(paro_error::data_corrupted(
                "predicate scan header reserved bytes must be zero",
            ));
        }
        let dimension = read_u32_at(12) as usize;
        let column_count = read_u32_at(16) as usize;
        let point_count = usize::try_from(read_u64_at(24))
            .map_err(|_| paro_error::data_corrupted("predicate scan point count exceeds usize"))?;
        if dimension == 0 && point_count != 0 {
            return Err(paro_error::data_corrupted(
                "predicate scan dimension must be positive for a non-empty artifact",
            ));
        }
        let metadata_len = usize::try_from(read_u64_at(32)).map_err(|_| {
            paro_error::data_corrupted("predicate scan metadata length exceeds usize")
        })?;
        let serialized_len = usize::try_from(read_u64_at(40)).map_err(|_| {
            paro_error::data_corrupted("predicate scan artifact length exceeds usize")
        })?;
        if serialized_len != bytes.len()
            || metadata_len < PREDICATE_SCAN_HEADER_LEN
            || metadata_len > serialized_len
            || metadata_len % 64 != 0
        {
            return Err(paro_error::data_corrupted(
                "predicate scan artifact lengths are inconsistent",
            ));
        }
        if let Some(integrity) = &integrity {
            integrity.verify(0, metadata_len)?;
        }
        let mut cursor = PREDICATE_SCAN_HEADER_LEN;
        let mut payload_cursor = metadata_len;
        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            if cursor + COLUMN_HEADER_LEN > metadata_len {
                return Err(paro_error::data_corrupted(
                    "predicate scan column metadata is truncated",
                ));
            }
            let column_id = read_u32_at(cursor);
            if columns
                .last()
                .is_some_and(|column: &PredicateScanColumn| column.column_id >= column_id)
            {
                return Err(paro_error::data_corrupted(
                    "predicate scan columns must be strictly increasing",
                ));
            }
            let ordinal_count = read_u32_at(cursor + 4) as usize;
            let block_count = read_u32_at(cursor + 8) as usize;
            let null_range = PredicateScanRange {
                block_id: read_u32_at(cursor + 12),
                row_start: read_u32_at(cursor + 16),
                row_count: read_u32_at(cursor + 20),
                fingerprint: read_u64_at(cursor + 24),
            };
            cursor += COLUMN_HEADER_LEN;
            let mut ordinal_keys = Vec::with_capacity(ordinal_count);
            for _ in 0..ordinal_count {
                if cursor + std::mem::size_of::<u32>() > metadata_len {
                    return Err(paro_error::data_corrupted(
                        "predicate scan scalar-key length is truncated",
                    ));
                }
                let key_len = read_u32_at(cursor) as usize;
                cursor += std::mem::size_of::<u32>();
                let key_end = cursor.checked_add(key_len).ok_or_else(|| {
                    paro_error::data_corrupted("predicate scan scalar-key range overflow")
                })?;
                let key = bytes.get(cursor..key_end).ok_or_else(|| {
                    paro_error::data_corrupted("predicate scan scalar-key payload is truncated")
                })?;
                if key_end > metadata_len {
                    return Err(paro_error::data_corrupted(
                        "predicate scan scalar-key payload exceeds metadata",
                    ));
                }
                if ordinal_keys
                    .last()
                    .is_some_and(|previous: &Bytes| previous.as_ref() >= key)
                {
                    return Err(paro_error::data_corrupted(
                        "predicate scan scalar keys must be strictly ordered",
                    ));
                }
                ordinal_keys.push(Bytes::copy_from_slice(key));
                cursor = key_end;
            }
            let ordinal_bytes = ordinal_count
                .checked_mul(ORDINAL_RANGE_LEN)
                .ok_or_else(|| paro_error::data_corrupted("predicate scan ordinal map overflow"))?;
            if cursor + ordinal_bytes > metadata_len {
                return Err(paro_error::data_corrupted(
                    "predicate scan ordinal map is truncated",
                ));
            }
            let mut ordinal_ranges = Vec::with_capacity(ordinal_count);
            let mut referenced_blocks = vec![false; block_count];
            for _ in 0..ordinal_count {
                let range = PredicateScanRange {
                    block_id: read_u32_at(cursor),
                    row_start: read_u32_at(cursor + 4),
                    row_count: read_u32_at(cursor + 8),
                    fingerprint: read_u64_at(cursor + 12),
                };
                if range.block_id as usize >= block_count || range.row_count == 0 {
                    return Err(paro_error::data_corrupted(
                        "predicate scan ordinal references an invalid block range",
                    ));
                }
                referenced_blocks[range.block_id as usize] = true;
                ordinal_ranges.push(range);
                cursor += ORDINAL_RANGE_LEN;
            }
            if null_range.is_missing() {
                if null_range.row_start != 0
                    || null_range.row_count != 0
                    || null_range.fingerprint != 0
                {
                    return Err(paro_error::data_corrupted(
                        "predicate scan missing NULL range has a non-zero payload",
                    ));
                }
            } else if null_range.block_id as usize >= block_count || null_range.row_count == 0 {
                return Err(paro_error::data_corrupted(
                    "predicate scan NULL ordinal references an invalid block range",
                ));
            } else {
                referenced_blocks[null_range.block_id as usize] = true;
            }
            let mut blocks = Vec::with_capacity(block_count);
            let mut column_rows = 0usize;
            for _ in 0..block_count {
                if cursor + BLOCK_HEADER_LEN > metadata_len {
                    return Err(paro_error::data_corrupted(
                        "predicate scan block metadata is truncated",
                    ));
                }
                let rows = read_u32_at(cursor) as usize;
                if read_u32_at(cursor + 4) != 0 {
                    return Err(paro_error::data_corrupted(
                        "predicate scan block reserved bytes must be zero",
                    ));
                }
                let row_ids_offset = usize::try_from(read_u64_at(cursor + 8)).map_err(|_| {
                    paro_error::data_corrupted("predicate scan row offset exceeds usize")
                })?;
                let vectors_offset = usize::try_from(read_u64_at(cursor + 16)).map_err(|_| {
                    paro_error::data_corrupted("predicate scan vector offset exceeds usize")
                })?;
                cursor += BLOCK_HEADER_LEN;
                let row_bytes = rows
                    .checked_mul(std::mem::size_of::<PointOffset>())
                    .ok_or_else(|| {
                        paro_error::data_corrupted("predicate scan row range overflow")
                    })?;
                let vector_bytes = rows
                    .checked_mul(dimension)
                    .and_then(|values| values.checked_mul(std::mem::size_of::<f32>()))
                    .ok_or_else(|| {
                        paro_error::data_corrupted("predicate scan vector range overflow")
                    })?;
                let expected_vectors_offset =
                    payload_cursor.checked_add(row_bytes).ok_or_else(|| {
                        paro_error::data_corrupted("predicate scan row range overflow")
                    })?;
                let expected_end = expected_vectors_offset
                    .checked_add(vector_bytes)
                    .ok_or_else(|| {
                        paro_error::data_corrupted("predicate scan vector range overflow")
                    })?;
                if rows == 0
                    || row_ids_offset != payload_cursor
                    || vectors_offset != expected_vectors_offset
                    || expected_end > serialized_len
                {
                    return Err(paro_error::data_corrupted(
                        "predicate scan block payload ranges are inconsistent",
                    ));
                }
                blocks.push(PredicateScanBlock::Encoded {
                    rows,
                    row_ids_offset,
                    vectors_offset,
                });
                payload_cursor = expected_end;
                column_rows = column_rows.saturating_add(rows);
            }
            if column_rows != point_count {
                return Err(paro_error::data_corrupted(format!(
                    "predicate scan column {column_id} covers {column_rows} of {point_count} points"
                )));
            }
            if referenced_blocks.iter().any(|referenced| !referenced) {
                return Err(paro_error::data_corrupted(
                    "predicate scan contains an unreachable scalar block",
                ));
            }
            let mut ranges_by_block = vec![Vec::new(); block_count];
            for range in ordinal_ranges
                .iter()
                .copied()
                .chain((!null_range.is_missing()).then_some(null_range))
            {
                ranges_by_block[range.block_id as usize].push((range.row_start, range.row_count));
            }
            for (block_id, ranges) in ranges_by_block.iter_mut().enumerate() {
                ranges.sort_unstable();
                let mut expected_start = 0u32;
                for &(start, count) in ranges.iter() {
                    if start != expected_start {
                        return Err(paro_error::data_corrupted(
                            "predicate scan ordinal ranges do not partition their block",
                        ));
                    }
                    expected_start = expected_start.checked_add(count).ok_or_else(|| {
                        paro_error::data_corrupted("predicate scan ordinal range overflow")
                    })?;
                }
                if expected_start as usize != blocks[block_id].rows() {
                    return Err(paro_error::data_corrupted(
                        "predicate scan ordinal ranges do not cover their block",
                    ));
                }
            }
            columns.push(PredicateScanColumn {
                column_id,
                ordinal_keys: ordinal_keys.into_boxed_slice(),
                ordinal_ranges: ordinal_ranges.into_boxed_slice(),
                null_range,
                blocks: blocks.into_boxed_slice(),
            });
        }
        if cursor > metadata_len
            || bytes[cursor..metadata_len].iter().any(|byte| *byte != 0)
            || payload_cursor != serialized_len
        {
            return Err(paro_error::data_corrupted(
                "predicate scan metadata padding or payload length is invalid",
            ));
        }
        Ok(Self {
            dimension,
            point_count,
            columns: columns.into_boxed_slice(),
            backing: Some(backing),
            build_source: None,
            integrity,
            serialized_len,
        })
    }

    fn metadata_len(columns: &[PredicateScanColumn]) -> Result<usize> {
        let mut len = PREDICATE_SCAN_HEADER_LEN;
        for column in columns {
            let ordinal_bytes = column
                .ordinal_ranges
                .len()
                .checked_mul(ORDINAL_RANGE_LEN)
                .ok_or_else(|| paro_error::out_of_range("predicate scan metadata length"))?;
            let block_bytes = column
                .blocks
                .len()
                .checked_mul(BLOCK_HEADER_LEN)
                .ok_or_else(|| paro_error::out_of_range("predicate scan metadata length"))?;
            let scalar_key_bytes = column.ordinal_keys.iter().try_fold(0usize, |bytes, key| {
                bytes
                    .checked_add(std::mem::size_of::<u32>())
                    .and_then(|bytes| bytes.checked_add(key.len()))
                    .ok_or_else(|| paro_error::out_of_range("predicate scan metadata length"))
            })?;
            len = len
                .checked_add(COLUMN_HEADER_LEN)
                .and_then(|len| len.checked_add(scalar_key_bytes))
                .and_then(|len| len.checked_add(ordinal_bytes))
                .and_then(|len| len.checked_add(block_bytes))
                .ok_or_else(|| paro_error::out_of_range("predicate scan metadata length"))?;
        }
        len.checked_add(63)
            .map(|value| value / 64 * 64)
            .ok_or_else(|| paro_error::out_of_range("predicate scan metadata alignment"))
    }

    fn encoded_len(dimension: usize, columns: &[PredicateScanColumn]) -> Result<usize> {
        let mut len = Self::metadata_len(columns)?;
        for column in columns {
            for block in column.blocks.iter() {
                let row_bytes = block
                    .rows()
                    .checked_mul(std::mem::size_of::<PointOffset>())
                    .ok_or_else(|| paro_error::out_of_range("predicate scan artifact length"))?;
                let vector_bytes = block
                    .rows()
                    .checked_mul(dimension)
                    .and_then(|values| values.checked_mul(std::mem::size_of::<f32>()))
                    .ok_or_else(|| paro_error::out_of_range("predicate scan artifact length"))?;
                len = len
                    .checked_add(row_bytes)
                    .and_then(|len| len.checked_add(vector_bytes))
                    .ok_or_else(|| paro_error::out_of_range("predicate scan artifact length"))?;
            }
        }
        Ok(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::hnsw::InMemoryVectorStorage;
    use crate::index::{ExactRowPartitions, ExactRowSet, OrdinalRowSet};
    use roaring::RoaringBitmap;

    fn test_dictionary_values(ordinals: &[u16]) -> Box<[Option<Bytes>]> {
        ordinals
            .iter()
            .map(|ordinal| Some(Bytes::copy_from_slice(&ordinal.to_le_bytes())))
            .collect()
    }

    fn build_layout() -> PredicateScanLayout {
        let layout = PredicateScanLayout::from_build_columns(
            2,
            4,
            vec![PredicateScanBuildColumn {
                column_id: 7,
                blocks: vec![PredicateScanBuildBlock {
                    dictionary_ordinals: vec![0, 1].into_boxed_slice(),
                    dictionary_values: test_dictionary_values(&[0, 1]),
                    ordinal_row_counts: vec![2, 2].into_boxed_slice(),
                    ordinal_fingerprints: vec![
                        crate::index::bitmap::posting_fingerprint(&RoaringBitmap::from_iter([
                            0, 2,
                        ])),
                        crate::index::bitmap::posting_fingerprint(&RoaringBitmap::from_iter([
                            1, 3,
                        ])),
                    ]
                    .into_boxed_slice(),
                    row_ids: vec![0, 2, 1, 3].into_boxed_slice(),
                }],
            }],
            Arc::new(InMemoryVectorStorage::new(
                vec![0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5],
                2,
            )),
        )
        .unwrap();
        PredicateScanLayout::deserialize_bytes(Bytes::from(layout.serialize().unwrap())).unwrap()
    }

    #[test]
    fn covering_layout_roundtrip_selects_blocks_by_dictionary_ordinal() {
        let encoded = build_layout().serialize().unwrap();
        let restored = PredicateScanLayout::deserialize_bytes(Bytes::from(encoded)).unwrap();
        let postings = [ExactOrdinalPosting::new(
            1,
            Arc::new(RoaringBitmap::from_iter([1, 3])),
        )];
        let blocks = restored
            .selected_ranges_for_partition(7, 0..4, &postings)
            .unwrap()
            .expect("configured ordinal has a covering block");

        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].row_ids(), &[1, 3]);
        assert_eq!(blocks[0].vectors(), &[1.0, 1.5, 3.0, 3.5]);
    }

    #[test]
    fn covering_layout_validates_shifted_partition_posting_unions() {
        let local_row_ordinals = Arc::from([0_u16, 1]);
        let first: Arc<dyn ExactRowSet> = Arc::new(OrdinalRowSet::new(
            7,
            Arc::clone(&local_row_ordinals),
            vec![0b10].into_boxed_slice(),
            false,
            1,
            vec![ExactOrdinalPosting::new(
                1,
                Arc::new(RoaringBitmap::from_iter([1])),
            )]
            .into_boxed_slice(),
        ));
        let second: Arc<dyn ExactRowSet> = Arc::new(OrdinalRowSet::new(
            7,
            local_row_ordinals,
            vec![0b10].into_boxed_slice(),
            false,
            1,
            vec![ExactOrdinalPosting::new(
                1,
                Arc::new(RoaringBitmap::from_iter([1])),
            )]
            .into_boxed_slice(),
        ));
        let layout = build_layout();
        let ExactRowPartitions::OrdinalSelection(first_selection) = first.physical_partitions()
        else {
            panic!("expected ordinal postings")
        };
        let ExactRowPartitions::OrdinalSelection(second_selection) = second.physical_partitions()
        else {
            panic!("expected ordinal postings")
        };
        let first_ranges = layout
            .selected_ranges_for_partition(7, 0..2, first_selection.selected_postings())
            .unwrap()
            .expect("first local posting maps into the generation range");
        let second_ranges = layout
            .selected_ranges_for_partition(7, 2..4, second_selection.selected_postings())
            .unwrap()
            .expect("second local posting maps into the generation range");
        assert_eq!(first_ranges[0].row_ids(), &[1]);
        assert_eq!(first_ranges[0].vectors(), &[1.0, 1.5]);
        assert_eq!(second_ranges[0].row_ids(), &[3]);
        assert_eq!(second_ranges[0].vectors(), &[3.0, 3.5]);
    }

    #[test]
    fn covering_layout_maps_by_scalar_key_instead_of_segment_ordinal() {
        let restored = PredicateScanLayout::deserialize_bytes(Bytes::from(
            build_layout().serialize().unwrap(),
        ))
        .unwrap();
        let local_ordinal_differs = [ExactOrdinalPosting::from_index(
            91,
            ExactScalarKey::Value(Bytes::copy_from_slice(&0_u16.to_le_bytes())),
            Arc::new(RoaringBitmap::from_iter([0, 2])),
        )];
        let ranges = restored
            .selected_ranges_for_partition(7, 0..4, &local_ordinal_differs)
            .unwrap()
            .expect("canonical scalar key maps independently of local ordinal");
        assert_eq!(ranges[0].row_ids(), &[0, 2]);
    }

    #[test]
    fn covering_layout_rejects_duplicate_row_coverage() {
        let error = PredicateScanLayout::from_build_columns(
            1,
            2,
            vec![PredicateScanBuildColumn {
                column_id: 7,
                blocks: vec![
                    PredicateScanBuildBlock {
                        dictionary_ordinals: vec![0].into_boxed_slice(),
                        dictionary_values: test_dictionary_values(&[0]),
                        ordinal_row_counts: vec![2].into_boxed_slice(),
                        ordinal_fingerprints: vec![crate::index::bitmap::posting_fingerprint(
                            &RoaringBitmap::from_iter([0, 1]),
                        )]
                        .into_boxed_slice(),
                        row_ids: vec![0, 1].into_boxed_slice(),
                    },
                    PredicateScanBuildBlock {
                        dictionary_ordinals: vec![1].into_boxed_slice(),
                        dictionary_values: test_dictionary_values(&[1]),
                        ordinal_row_counts: vec![1].into_boxed_slice(),
                        ordinal_fingerprints: vec![crate::index::bitmap::posting_fingerprint(
                            &RoaringBitmap::from_iter([1]),
                        )]
                        .into_boxed_slice(),
                        row_ids: vec![1].into_boxed_slice(),
                    },
                ],
            }],
            Arc::new(InMemoryVectorStorage::new(vec![0.0, 1.0], 1)),
        )
        .unwrap_err();
        assert!(error.to_string().contains("repeats a row"));
    }
}
